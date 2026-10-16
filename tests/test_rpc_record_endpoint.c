#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_endpoint.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define CAP 6U
#define BODY 4096U

/* 包装只属于本目标；正常路径使用真实 Channel/Reactor。刻意损坏的是
 * 客户端已编码、尚未提交的私有 OPEN 前缀，以检验实际线上的拒绝响应。 */
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __real_tr_stream_close(struct tr_stream_handle);
static _Atomic(struct tr_channel *) server_channel, client_channel;
static _Atomic unsigned corrupt_open, blocked_kind, block_close;
static _Atomic unsigned attempts[6], accepted[6], close_attempts, closes;
static _Atomic unsigned sequence, final_seq, close_seq;
static _Atomic int blocked_fd = -1;
static _Atomic unsigned blocked_writes, abort_kind;
static struct tr_conn_handle server_connection;
static _Atomic bool hold_final_transmit;
static int server_fd;
static _Atomic(struct tr_reactor *) server_reactor;
static _Atomic bool fail_poll;
static _Atomic unsigned poll_faults;
int __real_epoll_wait(int, struct epoll_event *, int, int);
int __wrap_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout)
{
	if (tr_reactor_in_owner_context_for(atomic_load(&server_reactor)) &&
	    atomic_exchange(&fail_poll, false)) {
		atomic_fetch_add(&poll_faults, 1);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, count, timeout);
}
static _Thread_local bool tracking_endpoint;
static struct tr_reactor_timer_handle endpoint_timer;
static _Atomic bool fail_detach, fail_timer;
static _Atomic unsigned timer_ticks;
static tr_reactor_timer_cb real_endpoint_tick;
static void *endpoint_tick_arg;

ssize_t __real_sendmsg(int, const struct msghdr *, int);
ssize_t __wrap_sendmsg(int fd, const struct msghdr *msg, int flags)
{
	if (fd == atomic_load(&blocked_fd)) {
		atomic_fetch_add(&blocked_writes, 1);
		errno = EAGAIN;
		return -1;
	}
	return __real_sendmsg(fd, msg, flags);
}
int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
int __wrap_tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb channel_event, void *arg)
{
	if (c == atomic_load(&server_channel) && !data &&
	    atomic_load(&fail_detach))
		return TR_ERR_SYS;
	return __real_tr_channel_set_handler(c, data, event, channel_event,
	                                     arg);
}
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
static uint64_t observed_tick(void *arg, uint64_t now)
{
	(void)arg;
	atomic_fetch_add(&timer_ticks, 1);
	return real_endpoint_tick(endpoint_tick_arg, now);
}
int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	if (!tracking_endpoint)
		return __real_tr_reactor_timer_register(r, cb, arg, out);
	real_endpoint_tick = cb;
	endpoint_tick_arg = arg;
	int ret = __real_tr_reactor_timer_register(r, observed_tick, NULL, out);
	if (ret == TR_OK)
		endpoint_timer = *out;
	return ret;
}
int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle h)
{
	if (h.reactor == endpoint_timer.reactor &&
	    h.slot == endpoint_timer.slot &&
	    h.generation == endpoint_timer.generation &&
	    atomic_load(&fail_timer))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_unregister(h);
}
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *b, uint32_t n)
{
	unsigned kind = 0;
	if (n && b[0]->len >= TR_RPC_RECORD_HEADER_SIZE &&
	    !memcmp(b[0]->data, "TRPC", 4)) {
		kind = tr_get_le16(b[0]->data + 6);
		CHECK(kind >= 1 && kind <= 5);
		if (stream.channel == atomic_load(&client_channel) &&
		    kind == TR_RPC_RECORD_OPEN) {
			unsigned fault = atomic_exchange(&corrupt_open, 0);
			if (fault == 1)
				tr_put_le32(b[0]->data + 12, 9999);
			if (fault == 2) {
				CHECK(b[0]->len > 51);
				b[0]->data[51] = ':';
			}
			if (fault == 3)
				tr_put_le32(b[0]->data + 32,
				            TR_RPC_METADATA_MAX_BYTES + 1);
		}
		if (stream.channel == atomic_load(&server_channel)) {
			atomic_fetch_add(&attempts[kind], 1);
			if (kind == TR_RPC_RECORD_FINAL_STATUS &&
			    atomic_exchange(&hold_final_transmit, false))
				atomic_store(&blocked_fd, server_fd);
			if (kind == atomic_load(&abort_kind)) {
				atomic_store(&abort_kind, 0);
				CHECK(tr_reactor_abort_on_owner(
				              server_connection, TR_ERR_SYS) ==
				      TR_OK);
				return TR_ERR_CLOSED;
			}
			if (kind == atomic_load(&blocked_kind))
				return TR_AGAIN;
		}
	}
	int ret = __real_tr_stream_sendv(stream, b, n);
	if (ret == TR_OK && kind &&
	    stream.channel == atomic_load(&server_channel)) {
		atomic_fetch_add(&accepted[kind], 1);
		if (kind == TR_RPC_RECORD_FINAL_STATUS)
			atomic_store(&final_seq,
			             atomic_fetch_add(&sequence, 1) + 1);
	}
	return ret;
}
int __wrap_tr_stream_close(struct tr_stream_handle stream)
{
	bool observed = stream.channel == atomic_load(&server_channel);
	if (observed) {
		atomic_fetch_add(&close_attempts, 1);
		if (atomic_load(&block_close))
			return TR_AGAIN;
	}
	int ret = __real_tr_stream_close(stream);
	if (observed && ret == TR_OK) {
		atomic_fetch_add(&closes, 1);
		atomic_store(&close_seq, atomic_fetch_add(&sequence, 1) + 1);
	}
	return ret;
}
struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered, open;
};
static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void wait_count(_Atomic unsigned *value, unsigned target)
{
	uint64_t end = now_ms() + 15000;
	while (atomic_load(value) < target) {
		CHECK(now_ms() < end);
		sched_yield();
	}
}
static void gate_init(struct gate *g)
{
	pthread_condattr_t attr;
	*g = (struct gate){0};
	CHECK(pthread_mutex_init(&g->lock, NULL) == 0);
	CHECK(pthread_condattr_init(&attr) == 0);
	CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	CHECK(pthread_cond_init(&g->cond, &attr) == 0);
	CHECK(pthread_condattr_destroy(&attr) == 0);
}
static void gate_wait(struct gate *g, bool worker)
{
	struct timespec end;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += 15;
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	if (worker) {
		g->entered = true;
		CHECK(pthread_cond_broadcast(&g->cond) == 0);
	}
	while (worker ? !g->open : !g->entered)
		CHECK(pthread_cond_timedwait(&g->cond, &g->lock, &end) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}
static void gate_open(struct gate *g)
{
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	g->open = true;
	CHECK(pthread_cond_broadcast(&g->cond) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}
static void gate_destroy(struct gate *g)
{
	CHECK(pthread_cond_destroy(&g->cond) == 0);
	CHECK(pthread_mutex_destroy(&g->lock) == 0);
}

struct fixture;
struct method_context {
	struct fixture *f;
	unsigned index;
};
struct business {
	_Atomic unsigned active, seen[6];
	uint8_t data[BODY];
	size_t size;
};
struct peer {
	struct fixture *f;
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_conn_handle connection;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	unsigned method[CAP];
	int fd;
	_Atomic unsigned opened, terminals[CAP], messages[CAP], headers[CAP],
	        remote_closed[CAP];
	enum tr_rpc_status status[CAP];
	enum tr_rpc_record_delivery delivery[CAP];
	size_t received_bytes[CAP];
	bool server, disabled, hold;
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
};
struct fixture {
	struct peer client, server;
	struct tr_task_executor *executor;
	struct tr_rpc_record_endpoint *endpoint;
	struct tr_rpc_record_endpoint_config config;
	struct tr_rpc_dispatch_method methods[4];
	struct method_context contexts[4];
	struct business business[CAP];
	struct tr_rpc_endpoint_result results[CAP];
	uint64_t executor_budget;
	struct gate gate;
	_Atomic bool use_gate;
	_Atomic unsigned work_error;
};
static enum tr_rpc_status work(struct tr_rpc_dispatch_id id,
                               const struct tr_rpc_record_view *view,
                               const struct tr_task_cancel *cancel,
                               struct tr_rpc_record_reply *reply, void *arg)
{
	struct method_context *m = arg;
	struct fixture *f = m->f;
	struct business *b = &f->business[id.slot];
	CHECK(tr_task_in_worker_context() && !tr_reactor_in_owner_context());
	CHECK(atomic_fetch_add(&b->active, 1) == 0);
	unsigned kind = view->header.kind;
	enum tr_rpc_status status = TR_RPC_STATUS_OK;
	if (kind == TR_RPC_RECORD_OPEN)
		b->size = 0;
	if (kind == TR_RPC_RECORD_MESSAGE) {
		if (atomic_exchange(&f->use_gate, false))
			gate_wait(&f->gate, true);
		b->size = view->header.decoded_size;
		CHECK(b->size <= BODY);
		for (size_t i = 0; i < b->size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7 + 3));
		if (b->size)
			memcpy(b->data, view->body, b->size);
		if (f->methods[m->index].descriptor.response_cardinality ==
		    TR_RPC_MANY)
			CHECK(tr_rpc_record_reply_append(reply, b->data,
			                                 b->size) == TR_OK);
	}
	if (kind == TR_RPC_RECORD_HALF_CLOSE &&
	    f->methods[m->index].descriptor.response_cardinality == TR_RPC_ONE)
		CHECK(tr_rpc_record_reply_append(reply, b->data, b->size) ==
		      TR_OK);
	if (kind == TR_RPC_RECORD_OPEN && atomic_load(&f->work_error))
		status = (enum tr_rpc_status)atomic_load(&f->work_error);
	(void)cancel; /* 迟到成功必须由调度终局处理，不能依赖业务自觉停止。 */
	atomic_fetch_add(&b->seen[kind], 1);
	CHECK(atomic_fetch_sub(&b->active, 1) == 1);
	return status;
}
static void endpoint_terminal(struct tr_stream_handle s,
                              struct tr_rpc_endpoint_result result, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_endpoint *same = f->endpoint;
	CHECK(s.channel == f->server.channel && s.slot < CAP);
	CHECK((result.delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (result.delivery == TR_RPC_RECORD_ON_OWNER) {
		CHECK(tr_rpc_record_endpoint_close_on_owner(&same) ==
		      TR_ERR_STATE);
	} else {
		CHECK(result.status == TR_RPC_STATUS_CANCELLED);
		CHECK(tr_rpc_record_endpoint_reap_stopped(&same) ==
		      TR_ERR_STATE);
	}
	CHECK(same == f->endpoint);
	f->server.streams[s.slot] = s;
	f->results[s.slot] = result;
	CHECK(atomic_fetch_add(&f->server.terminals[s.slot], 1) == 0);
}
static unsigned client_slot(struct peer *p, struct tr_rpc_record_stream *call)
{
	for (unsigned i = 0; i < CAP; ++i)
		if (p->calls[i] == call)
			return i;
	CHECK(0);
	return 0;
}
static void client_received(struct tr_rpc_record_stream *call,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	CHECK(!atomic_load(&p->remote_closed[i]));
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		for (size_t j = 0; j < view->header.decoded_size; ++j)
			CHECK(view->body[j] == (uint8_t)(j * 7 + 3));
		p->received_bytes[i] += view->header.decoded_size;
		atomic_fetch_add(&p->messages[i], 1);
	}
	if (view->header.kind == TR_RPC_RECORD_INITIAL_METADATA)
		atomic_fetch_add(&p->headers[i], 1);
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	p->status[i] = status;
	p->delivery[i] = delivery;
	CHECK(atomic_fetch_add(&p->terminals[i], 1) == 0);
}
static enum tr_stream_data_disposition client_data(struct tr_stream_handle s,
                                                   uint64_t id,
                                                   struct tr_buffer *payload,
                                                   void *arg)
{
	struct peer *p = arg;
	(void)id;
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(s.slot < CAP && p->calls[s.slot]);
	CHECK(tr_rpc_record_stream_receive_on_owner(p->calls[s.slot], s,
	                                            payload->data,
	                                            payload->len) == TR_OK);
	if (p->hold) {
		CHECK(!p->held);
		p->held = payload;
		p->held_stream = s;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void client_event(struct tr_stream_handle s, enum tr_stream_event e,
                         int status, void *arg)
{
	struct peer *p = arg;
	(void)status;
	if (p->disabled)
		return;
	CHECK(s.slot < CAP);
	if (e == TR_STREAM_EVENT_OPENED) {
		p->streams[s.slot] = s;
		/* OPENED 是 ACK 时机；绑定先在原 owner 上建立，这里接受 OPEN，
		 * 再允许同批次早到拒绝 FINAL 被解析，测试本身不制造跨轮次窗口。
		 */
		CHECK(p->calls[s.slot]);
		const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
		struct tr_rpc_record_header h = {
		        .kind = TR_RPC_RECORD_OPEN,
		        .service_id = 7,
		        .method_id = 9 + p->method[s.slot],
		        .metadata_size = sizeof(metadata)};
		CHECK(tr_rpc_record_stream_send_on_owner(
		              p->calls[s.slot], &h, metadata, NULL) == TR_OK);
		atomic_fetch_add(&p->opened, 1);
	} else if (p->calls[s.slot]) {
		if (e == TR_STREAM_EVENT_REMOTE_CLOSED)
			atomic_fetch_add(&p->remote_closed[s.slot], 1);
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[s.slot], s,
		                                          e) == TR_OK);
	}
}
static void tcp_pair(int *a, int *b)
{
	int listener = -1, ret;
	uint16_t port;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) == TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, a);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	uint64_t until = now_ms() + 15000;
	while (tr_tcp_accept(listener, b) == TR_AGAIN) {
		struct pollfd fd = {listener, POLLIN, 0};
		CHECK(now_ms() < until && poll(&fd, 1, 100) >= 0);
	}
	CHECK(*b >= 0);
	if (ret == TR_IN_PROGRESS) {
		struct pollfd fd = {*a, POLLOUT, 0};
		CHECK(poll(&fd, 1, 1000) == 1);
		CHECK(tr_tcp_finish_connect(*a) == TR_OK);
	}
	CHECK(close(listener) == 0);
}

static void peer_init(struct peer *p, struct fixture *f, int fd, bool server,
                      uint64_t window)
{
	struct tr_reactor_config rc = {.max_connections = 4,
	                               .command_capacity = 64,
	                               .tx_item_capacity = 64,
	                               .control_tx_item_capacity = 64,
	                               .rx_buffer_count = 16,
	                               .rx_buffer_size = 256,
	                               .max_payload_len = 256};
	struct tr_channel_config cc = {.role = server ? TR_CHANNEL_SERVER
	                                              : TR_CHANNEL_CLIENT,
	                               .mode = TR_CHANNEL_SHARED_CONNECTION,
	                               .max_streams = CAP,
	                               .initial_window_bytes = window,
	                               .window_update_threshold_bytes = 1,
	                               .max_message_bytes = BODY + 1024};
	*p = (struct peer){.f = f, .server = server, .fd = fd};
	atomic_init(&p->opened, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->messages[i], 0);
		atomic_init(&p->headers[i], 0);
		atomic_init(&p->remote_closed[i], 0);
	}
	tr_memory_budget_init(&p->budget, 32 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &p->connection) == TR_OK);
	if (server)
		CHECK(tr_channel_create_deferred(
		              &cc, p->connection, p->connection, NULL, NULL,
		              NULL, NULL, &p->channel) == TR_OK);
	else
		CHECK(tr_channel_create(&cc, p->connection, p->connection,
		                        client_data, client_event, NULL, p,
		                        &p->channel) == TR_OK);
}
static int start_server(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config c = {2, 4};
	CHECK(tr_task_executor_create(&c, &f->server.budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	f->executor_budget = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_rpc_record_endpoint_create_on_owner(
	              f->server.channel, f->executor, f->methods, 4, &f->config,
	              endpoint_terminal, f, &f->server.budget,
	              &f->endpoint) == TR_OK);
	tracking_endpoint = true;
	CHECK(tr_rpc_record_endpoint_start_on_owner(f->endpoint) == TR_OK);
	tracking_endpoint = false;
	return tr_channel_start(f->server.channel);
}
static void fixture_init_full(struct fixture *f, uint64_t window,
                              uint32_t capacity, uint64_t final_wait)
{
	int a = -1, b = -1;
	*f = (struct fixture){
	        .config = {
	                {capacity, 8, 4, BODY * 4}, 2, 2, 1000000, final_wait}};
	gate_init(&f->gate);
	atomic_init(&f->use_gate, false);
	atomic_init(&f->work_error, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&f->business[i].active, 0);
		for (unsigned j = 0; j < 6; ++j)
			atomic_init(&f->business[i].seen[j], 0);
	}
	for (unsigned i = 0; i < 4; ++i) {
		f->contexts[i] = (struct method_context){f, i};
		f->methods[i] = (struct tr_rpc_dispatch_method){
		        {7, 9 + i, i < 2 ? TR_RPC_ONE : TR_RPC_MANY,
		         (i & 1) ? TR_RPC_MANY : TR_RPC_ONE, TR_RPC_CODEC_RAW,
		         TR_RPC_CODEC_RAW, TR_LANE_BULK, BODY, BODY},
		        work,
		        &f->contexts[i]};
	}
	atomic_store(&blocked_kind, 0);
	atomic_store(&block_close, 0);
	atomic_store(&corrupt_open, 0);
	atomic_store(&close_attempts, 0);
	atomic_store(&closes, 0);
	atomic_store(&sequence, 0);
	atomic_store(&blocked_fd, -1);
	atomic_store(&blocked_writes, 0);
	atomic_store(&abort_kind, 0);
	atomic_store(&fail_detach, false);
	atomic_store(&fail_timer, false);
	atomic_store(&timer_ticks, 0);
	endpoint_timer = (struct tr_reactor_timer_handle){0};
	atomic_store(&final_seq, 0);
	atomic_store(&close_seq, 0);
	for (unsigned i = 0; i < 6; ++i) {
		atomic_store(&attempts[i], 0);
		atomic_store(&accepted[i], 0);
	}
	tcp_pair(&a, &b);
	peer_init(&f->client, f, a, false, window);
	peer_init(&f->server, f, b, true, window);
	server_connection = f->server.connection;
	atomic_store(&server_reactor, f->server.reactor);
	atomic_store(&fail_poll, false);
	atomic_store(&poll_faults, 0);
	server_fd = f->server.fd;
	atomic_store(&hold_final_transmit, false);
	atomic_store(&server_channel, f->server.channel);
	atomic_store(&client_channel, f->client.channel);
	CHECK(tr_reactor_call(f->server.reactor, start_server, f) == TR_OK);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
}
static void fixture_init(struct fixture *f, uint64_t window, uint32_t capacity)
{
	fixture_init_full(f, window, capacity, 1000000000);
}
struct client_create {
	struct fixture *f;
	struct tr_stream_handle stream;
	unsigned method;
};
static int bind_client(void *arg)
{
	struct client_create *r = arg;
	struct peer *p = &r->f->client;
	struct tr_rpc_record_stream_config cfg = {
	        TR_RPC_RECORD_REQUEST_DIRECTION,
	        r->f->methods[r->method].descriptor};
	struct tr_rpc_record_stream_callbacks cb = {client_received,
	                                            client_terminal, p};
	return tr_rpc_record_stream_create_on_owner(
	        r->stream, &cfg, &cb, &p->budget, &p->calls[r->stream.slot]);
}
static int open_on_owner(void *arg)
{
	struct client_create *r = arg;
	int ret =
	        tr_stream_open(r->f->client.channel, TR_LANE_BULK, &r->stream);
	if (ret != TR_OK)
		return ret;
	r->f->client.method[r->stream.slot] = r->method;
	return bind_client(r);
}
static unsigned open_call(struct fixture *f, unsigned method)
{
	struct client_create r = {.f = f, .method = method};
	unsigned count = atomic_load(&f->client.opened) + 1;
	CHECK(tr_reactor_call(f->client.reactor, open_on_owner, &r) == TR_OK);
	wait_count(&f->client.opened, count);
	return r.stream.slot;
}
struct bytes {
	struct tr_buffer b;
	uint8_t data[];
};
static void bytes_free(struct tr_buffer *b)
{
	free(b);
}
static struct tr_buffer *body_new(size_t size)
{
	struct bytes *b = malloc(sizeof(*b) + size);
	CHECK(b);
	b->b = (struct tr_buffer){.data = b->data,
	                          .capacity = (uint32_t)size,
	                          .len = (uint32_t)size,
	                          .release_cb = bytes_free};
	for (size_t i = 0; i < size; ++i)
		b->data[i] = (uint8_t)(i * 7 + 3);
	return &b->b;
}
struct send_request {
	struct fixture *f;
	unsigned slot;
	struct tr_rpc_record_header h;
	struct tr_buffer *b;
};
static int send_on_owner(void *arg)
{
	struct send_request *r = arg;
	const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
	return tr_rpc_record_stream_send_on_owner(
	        r->f->client.calls[r->slot], &r->h,
	        r->h.metadata_size ? metadata : NULL, &r->b);
}
static void send_record(struct fixture *f, unsigned slot, unsigned method,
                        enum tr_rpc_record_kind kind, size_t size)
{
	struct send_request r = {.f = f, .slot = slot, .h = {.kind = kind}};
	if (kind == TR_RPC_RECORD_OPEN) {
		r.h.service_id = 7;
		r.h.method_id = 9 + method;
		r.h.metadata_size = 7;
	} else if (kind == TR_RPC_RECORD_MESSAGE) {
		r.h.codec_id = TR_RPC_CODEC_RAW;
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		r.b = body_new(size);
	}
	int ret;
	uint64_t until = now_ms() + 15000;
	while ((ret = tr_reactor_call(f->client.reactor, send_on_owner, &r)) ==
	       TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r.b);
}
static void request(struct fixture *f, unsigned slot, unsigned method,
                    unsigned messages, size_t size)
{
	for (unsigned i = 0; i < messages; ++i)
		send_record(f, slot, method, TR_RPC_RECORD_MESSAGE, size);
	send_record(f, slot, method, TR_RPC_RECORD_HALF_CLOSE, 0);
}

static int close_ep(void *arg)
{
	return tr_rpc_record_endpoint_close_on_owner(
	        &((struct fixture *)arg)->endpoint);
}
static int destroy_executor(void *arg)
{
	struct fixture *f = arg;
	size_t n;
	CHECK(tr_task_executor_stop(f->executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_dispatch(f->executor, 8, &n) == TR_OK);
	int ret = tr_task_executor_join(f->executor);
	return ret == TR_OK ? tr_task_executor_destroy(&f->executor) : ret;
}
static int disable_client(void *arg)
{
	((struct peer *)arg)->disabled = true;
	return TR_OK;
}
static void fixture_destroy(struct fixture *f, bool stopped)
{
	atomic_store(&blocked_kind, 0);
	atomic_store(&block_close, 0);
	if (!stopped) {
		uint64_t until = now_ms() + 15000;
		int ret = TR_OK;
		while (f->endpoint &&
		       (ret = tr_reactor_call(f->server.reactor, close_ep,
		                              f)) == TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK && !f->endpoint);
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_executor, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	}
	CHECK(!f->endpoint && !f->executor);
	CHECK(tr_reactor_call(f->client.reactor, disable_client, &f->client) ==
	      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.calls[i]) == TR_OK);
	struct peer *p[] = {&f->client, &f->server};
	for (unsigned i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(p[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(p[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&p[i]->pool) == TR_OK);
		CHECK(atomic_load(&p[i]->budget.current_bytes) == 0);
	}
	atomic_store(&server_channel, NULL);
	atomic_store(&client_channel, NULL);
	gate_destroy(&f->gate);
}
static void completed(struct fixture *f, unsigned slot,
                      enum tr_rpc_status status)
{
	wait_count(&f->client.terminals[slot], 1);
	wait_count(&f->server.terminals[slot], 1);
	wait_count(&f->client.remote_closed[slot], 1);
	CHECK(f->client.status[slot] == status &&
	      f->results[slot].status == status);
	CHECK(f->results[slot].final_accepted);
}
static void test_shapes(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	for (unsigned m = 0; m < 4; ++m) {
		unsigned slot = open_call(&f, m), n = m < 2 ? 1 : 2;
		request(&f, slot, m, n, BODY);
		completed(&f, slot, TR_RPC_STATUS_OK);
		CHECK(atomic_load(&f.client.messages[slot]) ==
		      ((m & 1) ? n : 1));
		CHECK(f.client.received_bytes[slot] ==
		      BODY * ((m & 1) ? n : 1));
	}
	unsigned s = open_call(&f, 3);
	request(&f, s, 3, 0, 0);
	completed(&f, s, TR_RPC_STATUS_OK);
	s = open_call(&f, 0);
	request(&f, s, 0, 1, 0);
	completed(&f, s, TR_RPC_STATUS_OK);
	CHECK(atomic_load(&closes) == 6 &&
	      atomic_load(&accepted[TR_RPC_RECORD_FINAL_STATUS]) == 6);
	fixture_destroy(&f, false);
	puts("endpoint_tcp: shapes");
}
static void test_remote_errors(void)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		struct fixture f;
		fixture_init(&f, 1024 * 1024, CAP);
		atomic_store(&corrupt_open, fault);
		unsigned slot = open_call(&f, 0);
		completed(&f, slot,
		          fault == 1 ? TR_RPC_STATUS_UNIMPLEMENTED
		                     : TR_RPC_STATUS_INVALID_ARGUMENT);
		for (unsigned i = 0; i < CAP; ++i)
			CHECK(!atomic_load(
			        &f.business[i].seen[TR_RPC_RECORD_OPEN]));
		unsigned good = open_call(&f, 0);
		request(&f, good, 0, 1, 80);
		completed(&f, good, TR_RPC_STATUS_OK);
		fixture_destroy(&f, false);
	}
	puts("endpoint_tcp: remote_errors");
}
static void test_capacity(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 1);
	unsigned first = open_call(&f, 0);
	wait_count(&f.business[0].seen[TR_RPC_RECORD_OPEN], 1);
	unsigned second = open_call(&f, 0);
	completed(&f, second, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	send_record(&f, first, 0, TR_RPC_RECORD_MESSAGE, 20);
	send_record(&f, first, 0, TR_RPC_RECORD_HALF_CLOSE, 0);
	completed(&f, first, TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
	puts("endpoint_tcp: capacity");
}
static void test_send_retry(bool local)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	if (local)
		atomic_store(&corrupt_open, 1);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	atomic_store(&block_close, 1);
	unsigned s = open_call(&f, 0);
	if (!local)
		request(&f, s, 0, 1, 80);
	wait_count(&attempts[TR_RPC_RECORD_FINAL_STATUS], 10);
	CHECK(!atomic_load(&f.client.terminals[s]) &&
	      !atomic_load(&close_attempts));
	atomic_store(&blocked_kind, 0);
	wait_count(&close_attempts, 10);
	wait_count(&f.client.terminals[s], 1);
	CHECK(!atomic_load(&f.client.remote_closed[s]) &&
	      !atomic_load(&f.server.terminals[s]));
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_FINAL_STATUS]) == 1);
	atomic_store(&block_close, 0);
	completed(&f, s,
	          local ? TR_RPC_STATUS_UNIMPLEMENTED : TR_RPC_STATUS_OK);
	CHECK(atomic_load(&close_seq) > atomic_load(&final_seq));
	fixture_destroy(&f, false);
	puts("endpoint_tcp: final_close_retry");
}
static int hold_client(void *arg)
{
	((struct fixture *)arg)->client.hold = true;
	return TR_OK;
}
static int release_client(void *arg)
{
	struct peer *p = &((struct fixture *)arg)->client;
	p->hold = false;
	CHECK(p->held);
	int ret = tr_stream_release_payload(p->held_stream, p->held);
	p->held = NULL;
	return ret;
}
static void test_window(void)
{
	struct fixture f;
	fixture_init(&f, 128, CAP);
	unsigned s = open_call(&f, 0);
	CHECK(tr_reactor_call(f.client.reactor, hold_client, &f) == TR_OK);
	request(&f, s, 0, 1, 80);
	wait_count(&f.client.headers[s], 1);
	wait_count(&attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(!atomic_load(&f.client.messages[s]) &&
	      !atomic_load(&close_attempts));
	CHECK(tr_reactor_call(f.client.reactor, release_client, &f) == TR_OK);
	completed(&f, s, TR_RPC_STATUS_OK);
	CHECK(atomic_load(&close_seq) > atomic_load(&final_seq));
	fixture_destroy(&f, false);
	puts("endpoint_tcp: real_window");
}
static void test_stop_running(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	unsigned s = open_call(&f, 0);
	atomic_store(&f.use_gate, true);
	request(&f, s, 0, 1, 80);
	gate_wait(&f.gate, false);
	CHECK(tr_reactor_call(f.server.reactor, close_ep, &f) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminals[s]));
	gate_open(&f.gate);
	completed(&f, s, TR_RPC_STATUS_CANCELLED);
	CHECK(!atomic_load(&f.client.messages[s]));
	fixture_destroy(&f, false);
	puts("endpoint_tcp: late_result");
}
static int owner_noop(void *arg)
{
	(void)arg;
	return TR_OK;
}
static void test_stopped_recovery(bool fatal)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	unsigned s = open_call(&f, 0);
	atomic_store(&f.use_gate, true);
	request(&f, s, 0, 1, 80);
	gate_wait(&f.gate, false);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_ERR_STATE);
	if (fatal) {
		atomic_store(&fail_poll, true);
		int wake = tr_reactor_call(f.server.reactor, owner_noop, NULL);
		CHECK(wake == TR_OK || wake == TR_ERR_CLOSED);
		wait_count(&poll_faults, 1);
	}
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminals[s]));
	CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_adopt_cleanup(f.executor) == TR_OK);
	gate_open(&f.gate);
	uint64_t end = now_ms() + 15000;
	int ret;
	while ((ret = tr_task_executor_join(f.executor)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	size_t n;
	CHECK(tr_task_executor_dispatch(f.executor, 8, &n) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_OK);
	CHECK(atomic_load(&f.server.terminals[s]) == 1);
	CHECK(f.results[s].delivery == TR_RPC_RECORD_OWNER_STOPPED &&
	      !f.results[s].final_accepted);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	fixture_destroy(&f, true);
	puts(fatal ? "endpoint_tcp: fatal_owner_recovery"
	           : "endpoint_tcp: stopped_recovery");
}
static void test_inline_disconnect(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	atomic_store(&abort_kind, TR_RPC_RECORD_MESSAGE);
	unsigned s = open_call(&f, 0);
	request(&f, s, 0, 1, 80);
	wait_count(&f.client.terminals[s], 1);
	wait_count(&f.server.terminals[s], 1);
	CHECK(!f.results[s].final_accepted &&
	      f.client.status[s] != TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
	puts("endpoint_tcp: inline_disconnect");
}
static void test_detach_retry(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	atomic_store(&fail_detach, true);
	CHECK(tr_reactor_call(f.server.reactor, close_ep, &f) == TR_ERR_SYS &&
	      f.endpoint);
	unsigned s = open_call(&f, 0);
	completed(&f, s, TR_RPC_STATUS_UNAVAILABLE);
	atomic_store(&fail_detach, false);
	atomic_store(&fail_timer, true);
	CHECK(tr_reactor_call(f.server.reactor, close_ep, &f) == TR_ERR_SYS &&
	      f.endpoint);
	unsigned ticks = atomic_load(&timer_ticks);
	wait_count(&timer_ticks, ticks + 3);
	atomic_store(&fail_timer, false);
	fixture_destroy(&f, false);
	puts("endpoint_tcp: detach_retry_late_stream");
}

static void test_tx_survives_endpoint(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, CAP);
	atomic_store(&hold_final_transmit, true);
	atomic_store(&corrupt_open, 1);
	unsigned slot = open_call(&f, 0);
	wait_count(&f.server.terminals[slot], 1);
	wait_count(&blocked_writes, 1);
	CHECK(!atomic_load(&f.client.terminals[slot]));
	CHECK(tr_reactor_call(f.server.reactor, close_ep, &f) == TR_OK &&
	      !f.endpoint);
	CHECK(atomic_load(&f.server.budget.current_bytes) > f.executor_budget);
	atomic_store(&blocked_fd, -1);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.client.remote_closed[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_UNIMPLEMENTED);
	uint64_t end = now_ms() + 15000;
	while (atomic_load(&f.server.budget.current_bytes) !=
	       f.executor_budget) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	fixture_destroy(&f, false);
	puts("endpoint_tcp: tx_outlives_endpoint");
}
static void test_final_timeout(void)
{
	struct fixture f;
	fixture_init_full(&f, 1024 * 1024, CAP, 20000000);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	atomic_store(&corrupt_open, 1);
	unsigned slot = open_call(&f, 0);
	wait_count(&f.server.terminals[slot], 1);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.client.remote_closed[slot], 1);
	CHECK(f.results[slot].status == TR_RPC_STATUS_UNIMPLEMENTED &&
	      !f.results[slot].final_accepted);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_FINAL_STATUS]) &&
	      atomic_load(&closes) == 1);
	fixture_destroy(&f, false);
	puts("endpoint_tcp: final_timeout_is_not_delivery");
}
static int recycle_client(void *arg)
{
	struct fixture *f = arg;
	struct peer *p = &f->client;
	CHECK(tr_stream_close(p->streams[0]) == TR_OK);
	return tr_rpc_record_stream_destroy_on_owner(&p->calls[0]);
}
static int server_retired(void *arg)
{
	struct fixture *f = arg;
	struct tr_stream_diagnostics stats;
	int ret = tr_stream_get_diagnostics(f->server.streams[0], &stats);
	if (ret == TR_OK)
		return TR_AGAIN;
	CHECK(ret == TR_ERR_STALE);
	/* 每轮数量限额为2；跨完整环路让宿主退役不再活动的旧调用。 */
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(tr_rpc_record_endpoint_pump_on_owner(f->endpoint) ==
		      TR_OK);
	return TR_OK;
}
static void test_real_slot_reuse(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 1);
	uint32_t generation = 0;
	for (unsigned round = 0; round < 8; ++round) {
		unsigned slot = open_call(&f, 0);
		CHECK(slot == 0);
		CHECK(f.client.streams[slot].generation != generation);
		generation = f.client.streams[slot].generation;
		request(&f, slot, 0, 1, 20);
		completed(&f, slot, TR_RPC_STATUS_OK);
		CHECK(tr_reactor_call(f.client.reactor, recycle_client, &f) ==
		      TR_OK);
		uint64_t end = now_ms() + 15000;
		int ret;
		while ((ret = tr_reactor_call(f.server.reactor, server_retired,
		                              &f)) == TR_AGAIN) {
			CHECK(now_ms() < end);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		atomic_store(&f.client.terminals[slot], 0);
		atomic_store(&f.server.terminals[slot], 0);
		atomic_store(&f.client.remote_closed[slot], 0);
		atomic_store(&f.client.messages[slot], 0);
		atomic_store(&f.client.headers[slot], 0);
	}
	fixture_destroy(&f, false);
	puts("endpoint_tcp: actual_stream_slot_reuse");
}

int main(void)
{
	test_shapes();
	test_remote_errors();
	test_capacity();
	test_send_retry(false);
	test_send_retry(true);
	test_window();
	test_stop_running();
	test_stopped_recovery(false);
	test_stopped_recovery(true);
	test_inline_disconnect();
	test_detach_retry();
	test_tx_survives_endpoint();
	test_final_timeout();
	test_real_slot_reuse();
	puts("rpc_record_endpoint: ok");
	return 0;
}
