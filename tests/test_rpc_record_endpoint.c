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
#define CAP 4U
#define BODY 4096U

/* 仅测试链接包装；正常准入/发送/关闭始终调用真实传输，不使用事件循环替身。 */
static _Atomic(struct tr_channel *) watched_server, watched_client;
static _Atomic bool block_final, block_close, corrupt_open;
static _Atomic unsigned final_attempts, final_accepted, close_attempts,
        close_accepted, natural_backpressure;
static _Atomic uintptr_t error_buffer;
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __real_tr_stream_close(struct tr_stream_handle);
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	unsigned kind = count && buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE
	                        ? tr_get_le16(buffers[0]->data + 6)
	                        : 0;
	if (stream.channel == atomic_load(&watched_client) &&
	    kind == TR_RPC_RECORD_OPEN &&
	    atomic_exchange(&corrupt_open, false)) {
		CHECK(buffers[0]->len > TR_RPC_RECORD_HEADER_SIZE + 3);
		buffers[0]->data[TR_RPC_RECORD_HEADER_SIZE + 3] = 'A';
	}
	bool final = stream.channel == atomic_load(&watched_server) &&
	             kind == TR_RPC_RECORD_FINAL_STATUS;
	if (final) {
		atomic_fetch_add(&final_attempts, 1);
		if (atomic_load(&block_final)) {
			uintptr_t old = atomic_exchange(&error_buffer,
			                                (uintptr_t)buffers[0]);
			CHECK(!old || old == (uintptr_t)buffers[0]);
			return TR_AGAIN;
		}
	}
	int ret = __real_tr_stream_sendv(stream, buffers, count);
	if (final && ret == TR_OK)
		atomic_fetch_add(&final_accepted, 1);
	if (final && ret == TR_AGAIN)
		atomic_fetch_add(&natural_backpressure, 1);
	return ret;
}
int __wrap_tr_stream_close(struct tr_stream_handle stream)
{
	bool server = stream.channel == atomic_load(&watched_server);
	if (server) {
		atomic_fetch_add(&close_attempts, 1);
		if (atomic_load(&block_close))
			return TR_AGAIN;
	}
	int ret = __real_tr_stream_close(stream);
	if (server && ret == TR_OK)
		atomic_fetch_add(&close_accepted, 1);
	return ret;
}

/* 只对宿主发布的处理器/定时器注入解绑失败，原源继续真实存在。 */
static _Atomic bool fail_handler_detach, fail_timer_detach, inject_fatal;
static _Atomic unsigned fatal_fired;
static _Atomic(struct tr_reactor *) watched_reactor;
static _Thread_local bool capture_timer;
static struct tr_reactor_timer_handle endpoint_timer;
int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
int __wrap_tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb ce, void *arg)
{
	if (c == atomic_load(&watched_server) && !data &&
	    atomic_exchange(&fail_handler_detach, false))
		return TR_ERR_SYS;
	return __real_tr_channel_set_handler(c, data, event, ce, arg);
}
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	int ret = __real_tr_reactor_timer_register(r, cb, arg, out);
	if (ret == TR_OK && capture_timer)
		endpoint_timer = *out;
	return ret;
}
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle h)
{
	if (h.reactor == endpoint_timer.reactor &&
	    h.slot == endpoint_timer.slot &&
	    h.generation == endpoint_timer.generation &&
	    atomic_exchange(&fail_timer_detach, false))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_unregister(h);
}
int __real_epoll_wait(int, struct epoll_event *, int, int);
int __wrap_epoll_wait(int fd, struct epoll_event *events, int maxevents,
                      int timeout)
{
	if (tr_reactor_in_owner_context_for(atomic_load(&watched_reactor)) &&
	    atomic_exchange(&inject_fatal, false)) {
		atomic_fetch_add(&fatal_fired, 1);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, maxevents, timeout);
}

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void wait_count(_Atomic unsigned *value, unsigned n)
{
	uint64_t end = now_ms() + 15000;
	while (atomic_load(value) < n) {
		CHECK(now_ms() < end);
		sched_yield();
	}
}
struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered, open;
};
static void gate_init(struct gate *g)
{
	*g = (struct gate){0};
	pthread_condattr_t attr;
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
struct fixture;
struct peer {
	struct fixture *f;
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle stream[CAP];
	struct tr_rpc_record_stream *call[CAP];
	_Atomic unsigned opened, packets, terminal[CAP], messages[CAP],
	        remote_close[CAP];
	enum tr_rpc_status status[CAP];
	struct tr_rpc_endpoint_result result[CAP];
	uint64_t generation[CAP];
	size_t bytes[CAP];
	bool disabled, hold;
	struct tr_rpc_method_desc next_method;
	struct tr_buffer *held[CAP * 4];
	struct tr_stream_handle held_stream[CAP * 4];
	size_t nheld;
};
struct fixture {
	struct peer client, server;
	struct tr_rpc_record_endpoint *endpoint;
	struct tr_task_executor *executor;
	struct tr_rpc_dispatch_method method;
	struct tr_rpc_record_endpoint_config config;
	struct gate gate;
	_Atomic bool hold_work;
	_Atomic unsigned work_calls[6], active_work;
};
static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct fixture *f = arg;
	(void)id;
	(void)cancel;
	CHECK(!tr_reactor_in_owner_context());
	atomic_fetch_add(&f->active_work, 1);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		if (atomic_exchange(&f->hold_work, false))
			gate_wait(&f->gate, true);
		for (size_t i = 0; i < view->header.decoded_size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 11 + 7));
		CHECK(tr_rpc_record_reply_append(reply, view->body,
		                                 view->header.decoded_size) ==
		      TR_OK);
	}
	atomic_fetch_add(&f->work_calls[view->header.kind], 1);
	atomic_fetch_sub(&f->active_work, 1);
	return TR_RPC_STATUS_OK;
}
static void server_terminal(struct tr_stream_handle s,
                            struct tr_rpc_endpoint_result result, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_endpoint *same = f->endpoint;
	CHECK(s.channel == f->server.channel && s.slot < CAP);
	if (result.delivery == TR_RPC_RECORD_ON_OWNER) {
		CHECK(tr_reactor_in_owner_context_for(f->server.reactor));
		CHECK(tr_rpc_record_endpoint_close_on_owner(&same) ==
		      TR_ERR_STATE);
	} else {
		CHECK(!tr_reactor_in_owner_context());
		CHECK(result.status == TR_RPC_STATUS_CANCELLED &&
		      !result.final_accepted);
		CHECK(tr_rpc_record_endpoint_reap_stopped(&same) ==
		      TR_ERR_STATE);
	}
	CHECK(same == f->endpoint);
	f->server.result[s.slot] = result;
	f->server.generation[s.slot] = s.generation;
	atomic_fetch_add(&f->server.terminal[s.slot], 1);
}
static unsigned client_slot(struct peer *p, struct tr_rpc_record_stream *call)
{
	for (unsigned i = 0; i < CAP; ++i)
		if (p->call[i] == call)
			return i;
	CHECK(false);
	return 0;
}
static void client_received(struct tr_rpc_record_stream *call,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		for (size_t j = 0; j < view->header.decoded_size; ++j)
			CHECK(view->body[j] == (uint8_t)(j * 11 + 7));
		p->bytes[i] += view->header.decoded_size;
		atomic_fetch_add(&p->messages[i], 1);
	}
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	(void)delivery;
	p->status[i] = status;
	atomic_fetch_add(&p->terminal[i], 1);
}
static enum tr_stream_data_disposition client_data(struct tr_stream_handle s,
                                                   uint64_t id,
                                                   struct tr_buffer *b,
                                                   void *arg)
{
	struct peer *p = arg;
	(void)id;
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(s.slot < CAP && p->call[s.slot]);
	CHECK(tr_rpc_record_stream_receive_on_owner(p->call[s.slot], s, b->data,
	                                            b->len) == TR_OK);
	atomic_fetch_add(&p->packets, 1);
	if (p->hold) {
		CHECK(p->nheld < CAP * 4);
		p->held[p->nheld] = b;
		p->held_stream[p->nheld++] = s;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void client_event(struct tr_stream_handle s, enum tr_stream_event event,
                         int status, void *arg)
{
	struct peer *p = arg;
	(void)status;
	if (p->disabled)
		return;
	CHECK(s.slot < CAP);
	if (event == TR_STREAM_EVENT_OPENED) {
		p->stream[s.slot] = s;
		if (p->call[s.slot])
			CHECK(tr_rpc_record_stream_destroy_on_owner(
			              &p->call[s.slot]) == TR_OK);
		struct tr_rpc_record_stream_config cfg = {
		        TR_RPC_RECORD_REQUEST_DIRECTION, p->next_method};
		struct tr_rpc_record_stream_callbacks cb = {client_received,
		                                            client_terminal, p};
		CHECK(tr_rpc_record_stream_create_on_owner(
		              s, &cfg, &cb, &p->budget, &p->call[s.slot]) ==
		      TR_OK);
		atomic_fetch_add(&p->opened, 1);
	} else if (p->call[s.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->call[s.slot], s,
		                                          event) == TR_OK);
		if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
			/* 记录层已处理全部先行 DATA，不能在 FINAL
			 * 之前接受成功关闭。 */
			CHECK(atomic_load(&p->terminal[s.slot]) != 0);
			int ret = tr_stream_close(s);
			CHECK(ret == TR_OK || ret == TR_ERR_CLOSED ||
			      ret == TR_ERR_STALE);
			atomic_fetch_add(&p->remote_close[s.slot], 1);
		}
	}
}
static void tcp_pair(int *a, int *b)
{
	int listener = -1, ret;
	uint16_t port;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) == TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, a);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	uint64_t end = now_ms() + 10000;
	while (tr_tcp_accept(listener, b) == TR_AGAIN) {
		struct pollfd fd = {listener, POLLIN, 0};
		CHECK(now_ms() < end && poll(&fd, 1, 100) >= 0);
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
	struct tr_conn_handle conn;
	*p = (struct peer){.f = f};
	atomic_init(&p->opened, 0);
	atomic_init(&p->packets, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminal[i], 0);
		atomic_init(&p->messages[i], 0);
		atomic_init(&p->remote_close[i], 0);
	}
	tr_memory_budget_init(&p->budget, 16 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	/* 服务端延后握手：宿主处理器先发布，不能让早到的 OPEN 落入空回调。 */
	CHECK(tr_channel_create_deferred(
	              &cc, conn, conn, server ? NULL : client_data,
	              server ? NULL : client_event, NULL, server ? NULL : p,
	              &p->channel) == TR_OK);
}
static int start_host(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config cfg = {2, CAP};
	CHECK(tr_task_executor_create(&cfg, &f->server.budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	CHECK(tr_rpc_record_endpoint_create_on_owner(
	              f->server.channel, f->executor, &f->method, 1, &f->config,
	              server_terminal, f, &f->server.budget,
	              &f->endpoint) == TR_OK);
	capture_timer = true;
	CHECK(tr_rpc_record_endpoint_start_on_owner(f->endpoint) == TR_OK);
	capture_timer = false;
	return tr_channel_start(f->server.channel);
}
static unsigned fixture_capacity = CAP;
static void fixture_init(struct fixture *f, uint64_t window,
                         uint64_t final_wait)
{
	*f = (struct fixture){
	        .config = {{CAP, 8, 4, BODY * 4}, 2, 2, 1000000, final_wait}};
	f->config.dispatch.capacity = fixture_capacity;
	f->method = (struct tr_rpc_dispatch_method){
	        {7, 9, TR_RPC_ONE, TR_RPC_ONE, TR_RPC_CODEC_RAW,
	         TR_RPC_CODEC_RAW, TR_LANE_BULK, BODY, BODY},
	        business,
	        f};
	atomic_init(&f->hold_work, false);
	atomic_init(&f->active_work, 0);
	for (unsigned i = 0; i < 6; ++i)
		atomic_init(&f->work_calls[i], 0);
	gate_init(&f->gate);
	atomic_store(&block_final, false);
	atomic_store(&block_close, false);
	atomic_store(&corrupt_open, false);
	atomic_store(&final_attempts, 0);
	atomic_store(&final_accepted, 0);
	atomic_store(&close_attempts, 0);
	atomic_store(&close_accepted, 0);
	atomic_store(&natural_backpressure, 0);
	atomic_store(&error_buffer, 0);
	atomic_store(&fail_handler_detach, false);
	atomic_store(&fail_timer_detach, false);
	atomic_store(&inject_fatal, false);
	atomic_store(&fatal_fired, 0);
	endpoint_timer = (struct tr_reactor_timer_handle){0};
	int a = -1, b = -1;
	tcp_pair(&a, &b);
	peer_init(&f->client, f, a, false, window);
	peer_init(&f->server, f, b, true, window);
	atomic_store(&watched_client, f->client.channel);
	atomic_store(&watched_server, f->server.channel);
	atomic_store(&watched_reactor, f->server.reactor);
	CHECK(tr_reactor_call(f->server.reactor, start_host, f) == TR_OK);
	CHECK(tr_channel_start(f->client.channel) == TR_OK);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
}
struct open_request {
	struct fixture *f;
	struct tr_stream_handle stream;
	bool unknown;
};
static int open_client(void *arg)
{
	struct open_request *r = arg;
	struct peer *p = &r->f->client;
	p->next_method = r->f->method.descriptor;
	if (r->unknown)
		p->next_method.method_id = 999;
	return tr_stream_open(p->channel, TR_LANE_BULK, &r->stream);
}
static unsigned open_call(struct fixture *f, bool unknown)
{
	struct open_request r = {.f = f, .unknown = unknown};
	unsigned n = atomic_load(&f->client.opened) + 1;
	CHECK(tr_reactor_call(f->client.reactor, open_client, &r) == TR_OK);
	wait_count(&f->client.opened, n);
	return r.stream.slot;
}
struct bytes {
	struct tr_buffer buffer;
	uint8_t data[];
};
static void free_bytes(struct tr_buffer *b)
{
	free(b);
}
static struct tr_buffer *body_new(size_t size)
{
	struct bytes *b = malloc(sizeof(*b) + size);
	CHECK(b);
	b->buffer = (struct tr_buffer){.data = b->data,
	                               .capacity = (uint32_t)size,
	                               .len = (uint32_t)size,
	                               .release_cb = free_bytes};
	for (size_t i = 0; i < size; ++i)
		b->data[i] = (uint8_t)(i * 11 + 7);
	return &b->buffer;
}
struct send_request {
	struct fixture *f;
	unsigned slot;
	struct tr_rpc_record_header h;
	struct tr_buffer *b;
};
static int send_owner(void *arg)
{
	struct send_request *r = arg;
	const uint8_t metadata[] = {1, 1, 0, 'k', 'v'};
	return tr_rpc_record_stream_send_on_owner(
	        r->f->client.call[r->slot], &r->h,
	        r->h.metadata_size ? metadata : NULL, &r->b);
}
static void send_record(struct fixture *f, unsigned slot,
                        enum tr_rpc_record_kind kind, size_t size, bool unknown)
{
	struct send_request r = {.f = f, .slot = slot, .h = {.kind = kind}};
	if (kind == TR_RPC_RECORD_OPEN) {
		r.h.service_id = 7;
		r.h.method_id = unknown ? 999 : 9;
		r.h.metadata_size = 5;
	} else if (kind == TR_RPC_RECORD_MESSAGE) {
		r.h.codec_id = TR_RPC_CODEC_RAW;
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		r.b = body_new(size);
	}
	uint64_t end = now_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(f->client.reactor, send_owner, &r)) ==
	       TR_AGAIN) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r.b);
}
static void request(struct fixture *f, unsigned slot, size_t size)
{
	send_record(f, slot, TR_RPC_RECORD_OPEN, 0, false);
	send_record(f, slot, TR_RPC_RECORD_MESSAGE, size, false);
	send_record(f, slot, TR_RPC_RECORD_HALF_CLOSE, 0, false);
}
static int close_host(void *arg)
{
	return tr_rpc_record_endpoint_close_on_owner(
	        &((struct fixture *)arg)->endpoint);
}
static int destroy_executor(void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_task_executor_stop(f->executor, TR_TASK_CANCEL) == TR_OK);
	int ret = tr_task_executor_join(f->executor);
	return ret == TR_OK ? tr_task_executor_destroy(&f->executor) : ret;
}
static int noop(void *arg)
{
	(void)arg;
	return TR_OK;
}
static int disable_client(void *arg)
{
	((struct peer *)arg)->disabled = true;
	return TR_OK;
}
static void fixture_destroy(struct fixture *f, bool stopped)
{
	atomic_store(&block_final, false);
	atomic_store(&block_close, false);
	if (!stopped) {
		uint64_t end = now_ms() + 15000;
		int ret;
		ret = TR_OK;
		while (f->endpoint &&
		       (ret = tr_reactor_call(f->server.reactor, close_host,
		                              f)) == TR_AGAIN) {
			CHECK(now_ms() < end);
			sched_yield();
		}
		CHECK(ret == TR_OK && !f->endpoint);
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_executor, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < end);
			sched_yield();
		}
		CHECK(ret == TR_OK && !f->executor);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	}
	CHECK(!f->endpoint && !f->executor);
	CHECK(tr_reactor_call(f->client.reactor, disable_client, &f->client) ==
	      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.call[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.call[i]) == TR_OK);
	atomic_store(&watched_server, NULL);
	atomic_store(&watched_client, NULL);
	atomic_store(&watched_reactor, NULL);
	struct peer *peers[] = {&f->client, &f->server};
	for (unsigned i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->pool) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
	CHECK(pthread_cond_destroy(&f->gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&f->gate.lock) == 0);
}

static void test_success(void)
{
	struct fixture f;
	fixture_init(&f, 65536, 1000000000);
	unsigned s = open_call(&f, false);
	request(&f, s, BODY);
	wait_count(&f.client.remote_close[s], 1);
	wait_count(&f.server.terminal[s], 1);
	CHECK(f.client.status[s] == TR_RPC_STATUS_OK &&
	      f.client.bytes[s] == BODY);
	CHECK(f.server.result[s].status == TR_RPC_STATUS_OK &&
	      f.server.result[s].final_accepted);
	CHECK(atomic_load(&final_accepted) == 1 &&
	      atomic_load(&close_accepted) == 1);
	CHECK(atomic_load(&f.work_calls[TR_RPC_RECORD_MESSAGE]) == 1);
	fixture_destroy(&f, false);
}
static void test_reject_and_reuse(bool malformed)
{
	struct fixture f;
	fixture_init(&f, 65536, 1000000000);
	unsigned s = open_call(&f, !malformed);
	atomic_store(&corrupt_open, malformed);
	send_record(&f, s, TR_RPC_RECORD_OPEN, 0, !malformed);
	wait_count(&f.client.remote_close[s], 1);
	wait_count(&f.server.terminal[s], 1);
	CHECK(f.client.status[s] == (malformed ? TR_RPC_STATUS_INVALID_ARGUMENT
	                                       : TR_RPC_STATUS_UNIMPLEMENTED));
	CHECK(f.server.result[s].final_accepted);
	CHECK(atomic_load(&f.work_calls[TR_RPC_RECORD_OPEN]) == 0);
	CHECK(tr_reactor_call(f.server.reactor, noop, NULL) == TR_OK);
	/* 第二个合法调用仍在同一TCP连接完成；关闭确认后允许复用传输槽位。 */
	unsigned n = open_call(&f, false);
	unsigned expected = atomic_load(&f.client.remote_close[n]) + 1;
	request(&f, n, 0);
	wait_count(&f.client.remote_close[n], expected);
	CHECK(f.client.status[n] == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.work_calls[TR_RPC_RECORD_MESSAGE]) == 1);
	fixture_destroy(&f, false);
}
static void test_final_and_close_backpressure(bool expiry)
{
	struct fixture f;
	fixture_init(&f, 65536, expiry ? 20000000 : 3000000000);
	atomic_store(&block_final, true);
	atomic_store(&block_close, !expiry);
	unsigned s = open_call(&f, true);
	send_record(&f, s, TR_RPC_RECORD_OPEN, 0, true);
	if (!expiry) {
		wait_count(&final_attempts, 10);
		CHECK(!atomic_load(&close_attempts) &&
		      !atomic_load(&f.server.terminal[s]));
		atomic_store(&block_final, false);
		wait_count(&f.client.terminal[s], 1);
		wait_count(&close_attempts, 10);
		CHECK(atomic_load(&final_accepted) == 1);
		CHECK(!atomic_load(&f.server.terminal[s]) &&
		      !atomic_load(&f.client.remote_close[s]));
		atomic_store(&block_close, false);
	}
	wait_count(&f.server.terminal[s], 1);
	wait_count(&f.client.remote_close[s], 1);
	CHECK(f.server.result[s].status == TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(f.server.result[s].final_accepted == !expiry);
	CHECK(f.client.status[s] == (expiry ? TR_RPC_STATUS_UNAVAILABLE
	                                    : TR_RPC_STATUS_UNIMPLEMENTED));
	CHECK(atomic_load(&f.work_calls[TR_RPC_RECORD_OPEN]) == 0);
	fixture_destroy(&f, false);
}
static int hold_rx(void *arg)
{
	((struct fixture *)arg)->client.hold = true;
	return TR_OK;
}
static int release_rx(void *arg)
{
	struct peer *p = &((struct fixture *)arg)->client;
	p->hold = false;
	for (size_t i = 0; i < p->nheld; ++i) {
		int ret = tr_stream_release_payload(p->held_stream[i],
		                                    p->held[i]);
		CHECK(ret == TR_OK || ret == TR_AGAIN);
	}
	p->nheld = 0;
	return TR_OK;
}
static void test_real_window(void)
{
	struct fixture f;
	fixture_init(&f, 128, 1000000000);
	CHECK(tr_reactor_call(f.client.reactor, hold_rx, &f) == TR_OK);
	unsigned s = open_call(&f, false);
	request(&f, s, 32);
	wait_count(&f.client.messages[s], 1);
	wait_count(&natural_backpressure, 2);
	CHECK(!atomic_load(&f.client.terminal[s]) &&
	      !atomic_load(&close_attempts));
	CHECK(atomic_load(&f.work_calls[TR_RPC_RECORD_MESSAGE]) == 1);
	CHECK(tr_reactor_call(f.client.reactor, release_rx, &f) == TR_OK);
	wait_count(&f.client.remote_close[s], 1);
	wait_count(&f.server.terminal[s], 1);
	CHECK(f.client.status[s] == TR_RPC_STATUS_OK &&
	      f.server.result[s].final_accepted);
	CHECK(atomic_load(&final_accepted) == 1);
	fixture_destroy(&f, false);
}
static void test_stop_with_work(unsigned mode)
{
	struct fixture f;
	fixture_init(&f, 65536, 1000000000);
	atomic_store(&f.hold_work, true);
	unsigned s = open_call(&f, false);
	request(&f, s, 32);
	gate_wait(&f.gate, false);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_ERR_STATE);
	if (mode != 2)
		CHECK(tr_reactor_call(f.server.reactor, close_host, &f) ==
		      TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminal[s]) &&
	      atomic_load(&f.active_work) == 1);
	if (!mode) {
		gate_open(&f.gate);
		wait_count(&f.client.remote_close[s], 1);
		wait_count(&f.server.terminal[s], 1);
		CHECK(f.client.status[s] == TR_RPC_STATUS_CANCELLED);
		CHECK(!atomic_load(&f.client.messages[s]));
		fixture_destroy(&f, false);
		return;
	}
	if (mode == 2) {
		atomic_store(&inject_fatal, true);
		wait_count(&fatal_fired, 1);
	}
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminal[s]));
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
	size_t delivered;
	CHECK(tr_task_executor_dispatch(f.executor, CAP, &delivered) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_OK);
	CHECK(atomic_load(&f.server.terminal[s]) == 1);
	CHECK(f.server.result[s].delivery == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(!f.server.result[s].final_accepted);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	fixture_destroy(&f, true);
}
/* 无 RPC OPEN 的传输准入拒绝不能发送虚构的 RPC 最终记录。 */
static void test_preopen_reject(void)
{
	struct fixture f;
	fixture_capacity = 1;
	fixture_init(&f, 65536, 1000000000);
	fixture_capacity = CAP;
	atomic_store(&f.hold_work, true);
	unsigned first = open_call(&f, false);
	request(&f, first, 16);
	gate_wait(&f.gate, false);
	unsigned rejected = open_call(&f, false);
	wait_count(&f.client.remote_close[rejected], 1);
	wait_count(&f.server.terminal[rejected], 1);
	CHECK(f.client.status[rejected] == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(f.server.result[rejected].status ==
	      TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(!f.server.result[rejected].final_accepted);
	CHECK(!atomic_load(&final_attempts));
	gate_open(&f.gate);
	wait_count(&f.client.remote_close[first], 1);
	CHECK(f.client.status[first] == TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static void test_detach_retry_with_new_stream(void)
{
	struct fixture f;
	fixture_init(&f, 65536, 1000000000);
	atomic_store(&fail_handler_detach, true);
	CHECK(tr_reactor_call(f.server.reactor, close_host, &f) == TR_ERR_SYS);
	CHECK(f.endpoint);
	/* 派发器已退出但处理器仍真实发布；此时新流仍须收到传输拒绝。 */
	unsigned s = open_call(&f, false);
	wait_count(&f.client.remote_close[s], 1);
	wait_count(&f.server.terminal[s], 1);
	CHECK(f.client.status[s] == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(f.server.result[s].status == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(!f.server.result[s].final_accepted &&
	      !atomic_load(&final_attempts));
	atomic_store(&fail_timer_detach, true);
	CHECK(tr_reactor_call(f.server.reactor, close_host, &f) == TR_ERR_SYS);
	CHECK(f.endpoint);
	CHECK(tr_reactor_call(f.server.reactor, close_host, &f) == TR_OK);
	CHECK(!f.endpoint);
	CHECK(atomic_load(&f.server.terminal[s]) == 1);
	fixture_destroy(&f, false);
}
int main(void)
{
	test_preopen_reject();
	test_success();
	test_reject_and_reuse(false);
	test_reject_and_reuse(true);
	test_final_and_close_backpressure(false);
	test_final_and_close_backpressure(true);
	test_real_window();
	test_stop_with_work(0);
	test_stop_with_work(1);
	test_stop_with_work(2);
	test_detach_retry_with_new_stream();
	puts("rpc_record_endpoint: 11 scenarios passed");
	return 0;
}
