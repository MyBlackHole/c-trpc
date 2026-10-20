#define _POSIX_C_SOURCE 200809L
#include "../src/execution/worker_context.h"
#include "../src/rpc/rpc_record_endpoint.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <poll.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/socket.h>
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
#define BODY 65537U

int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
static _Atomic(struct tr_channel *) observed_channel;
static _Atomic unsigned blocked_kind, send_attempts[6];
static _Atomic unsigned final_attempt_mask, final_accepted[CAP];
static _Atomic(struct tr_channel *) client_channel;
static _Atomic bool corrupt_open, reject_close, fail_detach, fail_timer_detach;
static _Atomic unsigned close_attempts, premature_close, fatal_polls;
static _Atomic unsigned hard_close_target, hard_close_attempts;
static _Atomic bool hard_close_block;
static _Atomic(struct tr_reactor *) fatal_reactor;
static _Atomic bool fail_poll;
/* 只在指定连接真正写入内核时注入 EAGAIN；上层仍须完成真实 TX 接管。 */
static _Atomic int blocked_fd = -1, observed_fd = -1;
static _Atomic unsigned blocked_writes, abort_kind;
static struct tr_conn_handle observed_connection;
static _Atomic bool hold_final_transmit;
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
static _Thread_local bool capture_timer;
static _Thread_local struct fixture *capture_endpoint;
static struct tr_reactor_timer_handle endpoint_timer;

int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	unsigned server_kind = 0;
	if (stream.channel == atomic_load(&client_channel) && count &&
	    buffers[0]->len > TR_RPC_RECORD_HEADER_SIZE &&
	    tr_get_le16(buffers[0]->data + 6) == TR_RPC_RECORD_OPEN &&
	    atomic_exchange(&corrupt_open, false)) {
		/* 真正发送非法元数据；在发送方本地校验之后制造不可信对端输入。
		 */
		buffers[0]->data[TR_RPC_RECORD_HEADER_SIZE] = 0;
	}
	if (stream.channel == atomic_load(&observed_channel) && count &&
	    buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE &&
	    !memcmp(buffers[0]->data, "TRPC", 4)) {
		unsigned kind = tr_get_le16(buffers[0]->data + 6);
		CHECK(kind >= 1 && kind <= 5);
		server_kind = kind;
		atomic_fetch_add(&send_attempts[kind], 1);
		if (kind == TR_RPC_RECORD_FINAL_STATUS) {
			CHECK(stream.slot < CAP);
			atomic_fetch_or(&final_attempt_mask, 1U << stream.slot);
		}
		if (kind == TR_RPC_RECORD_FINAL_STATUS &&
		    atomic_exchange(&hold_final_transmit, false))
			atomic_store(&blocked_fd, atomic_load(&observed_fd));
		if (kind == atomic_load(&abort_kind) &&
		    atomic_exchange(&abort_kind, 0) == kind) {
			CHECK(tr_reactor_abort_on_owner(observed_connection,
			                                TR_ERR_SYS) == TR_OK);
			return TR_ERR_CLOSED;
		}
		if (kind == atomic_load(&blocked_kind)) {
			return TR_AGAIN;
		}
	}
	int ret = __real_tr_stream_sendv(stream, buffers, count);
	if (server_kind == TR_RPC_RECORD_FINAL_STATUS && ret == TR_OK)
		atomic_fetch_add(&final_accepted[stream.slot], 1);
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
	struct fixture *fixture;
	unsigned index;
};
struct business_state {
	_Atomic unsigned active;
	_Atomic unsigned seen[6];
	uint64_t generation;
	uint8_t saved[BODY];
	size_t size;
	bool closed;
};
struct peer {
	struct fixture *fixture;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	unsigned method[CAP];
	_Atomic unsigned opened, packets, terminals[CAP], messages[CAP],
	        initial[CAP], ingress_bytes[CAP];
	uint32_t status[CAP];
	size_t received_bytes[CAP];
	enum tr_rpc_record_delivery delivery[CAP];
	_Atomic int last_channel_event, last_channel_status;
	_Atomic unsigned channel_down_events;
	bool final_accepted[CAP];
	bool server, disabled, hold;
	bool withhold_open[CAP];
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
};
struct fixture {
	struct peer client, server;
	struct tr_task_executor *executor;
	struct tr_rpc_record_endpoint *endpoint;
	struct tr_rpc_record_dispatch_config config;
	struct tr_rpc_dispatch_method methods[4];
	struct method_context contexts[4];
	struct business_state business[CAP];
	struct gate gate;
	struct gate credit_gate;
	_Atomic bool use_gate;
	_Atomic bool hold_final_credit;
	_Atomic bool stop_credit_on_final;
	_Atomic unsigned reject_method;
	_Atomic bool bad_reply;
	unsigned slow_variant;
	unsigned batch;
	uint64_t open_timeout_ns;
	uint64_t executor_budget;
	uint64_t final_wait_ns;
	tr_stream_data_cb endpoint_data;
	tr_stream_event_cb endpoint_event;
	void *endpoint_arg;
};
static enum tr_rpc_status handler(struct tr_rpc_dispatch_id id,
                                  const struct tr_rpc_record_view *view,
                                  const struct tr_task_cancel *cancel,
                                  struct tr_rpc_record_reply *reply, void *arg)
{
	struct method_context *m = arg;
	struct fixture *f = m->fixture;
	struct business_state *s = &f->business[id.slot];
	unsigned kind = view->header.kind;
	enum tr_rpc_status status = TR_RPC_STATUS_OK;
	CHECK(tr_execution_in_worker_context());
	CHECK(!tr_reactor_in_owner_context());
	CHECK(atomic_fetch_add(&s->active, 1) == 0);
	if (kind == TR_RPC_RECORD_OPEN) {
		s->generation = id.generation;
		s->closed = false;
		s->size = 0;
		CHECK(view->header.method_id ==
		      f->methods[m->index].descriptor.method_id);
		CHECK(view->header.metadata_size == 7);
		const uint8_t expected[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
		CHECK(!memcmp(view->metadata, expected, sizeof(expected)));
	} else {
		CHECK(id.generation == s->generation && !s->closed);
	}
	if (kind == TR_RPC_RECORD_MESSAGE) {
		if (atomic_exchange(&f->use_gate, false))
			gate_wait(&f->gate, true);
		/* 业务读取独立任务快照，不保留宿主回调中的借用接收视图。 */
		for (size_t i = 0; i < view->header.decoded_size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7 + 3));
		s->size = view->header.decoded_size;
		if (s->size)
			memcpy(s->saved, view->body, s->size);
		if (f->methods[m->index].descriptor.response_cardinality ==
		    TR_RPC_MANY)
			for (unsigned i = 0; i < f->batch; ++i) {
				int ret = tr_rpc_record_reply_append(
				        reply, view->body, s->size);
				if (ret != TR_OK)
					status =
					        TR_RPC_STATUS_RESOURCE_EXHAUSTED;
			}
	}
	if (kind == TR_RPC_RECORD_HALF_CLOSE) {
		s->closed = true;
		if (f->methods[m->index].descriptor.response_cardinality ==
		            TR_RPC_ONE &&
		    !f->bad_reply) {
			int ret = tr_rpc_record_reply_append(reply, s->saved,
			                                     s->size);
			if (ret != TR_OK)
				status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		}
	}
	if (atomic_load(&f->reject_method) == m->index + 1 &&
	    kind == TR_RPC_RECORD_OPEN) {
		CHECK(tr_rpc_record_reply_finish(
		              reply, TR_RPC_STATUS_PERMISSION_DENIED) == TR_OK);
		CHECK(tr_rpc_record_reply_finish(reply, TR_RPC_STATUS_OK) ==
		      TR_ERR_STATE);
		CHECK(tr_rpc_record_reply_append(reply, NULL, 0) ==
		      TR_ERR_STATE);
	}
	/* 测试允许不合作业务返回成功，取消后的私有结果仍必须被引擎丢弃。 */
	(void)cancel;
	atomic_fetch_add(&s->seen[kind], 1);
	CHECK(atomic_fetch_sub(&s->active, 1) == 1);
	return status;
}
/* 包装只模拟容量/注销/I/O 故障，合法路径始终进入真实生产实现。 */
int __real_tr_stream_close(struct tr_stream_handle);
int __wrap_tr_stream_close(struct tr_stream_handle stream)
{
	if (stream.channel == atomic_load(&observed_channel)) {
		atomic_fetch_add(&close_attempts, 1);
		if (atomic_load(&blocked_kind) == TR_RPC_RECORD_FINAL_STATUS)
			atomic_fetch_add(&premature_close, 1);
		if (stream.slot == atomic_load(&hard_close_target) &&
		    atomic_load(&hard_close_block)) {
			atomic_fetch_add(&hard_close_attempts, 1);
			return TR_ERR_SYS;
		}
		if (atomic_load(&reject_close))
			return TR_AGAIN;
	}
	return __real_tr_stream_close(stream);
}
int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
static enum tr_stream_data_disposition
observe_endpoint_data(struct tr_stream_handle stream, uint64_t id,
                      struct tr_buffer *payload, void *arg)
{
	struct fixture *f = arg;
	CHECK(stream.slot < CAP);
	enum tr_stream_data_disposition ret =
	        f->endpoint_data(stream, id, payload, f->endpoint_arg);
	atomic_fetch_add(&f->server.ingress_bytes[stream.slot], payload->len);
	return ret;
}
static void observe_endpoint_event(struct tr_stream_handle stream,
                                   enum tr_stream_event event, int status,
                                   void *arg)
{
	struct fixture *f = arg;
	if (event == TR_STREAM_EVENT_OPENED)
		f->server.streams[stream.slot] = stream;
	f->endpoint_event(stream, event, status, f->endpoint_arg);
	if (event == TR_STREAM_EVENT_OPENED)
		atomic_fetch_add(&f->server.opened, 1);
}
int __wrap_tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb changed, void *arg)
{
	if (c == atomic_load(&observed_channel) && !data && !event &&
	    atomic_exchange(&fail_detach, false))
		return TR_ERR_SYS;
	if (capture_endpoint && capture_endpoint->open_timeout_ns && data &&
	    event) {
		struct fixture *f = capture_endpoint;
		f->endpoint_data = data;
		f->endpoint_event = event;
		f->endpoint_arg = arg;
		return __real_tr_channel_set_handler(c, observe_endpoint_data,
		                                     observe_endpoint_event,
		                                     changed, f);
	}
	return __real_tr_channel_set_handler(c, data, event, changed, arg);
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
int __wrap_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout)
{
	struct tr_reactor *r = atomic_load(&fatal_reactor);
	if (r && tr_reactor_in_owner_context_for(r) &&
	    atomic_exchange(&fail_poll, false)) {
		atomic_fetch_add(&fatal_polls, 1);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, count, timeout);
}
static void server_terminal(struct tr_stream_handle stream,
                            struct tr_rpc_endpoint_result result, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_endpoint *same = f->endpoint;
	CHECK(stream.slot < CAP);
	CHECK((result.delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (result.delivery == TR_RPC_RECORD_ON_OWNER) {
		CHECK(tr_rpc_record_endpoint_close_on_owner(&same) ==
		      TR_ERR_STATE);
		bool drained = true;
		CHECK(tr_rpc_record_endpoint_is_drained_on_owner(
		              f->endpoint, &drained) == TR_OK &&
		      !drained);
	} else {
		CHECK(result.status == TR_RPC_STATUS_CANCELLED &&
		      !result.final_accepted);
		CHECK(tr_rpc_record_endpoint_reap_stopped(&same) ==
		      TR_ERR_STATE);
	}
	CHECK(same == f->endpoint);
	f->server.status[stream.slot] = (uint32_t)result.status;
	f->server.delivery[stream.slot] = result.delivery;
	f->server.final_accepted[stream.slot] = result.final_accepted;
	CHECK(atomic_fetch_add(&f->server.terminals[stream.slot], 1) == 0);
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
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		for (size_t j = 0; j < view->header.decoded_size; ++j)
			CHECK(view->body[j] == (uint8_t)(j * 7 + 3));
		p->received_bytes[i] += view->header.decoded_size;
		atomic_fetch_add(&p->messages[i], 1);
	} else if (view->header.kind == TR_RPC_RECORD_INITIAL_METADATA) {
		atomic_fetch_add(&p->initial[i], 1);
	}
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	if (status == TR_RPC_STATUS_OK &&
	    atomic_load(&p->fixture->stop_credit_on_final))
		CHECK(tr_stream_stop_credit_on_owner(p->streams[i]) == TR_OK);
	p->status[i] = status;
	p->delivery[i] = delivery;
	CHECK(atomic_fetch_add(&p->terminals[i], 1) == 0);
}
static enum tr_stream_data_disposition on_data(struct tr_stream_handle stream,
                                               uint64_t id, struct tr_buffer *b,
                                               void *arg)
{
	struct peer *p = arg;
	(void)id;
	CHECK(!p->server);
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(stream.slot < CAP && p->calls[stream.slot]);
	CHECK(tr_rpc_record_stream_receive_on_owner(
	              p->calls[stream.slot], stream, b->data, b->len) == TR_OK);
	if (atomic_load(&p->fixture->hold_final_credit) &&
	    b->len >= TR_RPC_RECORD_HEADER_SIZE &&
	    tr_get_le16(b->data + 6) == TR_RPC_RECORD_FINAL_STATUS)
		gate_wait(&p->fixture->credit_gate, true);
	atomic_fetch_add(&p->packets, 1);
	if (p->hold) {
		CHECK(!p->held);
		p->held = b;
		p->held_stream = stream;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void on_event(struct tr_stream_handle stream, enum tr_stream_event event,
                     int status, void *arg)
{
	struct peer *p = arg;
	CHECK(!p->server);
	if (p->disabled)
		return;
	CHECK(stream.slot < CAP);
	if (event == TR_STREAM_EVENT_OPENED) {
		p->streams[stream.slot] = stream;
		CHECK(p->calls[stream.slot]);
		const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
		struct tr_rpc_record_header h = {
		        .kind = TR_RPC_RECORD_OPEN,
		        .service_id = 7,
		        .method_id = 9 + p->method[stream.slot],
		        .metadata_size = 7};
		/* STREAM_OPEN_ACK 的 owner 回调中立即提交 OPEN；不能在 OPENING
		 * 阶段发送，也不能等到后续独立命令才建立本地调用语义。 */
		if (!p->withhold_open[stream.slot])
			CHECK(tr_rpc_record_stream_send_on_owner(
			              p->calls[stream.slot], &h, metadata,
			              NULL) == TR_OK);
		atomic_fetch_add(&p->opened, 1);
	} else if (p->calls[stream.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[stream.slot],
		                                          stream, event,
		                                          status) == TR_OK);
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
/* Channel 通知发生在既有 owner 上；这里只做原子观测，不改变关闭决策。 */
static void note_event(struct tr_channel *channel, enum tr_channel_event event,
                       int status, void *arg)
{
	struct peer *peer = arg;

	(void)channel;
	atomic_store(&peer->last_channel_event, (int)event);
	atomic_store(&peer->last_channel_status, status);
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN)
		atomic_fetch_add(&peer->channel_down_events, 1);
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
	struct tr_channel_config cc = {
	        .role = server ? TR_CHANNEL_SERVER : TR_CHANNEL_CLIENT,
	        .mode = TR_CHANNEL_SHARED_CONNECTION,
	        .feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET,
	        .max_streams = CAP,
	        .initial_window_bytes = window,
	        .window_update_threshold_bytes = 1,
	        .max_message_bytes = BODY + 1024};
	struct tr_conn_handle conn;
	*p = (struct peer){.fixture = f, .server = server};
	atomic_init(&p->opened, 0);
	atomic_init(&p->packets, 0);
	atomic_init(&p->last_channel_event, 0);
	atomic_init(&p->last_channel_status, 0);
	atomic_init(&p->channel_down_events, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->messages[i], 0);
		atomic_init(&p->initial[i], 0);
		atomic_init(&p->ingress_bytes[i], 0);
	}
	tr_memory_budget_init(&p->budget, 16 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	p->connection = conn;
	if (server) {
		observed_connection = conn;
		atomic_store(&observed_fd, fd);
	}
	CHECK(tr_channel_create(&cc, conn, conn, server ? NULL : on_data,
	                        server ? NULL : on_event, note_event, p,
	                        &p->channel) == TR_OK);
}
static int start_server(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config cfg = {2, 4};
	struct tr_rpc_record_endpoint_config epc = {
	        .dispatch = f->config,
	        .completion_limit = 2,
	        .progress_limit = 2,
	        .progress_period_ns = 1000000,
	        .final_wait_ns = f->final_wait_ns,
	        .open_timeout_ns = f->open_timeout_ns};
	CHECK(tr_task_executor_create(&cfg, &f->server.budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	f->executor_budget = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_rpc_record_endpoint_create_on_owner(
	              f->server.channel, f->executor, f->methods, 4, &epc,
	              server_terminal, f, &f->server.budget,
	              &f->endpoint) == TR_OK);
	capture_timer = true;
	capture_endpoint = f;
	CHECK(tr_rpc_record_endpoint_start_on_owner(f->endpoint) == TR_OK);
	capture_endpoint = NULL;
	capture_timer = false;
	return TR_OK;
}
static void fixture_init_full(struct fixture *f, uint64_t window,
                              uint32_t pending, uint64_t open_timeout_ns,
                              uint64_t final_wait_ns)
{
	int a = -1, b = -1;
	*f = (struct fixture){.config = {CAP, pending, 4, BODY * 4},
	                      .batch = 2,
	                      .open_timeout_ns = open_timeout_ns,
	                      .final_wait_ns = final_wait_ns};
	gate_init(&f->gate);
	gate_init(&f->credit_gate);
	atomic_init(&f->use_gate, false);
	atomic_init(&f->hold_final_credit, false);
	atomic_init(&f->stop_credit_on_final, false);
	atomic_init(&f->reject_method, 0);
	atomic_init(&f->bad_reply, false);

	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&f->business[i].active, 0);
		for (unsigned j = 0; j < 6; ++j)
			atomic_init(&f->business[i].seen[j], 0);
	}
	for (unsigned i = 0; i < 4; ++i) {
		f->contexts[i] = (struct method_context){f, i};
		f->methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {7, 9 + i,
		                       i < 2 ? TR_RPC_ONE : TR_RPC_MANY,
		                       (i & 1) ? TR_RPC_MANY : TR_RPC_ONE,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_BULK, BODY, BODY},
		        .work = handler,
		        .arg = &f->contexts[i]};
	}
	tcp_pair(&a, &b);
	peer_init(&f->client, f, a, false, window);
	peer_init(&f->server, f, b, true, window);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_reactor_call(f->server.reactor, start_server, f) == TR_OK);
	atomic_store(&observed_channel, f->server.channel);
	atomic_store(&client_channel, f->client.channel);
	atomic_store(&corrupt_open, false);
	atomic_store(&reject_close, false);
	atomic_store(&fail_detach, false);
	atomic_store(&fail_timer_detach, false);
	atomic_store(&close_attempts, 0);
	atomic_store(&hard_close_target, UINT32_MAX);
	atomic_store(&hard_close_attempts, 0);
	atomic_store(&hard_close_block, false);
	atomic_store(&premature_close, 0);
	atomic_store(&fatal_polls, 0);
	atomic_store(&fatal_reactor, f->server.reactor);
	atomic_store(&fail_poll, false);
	atomic_store(&blocked_kind, 0);
	atomic_store(&blocked_fd, -1);
	atomic_store(&blocked_writes, 0);
	atomic_store(&hold_final_transmit, false);
	atomic_store(&abort_kind, 0);
	for (unsigned i = 0; i < 6; ++i)
		atomic_store(&send_attempts[i], 0);
	atomic_store(&final_attempt_mask, 0);
	for (unsigned i = 0; i < CAP; ++i)
		atomic_store(&final_accepted[i], 0);
}
static void fixture_init_open_timeout(struct fixture *f, uint64_t window,
                                      uint32_t pending,
                                      uint64_t open_timeout_ns)
{
	fixture_init_full(f, window, pending, open_timeout_ns,
	                  UINT64_C(30000000000));
}
static void fixture_init(struct fixture *f, uint64_t window, uint32_t pending)
{
	fixture_init_open_timeout(f, window, pending, 0);
}
struct client_create {
	struct fixture *f;
	struct tr_stream_handle stream;
	unsigned method;
	bool withhold_open;
	bool saturate_control;
	const char *phase;
	unsigned retries;
};
/* 失败时才查询真实 Channel/传输快照，保持成功热路径和准入不变。
 * 只读接口各自锁定状态，并非同一时刻原子快照。 */
static void log_open_failure(const struct client_create *r, int result)
{
	struct tr_channel_stats client = {0}, server = {0};
	enum tr_connection_state conn_state = TR_CONN_FREE;
	int client_ret = tr_channel_get_stats(r->f->client.channel, &client);
	int server_ret = tr_channel_get_stats(r->f->server.channel, &server);
	struct tr_conn_handle conn = r->f->client.connection;
	int state_ret = tr_reactor_get_connection_state(conn, &conn_state);

	fprintf(stderr, "慢 OPEN 变体：%u\n", r->f->slow_variant);
	fprintf(stderr,
	        "STREAM_OPEN 失败快照：phase=%s method=%u ret=%d "
	        "active=%u retries=%u conn_ret=%d conn_state=%d "
	        "client_stats=%d client_state=%d lanes=%d/%d "
	        "client_conn=%d/%d opened=%llu closed=%llu errors=%llu "
	        "server_stats=%d server_state=%d lanes=%d/%d "
	        "server_conn=%d/%d opened=%llu closed=%llu errors=%llu\n",
	        r->phase ? r->phase : "未指定", r->method, result,
	        tr_channel_active_streams(r->f->client.channel), r->retries,
	        state_ret, (int)conn_state, client_ret, (int)client.state,
	        (int)client.control_lane_state, (int)client.bulk_lane_state,
	        (int)client.control_connection.state,
	        (int)client.bulk_connection.state,
	        (unsigned long long)client.streams_opened,
	        (unsigned long long)client.streams_closed,
	        (unsigned long long)client.stream_errors, server_ret,
	        (int)server.state, (int)server.control_lane_state,
	        (int)server.bulk_lane_state,
	        (int)server.control_connection.state,
	        (int)server.bulk_connection.state,
	        (unsigned long long)server.streams_opened,
	        (unsigned long long)server.streams_closed,
	        (unsigned long long)server.stream_errors);
	fprintf(stderr,
	        "Channel 事件：client=%d status=%d down=%u "
	        "server=%d status=%d down=%u\n",
	        atomic_load(&r->f->client.last_channel_event),
	        atomic_load(&r->f->client.last_channel_status),
	        atomic_load(&r->f->client.channel_down_events),
	        atomic_load(&r->f->server.last_channel_event),
	        atomic_load(&r->f->server.last_channel_status),
	        atomic_load(&r->f->server.channel_down_events));
}
static int client_open(void *arg)
{
	struct client_create *r = arg;
	struct peer *p = &r->f->client;
	struct tr_rpc_record_stream_config cfg = {
	        TR_RPC_RECORD_REQUEST_DIRECTION,
	        r->f->methods[r->method < 4 ? r->method : 0].descriptor};
	struct tr_rpc_record_stream_callbacks cb = {client_received,
	                                            client_terminal, p};
	cfg.method.method_id = 9 + r->method;
	if (r->saturate_control) {
		struct tr_reactor_stats stats;
		int ret;
		/* 同一所有者命令内填满真实控制发送池；当前命令返回前
		 * SEND 无法被消费。PONG 不触发对端再回包，不改变流身份。 */
		r->saturate_control = false;
		CHECK(tr_reactor_get_stats(p->reactor, &stats) == TR_OK);
		for (unsigned i = 0; i < stats.control_tx_item_pool.capacity;
		     ++i) {
			ret = tr_reactor_send(p->connection, TR_FRAME_PONG, 0,
			                      0, 0, NULL);
			if (ret != TR_OK)
				break;
		}
		CHECK(tr_reactor_get_stats(p->reactor, &stats) == TR_OK);
		CHECK(stats.control_tx_item_pool.current ==
		      stats.control_tx_item_pool.capacity);
	}
	struct tr_stream_handle before = r->stream;
	int ret = tr_stream_open(p->channel, TR_LANE_BULK, &r->stream);
	if (ret != TR_OK) {
		/* 只允许在尚未接管网络流时重试；已接受后的记录绑定失败
		 * 仍由原断言拒绝，不能重新 OPEN 隐藏部分成功。 */
		CHECK(r->stream.channel == before.channel &&
		      r->stream.slot == before.slot &&
		      r->stream.generation == before.generation);
		if (ret != TR_AGAIN)
			log_open_failure(r, ret);
		return ret;
	}
	CHECK(tr_rpc_record_stream_create_on_owner(
	              r->stream, &cfg, &cb, &p->budget,
	              &p->calls[r->stream.slot]) == TR_OK);
	p->method[r->stream.slot] = r->method;
	p->withhold_open[r->stream.slot] = r->withhold_open;
	return TR_OK;
}
static unsigned open_call_request(struct client_create *r)
{
	struct fixture *f = r->f;
	unsigned opened = atomic_load(&f->client.opened);
	uint64_t end = now_ms() + 15000;
	int ret;

	/* 必须退出所有者命令后再重试，让 SEND 前驱和释放回调推进。
	 * 所有尝试共用原有15秒预算；其他错误立即失败，不扩大资源池。 */
	while ((ret = tr_reactor_call(f->client.reactor, client_open, r)) ==
	       TR_AGAIN) {
		r->retries++;
		CHECK(now_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	while (atomic_load(&f->client.opened) < opened + 1) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	return r->stream.slot;
}
static unsigned open_call_stage(struct fixture *f, unsigned method,
                                bool withhold_open, const char *phase)
{
	struct client_create r = {0};

	r.f = f;
	r.method = method;
	r.withhold_open = withhold_open;
	r.phase = phase;
	return open_call_request(&r);
}
static unsigned open_call_mode(struct fixture *f, unsigned method,
                               bool withhold_open)
{
	return open_call_stage(f, method, withhold_open, "普通调用");
}
static unsigned open_call(struct fixture *f, unsigned method)
{
	return open_call_mode(f, method, false);
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
	/* OPEN 已在 STREAM_OPEN_ACK 的所有者回调中提交。 */
	for (unsigned i = 0; i < messages; ++i)
		send_record(f, slot, method, TR_RPC_RECORD_MESSAGE, size);
	send_record(f, slot, method, TR_RPC_RECORD_HALF_CLOSE, 0);
}
static int disable_peer(void *arg)
{
	((struct peer *)arg)->disabled = true;
	return TR_OK;
}
static int close_endpoint(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_endpoint_close_on_owner(&f->endpoint);
}
static int destroy_executor(void *arg)
{
	struct fixture *f = arg;
	size_t count;
	CHECK(tr_task_executor_stop(f->executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_dispatch(f->executor, 8, &count) == TR_OK);
	int ret = tr_task_executor_join(f->executor);
	return ret == TR_OK ? tr_task_executor_destroy(&f->executor) : ret;
}
static int until_owner(struct fixture *f, int (*fn)(void *))
{
	uint64_t end = now_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(f->server.reactor, fn, f)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	return ret;
}
static void fixture_destroy(struct fixture *f, bool stopped)
{
	if (!stopped) {
		if (f->endpoint)
			CHECK(until_owner(f, close_endpoint) == TR_OK);
		CHECK(until_owner(f, destroy_executor) == TR_OK);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	}
	CHECK(!f->endpoint && !f->executor);
	CHECK(tr_reactor_call(f->client.reactor, disable_peer, &f->client) ==
	      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.calls[i]) == TR_OK);
	struct peer *peers[] = {&f->client, &f->server};
	for (unsigned i = 0; i < 2; ++i) {
		CHECK(!peers[i]->held);
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->pool) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
	atomic_store(&observed_channel, NULL);
	atomic_store(&observed_fd, -1);
	CHECK(atomic_load(&blocked_fd) == -1);
	atomic_store(&client_channel, NULL);
	atomic_store(&fatal_reactor, NULL);
	gate_destroy(&f->gate);
	gate_destroy(&f->credit_gate);
}
static void check_final(struct fixture *f, unsigned slot,
                        enum tr_rpc_status status)
{
	wait_count(&f->client.terminals[slot], 1);
	wait_count(&f->server.terminals[slot], 1);
	CHECK(f->client.status[slot] == (uint32_t)status);
	CHECK(f->server.status[slot] == (uint32_t)status);
	CHECK(f->server.final_accepted[slot]);
}
static void wait_network_retired(struct fixture *f);
static void test_shapes(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	for (unsigned m = 0; m < 4; ++m) {
		unsigned slot = open_call(&f, m);
		unsigned count = m < 2 ? 1 : 0;
		request(&f, slot, m, count, m == 0 ? BODY : 0);
		check_final(&f, slot, TR_RPC_STATUS_OK);
		CHECK(atomic_load(&f.client.messages[slot]) ==
		      ((m & 1) ? count * 2 : 1));
	}
	fixture_destroy(&f, false);
}
static void test_rejection(bool metadata)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&corrupt_open, metadata);
	unsigned rejected = open_call(&f, metadata ? 0 : 100);
	check_final(&f, rejected,
	            metadata ? TR_RPC_STATUS_INVALID_ARGUMENT
	                     : TR_RPC_STATUS_UNIMPLEMENTED);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(!atomic_load(&f.business[i].seen[TR_RPC_RECORD_OPEN]));
	unsigned good = open_call(&f, 0);
	request(&f, good, 0, 1, 80);
	check_final(&f, good, TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static void test_business_error(bool missing_response)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	if (missing_response)
		atomic_store(&f.bad_reply, true);
	else
		atomic_store(&f.reject_method, 1);
	unsigned slot = open_call(&f, 0);
	if (missing_response)
		request(&f, slot, 0, 1, 80);
	check_final(&f, slot,
	            missing_response ? TR_RPC_STATUS_INTERNAL
	                             : TR_RPC_STATUS_PERMISSION_DENIED);
	/* 缺少 ONE 响应时，错误的 OK 在本地预检即被拒绝，不到 sendv。
	 * 派发器只发送一条合法错误 FINAL，宿主不能再重复终局。 */
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) == 1);
	fixture_destroy(&f, false);
}
static void test_final_backpressure(bool business_success)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	atomic_store(&reject_close, true);
	unsigned slot = open_call(&f, business_success ? 0 : 100);
	enum tr_rpc_status expected = business_success
	                                      ? TR_RPC_STATUS_OK
	                                      : TR_RPC_STATUS_UNIMPLEMENTED;
	if (business_success)
		request(&f, slot, 0, 1, 80);
	wait_count(&send_attempts[TR_RPC_RECORD_FINAL_STATUS], 10);
	CHECK(!atomic_load(&f.client.terminals[slot]) &&
	      !atomic_load(&f.server.terminals[slot]));
	CHECK(!atomic_load(&close_attempts) && !atomic_load(&premature_close));
	atomic_store(&blocked_kind, 0);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&close_attempts, 10);
	CHECK(f.client.status[slot] == (uint32_t)expected);
	CHECK(!atomic_load(&f.server.terminals[slot]));
	unsigned accepted =
	        atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]);
	atomic_store(&reject_close, false);
	check_final(&f, slot, expected);
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) ==
	      accepted);
	fixture_destroy(&f, false);
}
/* CLOSE 硬错误只属于目标流：周期推进持续重试，另一流在同一
 * Channel 上独立完成；解除故障后原 FINAL 不得重发或双报。 */
static void test_close_hard_error_isolation(void)
{
	struct fixture f;
	fixture_init_open_timeout(&f, 1024 * 1024, 8, UINT64_C(15000000000));
	unsigned blocked = open_call(&f, 100);
	wait_count(&f.server.opened, 1);
	CHECK(f.server.streams[blocked].channel == f.server.channel);
	atomic_store(&hard_close_target, f.server.streams[blocked].slot);
	atomic_store(&hard_close_block, true);
	wait_count(&hard_close_attempts, 3);
	wait_count(&f.client.terminals[blocked], 1);
	CHECK(!atomic_load(&f.server.terminals[blocked]));
	unsigned finals =
	        atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]);
	CHECK(finals == 1);

	unsigned healthy = open_call(&f, 0);
	request(&f, healthy, 0, 1, 80);
	check_final(&f, healthy, TR_RPC_STATUS_OK);
	CHECK(!atomic_load(&f.server.terminals[blocked]));
	CHECK(f.client.status[blocked] == TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) ==
	      finals + 1);
	atomic_store(&hard_close_block, false);
	check_final(&f, blocked, TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) ==
	      finals + 1);
	CHECK(atomic_load(&hard_close_attempts) >= 3);
	CHECK(atomic_load(&final_accepted[blocked]) == 1 &&
	      atomic_load(&final_accepted[healthy]) == 1);
	CHECK(tr_stream_close(f.client.streams[blocked]) == TR_OK);
	CHECK(tr_stream_close(f.client.streams[healthy]) == TR_OK);
	wait_network_retired(&f);
	fixture_destroy(&f, false);
}
/* 多条流的 FINAL 和 CLOSE 先后受阻；持续 CLOSE 重试按周期推进，
 * 恢复后每条流仍只交付一次原终局。 */
static void test_parallel_final_close_pressure(void)
{
	struct fixture f;
	unsigned slots[CAP];
	struct timespec pause = {0, 50000000};
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	atomic_store(&reject_close, true);
	for (unsigned i = 0; i < CAP; ++i)
		slots[i] = open_call(&f, i & 1 ? 0 : 100);
	CHECK(tr_channel_active_streams(f.client.channel) == CAP);
	uint64_t open_until = now_ms() + 15000;
	while (tr_channel_active_streams(f.server.channel) != CAP) {
		CHECK(now_ms() < open_until);
		sched_yield();
	}
	for (unsigned i = 1; i < CAP; i += 2)
		request(&f, slots[i], 0, 1, 80);
	wait_count(&final_attempt_mask, (1U << CAP) - 1);
	CHECK(!atomic_load(&close_attempts));
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(!atomic_load(&f.client.terminals[slots[i]]) &&
		      !atomic_load(&f.server.terminals[slots[i]]));
	atomic_store(&blocked_kind, 0);
	for (unsigned i = 0; i < CAP; ++i)
		wait_count(&f.client.terminals[slots[i]], 1);
	wait_count(&close_attempts, 10);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(!atomic_load(&f.server.terminals[slots[i]]));
	uint64_t before_ms = now_ms();
	unsigned before_attempts = atomic_load(&close_attempts);
	while (nanosleep(&pause, &pause) != 0)
		CHECK(errno == EINTR);
	uint64_t elapsed_ms = now_ms() - before_ms;
	unsigned attempts = atomic_load(&close_attempts) - before_attempts;
	CHECK(attempts > 0 && attempts <= CAP * (elapsed_ms + 2));
	atomic_store(&reject_close, false);
	for (unsigned i = 0; i < CAP; ++i)
		check_final(&f, slots[i],
		            i & 1 ? TR_RPC_STATUS_OK
		                  : TR_RPC_STATUS_UNIMPLEMENTED);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(atomic_load(&final_accepted[i]) == 1);
	for (unsigned i = 1; i < CAP; i += 2)
		CHECK(atomic_load(&f.client.messages[slots[i]]) == 1 &&
		      f.client.received_bytes[slots[i]] == 80);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(tr_stream_close(f.client.streams[slots[i]]) == TR_OK);
	wait_network_retired(&f);
	fixture_destroy(&f, false);
}
static int hold_client(void *arg)
{
	((struct fixture *)arg)->client.hold = true;
	return TR_OK;
}
static int release_client(void *arg)
{
	struct fixture *f = arg;
	f->client.hold = false;
	CHECK(f->client.held);
	int ret = tr_stream_release_payload(f->client.held_stream,
	                                    f->client.held);
	f->client.held = NULL;
	return ret;
}
static void test_real_window(void)
{
	struct fixture f;
	fixture_init(&f, 128, 8);
	unsigned slot = open_call(&f, 0);
	CHECK(tr_reactor_call(f.client.reactor, hold_client, &f) == TR_OK);
	request(&f, slot, 0, 1, 80);
	wait_count(&f.client.initial[slot], 1);
	wait_count(&send_attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(!atomic_load(&f.client.messages[slot]));
	CHECK(!atomic_load(&f.server.terminals[slot]));
	CHECK(tr_reactor_call(f.client.reactor, release_client, &f) == TR_OK);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
struct peer_reset_request {
	struct tr_stream_handle stream;
	int reason;
};
static int reset_peer_on_owner(void *arg)
{
	struct peer_reset_request *r = arg;
	return tr_stream_reset_on_owner(r->stream, r->reason);
}
static void wait_network_retired(struct fixture *f)
{
	uint64_t end = now_ms() + 15000;
	while (tr_channel_active_streams(f->client.channel) ||
	       tr_channel_active_streams(f->server.channel)) {
		CHECK(now_ms() < end);
		sched_yield();
	}
}
struct fragment_request {
	struct tr_stream_handle stream;
	struct tr_buffer *payload;
};
static int send_fragment_on_owner(void *arg)
{
	struct fragment_request *r = arg;
	int ret = tr_stream_send(r->stream, r->payload);
	if (ret == TR_OK)
		r->payload = NULL;
	return ret;
}
static void send_fragment(struct fixture *f, unsigned slot, const uint8_t *data,
                          size_t size)
{
	struct fragment_request r = {f->client.streams[slot], body_new(size)};
	memcpy(r.payload->data, data, size);
	uint64_t end = now_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(f->client.reactor, send_fragment_on_owner,
	                              &r)) == TR_AGAIN) {
		CHECK(r.payload && now_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r.payload);
}
static int endpoint_quiet(void *arg)
{
	struct fixture *f = arg;
	bool quiet;
	int ret =
	        tr_rpc_record_endpoint_is_drained_on_owner(f->endpoint, &quiet);
	return ret == TR_OK ? quiet ? TR_OK : TR_AGAIN : ret;
}
static int prepare_server_reuse(void *arg)
{
	struct fixture *f = arg;
	CHECK(endpoint_quiet(f) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		atomic_store(&f->server.terminals[i], 0);
	return TR_OK;
}
static int prepare_client_reuse(void *arg)
{
	struct fixture *f = arg;
	for (unsigned i = 0; i < CAP; ++i) {
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_destroy_on_owner(
			              &f->client.calls[i]) == TR_OK);
		atomic_store(&f->client.terminals[i], 0);
		atomic_store(&f->client.messages[i], 0);
		atomic_store(&f->client.initial[i], 0);
	}
	return TR_OK;
}
static void test_slow_open(unsigned partial)
{
	struct fixture f;
	/* 真实 timer 驱动总期限；宽裕时间只避免调度抖动影响造包阶段。
	 * 纳秒级精确边界和不续期由派发器专测独立核验。final_wait 仍为30s。 */
	fixture_init_open_timeout(&f, 1024 * 1024, 8, UINT64_C(2000000000));
	atomic_store(&f.stop_credit_on_final, true);
	f.slow_variant = partial;
	uint64_t baseline = atomic_load(&f.server.budget.current_bytes);
	unsigned healthy = open_call_stage(&f, 3, false, "慢 OPEN/健康流");
	atomic_store(&f.use_gate, true);
	request(&f, healthy, 3, 1, 80);
	gate_wait(&f.gate, false);
	CHECK(atomic_load(&f.business[0].active) == 1);
	unsigned slow = open_call_stage(&f, 0, true, "慢 OPEN/等待超时");
	CHECK(slow != healthy);
	wait_count(&f.server.opened, 2);
	struct tr_stream_handle old = f.client.streams[slow];
	if (partial) {
		uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
		const struct tr_rpc_record_header header = {
		        .kind = TR_RPC_RECORD_OPEN,
		        .service_id = 7,
		        .method_id = 9,
		        .metadata_size = 7,
		        .remaining_timeout_ns = UINT64_MAX};
		const struct tr_rpc_record_limits limits = {
		        BODY, BODY, 7, TR_RPC_RECORD_ACCEPT_NONE};
		const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
		CHECK(tr_rpc_record_encode_header(open, sizeof(open), &header,
		                                  &limits) == TR_OK);
		memcpy(open + TR_RPC_RECORD_HEADER_SIZE, metadata,
		       sizeof(metadata));
		/* 每段都是实际传输消息，Endpoint 的记录解析器跨消息持有前缀。
		 */
		send_fragment(&f, slow, open, 7);
		wait_count(&f.server.ingress_bytes[slow], 7);
		if (partial == 2) {
			send_fragment(&f, slow, open + 7, sizeof(open) - 8);
			wait_count(&f.server.ingress_bytes[slow],
			           sizeof(open) - 1);
		}
	}
	wait_count(&f.client.terminals[slow], 1);
	wait_count(&f.server.terminals[slow], 1);
	CHECK(f.client.status[slow] == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      f.server.status[slow] == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      !f.server.final_accepted[slow]);
	CHECK(!atomic_load(&f.client.initial[slow]) &&
	      !atomic_load(&f.client.messages[slow]) &&
	      !atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) &&
	      !atomic_load(&close_attempts));
	CHECK(!atomic_load(&f.business[1].seen[TR_RPC_RECORD_OPEN]) &&
	      atomic_load(&f.business[0].active) == 1 &&
	      !atomic_load(&f.client.terminals[healthy]) &&
	      !atomic_load(&f.server.terminals[healthy]));
	/* 另一流的工作仍在真实 gate 内。两端从2条网络流收敛到1条，证明
	 * 慢 OPEN 走了对称 RESET，不以关闭连接或伪造 worker done 回收。 */
	uint64_t end = now_ms() + 15000;
	while (tr_channel_active_streams(f.client.channel) != 1 ||
	       tr_channel_active_streams(f.server.channel) != 1) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	gate_open(&f.gate);
	check_final(&f, healthy, TR_RPC_STATUS_OK);
	CHECK(tr_stream_close(f.client.streams[healthy]) == TR_OK);
	wait_network_retired(&f);
	CHECK(until_owner(&f, endpoint_quiet) == TR_OK);
	CHECK(atomic_load(&f.server.budget.current_bytes) == baseline);
	CHECK(tr_reactor_call(f.server.reactor, prepare_server_reuse, &f) ==
	      TR_OK);
	CHECK(tr_reactor_call(f.client.reactor, prepare_client_reuse, &f) ==
	      TR_OK);
	unsigned filler = open_call_stage(&f, 0, false, "慢 OPEN/复用1");
	unsigned reused = open_call_stage(&f, 0, false, "慢 OPEN/复用2");
	CHECK(filler == healthy && reused == slow &&
	      f.client.streams[reused].generation != old.generation);
	struct peer_reset_request stale = {old, TR_ERR_TIMEOUT};
	CHECK(tr_reactor_call(f.client.reactor, reset_peer_on_owner, &stale) ==
	      TR_ERR_STALE);
	request(&f, filler, 0, 1, 80);
	request(&f, reused, 0, 1, 80);
	check_final(&f, filler, TR_RPC_STATUS_OK);
	check_final(&f, reused, TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
/* 终局回调可先于 Channel 消费信用返回；应用同时关闭本地半流时，
 * 已退役的对端流不能因迟到 WINDOW_UPDATE 使整条连接中止。 */
static void test_close_before_credit(void)
{
	struct fixture f;
	unsigned slot, reused;

	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&f.stop_credit_on_final, true);
	atomic_store(&f.hold_final_credit, true);
	slot = open_call(&f, 0);
	request(&f, slot, 0, 1, 80);
	check_final(&f, slot, TR_RPC_STATUS_OK);
	gate_wait(&f.credit_gate, false);
	CHECK(tr_stream_close(f.client.streams[slot]) == TR_OK);
	gate_open(&f.credit_gate);
	wait_network_retired(&f);
	CHECK(until_owner(&f, endpoint_quiet) == TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, prepare_server_reuse, &f) ==
	      TR_OK);
	CHECK(tr_reactor_call(f.client.reactor, prepare_client_reuse, &f) ==
	      TR_OK);
	reused = open_call_stage(&f, 0, false, "终局后信用/槽位复用");
	CHECK(reused == slot);
	request(&f, reused, 0, 1, 80);
	check_final(&f, reused, TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static void test_peer_reset(bool after_final)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned method = after_final ? 0 : 3;
	unsigned slot = open_call(&f, method);
	CHECK(slot == 0);
	atomic_store(&f.use_gate, !after_final);
	request(&f, slot, method, 1, 80);
	if (after_final) {
		check_final(&f, slot, TR_RPC_STATUS_OK);
	} else {
		gate_wait(&f.gate, false);
		CHECK(atomic_load(&f.business[0].active) == 1 &&
		      !atomic_load(&f.server.terminals[slot]));
	}
	struct peer_reset_request reset = {f.client.streams[slot],
	                                   TR_ERR_TIMEOUT};
	CHECK(tr_reactor_call(f.client.reactor, reset_peer_on_owner, &reset) ==
	      TR_OK);
	wait_count(&f.client.terminals[slot], 1);
	wait_network_retired(&f);
	if (!after_final) {
		/* 两端真实 RESET 已完成，业务仍在 gate 中使用拥有型正文。
		 * Endpoint 不得把 Channel 静默冒充 worker done。 */
		CHECK(!atomic_load(&f.server.terminals[slot]) &&
		      atomic_load(&f.business[0].active) == 1);
		CHECK(f.client.status[slot] ==
		              TR_RPC_STATUS_DEADLINE_EXCEEDED &&
		      !atomic_load(&f.client.messages[slot]));
		gate_open(&f.gate);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(f.server.status[slot] ==
		              TR_RPC_STATUS_DEADLINE_EXCEEDED &&
		      !f.server.final_accepted[slot] &&
		      !atomic_load(&f.business[0].active) &&
		      !atomic_load(&f.client.messages[slot]));
	} else {
		/* Server 的 FINAL+CLOSE 已接受；后到 RESET 仍应对称响应，
		 * 已有的两端 OK 终局与 FINAL 接受证据保持不变。 */
		CHECK(f.client.status[slot] == TR_RPC_STATUS_OK &&
		      f.server.status[slot] == TR_RPC_STATUS_OK &&
		      f.server.final_accepted[slot]);
	}
	CHECK(atomic_load(&f.client.terminals[slot]) == 1 &&
	      atomic_load(&f.server.terminals[slot]) == 1);
	fixture_destroy(&f, false);
}
static void test_shutdown_failure(unsigned stage, bool withhold_open)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 100);
	check_final(&f, slot, TR_RPC_STATUS_UNIMPLEMENTED);
	if (stage == 0)
		atomic_store(&fail_detach, true);
	else
		atomic_store(&fail_timer_detach, true);
	CHECK(until_owner(&f, close_endpoint) == TR_ERR_SYS);
	CHECK(f.endpoint && atomic_load(&f.server.budget.current_bytes) > 0);
	if (stage == 0) {
		/* 派发器已释放但处理器解绑失败，新的流必须被安全拒绝，
		 * 不得访问旧派发器或遗失新的关闭责任。 */
		unsigned late = open_call_mode(&f, 0, withhold_open);
		check_final(&f, late, TR_RPC_STATUS_UNAVAILABLE);
	}
	CHECK(until_owner(&f, close_endpoint) == TR_OK && !f.endpoint);
	fixture_destroy(&f, false);
}
static int inject_fatal(void *arg)
{
	(void)arg;
	atomic_store(&fail_poll, true);
	return TR_OK;
}
static void test_stopped_recovery(bool fatal)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 3);
	atomic_store(&f.use_gate, true);
	request(&f, slot, 3, 1, 80);
	gate_wait(&f.gate, false);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_ERR_STATE);
	if (fatal) {
		CHECK(tr_reactor_call(f.server.reactor, inject_fatal, &f) ==
		      TR_OK);
		wait_count(&fatal_polls, 1);
	}
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminals[slot]));
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
	CHECK(tr_task_executor_dispatch(f.executor, 8, &delivered) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.endpoint) == TR_OK);
	CHECK(atomic_load(&f.server.terminals[slot]) == 1);
	CHECK(f.server.delivery[slot] == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(!f.server.final_accepted[slot]);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	fixture_destroy(&f, true);
}
static void test_inline_disconnect(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&abort_kind, TR_RPC_RECORD_MESSAGE);
	unsigned slot = open_call(&f, 0);
	request(&f, slot, 0, 1, 80);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(!f.server.final_accepted[slot] &&
	      f.client.status[slot] != TR_RPC_STATUS_OK);
	CHECK(atomic_load(&abort_kind) == 0);
	fixture_destroy(&f, false);
	puts("rpc_record_endpoint_tcp: inline disconnect passed");
}
static void test_final_timeout(void)
{
	struct fixture f;
	fixture_init_full(&f, 1024 * 1024, 8, 0, UINT64_C(20000000));
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	unsigned slot = open_call(&f, 100);
	wait_count(&f.server.terminals[slot], 1);
	wait_count(&f.client.terminals[slot], 1);
	/* 当前采用预留 RESET，而非旧端点的普通 CLOSE。详细方法错误未
	 * 通过 FINAL 送达；有限传输映射只报告 INTERNAL，不伪造原错误。 */
	CHECK(f.server.status[slot] == TR_RPC_STATUS_UNIMPLEMENTED &&
	      !f.server.final_accepted[slot]);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_INTERNAL);
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) > 0 &&
	      atomic_load(&close_attempts) == 0);
	atomic_store(&blocked_kind, 0);
	fixture_destroy(&f, false);
	puts("rpc_record_endpoint_tcp: FINAL timeout without delivery passed");
}
/* 与契约替身持有指针不同，这里阻塞真实 sendmsg；Endpoint 已销毁后，
 * 同一个 Reactor 仍须把原错误帧写给对端，再归还独立帧预算。 */
static void test_tx_survives_endpoint(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&hold_final_transmit, true);
	unsigned slot = open_call(&f, 100);
	wait_count(&f.server.terminals[slot], 1);
	wait_count(&blocked_writes, 1);
	CHECK(f.server.final_accepted[slot]);
	CHECK(!atomic_load(&f.client.terminals[slot]));
	CHECK(until_owner(&f, close_endpoint) == TR_OK && !f.endpoint);
	CHECK(atomic_load(&f.server.budget.current_bytes) > f.executor_budget);
	atomic_store(&blocked_fd, -1);
	wait_count(&f.client.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(atomic_load(&send_attempts[TR_RPC_RECORD_FINAL_STATUS]) == 1);
	uint64_t end = now_ms() + 15000;
	while (atomic_load(&f.server.budget.current_bytes) !=
	       f.executor_budget) {
		CHECK(now_ms() < end);
		sched_yield();
	}
	fixture_destroy(&f, false);
	puts("rpc_record_endpoint_tcp: TX outlives endpoint passed");
}
static int recycle_client(void *arg)
{
	struct fixture *f = arg;
	int ret = tr_stream_close(f->client.streams[0]);
	if (ret != TR_OK)
		return ret;
	return tr_rpc_record_stream_destroy_on_owner(&f->client.calls[0]);
}
static int server_retired(void *arg)
{
	struct fixture *f = arg;
	struct tr_stream_diagnostics stats;
	int ret = tr_stream_get_diagnostics(f->server.streams[0], &stats);
	if (ret == TR_OK)
		return TR_AGAIN;
	CHECK(ret == TR_ERR_STALE);
	/* 执行有限完整轮转，等待宿主旧调用退役；不是增加后台推进源。 */
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(tr_rpc_record_endpoint_pump_on_owner(f->endpoint) ==
		      TR_OK);
	return TR_OK;
}
static void test_real_slot_reuse(void)
{
	struct fixture f;
	/* 使用已有观察包装记录服务器的真实传输身份，不访问宿主私有槽。 */
	fixture_init_open_timeout(&f, 1024 * 1024, 8, UINT64_C(15000000000));
	uint64_t generation = 0;
	for (unsigned round = 0; round < 8; ++round) {
		unsigned slot = open_call(&f, 0);
		CHECK(slot == 0 &&
		      f.client.streams[slot].generation != generation);
		generation = f.client.streams[slot].generation;
		request(&f, slot, 0, 1, 20);
		check_final(&f, slot, TR_RPC_STATUS_OK);
		uint64_t end = now_ms() + 15000;
		int ret;
		while ((ret = tr_reactor_call(f.client.reactor, recycle_client,
		                              &f)) == TR_AGAIN) {
			CHECK(now_ms() < end);
			sched_yield();
		}
		CHECK(ret == TR_OK && !f.client.calls[slot]);
		CHECK(until_owner(&f, server_retired) == TR_OK);
		atomic_store(&f.client.terminals[slot], 0);
		atomic_store(&f.server.terminals[slot], 0);
		atomic_store(&f.client.messages[slot], 0);
		atomic_store(&f.client.initial[slot], 0);
	}
	fixture_destroy(&f, false);
	puts("rpc_record_endpoint_tcp: 8 real slot generations passed");
}
static void test_open_tx_pressure(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	struct client_create r = {.f = &f, .saturate_control = true};
	unsigned slot = open_call_request(&r);
	CHECK(r.retries > 0 && !r.saturate_control);
	CHECK(atomic_load(&f.client.opened) == 1);
	request(&f, slot, 0, 1, 80);
	check_final(&f, slot, TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.business[slot].seen[TR_RPC_RECORD_OPEN]) == 1);
	fixture_destroy(&f, false);
	puts("rpc_record_endpoint_tcp: bounded OPEN admission passed");
}
int main(void)
{
	test_tx_survives_endpoint();
	test_inline_disconnect();
	test_final_timeout();
	test_real_slot_reuse();
	test_open_tx_pressure();
	test_close_before_credit();
	for (unsigned partial = 0; partial < 3; ++partial)
		test_slow_open(partial);
	puts("rpc_record_endpoint_tcp: 3 slow OPEN cases passed");
	test_shapes();
	test_rejection(false);
	test_rejection(true);
	test_business_error(false);
	test_business_error(true);
	test_final_backpressure(false);
	test_final_backpressure(true);
	test_close_hard_error_isolation();
	test_parallel_final_close_pressure();
	test_real_window();
	test_peer_reset(false);
	test_peer_reset(true);
	test_shutdown_failure(0, false);
	test_shutdown_failure(0, true);
	test_shutdown_failure(1, false);
	test_stopped_recovery(false);
	test_stopped_recovery(true);
	puts("rpc_record_endpoint_tcp: ok");
	return 0;
}
