#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_dispatch.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <poll.h>
#include <errno.h>
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

void *__real_malloc(size_t);
void *__real_calloc(size_t, size_t);
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __real_clock_gettime(clockid_t, struct timespec *);
struct admission_clock {
	uint64_t now, advanced;
	unsigned reads, advance_at, fail_at, invalid;
};
static _Thread_local struct admission_clock *admission_clock;
int __wrap_clock_gettime(clockid_t id, struct timespec *out)
{
	struct admission_clock *clock = admission_clock;
	if (!clock)
		return __real_clock_gettime(id, out);
	CHECK(id == CLOCK_MONOTONIC);
	++clock->reads;
	if (clock->reads == clock->fail_at) {
		errno = EIO;
		return -1;
	}
	if (clock->reads == clock->advance_at)
		clock->now = clock->advanced;
	out->tv_sec = (time_t)(clock->now / UINT64_C(1000000000));
	out->tv_nsec = (long)(clock->now % UINT64_C(1000000000));
	if (clock->invalid == 1)
		out->tv_sec = -1;
	if (clock->invalid == 2)
		out->tv_nsec = -1;
	if (clock->invalid == 3)
		out->tv_nsec = 1000000000L;
	if (clock->invalid == 4)
		out->tv_sec = (time_t)(UINT64_MAX / UINT64_C(1000000000) + 1);
	return 0;
}
static _Thread_local unsigned fail_at, allocations;
static _Atomic bool fail_worker_malloc;
static _Atomic(struct tr_channel *) observed_channel;
static _Atomic unsigned blocked_kind, send_attempts[6];
static _Atomic uintptr_t retry_body;
void *__wrap_malloc(size_t size)
{
	if ((fail_at && ++allocations == fail_at) ||
	    (tr_task_in_worker_context() &&
	     atomic_exchange(&fail_worker_malloc, false)))
		return NULL;
	return __real_malloc(size);
}
void *__wrap_calloc(size_t n, size_t size)
{
	if (fail_at && ++allocations == fail_at)
		return NULL;
	return __real_calloc(n, size);
}
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	if (stream.channel == atomic_load(&observed_channel) && count &&
	    buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE &&
	    !memcmp(buffers[0]->data, "TRPC", 4)) {
		unsigned kind = tr_get_le16(buffers[0]->data + 6);
		CHECK(kind >= 1 && kind <= 5);
		atomic_fetch_add(&send_attempts[kind], 1);
		if (kind == atomic_load(&blocked_kind)) {
			if (kind == TR_RPC_RECORD_MESSAGE && count == 2) {
				uintptr_t prior = atomic_exchange(
				        &retry_body, (uintptr_t)buffers[1]);
				CHECK(prior == 0 ||
				      prior == (uintptr_t)buffers[1]);
			}
			return TR_AGAIN;
		}
	}
	return __real_tr_stream_sendv(stream, buffers, count);
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
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	struct tr_rpc_dispatch_handle handles[CAP];
	_Atomic unsigned opened, packets, terminals[CAP], messages[CAP],
	        initial[CAP];
	uint32_t status[CAP];
	size_t received_bytes[CAP];
	enum tr_rpc_record_delivery delivery[CAP];
	bool final_accepted[CAP];
	bool server, disabled, hold;
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
};
struct fixture {
	struct peer client, server;
	struct tr_task_executor *executor;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_reactor_timer_handle timer;
	struct tr_rpc_record_dispatch_config config;
	struct tr_rpc_dispatch_method methods[4];
	struct method_context contexts[4];
	struct business_state business[CAP];
	struct gate gate;
	_Atomic bool use_gate;
	_Atomic unsigned reject_method;
	struct tr_rpc_record_dispatch_stats stats;
	bool timer_registered, driving;
	_Atomic bool bad_reply, oversized_reply;
	_Atomic unsigned drive_turns;
	unsigned batch;
	uint64_t open_due;
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
	CHECK(tr_task_in_worker_context());
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
		/* 原RX已在owner返回前被写为0xdd并释放。这里只能读拥有型副本。
		 */
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
		if (f->oversized_reply) {
			CHECK(tr_rpc_record_reply_append(reply, NULL,
			                                 BODY + 1) ==
			      TR_ERR_BAD_LENGTH);
			/* 故意忽略错误并返回OK，核心也不得交付截断成功。 */
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
static void server_terminal(struct tr_rpc_dispatch_handle handle,
                            struct tr_stream_handle stream,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery,
                            bool final_accepted, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_dispatch *same = f->dispatch;
	CHECK(stream.slot < CAP && handle.dispatch == f->dispatch);
	CHECK(handle.id.generation ==
	      f->server.handles[stream.slot].id.generation);
	CHECK(atomic_load(&f->business[handle.id.slot].active) == 0);
	CHECK((delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (delivery == TR_RPC_RECORD_ON_OWNER) {
		struct tr_rpc_dispatch_result result;
		CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &result) ==
		      TR_OK);
		CHECK(result.terminal && result.status == status &&
		      result.final_accepted == final_accepted);
		CHECK(tr_rpc_record_dispatch_destroy_on_owner(&same) ==
		      TR_ERR_STATE);
		CHECK(tr_rpc_record_dispatch_retire_on_owner(handle) ==
		      TR_ERR_STATE);
	} else {
		CHECK(status == TR_RPC_STATUS_CANCELLED);
		CHECK(tr_rpc_record_dispatch_reap_stopped(&same) ==
		      TR_ERR_STATE);
	}
	CHECK(same == f->dispatch);
	f->server.status[stream.slot] = (uint32_t)status;
	f->server.delivery[stream.slot] = delivery;
	f->server.final_accepted[stream.slot] = final_accepted;
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
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(stream.slot < CAP);
	if (p->server) {
		int ret = tr_rpc_record_dispatch_receive_on_owner(
		        p->fixture->dispatch, stream, b->data, b->len);
		CHECK(ret == TR_OK || ret == TR_ERR_STATE ||
		      ret == TR_ERR_NOMEM);
		memset(b->data, 0xdd, b->len);
	} else if (p->calls[stream.slot]) {
		CHECK(tr_rpc_record_stream_receive_on_owner(
		              p->calls[stream.slot], stream, b->data, b->len) ==
		      TR_OK);
	}
	atomic_fetch_add(&p->packets, 1);
	if (p->hold) {
		CHECK(!p->held);
		p->held = b;
		p->held_stream = stream;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void on_event(struct tr_stream_handle s, enum tr_stream_event e,
                     int status, void *arg)
{
	struct peer *p = arg;
	if (p->disabled)
		return;
	CHECK(s.slot < CAP);
	if (e == TR_STREAM_EVENT_OPENED) {
		p->streams[s.slot] = s;
		if (p->server) {
			int ret =
			        p->fixture->open_due
			                ? tr_rpc_record_dispatch_admit_until_on_owner(
			                          p->fixture->dispatch, s,
			                          p->fixture->open_due,
			                          &p->handles[s.slot])
			                : tr_rpc_record_dispatch_admit_on_owner(
			                          p->fixture->dispatch, s,
			                          &p->handles[s.slot]);
			CHECK(ret == TR_OK);
		}
		atomic_fetch_add(&p->opened, 1);
	} else if (p->server && p->handles[s.slot].dispatch) {
		CHECK(tr_rpc_record_dispatch_event_on_owner(
		              p->fixture->dispatch, s, e, status) == TR_OK);
	} else if (!p->server && p->calls[s.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[s.slot], s,
		                                          e, status) == TR_OK);
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
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->messages[i], 0);
		atomic_init(&p->initial[i], 0);
	}
	tr_memory_budget_init(&p->budget, 16 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	CHECK(tr_channel_create(&cc, conn, conn, on_data, on_event, NULL, p,
	                        &p->channel) == TR_OK);
}
static uint64_t drive(void *arg, uint64_t now)
{
	struct fixture *f = arg;
	size_t delivered;
	if (f->driving) {
		atomic_fetch_add(&f->drive_turns, 1);
		CHECK(tr_task_executor_dispatch(f->executor, 2, &delivered) ==
		      TR_OK);
		if (f->dispatch)
			CHECK(tr_rpc_record_dispatch_pump_on_owner(f->dispatch,
			                                           2) == TR_OK);
	}
	return now + 1000000;
}
static int create_dispatch(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_create_on_owner(
	        f->server.channel, f->executor, f->methods, 4, &f->config,
	        server_terminal, f, &f->server.budget, &f->dispatch);
}
static int start_server(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config cfg = {2, 4};
	CHECK(tr_task_executor_create(&cfg, &f->server.budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	CHECK(create_dispatch(f) == TR_OK);
	CHECK(tr_reactor_timer_register(f->server.reactor, drive, f,
	                                &f->timer) == TR_OK);
	f->timer_registered = true;
	f->driving = true;
	CHECK(tr_reactor_timer_arm(f->timer, (now_ms() + 1) * 1000000) ==
	      TR_OK);
	return TR_OK;
}
static void fixture_init(struct fixture *f, uint64_t window, uint32_t pending)
{
	int a = -1, b = -1;
	*f = (struct fixture){.config = {CAP, pending, 4, BODY * 4},
	                      .batch = 2};
	gate_init(&f->gate);
	atomic_init(&f->use_gate, false);
	atomic_init(&f->reject_method, 0);
	atomic_init(&f->bad_reply, false);
	atomic_init(&f->oversized_reply, false);
	atomic_init(&f->drive_turns, 0);
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
	atomic_store(&blocked_kind, 0);
	atomic_store(&retry_body, 0);
	for (unsigned i = 0; i < 6; ++i)
		atomic_store(&send_attempts[i], 0);
}
struct client_create {
	struct fixture *f;
	struct tr_stream_handle stream;
	unsigned method;
};
static int client_bind(void *arg)
{
	struct client_create *r = arg;
	struct peer *p = &r->f->client;
	struct tr_rpc_record_stream_config config = {
	        TR_RPC_RECORD_REQUEST_DIRECTION,
	        r->f->methods[r->method].descriptor};
	struct tr_rpc_record_stream_callbacks callbacks = {client_received,
	                                                   client_terminal, p};
	return tr_rpc_record_stream_create_on_owner(r->stream, &config,
	                                            &callbacks, &p->budget,
	                                            &p->calls[r->stream.slot]);
}
static unsigned open_call(struct fixture *f, unsigned method)
{
	struct tr_stream_handle stream;
	unsigned count = atomic_load(&f->client.opened) + 1;
	CHECK(tr_stream_open(f->client.channel, TR_LANE_BULK, &stream) ==
	      TR_OK);
	wait_count(&f->client.opened, count);
	wait_count(&f->server.opened, count);
	struct client_create r = {f, stream, method};
	CHECK(tr_reactor_call(f->client.reactor, client_bind, &r) == TR_OK);
	return stream.slot;
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
	send_record(f, slot, method, TR_RPC_RECORD_OPEN, 0);
	for (unsigned i = 0; i < messages; ++i)
		send_record(f, slot, method, TR_RPC_RECORD_MESSAGE, size);
	send_record(f, slot, method, TR_RPC_RECORD_HALF_CLOSE, 0);
}
static int snapshot(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                 &f->stats);
}
static int disable_peer(void *arg)
{
	((struct peer *)arg)->disabled = true;
	return TR_OK;
}
static int disable_driver(void *arg)
{
	struct fixture *f = arg;
	f->driving = false;
	if (f->timer_registered) {
		CHECK(tr_reactor_timer_unregister(f->timer) == TR_OK);
		f->timer_registered = false;
	}
	return TR_OK;
}
static int destroy_dispatch(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_destroy_on_owner(&f->dispatch);
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
static void fixture_destroy(struct fixture *f, bool stopped)
{
	atomic_store(&observed_channel, NULL);
	if (!stopped) {
		CHECK(tr_reactor_call(f->server.reactor, disable_peer,
		                      &f->server) == TR_OK);
		uint64_t until = now_ms() + 15000;
		int ret;
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_dispatch, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		CHECK(tr_reactor_call(f->server.reactor, disable_driver, f) ==
		      TR_OK);
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_executor, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	}
	CHECK(!f->dispatch && !f->executor);
	CHECK(tr_reactor_call(f->client.reactor, disable_peer, &f->client) ==
	      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.calls[i]) == TR_OK);
	struct peer *peers[] = {&f->client, &f->server};
	for (unsigned i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->pool) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
	gate_destroy(&f->gate);
}
static void test_shapes(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	for (unsigned m = 0; m < 4; ++m) {
		unsigned slot = open_call(&f, m);
		unsigned count = m < 2 ? 1 : 2;
		request(&f, slot, m, count, m == 2 ? 0 : 1024);
		wait_count(&f.client.terminals[slot], 1);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
		CHECK(f.server.status[slot] == TR_RPC_STATUS_OK &&
		      f.server.final_accepted[slot]);
		CHECK(atomic_load(&f.client.messages[slot]) ==
		      ((m & 1) ? count * 2 : 1));
		unsigned route = f.server.handles[slot].id.slot;
		CHECK(atomic_load(
		              &f.business[route].seen[TR_RPC_RECORD_OPEN]) ==
		      1);
		CHECK(atomic_load(
		              &f.business[route].seen[TR_RPC_RECORD_MESSAGE]) ==
		      count);
		CHECK(atomic_load(&f.business[route]
		                           .seen[TR_RPC_RECORD_HALF_CLOSE]) ==
		      1);
	}
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 0 && f.stats.terminal_calls == 4);
	fixture_destroy(&f, false);
}
static void test_parallel_and_owned_data(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned a = open_call(&f, 0), b = open_call(&f, 2);
	atomic_store(&f.use_gate, true);
	request(&f, a, 0, 1, BODY);
	gate_wait(&f.gate, false);
	request(&f, b, 2, 0, 0);
	wait_count(&f.client.terminals[b], 1);
	CHECK(!atomic_load(&f.client.terminals[a]));
	CHECK(atomic_load(&f.business[f.server.handles[a].id.slot]
	                           .seen[TR_RPC_RECORD_HALF_CLOSE]) == 0);
	gate_open(&f.gate);
	wait_count(&f.client.terminals[a], 1);
	wait_count(&f.server.terminals[a], 1);
	CHECK(f.client.received_bytes[a] == BODY &&
	      f.client.status[b] == TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static void test_send_retry(unsigned kind)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 0);
	atomic_store(&blocked_kind, kind);
	request(&f, slot, 0, 1, 80);
	wait_count(&send_attempts[kind], 10);
	CHECK(!atomic_load(&f.client.terminals[slot]) &&
	      !atomic_load(&f.server.terminals[slot]));
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.reply_calls == 1 && f.stats.held_events == 1);
	unsigned route = f.server.handles[slot].id.slot;
	CHECK(atomic_load(&f.business[route].seen[TR_RPC_RECORD_HALF_CLOSE]) ==
	      1);
	atomic_store(&blocked_kind, 0);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(atomic_load(&f.client.messages[slot]) == 1 &&
	      f.client.received_bytes[slot] == 80);
	CHECK(atomic_load(&f.client.initial[slot]) == 1);
	CHECK(atomic_load(&f.business[route].seen[TR_RPC_RECORD_HALF_CLOSE]) ==
	      1);
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
static int cancel_first(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_abort_on_owner(f->server.handles[0],
	                                             TR_RPC_STATUS_CANCELLED);
}
static int reset_first(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_dispatch_handle handle = f->server.handles[0];
	struct tr_rpc_dispatch_result before, after;
	struct tr_rpc_record_dispatch_stats stats_before, stats_after;
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &before) == TR_OK);
	CHECK(!before.terminal && !before.final_accepted);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(
	              f->dispatch, &stats_before) == TR_OK);
	CHECK(tr_rpc_record_dispatch_event_on_owner(
	              f->dispatch, f->server.streams[0], TR_STREAM_EVENT_RESET,
	              TR_AGAIN) == TR_ERR_INVALID);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &after) == TR_OK);
	CHECK(after.status == before.status &&
	      after.terminal == before.terminal &&
	      after.final_accepted == before.final_accepted);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                &stats_after) == TR_OK);
	CHECK(stats_after.held_events == stats_before.held_events &&
	      stats_after.terminal_calls == stats_before.terminal_calls);
	CHECK(tr_rpc_record_dispatch_event_on_owner(
	              f->dispatch, f->server.streams[0], TR_STREAM_EVENT_RESET,
	              TR_ERR_CLOSED) == TR_OK);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &after) == TR_OK);
	CHECK(after.terminal && after.status == TR_RPC_STATUS_CANCELLED &&
	      !after.final_accepted);
	CHECK(!atomic_load(&f->server.terminals[0]) &&
	      atomic_load(&f->business[handle.id.slot].active) == 1);
	CHECK(tr_rpc_record_dispatch_retire_on_owner(handle) == TR_ERR_STATE);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, NULL) ==
	      TR_ERR_INVALID);
	++handle.id.generation;
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &before) ==
	      TR_ERR_STALE);
	return TR_OK;
}
static void test_cancel_and_stop(unsigned mode)
{
	bool stopped = mode != 0;
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 3);
	CHECK(slot == 0);
	atomic_store(&f.use_gate, true);
	request(&f, slot, 3, 2, 80);
	gate_wait(&f.gate, false);
	if (mode == 0) {
		struct tr_rpc_dispatch_result result = {0};
		CHECK(tr_rpc_record_dispatch_result_on_owner(
		              f.server.handles[slot], &result) == TR_ERR_STATE);
		CHECK(tr_reactor_call(f.server.reactor, reset_first, &f) ==
		      TR_OK);
	} else if (mode != 2)
		CHECK(tr_reactor_call(f.server.reactor, cancel_first, &f) ==
		      TR_OK);
	CHECK(!atomic_load(&f.server.terminals[slot]));
	if (!stopped) {
		CHECK(tr_reactor_call(f.server.reactor, destroy_dispatch, &f) ==
		      TR_AGAIN);
		CHECK(f.dispatch);
		gate_open(&f.gate);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(f.server.delivery[slot] == TR_RPC_RECORD_ON_OWNER);
		CHECK(f.server.status[slot] == TR_RPC_STATUS_CANCELLED);
		CHECK(!f.server.final_accepted[slot]);
		fixture_destroy(&f, false);
		return;
	}
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_ERR_STATE);
	CHECK(tr_reactor_call(f.server.reactor, disable_peer, &f.server) ==
	      TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, disable_driver, &f) == TR_OK);
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminals[slot]));
	CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_adopt_cleanup(f.executor) == TR_OK);
	gate_open(&f.gate);
	uint64_t until = now_ms() + 15000;
	int ret;
	while ((ret = tr_task_executor_join(f.executor)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	size_t delivered;
	CHECK(tr_task_executor_dispatch(f.executor, 8, &delivered) == TR_OK);
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_OK);
	CHECK(atomic_load(&f.server.terminals[slot]) == 1);
	CHECK(f.server.delivery[slot] == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	fixture_destroy(&f, true);
}
static void test_business_error(unsigned mode)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 0);
	if (mode == 0)
		atomic_store(&f.reject_method, 1);
	else if (mode == 1)
		f.bad_reply = true;
	else if (mode == 2)
		f.oversized_reply = true;
	else
		atomic_store(&fail_worker_malloc, true);
	if (mode == 0)
		send_record(&f, slot, 0, TR_RPC_RECORD_OPEN, 0);
	else
		request(&f, slot, 0, 1, 40);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	uint32_t expected = mode == 0   ? TR_RPC_STATUS_PERMISSION_DENIED
	                    : mode == 1 ? TR_RPC_STATUS_INTERNAL
	                                : TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	CHECK(f.client.status[slot] == expected &&
	      f.server.status[slot] == expected);
	CHECK(f.server.final_accepted[slot]);
	CHECK(!atomic_load(&f.client.messages[slot]));
	fixture_destroy(&f, false);
}
static int pause_driver(void *arg)
{
	((struct fixture *)arg)->driving = false;
	return TR_OK;
}
struct admission_setup {
	struct fixture *fixture;
	uint64_t due;
};
static int prepare_admission(void *arg)
{
	struct admission_setup *setup = arg;
	setup->fixture->driving = false;
	setup->fixture->open_due = setup->due;
	return TR_OK;
}
struct admission_step {
	struct fixture *fixture;
	unsigned slot;
	const uint8_t *data;
	size_t size;
	struct admission_clock clock;
	struct tr_rpc_dispatch_result result;
	struct tr_rpc_record_dispatch_stats stats;
	uint64_t before, after;
	int status;
	bool pump;
};
static int run_admission_step(void *arg)
{
	struct admission_step *step = arg;
	struct fixture *f = step->fixture;
	CHECK(!admission_clock);
	step->clock.reads = 0;
	step->before = atomic_load(&f->server.budget.current_bytes);
	/* 只包住实际派发入口；Reactor 定时器和真实业务线程仍使用本机时钟。
	 * 本组暂停自动派发，因此固定 due 不会被另一个 owner turn 抢先观察。 */
	admission_clock = &step->clock;
	step->status =
	        step->pump
	                ? tr_rpc_record_dispatch_pump_on_owner(f->dispatch, CAP)
	                : tr_rpc_record_dispatch_receive_on_owner(
	                          f->dispatch, f->server.streams[step->slot],
	                          step->data, step->size);
	admission_clock = NULL;
	step->after = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_rpc_record_dispatch_result_on_owner(
	              f->server.handles[step->slot], &step->result) == TR_OK);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                &step->stats) == TR_OK);
	return TR_OK;
}
static void admission_step(struct admission_step *step)
{
	CHECK(tr_reactor_call(step->fixture->server.reactor, run_admission_step,
	                      step) == TR_OK);
}
static int retire_admission(void *arg)
{
	struct admission_step *step = arg;
	struct peer *peer = &step->fixture->server;
	CHECK(tr_rpc_record_dispatch_retire_on_owner(
	              peer->handles[step->slot]) == TR_OK);
	peer->handles[step->slot] = (struct tr_rpc_dispatch_handle){0};
	step->after = atomic_load(&peer->budget.current_bytes);
	return TR_OK;
}
static unsigned admission_begin(struct fixture *f, uint64_t due)
{
	fixture_init(f, 1024 * 1024, 8);
	struct admission_setup setup = {f, due};
	CHECK(tr_reactor_call(f->server.reactor, prepare_admission, &setup) ==
	      TR_OK);
	return open_call(f, 0);
}
static void admission_open(uint8_t out[TR_RPC_RECORD_HEADER_SIZE + 7],
                           uint64_t call_timeout)
{
	const struct tr_rpc_record_header header = {.kind = TR_RPC_RECORD_OPEN,
	                                            .service_id = 7,
	                                            .method_id = 9,
	                                            .metadata_size = 7,
	                                            .remaining_timeout_ns =
	                                                    call_timeout};
	const struct tr_rpc_record_limits limits = {BODY, BODY, 7,
	                                            TR_RPC_RECORD_ACCEPT_NONE};
	const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
	CHECK(tr_rpc_record_encode_header(out, TR_RPC_RECORD_HEADER_SIZE,
	                                  &header, &limits) == TR_OK);
	memcpy(out + TR_RPC_RECORD_HEADER_SIZE, metadata, sizeof(metadata));
}
static void admission_terminal(struct admission_step *step,
                               enum tr_rpc_status expected, bool admission)
{
	CHECK(step->result.terminal && step->result.status == expected &&
	      !step->result.final_accepted &&
	      step->result.admission_failed == admission);
	CHECK(step->stats.held_events == 0 && !step->stats.submitted_calls &&
	      !step->stats.reply_calls);
	CHECK(atomic_load(&step->fixture->server.terminals[step->slot]) == 1);
	for (unsigned slot = 0; slot < CAP; ++slot)
		for (unsigned kind = 1; kind <= 5; ++kind)
			CHECK(!atomic_load(
			        &step->fixture->business[slot].seen[kind]));
}
static void test_open_admission_partial_and_timer(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	/* 未验证固定头即便声称无限大的 CALL 预算，也不能解除本机准入期限。 */
	admission_open(open, UINT64_MAX);
	for (unsigned partial = 0; partial < 2; ++partial) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		struct admission_step step = {.fixture = &f,
		                              .slot = slot,
		                              .clock = {.now = due - 100},
		                              .pump = !partial,
		                              .data = open,
		                              .size = 7};
		admission_step(&step);
		uint64_t baseline = step.before;
		uint64_t partial_bytes = 0;
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == 1 && !step.stats.held_events);
		if (partial) {
			step.data = open + 7;
			step.size = sizeof(open) - 8;
			step.clock.now = due - 1;
			admission_step(&step);
			CHECK(step.status == TR_OK && !step.result.terminal &&
			      step.clock.reads == 1 && step.after > baseline &&
			      !step.stats.held_events);
			partial_bytes = step.after;
			step.data = open + sizeof(open) - 1;
			step.size = 1;
		}
		step.clock.now = due;
		admission_step(&step);
		CHECK(step.status == (partial ? TR_ERR_STATE : TR_OK) &&
		      step.clock.reads == 1);
		CHECK(partial ? partial_bytes - step.after == sizeof(open)
		              : step.after == baseline);
		admission_terminal(&step, TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		/* 已知终局不重读故障时钟，也不在重复输入时重选 INTERNAL。 */
		step.clock.fail_at = 1;
		admission_step(&step);
		CHECK(step.clock.reads == 0);
		admission_terminal(&step, TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		CHECK(tr_reactor_call(f.server.reactor, retire_admission,
		                      &step) == TR_OK);
		CHECK(step.after == baseline);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_validation_boundary(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	admission_open(open, 0);
	for (unsigned fault = 0; fault < 2; ++fault) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		struct admission_step step = {
		        .fixture = &f,
		        .slot = slot,
		        .data = open,
		        .size = sizeof(open),
		        .clock = {.now = due - 1,
		                  .advanced = due,
		                  .advance_at = 2,
		                  .fail_at = fault ? 2 : 0}};
		admission_step(&step);
		/* 输入已被真正解码吸收，所以返回 OK；内部回调仍须拒绝准入。
		 * 仅保留外层 receive 检查会使这个 OPEN 进入业务队列。 */
		CHECK(step.status == TR_OK && step.clock.reads == 2 &&
		      step.after > step.before);
		admission_terminal(&step,
		                   fault ? TR_RPC_STATUS_INTERNAL
		                         : TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		CHECK(tr_reactor_call(f.server.reactor, retire_admission,
		                      &step) == TR_OK);
		CHECK(step.after == step.before);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_clock_failure(void)
{
	uint8_t partial[] = {'T', 'R', 'P', 'C', 2, 0, 1};
	for (unsigned fault = 0; fault < 6; ++fault) {
		struct fixture f;
		unsigned slot = admission_begin(&f, UINT64_C(10000000000));
		struct admission_step step = {
		        .fixture = &f,
		        .slot = slot,
		        .data = partial,
		        .size = sizeof(partial),
		        .pump = fault == 5,
		        .clock = {.now = 1,
		                  .fail_at = (fault == 0 || fault == 5) ? 1 : 0,
		                  .invalid = fault < 5 ? fault : 0}};
		admission_step(&step);
		CHECK(step.status == (step.pump ? TR_OK : TR_ERR_STATE) &&
		      step.clock.reads == 1 && step.after == step.before);
		admission_terminal(&step, TR_RPC_STATUS_INTERNAL, true);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_handoff(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	const uint8_t message[] = {'T', 'R', 'P', 'C', 2, 0, 3};
	for (unsigned finite = 0; finite < 2; ++finite) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		admission_open(open, finite ? 1000 : 0);
		struct admission_step step = {.fixture = &f,
		                              .slot = slot,
		                              .data = open,
		                              .size = sizeof(open) - 1,
		                              .clock = {.now = due - 800}};
		admission_step(&step);
		CHECK(step.status == TR_OK && step.clock.reads == 1 &&
		      !step.stats.held_events);
		step.data = open + sizeof(open) - 1;
		step.size = 1;
		step.clock.now = due - 100;
		admission_step(&step);
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == (finite ? 4U : 2U) &&
		      step.stats.held_events == 1);
		step.data = message;
		step.size = sizeof(message);
		step.clock.now = due + 899;
		step.clock.fail_at = finite ? 0 : 1;
		admission_step(&step);
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == (finite ? 1U : 0U));
		if (finite) {
			step.clock.now = due + 900;
			admission_step(&step);
			CHECK(step.status == TR_ERR_STATE &&
			      step.clock.reads == 1);
			admission_terminal(
			        &step, TR_RPC_STATUS_DEADLINE_EXCEEDED, false);
		}
		fixture_destroy(&f, false);
	}
	/* 保留低层旧入口：0 不启用 OPEN 期限，完整无限 CALL 不读取时钟。 */
	struct fixture f;
	unsigned slot = admission_begin(&f, 0);
	admission_open(open, 0);
	struct admission_step step = {.fixture = &f,
	                              .slot = slot,
	                              .data = open,
	                              .size = sizeof(open),
	                              .clock = {.now = due, .fail_at = 1}};
	admission_step(&step);
	CHECK(step.status == TR_OK && !step.result.terminal &&
	      !step.clock.reads && step.stats.held_events == 1);
	fixture_destroy(&f, false);
}
static void test_pending_limit(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 2);
	CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) == TR_OK);
	unsigned slot = open_call(&f, 3);
	request(&f, slot, 3, 2, 16);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.server.status[slot] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(!f.server.final_accepted[slot]);
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 0 && f.stats.submitted_calls == 0);
	for (unsigned i = 0; i < 6; ++i)
		CHECK(!atomic_load(&f.business[0].seen[i]));
	fixture_destroy(&f, false);
}
static int constructor_probe(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_dispatch *d = NULL;
	uint64_t before = atomic_load(&f->server.budget.current_bytes);
	unsigned success = 0;
	for (unsigned i = 1; i < 12; ++i) {
		fail_at = i;
		allocations = 0;
		int ret = tr_rpc_record_dispatch_create_on_owner(
		        f->server.channel, f->executor, f->methods, 4,
		        &f->config, server_terminal, f, &f->server.budget, &d);
		fail_at = 0;
		if (ret == TR_OK) {
			CHECK(d);
			CHECK(tr_rpc_record_dispatch_destroy_on_owner(&d) ==
			      TR_OK);
			CHECK(atomic_load(&f->server.budget.current_bytes) ==
			      before);
			success = i;
			break;
		}
		CHECK(ret == TR_ERR_NOMEM && !d);
		CHECK(atomic_load(&f->server.budget.current_bytes) == before);
	}
	CHECK(success == 8);
	return TR_OK;
}
struct wrong_executor_request {
	struct fixture *fixture;
	struct tr_task_executor *foreign;
};
static int wrong_executor_owner(void *arg)
{
	struct wrong_executor_request *request = arg;
	struct fixture *f = request->fixture;
	struct tr_rpc_record_dispatch *d = NULL;
	uint64_t before = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_task_executor_is_current_owner(f->executor));
	CHECK(!tr_task_executor_is_current_owner(request->foreign));
	CHECK(tr_rpc_record_dispatch_create_on_owner(
	              f->server.channel, request->foreign, f->methods, 4,
	              &f->config, server_terminal, f, &f->server.budget,
	              &d) == TR_ERR_STATE);
	CHECK(!d && atomic_load(&f->server.budget.current_bytes) == before);
	return TR_OK;
}
static void test_constructor(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	CHECK(tr_reactor_call(f.server.reactor, constructor_probe, &f) ==
	      TR_OK);
	struct tr_memory_budget budget;
	struct tr_task_executor *foreign = NULL;
	struct tr_task_executor_config config = {1, 1};
	tr_memory_budget_init(&budget, 65536);
	CHECK(tr_task_executor_create(&config, &budget, &foreign) == TR_OK);
	CHECK(tr_task_executor_is_current_owner(foreign));
	struct wrong_executor_request request = {&f, foreign};
	CHECK(tr_reactor_call(f.server.reactor, wrong_executor_owner,
	                      &request) == TR_OK);
	CHECK(tr_task_executor_destroy(&foreign) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
	fixture_destroy(&f, false);
}

struct shared_tasks {
	struct fixture *fixture;
	_Atomic unsigned done;
};
static int shared_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct shared_tasks *jobs = arg;
	(void)cancel;
	gate_wait(&jobs->fixture->gate, true);
	return TR_OK;
}
static void shared_done(void *arg, struct tr_task_handle handle,
                        struct tr_task_result result)
{
	struct shared_tasks *jobs = arg;
	(void)handle;
	CHECK(!result.cancelled && result.work_status == TR_OK);
	atomic_fetch_add(&jobs->done, 1);
}
static int occupy_executor(void *arg)
{
	struct shared_tasks *jobs = arg;
	struct tr_task_spec spec = {shared_work, shared_done, jobs};
	for (unsigned i = 0; i < 4; ++i) {
		struct tr_task_handle handle;
		CHECK(tr_task_executor_submit(jobs->fixture->executor, &spec,
		                              &handle) == TR_OK);
	}
	return TR_OK;
}
static void test_shared_executor_backpressure(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	struct shared_tasks jobs = {.fixture = &f};
	atomic_init(&jobs.done, 0);
	CHECK(tr_reactor_call(f.server.reactor, occupy_executor, &jobs) ==
	      TR_OK);
	gate_wait(&f.gate, false);
	unsigned slot = open_call(&f, 0);
	request(&f, slot, 0, 1, 64);
	unsigned turns = atomic_load(&f.drive_turns);
	wait_count(&f.drive_turns, turns + 10);
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 3 && f.stats.submitted_calls == 0);
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]));
	gate_open(&f.gate);
	wait_count(&jobs.done, 4);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
	for (unsigned kind = TR_RPC_RECORD_OPEN;
	     kind <= TR_RPC_RECORD_HALF_CLOSE; ++kind)
		if (kind != TR_RPC_RECORD_INITIAL_METADATA)
			CHECK(atomic_load(&f.business[0].seen[kind]) == 1);
	fixture_destroy(&f, false);
}

int main(void)
{
	test_open_admission_partial_and_timer();
	test_open_admission_validation_boundary();
	test_open_admission_clock_failure();
	test_open_admission_handoff();
	test_shared_executor_backpressure();
	test_constructor();
	test_shapes();
	test_parallel_and_owned_data();
	test_send_retry(TR_RPC_RECORD_INITIAL_METADATA);
	test_send_retry(TR_RPC_RECORD_MESSAGE);
	test_send_retry(TR_RPC_RECORD_FINAL_STATUS);
	test_real_window();
	for (unsigned i = 0; i < 4; ++i)
		test_business_error(i);
	test_pending_limit();
	test_cancel_and_stop(0);
	test_cancel_and_stop(1);
	test_cancel_and_stop(2);
	puts("rpc_record_dispatch: ok");
	return 0;
}
