#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_endpoint.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

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

static _Atomic(struct tr_channel *) server_channel;
static _Atomic unsigned reject_final, reject_close, final_attempts,
        close_attempts;
static _Atomic bool fail_register, fail_arm, fail_handler, fail_detach;
static _Atomic bool block_worker, hold_final;
static struct tr_buffer *held_final;
static _Thread_local bool track_create;
static _Thread_local unsigned calloc_count;
static _Atomic unsigned fail_calloc_at;
void *__real_calloc(size_t, size_t);
void *__wrap_calloc(size_t n, size_t size)
{
	if (track_create && ++calloc_count == atomic_load(&fail_calloc_at))
		return NULL;
	return __real_calloc(n, size);
}
static pthread_mutex_t gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gate_cond = PTHREAD_COND_INITIALIZER;
static bool gate_entered, gate_open;
static struct tr_stream_handle observed_stream;

int __real_tr_stream_send(struct tr_stream_handle, struct tr_buffer *);
int __real_tr_stream_close(struct tr_stream_handle);
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle, uint64_t);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
int __real_tr_rpc_record_dispatch_admit_on_owner(
        struct tr_rpc_record_dispatch *, struct tr_stream_handle,
        struct tr_rpc_dispatch_handle *);

int __wrap_tr_stream_send(struct tr_stream_handle s, struct tr_buffer *b)
{
	if (s.channel == atomic_load(&server_channel)) {
		atomic_fetch_add(&final_attempts, 1);
		if (atomic_exchange(&hold_final, false)) {
			CHECK(!held_final);
			held_final = b;
			return TR_OK;
		}
		if (atomic_load(&reject_final)) {
			atomic_fetch_sub(&reject_final, 1);
			return TR_AGAIN;
		}
	}
	return __real_tr_stream_send(s, b);
}
int __wrap_tr_stream_close(struct tr_stream_handle s)
{
	if (s.channel == atomic_load(&server_channel)) {
		atomic_fetch_add(&close_attempts, 1);
		if (atomic_load(&reject_close)) {
			atomic_fetch_sub(&reject_close, 1);
			return TR_AGAIN;
		}
	}
	return __real_tr_stream_close(s);
}
int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	if (atomic_exchange(&fail_register, false))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_register(r, cb, arg, out);
}
int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle h, uint64_t n)
{
	if (atomic_exchange(&fail_arm, false))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_arm(h, n);
}
int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle h)
{
	if (atomic_exchange(&fail_detach, false))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_unregister(h);
}
int __wrap_tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb state, void *arg)
{
	if (data && atomic_exchange(&fail_handler, false))
		return TR_ERR_SYS;
	return __real_tr_channel_set_handler(c, data, event, state, arg);
}
int __wrap_tr_rpc_record_dispatch_admit_on_owner(
        struct tr_rpc_record_dispatch *d, struct tr_stream_handle s,
        struct tr_rpc_dispatch_handle *out)
{
	int ret = __real_tr_rpc_record_dispatch_admit_on_owner(d, s, out);
	if (ret == TR_OK)
		observed_stream = s;
	return ret;
}

struct fixture;
struct peer {
	struct fixture *f;
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	_Atomic unsigned finals[CAP], closes, messages[CAP];
	enum tr_rpc_status status[CAP];
	bool stopped;
};
struct fixture {
	struct peer client, server;
	struct tr_task_executor *executor;
	struct tr_rpc_record_endpoint *ep;
	struct tr_rpc_record_endpoint_config cfg;
	struct tr_rpc_record_endpoint_result results[CAP];
	_Atomic unsigned terminals, work_events, running;
	uint64_t reserved;
};
static uint64_t now_ms(void)
{
	struct timespec t;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
	return (uint64_t)t.tv_sec * 1000 + (uint64_t)t.tv_nsec / 1000000;
}
static void wait_count(_Atomic unsigned *n, unsigned wanted)
{
	uint64_t until = now_ms() + 15000;
	while (atomic_load(n) < wanted) {
		CHECK(now_ms() < until);
		sched_yield();
	}
}
static struct tr_rpc_method_desc descriptor(uint32_t method)
{
	return (struct tr_rpc_method_desc){.service_id = 1,
	                                   .method_id = method,
	                                   .request_cardinality = TR_RPC_ONE,
	                                   .response_cardinality = TR_RPC_ONE,
	                                   .request_codec_id = 1,
	                                   .response_codec_id = 1,
	                                   .max_request_bytes = BODY,
	                                   .max_response_bytes = BODY,
	                                   .lane = TR_RPC_LANE_CONTROL};
}
static enum tr_rpc_status work(struct tr_rpc_dispatch_id id,
                               const struct tr_rpc_record_view *view,
                               const struct tr_task_cancel *cancel,
                               struct tr_rpc_record_reply *reply, void *arg)
{
	struct fixture *f = arg;
	(void)id;
	(void)cancel;
	CHECK(!tr_reactor_in_owner_context());
	atomic_fetch_add(&f->running, 1);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		if (atomic_exchange(&block_worker, false)) {
			struct timespec end;
			CHECK(clock_gettime(CLOCK_REALTIME, &end) == 0);
			end.tv_sec += 15;
			CHECK(pthread_mutex_lock(&gate_lock) == 0);
			gate_entered = true;
			CHECK(pthread_cond_broadcast(&gate_cond) == 0);
			while (!gate_open)
				CHECK(pthread_cond_timedwait(&gate_cond,
				                             &gate_lock,
				                             &end) == 0);
			CHECK(pthread_mutex_unlock(&gate_lock) == 0);
		}
		CHECK(tr_rpc_record_reply_append(reply, view->body,
		                                 view->header.decoded_size) ==
		      TR_OK);
	}
	atomic_fetch_add(&f->work_events, 1);
	atomic_fetch_sub(&f->running, 1);
	return TR_RPC_STATUS_OK;
}
static void server_terminal(struct tr_stream_handle stream,
                            struct tr_rpc_record_endpoint_result result,
                            void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_endpoint *same = f->ep;
	CHECK(stream.channel == f->server.channel && stream.slot < CAP);
	CHECK(!atomic_load(&f->running));
	CHECK((result.delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (result.delivery == TR_RPC_RECORD_ON_OWNER)
		CHECK(tr_rpc_record_endpoint_close_on_owner(&same) ==
		      TR_ERR_STATE);
	else
		CHECK(tr_rpc_record_endpoint_reap_stopped(&same) ==
		      TR_ERR_STATE);
	CHECK(same == f->ep);
	f->results[stream.slot] = result;
	atomic_fetch_add(&f->terminals, 1);
}
static unsigned client_index(struct peer *p, struct tr_rpc_record_stream *call)
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
	unsigned index = client_index(p, call);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		for (uint32_t i = 0; i < view->header.decoded_size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 11));
		atomic_fetch_add(&p->messages[index], 1);
	}
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned index = client_index(p, call);
	(void)delivery;
	p->status[index] = status;
	CHECK(atomic_fetch_add(&p->finals[index], 1) == 0);
}
static enum tr_stream_data_disposition client_data(struct tr_stream_handle s,
                                                   uint64_t id,
                                                   struct tr_buffer *b,
                                                   void *arg)
{
	struct peer *p = arg;
	(void)id;
	CHECK(s.slot < CAP && p->calls[s.slot]);
	CHECK(tr_rpc_record_stream_receive_on_owner(p->calls[s.slot], s,
	                                            b->data, b->len) == TR_OK);
	return TR_STREAM_DATA_RELEASE;
}
static void client_event(struct tr_stream_handle s, enum tr_stream_event event,
                         int status, void *arg)
{
	struct peer *p = arg;
	(void)status;
	if (event == TR_STREAM_EVENT_OPENED)
		p->streams[s.slot] = s;
	else if (p->calls[s.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[s.slot], s,
		                                          event) == TR_OK);
		if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
			int ret = tr_stream_close(s);
			CHECK(ret == TR_OK || ret == TR_ERR_CLOSED ||
			      ret == TR_ERR_STALE);
			atomic_fetch_add(&p->closes, 1);
		}
	}
}
static void tcp_pair(int *a, int *b)
{
	int listen_fd, ret;
	uint16_t port;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listen_fd, &port) ==
	      TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, a);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	uint64_t until = now_ms() + 15000;
	while (tr_tcp_accept(listen_fd, b) == TR_AGAIN) {
		struct pollfd fd = {listen_fd, POLLIN, 0};
		CHECK(now_ms() < until && poll(&fd, 1, 100) >= 0);
	}
	CHECK(*b >= 0);
	if (ret == TR_IN_PROGRESS) {
		struct pollfd fd = {*a, POLLOUT, 0};
		CHECK(poll(&fd, 1, 1000) == 1);
		CHECK(tr_tcp_finish_connect(*a) == TR_OK);
	}
	CHECK(close(listen_fd) == 0);
}
static void peer_init(struct peer *p, struct fixture *f, int fd, bool server)
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
	                               .initial_window_bytes = 65536,
	                               .window_update_threshold_bytes = 1,
	                               .max_message_bytes = BODY + 1024};
	struct tr_conn_handle conn;
	*p = (struct peer){.f = f};
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->finals[i], 0);
		atomic_init(&p->messages[i], 0);
	}
	atomic_init(&p->closes, 0);
	tr_memory_budget_init(&p->budget, 16 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	CHECK(tr_channel_create(&cc, conn, conn, server ? NULL : client_data,
	                        server ? NULL : client_event, NULL,
	                        server ? NULL : p, &p->channel) == TR_OK);
}
static int start_endpoint(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config config = {2, 8};
	struct tr_rpc_dispatch_method method = {descriptor(1), work, f};
	int ret;
	CHECK(tr_task_executor_create(&config, &f->server.budget,
	                              &f->executor) == TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	track_create = true;
	calloc_count = 0;
	ret = tr_rpc_record_endpoint_create_on_owner(
	        f->server.channel, f->executor, &method, 1, &f->cfg,
	        server_terminal, f, &f->server.budget, &f->ep);
	track_create = false;
	return ret == TR_OK ? tr_rpc_record_endpoint_start_on_owner(f->ep)
	                    : ret;
}
static void setup(struct fixture *f)
{
	int a = -1, b = -1;
	*f = (struct fixture){
	        .cfg = {{CAP, 8, 2, BODY}, 2, 1000000, 200000000}};
	atomic_init(&f->terminals, 0);
	atomic_init(&f->work_events, 0);
	atomic_init(&f->running, 0);
	atomic_store(&reject_final, 0);
	atomic_store(&reject_close, 0);
	atomic_store(&final_attempts, 0);
	atomic_store(&close_attempts, 0);
	tcp_pair(&a, &b);
	peer_init(&f->server, f, b, true);
	peer_init(&f->client, f, a, false);
	atomic_store(&server_channel, f->server.channel);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_CONTROL, 5000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_CONTROL, 5000) ==
	      TR_OK);
}
static int close_endpoint(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_endpoint_close_on_owner(&f->ep);
}
static int shutdown_executor(void *arg)
{
	struct fixture *f = arg;
	int ret;
	CHECK(tr_task_executor_stop(f->executor, TR_TASK_DRAIN) == TR_OK);
	ret = tr_task_executor_join(f->executor);
	if (ret == TR_OK)
		ret = tr_task_executor_destroy(&f->executor);
	return ret;
}
static int destroy_calls(void *arg)
{
	struct peer *p = arg;
	CHECK(tr_channel_set_handler(p->channel, NULL, NULL, NULL, NULL) ==
	      TR_OK);
	for (unsigned i = 0; i < CAP; ++i) {
		if (!p->calls[i])
			continue;
		(void)tr_rpc_record_stream_abort_on_owner(
		        p->calls[i], TR_RPC_STATUS_CANCELLED);
		CHECK(tr_rpc_record_stream_destroy_on_owner(&p->calls[i]) ==
		      TR_OK);
	}
	return TR_OK;
}
static void teardown(struct fixture *f)
{
	uint64_t until = now_ms() + 15000;
	int ret;
	if (f->ep) {
		while ((ret = tr_reactor_call(f->server.reactor, close_endpoint,
		                              f)) == TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
	}
	if (f->executor) {
		while ((ret = tr_reactor_call(f->server.reactor,
		                              shutdown_executor, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
	}
	CHECK(tr_reactor_call(f->client.reactor, destroy_calls, &f->client) ==
	      TR_OK);
	CHECK(tr_channel_destroy(f->client.channel) == TR_OK);
	CHECK(tr_channel_destroy(f->server.channel) == TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->client.reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->server.reactor) == TR_OK);
	CHECK(tr_buffer_pool_destroy(&f->client.pool) == TR_OK);
	CHECK(tr_buffer_pool_destroy(&f->server.pool) == TR_OK);
	CHECK(atomic_load(&f->client.budget.current_bytes) == 0);
	CHECK(atomic_load(&f->server.budget.current_bytes) == 0);
	atomic_store(&server_channel, NULL);
}
struct request {
	struct fixture *f;
	uint32_t method;
	unsigned slot;
	bool full, bad;
};
struct bytes {
	struct tr_buffer buffer;
	uint8_t data[];
};
static void release_bytes(struct tr_buffer *b)
{
	free(b);
}
static struct tr_buffer *body(size_t size)
{
	struct bytes *b = malloc(sizeof(*b) + size);
	CHECK(b);
	b->buffer = (struct tr_buffer){.data = b->data,
	                               .len = (uint32_t)size,
	                               .capacity = (uint32_t)size,
	                               .release_cb = release_bytes};
	for (size_t i = 0; i < size; ++i)
		b->data[i] = (uint8_t)(i * 11);
	return &b->buffer;
}
static int begin(void *arg)
{
	struct request *r = arg;
	struct peer *p = &r->f->client;
	struct tr_stream_handle s;
	struct tr_rpc_record_stream_config cfg = {
	        TR_RPC_RECORD_REQUEST_DIRECTION, descriptor(r->method)};
	struct tr_rpc_record_stream_callbacks cb = {client_received,
	                                            client_terminal, p};
	struct tr_rpc_record_header h = {.kind = TR_RPC_RECORD_OPEN,
	                                 .service_id = 1,
	                                 .method_id = r->method};
	CHECK(tr_stream_open(p->channel, TR_LANE_CONTROL, &s) == TR_OK);
	r->slot = s.slot;
	CHECK(tr_rpc_record_stream_create_on_owner(s, &cfg, &cb, &p->budget,
	                                           &p->calls[s.slot]) == TR_OK);
	CHECK(tr_rpc_record_stream_send_on_owner(p->calls[s.slot], &h, NULL,
	                                         NULL) == TR_OK);
	if (r->bad) {
		const struct tr_rpc_record_limits limits = {
		        BODY, BODY, 0, TR_RPC_RECORD_ACCEPT_NONE};
		struct tr_buffer *b = body(TR_RPC_RECORD_HEADER_SIZE);
		h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_MESSAGE,
		                                  .codec_id = 99};
		CHECK(tr_rpc_record_encode_header(b->data, b->len, &h,
		                                  &limits) == TR_OK);
		CHECK(tr_stream_send(s, b) == TR_OK);
	}
	if (r->full) {
		struct tr_buffer *b = body(BODY);
		h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_MESSAGE,
		                                  .codec_id = 1,
		                                  .encoded_size = BODY,
		                                  .decoded_size = BODY};
		CHECK(tr_rpc_record_stream_send_on_owner(p->calls[s.slot], &h,
		                                         NULL, &b) == TR_OK);
		CHECK(!b);
		h = (struct tr_rpc_record_header){
		        .kind = TR_RPC_RECORD_HALF_CLOSE};
		CHECK(tr_rpc_record_stream_send_on_owner(p->calls[s.slot], &h,
		                                         NULL, NULL) == TR_OK);
	}
	return TR_OK;
}
static void test_exchange(unsigned mode)
{
	struct fixture f;
	struct request r;
	setup(&f);
	if (mode == 4)
		f.cfg.final_timeout_ns = 10000000;
	CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) == TR_OK);
	if (mode == 3) {
		f.reserved = f.server.budget.limit_bytes -
		             atomic_load(&f.server.budget.current_bytes);
		CHECK(tr_memory_budget_reserve(&f.server.budget, f.reserved) ==
		      TR_OK);
	}
	if (mode == 1) {
		atomic_store(&reject_final, 10);
		atomic_store(&reject_close, 10);
	}
	if (mode == 4)
		atomic_store(&reject_final, 100000);
	r = (struct request){.f = &f,
	                     .method = (mode == 1 || mode == 4) ? 99 : 1,
	                     .full = mode == 0,
	                     .bad = mode == 2};
	CHECK(tr_reactor_call(f.client.reactor, begin, &r) == TR_OK);
	wait_count(&f.client.finals[r.slot], 1);
	wait_count(&f.client.closes, 1);
	wait_count(&f.terminals, 1);
	if (mode == 0) {
		CHECK(f.client.status[r.slot] == TR_RPC_STATUS_OK);
		CHECK(atomic_load(&f.client.messages[r.slot]) == 1);
		CHECK(atomic_load(&f.work_events) == 3);
	} else if (mode == 4) {
		CHECK(f.client.status[r.slot] != TR_RPC_STATUS_OK);
		CHECK(!f.results[0].final_accepted &&
		      f.results[0].close_accepted);
		CHECK(f.results[0].transport_error == TR_AGAIN);
		CHECK(atomic_load(&final_attempts) < 1000);
	} else {
		CHECK(f.client.status[r.slot] ==
		      (mode == 1   ? TR_RPC_STATUS_UNIMPLEMENTED
		       : mode == 2 ? TR_RPC_STATUS_INVALID_ARGUMENT
		                   : TR_RPC_STATUS_RESOURCE_EXHAUSTED));
		CHECK(f.results[0].final_accepted &&
		      f.results[0].close_accepted);
	}
	if (mode == 1) {
		CHECK(atomic_load(&final_attempts) == 11);
		CHECK(atomic_load(&close_attempts) == 11);
		CHECK(!atomic_load(&f.work_events));
	}
	if (f.reserved)
		CHECK(tr_memory_budget_release(&f.server.budget, f.reserved) ==
		      TR_OK);
	atomic_store(&reject_final, 0);
	teardown(&f);
}
static void test_attach_failures(void)
{
	for (unsigned phase = 0; phase < 3; ++phase) {
		struct fixture f;
		setup(&f);
		if (phase == 0)
			atomic_store(&fail_register, true);
		if (phase == 1)
			atomic_store(&fail_arm, true);
		if (phase == 2)
			atomic_store(&fail_handler, true);
		CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) ==
		      TR_ERR_SYS);
		CHECK(f.ep);
		if (phase) {
			atomic_store(&fail_detach, true);
			CHECK(tr_reactor_call(f.server.reactor, close_endpoint,
			                      &f) == TR_ERR_SYS);
			CHECK(f.ep);
		}
		teardown(&f);
	}
}
static int cancel_call(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_endpoint_abort_on_owner(f->ep, observed_stream,
	                                             TR_RPC_STATUS_CANCELLED);
}
static void test_cancel(void)
{
	struct fixture f;
	struct request r;
	struct timespec end;
	setup(&f);
	CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) == TR_OK);
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	gate_entered = gate_open = false;
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	atomic_store(&block_worker, true);
	r = (struct request){.f = &f, .method = 1, .full = true};
	CHECK(tr_reactor_call(f.client.reactor, begin, &r) == TR_OK);
	CHECK(clock_gettime(CLOCK_REALTIME, &end) == 0);
	end.tv_sec += 15;
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	while (!gate_entered)
		CHECK(pthread_cond_timedwait(&gate_cond, &gate_lock, &end) ==
		      0);
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	CHECK(tr_reactor_call(f.server.reactor, cancel_call, &f) == TR_OK);
	CHECK(!atomic_load(&f.terminals));
	CHECK(tr_reactor_call(f.server.reactor, close_endpoint, &f) ==
	      TR_AGAIN);
	CHECK(f.ep);
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	gate_open = true;
	CHECK(pthread_cond_broadcast(&gate_cond) == 0);
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	wait_count(&f.client.finals[r.slot], 1);
	wait_count(&f.terminals, 1);
	CHECK(f.client.status[r.slot] == TR_RPC_STATUS_CANCELLED);
	CHECK(!atomic_load(&f.client.messages[r.slot]));
	teardown(&f);
}
static void test_stopped(void)
{
	struct fixture f;
	struct request r;
	struct timespec end;
	uint64_t until;
	int ret;
	size_t count;
	setup(&f);
	CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_ERR_STATE);
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	gate_entered = gate_open = false;
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	atomic_store(&block_worker, true);
	r = (struct request){.f = &f, .method = 1, .full = true};
	CHECK(tr_reactor_call(f.client.reactor, begin, &r) == TR_OK);
	CHECK(clock_gettime(CLOCK_REALTIME, &end) == 0);
	end.tv_sec += 15;
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	while (!gate_entered)
		CHECK(pthread_cond_timedwait(&gate_cond, &gate_lock, &end) ==
		      0);
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_AGAIN);
	CHECK(f.ep && !atomic_load(&f.terminals));
	CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_adopt_cleanup(f.executor) == TR_OK);
	CHECK(tr_task_executor_join(f.executor) == TR_AGAIN);
	CHECK(pthread_mutex_lock(&gate_lock) == 0);
	gate_open = true;
	CHECK(pthread_cond_broadcast(&gate_cond) == 0);
	CHECK(pthread_mutex_unlock(&gate_lock) == 0);
	until = now_ms() + 15000;
	while ((ret = tr_task_executor_join(f.executor)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	CHECK(tr_task_executor_dispatch(f.executor, 8, &count) == TR_OK);
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_OK && !f.ep);
	CHECK(atomic_load(&f.terminals) == 1);
	CHECK(f.results[0].delivery == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(f.results[0].status == TR_RPC_STATUS_CANCELLED);
	CHECK(!f.results[0].final_accepted && !f.results[0].close_accepted);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	teardown(&f);
}
static void test_final_storage_outlives_endpoint(void)
{
	struct fixture f;
	struct request r;
	uint64_t until;
	int ret;
	setup(&f);
	CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) == TR_OK);
	atomic_store(&hold_final, true);
	r = (struct request){.f = &f, .method = 99};
	CHECK(tr_reactor_call(f.client.reactor, begin, &r) == TR_OK);
	wait_count(&f.terminals, 1);
	CHECK(held_final);
	CHECK(tr_reactor_call(f.server.reactor, close_endpoint, &f) == TR_OK);
	until = now_ms() + 15000;
	while ((ret = tr_reactor_call(f.server.reactor, shutdown_executor,
	                              &f)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK && !f.ep && !f.executor);
	CHECK(atomic_load(&f.server.budget.current_bytes) > 0);
	/* 测试发送端接住了所有权，故意延迟最后TX释放；不是对端收包证据。 */
	tr_buffer_release(held_final);
	held_final = NULL;
	CHECK(atomic_load(&f.server.budget.current_bytes) == 0);
	teardown(&f);
}
static void test_control_allocation_failures(void)
{
	for (unsigned i = 1; i <= 2; ++i) {
		struct fixture f;
		setup(&f);
		atomic_store(&fail_calloc_at, i);
		CHECK(tr_reactor_call(f.server.reactor, start_endpoint, &f) ==
		      TR_ERR_NOMEM);
		atomic_store(&fail_calloc_at, 0);
		CHECK(!f.ep && f.executor);
		teardown(&f);
	}
}
int main(void)
{
	test_control_allocation_failures();
	for (unsigned i = 0; i < 5; ++i)
		test_exchange(i);
	test_attach_failures();
	test_cancel();
	test_stopped();
	test_final_storage_outlives_endpoint();
	puts("rpc_record_endpoint: ok");
	return 0;
}
