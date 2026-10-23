#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_stream.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/rpc_codec.h"
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
#define BODY_MAX 131072U
#define CALLS 4U

void *__real_malloc(size_t size);
static _Thread_local int fail_malloc;
void *__wrap_malloc(size_t size)
{
	if (fail_malloc) {
		fail_malloc = 0;
		return NULL;
	}
	return __real_malloc(size);
}
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
static _Thread_local int reject_send;
static _Thread_local const struct tr_buffer *expected_body;
int __wrap_tr_stream_sendv(struct tr_stream_handle s,
                           struct tr_buffer *const *p, uint32_t n)
{
	if (expected_body)
		CHECK(n == 2 && p[1] == expected_body);
	if (reject_send) {
		int ret = reject_send;
		reject_send = 0;
		return ret;
	}
	return __real_tr_stream_sendv(s, p, n);
}

struct peer;
struct call_context {
	struct peer *peer;
	struct tr_stream_handle stream;
	struct tr_rpc_record_stream *call;
	_Atomic unsigned seen[6];
	_Atomic unsigned terminals;
	_Atomic int last_receive;
	uint32_t final_status;
	enum tr_rpc_record_delivery delivery;
	uint64_t checksum;
	size_t last_body;
	unsigned direct_bodies;
};
struct peer {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_buffer_pool reassembly;
	struct tr_memory_budget budget;
	struct tr_rpc_record_stream_config config;
	struct call_context calls[CALLS];
	_Atomic unsigned opened;
	struct tr_buffer *held[8];
	struct tr_stream_handle held_streams[8];
	size_t held_count;
	int hold;
	int route_disabled;
	const uint8_t *current_data;
	size_t current_size;
};
struct fixture {
	struct peer client, server;
};

static uint64_t monotonic_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void wait_counter(_Atomic unsigned *counter, unsigned n)
{
	uint64_t end = monotonic_ms() + 15000;
	while (atomic_load(counter) < n) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
}
static uint64_t sum(const uint8_t *bytes, size_t n)
{
	uint64_t value = 0;
	for (size_t i = 0; i < n; ++i)
		value += bytes[i];
	return value;
}
static void received(struct tr_rpc_record_stream *call,
                     const struct tr_rpc_record_view *view, void *arg)
{
	struct call_context *c = arg;
	struct tr_rpc_record_state state;
	size_t cached;
	struct tr_rpc_record_stream *same = call;
	CHECK(call == c->call);
	CHECK(tr_reactor_in_owner_context_for(c->peer->reactor));
	CHECK(tr_rpc_record_stream_snapshot_on_owner(call, &state, &cached) ==
	      TR_OK);
	CHECK(tr_rpc_record_stream_destroy_on_owner(&same) == TR_ERR_STATE);
	CHECK(same == call);
	CHECK(tr_rpc_record_stream_receive_on_owner(call, c->stream, NULL, 0) ==
	      TR_ERR_STATE);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		c->last_body = view->header.decoded_size;
		c->checksum = sum(view->body, c->last_body);
		if (view->body &&
		    (uintptr_t)view->body >= (uintptr_t)c->peer->current_data &&
		    (uintptr_t)view->body + c->last_body <=
		            (uintptr_t)c->peer->current_data +
		                    c->peer->current_size)
			++c->direct_bodies;
	}
	atomic_fetch_add(&c->seen[view->header.kind], 1);
}
static void terminal(struct tr_rpc_record_stream *call,
                     enum tr_rpc_status status,
                     enum tr_rpc_record_delivery delivery, void *arg)
{
	struct call_context *c = arg;
	struct tr_rpc_record_stream *same = call;
	CHECK(c->call == call);
	CHECK((delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(c->peer->reactor));
	if (delivery == TR_RPC_RECORD_ON_OWNER)
		CHECK(tr_rpc_record_stream_destroy_on_owner(&same) ==
		      TR_ERR_STATE);
	else
		CHECK(status == TR_RPC_STATUS_CANCELLED);
	c->final_status = (uint32_t)status;
	c->delivery = delivery;
	CHECK(atomic_fetch_add(&c->terminals, 1) == 0);
}
static enum tr_stream_data_disposition
on_data(struct tr_stream_handle s, uint64_t id, struct tr_buffer *b, void *arg)
{
	struct peer *p = arg;
	struct call_context *c;
	(void)id;
	CHECK(s.slot < CALLS);
	c = &p->calls[s.slot];
	if (p->route_disabled || !c->call)
		return TR_STREAM_DATA_RELEASE;
	p->current_data = b->data;
	p->current_size = b->len;
	atomic_store(&c->last_receive, tr_rpc_record_stream_receive_on_owner(
	                                       c->call, s, b->data, b->len));
	p->current_data = NULL;
	p->current_size = 0;
	if (p->hold) {
		CHECK(p->held_count < 8);
		p->held[p->held_count] = b;
		p->held_streams[p->held_count++] = s;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void on_event(struct tr_stream_handle s, enum tr_stream_event e,
                     int status, void *arg)
{
	struct peer *p = arg;
	struct call_context *c;
	CHECK(s.slot < CALLS);
	c = &p->calls[s.slot];
	if (p->route_disabled)
		return;
	if (e == TR_STREAM_EVENT_OPENED) {
		struct tr_rpc_record_stream_callbacks callbacks = {received,
		                                                   terminal, c};
		CHECK(!c->call);
		c->stream = s;
		CHECK(tr_rpc_record_stream_create_on_owner(
		              s, &p->config, &callbacks, &p->budget,
		              &c->call) == TR_OK);
		atomic_fetch_add(&p->opened, 1);
	} else if (c->call) {
		CHECK(tr_rpc_record_stream_event_on_owner(c->call, s, e,
		                                          status) == TR_OK);
	}
}
static void tcp_pair(int *client, int *server)
{
	int listener = -1;
	uint16_t port;
	struct pollfd fds[2];
	int ret;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) == TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, client);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	*server = -1;
	uint64_t end = monotonic_ms() + 15000;
	while (tr_tcp_accept(listener, server) == TR_AGAIN) {
		CHECK(monotonic_ms() < end);
		fds[0] = (struct pollfd){listener, POLLIN, 0};
		fds[1] = (struct pollfd){*client, POLLOUT, 0};
		CHECK(poll(fds, 2, 100) >= 0);
	}
	CHECK(*server >= 0);
	if (ret == TR_IN_PROGRESS) {
		fds[0] = (struct pollfd){*client, POLLOUT, 0};
		CHECK(poll(fds, 1, 1000) == 1);
		CHECK(tr_tcp_finish_connect(*client) == TR_OK);
	}
	CHECK(close(listener) == 0);
}
static void init_peer(struct peer *p, int fd, int client,
                      enum tr_rpc_cardinality req, enum tr_rpc_cardinality resp,
                      uint64_t window, uint64_t threshold, uint32_t frame,
                      uint32_t message)
{
	struct tr_reactor_config rc = {.max_connections = 4,
	                               .command_capacity = 64,
	                               .tx_item_capacity = 64,
	                               .control_tx_item_capacity = 64,
	                               .rx_buffer_count = 16,
	                               .rx_buffer_size = 256,
	                               .max_payload_len = frame};
	struct tr_channel_config cc = {
	        .role = client ? TR_CHANNEL_CLIENT : TR_CHANNEL_SERVER,
	        .mode = TR_CHANNEL_SHARED_CONNECTION,
	        .feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET,
	        .max_streams = CALLS,
	        .initial_window_bytes = window,
	        .window_update_threshold_bytes = threshold,
	        .max_message_bytes = message};
	struct tr_conn_handle conn;
	*p = (struct peer){0};
	atomic_init(&p->opened, 0);
	for (size_t i = 0; i < CALLS; ++i) {
		p->calls[i].peer = p;
		for (size_t j = 0; j < 6; ++j)
			atomic_init(&p->calls[i].seen[j], 0);
		atomic_init(&p->calls[i].terminals, 0);
		atomic_init(&p->calls[i].last_receive, 0);
	}
	p->config = (struct tr_rpc_record_stream_config){
	        .send_direction = client ? TR_RPC_RECORD_REQUEST_DIRECTION
	                                 : TR_RPC_RECORD_RESPONSE_DIRECTION,
	        .method = {7, 9, req, resp, TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
	                   TR_LANE_BULK, BODY_MAX, BODY_MAX}};
	tr_memory_budget_init(&p->budget, 4 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->reassembly, 4, BODY_MAX + 1024) == TR_OK);
	cc.reassembly_pool = &p->reassembly;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	CHECK(tr_channel_create(&cc, conn, conn, on_data, on_event, NULL, p,
	                        &p->channel) == TR_OK);
}
static void init_fixture_windows(struct fixture *f, enum tr_rpc_cardinality req,
                                 enum tr_rpc_cardinality resp,
                                 uint64_t client_window, uint64_t server_window,
                                 uint64_t threshold, uint32_t frame,
                                 uint32_t message)
{
	int a, b;
	tcp_pair(&a, &b);
	init_peer(&f->client, a, 1, req, resp, client_window, threshold, frame,
	          message);
	init_peer(&f->server, b, 0, req, resp, server_window, threshold, frame,
	          message);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
}
static void init_fixture(struct fixture *f, enum tr_rpc_cardinality req,
                         enum tr_rpc_cardinality resp, uint64_t window)
{
	init_fixture_windows(f, req, resp, window, window, 1, 256,
	                     BODY_MAX + 1024);
}
static void open_call(struct fixture *f, unsigned count)
{
	struct tr_stream_handle stream;
	CHECK(tr_stream_open(f->client.channel, TR_LANE_BULK, &stream) ==
	      TR_OK);
	wait_counter(&f->client.opened, count);
	wait_counter(&f->server.opened, count);
}
struct owned {
	struct tr_buffer buffer;
	_Atomic unsigned *released;
	uint8_t data[];
};
static void release_body(struct tr_buffer *buffer)
{
	struct owned *owned = (struct owned *)buffer;
	_Atomic unsigned *released = owned->released;
	free(owned);
	if (released)
		atomic_fetch_add(released, 1);
}
static struct tr_buffer *make_buffer(const void *data, size_t n,
                                     _Atomic unsigned *released)
{
	struct owned *owned = malloc(sizeof(*owned) + n);
	CHECK(owned);
	owned->released = released;
	owned->buffer = (struct tr_buffer){.data = owned->data,
	                                   .capacity = (uint32_t)n,
	                                   .len = (uint32_t)n,
	                                   .release_cb = release_body};
	if (n)
		memcpy(owned->data, data, n);
	return &owned->buffer;
}
struct send_request {
	struct call_context *c;
	struct tr_rpc_record_header h;
	const uint8_t *md;
	struct tr_buffer *body;
	int reject;
	int oom;
};
static int send_owner(void *arg)
{
	struct send_request *r = arg;
	int ret;
	reject_send = r->reject;
	fail_malloc = r->oom;
	expected_body = r->body && r->body->len ? r->body : NULL;
	ret = tr_rpc_record_stream_send_on_owner(r->c->call, &r->h, r->md,
	                                         &r->body);
	reject_send = 0;
	fail_malloc = 0;
	expected_body = NULL;
	return ret;
}
static void send_until(struct send_request *r)
{
	uint64_t end = monotonic_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(r->c->peer->reactor, send_owner, r)) ==
	       TR_AGAIN) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r->body);
}
static struct tr_rpc_record_header head(enum tr_rpc_record_kind kind)
{
	struct tr_rpc_record_header h = {.kind = kind};
	if (kind == TR_RPC_RECORD_OPEN) {
		h.service_id = 7;
		h.method_id = 9;
	}
	if (kind == TR_RPC_RECORD_MESSAGE)
		h.codec_id = TR_RPC_CODEC_RAW;
	return h;
}
static void send_kind(struct call_context *c, enum tr_rpc_record_kind kind)
{
	struct send_request r = {.c = c, .h = head(kind)};
	send_until(&r);
}
struct snapshot {
	struct call_context *c;
	struct tr_rpc_record_state state;
	size_t cached;
};
static int snapshot_owner(void *arg)
{
	struct snapshot *s = arg;
	return tr_rpc_record_stream_snapshot_on_owner(s->c->call, &s->state,
	                                              &s->cached);
}
static struct snapshot snapshot(struct call_context *c)
{
	struct snapshot s = {.c = c};
	CHECK(tr_reactor_call(c->peer->reactor, snapshot_owner, &s) == TR_OK);
	return s;
}
static int release_held(void *arg)
{
	struct peer *p = arg;
	for (size_t i = 0; i < p->held_count; ++i)
		CHECK(tr_stream_release_payload(p->held_streams[i],
		                                p->held[i]) == TR_OK);
	p->held_count = 0;
	p->hold = 0;
	return TR_OK;
}
static int cleanup_owner(void *arg)
{
	struct peer *p = arg;
	CHECK(release_held(p) == TR_OK);
	p->route_disabled = 1;
	for (size_t i = 0; i < CALLS; ++i)
		if (p->calls[i].call) {
			int ret = tr_rpc_record_stream_abort_on_owner(
			        p->calls[i].call, TR_RPC_STATUS_CANCELLED);
			CHECK(ret == TR_OK || ret == TR_ERR_STATE);
			CHECK(tr_rpc_record_stream_destroy_on_owner(
			              &p->calls[i].call) == TR_OK);
		}
	return TR_OK;
}
static void destroy_fixture(struct fixture *f)
{
	struct peer *peers[] = {&f->client, &f->server};
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_call(peers[i]->reactor, cleanup_owner,
		                      peers[i]) == TR_OK);
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_stop(peers[i]->reactor) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->reassembly) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
}

static void test_roundtrip(enum tr_rpc_cardinality req,
                           enum tr_rpc_cardinality resp, size_t size)
{
	struct fixture f;
	struct call_context *a, *b;
	struct send_request r;
	_Atomic unsigned released = 0;
	uint8_t *data = malloc(size ? size : 1);
	static const uint8_t metadata[] = {4,   3,   0,   'a', 'u',
	                                   't', 'h', 'a', 'b', 'c'};
	CHECK(data);
	for (size_t i = 0; i < size; ++i)
		data[i] = (uint8_t)(i * 7U);
	init_fixture(&f, req, resp, 4 * BODY_MAX);
	open_call(&f, 1);
	a = &f.client.calls[0];
	b = &f.server.calls[0];
	r = (struct send_request){
	        .c = a, .h = head(TR_RPC_RECORD_OPEN), .md = metadata};
	r.h.metadata_size = sizeof(metadata);
	send_until(&r);
	wait_counter(&b->seen[TR_RPC_RECORD_OPEN], 1);
	CHECK(atomic_load(&b->seen[TR_RPC_RECORD_MESSAGE]) == 0);
	if (req == TR_RPC_ONE || size) {
		r = (struct send_request){
		        .c = a,
		        .h = head(TR_RPC_RECORD_MESSAGE),
		        .body = make_buffer(data, size, &released)};
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		send_until(&r);
		wait_counter(&b->seen[TR_RPC_RECORD_MESSAGE], 1);
		CHECK(b->last_body == size && b->checksum == sum(data, size));
		if (size)
			CHECK(b->direct_bodies == 1);
	}
	send_kind(a, TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&b->seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(!snapshot(a).state.terminal && !snapshot(b).state.terminal);
	send_kind(b, TR_RPC_RECORD_INITIAL_METADATA);
	if (resp == TR_RPC_ONE || size) {
		r = (struct send_request){
		        .c = b,
		        .h = head(TR_RPC_RECORD_MESSAGE),
		        .body = make_buffer(data, size, &released)};
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		send_until(&r);
		wait_counter(&a->seen[TR_RPC_RECORD_MESSAGE], 1);
		CHECK(!snapshot(a).state.terminal &&
		      atomic_load(&a->terminals) == 0);
		CHECK(a->last_body == size && a->checksum == sum(data, size));
	}
	send_kind(b, TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&a->terminals, 1);
	wait_counter(&b->terminals, 1);
	CHECK(a->final_status == TR_RPC_STATUS_OK &&
	      b->final_status == TR_RPC_STATUS_OK);
	CHECK(snapshot(a).cached == 0 && snapshot(b).cached == 0);
	destroy_fixture(&f);
	CHECK(atomic_load(&released) ==
	      (unsigned)((req == TR_RPC_ONE || size) +
	                 (resp == TR_RPC_ONE || size)));
	free(data);
}

static int set_hold(void *arg)
{
	((struct peer *)arg)->hold = 1;
	return TR_OK;
}
static void test_backpressure(void)
{
	struct fixture f;
	struct call_context *a, *b;
	struct send_request r;
	struct snapshot before, after;
	struct tr_buffer *original;
	uint8_t data[16] = {1};
	_Atomic unsigned released = 0;
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 128);
	open_call(&f, 1);
	a = &f.client.calls[0];
	b = &f.server.calls[0];
	CHECK(tr_reactor_call(f.server.reactor, set_hold, &f.server) == TR_OK);
	r = (struct send_request){
	        .c = a, .h = head(TR_RPC_RECORD_OPEN), .reject = TR_AGAIN};
	before = snapshot(a);
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &r) == TR_AGAIN);
	after = snapshot(a);
	CHECK(memcmp(&before.state, &after.state, sizeof(before.state)) == 0);
	r.reject = 0;
	r.oom = 1;
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &r) ==
	      TR_ERR_NOMEM);
	CHECK(!snapshot(a).state.opened);
	r.oom = 0;
	send_until(&r);
	wait_counter(&b->seen[TR_RPC_RECORD_OPEN], 1);
	r = (struct send_request){
	        .c = a,
	        .h = head(TR_RPC_RECORD_MESSAGE),
	        .reject = TR_AGAIN,
	        .body = make_buffer(data, sizeof(data), &released)};
	original = r.body;
	r.h.encoded_size = r.h.decoded_size = sizeof(data);
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &r) == TR_AGAIN);
	CHECK(r.body == original && !atomic_load(&released));
	CHECK(snapshot(a).state.request_messages == 0);
	r.reject = 0;
	send_until(&r);
	wait_counter(&b->seen[TR_RPC_RECORD_MESSAGE], 1);
	/* 真实窗口128已占112；不是故障注入。半关闭固定头48不能准入。 */
	r = (struct send_request){.c = a, .h = head(TR_RPC_RECORD_HALF_CLOSE)};
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &r) == TR_AGAIN);
	CHECK(!snapshot(a).state.request_closed);
	CHECK(tr_reactor_call(f.server.reactor, release_held, &f.server) ==
	      TR_OK);
	send_until(&r);
	wait_counter(&b->seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(snapshot(a).state.request_closed);
	send_kind(b, TR_RPC_RECORD_INITIAL_METADATA);
	send_kind(b, TR_RPC_RECORD_MESSAGE);
	send_kind(b, TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&a->terminals, 1);
	destroy_fixture(&f);
	CHECK(atomic_load(&released) == 1);
}

static void test_window_size_rejection(uint32_t frame)
{
	struct fixture f;
	uint8_t data[129] = {1};
	_Atomic unsigned released = 0;
	init_fixture_windows(&f, TR_RPC_MANY, TR_RPC_MANY, 64, 128, 1, frame,
	                     BODY_MAX + 1024);
	open_call(&f, 1);
	struct call_context *peers[] = {&f.client.calls[0], &f.server.calls[0]};
	const uint64_t windows[] = {128, 64};
	send_kind(peers[0], TR_RPC_RECORD_OPEN);
	wait_counter(&peers[1]->seen[TR_RPC_RECORD_OPEN], 1);
	send_kind(peers[1], TR_RPC_RECORD_INITIAL_METADATA);
	wait_counter(&peers[0]->seen[TR_RPC_RECORD_INITIAL_METADATA], 1);
	for (unsigned side = 0; side < 2; ++side) {
		struct call_context *sender = peers[side],
		                    *receiver = peers[1 - side];
		unsigned messages = 0;
		for (unsigned round = 0; round < 2; ++round) {
			struct tr_stream_diagnostics before, after;
			struct snapshot state = snapshot(sender);
			struct send_request r = {
			        .c = sender, .h = head(TR_RPC_RECORD_MESSAGE)};
			size_t size =
			        windows[side] + 1 - TR_RPC_RECORD_HEADER_SIZE;
			r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
			r.body = make_buffer(data, size, &released);
			struct tr_buffer *original = r.body;
			unsigned releases = atomic_load(&released);
			CHECK(tr_stream_get_diagnostics(sender->stream,
			                                &before) == TR_OK);
			/* 初始信用和累计绝对信用不同。第二轮已经成功发送超过
			 * W， 同一条 W+1 消息仍必须立即拒绝，不留给重试定时器。
			 */
			if (round)
				CHECK(before.flow.tx_sent_bytes >
				      windows[side]);
			for (unsigned retry = 0; retry < 2; ++retry) {
				int ret = tr_reactor_call(sender->peer->reactor,
				                          send_owner, &r);
				if (ret != TR_ERR_BAD_LENGTH)
					fprintf(stderr,
					        "window=%llu record=%zu "
					        "send=%d\n",
					        (unsigned long long)
					                windows[side],
					        size + TR_RPC_RECORD_HEADER_SIZE,
					        ret);
				CHECK(ret == TR_ERR_BAD_LENGTH);
			}
			CHECK(r.body == original &&
			      atomic_load(&released) == releases);
			CHECK(tr_stream_get_diagnostics(sender->stream,
			                                &after) == TR_OK);
			CHECK(before.flow.tx_sent_bytes ==
			              after.flow.tx_sent_bytes &&
			      before.next_tx_message_id ==
			              after.next_tx_message_id);
			struct snapshot unchanged = snapshot(sender);
			CHECK(!memcmp(&state.state, &unchanged.state,
			              sizeof(state.state)));
			tr_buffer_release(r.body);
			for (unsigned delta = 1; delta <= 2; ++delta) {
				size = windows[side] -
				       TR_RPC_RECORD_HEADER_SIZE - (2 - delta);
				r.h.encoded_size = r.h.decoded_size =
				        (uint32_t)size;
				r.body = make_buffer(data, size, NULL);
				send_until(&r);
				wait_counter(
				        &receiver->seen[TR_RPC_RECORD_MESSAGE],
				        ++messages);
				CHECK(receiver->last_body == size &&
				      receiver->checksum == 1);
			}
		}
	}
	send_kind(peers[0], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&peers[1]->seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	send_kind(peers[1], TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&peers[0]->terminals, 1);
	CHECK(peers[0]->final_status == TR_RPC_STATUS_OK);
	destroy_fixture(&f);
	CHECK(atomic_load(&released) == 4);
}

static void test_credit_threshold(uint64_t window, uint32_t message,
                                  uint64_t threshold, uint64_t expected,
                                  uint64_t updates)
{
	struct fixture f;
	uint8_t data[256] = {1};
	init_fixture_windows(&f, TR_RPC_MANY, TR_RPC_MANY, window, window,
	                     threshold, 32, message);
	open_call(&f, 1);
	struct call_context *a = &f.client.calls[0], *b = &f.server.calls[0];
	send_kind(a, TR_RPC_RECORD_OPEN);
	wait_counter(&b->seen[TR_RPC_RECORD_OPEN], 1);
	(void)snapshot(b); /* 所有者屏障：数据回调后的消费记账已经完成。 */
	struct tr_stream_flow_state flow;
	CHECK(tr_stream_get_flow_state(b->stream, &flow) == TR_OK);
	CHECK(flow.rx_received_bytes == TR_RPC_RECORD_HEADER_SIZE &&
	      flow.rx_consumed_bytes == TR_RPC_RECORD_HEADER_SIZE);
	uint64_t maximum = window < message ? window : message;
	if (flow.rx_advertised_limit != expected)
		fprintf(stderr,
		        "window=%llu message=%u threshold=%llu consumed=%llu "
		        "credit=%llu expected=%llu\n",
		        (unsigned long long)window, message,
		        (unsigned long long)threshold,
		        (unsigned long long)flow.rx_consumed_bytes,
		        (unsigned long long)flow.rx_advertised_limit,
		        (unsigned long long)expected);
	CHECK(flow.rx_advertised_limit == expected);
	/* 最大合法记录重复发送；允许保留安全的少量债务，而非一律每帧更新。 */
	for (unsigned i = 1; i <= 3; ++i) {
		struct send_request r = {.c = a,
		                         .h = head(TR_RPC_RECORD_MESSAGE)};
		size_t size = (size_t)maximum - TR_RPC_RECORD_HEADER_SIZE;
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		r.body = make_buffer(data, size, NULL);
		send_until(&r);
		wait_counter(&b->seen[TR_RPC_RECORD_MESSAGE], i);
	}
	(void)snapshot(b);
	struct tr_channel_stats stats;
	CHECK(tr_channel_get_stats(f.server.channel, &stats) == TR_OK);
	CHECK(stats.window_updates_tx == updates);
	printf("credit threshold: window=%llu message=%u requested=%llu "
	       "OPEN-credit=%llu updates=%llu\n",
	       (unsigned long long)window, message,
	       (unsigned long long)threshold, (unsigned long long)expected,
	       (unsigned long long)stats.window_updates_tx);
	send_kind(a, TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&b->seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	send_kind(b, TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&a->terminals, 1);
	CHECK(a->final_status == TR_RPC_STATUS_OK);
	destroy_fixture(&f);
}

static void test_held_window_isolation(void)
{
	struct fixture f;
	init_fixture_windows(&f, TR_RPC_MANY, TR_RPC_MANY, 128, 128, 128, 32,
	                     256);
	open_call(&f, 1);
	struct call_context *a = &f.client.calls[0], *b = &f.server.calls[0];
	CHECK(tr_reactor_call(f.server.reactor, set_hold, &f.server) == TR_OK);
	send_kind(a, TR_RPC_RECORD_OPEN);
	send_kind(a, TR_RPC_RECORD_MESSAGE);
	wait_counter(&b->seen[TR_RPC_RECORD_MESSAGE], 1);
	struct send_request blocked = {.c = a,
	                               .h = head(TR_RPC_RECORD_MESSAGE)};
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &blocked) ==
	      TR_AGAIN);
	CHECK(snapshot(a).state.request_messages == 1);
	/* 第一个流保留 96/128 字节时，另一流可以走完零消息 MANY 调用。 */
	open_call(&f, 2);
	send_kind(&f.client.calls[1], TR_RPC_RECORD_OPEN);
	send_kind(&f.client.calls[1], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&f.server.calls[1].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	send_kind(&f.server.calls[1], TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&f.client.calls[1].terminals, 1);
	CHECK(f.client.calls[1].final_status == TR_RPC_STATUS_OK);
	CHECK(tr_reactor_call(f.client.reactor, send_owner, &blocked) ==
	      TR_AGAIN);
	CHECK(tr_reactor_call(f.server.reactor, release_held, &f.server) ==
	      TR_OK);
	send_until(&blocked);
	send_kind(a, TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&b->seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(atomic_load(&b->seen[TR_RPC_RECORD_MESSAGE]) == 2);
	send_kind(b, TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&a->terminals, 1);
	CHECK(a->final_status == TR_RPC_STATUS_OK);
	destroy_fixture(&f);
}

struct raw_request {
	struct call_context *c;
	struct tr_buffer *buffer;
};
static int raw_owner(void *arg)
{
	struct raw_request *r = arg;
	int ret = tr_stream_send(r->c->stream, r->buffer);
	if (ret == TR_OK)
		r->buffer = NULL;
	return ret;
}
static void raw_send(struct call_context *c, const void *data, size_t size)
{
	struct raw_request r = {c, make_buffer(data, size, NULL)};
	int ret;
	uint64_t end = monotonic_ms() + 15000;
	while ((ret = tr_reactor_call(c->peer->reactor, raw_owner, &r)) ==
	       TR_AGAIN) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r.buffer);
}
static size_t encode(uint8_t *bytes, struct tr_rpc_record_header h,
                     const uint8_t *md, const uint8_t *body)
{
	struct tr_rpc_record_limits limits = {
	        UINT32_MAX, UINT32_MAX, TR_RPC_METADATA_MAX_BYTES,
	        TR_RPC_RECORD_ACCEPT_NONE | TR_RPC_RECORD_ACCEPT_ZSTD};
	CHECK(tr_rpc_record_encode_header(bytes, TR_RPC_RECORD_HEADER_SIZE, &h,
	                                  &limits) == TR_OK);
	if (h.metadata_size)
		memcpy(bytes + TR_RPC_RECORD_HEADER_SIZE, md, h.metadata_size);
	if (h.encoded_size && body)
		memcpy(bytes + TR_RPC_RECORD_HEADER_SIZE + h.metadata_size,
		       body, h.encoded_size);
	return TR_RPC_RECORD_HEADER_SIZE + h.metadata_size +
	       (body ? h.encoded_size : 0);
}
static void test_incremental_and_invalid(void)
{
	struct fixture f;
	uint8_t bytes[512];
	size_t used, off;
	static const uint8_t md[] = {3, 1, 0, 'k', 'e', 'y', 42};
	struct tr_rpc_record_header h;
	init_fixture(&f, TR_RPC_MANY, TR_RPC_MANY, 4096);
	open_call(&f, 1);
	h = head(TR_RPC_RECORD_OPEN);
	h.metadata_size = sizeof(md);
	used = encode(bytes, h, md, NULL);
	/* 每次一个字节，固定头和TLV都会跨不同逻辑Stream消息。 */
	for (off = 0; off < used; ++off)
		raw_send(&f.client.calls[0], bytes + off, 1);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	CHECK(snapshot(&f.server.calls[0]).cached == 0);
	off = encode(bytes, head(TR_RPC_RECORD_MESSAGE), NULL, NULL);
	off += encode(bytes + off, head(TR_RPC_RECORD_MESSAGE), NULL, NULL);
	off += encode(bytes + off, head(TR_RPC_RECORD_HALF_CLOSE), NULL, NULL);
	raw_send(&f.client.calls[0], bytes, off);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(snapshot(&f.server.calls[0]).state.request_messages == 2);
	/* 同连接另一调用：重复TLV被拒绝，先前合法调用状态不变。 */
	open_call(&f, 2);
	h = head(TR_RPC_RECORD_OPEN);
	h.metadata_size = sizeof(md) * 2;
	uint8_t duplicate[sizeof(md) * 2];
	memcpy(duplicate, md, sizeof(md));
	memcpy(duplicate + sizeof(md), md, sizeof(md));
	used = encode(bytes, h, duplicate, NULL);
	raw_send(&f.client.calls[1], bytes, used);
	wait_counter(&f.server.calls[1].terminals, 1);
	CHECK(f.server.calls[1].final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	CHECK(atomic_load(&f.server.calls[1].seen[TR_RPC_RECORD_OPEN]) == 0);
	CHECK(!snapshot(&f.server.calls[0]).state.terminal);
	destroy_fixture(&f);
}

static void test_error_isolation(void)
{
	struct fixture f;
	uint8_t bytes[128];
	static const uint8_t duplicates[] = {1, 0, 0, 'x', 1, 0, 0, 'x'};
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_OPEN);
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_MESSAGE);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	open_call(&f, 2);
	h.metadata_size = sizeof(duplicates);
	size_t n = encode(bytes, h, duplicates, NULL);
	raw_send(&f.client.calls[1], bytes, n);
	wait_counter(&f.server.calls[1].terminals, 1);
	CHECK(f.server.calls[1].final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	send_kind(&f.server.calls[0], TR_RPC_RECORD_INITIAL_METADATA);
	send_kind(&f.server.calls[0], TR_RPC_RECORD_MESSAGE);
	send_kind(&f.server.calls[0], TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&f.client.calls[0].terminals, 1);
	CHECK(f.client.calls[0].final_status == TR_RPC_STATUS_OK);
	CHECK(tr_channel_active_streams(f.client.channel) == 2);
	CHECK(tr_channel_active_streams(f.server.channel) == 2);
	destroy_fixture(&f);
}

struct quota {
	struct peer *peer;
	uint64_t bytes;
};
static int hold_quota(void *arg)
{
	struct quota *q = arg;
	q->bytes = q->peer->budget.limit_bytes -
	           atomic_load(&q->peer->budget.current_bytes);
	return tr_memory_budget_reserve(&q->peer->budget, q->bytes);
}
static void test_receive_quota(void)
{
	struct fixture f;
	struct quota quota = {0};
	uint8_t bytes[48];
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_OPEN);
	struct tr_rpc_record_limits limits = {BODY_MAX, BODY_MAX, 512,
	                                      TR_RPC_RECORD_ACCEPT_NONE};
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	quota.peer = &f.server;
	CHECK(tr_reactor_call(f.server.reactor, hold_quota, &quota) == TR_OK);
	h.metadata_size = 8;
	CHECK(tr_rpc_record_encode_header(bytes, sizeof(bytes), &h, &limits) ==
	      TR_OK);
	raw_send(&f.client.calls[0], bytes, sizeof(bytes));
	wait_counter(&f.server.calls[0].terminals, 1);
	CHECK(f.server.calls[0].final_status ==
	      TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN]) == 0);
	CHECK(snapshot(&f.server.calls[0]).cached == 0);
	CHECK(tr_memory_budget_release(&f.server.budget, quota.bytes) == TR_OK);
	destroy_fixture(&f);
}
static int abort_owner(void *arg)
{
	struct call_context *c = arg;
	return tr_rpc_record_stream_abort_on_owner(c->call,
	                                           TR_RPC_STATUS_CANCELLED);
}
static void test_partial_cancel(void)
{
	struct fixture f;
	uint8_t bytes[64] = {0};
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_MESSAGE);
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	h.encoded_size = h.decoded_size = 32;
	encode(bytes, h, NULL, NULL);
	raw_send(&f.client.calls[0], bytes, 52);
	uint64_t until = monotonic_ms() + 15000;
	while (snapshot(&f.server.calls[0]).cached == 0) {
		CHECK(monotonic_ms() < until);
		sched_yield();
	}
	CHECK(tr_reactor_call(f.server.reactor, abort_owner,
	                      &f.server.calls[0]) == TR_OK);
	CHECK(snapshot(&f.server.calls[0]).cached == 0);
	CHECK(atomic_load(&f.server.calls[0].terminals) == 1);
	CHECK(tr_reactor_call(f.server.reactor, abort_owner,
	                      &f.server.calls[0]) == TR_ERR_STATE);
	raw_send(&f.client.calls[0], bytes, 28);
	CHECK(atomic_load(&f.server.calls[0].seen[TR_RPC_RECORD_MESSAGE]) == 0);
	destroy_fixture(&f);
}

struct reset_event_request {
	struct call_context *call;
	int reason;
};
static int reset_event_on_owner(void *arg)
{
	struct reset_event_request *r = arg;
	return tr_rpc_record_stream_event_on_owner(
	        r->call->call, r->call->stream, TR_STREAM_EVENT_RESET,
	        r->reason);
}
static void reject_invalid_reset(struct call_context *call)
{
	const int reasons[] = {TR_OK, TR_AGAIN, TR_ERR_INVALID,
	                       TR_ERR_UNSUPPORTED};
	struct snapshot before = snapshot(call);
	uint64_t bytes = atomic_load(&call->peer->budget.current_bytes);
	CHECK(!before.state.terminal);
	for (size_t i = 0; i < sizeof(reasons) / sizeof(reasons[0]); ++i) {
		struct reset_event_request r = {call, reasons[i]};
		CHECK(tr_reactor_call(call->peer->reactor, reset_event_on_owner,
		                      &r) == TR_ERR_INVALID);
		struct snapshot after = snapshot(call);
		CHECK(!memcmp(&before.state, &after.state,
		              sizeof(before.state)));
		CHECK(after.cached == before.cached &&
		      atomic_load(&call->peer->budget.current_bytes) == bytes &&
		      !atomic_load(&call->terminals));
	}
}
static int half_close_event_on_owner(void *arg)
{
	struct call_context *call = arg;
	return tr_rpc_record_stream_event_on_owner(
	        call->call, call->stream, TR_STREAM_EVENT_REMOTE_CLOSED, TR_OK);
}
static void test_reset_after_half_close(void)
{
	struct fixture f;
	init_fixture(&f, TR_RPC_MANY, TR_RPC_MANY, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	struct call_context *call = &f.server.calls[0];
	CHECK(snapshot(call).state.request_closed);
	CHECK(tr_reactor_call(f.server.reactor, half_close_event_on_owner,
	                      call) == TR_OK);
	CHECK(!atomic_load(&call->terminals));
	reject_invalid_reset(call);
	struct reset_event_request r = {call, TR_ERR_TIMEOUT};
	CHECK(tr_reactor_call(f.server.reactor, reset_event_on_owner, &r) ==
	      TR_OK);
	CHECK(snapshot(call).state.terminal && snapshot(call).cached == 0 &&
	      call->final_status == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      atomic_load(&call->terminals) == 1);
	CHECK(tr_reactor_call(f.server.reactor, reset_event_on_owner, &r) ==
	      TR_OK);
	CHECK(atomic_load(&call->terminals) == 1);
	destroy_fixture(&f);
}
static void test_reset_partial_open(void)
{
	struct fixture f;
	uint8_t bytes[64];
	const uint8_t metadata[] = {1, 0, 0, 'k'};
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_OPEN);
	init_fixture(&f, TR_RPC_MANY, TR_RPC_MANY, 4096);
	open_call(&f, 1);
	h.metadata_size = sizeof(metadata);
	(void)encode(bytes, h, metadata, NULL);
	raw_send(&f.client.calls[0], bytes, TR_RPC_RECORD_HEADER_SIZE + 1);
	struct call_context *call = &f.server.calls[0];
	uint64_t until = monotonic_ms() + 15000;
	while (!snapshot(call).cached) {
		CHECK(monotonic_ms() < until);
		sched_yield();
	}
	CHECK(!atomic_load(&call->seen[TR_RPC_RECORD_OPEN]));
	reject_invalid_reset(call);
	struct reset_event_request r = {call, TR_ERR_CLOSED};
	CHECK(tr_reactor_call(f.server.reactor, reset_event_on_owner, &r) ==
	      TR_OK);
	CHECK(snapshot(call).cached == 0 &&
	      call->final_status == TR_RPC_STATUS_CANCELLED &&
	      atomic_load(&call->terminals) == 1 &&
	      !atomic_load(&call->seen[TR_RPC_RECORD_OPEN]));
	destroy_fixture(&f);
}
static void test_reset_preserves_final(void)
{
	struct fixture f;
	init_fixture(&f, TR_RPC_MANY, TR_RPC_MANY, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	send_kind(&f.server.calls[0], TR_RPC_RECORD_INITIAL_METADATA);
	send_kind(&f.server.calls[0], TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&f.client.calls[0].terminals, 1);
	wait_counter(&f.server.calls[0].terminals, 1);
	struct call_context *calls[] = {&f.client.calls[0], &f.server.calls[0]};
	for (size_t i = 0; i < sizeof(calls) / sizeof(calls[0]); ++i) {
		struct snapshot before = snapshot(calls[i]);
		struct reset_event_request r = {calls[i], TR_ERR_TIMEOUT};
		CHECK(tr_reactor_call(calls[i]->peer->reactor,
		                      reset_event_on_owner, &r) == TR_OK);
		struct snapshot after = snapshot(calls[i]);
		CHECK(!memcmp(&before.state, &after.state,
		              sizeof(before.state)));
		CHECK(calls[i]->final_status == TR_RPC_STATUS_OK &&
		      atomic_load(&calls[i]->terminals) == 1);
	}
	destroy_fixture(&f);
}

static int identity_owner(void *arg)
{
	struct call_context *c = arg;
	struct tr_stream_handle stale = c->stream;
	struct tr_rpc_record_state before, after;
	size_t cached;
	CHECK(tr_rpc_record_stream_snapshot_on_owner(c->call, &before,
	                                             &cached) == TR_OK);
	++stale.generation;
	CHECK(tr_rpc_record_stream_receive_on_owner(c->call, stale, NULL, 0) ==
	      TR_ERR_STALE);
	CHECK(tr_rpc_record_stream_event_on_owner(
	              c->call, stale, TR_STREAM_EVENT_ERROR, TR_ERR_CLOSED) ==
	      TR_ERR_STALE);
	CHECK(tr_rpc_record_stream_snapshot_on_owner(c->call, &after,
	                                             &cached) == TR_OK);
	CHECK(!memcmp(&before, &after, sizeof(before)));
	return TR_OK;
}
static void test_owner_identity(void)
{
	struct fixture f;
	struct snapshot state;
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	state = (struct snapshot){.c = &f.client.calls[0]};
	CHECK(tr_reactor_call(f.server.reactor, snapshot_owner, &state) ==
	      TR_ERR_STATE);
	CHECK(tr_reactor_call(f.client.reactor, identity_owner,
	                      &f.client.calls[0]) == TR_OK);
	destroy_fixture(&f);
}
static int fail_receive_alloc(void *arg)
{
	(void)arg;
	fail_malloc = 1;
	return TR_OK;
}
static void test_receive_faults(unsigned kind)
{
	struct fixture f;
	uint8_t bytes[256];
	struct tr_rpc_record_header h;
	size_t n;
	static const uint8_t md[] = {3, 1, 0, 'k', 'e', 'y', 42};
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	h = head(TR_RPC_RECORD_OPEN);
	if (kind == 0)
		h.method_id++;
	if (kind == 1) {
		h.metadata_size = sizeof(md);
		CHECK(tr_reactor_call(f.server.reactor, fail_receive_alloc,
		                      NULL) == TR_OK);
	}
	if (kind == 2) {
		h.metadata_size = sizeof(md);
	}
	n = encode(bytes, h, md, NULL);
	if (kind == 1)
		n = TR_RPC_RECORD_HEADER_SIZE; /* 仅完整头，分配正文缓存时失败。
		                                */
	if (kind == 2)
		bytes[TR_RPC_RECORD_HEADER_SIZE + 3] = ':';
	if (kind >= 3) {
		send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
		wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
		h = head(TR_RPC_RECORD_MESSAGE);
		h.encoded_size = h.decoded_size = kind == 3 ? UINT32_MAX : 1;
		if (kind == 4)
			h.compression = TR_RPC_RECORD_ZSTD;
		if (kind == 5)
			++h.codec_id;
		n = encode(bytes, h, NULL, NULL); /* 超限固定头，尚无正文。 */
	}
	raw_send(&f.client.calls[0], bytes, n);
	wait_counter(&f.server.calls[0].terminals, 1);
	CHECK(f.server.calls[0].final_status ==
	      (kind == 1 ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
	                 : TR_RPC_STATUS_INVALID_ARGUMENT));
	CHECK(snapshot(&f.server.calls[0]).cached == 0);
	destroy_fixture(&f);
}
static int close_stream_owner(void *arg)
{
	return tr_stream_close(((struct call_context *)arg)->stream);
}
static void test_truncation(void)
{
	struct fixture f;
	uint8_t bytes[48];
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_MESSAGE);
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	h.encoded_size = h.decoded_size = 17;
	encode(bytes, h, NULL, NULL);
	raw_send(&f.client.calls[0], bytes, 13);
	CHECK(tr_reactor_call(f.client.reactor, close_stream_owner,
	                      &f.client.calls[0]) == TR_OK);
	wait_counter(&f.server.calls[0].terminals, 1);
	CHECK(f.server.calls[0].final_status == TR_RPC_STATUS_DATA_LOSS);
	destroy_fixture(&f);
}
static int disable_routes(void *arg)
{
	((struct peer *)arg)->route_disabled = 1;
	return TR_OK;
}
static void test_stopped_reaping(void)
{
	struct fixture f;
	struct peer *peers[] = {&f.client, &f.server};
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	for (size_t i = 0; i < 2; ++i) {
		CHECK(tr_rpc_record_stream_reap_stopped(
		              &peers[i]->calls[0].call) == TR_ERR_STATE);
		CHECK(tr_reactor_call(peers[i]->reactor, disable_routes,
		                      peers[i]) == TR_OK);
	}
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_stop(peers[i]->reactor) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		CHECK(tr_rpc_record_stream_reap_stopped(
		              &peers[i]->calls[0].call) == TR_OK);
		CHECK(peers[i]->calls[0].delivery ==
		      TR_RPC_RECORD_OWNER_STOPPED);
		CHECK(atomic_load(&peers[i]->calls[0].terminals) == 1);
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->reassembly) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
}
int main(void)
{
	for (unsigned a = 0; a < 2; ++a)
		for (unsigned b = 0; b < 2; ++b)
			test_roundtrip(a ? TR_RPC_MANY : TR_RPC_ONE,
			               b ? TR_RPC_MANY : TR_RPC_ONE, 0);
	test_roundtrip(TR_RPC_ONE, TR_RPC_ONE, 65537);
	test_backpressure();
	test_window_size_rejection(256);
	test_window_size_rejection(32);
	test_credit_threshold(128, 256, 128, 176, 5);
	test_credit_threshold(256, 128, 200, 256, 3);
	test_credit_threshold(256, 128, 32, 304, 5);
	test_held_window_isolation();
	test_incremental_and_invalid();
	for (unsigned i = 0; i < 6; ++i)
		test_receive_faults(i);
	test_error_isolation();
	test_receive_quota();
	test_partial_cancel();
	test_reset_after_half_close();
	test_reset_partial_open();
	test_reset_preserves_final();
	test_owner_identity();
	test_truncation();
	test_stopped_reaping();
	puts("rpc_record_stream: ok");
	return 0;
}
