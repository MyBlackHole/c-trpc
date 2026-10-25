#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_stream.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/message/builder.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/runtime/message_service.h"
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
#include <zstd.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define BODY_MAX 131072U
#define CALLS 5U

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
struct held_transform {
	tr_message_service_done_fn done;
	void *arg;
	struct tr_task_handle handle;
	int pending;
	int cancelled;
};
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
	uint8_t message_first_byte[8];
	uint64_t message_checksums[8];
	size_t message_sizes[8];
	uint32_t message_compression[8];
	uint32_t message_encoded_sizes[8];
	uint8_t last_metadata[TR_RPC_METADATA_MAX_BYTES];
	size_t last_metadata_size;
};
struct peer {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_message_service *message_service;
	struct tr_message_service_config message_config;
	struct held_transform held_transforms[4];
	_Atomic unsigned held_submissions;
	_Atomic unsigned held_cancellations;
	int hold_message_transforms;
	const uint8_t *expected_transform_input;
	size_t expected_transform_input_size;
	int check_transform_input;
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
	CHECK(view->header.metadata_size <= sizeof(c->last_metadata));
	c->last_metadata_size = view->header.metadata_size;
	if (c->last_metadata_size)
		memcpy(c->last_metadata, view->metadata, c->last_metadata_size);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		unsigned index = atomic_load(&c->seen[TR_RPC_RECORD_MESSAGE]);
		c->last_body = view->header.decoded_size;
		c->checksum = sum(view->body, c->last_body);
		if (index < 8U) {
			c->message_first_byte[index] =
			        view->body && c->last_body ? view->body[0] : 0U;
			c->message_checksums[index] = c->checksum;
			c->message_sizes[index] = c->last_body;
			c->message_compression[index] =
			        view->header.compression;
			c->message_encoded_sizes[index] =
			        view->header.encoded_size;
		}
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
static int peer_message_service_start_on_owner(void *arg)
{
	struct peer *peer = arg;
	int ret = tr_message_service_create_on_owner(
	        peer->reactor, &peer->message_config, &peer->budget,
	        &peer->message_service);
	return ret == TR_OK ? tr_message_service_start_on_owner(
	                              peer->message_service)
	                    : ret;
}

static int
peer_message_submit_adapter(void *context, struct tr_message *input,
                            const struct tr_message_transform_config *config,
                            tr_message_service_done_fn done, void *arg,
                            struct tr_task_handle *handle)
{
	struct peer *peer = context;
	if (peer->check_transform_input) {
		struct tr_message_view view;
		CHECK(tr_message_view(input, &view) == TR_OK);
		CHECK(view.len == peer->expected_transform_input_size);
		CHECK(view.len == 0U ||
		      memcmp(view.data, peer->expected_transform_input,
		             view.len) == 0);
	}
	if (peer->hold_message_transforms) {
		for (uint32_t slot = 0; slot < 4U; ++slot) {
			struct held_transform *held =
			        &peer->held_transforms[slot];
			if (held->pending)
				continue;
			if (tr_message_release(input) != TR_OK)
				return TR_ERR_STATE;
			held->done = done;
			held->arg = arg;
			held->handle = (struct tr_task_handle){
			        .instance = 1U,
			        .generation =
			                atomic_fetch_add(
			                        &peer->held_submissions, 1) +
			                1U,
			        .slot = slot};
			held->pending = 1;
			held->cancelled = 0;
			*handle = held->handle;
			return TR_OK;
		}
		return TR_AGAIN;
	}
	return peer->message_service ? tr_message_service_submit_on_owner(
	                                       peer->message_service, input,
	                                       config, done, arg, handle)
	                             : TR_ERR_CLOSED;
}

static int peer_message_cancel_adapter(void *context,
                                       struct tr_task_handle handle)
{
	struct peer *peer = context;
	if (peer->hold_message_transforms) {
		for (size_t i = 0; i < 4; ++i) {
			struct held_transform *held = &peer->held_transforms[i];
			if (held->pending &&
			    handle.instance == held->handle.instance &&
			    handle.generation == held->handle.generation &&
			    handle.slot == held->handle.slot) {
				held->cancelled = 1;
				atomic_fetch_add(&peer->held_cancellations, 1);
				return TR_OK;
			}
		}
		return TR_ERR_STALE;
	}
	return peer->message_service ? tr_message_service_cancel(
	                                       peer->message_service, handle)
	                             : TR_ERR_CLOSED;
}

struct peer_message_bind_request {
	struct peer *peer;
	int ret;
};

static int peer_message_bind_on_owner(void *arg)
{
	struct peer_message_bind_request *request = arg;
	request->ret = tr_channel_bind_message_service_on_owner(
	        request->peer->channel, request->peer->reactor,
	        peer_message_submit_adapter, peer_message_cancel_adapter,
	        request->peer);
	return request->ret;
}

static void init_peer(struct peer *p, int fd, int client,
                      enum tr_rpc_cardinality req, enum tr_rpc_cardinality resp,
                      uint64_t window, uint64_t threshold, uint32_t frame,
                      uint32_t message, uint32_t max_encoded_wire_bytes,
                      bool enable_message_caps, uint32_t receive_algorithms)
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
	        .feature_bits =
	                TR_CHANNEL_FEATURE_STREAM_RESET |
	                (enable_message_caps ? TR_CHANNEL_FEATURE_MESSAGE_CAPS
	                                     : 0U),
	        .receive_compression_algorithms = receive_algorithms,
	        .max_encoded_message_bytes = max_encoded_wire_bytes,
	        .max_compression_workspace_bytes =
	                enable_message_caps ? 1024U * 1024U : 0U,
	        .max_compression_window_log = enable_message_caps ? 20U : 0U,
	        .max_streams = CALLS,
	        .initial_window_bytes = window,
	        .window_update_threshold_bytes = threshold,
	        .max_message_bytes = message};
	struct tr_conn_handle conn;
	*p = (struct peer){0};
	atomic_init(&p->opened, 0);
	atomic_init(&p->held_submissions, 0);
	atomic_init(&p->held_cancellations, 0);
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
	                   TR_LANE_BULK, BODY_MAX, BODY_MAX, 0, 0}};
	tr_memory_budget_init(&p->budget, 4 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->reassembly, 4, BODY_MAX + 1024) == TR_OK);
	cc.reassembly_pool = &p->reassembly;
	if (enable_message_caps)
		cc.memory_budget = &p->budget;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	if (enable_message_caps) {
		p->message_config = (struct tr_message_service_config){
		        .tasks = {1U, 4U},
		        .dispatch_limit = 4U,
		        .watchdog_ns = UINT64_C(10000000)};
		CHECK(tr_reactor_call(p->reactor,
		                      peer_message_service_start_on_owner,
		                      p) == TR_OK);
		CHECK(tr_channel_create_deferred(&cc, conn, conn, on_data,
		                                 on_event, NULL, p,
		                                 &p->channel) == TR_OK);
		struct peer_message_bind_request bind = {p, TR_ERR_STATE};
		CHECK(tr_reactor_call(p->reactor, peer_message_bind_on_owner,
		                      &bind) == TR_OK);
		CHECK(bind.ret == TR_OK);
		CHECK(tr_channel_start(p->channel) == TR_OK);
	} else {
		CHECK(tr_channel_create(&cc, conn, conn, on_data, on_event,
		                        NULL, p, &p->channel) == TR_OK);
	}
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
	          message, 0U, false, 0U);
	init_peer(&f->server, b, 0, req, resp, server_window, threshold, frame,
	          message, 0U, false, 0U);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
}

static void init_fixture_compressed_receive_limits(
        struct fixture *f, enum tr_rpc_cardinality request,
        enum tr_rpc_cardinality response, uint64_t window,
        uint32_t server_encoded_wire_limit)
{
	int a, b;
	tcp_pair(&a, &b);
	init_peer(&f->client, a, 1, request, response, window, 1U, 256U,
	          BODY_MAX + 1024U, 0U, true, 0U);
	init_peer(&f->server, b, 0, request, response, window, 1U, 256U,
	          BODY_MAX + 1024U, server_encoded_wire_limit, true,
	          TR_MESSAGE_ALGORITHM_ZSTD);
	for (size_t i = 0; i < 2; ++i) {
		struct peer *peer = i == 0 ? &f->client : &f->server;
		peer->config.method.request_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
		peer->config.method.response_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
	}
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
}
static void init_fixture_compressed_receive_window(
        struct fixture *f, enum tr_rpc_cardinality request,
        enum tr_rpc_cardinality response, uint64_t window)
{
	init_fixture_compressed_receive_limits(f, request, response, window,
	                                       BODY_MAX + 1024U);
}
static void init_fixture_compressed_receive(struct fixture *f,
                                            enum tr_rpc_cardinality request,
                                            enum tr_rpc_cardinality response)
{
	init_fixture_compressed_receive_window(f, request, response, 4096U);
}

static void
init_fixture_compressed_response_receive(struct fixture *f,
                                         enum tr_rpc_cardinality request,
                                         enum tr_rpc_cardinality response)
{
	int a, b;
	tcp_pair(&a, &b);
	init_peer(&f->client, a, 1, request, response, 4096U, 1U, 256U,
	          BODY_MAX + 1024U, BODY_MAX + 1024U, true,
	          TR_MESSAGE_ALGORITHM_ZSTD);
	init_peer(&f->server, b, 0, request, response, 4096U, 1U, 256U,
	          BODY_MAX + 1024U, BODY_MAX + 1024U, true,
	          TR_MESSAGE_ALGORITHM_ZSTD);
	for (size_t i = 0; i < 2U; ++i) {
		struct peer *peer = i == 0U ? &f->client : &f->server;
		peer->config.method.request_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
		peer->config.method.response_compression_algorithms =
		        TR_RPC_COMPRESSION_MASK_ZSTD;
	}
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
static void open_calls(struct fixture *f, unsigned count)
{
	for (unsigned i = 0; i < count; ++i) {
		struct tr_stream_handle stream;
		CHECK(tr_stream_open(f->client.channel, TR_LANE_BULK,
		                     &stream) == TR_OK);
	}
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
	const uint8_t *expected_transform_input;
	size_t expected_transform_input_size;
	int ret;
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

static int send_compressed_owner(void *arg)
{
	struct send_request *request = arg;
	struct peer *peer = request->c->peer;
	peer->expected_transform_input = request->expected_transform_input;
	peer->expected_transform_input_size =
	        request->expected_transform_input_size;
	peer->check_transform_input = request->expected_transform_input != NULL;
	request->ret = tr_rpc_record_stream_send_compressed_on_owner(
	        request->c->call, &request->h, request->md, &request->body);
	peer->check_transform_input = 0;
	peer->expected_transform_input = NULL;
	peer->expected_transform_input_size = 0U;
	return request->ret;
}

static void send_compressed_until(struct send_request *request)
{
	uint64_t end = monotonic_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(request->c->peer->reactor,
	                              send_compressed_owner, request)) ==
	       TR_AGAIN) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(ret == TR_OK && !request->body);
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

static int close_peer_message_service_on_owner(void *arg)
{
	struct peer *peer = arg;
	return peer->message_service ? tr_message_service_close_on_owner(
	                                       &peer->message_service)
	                             : TR_OK;
}

static void destroy_fixture(struct fixture *f)
{
	struct peer *peers[] = {&f->client, &f->server};
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_call(peers[i]->reactor, cleanup_owner,
		                      peers[i]) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		int ret;
		do {
			ret = tr_reactor_call(
			        peers[i]->reactor,
			        close_peer_message_service_on_owner, peers[i]);
			if (ret == TR_AGAIN)
				sched_yield();
		} while (ret == TR_AGAIN);
		CHECK(ret == TR_OK);
	}
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

static size_t encode_zstd_message(uint8_t *wire, size_t wire_capacity,
                                  const uint8_t *source, size_t source_size,
                                  int level)
{
	uint8_t encoded[ZSTD_compressBound(BODY_MAX)];
	struct tr_rpc_record_header header = head(TR_RPC_RECORD_MESSAGE);
	size_t encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                                    source_size, level);
	CHECK(!ZSTD_isError(encoded_size) && encoded_size != 0U);
	header.compression = TR_RPC_RECORD_ZSTD;
	header.encoded_size = (uint32_t)encoded_size;
	header.decoded_size = (uint32_t)source_size;
	CHECK(wire_capacity >= TR_RPC_RECORD_HEADER_SIZE + encoded_size);
	return encode(wire, header, NULL, encoded);
}

struct complete_held_transform_request {
	struct peer *peer;
	const uint8_t *body;
	size_t body_size;
	uint32_t slot;
};

struct complete_held_slot_request {
	struct peer *peer;
	uint32_t slot;
};

static int hold_peer_message_transforms_on_owner(void *arg);
static int complete_held_transform_success_on_owner(void *arg);

struct stream_event_request {
	struct call_context *call;
	enum tr_stream_event event;
	int status;
};

struct receive_wire_request {
	struct call_context *call;
	const uint8_t *bytes;
	size_t size;
	int ret;
};

static int receive_wire_on_owner(void *arg)
{
	struct receive_wire_request *request = arg;
	request->ret = tr_rpc_record_stream_receive_on_owner(
	        request->call->call, request->call->stream, request->bytes,
	        request->size);
	return request->ret;
}

static int stream_event_on_owner(void *arg)
{
	struct stream_event_request *request = arg;
	return tr_rpc_record_stream_event_on_owner(
	        request->call->call, request->call->stream, request->event,
	        request->status);
}

static void test_compressed_receive_close_waits_for_final_decode(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[4096];
	uint8_t message_wire[TR_RPC_RECORD_HEADER_SIZE +
	                     ZSTD_compressBound(sizeof(source))];
	uint8_t initial_wire[TR_RPC_RECORD_HEADER_SIZE];
	uint8_t final_wire[TR_RPC_RECORD_HEADER_SIZE];
	uint8_t combined[sizeof(message_wire) + sizeof(final_wire)];
	struct complete_held_transform_request complete;
	struct stream_event_request close_event;
	struct receive_wire_request receive;
	size_t message_size, initial_size, final_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 29U);
	init_fixture_compressed_response_receive(&f, TR_RPC_MANY, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1U);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	send_kind(client, TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&server->seen[TR_RPC_RECORD_HALF_CLOSE], 1U);

	initial_size = encode(initial_wire,
	                      head(TR_RPC_RECORD_INITIAL_METADATA), NULL, NULL);
	receive = (struct receive_wire_request){client, initial_wire,
	                                        initial_size, TR_ERR_STATE};
	CHECK(tr_reactor_call(f.client.reactor, receive_wire_on_owner,
	                      &receive) == TR_OK);
	CHECK(receive.ret == TR_OK);
	message_size = encode_zstd_message(message_wire, sizeof(message_wire),
	                                   source, sizeof(source), 3);
	final_size = encode(final_wire, head(TR_RPC_RECORD_FINAL_STATUS), NULL,
	                    NULL);
	memcpy(combined, message_wire, message_size);
	memcpy(combined + message_size, final_wire, final_size);
	receive = (struct receive_wire_request){
	        client, combined, message_size + final_size, TR_ERR_STATE};
	CHECK(tr_reactor_call(f.client.reactor, receive_wire_on_owner,
	                      &receive) == TR_OK);
	CHECK(receive.ret == TR_OK);
	wait_counter(&f.client.held_submissions, 1U);

	/* CLOSE follows DATA on the stream, but async decode has not committed
	 * the queued compressed MESSAGE or the FINAL that follows it. */
	close_event = (struct stream_event_request){
	        client, TR_STREAM_EVENT_REMOTE_CLOSED, TR_OK};
	CHECK(tr_reactor_call(f.client.reactor, stream_event_on_owner,
	                      &close_event) == TR_OK);
	CHECK(atomic_load(&client->terminals) == 0U);
	CHECK(!f.client.held_transforms[0].cancelled);

	complete = (struct complete_held_transform_request){&f.client, source,
	                                                    sizeof(source), 0U};
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	wait_counter(&client->terminals, 1U);
	CHECK(client->final_status == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&client->seen[TR_RPC_RECORD_MESSAGE]) == 1U);
	CHECK(atomic_load(&client->seen[TR_RPC_RECORD_FINAL_STATUS]) == 1U);
	destroy_fixture(&f);
}

static void test_compressed_receive_delivers_decoded_body(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[4096];
	uint8_t encoded[ZSTD_compressBound(sizeof(source))];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE + sizeof(encoded)];
	struct tr_rpc_record_header header;
	size_t encoded_size, wire_size;
	uint64_t end;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 23U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                             sizeof(source), 3);
	CHECK(!ZSTD_isError(encoded_size) && encoded_size != 0U);
	header = head(TR_RPC_RECORD_MESSAGE);
	header.compression = TR_RPC_RECORD_ZSTD;
	header.encoded_size = (uint32_t)encoded_size;
	header.decoded_size = sizeof(source);
	wire_size = encode(wire, header, NULL, encoded);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) &&
	       !atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 1U);
	CHECK(server->last_body == sizeof(source));
	CHECK(server->checksum == sum(source, sizeof(source)));
	CHECK(server->direct_bodies == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_crosses_small_stream_window(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[12288];
	_Atomic unsigned released = 0;
	struct tr_buffer *body;
	struct send_request send = {0};
	uint64_t end;

	uint32_t state = 0x6d2b79f5U;
	for (size_t i = 0; i < sizeof(source); ++i) {
		state ^= state << 13;
		state ^= state >> 17;
		state ^= state << 5;
		source[i] = (uint8_t)state;
	}
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	body = make_buffer(source, sizeof(source), &released);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = body;
	send_compressed_until(&send);
	CHECK(atomic_load(&released) == 1U);
	end = monotonic_ms() + 10000;
	while (!atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) &&
	       !atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 1U);
	CHECK(server->message_compression[0] == TR_RPC_RECORD_ZSTD);
	CHECK(server->message_encoded_sizes[0] != sizeof(source));
	CHECK(server->message_sizes[0] == sizeof(source));
	CHECK(server->message_checksums[0] == sum(source, sizeof(source)));
	CHECK(server->direct_bodies == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_policy_rejection_retains_body(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[32];
	_Atomic unsigned released = 0;
	struct send_request send = {0};
	struct tr_buffer *original;

	memset(source, 0x5a, sizeof(source));
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096U);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), &released);
	original = send.body;
	CHECK(tr_reactor_call(client->peer->reactor, send_compressed_owner,
	                      &send) == TR_ERR_UNSUPPORTED);
	CHECK(send.body == original && atomic_load(&released) == 0U);

	/* 未获准的压缩尝试不能提交 MESSAGE 状态；同一正文仍可按 NONE 发送。 */
	send.h.compression = TR_RPC_RECORD_NONE;
	send.h.encoded_size = sizeof(source);
	send_until(&send);
	wait_counter(&released, 1);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 1);
	CHECK(server->message_compression[0] == TR_RPC_RECORD_NONE);
	CHECK(server->message_sizes[0] == sizeof(source));
	destroy_fixture(&f);
}

static void test_compressed_send_encoded_limit_emits_no_partial_record(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[1024];
	_Atomic unsigned released = 0;
	struct send_request send = {0};
	uint32_t state = 0x6d2b79f5U;

	for (size_t i = 0; i < sizeof(source); ++i) {
		state ^= state << 13;
		state ^= state >> 17;
		state ^= state << 5;
		source[i] = (uint8_t)state;
	}
	/* 对端只允许 64 wire bytes，扣掉 envelope 后不够容纳此正文的帧。 */
	init_fixture_compressed_receive_limits(&f, TR_RPC_ONE, TR_RPC_ONE,
	                                       4096U, 64U);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), &released);
	send_compressed_until(&send);
	CHECK(atomic_load(&released) == 1U);
	wait_counter(&client->terminals, 1);
	CHECK(client->final_status == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_c_less_than_window(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[1024];
	struct send_request send = {0};

	memset(source, 'c', sizeof(source));
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	send.c = &f.client.calls[0];
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	send_compressed_until(&send);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 1);
	CHECK(TR_RPC_RECORD_HEADER_SIZE + server->message_encoded_sizes[0] <
	      4096U);
	CHECK(server->message_sizes[0] == sizeof(source));
	CHECK(server->message_checksums[0] == sum(source, sizeof(source)));
	destroy_fixture(&f);
}

static void test_compressed_send_c_equals_window(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[128];
	uint8_t encoded[ZSTD_compressBound(sizeof(source))];
	_Atomic unsigned released = 0;
	struct send_request send = {0};
	struct tr_stream_diagnostics diagnostics;
	size_t encoded_size;
	ZSTD_CCtx *zstd;
	uint64_t record_size, end;

	memset(source, 0x6b, sizeof(source));
	zstd = ZSTD_createCCtx();
	CHECK(zstd != NULL);
	CHECK(!ZSTD_isError(
	        ZSTD_CCtx_setParameter(zstd, ZSTD_c_compressionLevel, 3)));
	CHECK(!ZSTD_isError(
	        ZSTD_CCtx_setParameter(zstd, ZSTD_c_windowLog, 20)));
	CHECK(!ZSTD_isError(ZSTD_CCtx_setParameter(zstd, ZSTD_c_nbWorkers, 0)));
	CHECK(!ZSTD_isError(
	        ZSTD_CCtx_setParameter(zstd, ZSTD_c_checksumFlag, 1)));
	CHECK(!ZSTD_isError(ZSTD_CCtx_setPledgedSrcSize(zstd, sizeof(source))));
	encoded_size = ZSTD_compress2(zstd, encoded, sizeof(encoded), source,
	                              sizeof(source));
	CHECK(!ZSTD_isError(ZSTD_freeCCtx(zstd)));
	CHECK(!ZSTD_isError(encoded_size) && encoded_size != 0U);
	record_size = TR_RPC_RECORD_HEADER_SIZE + encoded_size;
	CHECK(record_size > TR_RPC_RECORD_HEADER_SIZE && record_size < 256U);
	init_fixture_compressed_receive_window(&f, TR_RPC_ONE, TR_RPC_ONE,
	                                       record_size);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	end = monotonic_ms() + 5000;
	do {
		CHECK(tr_stream_get_diagnostics(client->stream, &diagnostics) ==
		      TR_OK);
		if (diagnostics.flow.tx_send_limit >=
		    diagnostics.flow.tx_sent_bytes + record_size)
			break;
		CHECK(monotonic_ms() < end);
		sched_yield();
	} while (true);
	CHECK(diagnostics.flow.tx_send_limit - diagnostics.flow.tx_sent_bytes ==
	      record_size);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), &released);
	send_compressed_until(&send);
	CHECK(atomic_load(&released) == 1U);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 1);
	CHECK(TR_RPC_RECORD_HEADER_SIZE + server->message_encoded_sizes[0] ==
	      record_size);
	CHECK(record_size ==
	      diagnostics.flow.tx_send_limit - diagnostics.flow.tx_sent_bytes);
	CHECK(server->message_sizes[0] == sizeof(source));
	CHECK(server->message_checksums[0] == sum(source, sizeof(source)));
	destroy_fixture(&f);
}

static void test_compressed_receive_preserves_record_order(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t first[8192], second[8192];
	uint8_t first_wire[TR_RPC_RECORD_HEADER_SIZE +
	                   ZSTD_compressBound(sizeof(first))];
	uint8_t second_wire[TR_RPC_RECORD_HEADER_SIZE +
	                    ZSTD_compressBound(sizeof(second))];
	uint8_t combined[sizeof(first_wire) + sizeof(second_wire)];
	size_t first_size, second_size;
	uint64_t end;

	for (size_t i = 0; i < sizeof(first); ++i) {
		first[i] = (uint8_t)(i % 19U);
		second[i] = (uint8_t)(200U - (i % 17U));
	}
	first[0] = 0x31U;
	second[0] = 0x92U;
	init_fixture_compressed_receive(&f, TR_RPC_MANY, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.server.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.server) == TR_OK);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	first_size = encode_zstd_message(first_wire, sizeof(first_wire), first,
	                                 sizeof(first), 3);
	second_size = encode_zstd_message(second_wire, sizeof(second_wire),
	                                  second, sizeof(second), 7);
	memcpy(combined, first_wire, first_size);
	memcpy(combined + first_size, second_wire, second_size);
	raw_send(&f.client.calls[0], combined, first_size + second_size);
	end = monotonic_ms() + 5000;
	while (atomic_load(&f.server.held_submissions) < 2U) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	struct complete_held_transform_request complete = {&f.server, second,
	                                                   sizeof(second), 1U};
	CHECK(tr_reactor_call(f.server.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	complete = (struct complete_held_transform_request){&f.server, first,
	                                                    sizeof(first), 0U};
	CHECK(tr_reactor_call(f.server.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 2);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 2U);
	CHECK(server->message_first_byte[0] == first[0]);
	CHECK(server->message_first_byte[1] == second[0]);
	CHECK(server->message_checksums[0] == sum(first, sizeof(first)));
	CHECK(server->message_checksums[1] == sum(second, sizeof(second)));
	CHECK(server->message_sizes[0] == sizeof(first));
	CHECK(server->message_sizes[1] == sizeof(second));
	destroy_fixture(&f);
}

static void test_compressed_receive_rejects_invalid_zstd(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t malformed[32];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE + sizeof(malformed)];
	struct tr_rpc_record_header header = head(TR_RPC_RECORD_MESSAGE);
	size_t wire_size;
	uint64_t end;

	memset(malformed, 0xa5, sizeof(malformed));
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	header.compression = TR_RPC_RECORD_ZSTD;
	header.encoded_size = sizeof(malformed);
	header.decoded_size = 4096U;
	wire_size = encode(wire, header, NULL, malformed);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(server->final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	destroy_fixture(&f);
}

static void test_compressed_receive_rejects_trailing_and_size_mismatch(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[2048], encoded[ZSTD_compressBound(sizeof(source))];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE + sizeof(encoded) + 1U];
	struct tr_rpc_record_header header = head(TR_RPC_RECORD_MESSAGE);
	size_t encoded_size, wire_size;
	uint64_t end;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 29U);
	encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                             sizeof(source), 3);
	CHECK(!ZSTD_isError(encoded_size));
	encoded[encoded_size] = 0xa5U;
	header.compression = TR_RPC_RECORD_ZSTD;
	header.encoded_size = (uint32_t)(encoded_size + 1U);
	header.decoded_size = sizeof(source);
	wire_size = encode(wire, header, NULL, encoded);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(server->final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	destroy_fixture(&f);

	header.encoded_size = (uint32_t)(encoded_size - 1U);
	header.decoded_size = sizeof(source);
	wire_size = encode(wire, header, NULL, encoded);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(server->final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	destroy_fixture(&f);

	header.encoded_size = (uint32_t)encoded_size;
	header.decoded_size = sizeof(source) - 1U;
	wire_size = encode(wire, header, NULL, encoded);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&server->terminals)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(server->final_status == TR_RPC_STATUS_INVALID_ARGUMENT);
	destroy_fixture(&f);
}

static int hold_peer_message_transforms_on_owner(void *arg)
{
	struct peer *peer = arg;
	peer->hold_message_transforms = 1;
	return TR_OK;
}

struct abort_call_request {
	struct call_context *call;
	enum tr_rpc_status status;
};

static int abort_call_on_owner(void *arg)
{
	struct abort_call_request *request = arg;
	return tr_rpc_record_stream_abort_on_owner(request->call->call,
	                                           request->status);
}

static int complete_held_transform_on_owner(void *arg)
{
	struct peer *peer = arg;
	struct held_transform *held = &peer->held_transforms[0];
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	if (!held->pending || !held->cancelled)
		return TR_ERR_STATE;
	done = held->done;
	done_arg = held->arg;
	handle = held->handle;
	held->pending = 0;
	held->done = NULL;
	held->arg = NULL;
	done(done_arg, handle,
	     (struct tr_message_service_result){TR_MESSAGE_TRANSFORM_CANCELLED,
	                                        TR_MESSAGE_SERVICE_ON_OWNER},
	     NULL);
	return TR_OK;
}

static int complete_held_cancelled_slot_on_owner(void *arg)
{
	struct complete_held_slot_request *request = arg;
	struct peer *peer = request->peer;
	struct held_transform *held;
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	if (request->slot >= 4U)
		return TR_ERR_INVALID;
	held = &peer->held_transforms[request->slot];
	if (!held->pending || !held->cancelled)
		return TR_ERR_STATE;
	done = held->done;
	done_arg = held->arg;
	handle = held->handle;
	held->pending = 0;
	held->done = NULL;
	held->arg = NULL;
	done(done_arg, handle,
	     (struct tr_message_service_result){TR_MESSAGE_TRANSFORM_CANCELLED,
	                                        TR_MESSAGE_SERVICE_ON_OWNER},
	     NULL);
	return TR_OK;
}

static int complete_held_transform_owner_stopped(void *arg)
{
	struct peer *peer = arg;
	struct held_transform *held = &peer->held_transforms[0];
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	if (!held->pending)
		return TR_ERR_STATE;
	done = held->done;
	done_arg = held->arg;
	handle = held->handle;
	held->pending = 0;
	held->done = NULL;
	held->arg = NULL;
	done(done_arg, handle,
	     (struct tr_message_service_result){
	             TR_MESSAGE_TRANSFORM_CANCELLED,
	             TR_MESSAGE_SERVICE_OWNER_STOPPED},
	     NULL);
	return TR_OK;
}

static int complete_held_transform_success_on_owner(void *arg)
{
	struct complete_held_transform_request *request = arg;
	struct peer *peer = request->peer;
	struct held_transform *held;
	struct tr_message_builder *builder = NULL;
	struct tr_message message = {0};
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	int ret;
	if (request->slot >= 4U)
		return TR_ERR_INVALID;
	held = &peer->held_transforms[request->slot];
	if (!held->pending || held->cancelled)
		return TR_ERR_STATE;
	done = held->done;
	done_arg = held->arg;
	handle = held->handle;
	held->pending = 0;
	held->done = NULL;
	held->arg = NULL;
	ret = tr_message_builder_create(&peer->budget, request->body_size,
	                                &builder);
	if (ret == TR_OK)
		ret = tr_message_builder_append(builder, request->body,
		                                request->body_size);
	if (ret == TR_OK)
		ret = tr_message_builder_publish(&builder, &message);
	if (builder)
		(void)tr_message_builder_cancel(&builder);
	if (ret != TR_OK)
		return ret;
	done(done_arg, handle,
	     (struct tr_message_service_result){TR_MESSAGE_TRANSFORM_OK,
	                                        TR_MESSAGE_SERVICE_ON_OWNER},
	     &message);
	return tr_message_release(&message);
}

static void test_compressed_send_cancel_waits_for_completion(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[1024];
	struct send_request send = {0};
	struct abort_call_request abort_request;

	memset(source, 0x3c, sizeof(source));
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1);
	abort_request =
	        (struct abort_call_request){client, TR_RPC_STATUS_CANCELLED};
	CHECK(tr_reactor_call(f.client.reactor, abort_call_on_owner,
	                      &abort_request) == TR_OK);
	CHECK(atomic_load(&f.client.held_cancellations) == 1U);
	CHECK(atomic_load(&client->terminals) == 0U);
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_on_owner,
	                      &f.client) == TR_OK);
	wait_counter(&client->terminals, 1);
	CHECK(client->final_status == TR_RPC_STATUS_CANCELLED);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_queue_full_preserves_independent_streams(void)
{
	struct fixture f;
	uint8_t source[CALLS][1024];
	struct send_request sends[CALLS] = {0};
	struct abort_call_request abort_request;
	struct complete_held_slot_request complete_request;
	_Atomic unsigned released = 0;

	for (size_t i = 0; i < CALLS; ++i)
		for (size_t j = 0; j < sizeof(source[i]); ++j)
			source[i][j] = (uint8_t)(i * 31U + j * 17U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	for (size_t i = 0; i < CALLS; ++i) {
		struct tr_stream_handle stream;
		CHECK(tr_stream_open(f.client.channel, TR_LANE_BULK, &stream) ==
		      TR_OK);
	}
	wait_counter(&f.client.opened, CALLS);
	wait_counter(&f.server.opened, CALLS);

	for (size_t i = 0; i < CALLS; ++i) {
		send_kind(&f.client.calls[i], TR_RPC_RECORD_OPEN);
		wait_counter(&f.server.calls[i].seen[TR_RPC_RECORD_OPEN], 1U);
		sends[i].c = &f.client.calls[i];
		sends[i].h = head(TR_RPC_RECORD_MESSAGE);
		sends[i].h.compression = TR_RPC_RECORD_ZSTD;
		sends[i].h.decoded_size = sizeof(source[i]);
		sends[i].body =
		        make_buffer(source[i], sizeof(source[i]), &released);
	}

	/* 四个独立 Stream 各占一个变换槽；第五个提交失败必须保留正文。 */
	for (size_t i = 0; i < 4U; ++i) {
		CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner,
		                      &sends[i]) == TR_OK);
		CHECK(sends[i].body == NULL);
	}
	wait_counter(&f.client.held_submissions, 4U);
	CHECK(atomic_load(&released) == 4U);
	struct tr_buffer *fifth_body = sends[4].body;
	CHECK(fifth_body != NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner,
	                      &sends[4]) == TR_AGAIN);
	CHECK(sends[4].body == fifth_body);
	CHECK(atomic_load(&released) == 4U);

	for (size_t i = 0; i < 4U; ++i) {
		abort_request = (struct abort_call_request){
		        &f.client.calls[i], TR_RPC_STATUS_CANCELLED};
		CHECK(tr_reactor_call(f.client.reactor, abort_call_on_owner,
		                      &abort_request) == TR_OK);
	}
	CHECK(atomic_load(&f.client.held_cancellations) == 4U);
	for (size_t i = 0; i < 4U; ++i) {
		complete_request = (struct complete_held_slot_request){
		        &f.client, (uint32_t)i};
		CHECK(tr_reactor_call(f.client.reactor,
		                      complete_held_cancelled_slot_on_owner,
		                      &complete_request) == TR_OK);
		wait_counter(&f.client.calls[i].terminals, 1U);
		CHECK(f.client.calls[i].final_status ==
		      TR_RPC_STATUS_CANCELLED);
		CHECK(atomic_load(
		              &f.server.calls[i].seen[TR_RPC_RECORD_MESSAGE]) ==
		      0U);
	}

	/* 队列有空位后重试同一条记录，证明失败未提交 RPC 状态。 */
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner,
	                      &sends[4]) == TR_OK);
	CHECK(sends[4].body == NULL);
	wait_counter(&f.client.held_submissions, 5U);
	CHECK(atomic_load(&released) == 5U);
	abort_request = (struct abort_call_request){&f.client.calls[4],
	                                            TR_RPC_STATUS_CANCELLED};
	CHECK(tr_reactor_call(f.client.reactor, abort_call_on_owner,
	                      &abort_request) == TR_OK);
	CHECK(atomic_load(&f.client.held_cancellations) == 5U);
	complete_request = (struct complete_held_slot_request){&f.client, 0U};
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_cancelled_slot_on_owner,
	                      &complete_request) == TR_OK);
	wait_counter(&f.client.calls[4].terminals, 1U);
	CHECK(f.client.calls[4].final_status == TR_RPC_STATUS_CANCELLED);
	CHECK(atomic_load(&f.server.calls[4].seen[TR_RPC_RECORD_MESSAGE]) ==
	      0U);
	destroy_fixture(&f);
}

static void test_compressed_send_independent_streams_complete_out_of_order(void)
{
	struct fixture f;
	uint8_t source[4][512];
	uint8_t encoded[4][ZSTD_compressBound(sizeof(source[0]))];
	size_t encoded_size[4];
	struct send_request sends[4] = {0};
	struct complete_held_transform_request complete;
	const unsigned completion_order[] = {2U, 0U, 3U, 1U};
	_Atomic unsigned released = 0;

	for (size_t i = 0; i < 4U; ++i) {
		for (size_t j = 0; j < sizeof(source[i]); ++j)
			source[i][j] = (uint8_t)(i * 37U + j % 23U);
		encoded_size[i] =
		        ZSTD_compress(encoded[i], sizeof(encoded[i]), source[i],
		                      sizeof(source[i]), 3);
		CHECK(!ZSTD_isError(encoded_size[i]));
	}
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_calls(&f, 4U);
	for (size_t i = 0; i < 4U; ++i) {
		send_kind(&f.client.calls[i], TR_RPC_RECORD_OPEN);
		wait_counter(&f.server.calls[i].seen[TR_RPC_RECORD_OPEN], 1U);
		sends[i].c = &f.client.calls[i];
		sends[i].h = head(TR_RPC_RECORD_MESSAGE);
		sends[i].h.compression = TR_RPC_RECORD_ZSTD;
		sends[i].h.decoded_size = sizeof(source[i]);
		sends[i].body =
		        make_buffer(source[i], sizeof(source[i]), &released);
		CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner,
		                      &sends[i]) == TR_OK);
		CHECK(sends[i].body == NULL);
	}
	wait_counter(&f.client.held_submissions, 4U);
	CHECK(atomic_load(&released) == 4U);

	/* 独立 Stream 的完成可乱序；每条仍应只交付自己的完整正文。 */
	for (size_t i = 0; i < 4U; ++i) {
		unsigned slot = completion_order[i];
		complete = (struct complete_held_transform_request){
		        &f.client, encoded[slot], encoded_size[slot], slot};
		CHECK(tr_reactor_call(f.client.reactor,
		                      complete_held_transform_success_on_owner,
		                      &complete) == TR_OK);
		wait_counter(&f.server.calls[slot].seen[TR_RPC_RECORD_MESSAGE],
		             1U);
		CHECK(f.server.calls[slot].message_sizes[0] ==
		      sizeof(source[slot]));
		CHECK(f.server.calls[slot].message_checksums[0] ==
		      sum(source[slot], sizeof(source[slot])));
		CHECK(f.server.calls[slot].message_compression[0] ==
		      TR_RPC_RECORD_ZSTD);
		CHECK(f.server.calls[slot].message_encoded_sizes[0] ==
		      encoded_size[slot]);
	}
	destroy_fixture(&f);
}

static void
test_compressed_transform_receives_only_body_with_auth_metadata(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[256];
	uint8_t encoded[ZSTD_compressBound(sizeof(source))];
	static const uint8_t metadata[] = {
	        4U,  12U, 0U,  'a', 'u', 't', 'h', 'A', 'U', 'T', 'H',
	        '-', 'C', 'R', 'E', 'D', '-', '0', '1', 9U,  10U, 0U,
	        'x', '-', 'p', 'e', 'e', 'r', '-', 'i', 'd', 'P', 'E',
	        'E', 'R', '-', 'I', 'D', '-', '4', '2'};
	struct send_request open = {0};
	struct send_request send = {0};
	struct complete_held_transform_request complete;
	size_t encoded_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 17U);
	encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                             sizeof(source), 3);
	CHECK(!ZSTD_isError(encoded_size));
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1U);
	server = &f.server.calls[0];
	open.c = &f.client.calls[0];
	open.h = head(TR_RPC_RECORD_OPEN);
	open.h.metadata_size = sizeof(metadata);
	open.md = metadata;
	send_until(&open);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	CHECK(server->last_metadata_size == sizeof(metadata));
	CHECK(memcmp(server->last_metadata, metadata, sizeof(metadata)) == 0);
	send.c = &f.client.calls[0];
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	send.expected_transform_input = source;
	send.expected_transform_input_size = sizeof(source);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	complete = (struct complete_held_transform_request){&f.client, encoded,
	                                                    encoded_size, 0U};
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 1U);
	CHECK(server->message_sizes[0] == sizeof(source));
	CHECK(server->message_checksums[0] == sum(source, sizeof(source)));
	CHECK(server->last_metadata_size == 0U);
	destroy_fixture(&f);
}

static void test_compressed_receive_preserves_interleaved_tx_state(void)
{
	struct fixture f;
	struct call_context *server, *client;
	uint8_t request_body[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(request_body))];
	size_t wire_size;
	struct complete_held_transform_request complete;
	struct snapshot server_state;

	for (size_t i = 0; i < sizeof(request_body); ++i)
		request_body[i] = (uint8_t)(i % 13U);
	init_fixture_compressed_receive(&f, TR_RPC_MANY, TR_RPC_MANY);
	CHECK(tr_reactor_call(f.server.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.server) == TR_OK);
	open_call(&f, 1);
	server = &f.server.calls[0];
	client = &f.client.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	wire_size = encode_zstd_message(wire, sizeof(wire), request_body,
	                                sizeof(request_body), 3);
	raw_send(client, wire, wire_size);
	uint64_t end = monotonic_ms() + 5000;
	while (!atomic_load(&f.server.held_submissions)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	send_kind(server, TR_RPC_RECORD_INITIAL_METADATA);
	wait_counter(&client->seen[TR_RPC_RECORD_INITIAL_METADATA], 1);
	send_kind(server, TR_RPC_RECORD_MESSAGE);
	wait_counter(&client->seen[TR_RPC_RECORD_MESSAGE], 1);
	complete = (struct complete_held_transform_request){
	        &f.server, request_body, sizeof(request_body), 0U};
	CHECK(tr_reactor_call(f.server.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	wait_counter(&server->seen[TR_RPC_RECORD_MESSAGE], 1);
	server_state = snapshot(server);
	CHECK(server_state.state.request_messages == 1U);
	CHECK(server_state.state.response_headers == 1U);
	CHECK(server_state.state.response_messages == 1U);
	CHECK(server->checksum == sum(request_body, sizeof(request_body)));
	destroy_fixture(&f);
}

static void test_compressed_receive_abort_waits_for_cancel_completion(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(source))];
	size_t wire_size;
	uint64_t end;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 11U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.server.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.server) == TR_OK);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	wire_size = encode_zstd_message(wire, sizeof(wire), source,
	                                sizeof(source), 3);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&f.server.held_submissions)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	struct abort_call_request abort_request = {server,
	                                           TR_RPC_STATUS_CANCELLED};
	CHECK(tr_reactor_call(f.server.reactor, abort_call_on_owner,
	                      &abort_request) == TR_OK);
	CHECK(atomic_load(&f.server.held_cancellations) == 1U);
	CHECK(atomic_load(&server->terminals) == 0U);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(tr_reactor_call(f.server.reactor,
	                      complete_held_transform_on_owner,
	                      &f.server) == TR_OK);
	wait_counter(&server->terminals, 1);
	CHECK(server->final_status == TR_RPC_STATUS_CANCELLED);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

struct reset_stream_request {
	struct call_context *call;
	int status;
};

static int reset_stream_on_owner(void *arg)
{
	struct reset_stream_request *request = arg;
	return tr_stream_reset_on_owner(request->call->stream, request->status);
}

static int abort_bulk_lane_on_owner(void *arg)
{
	struct peer *peer = arg;
	return tr_channel_abort_lane_on_owner(peer->channel, TR_LANE_BULK,
	                                      TR_ERR_CLOSED);
}

struct message_limits_probe {
	struct peer *peer;
	struct tr_channel_message_limits limits;
	int result;
};

static int probe_message_limits_on_owner(void *arg)
{
	struct message_limits_probe *probe = arg;
	probe->result = tr_channel_message_limits_on_owner(
	        probe->peer->channel, TR_LANE_BULK, &probe->limits);
	return TR_OK;
}

static void wait_channel_lane_down(struct peer *peer, enum tr_lane lane)
{
	uint64_t end = monotonic_ms() + 5000U;
	for (;;) {
		enum tr_channel_lane_state state;
		CHECK(tr_channel_get_lane_state(peer->channel, lane, &state) ==
		      TR_OK);
		if (state == TR_CHANNEL_LANE_DOWN)
			return;
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
}

static void replace_shared_fixture_connection(struct fixture *fixture)
{
	int client_fd, server_fd;
	struct tr_conn_handle client_connection, server_connection;
	tcp_pair(&client_fd, &server_fd);
	CHECK(tr_reactor_adopt_fd(fixture->client.reactor, client_fd,
	                          &client_connection) == TR_OK);
	CHECK(tr_reactor_adopt_fd(fixture->server.reactor, server_fd,
	                          &server_connection) == TR_OK);
	CHECK(tr_channel_replace_connection(fixture->client.channel,
	                                    TR_LANE_CONTROL,
	                                    client_connection) == TR_OK);
	CHECK(tr_channel_replace_connection(fixture->server.channel,
	                                    TR_LANE_CONTROL,
	                                    server_connection) == TR_OK);
	CHECK(tr_channel_wait_ready(fixture->client.channel, TR_LANE_BULK,
	                            10000U) == TR_OK);
	CHECK(tr_channel_wait_ready(fixture->server.channel, TR_LANE_BULK,
	                            10000U) == TR_OK);
}

static int complete_held_transform_success_after_cancel_on_owner(void *arg)
{
	struct complete_held_transform_request *request = arg;
	struct peer *peer = request->peer;
	struct held_transform *held;
	struct tr_message_builder *builder = NULL;
	struct tr_message message = {0};
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	int ret;
	if (request->slot >= 4U)
		return TR_ERR_INVALID;
	held = &peer->held_transforms[request->slot];
	if (!held->pending || !held->cancelled)
		return TR_ERR_STATE;
	done = held->done;
	done_arg = held->arg;
	handle = held->handle;
	held->pending = 0;
	held->done = NULL;
	held->arg = NULL;
	ret = tr_message_builder_create(&peer->budget, request->body_size,
	                                &builder);
	if (ret == TR_OK)
		ret = tr_message_builder_append(builder, request->body,
		                                request->body_size);
	if (ret == TR_OK)
		ret = tr_message_builder_publish(&builder, &message);
	if (builder)
		(void)tr_message_builder_cancel(&builder);
	if (ret != TR_OK)
		return ret;
	done(done_arg, handle,
	     (struct tr_message_service_result){TR_MESSAGE_TRANSFORM_OK,
	                                        TR_MESSAGE_SERVICE_ON_OWNER},
	     &message);
	return tr_message_release(&message);
}

struct destroy_call_request {
	struct call_context *call;
	int result;
};

static int destroy_call_on_owner(void *arg)
{
	struct destroy_call_request *request = arg;
	request->result =
	        tr_rpc_record_stream_destroy_on_owner(&request->call->call);
	return TR_OK;
}

static int hold_peer_data_on_owner(void *arg)
{
	((struct peer *)arg)->hold = 1;
	return TR_OK;
}

static void
test_compressed_send_replacement_handshake_retires_old_transform(void)
{
	struct fixture f;
	struct call_context *client, *server;
	struct message_limits_probe probe = {0};
	uint8_t source[4096];
	uint8_t encoded[ZSTD_compressBound(sizeof(source))];
	struct send_request send = {0};
	struct complete_held_transform_request complete;
	size_t encoded_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 29U);
	encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                             sizeof(source), 3);
	CHECK(!ZSTD_isError(encoded_size));
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1U);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1U);

	CHECK(tr_reactor_call(f.client.reactor, abort_bulk_lane_on_owner,
	                      &f.client) == TR_OK);
	wait_counter(&f.client.held_cancellations, 1U);
	wait_channel_lane_down(&f.client, TR_LANE_BULK);
	wait_channel_lane_down(&f.server, TR_LANE_BULK);
	probe.peer = &f.client;
	CHECK(tr_reactor_call(f.client.reactor, probe_message_limits_on_owner,
	                      &probe) == TR_OK);
	CHECK(probe.result == TR_AGAIN);

	replace_shared_fixture_connection(&f);
	probe = (struct message_limits_probe){.peer = &f.client};
	CHECK(tr_reactor_call(f.client.reactor, probe_message_limits_on_owner,
	                      &probe) == TR_OK);
	CHECK(probe.result == TR_OK);
	CHECK(probe.limits.peer_caps_ready);
	CHECK(probe.limits.peer_receive_algorithms & TR_MESSAGE_ALGORITHM_ZSTD);

	complete = (struct complete_held_transform_request){&f.client, encoded,
	                                                    encoded_size, 0U};
	CHECK(tr_reactor_call(
	              f.client.reactor,
	              complete_held_transform_success_after_cancel_on_owner,
	              &complete) == TR_OK);
	wait_counter(&client->terminals, 1U);
	CHECK(client->final_status == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_destroy_refuses_live_transform(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[12288];
	uint8_t encoded[ZSTD_compressBound(sizeof(source))];
	struct send_request send = {0};
	struct complete_held_transform_request complete;
	struct destroy_call_request destroy = {0};
	struct abort_call_request abort_request;
	size_t encoded_size;
	uint64_t bytes_before_completion;

	uint32_t random = UINT32_C(0x81a5f00d);
	for (size_t i = 0; i < sizeof(source); ++i) {
		random = random * UINT32_C(1664525) + UINT32_C(1013904223);
		source[i] = (uint8_t)(random >> 24);
	}
	encoded_size = ZSTD_compress(encoded, sizeof(encoded), source,
	                             sizeof(source), 3);
	CHECK(!ZSTD_isError(encoded_size));
	CHECK(encoded_size + TR_RPC_RECORD_HEADER_SIZE > 4096U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1U);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	CHECK(tr_reactor_call(f.server.reactor, hold_peer_data_on_owner,
	                      &f.server) == TR_OK);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1U);
	bytes_before_completion = atomic_load(&f.client.budget.current_bytes);
	complete = (struct complete_held_transform_request){&f.client, encoded,
	                                                    encoded_size, 0U};
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_success_on_owner,
	                      &complete) == TR_OK);
	CHECK(atomic_load(&f.client.budget.current_bytes) >
	      bytes_before_completion);

	destroy.call = client;
	CHECK(tr_reactor_call(f.client.reactor, destroy_call_on_owner,
	                      &destroy) == TR_OK);
	CHECK(destroy.result == TR_ERR_STATE);
	CHECK(client->call != NULL);

	abort_request =
	        (struct abort_call_request){client, TR_RPC_STATUS_CANCELLED};
	CHECK(tr_reactor_call(f.client.reactor, abort_call_on_owner,
	                      &abort_request) == TR_OK);
	wait_counter(&client->terminals, 1U);
	destroy.result = TR_ERR_STATE;
	CHECK(tr_reactor_call(f.client.reactor, destroy_call_on_owner,
	                      &destroy) == TR_OK);
	CHECK(destroy.result == TR_OK);
	CHECK(client->call == NULL);
	destroy_fixture(&f);
}

static void test_compressed_send_reset_retires_pending_transform(void)
{
	struct fixture f;
	struct call_context *client, *server;
	uint8_t source[4096];
	struct send_request send = {0};
	struct reset_stream_request reset;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 19U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1U);

	reset = (struct reset_stream_request){client, TR_ERR_CLOSED};
	CHECK(tr_reactor_call(f.client.reactor, reset_stream_on_owner,
	                      &reset) == TR_OK);
	CHECK(atomic_load(&f.client.held_cancellations) == 1U);
	CHECK(atomic_load(&client->terminals) == 0U);
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_on_owner,
	                      &f.client) == TR_OK);
	wait_counter(&client->terminals, 1U);
	CHECK(client->final_status == TR_RPC_STATUS_CANCELLED);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

static void test_compressed_send_lane_loss_retires_pending_transform(void)
{
	struct fixture f;
	struct call_context *client, *server;
	struct message_limits_probe probe = {0};
	uint8_t source[4096];
	struct send_request send = {0};

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 23U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1);
	client = &f.client.calls[0];
	server = &f.server.calls[0];
	send_kind(client, TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1U);
	send.c = client;
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1U);

	CHECK(tr_reactor_call(f.client.reactor, abort_bulk_lane_on_owner,
	                      &f.client) == TR_OK);
	wait_counter(&f.client.held_cancellations, 1U);
	probe.peer = &f.client;
	CHECK(tr_reactor_call(f.client.reactor, probe_message_limits_on_owner,
	                      &probe) == TR_OK);
	CHECK(probe.result == TR_AGAIN);
	CHECK(atomic_load(&client->terminals) == 0U);
	CHECK(tr_reactor_call(f.client.reactor,
	                      complete_held_transform_on_owner,
	                      &f.client) == TR_OK);
	wait_counter(&client->terminals, 1U);
	CHECK(client->final_status == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	destroy_fixture(&f);
}

static void test_compressed_receive_reset_retires_pending_stream(void)
{
	struct fixture f;
	struct call_context *server;
	uint8_t source[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(source))];
	struct reset_stream_request reset;
	size_t wire_size;
	uint64_t end;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 7U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.server.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.server) == TR_OK);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);
	wire_size = encode_zstd_message(wire, sizeof(wire), source,
	                                sizeof(source), 3);
	raw_send(&f.client.calls[0], wire, wire_size);
	end = monotonic_ms() + 5000;
	while (!atomic_load(&f.server.held_submissions)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	reset = (struct reset_stream_request){&f.client.calls[0],
	                                      TR_ERR_CLOSED};
	CHECK(tr_reactor_call(f.client.reactor, reset_stream_on_owner,
	                      &reset) == TR_OK);
	wait_counter(&f.server.held_cancellations, 1);
	CHECK(atomic_load(&server->terminals) == 0U);
	CHECK(tr_reactor_call(f.server.reactor,
	                      complete_held_transform_on_owner,
	                      &f.server) == TR_OK);
	wait_counter(&server->terminals, 1);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(server->final_status == TR_RPC_STATUS_CANCELLED);
	destroy_fixture(&f);
}

static int disable_peer_routes_on_owner(void *arg)
{
	((struct peer *)arg)->route_disabled = 1;
	return TR_OK;
}

static void test_compressed_receive_owner_stopped_reaps_pending_task(void)
{
	struct fixture f;
	struct peer *peers[] = {&f.client, &f.server};
	uint8_t source[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(source))];
	size_t wire_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 31U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.server.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.server) == TR_OK);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	wire_size = encode_zstd_message(wire, sizeof(wire), source,
	                                sizeof(source), 3);
	raw_send(&f.client.calls[0], wire, wire_size);
	uint64_t end = monotonic_ms() + 5000;
	while (!atomic_load(&f.server.held_submissions)) {
		CHECK(monotonic_ms() < end);
		sched_yield();
	}
	for (size_t i = 0; i < 2; ++i) {
		int ret;
		end = monotonic_ms() + 5000;
		do {
			ret = tr_reactor_call(
			        peers[i]->reactor,
			        close_peer_message_service_on_owner, peers[i]);
			if (ret == TR_AGAIN)
				sched_yield();
			CHECK(monotonic_ms() < end);
		} while (ret == TR_AGAIN);
		CHECK(ret == TR_OK);
	}
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_call(peers[i]->reactor,
		                      disable_peer_routes_on_owner,
		                      peers[i]) == TR_OK);
	for (size_t i = 0; i < 2; ++i)
		CHECK(tr_reactor_stop(peers[i]->reactor) == TR_OK);
	CHECK(complete_held_transform_owner_stopped(&f.server) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		CHECK(tr_rpc_record_stream_reap_stopped(
		              &peers[i]->calls[0].call) == TR_OK);
		CHECK(peers[i]->calls[0].delivery ==
		      TR_RPC_RECORD_OWNER_STOPPED);
		CHECK(atomic_load(&peers[i]->calls[0].terminals) == 1U);
	}
	CHECK(atomic_load(&f.server.calls[0].seen[TR_RPC_RECORD_MESSAGE]) ==
	      0U);
	for (size_t i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->reassembly) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0U);
	}
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

static int reset_quota_stream_on_owner(void *arg)
{
	struct call_context *call = arg;
	return tr_stream_reset_on_owner(call->stream, TR_ERR_NOMEM);
}

static void expect_channel_keepalive_progress(struct tr_channel *channel)
{
	struct tr_channel_keepalive_config keepalive = {10U, 5000U};
	struct tr_channel_stats stats;
	uint64_t pongs_before;

	CHECK(tr_channel_get_stats(channel, &stats) == TR_OK);
	pongs_before = stats.keepalive_pongs_received;
	CHECK(tr_channel_enable_keepalive(channel, &keepalive) == TR_OK);
	for (unsigned attempt = 0U; attempt < 3000U; ++attempt) {
		struct timespec pause = {0, 1000000L};
		CHECK(tr_channel_get_stats(channel, &stats) == TR_OK);
		if (stats.keepalive_pongs_received > pongs_before)
			break;
		(void)nanosleep(&pause, NULL);
	}
	CHECK(stats.keepalive_pings_sent > 0U);
	CHECK(stats.keepalive_pongs_received > pongs_before);
	CHECK(tr_channel_disable_keepalive(channel) == TR_OK);
}

static void test_receive_quota_preserves_peer(void)
{
	struct fixture f;
	struct quota quota = {0};
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE + 32U];
	uint8_t payload[32];
	struct tr_rpc_record_header h = head(TR_RPC_RECORD_MESSAGE);
	size_t wire_size;

	memset(payload, 0x5a, sizeof(payload));
	init_fixture(&f, TR_RPC_ONE, TR_RPC_ONE, 4096);
	open_call(&f, 1);
	open_call(&f, 2);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1);
	send_kind(&f.client.calls[1], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[1].seen[TR_RPC_RECORD_OPEN], 1);

	quota.peer = &f.server;
	quota.bytes = f.server.budget.limit_bytes -
	              atomic_load(&f.server.budget.current_bytes);
	CHECK(quota.bytes > 0U);
	CHECK(tr_memory_budget_reserve(&f.server.budget, quota.bytes) == TR_OK);

	/* 只送正文记录头，迫使接收端为后续正文预约缓存。 */
	h.encoded_size = h.decoded_size = sizeof(payload);
	wire_size = encode(wire, h, NULL, NULL);
	CHECK(wire_size == TR_RPC_RECORD_HEADER_SIZE);
	raw_send(&f.client.calls[0], wire, wire_size);
	wait_counter(&f.server.calls[0].terminals, 1);
	CHECK(f.server.calls[0].final_status ==
	      TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	/* 记录流只终结本地调用；由上层适配器发送 RESET 通知对端。 */
	CHECK(tr_reactor_call(f.server.reactor, reset_quota_stream_on_owner,
	                      &f.server.calls[0]) == TR_OK);
	wait_counter(&f.client.calls[0].terminals, 1);
	CHECK(f.client.calls[0].final_status ==
	      TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&f.server.calls[0].seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(snapshot(&f.server.calls[0]).cached == 0U);
	CHECK(snapshot(&f.client.calls[0]).cached == 0U);
	CHECK(!snapshot(&f.server.calls[1]).state.terminal);
	CHECK(!snapshot(&f.client.calls[1]).state.terminal);
	expect_channel_keepalive_progress(f.client.channel);

	CHECK(tr_memory_budget_release(&f.server.budget, quota.bytes) == TR_OK);

	/* 同一 Channel 上的另一条调用继续收发并正常完成。 */
	struct send_request request = {
	        .c = &f.client.calls[1],
	        .h = head(TR_RPC_RECORD_MESSAGE),
	        .body = make_buffer(payload, sizeof(payload), NULL)};
	request.h.encoded_size = request.h.decoded_size = sizeof(payload);
	send_until(&request);
	wait_counter(&f.server.calls[1].seen[TR_RPC_RECORD_MESSAGE], 1);
	send_kind(&f.client.calls[1], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&f.server.calls[1].seen[TR_RPC_RECORD_HALF_CLOSE], 1);
	send_kind(&f.server.calls[1], TR_RPC_RECORD_INITIAL_METADATA);
	send_kind(&f.server.calls[1], TR_RPC_RECORD_MESSAGE);
	send_kind(&f.server.calls[1], TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&f.server.calls[1].terminals, 1);
	wait_counter(&f.client.calls[1].terminals, 1);
	CHECK(f.server.calls[1].final_status == TR_RPC_STATUS_OK);
	CHECK(f.client.calls[1].final_status == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.server.calls[0].terminals) == 1U);
	CHECK(atomic_load(&f.client.calls[0].terminals) == 1U);
	CHECK(atomic_load(&f.server.calls[1].terminals) == 1U);
	CHECK(atomic_load(&f.client.calls[1].terminals) == 1U);
	CHECK(snapshot(&f.server.calls[1]).cached == 0U);
	CHECK(snapshot(&f.client.calls[1]).cached == 0U);
	destroy_fixture(&f);
}

static void test_compressed_receive_workspace_exhaustion(void)
{
	struct fixture f;
	struct quota quota = {0};
	struct call_context *server;
	uint8_t source[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(source))];
	size_t wire_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 23U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	server = &f.server.calls[0];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&server->seen[TR_RPC_RECORD_OPEN], 1);

	quota.peer = &f.server;
	quota.bytes = f.server.budget.limit_bytes -
	              atomic_load(&f.server.budget.current_bytes) -
	              512U * 1024U;
	CHECK(quota.bytes > 0U);
	CHECK(tr_memory_budget_reserve(&f.server.budget, quota.bytes) == TR_OK);
	wire_size = encode_zstd_message(wire, sizeof(wire), source,
	                                sizeof(source), 3);
	raw_send(&f.client.calls[0], wire, wire_size);
	wait_counter(&server->terminals, 1);
	CHECK(server->final_status == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&server->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(snapshot(server).state.request_messages == 0U);
	CHECK(tr_memory_budget_release(&f.server.budget, quota.bytes) == TR_OK);
	destroy_fixture(&f);
}

static void test_compressed_workspace_exhaustion_preserves_peer(void)
{
	struct fixture f;
	struct quota quota = {0};
	struct call_context *failed, *peer_server;
	struct send_request request = {0};
	struct tr_channel_stats client_stats, server_stats;
	uint8_t source[4096];
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE +
	             ZSTD_compressBound(sizeof(source))];
	size_t wire_size;

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 23U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	open_call(&f, 1);
	open_call(&f, 1);
	failed = &f.server.calls[0];
	peer_server = &f.server.calls[1];
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&failed->seen[TR_RPC_RECORD_OPEN], 1U);
	send_kind(&f.client.calls[1], TR_RPC_RECORD_OPEN);
	wait_counter(&peer_server->seen[TR_RPC_RECORD_OPEN], 1U);

	quota.peer = &f.server;
	quota.bytes = f.server.budget.limit_bytes -
	              atomic_load(&f.server.budget.current_bytes) -
	              512U * 1024U;
	CHECK(quota.bytes > 0U);
	CHECK(tr_memory_budget_reserve(&f.server.budget, quota.bytes) == TR_OK);
	wire_size = encode_zstd_message(wire, sizeof(wire), source,
	                                sizeof(source), 3);
	raw_send(&f.client.calls[0], wire, wire_size);
	wait_counter(&failed->terminals, 1U);
	CHECK(failed->final_status == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&failed->seen[TR_RPC_RECORD_MESSAGE]) == 0U);
	CHECK(snapshot(failed).state.request_messages == 0U);
	CHECK(snapshot(failed).cached == 0U);

	/* Record Stream 负责本地终局；上层用 RESET 将配额失败告知对端。 */
	CHECK(tr_reactor_call(f.server.reactor, reset_quota_stream_on_owner,
	                      failed) == TR_OK);
	wait_counter(&f.client.calls[0].terminals, 1U);
	CHECK(f.client.calls[0].final_status ==
	      TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(atomic_load(&peer_server->terminals) == 0U);
	CHECK(atomic_load(&f.client.calls[1].terminals) == 0U);
	CHECK(snapshot(&f.client.calls[0]).cached == 0U);
	CHECK(tr_channel_get_stats(f.client.channel, &client_stats) == TR_OK);
	CHECK(tr_channel_get_stats(f.server.channel, &server_stats) == TR_OK);
	CHECK(client_stats.control_lane_state == TR_CHANNEL_LANE_UP &&
	      client_stats.bulk_lane_state == TR_CHANNEL_LANE_UP);
	CHECK(server_stats.control_lane_state == TR_CHANNEL_LANE_UP &&
	      server_stats.bulk_lane_state == TR_CHANNEL_LANE_UP);

	/* 配额仍保留且解码已被拒绝时，CONTROL PING/PONG 仍须完成。 */
	expect_channel_keepalive_progress(f.client.channel);

	CHECK(tr_memory_budget_release(&f.server.budget, quota.bytes) == TR_OK);
	request.c = &f.client.calls[1];
	request.h = head(TR_RPC_RECORD_MESSAGE);
	request.h.compression = TR_RPC_RECORD_ZSTD;
	request.h.decoded_size = sizeof(source);
	request.body = make_buffer(source, sizeof(source), NULL);
	send_compressed_until(&request);
	wait_counter(&peer_server->seen[TR_RPC_RECORD_MESSAGE], 1U);
	CHECK(peer_server->message_compression[0] == TR_RPC_RECORD_ZSTD);
	CHECK(peer_server->message_sizes[0] == sizeof(source));
	CHECK(peer_server->checksum == sum(source, sizeof(source)));
	send_kind(&f.client.calls[1], TR_RPC_RECORD_HALF_CLOSE);
	wait_counter(&peer_server->seen[TR_RPC_RECORD_HALF_CLOSE], 1U);
	send_kind(peer_server, TR_RPC_RECORD_INITIAL_METADATA);
	send_kind(peer_server, TR_RPC_RECORD_MESSAGE);
	send_kind(peer_server, TR_RPC_RECORD_FINAL_STATUS);
	wait_counter(&peer_server->terminals, 1U);
	wait_counter(&f.client.calls[1].terminals, 1U);
	CHECK(peer_server->final_status == TR_RPC_STATUS_OK);
	CHECK(f.client.calls[1].final_status == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&failed->terminals) == 1U &&
	      atomic_load(&f.client.calls[0].terminals) == 1U &&
	      atomic_load(&peer_server->terminals) == 1U &&
	      atomic_load(&f.client.calls[1].terminals) == 1U);
	CHECK(snapshot(peer_server).cached == 0U);
	CHECK(snapshot(&f.client.calls[1]).cached == 0U);
	destroy_fixture(&f);
	puts("rpc_record_stream: compressed workspace exhaustion preserves peer "
	     "passed");
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

static void test_compressed_send_owner_stopped_reaps_pending_task(void)
{
	struct fixture f;
	struct peer *peers[] = {&f.client, &f.server};
	uint8_t source[4096];
	struct send_request send = {0};

	for (size_t i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i % 29U);
	init_fixture_compressed_receive(&f, TR_RPC_ONE, TR_RPC_ONE);
	CHECK(tr_reactor_call(f.client.reactor,
	                      hold_peer_message_transforms_on_owner,
	                      &f.client) == TR_OK);
	open_call(&f, 1);
	send_kind(&f.client.calls[0], TR_RPC_RECORD_OPEN);
	wait_counter(&f.server.calls[0].seen[TR_RPC_RECORD_OPEN], 1U);
	send.c = &f.client.calls[0];
	send.h = head(TR_RPC_RECORD_MESSAGE);
	send.h.compression = TR_RPC_RECORD_ZSTD;
	send.h.decoded_size = sizeof(source);
	send.body = make_buffer(source, sizeof(source), NULL);
	CHECK(tr_reactor_call(f.client.reactor, send_compressed_owner, &send) ==
	      TR_OK);
	CHECK(send.body == NULL);
	wait_counter(&f.client.held_submissions, 1U);

	for (size_t i = 0; i < 2U; ++i) {
		int ret;
		uint64_t end = monotonic_ms() + 5000U;
		do {
			ret = tr_reactor_call(
			        peers[i]->reactor,
			        close_peer_message_service_on_owner, peers[i]);
			if (ret == TR_AGAIN)
				sched_yield();
			CHECK(monotonic_ms() < end);
		} while (ret == TR_AGAIN);
		CHECK(ret == TR_OK);
	}
	for (size_t i = 0; i < 2U; ++i)
		CHECK(tr_reactor_call(peers[i]->reactor,
		                      disable_peer_routes_on_owner,
		                      peers[i]) == TR_OK);
	for (size_t i = 0; i < 2U; ++i)
		CHECK(tr_reactor_stop(peers[i]->reactor) == TR_OK);
	CHECK(complete_held_transform_owner_stopped(&f.client) == TR_OK);
	for (size_t i = 0; i < 2U; ++i) {
		CHECK(tr_rpc_record_stream_reap_stopped(
		              &peers[i]->calls[0].call) == TR_OK);
		CHECK(peers[i]->calls[0].delivery ==
		      TR_RPC_RECORD_OWNER_STOPPED);
		CHECK(atomic_load(&peers[i]->calls[0].terminals) == 1U);
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->reassembly) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0U);
	}
}
int main(void)
{
	test_compressed_receive_close_waits_for_final_decode();
	test_compressed_receive_delivers_decoded_body();
	test_compressed_send_crosses_small_stream_window();
	test_compressed_send_policy_rejection_retains_body();
	test_compressed_send_encoded_limit_emits_no_partial_record();
	test_compressed_send_c_less_than_window();
	test_compressed_send_c_equals_window();
	test_compressed_receive_preserves_record_order();
	test_compressed_receive_rejects_invalid_zstd();
	test_compressed_receive_rejects_trailing_and_size_mismatch();
	test_compressed_receive_abort_waits_for_cancel_completion();
	test_compressed_receive_reset_retires_pending_stream();
	test_compressed_receive_owner_stopped_reaps_pending_task();
	test_compressed_send_reset_retires_pending_transform();
	test_compressed_send_lane_loss_retires_pending_transform();
	test_compressed_send_owner_stopped_reaps_pending_task();
	test_compressed_receive_preserves_interleaved_tx_state();
	test_compressed_send_cancel_waits_for_completion();
	test_compressed_send_queue_full_preserves_independent_streams();
	test_compressed_send_independent_streams_complete_out_of_order();
	test_compressed_transform_receives_only_body_with_auth_metadata();
	test_compressed_send_replacement_handshake_retires_old_transform();
	test_compressed_send_destroy_refuses_live_transform();
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
	test_receive_quota_preserves_peer();
	test_compressed_receive_workspace_exhaustion();
	test_compressed_workspace_exhaustion_preserves_peer();
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
