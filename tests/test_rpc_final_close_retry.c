#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "../src/transport/protocol/wire.h"
#include "tr/rpc.h"
#include "tr/status.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/*
 * Inject failure only on the server's Transport STREAM_CLOSE. Its final RPC
 * STATUS and all unrelated control frames must continue to flow normally.
 */
#define CALL_COUNT 3U

static _Atomic uint32_t server_connection_slot = UINT32_MAX;
static _Atomic uint32_t fail_server_close;
static _Atomic uint32_t close_attempts;

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload);

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload)
{
	if (type == TR_FRAME_STREAM_CLOSE &&
	    connection.slot == atomic_load_explicit(&server_connection_slot,
						    memory_order_relaxed)) {
		uint32_t left = atomic_load_explicit(&fail_server_close,
						    memory_order_relaxed);

		atomic_fetch_add_explicit(&close_attempts, 1U,
					  memory_order_relaxed);
		while (left != 0U) {
			if (atomic_compare_exchange_weak_explicit(
				    &fail_server_close, &left, left - 1U,
				    memory_order_relaxed, memory_order_relaxed))
				return TR_AGAIN;
		}
	}
	return __real_tr_reactor_send(connection, type, flags, stream_id,
				      message_id, payload);
}

struct test_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened;
	unsigned finished;
	unsigned messages;
	unsigned server_closed;
	unsigned finished_once[CALL_COUNT];
	int finished_status[CALL_COUNT];
	int last_close_status;
};

struct client_arg {
	struct test_context *ctx;
	unsigned index;
};

static void pause_ms(long ms)
{
	struct timespec pause = { ms / 1000L, (ms % 1000L) * 1000000L };

	(void)nanosleep(&pause, NULL);
}

static void make_nonblocking_pair(int *a, int *b)
{
	int pair[2];
	int i;

	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0);
	for (i = 0; i < 2; ++i) {
		int flags = fcntl(pair[i], F_GETFL, 0);
		int fdflags = fcntl(pair[i], F_GETFD, 0);

		assert(flags >= 0 && fdflags >= 0);
		assert(fcntl(pair[i], F_SETFL, flags | O_NONBLOCK) == 0);
		assert(fcntl(pair[i], F_SETFD, fdflags | FD_CLOEXEC) == 0);
	}
	*a = pair[0];
	*b = pair[1];
}

static void wait_count(struct test_context *ctx, unsigned *value,
		       unsigned count)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < count && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline);
	if (*value < count)
		fprintf(stderr, "RPC close wait: wanted=%u actual=%u opened=%u "
			"finished=%u messages=%u server_closed=%u attempts=%u\n",
			count, *value, ctx->opened, ctx->finished,
			ctx->messages, ctx->server_closed,
			atomic_load_explicit(&close_attempts,
					     memory_order_relaxed));
	assert(*value >= count);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_attempts(uint32_t target)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (atomic_load_explicit(&close_attempts,
					 memory_order_relaxed) >= target)
			return;
		pause_ms(1);
	}
	assert(!"RPC close retry was not dispatched by the owner timer");
}

static void wait_empty(struct tr_channel *client, struct tr_channel *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_channel_active_streams(client) == 0U &&
		    tr_channel_active_streams(server) == 0U)
			return;
		pause_ms(1);
	}
	fprintf(stderr, "Channel Streams still active: client=%u server=%u\n",
		tr_channel_active_streams(client), tr_channel_active_streams(server));
	assert(!"terminal STREAM_CLOSE did not retire the Streams");
}

static void wait_pool_full(struct tr_buffer_pool *pool, uint32_t count)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_buffer_pool_free_count(pool) == count)
			return;
		pause_ms(1);
	}
	assert(tr_buffer_pool_free_count(pool) == count);
}

static int on_server_open(struct tr_rpc_call_handle call, void *arg)
{
	(void)call;
	(void)arg;
	return TR_OK;
}

static enum tr_rpc_message_disposition
on_server_message(struct tr_rpc_call_handle call,
		  const struct tr_rpc_message *message, void *arg)
{
	struct test_context *ctx = arg;

	assert(message && message->bytes.len == 4U);
	assert(memcmp(message->bytes.data, "work", 4U) == 0);
	/* This must return success even when the following close is congested. */
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
	pthread_mutex_lock(&ctx->lock);
	ctx->messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void on_server_close(struct tr_rpc_call_handle call, int status,
			    void *arg)
{
	struct test_context *ctx = arg;

	(void)call;
	pthread_mutex_lock(&ctx->lock);
	ctx->server_closed++;
	ctx->last_close_status = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void on_client_event(struct tr_rpc_call_handle call,
			    enum tr_rpc_call_event event, int status, void *arg)
{
	struct client_arg *cb = arg;
	struct test_context *ctx = cb->ctx;

	(void)call;
	assert(cb->index < CALL_COUNT);
	pthread_mutex_lock(&ctx->lock);
	if (event == TR_RPC_CALL_EVENT_OPENED) {
		ctx->opened++;
	} else if (event == TR_RPC_CALL_EVENT_FINISHED) {
		assert(ctx->finished_once[cb->index] == 0U);
		ctx->finished_once[cb->index] = 1U;
		ctx->finished_status[cb->index] = status;
		ctx->finished++;
	} else if (event == TR_RPC_CALL_EVENT_ERROR) {
		assert(!"transport failure must not replace an accepted final STATUS");
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void start_and_finish(struct tr_rpc_endpoint *endpoint,
			     struct client_arg *arg,
			     struct tr_rpc_call_handle *out)
{
	struct tr_rpc_call_callbacks callbacks;
	static const uint8_t request_data[] = "work";
	const struct tr_rpc_bytes request = { request_data, 4U };

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_event = on_client_event;
	callbacks.arg = arg;
	assert(tr_rpc_call_start(endpoint, 1U, 1U, &callbacks, out) == TR_OK);
	wait_count(arg->ctx, &arg->ctx->opened, arg->index + 1U);
	assert(tr_rpc_call_send(*out, &request) == TR_OK);
	assert(tr_rpc_call_close_send(*out) == TR_OK);
}

int main(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_rpc_call_handle calls[CALL_COUNT];
	struct client_arg args[CALL_COUNT];
	struct tr_buffer_pool rpc_pool;
	struct test_context ctx;
	uint32_t attempts_before;
	uint32_t attempts_after;
	int client_fd;
	int server_fd;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_nonblocking_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 128U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 128U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);
	atomic_store_explicit(&server_connection_slot, server_conn.slot,
			      memory_order_relaxed);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 2U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL, &client_channel) ==
	       TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL, &server_channel) ==
	       TR_OK);
	assert(tr_channel_wait_ready(client_channel, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);
	assert(tr_channel_wait_ready(server_channel, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);

	assert(tr_buffer_pool_init(&rpc_pool, 128U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 2U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config, &client_rpc) ==
	       TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);
	memset(&handlers, 0, sizeof(handlers));
	handlers.on_open = on_server_open;
	handlers.on_message = on_server_message;
	handlers.on_close = on_server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &method, &handlers,
					      &ctx) == TR_OK);

	for (i = 0; i < CALL_COUNT; ++i) {
		args[i].ctx = &ctx;
		args[i].index = i;
	}

	/* Normal completion: four failed close admissions, then auto recovery. */
	atomic_store_explicit(&fail_server_close, 4U, memory_order_relaxed);
	start_and_finish(client_rpc, &args[0], &calls[0]);
	wait_count(&ctx, &ctx.messages, 1U);
	wait_count(&ctx, &ctx.finished, 1U);
	wait_attempts(5U);
	wait_count(&ctx, &ctx.server_closed, 1U);
	wait_empty(client_channel, server_channel);
	assert(ctx.finished_status[0] == TR_RPC_STATUS_OK);

	/* Keep failing, prove bounded timer progress without spinning or flush. */
	atomic_store_explicit(&fail_server_close, 100U, memory_order_relaxed);
	attempts_before = atomic_load_explicit(&close_attempts,
					      memory_order_relaxed);
	start_and_finish(client_rpc, &args[1], &calls[1]);
	wait_count(&ctx, &ctx.messages, 2U);
	wait_count(&ctx, &ctx.finished, 2U);
	wait_attempts(attempts_before + 3U);
	pause_ms(75);
	attempts_after = atomic_load_explicit(&close_attempts,
					     memory_order_relaxed);
	assert(attempts_after - attempts_before < 20U);
	assert(tr_channel_active_streams(server_channel) != 0U);
	atomic_store_explicit(&fail_server_close, 0U, memory_order_relaxed);
	wait_count(&ctx, &ctx.server_closed, 2U);
	wait_empty(client_channel, server_channel);
	assert(ctx.finished_status[1] == TR_RPC_STATUS_OK);

	/*
	 * Disconnect with an in-flight close retry; the Reactor callback must
	 * retire the Call and prevent a stale timer from touching the Stream.
	 */
	atomic_store_explicit(&fail_server_close, 100U, memory_order_relaxed);
	attempts_before = atomic_load_explicit(&close_attempts,
					      memory_order_relaxed);
	start_and_finish(client_rpc, &args[2], &calls[2]);
	wait_count(&ctx, &ctx.messages, 3U);
	wait_count(&ctx, &ctx.finished, 3U);
	wait_attempts(attempts_before + 2U);
	assert(tr_reactor_abort(server_conn, TR_ERR_SYS) == TR_OK);
	wait_empty(client_channel, server_channel);
	wait_count(&ctx, &ctx.server_closed, 3U);

	pthread_mutex_lock(&ctx.lock);
	for (i = 0; i < CALL_COUNT; ++i) {
		assert(ctx.finished_once[i] == 1U);
		assert(ctx.finished_status[i] == TR_RPC_STATUS_OK);
	}
	assert(ctx.server_closed == CALL_COUNT);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_rpc_endpoint_destroy(client_rpc) == TR_OK);
	assert(tr_rpc_endpoint_destroy(server_rpc) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	wait_pool_full(&rpc_pool, 128U);
	assert(tr_buffer_pool_destroy(&rpc_pool) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	return 0;
}
