#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/transport/channel/channel.h"
#include "../src/execution/reactor.h"
#include "tr/rpc.h"
#include "tr/status.h"
#include "../src/rpc/rpc_internal.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define QUEUED_CAPACITY 16U
#define ACCEPTED_CALLS (QUEUED_CAPACITY + 1U)
#define OVERLOAD_INDEX ACCEPTED_CALLS
#define RECOVERY_INDEX (OVERLOAD_INDEX + 1U)
#define CALL_COUNT (RECOVERY_INDEX + 1U)

struct stream_overload_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned client_opened;
	unsigned client_finished;
	unsigned server_opened;
	unsigned server_messages;
	unsigned server_closed;
	int release;
	int opened[CALL_COUNT];
	int finished[CALL_COUNT];
	int status[CALL_COUNT];
};

struct client_arg {
	struct stream_overload_ctx *ctx;
	unsigned index;
};

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

static void wait_lane_up(struct tr_channel *channel)
{
	enum tr_channel_lane_state state = TR_CHANNEL_LANE_DOWN;
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		assert(tr_channel_get_lane_state(channel, TR_LANE_CONTROL, &state) ==
		       TR_OK);
		if (state == TR_CHANNEL_LANE_UP)
			return;
		{
			struct timespec pause = { 0, 1000000L };
			(void)nanosleep(&pause, NULL);
		}
	}
	assert(state == TR_CHANNEL_LANE_UP);
}

static void wait_counter(struct stream_overload_ctx *ctx, unsigned *value,
			 unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_finished(struct stream_overload_ctx *ctx, unsigned index)
{
	struct timespec deadline;
	int ret = 0;

	assert(index < CALL_COUNT);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (!ctx->finished[index] && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->finished[index]);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_executor_queue(struct tr_rpc_endpoint *endpoint,
				unsigned queued, unsigned running)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		struct tr_rpc_endpoint_stats stats;
		assert(tr_rpc_endpoint_get_stats(endpoint, &stats) == TR_OK);
		if (stats.executor_queued_tasks == queued &&
		    stats.executor_running_tasks == running)
			return;
		{
			struct timespec pause = { 0, 1000000L };
			(void)nanosleep(&pause, NULL);
		}
	}
	assert(!"stream executor did not reach saturation");
}

static void wait_pool_full(struct tr_buffer_pool *pool, uint32_t count)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		if (tr_buffer_pool_free_count(pool) == count)
			return;
		{
			struct timespec pause = { 0, 1000000L };
			(void)nanosleep(&pause, NULL);
		}
	}
	assert(tr_buffer_pool_free_count(pool) == count);
}

static int server_open(struct tr_rpc_call_handle call, void *arg)
{
	struct stream_overload_ctx *ctx = arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_opened++;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_rpc_message_disposition
server_message(struct tr_rpc_call_handle call,
	       const struct tr_rpc_message *message, void *arg)
{
	struct stream_overload_ctx *ctx = arg;

	assert(message != NULL);
	assert(message->bytes.len == 4U);
	assert(memcmp(message->bytes.data, "work", 4U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->server_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
	return TR_RPC_MESSAGE_RELEASE;
}

static void server_close(struct tr_rpc_call_handle call, int status, void *arg)
{
	struct stream_overload_ctx *ctx = arg;
	(void)call;
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_closed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void client_event(struct tr_rpc_call_handle call,
			 enum tr_rpc_call_event event, int status, void *arg)
{
	struct client_arg *client = arg;
	struct stream_overload_ctx *ctx = client->ctx;
	(void)call;

	assert(client->index < CALL_COUNT);
	pthread_mutex_lock(&ctx->lock);
	if (event == TR_RPC_CALL_EVENT_OPENED) {
		assert(!ctx->opened[client->index]);
		ctx->opened[client->index] = 1;
		ctx->client_opened++;
	} else if (event == TR_RPC_CALL_EVENT_FINISHED) {
		assert(!ctx->finished[client->index]);
		ctx->finished[client->index] = 1;
		ctx->status[client->index] = status;
		ctx->client_finished++;
	} else if (event == TR_RPC_CALL_EVENT_ERROR) {
		assert(!"stream overload must not surface as transport error");
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void start_call(struct tr_rpc_endpoint *client_rpc,
		       struct client_arg *arg, struct tr_rpc_call_handle *call)
{
	struct tr_rpc_call_callbacks callbacks;

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_event = client_event;
	callbacks.arg = arg;
	assert(tr_rpc_call_start(client_rpc, 1U, 2U, &callbacks, call) == TR_OK);
}

static void send_request(struct tr_rpc_call_handle call)
{
	struct tr_rpc_bytes request = {
		.data = (const uint8_t *)"work",
		.len = 4U
	};

	assert(tr_rpc_call_send(call, &request) == TR_OK);
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
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle calls[CALL_COUNT];
	struct client_arg args[CALL_COUNT];
	struct stream_overload_ctx ctx;
	int client_fd;
	int server_fd;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_nonblocking_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 256U;
	reactor_config.tx_item_capacity = 256U;
	reactor_config.control_tx_item_capacity = 128U;
	reactor_config.rx_buffer_count = 256U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 512U * 1024U;
	reactor_config.tx_budget_bytes = 512U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 64U;
	channel_config.initial_window_bytes = 512U * 1024U;
	channel_config.window_update_threshold_bytes = 64U * 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL, &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL, &server_channel) == TR_OK);
	wait_lane_up(client_channel);
	wait_lane_up(server_channel);

	assert(tr_buffer_pool_init(&rpc_pool, 256U, 4096U) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 4U;
	rpc_config.max_calls = 64U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 2U;
	rpc_config.executor_queue_capacity = 128U;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config, &client_rpc) ==
	       TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = QUEUED_CAPACITY;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 2U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_open = server_open;
	handlers.on_message = server_message;
	handlers.on_close = server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &method, &handlers, &ctx) ==
	       TR_OK);

	for (i = 0; i < CALL_COUNT; ++i) {
		args[i].ctx = &ctx;
		args[i].index = i;
		start_call(client_rpc, &args[i], &calls[i]);
	}
	wait_counter(&ctx, &ctx.client_opened, CALL_COUNT);

	/* Occupy the only worker with the first streaming admission. */
	send_request(calls[0]);
	wait_counter(&ctx, &ctx.server_opened, 1U);

	/* Fill all executor nodes with first-message streaming tasks. */
	for (i = 1U; i < ACCEPTED_CALLS; ++i)
		send_request(calls[i]);
	wait_executor_queue(server_rpc, QUEUED_CAPACITY, 1U);

	/*
	 * The next first message is rejected before on_open/on_message. It must
	 * finish as RESOURCE_EXHAUSTED, not ERROR/UNAVAILABLE, and the connection
	 * remains usable.
	 */
	send_request(calls[OVERLOAD_INDEX]);
	wait_finished(&ctx, OVERLOAD_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[OVERLOAD_INDEX] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	assert(ctx.server_opened == 1U);
	assert(ctx.server_messages == 0U);
	assert(ctx.server_closed == 0U);
	pthread_mutex_unlock(&ctx.lock);

	pthread_mutex_lock(&ctx.lock);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	wait_counter(&ctx, &ctx.client_finished, ACCEPTED_CALLS + 1U);
	pthread_mutex_lock(&ctx.lock);
	for (i = 0; i < ACCEPTED_CALLS; ++i) {
		assert(ctx.finished[i]);
		assert(ctx.status[i] == TR_RPC_STATUS_OK);
	}
	assert(ctx.server_opened == ACCEPTED_CALLS);
	assert(ctx.server_messages == ACCEPTED_CALLS);
	assert(ctx.server_closed == 0U);
	pthread_mutex_unlock(&ctx.lock);

	/* Same Channel/Connection accepts a later streaming Call after overload. */
	send_request(calls[RECOVERY_INDEX]);
	wait_finished(&ctx, RECOVERY_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[RECOVERY_INDEX] == TR_RPC_STATUS_OK);
	assert(ctx.server_opened == ACCEPTED_CALLS + 1U);
	assert(ctx.server_messages == ACCEPTED_CALLS + 1U);
	assert(ctx.server_closed == 0U);
	pthread_mutex_unlock(&ctx.lock);

	wait_pool_full(&rpc_pool, 256U);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	return 0;
}
