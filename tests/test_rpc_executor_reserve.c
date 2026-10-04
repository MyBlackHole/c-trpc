#define _POSIX_C_SOURCE 200809L
#include "tr/buffer.h"
#include "tr/channel.h"
#include "tr/reactor.h"
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

#define QUEUE_CAPACITY 16U
#define CONTINUATION_RESERVE 2U
#define ADMISSION_LIMIT (QUEUE_CAPACITY - CONTINUATION_RESERVE)
#define CALL_A 0U
#define FILLER_BASE 1U
#define OVERLOAD_INDEX (FILLER_BASE + ADMISSION_LIMIT)
#define RECOVERY_INDEX (OVERLOAD_INDEX + 1U)
#define CALL_COUNT (RECOVERY_INDEX + 1U)

struct reserve_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned client_opened;
	unsigned client_finished;
	unsigned server_opened;
	unsigned a_messages;
	unsigned filler_messages;
	unsigned recovery_messages;
	unsigned server_closed;
	unsigned expected_a_sequence;
	int blocker_waiting;
	int release_blocker;
	int opened[CALL_COUNT];
	int finished[CALL_COUNT];
	int status[CALL_COUNT];
};

struct client_arg {
	struct reserve_ctx *ctx;
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

static void pause_1ms(void)
{
	struct timespec pause = { 0, 1000000L };
	(void)nanosleep(&pause, NULL);
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
		pause_1ms();
	}
	assert(state == TR_CHANNEL_LANE_UP);
}

static void wait_counter(struct reserve_ctx *ctx, unsigned *value,
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

static void wait_flag(struct reserve_ctx *ctx, int *flag)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (!*flag && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(*flag);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_finished(struct reserve_ctx *ctx, unsigned index)
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

static void wait_executor(struct tr_rpc_endpoint *endpoint,
			  unsigned queued, unsigned running)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_rpc_endpoint_stats stats;

		assert(tr_rpc_endpoint_get_stats(endpoint, &stats) == TR_OK);
		assert(stats.executor_queue_capacity == QUEUE_CAPACITY);
		assert(stats.executor_continuation_reserve ==
		       CONTINUATION_RESERVE);
		if (stats.executor_queued_tasks == queued &&
		    stats.executor_running_tasks == running)
			return;
		pause_1ms();
	}
	assert(!"executor did not reach expected reserve state");
}

static void wait_pool_full(struct tr_buffer_pool *pool, uint32_t count)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_buffer_pool_free_count(pool) == count)
			return;
		pause_1ms();
	}
	assert(tr_buffer_pool_free_count(pool) == count);
}

static void wait_channels_idle(struct tr_channel *client,
			       struct tr_channel *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_channel_active_streams(client) == 0U &&
		    tr_channel_active_streams(server) == 0U)
			return;
		pause_1ms();
	}
	assert(tr_channel_active_streams(client) == 0U);
	assert(tr_channel_active_streams(server) == 0U);
}

static int server_open(struct tr_rpc_call_handle call, void *arg)
{
	struct reserve_ctx *ctx = arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_opened++;
	if (ctx->server_opened == 1U) {
		ctx->blocker_waiting = 1;
		pthread_cond_broadcast(&ctx->cond);
		while (!ctx->release_blocker)
			pthread_cond_wait(&ctx->cond, &ctx->lock);
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_rpc_message_disposition
server_message(struct tr_rpc_call_handle call,
	       const struct tr_rpc_message *message, void *arg)
{
	struct reserve_ctx *ctx = arg;
	const uint8_t *data;

	assert(message != NULL);
	assert(message->bytes.len == 2U);
	data = message->bytes.data;
	assert(data != NULL);

	pthread_mutex_lock(&ctx->lock);
	if (data[0] == 'A') {
		ctx->a_messages++;
		assert((unsigned)(data[1] - '0') == ctx->expected_a_sequence);
		ctx->expected_a_sequence++;
	} else if (data[0] == 'F') {
		ctx->filler_messages++;
	} else if (data[0] == 'R') {
		ctx->recovery_messages++;
	} else {
		assert(!"unexpected reserve test payload");
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	if ((data[0] == 'A' && data[1] == '3') ||
	    data[0] == 'F' || data[0] == 'R')
		assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
	return TR_RPC_MESSAGE_RELEASE;
}

static void server_close(struct tr_rpc_call_handle call, int status, void *arg)
{
	struct reserve_ctx *ctx = arg;
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
	struct reserve_ctx *ctx = client->ctx;
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
		assert(!"reserve pressure must not become transport error");
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void start_call(struct tr_rpc_endpoint *client_rpc, uint32_t method_id,
		       struct client_arg *arg, struct tr_rpc_call_handle *call)
{
	struct tr_rpc_call_callbacks callbacks;

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_event = client_event;
	callbacks.arg = arg;
	assert(tr_rpc_call_start(client_rpc, 1U, method_id, &callbacks, call) ==
	       TR_OK);
}

static void send_tag(struct tr_rpc_call_handle call, char a, char b)
{
	uint8_t bytes[2] = { (uint8_t)a, (uint8_t)b };
	struct tr_rpc_bytes request = { bytes, sizeof(bytes) };

	assert(tr_rpc_call_send(call, &request) == TR_OK);
}

int main(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc stream_method;
	struct tr_rpc_method_desc filler_method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_rpc_endpoint *invalid_rpc = NULL;
	struct tr_rpc_endpoint_stats executor_stats;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle calls[CALL_COUNT];
	struct client_arg args[CALL_COUNT];
	struct reserve_ctx ctx;
	int client_fd;
	int server_fd;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	ctx.expected_a_sequence = 1U;
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
	rpc_config.executor_queue_capacity = QUEUE_CAPACITY;
	rpc_config.executor_continuation_reserve = QUEUE_CAPACITY;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &invalid_rpc) ==
	       TR_ERR_INVALID);
	assert(invalid_rpc == NULL);

	rpc_config.executor_continuation_reserve = CONTINUATION_RESERVE;
	rpc_config.observability_flags = TR_OBSERVABILITY_TIMING;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&stream_method, 0, sizeof(stream_method));
	stream_method.service_id = 1U;
	stream_method.method_id = 1U;
	stream_method.request_cardinality = TR_RPC_MANY;
	stream_method.response_cardinality = TR_RPC_MANY;
	stream_method.request_codec_id = TR_RPC_CODEC_RAW;
	stream_method.response_codec_id = TR_RPC_CODEC_RAW;
	stream_method.lane = TR_LANE_CONTROL;
	stream_method.max_request_bytes = 64U;
	stream_method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &stream_method, NULL, NULL) ==
	       TR_OK);

	filler_method = stream_method;
	filler_method.method_id = 2U;
	filler_method.request_cardinality = TR_RPC_ONE;
	assert(tr_rpc_register_method(client_rpc, &filler_method, NULL, NULL) ==
	       TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_open = server_open;
	handlers.on_message = server_message;
	handlers.on_close = server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &stream_method,
					      &handlers, &ctx) == TR_OK);
	assert(tr_rpc_register_stream_method(server_rpc, &filler_method,
					      &handlers, &ctx) == TR_OK);

	for (i = 0; i < CALL_COUNT; ++i) {
		args[i].ctx = &ctx;
		args[i].index = i;
	}

	/* A is already admitted and occupies the only worker. */
	start_call(client_rpc, 1U, &args[CALL_A], &calls[CALL_A]);
	for (i = 0; i <= ADMISSION_LIMIT; ++i) {
		unsigned index = FILLER_BASE + i;
		start_call(client_rpc, 2U, &args[index], &calls[index]);
	}
	wait_counter(&ctx, &ctx.client_opened, OVERLOAD_INDEX + 1U);

	send_tag(calls[CALL_A], 'A', '1');
	wait_flag(&ctx, &ctx.blocker_waiting);

	/* New first tasks may consume only 14/16 nodes. */
	for (i = 0; i < ADMISSION_LIMIT; ++i) {
		unsigned index = FILLER_BASE + i;
		send_tag(calls[index], 'F', (char)('0' + (i % 10U)));
	}
	wait_executor(server_rpc, ADMISSION_LIMIT, 1U);

	/*
	 * This first task is rejected while two physical executor nodes still
	 * remain.  It proves the reserve is admission policy, not hard-full.
	 */
	send_tag(calls[OVERLOAD_INDEX], 'F', 'X');
	wait_finished(&ctx, OVERLOAD_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[OVERLOAD_INDEX] ==
	       TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	assert(ctx.server_opened == 1U);
	assert(ctx.filler_messages == 0U);
	pthread_mutex_unlock(&ctx.lock);
	wait_executor(server_rpc, ADMISSION_LIMIT, 1U);

	/*
	 * A2/A3 are continuations of an already accepted Call and may consume
	 * the two reserved nodes even though new admissions are blocked.
	 */
	send_tag(calls[CALL_A], 'A', '2');
	wait_executor(server_rpc, ADMISSION_LIMIT + 1U, 1U);
	send_tag(calls[CALL_A], 'A', '3');
	wait_executor(server_rpc, QUEUE_CAPACITY, 1U);

	pthread_mutex_lock(&ctx.lock);
	ctx.release_blocker = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	wait_finished(&ctx, CALL_A);
	for (i = 0; i < ADMISSION_LIMIT; ++i)
		wait_finished(&ctx, FILLER_BASE + i);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[CALL_A] == TR_RPC_STATUS_OK);
	assert(ctx.a_messages == 3U);
	assert(ctx.expected_a_sequence == 4U);
	assert(ctx.filler_messages == ADMISSION_LIMIT);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_rpc_call_close_send(calls[CALL_A]) == TR_OK);
	for (i = 0; i < ADMISSION_LIMIT; ++i)
		assert(tr_rpc_call_close_send(calls[FILLER_BASE + i]) == TR_OK);
	assert(tr_rpc_call_close_send(calls[OVERLOAD_INDEX]) == TR_OK);
	wait_executor(server_rpc, 0U, 0U);

	/* Admission recovers on the same connection after queued work drains. */
	start_call(client_rpc, 2U, &args[RECOVERY_INDEX], &calls[RECOVERY_INDEX]);
	wait_counter(&ctx, &ctx.client_opened, CALL_COUNT);
	send_tag(calls[RECOVERY_INDEX], 'R', '1');
	wait_finished(&ctx, RECOVERY_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[RECOVERY_INDEX] == TR_RPC_STATUS_OK);
	assert(ctx.recovery_messages == 1U);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_rpc_call_close_send(calls[RECOVERY_INDEX]) == TR_OK);

	/*
	 * close_send() publishes transport half-close asynchronously. An empty
	 * executor snapshot before the peer observes that half-close is not a
	 * quiescence barrier: the later Stream event may enqueue one final
	 * lifecycle task. Wait until both Channel Stream tables are empty first,
	 * then require the executor and message pool to be fully drained.
	 */
	wait_channels_idle(client_channel, server_channel);
	wait_executor(server_rpc, 0U, 0U);
	wait_pool_full(&rpc_pool, 256U);

	assert(tr_rpc_endpoint_get_stats(server_rpc, &executor_stats) == TR_OK);
	assert(executor_stats.observability_flags == TR_OBSERVABILITY_TIMING);
	assert(executor_stats.executor_queue.capacity == QUEUE_CAPACITY);
	assert(executor_stats.executor_queue.current == 0U);
	assert(executor_stats.executor_queue.peak == QUEUE_CAPACITY);
	assert(executor_stats.executor_queue.full_events == 0U);
	assert(executor_stats.executor_admission_limit_hits == 1U);
	assert(executor_stats.executor_hard_full_events == 0U);
	assert(executor_stats.executor_ready_calls == 0U);
	assert(executor_stats.executor_ready_calls_peak != 0U);
	assert(executor_stats.executor_enqueued_tasks != 0U);
	assert(executor_stats.executor_taken_tasks ==
	       executor_stats.executor_enqueued_tasks);
	assert(executor_stats.executor_queue_wait_ns.samples <=
	       executor_stats.executor_taken_tasks);
	assert(executor_stats.executor_handler_ns.samples <=
	       executor_stats.executor_taken_tasks);
	assert(executor_stats.executor_queue_wait_ns.samples != 0U);
	assert(executor_stats.executor_handler_ns.samples != 0U);

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
