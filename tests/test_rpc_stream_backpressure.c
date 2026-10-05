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
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_CAPACITY 16U
#define FILLER_COUNT (QUEUE_CAPACITY + 1U)
#define CALL_A 0U
#define CALL_B 1U
#define FILLER_BASE 2U
#define RECOVERY_CALL (FILLER_BASE + FILLER_COUNT)
#define CALL_COUNT (RECOVERY_CALL + 1U)

struct backpressure_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned client_opened;
	unsigned client_finished;
	unsigned server_opened;
	unsigned a_messages;
	unsigned b_messages;
	unsigned filler_messages;
	unsigned recovery_messages;
	unsigned server_closed;
	unsigned server_resource_closed;
	int last_server_close_status;
	int blocker_waiting;
	int release_blocker;
	int opened[CALL_COUNT];
	int finished[CALL_COUNT];
	int status[CALL_COUNT];
	struct tr_stream_handle a_stream;
	struct tr_stream_handle b_stream;
};

struct client_arg {
	struct backpressure_ctx *ctx;
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

static void wait_counter(struct backpressure_ctx *ctx, unsigned *value,
			 unsigned target, const char *name)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	if (*value < target)
		fprintf(stderr, "wait %s timed out: got=%u target=%u ret=%d\n",
			name, *value, target, ret);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_flag(struct backpressure_ctx *ctx, int *flag)
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

static void wait_finished(struct backpressure_ctx *ctx, unsigned index)
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
		if (stats.executor_queued_tasks == queued &&
		    stats.executor_running_tasks == running)
			return;
		pause_1ms();
	}
	assert(!"executor did not reach expected state");
}

static struct tr_stream_flow_state wait_flow_balanced(struct tr_stream_handle stream)
{
	struct tr_stream_flow_state flow = {0};
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
		if (flow.rx_received_bytes == flow.rx_consumed_bytes)
			return flow;
		pause_1ms();
	}
	assert(!"stream flow did not become balanced");
	return flow;
}

static void wait_flow_backpressured(struct tr_stream_handle stream,
				    const struct tr_stream_flow_state *before)
{
	struct tr_stream_flow_state flow = {0};
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
		if (flow.rx_received_bytes > before->rx_received_bytes &&
		    flow.rx_consumed_bytes == before->rx_consumed_bytes)
			return;
		pause_1ms();
	}
	assert(!"pending executor message returned RX credit too early");
}

static void wait_flow_consumed_after(struct tr_stream_handle stream,
				     const struct tr_stream_flow_state *before)
{
	struct tr_stream_flow_state flow = {0};
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
		if (flow.rx_consumed_bytes > before->rx_consumed_bytes &&
		    flow.rx_consumed_bytes == flow.rx_received_bytes)
			return;
		pause_1ms();
	}
	assert(!"retried executor message did not return RX credit");
}

static int server_open(struct tr_rpc_call_handle call, void *arg)
{
	struct backpressure_ctx *ctx = arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_opened++;
	if (ctx->server_opened == 3U) {
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
	struct backpressure_ctx *ctx = arg;
	const uint8_t *data;

	assert(message != NULL);
	assert(message->bytes.len == 2U);
	data = message->bytes.data;
	assert(data != NULL);

	pthread_mutex_lock(&ctx->lock);
	switch (data[0]) {
	case 'A':
		ctx->a_messages++;
		if (ctx->a_messages == 1U) {
			struct tr_stream_handle stream;
			assert(tr_rpc_message_stream_internal(message, &stream) == TR_OK);
			ctx->a_stream = stream;
		}
		break;
	case 'B':
		ctx->b_messages++;
		if (ctx->b_messages == 1U) {
			struct tr_stream_handle stream;
			assert(tr_rpc_message_stream_internal(message, &stream) == TR_OK);
			ctx->b_stream = stream;
		}
		break;
	case 'F':
		ctx->filler_messages++;
		break;
	case 'R':
		ctx->recovery_messages++;
		break;
	default:
		assert(!"unexpected test payload");
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
	struct backpressure_ctx *ctx = arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_closed++;
	ctx->last_server_close_status = status;
	if (status == TR_RPC_STATUS_RESOURCE_EXHAUSTED)
		ctx->server_resource_closed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void client_event(struct tr_rpc_call_handle call,
			 enum tr_rpc_call_event event, int status, void *arg)
{
	struct client_arg *client = arg;
	struct backpressure_ctx *ctx = client->ctx;
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
		assert(!"executor backpressure must not surface as transport error");
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
	struct tr_rpc_method_desc target_method;
	struct tr_rpc_method_desc filler_method;
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
	struct backpressure_ctx ctx;
	struct tr_stream_flow_state a_before;
	struct tr_stream_flow_state b_before;
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
	rpc_config.executor_queue_capacity = QUEUE_CAPACITY;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&target_method, 0, sizeof(target_method));
	target_method.service_id = 1U;
	target_method.method_id = 1U;
	target_method.request_cardinality = TR_RPC_MANY;
	target_method.response_cardinality = TR_RPC_MANY;
	target_method.request_codec_id = TR_RPC_CODEC_RAW;
	target_method.response_codec_id = TR_RPC_CODEC_RAW;
	target_method.lane = TR_LANE_CONTROL;
	target_method.max_request_bytes = 64U;
	target_method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &target_method, NULL, NULL) ==
	       TR_OK);

	filler_method = target_method;
	filler_method.method_id = 2U;
	filler_method.request_cardinality = TR_RPC_ONE;
	assert(tr_rpc_register_method(client_rpc, &filler_method, NULL, NULL) ==
	       TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_open = server_open;
	handlers.on_message = server_message;
	handlers.on_close = server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &target_method,
					      &handlers, &ctx) == TR_OK);
	assert(tr_rpc_register_stream_method(server_rpc, &filler_method,
					      &handlers, &ctx) == TR_OK);

	for (i = 0; i < CALL_COUNT; ++i) {
		args[i].ctx = &ctx;
		args[i].index = i;
	}

	/* 过载开始前，两个 Call 已经进入应用回调。 */
	start_call(client_rpc, 1U, &args[CALL_A], &calls[CALL_A]);
	start_call(client_rpc, 1U, &args[CALL_B], &calls[CALL_B]);
	wait_counter(&ctx, &ctx.client_opened, 2U, "initial client opens");

	send_tag(calls[CALL_A], 'A', '1');
	wait_counter(&ctx, &ctx.a_messages, 1U, "A1 callback");
	a_before = wait_flow_balanced(ctx.a_stream);

	send_tag(calls[CALL_B], 'B', '1');
	wait_counter(&ctx, &ctx.b_messages, 1U, "B1 callback");
	b_before = wait_flow_balanced(ctx.b_stream);

	/* 一个填充任务运行并阻塞；其余 16 个任务填满所有 Executor 节点。 */
	for (i = 0; i < FILLER_COUNT; ++i)
		start_call(client_rpc, 2U, &args[FILLER_BASE + i],
			   &calls[FILLER_BASE + i]);
	wait_counter(&ctx, &ctx.client_opened, FILLER_BASE + FILLER_COUNT,
		     "filler client opens");

	send_tag(calls[FILLER_BASE], 'F', '0');
	wait_flag(&ctx, &ctx.blocker_waiting);
	for (i = 1U; i < FILLER_COUNT; ++i)
		send_tag(calls[FILLER_BASE + i], 'F', (char)('0' + (i % 10U)));
	wait_executor(server_rpc, QUEUE_CAPACITY, 1U);

	/*
	 * A2 和 B2 无法进入已满的 Executor；每个
	 * Call 各保留一个，并且其接收流控额度必须继续扣留。
	 */
	send_tag(calls[CALL_A], 'A', '2');
	wait_flow_backpressured(ctx.a_stream, &a_before);
	send_tag(calls[CALL_B], 'B', '2');
	wait_flow_backpressured(ctx.b_stream, &b_before);

	/*
	 * B3 超过 Call B 的单待处理任务上限。该 Call 可能已经
	 * 产生副作用（B1 已运行），因此只终止该 Call，并返回
	 * 最终 RESOURCE_EXHAUSTED 状态；连接必须继续存活。
	 */
	send_tag(calls[CALL_B], 'B', '3');
	wait_finished(&ctx, CALL_B);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[CALL_B] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	assert(ctx.b_messages == 1U);
	assert(!ctx.finished[CALL_A]);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_rpc_call_close_send(calls[CALL_B]) == TR_OK);

	/* 释放 Executor 容量后，A2 必须由 Reactor 所有者重试。 */
	pthread_mutex_lock(&ctx.lock);
	ctx.release_blocker = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	wait_counter(&ctx, &ctx.a_messages, 2U, "A2 retry callback");
	wait_flow_consumed_after(ctx.a_stream, &a_before);

	/* Call A 在背压后仍可使用，并正常完成。 */
	send_tag(calls[CALL_A], 'A', '3');
	wait_finished(&ctx, CALL_A);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[CALL_A] == TR_RPC_STATUS_OK);
	assert(ctx.a_messages == 3U);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_rpc_call_close_send(calls[CALL_A]) == TR_OK);

	/* 所有填充 Call 都在待处理重试前已经准入，仍必须正常排空。 */
	for (i = 0; i < FILLER_COUNT; ++i)
		wait_finished(&ctx, FILLER_BASE + i);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.filler_messages == FILLER_COUNT);
	pthread_mutex_unlock(&ctx.lock);
	for (i = 0; i < FILLER_COUNT; ++i)
		assert(tr_rpc_call_close_send(calls[FILLER_BASE + i]) == TR_OK);

	wait_executor(server_rpc, 0U, 0U);

	/* 过载后，同一 Channel/Connection 上的新 Call 能成功执行。 */
	start_call(client_rpc, 1U, &args[RECOVERY_CALL], &calls[RECOVERY_CALL]);
	wait_counter(&ctx, &ctx.client_opened, CALL_COUNT, "recovery client open");
	send_tag(calls[RECOVERY_CALL], 'R', '1');
	wait_finished(&ctx, RECOVERY_CALL);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[RECOVERY_CALL] == TR_RPC_STATUS_OK);
	assert(ctx.recovery_messages == 1U);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_rpc_call_close_send(calls[RECOVERY_CALL]) == TR_OK);

	{
		struct timespec deadline;
		int ret = 0;

		assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
		deadline.tv_sec += 10;
		pthread_mutex_lock(&ctx.lock);
		while (ctx.server_resource_closed < 1U && ret == 0)
			ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock,
						     &deadline);
		if (ctx.server_resource_closed < 1U)
			fprintf(stderr,
				"overloaded close missing: closed=%u resource=%u last_status=%d ret=%d\n",
				ctx.server_closed, ctx.server_resource_closed,
				ctx.last_server_close_status, ret);
		assert(ctx.server_resource_closed >= 1U);
		pthread_mutex_unlock(&ctx.lock);
	}
	wait_executor(server_rpc, 0U, 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	assert(tr_buffer_pool_free_count(&rpc_pool) == 256U);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	return 0;
}
