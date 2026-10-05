#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/transport/channel/channel.h"
#include "../src/execution/reactor.h"
#include "tr/rpc.h"
#include "tr/status.h"
#include "../src/rpc/rpc_internal.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define SATURATED_QUEUED 16U
#define INITIAL_ACCEPTED (SATURATED_QUEUED + 1U)
#define OVERLOAD_INDEX INITIAL_ACCEPTED
#define RECOVERY_INDEX (OVERLOAD_INDEX + 1U)
#define RESULT_COUNT (RECOVERY_INDEX + 1U)

struct overload_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned handler_entered;
	unsigned handler_completed;
	unsigned results;
	int release;
	int done[RESULT_COUNT];
	int status[RESULT_COUNT];
};

struct result_arg {
	struct overload_ctx *ctx;
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

static void wait_handler_entered(struct overload_ctx *ctx, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->handler_entered < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->handler_entered >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_result(struct overload_ctx *ctx, unsigned index)
{
	struct timespec deadline;
	int ret = 0;

	assert(index < RESULT_COUNT);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx->lock);
	while (!ctx->done[index] && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->done[index]);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_results(struct overload_ctx *ctx, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->results < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->results >= target);
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
	assert(!"executor did not reach expected saturation");
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

static int blocking_handler(struct tr_rpc_call_handle call,
			    const struct tr_rpc_bytes *request,
			    struct tr_rpc_unary_response *response, void *arg)
{
	struct overload_ctx *ctx = arg;
	static const uint8_t reply[] = "ok";
	(void)call;
	assert(request != NULL);
	assert(request->len == 4U);
	assert(memcmp(request->data, "work", 4U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->handler_entered++;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	ctx->handler_completed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = 2U;
	return TR_OK;
}

static void overload_result(struct tr_rpc_call_handle call, int status,
			    const struct tr_rpc_bytes *response, void *arg)
{
	struct result_arg *result = arg;
	struct overload_ctx *ctx = result->ctx;
	(void)call;

	assert(result->index < RESULT_COUNT);
	if (status == TR_RPC_STATUS_OK) {
		assert(response != NULL);
		assert(response->len == 2U);
		assert(memcmp(response->data, "ok", 2U) == 0);
	} else if (status == TR_RPC_STATUS_RESOURCE_EXHAUSTED) {
		assert(response != NULL);
		assert(response->len == 0U);
	} else {
		assert(!"unexpected overload result status");
	}

	pthread_mutex_lock(&ctx->lock);
	assert(!ctx->done[result->index]);
	ctx->done[result->index] = 1;
	ctx->status[result->index] = status;
	ctx->results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void submit_call(struct tr_rpc_endpoint *client,
			struct result_arg *result,
			struct tr_rpc_call_handle *call)
{
	struct tr_rpc_bytes request = {
		.data = (const uint8_t *)"work",
		.len = 4U
	};

	assert(tr_rpc_unary_call(client, 1U, 1U, &request, overload_result,
				 result, call) == TR_OK);
}

int main(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle calls[RESULT_COUNT];
	struct result_arg result_args[RESULT_COUNT];
	struct overload_ctx ctx;
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
	reactor_config.tx_item_capacity = 128U;
	reactor_config.control_tx_item_capacity = 64U;
	reactor_config.rx_buffer_count = 128U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 256U * 1024U;
	reactor_config.tx_budget_bytes = 256U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 64U;
	channel_config.initial_window_bytes = 256U * 1024U;
	channel_config.window_update_threshold_bytes = 64U * 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL, &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL, &server_channel) == TR_OK);
	wait_lane_up(client_channel);
	wait_lane_up(server_channel);

	assert(tr_buffer_pool_init(&rpc_pool, 128U, 4096U) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 4U;
	rpc_config.max_calls = 64U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 2U;
	rpc_config.executor_queue_capacity = 64U;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config, &client_rpc) ==
	       TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = SATURATED_QUEUED;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method, blocking_handler, &ctx) ==
	       TR_OK);

	for (i = 0; i < RESULT_COUNT; ++i) {
		result_args[i].ctx = &ctx;
		result_args[i].index = i;
	}

	/* 一个正在运行的处理器占用唯一工作线程。 */
	submit_call(client_rpc, &result_args[0], &calls[0]);
	wait_handler_entered(&ctx, 1U);

	/* 在工作线程保持阻塞时填满全部 16 个执行器队列节点。 */
	for (i = 1U; i < INITIAL_ACCEPTED; ++i)
		submit_call(client_rpc, &result_args[i], &calls[i]);
	wait_executor_queue(server_rpc, SATURATED_QUEUED, 1U);

	/* 下一个一元调用在进入处理器前被显式拒绝。 */
	submit_call(client_rpc, &result_args[OVERLOAD_INDEX],
		    &calls[OVERLOAD_INDEX]);
	wait_result(&ctx, OVERLOAD_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[OVERLOAD_INDEX] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	assert(ctx.handler_entered == 1U);
	assert(ctx.handler_completed == 0U);
	pthread_mutex_unlock(&ctx.lock);

	/* 释放已经接受的工作；被拒绝的 Call 不能污染整个连接。 */
	pthread_mutex_lock(&ctx.lock);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_results(&ctx, INITIAL_ACCEPTED + 1U);

	pthread_mutex_lock(&ctx.lock);
	for (i = 0; i < INITIAL_ACCEPTED; ++i) {
		assert(ctx.done[i]);
		assert(ctx.status[i] == TR_RPC_STATUS_OK);
	}
	assert(ctx.handler_entered == INITIAL_ACCEPTED);
	assert(ctx.handler_completed == INITIAL_ACCEPTED);
	pthread_mutex_unlock(&ctx.lock);

	/* 过载响应后，同一个 Channel/Connection 仍然可以接受新工作。 */
	submit_call(client_rpc, &result_args[RECOVERY_INDEX],
		    &calls[RECOVERY_INDEX]);
	wait_result(&ctx, RECOVERY_INDEX);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status[RECOVERY_INDEX] == TR_RPC_STATUS_OK);
	assert(ctx.handler_entered == INITIAL_ACCEPTED + 1U);
	assert(ctx.handler_completed == INITIAL_ACCEPTED + 1U);
	pthread_mutex_unlock(&ctx.lock);

	wait_pool_full(&rpc_pool, 128U);
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
