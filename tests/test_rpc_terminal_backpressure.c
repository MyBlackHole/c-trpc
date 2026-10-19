#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "tr/rpc.h"
#include "tr/status.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 将真实客户端执行器保持在一个运行中任务与十六个排队任务的状态。
 * CANCELLED 结果与成功一元响应都必须在满环形队列下被保留，
 * 不要求应用调用来刷新内部工作。
 */
#define QUEUE_CAPACITY 16U
#define FILLER_COUNT QUEUE_CAPACITY
#define CANCEL_INDEX (FILLER_COUNT + 1U)
#define RESPONSE_INDEX (CANCEL_INDEX + 1U)
#define CALL_COUNT (RESPONSE_INDEX + 1U)

struct test_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int first_callback_entered;
	int release_first_callback;
	int cancel_handler_entered;
	int release_cancel_handler;
	unsigned delivered;
	unsigned callback_count[CALL_COUNT];
	int statuses[CALL_COUNT];
	struct tr_rpc_endpoint *client_rpc;
};

struct callback_arg {
	struct test_context *context;
	unsigned index;
};

/* 只在当前测试二进制的链接阶段拦截 RPC 待重试调度边界。
 * arg 等于 Endpoint 指针时，是 retry 回调，而不是独立完成项描述符。
 */
typedef void (*complete_cb)(void *);

static _Atomic(uintptr_t) retry_target;
static atomic_int fail_retry_once;
static atomic_uint retry_attempts;
static atomic_uint retry_failures;
static atomic_uint retry_successes;
/* 仅测试二进制：在回调持有自身强引用、尚未提交到 Reactor 时冻结工作线程。 */
static atomic_int hold_retry_success_once;
static pthread_mutex_t retry_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t retry_gate_cond = PTHREAD_COND_INITIALIZER;
static int retry_gate_entered, retry_gate_open;

int __real_tr_reactor_complete(struct tr_reactor *, complete_cb, void *);

int __wrap_tr_reactor_complete(struct tr_reactor *r, complete_cb fn, void *arg)
{
	int ret;

	if (atomic_load(&retry_target) == 0U ||
	    (uintptr_t)arg != atomic_load(&retry_target))
		return __real_tr_reactor_complete(r, fn, arg);

	atomic_fetch_add(&retry_attempts, 1U);
	if (atomic_exchange(&fail_retry_once, 0)) {
		atomic_fetch_add(&retry_failures, 1U);
		return TR_ERR_SYS;
	}

	if (atomic_exchange(&hold_retry_success_once, 0)) {
		pthread_mutex_lock(&retry_gate_lock);
		retry_gate_entered = 1;
		pthread_cond_broadcast(&retry_gate_cond);
		while (!retry_gate_open)
			pthread_cond_wait(&retry_gate_cond, &retry_gate_lock);
		pthread_mutex_unlock(&retry_gate_lock);
	}
	ret = __real_tr_reactor_complete(r, fn, arg);
	if (ret == TR_OK)
		atomic_fetch_add(&retry_successes, 1U);
	return ret;
}

static void wait_retry_gate(void)
{
	pthread_mutex_t *lock = &retry_gate_lock;
	pthread_cond_t *cond = &retry_gate_cond;
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(lock);
	while (!retry_gate_entered && ret == 0)
		ret = pthread_cond_timedwait(cond, lock, &deadline);
	assert(retry_gate_entered);
	pthread_mutex_unlock(lock);
}

static void release_retry_gate(void)
{
	pthread_mutex_lock(&retry_gate_lock);
	assert(retry_gate_entered && !retry_gate_open);
	retry_gate_open = 1;
	pthread_cond_broadcast(&retry_gate_cond);
	pthread_mutex_unlock(&retry_gate_lock);
}

static void pause_1ms(void)
{
	const struct timespec pause = { 0, 1000000L };

	(void)nanosleep(&pause, NULL);
}

static void make_socket_pair(int *client, int *server)
{
	int fds[2];
	int i;

	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
	for (i = 0; i < 2; ++i) {
		int flags = fcntl(fds[i], F_GETFL, 0);
		int fdflags = fcntl(fds[i], F_GETFD, 0);

		assert(flags >= 0 && fdflags >= 0);
		assert(fcntl(fds[i], F_SETFL, flags | O_NONBLOCK) == 0);
		assert(fcntl(fds[i], F_SETFD, fdflags | FD_CLOEXEC) == 0);
	}
	*client = fds[0];
	*server = fds[1];
}

static void wait_lane_up(struct tr_channel *channel)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		enum tr_channel_lane_state state = TR_CHANNEL_LANE_DOWN;

		assert(tr_channel_get_lane_state(
			       channel, TR_LANE_CONTROL, &state) == TR_OK);
		if (state == TR_CHANNEL_LANE_UP)
			return;
		pause_1ms();
	}
	assert(!"Channel handshake did not finish");
}

static void wait_flag(struct test_context *ctx, int *flag)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx->lock);
	while (!*flag && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline);
	assert(*flag);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_deliveries(struct test_context *ctx, unsigned count)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->delivered < count && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline);
	assert(ctx->delivered == count);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_executor_full(struct tr_rpc_endpoint *endpoint)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_rpc_endpoint_stats stats;

		assert(tr_rpc_endpoint_get_stats(endpoint, &stats) == TR_OK);
		if (stats.executor_queued_tasks == QUEUE_CAPACITY &&
		    stats.executor_running_tasks == 1U)
			return;
		pause_1ms();
	}
	assert(!"client executor did not become full");
}

static void wait_backpressure(struct tr_rpc_endpoint *endpoint,
			      uint64_t events)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_rpc_endpoint_stats stats;

		assert(tr_rpc_endpoint_get_stats(endpoint, &stats) == TR_OK);
		if (stats.executor_hard_full_events >= events)
			return;
		pause_1ms();
	}
	assert(!"expected terminal admission backpressure was not observed");
}

static void wait_pool_idle(struct tr_buffer_pool *pool)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_buffer_pool_free_count(pool) == 256U)
			return;
		pause_1ms();
	}
	assert(tr_buffer_pool_free_count(pool) == 256U);
}

static int immediate_handler(struct tr_rpc_call_handle call,
			     const struct tr_rpc_bytes *request,
			     struct tr_rpc_unary_response *response, void *arg)
{
	static const uint8_t reply[] = "ok";

	(void)call;
	(void)arg;
	assert(request && request->len == 4U);
	assert(memcmp(request->data, "work", 4U) == 0);
	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = 2U;
	return TR_OK;
}

static int held_handler(struct tr_rpc_call_handle call,
			const struct tr_rpc_bytes *request,
			struct tr_rpc_unary_response *response, void *arg)
{
	struct test_context *ctx = arg;

	(void)call;
	assert(request && request->len == 4U);
	pthread_mutex_lock(&ctx->lock);
	ctx->cancel_handler_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->release_cancel_handler)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);
	response->status = TR_RPC_STATUS_OK;
	response->message.data = (const uint8_t *)"ok";
	response->message.len = 2U;
	return TR_OK;
}

static void result_callback(struct tr_rpc_call_handle call, int status,
			    const struct tr_rpc_bytes *response, void *arg)
{
	struct callback_arg *cb = arg;
	struct test_context *ctx = cb->context;

	(void)call;
	assert(cb->index < CALL_COUNT);
	if (status == TR_RPC_STATUS_OK) {
		assert(response && response->len == 2U);
		assert(memcmp(response->data, "ok", 2U) == 0);
	} else {
		assert(status == TR_RPC_STATUS_CANCELLED);
	}

	pthread_mutex_lock(&ctx->lock);
	if (cb->index == 0U) {
		/* Endpoint 的回调正在执行时，不能从执行器线程销毁自己。
		 * 失败必须保持对象和外层强引用，后续所有者仍可正常销毁。
		 */
		assert(tr_rpc_endpoint_destroy(ctx->client_rpc) ==
		       TR_ERR_STATE);
		ctx->first_callback_entered = 1;
		pthread_cond_broadcast(&ctx->cond);
		while (!ctx->release_first_callback)
			pthread_cond_wait(&ctx->cond, &ctx->lock);
	}
	assert(ctx->callback_count[cb->index] == 0U);
	ctx->callback_count[cb->index]++;
	ctx->statuses[cb->index] = status;
	ctx->delivered++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void start_unary(struct tr_rpc_endpoint *endpoint, uint32_t method_id,
			struct callback_arg *cb, struct tr_rpc_call_handle *out)
{
	const struct tr_rpc_bytes request = {
		.data = (const uint8_t *)"work",
		.len = 4U
	};

	assert(tr_rpc_unary_call(endpoint, 1U, method_id, &request,
				 result_callback, cb, out) == TR_OK);
}

static int reject_destroy(void *arg)
{
	return tr_rpc_endpoint_destroy((struct tr_rpc_endpoint *)arg);
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
	struct tr_rpc_call_handle calls[CALL_COUNT];
	struct callback_arg args[CALL_COUNT];
	struct tr_buffer_pool pool;
	struct test_context ctx;
	int client_fd;
	int server_fd;
	int destroy_status;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_socket_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 256U;
	reactor_config.tx_item_capacity = 128U;
	reactor_config.control_tx_item_capacity = 64U;
	reactor_config.rx_buffer_count = 256U;
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
				 NULL, NULL, NULL, NULL, &client_channel) ==
	       TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL, &server_channel) ==
	       TR_OK);
	wait_lane_up(client_channel);
	wait_lane_up(server_channel);

	assert(tr_buffer_pool_init(&pool, 256U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 4U;
	rpc_config.max_calls = 64U;
	rpc_config.message_pool = &pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = QUEUE_CAPACITY;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	ctx.client_rpc = client_rpc;
	/* Reactor 所有者回调内同样不得同步回收仍被事件分派使用的 Endpoint。 */
	destroy_status = tr_reactor_call(reactor, reject_destroy, client_rpc);
	assert(destroy_status == TR_ERR_STATE);

	rpc_config.role = TR_RPC_SERVER;
	rpc_config.executor_threads = 2U;
	rpc_config.executor_queue_capacity = 64U;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

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
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      immediate_handler, NULL) == TR_OK);
	method.method_id = 2U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      held_handler, &ctx) == TR_OK);

	for (i = 0; i < CALL_COUNT; ++i) {
		args[i].context = &ctx;
		args[i].index = i;
	}

	start_unary(client_rpc, 1U, &args[0], &calls[0]);
	wait_flag(&ctx, &ctx.first_callback_entered);
	for (i = 1U; i <= FILLER_COUNT; ++i)
		start_unary(client_rpc, 1U, &args[i], &calls[i]);
	wait_executor_full(client_rpc);

	/* 阻止此 Call 的服务端响应，直到 CANCEL 已提交。
	 * 客户端队列已满时，其终止回调不得丢弃。
	 */
	start_unary(client_rpc, 2U, &args[CANCEL_INDEX],
		    &calls[CANCEL_INDEX]);
	wait_flag(&ctx, &ctx.cancel_handler_entered);
	assert(tr_rpc_call_cancel(calls[CANCEL_INDEX]) == TR_OK);
	wait_backpressure(client_rpc, 1U);

	/* 重试时，已收到的成功响应也拥有一个接收缓冲区。 */
	start_unary(client_rpc, 1U, &args[RESPONSE_INDEX],
		    &calls[RESPONSE_INDEX]);
	wait_backpressure(client_rpc, 2U);

	/* 物理断开不得替换任一保留的终止结果。 */
	assert(tr_reactor_abort(client_conn, TR_ERR_SYS) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.delivered == 0U);
	ctx.release_cancel_handler = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	/* 工作线程 take() 会为 Endpoint 提交重试回调。第一次强制返回
	 * TR_ERR_SYS，验证 scheduler 归还这一次回调强引用，下一次 take()
	 * 仍能重新调度，且不会因此遗失 terminal task 或产生双重回调。
	 * 被阻塞的客户端只有一个工作线程，设置后才让它继续 take()，
	 * 因此注入点不依赖睡眠或系统调度时序。
	 */
	atomic_store(&retry_target, (uintptr_t)client_rpc);
	atomic_store(&fail_retry_once, 1);
	atomic_store(&hold_retry_success_once, 1);

	/* 工作线程 take() 必须重新调度两个待处理的 Call 本地终止槽位。 */
	pthread_mutex_lock(&ctx.lock);
	ctx.release_first_callback = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_retry_gate();
	/* 重试调度暂时停留在工作线程；Reactor 与统计查询仍可推进。
	 * 此时完成项强引用仍属于尚未提交的调用者，而不是 Reactor。
	 */
	assert(atomic_load(&retry_failures) == 1U);
	assert(atomic_load(&retry_successes) == 0U);
	assert(tr_reactor_quiesce(reactor) == TR_OK);
	/* owner 线程也不能在重试完成项尚未交接时销毁 Endpoint。 */
	assert(tr_reactor_call(reactor, reject_destroy, client_rpc) ==
	       TR_ERR_STATE);
	{
		struct tr_rpc_endpoint_stats stats;

		assert(tr_rpc_endpoint_get_stats(client_rpc, &stats) == TR_OK);
		assert(stats.executor_hard_full_events >= 2U);
	}
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.delivered < CALL_COUNT);
	pthread_mutex_unlock(&ctx.lock);
	release_retry_gate();
	wait_deliveries(&ctx, CALL_COUNT);
	assert(atomic_load(&retry_failures) == 1U);
	assert(atomic_load(&retry_attempts) >= 2U);
	assert(atomic_load(&retry_successes) >= 1U);
	atomic_store(&retry_target, (uintptr_t)0U);

	pthread_mutex_lock(&ctx.lock);
	for (i = 0; i < CALL_COUNT; ++i) {
		assert(ctx.callback_count[i] == 1U);
		assert(ctx.statuses[i] ==
		       (i == CANCEL_INDEX ? TR_RPC_STATUS_CANCELLED :
					      TR_RPC_STATUS_OK));
	}
	pthread_mutex_unlock(&ctx.lock);

	wait_pool_idle(&pool);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_rpc_endpoint_destroy(client_rpc) == TR_OK);
	assert(tr_rpc_endpoint_destroy(server_rpc) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	return 0;
}
