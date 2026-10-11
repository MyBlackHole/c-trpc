#define _GNU_SOURCE
#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "tr/rpc.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 只在本测试二进制中包装时钟和 RPC timer arm。 */
static _Thread_local int fail_monotonic_clock_once;
static atomic_uint fail_next_timer_arm;
static atomic_uint injected_arm_failures;

int __real_clock_gettime(clockid_t clock_id, struct timespec *out);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			       uint64_t deadline_ns);

int __wrap_clock_gettime(clockid_t clock_id, struct timespec *out)
{
	if (clock_id == CLOCK_MONOTONIC && fail_monotonic_clock_once) {
		fail_monotonic_clock_once = 0;
		errno = EIO;
		return -1;
	}
	return __real_clock_gettime(clock_id, out);
}

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			       uint64_t deadline_ns)
{
	unsigned expected = 1U;

	if (deadline_ns != 0U &&
	    atomic_compare_exchange_strong(&fail_next_timer_arm,
					   &expected, 0U)) {
		atomic_fetch_add(&injected_arm_failures, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_arm(handle, deadline_ns);
}

struct result_record {
	unsigned finished;
	unsigned errors;
	int status;
};

struct test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct result_record results[4];
};

struct callback_arg {
	struct test_ctx *ctx;
	unsigned index;
};

struct fixture {
	struct tr_reactor *reactor;
	struct tr_channel *client_channel;
	struct tr_channel *server_channel;
	struct tr_rpc_endpoint *client_rpc;
	struct tr_buffer_pool pool;
	struct test_ctx ctx;
};

static void on_event(struct tr_rpc_call_handle call,
		     enum tr_rpc_call_event event, int status, void *arg)
{
	struct callback_arg *callback = (struct callback_arg *)arg;
	struct result_record *record;

	(void)call;
	assert(callback->index < 4U);
	pthread_mutex_lock(&callback->ctx->lock);
	record = &callback->ctx->results[callback->index];
	if (event == TR_RPC_CALL_EVENT_FINISHED ||
	    event == TR_RPC_CALL_EVENT_ERROR) {
		record->finished++;
		if (event == TR_RPC_CALL_EVENT_ERROR)
			record->errors++;
		record->status = status;
		pthread_cond_broadcast(&callback->ctx->cond);
	}
	pthread_mutex_unlock(&callback->ctx->lock);
}

static void wait_finished(struct test_ctx *ctx, unsigned index,
			  int status)
{
	struct timespec deadline;
	int ret = 0;

	assert(index < 4U);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 8;
	pthread_mutex_lock(&ctx->lock);
	while (!ctx->results[index].finished && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	if (!ctx->results[index].finished)
		fprintf(stderr, "RPC deadline result %u never completed\n", index);
	assert(ctx->results[index].finished == 1U);
	assert(ctx->results[index].status == status);
	pthread_mutex_unlock(&ctx->lock);
}

static void verify_heap(struct fixture *f, uint32_t expected_count,
			const struct tr_rpc_call_handle *root_expected,
			uint64_t *deadline_out)
{
	struct tr_rpc_call_handle root;
	uint64_t deadline;
	uint32_t count;

	assert(tr_rpc_deadline_heap_snapshot(f->client_rpc, &count, &root,
					     &deadline) == TR_OK);
	assert(count == expected_count);
	if (root_expected)
		assert(memcmp(&root, root_expected, sizeof(root)) == 0);
	if (deadline_out)
		*deadline_out = deadline;
}

static void fixture_init(struct fixture *f)
{
	struct tr_channel_config config = {0};
	struct tr_rpc_endpoint_config rpc_config = {0};
	struct tr_rpc_method_desc method = {0};
	struct tr_conn_handle client_connection;
	struct tr_conn_handle server_connection;
	int fds[2];

	memset(f, 0, sizeof(*f));
	assert(pthread_mutex_init(&f->ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&f->ctx.cond, NULL) == 0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			  0, fds) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &f->reactor) ==
	       TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(f->reactor, fds[0], &client_connection) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(f->reactor, fds[1], &server_connection) ==
	       TR_OK);

	config.role = TR_CHANNEL_CLIENT;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 8U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&config, client_connection, client_connection,
				 NULL, NULL, NULL, NULL,
				 &f->client_channel) == TR_OK);
	config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&config, server_connection, server_connection,
				 NULL, NULL, NULL, NULL,
				 &f->server_channel) == TR_OK);
	assert(tr_channel_wait_ready(f->client_channel, TR_LANE_CONTROL,
				     5000U) == TR_OK);
	assert(tr_channel_wait_ready(f->server_channel, TR_LANE_CONTROL,
				     5000U) == TR_OK);

	assert(tr_buffer_pool_init(&f->pool, 16U, 4096U) == TR_OK);
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 8U;
	rpc_config.message_pool = &f->pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create(f->client_channel, &rpc_config,
				      &f->client_rpc) == TR_OK);

	method.service_id = 92U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_MANY;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(f->client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	method.method_id = 2U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	assert(tr_rpc_register_method(f->client_rpc, &method, NULL, NULL) ==
	       TR_OK);
}

static void fixture_destroy(struct fixture *f)
{
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_rpc_endpoint_destroy(f->client_rpc) == TR_OK);
	assert(tr_channel_destroy(f->client_channel) == TR_OK);
	assert(tr_channel_destroy(f->server_channel) == TR_OK);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	assert(tr_buffer_pool_destroy(&f->pool) == TR_OK);
	assert(pthread_cond_destroy(&f->ctx.cond) == 0);
	assert(pthread_mutex_destroy(&f->ctx.lock) == 0);
}

static int start_stream(struct fixture *f, uint32_t timeout_ms,
			struct callback_arg *arg, struct tr_rpc_call_handle *out)
{
	struct tr_rpc_call_options options = { .timeout_ms = timeout_ms };
	struct tr_rpc_call_callbacks callbacks = { .on_event = on_event,
						   .arg = arg };

	return tr_rpc_call_start_ex(f->client_rpc, 92U, 1U, &options,
				    &callbacks, out);
}

static void test_clock_and_arm_admission(struct fixture *f)
{
	struct tr_rpc_call_options options = { .timeout_ms = 5000U };
	struct tr_rpc_call_handle failed = { {0} };
	struct tr_rpc_call_handle long_call, reused_call;
	struct tr_rpc_bytes request = { (const uint8_t *)"x", 1U };
	struct callback_arg arg = { .ctx = &f->ctx, .index = 0U };
	struct callback_arg reused_arg = { .ctx = &f->ctx, .index = 3U };
	uint64_t original_deadline, root_deadline;
	struct tr_rpc_endpoint_stats stats;

	/* 两个 Client 入口在提交任何 Call ownership 前发现时钟失败。 */
	fail_monotonic_clock_once = 1;
	assert(start_stream(f, 5000U, &arg, &failed) == TR_ERR_SYS);
	assert(fail_monotonic_clock_once == 0);
	verify_heap(f, 0U, NULL, NULL);
	fail_monotonic_clock_once = 1;
	assert(tr_rpc_unary_call_ex(f->client_rpc, 92U, 2U, &request,
				    &options, NULL, NULL, &failed) == TR_ERR_SYS);
	assert(fail_monotonic_clock_once == 0);
	verify_heap(f, 0U, NULL, NULL);

	/* arm 失败时必须撤回新期限，不能在 heap 留下无 timer 的 Call。 */
	atomic_store(&fail_next_timer_arm, 1U);
	assert(start_stream(f, 5000U, &arg, &failed) == TR_ERR_SYS);
	assert(atomic_load(&fail_next_timer_arm) == 0U);
	verify_heap(f, 0U, NULL, NULL);

	/* Unary 入口使用相同的期限提交屏障，arm 失败也必须撤回。 */
	atomic_store(&fail_next_timer_arm, 1U);
	assert(tr_rpc_unary_call_ex(f->client_rpc, 92U, 2U, &request,
				    &options, NULL, NULL, &failed) == TR_ERR_SYS);
	assert(atomic_load(&fail_next_timer_arm) == 0U);
	verify_heap(f, 0U, NULL, NULL);
	assert(tr_rpc_endpoint_get_stats(f->client_rpc, &stats) == TR_OK);
	assert(stats.semantic.calls_inflight == 0U);

	/* 同一 Call slot 重新分配，不应被此前未提交的期限污染。 */
	assert(start_stream(f, 5000U, &arg, &long_call) == TR_OK);
	verify_heap(f, 1U, &long_call, &original_deadline);

	/* 更早的新 deadline arm 失败：保留旧根及原本的绝对期限。 */
	atomic_store(&fail_next_timer_arm, 1U);
	assert(start_stream(f, 1000U, &arg, &failed) == TR_ERR_SYS);
	verify_heap(f, 1U, &long_call, &root_deadline);
	assert(root_deadline == original_deadline);

	/* 失败后仍能在同一 Endpoint 上创建、取消新的 Call。 */
	assert(start_stream(f, 3000U, &reused_arg, &reused_call) == TR_OK);
	verify_heap(f, 2U, &reused_call, NULL);
	assert(tr_rpc_call_cancel(reused_call) == TR_OK);
	assert(tr_rpc_call_cancel(long_call) == TR_OK);
	verify_heap(f, 0U, NULL, NULL);
	wait_finished(&f->ctx, 0U, TR_RPC_STATUS_CANCELLED);
	wait_finished(&f->ctx, 3U, TR_RPC_STATUS_CANCELLED);
	assert(atomic_load(&injected_arm_failures) == 3U);
}

static void test_callback_rearm_failure(struct fixture *f)
{
	struct tr_rpc_call_handle first, second;
	struct callback_arg first_arg = { .ctx = &f->ctx, .index = 1U };
	struct callback_arg second_arg = { .ctx = &f->ctx, .index = 2U };

	/*
	 * 第一个 deadline 到期后，cancel 路径会尝试把共享 timer
	 * arm 到第二个 Call；注入这唯一一次失败。timer callback
	 * 必须用返回值在原来的 timer queue 中恢复后继期限。
	 */
	assert(start_stream(f, 250U, &first_arg, &first) == TR_OK);
	assert(start_stream(f, 550U, &second_arg, &second) == TR_OK);
	verify_heap(f, 2U, &first, NULL);
	atomic_store(&fail_next_timer_arm, 1U);
	wait_finished(&f->ctx, 1U, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	wait_finished(&f->ctx, 2U, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	assert(atomic_load(&fail_next_timer_arm) == 0U);
	assert(atomic_load(&injected_arm_failures) == 4U);
	verify_heap(f, 0U, NULL, NULL);
}

int main(void)
{
	struct fixture f;

	fixture_init(&f);
	test_clock_and_arm_admission(&f);
	test_callback_rearm_failure(&f);
	fixture_destroy(&f);
	puts("RPC timeout clock/arm failure and callback rearm: ok");
	return 0;
}
