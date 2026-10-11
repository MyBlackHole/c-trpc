#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/execution/reactor_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "../src/transport/protocol/wire.h"
#include "tr/rpc.h"
#include "tr/status.h"

#include <assert.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define CASE_COUNT 5U
#define POOL_SIZE 64U

/* Test-only fault injection: never compiled into trcore. */
static _Thread_local int fail_worker_completion_malloc;
static _Atomic(struct tr_buffer_pool *) server_pool;
static _Atomic uint32_t fail_server_encoding;
static _Atomic uint32_t fail_server_close;
static _Atomic uint32_t server_close_attempts;
static _Atomic uint32_t server_connection_slot = UINT32_MAX;

static _Atomic int scheduling_fault;
static _Atomic unsigned scheduling_failures;
static _Atomic unsigned fault_close_count;
static _Thread_local int fail_next_clock;
static _Thread_local int fail_next_arm;

int __real_clock_gettime(clockid_t id, struct timespec *ts);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                               uint64_t deadline_ns);

int __wrap_clock_gettime(clockid_t id, struct timespec *ts)
{
	if (id == CLOCK_MONOTONIC && fail_next_clock) {
		fail_next_clock = 0;
		atomic_fetch_add(&scheduling_failures, 1U);
		errno = EIO;
		return -1;
	}
	return __real_clock_gettime(id, ts);
}

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                               uint64_t deadline_ns)
{
	if (fail_next_arm) {
		int ret = fail_next_arm == 3 ? TR_ERR_STALE : TR_ERR_SYS;

		fail_next_arm = 0;
		atomic_fetch_add(&scheduling_failures, 1U);
		return ret;
	}
	return __real_tr_reactor_timer_arm(timer, deadline_ns);
}


void *__real_malloc(size_t size);
int __real_tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
			     struct tr_buffer **out);
int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload);

void *__wrap_malloc(size_t size)
{
	if (fail_worker_completion_malloc) {
		fail_worker_completion_malloc = 0;
		return NULL;
	}
	return __real_malloc(size);
}

int __wrap_tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
			      struct tr_buffer **out)
{
	uint32_t count = atomic_load_explicit(&fail_server_encoding,
					     memory_order_relaxed);

	if (pool == atomic_load_explicit(&server_pool,
					memory_order_relaxed)) {
		while (count != 0U) {
			if (atomic_compare_exchange_weak_explicit(
				    &fail_server_encoding, &count, count - 1U,
				    memory_order_relaxed, memory_order_relaxed))
				return TR_AGAIN;
		}
	}
	return __real_tr_buffer_acquire(pool, min_capacity, out);
}

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			    uint32_t flags, uint32_t stream_id,
			    uint64_t message_id, struct tr_buffer *payload)
{
	uint32_t count = atomic_load_explicit(&fail_server_close,
					     memory_order_relaxed);

	if (type == TR_FRAME_STREAM_CLOSE &&
	    connection.slot == atomic_load_explicit(&server_connection_slot,
						    memory_order_relaxed)) {
		atomic_fetch_add_explicit(&server_close_attempts, 1U,
					  memory_order_relaxed);
		while (count != 0U) {
			if (atomic_compare_exchange_weak_explicit(
				    &fail_server_close, &count, count - 1U,
				    memory_order_relaxed, memory_order_relaxed)) {
				int fault = atomic_load(&scheduling_fault);
				unsigned seen = atomic_fetch_add(&fault_close_count, 1U) + 1U;

				fail_next_clock = fault == 1;
				fail_next_arm = fault == 4 ? (seen == 2U ? 3 : 0) :
					(fault >= 2 ? fault : 0);
				return TR_AGAIN;
			}
		}
	}
	return __real_tr_reactor_send(connection, type, flags, stream_id,
				      message_id, payload);
}

struct test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned handled;
	unsigned finished;
	int reconnect_gate;
	int reconnect_release;
	unsigned reconnect_calls;
	struct tr_rpc_call_handle reconnect_handles[2];
	int seen[CASE_COUNT];
	int statuses[CASE_COUNT];
};

struct result_arg {
	struct test_ctx *ctx;
	unsigned index;
};

static void pause_ms(void)
{
	const struct timespec pause = { 0, 1000000L };

	(void)nanosleep(&pause, NULL);
}

static void pair_nonblocking(int *a, int *b)
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
	*a = fds[0];
	*b = fds[1];
}

static void wait_handled(struct test_ctx *ctx, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->handled < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->handled >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_result(struct test_ctx *ctx, unsigned index)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (!ctx->seen[index] && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	if (!ctx->seen[index])
		fprintf(stderr,
			"unary failure case %u: no result, handled=%u finished=%u\n",
			index, ctx->handled, ctx->finished);
	assert(ctx->seen[index] == 1);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_clean(struct tr_channel *client_channel,
		       struct tr_channel *server_channel,
		       struct tr_rpc_endpoint *client_rpc,
		       struct tr_rpc_endpoint *server_rpc)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_rpc_endpoint_stats client_stats;
		struct tr_rpc_endpoint_stats server_stats;

		assert(tr_rpc_endpoint_get_stats(client_rpc, &client_stats) ==
		       TR_OK);
		assert(tr_rpc_endpoint_get_stats(server_rpc, &server_stats) ==
		       TR_OK);
		if (tr_channel_active_streams(client_channel) == 0U &&
		    tr_channel_active_streams(server_channel) == 0U &&
		    client_stats.active_calls == 0U &&
		    client_stats.terminal_calls == 0U &&
		    server_stats.active_calls == 0U &&
		    server_stats.terminal_calls == 0U)
			return;
		pause_ms();
	}
	assert(!"Unary error retained a Stream or Call after completion");
}

static void wait_pool(struct tr_buffer_pool *pool)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_buffer_pool_free_count(pool) == POOL_SIZE)
			return;
		pause_ms();
	}
	assert(tr_buffer_pool_free_count(pool) == POOL_SIZE);
}

static void wait_close_attempts(uint32_t target)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (atomic_load_explicit(&server_close_attempts,
					 memory_order_relaxed) >= target)
			return;
		pause_ms();
	}
	assert(!"no automatic Stream half-close retry");
}

static int on_unary(struct tr_rpc_call_handle call,
		    const struct tr_rpc_bytes *request,
		    struct tr_rpc_unary_response *response, void *arg)
{
	struct test_ctx *ctx = arg;
	static const uint8_t ok[] = "ok";
	unsigned id;

	assert(request != NULL && request->len == 1U && request->data);
	id = (unsigned)(request->data[0] - (uint8_t)'0');
	assert(id < CASE_COUNT);

	pthread_mutex_lock(&ctx->lock);
	ctx->handled++;
	if (ctx->reconnect_gate && id == 3U) {
		assert(ctx->reconnect_calls < 2U);
		ctx->reconnect_handles[ctx->reconnect_calls++] = call;
	}
	pthread_cond_broadcast(&ctx->cond);
	while (ctx->reconnect_gate && id == 3U && !ctx->reconnect_release)
		assert(pthread_cond_wait(&ctx->cond, &ctx->lock) == 0);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = ok;
	response->message.len = 2U;

	switch (id) {
	case 0:
		/* Fail the worker's first malloc after handler completion. */
		fail_worker_completion_malloc = 1;
		break;
	case 1:
		/* Normal response fails, then empty INTERNAL response succeeds. */
		atomic_store_explicit(&fail_server_encoding, 1U,
				      memory_order_relaxed);
		break;
	case 2:
		/* Neither normal nor error RESPONSE fits the available pool. */
		atomic_store_explicit(&fail_server_encoding, 2U,
				      memory_order_relaxed);
		atomic_store_explicit(&fail_server_close, 3U,
				      memory_order_relaxed);
		break;
	case 4:
		/* 成功 RESPONSE 同样拥有背压中的半关闭义务。 */
		atomic_store_explicit(&fail_server_close, 3U,
				      memory_order_relaxed);
		break;
	default:
		break;
	}
	return TR_OK;
}

static void on_result(struct tr_rpc_call_handle call, int status,
		      const struct tr_rpc_bytes *response, void *arg)
{
	struct result_arg *result = arg;
	struct test_ctx *ctx = result->ctx;

	(void)call;
	assert(result->index < CASE_COUNT);
	if (status == TR_RPC_STATUS_OK) {
		assert(response && response->len == 2U);
		assert(memcmp(response->data, "ok", 2U) == 0);
	} else if (status == TR_RPC_STATUS_INTERNAL) {
		assert(response && response->len == 0U);
	} else {
		assert(status == TR_RPC_STATUS_UNAVAILABLE);
		assert(response == NULL);
	}

	pthread_mutex_lock(&ctx->lock);
	assert(!ctx->seen[result->index]);
	ctx->seen[result->index] = 1;
	ctx->statuses[result->index] = status;
	ctx->finished++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void run_case(int fault)
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
	struct tr_buffer_pool client_pool;
	struct tr_buffer_pool server_buffer_pool;
	struct tr_rpc_call_handle calls[CASE_COUNT];
	struct result_arg args[CASE_COUNT];
	struct test_ctx ctx;
	uint32_t before;
	int client_fd;
	int server_fd;
	unsigned i;

	atomic_store(&scheduling_fault, fault);
	atomic_store(&scheduling_failures, 0U);
	atomic_store(&fault_close_count, 0U);
	atomic_store(&fail_server_encoding, 0U);
	atomic_store(&fail_server_close, 0U);
	atomic_store(&server_close_attempts, 0U);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	pair_nonblocking(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 128U;
	reactor_config.control_tx_item_capacity = 64U;
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
	channel_config.max_streams = 4U;
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

	assert(tr_buffer_pool_init(&client_pool, POOL_SIZE, 512U) == TR_OK);
	assert(tr_buffer_pool_init(&server_buffer_pool, POOL_SIZE, 512U) ==
	       TR_OK);
	atomic_store_explicit(&server_pool, &server_buffer_pool,
			      memory_order_relaxed);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 4U;
	rpc_config.message_pool = &client_pool;
	rpc_config.executor_threads = fault == 4 ? 2U : 1U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	rpc_config.message_pool = &server_buffer_pool;
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
	method.max_request_bytes = 32U;
	method.max_response_bytes = 32U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method, on_unary, &ctx) ==
	       TR_OK);

	for (i = 0; i < (fault ? 3U : CASE_COUNT); ++i) {
		uint8_t code = (uint8_t)('0' + i);
		struct tr_rpc_bytes request = { &code, 1U };

		args[i].ctx = &ctx;
		args[i].index = i;
		before = atomic_load_explicit(&server_close_attempts,
					      memory_order_relaxed);
		assert(tr_rpc_unary_call(client_rpc, 1U, 1U, &request, on_result,
					 &args[i], &calls[i]) == TR_OK);
		wait_handled(&ctx, i + 1U);
		wait_result(&ctx, i);
		if (i == 2U || i == 4U)
			wait_close_attempts(before + (fault ? 1U : 4U));
		wait_clean(client_channel, server_channel, client_rpc,
			   server_rpc);
		wait_pool(&client_pool);
		wait_pool(&server_buffer_pool);
	}

	if (fault == 4) {
		const struct tr_rpc_bytes request = { (const uint8_t *)"3", 1U };

		/*
		 * 第一轮半关闭背压正常 arm；完成任务在同一 owner turn 归还引用。
		 * 第二轮由 timer 驱动，注入 STALE 后必须安全退休无 task_ref 的槽。
		 */
		assert(atomic_load(&fault_close_count) == 2U);
		atomic_store(&scheduling_fault, 0);
		atomic_store(&fail_server_close, 0U);
		pair_nonblocking(&client_fd, &server_fd);
		assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
		assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);
		atomic_store(&server_connection_slot, server_conn.slot);
		assert(tr_channel_replace_connection(client_channel, TR_LANE_CONTROL,
			client_conn) == TR_OK);
		assert(tr_channel_replace_connection(server_channel, TR_LANE_CONTROL,
			server_conn) == TR_OK);
		assert(tr_channel_wait_ready(client_channel, TR_LANE_CONTROL, 5000U) == TR_OK);
		assert(tr_channel_wait_ready(server_channel, TR_LANE_CONTROL, 5000U) == TR_OK);
		/*
		 * 顺序 allocate/free 一个 Call 无法发现 free-list 自环。两个处理器
		 * 都停在门闩上，确保客户端与服务端各有两个不同的存活槽位。
		 */
		pthread_mutex_lock(&ctx.lock);
		ctx.reconnect_gate = 1;
		pthread_mutex_unlock(&ctx.lock);
		for (i = 3U; i < 5U; ++i) {
			args[i].ctx = &ctx;
			args[i].index = i;
			assert(tr_rpc_unary_call(client_rpc, 1U, 1U, &request, on_result,
				&args[i], &calls[i]) == TR_OK);
		}
		wait_handled(&ctx, 5U);
		assert(memcmp(&calls[3], &calls[4], sizeof(calls[3])) != 0);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.reconnect_calls == 2U);
		assert(memcmp(&ctx.reconnect_handles[0], &ctx.reconnect_handles[1],
			sizeof(ctx.reconnect_handles[0])) != 0);
		assert(ctx.finished == 3U);
		pthread_mutex_unlock(&ctx.lock);
		{
			struct tr_rpc_endpoint_stats stats;

			assert(tr_rpc_endpoint_get_stats(client_rpc, &stats) == TR_OK);
			assert(stats.active_calls == 2U);
			assert(tr_rpc_endpoint_get_stats(server_rpc, &stats) == TR_OK);
			assert(stats.active_calls == 2U);
		}
		pthread_mutex_lock(&ctx.lock);
		ctx.reconnect_release = 1;
		pthread_cond_broadcast(&ctx.cond);
		pthread_mutex_unlock(&ctx.lock);
		wait_result(&ctx, 3U);
		wait_result(&ctx, 4U);
		wait_clean(client_channel, server_channel, client_rpc, server_rpc);
		wait_pool(&client_pool);
		wait_pool(&server_buffer_pool);
		assert(ctx.statuses[3] == TR_RPC_STATUS_OK);
		assert(ctx.statuses[4] == TR_RPC_STATUS_OK);
		{
			struct tr_rpc_semantic_stats stats;

			assert(tr_rpc_endpoint_get_semantic_stats(client_rpc, &stats) == TR_OK);
			assert(stats.calls_started == 5U && stats.calls_finished == 5U);
			assert(stats.calls_inflight == 0U);
			assert(tr_rpc_endpoint_get_semantic_stats(server_rpc, &stats) == TR_OK);
			assert(stats.calls_started == 5U && stats.calls_finished == 5U);
			assert(stats.calls_inflight == 0U);
		}
	}

	assert(ctx.statuses[0] == TR_RPC_STATUS_INTERNAL);
	assert(ctx.statuses[1] == TR_RPC_STATUS_INTERNAL);
	assert(ctx.statuses[2] == TR_RPC_STATUS_UNAVAILABLE);
	if (!fault) {
		assert(ctx.statuses[3] == TR_RPC_STATUS_OK);
		assert(ctx.statuses[4] == TR_RPC_STATUS_OK);
	} else {
		enum tr_channel_lane_state state;

		assert(atomic_load(&scheduling_failures) == 1U);
		assert(tr_channel_get_lane_state(server_channel, TR_LANE_CONTROL,
			&state) == TR_OK);
		assert(state == (fault == 4 ? TR_CHANNEL_LANE_UP : TR_CHANNEL_LANE_DOWN));
	}
	assert(ctx.finished == (fault == 4 ? 5U : (fault ? 3U : CASE_COUNT)));
	assert(ctx.handled == (fault == 4 ? 5U : (fault ? 3U : CASE_COUNT)));
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_rpc_endpoint_destroy(client_rpc) == TR_OK);
	assert(tr_rpc_endpoint_destroy(server_rpc) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(tr_buffer_pool_destroy(&server_buffer_pool) == TR_OK);
	assert(tr_buffer_pool_destroy(&client_pool) == TR_OK);
	atomic_store_explicit(&server_pool, NULL, memory_order_relaxed);
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);

}

int main(void)
{
	run_case(0);
	run_case(1);
	run_case(2);
	run_case(3);
	run_case(4);
	return 0;
}
