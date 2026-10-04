#include "tr/client.h"
#include "tr/reactor.h"
#include "tr/server.h"
#include "tr/status.h"
#include "../src/channel_internal.h"
#include "../src/runtime_internal.h"
#include "../src/facade_tuning_internal.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

/*
 * 仅此测试通过链接器 --wrap 观察库对 pthread 的调用。计数不包含宿主或
 * sanitizer 的后台线程，也不依赖 /proc 采样、sleep 或生产代码测试钩子。
 * reset 仅在本轮全部线程 join 后执行；原子计数允许未来从不同线程回收。
 */
static atomic_uint create_attempts;
static atomic_uint created_threads;
static atomic_uint join_attempts;
static atomic_uint joined_threads;
static atomic_uint fail_create_at;

int __real_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
			 void *(*start)(void *), void *arg);
int __real_pthread_join(pthread_t thread, void **result);

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
			 void *(*start)(void *), void *arg)
{
	unsigned attempt = atomic_fetch_add(&create_attempts, 1U) + 1U;
	int ret;

	if (attempt == atomic_load(&fail_create_at))
		return EAGAIN;
	ret = __real_pthread_create(thread, attr, start, arg);
	if (ret == 0)
		atomic_fetch_add(&created_threads, 1U);
	return ret;
}

int __wrap_pthread_join(pthread_t thread, void **result)
{
	int ret;

	atomic_fetch_add(&join_attempts, 1U);
	ret = __real_pthread_join(thread, result);
	if (ret == 0)
		atomic_fetch_add(&joined_threads, 1U);
	return ret;
}

static void expect_threads(unsigned created, unsigned joined)
{
	unsigned actual_created = atomic_load(&created_threads);
	unsigned actual_joined = atomic_load(&joined_threads);
	unsigned actual_joins = atomic_load(&join_attempts);

	if (actual_created != created || actual_joined != joined ||
	    actual_joins != joined)
		fprintf(stderr, "threads: created=%u joined=%u join_attempts=%u; "
			"expected created=%u joined=%u\n", actual_created,
			actual_joined, actual_joins, created, joined);
	assert(actual_created == created);
	assert(actual_joined == joined);
	assert(actual_joins == joined);
}

static void reset_probe(unsigned fail_at)
{
	/* 成功创建必须一一对应成功 join；重复 join 同样算失败。 */
	expect_threads(atomic_load(&created_threads),
		       atomic_load(&created_threads));
	atomic_store(&create_attempts, 0U);
	atomic_store(&created_threads, 0U);
	atomic_store(&join_attempts, 0U);
	atomic_store(&joined_threads, 0U);
	atomic_store(&fail_create_at, fail_at);
}

static void small_limits(struct tr_facade_limits *limits)
{
	tr_facade_limits_init(limits);
	limits->max_streams = 8U;
	limits->max_methods = 4U;
	limits->max_calls = 8U;
	limits->max_frame_payload_bytes = 256U;
	limits->max_message_bytes = 1024U;
	limits->initial_window_bytes = 4096U;
	limits->window_update_threshold_bytes = 256U;
	limits->rpc_message_buffer_bytes = 256U;
}

static void server_config_init(struct tr_server_config *config)
{
	tr_server_config_init(config);
	small_limits(&config->limits);
	config->max_peers = 2U;
	config->keepalive_interval_ms = 10U;
	config->keepalive_timeout_ms = 50U;
}

static void server_tuning_init(struct tr_facade_tuning *tuning,
			       unsigned workers)
{
	tr_facade_tuning_init(tuning);
	tuning->executor_threads = workers;
	tuning->executor_queue_capacity = 16U;
}

static void listen_loopback(struct tr_server *server)
{
	uint16_t port = 0;

	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(port != 0);
}

static void test_client_create_destroy_threads(void)
{
	unsigned iteration;

	for (iteration = 0; iteration < 4U; ++iteration) {
		struct tr_client_config config;
		struct tr_client *client = NULL;

		tr_client_config_init(&config);
		small_limits(&config.limits);
		config.keepalive_interval_ms = iteration % 2U ? 10U : 0U;
		config.keepalive_timeout_ms = 50U;
		reset_probe(0U);
		assert(tr_client_create(&config, &client) == TR_OK);
		assert(client != NULL);
		/* 未 connect 时只有 Reactor，没有闲置的 maintenance thread。 */
		expect_threads(1U, 0U);
		tr_client_destroy(client);
		expect_threads(1U, 1U);
	}
}

static void test_channel_reconnect_uses_no_extra_thread(void)
{
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client = NULL;
	struct tr_channel *server = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	int sockets[2];

	reset_probe(0U);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	expect_threads(1U, 0U);

	assert(tr_reactor_adopt_fd(reactor, sockets[0], &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sockets[1], &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(
		       &channel_config, client_conn, client_conn,
		       NULL, NULL, NULL, NULL, &client) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create_deferred(
		       &channel_config, server_conn, server_conn,
		       NULL, NULL, NULL, NULL, &server) == TR_OK);
	assert(tr_channel_start(client) == TR_OK);
	assert(tr_channel_start(server) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = 1U;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 40U;
	reconnect_config.connect_timeout_ms = 50U;
	assert(tr_channel_enable_client_reconnect(
		       client, &reconnect_config) == TR_OK);

	/* reconnect 只增加 Reactor timer/connector state，不创建 maintenance pthread。 */
	expect_threads(1U, 0U);

	assert(tr_channel_disable_client_reconnect(client) == TR_OK);
	tr_channel_destroy(client);
	tr_channel_destroy(server);
	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(1U, 1U);
	tr_reactor_destroy(reactor);
}

static void test_server_create_start_destroy_threads(void)
{
	unsigned workers;
	unsigned start;

	for (workers = 1U; workers <= 3U; workers += 2U) {
		for (start = 0U; start <= 1U; ++start) {
			struct tr_server_config config;
			struct tr_facade_tuning tuning;
			struct tr_server *server = NULL;
			unsigned total = workers;

			server_config_init(&config);
			server_tuning_init(&tuning, workers);
			reset_probe(0U);
			assert(tr_server_create_with_tuning(
				       &config, &tuning, &server) == TR_OK);
			assert(server != NULL);
			/* create 只启动 shard-local executor，不启动 Reactor/timer。 */
			expect_threads(workers, 0U);
			if (start) {
				listen_loopback(server);
				expect_threads(workers, 0U);
				assert(tr_server_start(server) == TR_OK);
				total += 1U; /* accept/cleanup 都由 Reactor 事件驱动。 */
				expect_threads(total, 0U);
			}
			tr_server_destroy(server);
			expect_threads(total, total);
		}
	}
}

static void test_server_multi_shard_threads(void)
{
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;

	server_config_init(&config);
	server_tuning_init(&tuning, 4U);
	config.shard_count = 2U;
	config.max_peers = 4U;

	reset_probe(0U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_OK);
	assert(server != NULL);

	/* Four total workers are split across two shard-local executors. */
	expect_threads(4U, 0U);
	listen_loopback(server);
	expect_threads(4U, 0U);

	/* Server start adds exactly one Reactor per shard. */
	assert(tr_server_start(server) == TR_OK);
	expect_threads(6U, 0U);

	tr_server_destroy(server);
	expect_threads(6U, 6U);
}

static void test_server_multi_shard_rejects_undersized_budget(void)
{
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;

	server_config_init(&config);
	server_tuning_init(&tuning, 1U);
	config.shard_count = 2U;
	config.max_peers = 2U;

	reset_probe(0U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_ERR_INVALID);
	assert(server == NULL);
	expect_threads(0U, 0U);
}

static void test_server_internal_tuning_respects_shard_minimum(void)
{
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;

	server_config_init(&config);
	config.shard_count = 2U;
	config.max_peers = 2U;

	server_tuning_init(&tuning, 2U);
	tuning.rx_buffer_count = 1U;

	reset_probe(0U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_ERR_INVALID);
	assert(server == NULL);
	expect_threads(0U, 0U);

	/*
	 * Public create owns hidden tuning defaults and must not expose this
	 * implementation-capacity failure for the same valid semantic config.
	 */
	assert(tr_server_create(&config, &server) == TR_OK);
	assert(server != NULL);
	/* Public hidden default remains four workers. */
	expect_threads(4U, 0U);
	tr_server_destroy(server);
	expect_threads(4U, 4U);
}

static void runtime_multi_shard_config_init(
	struct tr_runtime_config *config,
	struct tr_runtime_shard_config shards[3])
{
	uint32_t i;
	static const uint32_t worker_counts[3] = { 1U, 2U, 1U };

	memset(config, 0, sizeof(*config));
	memset(shards, 0, sizeof(*shards) * 3U);
	config->shard_count = 3U;
	config->shards = shards;

	for (i = 0; i < 3U; ++i) {
		shards[i].peer_capacity = 2U;
		shards[i].rpc_executor.endpoint_capacity = 2U;
		shards[i].rpc_executor.max_calls_per_endpoint = 2U;
		shards[i].rpc_executor.thread_count = worker_counts[i];
	}
}

static void test_runtime_multi_shard_threads(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);
	reset_probe(0U);

	/* Per-shard executor budgets sum to four workers; they are not multiplied. */
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(4U, 0U);

	/* Runtime start adds exactly one Reactor owner per shard. */
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(7U, 0U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(7U, 3U);
	tr_runtime_destroy(runtime);
	expect_threads(7U, 7U);
}

static void test_runtime_multi_shard_start_rollback(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);

	/*
	 * Attempts 1..4 are shard-local executor workers. Attempt 5 starts
	 * Reactor 0; attempt 6 fails Reactor 1. Runtime must join Reactor 0 while
	 * leaving the four executor workers owned by the still-live Runtime.
	 */
	reset_probe(6U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(4U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 6U);
	expect_threads(5U, 1U);

	tr_runtime_destroy(runtime);
	expect_threads(5U, 5U);
}

static void test_client_thread_start_failure(void)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;

	tr_client_config_init(&config);
	small_limits(&config.limits);
	reset_probe(1U);
	assert(tr_client_create(&config, &client) == TR_ERR_SYS);
	assert(client == NULL);
	assert(atomic_load(&create_attempts) == 1U);
	expect_threads(0U, 0U);
}

static void test_server_worker_start_failures(void)
{
	unsigned fail_at;

	for (fail_at = 1U; fail_at <= 3U; ++fail_at) {
		struct tr_server_config config;
		struct tr_facade_tuning tuning;
		struct tr_server *server = NULL;

		server_config_init(&config);
		server_tuning_init(&tuning, 3U);
		reset_probe(fail_at);
		assert(tr_server_create_with_tuning(
			       &config, &tuning, &server) == TR_ERR_SYS);
		assert(server == NULL);
		assert(atomic_load(&create_attempts) == fail_at);
		/* 第 N 个 worker 启动失败，前 N-1 个必须已经退出并 join。 */
		expect_threads(fail_at - 1U, fail_at - 1U);
	}
}

static void test_server_runtime_start_failures(void)
{
	const unsigned workers = 3U;
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;

	/* accept/reaper 均已移除；Server start 唯一 pthread 启动点是 Reactor。 */
	server_config_init(&config);
	server_tuning_init(&tuning, workers);
	reset_probe(workers + 1U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_OK);
	listen_loopback(server);
	assert(tr_server_start(server) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == workers + 1U);
	expect_threads(workers, 0U);
	tr_server_destroy(server);
	expect_threads(workers, workers);
}

#define RUN_TEST(fn) do { fn(); puts(#fn ": ok"); } while (0)

int main(void)
{
	RUN_TEST(test_client_create_destroy_threads);
	RUN_TEST(test_channel_reconnect_uses_no_extra_thread);
	RUN_TEST(test_server_create_start_destroy_threads);
	RUN_TEST(test_server_multi_shard_threads);
	RUN_TEST(test_server_multi_shard_rejects_undersized_budget);
	RUN_TEST(test_server_internal_tuning_respects_shard_minimum);
	RUN_TEST(test_runtime_multi_shard_threads);
	RUN_TEST(test_runtime_multi_shard_start_rollback);
	RUN_TEST(test_client_thread_start_failure);
	RUN_TEST(test_server_worker_start_failures);
	RUN_TEST(test_server_runtime_start_failures);
	reset_probe(0U);
	return 0;
}
