#include "tr/client.h"
#include "../src/execution/reactor.h"
#include "tr/server.h"
#include "tr/status.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/runtime/runtime_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/execution/buffer.h"
#include "../src/facade_tuning_internal.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * 仅此测试通过链接器 --wrap 观察库对 pthread 的调用。计数不包含宿主或
 * sanitizer 的后台线程，也不依赖 /proc 采样、sleep 或生产代码测试钩子。
 * reset 仅在本轮全部线程 join 后执行；原子计数允许未来从不同线程回收。
 */
static atomic_uint create_attempts;
static atomic_uint created_threads;
static atomic_uint join_attempts;
static atomic_uint joined_threads;
static atomic_uint join_failures;
static atomic_uint fail_create_at;
static atomic_uint fail_join_at;

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
	unsigned attempt = atomic_fetch_add(&join_attempts, 1U) + 1U;
	int ret;

	if (attempt == atomic_load(&fail_join_at)) {
		atomic_fetch_add(&join_failures, 1U);
		return EINVAL;
	}

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
	unsigned actual_join_failures = atomic_load(&join_failures);

	if (actual_created != created || actual_joined != joined ||
	    actual_joins != actual_joined + actual_join_failures)
		fprintf(stderr,
			"threads: created=%u joined=%u join_attempts=%u "
			"join_failures=%u; expected created=%u joined=%u\n",
			actual_created, actual_joined, actual_joins,
			actual_join_failures, created, joined);
	assert(actual_created == created);
	assert(actual_joined == joined);
	assert(actual_joins == actual_joined + actual_join_failures);
}

static void reset_probe(unsigned fail_at)
{
	/* 成功创建必须最终一一对应成功 join；失败 join 只允许显式 retry。 */
	expect_threads(atomic_load(&created_threads),
		       atomic_load(&created_threads));
	atomic_store(&create_attempts, 0U);
	atomic_store(&created_threads, 0U);
	atomic_store(&join_attempts, 0U);
	atomic_store(&joined_threads, 0U);
	atomic_store(&join_failures, 0U);
	atomic_store(&fail_create_at, fail_at);
	atomic_store(&fail_join_at, 0U);
}

static void fail_join_once_at(unsigned attempt)
{
	assert(attempt != 0U);
	atomic_store(&fail_join_at, attempt);
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
		assert(tr_client_destroy(client) == TR_OK);
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
	assert(tr_channel_destroy(client) == TR_OK);
	assert(tr_channel_destroy(server) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(1U, 1U);
	assert(tr_reactor_destroy(reactor) == TR_OK);
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
			unsigned total = 0U;

			server_config_init(&config);
			server_tuning_init(&tuning, workers);
			reset_probe(0U);
			assert(tr_server_create_with_tuning(
				       &config, &tuning, &server) == TR_OK);
			assert(server != NULL);
			/* create 只建立 executor soft-state，不启动任何 pthread。 */
			expect_threads(0U, 0U);
			if (start) {
				listen_loopback(server);
				expect_threads(0U, 0U);
				assert(tr_server_start(server) == TR_OK);
				total = workers + 1U;
				/* worker epoch 与唯一 Reactor owner 都由 start() 建立。 */
				expect_threads(total, 0U);
			}
			assert(tr_server_destroy(server) == TR_OK);
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

	/* create/listen 都不启动 executor worker。 */
	expect_threads(0U, 0U);
	listen_loopback(server);
	expect_threads(0U, 0U);

	/* start 同时建立四个 shard-local worker 与两个 Reactor owner。 */
	assert(tr_server_start(server) == TR_OK);
	expect_threads(6U, 0U);

	assert(tr_server_destroy(server) == TR_OK);
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
	expect_threads(0U, 0U);
	listen_loopback(server);
	assert(tr_server_start(server) == TR_OK);
	/* Public hidden default remains four workers + two shard Reactor owner。 */
	expect_threads(6U, 0U);
	assert(tr_server_destroy(server) == TR_OK);
	expect_threads(6U, 6U);
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

	/* create 只分配 shard/executor soft-state。 */
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	/* Runtime start 建立四个 worker + 三个 Reactor owner。 */
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(7U, 0U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(7U, 7U);
	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(7U, 7U);
}

static void test_reactor_join_failure_is_retryable(void)
{
	struct tr_reactor *reactor = NULL;

	reset_probe(0U);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	expect_threads(1U, 0U);

	/*
	 * STOP is already accepted before pthread_join(). A failed join must keep
	 * the exact owner thread handle and retry only the join barrier, never queue
	 * a second STOP to an owner that may already have exited.
	 */
	fail_join_once_at(1U);
	assert(tr_reactor_stop(reactor) == TR_ERR_SYS);
	expect_threads(1U, 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(1U, 1U);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static void test_runtime_multi_shard_start_rollback(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);

	/*
	 * shard0: worker(1), Reactor(2)；shard1: worker(3,4), Reactor(5)。
	 * 第 5 次 pthread_create 失败后，当前与此前 shard 的 worker/Reactor
	 * epoch 都必须在 start() 返回前完整 rollback。
	 */
	reset_probe(5U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 5U);
	expect_threads(4U, 4U);

	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(4U, 4U);
}

static void test_runtime_start_rollback_join_failure_is_retryable(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);

	/*
	 * shard1 Reactor 创建失败后开始 rollback；再让 shard1 第一个 worker join
	 * 失败。Runtime 必须保留该 group epoch，且下一次 start 在创建任何新线程
	 * 之前就因 startability preflight 返回 TR_ERR_STATE。
	 */
	reset_probe(5U);
	fail_join_once_at(1U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 5U);
	/* shard0 Reactor+worker 已收敛，shard1 两个 worker 等待 retry join。 */
	expect_threads(4U, 2U);

	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(atomic_load(&create_attempts) == 5U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(4U, 4U);

	/* 完整收敛后允许开启全新的 worker/Reactor epoch。 */
	reset_probe(0U);
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(7U, 0U);
	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(7U, 7U);

	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(7U, 7U);
}

static void test_runtime_executor_group_join_failure_is_retryable(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard;
	struct tr_runtime *runtime = NULL;

	memset(&config, 0, sizeof(config));
	memset(&shard, 0, sizeof(shard));
	config.shard_count = 1U;
	config.shards = &shard;
	shard.rpc_executor.endpoint_capacity = 3U;
	shard.rpc_executor.max_calls_per_endpoint = 2U;
	shard.rpc_executor.thread_count = 3U;

	reset_probe(0U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);
	assert(tr_runtime_start(runtime) == TR_OK);
	/* 三个 group worker + 一个 Reactor owner。 */
	expect_threads(4U, 0U);

	/*
	 * destroy 先 join Reactor，再 join worker[0]，随后 worker[1] 第一次 join
	 * 被注入失败。retry 必须从 worker[1] 继续，不能重复 join 已收敛线程。
	 */
	fail_join_once_at(3U);
	assert(tr_runtime_destroy(runtime) == TR_ERR_SYS);
	expect_threads(4U, 2U);

	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(4U, 4U);
}

static void test_standalone_rpc_executor_join_failure_is_retryable(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_channel_config channel_config;
	struct tr_channel *channel = NULL;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_endpoint *endpoint = NULL;
	struct tr_buffer_pool message_pool;
	struct tr_conn_handle connection;
	int sockets[2];

	memset(&message_pool, 0, sizeof(message_pool));
	reset_probe(0U);
	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sockets[0], &connection) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(
		       &channel_config, connection, connection,
		       NULL, NULL, NULL, NULL, &channel) == TR_OK);

	assert(tr_buffer_pool_init(&message_pool, 8U, 1024U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 2U;
	rpc_config.message_pool = &message_pool;
	rpc_config.executor_threads = 2U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create_with_executor_group(
		       channel, &rpc_config, NULL, &endpoint) == TR_OK);
	assert(endpoint != NULL);
	/* 一个 Reactor owner + 两个 standalone RPC worker。 */
	expect_threads(3U, 0U);

	/*
	 * 第一个 RPC worker join 成功，第二个 join 失败。Endpoint 必须保持所有权，
	 * 重试时只 join 第二个 worker，不能释放 mutex/queue/Endpoint storage。
	 */
	fail_join_once_at(2U);
	assert(tr_rpc_endpoint_destroy(endpoint) == TR_ERR_SYS);
	expect_threads(3U, 1U);

	assert(tr_rpc_endpoint_destroy(endpoint) == TR_OK);
	endpoint = NULL;
	expect_threads(3U, 2U);

	assert(tr_channel_destroy(channel) == TR_OK);
	channel = NULL;
	assert(tr_buffer_pool_destroy(&message_pool) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(3U, 3U);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(sockets[1]) == 0);
}

static void test_standalone_rpc_start_rollback_keeps_owner(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_channel_config channel_config;
	struct tr_channel *channel = NULL;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_endpoint *endpoint = NULL;
	struct tr_buffer_pool message_pool;
	struct tr_conn_handle connection;
	int sockets[2];

	memset(&message_pool, 0, sizeof(message_pool));
	reset_probe(0U);
	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sockets[0], &connection) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(
		       &channel_config, connection, connection,
		       NULL, NULL, NULL, NULL, &channel) == TR_OK);
	assert(tr_buffer_pool_init(&message_pool, 8U, 1024U) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 2U;
	rpc_config.message_pool = &message_pool;
	rpc_config.executor_threads = 2U;
	rpc_config.executor_queue_capacity = 16U;

	/*
	 * Reactor 已占第 1 次 pthread_create。让第 2 个 RPC worker（全局第 3 次）
	 * 创建失败，再让 startup rollback 的第 1 次 join 失败。
	 *
	 * Endpoint publication 已完成且 ownership 已先放进 *out，因此 lifecycle
	 * rollback 失败时必须保留 endpoint != NULL，绝不能泄漏无主对象。
	 */
	atomic_store(&fail_create_at, 3U);
	fail_join_once_at(1U);
	assert(tr_rpc_endpoint_create_with_executor_group(
		       channel, &rpc_config, NULL, &endpoint) == TR_ERR_SYS);
	assert(endpoint != NULL);
	expect_threads(2U, 0U);

	/* join fault 已消费；caller 用保留的 ownership 重试即可完成收敛。 */
	assert(tr_rpc_endpoint_destroy(endpoint) == TR_OK);
	endpoint = NULL;
	expect_threads(2U, 1U);

	assert(tr_channel_destroy(channel) == TR_OK);
	assert(tr_buffer_pool_destroy(&message_pool) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(2U, 2U);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(sockets[1]) == 0);
}

static void test_facade_internal_tuning_rejects_invalid_observability(void)
{
	struct tr_client_config client_config;
	struct tr_server_config server_config;
	struct tr_facade_tuning tuning;
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;

	tr_client_config_init(&client_config);
	small_limits(&client_config.limits);
	tr_facade_tuning_init(&tuning);
	tuning.observability_flags = UINT32_C(0x80000000);

	reset_probe(0U);
	assert(tr_client_create_with_tuning(
		       &client_config, &tuning, &client) == TR_ERR_INVALID);
	assert(client == NULL);
	expect_threads(0U, 0U);

	tr_server_config_init(&server_config);
	small_limits(&server_config.limits);
	tr_facade_tuning_init(&tuning);
	tuning.observability_flags = UINT32_C(0x80000000);

	reset_probe(0U);
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_ERR_INVALID);
	assert(server == NULL);
	expect_threads(0U, 0U);
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
			       &config, &tuning, &server) == TR_OK);
		assert(server != NULL);
		expect_threads(0U, 0U);
		listen_loopback(server);

		assert(tr_server_start(server) == TR_ERR_SYS);
		assert(atomic_load(&create_attempts) == fail_at);
		/* 第 N 个 worker 启动失败，前 N-1 个在 start rollback 中完成 join。 */
		expect_threads(fail_at - 1U, fail_at - 1U);
		assert(tr_server_destroy(server) == TR_OK);
		expect_threads(fail_at - 1U, fail_at - 1U);
	}
}

static void test_server_runtime_start_failures(void)
{
	const unsigned workers = 3U;
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;

	/* 三个 worker 成功后，第 4 次 pthread_create（Reactor）失败。 */
	server_config_init(&config);
	server_tuning_init(&tuning, workers);
	reset_probe(workers + 1U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_OK);
	expect_threads(0U, 0U);
	listen_loopback(server);
	assert(tr_server_start(server) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == workers + 1U);
	/* Runtime start rollback 已经 join 三个 worker。 */
	expect_threads(workers, workers);
	assert(tr_server_destroy(server) == TR_OK);
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
	RUN_TEST(test_reactor_join_failure_is_retryable);
	RUN_TEST(test_runtime_multi_shard_start_rollback);
	RUN_TEST(test_runtime_start_rollback_join_failure_is_retryable);
	RUN_TEST(test_runtime_executor_group_join_failure_is_retryable);
	RUN_TEST(test_standalone_rpc_executor_join_failure_is_retryable);
	RUN_TEST(test_standalone_rpc_start_rollback_keeps_owner);
	RUN_TEST(test_facade_internal_tuning_rejects_invalid_observability);
	RUN_TEST(test_client_thread_start_failure);
	RUN_TEST(test_server_worker_start_failures);
	RUN_TEST(test_server_runtime_start_failures);
	reset_probe(0U);
	return 0;
}
