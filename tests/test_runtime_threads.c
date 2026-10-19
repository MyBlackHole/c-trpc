#include "../src/facade_legacy_internal.h"
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
#include <time.h>
#include <sys/socket.h>
#include <unistd.h>

/* 仅此测试通过链接器 --wrap 观察库对 pthread 的调用。计数不包含宿主或
 * Sanitizer 的后台线程，也不依赖 /proc 采样、休眠或生产代码测试钩子。
 * 重置仅在本轮全部线程回收后执行；原子计数允许未来从不同线程回收。
 */
static atomic_uint create_attempts;
static atomic_uint created_threads;
static atomic_uint join_attempts;
static atomic_uint joined_threads;
static atomic_uint join_failures;
static atomic_uint fail_create_at;
static atomic_uint fail_join_at;

/* 仅测试目标：在 Runtime 控制操作跨过生命周期门禁后，
 * 于真实 pthread_create/join 边界冻结调用方，验证另一个控制者拒绝。
 */
struct runtime_control_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int open;
};

static struct runtime_control_gate create_gate, join_gate;
static _Thread_local int hold_runtime_create, hold_runtime_join;

static void runtime_gate_enter(struct runtime_control_gate *gate)
{
	pthread_mutex_lock(&gate->lock);
	gate->entered = 1;
	pthread_cond_broadcast(&gate->cond);
	while (!gate->open)
		pthread_cond_wait(&gate->cond, &gate->lock);
	pthread_mutex_unlock(&gate->lock);
}

static void runtime_gate_wait(struct runtime_control_gate *gate)
{
	pthread_mutex_t *lock = &gate->lock;
	pthread_cond_t *cond = &gate->cond;
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(lock);
	while (!gate->entered && ret == 0)
		ret = pthread_cond_timedwait(cond, lock, &deadline);
	assert(gate->entered);
	pthread_mutex_unlock(lock);
}

static void runtime_gate_open(struct runtime_control_gate *gate)
{
	pthread_mutex_lock(&gate->lock);
	assert(gate->entered && !gate->open);
	gate->open = 1;
	pthread_cond_broadcast(&gate->cond);
	pthread_mutex_unlock(&gate->lock);
}

static void runtime_gate_rearm(struct runtime_control_gate *gate)
{
	pthread_mutex_lock(&gate->lock);
	assert(gate->entered && gate->open);
	gate->entered = 0;
	gate->open = 0;
	pthread_mutex_unlock(&gate->lock);
}

static void runtime_gate_init(struct runtime_control_gate *gate)
{
	memset(gate, 0, sizeof(*gate));
	assert(pthread_mutex_init(&gate->lock, NULL) == 0);
	assert(pthread_cond_init(&gate->cond, NULL) == 0);
}

static void runtime_gate_destroy(struct runtime_control_gate *gate)
{
	assert(pthread_cond_destroy(&gate->cond) == 0);
	assert(pthread_mutex_destroy(&gate->lock) == 0);
}

int __real_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
			 void *(*start)(void *), void *arg);
int __real_pthread_join(pthread_t thread, void **result);

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
			 void *(*start)(void *), void *arg)
{
	unsigned attempt = atomic_fetch_add(&create_attempts, 1U) + 1U;
	int ret;

	if (hold_runtime_create) {
		hold_runtime_create = 0;
		runtime_gate_enter(&create_gate);
	}
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

	if (hold_runtime_join) {
		hold_runtime_join = 0;
		runtime_gate_enter(&join_gate);
	}
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
	/* 成功创建必须最终一一对应成功回收；回收失败只允许显式重试。 */
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
		assert(tr_client_legacy_create(&config, &client) == TR_OK);
		assert(client != NULL);
		/* 未连接时只有反应器，没有闲置的维护线程。 */
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

	/* 重连只增加反应器定时器与连接器状态，不创建维护 pthread。 */
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
			/* 创建只建立执行器软状态，不启动任何 pthread。 */
			expect_threads(0U, 0U);
			if (start) {
				listen_loopback(server);
				expect_threads(0U, 0U);
				assert(tr_server_start(server) == TR_OK);
				total = workers + 1U;
				/* 工作线程轮次与唯一反应器所有者都由 start()
				 * 建立。 */
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

	/* 创建与监听都不启动执行器工作线程。 */
	expect_threads(0U, 0U);
	listen_loopback(server);
	expect_threads(0U, 0U);

	/* 启动同时建立四个分片本地工作线程与两个反应器所有者。 */
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

	/* 公开创建拥有隐藏调优默认值；对于相同的有效语义配置，
	 * 不得暴露此实现容量失败。
	 */
	assert(tr_server_legacy_create(&config, &server) == TR_OK);
	assert(server != NULL);
	expect_threads(0U, 0U);
	listen_loopback(server);
	assert(tr_server_start(server) == TR_OK);
	/* 公开隐藏默认值仍为四个工作线程与两个分片反应器所有者。 */
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

	/* 创建只分配分片与执行器软状态。 */
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	/* 运行时启动建立四个工作线程与三个反应器所有者。 */
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(7U, 0U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(7U, 7U);
	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(7U, 7U);
}

struct runtime_control_probe {
	struct tr_runtime *runtime;
	int result;
};

static void *runtime_control_start(void *arg)
{
	struct runtime_control_probe *probe = arg;

	hold_runtime_create = 1;
	probe->result = tr_runtime_start(probe->runtime);
	return NULL;
}

static void *runtime_control_stop(void *arg)
{
	struct runtime_control_probe *probe = arg;

	hold_runtime_join = 1;
	probe->result = tr_runtime_stop(probe->runtime);
	return NULL;
}

static void *runtime_control_destroy(void *arg)
{
	struct runtime_control_probe *probe = arg;

	hold_runtime_join = 1;
	probe->result = tr_runtime_destroy(probe->runtime);
	return NULL;
}

static void test_runtime_control_lifecycle_serialized(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard;
	struct tr_runtime *runtime = NULL;
	struct runtime_control_probe probe;
	pthread_t controller;
	unsigned created;
	int ret;
	void *(*destroy_fn)(void *) = runtime_control_destroy;

	memset(&config, 0, sizeof(config));
	memset(&shard, 0, sizeof(shard));
	config.shard_count = 1U;
	config.shards = &shard;
	shard.rpc_executor.endpoint_capacity = 2U;
	shard.rpc_executor.max_calls_per_endpoint = 2U;
	shard.rpc_executor.thread_count = 2U;

	reset_probe(0U);
	runtime_gate_init(&create_gate);
	runtime_gate_init(&join_gate);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	probe.runtime = runtime;
	probe.result = TR_ERR_STATE;

	/* 一条控制线程已经进入启动轮次，但尚未创建第一条工作线程。 */
	ret = pthread_create(&controller, NULL, runtime_control_start, &probe);
	assert(ret == 0);
	runtime_gate_wait(&create_gate);
	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(tr_runtime_stop(runtime) == TR_ERR_STATE);
	assert(tr_runtime_destroy(runtime) == TR_ERR_STATE);
	runtime_gate_open(&create_gate);
	assert(pthread_join(controller, NULL) == 0);
	assert(probe.result == TR_OK);

	/* 当前控制线程已进入停止轮次，真实线程仍未完成 join。 */
	probe.result = TR_ERR_STATE;
	ret = pthread_create(&controller, NULL, runtime_control_stop, &probe);
	assert(ret == 0);
	runtime_gate_wait(&join_gate);
	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(tr_runtime_stop(runtime) == TR_ERR_STATE);
	assert(tr_runtime_destroy(runtime) == TR_ERR_STATE);
	runtime_gate_open(&join_gate);
	assert(pthread_join(controller, NULL) == 0);
	assert(probe.result == TR_OK);

	/* stop 返回后可以重新启动，最终 destroy 必须继续持有整个
	 * 生命周期门禁直到线程收敛与分片释放，不能让其他控制者插入。
	 */
	assert(tr_runtime_start(runtime) == TR_OK);
	runtime_gate_rearm(&join_gate);
	probe.result = TR_ERR_STATE;
	ret = pthread_create(&controller, NULL, destroy_fn, &probe);
	assert(ret == 0);
	runtime_gate_wait(&join_gate);
	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(tr_runtime_stop(runtime) == TR_ERR_STATE);
	assert(tr_runtime_destroy(runtime) == TR_ERR_STATE);
	runtime_gate_open(&join_gate);
	assert(pthread_join(controller, NULL) == 0);
	assert(probe.result == TR_OK);
	/* probe.runtime 指向的存储已由 destroy 释放，禁止再次访问。 */
	runtime = NULL;
	runtime_gate_destroy(&join_gate);
	runtime_gate_destroy(&create_gate);
	created = atomic_load(&created_threads);
	expect_threads(created, created);
}

static void test_reactor_join_failure_is_retryable(void)
{
	struct tr_reactor *reactor = NULL;

	reset_probe(0U);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	expect_threads(1U, 0U);

	/* pthread_join() 前 STOP 已被接受。线程回收失败必须保留精确的
	 * 所有者线程句柄，只重试回收屏障，绝不向可能已经退出的所有者
	 * 排入第二个 STOP。
	 */
	fail_join_once_at(1U);
	assert(tr_reactor_stop(reactor) == TR_ERR_SYS);
	expect_threads(1U, 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	expect_threads(1U, 1U);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static void test_reactor_destroy_join_failure_is_retryable(void)
{
	struct tr_reactor *reactor = NULL;

	reset_probe(0U);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	expect_threads(1U, 0U);

	/* 销毁内部必须把停止或线程回收失败当作正常可返回的生命周期错误。
	 * 第一次线程回收失败时对象仍由调用者拥有；第二次销毁重试同一屏障。
	 */
	fail_join_once_at(1U);
	assert(tr_reactor_destroy(reactor) == TR_ERR_SYS);
	expect_threads(1U, 0U);

	assert(tr_reactor_destroy(reactor) == TR_OK);
	expect_threads(1U, 1U);
}

static void test_runtime_multi_shard_start_rollback(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);

	/* 分片 0：工作线程(1)、反应器(2)；分片 1：工作线程(3,4)、反应器(5)。
	 * 第 5 次 pthread_create 失败后，当前与此前分片的工作线程和反应器
	 * 轮次都必须在 start() 返回前完整回滚。
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

	/* 分片 1 反应器创建失败后开始回滚；再让分片 1 第一个工作线程回收失败。
	 * Runtime 必须保留该组轮次，且下一次启动在创建任何新线程之前，
	 * 就因可启动性预检返回 TR_ERR_STATE。
	 */
	reset_probe(5U);
	fail_join_once_at(1U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 5U);
	/* 分片 0 反应器与工作线程已收敛，分片 1 两个工作线程等待重试回收。 */
	expect_threads(4U, 2U);

	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(atomic_load(&create_attempts) == 5U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(4U, 4U);

	/* 完整收敛后允许开启全新的工作线程与反应器轮次。 */
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
	/* 三个组工作线程与一个反应器所有者。 */
	expect_threads(4U, 0U);

	/* 销毁先回收反应器，再回收 worker[0]，随后 worker[1] 第一次回收
	 * 被注入失败。重试必须从 worker[1] 继续，不能重复回收已收敛线程。
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
	/* 一个反应器所有者与两个独立 RPC 工作线程。 */
	expect_threads(3U, 0U);

	/* 第一个 RPC 工作线程回收成功，第二个回收失败。
	 * Endpoint 必须保持所有权，重试时只回收第二个工作线程。
	 * 不能释放互斥锁、队列或 Endpoint 存储。
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

	/* 反应器已占第 1 次 pthread_create。让第 2 个 RPC 工作线程（全局第 3
	 * 次） 创建失败，再让启动回滚的第 1 次线程回收失败。
	 *
	 * Endpoint 发布已完成且所有权已先放进 *out，因此生命周期
	 * 回滚失败时必须保留 endpoint != NULL，绝不能泄漏无主对象。
	 */
	atomic_store(&fail_create_at, 3U);
	fail_join_once_at(1U);
	assert(tr_rpc_endpoint_create_with_executor_group(
		       channel, &rpc_config, NULL, &endpoint) == TR_ERR_SYS);
	assert(endpoint != NULL);
	expect_threads(2U, 0U);

	/* 线程回收故障已消耗；调用者用保留的所有权重试即可完成收敛。 */
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
	assert(tr_client_legacy_create(&config, &client) == TR_ERR_SYS);
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
		/* 第 N 个工作线程启动失败，前 N-1 个在启动回滚中完成回收。 */
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

	/* 三个工作线程成功后，第 4 次 pthread_create（反应器）失败。 */
	server_config_init(&config);
	server_tuning_init(&tuning, workers);
	reset_probe(workers + 1U);
	assert(tr_server_create_with_tuning(
		       &config, &tuning, &server) == TR_OK);
	expect_threads(0U, 0U);
	listen_loopback(server);
	assert(tr_server_start(server) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == workers + 1U);
	/* 运行时启动回滚已经回收三个工作线程。 */
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
	RUN_TEST(test_runtime_control_lifecycle_serialized);
	RUN_TEST(test_reactor_join_failure_is_retryable);
	RUN_TEST(test_reactor_destroy_join_failure_is_retryable);
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
