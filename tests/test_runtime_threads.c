#include "tr/client.h"
#include "../src/execution/reactor.h"
#include "tr/server.h"
#include "tr/status.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/runtime/runtime_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/execution/buffer.h"
#include "server_v2_test_fixture.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
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

static struct tr_rpc_method_desc runtime_test_method(void)
{
	struct tr_rpc_method_desc method = {0};

	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1U;
	method.max_response_bytes = 1U;
	return method;
}

static void listen_loopback(struct tr_server *server)
{
	uint16_t port = 0;

	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(port != 0);
}

static void destroy_server_until_complete(struct tr_server *server)
{
	unsigned retries = 0U;
	int ret;

	do {
		ret = tr_server_destroy(server);
		if (ret == TR_AGAIN) {
			assert(++retries < 10000U);
			sched_yield();
		}
	} while (ret == TR_AGAIN);
	assert(ret == TR_OK);
}

static void test_client_create_destroy_threads(void)
{
	unsigned iteration;

	for (iteration = 0; iteration < 4U; ++iteration) {
		struct tr_client_config config;
		struct tr_rpc_method_desc method = runtime_test_method();
		struct tr_client *client = NULL;

		tr_client_config_init(&config);
		small_limits(&config.limits);
		config.keepalive_interval_ms = iteration % 2U ? 10U : 0U;
		config.keepalive_timeout_ms = 50U;
		reset_probe(0U);
		assert(tr_client_create(&config, &method, 1U, &client) ==
		       TR_OK);
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
	unsigned start;

	for (start = 0U; start <= 1U; ++start) {
		struct tr_server_config config;
		struct tr_server *server = NULL;
		unsigned total = 0U;

		server_config_init(&config);
		reset_probe(0U);
		assert(tr_test_server_v2_create(&config, &server) == TR_OK);
		assert(server != NULL);
		/* 构造只建立执行器软状态，不启动任何 pthread。 */
		expect_threads(0U, 0U);
		if (start) {
			listen_loopback(server);
			expect_threads(0U, 0U);
			assert(tr_server_start(server) == TR_OK);
			total = 5U;
			/* 四个 V2 任务线程与一个反应器所有者都由 start 建立。 */
			expect_threads(total, 0U);
		}
		destroy_server_until_complete(server);
		expect_threads(total, total);
	}
}

static void test_server_multi_shard_threads(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;

	server_config_init(&config);
	config.shard_count = 2U;
	config.max_peers = 4U;

	reset_probe(0U);
	assert(tr_test_server_v2_create(&config, &server) == TR_OK);
	assert(server != NULL);

	/* 创建与监听都不启动执行器工作线程。 */
	expect_threads(0U, 0U);
	listen_loopback(server);
	expect_threads(0U, 0U);

	/* 启动同时建立四个分片本地工作线程与两个反应器所有者。 */
	assert(tr_server_start(server) == TR_OK);
	expect_threads(6U, 0U);

	destroy_server_until_complete(server);
	expect_threads(6U, 6U);
}

static void test_server_rejects_invalid_public_limits(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;

	server_config_init(&config);
	config.limits.max_message_bytes = 1U;

	reset_probe(0U);
	assert(tr_test_server_v2_create(&config, &server) == TR_ERR_INVALID);
	assert(server == NULL);
	expect_threads(0U, 0U);
}

static void runtime_multi_shard_config_init(
	struct tr_runtime_config *config,
	struct tr_runtime_shard_config shards[3])
{
	memset(config, 0, sizeof(*config));
	memset(shards, 0, sizeof(*shards) * 3U);
	config->shard_count = 3U;
	config->shards = shards;

	for (uint32_t i = 0; i < 3U; ++i)
		shards[i].peer_capacity = 2U;
}

/* Runtime 只管理 Reactor 与显式启用的消息服务；无消息服务时不应
 * 创建额外工作线程。此处用真实 Runtime 和 pthread 计数检查。
 */
static void test_runtime_without_legacy_execution(void)
{
	struct tr_runtime_shard_config shard = {0};
	struct tr_runtime_config config = {0};
	struct tr_runtime *runtime = NULL;

	config.shard_count = 1U;
	config.shards = &shard;
	reset_probe(0U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(1U, 0U);
	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(1U, 1U);
	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(1U, 1U);
}

static void test_runtime_multi_shard_threads(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);
	reset_probe(0U);

	/* 创建只分配分片与反应器软状态。 */
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	/* Runtime 只为每个分片创建一个反应器所有者。 */
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(3U, 0U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(3U, 3U);
	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(3U, 3U);
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

	/* 分片 0 的反应器创建成功，分片 1 的反应器创建失败；此前启动的
	 * 分片必须在 start() 返回前完整回滚。
	 */
	reset_probe(2U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 2U);
	expect_threads(1U, 1U);

	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(1U, 1U);
}

static void test_runtime_start_rollback_join_failure_is_retryable(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;

	runtime_multi_shard_config_init(&config, shards);

	/* 分片 1 反应器创建失败后开始回滚；分片 0 的反应器回收失败。
	 * Runtime 必须保留该轮次，且下一次启动在创建任何新线程之前，
	 * 就因可启动性预检返回 TR_ERR_STATE。
	 */
	reset_probe(2U);
	fail_join_once_at(1U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	expect_threads(0U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_SYS);
	assert(atomic_load(&create_attempts) == 2U);
	/* 分片 0 的反应器等待重试回收。 */
	expect_threads(1U, 0U);

	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(atomic_load(&create_attempts) == 2U);

	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(1U, 1U);

	/* 完整收敛后允许开启全新的工作线程与反应器轮次。 */
	reset_probe(0U);
	assert(tr_runtime_start(runtime) == TR_OK);
	expect_threads(3U, 0U);
	assert(tr_runtime_stop(runtime) == TR_OK);
	expect_threads(3U, 3U);

	assert(tr_runtime_destroy(runtime) == TR_OK);
	expect_threads(3U, 3U);
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

static void test_client_thread_start_failure(void)
{
	struct tr_client_config config;
	struct tr_rpc_method_desc method = runtime_test_method();
	struct tr_client *client = NULL;

	tr_client_config_init(&config);
	small_limits(&config.limits);
	reset_probe(1U);
	assert(tr_client_create(&config, &method, 1U, &client) == TR_ERR_SYS);
	assert(client == NULL);
	assert(atomic_load(&create_attempts) == 1U);
	expect_threads(0U, 0U);
}

static void test_server_worker_start_failures(void)
{
	unsigned fail_at;

	for (fail_at = 1U; fail_at <= 5U; ++fail_at) {
		struct tr_server_config config;
		struct tr_server *server = NULL;

		server_config_init(&config);
		reset_probe(fail_at);
		assert(tr_test_server_v2_create(&config, &server) == TR_OK);
		assert(server != NULL);
		expect_threads(0U, 0U);
		listen_loopback(server);

		int start_ret = tr_server_start(server);
		assert(start_ret == TR_ERR_SYS || start_ret == TR_AGAIN);
		if (fail_at == 1U)
			assert(start_ret == TR_ERR_SYS);
		assert(atomic_load(&create_attempts) == fail_at);
		/* V2 启动先创建每分片 Reactor，再创建共享任务线程；失败后的
		 * 清理可能先返回 AGAIN，Server destroy 必须继续回收此前成功创建的线程。
		 */
		destroy_server_until_complete(server);
		expect_threads(fail_at - 1U, fail_at - 1U);
	}
}

#define RUN_TEST(fn) do { fn(); puts(#fn ": ok"); } while (0)

int main(void)
{
	RUN_TEST(test_client_create_destroy_threads);
	RUN_TEST(test_channel_reconnect_uses_no_extra_thread);
	RUN_TEST(test_server_create_start_destroy_threads);
	RUN_TEST(test_server_multi_shard_threads);
	RUN_TEST(test_server_rejects_invalid_public_limits);
	RUN_TEST(test_runtime_without_legacy_execution);
	RUN_TEST(test_runtime_multi_shard_threads);
	RUN_TEST(test_runtime_control_lifecycle_serialized);
	RUN_TEST(test_reactor_join_failure_is_retryable);
	RUN_TEST(test_reactor_destroy_join_failure_is_retryable);
	RUN_TEST(test_runtime_multi_shard_start_rollback);
	RUN_TEST(test_runtime_start_rollback_join_failure_is_retryable);
	RUN_TEST(test_standalone_rpc_executor_join_failure_is_retryable);
	RUN_TEST(test_standalone_rpc_start_rollback_keeps_owner);
	RUN_TEST(test_client_thread_start_failure);
	RUN_TEST(test_server_worker_start_failures);
	reset_probe(0U);
	return 0;
}
