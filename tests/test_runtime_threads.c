#include "tr/client.h"
#include "tr/server.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>

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
	limits->command_capacity = 32U;
	limits->tx_item_capacity = 16U;
	limits->control_tx_item_capacity = 8U;
	limits->rx_buffer_count = 8U;
	limits->rpc_message_pool_count = 16U;
	limits->rpc_message_buffer_bytes = 256U;
	limits->reassembly_pool_count = 8U;
	limits->executor_threads = 1U;
	limits->executor_queue_capacity = 16U;
}

static void server_config_init(struct tr_server_config *config,
			       unsigned workers)
{
	tr_server_config_init(config);
	small_limits(&config->limits);
	config->limits.executor_threads = workers;
	config->max_peers = 2U;
	config->keepalive_interval_ms = 10U;
	config->keepalive_timeout_ms = 50U;
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

static void test_server_create_start_destroy_threads(void)
{
	unsigned workers;
	unsigned start;

	for (workers = 1U; workers <= 3U; workers += 2U) {
		for (start = 0U; start <= 1U; ++start) {
			struct tr_server_config config;
			struct tr_server *server = NULL;
			unsigned total = workers;

			server_config_init(&config, workers);
			reset_probe(0U);
			assert(tr_server_create(&config, &server) == TR_OK);
			assert(server != NULL);
			/* create 只启动共享 executor，不启动 Reactor/timer。 */
			expect_threads(workers, 0U);
			if (start) {
				listen_loopback(server);
				expect_threads(workers, 0U);
				assert(tr_server_start(server) == TR_OK);
				total += 3U; /* Reactor + accept + reaper。 */
				expect_threads(total, 0U);
			}
			tr_server_destroy(server);
			expect_threads(total, total);
		}
	}
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
		struct tr_server *server = NULL;

		server_config_init(&config, 3U);
		reset_probe(fail_at);
		assert(tr_server_create(&config, &server) == TR_ERR_SYS);
		assert(server == NULL);
		assert(atomic_load(&create_attempts) == fail_at);
		/* 第 N 个 worker 启动失败，前 N-1 个必须已经退出并 join。 */
		expect_threads(fail_at - 1U, fail_at - 1U);
	}
}

static void test_server_runtime_start_failures(void)
{
	const unsigned workers = 3U;
	unsigned stage;

	/* 依次让 Reactor、reaper、accept 的 pthread_create 失败。 */
	for (stage = 1U; stage <= 3U; ++stage) {
		struct tr_server_config config;
		struct tr_server *server = NULL;
		unsigned total = workers + stage - 1U;

		server_config_init(&config, workers);
		reset_probe(workers + stage);
		assert(tr_server_create(&config, &server) == TR_OK);
		listen_loopback(server);
		assert(tr_server_start(server) == TR_ERR_SYS);
		assert(atomic_load(&create_attempts) == workers + stage);
		/* start 回滚新增线程，但 executor 仍由存活的 Server 持有。 */
		expect_threads(total, stage - 1U);
		tr_server_destroy(server);
		expect_threads(total, total);
	}
}

#define RUN_TEST(fn) do { fn(); puts(#fn ": ok"); } while (0)

int main(void)
{
	RUN_TEST(test_client_create_destroy_threads);
	RUN_TEST(test_server_create_start_destroy_threads);
	RUN_TEST(test_client_thread_start_failure);
	RUN_TEST(test_server_worker_start_failures);
	RUN_TEST(test_server_runtime_start_failures);
	reset_probe(0U);
	return 0;
}
