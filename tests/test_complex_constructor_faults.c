/* 复杂构造器在实际分配与同步初始化失败时的所有权回滚。
 * 故障仅由本测试二进制的链接器 --wrap 提供，不进入生产库。
 */
#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/memory_budget.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_OWNED 48U

struct fault_probe {
	int active;
	unsigned fail_alloc_at;
	unsigned fail_mutex_at;
	unsigned fail_cond_at;
	unsigned alloc_calls;
	unsigned alloc_failures;
	unsigned alloc_successes;
	unsigned freed;
	unsigned mutex_calls;
	unsigned mutex_failures;
	unsigned mutex_successes;
	unsigned mutex_destroys;
	unsigned cond_calls;
	unsigned cond_failures;
	unsigned cond_successes;
	unsigned cond_destroys;
	void *owned[MAX_OWNED];
};

static _Thread_local struct fault_probe probe;

void *__real_calloc(size_t count, size_t size);
void *__real_malloc(size_t size);
void __real_free(void *ptr);
int __real_pthread_mutex_init(pthread_mutex_t *lock,
			     const pthread_mutexattr_t *attr);
int __real_pthread_mutex_destroy(pthread_mutex_t *lock);
int __real_pthread_cond_init(pthread_cond_t *cond,
			    const pthread_condattr_t *attr);
int __real_pthread_cond_destroy(pthread_cond_t *cond);

static void track_alloc(void *ptr)
{
	unsigned i;

	if (!probe.active || !ptr)
		return;
	for (i = 0; i < MAX_OWNED; ++i) {
		if (!probe.owned[i]) {
			probe.owned[i] = ptr;
			probe.alloc_successes++;
			return;
		}
	}
	assert(!"constructor fault ledger exhausted");
}

static void track_free(void *ptr)
{
	unsigned i;

	if (!probe.active || !ptr)
		return;
	for (i = 0; i < MAX_OWNED; ++i) {
		if (probe.owned[i] == ptr) {
			probe.owned[i] = NULL;
			probe.freed++;
			return;
		}
	}
	assert(!"unexpected or duplicate constructor free");
}

void *__wrap_calloc(size_t count, size_t size)
{
	void *ptr;

	if (probe.active && ++probe.alloc_calls == probe.fail_alloc_at) {
		probe.alloc_failures++;
		errno = ENOMEM;
		return NULL;
	}
	ptr = __real_calloc(count, size);
	track_alloc(ptr);
	return ptr;
}

void *__wrap_malloc(size_t size)
{
	void *ptr;

	if (probe.active && ++probe.alloc_calls == probe.fail_alloc_at) {
		probe.alloc_failures++;
		errno = ENOMEM;
		return NULL;
	}
	ptr = __real_malloc(size);
	track_alloc(ptr);
	return ptr;
}

void __wrap_free(void *ptr)
{
	track_free(ptr);
	__real_free(ptr);
}

int __wrap_pthread_mutex_init(pthread_mutex_t *lock,
			      const pthread_mutexattr_t *attr)
{
	int ret;

	if (probe.active && ++probe.mutex_calls == probe.fail_mutex_at) {
		probe.mutex_failures++;
		return EAGAIN;
	}
	ret = __real_pthread_mutex_init(lock, attr);
	if (probe.active && ret == 0)
		probe.mutex_successes++;
	return ret;
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *lock)
{
	int ret = __real_pthread_mutex_destroy(lock);

	if (probe.active && ret == 0)
		probe.mutex_destroys++;
	return ret;
}

int __wrap_pthread_cond_init(pthread_cond_t *cond,
			     const pthread_condattr_t *attr)
{
	int ret;

	if (probe.active && ++probe.cond_calls == probe.fail_cond_at) {
		probe.cond_failures++;
		return EAGAIN;
	}
	ret = __real_pthread_cond_init(cond, attr);
	if (probe.active && ret == 0)
		probe.cond_successes++;
	return ret;
}

int __wrap_pthread_cond_destroy(pthread_cond_t *cond)
{
	int ret = __real_pthread_cond_destroy(cond);

	if (probe.active && ret == 0)
		probe.cond_destroys++;
	return ret;
}

static void begin_fault(unsigned alloc_at, unsigned mutex_at,
			unsigned cond_at)
{
	assert(!probe.active);
	memset(&probe, 0, sizeof(probe));
	probe.fail_alloc_at = alloc_at;
	probe.fail_mutex_at = mutex_at;
	probe.fail_cond_at = cond_at;
	probe.active = 1;
}

static void finish_fault(void)
{
	unsigned i;

	assert(probe.active);
	/* 必须实际命中要求注入的失败，而不是执行正常路径后误报通过。 */
	assert(probe.alloc_failures == (probe.fail_alloc_at != 0U));
	assert(probe.mutex_failures == (probe.fail_mutex_at != 0U));
	assert(probe.cond_failures == (probe.fail_cond_at != 0U));
	assert(probe.alloc_successes == probe.freed);
	assert(probe.mutex_successes == probe.mutex_destroys);
	assert(probe.cond_successes == probe.cond_destroys);
	for (i = 0; i < MAX_OWNED; ++i)
		assert(probe.owned[i] == NULL);
	probe.active = 0;
}

static unsigned fd_count(void)
{
	DIR *dir = opendir("/proc/self/fd");
	struct dirent *entry;
	unsigned count = 0U;

	assert(dir);
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, ".."))
			count++;
	}
	assert(closedir(dir) == 0);
	return count;
}

static void assert_budget_empty(struct tr_memory_budget *budget)
{
	struct tr_memory_budget_stats stats;

	tr_memory_budget_get_stats(budget, &stats);
	assert(stats.current_bytes == 0U);
}

/* 反应器控制锁、命令队列锁、完成队列锁依次占前三次初始化。
 * 两个 TX 池分别是第 4、5 次 mutex_init；第 8、9 次分配对应
 * 两个池的实际存储。故障改变上述顺序时测试必须显式失败。
 */
static void test_reactor_tx_pool_case(unsigned fail_alloc,
				      unsigned fail_mutex, int expected)
{
	struct tr_reactor_config config = { 0 };
	struct tr_memory_budget budget;
	struct tr_reactor *reactor = NULL;
	unsigned before = fd_count();
	int ret;

	tr_memory_budget_init(&budget, 0U);
	config.memory_budget = &budget;

	begin_fault(fail_alloc, fail_mutex, 0U);
	ret = tr_reactor_create(&config, NULL, NULL, NULL, &reactor);
	assert(ret == expected);
	if (ret == TR_OK) {
		assert(reactor != NULL);
		assert(tr_reactor_destroy(reactor) == TR_OK);
	} else {
		assert(reactor == NULL);
	}
	finish_fault();
	assert_budget_empty(&budget);
	assert(fd_count() == before);

	if (ret != TR_OK) {
		/* 回滚完成后同一预算与配置仍可构造、启动和停止。 */
		assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) ==
		       TR_OK);
		assert(tr_reactor_start(reactor) == TR_OK);
		assert(tr_reactor_stop(reactor) == TR_OK);
		assert(tr_reactor_destroy(reactor) == TR_OK);
		assert_budget_empty(&budget);
		assert(fd_count() == before);
	}
}

static void test_rpc_group_case(unsigned fail_alloc, unsigned fail_mutex,
				unsigned fail_cond, int expected)
{
	struct tr_rpc_executor_group *group = NULL;
	int ret;

	begin_fault(fail_alloc, fail_mutex, fail_cond);
	ret = tr_rpc_executor_group_create(2U, 4U, 2U, &group);
	assert(ret == expected);
	if (ret == TR_OK) {
		assert(group);
		assert(tr_rpc_executor_group_destroy_checked(group) == TR_OK);
	} else {
		assert(group == NULL);
	}
	finish_fault();

	if (ret != TR_OK) {
		/* 失败未发布任何执行器线程或存储，可立即重建并销毁。 */
		assert(tr_rpc_executor_group_create(2U, 4U, 2U, &group) ==
		       TR_OK);
		assert(tr_rpc_executor_group_can_start(group) == TR_OK);
		assert(tr_rpc_executor_group_destroy_checked(group) == TR_OK);
	}
}

struct channel_fixture {
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	int peer_fd;
};

static void channel_fixture_create(struct channel_fixture *fixture)
{
	int sockets[2];

	memset(fixture, 0, sizeof(*fixture));
	fixture->peer_fd = -1;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			  0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &fixture->reactor) ==
	       TR_OK);
	assert(tr_reactor_start(fixture->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(fixture->reactor, sockets[0],
				   &fixture->connection) == TR_OK);
	fixture->peer_fd = sockets[1];
	/* 只有确认 owner 已处理接管请求后，才能检查是否被错误发布。 */
	assert(tr_reactor_quiesce(fixture->reactor) == TR_OK);
}

static void assert_connection_active(struct channel_fixture *fixture)
{
	enum tr_connection_state state = TR_CONN_ERROR;

	assert(tr_reactor_get_connection_state(fixture->connection, &state) ==
	       TR_OK);
	assert(state == TR_CONN_ACTIVE);
}

static void channel_fixture_destroy(struct channel_fixture *fixture)
{
	assert(tr_reactor_close(fixture->connection) == TR_OK);
	assert(tr_reactor_quiesce(fixture->reactor) == TR_OK);
	assert(tr_reactor_stop(fixture->reactor) == TR_OK);
	assert(tr_reactor_destroy(fixture->reactor) == TR_OK);
	assert(close(fixture->peer_fd) == 0);
}

static struct tr_channel_config channel_config(void)
{
	struct tr_channel_config config = { 0 };

	config.role = TR_CHANNEL_SERVER;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.min_protocol_version = 1U;
	config.max_protocol_version = 1U;
	return config;
}

static void channel_reject(struct channel_fixture *fixture,
			   const struct tr_channel_config *config,
			   unsigned fail_alloc, unsigned fail_mutex,
			   unsigned fail_cond, int expected)
{
	struct tr_channel *channel = NULL;
	int ret;

	begin_fault(fail_alloc, fail_mutex, fail_cond);
	ret = tr_channel_create_deferred(config, fixture->connection,
					 fixture->connection, NULL, NULL,
					 NULL, NULL, &channel);
	assert(ret == expected);
	assert(channel == NULL);
	finish_fault();
	assert_connection_active(fixture);
}

static void test_channel_early_rollback(void)
{
	struct channel_fixture fixture;
	struct tr_channel_config config = channel_config();
	struct tr_buffer_pool reassembly = { 0 };
	struct tr_channel *channel = NULL;

	channel_fixture_create(&fixture);

	/* 协议版本范围非法时，已分配的 Channel 必须被自动清理。 */
	config.min_protocol_version = 2U;
	channel_reject(&fixture, &config, 0U, 0U, 0U, TR_ERR_INVALID);
	config = channel_config();
	config.role = (enum tr_channel_role)99;
	channel_reject(&fixture, &config, 0U, 0U, 0U, TR_ERR_INVALID);

	assert(tr_buffer_pool_init(&reassembly, 1U, 64U) == TR_OK);
	config = channel_config();
	config.reassembly_pool = &reassembly;
	config.max_message_bytes = 65U;
	channel_reject(&fixture, &config, 0U, 0U, 0U, TR_ERR_INVALID);
	assert(tr_buffer_pool_destroy(&reassembly) == TR_OK);

	/* 仍未发布任何 handler/timer 时，清理必须撤回同步对象和数组。 */
	config = channel_config();
	channel_reject(&fixture, &config, 1U, 0U, 0U, TR_ERR_NOMEM);
	channel_reject(&fixture, &config, 0U, 1U, 0U, TR_ERR_INVALID);
	channel_reject(&fixture, &config, 0U, 0U, 1U, TR_ERR_SYS);
	channel_reject(&fixture, &config, 2U, 0U, 0U, TR_ERR_NOMEM);
	channel_reject(&fixture, &config, 3U, 0U, 0U, TR_ERR_NOMEM);

	/* 早期失败没有影响连接能力，随后可以发布并正常拆除 Channel。 */
	assert(tr_channel_create_deferred(&config, fixture.connection,
					  fixture.connection, NULL, NULL,
					  NULL, NULL, &channel) == TR_OK);
	assert(channel != NULL);
	assert(tr_channel_destroy(channel) == TR_OK);
	assert_connection_active(&fixture);
	channel_fixture_destroy(&fixture);
}

int main(void)
{
	/* 构造顺序变化会直接导致指定故障未触发，从而让测试失败。 */
	test_reactor_tx_pool_case(8U, 0U, TR_ERR_NOMEM);
	test_reactor_tx_pool_case(9U, 0U, TR_ERR_NOMEM);
	test_reactor_tx_pool_case(0U, 4U, TR_ERR_INVALID);
	test_reactor_tx_pool_case(0U, 5U, TR_ERR_INVALID);
	test_reactor_tx_pool_case(0U, 0U, TR_OK);

	test_rpc_group_case(1U, 0U, 0U, TR_ERR_NOMEM);
	test_rpc_group_case(2U, 0U, 0U, TR_ERR_NOMEM);
	test_rpc_group_case(0U, 1U, 0U, TR_ERR_INVALID);
	test_rpc_group_case(0U, 0U, 1U, TR_ERR_INVALID);
	test_rpc_group_case(0U, 0U, 0U, TR_OK);

	test_channel_early_rollback();
	puts("complex constructor fault injection: ok");
	return 0;
}
