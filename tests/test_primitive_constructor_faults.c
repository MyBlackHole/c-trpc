/* 针对四种基础资源构造器的真实分配与同步初始化失败注入。
 * 仅本测试的链接器 --wrap 截获 libc/pthread 边界；生产对象与 API 不添加测试钩子。
 */
#include "../src/execution/buffer.h"
#include "../src/execution/buffer_internal.h"
#include "../src/execution/command_queue.h"
#include "../src/execution/completion_queue.h"
#include "../src/execution/timer_queue.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TRACKED 8U

struct fault_state {
	int active;
	unsigned fail_alloc_at;
	unsigned fail_mutex_at;
	unsigned fail_cond_at;
	unsigned alloc_calls;
	unsigned alloc_successes;
	unsigned free_calls;
	unsigned mutex_calls;
	unsigned mutex_successes;
	unsigned mutex_destroys;
	unsigned cond_calls;
	unsigned cond_successes;
	unsigned cond_destroys;
	void *tracked[MAX_TRACKED];
};

static struct fault_state fault;

void *__real_calloc(size_t count, size_t size);
void *__real_malloc(size_t size);
void __real_free(void *ptr);
int __real_pthread_mutex_init(pthread_mutex_t *lock,
			      const pthread_mutexattr_t *attr);
int __real_pthread_mutex_destroy(pthread_mutex_t *lock);
int __real_pthread_cond_init(pthread_cond_t *cond,
			     const pthread_condattr_t *attr);
int __real_pthread_cond_destroy(pthread_cond_t *cond);

static void track_allocation(void *ptr)
{
	unsigned i;

	if (!ptr || !fault.active)
		return;
	for (i = 0U; i < MAX_TRACKED; ++i) {
		if (!fault.tracked[i]) {
			fault.tracked[i] = ptr;
			fault.alloc_successes++;
			return;
		}
	}
	assert(!"exceeded bounded fault-injection allocation ledger");
}

static void track_free(void *ptr)
{
	unsigned i;

	if (!ptr || !fault.active)
		return;
	for (i = 0U; i < MAX_TRACKED; ++i) {
		if (fault.tracked[i] == ptr) {
			fault.tracked[i] = NULL;
			fault.free_calls++;
			return;
		}
	}
	assert(!"unexpected or duplicate free in primitive constructor");
}

void *__wrap_calloc(size_t count, size_t size)
{
	void *ptr;

	if (fault.active && ++fault.alloc_calls == fault.fail_alloc_at)
		return NULL;
	ptr = __real_calloc(count, size);
	track_allocation(ptr);
	return ptr;
}

void *__wrap_malloc(size_t size)
{
	void *ptr;

	if (fault.active && ++fault.alloc_calls == fault.fail_alloc_at)
		return NULL;
	ptr = __real_malloc(size);
	track_allocation(ptr);
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

	if (fault.active && ++fault.mutex_calls == fault.fail_mutex_at)
		return EAGAIN;
	ret = __real_pthread_mutex_init(lock, attr);
	if (fault.active && ret == 0)
		fault.mutex_successes++;
	return ret;
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *lock)
{
	int ret = __real_pthread_mutex_destroy(lock);

	if (fault.active && ret == 0)
		fault.mutex_destroys++;
	return ret;
}

int __wrap_pthread_cond_init(pthread_cond_t *cond,
			     const pthread_condattr_t *attr)
{
	int ret;

	if (fault.active && ++fault.cond_calls == fault.fail_cond_at)
		return EAGAIN;
	ret = __real_pthread_cond_init(cond, attr);
	if (fault.active && ret == 0)
		fault.cond_successes++;
	return ret;
}

int __wrap_pthread_cond_destroy(pthread_cond_t *cond)
{
	int ret = __real_pthread_cond_destroy(cond);

	if (fault.active && ret == 0)
		fault.cond_destroys++;
	return ret;
}

static void start_fault(unsigned alloc_at, unsigned mutex_at,
			unsigned cond_at)
{
	assert(!fault.active);
	memset(&fault, 0, sizeof(fault));
	fault.fail_alloc_at = alloc_at;
	fault.fail_mutex_at = mutex_at;
	fault.fail_cond_at = cond_at;
	fault.active = 1;
}

static void finish_fault(unsigned allocations, unsigned mutexes,
			 unsigned conds)
{
	unsigned i;

	assert(fault.active);
	assert(fault.alloc_calls == allocations);
	assert(fault.mutex_calls == mutexes);
	assert(fault.cond_calls == conds);
	assert(fault.alloc_successes == fault.free_calls);
	assert(fault.mutex_successes == fault.mutex_destroys);
	assert(fault.cond_successes == fault.cond_destroys);
	for (i = 0U; i < MAX_TRACKED; ++i)
		assert(fault.tracked[i] == NULL);
	fault.active = 0;
}

static void assert_budget_empty(const struct tr_memory_budget *budget,
				uint64_t rejection_events)
{
	struct tr_memory_budget_stats stats;

	tr_memory_budget_get_stats(budget, &stats);
	assert(stats.current_bytes == 0U);
	assert(stats.rejection_events == rejection_events);
}

static void test_fixed_buffer_case(unsigned fail_alloc, unsigned fail_mutex,
				   int expected, unsigned alloc_calls,
				   unsigned mutex_calls)
{
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_buffer *buffer = NULL;
	int ret;

	tr_memory_budget_init(&budget, 0U);
	start_fault(fail_alloc, fail_mutex, 0U);
	ret = tr_buffer_pool_init_budgeted(&pool, 2U, 128U, &budget);
	assert(ret == expected);
	if (ret != TR_OK) {
		/* 初始化失败尚未发布池或锁，销毁空池必须安全。 */
		assert(pool.buffers == NULL && pool.storage == NULL);
		assert(pool.buffer_count == 0U);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	} else {
		assert(tr_buffer_acquire(&pool, 32U, &buffer) == TR_OK);
		tr_buffer_release(buffer);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	}
	assert_budget_empty(&budget, 0U);
	finish_fault(alloc_calls, mutex_calls, 0U);

	if (ret != TR_OK) {
		/* 同一个池对象和预算实例可以在完整回滚后重新构造。 */
		start_fault(0U, 0U, 0U);
		assert(tr_buffer_pool_init_budgeted(&pool, 2U, 128U,
						    &budget) == TR_OK);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
		assert_budget_empty(&budget, 0U);
		finish_fault(2U, 1U, 0U);
	}
}

static void test_fixed_buffer_budget_rejections(void)
{
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	uint64_t descriptors = 2U * (uint64_t)sizeof(struct tr_buffer);
	unsigned i;

	for (i = 0U; i < 2U; ++i) {
		uint64_t limit = i == 0U ? descriptors - 1U :
				 descriptors + 2U * 128U - 1U;

		tr_memory_budget_init(&budget, limit);
		start_fault(0U, 0U, 0U);
		assert(tr_buffer_pool_init_budgeted(&pool, 2U, 128U,
						    &budget) == TR_AGAIN);
		assert(pool.buffers == NULL && pool.storage == NULL);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
		assert_budget_empty(&budget, 1U);
		finish_fault(0U, 0U, 0U);

		/* 预算拒绝不留下内存或锁；重新放宽限制即可复用同一对象。 */
		tr_memory_budget_init(&budget, 0U);
		start_fault(0U, 0U, 0U);
		assert(tr_buffer_pool_init_budgeted(&pool, 2U, 128U,
						    &budget) == TR_OK);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
		assert_budget_empty(&budget, 0U);
		finish_fault(2U, 1U, 0U);
	}
}

static void test_dynamic_buffer_case(unsigned fail_alloc,
				     unsigned fail_mutex, int expected,
				     unsigned alloc_calls, unsigned mutex_calls)
{
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	int ret;

	tr_memory_budget_init(&budget, 0U);
	start_fault(fail_alloc, fail_mutex, 0U);
	ret = tr_buffer_pool_init_dynamic_budgeted(
		&pool, 2U, 4096U, &budget);
	assert(ret == expected);
	if (ret != TR_OK) {
		assert(pool.buffers == NULL && pool.buffer_count == 0U);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	} else {
		assert(pool.buffers && !pool.storage);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	}
	assert_budget_empty(&budget, 0U);
	finish_fault(alloc_calls, mutex_calls, 0U);

	if (ret != TR_OK) {
		start_fault(0U, 0U, 0U);
		assert(tr_buffer_pool_init_dynamic_budgeted(
			       &pool, 2U, 4096U, &budget) == TR_OK);
		assert(tr_buffer_pool_destroy(&pool) == TR_OK);
		assert_budget_empty(&budget, 0U);
		finish_fault(1U, 1U, 0U);
	}
}

static void test_command_queue_case(unsigned fail_alloc,
				    unsigned fail_mutex, unsigned fail_cond,
				    int expected, unsigned allocations,
				    unsigned mutexes, unsigned conds)
{
	struct tr_command_queue queue;
	struct tr_command command = { .type = TR_CMD_RESUME_RX };
	struct tr_command out;

	start_fault(fail_alloc, fail_mutex, fail_cond);
	assert(tr_command_queue_init(&queue, 2U) == expected);
	if (expected == TR_OK) {
		assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
		assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
		tr_command_queue_destroy(&queue);
	} else {
		/* 失败构造没有成功的同步对象，不可调用完整 destroy。 */
		assert(queue.items == NULL && queue.capacity == 0U);
	}
	finish_fault(allocations, mutexes, conds);

	if (expected != TR_OK) {
		start_fault(0U, 0U, 0U);
		assert(tr_command_queue_init(&queue, 2U) == TR_OK);
		assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
		assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
		tr_command_queue_destroy(&queue);
		finish_fault(1U, 1U, 1U);
	}
}

static void noop_completion(void *arg)
{
	(void)arg;
}

static void test_completion_queue_case(unsigned fail_alloc,
				       unsigned fail_mutex, unsigned fail_cond,
				       int expected, unsigned allocations,
				       unsigned mutexes, unsigned conds)
{
	struct tr_completion_queue queue;
	struct tr_completion item = { noop_completion, NULL };
	struct tr_completion out;

	start_fault(fail_alloc, fail_mutex, fail_cond);
	assert(tr_completion_queue_init(&queue, 2U) == expected);
	if (expected == TR_OK) {
		assert(tr_completion_queue_open(&queue) == TR_OK);
		assert(tr_completion_queue_push(&queue, &item, NULL) == TR_OK);
		assert(tr_completion_queue_pop_batch(&queue, &out, 1U,
						     NULL) == 1U);
		tr_completion_queue_close(&queue);
		tr_completion_queue_destroy(&queue);
	} else {
		assert(queue.items == NULL && queue.capacity == 0U);
	}
	finish_fault(allocations, mutexes, conds);

	if (expected != TR_OK) {
		start_fault(0U, 0U, 0U);
		assert(tr_completion_queue_init(&queue, 2U) == TR_OK);
		assert(tr_completion_queue_open(&queue) == TR_OK);
		assert(tr_completion_queue_push(&queue, &item, NULL) == TR_OK);
		assert(tr_completion_queue_pop_batch(&queue, &out, 1U,
						     NULL) == 1U);
		tr_completion_queue_close(&queue);
		tr_completion_queue_destroy(&queue);
		finish_fault(1U, 1U, 1U);
	}
}

static uint64_t noop_timer(void *arg, uint64_t now)
{
	(void)arg;
	(void)now;
	return 0U;
}

static void test_timer_queue_case(unsigned fail_alloc, int expected,
				  unsigned allocations)
{
	struct tr_timer_queue queue;
	struct tr_timer_token token;

	start_fault(fail_alloc, 0U, 0U);
	assert(tr_timer_queue_init(&queue, 2U) == expected);
	if (expected == TR_OK) {
		assert(tr_timer_queue_register(&queue, noop_timer, NULL,
					       &token) == TR_OK);
		assert(tr_timer_queue_unregister(&queue, token) == TR_OK);
	} else {
		assert(queue.entries == NULL && queue.heap == NULL);
	}
	tr_timer_queue_destroy(&queue);
	finish_fault(allocations, 0U, 0U);

	if (expected != TR_OK) {
		start_fault(0U, 0U, 0U);
		assert(tr_timer_queue_init(&queue, 2U) == TR_OK);
		assert(tr_timer_queue_register(&queue, noop_timer, NULL,
					       &token) == TR_OK);
		assert(tr_timer_queue_unregister(&queue, token) == TR_OK);
		tr_timer_queue_destroy(&queue);
		finish_fault(2U, 0U, 0U);
	}
}

int main(void)
{
	test_fixed_buffer_case(1U, 0U, TR_ERR_NOMEM, 1U, 0U);
	test_fixed_buffer_case(2U, 0U, TR_ERR_NOMEM, 2U, 0U);
	test_fixed_buffer_case(0U, 1U, TR_ERR_INVALID, 2U, 1U);
	test_fixed_buffer_case(0U, 0U, TR_OK, 2U, 1U);
	test_fixed_buffer_budget_rejections();

	test_dynamic_buffer_case(1U, 0U, TR_ERR_NOMEM, 1U, 0U);
	test_dynamic_buffer_case(0U, 1U, TR_ERR_INVALID, 1U, 1U);
	test_dynamic_buffer_case(0U, 0U, TR_OK, 1U, 1U);

	test_command_queue_case(1U, 0U, 0U, TR_ERR_NOMEM, 1U, 0U, 0U);
	test_command_queue_case(0U, 1U, 0U, TR_ERR_INVALID, 1U, 1U, 0U);
	test_command_queue_case(0U, 0U, 1U, TR_ERR_INVALID, 1U, 1U, 1U);
	test_command_queue_case(0U, 0U, 0U, TR_OK, 1U, 1U, 1U);

	test_completion_queue_case(1U, 0U, 0U, TR_ERR_NOMEM, 1U, 0U, 0U);
	test_completion_queue_case(0U, 1U, 0U, TR_ERR_INVALID, 1U, 1U, 0U);
	test_completion_queue_case(0U, 0U, 1U, TR_ERR_INVALID, 1U, 1U, 1U);
	test_completion_queue_case(0U, 0U, 0U, TR_OK, 1U, 1U, 1U);

	test_timer_queue_case(1U, TR_ERR_NOMEM, 1U);
	test_timer_queue_case(2U, TR_ERR_NOMEM, 2U);
	test_timer_queue_case(0U, TR_OK, 2U);
	puts("primitive constructor fault injection: ok");
	return 0;
}
