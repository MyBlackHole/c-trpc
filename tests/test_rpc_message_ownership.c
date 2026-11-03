#include "tr/trpc.h"
#include "../src/execution/buffer.h"
#include "../src/execution/buffer_internal.h"
#include "../src/memory_budget.h"

#include <assert.h>
#include <stdint.h>

static void test_dynamic_pool(void)
{
	struct tr_buffer_pool pool;
	struct tr_buffer *a = NULL, *b = NULL, *c = NULL;
	assert(tr_buffer_pool_init_dynamic(&pool, 2U, 4096U) == TR_OK);
	assert(tr_buffer_acquire(&pool, 64U, &a) == TR_OK);
	assert(a && a->capacity >= 64U);
	assert(tr_buffer_acquire(&pool, 2048U, &b) == TR_OK);
	assert(b && b->capacity >= 2048U);
	assert(tr_buffer_acquire(&pool, 1U, &c) == TR_AGAIN);
	assert(c == NULL);
	assert(tr_buffer_acquire(&pool, 4097U, &c) == TR_ERR_BAD_LENGTH);
	assert(c == NULL);
	tr_buffer_release(a);
	a = NULL;
	assert(tr_buffer_acquire(&pool, 3072U, &c) == TR_OK);
	assert(c && c->capacity >= 3072U);
	tr_buffer_release(c);
	tr_buffer_release(b);
	assert(tr_buffer_pool_free_count(&pool) == 2U);
	tr_buffer_pool_destroy(&pool);
}

static void test_budgeted_buffer_pool(void)
{
	struct tr_memory_budget budget;
	struct tr_memory_budget_stats stats;
	struct tr_buffer_pool pool;
	struct tr_buffer *buffer = NULL;
	uint64_t descriptor_bytes = (uint64_t)sizeof(struct tr_buffer);
	uint64_t fixed_bytes =
		UINT64_C(2) * descriptor_bytes + UINT64_C(2) * 64U;

	tr_memory_budget_init(&budget, fixed_bytes);
	assert(tr_buffer_pool_init_budgeted(
		       &pool, 2U, 64U, &budget) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == fixed_bytes);
	assert(stats.peak_bytes == fixed_bytes);
	assert(stats.rejection_events == 0U);
	tr_buffer_pool_destroy(&pool);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
	assert(stats.peak_bytes == fixed_bytes);

	/* 动态池在初始化时预留描述符，按需增长存储。
	 * 释放后保留的容量仍被记账，只在池销毁时归还。
	 */
	tr_memory_budget_init(&budget, descriptor_bytes + 64U);
	assert(tr_buffer_pool_init_dynamic_budgeted(
		       &pool, 1U, 128U, &budget) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes);

	assert(tr_buffer_acquire(&pool, 64U, &buffer) == TR_OK);
	assert(buffer != NULL && buffer->capacity == 64U);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes + 64U);
	tr_buffer_release(buffer);
	buffer = NULL;

	assert(tr_buffer_acquire(&pool, 65U, &buffer) == TR_AGAIN);
	assert(buffer == NULL);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes + 64U);
	assert(stats.peak_bytes == descriptor_bytes + 64U);
	assert(stats.rejection_events == 1U);

	tr_buffer_pool_destroy(&pool);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
	assert(stats.peak_bytes == descriptor_bytes + 64U);
}

int main(void)
{
	test_dynamic_pool();
	test_budgeted_buffer_pool();
	return 0;
}
