#include "../src/execution/buffer.h"
#include "../src/execution/buffer_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void test_destroy_closes_acquire_until_holders_return(void)
{
	struct tr_buffer_pool pool;
	struct tr_buffer *first = NULL;
	struct tr_buffer *second = NULL;
	struct tr_buffer *rejected = NULL;

	assert(tr_buffer_pool_init(&pool, 2U, 128U) == TR_OK);
	assert(tr_buffer_acquire(&pool, 32U, &first) == TR_OK);
	assert(tr_buffer_acquire(&pool, 64U, &second) == TR_OK);
	assert(tr_buffer_pool_free_count(&pool) == 0U);

	/*
	 * Terminal destroy closes new admission but keeps storage and lock alive
	 * while existing holders still own descriptors.
	 */
	assert(tr_buffer_pool_destroy(&pool) == TR_ERR_STATE);
	assert(tr_buffer_acquire(&pool, 16U, &rejected) == TR_ERR_CLOSED);
	assert(rejected == NULL);

	tr_buffer_release(first);
	assert(tr_buffer_pool_free_count(&pool) == 1U);

	/*
	 * Duplicate release must not manufacture free_count and hide the second
	 * outstanding descriptor from terminal destroy.
	 */
	tr_buffer_release(first);
	assert(tr_buffer_pool_free_count(&pool) == 1U);
	assert(tr_buffer_pool_destroy(&pool) == TR_ERR_STATE);

	tr_buffer_release(second);
	assert(tr_buffer_pool_free_count(&pool) == 2U);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
}

static void test_dynamic_pool_retry_destroy(void)
{
	struct tr_buffer_pool pool;
	struct tr_buffer *buffer = NULL;

	assert(tr_buffer_pool_init_dynamic(&pool, 1U, 4096U) == TR_OK);
	assert(tr_buffer_acquire(&pool, 2048U, &buffer) == TR_OK);
	assert(buffer->capacity >= 2048U);

	assert(tr_buffer_pool_destroy(&pool) == TR_ERR_STATE);
	tr_buffer_release(buffer);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
}

static void test_empty_and_zero_pool_destroy(void)
{
	struct tr_buffer_pool zero;

	memset(&zero, 0, sizeof(zero));
	assert(tr_buffer_pool_destroy(NULL) == TR_OK);
	assert(tr_buffer_pool_destroy(&zero) == TR_OK);

	assert(tr_buffer_pool_init(&zero, 1U, 64U) == TR_OK);
	assert(tr_buffer_pool_destroy(&zero) == TR_OK);
}

int main(void)
{
	test_destroy_closes_acquire_until_holders_return();
	test_dynamic_pool_retry_destroy();
	test_empty_and_zero_pool_destroy();
	puts("buffer pool terminal lifecycle: ok");
	return 0;
}
