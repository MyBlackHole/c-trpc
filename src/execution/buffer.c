#include "buffer.h"
#include "tr/status.h"
#include "buffer_internal.h"
#include "../memory_budget.h"
#include "../observability_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

TR_DEFINE_PTR_OWNERSHIP(tr_buffer_array, struct tr_buffer, free)
TR_DEFINE_PTR_OWNERSHIP(tr_buffer_storage, uint8_t, free)

static int tr_buffer_budget_reserve(
	struct tr_memory_budget *budget, uint64_t bytes)
{
	return budget ? tr_memory_budget_reserve(budget, bytes) : TR_OK;
}

static void tr_buffer_budget_release(
	struct tr_memory_budget *budget, uint64_t bytes)
{
	if (budget && bytes != 0U)
		(void)tr_memory_budget_release(budget, bytes);
}

int tr_buffer_pool_init_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t buffer_size, struct tr_memory_budget *budget)
{
	struct tr_buffer *buffers TR_AUTO(tr_buffer_array_cleanup) = NULL;
	uint8_t *storage TR_AUTO(tr_buffer_storage_cleanup) = NULL;
	uint64_t descriptor_bytes;
	uint64_t storage_bytes;
	uint32_t i;
	int ret;

	if (!pool || buffer_count == 0 || buffer_size == 0)
		return TR_ERR_INVALID;
	if ((size_t)buffer_count > SIZE_MAX / (size_t)buffer_size)
		return TR_ERR_BAD_LENGTH;

	memset(pool, 0, sizeof(*pool));
	descriptor_bytes =
		(uint64_t)buffer_count * (uint64_t)sizeof(*buffers);
	storage_bytes =
		(uint64_t)buffer_count * (uint64_t)buffer_size;

	ret = tr_buffer_budget_reserve(budget, descriptor_bytes);
	if (ret != TR_OK)
		return ret;
	ret = tr_buffer_budget_reserve(budget, storage_bytes);
	if (ret != TR_OK) {
		tr_buffer_budget_release(budget, descriptor_bytes);
		return ret;
	}

	buffers =
		(struct tr_buffer *)calloc(buffer_count, sizeof(*buffers));
	if (!buffers) {
		ret = TR_ERR_NOMEM;
		goto fail_budget;
	}

	storage = (uint8_t *)malloc((size_t)buffer_count * buffer_size);
	if (!storage) {
		ret = TR_ERR_NOMEM;
		goto fail_budget;
	}

	/*
	 * 所有可能失败的 allocation 完成后再初始化 mutex。
	 * 从这里开始构造过程不会再失败，因此 pool 正式接管全部资源 ownership。
	 */
	if (pthread_mutex_init(&pool->lock, NULL) != 0) {
		ret = TR_ERR_INVALID;
		goto fail_budget;
	}

	pool->buffers = tr_buffer_array_take(&buffers);
	pool->storage = tr_buffer_storage_take(&storage);
	pool->buffer_count = buffer_count;
	pool->buffer_size = buffer_size;
	pool->free_count = buffer_count;
	pool->memory_budget = budget;

	for (i = 0; i < buffer_count; ++i) {
		struct tr_buffer *buf = &pool->buffers[i];
		buf->data = pool->storage + ((size_t)i * buffer_size);
		buf->capacity = buffer_size;
		buf->len = 0;
		buf->pool = pool;
		buf->next = pool->free_list;
		pool->free_list = buf;
	}

	return TR_OK;

fail_budget:
	tr_buffer_budget_release(budget, storage_bytes);
	tr_buffer_budget_release(budget, descriptor_bytes);
	return ret;
}

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size)
{
	return tr_buffer_pool_init_budgeted(
		pool, buffer_count, buffer_size, NULL);
}

int tr_buffer_pool_init_dynamic_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t max_buffer_size, struct tr_memory_budget *budget)
{
	struct tr_buffer *buffers TR_AUTO(tr_buffer_array_cleanup) = NULL;
	uint64_t descriptor_bytes;
	uint32_t i;
	int ret;

	if (!pool || buffer_count == 0U || max_buffer_size == 0U)
		return TR_ERR_INVALID;
	memset(pool, 0, sizeof(*pool));
	descriptor_bytes =
		(uint64_t)buffer_count * (uint64_t)sizeof(*buffers);
	ret = tr_buffer_budget_reserve(budget, descriptor_bytes);
	if (ret != TR_OK)
		return ret;

	buffers = (struct tr_buffer *)calloc(buffer_count, sizeof(*buffers));
	if (!buffers) {
		tr_buffer_budget_release(budget, descriptor_bytes);
		return TR_ERR_NOMEM;
	}

	/*
	 * Ownership is bounded by descriptor count. Storage grows only after a
	 * descriptor is checked out; storage == NULL distinguishes this internal
	 * mode from the fixed contiguous pool.
	 */
	if (pthread_mutex_init(&pool->lock, NULL) != 0) {
		tr_buffer_budget_release(budget, descriptor_bytes);
		return TR_ERR_INVALID;
	}

	pool->buffers = tr_buffer_array_take(&buffers);
	pool->buffer_count = buffer_count;
	pool->buffer_size = max_buffer_size;
	pool->free_count = buffer_count;
	pool->memory_budget = budget;
	for (i = 0; i < buffer_count; ++i) {
		struct tr_buffer *buf = &pool->buffers[i];
		buf->pool = pool;
		buf->next = pool->free_list;
		pool->free_list = buf;
	}
	return TR_OK;
}

int tr_buffer_pool_init_dynamic(struct tr_buffer_pool *pool,
				uint32_t buffer_count,
				uint32_t max_buffer_size)
{
	return tr_buffer_pool_init_dynamic_budgeted(
		pool, buffer_count, max_buffer_size, NULL);
}

int tr_buffer_pool_destroy(struct tr_buffer_pool *pool)
{
	struct tr_memory_budget *budget;
	struct tr_buffer *buffers;
	uint8_t *storage;
	uint64_t descriptor_bytes;
	uint64_t fixed_storage_bytes = 0U;
	uint32_t buffer_count;
	uint32_t buffer_size;
	uint32_t i;

	if (!pool)
		return TR_OK;

	/*
	 * Zeroed/uninitialized and already-destroyed pools are harmless. This check
	 * intentionally happens before touching lock, which is not initialized in
	 * those states.
	 */
	if (!pool->buffers && pool->buffer_count == 0U)
		return TR_OK;

	pthread_mutex_lock(&pool->lock);
	pool->closed = 1;

	if (pool->free_count != pool->buffer_count) {
		pthread_mutex_unlock(&pool->lock);
		return TR_ERR_STATE;
	}
	for (i = 0; i < pool->buffer_count; ++i) {
		if (pool->buffers[i].checked_out) {
			pthread_mutex_unlock(&pool->lock);
			return TR_ERR_STATE;
		}
	}

	/*
	 * From this point the terminal caller contract guarantees no new API
	 * entrants. Detach storage from the pool while holding the lock so a failed
	 * earlier destroy can be retried after all holders return.
	 */
	budget = pool->memory_budget;
	buffers = pool->buffers;
	storage = pool->storage;
	buffer_count = pool->buffer_count;
	buffer_size = pool->buffer_size;
	descriptor_bytes =
		(uint64_t)buffer_count * (uint64_t)sizeof(*buffers);
	if (storage)
		fixed_storage_bytes =
			(uint64_t)buffer_count * (uint64_t)buffer_size;

	pool->buffers = NULL;
	pool->storage = NULL;
	pool->free_list = NULL;
	pool->buffer_count = 0U;
	pool->buffer_size = 0U;
	pool->free_count = 0U;
	pool->peak_in_use = 0U;
	pool->exhausted_events = 0U;
	pool->memory_budget = NULL;
	pthread_mutex_unlock(&pool->lock);

	if (!storage && buffers)
		for (i = 0; i < buffer_count; ++i)
			tr_buffer_budget_release(
				budget, (uint64_t)buffers[i].capacity);
	free(storage);
	free(buffers);

	tr_buffer_budget_release(budget, fixed_storage_bytes);
	tr_buffer_budget_release(budget, descriptor_bytes);
	pthread_mutex_destroy(&pool->lock);
	return TR_OK;
}

int tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
		      struct tr_buffer **out)
{
	struct tr_buffer *buf;
	int dynamic;

	if (!pool || !out || min_capacity == 0)
		return TR_ERR_INVALID;

	*out = NULL;

	pthread_mutex_lock(&pool->lock);
	if (pool->closed) {
		pthread_mutex_unlock(&pool->lock);
		return TR_ERR_CLOSED;
	}
	if (min_capacity > pool->buffer_size) {
		pthread_mutex_unlock(&pool->lock);
		return TR_ERR_BAD_LENGTH;
	}

	buf = pool->free_list;
	if (!buf) {
		pool->exhausted_events++;
		pthread_mutex_unlock(&pool->lock);
		return TR_AGAIN;
	}

	pool->free_list = buf->next;
	pool->free_count--;
	assert(!buf->checked_out);
	buf->checked_out = 1U;
	tr_observe_high_water_u32(&pool->peak_in_use,
				  pool->buffer_count - pool->free_count);

	buf->next = NULL;
	buf->len = 0;
	buf->release_cb = NULL;
	dynamic = pool->storage == NULL;

	pthread_mutex_unlock(&pool->lock);

	if (dynamic && buf->capacity < min_capacity) {
		uint32_t old_capacity = buf->capacity;
		uint64_t growth =
			(uint64_t)min_capacity - (uint64_t)old_capacity;
		uint8_t *data;
		int ret;

		ret = tr_buffer_budget_reserve(pool->memory_budget, growth);
		if (ret != TR_OK) {
			tr_buffer_release(buf);
			return ret;
		}

		data = (uint8_t *)realloc(buf->data, min_capacity);
		if (!data) {
			tr_buffer_budget_release(pool->memory_budget, growth);
			tr_buffer_release(buf);
			return TR_ERR_NOMEM;
		}
		buf->data = data;
		buf->capacity = min_capacity;
	}

	*out = buf;
	return TR_OK;
}

void tr_buffer_release(struct tr_buffer *buffer)
{
	struct tr_buffer_pool *pool;
	tr_buffer_release_cb release_cb;

	if (!buffer)
		return;

	release_cb = buffer->release_cb;
	if (!buffer->pool && release_cb) {
		buffer->release_cb = NULL;
		release_cb(buffer);
		return;
	}

	if (!buffer->pool)
		return;
	pool = buffer->pool;

	pthread_mutex_lock(&pool->lock);
	/*
	 * A duplicate release must never manufacture free_count and make terminal
	 * destroy believe another descriptor has been returned.
	 */
	if (!buffer->checked_out) {
		pthread_mutex_unlock(&pool->lock);
		return;
	}

	buffer->checked_out = 0U;
	buffer->len = 0;
	buffer->next = pool->free_list;
	pool->free_list = buffer;
	assert(pool->free_count < pool->buffer_count);
	pool->free_count++;

	pthread_mutex_unlock(&pool->lock);
}

uint32_t tr_buffer_pool_free_count(struct tr_buffer_pool *pool)
{
	uint32_t count;

	if (!pool)
		return 0;

	pthread_mutex_lock(&pool->lock);
	count = pool->free_count;
	pthread_mutex_unlock(&pool->lock);

	return count;
}

int tr_buffer_pool_get_stats(struct tr_buffer_pool *pool,
			     struct tr_pool_observation *out)
{
	if (!pool || !out)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&pool->lock);
	out->capacity = pool->buffer_count;
	out->current = pool->buffer_count - pool->free_count;
	out->peak = pool->peak_in_use;
	out->exhausted_events = pool->exhausted_events;
	pthread_mutex_unlock(&pool->lock);
	return TR_OK;
}
