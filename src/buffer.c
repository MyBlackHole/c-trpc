#include "tr/buffer.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

TR_DEFINE_PTR_OWNERSHIP(tr_buffer_array, struct tr_buffer, free)
TR_DEFINE_PTR_OWNERSHIP(tr_buffer_storage, uint8_t, free)

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size)
{
	struct tr_buffer *buffers TR_AUTO(tr_buffer_array_cleanup) = NULL;
	uint8_t *storage TR_AUTO(tr_buffer_storage_cleanup) = NULL;
	uint32_t i;

	if (!pool || buffer_count == 0 || buffer_size == 0)
		return TR_ERR_INVALID;
	if ((size_t)buffer_count > SIZE_MAX / (size_t)buffer_size)
		return TR_ERR_BAD_LENGTH;

	memset(pool, 0, sizeof(*pool));

	buffers =
		(struct tr_buffer *)calloc(buffer_count, sizeof(*buffers));
	if (!buffers)
		return TR_ERR_NOMEM;

	storage = (uint8_t *)malloc((size_t)buffer_count * buffer_size);
	if (!storage)
		return TR_ERR_NOMEM;

	/*
	 * 所有可能失败的 allocation 完成后再初始化 mutex。
	 * 从这里开始构造过程不会再失败，因此 pool 正式接管全部资源 ownership。
	 */
	if (pthread_mutex_init(&pool->lock, NULL) != 0)
		return TR_ERR_INVALID;

	pool->buffers = tr_buffer_array_take(&buffers);
	pool->storage = tr_buffer_storage_take(&storage);
	pool->buffer_count = buffer_count;
	pool->buffer_size = buffer_size;
	pool->free_count = buffer_count;

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
}

void tr_buffer_pool_destroy(struct tr_buffer_pool *pool)
{
	if (!pool)
		return;

	free(pool->storage);
	free(pool->buffers);
	pool->storage = NULL;
	pool->buffers = NULL;
	pool->free_list = NULL;
	pool->buffer_count = 0;
	pool->buffer_size = 0;
	pool->free_count = 0;
	pthread_mutex_destroy(&pool->lock);
}

int tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
		      struct tr_buffer **out)
{
	struct tr_buffer *buf;

	if (!pool || !out || min_capacity == 0)
		return TR_ERR_INVALID;

	*out = NULL;

	if (min_capacity > pool->buffer_size)
		return TR_ERR_BAD_LENGTH;

	pthread_mutex_lock(&pool->lock);

	buf = pool->free_list;
	if (!buf) {
		pthread_mutex_unlock(&pool->lock);
		return TR_AGAIN;
	}

	pool->free_list = buf->next;
	pool->free_count--;

	buf->next = NULL;
	buf->len = 0;

	pthread_mutex_unlock(&pool->lock);

	*out = buf;
	return TR_OK;
}

void tr_buffer_release(struct tr_buffer *buffer)
{
	struct tr_buffer_pool *pool;

	if (!buffer || !buffer->pool)
		return;

	pool = buffer->pool;

	pthread_mutex_lock(&pool->lock);

	buffer->len = 0;
	buffer->next = pool->free_list;
	pool->free_list = buffer;
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
