#include "tr/buffer.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

TR_DEFINE_PTR_OWNERSHIP(tr_buffer_array, struct tr_buffer, free)
TR_DEFINE_PTR_OWNERSHIP(tr_buffer_storage, uint8_t, free)

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size)
{
	uint32_t i;

	if (!pool || buffer_count == 0 || buffer_size == 0)
		return TR_ERR_INVALID;

	memset(pool, 0, sizeof(*pool));

	if (pthread_mutex_init(&pool->lock, NULL) != 0)
		return TR_ERR_INVALID;

	pool->buffers = (struct tr_buffer *)calloc(buffer_count,
						   sizeof(*pool->buffers));
	if (!pool->buffers)
		goto fail_lock;

	pool->storage = (uint8_t *)malloc((size_t)buffer_count * buffer_size);
	if (!pool->storage)
		goto fail_buffers;

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

fail_buffers:
	free(pool->buffers);
	pool->buffers = NULL;
fail_lock:
	pthread_mutex_destroy(&pool->lock);
	return TR_ERR_NOMEM;
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
