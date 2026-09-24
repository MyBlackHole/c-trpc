#ifndef TR_BUFFER_H
#define TR_BUFFER_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

struct tr_buffer {
	uint8_t *data;
	uint32_t capacity;
	uint32_t len;

	struct tr_buffer *next;
	struct tr_buffer_pool *pool;
};

struct tr_buffer_pool {
	pthread_mutex_t lock;

	struct tr_buffer *buffers;
	uint8_t *storage;
	struct tr_buffer *free_list;

	uint32_t buffer_count;
	uint32_t buffer_size;
	uint32_t free_count;
};

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size);
void tr_buffer_pool_destroy(struct tr_buffer_pool *pool);

int tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
		      struct tr_buffer **out);
void tr_buffer_release(struct tr_buffer *buffer);

uint32_t tr_buffer_pool_free_count(struct tr_buffer_pool *pool);

#endif
