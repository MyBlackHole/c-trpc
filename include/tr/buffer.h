#ifndef TR_BUFFER_H
#define TR_BUFFER_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "tr/cleanup.h"
#include "tr/observability.h"

struct tr_buffer;
typedef void (*tr_buffer_release_cb)(
	struct tr_buffer *buffer, void *arg);

struct tr_buffer {
	uint8_t *data;
	uint32_t capacity;
	uint32_t len;

	struct tr_buffer *next;
	struct tr_buffer_pool *pool;

	/*
	 * Optional one-shot release hook for non-pool buffers. When set,
	 * tr_buffer_release() invokes it instead of returning the buffer to pool.
	 * The callback owns final disposal and may free the buffer itself.
	 */
	tr_buffer_release_cb release_cb;
	void *release_arg;
};

struct tr_buffer_pool {
	pthread_mutex_t lock;

	struct tr_buffer *buffers;
	uint8_t *storage;
	struct tr_buffer *free_list;

	uint32_t buffer_count;
	uint32_t buffer_size;
	uint32_t free_count;
	uint32_t peak_in_use;
	uint64_t exhausted_events;
};

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size);
void tr_buffer_pool_destroy(struct tr_buffer_pool *pool);

int tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
		      struct tr_buffer **out);
void tr_buffer_release(struct tr_buffer *buffer);

/*
 * scope-owned buffer 离开作用域时会自动归还所属 pool。
 * 只有明确需要把 ownership 转移到当前作用域之外时，才调用 tr_buffer_take()。
 */
TR_DEFINE_PTR_OWNERSHIP(tr_buffer, struct tr_buffer, tr_buffer_release)

uint32_t tr_buffer_pool_free_count(struct tr_buffer_pool *pool);
int tr_buffer_pool_get_stats(struct tr_buffer_pool *pool,
			     struct tr_pool_observation *out);

#endif
