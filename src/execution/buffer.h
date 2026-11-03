#ifndef TR_BUFFER_H
#define TR_BUFFER_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "../cleanup.h"
#include "../observability.h"

struct tr_buffer;
struct tr_memory_budget;
typedef void (*tr_buffer_release_cb)(struct tr_buffer *buffer);

struct tr_buffer {
	uint8_t *data;
	uint32_t capacity;
	uint32_t len;

	struct tr_buffer *next;
	struct tr_buffer_pool *pool;
	/* 池描述符所有权：仅在被一个持有者取出期间为 1。 */
	uint8_t checked_out;

	/* 非池缓冲区的可选一次性释放钩子。设置后，
	 * tr_buffer_release() 调用它，而不是将缓冲区归还池。
	 * 回调负责最终处置，也可以释放缓冲区本身。
	 */
	tr_buffer_release_cb release_cb;
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
	/* 终止准入门禁。一旦销毁观察到尚未归还的持有者，
	 * 就永久关闭新获取；现有持有者仍可释放。
	 */
	int closed;

	/* 可选的内部记账所有者；NULL 保留旧有不记账模式。 */
	struct tr_memory_budget *memory_budget;
};

int tr_buffer_pool_init(struct tr_buffer_pool *pool, uint32_t buffer_count,
			uint32_t buffer_size);
/* 终止销毁。调用者必须已经阻止新的 API 调用进入。
 * 若仍有被取出的描述符，则关闭新获取并返回 TR_ERR_STATE，
 * 同时保留存储与互斥锁，以便现有持有者释放后重试销毁。
 */
int tr_buffer_pool_destroy(struct tr_buffer_pool *pool);

int tr_buffer_acquire(struct tr_buffer_pool *pool, uint32_t min_capacity,
		      struct tr_buffer **out);
void tr_buffer_release(struct tr_buffer *buffer);

/* 作用域拥有的缓冲区离开作用域时会自动归还所属池。
 * 只有明确需要把所有权转移到当前作用域之外时，才调用 tr_buffer_take()。
 */
TR_DEFINE_PTR_OWNERSHIP(tr_buffer, struct tr_buffer, tr_buffer_release)

uint32_t tr_buffer_pool_free_count(struct tr_buffer_pool *pool);
int tr_buffer_pool_get_stats(struct tr_buffer_pool *pool,
			     struct tr_pool_observation *out);

#endif
