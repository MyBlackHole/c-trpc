#ifndef TR_COMPLETION_QUEUE_H
#define TR_COMPLETION_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

struct tr_completion {
	void (*fn)(void *arg);
	void *arg;
};

struct tr_completion_queue {
	pthread_mutex_t lock;
	struct tr_completion *items;
	uint32_t capacity;
	uint32_t head;
	uint32_t tail;
	uint32_t count;
	int wake_pending;
};

int tr_completion_queue_init(struct tr_completion_queue *queue,
			     uint32_t capacity);
void tr_completion_queue_destroy(struct tr_completion_queue *queue);

/*
 * TR_OK 后 completion ownership 已转移给 queue。
 * TR_AGAIN 表示有界 queue 已满，调用方仍拥有 arg。
 */
int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake);

/*
 * 最多弹出 max_completions 项；has_more 返回本批之后是否仍有积压。
 * 只有 queue 真正变空时才清除 wake_pending。
 */
size_t tr_completion_queue_pop_batch(struct tr_completion_queue *queue,
				     struct tr_completion *out,
				     size_t max_completions,
				     int *has_more);

#endif
