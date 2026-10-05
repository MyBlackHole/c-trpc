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
	pthread_cond_t not_full;
	struct tr_completion *items;
	uint32_t capacity;
	uint32_t head;
	uint32_t tail;
	uint32_t count;
	uint32_t peak_count;
	uint64_t full_events;
	uint64_t admission_generation;
	uint32_t waiters;
	int accepting;
	int wake_pending;
};

int tr_completion_queue_init(struct tr_completion_queue *queue,
			     uint32_t capacity);
void tr_completion_queue_destroy(struct tr_completion_queue *queue);

/*
 * 准入状态由队列本地维护，因此完成事件生产者不需要经过 Reactor 控制面串行化。
 * open/close 由 queue->lock 线性化。
 */
int tr_completion_queue_open(struct tr_completion_queue *queue);
void tr_completion_queue_close(struct tr_completion_queue *queue);

/*
 * TR_OK 后 completion ownership 已转移给 queue。
 * TR_AGAIN 表示有界 queue 已满，调用方仍拥有 arg。
 * TR_ERR_CLOSED 表示 admission 已关闭，调用方仍拥有 arg。
 */
int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake);

/*
 * 工作线程完成路径使用的阻塞式生产者移交接口。
 *
 * Queue full 时 producer 在 not_full 上睡眠，不做 sched_yield 自旋。
 * TR_OK 表示 ownership 已转移；close 会唤醒全部 waiter 并返回
 * TR_ERR_CLOSED，此时 ownership 仍属于调用方。
 */
int tr_completion_queue_push_wait(
	struct tr_completion_queue *queue,
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
