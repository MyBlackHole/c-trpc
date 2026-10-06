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
 * Admission is queue-local so completion producers do not need Reactor
 * control-plane serialization. open/close are linearized by queue->lock.
 */
int tr_completion_queue_open(struct tr_completion_queue *queue);
void tr_completion_queue_close(struct tr_completion_queue *queue);

/*
 * 返回当前开放 admission epoch 的 generation；queue 已关闭时返回 0。
 * producer 必须在开始一次 handoff 时取得该 token，并把同一 token 传给
 * push_wait()，这样 stop/reopen 不能把旧 producer 误接收到新 epoch。
 */
uint64_t tr_completion_queue_admission_generation(
	struct tr_completion_queue *queue);

/*
 * TR_OK 后 completion ownership 已转移给 queue。
 * TR_AGAIN 表示有界 queue 已满，调用方仍拥有 arg。
 * TR_ERR_CLOSED 表示 admission 已关闭，调用方仍拥有 arg。
 */
int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake);

/*
 * Blocking producer handoff used by worker completion paths.
 *
 * Queue full 时 producer 在 not_full 上睡眠，不做 sched_yield 自旋。
 * expected_generation 必须来自本次 handoff 开始时取得的
 * tr_completion_queue_admission_generation()。generation 变化或 admission
 * 关闭都返回 TR_ERR_CLOSED；因此旧 epoch producer 不能穿透 reopen。
 * TR_OK 表示 ownership 已转移；TR_ERR_CLOSED 时 ownership 仍属于调用方。
 */
int tr_completion_queue_push_wait(
	struct tr_completion_queue *queue,
	const struct tr_completion *completion,
	uint64_t expected_generation,
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
