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

/* 准入位于队列本地，因此完成项生产者无需经过反应器控制平面的串行化。
 * 开放与关闭由 queue->lock 线性化。
 */
int tr_completion_queue_open(struct tr_completion_queue *queue);
void tr_completion_queue_close(struct tr_completion_queue *queue);

/* 返回当前开放准入轮次的 generation；队列已关闭时返回 0。
 * 生产者必须在开始一次交接时取得该令牌，并把同一令牌传给
 * push_wait()，这样停止或重开不能把旧生产者误接收到新轮次。
 */
uint64_t tr_completion_queue_admission_generation(
	struct tr_completion_queue *queue);

/* TR_OK 后完成项所有权已转移给队列。
 * TR_AGAIN 表示有界队列已满，调用方仍拥有 arg。
 * TR_ERR_CLOSED 表示准入已关闭，调用方仍拥有 arg。
 */
int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake);

/* 工作线程完成路径使用的阻塞式生产者交接。
 *
 * 队列满时生产者在 not_full 上睡眠，不做 sched_yield 自旋。
 * expected_generation 必须来自本次交接开始时取得的
 * tr_completion_queue_admission_generation()。generation 变化或准入
 * 关闭都返回 TR_ERR_CLOSED；因此旧轮次生产者不能穿透重开。
 * TR_OK 表示所有权已转移；TR_ERR_CLOSED 时所有权仍属于调用方。
 */
int tr_completion_queue_push_wait(
	struct tr_completion_queue *queue,
	const struct tr_completion *completion,
	uint64_t expected_generation,
	int *need_wake);

/* 最多弹出 max_completions 项；has_more 返回本批之后是否仍有积压。
 * 只有队列真正变空时才清除 wake_pending。
 */
size_t tr_completion_queue_pop_batch(struct tr_completion_queue *queue,
				     struct tr_completion *out,
				     size_t max_completions,
				     int *has_more);

#endif
