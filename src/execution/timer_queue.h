#ifndef TR_TIMER_QUEUE_H
#define TR_TIMER_QUEUE_H

#include <stddef.h>
#include <stdint.h>

typedef uint64_t (*tr_timer_callback)(void *arg, uint64_t now_ns);

struct tr_timer_token {
	uint32_t slot;
	uint32_t generation;
};

struct tr_timer_entry {
	uint32_t generation;
	/* 已注册项使用 heap_pos；空闲项使用 next_free。 */
	union {
		uint32_t heap_pos;
		uint32_t next_free;
	};
	uint64_t deadline_ns;
	uint64_t version;
	tr_timer_callback callback;
	void *arg;
	int used;
	int running;
};

struct tr_timer_queue {
	struct tr_timer_entry *entries;
	uint32_t *heap;
	uint32_t capacity;
	uint32_t size;
	uint32_t free_head;
};

int tr_timer_queue_init(struct tr_timer_queue *queue, uint32_t capacity);
void tr_timer_queue_destroy(struct tr_timer_queue *queue);

/* 仅限所有者，从有界空闲链表进行无分配的 O(1) 注册。
 * 解除定时的定时器仍拥有其槽位；只有注销才释放它。
 * 槽位复用顺序属于实现细节，不是令牌契约的一部分。
 */
int tr_timer_queue_register(struct tr_timer_queue *queue,
			    tr_timer_callback callback, void *arg,
			    struct tr_timer_token *out);
int tr_timer_queue_arm(struct tr_timer_queue *queue,
		       struct tr_timer_token token, uint64_t deadline_ns);
int tr_timer_queue_unregister(struct tr_timer_queue *queue,
			      struct tr_timer_token token);

uint64_t tr_timer_queue_next_deadline(const struct tr_timer_queue *queue);

/* 仅限反应器所有者。最多执行 max_callbacks 个绝对 CLOCK_MONOTONIC
 * 截止时间 <= now_ns 的回调。has_more_due 报告预算耗尽后是否
 * 仍有其他已到期定时器，包括预算为零的情况。
 * NULL 队列视为空，报告没有到期工作。
 */
size_t tr_timer_queue_run_due(struct tr_timer_queue *queue, uint64_t now_ns,
			      size_t max_callbacks, int *has_more_due);

#endif
