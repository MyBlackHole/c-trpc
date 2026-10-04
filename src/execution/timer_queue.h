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
	/* A registered entry uses heap_pos; a free entry uses next_free. */
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

/*
 * Owner-only, allocation-free O(1) registration from the bounded free list.
 * A disarmed timer still owns its slot; only unregister releases it. Slot
 * reuse order is an implementation detail, not part of the token contract.
 */
int tr_timer_queue_register(struct tr_timer_queue *queue,
			    tr_timer_callback callback, void *arg,
			    struct tr_timer_token *out);
int tr_timer_queue_arm(struct tr_timer_queue *queue,
		       struct tr_timer_token token, uint64_t deadline_ns);
int tr_timer_queue_unregister(struct tr_timer_queue *queue,
			      struct tr_timer_token token);

uint64_t tr_timer_queue_next_deadline(const struct tr_timer_queue *queue);

/*
 * Reactor owner only. Executes at most max_callbacks callbacks whose absolute
 * CLOCK_MONOTONIC deadline is <= now_ns. has_more_due reports whether another
 * already-due timer remains after the budget is consumed, including a zero
 * budget. A NULL queue is treated as empty and reports no due work.
 */
size_t tr_timer_queue_run_due(struct tr_timer_queue *queue, uint64_t now_ns,
			      size_t max_callbacks, int *has_more_due);

#endif
