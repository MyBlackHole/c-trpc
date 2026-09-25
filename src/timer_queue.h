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
	uint32_t heap_pos;
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
};

int tr_timer_queue_init(struct tr_timer_queue *queue, uint32_t capacity);
void tr_timer_queue_destroy(struct tr_timer_queue *queue);

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
 * already-due timer remains after the budget is consumed.
 */
size_t tr_timer_queue_run_due(struct tr_timer_queue *queue, uint64_t now_ns,
			      size_t max_callbacks, int *has_more_due);

#endif
