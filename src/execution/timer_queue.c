#include "timer_queue.h"

#include "tr/cleanup.h"
#include "tr/status.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define TR_TIMER_HEAP_NONE UINT32_MAX

TR_DEFINE_PTR_OWNERSHIP(tr_timer_entry_array, struct tr_timer_entry, free)
TR_DEFINE_PTR_OWNERSHIP(tr_timer_heap_array, uint32_t, free)

static int tr_timer_token_valid(const struct tr_timer_queue *queue,
				struct tr_timer_token token,
				struct tr_timer_entry **out)
{
	struct tr_timer_entry *entry;

	if (!queue || token.slot >= queue->capacity)
		return 0;
	entry = &queue->entries[token.slot];
	if (!entry->used || entry->generation != token.generation)
		return 0;
	if (out)
		*out = entry;
	return 1;
}

static int tr_timer_before(const struct tr_timer_queue *queue,
			   uint32_t a_slot, uint32_t b_slot)
{
	const struct tr_timer_entry *a = &queue->entries[a_slot];
	const struct tr_timer_entry *b = &queue->entries[b_slot];

	if (a->deadline_ns != b->deadline_ns)
		return a->deadline_ns < b->deadline_ns;
	return a_slot < b_slot;
}

static void tr_timer_heap_swap(struct tr_timer_queue *queue,
			       uint32_t a, uint32_t b)
{
	uint32_t tmp = queue->heap[a];

	queue->heap[a] = queue->heap[b];
	queue->heap[b] = tmp;
	queue->entries[queue->heap[a]].heap_pos = a;
	queue->entries[queue->heap[b]].heap_pos = b;
}

static void tr_timer_heap_up(struct tr_timer_queue *queue, uint32_t pos)
{
	while (pos != 0) {
		uint32_t parent = (pos - 1U) / 2U;

		if (!tr_timer_before(queue, queue->heap[pos],
				    queue->heap[parent]))
			break;
		tr_timer_heap_swap(queue, pos, parent);
		pos = parent;
	}
}

static void tr_timer_heap_down(struct tr_timer_queue *queue, uint32_t pos)
{
	for (;;) {
		uint32_t left = pos * 2U + 1U;
		uint32_t right = left + 1U;
		uint32_t best = pos;

		if (left < queue->size &&
		    tr_timer_before(queue, queue->heap[left],
				    queue->heap[best]))
			best = left;
		if (right < queue->size &&
		    tr_timer_before(queue, queue->heap[right],
				    queue->heap[best]))
			best = right;
		if (best == pos)
			break;
		tr_timer_heap_swap(queue, pos, best);
		pos = best;
	}
}

static void tr_timer_heap_remove(struct tr_timer_queue *queue, uint32_t pos)
{
	uint32_t removed_slot;
	uint32_t last_slot;

	if (pos >= queue->size)
		return;

	removed_slot = queue->heap[pos];
	queue->size--;
	if (pos != queue->size) {
		last_slot = queue->heap[queue->size];
		queue->heap[pos] = last_slot;
		queue->entries[last_slot].heap_pos = pos;
		tr_timer_heap_up(queue, pos);
		pos = queue->entries[last_slot].heap_pos;
		tr_timer_heap_down(queue, pos);
	}
	queue->entries[removed_slot].heap_pos = TR_TIMER_HEAP_NONE;
}

int tr_timer_queue_init(struct tr_timer_queue *queue, uint32_t capacity)
{
	struct tr_timer_entry *entries TR_AUTO(tr_timer_entry_array_cleanup) =
		NULL;
	uint32_t *heap TR_AUTO(tr_timer_heap_array_cleanup) = NULL;
	uint32_t i;

	if (!queue || capacity == 0)
		return TR_ERR_INVALID;

	memset(queue, 0, sizeof(*queue));
	entries = (struct tr_timer_entry *)calloc(capacity, sizeof(*entries));
	heap = (uint32_t *)calloc(capacity, sizeof(*heap));
	if (!entries || !heap)
		return TR_ERR_NOMEM;

	for (i = 0; i < capacity; ++i)
		entries[i].next_free = i + 1U < capacity ? i + 1U :
				      TR_TIMER_HEAP_NONE;

	queue->entries = tr_timer_entry_array_take(&entries);
	queue->heap = tr_timer_heap_array_take(&heap);
	queue->capacity = capacity;
	return TR_OK;
}

void tr_timer_queue_destroy(struct tr_timer_queue *queue)
{
	if (!queue)
		return;
	free(queue->entries);
	free(queue->heap);
	memset(queue, 0, sizeof(*queue));
}

int tr_timer_queue_register(struct tr_timer_queue *queue,
			    tr_timer_callback callback, void *arg,
			    struct tr_timer_token *out)
{
	struct tr_timer_entry *entry;
	uint32_t slot;
	uint32_t generation;

	if (!queue || !callback || !out)
		return TR_ERR_INVALID;
	if (queue->capacity == 0 || queue->free_head == TR_TIMER_HEAP_NONE)
		return TR_AGAIN;

	slot = queue->free_head;
	entry = &queue->entries[slot];
	queue->free_head = entry->next_free;
	generation = entry->generation + 1U;
	if (generation == 0)
		generation = 1U;
	memset(entry, 0, sizeof(*entry));
	entry->generation = generation;
	entry->heap_pos = TR_TIMER_HEAP_NONE;
	entry->version = 1U;
	entry->callback = callback;
	entry->arg = arg;
	entry->used = 1;

	out->slot = slot;
	out->generation = generation;
	return TR_OK;
}

int tr_timer_queue_arm(struct tr_timer_queue *queue,
		       struct tr_timer_token token, uint64_t deadline_ns)
{
	struct tr_timer_entry *entry;
	uint64_t old_deadline;
	uint32_t pos;

	if (!tr_timer_token_valid(queue, token, &entry))
		return TR_ERR_STALE;

	old_deadline = entry->deadline_ns;
	entry->deadline_ns = deadline_ns;
	entry->version++;
	if (entry->version == 0)
		entry->version = 1U;

	if (deadline_ns == 0) {
		if (entry->heap_pos != TR_TIMER_HEAP_NONE)
			tr_timer_heap_remove(queue, entry->heap_pos);
		return TR_OK;
	}

	if (entry->heap_pos == TR_TIMER_HEAP_NONE) {
		if (queue->size == queue->capacity)
			return TR_AGAIN;
		pos = queue->size++;
		queue->heap[pos] = token.slot;
		entry->heap_pos = pos;
		tr_timer_heap_up(queue, pos);
		return TR_OK;
	}

	pos = entry->heap_pos;
	if (old_deadline == 0 || deadline_ns < old_deadline)
		tr_timer_heap_up(queue, pos);
	else if (deadline_ns > old_deadline)
		tr_timer_heap_down(queue, pos);
	return TR_OK;
}

int tr_timer_queue_unregister(struct tr_timer_queue *queue,
			      struct tr_timer_token token)
{
	struct tr_timer_entry *entry;

	if (!tr_timer_token_valid(queue, token, &entry))
		return TR_ERR_STALE;

	if (entry->heap_pos != TR_TIMER_HEAP_NONE)
		tr_timer_heap_remove(queue, entry->heap_pos);
	entry->deadline_ns = 0;
	entry->version++;
	entry->used = 0;
	entry->running = 0;
	entry->callback = NULL;
	entry->arg = NULL;
	/* Generation survives reuse; stale tokens cannot release this slot twice. */
	entry->next_free = queue->free_head;
	queue->free_head = token.slot;
	return TR_OK;
}

uint64_t tr_timer_queue_next_deadline(const struct tr_timer_queue *queue)
{
	if (!queue || queue->size == 0)
		return 0;
	return queue->entries[queue->heap[0]].deadline_ns;
}

size_t tr_timer_queue_run_due(struct tr_timer_queue *queue, uint64_t now_ns,
			      size_t max_callbacks, int *has_more_due)
{
	size_t count = 0;
	int more = 0;

	if (has_more_due)
		*has_more_due = 0;
	if (!queue)
		return 0;

	while (count < max_callbacks && queue->size != 0) {
		struct tr_timer_entry *entry;
		tr_timer_callback callback;
		struct tr_timer_token token;
		uint32_t slot;
		uint32_t generation;
		uint64_t version;
		uint64_t next_ns;

		slot = queue->heap[0];
		entry = &queue->entries[slot];
		if (entry->deadline_ns > now_ns)
			break;

		generation = entry->generation;
		version = entry->version;
		callback = entry->callback;
		token.slot = slot;
		token.generation = generation;

		tr_timer_heap_remove(queue, 0);
		entry->deadline_ns = 0;
		entry->running = 1;

		next_ns = callback ? callback(entry->arg, now_ns) : 0;

		entry = &queue->entries[slot];
		if (entry->used && entry->generation == generation) {
			entry->running = 0;
			/*
			 * callback 内显式 arm/unregister 会改变 version 或
			 * generation，此时 callback 返回值不能覆盖该操作。
			 */
			if (entry->version == version && next_ns != 0)
				(void)tr_timer_queue_arm(queue, token, next_ns);
		}
		count++;
	}

	if (queue->size != 0 &&
	    queue->entries[queue->heap[0]].deadline_ns <= now_ns)
		more = 1;
	if (has_more_due)
		*has_more_due = more;
	return count;
}
