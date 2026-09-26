#include "completion_queue.h"

#include "tr/cleanup.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

TR_DEFINE_PTR_OWNERSHIP(tr_completion_array, struct tr_completion, free)

int tr_completion_queue_init(struct tr_completion_queue *queue,
			     uint32_t capacity)
{
	struct tr_completion *items TR_AUTO(tr_completion_array_cleanup) = NULL;

	if (!queue || capacity == 0)
		return TR_ERR_INVALID;

	memset(queue, 0, sizeof(*queue));
	items = (struct tr_completion *)calloc(capacity, sizeof(*items));
	if (!items)
		return TR_ERR_NOMEM;

	if (pthread_mutex_init(&queue->lock, NULL) != 0)
		return TR_ERR_INVALID;

	queue->items = tr_completion_array_take(&items);
	queue->capacity = capacity;
	return TR_OK;
}

void tr_completion_queue_destroy(struct tr_completion_queue *queue)
{
	if (!queue)
		return;

	free(queue->items);
	queue->items = NULL;
	queue->capacity = 0;
	queue->head = 0;
	queue->tail = 0;
	queue->count = 0;
	queue->wake_pending = 0;
	pthread_mutex_destroy(&queue->lock);
}

int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake)
{
	int wake = 0;

	if (!queue || !completion || !completion->fn)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	if (queue->count == queue->capacity) {
		pthread_mutex_unlock(&queue->lock);
		return TR_AGAIN;
	}

	queue->items[queue->tail] = *completion;
	queue->tail = (queue->tail + 1U) % queue->capacity;
	queue->count++;

	if (!queue->wake_pending) {
		queue->wake_pending = 1;
		wake = 1;
	}
	pthread_mutex_unlock(&queue->lock);

	if (need_wake)
		*need_wake = wake;
	return TR_OK;
}

size_t tr_completion_queue_pop_batch(struct tr_completion_queue *queue,
				     struct tr_completion *out,
				     size_t max_completions,
				     int *has_more)
{
	size_t count = 0;
	int more = 0;

	if (has_more)
		*has_more = 0;
	if (!queue || !out)
		return 0;

	pthread_mutex_lock(&queue->lock);
	while (count < max_completions && queue->count != 0) {
		out[count++] = queue->items[queue->head];
		queue->head = (queue->head + 1U) % queue->capacity;
		queue->count--;
	}

	more = queue->count != 0;
	if (!more)
		queue->wake_pending = 0;
	pthread_mutex_unlock(&queue->lock);

	if (has_more)
		*has_more = more;
	return count;
}
