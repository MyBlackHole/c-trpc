#include "tr/command_queue.h"
#include "tr/status.h"
#include "tr/cleanup.h"
#include "observability_internal.h"

#include <stdlib.h>
#include <string.h>

TR_DEFINE_PTR_OWNERSHIP(tr_command_array, struct tr_command, free)

int tr_command_queue_init(struct tr_command_queue *queue, uint32_t capacity)
{
	struct tr_command *items TR_AUTO(tr_command_array_cleanup) = NULL;

	if (!queue || capacity == 0)
		return TR_ERR_INVALID;

	memset(queue, 0, sizeof(*queue));

	items = (struct tr_command *)calloc(capacity, sizeof(*items));
	if (!items)
		return TR_ERR_NOMEM;

	if (pthread_mutex_init(&queue->lock, NULL) != 0)
		return TR_ERR_INVALID;

	queue->items = tr_command_array_take(&items);
	queue->capacity = capacity;
	return TR_OK;
}

void tr_command_queue_destroy(struct tr_command_queue *queue)
{
	if (!queue)
		return;

	free(queue->items);
	queue->items = NULL;
	queue->capacity = 0;
	queue->head = 0;
	queue->tail = 0;
	queue->count = 0;
	queue->peak_count = 0;
	queue->full_events = 0;
	queue->pushed_send = 0;
	queue->full_send = 0;
	queue->pushed_resume_rx = 0;
	queue->full_resume_rx = 0;
	queue->pushed_call = 0;
	queue->full_call = 0;
	queue->pushed_other = 0;
	queue->full_other = 0;
	queue->wake_pending = 0;
	pthread_mutex_destroy(&queue->lock);
}

int tr_command_queue_push(struct tr_command_queue *queue,
			  const struct tr_command *command, int *need_wake)
{
	int wake = 0;

	if (!queue || !command)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);

	if (queue->count == queue->capacity) {
		queue->full_events++;
		switch (command->type) {
		case TR_CMD_SEND:
			queue->full_send++;
			break;
		case TR_CMD_RESUME_RX:
			queue->full_resume_rx++;
			break;
		case TR_CMD_CALL:
			queue->full_call++;
			break;
		default:
			queue->full_other++;
			break;
		}
		pthread_mutex_unlock(&queue->lock);
		return TR_AGAIN;
	}

	switch (command->type) {
	case TR_CMD_SEND:
		queue->pushed_send++;
		break;
	case TR_CMD_RESUME_RX:
		queue->pushed_resume_rx++;
		break;
	case TR_CMD_CALL:
		queue->pushed_call++;
		break;
	default:
		queue->pushed_other++;
		break;
	}

	queue->items[queue->tail] = *command;
	queue->tail = (queue->tail + 1U) % queue->capacity;
	queue->count++;
	tr_observe_high_water_u32(&queue->peak_count, queue->count);

	if (!queue->wake_pending) {
		queue->wake_pending = 1;
		wake = 1;
	}

	pthread_mutex_unlock(&queue->lock);

	if (need_wake)
		*need_wake = wake;

	return TR_OK;
}

int tr_command_queue_is_empty(struct tr_command_queue *queue)
{
	int empty;

	if (!queue)
		return 0;

	pthread_mutex_lock(&queue->lock);
	empty = queue->count == 0;
	pthread_mutex_unlock(&queue->lock);
	return empty;
}

size_t tr_command_queue_pop_batch(struct tr_command_queue *queue,
				  struct tr_command *out, size_t max_commands)
{
	size_t count = 0;

	if (!queue || !out || max_commands == 0)
		return 0;

	pthread_mutex_lock(&queue->lock);

	while (count < max_commands && queue->count != 0) {
		out[count++] = queue->items[queue->head];
		queue->head = (queue->head + 1U) % queue->capacity;
		queue->count--;
	}

	if (queue->count == 0)
		queue->wake_pending = 0;

	pthread_mutex_unlock(&queue->lock);
	return count;
}
