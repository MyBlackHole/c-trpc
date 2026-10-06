#include "command_queue.h"
#include "tr/status.h"
#include "../cleanup.h"
#include "../observability_internal.h"

#include <assert.h>
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
	if (pthread_cond_init(&queue->not_full, NULL) != 0) {
		pthread_mutex_destroy(&queue->lock);
		return TR_ERR_INVALID;
	}

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
#ifndef NDEBUG
	assert(queue->waiters == 0U);
#endif
	queue->full_events = 0;
	queue->wait_generation = 0U;
	queue->waiters = 0U;
	queue->wait_accepting = 0;
	queue->pushed_send = 0;
	queue->full_send = 0;
	queue->pushed_resume_rx = 0;
	queue->full_resume_rx = 0;
	queue->pushed_call = 0;
	queue->full_call = 0;
	queue->pushed_other = 0;
	queue->full_other = 0;
	queue->wake_pending = 0;
	pthread_cond_destroy(&queue->not_full);
	pthread_mutex_destroy(&queue->lock);
}

static void tr_command_queue_observe_full_locked(
	struct tr_command_queue *queue, const struct tr_command *command)
{
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
}

static void tr_command_queue_observe_push_locked(
	struct tr_command_queue *queue, const struct tr_command *command)
{
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
}

static int tr_command_queue_enqueue_locked(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake)
{
	int wake = 0;

	tr_command_queue_observe_push_locked(queue, command);
	queue->items[queue->tail] = *command;
	queue->tail = (queue->tail + 1U) % queue->capacity;
	queue->count++;
	tr_observe_high_water_u32(&queue->peak_count, queue->count);

	if (!queue->wake_pending) {
		queue->wake_pending = 1;
		wake = 1;
	}
	if (need_wake)
		*need_wake = wake;
	return TR_OK;
}

int tr_command_queue_push(struct tr_command_queue *queue,
			  const struct tr_command *command, int *need_wake)
{
	int ret;

	if (!queue || !command)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	if (queue->count == queue->capacity) {
		tr_command_queue_observe_full_locked(queue, command);
		pthread_mutex_unlock(&queue->lock);
		return TR_AGAIN;
	}

	ret = tr_command_queue_enqueue_locked(queue, command, need_wake);
	pthread_mutex_unlock(&queue->lock);
	return ret;
}

int tr_command_queue_push_batch(
	struct tr_command_queue *queue, const struct tr_command *commands,
	size_t count, int *need_wake)
{
	size_t i;
	int wake = 0;

	if (!queue || !commands || count == 0U ||
	    count > (size_t)UINT32_MAX)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	if (count > (size_t)(queue->capacity - queue->count)) {
		/*
		 * One producer transaction hit capacity. Attribute the pressure to
		 * the first command; no command ownership transfers on failure.
		 */
		tr_command_queue_observe_full_locked(queue, &commands[0]);
		pthread_mutex_unlock(&queue->lock);
		return TR_AGAIN;
	}

	for (i = 0; i < count; ++i) {
		int item_wake = 0;

		(void)tr_command_queue_enqueue_locked(
			queue, &commands[i], &item_wake);
		if (item_wake)
			wake = 1;
	}
	if (need_wake)
		*need_wake = wake;
	pthread_mutex_unlock(&queue->lock);
	return TR_OK;
}

int tr_command_queue_wait_open(struct tr_command_queue *queue)
{
	if (!queue)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	queue->wait_generation++;
	if (queue->wait_generation == 0U)
		queue->wait_generation = 1U;
	queue->wait_accepting = 1;
	pthread_mutex_unlock(&queue->lock);
	return TR_OK;
}

void tr_command_queue_wait_close(struct tr_command_queue *queue)
{
	if (!queue)
		return;

	pthread_mutex_lock(&queue->lock);
	queue->wait_generation++;
	if (queue->wait_generation == 0U)
		queue->wait_generation = 1U;
	queue->wait_accepting = 0;
	pthread_cond_broadcast(&queue->not_full);
	pthread_mutex_unlock(&queue->lock);
}

uint64_t tr_command_queue_wait_generation(struct tr_command_queue *queue)
{
	uint64_t generation;

	if (!queue)
		return 0U;

	pthread_mutex_lock(&queue->lock);
	generation = queue->wait_accepting ? queue->wait_generation : 0U;
	pthread_mutex_unlock(&queue->lock);
	return generation;
}

int tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t expected_generation, int *need_wake)
{
	int waited = 0;
	int ret;

	if (!queue || !command || expected_generation == 0U)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	while (queue->wait_accepting &&
	       queue->wait_generation == expected_generation &&
	       queue->count == queue->capacity) {
		int error;

		if (!waited) {
			tr_command_queue_observe_full_locked(queue, command);
			waited = 1;
		}
		queue->waiters++;
		error = pthread_cond_wait(&queue->not_full, &queue->lock);
		queue->waiters--;
		if (error != 0) {
			pthread_mutex_unlock(&queue->lock);
			return TR_ERR_SYS;
		}
	}

	if (!queue->wait_accepting ||
	    queue->wait_generation != expected_generation) {
		pthread_mutex_unlock(&queue->lock);
		return TR_ERR_CLOSED;
	}

	ret = tr_command_queue_enqueue_locked(queue, command, need_wake);
	pthread_mutex_unlock(&queue->lock);
	return ret;
}

int tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake)
{
	int waited = 0;
	int ret;

	if (!queue || !command)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	while (queue->count == queue->capacity) {
		int error;

		if (!waited) {
			tr_command_queue_observe_full_locked(queue, command);
			waited = 1;
		}
		queue->waiters++;
		error = pthread_cond_wait(&queue->not_full, &queue->lock);
		queue->waiters--;
		if (error != 0) {
			pthread_mutex_unlock(&queue->lock);
			return TR_ERR_SYS;
		}
	}

	ret = tr_command_queue_enqueue_locked(queue, command, need_wake);
	pthread_mutex_unlock(&queue->lock);
	return ret;
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

	/*
	 * 一个 Reactor batch 可能释放多个 command slot。broadcast 让所有同步
	 * waiter/STOP waiter 重新竞争实际空位，避免已有容量时仍有 producer 睡眠。
	 */
	if (count != 0U && queue->waiters != 0U)
		pthread_cond_broadcast(&queue->not_full);

	pthread_mutex_unlock(&queue->lock);
	return count;
}
