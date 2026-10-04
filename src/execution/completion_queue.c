#include "completion_queue.h"

#include "tr/cleanup.h"
#include "tr/status.h"
#include "../observability_internal.h"

#include <assert.h>
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
	if (pthread_cond_init(&queue->not_full, NULL) != 0) {
		pthread_mutex_destroy(&queue->lock);
		return TR_ERR_INVALID;
	}

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
	queue->peak_count = 0;
#ifndef NDEBUG
	assert(queue->waiters == 0U);
#endif
	queue->full_events = 0;
	queue->admission_generation = 0U;
	queue->waiters = 0U;
	queue->accepting = 0;
	queue->wake_pending = 0;
	pthread_cond_destroy(&queue->not_full);
	pthread_mutex_destroy(&queue->lock);
}

int tr_completion_queue_open(struct tr_completion_queue *queue)
{
	if (!queue)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	queue->admission_generation++;
	if (queue->admission_generation == 0U)
		queue->admission_generation = 1U;
	queue->accepting = 1;
	pthread_mutex_unlock(&queue->lock);
	return TR_OK;
}

void tr_completion_queue_close(struct tr_completion_queue *queue)
{
	if (!queue)
		return;

	pthread_mutex_lock(&queue->lock);
	queue->admission_generation++;
	if (queue->admission_generation == 0U)
		queue->admission_generation = 1U;
	queue->accepting = 0;
	/*
	 * stop/close 是 producer wait 的生命周期 fence。所有因 full 阻塞的
	 * worker 必须立即醒来观察 CLOSED，不能依赖 Reactor 再消费一批。
	 * generation 同时阻止旧 waiter 在后续 reopen 后误入新的运行期。
	 */
	pthread_cond_broadcast(&queue->not_full);
	pthread_mutex_unlock(&queue->lock);
}

int tr_completion_queue_push(struct tr_completion_queue *queue,
			     const struct tr_completion *completion,
			     int *need_wake)
{
	int wake = 0;

	if (!queue || !completion || !completion->fn)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	if (!queue->accepting) {
		pthread_mutex_unlock(&queue->lock);
		return TR_ERR_CLOSED;
	}
	if (queue->count == queue->capacity) {
		queue->full_events++;
		pthread_mutex_unlock(&queue->lock);
		return TR_AGAIN;
	}

	queue->items[queue->tail] = *completion;
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

int tr_completion_queue_push_wait(
	struct tr_completion_queue *queue,
	const struct tr_completion *completion,
	int *need_wake)
{
	int wake = 0;
	int waited = 0;
	uint64_t admission_generation;

	if (!queue || !completion || !completion->fn)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&queue->lock);
	admission_generation = queue->admission_generation;
	while (queue->accepting &&
	       queue->admission_generation == admission_generation &&
	       queue->count == queue->capacity) {
		int error;

		if (!waited) {
			queue->full_events++;
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

	if (!queue->accepting ||
	    queue->admission_generation != admission_generation) {
		pthread_mutex_unlock(&queue->lock);
		return TR_ERR_CLOSED;
	}

	queue->items[queue->tail] = *completion;
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

	/*
	 * 一次 batch 可能释放多个 slot。broadcast 避免只唤醒一个 producer 后，
	 * 其他 waiter 在 queue 已有空位时仍永久睡眠；producer 数量受 worker
	 * budget 限制，不在 socket I/O 热路径。
	 */
	if (count != 0U && queue->waiters != 0U)
		pthread_cond_broadcast(&queue->not_full);
	pthread_mutex_unlock(&queue->lock);

	if (has_more)
		*has_more = more;
	return count;
}
