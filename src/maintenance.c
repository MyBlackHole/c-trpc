#include "maintenance.h"

#include "tr/status.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct tr_maintenance_entry {
	uint32_t generation;
	int used;
	int running;
	uint64_t deadline_ns;
	uint64_t version;
	tr_maintenance_cb callback;
	void *arg;
};

struct tr_maintenance_scheduler {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t thread;
	struct tr_maintenance_entry *entries;
	uint32_t capacity;
	int started;
	int stopping;
};

uint64_t tr_maintenance_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static int tr_maintenance_handle_valid_locked(
	struct tr_maintenance_handle handle,
	struct tr_maintenance_entry **out)
{
	struct tr_maintenance_scheduler *scheduler = handle.scheduler;
	struct tr_maintenance_entry *entry;

	if (!scheduler || handle.slot >= scheduler->capacity)
		return 0;

	entry = &scheduler->entries[handle.slot];
	if (!entry->used || entry->generation != handle.generation)
		return 0;

	if (out)
		*out = entry;
	return 1;
}

static int tr_maintenance_find_due_locked(
	struct tr_maintenance_scheduler *scheduler, uint64_t now_ns,
	uint32_t *slot_out, uint64_t *earliest_out)
{
	uint64_t earliest = 0;
	uint32_t due_slot = UINT32_MAX;
	uint32_t i;

	for (i = 0; i < scheduler->capacity; ++i) {
		struct tr_maintenance_entry *entry = &scheduler->entries[i];

		if (!entry->used || entry->running || entry->deadline_ns == 0)
			continue;
		if (entry->deadline_ns <= now_ns) {
			due_slot = i;
			break;
		}
		if (earliest == 0 || entry->deadline_ns < earliest)
			earliest = entry->deadline_ns;
	}

	if (slot_out)
		*slot_out = due_slot;
	if (earliest_out)
		*earliest_out = earliest;
	return due_slot != UINT32_MAX;
}

static void *tr_maintenance_thread_main(void *arg)
{
	struct tr_maintenance_scheduler *scheduler =
		(struct tr_maintenance_scheduler *)arg;

	pthread_mutex_lock(&scheduler->lock);
	while (!scheduler->stopping) {
		uint64_t now_ns = tr_maintenance_now_ns();
		uint64_t earliest = 0;
		uint32_t slot = UINT32_MAX;

		if (!tr_maintenance_find_due_locked(scheduler, now_ns, &slot,
						    &earliest)) {
			if (scheduler->stopping)
				break;
			if (earliest == 0) {
				pthread_cond_wait(&scheduler->cond,
						  &scheduler->lock);
			} else {
				struct timespec ts;
				ts.tv_sec =
					(time_t)(earliest /
						 UINT64_C(1000000000));
				ts.tv_nsec =
					(long)(earliest %
					       UINT64_C(1000000000));
				(void)pthread_cond_timedwait(
					&scheduler->cond, &scheduler->lock,
					&ts);
			}
			continue;
		}

		{
			struct tr_maintenance_entry *entry =
				&scheduler->entries[slot];
			tr_maintenance_cb callback = entry->callback;
			void *callback_arg = entry->arg;
			uint32_t generation = entry->generation;
			uint64_t version = entry->version;
			uint64_t next_ns;

			entry->running = 1;
			entry->deadline_ns = 0;
			pthread_mutex_unlock(&scheduler->lock);

			next_ns = callback ?
					  callback(callback_arg,
						   tr_maintenance_now_ns()) :
					  0;

			pthread_mutex_lock(&scheduler->lock);
			entry = &scheduler->entries[slot];
			if (entry->used &&
			    entry->generation == generation) {
				entry->running = 0;
				/*
				 * callback 执行期间如果其他线程重新 arm，
				 * version 会变化，此时不能用 callback 返回值
				 * 覆盖更新后的 deadline。
				 */
				if (entry->version == version)
					entry->deadline_ns = next_ns;
				pthread_cond_broadcast(&scheduler->cond);
			}
		}
	}
	pthread_mutex_unlock(&scheduler->lock);
	return NULL;
}

int tr_maintenance_scheduler_create(uint32_t capacity,
				    struct tr_maintenance_scheduler **out)
{
	struct tr_maintenance_scheduler *scheduler;
	pthread_condattr_t attr;
	int attr_ready = 0;

	if (!out || capacity == 0)
		return TR_ERR_INVALID;
	*out = NULL;

	scheduler = (struct tr_maintenance_scheduler *)calloc(
		1, sizeof(*scheduler));
	if (!scheduler)
		return TR_ERR_NOMEM;

	scheduler->entries = (struct tr_maintenance_entry *)calloc(
		capacity, sizeof(*scheduler->entries));
	if (!scheduler->entries) {
		free(scheduler);
		return TR_ERR_NOMEM;
	}
	scheduler->capacity = capacity;

	if (pthread_mutex_init(&scheduler->lock, NULL) != 0)
		goto fail;
	if (pthread_condattr_init(&attr) != 0)
		goto fail_lock;
	attr_ready = 1;
	if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0)
		goto fail_attr;
	if (pthread_cond_init(&scheduler->cond, &attr) != 0)
		goto fail_attr;
	pthread_condattr_destroy(&attr);
	attr_ready = 0;

	if (pthread_create(&scheduler->thread, NULL,
			   tr_maintenance_thread_main, scheduler) != 0)
		goto fail_cond;
	scheduler->started = 1;

	*out = scheduler;
	return TR_OK;

fail_cond:
	pthread_cond_destroy(&scheduler->cond);
fail_attr:
	if (attr_ready)
		pthread_condattr_destroy(&attr);
fail_lock:
	pthread_mutex_destroy(&scheduler->lock);
fail:
	free(scheduler->entries);
	free(scheduler);
	return TR_ERR_SYS;
}

void tr_maintenance_scheduler_destroy(struct tr_maintenance_scheduler *scheduler)
{
	if (!scheduler)
		return;

	pthread_mutex_lock(&scheduler->lock);
	scheduler->stopping = 1;
	pthread_cond_broadcast(&scheduler->cond);
	pthread_mutex_unlock(&scheduler->lock);

	if (scheduler->started)
		(void)pthread_join(scheduler->thread, NULL);

	pthread_cond_destroy(&scheduler->cond);
	pthread_mutex_destroy(&scheduler->lock);
	free(scheduler->entries);
	free(scheduler);
}

int tr_maintenance_register(struct tr_maintenance_scheduler *scheduler,
			    tr_maintenance_cb callback, void *arg,
			    struct tr_maintenance_handle *out)
{
	uint32_t i;

	if (!scheduler || !callback || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));
	pthread_mutex_lock(&scheduler->lock);
	if (scheduler->stopping) {
		pthread_mutex_unlock(&scheduler->lock);
		return TR_ERR_CLOSED;
	}

	for (i = 0; i < scheduler->capacity; ++i) {
		struct tr_maintenance_entry *entry = &scheduler->entries[i];
		uint32_t generation;

		if (entry->used)
			continue;

		generation = entry->generation + 1U;
		if (generation == 0)
			generation = 1U;
		memset(entry, 0, sizeof(*entry));
		entry->generation = generation;
		entry->used = 1;
		entry->callback = callback;
		entry->arg = arg;
		entry->version = 1U;

		out->scheduler = scheduler;
		out->slot = i;
		out->generation = generation;
		pthread_mutex_unlock(&scheduler->lock);
		return TR_OK;
	}

	pthread_mutex_unlock(&scheduler->lock);
	return TR_AGAIN;
}

int tr_maintenance_arm(struct tr_maintenance_handle handle,
		       uint64_t deadline_ns)
{
	struct tr_maintenance_scheduler *scheduler = handle.scheduler;
	struct tr_maintenance_entry *entry;

	if (!scheduler)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&scheduler->lock);
	if (!tr_maintenance_handle_valid_locked(handle, &entry)) {
		pthread_mutex_unlock(&scheduler->lock);
		return TR_ERR_STALE;
	}
	if (scheduler->stopping) {
		pthread_mutex_unlock(&scheduler->lock);
		return TR_ERR_CLOSED;
	}

	entry->deadline_ns = deadline_ns;
	entry->version++;
	if (entry->version == 0)
		entry->version = 1U;
	pthread_cond_signal(&scheduler->cond);
	pthread_mutex_unlock(&scheduler->lock);
	return TR_OK;
}

int tr_maintenance_unregister(struct tr_maintenance_handle handle)
{
	struct tr_maintenance_scheduler *scheduler = handle.scheduler;
	struct tr_maintenance_entry *entry;

	if (!scheduler)
		return TR_ERR_INVALID;
	if (scheduler->started &&
	    pthread_equal(pthread_self(), scheduler->thread))
		return TR_ERR_STATE;

	pthread_mutex_lock(&scheduler->lock);
	if (!tr_maintenance_handle_valid_locked(handle, &entry)) {
		pthread_mutex_unlock(&scheduler->lock);
		return TR_ERR_STALE;
	}

	entry->deadline_ns = 0;
	entry->version++;
	while (entry->running)
		pthread_cond_wait(&scheduler->cond, &scheduler->lock);

	entry->used = 0;
	entry->callback = NULL;
	entry->arg = NULL;
	pthread_cond_signal(&scheduler->cond);
	pthread_mutex_unlock(&scheduler->lock);
	return TR_OK;
}
