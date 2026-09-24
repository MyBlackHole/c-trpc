#ifndef TR_GUARD_H
#define TR_GUARD_H

#include <pthread.h>

#include "tr/cleanup.h"

struct tr_mutex_guard {
	pthread_mutex_t *mutex;
};

static inline int tr_mutex_guard_acquire(struct tr_mutex_guard *guard,
					 pthread_mutex_t *mutex)
{
	int ret;

	if (!guard || !mutex)
		return -1;

	ret = pthread_mutex_lock(mutex);
	if (ret != 0)
		return ret;
	guard->mutex = mutex;
	return 0;
}

static inline int tr_mutex_guard_unlock(struct tr_mutex_guard *guard)
{
	pthread_mutex_t *mutex;

	if (!guard || !guard->mutex)
		return 0;

	mutex = guard->mutex;
	guard->mutex = NULL;
	return pthread_mutex_unlock(mutex);
}

static inline void tr_mutex_guard_cleanup(struct tr_mutex_guard *guard)
{
	(void)tr_mutex_guard_unlock(guard);
}

#endif
