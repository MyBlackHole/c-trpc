#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <time.h>

/* 除持锁观测值外，其余配置在 pthread_create 至线程回收期间保持不变。 */
static pthread_t caller;
static int expected_timeout;
static int observed;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t condition = PTHREAD_COND_INITIALIZER;

int __real_clock_gettime(clockid_t clock_id, struct timespec *ts);
int __real_epoll_wait(int fd, struct epoll_event *events, int max, int timeout);

int __wrap_clock_gettime(clockid_t clock_id, struct timespec *ts)
{
	if (clock_id == CLOCK_MONOTONIC && !pthread_equal(pthread_self(), caller)) {
		ts->tv_sec = 0;
		ts->tv_nsec = 1;
		return 0;
	}
	return __real_clock_gettime(clock_id, ts);
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int max, int timeout)
{
	if (timeout != expected_timeout)
		fprintf(stderr, "epoll timeout=%d expected=%d\n",
			timeout, expected_timeout);
	assert(timeout == expected_timeout);
	assert(pthread_mutex_lock(&lock) == 0);
	observed = 1;
	assert(pthread_cond_signal(&condition) == 0);
	assert(pthread_mutex_unlock(&lock) == 0);
	return __real_epoll_wait(fd, events, max, timeout);
}

static uint64_t unexpected_callback(void *arg, uint64_t now)
{
	(void)arg;
	(void)now;
	assert(0 && "fixed owner clock cannot reach future deadline");
	return 0U;
}

static void check_timeout(uint64_t delta_ns, int milliseconds)
{
	struct tr_reactor *reactor = NULL;
	struct tr_reactor_timer_handle timer;
	struct timespec deadline;

	expected_timeout = milliseconds;
	observed = 0;
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_timer_register(reactor, unexpected_callback, NULL,
					 &timer) == TR_OK);
	assert(tr_reactor_timer_arm(timer, 1U + delta_ns) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&lock) == 0);
	while (!observed)
		assert(pthread_cond_timedwait(&condition, &lock, &deadline) == 0);
	assert(pthread_mutex_unlock(&lock) == 0);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_timer_unregister(timer) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

int main(void)
{
	caller = pthread_self();
	check_timeout(1U, 1);
	check_timeout(UINT64_C(1000000), 1);
	check_timeout(UINT64_C(1000001), 2);
	check_timeout((uint64_t)INT_MAX * UINT64_C(1000000), INT_MAX);
	check_timeout((uint64_t)INT_MAX * UINT64_C(1000000) + 1U, INT_MAX);
	check_timeout(UINT64_MAX - 1U, INT_MAX);
	return 0;
}
