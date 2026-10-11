#ifndef TR_TEST_DRAIN_DEADLINE_PROBE_H
#define TR_TEST_DRAIN_DEADLINE_PROBE_H

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "../src/execution/deadline_internal.h"
#include "tr/status.h"

/*
 * 测试链接期故障注入：只有发起 wait_drained 的控制线程启用 probe。
 * Reactor owner / 其他测试线程直接进入原函数，避免影响真实状态推进。
 */
struct tr_test_drain_deadline_probe {
	struct timespec absolute;
	uint32_t expected_ms;
	unsigned int init_calls;
	unsigned int timedwait_calls;
	int fail_init;
};

static _Thread_local struct tr_test_drain_deadline_probe
	*tr_test_active_drain_deadline_probe;

int __real_tr_deadline_init_ms(struct tr_deadline *deadline,
			       uint32_t timeout_ms);
int __real_pthread_cond_timedwait(pthread_cond_t *cond,
				  pthread_mutex_t *mutex,
				  const struct timespec *abstime);

int __wrap_tr_deadline_init_ms(struct tr_deadline *deadline,
			      uint32_t timeout_ms)
{
	struct tr_test_drain_deadline_probe *probe =
		tr_test_active_drain_deadline_probe;
	int ret;

	if (!probe)
		return __real_tr_deadline_init_ms(deadline, timeout_ms);
	assert(deadline != NULL);
	assert(timeout_ms == probe->expected_ms);
	probe->init_calls++;
	if (probe->fail_init)
		return TR_ERR_SYS;

	ret = __real_tr_deadline_init_ms(deadline, timeout_ms);
	assert(ret == TR_OK);
	assert(deadline->infinite == 0);
	probe->absolute = deadline->absolute;
	return ret;
}

int __wrap_pthread_cond_timedwait(pthread_cond_t *cond,
				  pthread_mutex_t *mutex,
				  const struct timespec *abstime)
{
	struct tr_test_drain_deadline_probe *probe =
		tr_test_active_drain_deadline_probe;

	if (!probe)
		return __real_pthread_cond_timedwait(cond, mutex, abstime);
	assert(cond != NULL);
	assert(mutex != NULL);
	assert(abstime != NULL);
	assert(probe->init_calls == 1U);
	assert(probe->fail_init == 0);
	assert(abstime->tv_sec == probe->absolute.tv_sec);
	assert(abstime->tv_nsec == probe->absolute.tv_nsec);
	probe->timedwait_calls++;
	/* 模拟一次真实的 condwait 超时；调用方仍然持有 mutex。 */
	return ETIMEDOUT;
}

static void tr_test_drain_deadline_probe_begin(
	struct tr_test_drain_deadline_probe *probe,
	uint32_t timeout_ms, int fail_init)
{
	assert(tr_test_active_drain_deadline_probe == NULL);
	assert(probe != NULL);
	memset(probe, 0, sizeof(*probe));
	probe->expected_ms = timeout_ms;
	probe->fail_init = fail_init;
	tr_test_active_drain_deadline_probe = probe;
}

static void tr_test_drain_deadline_probe_end(
	struct tr_test_drain_deadline_probe *probe,
	unsigned int init_calls, unsigned int timedwait_calls)
{
	assert(tr_test_active_drain_deadline_probe == probe);
	tr_test_active_drain_deadline_probe = NULL;
	assert(probe->init_calls == init_calls);
	assert(probe->timedwait_calls == timedwait_calls);
}

#endif
