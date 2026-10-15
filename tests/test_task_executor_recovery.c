#define _POSIX_C_SOURCE 200809L
#include "../src/execution/task_executor_recovery.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)

struct fixture {
	struct tr_task_executor *executor;
	struct tr_memory_budget budget;
	struct tr_task_handle handle;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t reaper;
	bool released;
	unsigned mode;
	unsigned done;
	_Atomic unsigned work;
	_Atomic unsigned notifications;
};

static _Thread_local bool fail_join, fail_mutex_destroy;
int __real_pthread_join(pthread_t thread, void **result);
int __real_pthread_mutex_destroy(pthread_mutex_t *lock);

int __wrap_pthread_join(pthread_t thread, void **result)
{
	if (fail_join) {
		fail_join = false;
		return EBUSY;
	}
	return __real_pthread_join(thread, result);
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *lock)
{
	if (fail_mutex_destroy) {
		fail_mutex_destroy = false;
		return EBUSY;
	}
	return __real_pthread_mutex_destroy(lock);
}

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static int work(void *arg, const struct tr_task_cancel *cancel)
{
	struct fixture *f = arg;
	struct timespec until;
	CHECK(clock_gettime(CLOCK_REALTIME, &until) == 0);
	until.tv_sec += 10;
	CHECK(pthread_mutex_lock(&f->lock) == 0);
	while (!f->released)
		CHECK(pthread_cond_timedwait(&f->cond, &f->lock, &until) == 0);
	CHECK(pthread_mutex_unlock(&f->lock) == 0);
	CHECK(tr_task_cancel_requested(cancel) == (f->mode == 0));
	atomic_fetch_add(&f->work, 1);
	return 73;
}

static void notify(void *arg)
{
	struct fixture *f = arg;
	atomic_fetch_add(&f->notifications, 1);
}

static void done(void *arg, struct tr_task_handle handle,
                 struct tr_task_result result)
{
	struct fixture *f = arg;
	CHECK(pthread_equal(pthread_self(), f->reaper));
	CHECK(handle.instance == f->handle.instance &&
	      handle.generation == f->handle.generation);
	CHECK(atomic_load(&f->work) == 1);
	CHECK(result.work_status == 73 && result.cancelled == (f->mode == 0));
	CHECK(tr_task_executor_takeover_quiesced(f->executor) == TR_ERR_STATE);
	++f->done;
}

static int join_until(struct tr_task_executor *executor)
{
	uint64_t until = now_ms() + 10000;
	int ret;
	while ((ret = tr_task_executor_join(executor)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	return ret;
}

static void *old_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config config = {1, 1};
	struct tr_task_spec spec = {work, done, f};
	CHECK(tr_task_executor_create(&config, &f->budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_set_notify(f->executor, notify, f) == TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	if (f->mode < 2)
		CHECK(tr_task_executor_submit(f->executor, &spec, &f->handle) ==
		      TR_OK);
	if (f->mode == 1) {
		uint64_t until = now_ms() + 10000;
		while (!atomic_load(&f->notifications)) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(tr_task_executor_stop(f->executor, TR_TASK_DRAIN) ==
		      TR_OK);
		fail_join = true;
		CHECK(join_until(f->executor) == TR_ERR_SYS);
	} else if (f->mode == 2) {
		CHECK(tr_task_executor_stop(f->executor, TR_TASK_DRAIN) ==
		      TR_OK);
		CHECK(join_until(f->executor) == TR_OK);
		/* 条件变量先销毁成功，互斥锁销毁失败；接管不能再广播条件变量。
		 */
		fail_mutex_destroy = true;
		CHECK(tr_task_executor_destroy(&f->executor) == TR_ERR_SYS);
	}
	return NULL;
}

static void *second_reaper(void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_task_executor_takeover_quiesced(f->executor) == TR_ERR_STATE);
	return NULL;
}

static void test_recovery(unsigned mode)
{
	struct fixture f = {.mode = mode, .released = mode != 0};
	pthread_t owner, contender;
	size_t count;
	uint64_t reserved;
	atomic_init(&f.work, 0);
	atomic_init(&f.notifications, 0);
	f.reaper = pthread_self();
	tr_memory_budget_init(&f.budget, 1024 * 1024);
	CHECK(pthread_mutex_init(&f.lock, NULL) == 0);
	CHECK(pthread_cond_init(&f.cond, NULL) == 0);
	CHECK(pthread_create(&owner, NULL, old_owner, &f) == 0);
	/* 这是实际旧调度线程的 pthread_join，不用“看起来已经退出”代替。 */
	CHECK(pthread_join(owner, NULL) == 0);
	reserved = atomic_load(&f.budget.current_bytes);
	CHECK(reserved > 0 && f.executor);
	if (mode == 0) {
		CHECK(tr_task_executor_takeover_quiesced(f.executor) ==
		      TR_ERR_STATE);
		CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) ==
		      TR_OK);
	}
	CHECK(tr_task_executor_takeover_quiesced(f.executor) == TR_OK);
	CHECK(tr_task_executor_takeover_quiesced(f.executor) == TR_OK);
	CHECK(pthread_create(&contender, NULL, second_reaper, &f) == 0);
	CHECK(pthread_join(contender, NULL) == 0);
	if (mode == 0) {
		CHECK(tr_task_executor_join(f.executor) == TR_AGAIN);
		CHECK(tr_task_executor_dispatch(f.executor, 1, &count) ==
		      TR_OK);
		CHECK(count == 0 && f.done == 0);
		CHECK(atomic_load(&f.budget.current_bytes) == reserved);
		CHECK(pthread_mutex_lock(&f.lock) == 0);
		f.released = true;
		CHECK(pthread_cond_broadcast(&f.cond) == 0);
		CHECK(pthread_mutex_unlock(&f.lock) == 0);
	}
	if (mode < 2) {
		CHECK(join_until(f.executor) == TR_OK);
		CHECK(tr_task_executor_destroy(&f.executor) == TR_AGAIN);
		CHECK(tr_task_executor_dispatch(f.executor, 1, &count) ==
		      TR_OK);
		CHECK(count == 1 && f.done == 1);
		CHECK(tr_task_executor_cancel(f.executor, f.handle) ==
		      TR_ERR_STALE);
		/* mode=0 的完成发生在接管后，旧通知不得再运行。 */
		CHECK(atomic_load(&f.notifications) == (mode == 1 ? 1U : 0U));
	}
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK && !f.executor);
	CHECK(atomic_load(&f.budget.current_bytes) == 0);
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
}

int main(void)
{
	for (unsigned mode = 0; mode < 3; ++mode)
		test_recovery(mode);
	puts("task_executor_recovery: ok");
	return 0;
}
