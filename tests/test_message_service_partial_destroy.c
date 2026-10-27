#define _POSIX_C_SOURCE 200809L
#include "../src/runtime/message_service.h"
#include "../src/execution/reactor_internal.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define MIB ((size_t)1024 * 1024)

/* 仅此测试跟踪实际成功销毁的同步对象，不用整个 executor_destroy 返回
 * 一个合成错误来替代“条件变量已销毁、互斥锁仍存活”的中间状态。 */
static _Thread_local bool inside_destroy, creating_service;
static atomic_uintptr_t retired_condition;
static atomic_uintptr_t surviving_mutex;
static atomic_uint condition_destroys, mutex_failures, mutex_destroys;
static atomic_uint failures_left;
static atomic_int service_fd = -1;

int __real_tr_task_executor_destroy(struct tr_task_executor **);
int __real_pthread_cond_destroy(pthread_cond_t *);
int __real_pthread_mutex_destroy(pthread_mutex_t *);
int __real_pthread_cond_signal(pthread_cond_t *);
int __real_pthread_cond_broadcast(pthread_cond_t *);
int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
int __real_eventfd(unsigned int, int);

int __wrap_tr_task_executor_destroy(struct tr_task_executor **executor)
{
	int ret;
	CHECK(!inside_destroy);
	inside_destroy = true;
	ret = __real_tr_task_executor_destroy(executor);
	inside_destroy = false;
	return ret;
}

static void condition_is_live(pthread_cond_t *condition)
{
	CHECK((uintptr_t)condition != atomic_load(&retired_condition));
}

int __wrap_pthread_cond_destroy(pthread_cond_t *condition)
{
	int ret;
	condition_is_live(condition);
	ret = __real_pthread_cond_destroy(condition);
	if (inside_destroy && ret == 0) {
		CHECK(atomic_load(&retired_condition) == 0);
		atomic_store(&retired_condition, (uintptr_t)condition);
		atomic_fetch_add(&condition_destroys, 1);
	}
	return ret;
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	int ret;
	if (inside_destroy && atomic_load(&failures_left)) {
		CHECK(atomic_load(&retired_condition) != 0);
		atomic_store(&surviving_mutex, (uintptr_t)mutex);
		atomic_fetch_sub(&failures_left, 1);
		atomic_fetch_add(&mutex_failures, 1);
		return EBUSY;
	}
	ret = __real_pthread_mutex_destroy(mutex);
	if (inside_destroy && ret == 0 &&
	    (uintptr_t)mutex == atomic_load(&surviving_mutex))
		atomic_fetch_add(&mutex_destroys, 1);
	return ret;
}

int __wrap_pthread_cond_signal(pthread_cond_t *condition)
{
	condition_is_live(condition);
	return __real_pthread_cond_signal(condition);
}

int __wrap_pthread_cond_broadcast(pthread_cond_t *condition)
{
	condition_is_live(condition);
	return __real_pthread_cond_broadcast(condition);
}

int __wrap_pthread_cond_wait(pthread_cond_t *condition, pthread_mutex_t *mutex)
{
	condition_is_live(condition);
	return __real_pthread_cond_wait(condition, mutex);
}

int __wrap_eventfd(unsigned int initial, int flags)
{
	int fd = __real_eventfd(initial, flags);
	if (creating_service && fd >= 0)
		atomic_store(&service_fd, fd);
	return fd;
}

struct fixture {
	struct tr_reactor *reactor;
	struct tr_message_service *service;
	struct tr_memory_budget budget;
	int reaper_result;
};

static int create_on_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_message_service_config config = {{1, 1}, 1, 10000000};
	int ret;
	creating_service = true;
	ret = tr_message_service_create_on_owner(f->reactor, &config,
	                                         &f->budget, &f->service);
	creating_service = false;
	return ret == TR_OK ? tr_message_service_start_on_owner(f->service)
	                    : ret;
}

static int close_on_owner(void *arg)
{
	struct fixture *f = arg;
	return tr_message_service_close_on_owner(&f->service);
}

static uint64_t now_seconds(void)
{
	struct timespec now;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec;
}

static void *reap_then_exit(void *arg)
{
	struct fixture *f = arg;
	f->reaper_result = tr_message_service_reap_stopped(&f->service);
	return NULL;
}

static void test_partial_destroy(bool switch_reaper)
{
	struct fixture f = {0};
	pthread_t thread;
	uint64_t held, limit;
	int fd, ret;

	atomic_store(&retired_condition, 0);
	atomic_store(&surviving_mutex, 0);
	atomic_store(&condition_destroys, 0);
	atomic_store(&mutex_failures, 0);
	atomic_store(&mutex_destroys, 0);
	atomic_store(&failures_left, 2);
	atomic_store(&service_fd, -1);
	tr_memory_budget_init(&f.budget, 8 * MIB);
	CHECK(tr_reactor_create(NULL, NULL, NULL, NULL, &f.reactor) == TR_OK);
	CHECK(tr_reactor_start(f.reactor) == TR_OK);
	CHECK(tr_reactor_call(f.reactor, create_on_owner, &f) == TR_OK);
	limit = now_seconds() + 15;
	while ((ret = tr_reactor_call(f.reactor, close_on_owner, &f)) ==
	       TR_AGAIN) {
		CHECK(now_seconds() < limit);
		sched_yield();
	}
	CHECK(ret == TR_ERR_SYS && f.service);
	CHECK(atomic_load(&condition_destroys) == 1 &&
	      atomic_load(&mutex_failures) == 1 &&
	      !atomic_load(&mutex_destroys));
	held = atomic_load(&f.budget.current_bytes);
	fd = atomic_load(&service_fd);
	CHECK(held > 0 && fd >= 0 && fcntl(fd, F_GETFD) >= 0);
	/* 旧 owner 尚未 join，不能仅因子执行器已经部分销毁就提前接管。 */
	CHECK(tr_message_service_reap_stopped(&f.service) == TR_ERR_STATE);
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	if (switch_reaper) {
		CHECK(pthread_create(&thread, NULL, reap_then_exit, &f) == 0);
		CHECK(pthread_join(thread, NULL) == 0);
		ret = f.reaper_result;
	} else {
		ret = tr_message_service_reap_stopped(&f.service);
	}
	CHECK(ret == TR_ERR_SYS && f.service);
	CHECK(atomic_load(&condition_destroys) == 1 &&
	      atomic_load(&mutex_failures) == 2 &&
	      !atomic_load(&mutex_destroys));
	CHECK(atomic_load(&f.budget.current_bytes) == held &&
	      fcntl(fd, F_GETFD) >= 0);
	CHECK(tr_reactor_start(f.reactor) == TR_ERR_STATE);
	CHECK(tr_reactor_destroy(f.reactor) == TR_ERR_STATE);
	/* 租约跨失败保持；新的串行回收线程只能继续尚未完成的互斥锁销毁。 */
	ret = tr_message_service_reap_stopped(&f.service);
	if (ret != TR_OK)
		fprintf(stderr, "final service reap returned %d\n", ret);
	CHECK(ret == TR_OK && !f.service);
	CHECK(atomic_load(&condition_destroys) == 1 &&
	      atomic_load(&mutex_destroys) == 1);
	CHECK(atomic_load(&f.budget.current_bytes) == 0);
	CHECK(fcntl(fd, F_GETFD) < 0 && errno == EBADF);
	atomic_store(&retired_condition, 0);
	atomic_store(&surviving_mutex, 0);
	CHECK(tr_reactor_destroy(f.reactor) == TR_OK);
}

int main(void)
{
	test_partial_destroy(false);
	test_partial_destroy(true);
	puts("message_service_partial_destroy: ok");
	return 0;
}
