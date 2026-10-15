#define _POSIX_C_SOURCE 200809L
#include "../src/runtime/message_service.h"
#include "../src/message/storage.h"
#include "../src/execution/reactor_internal.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <zstd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
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
#define JOBS 3U

struct fixture {
	struct tr_reactor *reactor;
	struct tr_message_service *service;
	struct tr_memory_budget budget;
	struct tr_reactor_timer_handle timer;
	pthread_t owner;
	pthread_t reaper;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool worker_entered;
	bool worker_released;
	bool start;
	uint8_t *data;
	struct tr_message retained;
	_Atomic int fd;
	_Atomic bool gate_worker;
	_Atomic bool hold_delivery;
	_Atomic bool fail_poll;
	_Atomic bool poll_failed;
	_Atomic bool fail_parent_join;
	_Atomic bool fail_worker_join;
	_Atomic bool fail_aux;
	_Atomic bool fail_timer;
	_Atomic bool fail_final_mutex;
	_Atomic bool event_live;
	_Atomic bool timer_live;
	_Atomic unsigned notifications;
	_Atomic unsigned normal[JOBS];
	_Atomic unsigned abandoned[JOBS];
	_Atomic unsigned released[JOBS];
	_Atomic unsigned closes;
	tr_reactor_aux_event_cb event;
	tr_reactor_timer_cb timer_cb;
	void *event_arg;
	void *timer_arg;
};

struct job {
	struct fixture *fixture;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_task_handle handle;
	unsigned index;
};

static struct fixture *active;
static _Thread_local bool creating, mutex_failure;
int __real_eventfd(unsigned, int);
int __real_close(int);
ssize_t __real_write(int, const void *, size_t);
int __real_epoll_wait(int, struct epoll_event *, int, int);
int __real_pthread_join(pthread_t, void **);
int __real_pthread_mutex_destroy(pthread_mutex_t *);
int __real_tr_task_executor_destroy(struct tr_task_executor **);
int __real_tr_reactor_aux_event_register(struct tr_reactor *, int, uint32_t,
                                         tr_reactor_aux_event_cb, void *);
int __real_tr_reactor_aux_event_unregister(struct tr_reactor *, int);
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
size_t __real_ZSTD_compressStream2(ZSTD_CCtx *, ZSTD_outBuffer *,
                                   ZSTD_inBuffer *, ZSTD_EndDirective);

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static struct timespec deadline(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_REALTIME, &ts) == 0);
	ts.tv_sec += 10;
	return ts;
}

int __wrap_eventfd(unsigned initial, int flags)
{
	int fd = __real_eventfd(initial, flags);
	if (active && creating && fd >= 0)
		atomic_store(&active->fd, fd);
	return fd;
}

int __wrap_close(int fd)
{
	if (active && fd == atomic_load(&active->fd)) {
		CHECK(!atomic_load(&active->event_live));
		CHECK(!atomic_load(&active->timer_live));
		atomic_fetch_add(&active->closes, 1);
	}
	return __real_close(fd);
}

ssize_t __wrap_write(int fd, const void *data, size_t length)
{
	ssize_t ret = __real_write(fd, data, length);
	if (active && fd == atomic_load(&active->fd) && ret == (ssize_t)length)
		atomic_fetch_add(&active->notifications, 1);
	return ret;
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout)
{
	if (active && atomic_exchange(&active->fail_poll, false)) {
		atomic_store(&active->poll_failed, true);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, count, timeout);
}

int __wrap_pthread_join(pthread_t thread, void **result)
{
	if (active && atomic_exchange(&active->fail_parent_join, false)) {
		/* 这里只比较尚未成功 join 的线程 ID，不探测已失效的 pthread_t。
		 */
		CHECK(pthread_equal(thread, active->owner));
		return EBUSY;
	}
	if (active && atomic_exchange(&active->fail_worker_join, false))
		return EBUSY;
	return __real_pthread_join(thread, result);
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *lock)
{
	if (mutex_failure) {
		mutex_failure = false;
		return EBUSY;
	}
	return __real_pthread_mutex_destroy(lock);
}

int __wrap_tr_task_executor_destroy(struct tr_task_executor **executor)
{
	CHECK(!atomic_load(&active->event_live) &&
	      !atomic_load(&active->timer_live));
	if (atomic_exchange(&active->fail_final_mutex, false))
		mutex_failure = true;
	return __real_tr_task_executor_destroy(executor);
}

static void event_dispatch(int fd, uint32_t events, void *arg)
{
	struct fixture *f = arg;
	if (!atomic_load(&f->hold_delivery))
		f->event(fd, events, f->event_arg);
}

static uint64_t timer_dispatch(void *arg, uint64_t now)
{
	struct fixture *f = arg;
	return atomic_load(&f->hold_delivery) ? now + 10000000
	                                      : f->timer_cb(f->timer_arg, now);
}

int __wrap_tr_reactor_aux_event_register(struct tr_reactor *r, int fd,
                                         uint32_t events,
                                         tr_reactor_aux_event_cb cb, void *arg)
{
	int ret;
	if (!active || fd != atomic_load(&active->fd))
		return __real_tr_reactor_aux_event_register(r, fd, events, cb,
		                                            arg);
	active->event = cb;
	active->event_arg = arg;
	ret = __real_tr_reactor_aux_event_register(r, fd, events,
	                                           event_dispatch, active);
	if (ret == TR_OK)
		atomic_store(&active->event_live, true);
	return ret;
}

int __wrap_tr_reactor_aux_event_unregister(struct tr_reactor *r, int fd)
{
	int ret;
	if (active && fd == atomic_load(&active->fd) &&
	    atomic_exchange(&active->fail_aux, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_aux_event_unregister(r, fd);
	if (ret == TR_OK && active && fd == atomic_load(&active->fd))
		atomic_store(&active->event_live, false);
	return ret;
}

int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	int ret;
	if (!creating)
		return __real_tr_reactor_timer_register(r, cb, arg, out);
	active->timer_cb = cb;
	active->timer_arg = arg;
	ret = __real_tr_reactor_timer_register(r, timer_dispatch, active, out);
	if (ret == TR_OK) {
		active->timer = *out;
		atomic_store(&active->timer_live, true);
	}
	return ret;
}

int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle timer)
{
	bool watched = active && timer.reactor == active->timer.reactor &&
	               timer.slot == active->timer.slot &&
	               timer.generation == active->timer.generation;
	int ret;
	if (watched && atomic_exchange(&active->fail_timer, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_timer_unregister(timer);
	if (watched && ret == TR_OK)
		atomic_store(&active->timer_live, false);
	return ret;
}

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *c, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(c, out, in, end);
	struct fixture *f = active;
	if (f && atomic_exchange(&f->gate_worker, false)) {
		struct timespec until = deadline();
		CHECK(pthread_mutex_lock(&f->lock) == 0);
		f->worker_entered = true;
		CHECK(pthread_cond_broadcast(&f->cond) == 0);
		while (!f->worker_released)
			CHECK(pthread_cond_timedwait(&f->cond, &f->lock,
			                             &until) == 0);
		CHECK(pthread_mutex_unlock(&f->lock) == 0);
	}
	return ret;
}

static void source_release(void *arg)
{
	struct job *job = arg;
	atomic_fetch_add(&job->fixture->released[job->index], 1);
}

static int noop(void *arg)
{
	(void)arg;
	return TR_OK;
}

static void abandon(void *arg, struct tr_task_handle handle)
{
	struct job *job = arg;
	struct fixture *f = job->fixture;
	struct tr_message_service *same = f->service;
	unsigned index = job->index;
	CHECK(!tr_reactor_in_owner_context());
	CHECK(pthread_equal(pthread_self(), f->reaper));
	CHECK(handle.instance == job->handle.instance &&
	      handle.generation == job->handle.generation &&
	      handle.slot == job->handle.slot);
	CHECK(atomic_load(&f->released[index]) == 1);
	CHECK(atomic_load(&f->normal[index]) == 0);
	CHECK(tr_message_service_reap_stopped(&same, abandon) == TR_ERR_STATE);
	/* 回收回调不在父 ctl_lock 内，否则这里会死锁。 */
	CHECK(tr_reactor_call_or_stopped(f->reactor, noop, NULL) == TR_OK);
	free(job);
	CHECK(atomic_fetch_add(&f->abandoned[index], 1) == 0);
}

static void done(void *arg, struct tr_task_handle handle,
                 enum tr_message_transform_outcome outcome,
                 struct tr_message *message)
{
	struct job *job = arg;
	struct fixture *f = job->fixture;
	unsigned index = job->index;
	(void)handle;
	CHECK(tr_reactor_in_owner_context());
	CHECK(pthread_equal(pthread_self(), f->owner));
	CHECK(outcome == TR_MESSAGE_TRANSFORM_OK);
	CHECK(atomic_load(&f->released[index]) == 1);
	CHECK(tr_message_clone(message, &f->retained) == TR_OK);
	free(job);
	atomic_fetch_add(&f->normal[index], 1);
}

static int create(void *arg)
{
	struct fixture *f = arg;
	struct tr_message_service_config config = {{1, JOBS}, 1, 10000000};
	int ret;
	f->owner = pthread_self();
	creating = true;
	ret = tr_message_service_create_on_owner(f->reactor, &config,
	                                         &f->budget, &f->service);
	if (ret == TR_OK && f->start)
		ret = tr_message_service_start_on_owner(f->service);
	creating = false;
	return ret;
}

struct submit_request {
	struct fixture *fixture;
	unsigned begin;
	unsigned end;
};

static int submit(void *arg)
{
	struct submit_request *request = arg;
	struct fixture *f = request->fixture;
	struct tr_message_transform_config config = {
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {4 * MIB, 4 * MIB},
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3,
	};
	for (unsigned i = request->begin; i < request->end; ++i) {
		struct job *job = calloc(1, sizeof(*job));
		CHECK(job);
		job->fixture = f;
		job->index = i;
		CHECK(tr_message_storage_reserve(&job->storage, &f->budget,
		                                 MIB) == TR_OK);
		CHECK(tr_message_storage_publish(&job->storage, f->data, MIB,
		                                 source_release, job,
		                                 &job->input) == TR_OK);
		CHECK(tr_message_service_submit_on_owner(
		              f->service, &job->input, &config, done, job,
		              &job->handle) == TR_OK);
	}
	return TR_OK;
}

static int arm_fatal(void *arg)
{
	struct fixture *f = arg;
	atomic_store(&f->fail_poll, true);
	return TR_OK;
}

static int normal_close(void *arg)
{
	struct fixture *f = arg;
	return tr_message_service_close_on_owner(&f->service);
}

static void init(struct fixture *f, bool start)
{
	*f = (struct fixture){.start = start, .reaper = pthread_self()};
	atomic_init(&f->fd, -1);
	atomic_init(&f->gate_worker, false);
	atomic_init(&f->hold_delivery, false);
	atomic_init(&f->fail_poll, false);
	atomic_init(&f->poll_failed, false);
	atomic_init(&f->fail_parent_join, false);
	atomic_init(&f->fail_worker_join, false);
	atomic_init(&f->fail_aux, false);
	atomic_init(&f->fail_timer, false);
	atomic_init(&f->fail_final_mutex, false);
	atomic_init(&f->event_live, false);
	atomic_init(&f->timer_live, false);
	atomic_init(&f->notifications, 0);
	atomic_init(&f->closes, 0);
	for (unsigned i = 0; i < JOBS; ++i) {
		atomic_init(&f->normal[i], 0);
		atomic_init(&f->abandoned[i], 0);
		atomic_init(&f->released[i], 0);
	}
	tr_memory_budget_init(&f->budget, 128 * MIB);
	CHECK(pthread_mutex_init(&f->lock, NULL) == 0);
	CHECK(pthread_cond_init(&f->cond, NULL) == 0);
	f->data = malloc(MIB);
	CHECK(f->data);
	memset(f->data, 'r', MIB);
	active = f;
	CHECK(tr_reactor_create(NULL, NULL, NULL, NULL, &f->reactor) == TR_OK);
	CHECK(tr_reactor_start(f->reactor) == TR_OK);
	CHECK(tr_reactor_call(f->reactor, create, f) == TR_OK);
}

static unsigned abandoned_count(struct fixture *f)
{
	unsigned count = 0;
	for (unsigned i = 0; i < JOBS; ++i)
		count += atomic_load(&f->abandoned[i]);
	return count;
}

static int reap_until(struct fixture *f)
{
	uint64_t until = now_ms() + 10000;
	int ret;
	do {
		unsigned before = abandoned_count(f);
		ret = tr_message_service_reap_stopped(&f->service, abandon);
		CHECK(abandoned_count(f) - before <= 1);
		CHECK(now_ms() < until);
		if (ret == TR_AGAIN)
			sched_yield();
	} while (ret == TR_AGAIN);
	return ret;
}

static void destroy(struct fixture *f)
{
	CHECK(!f->service && !atomic_load(&f->event_live) &&
	      !atomic_load(&f->timer_live));
	CHECK(atomic_load(&f->closes) == 1);
	atomic_store(&f->fd, -1);
	/* 完成回收后才重启：陈旧辅助源/定时器不能访问已经释放的服务。 */
	CHECK(tr_reactor_start(f->reactor) == TR_OK);
	CHECK(tr_reactor_call(f->reactor, noop, NULL) == TR_OK);
	CHECK(tr_reactor_stop(f->reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->reactor) == TR_OK);
	if (atomic_load(&f->normal[0])) {
		CHECK(atomic_load(&f->budget.current_bytes) > 0);
		CHECK(tr_message_release(&f->retained) == TR_OK);
	}
	CHECK(atomic_load(&f->budget.current_bytes) == 0);
	free(f->data);
	CHECK(pthread_cond_destroy(&f->cond) == 0);
	CHECK(pthread_mutex_destroy(&f->lock) == 0);
	active = NULL;
}

static void *second_reaper(void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_message_service_reap_stopped(&f->service, abandon) ==
	      TR_ERR_STATE);
	return NULL;
}

static void test_running_work(bool fatal)
{
	struct fixture f;
	struct submit_request request;
	struct timespec until;
	uint64_t reserved;
	pthread_t contender;
	int fd;
	init(&f, true);
	/* 运行中错误调用不能停止新准入或迁移回调。 */
	CHECK(tr_message_service_reap_stopped(&f.service, abandon) ==
	      TR_ERR_STATE);
	atomic_store(&f.gate_worker, true);
	request = (struct submit_request){&f, 0, JOBS};
	CHECK(tr_reactor_call(f.reactor, submit, &request) == TR_OK);
	until = deadline();
	CHECK(pthread_mutex_lock(&f.lock) == 0);
	while (!f.worker_entered)
		CHECK(pthread_cond_timedwait(&f.cond, &f.lock, &until) == 0);
	CHECK(pthread_mutex_unlock(&f.lock) == 0);
	reserved = atomic_load(&f.budget.current_bytes);
	fd = atomic_load(&f.fd);
	if (fatal) {
		uint64_t end = now_ms() + 10000;
		CHECK(tr_reactor_call(f.reactor, arm_fatal, &f) == TR_OK);
		while (!atomic_load(&f.poll_failed)) {
			CHECK(now_ms() < end);
			sched_yield();
		}
	}
	atomic_store(&f.fail_parent_join, true);
	CHECK(tr_reactor_stop(f.reactor) == TR_ERR_SYS);
	/* 线程可能已退出，但 join 未成功，不能只看 accepting=false 就接管。 */
	CHECK(tr_message_service_reap_stopped(&f.service, abandon) ==
	      TR_ERR_CLOSED);
	CHECK(f.service && atomic_load(&f.event_live) &&
	      atomic_load(&f.timer_live));
	CHECK(atomic_load(&f.budget.current_bytes) == reserved);
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	CHECK(tr_message_service_reap_stopped(&f.service, abandon) == TR_AGAIN);
	CHECK(!atomic_load(&f.event_live) && !atomic_load(&f.timer_live));
	CHECK(fcntl(fd, F_GETFD) >= 0 && atomic_load(&f.closes) == 0);
	CHECK(atomic_load(&f.budget.current_bytes) == reserved);
	CHECK(pthread_create(&contender, NULL, second_reaper, &f) == 0);
	CHECK(pthread_join(contender, NULL) == 0);
	CHECK(pthread_mutex_lock(&f.lock) == 0);
	f.worker_released = true;
	CHECK(pthread_cond_broadcast(&f.cond) == 0);
	CHECK(pthread_mutex_unlock(&f.lock) == 0);
	atomic_store(&f.fail_worker_join, true);
	CHECK(reap_until(&f) == TR_ERR_SYS);
	CHECK(f.service && fcntl(fd, F_GETFD) >= 0);
	CHECK(reap_until(&f) == TR_OK && !f.service);
	CHECK(abandoned_count(&f) == JOBS);
	/* 接管清通知后，所有迟到工作均不能再写旧 eventfd。 */
	CHECK(atomic_load(&f.notifications) == 0);
	destroy(&f);
}

static void test_completed_and_retained(void)
{
	struct fixture f;
	struct submit_request request;
	uint64_t until;
	init(&f, true);
	request = (struct submit_request){&f, 0, 1};
	CHECK(tr_reactor_call(f.reactor, submit, &request) == TR_OK);
	until = now_ms() + 10000;
	while (!atomic_load(&f.normal[0])) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(tr_reactor_call(f.reactor, noop, NULL) == TR_OK);
	atomic_store(&f.hold_delivery, true);
	request.begin = 1;
	request.end = JOBS;
	CHECK(tr_reactor_call(f.reactor, submit, &request) == TR_OK);
	while (atomic_load(&f.notifications) < JOBS) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(tr_reactor_call(f.reactor, arm_fatal, &f) == TR_OK);
	while (!atomic_load(&f.poll_failed)) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	CHECK(reap_until(&f) == TR_OK);
	CHECK(atomic_load(&f.normal[0]) == 1 && !atomic_load(&f.abandoned[0]));
	for (unsigned i = 1; i < JOBS; ++i)
		CHECK(!atomic_load(&f.normal[i]) &&
		      atomic_load(&f.abandoned[i]) == 1);
	destroy(&f);
}

static void test_detach_and_partial_destroy(void)
{
	struct fixture f;
	uint64_t reserved;
	int fd;
	init(&f, true);
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	reserved = atomic_load(&f.budget.current_bytes);
	fd = atomic_load(&f.fd);
	atomic_store(&f.fail_aux, true);
	CHECK(tr_message_service_reap_stopped(&f.service, abandon) ==
	      TR_ERR_SYS);
	CHECK(atomic_load(&f.event_live) && atomic_load(&f.timer_live));
	CHECK(atomic_load(&f.budget.current_bytes) == reserved &&
	      fcntl(fd, F_GETFD) >= 0);
	atomic_store(&f.fail_timer, true);
	CHECK(tr_message_service_reap_stopped(&f.service, abandon) ==
	      TR_ERR_SYS);
	CHECK(!atomic_load(&f.event_live) && atomic_load(&f.timer_live));
	atomic_store(&f.fail_final_mutex, true);
	CHECK(reap_until(&f) == TR_ERR_SYS);
	CHECK(!atomic_load(&f.event_live) && !atomic_load(&f.timer_live));
	CHECK(f.service && fcntl(fd, F_GETFD) >= 0 &&
	      atomic_load(&f.closes) == 0);
	CHECK(reap_until(&f) == TR_OK);
	destroy(&f);

	/* 正常关闭已进入最终销毁后父 owner 才停止：同样接管剩余销毁责任。 */
	init(&f, true);
	atomic_store(&f.fail_final_mutex, true);
	{
		uint64_t until = now_ms() + 10000;
		int ret;
		do {
			ret = tr_reactor_call(f.reactor, normal_close, &f);
			CHECK(now_ms() < until);
		} while (ret == TR_AGAIN);
		CHECK(ret == TR_ERR_SYS && f.service);
	}
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	CHECK(reap_until(&f) == TR_OK);
	destroy(&f);
}

static void test_unstarted(void)
{
	struct fixture f;
	init(&f, false);
	CHECK(tr_reactor_stop(f.reactor) == TR_OK);
	CHECK(reap_until(&f) == TR_OK);
	destroy(&f);
}

int main(void)
{
	test_unstarted();
	test_running_work(false);
	test_running_work(true);
	test_completed_and_retained();
	test_detach_and_partial_destroy();
	puts("message_service_recovery: ok");
	return 0;
}
