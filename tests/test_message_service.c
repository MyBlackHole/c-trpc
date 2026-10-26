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

struct latch {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered;
	bool open;
};

static struct timespec deadline(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_REALTIME, &ts) == 0);
	ts.tv_sec += 15;
	return ts;
}

static void latch_init(struct latch *l)
{
	*l = (struct latch){0};
	CHECK(pthread_mutex_init(&l->lock, NULL) == 0);
	CHECK(pthread_cond_init(&l->cond, NULL) == 0);
}

static void latch_wait(struct latch *l, bool worker)
{
	struct timespec until = deadline();
	CHECK(pthread_mutex_lock(&l->lock) == 0);
	if (worker) {
		l->entered = true;
		CHECK(pthread_cond_broadcast(&l->cond) == 0);
	}
	while (worker ? !l->open : !l->entered)
		CHECK(pthread_cond_timedwait(&l->cond, &l->lock, &until) == 0);
	CHECK(pthread_mutex_unlock(&l->lock) == 0);
}

static void latch_open(struct latch *l)
{
	CHECK(pthread_mutex_lock(&l->lock) == 0);
	l->open = true;
	CHECK(pthread_cond_broadcast(&l->cond) == 0);
	CHECK(pthread_mutex_unlock(&l->lock) == 0);
}

static void latch_destroy(struct latch *l)
{
	CHECK(pthread_cond_destroy(&l->cond) == 0);
	CHECK(pthread_mutex_destroy(&l->lock) == 0);
}

/* 全部包装仅链接到本测试；只影响当前消息服务，不改变 Reactor 自身的通知 fd。 */
static _Thread_local bool tracking;
static _Thread_local unsigned allocation_calls, thread_calls;
static _Atomic unsigned fail_allocation_at, fail_thread_at;
static _Atomic bool fail_submit, fail_executor_destroy;
static _Atomic int watched_fd = -1, fail_write_errno, fail_read_errno;
static _Atomic bool fail_eventfd, fail_register, fail_timer_register;
static _Atomic bool fail_arm, fail_unregister, fail_timer_unregister;
static _Atomic bool event_live, timer_live, suppress_watchdog;
static _Atomic unsigned event_calls, timer_calls, algorithm_calls;
static _Atomic unsigned write_calls, service_closes;
static _Atomic(struct latch *) algorithm_gate;
static _Atomic bool gate_once;
static struct tr_reactor_timer_handle watched_timer;
static tr_reactor_aux_event_cb actual_event;
static tr_reactor_timer_cb actual_timer;
static void *actual_event_arg, *actual_timer_arg;

void *__real_calloc(size_t, size_t);
int __real_pthread_create(pthread_t *, const pthread_attr_t *,
                          void *(*)(void *), void *);
int __real_tr_task_executor_submit(struct tr_task_executor *,
                                   const struct tr_task_spec *,
                                   struct tr_task_handle *);
int __real_tr_task_executor_destroy(struct tr_task_executor **);

void *__wrap_calloc(size_t count, size_t size)
{
	if (tracking && ++allocation_calls == atomic_load(&fail_allocation_at))
		return NULL;
	return __real_calloc(count, size);
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg)
{
	if (tracking && ++thread_calls == atomic_load(&fail_thread_at))
		return EAGAIN;
	return __real_pthread_create(thread, attr, fn, arg);
}

int __wrap_tr_task_executor_submit(struct tr_task_executor *executor,
                                   const struct tr_task_spec *spec,
                                   struct tr_task_handle *out)
{
	if (atomic_exchange(&fail_submit, false))
		return TR_AGAIN;
	return __real_tr_task_executor_submit(executor, spec, out);
}

int __wrap_tr_task_executor_destroy(struct tr_task_executor **executor)
{
	CHECK(!atomic_load(&event_live) && !atomic_load(&timer_live));
	if (atomic_exchange(&fail_executor_destroy, false))
		return TR_ERR_SYS;
	return __real_tr_task_executor_destroy(executor);
}

int __real_eventfd(unsigned initial, int flags);
ssize_t __real_write(int fd, const void *data, size_t bytes);
ssize_t __real_read(int fd, void *data, size_t bytes);
int __real_close(int fd);
int __real_tr_reactor_aux_event_register(struct tr_reactor *, int, uint32_t,
                                         tr_reactor_aux_event_cb, void *);
int __real_tr_reactor_aux_event_unregister(struct tr_reactor *, int);
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle, uint64_t);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
size_t __real_ZSTD_compressStream2(ZSTD_CCtx *, ZSTD_outBuffer *,
                                   ZSTD_inBuffer *, ZSTD_EndDirective);
size_t __real_ZSTD_decompressStream(ZSTD_DCtx *, ZSTD_outBuffer *,
                                    ZSTD_inBuffer *);

int __wrap_eventfd(unsigned initial, int flags)
{
	int fd;
	if (tracking && atomic_exchange(&fail_eventfd, false)) {
		errno = EMFILE;
		return -1;
	}
	fd = __real_eventfd(initial, flags);
	if (tracking && fd >= 0) {
		CHECK((flags & (EFD_NONBLOCK | EFD_CLOEXEC)) ==
		      (EFD_NONBLOCK | EFD_CLOEXEC));
		atomic_store(&watched_fd, fd);
	}
	return fd;
}

ssize_t __wrap_write(int fd, const void *data, size_t bytes)
{
	if (fd == atomic_load(&watched_fd)) {
		int error = atomic_load(&fail_write_errno);
		atomic_fetch_add(&write_calls, 1);
		if (error) {
			errno = error;
			return -1;
		}
	}
	return __real_write(fd, data, bytes);
}

ssize_t __wrap_read(int fd, void *data, size_t bytes)
{
	if (fd == atomic_load(&watched_fd)) {
		int error = atomic_exchange(&fail_read_errno, 0);
		if (error) {
			errno = error;
			return -1;
		}
	}
	return __real_read(fd, data, bytes);
}

int __wrap_close(int fd)
{
	if (fd == atomic_load(&watched_fd)) {
		CHECK(!atomic_load(&event_live) && !atomic_load(&timer_live));
		atomic_fetch_add(&service_closes, 1);
	}
	return __real_close(fd);
}

static void observe_event(int fd, uint32_t events, void *arg)
{
	(void)arg;
	atomic_fetch_add(&event_calls, 1);
	CHECK(atomic_load(&event_live));
	actual_event(fd, events, actual_event_arg);
}

static uint64_t observe_timer(void *arg, uint64_t now)
{
	(void)arg;
	atomic_fetch_add(&timer_calls, 1);
	if (atomic_load(&suppress_watchdog))
		return now + 1000000000;
	return actual_timer(actual_timer_arg, now);
}

int __wrap_tr_reactor_aux_event_register(struct tr_reactor *r, int fd,
                                         uint32_t events,
                                         tr_reactor_aux_event_cb cb, void *arg)
{
	int ret;
	if (!tracking)
		return __real_tr_reactor_aux_event_register(r, fd, events, cb,
		                                            arg);
	if (atomic_exchange(&fail_register, false))
		return TR_ERR_SYS;
	actual_event = cb;
	actual_event_arg = arg;
	ret = __real_tr_reactor_aux_event_register(r, fd, events, observe_event,
	                                           NULL);
	if (ret == TR_OK)
		atomic_store(&event_live, true);
	return ret;
}

int __wrap_tr_reactor_aux_event_unregister(struct tr_reactor *r, int fd)
{
	int ret;
	if (fd == atomic_load(&watched_fd) &&
	    atomic_exchange(&fail_unregister, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_aux_event_unregister(r, fd);
	if (ret == TR_OK && fd == atomic_load(&watched_fd))
		atomic_store(&event_live, false);
	return ret;
}

int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	int ret;
	if (!tracking)
		return __real_tr_reactor_timer_register(r, cb, arg, out);
	if (atomic_exchange(&fail_timer_register, false))
		return TR_ERR_SYS;
	actual_timer = cb;
	actual_timer_arg = arg;
	ret = __real_tr_reactor_timer_register(r, observe_timer, NULL, out);
	if (ret == TR_OK) {
		watched_timer = *out;
		atomic_store(&timer_live, true);
	}
	return ret;
}

static bool our_timer(struct tr_reactor_timer_handle h)
{
	return h.reactor == watched_timer.reactor &&
	       h.slot == watched_timer.slot &&
	       h.generation == watched_timer.generation;
}

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle h, uint64_t due)
{
	if (our_timer(h) && atomic_exchange(&fail_arm, false))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_arm(h, due);
}

int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle h)
{
	int ret;
	if (our_timer(h) && atomic_exchange(&fail_timer_unregister, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_timer_unregister(h);
	if (ret == TR_OK && our_timer(h))
		atomic_store(&timer_live, false);
	return ret;
}

static void algorithm_checkpoint(void)
{
	struct latch *gate = atomic_load(&algorithm_gate);
	atomic_fetch_add(&algorithm_calls, 1);
	if (gate && atomic_exchange(&gate_once, false))
		latch_wait(gate, true);
}

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *c, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(c, out, in, end);
	algorithm_checkpoint();
	return ret;
}

size_t __wrap_ZSTD_decompressStream(ZSTD_DCtx *c, ZSTD_outBuffer *out,
                                    ZSTD_inBuffer *in)
{
	size_t ret = __real_ZSTD_decompressStream(c, out, in);
	algorithm_checkpoint();
	return ret;
}

struct fixture;
struct sample {
	struct fixture *fixture;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_message retained;
	struct tr_task_handle handle;
	_Atomic unsigned releases;
	unsigned completions;
	enum tr_message_transform_outcome outcome;
};

struct fixture {
	struct tr_reactor *reactor;
	struct tr_message_service *service;
	struct tr_memory_budget budget;
	struct tr_message_service_config config;
	pthread_t owner;
	pthread_mutex_t lock;
	pthread_cond_t condition;
	size_t completions;
	struct tr_message_service_stats stats;
};

static int create_service(void *arg)
{
	struct fixture *f = arg;
	int ret;
	f->owner = pthread_self();
	allocation_calls = thread_calls = 0;
	tracking = true;
	ret = tr_message_service_create_on_owner(f->reactor, &f->config,
	                                         &f->budget, &f->service);
	if (ret == TR_OK)
		ret = tr_message_service_start_on_owner(f->service);
	tracking = false;
	return ret;
}

static int close_service(void *arg)
{
	struct fixture *f = arg;
	return tr_message_service_close_on_owner(&f->service);
}

static int get_stats(void *arg)
{
	struct fixture *f = arg;
	return tr_message_service_get_stats_on_owner(f->service, &f->stats);
}

static void init_fixture(struct fixture *f)
{
	*f = (struct fixture){.config = {{1, 4}, 1, 10000000}};
	atomic_store(&watched_fd, -1);
	atomic_store(&event_calls, 0);
	atomic_store(&timer_calls, 0);
	atomic_store(&algorithm_calls, 0);
	atomic_store(&write_calls, 0);
	atomic_store(&service_closes, 0);
	atomic_store(&suppress_watchdog, false);
	watched_timer = (struct tr_reactor_timer_handle){0};
	tr_memory_budget_init(&f->budget, 128 * MIB);
	CHECK(pthread_mutex_init(&f->lock, NULL) == 0);
	CHECK(pthread_cond_init(&f->condition, NULL) == 0);
	CHECK(tr_reactor_create(NULL, NULL, NULL, NULL, &f->reactor) == TR_OK);
	CHECK(tr_reactor_start(f->reactor) == TR_OK);
}

static uint64_t now_seconds(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec;
}

static void close_until(struct fixture *f)
{
	uint64_t until = now_seconds() + 15;
	int ret;
	while ((ret = tr_reactor_call(f->reactor, close_service, f)) ==
	       TR_AGAIN) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(ret == TR_OK && !f->service);
}

static void destroy_fixture(struct fixture *f)
{
	if (f->service)
		close_until(f);
	atomic_store(&watched_fd, -1);
	CHECK(tr_reactor_stop(f->reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->reactor) == TR_OK);
	CHECK(atomic_load(&f->budget.current_bytes) == 0);
	CHECK(pthread_cond_destroy(&f->condition) == 0);
	CHECK(pthread_mutex_destroy(&f->lock) == 0);
}

static void sample_release(void *arg)
{
	struct sample *s = arg;
	atomic_fetch_add(&s->releases, 1);
}

static void init_sample(struct sample *s, struct fixture *f,
                        const uint8_t *data, size_t bytes)
{
	*s = (struct sample){.fixture = f};
	atomic_init(&s->releases, 0);
	CHECK(tr_message_storage_reserve(&s->storage, &f->budget, bytes) ==
	      TR_OK);
	CHECK(tr_message_storage_publish(&s->storage, data, bytes,
	                                 sample_release, s,
	                                 &s->input) == TR_OK);
}

static void done(void *arg, struct tr_task_handle handle,
                 struct tr_message_service_result result,
                 struct tr_message *message)
{
	enum tr_message_transform_outcome outcome = result.outcome;
	struct sample *s = arg;
	struct fixture *f = s->fixture;
	struct tr_message_service *same = f->service;
	struct tr_message_limits limits = {SIZE_MAX, SIZE_MAX};
	struct tr_message_view view;

	CHECK(result.delivery == TR_MESSAGE_SERVICE_ON_OWNER);
	CHECK(pthread_equal(pthread_self(), f->owner));
	CHECK(handle.instance == s->handle.instance &&
	      handle.generation == s->handle.generation &&
	      handle.slot == s->handle.slot);
	CHECK(atomic_load(&s->releases) == 1);
	CHECK(tr_message_service_close_on_owner(&same) == TR_ERR_STATE);
	CHECK(same == f->service);
	if (outcome == TR_MESSAGE_TRANSFORM_OK)
		CHECK(tr_message_none_move(message, &limits, &s->retained) ==
		      TR_OK);
	else
		CHECK(tr_message_view(message, &view) == TR_ERR_STATE);
	CHECK(pthread_mutex_lock(&f->lock) == 0);
	s->outcome = outcome;
	++s->completions;
	++f->completions;
	CHECK(pthread_cond_broadcast(&f->condition) == 0);
	CHECK(pthread_mutex_unlock(&f->lock) == 0);
}

struct request {
	struct sample *sample;
	struct tr_message_transform_config config;
};

static struct tr_message_transform_config config(bool decode, size_t bytes)
{
	return (struct tr_message_transform_config){
	        .kind = decode ? TR_MESSAGE_ZSTD_DECODE
	                       : TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {4 * MIB, 4 * MIB},
	        .decoded_size = bytes,
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3};
}

static int submit_sample(void *arg)
{
	struct request *r = arg;
	struct sample *s = r->sample;
	return tr_message_service_submit_on_owner(s->fixture->service,
	                                          &s->input, &r->config, done,
	                                          s, &s->handle);
}

static void wait_done(struct fixture *f, size_t count)
{
	struct timespec until = deadline();
	CHECK(pthread_mutex_lock(&f->lock) == 0);
	while (f->completions < count)
		CHECK(pthread_cond_timedwait(&f->condition, &f->lock, &until) ==
		      0);
	CHECK(pthread_mutex_unlock(&f->lock) == 0);
}

static void test_run(bool cancel, bool decode)
{
	struct fixture f;
	struct sample s, second;
	struct request r;
	struct latch gate;
	uint8_t *data = malloc(MIB), *encoded = NULL;
	size_t size = MIB;
	struct tr_message_view view;
	struct tr_task_handle old;

	CHECK(data);
	memset(data, 'z', MIB);
	init_fixture(&f);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	CHECK(tr_message_service_start_on_owner(f.service) == TR_ERR_STATE);
	if (decode) {
		encoded = malloc(ZSTD_compressBound(MIB));
		CHECK(encoded);
		size = ZSTD_compress(encoded, ZSTD_compressBound(MIB), data,
		                     MIB, 3);
		CHECK(!ZSTD_isError(size));
	}
	init_sample(&s, &f, decode ? encoded : data, size);
	r = (struct request){&s, config(decode, MIB)};
	if (cancel) {
		latch_init(&gate);
		atomic_store(&algorithm_gate, &gate);
		atomic_store(&gate_once, true);
	}
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_OK);
	CHECK(tr_message_view(&s.input, &view) == TR_ERR_STATE);
	if (cancel) {
		latch_wait(&gate, false);
		CHECK(atomic_load(&algorithm_calls) == 1);
		CHECK(tr_message_service_cancel(f.service, s.handle) == TR_OK);
		CHECK(atomic_load(&s.releases) == 0);
		latch_open(&gate);
	}
	wait_done(&f, 1);
	CHECK(s.completions == 1);
	if (cancel) {
		CHECK(s.outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
		CHECK(atomic_load(&algorithm_calls) == 1);
		atomic_store(&algorithm_gate, NULL);
		latch_destroy(&gate);
	} else {
		CHECK(s.outcome == TR_MESSAGE_TRANSFORM_OK);
		CHECK(tr_message_view(&s.retained, &view) == TR_OK);
		if (decode)
			CHECK(view.len == MIB &&
			      memcmp(view.data, data, MIB) == 0);
		else {
			uint8_t *roundtrip = malloc(MIB);
			CHECK(roundtrip);
			CHECK(ZSTD_decompress(roundtrip, MIB, view.data,
			                      view.len) == MIB);
			CHECK(memcmp(roundtrip, data, MIB) == 0);
			free(roundtrip);
		}
	}
	old = s.handle;
	/* 通过 owner 命令跨过最后回调返回屏障，再检验旧能力与实例。 */
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	CHECK(f.stats.held_jobs == 0 && !f.stats.notification_error);
	CHECK(tr_message_service_cancel(f.service, old) == TR_ERR_STALE);
	close_until(&f);
	CHECK(atomic_load(&service_closes) == 1);
	if (!cancel)
		CHECK(tr_message_release(&s.retained) == TR_OK);
	CHECK(atomic_load(&f.budget.current_bytes) == 0);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	init_sample(&second, &f, data, 1);
	r = (struct request){&second, config(false, 0)};
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_OK);
	CHECK(tr_message_service_cancel(f.service, old) == TR_ERR_STALE);
	wait_done(&f, 2);
	CHECK(tr_message_release(&second.retained) == TR_OK);
	destroy_fixture(&f);
	free(encoded);
	free(data);
}

static void test_start_failure(unsigned stage)
{
	struct fixture f;
	int fd;
	uint64_t held;

	init_fixture(&f);
	if (stage == 0)
		atomic_store(&fail_eventfd, true);
	if (stage == 1)
		atomic_store(&fail_register, true);
	if (stage == 2)
		atomic_store(&fail_timer_register, true);
	if (stage == 3)
		atomic_store(&fail_arm, true);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_ERR_SYS);
	if (stage == 0) {
		CHECK(!f.service && atomic_load(&f.budget.current_bytes) == 0);
	} else {
		CHECK(f.service);
		fd = atomic_load(&watched_fd);
		held = atomic_load(&f.budget.current_bytes);
		CHECK(held > 0 && fcntl(fd, F_GETFD) >= 0);
		if (stage >= 2) {
			atomic_store(&fail_unregister, true);
			CHECK(tr_reactor_call(f.reactor, close_service, &f) ==
			      TR_ERR_SYS);
			CHECK(f.service && atomic_load(&event_live));
			CHECK(fcntl(fd, F_GETFD) >= 0);
			CHECK(atomic_load(&f.budget.current_bytes) == held);
		}
		if (stage == 3) {
			atomic_store(&fail_timer_unregister, true);
			CHECK(tr_reactor_call(f.reactor, close_service, &f) ==
			      TR_ERR_SYS);
			CHECK(!atomic_load(&event_live) &&
			      atomic_load(&timer_live));
			CHECK(f.service && fcntl(fd, F_GETFD) >= 0);
		}
		close_until(&f);
		CHECK(atomic_load(&service_closes) == 1);
		CHECK(fcntl(fd, F_GETFD) < 0 && errno == EBADF);
	}
	destroy_fixture(&f);
}

static void test_notification_error(int error)
{
	struct fixture f;
	struct sample s, rejected;
	uint8_t data[32] = {0};
	struct request r;
	struct tr_message_view view;

	init_fixture(&f);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	init_sample(&s, &f, data, sizeof(data));
	r = (struct request){&s, config(false, 0)};
	atomic_store(&fail_write_errno, error);
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_OK);
	wait_done(&f, 1);
	CHECK(atomic_load(&timer_calls) > 0);
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	CHECK(f.stats.notification_error == error && !f.stats.accepting);
	CHECK(atomic_load(&write_calls) == (error == EINTR ? 4U : 1U));
	init_sample(&rejected, &f, data, sizeof(data));
	r.sample = &rejected;
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_ERR_CLOSED);
	CHECK(tr_message_view(&rejected.input, &view) == TR_OK);
	CHECK(rejected.completions == 0 &&
	      atomic_load(&rejected.releases) == 0);
	CHECK(tr_message_release(&rejected.input) == TR_OK);
	if (tr_message_view(&s.retained, &view) == TR_OK)
		CHECK(tr_message_release(&s.retained) == TR_OK);
	atomic_store(&fail_write_errno, 0);
	destroy_fixture(&f);
}

struct batch {
	struct fixture *f;
	struct sample samples[5];
	struct latch held_owner;
};

static int submit_batch(void *arg)
{
	struct batch *b = arg;
	struct request r;
	uint64_t saturated = UINT64_MAX - 1;
	/* 处于同一 owner turn，读事件尚不能清除饱和计数。 */
	CHECK(__real_write(atomic_load(&watched_fd), &saturated,
	                   sizeof(saturated)) == (ssize_t)sizeof(saturated));
	for (size_t i = 0; i < 4; ++i) {
		r = (struct request){&b->samples[i], config(false, 0)};
		CHECK(submit_sample(&r) == TR_OK);
	}
	r = (struct request){&b->samples[4], config(false, 0)};
	CHECK(submit_sample(&r) == TR_AGAIN);
	latch_wait(&b->held_owner, true);
	return TR_OK;
}

static void *batch_thread(void *arg)
{
	struct batch *b = arg;
	CHECK(tr_reactor_call(b->f->reactor, submit_batch, b) == TR_OK);
	return NULL;
}

static void test_dispatch_rearm(void)
{
	struct fixture f;
	struct batch b = {0};
	uint8_t data[64] = {0};
	pthread_t thread;
	uint64_t until;
	struct tr_message_view view;

	init_fixture(&f);
	atomic_store(&suppress_watchdog, true);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	b.f = &f;
	latch_init(&b.held_owner);
	for (size_t i = 0; i < 5; ++i)
		init_sample(&b.samples[i], &f, data, sizeof(data));
	CHECK(pthread_create(&thread, NULL, batch_thread, &b) == 0);
	latch_wait(&b.held_owner, false);
	until = now_seconds() + 15;
	while (atomic_load(&write_calls) < 4) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(f.completions == 0);
	latch_open(&b.held_owner);
	CHECK(pthread_join(thread, NULL) == 0);
	wait_done(&f, 4);
	CHECK(atomic_load(&event_calls) >= 4);
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	CHECK(!f.stats.notification_error && f.stats.held_jobs == 0);
	for (size_t i = 0; i < 4; ++i) {
		CHECK(b.samples[i].completions == 1);
		CHECK(tr_message_release(&b.samples[i].retained) == TR_OK);
	}
	CHECK(tr_message_view(&b.samples[4].input, &view) == TR_OK);
	CHECK(atomic_load(&b.samples[4].releases) == 0);
	CHECK(tr_message_release(&b.samples[4].input) == TR_OK);
	latch_destroy(&b.held_owner);
	destroy_fixture(&f);
}

static void test_allocation_and_thread_failure(void)
{
	bool reached_success = false;
	for (unsigned index = 1; index <= 16 && !reached_success; ++index) {
		struct fixture f;
		int ret;
		init_fixture(&f);
		atomic_store(&fail_allocation_at, index);
		ret = tr_reactor_call(f.reactor, create_service, &f);
		atomic_store(&fail_allocation_at, 0);
		if (ret == TR_OK) {
			reached_success = true;
			close_until(&f);
		} else {
			CHECK(ret == TR_ERR_NOMEM);
			if (f.service)
				close_until(&f);
			else
				CHECK(atomic_load(&f.budget.current_bytes) ==
				      0);
		}
		CHECK(atomic_load(&f.budget.current_bytes) == 0);
		destroy_fixture(&f);
	}
	CHECK(reached_success);
	for (unsigned index = 1; index <= 2; ++index) {
		struct fixture f;
		init_fixture(&f);
		f.config.tasks.workers = 2;
		atomic_store(&fail_thread_at, index);
		CHECK(tr_reactor_call(f.reactor, create_service, &f) ==
		      TR_ERR_SYS);
		atomic_store(&fail_thread_at, 0);
		CHECK(f.service && atomic_load(&event_live) &&
		      atomic_load(&timer_live));
		close_until(&f);
		CHECK(atomic_load(&service_closes) == 1);
		destroy_fixture(&f);
	}
}

static void test_submit_and_final_destroy_failure(void)
{
	struct fixture f;
	struct sample s;
	struct request r;
	struct tr_message_view before, after;
	uint8_t data[32] = {0};
	uint64_t reserved;
	int fd;

	init_fixture(&f);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	init_sample(&s, &f, data, sizeof(data));
	r = (struct request){&s, config(false, 0)};
	CHECK(tr_message_view(&s.input, &before) == TR_OK);
	reserved = atomic_load(&f.budget.current_bytes);
	atomic_store(&fail_submit, true);
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_AGAIN);
	CHECK(atomic_load(&f.budget.current_bytes) == reserved);
	CHECK(tr_message_view(&s.input, &after) == TR_OK);
	CHECK(before.data == after.data && before.len == after.len);
	CHECK(s.handle.generation == 0 && atomic_load(&s.releases) == 0);
	CHECK(s.completions == 0);
	CHECK(tr_reactor_call(f.reactor, submit_sample, &r) == TR_OK);
	wait_done(&f, 1);
	CHECK(tr_message_release(&s.retained) == TR_OK);
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	CHECK(f.stats.held_jobs == 0);
	atomic_store(&fail_executor_destroy, true);
	fd = atomic_load(&watched_fd);
	/* 不假定工作线程已经退出，第一次关闭允许返回 AGAIN。 */
	{
		uint64_t until = now_seconds() + 15;
		int ret;
		while ((ret = tr_reactor_call(f.reactor, close_service, &f)) ==
		       TR_AGAIN) {
			CHECK(now_seconds() <= until);
			sched_yield();
		}
		CHECK(ret == TR_ERR_SYS);
	}
	CHECK(f.service && !atomic_load(&event_live) &&
	      !atomic_load(&timer_live));
	CHECK(fcntl(fd, F_GETFD) >= 0 && atomic_load(&service_closes) == 0);
	close_until(&f);
	CHECK(atomic_load(&service_closes) == 1);
	destroy_fixture(&f);
}

/* 不同 Reactor 的 owner 不得互相操作生命周期；拒绝不能改变预算。 */
struct foreign_owner_request {
	struct fixture *fixture;
	struct tr_reactor *other;
};

static int foreign_owner_create(void *arg)
{
	struct foreign_owner_request *request = arg;
	struct tr_message_service *unexpected = NULL;
	int ret = tr_message_service_create_on_owner(
	        request->other, &request->fixture->config,
	        &request->fixture->budget, &unexpected);

	CHECK(unexpected == NULL);
	return ret;
}

static void test_reactor_identity(void)
{
	struct fixture f;
	struct tr_reactor *other = NULL;
	struct foreign_owner_request request;
	uint64_t before;

	init_fixture(&f);
	CHECK(tr_reactor_create(NULL, NULL, NULL, NULL, &other) == TR_OK);
	CHECK(tr_reactor_start(other) == TR_OK);
	CHECK(tr_reactor_call(f.reactor, create_service, &f) == TR_OK);
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	before = atomic_load(&f.budget.current_bytes);
	request = (struct foreign_owner_request){&f, other};
	CHECK(tr_reactor_call(f.reactor, foreign_owner_create, &request) ==
	      TR_ERR_STATE);
	CHECK(tr_reactor_call(other, get_stats, &f) == TR_ERR_STATE);
	CHECK(tr_reactor_call(other, close_service, &f) == TR_ERR_STATE);
	CHECK(tr_reactor_call(f.reactor, get_stats, &f) == TR_OK);
	CHECK(f.stats.accepting && f.stats.held_jobs == 0);
	CHECK(atomic_load(&f.budget.current_bytes) == before);
	CHECK(tr_reactor_stop(other) == TR_OK);
	CHECK(tr_reactor_destroy(other) == TR_OK);
	destroy_fixture(&f);
	puts("message_service: reactor identity passed");
}

int main(void)
{
	test_reactor_identity();
	test_allocation_and_thread_failure();
	test_submit_and_final_destroy_failure();
	for (unsigned stage = 0; stage < 4; ++stage)
		test_start_failure(stage);
	for (unsigned decode = 0; decode < 2; ++decode) {
		test_run(false, decode != 0);
		test_run(true, decode != 0);
	}
	test_notification_error(EIO);
	test_notification_error(EINTR);
	test_dispatch_rearm();
	puts("message_service: ok");
	return 0;
}
