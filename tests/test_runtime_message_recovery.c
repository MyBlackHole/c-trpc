#define _POSIX_C_SOURCE 200809L
#include "../src/runtime/runtime_internal.h"
#include "../src/message/storage.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/async_operation_internal.h"
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

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered;
	bool open;
};

static void gate_init(struct gate *g)
{
	*g = (struct gate){0};
	CHECK(pthread_mutex_init(&g->lock, NULL) == 0);
	CHECK(pthread_cond_init(&g->cond, NULL) == 0);
}

static void gate_wait(struct gate *g, bool worker)
{
	struct timespec end;
	CHECK(clock_gettime(CLOCK_REALTIME, &end) == 0);
	end.tv_sec += 15;
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	if (worker) {
		g->entered = true;
		CHECK(pthread_cond_broadcast(&g->cond) == 0);
	}
	while (worker ? !g->open : !g->entered)
		CHECK(pthread_cond_timedwait(&g->cond, &g->lock, &end) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

static void gate_open(struct gate *g)
{
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	g->open = true;
	CHECK(pthread_cond_broadcast(&g->cond) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

static void gate_destroy(struct gate *g)
{
	CHECK(pthread_cond_destroy(&g->cond) == 0);
	CHECK(pthread_mutex_destroy(&g->lock) == 0);
}

static uint64_t now_seconds(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec;
}

static _Atomic(struct tr_reactor *) target;
static _Atomic(struct gate *) algorithm_gate;
static _Atomic bool trap_algorithm, fail_poll;
static _Atomic unsigned poll_failures, callback_count, algorithm_calls;
static _Atomic bool fail_event_del, fail_timer_del, fail_final_destroy;
static _Atomic int message_fd = -1;
static _Atomic bool event_live, timer_live;
static _Atomic unsigned message_closes;
static pthread_t original_owner;
static _Atomic bool owner_id_valid;
static bool fail_owner_join, fail_task_join;
static _Atomic bool fail_message_thread;
static _Atomic unsigned successful_owner_joins;
static _Atomic bool suppress_timer;
static _Atomic unsigned completion_signals;
static _Atomic unsigned message_publications, publication_failures;
static tr_reactor_timer_cb timer_callback;
static void *timer_argument;

/* 保留真实发布路径，核对停止回收没有抢先作废仍由 worker 持有的能力。 */
int __real_tr_async_operation_publish(struct tr_async_operation_pool *,
                                      struct tr_async_operation_ref *,
                                      struct tr_async_operation_completion,
                                      struct tr_async_operation_ref *, int *);

int __wrap_tr_async_operation_publish(
        struct tr_async_operation_pool *pool,
        struct tr_async_operation_ref *producer,
        struct tr_async_operation_completion completion,
        struct tr_async_operation_ref *published, int *ready)
{
	int ret = __real_tr_async_operation_publish(pool, producer, completion,
	                                            published, ready);
	atomic_fetch_add(&message_publications, 1);
	if (ret != TR_OK) {
		atomic_fetch_add(&publication_failures, 1);
		fprintf(stderr, "message_publication_failure=%d\n", ret);
	}
	return ret;
}

/* 只在目标执行器真实销毁调用内定位同步对象，不读取私有结构布局。 */
static struct tr_message_service *captured_service;
static _Thread_local bool inside_task_destroy;
static _Atomic bool inspect_partial_destroy, fail_real_mutex_destroy;
static _Atomic(pthread_cond_t *) destroyed_condition;
static _Atomic unsigned condition_destroys, mutex_destroy_failures;
static _Atomic(struct tr_reactor *) fail_destroy_reactor;

int __real_tr_reactor_destroy(struct tr_reactor *);
int __wrap_tr_reactor_destroy(struct tr_reactor *reactor)
{
	struct tr_reactor *expected = reactor;
	if (reactor && atomic_compare_exchange_strong(&fail_destroy_reactor,
	                                              &expected, NULL))
		return TR_ERR_SYS;
	return __real_tr_reactor_destroy(reactor);
}

int __real_tr_message_service_create_on_owner(
        struct tr_reactor *, const struct tr_message_service_config *,
        struct tr_memory_budget *, struct tr_message_service **);
int __real_pthread_cond_destroy(pthread_cond_t *);
int __real_pthread_mutex_destroy(pthread_mutex_t *);
int __real_pthread_cond_broadcast(pthread_cond_t *);
int __real_pthread_cond_signal(pthread_cond_t *);
int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);

int __wrap_tr_message_service_create_on_owner(
        struct tr_reactor *reactor,
        const struct tr_message_service_config *config,
        struct tr_memory_budget *budget, struct tr_message_service **out)
{
	int ret = __real_tr_message_service_create_on_owner(reactor, config,
	                                                    budget, out);
	if (ret == TR_OK && reactor == atomic_load(&target))
		captured_service = *out;
	return ret;
}

int __wrap_pthread_cond_destroy(pthread_cond_t *condition)
{
	int ret;
	if (!inside_task_destroy || !atomic_load(&inspect_partial_destroy))
		return __real_pthread_cond_destroy(condition);
	CHECK(atomic_load(&destroyed_condition) == NULL);
	ret = __real_pthread_cond_destroy(condition);
	if (ret == 0) {
		atomic_store(&destroyed_condition, condition);
		atomic_fetch_add(&condition_destroys, 1);
	}
	return ret;
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	if (inside_task_destroy && atomic_load(&inspect_partial_destroy) &&
	    atomic_exchange(&fail_real_mutex_destroy, false)) {
		/* 条件变量已经真实销毁，不是从组合函数入口直接返回错误。 */
		CHECK(atomic_load(&destroyed_condition) != NULL);
		atomic_fetch_add(&mutex_destroy_failures, 1);
		return EBUSY;
	}
	return __real_pthread_mutex_destroy(mutex);
}

static void check_live_condition(pthread_cond_t *condition)
{
	/* 先断言再进入 pthread，避免让负向用例真的执行未定义行为。 */
	CHECK(condition != atomic_load(&destroyed_condition));
}

int __wrap_pthread_cond_broadcast(pthread_cond_t *condition)
{
	check_live_condition(condition);
	return __real_pthread_cond_broadcast(condition);
}

int __wrap_pthread_cond_signal(pthread_cond_t *condition)
{
	check_live_condition(condition);
	return __real_pthread_cond_signal(condition);
}

int __wrap_pthread_cond_wait(pthread_cond_t *condition, pthread_mutex_t *mutex)
{
	check_live_condition(condition);
	return __real_pthread_cond_wait(condition, mutex);
}

static uint64_t observed_timer(void *arg, uint64_t now)
{
	(void)arg;
	if (atomic_load(&suppress_timer))
		return now + 1000000000;
	return timer_callback(timer_argument, now);
}

ssize_t __real_write(int, const void *, size_t);
ssize_t __wrap_write(int fd, const void *data, size_t length)
{
	ssize_t ret = __real_write(fd, data, length);
	if (fd == atomic_load(&message_fd) && ret == (ssize_t)length)
		atomic_fetch_add(&completion_signals, 1);
	return ret;
}

int __real_epoll_wait(int, struct epoll_event *, int, int);
int __real_pthread_join(pthread_t, void **);
int __real_pthread_create(pthread_t *, const pthread_attr_t *,
                          void *(*)(void *), void *);
int __real_close(int);
int __real_tr_reactor_aux_event_register(struct tr_reactor *, int, uint32_t,
                                         tr_reactor_aux_event_cb, void *);
int __real_tr_reactor_aux_event_unregister(struct tr_reactor *, int);
int __real_tr_reactor_timer_register(struct tr_reactor *, tr_reactor_timer_cb,
                                     void *, struct tr_reactor_timer_handle *);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle);
int __real_tr_task_executor_destroy(struct tr_task_executor **);
size_t __real_ZSTD_compressStream2(ZSTD_CCtx *, ZSTD_outBuffer *,
                                   ZSTD_inBuffer *, ZSTD_EndDirective);

int __wrap_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout)
{
	if (tr_reactor_in_owner_context_for(atomic_load(&target)) &&
	    atomic_exchange(&fail_poll, false)) {
		atomic_fetch_add(&poll_failures, 1);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, count, timeout);
}

int __wrap_pthread_join(pthread_t thread, void **result)
{
	bool owner = atomic_load(&owner_id_valid) &&
	             pthread_equal(thread, original_owner);
	int ret;
	if ((owner && fail_owner_join) || (!owner && fail_task_join)) {
		fail_owner_join = false;
		fail_task_join = false;
		return EBUSY;
	}
	ret = __real_pthread_join(thread, result);
	if (ret == 0 && owner) {
		atomic_fetch_add(&successful_owner_joins, 1);
		atomic_store(&owner_id_valid, false);
	}
	return ret;
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg)
{
	if (tr_reactor_in_owner_context_for(atomic_load(&target)) &&
	    atomic_exchange(&fail_message_thread, false))
		return EAGAIN;
	return __real_pthread_create(thread, attr, fn, arg);
}

int __wrap_tr_reactor_aux_event_register(struct tr_reactor *r, int fd,
                                         uint32_t events,
                                         tr_reactor_aux_event_cb cb, void *arg)
{
	int ret = __real_tr_reactor_aux_event_register(r, fd, events, cb, arg);
	if (r == atomic_load(&target) && ret == TR_OK) {
		atomic_store(&message_fd, fd);
		atomic_store(&event_live, true);
	}
	return ret;
}

int __wrap_tr_reactor_aux_event_unregister(struct tr_reactor *r, int fd)
{
	int ret;
	if (fd == atomic_load(&message_fd) &&
	    atomic_exchange(&fail_event_del, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_aux_event_unregister(r, fd);
	if (fd == atomic_load(&message_fd) && ret == TR_OK)
		atomic_store(&event_live, false);
	return ret;
}

int __wrap_tr_reactor_timer_register(struct tr_reactor *r,
                                     tr_reactor_timer_cb cb, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	int ret;
	if (r == atomic_load(&target)) {
		timer_callback = cb;
		timer_argument = arg;
		ret = __real_tr_reactor_timer_register(r, observed_timer, NULL,
		                                       out);
	} else {
		ret = __real_tr_reactor_timer_register(r, cb, arg, out);
	}
	if (r == atomic_load(&target) && ret == TR_OK)
		atomic_store(&timer_live, true);
	return ret;
}

int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle handle)
{
	int ret;
	if (handle.reactor == atomic_load(&target) &&
	    atomic_exchange(&fail_timer_del, false))
		return TR_ERR_SYS;
	ret = __real_tr_reactor_timer_unregister(handle);
	if (handle.reactor == atomic_load(&target) && ret == TR_OK)
		atomic_store(&timer_live, false);
	return ret;
}

int __wrap_tr_task_executor_destroy(struct tr_task_executor **executor)
{
	int ret;
	if (atomic_exchange(&fail_final_destroy, false))
		return TR_ERR_SYS;
	CHECK(!inside_task_destroy);
	inside_task_destroy = true;
	ret = __real_tr_task_executor_destroy(executor);
	inside_task_destroy = false;
	return ret;
}

int __wrap_close(int fd)
{
	if (fd == atomic_load(&message_fd)) {
		CHECK(!atomic_load(&event_live) && !atomic_load(&timer_live));
		atomic_fetch_add(&message_closes, 1);
	}
	return __real_close(fd);
}

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *c, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(c, out, in, end);
	atomic_fetch_add(&algorithm_calls, 1);
	if (atomic_exchange(&trap_algorithm, false))
		gate_wait(atomic_load(&algorithm_gate), true);
	return ret;
}

struct fixture {
	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_reactor *reactor;
	pthread_t reaper;
};

struct sample {
	struct fixture *fixture;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_message retained;
	struct tr_task_handle handle;
	_Atomic unsigned releases;
	_Atomic unsigned completions;
	bool expect_recovery;
	bool probe_reentry;
};

static int record_owner(void *arg)
{
	(void)arg;
	original_owner = pthread_self();
	atomic_store(&owner_id_valid, true);
	return TR_OK;
}

static int arm_fatal(void *arg)
{
	(void)arg;
	atomic_store(&fail_poll, true);
	return TR_OK;
}

static void init_fixture(struct fixture *f, bool messages)
{
	struct tr_runtime_shard_config shard = {0};
	struct tr_runtime_config config = {1, &shard};
	*f = (struct fixture){.reaper = pthread_self()};
	shard.memory_budget_bytes = 128 * MIB;
	shard.reactor.max_connections = 4;
	shard.reactor.rx_buffer_count = 4;
	shard.reactor.rx_buffer_size = 4096;
	shard.reactor.max_payload_len = 4096;
	shard.reactor.tx_item_capacity = 8;
	shard.reactor.control_tx_item_capacity = 8;
	if (messages)
		shard.messages =
		        (struct tr_message_service_config){{1, 4}, 1, 10000000};
	atomic_store(&callback_count, 0);
	atomic_store(&poll_failures, 0);
	atomic_store(&algorithm_calls, 0);
	atomic_store(&message_closes, 0);
	atomic_store(&message_publications, 0);
	atomic_store(&publication_failures, 0);
	atomic_store(&successful_owner_joins, 0);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_create(&config, &f->runtime) == TR_OK);
	f->shard = tr_runtime_shard_at(f->runtime, 0);
	f->reactor = tr_runtime_shard_reactor(f->shard);
	atomic_store(&target, f->reactor);
}

static void start_fixture(struct fixture *f)
{
	CHECK(tr_runtime_start(f->runtime) == TR_OK);
	CHECK(tr_runtime_shard_call(f->shard, record_owner, NULL) == TR_OK);
}

static void sample_release(void *arg)
{
	struct sample *s = arg;
	atomic_fetch_add(&s->releases, 1);
}

static void init_sample(struct sample *s, struct fixture *f, const void *data,
                        size_t size, bool recovered)
{
	*s = (struct sample){.fixture = f, .expect_recovery = recovered};
	atomic_init(&s->releases, 0);
	atomic_init(&s->completions, 0);
	CHECK(tr_message_storage_reserve(
	              &s->storage, tr_runtime_shard_memory_budget(f->shard),
	              size) == TR_OK);
	CHECK(tr_message_storage_publish(&s->storage, data, size,
	                                 sample_release, s,
	                                 &s->input) == TR_OK);
}

static void done(void *arg, struct tr_task_handle handle,
                 struct tr_message_service_result result,
                 struct tr_message *message)
{
	struct sample *s = arg;
	struct fixture *f = s->fixture;
	struct tr_message_view view;
	CHECK(handle.instance && handle.generation);
	CHECK(atomic_load(&s->releases) == 1);
	if (s->expect_recovery) {
		CHECK(result.delivery == TR_MESSAGE_SERVICE_OWNER_STOPPED);
		CHECK(result.outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
		CHECK(!tr_reactor_in_owner_context());
		CHECK(pthread_equal(pthread_self(), f->reaper));
		CHECK(atomic_load(&successful_owner_joins) > 0);
		CHECK(tr_message_view(message, &view) == TR_ERR_STATE);
		/* 租约不能被回调绕过；不持 ctl_lock 调用本函数，故不会自死锁。
		 */
		CHECK(tr_reactor_start(f->reactor) == TR_ERR_STATE);
		CHECK(tr_reactor_destroy(f->reactor) == TR_ERR_STATE);
		if (s->probe_reentry)
			CHECK(tr_runtime_stop(f->runtime) == TR_ERR_STATE);
	} else {
		CHECK(result.delivery == TR_MESSAGE_SERVICE_ON_OWNER);
		CHECK(result.outcome == TR_MESSAGE_TRANSFORM_OK);
		CHECK(tr_reactor_in_owner_context_for(f->reactor));
		CHECK(tr_message_clone(message, &s->retained) == TR_OK);
	}
	CHECK(atomic_fetch_add(&s->completions, 1) == 0);
	atomic_fetch_add(&callback_count, 1);
}

static int submit_sample(struct sample *s)
{
	struct tr_message_transform_config config = {
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {2 * MIB, 2 * MIB},
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3};
	return tr_runtime_shard_message_submit(s->fixture->shard, &s->input,
	                                       &config, done, s, &s->handle);
}

static int stop_until(struct fixture *f)
{
	uint64_t until = now_seconds() + 15;
	int ret;
	while ((ret = tr_runtime_stop(f->runtime)) == TR_AGAIN) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	return ret;
}

static void test_recovery(bool fatal, unsigned failure)
{
	struct fixture f;
	struct sample a, b;
	struct gate gate;
	struct tr_reactor_recovery_guard probe = {0};
	uint8_t *data = calloc(1, MIB);
	uint64_t until;
	int fd;
	CHECK(data);
	init_fixture(&f, true);
	start_fixture(&f);
	gate_init(&gate);
	atomic_store(&algorithm_gate, &gate);
	atomic_store(&trap_algorithm, true);
	init_sample(&a, &f, data, MIB, true);
	init_sample(&b, &f, data, 1, true);
	a.probe_reentry = true;
	CHECK(submit_sample(&a) == TR_OK);
	gate_wait(&gate, false);
	CHECK(submit_sample(&b) == TR_OK);
	CHECK(tr_reactor_recovery_acquire(f.reactor, &probe) == TR_ERR_STATE);
	CHECK(!probe.reactor);
	if (fatal) {
		CHECK(tr_reactor_call(f.reactor, arm_fatal, NULL) == TR_OK);
		until = now_seconds() + 15;
		while (!atomic_load(&poll_failures)) {
			CHECK(now_seconds() <= until);
			sched_yield();
		}
	}
	/* 即使 epoll 已失败，失败的 join 仍不能充当接管证明。 */
	fail_owner_join = true;
	CHECK(tr_runtime_stop(f.runtime) == TR_ERR_SYS);
	CHECK(atomic_load(&callback_count) == 0);
	CHECK(tr_reactor_recovery_acquire(f.reactor, &probe) == TR_ERR_STATE);
	CHECK(!probe.reactor);
	CHECK(tr_runtime_stop(f.runtime) == TR_AGAIN);
	CHECK(atomic_load(&successful_owner_joins) == 1);
	CHECK(tr_reactor_start(f.reactor) == TR_ERR_STATE);
	CHECK(tr_reactor_destroy(f.reactor) == TR_ERR_STATE);
	CHECK(tr_runtime_start(f.runtime) == TR_ERR_STATE);
	CHECK(atomic_load(&a.releases) == 0);
	CHECK(atomic_load(&callback_count) == 0);
	CHECK(atomic_load(&message_publications) == 0);
	CHECK(atomic_load(&publication_failures) == 0);
	fd = atomic_load(&message_fd);
	CHECK(fcntl(fd, F_GETFD) >= 0);
	if (failure == 1)
		atomic_store(&fail_event_del, true);
	if (failure == 2)
		atomic_store(&fail_timer_del, true);
	if (failure == 3)
		fail_task_join = true;
	if (failure == 4)
		atomic_store(&fail_final_destroy, true);
	gate_open(&gate);
	if (failure) {
		CHECK(stop_until(&f) == TR_ERR_SYS);
		CHECK(fcntl(fd, F_GETFD) >= 0);
		CHECK(atomic_load(&message_closes) == 0);
		CHECK(tr_reactor_start(f.reactor) == TR_ERR_STATE);
		CHECK(tr_reactor_destroy(f.reactor) == TR_ERR_STATE);
		if (failure == 3) {
			CHECK(atomic_load(&message_publications) == 2);
			CHECK(atomic_load(&publication_failures) == 0);
			CHECK(atomic_load(&callback_count) == 0);
		}
	}
	CHECK(stop_until(&f) == TR_OK);
	CHECK(atomic_load(&message_publications) == 2);
	CHECK(atomic_load(&publication_failures) == 0);
	CHECK(atomic_load(&a.completions) == 1 &&
	      atomic_load(&b.completions) == 1);
	CHECK(atomic_load(&a.releases) == 1 && atomic_load(&b.releases) == 1);
	CHECK(atomic_load(&message_closes) == 1);
	CHECK(atomic_load(&algorithm_calls) == 1);
	CHECK(!atomic_load(&event_live) && !atomic_load(&timer_live));
	atomic_store(&algorithm_gate, NULL);
	gate_destroy(&gate);
	/* 成功回收后 Runtime 才能创建下一实例，重启不是清理失败的替代方案。 */
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	CHECK(tr_runtime_shard_call(f.shard, record_owner, NULL) == TR_OK);
	CHECK(stop_until(&f) == TR_OK);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
	free(data);
}

static void test_success_and_retained_budget(void)
{
	struct fixture f;
	struct sample s;
	const uint8_t data[] = "runtime owns message service";
	uint8_t decoded[sizeof(data)];
	struct tr_message_view view;
	uint64_t until = now_seconds() + 15;
	init_fixture(&f, true);
	start_fixture(&f);
	init_sample(&s, &f, data, sizeof(data), false);
	CHECK(submit_sample(&s) == TR_OK);
	while (atomic_load(&s.completions) == 0) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(tr_message_view(&s.retained, &view) == TR_OK);
	CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data, view.len) ==
	      sizeof(data));
	CHECK(memcmp(decoded, data, sizeof(data)) == 0);
	CHECK(stop_until(&f) == TR_OK);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_AGAIN);
	CHECK(tr_message_release(&s.retained) == TR_OK);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
}

static void test_disabled_and_start_failure(void)
{
	struct fixture f;
	struct sample s;
	const uint8_t data[] = "disabled";
	struct tr_message_view view;
	init_fixture(&f, false);
	start_fixture(&f);
	init_sample(&s, &f, data, sizeof(data), false);
	CHECK(submit_sample(&s) == TR_ERR_UNSUPPORTED);
	CHECK(tr_message_view(&s.input, &view) == TR_OK);
	CHECK(tr_message_release(&s.input) == TR_OK);
	CHECK(stop_until(&f) == TR_OK);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	init_fixture(&f, true);
	atomic_store(&fail_message_thread, true);
	CHECK(tr_runtime_start(f.runtime) == TR_ERR_SYS);
	CHECK(stop_until(&f) == TR_OK);
	CHECK(atomic_load(&message_closes) == 1);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
}

struct held_submission {
	struct sample *sample;
	struct gate *owner_gate;
};

static int submit_and_hold(void *arg)
{
	struct held_submission *held = arg;
	CHECK(submit_sample(held->sample) == TR_OK);
	gate_wait(held->owner_gate, true);
	atomic_store(&fail_poll, true);
	return TR_OK;
}

static void *submit_thread(void *arg)
{
	struct held_submission *held = arg;
	CHECK(tr_runtime_shard_call(held->sample->fixture->shard,
	                            submit_and_hold, held) == TR_OK);
	return NULL;
}

static int wrong_owner_create(void *arg)
{
	struct fixture *f = arg;
	struct tr_message_service *unexpected = NULL;
	struct tr_message_service_config config = {{1, 1}, 1, 10000000};
	int ret = tr_message_service_create_on_owner(
	        f->reactor, &config, tr_runtime_shard_memory_budget(f->shard),
	        &unexpected);
	CHECK(!unexpected);
	return ret;
}

static void test_completed_result_recovery(void)
{
	struct fixture f;
	struct sample s;
	struct gate held_owner;
	struct held_submission held = {&s, &held_owner};
	pthread_t submitter;
	uint8_t data[32] = {0};
	uint64_t until = now_seconds() + 15;
	struct tr_reactor *other = NULL;

	init_fixture(&f, true);
	start_fixture(&f);
	CHECK(tr_reactor_create(NULL, NULL, NULL, NULL, &other) == TR_OK);
	CHECK(tr_reactor_start(other) == TR_OK);
	CHECK(tr_reactor_call(other, wrong_owner_create, &f) == TR_ERR_STATE);
	CHECK(tr_reactor_stop(other) == TR_OK);
	CHECK(tr_reactor_destroy(other) == TR_OK);
	init_sample(&s, &f, data, sizeof(data), true);
	gate_init(&held_owner);
	atomic_store(&completion_signals, 0);
	atomic_store(&suppress_timer, true);
	CHECK(pthread_create(&submitter, NULL, submit_thread, &held) == 0);
	gate_wait(&held_owner, false);
	/* worker 已保存成功结果并发出通知，但 owner 被固定在提交 turn 内。
	 * 不允许后备定时器提前交付，下一次 poll 明确失败。 */
	while (atomic_load(&completion_signals) == 0) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(atomic_load(&s.releases) == 1 &&
	      atomic_load(&s.completions) == 0);
	gate_open(&held_owner);
	CHECK(pthread_join(submitter, NULL) == 0);
	while (atomic_load(&poll_failures) == 0) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(stop_until(&f) == TR_OK);
	CHECK(atomic_load(&s.completions) == 1);
	CHECK(atomic_load(&message_closes) == 1);
	atomic_store(&suppress_timer, false);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
	gate_destroy(&held_owner);
}

/* 原 owner 已经拆掉一部分同步对象后退出，恢复只可继续尚未完成的步骤。 */
static int close_captured_service(void *arg)
{
	struct tr_message_service *copy = captured_service;
	int ret;
	(void)arg;
	ret = tr_message_service_close_on_owner(&copy);
	CHECK(copy == captured_service);
	return ret;
}

static void test_partial_destroy_owner_exit(void)
{
	struct fixture f;
	int ret;
	int fd;
	uint64_t until = now_seconds() + 15;

	init_fixture(&f, true);
	start_fixture(&f);
	CHECK(captured_service);
	atomic_store(&condition_destroys, 0);
	atomic_store(&mutex_destroy_failures, 0);
	atomic_store(&destroyed_condition, NULL);
	atomic_store(&inspect_partial_destroy, true);
	atomic_store(&fail_real_mutex_destroy, true);
	while ((ret = tr_runtime_shard_call(f.shard, close_captured_service,
	                                    NULL)) == TR_AGAIN) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(ret == TR_ERR_SYS);
	CHECK(atomic_load(&condition_destroys) == 1);
	CHECK(atomic_load(&mutex_destroy_failures) == 1);
	CHECK(!atomic_load(&event_live) && !atomic_load(&timer_live));
	fd = atomic_load(&message_fd);
	CHECK(fcntl(fd, F_GETFD) >= 0 && atomic_load(&message_closes) == 0);

	CHECK(tr_reactor_call(f.reactor, arm_fatal, NULL) == TR_OK);
	while (!atomic_load(&poll_failures)) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	/* 接管之后再失败一次，验证租约与部分销毁进度均保留。 */
	atomic_store(&fail_real_mutex_destroy, true);
	CHECK(stop_until(&f) == TR_ERR_SYS);
	CHECK(atomic_load(&successful_owner_joins) == 1);
	CHECK(atomic_load(&condition_destroys) == 1);
	CHECK(atomic_load(&mutex_destroy_failures) == 2);
	CHECK(tr_reactor_start(f.reactor) == TR_ERR_STATE);
	CHECK(tr_reactor_destroy(f.reactor) == TR_ERR_STATE);
	CHECK(fcntl(fd, F_GETFD) >= 0 && atomic_load(&message_closes) == 0);
	CHECK(stop_until(&f) == TR_OK);
	CHECK(atomic_load(&condition_destroys) == 1);
	CHECK(atomic_load(&message_closes) == 1);
	atomic_store(&inspect_partial_destroy, false);
	atomic_store(&destroyed_condition, NULL);
	captured_service = NULL;
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
}

static uint64_t shard_bytes(struct tr_runtime_shard *shard)
{
	struct tr_memory_budget_stats stats;
	tr_runtime_shard_memory_stats(shard, &stats);
	CHECK(stats.current_bytes <= stats.limit_bytes);
	return stats.current_bytes;
}

static void wait_sample(struct sample *sample)
{
	uint64_t until = now_seconds() + 15;
	while (!atomic_load(&sample->completions)) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
}

static void *release_message_thread(void *arg)
{
	CHECK(tr_message_release(arg) == TR_OK);
	return NULL;
}

static void release_elsewhere(struct tr_message *message)
{
	pthread_t thread;
	CHECK(pthread_create(&thread, NULL, release_message_thread, message) ==
	      0);
	CHECK(pthread_join(thread, NULL) == 0);
}

static void test_multishard_retained_outputs(unsigned first)
{
	struct tr_runtime_shard_config shards[2] = {0};
	struct tr_runtime_config config = {2, shards};
	struct tr_runtime *runtime = NULL;
	struct fixture f[2] = {0};
	struct sample s[2];
	struct tr_message slices[2] = {0};
	struct tr_message_view views[2];
	uint64_t held[2];
	uint8_t expected[2];
	const char *payloads[2] = {"first shard retained data",
	                           "second shard retained data"};
	unsigned second = first ^ 1U;

	/* 单分片故障包装在本用例禁用，完成回调按各自具体 Reactor 校验。 */
	atomic_store(&target, NULL);
	atomic_store(&message_fd, -1);
	atomic_store(&owner_id_valid, false);
	for (size_t i = 0; i < 2; ++i) {
		shards[i].memory_budget_bytes = 128 * MIB;
		shards[i].reactor.max_connections = 4;
		shards[i].reactor.rx_buffer_count = 4;
		shards[i].reactor.rx_buffer_size = 4096;
		shards[i].reactor.max_payload_len = 4096;
		shards[i].reactor.tx_item_capacity = 8;
		shards[i].reactor.control_tx_item_capacity = 8;
		shards[i].messages =
		        (struct tr_message_service_config){{1, 4}, 1, 10000000};
	}
	CHECK(tr_runtime_create(&config, &runtime) == TR_OK);
	CHECK(tr_runtime_start(runtime) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		f[i].runtime = runtime;
		f[i].shard = tr_runtime_shard_at(runtime, (uint32_t)i);
		f[i].reactor = tr_runtime_shard_reactor(f[i].shard);
		init_sample(&s[i], &f[i], payloads[i], strlen(payloads[i]),
		            false);
		CHECK(submit_sample(&s[i]) == TR_OK);
	}
	for (size_t i = 0; i < 2; ++i) {
		wait_sample(&s[i]);
		CHECK(tr_message_view(&s[i].retained, &views[i]) == TR_OK);
		CHECK(views[i].len > 0);
		expected[i] = views[i].data[views[i].len - 1];
		CHECK(tr_message_slice(&s[i].retained, views[i].len - 1, 1,
		                       &slices[i]) == TR_OK);
	}
	CHECK(stop_until(&f[0]) == TR_OK);
	/* 错误可以在首个或第二个分片出现；仍释放另一分片，且 SYS 不被
	 * 较早或较晚的 AGAIN 掩盖。故障只注入一次，随后继续相同对象重试。 */
	atomic_store(&fail_destroy_reactor, f[first].reactor);
	CHECK(tr_runtime_destroy(runtime) == TR_ERR_SYS);
	CHECK(tr_runtime_shard_reactor(f[first].shard) == f[first].reactor);
	CHECK(tr_runtime_shard_reactor(f[second].shard) == NULL);
	CHECK(tr_runtime_start(runtime) == TR_ERR_STATE);
	CHECK(tr_runtime_destroy(runtime) == TR_AGAIN);
	CHECK(tr_runtime_start(runtime) == TR_ERR_STATE);
	for (size_t i = 0; i < 2; ++i) {
		/* 一个预算外壳被保留，不应阻碍另一分片已静默资源释放。 */
		CHECK(tr_runtime_shard_reactor(f[i].shard) == NULL);
		held[i] = shard_bytes(f[i].shard);
		CHECK(held[i] > 0);
		CHECK(tr_message_view(&slices[i], &views[i]) == TR_OK);
		CHECK(views[i].len == 1 && views[i].data[0] == expected[i]);
	}
	release_elsewhere(&s[first].retained);
	CHECK(shard_bytes(f[first].shard) == held[first]);
	CHECK(tr_message_release(&slices[first]) == TR_OK);
	CHECK(shard_bytes(f[first].shard) == 0);
	CHECK(shard_bytes(f[second].shard) == held[second]);
	CHECK(tr_runtime_destroy(runtime) == TR_AGAIN);
	CHECK(tr_runtime_start(runtime) == TR_ERR_STATE);
	CHECK(tr_message_release(&s[second].retained) == TR_OK);
	CHECK(shard_bytes(f[second].shard) == held[second]);
	release_elsewhere(&slices[second]);
	CHECK(shard_bytes(f[0].shard) == 0 && shard_bytes(f[1].shard) == 0);
	CHECK(tr_runtime_destroy(runtime) == TR_OK);
}

struct mixed_submission {
	struct sample *completed;
	struct sample *running;
	struct sample *queued;
	struct gate *owner_gate;
};

static int submit_mixed_and_hold(void *arg)
{
	struct mixed_submission *mixed = arg;
	unsigned signals = atomic_load(&completion_signals);
	uint64_t until = now_seconds() + 15;
	CHECK(submit_sample(mixed->completed) == TR_OK);
	while (atomic_load(&completion_signals) == signals) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	atomic_store(&trap_algorithm, true);
	CHECK(submit_sample(mixed->running) == TR_OK);
	CHECK(submit_sample(mixed->queued) == TR_OK);
	gate_wait(mixed->owner_gate, true);
	atomic_store(&fail_poll, true);
	return TR_OK;
}

static void *mixed_submitter(void *arg)
{
	struct mixed_submission *mixed = arg;
	CHECK(tr_runtime_shard_call(mixed->completed->fixture->shard,
	                            submit_mixed_and_hold, mixed) == TR_OK);
	return NULL;
}

static void test_mixed_result_domains(void)
{
	struct fixture f;
	struct sample retained, completed, running, queued;
	struct gate owner_gate, worker_gate;
	struct mixed_submission mixed = {&completed, &running, &queued,
	                                 &owner_gate};
	struct tr_message_view view;
	const uint8_t initial[] = "owner result must survive recovery";
	uint8_t decoded[sizeof(initial)];
	uint8_t *large = calloc(1, MIB);
	pthread_t submitter;
	uint64_t until = now_seconds() + 15;
	CHECK(large);
	init_fixture(&f, true);
	start_fixture(&f);
	init_sample(&retained, &f, initial, sizeof(initial), false);
	CHECK(submit_sample(&retained) == TR_OK);
	wait_sample(&retained);
	init_sample(&completed, &f, initial, sizeof(initial), true);
	init_sample(&running, &f, large, MIB, true);
	init_sample(&queued, &f, large, 1, true);
	gate_init(&owner_gate);
	gate_init(&worker_gate);
	atomic_store(&algorithm_gate, &worker_gate);
	atomic_store(&suppress_timer, true);
	CHECK(pthread_create(&submitter, NULL, mixed_submitter, &mixed) == 0);
	gate_wait(&owner_gate, false);
	gate_wait(&worker_gate, false);
	CHECK(atomic_load(&retained.completions) == 1);
	CHECK(atomic_load(&completed.releases) == 1 &&
	      atomic_load(&completed.completions) == 0);
	CHECK(atomic_load(&running.releases) == 0 &&
	      atomic_load(&queued.releases) == 0);
	gate_open(&owner_gate);
	CHECK(pthread_join(submitter, NULL) == 0);
	while (!atomic_load(&poll_failures)) {
		CHECK(now_seconds() <= until);
		sched_yield();
	}
	CHECK(tr_runtime_stop(f.runtime) == TR_AGAIN);
	CHECK(atomic_load(&successful_owner_joins) == 1);
	CHECK(atomic_load(&retained.completions) == 1);
	CHECK(tr_message_view(&retained.retained, &view) == TR_OK);
	CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data, view.len) ==
	      sizeof(initial));
	CHECK(memcmp(decoded, initial, sizeof(initial)) == 0);
	gate_open(&worker_gate);
	CHECK(stop_until(&f) == TR_OK);
	CHECK(atomic_load(&message_publications) == 4);
	CHECK(atomic_load(&publication_failures) == 0);
	CHECK(atomic_load(&retained.completions) == 1);
	CHECK(atomic_load(&completed.completions) == 1 &&
	      atomic_load(&running.completions) == 1 &&
	      atomic_load(&queued.completions) == 1);
	CHECK(atomic_load(&callback_count) == 4);
	atomic_store(&algorithm_gate, NULL);
	atomic_store(&suppress_timer, false);
	atomic_store(&message_fd, -1);
	CHECK(tr_runtime_destroy(f.runtime) == TR_AGAIN);
	CHECK(tr_message_view(&retained.retained, &view) == TR_OK);
	CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data, view.len) ==
	      sizeof(initial));
	CHECK(memcmp(decoded, initial, sizeof(initial)) == 0);
	CHECK(tr_message_release(&retained.retained) == TR_OK);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	atomic_store(&target, NULL);
	gate_destroy(&owner_gate);
	gate_destroy(&worker_gate);
	free(large);
}

int main(void)
{
	test_partial_destroy_owner_exit();
	test_mixed_result_domains();
	test_multishard_retained_outputs(0);
	test_multishard_retained_outputs(1);
	test_completed_result_recovery();
	test_success_and_retained_budget();
	test_disabled_and_start_failure();
	for (unsigned failure = 0; failure <= 4; ++failure)
		test_recovery(true, failure);
	test_recovery(false, 0);
	puts("runtime_message_recovery: ok");
	return 0;
}
