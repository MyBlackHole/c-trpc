#define _POSIX_C_SOURCE 200809L
#include "../src/execution/task_executor.h"
#include "../src/execution/worker_context.h"
#include "../src/execution/task_executor_internal.h"
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
#define MIB ((size_t)1024 * 1024)

void *__real_calloc(size_t count, size_t size);
int __real_pthread_mutex_init(pthread_mutex_t *mutex,
                              const pthread_mutexattr_t *attr);
int __real_pthread_cond_init(pthread_cond_t *cond,
                             const pthread_condattr_t *attr);
int __real_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg);
int __real_pthread_join(pthread_t thread, void **result);
int __real_pthread_cond_destroy(pthread_cond_t *cond);
int __real_pthread_mutex_destroy(pthread_mutex_t *mutex);

static _Thread_local unsigned alloc_count, fail_alloc, create_count,
        fail_create;
static _Thread_local unsigned join_count, fail_join, join_success;
static _Thread_local bool fail_mutex_init, fail_cond_init, fail_cond_destroy,
        fail_mutex_destroy;
static _Thread_local bool count_allocations;

void *__wrap_calloc(size_t count, size_t size)
{
	if (count_allocations) {
		++alloc_count;
		if (alloc_count == fail_alloc)
			return NULL;
	}
	return __real_calloc(count, size);
}

int __wrap_pthread_mutex_init(pthread_mutex_t *mutex,
                              const pthread_mutexattr_t *attr)
{
	if (fail_mutex_init) {
		fail_mutex_init = false;
		return ENOMEM;
	}
	return __real_pthread_mutex_init(mutex, attr);
}

int __wrap_pthread_cond_init(pthread_cond_t *cond,
                             const pthread_condattr_t *attr)
{
	if (fail_cond_init) {
		fail_cond_init = false;
		return ENOMEM;
	}
	return __real_pthread_cond_init(cond, attr);
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg)
{
	++create_count;
	if (create_count == fail_create)
		return EAGAIN;
	return __real_pthread_create(thread, attr, fn, arg);
}

int __wrap_pthread_join(pthread_t thread, void **result)
{
	int ret;

	++join_count;
	if (join_count == fail_join)
		return EBUSY;
	ret = __real_pthread_join(thread, result);
	if (ret == 0)
		++join_success;
	return ret;
}

int __wrap_pthread_cond_destroy(pthread_cond_t *cond)
{
	if (fail_cond_destroy) {
		fail_cond_destroy = false;
		return EBUSY;
	}
	return __real_pthread_cond_destroy(cond);
}

int __wrap_pthread_mutex_destroy(pthread_mutex_t *mutex)
{
	if (fail_mutex_destroy) {
		fail_mutex_destroy = false;
		return EBUSY;
	}
	return __real_pthread_mutex_destroy(mutex);
}

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered;
	bool open;
};

static void gate_init(struct gate *gate)
{
	*gate = (struct gate){0};
	CHECK(pthread_mutex_init(&gate->lock, NULL) == 0);
	CHECK(pthread_cond_init(&gate->cond, NULL) == 0);
}

static struct timespec timeout_at(void)
{
	struct timespec ts;

	CHECK(clock_gettime(CLOCK_REALTIME, &ts) == 0);
	ts.tv_sec += 10;
	return ts;
}

static void gate_enter(struct gate *gate)
{
	struct timespec end = timeout_at();

	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	gate->entered = true;
	CHECK(pthread_cond_broadcast(&gate->cond) == 0);
	while (!gate->open)
		CHECK(pthread_cond_timedwait(&gate->cond, &gate->lock, &end) ==
		      0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void gate_wait(struct gate *gate)
{
	struct timespec end = timeout_at();

	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	while (!gate->entered)
		CHECK(pthread_cond_timedwait(&gate->cond, &gate->lock, &end) ==
		      0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void gate_open(struct gate *gate)
{
	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	gate->open = true;
	CHECK(pthread_cond_broadcast(&gate->cond) == 0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void gate_destroy(struct gate *gate)
{
	CHECK(pthread_cond_destroy(&gate->cond) == 0);
	CHECK(pthread_mutex_destroy(&gate->lock) == 0);
}

static uint64_t now_ms(void)
{
	struct timespec ts;

	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static struct tr_task_executor_stats snapshot(struct tr_task_executor *executor)
{
	struct tr_task_executor_stats stats;

	CHECK(tr_task_executor_get_stats(executor, &stats) == TR_OK);
	CHECK(stats.outstanding == stats.queued + stats.running +
	                                   stats.completed + stats.delivering);
	CHECK(stats.outstanding <= stats.capacity);
	return stats;
}

static void wait_completed(struct tr_task_executor *executor, size_t count)
{
	uint64_t deadline = now_ms() + 10000;

	while (snapshot(executor).completed != count) {
		CHECK(now_ms() < deadline);
		sched_yield();
	}
}

static int join_until(struct tr_task_executor *executor)
{
	uint64_t deadline = now_ms() + 10000;
	int ret;

	while ((ret = tr_task_executor_join(executor)) == TR_AGAIN) {
		CHECK(now_ms() < deadline);
		sched_yield();
	}
	return ret;
}

static struct tr_task_executor *make_executor(struct tr_memory_budget *budget,
                                              size_t workers, size_t capacity)
{
	struct tr_task_executor *executor = NULL;
	struct tr_task_executor_config config = {workers, capacity};

	CHECK(tr_task_executor_create(&config, budget, &executor) == TR_OK);
	CHECK(tr_task_executor_start(executor) == TR_OK);
	return executor;
}

struct job {
	struct tr_task_executor *executor;
	struct gate *work_gate;
	struct gate *done_gate;
	pthread_t owner;
	_Atomic unsigned works;
	unsigned dones;
	bool saw_cancel;
	bool check_reentrant;
	int value;
	struct tr_task_result result;
	struct tr_task_handle delivered_handle;
};

static void job_init(struct job *job, struct tr_task_executor *executor,
                     int value)
{
	*job = (struct job){
	        .executor = executor, .owner = pthread_self(), .value = value};
	atomic_init(&job->works, 0);
}

static int work(void *arg, const struct tr_task_cancel *cancel)
{
	struct job *job = arg;
	size_t count = 777;

	CHECK(tr_execution_in_worker_context());
	CHECK(tr_task_executor_adopt_cleanup(job->executor) == TR_ERR_STATE);
	CHECK(!pthread_equal(pthread_self(), job->owner));
	CHECK(tr_task_executor_dispatch(job->executor, 1, &count) ==
	      TR_ERR_STATE);
	CHECK(count == 777);
	CHECK(tr_task_executor_join(job->executor) == TR_ERR_STATE);
	if (job->work_gate)
		gate_enter(job->work_gate);
	job->saw_cancel = tr_task_cancel_requested(cancel);
	atomic_fetch_add(&job->works, 1);
	return job->value;
}

static void done(void *arg, struct tr_task_handle handle,
                 struct tr_task_result result)
{
	struct job *job = arg;
	struct tr_task_executor *same = job->executor;
	struct tr_task_handle ignored = {0};
	struct tr_task_spec spec = {work, done, job};
	size_t count = 888;

	CHECK(!tr_execution_in_worker_context());
	CHECK(pthread_equal(pthread_self(), job->owner));
	CHECK(atomic_load(&job->works) == 1);
	CHECK(snapshot(job->executor).delivering == 1);
	CHECK(tr_task_executor_cancel(job->executor, handle) == TR_ERR_STATE);
	if (job->check_reentrant) {
		CHECK(tr_task_executor_dispatch(job->executor, 1, &count) ==
		      TR_ERR_STATE);
		CHECK(count == 888);
		CHECK(tr_task_executor_join(job->executor) == TR_ERR_STATE);
		CHECK(tr_task_executor_destroy(&same) == TR_ERR_STATE);
		CHECK(same == job->executor);
		CHECK(tr_task_executor_submit(job->executor, &spec, &ignored) ==
		      TR_AGAIN);
	}
	if (job->done_gate)
		gate_enter(job->done_gate);
	job->result = result;
	job->delivered_handle = handle;
	++job->dones;
}

static struct tr_task_handle submit(struct tr_task_executor *executor,
                                    struct job *job)
{
	struct tr_task_spec spec = {work, done, job};
	struct tr_task_handle handle = {0};

	CHECK(tr_task_executor_submit(executor, &spec, &handle) == TR_OK);
	return handle;
}

static void test_capacity_cancel_join(void)
{
	struct tr_memory_budget budget;
	struct tr_task_executor *executor;
	struct tr_task_handle first, second, sentinel = {777, 888, 999};
	struct job a, b, rejected;
	struct tr_task_spec spec;
	struct gate gate;
	size_t delivered;
	unsigned before;

	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 1, 2);
	gate_init(&gate);
	job_init(&a, executor, 31);
	job_init(&b, executor, 32);
	job_init(&rejected, executor, 33);
	a.work_gate = &gate;
	first = submit(executor, &a);
	gate_wait(&gate);
	second = submit(executor, &b);
	spec = (struct tr_task_spec){work, done, &rejected};
	count_allocations = true;
	before = alloc_count;
	for (unsigned i = 0; i < 100; ++i)
		CHECK(tr_task_executor_submit(executor, &spec, &sentinel) ==
		      TR_AGAIN);
	CHECK(alloc_count == before);
	count_allocations = false;
	CHECK(sentinel.instance == 777 && sentinel.generation == 888 &&
	      sentinel.slot == 999);
	CHECK(tr_task_executor_cancel(executor, second) == TR_OK);
	CHECK(tr_task_executor_cancel(executor, second) == TR_OK);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(tr_task_executor_join(executor) == TR_AGAIN);
	CHECK(tr_task_executor_submit(executor, &spec, &sentinel) ==
	      TR_ERR_CLOSED);
	/* DRAIN 可以升级取消，后来的 DRAIN 不能撤销已接受取消。 */
	CHECK(tr_task_executor_stop(executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	gate_open(&gate);
	CHECK(join_until(executor) == TR_OK);
	CHECK(snapshot(executor).completed == 2 && a.dones == 0 &&
	      b.dones == 0);
	CHECK(tr_task_executor_destroy(&executor) == TR_AGAIN);
	CHECK(tr_task_executor_cancel(executor, first) == TR_ERR_STATE);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(snapshot(executor).outstanding == 1);
	CHECK(tr_task_executor_dispatch(executor, 8, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(a.dones == 1 && b.dones == 1 && a.result.cancelled &&
	      b.result.cancelled);
	CHECK(a.result.work_status == 31 && b.result.work_status == 32);
	CHECK(a.saw_cancel && b.saw_cancel);
	CHECK(atomic_load(&rejected.works) == 0 && rejected.dones == 0);
	CHECK(tr_task_executor_cancel(executor, first) == TR_ERR_STALE);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK && executor == NULL);
	CHECK(atomic_load(&budget.current_bytes) == 0);
	gate_destroy(&gate);
}

static void test_generation_and_completion_capacity(void)
{
	struct tr_memory_budget budget;
	struct tr_task_executor *executor, *other;
	struct job a, b;
	struct tr_task_handle old, next, untouched = {0};
	struct tr_task_spec spec;
	size_t delivered;
	uint64_t generation = UINT64_MAX - 1;
	_Atomic uint64_t instance;
	uint64_t value = 123;

	CHECK(tr_task_generation_next(&generation) == TR_OK &&
	      generation == UINT64_MAX);
	CHECK(tr_task_generation_next(&generation) == TR_ERR_STATE &&
	      generation == UINT64_MAX);
	atomic_init(&instance, UINT64_MAX - 1);
	CHECK(tr_task_instance_next(&instance, &value) == TR_OK &&
	      value == UINT64_MAX);
	value = 123;
	CHECK(tr_task_instance_next(&instance, &value) == TR_ERR_STATE &&
	      value == 123);
	CHECK(atomic_load(&instance) == UINT64_MAX);

	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 1, 1);
	other = make_executor(&budget, 1, 1);
	job_init(&a, executor, 41);
	job_init(&b, executor, 42);
	a.check_reentrant = true;
	old = submit(executor, &a);
	wait_completed(executor, 1);
	CHECK(tr_task_executor_cancel(other, old) == TR_ERR_STALE);
	CHECK(tr_task_executor_cancel(executor, old) == TR_ERR_STATE);
	spec = (struct tr_task_spec){work, done, &b};
	CHECK(tr_task_executor_submit(executor, &spec, &untouched) == TR_AGAIN);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(a.dones == 1 && !a.result.cancelled);
	next = submit(executor, &b);
	CHECK(next.slot == old.slot && next.generation > old.generation);
	CHECK(tr_task_executor_cancel(executor, old) == TR_ERR_STALE);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(executor) == TR_OK);
	CHECK(tr_task_executor_stop(executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(!b.result.cancelled && b.result.work_status == 42);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(tr_task_executor_stop(other, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(other) == TR_OK);
	CHECK(tr_task_executor_destroy(&other) == TR_OK);

	executor = make_executor(&budget, 1, 1);
	job_init(&a, executor, 43);
	next = submit(executor, &a);
	CHECK(next.instance != old.instance);
	CHECK(tr_task_executor_cancel(executor, old) == TR_ERR_STALE);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(executor) == TR_OK);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
}

static void test_constructor_failures(void)
{
	struct tr_task_executor_config config = {3, 3};
	struct tr_memory_budget budget;
	struct tr_task_executor *executor = NULL;

	tr_memory_budget_init(&budget, 1);
	CHECK(tr_task_executor_create(&config, &budget, &executor) == TR_AGAIN);
	CHECK(executor == NULL && atomic_load(&budget.current_bytes) == 0);
	tr_memory_budget_init(&budget, MIB);
	for (unsigned i = 1; i <= 3; ++i) {
		count_allocations = true;
		alloc_count = 0;
		fail_alloc = i;
		CHECK(tr_task_executor_create(&config, &budget, &executor) ==
		      TR_ERR_NOMEM);
		CHECK(executor == NULL &&
		      atomic_load(&budget.current_bytes) == 0);
		count_allocations = false;
	}
	fail_alloc = 0;
	fail_mutex_init = true;
	CHECK(tr_task_executor_create(&config, &budget, &executor) ==
	      TR_ERR_SYS);
	CHECK(executor == NULL && atomic_load(&budget.current_bytes) == 0);
	fail_cond_init = true;
	CHECK(tr_task_executor_create(&config, &budget, &executor) ==
	      TR_ERR_SYS);
	CHECK(executor == NULL && atomic_load(&budget.current_bytes) == 0);
	for (unsigned i = 1; i <= 3; ++i) {
		struct job job;
		struct tr_task_handle handle = {0};
		struct tr_task_spec spec = {work, done, &job};

		CHECK(tr_task_executor_create(&config, &budget, &executor) ==
		      TR_OK);
		job_init(&job, executor, 7);
		create_count = 0;
		fail_create = i;
		CHECK(tr_task_executor_start(executor) == TR_ERR_SYS);
		fail_create = 0;
		CHECK(!snapshot(executor).accepting);
		CHECK(tr_task_executor_submit(executor, &spec, &handle) ==
		      TR_ERR_CLOSED);
		CHECK(tr_task_executor_destroy(&executor) == TR_ERR_STATE);
		CHECK(join_until(executor) == TR_OK);
		CHECK(atomic_load(&job.works) == 0 && job.dones == 0);
		CHECK(tr_task_executor_destroy(&executor) == TR_OK);
		CHECK(atomic_load(&budget.current_bytes) == 0);
	}
	CHECK(tr_task_executor_create(&config, &budget, &executor) == TR_OK);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
}

static void test_join_and_destroy_retry(void)
{
	struct tr_memory_budget budget;
	struct tr_task_executor *executor;
	uint64_t held;

	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 3, 3);
	held = atomic_load(&budget.current_bytes);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	join_count = join_success = 0;
	fail_join = 2;
	CHECK(join_until(executor) == TR_ERR_SYS);
	CHECK(join_success == 1 && executor != NULL);
	CHECK(atomic_load(&budget.current_bytes) == held);
	CHECK(tr_task_executor_destroy(&executor) == TR_ERR_STATE);
	fail_join = 0;
	CHECK(join_until(executor) == TR_OK);
	CHECK(join_success == 3 && join_count == 4);
	CHECK(tr_task_executor_join(executor) == TR_OK && join_count == 4);
	fail_cond_destroy = true;
	CHECK(tr_task_executor_destroy(&executor) == TR_ERR_SYS);
	CHECK(executor != NULL && atomic_load(&budget.current_bytes) == held);
	fail_mutex_destroy = true;
	CHECK(tr_task_executor_destroy(&executor) == TR_ERR_SYS);
	CHECK(executor != NULL && atomic_load(&budget.current_bytes) == held);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK && executor == NULL);
	CHECK(atomic_load(&budget.current_bytes) == 0);
}

struct inspector {
	struct job *job;
	struct gate *gate;
	struct tr_task_handle handle;
};

static void *inspect_callback(void *arg)
{
	struct inspector *inspector = arg;
	struct tr_task_executor *executor = inspector->job->executor;
	struct tr_task_spec spec = {work, done, inspector->job};
	struct tr_task_handle unchanged = {0};
	struct tr_task_executor_stats stats;
	size_t count = 0;

	gate_wait(inspector->gate);
	stats = snapshot(executor);
	CHECK(stats.outstanding == 1 && stats.delivering == 1 &&
	      stats.completed == 0);
	CHECK(tr_task_executor_submit(executor, &spec, &unchanged) == TR_AGAIN);
	CHECK(tr_task_executor_cancel(executor, inspector->handle) ==
	      TR_ERR_STATE);
	CHECK(tr_task_executor_dispatch(executor, 1, &count) == TR_ERR_STATE);
	gate_open(inspector->gate);
	return NULL;
}

static void test_callback_barrier(void)
{
	struct tr_memory_budget budget;
	struct tr_task_executor *executor;
	struct job job;
	struct gate gate;
	struct inspector inspector;
	pthread_t observer;
	size_t count;

	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 1, 1);
	gate_init(&gate);
	job_init(&job, executor, 19);
	job.done_gate = &gate;
	inspector = (struct inspector){&job, &gate, submit(executor, &job)};
	wait_completed(executor, 1);
	CHECK(pthread_create(&observer, NULL, inspect_callback, &inspector) ==
	      0);
	CHECK(tr_task_executor_dispatch(executor, 1, &count) == TR_OK &&
	      count == 1);
	CHECK(pthread_join(observer, NULL) == 0);
	CHECK(snapshot(executor).outstanding == 0);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(executor) == TR_OK);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
	gate_destroy(&gate);
}

struct owned_job {
	_Atomic unsigned *retired;
};

static int owned_work(void *arg, const struct tr_task_cancel *cancel)
{
	(void)arg;
	(void)cancel;
	CHECK(tr_execution_in_worker_context());
	return 9;
}

static void owned_done(void *arg, struct tr_task_handle handle,
                       struct tr_task_result result)
{
	struct owned_job *job = arg;
	_Atomic unsigned *retired = job->retired;

	CHECK(handle.generation != 0 && !result.cancelled &&
	      result.work_status == 9);
	free(job);
	atomic_fetch_add(retired, 1);
}

static void test_callback_frees_arg(void)
{
	struct tr_memory_budget budget;
	struct tr_task_executor *executor;
	struct owned_job *job = malloc(sizeof(*job));
	_Atomic unsigned retired = 0;
	struct tr_task_spec spec = {owned_work, owned_done, job};
	struct tr_task_handle handle;
	size_t count;

	CHECK(job);
	job->retired = &retired;
	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 1, 1);
	CHECK(tr_task_executor_submit(executor, &spec, &handle) == TR_OK);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(executor) == TR_OK);
	CHECK(tr_task_executor_dispatch(executor, 1, &count) == TR_OK &&
	      count == 1);
	CHECK(atomic_load(&retired) == 1);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
}

struct producer {
	struct tr_task_executor *executor;
	struct job *jobs;
	size_t begin;
	size_t end;
	_Atomic unsigned *finished;
};

static void *produce(void *arg)
{
	struct producer *producer = arg;
	uint64_t deadline = now_ms() + 10000;

	for (size_t i = producer->begin; i < producer->end; ++i) {
		struct tr_task_spec spec = {work, done, &producer->jobs[i]};
		struct tr_task_handle handle;
		int ret;

		while ((ret = tr_task_executor_submit(producer->executor, &spec,
		                                      &handle)) == TR_AGAIN) {
			CHECK(now_ms() < deadline);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		/* 发布后不再读写 arg；所有者可能已在 submit 返回前处理它。 */
	}
	atomic_fetch_add(producer->finished, 1);
	return NULL;
}

static void test_multi_producer(void)
{
	enum { N = 512, PRODUCERS = 4 };
	struct tr_memory_budget budget;
	struct tr_task_executor *executor;
	struct job jobs[N];
	struct producer producers[PRODUCERS];
	pthread_t threads[PRODUCERS];
	_Atomic unsigned finished = 0;
	uint64_t deadline = now_ms() + 10000;
	size_t total = 0;

	tr_memory_budget_init(&budget, MIB);
	executor = make_executor(&budget, 3, 7);
	for (size_t i = 0; i < N; ++i)
		job_init(&jobs[i], executor, (int)i);
	for (size_t i = 0; i < PRODUCERS; ++i) {
		producers[i] =
		        (struct producer){executor, jobs, i * (N / PRODUCERS),
		                          (i + 1) * (N / PRODUCERS), &finished};
		CHECK(pthread_create(&threads[i], NULL, produce,
		                     &producers[i]) == 0);
	}
	while (total < N) {
		size_t count;

		CHECK(now_ms() < deadline);
		CHECK(tr_task_executor_dispatch(executor, 2, &count) == TR_OK);
		CHECK(count <= 2);
		total += count;
		(void)snapshot(executor);
		sched_yield();
	}
	for (size_t i = 0; i < PRODUCERS; ++i)
		CHECK(pthread_join(threads[i], NULL) == 0);
	CHECK(atomic_load(&finished) == PRODUCERS);
	for (size_t i = 0; i < N; ++i)
		CHECK(jobs[i].dones == 1 && !jobs[i].result.cancelled &&
		      jobs[i].result.work_status == (int)i);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	CHECK(join_until(executor) == TR_OK);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
}

int main(void)
{
	test_constructor_failures();
	test_capacity_cancel_join();
	test_generation_and_completion_capacity();
	test_join_and_destroy_retry();
	test_callback_barrier();
	test_callback_frees_arg();
	test_multi_producer();
	puts("task_executor: ok");
	return 0;
}
