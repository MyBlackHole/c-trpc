#define _POSIX_C_SOURCE 200809L
#include "../src/execution/task_executor.h"
#include "../src/message/transform.h"
#include "../src/message/storage.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <zstd.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static void gate_wait(struct gate *gate, bool worker)
{
	struct timespec end;

	CHECK(clock_gettime(CLOCK_REALTIME, &end) == 0);
	end.tv_sec += 10;
	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	if (worker) {
		gate->entered = true;
		CHECK(pthread_cond_broadcast(&gate->cond) == 0);
	}
	while (worker ? !gate->open : !gate->entered)
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

struct message_job {
	struct tr_message_storage storage;
	struct tr_message_transform *operation;
	struct tr_message output;
	struct gate *before;
	struct gate *after;
	pthread_t owner;
	_Atomic unsigned releases;
	unsigned completions;
	enum tr_message_transform_outcome outcome;
	struct tr_task_result result;
};

static const unsigned char text[] = "bounded message completion and ownership";

static void input_release(void *arg)
{
	struct message_job *job = arg;

	atomic_fetch_add(&job->releases, 1);
}

static void prepare_job(struct message_job *job,
                        struct tr_memory_budget *budget)
{
	struct tr_message input = {0};
	struct tr_message_transform_config config = {
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {MIB, MIB},
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3,
	};

	*job = (struct message_job){.owner = pthread_self()};
	atomic_init(&job->releases, 0);
	CHECK(tr_message_storage_reserve(&job->storage, budget, sizeof(text)) ==
	      TR_OK);
	CHECK(tr_message_storage_publish(&job->storage, text, sizeof(text),
	                                 input_release, job, &input) == TR_OK);
	CHECK(tr_message_transform_prepare(&input, &config, budget,
	                                   &job->operation) == TR_OK);
}

/* 这是组合测试适配，不是正式网络适配器。只在 run 前传递取消，运行中
 * 的取消由执行器终局及 done 清理拦截成功输出；不宣称已实现后端中途抢占。
 */
static int message_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct message_job *job = arg;
	int ret;

	CHECK(!pthread_equal(pthread_self(), job->owner));
	if (job->before)
		gate_wait(job->before, true);
	if (tr_task_cancel_requested(cancel))
		CHECK(tr_message_transform_cancel(job->operation) == TR_OK);
	ret = tr_message_transform_run(job->operation);
	if (job->after)
		gate_wait(job->after, true);
	return ret;
}

static void message_done(void *arg, struct tr_task_handle handle,
                         struct tr_task_result result)
{
	struct message_job *job = arg;

	CHECK(pthread_equal(pthread_self(), job->owner));
	CHECK(handle.instance != 0 && result.work_status == TR_OK);
	CHECK(atomic_load(&job->releases) == 1);
	CHECK(tr_message_transform_finish(&job->operation, &job->outcome,
	                                  &job->output) == TR_OK);
	if (result.cancelled) {
		/* 算法已经成功不等于执行器已提交。迟到取消仍须清理成功载荷。 */
		if (job->outcome == TR_MESSAGE_TRANSFORM_OK)
			CHECK(tr_message_release(&job->output) == TR_OK);
		job->outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
	}
	job->result = result;
	++job->completions;
}

static void join_executor(struct tr_task_executor *executor)
{
	struct timespec begin, now;
	int ret;

	CHECK(clock_gettime(CLOCK_MONOTONIC, &begin) == 0);
	while ((ret = tr_task_executor_join(executor)) == TR_AGAIN) {
		CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		CHECK(now.tv_sec - begin.tv_sec < 10);
		sched_yield();
	}
	CHECK(ret == TR_OK);
}

static void test_message_execution(unsigned phase)
{
	struct tr_memory_budget budget;
	struct tr_task_executor_config config = {1, 1};
	struct tr_task_executor *executor = NULL;
	struct message_job job, rejected;
	struct tr_task_spec spec = {message_work, message_done, &job};
	struct tr_task_handle handle, rejected_handle = {0};
	struct gate gate;
	struct tr_message retained = {0};
	struct tr_message_view view;
	size_t delivered;
	uint64_t baseline;

	tr_memory_budget_init(&budget, 64 * MIB);
	CHECK(tr_task_executor_create(&config, &budget, &executor) == TR_OK);
	CHECK(tr_task_executor_start(executor) == TR_OK);
	baseline = atomic_load(&budget.current_bytes);
	gate_init(&gate);
	prepare_job(&job, &budget);
	if (phase == 1)
		job.before = &gate;
	if (phase == 2)
		job.after = &gate;
	CHECK(tr_task_executor_submit(executor, &spec, &handle) == TR_OK);
	if (phase != 0) {
		gate_wait(&gate, false);
		CHECK(job.completions == 0);
		CHECK(atomic_load(&job.releases) == (phase == 1 ? 0U : 1U));
		/* 第二个变换已接受，但执行器满载。所有权仍由提交方清理，
		 * 不能把 prepare 成功当成 executor submit 成功。
		 */
		prepare_job(&rejected, &budget);
		spec.arg = &rejected;
		CHECK(tr_task_executor_submit(executor, &spec,
		                              &rejected_handle) == TR_AGAIN);
		CHECK(rejected.operation != NULL && rejected.completions == 0);
		CHECK(tr_message_transform_cancel(rejected.operation) == TR_OK);
		CHECK(tr_message_transform_run(rejected.operation) == TR_OK);
		CHECK(tr_message_transform_finish(&rejected.operation,
		                                  &rejected.outcome,
		                                  &rejected.output) == TR_OK);
		CHECK(rejected.outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
		CHECK(tr_task_executor_cancel(executor, handle) == TR_OK);
		gate_open(&gate);
	}
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	join_executor(executor);
	CHECK(job.completions == 0 && job.operation != NULL);
	CHECK(tr_task_executor_destroy(&executor) == TR_AGAIN);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK &&
	      delivered == 1);
	CHECK(job.completions == 1 && job.operation == NULL);
	if (phase == 0) {
		unsigned char decoded[sizeof(text)];

		CHECK(!job.result.cancelled &&
		      job.outcome == TR_MESSAGE_TRANSFORM_OK);
		CHECK(tr_message_view(&job.output, &view) == TR_OK);
		CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data,
		                      view.len) == sizeof(text));
		CHECK(memcmp(decoded, text, sizeof(text)) == 0);
		CHECK(tr_message_clone(&job.output, &retained) == TR_OK);
		CHECK(tr_message_release(&job.output) == TR_OK);
		CHECK(atomic_load(&budget.current_bytes) > baseline);
	} else {
		CHECK(job.result.cancelled &&
		      job.outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
		CHECK(tr_message_view(&job.output, &view) == TR_ERR_STATE);
		CHECK(atomic_load(&budget.current_bytes) == baseline);
	}
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	if (phase == 0) {
		CHECK(atomic_load(&budget.current_bytes) != 0);
		CHECK(tr_message_release(&retained) == TR_OK);
	}
	CHECK(atomic_load(&budget.current_bytes) == 0);
	gate_destroy(&gate);
}

int main(void)
{
	for (unsigned phase = 0; phase < 3; ++phase)
		test_message_execution(phase);
	puts("task_message: ok");
	return 0;
}
