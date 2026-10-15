#define _POSIX_C_SOURCE 200809L
#include "../src/runtime/runtime_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/message/storage.h"
#include "tr/status.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <zstd.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define MIB ((size_t)1024 * 1024)

static const struct tr_message_transform_config transform = {
        .kind = TR_MESSAGE_ZSTD_ENCODE,
        .limits = {2 * MIB, 2 * MIB},
        .max_workspace_bytes = 16 * MIB,
        .max_window_log = 20,
        .compression_level = 3};

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered;
	bool open;
};

static struct gate algorithm_gate;
static _Atomic bool hold_algorithm;
static _Thread_local bool forbid_sync_call;
static _Thread_local unsigned forbidden_calls;

/* 只捕获本应在同步切换之前被拒绝的路径；合法路径始终调用真实 Reactor。
 * 错误实现会在真正制造跨所有者死锁之前被计数断言拒绝。
 */
int __real_tr_reactor_call(struct tr_reactor *, int (*)(void *), void *);
int __wrap_tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *),
                           void *arg)
{
	if (forbid_sync_call) {
		++forbidden_calls;
		return TR_ERR_STATE;
	}
	return __real_tr_reactor_call(reactor, fn, arg);
}

static struct timespec deadline(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	ts.tv_sec += 15;
	return ts;
}

static void check_time(struct timespec end)
{
	struct timespec now;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	CHECK(now.tv_sec < end.tv_sec ||
	      (now.tv_sec == end.tv_sec && now.tv_nsec < end.tv_nsec));
}

static void gate_init(void)
{
	pthread_condattr_t attr;
	algorithm_gate = (struct gate){0};
	CHECK(pthread_mutex_init(&algorithm_gate.lock, NULL) == 0);
	CHECK(pthread_condattr_init(&attr) == 0);
	CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	CHECK(pthread_cond_init(&algorithm_gate.cond, &attr) == 0);
	CHECK(pthread_condattr_destroy(&attr) == 0);
}

static void gate_wait(bool worker)
{
	struct timespec end = deadline();
	CHECK(pthread_mutex_lock(&algorithm_gate.lock) == 0);
	if (worker) {
		algorithm_gate.entered = true;
		CHECK(pthread_cond_broadcast(&algorithm_gate.cond) == 0);
	}
	while (worker ? !algorithm_gate.open : !algorithm_gate.entered)
		CHECK(pthread_cond_timedwait(&algorithm_gate.cond,
		                             &algorithm_gate.lock, &end) == 0);
	CHECK(pthread_mutex_unlock(&algorithm_gate.lock) == 0);
}

size_t __real_ZSTD_compressStream2(ZSTD_CCtx *, ZSTD_outBuffer *,
                                   ZSTD_inBuffer *, ZSTD_EndDirective);
size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *ctx, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(ctx, out, in, end);
	if (atomic_exchange(&hold_algorithm, false))
		gate_wait(true);
	return ret;
}

struct fixture {
	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard[2];
};

static void fixture_init(struct fixture *f, bool enabled)
{
	struct tr_runtime_shard_config shards[2] = {0};
	struct tr_runtime_config config = {2, shards};
	*f = (struct fixture){0};
	for (size_t i = 0; i < 2; ++i) {
		shards[i].memory_budget_bytes = 128 * MIB;
		shards[i].reactor.max_connections = 4;
		shards[i].reactor.rx_buffer_count = 4;
		shards[i].reactor.rx_buffer_size = 4096;
		shards[i].reactor.max_payload_len = 4096;
		shards[i].reactor.tx_item_capacity = 8;
		shards[i].reactor.control_tx_item_capacity = 8;
		if (enabled)
			shards[i].messages = (struct tr_message_service_config){
			        {1, 2}, 1, 10000000};
	}
	CHECK(tr_runtime_create(&config, &f->runtime) == TR_OK);
	for (size_t i = 0; i < 2; ++i)
		f->shard[i] = tr_runtime_shard_at(f->runtime, (uint32_t)i);
}

struct sample {
	struct tr_runtime_shard *shard;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_message retained;
	struct tr_task_handle handle;
	_Atomic unsigned releases;
	_Atomic unsigned completions;
	bool cancelled;
};

static void release_input(void *arg)
{
	struct sample *s = arg;
	CHECK(atomic_fetch_add(&s->releases, 1) == 0);
}

static void sample_init(struct sample *s, struct tr_runtime_shard *shard,
                        const uint8_t *data, size_t len, bool cancelled)
{
	*s = (struct sample){.shard = shard, .cancelled = cancelled};
	atomic_init(&s->releases, 0);
	atomic_init(&s->completions, 0);
	CHECK(tr_message_storage_reserve(&s->storage,
	                                 tr_runtime_shard_memory_budget(shard),
	                                 len) == TR_OK);
	CHECK(tr_message_storage_publish(&s->storage, data, len, release_input,
	                                 s, &s->input) == TR_OK);
}

static void done(void *arg, struct tr_task_handle handle,
                 struct tr_message_service_result result,
                 struct tr_message *message)
{
	struct sample *s = arg;
	struct tr_message_service_stats stats;
	struct tr_message_view view;
	CHECK(tr_reactor_in_owner_context_for(
	        tr_runtime_shard_reactor(s->shard)));
	CHECK(result.delivery == TR_MESSAGE_SERVICE_ON_OWNER);
	CHECK(atomic_load(&s->releases) == 1);
	CHECK(result.outcome == (s->cancelled ? TR_MESSAGE_TRANSFORM_CANCELLED
	                                      : TR_MESSAGE_TRANSFORM_OK));
	if (s->cancelled)
		CHECK(tr_message_view(message, &view) == TR_ERR_STATE);
	else
		CHECK(tr_message_clone(message, &s->retained) == TR_OK);
	/* 同一所有者的回调内快照/取消直接执行，不会同步等待自己；
	 * 当前终局不能被取消改写，容量在回调返回之前仍由本任务持有。
	 */
	CHECK(tr_runtime_shard_message_get_stats(s->shard, &stats) == TR_OK);
	CHECK(stats.tasks.delivering == 1);
	CHECK(stats.tasks.outstanding >= 1 && stats.held_jobs >= 1);
	CHECK(tr_runtime_shard_message_cancel(s->shard, handle) ==
	      TR_ERR_STATE);
	CHECK(atomic_fetch_add(&s->completions, 1) == 0);
}

static int submit(struct sample *s)
{
	return tr_runtime_shard_message_submit(s->shard, &s->input, &transform,
	                                       done, s, &s->handle);
}

static int submit_on_owner(void *arg)
{
	return submit(arg);
}

static void wait_done(struct sample *s)
{
	struct timespec end = deadline();
	while (!atomic_load(&s->completions)) {
		check_time(end);
		sched_yield();
	}
}

/* 整份输出只在成功后改变；失败不偷偷消费原消息或制造完成责任。 */
static void rejected_operations(struct sample *s, int expected)
{
	struct tr_message_service_stats stats;
	unsigned char saved[sizeof(stats)];
	struct tr_task_handle sentinel = {UINT64_MAX, UINT64_MAX, UINT32_MAX};
	struct tr_task_handle handle = sentinel;
	struct tr_message_view before, after;
	CHECK(tr_message_view(&s->input, &before) == TR_OK);
	memset(&stats, 0xa5, sizeof(stats));
	memcpy(saved, &stats, sizeof(stats));
	CHECK(tr_runtime_shard_message_submit(s->shard, &s->input, &transform,
	                                      done, s, &handle) == expected);
	CHECK(handle.instance == sentinel.instance &&
	      handle.generation == sentinel.generation &&
	      handle.slot == sentinel.slot);
	CHECK(tr_runtime_shard_message_cancel(s->shard, sentinel) == expected);
	CHECK(tr_runtime_shard_message_get_stats(s->shard, &stats) == expected);
	CHECK(memcmp(saved, &stats, sizeof(stats)) == 0);
	CHECK(tr_message_view(&s->input, &after) == TR_OK);
	CHECK(before.data == after.data && before.len == after.len);
	CHECK(atomic_load(&s->releases) == 0);
	CHECK(atomic_load(&s->completions) == 0);
}

static int forbidden_context(void *arg)
{
	forbidden_calls = 0;
	forbid_sync_call = true;
	rejected_operations(arg, TR_ERR_STATE);
	forbid_sync_call = false;
	CHECK(forbidden_calls == 0);
	return TR_OK;
}

static int worker_probe(void *arg, const struct tr_task_cancel *cancel)
{
	(void)cancel;
	CHECK(tr_task_in_worker_context());
	return forbidden_context(arg);
}

static void worker_done(void *arg, struct tr_task_handle handle,
                        struct tr_task_result result)
{
	(void)arg;
	(void)handle;
	CHECK(result.work_status == TR_OK && !result.cancelled);
}

static void check_worker_context(struct sample *s)
{
	struct tr_memory_budget budget;
	struct tr_task_executor_config config = {1, 1};
	struct tr_task_executor *executor = NULL;
	struct tr_task_spec task = {worker_probe, worker_done, s};
	struct tr_task_handle handle;
	struct timespec end = deadline();
	size_t delivered = 0;
	int ret;
	tr_memory_budget_init(&budget, MIB);
	CHECK(tr_task_executor_create(&config, &budget, &executor) == TR_OK);
	CHECK(tr_task_executor_start(executor) == TR_OK);
	CHECK(tr_task_executor_submit(executor, &task, &handle) == TR_OK);
	CHECK(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	while ((ret = tr_task_executor_join(executor)) == TR_AGAIN) {
		check_time(end);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	CHECK(tr_task_executor_dispatch(executor, 1, &delivered) == TR_OK);
	CHECK(delivered == 1);
	CHECK(tr_task_executor_destroy(&executor) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
}

static void stop_runtime(struct tr_runtime *runtime)
{
	struct timespec end = deadline();
	int ret;
	while ((ret = tr_runtime_stop(runtime)) == TR_AGAIN) {
		check_time(end);
		sched_yield();
	}
	CHECK(ret == TR_OK);
}

static void test_disabled_and_unstarted(void)
{
	struct fixture f;
	struct sample s;
	uint8_t data = 17;
	struct tr_message_service_stats stats;
	struct tr_task_handle handle = {0};
	CHECK(tr_runtime_shard_message_cancel(NULL, handle) == TR_ERR_INVALID);
	CHECK(tr_runtime_shard_message_get_stats(NULL, &stats) ==
	      TR_ERR_INVALID);
	for (unsigned enabled = 0; enabled < 2; ++enabled) {
		fixture_init(&f, enabled != 0);
		sample_init(&s, f.shard[0], &data, 1, false);
		rejected_operations(&s, enabled ? TR_ERR_CLOSED
		                                : TR_ERR_UNSUPPORTED);
		CHECK(tr_runtime_shard_message_get_stats(f.shard[0], NULL) ==
		      TR_ERR_INVALID);
		CHECK(tr_message_release(&s.input) == TR_OK);
		CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	}
}

static void test_cancel_snapshot_and_restart(void)
{
	struct fixture f;
	struct sample running, queued, retry, probe;
	struct tr_message_service_stats stats;
	struct tr_memory_budget_stats before, after;
	struct tr_task_handle old;
	struct tr_message_view view;
	uint8_t *data = calloc(1, MIB);
	uint8_t decoded[1];
	CHECK(data);
	fixture_init(&f, true);
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	gate_init();
	sample_init(&running, f.shard[0], data, MIB, true);
	sample_init(&queued, f.shard[0], data, 1, true);
	sample_init(&retry, f.shard[0], data, 1, false);
	sample_init(&probe, f.shard[0], data, 1, false);
	atomic_store(&hold_algorithm, true);
	CHECK(submit(&running) == TR_OK);
	gate_wait(false);
	CHECK(submit(&queued) == TR_OK);
	CHECK(tr_runtime_shard_message_get_stats(f.shard[0], &stats) == TR_OK);
	CHECK(stats.accepting && stats.event_attached && stats.timer_attached);
	CHECK(stats.tasks.capacity == 2 && stats.tasks.outstanding == 2);
	CHECK(stats.tasks.running == 1 && stats.tasks.queued == 1);
	CHECK(stats.tasks.completed == 0 && stats.tasks.delivering == 0);
	CHECK(stats.held_jobs == 2);
	tr_runtime_shard_memory_stats(f.shard[0], &before);
	CHECK(submit(&retry) == TR_AGAIN);
	CHECK(!retry.handle.instance && !retry.handle.generation);
	CHECK(tr_message_view(&retry.input, &view) == TR_OK);
	CHECK(view.data == data && view.len == 1);
	tr_runtime_shard_memory_stats(f.shard[0], &after);
	CHECK(before.current_bytes == after.current_bytes);
	/* 来自其他分片的真实能力不能取消此处运行中的任务。 */
	CHECK(tr_runtime_shard_message_cancel(f.shard[1], running.handle) ==
	      TR_ERR_STALE);
	CHECK(tr_runtime_shard_call(f.shard[1], forbidden_context, &probe) ==
	      TR_OK);
	check_worker_context(&probe);
	CHECK(tr_message_release(&probe.input) == TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], running.handle) ==
	      TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], running.handle) ==
	      TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], queued.handle) ==
	      TR_OK);
	CHECK(!atomic_load(&running.completions) &&
	      !atomic_load(&queued.completions));
	CHECK(!atomic_load(&running.releases) &&
	      !atomic_load(&queued.releases));
	CHECK(tr_runtime_shard_message_get_stats(f.shard[0], &stats) == TR_OK);
	CHECK(stats.tasks.outstanding == 2 && stats.held_jobs == 2);
	CHECK(pthread_mutex_lock(&algorithm_gate.lock) == 0);
	algorithm_gate.open = true;
	CHECK(pthread_cond_broadcast(&algorithm_gate.cond) == 0);
	CHECK(pthread_mutex_unlock(&algorithm_gate.lock) == 0);
	wait_done(&running);
	wait_done(&queued);
	CHECK(tr_runtime_shard_message_get_stats(f.shard[0], &stats) == TR_OK);
	CHECK(stats.tasks.outstanding == 0 && stats.held_jobs == 0);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], running.handle) ==
	      TR_ERR_STALE);
	/* 原满载拒绝的消息可以在同一 owner 中重试；再次固定工作者，
	 * 证明同一个槽位的新代次不会被旧能力取消，而非只检查空槽位。 */
	CHECK(pthread_mutex_lock(&algorithm_gate.lock) == 0);
	algorithm_gate.entered = false;
	algorithm_gate.open = false;
	CHECK(pthread_mutex_unlock(&algorithm_gate.lock) == 0);
	atomic_store(&hold_algorithm, true);
	CHECK(tr_runtime_shard_call(f.shard[0], submit_on_owner, &retry) ==
	      TR_OK);
	gate_wait(false);
	old = retry.handle.slot == running.handle.slot ? running.handle
	                                               : queued.handle;
	CHECK(old.slot == retry.handle.slot &&
	      old.instance == retry.handle.instance);
	CHECK(old.generation != retry.handle.generation);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], old) == TR_ERR_STALE);
	CHECK(pthread_mutex_lock(&algorithm_gate.lock) == 0);
	algorithm_gate.open = true;
	CHECK(pthread_cond_broadcast(&algorithm_gate.cond) == 0);
	CHECK(pthread_mutex_unlock(&algorithm_gate.lock) == 0);
	wait_done(&retry);
	CHECK(tr_message_view(&retry.retained, &view) == TR_OK);
	CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data, view.len) ==
	      1);
	CHECK(decoded[0] == data[0]);
	old = running.handle;
	stop_runtime(f.runtime);
	/* 使用独立的输入责任验证停止拒绝；拒绝后马上归还，不混淆保留输出测试。
	 */
	sample_init(&probe, f.shard[0], data, 1, false);
	rejected_operations(&probe, TR_ERR_CLOSED);
	CHECK(tr_message_release(&probe.input) == TR_OK);
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	CHECK(tr_runtime_shard_message_get_stats(f.shard[0], &stats) == TR_OK);
	CHECK(stats.accepting && stats.tasks.outstanding == 0);
	CHECK(tr_runtime_shard_message_cancel(f.shard[0], old) == TR_ERR_STALE);
	stop_runtime(f.runtime);
	CHECK(tr_runtime_destroy(f.runtime) == TR_AGAIN);
	CHECK(!tr_runtime_shard_reactor(f.shard[0]));
	CHECK(!tr_runtime_shard_reactor(f.shard[1]));
	CHECK(tr_runtime_start(f.runtime) == TR_ERR_STATE);
	/* 仅预算外壳仍存活时，三个操作一致返回 CLOSED，而不是空 Reactor
	 * INVALID。 */
	sample_init(&probe, f.shard[0], data, 1, false);
	rejected_operations(&probe, TR_ERR_CLOSED);
	CHECK(tr_message_release(&probe.input) == TR_OK);
	CHECK(tr_message_view(&retry.retained, &view) == TR_OK);
	CHECK(tr_message_release(&retry.retained) == TR_OK);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	CHECK(atomic_load(&running.completions) == 1);
	CHECK(atomic_load(&queued.completions) == 1);
	CHECK(atomic_load(&retry.completions) == 1);
	CHECK(pthread_cond_destroy(&algorithm_gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&algorithm_gate.lock) == 0);
	free(data);
}

int main(void)
{
	test_disabled_and_unstarted();
	test_cancel_snapshot_and_restart();
	puts("runtime_message_api: ok");
	return 0;
}
