#ifndef TR_RUNTIME_MESSAGE_CASES_H
#define TR_RUNTIME_MESSAGE_CASES_H

#include "../src/runtime/runtime_message.h"
#include "../src/runtime/runtime_message_internal.h"
#include "../src/message/storage.h"

#include <sched.h>
#include <stdlib.h>
#include <zstd.h>

#define RT_CHECK(expr)                                                         \
	do {                                                                   \
		if (!(expr)) {                                                 \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #expr);                                        \
			abort();                                               \
		}                                                              \
	} while (0)

static uint64_t rt_message_now(void)
{
	struct timespec ts;

	RT_CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec;
}

static void rt_message_stop(struct tr_runtime *runtime)
{
	uint64_t end = rt_message_now() + 15;
	int ret;

	while ((ret = tr_runtime_stop(runtime)) == TR_AGAIN) {
		RT_CHECK(rt_message_now() <= end);
		sched_yield();
	}
	RT_CHECK(ret == TR_OK);
}

static struct tr_message_service_config rt_message_config(void)
{
	return (struct tr_message_service_config){
	        .tasks = {.workers = 1, .capacity = 2},
	        .dispatch_limit = 1,
	        .watchdog_ns = UINT64_C(10000000),
	};
}

static int rt_message_enable(void *arg)
{
	struct tr_message_service_config config = rt_message_config();

	return tr_runtime_shard_message_enable_on_owner(arg, &config);
}

static int rt_message_disable(void *arg)
{
	return tr_runtime_shard_message_disable_on_owner(arg);
}

static int rt_message_stats(void *arg)
{
	struct tr_message_service_stats stats;

	return tr_runtime_shard_message_stats_on_owner(arg, &stats);
}

struct rt_message_case {
	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_message output;
	struct tr_task_handle handle;
	_Atomic unsigned releases;
	unsigned completions;
};

static void rt_message_release(void *arg)
{
	struct rt_message_case *test = arg;

	atomic_fetch_add(&test->releases, 1);
}

static void rt_message_done(void *arg, struct tr_task_handle handle,
                            enum tr_message_transform_outcome outcome,
                            struct tr_message *message)
{
	struct rt_message_case *test = arg;
	struct tr_message_limits limits = {SIZE_MAX, SIZE_MAX};

	RT_CHECK(tr_reactor_is_current_owner(
	        tr_runtime_shard_reactor(test->shard)));
	RT_CHECK(tr_runtime_stop(test->runtime) == TR_ERR_STATE);
	RT_CHECK(outcome == TR_MESSAGE_TRANSFORM_OK);
	RT_CHECK(handle.instance == test->handle.instance &&
	         handle.slot == test->handle.slot &&
	         handle.generation == test->handle.generation);
	RT_CHECK(atomic_load(&test->releases) == 1);
	RT_CHECK(tr_message_none_move(message, &limits, &test->output) ==
	         TR_OK);
	++test->completions;
}

static int rt_message_submit(void *arg)
{
	struct rt_message_case *test = arg;
	struct tr_message_transform_config config = {
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {4096, 4096},
	        .max_workspace_bytes = (size_t)16 * 1024 * 1024,
	        .max_window_log = 20,
	        .compression_level = 3,
	};

	return tr_runtime_shard_message_submit_on_owner(
	        test->shard, &test->input, &config, rt_message_done, test,
	        &test->handle);
}

static int rt_message_old_cancel(void *arg)
{
	struct rt_message_case *test = arg;

	return tr_runtime_shard_message_cancel_on_owner(test->shard,
	                                                test->handle);
}

static void test_runtime_messages_owned_lifecycle(void)
{
	struct tr_runtime_shard_config shards[2] = {0};
	struct tr_runtime_config config = {2, shards};
	struct tr_runtime *runtime = NULL;
	struct rt_message_case tests[2] = {0};
	const uint8_t data[] = "runtime-owned-message-output";

	for (size_t i = 0; i < 2; ++i)
		shards[i].memory_budget_bytes = UINT64_C(512) * 1024 * 1024;
	RT_CHECK(tr_runtime_create(&config, &runtime) == TR_OK);
	RT_CHECK(tr_runtime_start(runtime) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		struct rt_message_case *test = &tests[i];
		struct tr_memory_budget *budget;

		test->runtime = runtime;
		test->shard = tr_runtime_shard_at(runtime, (uint32_t)i);
		atomic_init(&test->releases, 0);
		RT_CHECK(tr_runtime_shard_call(test->shard, rt_message_stats,
		                               test->shard) == TR_ERR_CLOSED);
		RT_CHECK(tr_runtime_shard_call(test->shard, rt_message_enable,
		                               test->shard) == TR_OK);
		RT_CHECK(tr_runtime_shard_call(test->shard, rt_message_enable,
		                               test->shard) == TR_ERR_STATE);
		budget = tr_runtime_shard_memory_budget(test->shard);
		RT_CHECK(tr_message_storage_reserve(&test->storage, budget,
		                                    sizeof(data)) == TR_OK);
		RT_CHECK(tr_message_storage_publish(&test->storage, data,
		                                    sizeof(data),
		                                    rt_message_release, test,
		                                    &test->input) == TR_OK);
		RT_CHECK(tr_runtime_shard_call(test->shard, rt_message_submit,
		                               test) == TR_OK);
	}
	/* 不手动关闭服务或轮询完成。Runtime 停止必须先交付，再停止父 owner。 */
	rt_message_stop(runtime);
	for (size_t i = 0; i < 2; ++i) {
		struct tr_message_view view;
		uint8_t decoded[sizeof(data)];

		RT_CHECK(tests[i].completions == 1);
		RT_CHECK(tr_message_view(&tests[i].output, &view) == TR_OK);
		RT_CHECK(ZSTD_decompress(decoded, sizeof(decoded), view.data,
		                         view.len) == sizeof(data));
		RT_CHECK(memcmp(decoded, data, sizeof(data)) == 0);
	}
	/* 正常 stop 后允许重新启动，但不会暗中复活旧服务/旧任务代次。 */
	RT_CHECK(tr_runtime_start(runtime) == TR_OK);
	for (size_t i = 0; i < 2; ++i) {
		struct tr_runtime_shard *shard = tests[i].shard;

		RT_CHECK(tr_runtime_shard_call(shard, rt_message_stats,
		                               shard) == TR_ERR_CLOSED);
		RT_CHECK(tr_runtime_shard_call(shard, rt_message_enable,
		                               shard) == TR_OK);
		RT_CHECK(tr_runtime_shard_call(shard, rt_message_old_cancel,
		                               &tests[i]) == TR_ERR_STALE);
	}
	rt_message_stop(runtime);
	/* 执行资源可先释放，但消息仍引用分片预算。销毁必须可重试而不能 UAF。 */
	RT_CHECK(tr_runtime_destroy(runtime) == TR_AGAIN);
	RT_CHECK(tr_runtime_start(runtime) == TR_ERR_STATE);
	for (size_t i = 0; i < 2; ++i) {
		struct tr_memory_budget_stats stats;
		struct tr_message_view view;

		tr_runtime_shard_memory_stats(tests[i].shard, &stats);
		RT_CHECK(stats.current_bytes != 0);
		RT_CHECK(tr_message_view(&tests[i].output, &view) == TR_OK);
		RT_CHECK(tr_message_release(&tests[i].output) == TR_OK);
	}
	/* 所有 release 已返回，才重试销毁；零计数不能代替并发释放静默。 */
	RT_CHECK(tr_runtime_destroy(runtime) == TR_OK);
	puts("runtime_message_owned_lifecycle: ok");
}

static void test_runtime_messages_budget_retry(void)
{
	struct tr_runtime_shard_config shard_config = {0};
	struct tr_runtime_config config = {1, &shard_config};
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct tr_memory_budget *budget;
	struct tr_memory_budget_stats stats;
	uint64_t reserve;
	uint64_t end;
	int ret;
	int seen = 0;

	shard_config.memory_budget_bytes = UINT64_C(512) * 1024 * 1024;
	RT_CHECK(tr_runtime_create(&config, &runtime) == TR_OK);
	RT_CHECK(tr_runtime_start(runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0);
	budget = tr_runtime_shard_memory_budget(shard);
	tr_runtime_shard_memory_stats(shard, &stats);
	reserve = stats.limit_bytes - stats.current_bytes;
	RT_CHECK(tr_memory_budget_reserve(budget, reserve) == TR_OK);
	RT_CHECK(tr_runtime_shard_call(shard, rt_message_enable, shard) ==
	         TR_AGAIN);
	RT_CHECK(tr_runtime_shard_call(shard, rt_message_stats, shard) ==
	         TR_ERR_CLOSED);
	RT_CHECK(tr_memory_budget_release(budget, reserve) == TR_OK);
	RT_CHECK(tr_runtime_shard_call(shard, rt_message_enable, shard) ==
	         TR_OK);
	end = rt_message_now() + 15;
	while ((ret = tr_runtime_shard_call(shard, rt_message_disable,
	                                    shard)) == TR_AGAIN) {
		RT_CHECK(rt_message_now() <= end);
		sched_yield();
	}
	RT_CHECK(ret == TR_OK);
	RT_CHECK(tr_runtime_shard_call(shard, rt_message_disable, shard) ==
	         TR_OK);
	RT_CHECK(tr_runtime_shard_call(shard, runtime_owner_ping, &seen) ==
	         TR_OK);
	RT_CHECK(seen == 1);
	RT_CHECK(tr_runtime_destroy(runtime) == TR_OK);
	puts("runtime_message_budget_retry: ok");
}

/* 在真实 Runtime/Reactor 上使用受控生命周期提供方，只验证父子拆除契约。
 * 不把这些可控返回值宣称为真实算法或父 Reactor 致命退出故障。
 */
struct rt_message_owner_probe {
	struct tr_reactor *reactor;
	struct tr_memory_budget *budget;
	int close_result;
	unsigned opens;
	unsigned closes;
};

static struct rt_message_owner_probe *rt_message_probe;

static int rt_probe_open(struct tr_reactor *reactor,
                         const struct tr_message_service_config *config,
                         struct tr_memory_budget *budget,
                         struct tr_message_service **out)
{
	(void)config;
	RT_CHECK(tr_reactor_is_current_owner(reactor));
	RT_CHECK(*out == NULL);
	rt_message_probe->reactor = reactor;
	rt_message_probe->budget = budget;
	RT_CHECK(tr_memory_budget_reserve(budget, 64) == TR_OK);
	++rt_message_probe->opens;
	*out = (struct tr_message_service *)rt_message_probe;
	/* 部分启动失败，但资源仍有长期所有者，不可遗失。 */
	return TR_ERR_SYS;
}

static int rt_probe_close(struct tr_message_service **object)
{
	struct rt_message_owner_probe *probe =
	        (struct rt_message_owner_probe *)*object;

	RT_CHECK(tr_reactor_is_current_owner(probe->reactor));
	++probe->closes;
	if (probe->close_result != TR_OK)
		return probe->close_result;
	RT_CHECK(tr_memory_budget_release(probe->budget, 64) == TR_OK);
	*object = NULL;
	return TR_OK;
}

static int rt_probe_enable(void *arg)
{
	static const struct tr_runtime_message_ops ops = {
	        .open_on_owner = rt_probe_open,
	        .close_on_owner = rt_probe_close,
	};
	struct tr_message_service_config config = rt_message_config();

	return tr_runtime_shard_message_open_owned_on_owner(arg, &config, &ops);
}

static void test_runtime_messages_retained_failure(void)
{
	struct tr_runtime_shard_config shard_config = {0};
	struct tr_runtime_config config = {1, &shard_config};
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct rt_message_owner_probe probe = {.close_result = TR_AGAIN};
	struct tr_memory_budget_stats before, after;
	int seen = 0;

	shard_config.memory_budget_bytes = UINT64_C(512) * 1024 * 1024;
	RT_CHECK(tr_runtime_create(&config, &runtime) == TR_OK);
	RT_CHECK(tr_runtime_start(runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0);
	rt_message_probe = &probe;
	tr_runtime_shard_memory_stats(shard, &before);
	RT_CHECK(tr_runtime_shard_call(shard, rt_probe_enable, shard) ==
	         TR_ERR_SYS);
	RT_CHECK(probe.opens == 1);
	RT_CHECK(tr_runtime_shard_call(shard, rt_probe_enable, shard) ==
	         TR_ERR_STATE);
	RT_CHECK(tr_runtime_stop(runtime) == TR_AGAIN);
	RT_CHECK(tr_runtime_shard_call(shard, runtime_owner_ping, &seen) ==
	         TR_OK);
	RT_CHECK(seen == 1 && probe.closes == 1);
	probe.close_result = TR_ERR_SYS;
	RT_CHECK(tr_runtime_destroy(runtime) == TR_ERR_SYS);
	RT_CHECK(probe.closes == 2);
	RT_CHECK(tr_runtime_start(runtime) == TR_ERR_STATE);
	tr_runtime_shard_memory_stats(shard, &after);
	RT_CHECK(after.current_bytes == before.current_bytes + 64);
	probe.close_result = TR_OK;
	RT_CHECK(tr_runtime_stop(runtime) == TR_OK);
	RT_CHECK(probe.closes == 3);
	RT_CHECK(tr_runtime_destroy(runtime) == TR_OK);
	rt_message_probe = NULL;
	puts("runtime_message_retained_failure: ok");
}

#undef RT_CHECK
#endif
