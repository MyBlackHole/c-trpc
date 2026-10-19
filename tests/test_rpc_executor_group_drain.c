#define _POSIX_C_SOURCE 200809L

#include "../src/facade_legacy_internal.h"
#include "../src/execution/task_executor.h"
#include "../src/execution/worker_context.h"
#include "../src/memory_budget.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/facade_tuning_internal.h"
#include "tr/trpc.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

struct drain_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_server *server;
	struct tr_task_executor *cleanup_probe;
	unsigned messages[256];
	int first_entered;
	int release_first;
	int group_join_entered;
	int destroy_done;
	int destroy_status;
};

static pthread_mutex_t join_watch_lock = PTHREAD_MUTEX_INITIALIZER;
static struct drain_context *join_watch_context;
static unsigned join_watch_count;
static int join_watch_enabled;

/* 测试内用条件变量冻结真实 pthread_create/join 边界。
 * 不改生产线程实现，不依赖 sleep 猜测生命周期状态。
 */
struct lifecycle_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int open;
};

static struct lifecycle_gate create_gate = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.cond = PTHREAD_COND_INITIALIZER
};
static struct lifecycle_gate stop_gate = {
	.lock = PTHREAD_MUTEX_INITIALIZER,
	.cond = PTHREAD_COND_INITIALIZER
};
static _Thread_local int hold_group_create;
static _Thread_local int hold_group_join;
static _Thread_local int inject_group_join_failure;
static _Thread_local unsigned group_join_attempts;

static void lifecycle_gate_enter(struct lifecycle_gate *gate)
{
	pthread_mutex_lock(&gate->lock);
	gate->entered = 1;
	pthread_cond_broadcast(&gate->cond);
	while (!gate->open)
		pthread_cond_wait(&gate->cond, &gate->lock);
	pthread_mutex_unlock(&gate->lock);
}

static void lifecycle_gate_wait(struct lifecycle_gate *gate)
{
	struct timespec due;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &due) == 0);
	due.tv_sec += 15;
	pthread_mutex_lock(&gate->lock);
	while (!gate->entered && ret == 0)
		ret = pthread_cond_timedwait(&gate->cond, &gate->lock, &due);
	assert(gate->entered);
	pthread_mutex_unlock(&gate->lock);
}

static void lifecycle_gate_release(struct lifecycle_gate *gate)
{
	pthread_mutex_lock(&gate->lock);
	assert(gate->entered && !gate->open);
	gate->open = 1;
	pthread_cond_broadcast(&gate->cond);
	pthread_mutex_unlock(&gate->lock);
}

typedef pthread_attr_t ThreadAttr;
typedef void *(*ThreadFn)(void *);

int __real_pthread_create(pthread_t *, const ThreadAttr *, ThreadFn, void *);

int __wrap_pthread_create(pthread_t *id, const ThreadAttr *a, ThreadFn fn,
			  void *arg)
{
	if (hold_group_create) {
		hold_group_create = 0;
		lifecycle_gate_enter(&create_gate);
	}
	return __real_pthread_create(id, a, fn, arg);
}

int __real_pthread_join(pthread_t thread, void **result);

int __wrap_pthread_join(pthread_t thread, void **result)
{
	struct drain_context *context = NULL;

	if (hold_group_join) {
		hold_group_join = 0;
		lifecycle_gate_enter(&stop_gate);
	}
	if (inject_group_join_failure && ++group_join_attempts == 2U)
		return EINVAL;
	pthread_mutex_lock(&join_watch_lock);
	if (join_watch_enabled && ++join_watch_count == 2U)
		context = join_watch_context;
	pthread_mutex_unlock(&join_watch_lock);

	if (context) {
		pthread_mutex_lock(&context->lock);
		context->group_join_entered = 1;
		pthread_cond_broadcast(&context->cond);
		pthread_mutex_unlock(&context->lock);
	}
	return __real_pthread_join(thread, result);
}

static void deadline_after(struct timespec *deadline, time_t seconds)
{
	assert(clock_gettime(CLOCK_REALTIME, deadline) == 0);
	deadline->tv_sec += seconds;
}

static void wait_for_flag(struct drain_context *context, int *flag)
{
	struct timespec deadline;
	int ret = 0;

	deadline_after(&deadline, 15);
	pthread_mutex_lock(&context->lock);
	while (!*flag && ret == 0)
		ret = pthread_cond_timedwait(&context->cond, &context->lock,
					     &deadline);
	assert(ret == 0);
	assert(*flag);
	pthread_mutex_unlock(&context->lock);
}

static void wait_for_executor_state(struct drain_context *context,
				   uint64_t queued, uint64_t running,
				   uint64_t ready)
{
	struct timespec pause = { 0, 1000000L };
	unsigned attempt;

	for (attempt = 0; attempt < 10000U; ++attempt) {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(context->server, &stats) == TR_OK);
		if (stats.rpc.executor_queued_tasks_current == queued &&
		    stats.rpc.executor_running_tasks_current == running &&
		    stats.rpc.executor_ready_calls_current == ready)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"RPC 执行器未达到预期的排空状态");
}

static void wait_for_cancelled_executor_state(
	struct drain_context *context, uint64_t queued, uint64_t running,
	uint64_t ready)
{
	struct timespec pause = { 0, 1000000L };
	unsigned attempt;
	struct tr_server_stats last_stats;

	memset(&last_stats, 0, sizeof(last_stats));

	for (attempt = 0; attempt < 10000U; ++attempt) {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(context->server, &stats) == TR_OK);
		last_stats = stats;
		if (stats.rpc.calls_completed == 1U &&
		    stats.rpc.executor_queued_tasks_current == queued &&
		    stats.rpc.executor_running_tasks_current == running &&
		    stats.rpc.executor_ready_calls_current == ready)
			return;
		(void)nanosleep(&pause, NULL);
	}
	fprintf(stderr,
		"取消排空状态: completed=%llu queued=%llu running=%llu ready=%llu\n",
		(unsigned long long)last_stats.rpc.calls_completed,
		(unsigned long long)last_stats.rpc.executor_queued_tasks_current,
		(unsigned long long)last_stats.rpc.executor_running_tasks_current,
		(unsigned long long)last_stats.rpc.executor_ready_calls_current);
	assert(!"RPC 执行器未发布预期的取消任务状态");
}

static enum tr_rpc_message_disposition
server_message(struct tr_rpc_call_handle call,
	       const struct tr_rpc_message *message, void *arg)
{
	struct drain_context *context = (struct drain_context *)arg;
	unsigned char tag;
	(void)call;

	assert(tr_execution_in_worker_context());
	/* 真实 V1 业务线程不能接管通用执行器，拒绝必须发生在改写 owner 前。 */
	assert(tr_task_executor_adopt_cleanup(context->cleanup_probe) ==
	       TR_ERR_STATE);
	assert(message != NULL);
	assert(message->bytes.data != NULL);
	assert(message->bytes.len == 1U);
	tag = message->bytes.data[0];

	pthread_mutex_lock(&context->lock);
	context->messages[tag]++;
	if (tag == (unsigned char)'a') {
		context->first_entered = 1;
		pthread_cond_broadcast(&context->cond);
		while (!context->release_first)
			pthread_cond_wait(&context->cond, &context->lock);
	}
	pthread_cond_broadcast(&context->cond);
	pthread_mutex_unlock(&context->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void *destroy_server(void *arg)
{
	struct drain_context *context = (struct drain_context *)arg;
	int status = tr_server_destroy(context->server);

	pthread_mutex_lock(&context->lock);
	context->destroy_status = status;
	context->destroy_done = 1;
	pthread_cond_broadcast(&context->cond);
	pthread_mutex_unlock(&context->lock);
	return NULL;
}

static void send_message(struct tr_rpc_call_handle call, unsigned char tag)
{
	struct tr_rpc_bytes bytes = { &tag, 1U };
	struct timespec pause = { 0, 10000000L };
	unsigned attempt;
	int ret = TR_AGAIN;

	for (attempt = 0; attempt < 500U; ++attempt) {
		ret = tr_rpc_call_send(call, &bytes);
		if (ret == TR_OK)
			return;
		assert(ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	assert(ret == TR_OK);
}

static void test_server_destroy_drains_accepted_group_tasks(void)
{
	struct drain_context context;
	struct tr_memory_budget budget;
	struct tr_task_executor_config task_config = {.workers = 1,
	                                              .capacity = 1};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_rpc_call_handle first_call;
	struct tr_rpc_call_handle second_call;
	pthread_t destroy_thread;
	struct timespec deadline;
	int ret = 0;
	uint16_t port = 0U;

	memset(&context, 0, sizeof(context));
	assert(pthread_mutex_init(&context.lock, NULL) == 0);
	assert(pthread_cond_init(&context.cond, NULL) == 0);
	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_task_executor_create(&task_config, &budget,
	                               &context.cleanup_probe) == TR_OK);
	assert(tr_task_executor_stop(context.cleanup_probe, TR_TASK_DRAIN) ==
	       TR_OK);

	tr_server_config_init(&server_config);
	server_config.max_peers = 2U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 1U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);
	context.server = server;

	memset(&method, 0, sizeof(method));
	method.service_id = 91U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_MANY;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 16U;
	method.max_response_bytes = 16U;
	memset(&handlers, 0, sizeof(handlers));
	handlers.on_message = server_message;
	assert(tr_server_legacy_register_stream_method(
	               server, &method, &handlers, &context) == TR_OK);
	ret = tr_server_listen(server, "127.0.0.1", 0U, &port);
	if (ret != TR_OK)
		fprintf(stderr, "监听初始化失败: status=%d\n", ret);
	assert(ret == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	assert(tr_client_legacy_create(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_wait_ready(client, 5000U) == TR_OK);
	assert(tr_client_legacy_register_method(client, &method) == TR_OK);
	assert(tr_client_legacy_call_start(client, 91U, 1U, NULL,
	                                   &first_call) == TR_OK);
	send_message(first_call, (unsigned char)'a');
	wait_for_flag(&context, &context.first_entered);

	/* 同一调用的第二项任务留在端点队列中。 */
	send_message(first_call, (unsigned char)'b');
	wait_for_executor_state(&context, 1U, 1U, 0U);
	assert(tr_rpc_call_cancel(first_call) == TR_OK);
	wait_for_cancelled_executor_state(&context, 2U, 1U, 0U);

	/* 另一条调用就绪，用于覆盖端点中多个就绪调用的重调度。 */
	assert(tr_client_legacy_call_start(client, 91U, 1U, NULL,
	                                   &second_call) == TR_OK);
	send_message(second_call, (unsigned char)'c');
	wait_for_executor_state(&context, 3U, 1U, 1U);

	/* 单分片停机依次等待反应器所有者线程和 RPC 执行组工作线程。 */
	pthread_mutex_lock(&join_watch_lock);
	join_watch_context = &context;
	join_watch_count = 0U;
	join_watch_enabled = 1;
	pthread_mutex_unlock(&join_watch_lock);
	assert(pthread_create(&destroy_thread, NULL, destroy_server, &context) == 0);
	wait_for_flag(&context, &context.group_join_entered);

	pthread_mutex_lock(&context.lock);
	context.release_first = 1;
	pthread_cond_broadcast(&context.cond);
	pthread_mutex_unlock(&context.lock);

	deadline_after(&deadline, 15);
	pthread_mutex_lock(&context.lock);
	while (!context.destroy_done && ret == 0)
		ret = pthread_cond_timedwait(&context.cond, &context.lock,
					     &deadline);
	assert(ret == 0);
	assert(context.destroy_done);
	assert(context.destroy_status == TR_OK);
	assert(context.messages[(unsigned char)'a'] == 1U);
	assert(context.messages[(unsigned char)'b'] == 0U);
	assert(context.messages[(unsigned char)'c'] == 1U);
	pthread_mutex_unlock(&context.lock);

	pthread_mutex_lock(&join_watch_lock);
	join_watch_enabled = 0;
	join_watch_context = NULL;
	pthread_mutex_unlock(&join_watch_lock);
	assert(pthread_join(destroy_thread, NULL) == 0);
	server = NULL;
	context.server = NULL;
	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;
	assert(tr_task_executor_is_current_owner(context.cleanup_probe));
	assert(tr_task_executor_join(context.cleanup_probe) == TR_OK);
	assert(tr_task_executor_destroy(&context.cleanup_probe) == TR_OK);
	assert(atomic_load(&budget.current_bytes) == 0);
	pthread_cond_destroy(&context.cond);
	pthread_mutex_destroy(&context.lock);
}

struct worker_probe {
	struct tr_rpc_executor_group *legacy;
	unsigned done;
};

static int reject_legacy_lifecycle(void *arg,
                                   const struct tr_task_cancel *cancel)
{
	struct worker_probe *probe = arg;

	(void)cancel;
	assert(tr_execution_in_worker_context());
	/* 使用真实通用执行器线程，不伪造 TLS，也不进入可能自等待的旧路径。 */
	assert(tr_rpc_executor_group_can_start(probe->legacy) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_start(probe->legacy) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_stop(probe->legacy) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_destroy_checked(probe->legacy) ==
	       TR_ERR_STATE);
	return TR_OK;
}

static void probe_done(void *arg, struct tr_task_handle handle,
                       struct tr_task_result result)
{
	struct worker_probe *probe = arg;

	assert(!tr_execution_in_worker_context());
	assert(handle.generation != 0 && !result.cancelled);
	assert(result.work_status == TR_OK);
	++probe->done;
}

static void test_generic_worker_rejects_legacy_lifecycle(void)
{
	struct worker_probe probe = {0};
	struct tr_memory_budget budget;
	struct tr_task_executor *executor = NULL;
	struct tr_task_executor_config config = {.workers = 1, .capacity = 1};
	struct tr_task_spec spec = {reject_legacy_lifecycle, probe_done,
	                            &probe};
	struct tr_task_handle handle;
	struct timespec deadline, now, pause = {0, 1000000};
	int ret;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_rpc_executor_group_create(1, 1, 1, &probe.legacy) == TR_OK);
	assert(tr_task_executor_create(&config, &budget, &executor) == TR_OK);
	assert(tr_task_executor_start(executor) == TR_OK);
	assert(tr_task_executor_submit(executor, &spec, &handle) == TR_OK);
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	while (!probe.done) {
		size_t count;
		assert(tr_task_executor_dispatch(executor, 1, &count) == TR_OK);
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		assert(now.tv_sec < deadline.tv_sec ||
		       (now.tv_sec == deadline.tv_sec &&
		        now.tv_nsec < deadline.tv_nsec));
		(void)nanosleep(&pause, NULL);
	}
	assert(probe.done == 1);
	assert(tr_task_executor_stop(executor, TR_TASK_DRAIN) == TR_OK);
	while ((ret = tr_task_executor_join(executor)) == TR_AGAIN) {
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		assert(now.tv_sec < deadline.tv_sec ||
		       (now.tv_sec == deadline.tv_sec &&
		        now.tv_nsec < deadline.tv_nsec));
		(void)nanosleep(&pause, NULL);
	}
	assert(ret == TR_OK);
	assert(tr_task_executor_destroy(&executor) == TR_OK);
	assert(atomic_load(&budget.current_bytes) == 0);
	/* 工作线程的拒绝没有改变旧组状态；外部控制线程仍能启动和完整回收。 */
	assert(tr_rpc_executor_group_can_start(probe.legacy) == TR_OK);
	assert(tr_rpc_executor_group_start(probe.legacy) == TR_OK);
	assert(tr_rpc_executor_group_stop(probe.legacy) == TR_OK);
	assert(tr_rpc_executor_group_destroy_checked(probe.legacy) == TR_OK);
}

/* 两个控制调用都可能在释放组锁后操作 pthread_t 数组。
 * 必须验证另一控制线程不能并发创建、join 或释放该数组。
 */
struct group_operation {
	struct tr_rpc_executor_group *group;
	int result;
};

static void *group_start_thread(void *arg)
{
	struct group_operation *op = arg;

	hold_group_create = 1;
	op->result = tr_rpc_executor_group_start(op->group);
	return NULL;
}

static void *group_stop_thread(void *arg)
{
	struct group_operation *op = arg;

	hold_group_join = 1;
	op->result = tr_rpc_executor_group_stop(op->group);
	return NULL;
}

static void assert_group_busy(struct tr_rpc_executor_group *group)
{
	assert(tr_rpc_executor_group_can_start(group) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_start(group) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_stop(group) == TR_ERR_STATE);
	assert(tr_rpc_executor_group_destroy_checked(group) == TR_ERR_STATE);
}

static void test_group_lifecycle_owners_are_serialized(void)
{
	struct tr_rpc_executor_group *group = NULL;
	struct group_operation op;
	pthread_t controller;

	assert(tr_rpc_executor_group_create(1U, 2U, 2U, &group) == TR_OK);
	assert(tr_rpc_executor_group_can_start(group) == TR_OK);
	op.group = group;
	op.result = TR_ERR_STATE;
	assert(pthread_create(&controller, NULL, group_start_thread, &op) == 0);
	lifecycle_gate_wait(&create_gate);
	/* 第一条线程尚未创建，绝不允许并发启动、停止或销毁。 */
	assert_group_busy(group);
	lifecycle_gate_release(&create_gate);
	assert(pthread_join(controller, NULL) == 0);
	assert(op.result == TR_OK);
	assert(tr_rpc_executor_group_can_start(group) == TR_ERR_STATE);

	op.result = TR_ERR_STATE;
	assert(pthread_create(&controller, NULL, group_stop_thread, &op) == 0);
	lifecycle_gate_wait(&stop_gate);
	/* stop 已发布停止门禁，但尚未回收 pthread_t。 */
	assert_group_busy(group);
	lifecycle_gate_release(&stop_gate);
	assert(pthread_join(controller, NULL) == 0);
	assert(op.result == TR_OK);
	assert(tr_rpc_executor_group_can_start(group) == TR_OK);

	/* 控制门禁完全解除，下一工作线程轮次仍可启动并收敛。 */
	assert(tr_rpc_executor_group_start(group) == TR_OK);
	assert(tr_rpc_executor_group_stop(group) == TR_OK);
	assert(tr_rpc_executor_group_destroy_checked(group) == TR_OK);
}

static void test_group_join_failure_keeps_retry_owner(void)
{
	struct tr_rpc_executor_group *group = NULL;

	assert(tr_rpc_executor_group_create(1U, 2U, 2U, &group) == TR_OK);
	assert(tr_rpc_executor_group_start(group) == TR_OK);
	inject_group_join_failure = 1;
	group_join_attempts = 0U;
	/* 第一线程 join 成功，第二线程故意拒绝回收。 */
	assert(tr_rpc_executor_group_stop(group) == TR_ERR_SYS);
	assert(group_join_attempts == 2U);
	assert(tr_rpc_executor_group_can_start(group) == TR_ERR_STATE);
	/* 第二次从精确的未回收线程继续，不重复 join 第一条。 */
	assert(tr_rpc_executor_group_stop(group) == TR_OK);
	assert(group_join_attempts == 3U);
	inject_group_join_failure = 0;
	assert(tr_rpc_executor_group_can_start(group) == TR_OK);
	assert(tr_rpc_executor_group_start(group) == TR_OK);
	assert(tr_rpc_executor_group_stop(group) == TR_OK);
	assert(tr_rpc_executor_group_destroy_checked(group) == TR_OK);
}

int main(void)
{
	test_group_lifecycle_owners_are_serialized();
	test_group_join_failure_keeps_retry_owner();
	test_generic_worker_rejects_legacy_lifecycle();
	test_server_destroy_drains_accepted_group_tasks();
	return 0;
}
