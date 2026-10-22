#define _POSIX_C_SOURCE 200809L

#include "../src/execution/task_executor.h"
#include "../src/execution/worker_context.h"
#include "../src/memory_budget.h"
#include "../src/rpc/rpc_internal.h"
#include "tr/trpc.h"
#include "tr/rpc_v2.h"

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
	int work_entered;
	int release_work;
	unsigned server_terminals;
	enum tr_rpc_status server_status;
	unsigned destroy_attempts;
	int destroy_done;
	int destroy_status;
};

/* 测试内用条件变量冻结真实 pthread_create/join 边界。
 * 不改生产线程实现，不依赖 sleep 猜测生命周期状态。
 */
struct lifecycle_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int open;
};

static struct lifecycle_gate create_gate, stop_gate;

static void lifecycle_gate_init(struct lifecycle_gate *gate)
{
	assert(pthread_mutex_init(&gate->lock, NULL) == 0);
	assert(pthread_cond_init(&gate->cond, NULL) == 0);
}

static void lifecycle_gate_destroy(struct lifecycle_gate *gate)
{
	assert(pthread_cond_destroy(&gate->cond) == 0);
	assert(pthread_mutex_destroy(&gate->lock) == 0);
}
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

typedef pthread_t Tid;
typedef pthread_attr_t Attr;
typedef void *(*Entry)(void *);

int __real_pthread_create(Tid *, const Attr *, Entry, void *);

int __wrap_pthread_create(Tid *id, const Attr *a, Entry fn, void *arg)
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
	if (hold_group_join) {
		hold_group_join = 0;
		lifecycle_gate_enter(&stop_gate);
	}
	if (inject_group_join_failure && ++group_join_attempts == 2U)
		return EINVAL;
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

static void wait_for_destroy_attempt(struct drain_context *context)
{
	struct timespec deadline;
	int ret = 0;

	deadline_after(&deadline, 15);
	pthread_mutex_lock(&context->lock);
	while (!context->destroy_attempts && ret == 0)
		ret = pthread_cond_timedwait(&context->cond, &context->lock,
		                             &deadline);
	assert(ret == 0);
	assert(context->destroy_attempts != 0U);
	assert(context->destroy_status == TR_AGAIN);
	pthread_mutex_unlock(&context->lock);
}

static enum tr_rpc_status server_work(struct tr_rpc_v2_call_id call,
                                      const struct tr_rpc_v2_request *request,
                                      const struct tr_rpc_v2_cancel *cancel,
                                      struct tr_rpc_v2_reply *reply, void *arg)
{
	struct drain_context *context = (struct drain_context *)arg;
	(void)call;

	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		pthread_mutex_lock(&context->lock);
		context->work_entered = 1;
		pthread_cond_broadcast(&context->cond);
		while (!context->release_work)
			pthread_cond_wait(&context->cond, &context->lock);
		pthread_mutex_unlock(&context->lock);
		return tr_rpc_v2_cancel_requested(cancel) == 1
		               ? TR_RPC_STATUS_CANCELLED
		               : TR_RPC_STATUS_OK;
	}
	if (request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		struct tr_rpc_bytes empty = {NULL, 0U};
		assert(tr_rpc_v2_reply_append(reply, &empty) == TR_OK);
		assert(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) ==
		       TR_OK);
	}
	return TR_RPC_STATUS_OK;
}

static void server_terminal(struct tr_rpc_v2_call_id call,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct drain_context *context = (struct drain_context *)arg;
	(void)call;
	assert((unsigned)status < TR_RPC_STATUS_COUNT);
	assert(delivery == TR_RPC_V2_DELIVERY_LIVE ||
	       delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	pthread_mutex_lock(&context->lock);
	context->server_status = status;
	++context->server_terminals;
	pthread_cond_broadcast(&context->cond);
	pthread_mutex_unlock(&context->lock);
}

static void ignore_client_message(struct tr_rpc_v2_call call,
                                  const struct tr_rpc_bytes *body, void *arg)
{
	(void)call;
	(void)body;
	(void)arg;
}

static void ignore_client_terminal(struct tr_rpc_v2_call call,
                                   enum tr_rpc_status status,
                                   enum tr_rpc_v2_delivery delivery, void *arg)
{
	(void)call;
	(void)status;
	(void)delivery;
	(void)arg;
}

static void *destroy_server_retry(void *arg)
{
	struct drain_context *context = (struct drain_context *)arg;
	struct timespec pause = {0, 1000000L};
	int status = TR_AGAIN;

	for (unsigned attempt = 0; attempt < 15000U && status == TR_AGAIN;
	     ++attempt) {
		status = tr_server_destroy(context->server);
		pthread_mutex_lock(&context->lock);
		context->destroy_status = status;
		++context->destroy_attempts;
		if (status == TR_OK)
			context->destroy_done = 1;
		pthread_cond_broadcast(&context->cond);
		pthread_mutex_unlock(&context->lock);
		if (status == TR_AGAIN)
			(void)nanosleep(&pause, NULL);
	}
	return NULL;
}

static void test_v2_server_destroy_waits_for_accepted_work(void)
{
	struct drain_context context;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_method_desc client_method;
	struct tr_rpc_v2_method server_method;
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_bytes request = {(const uint8_t *)"hold", 4U};
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_v2_call call;
	pthread_t destroy_thread;
	struct timespec deadline;
	int ret = 0;
	uint16_t port = 0U;

	memset(&context, 0, sizeof(context));
	assert(pthread_mutex_init(&context.lock, NULL) == 0);
	assert(pthread_cond_init(&context.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	memset(&server_method, 0, sizeof(server_method));
	server_method.descriptor.service_id = 91U;
	server_method.descriptor.method_id = 1U;
	server_method.descriptor.request_cardinality = TR_RPC_ONE;
	server_method.descriptor.response_cardinality = TR_RPC_ONE;
	server_method.descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	server_method.descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	server_method.descriptor.lane = TR_RPC_LANE_CONTROL;
	server_method.descriptor.max_request_bytes = 16U;
	server_method.descriptor.max_response_bytes = 16U;
	server_method.work = server_work;
	server_method.terminal = server_terminal;
	server_method.arg = &context;
	assert(tr_server_create(&server_config, &server_method, 1U, &server) ==
	       TR_OK);
	context.server = server;
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	memset(&client_method, 0, sizeof(client_method));
	client_method.service_id = 91U;
	client_method.method_id = 1U;
	client_method.request_cardinality = TR_RPC_ONE;
	client_method.response_cardinality = TR_RPC_ONE;
	client_method.request_codec_id = TR_RPC_CODEC_RAW;
	client_method.response_codec_id = TR_RPC_CODEC_RAW;
	client_method.lane = TR_RPC_LANE_CONTROL;
	client_method.max_request_bytes = 16U;
	client_method.max_response_bytes = 16U;
	assert(tr_client_create(&client_config, &client_method, 1U, &client) ==
	       TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_wait_ready(client, 5000U) == TR_OK);
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = ignore_client_message;
	callbacks.terminal = ignore_client_terminal;
	assert(tr_client_unary_call(client, 91U, 1U, &request, NULL, &callbacks,
	                            &call) == TR_OK);
	wait_for_flag(&context, &context.work_entered);
	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;

	/* 挂起 work 时首次销毁保留 Server owner，并返回可重试的 AGAIN。 */
	assert(pthread_create(&destroy_thread, NULL, destroy_server_retry,
	                      &context) == 0);
	wait_for_destroy_attempt(&context);
	assert(pthread_mutex_lock(&context.lock) == 0);
	context.release_work = 1;
	assert(pthread_cond_broadcast(&context.cond) == 0);
	assert(pthread_mutex_unlock(&context.lock) == 0);

	deadline_after(&deadline, 15);
	assert(pthread_mutex_lock(&context.lock) == 0);
	while (!context.destroy_done && ret == 0)
		ret = pthread_cond_timedwait(&context.cond, &context.lock,
					     &deadline);
	assert(ret == 0);
	assert(context.destroy_done);
	assert(context.destroy_status == TR_OK);
	assert(context.destroy_attempts >= 2U);
	assert(context.server_terminals == 1U);
	assert((unsigned)context.server_status < TR_RPC_STATUS_COUNT);
	assert(pthread_mutex_unlock(&context.lock) == 0);

	assert(pthread_join(destroy_thread, NULL) == 0);
	context.server = NULL;
	assert(pthread_cond_destroy(&context.cond) == 0);
	assert(pthread_mutex_destroy(&context.lock) == 0);
}

/* R14：随 rpc.c Endpoint 闭包保留通用 TaskExecutor 与旧 RPC Executor group
 * 的底层生命周期覆盖。
 */
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

	lifecycle_gate_init(&create_gate);
	lifecycle_gate_init(&stop_gate);
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
	lifecycle_gate_destroy(&stop_gate);
	lifecycle_gate_destroy(&create_gate);
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
	test_v2_server_destroy_waits_for_accepted_work();
	return 0;
}
