#define _POSIX_C_SOURCE 200809L

#include "../src/facade_diagnostics_internal.h"
#include "../src/facade_tuning_internal.h"
#include "tr/trpc.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

struct drain_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_server *server;
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

int __real_pthread_join(pthread_t thread, void **result);

int __wrap_pthread_join(pthread_t thread, void **result)
{
	struct drain_context *context = NULL;

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
	assert(tr_server_register_stream_method(
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
	assert(tr_client_create(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_wait_ready(client, 5000U) == TR_OK);
	assert(tr_client_register_method(client, &method) == TR_OK);
	assert(tr_client_call_start(client, 91U, 1U, NULL, &first_call) == TR_OK);
	send_message(first_call, (unsigned char)'a');
	wait_for_flag(&context, &context.first_entered);

	/* 同一调用的第二项任务留在端点队列中。 */
	send_message(first_call, (unsigned char)'b');
	wait_for_executor_state(&context, 1U, 1U, 0U);
	assert(tr_rpc_call_cancel(first_call) == TR_OK);
	wait_for_cancelled_executor_state(&context, 2U, 1U, 0U);

	/* 另一条调用就绪，用于覆盖端点中多个就绪调用的重调度。 */
	assert(tr_client_call_start(client, 91U, 1U, NULL, &second_call) == TR_OK);
	send_message(second_call, (unsigned char)'c');
	wait_for_executor_state(&context, 3U, 1U, 1U);

	/* 单分片停机依次等待 Reactor 所有者线程和 RPC 执行组工作线程。 */
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
	pthread_cond_destroy(&context.cond);
	pthread_mutex_destroy(&context.lock);
}

int main(void)
{
	test_server_destroy_drains_accepted_group_tasks();
	return 0;
}
