#define _POSIX_C_SOURCE 200809L

#include "tr/trpc.h"
#include "tr/rpc_v2.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
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

int main(void)
{
	test_v2_server_destroy_waits_for_accepted_work();
	return 0;
}
