#include "tr/trpc.h"
#include "../src/execution/buffer.h"
#include "../src/execution/buffer_internal.h"
#include "../src/memory_budget.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LARGE_PAYLOAD_BYTES (320U * 1024U)
#define FACADE_MESSAGE_LIMIT (512U * 1024U)
#define FACADE_FRAME_LIMIT (64U * 1024U)

struct large_rpc_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	const uint8_t *reply;
	uint32_t reply_len;
	unsigned server_calls;
	unsigned client_results;
	int client_status;
	int request_ok;
	int response_ok;
};

static uint8_t pattern(uint32_t i, uint8_t salt)
{
	return (uint8_t)((i * 37U + salt) & 0xffU);
}

static int bytes_match(const uint8_t *data, uint32_t len, uint8_t salt)
{
	uint32_t i;
	if (!data || len != LARGE_PAYLOAD_BYTES)
		return 0;
	for (i = 0; i < len; ++i)
		if (data[i] != pattern(i, salt))
			return 0;
	return 1;
}

static int large_handler(struct tr_rpc_call_handle call,
			 const struct tr_rpc_bytes *request,
			 struct tr_rpc_unary_response *response, void *arg)
{
	struct large_rpc_ctx *ctx = (struct large_rpc_ctx *)arg;
	(void)call;
	pthread_mutex_lock(&ctx->lock);
	ctx->server_calls++;
	ctx->request_ok = bytes_match(request->data, request->len, 11U);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	response->status = TR_RPC_STATUS_OK;
	response->message.data = ctx->reply;
	response->message.len = ctx->reply_len;
	return TR_OK;
}

static void large_result(struct tr_rpc_call_handle call, int status,
			 const struct tr_rpc_bytes *response, void *arg)
{
	struct large_rpc_ctx *ctx = (struct large_rpc_ctx *)arg;
	(void)call;
	pthread_mutex_lock(&ctx->lock);
	ctx->client_results++;
	ctx->client_status = status;
	ctx->response_ok = response && bytes_match(response->data, response->len, 29U);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_dynamic_pool(void)
{
	struct tr_buffer_pool pool;
	struct tr_buffer *a = NULL, *b = NULL, *c = NULL;
	assert(tr_buffer_pool_init_dynamic(&pool, 2U, 4096U) == TR_OK);
	assert(tr_buffer_acquire(&pool, 64U, &a) == TR_OK);
	assert(a && a->capacity >= 64U);
	assert(tr_buffer_acquire(&pool, 2048U, &b) == TR_OK);
	assert(b && b->capacity >= 2048U);
	assert(tr_buffer_acquire(&pool, 1U, &c) == TR_AGAIN);
	assert(c == NULL);
	assert(tr_buffer_acquire(&pool, 4097U, &c) == TR_ERR_BAD_LENGTH);
	assert(c == NULL);
	tr_buffer_release(a);
	a = NULL;
	assert(tr_buffer_acquire(&pool, 3072U, &c) == TR_OK);
	assert(c && c->capacity >= 3072U);
	tr_buffer_release(c);
	tr_buffer_release(b);
	assert(tr_buffer_pool_free_count(&pool) == 2U);
	tr_buffer_pool_destroy(&pool);
}

static void test_budgeted_buffer_pool(void)
{
	struct tr_memory_budget budget;
	struct tr_memory_budget_stats stats;
	struct tr_buffer_pool pool;
	struct tr_buffer *buffer = NULL;
	uint64_t descriptor_bytes = (uint64_t)sizeof(struct tr_buffer);
	uint64_t fixed_bytes =
		UINT64_C(2) * descriptor_bytes + UINT64_C(2) * 64U;

	tr_memory_budget_init(&budget, fixed_bytes);
	assert(tr_buffer_pool_init_budgeted(
		       &pool, 2U, 64U, &budget) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == fixed_bytes);
	assert(stats.peak_bytes == fixed_bytes);
	assert(stats.rejection_events == 0U);
	tr_buffer_pool_destroy(&pool);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
	assert(stats.peak_bytes == fixed_bytes);

	/*
	 * Dynamic pool reserves descriptors at init and storage growth on demand.
	 * Retained capacity remains accounted after release and is returned only
	 * when the pool is destroyed.
	 */
	tr_memory_budget_init(&budget, descriptor_bytes + 64U);
	assert(tr_buffer_pool_init_dynamic_budgeted(
		       &pool, 1U, 128U, &budget) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes);

	assert(tr_buffer_acquire(&pool, 64U, &buffer) == TR_OK);
	assert(buffer != NULL && buffer->capacity == 64U);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes + 64U);
	tr_buffer_release(buffer);
	buffer = NULL;

	assert(tr_buffer_acquire(&pool, 65U, &buffer) == TR_AGAIN);
	assert(buffer == NULL);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == descriptor_bytes + 64U);
	assert(stats.peak_bytes == descriptor_bytes + 64U);
	assert(stats.rejection_events == 1U);

	tr_buffer_pool_destroy(&pool);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
	assert(stats.peak_bytes == descriptor_bytes + 64U);
}

static void test_large_facade_rpc(void)
{
	struct tr_server_config sc;
	struct tr_client_config cc;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct tr_rpc_semantic_stats cs, ss;
	struct large_rpc_ctx ctx;
	struct timespec deadline;
	uint8_t *req = (uint8_t *)malloc(LARGE_PAYLOAD_BYTES);
	uint8_t *reply = (uint8_t *)malloc(LARGE_PAYLOAD_BYTES);
	uint16_t port = 0U;
	uint32_t i;
	int ret = 0;

	assert(req && reply);
	for (i = 0; i < LARGE_PAYLOAD_BYTES; ++i) {
		req[i] = pattern(i, 11U);
		reply[i] = pattern(i, 29U);
	}
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	ctx.reply = reply;
	ctx.reply_len = LARGE_PAYLOAD_BYTES;

	tr_server_config_init(&sc);
	sc.max_peers = 1U;
	sc.keepalive_interval_ms = 0U;
	sc.limits.max_frame_payload_bytes = FACADE_FRAME_LIMIT;
	sc.limits.max_message_bytes = FACADE_MESSAGE_LIMIT;
	assert(tr_server_create(&sc, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 901U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_RPC_LANE_CONTROL;
	method.max_request_bytes = LARGE_PAYLOAD_BYTES;
	method.max_response_bytes = LARGE_PAYLOAD_BYTES;
	assert(tr_server_register_method(server, &method, large_handler, &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&cc);
	cc.keepalive_interval_ms = 0U;
	cc.connect_timeout_ms = 5000U;
	cc.limits.max_frame_payload_bytes = FACADE_FRAME_LIMIT;
	cc.limits.max_message_bytes = FACADE_MESSAGE_LIMIT;
	assert(tr_client_create(&cc, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_register_method(client, &method) == TR_OK);

	request.data = req;
	request.len = LARGE_PAYLOAD_BYTES;
	assert(tr_client_unary_call(client, 901U, 1U, &request, large_result, &ctx, &call) == TR_OK);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 20;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.client_results == 0U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.server_calls == 1U && ctx.client_results == 1U);
	assert(ctx.client_status == TR_RPC_STATUS_OK);
	assert(ctx.request_ok && ctx.response_ok);
	pthread_mutex_unlock(&ctx.lock);

	memset(&cs, 0, sizeof(cs));
	memset(&ss, 0, sizeof(ss));
	assert(tr_client_get_rpc_semantic_stats(client, &cs) == TR_OK);
	assert(tr_server_get_rpc_semantic_stats(server, &ss) == TR_OK);
	assert(cs.calls_finished == 1U && ss.calls_finished == 1U);

	ret = tr_client_begin_drain(client);
	assert(ret == TR_OK || ret == TR_AGAIN);
	assert(tr_client_wait_drained(client, 5000U) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	free(reply);
	free(req);
}

int main(void)
{
	test_dynamic_pool();
	test_budgeted_buffer_pool();
	test_large_facade_rpc();
	return 0;
}
