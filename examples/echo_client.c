#include "tr/client.h"
#include "tr/status.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct result_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int done;
	int status;
	char response[1024];
	uint32_t response_len;
};

static void on_result(struct tr_rpc_call_handle call, int status,
		      const struct tr_rpc_bytes *response, void *arg)
{
	struct result_ctx *ctx = (struct result_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->status = status;
	if (response) {
		ctx->response_len =
			response->len < sizeof(ctx->response) - 1U ?
				response->len :
				(uint32_t)sizeof(ctx->response) - 1U;
		memcpy(ctx->response, response->data, ctx->response_len);
		ctx->response[ctx->response_len] = '\0';
	}
	ctx->done = 1;
	pthread_cond_signal(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

int main(int argc, char **argv)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct result_ctx result;
	struct timespec deadline;
	const char *address = argc > 1 ? argv[1] : "127.0.0.1";
	uint16_t port = argc > 2 ? (uint16_t)strtoul(argv[2], NULL, 10) : 9000U;
	const char *text = argc > 3 ? argv[3] : "hello";
	int wait_ret = 0;

	memset(&result, 0, sizeof(result));
	pthread_mutex_init(&result.lock, NULL);
	pthread_cond_init(&result.cond, NULL);

	tr_client_config_init(&config);
	if (tr_client_create(&config, &client) != TR_OK ||
	    tr_client_connect(client, address, port) != TR_OK) {
		fprintf(stderr, "connect failed\n");
		tr_client_destroy(client);
		return 1;
	}

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U * 1024U;
	method.max_response_bytes = 64U * 1024U;

	if (tr_client_register_method(client, &method) != TR_OK) {
		tr_client_destroy(client);
		return 1;
	}

	request.data = (const uint8_t *)text;
	request.len = (uint32_t)strlen(text);
	if (tr_client_unary_call(client, 1U, 1U, &request, on_result, &result,
				 &call) != TR_OK) {
		tr_client_destroy(client);
		return 1;
	}

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&result.lock);
	while (!result.done && wait_ret == 0)
		wait_ret = pthread_cond_timedwait(&result.cond, &result.lock,
						  &deadline);
	if (result.done && result.status == TR_RPC_STATUS_OK)
		printf("response: %s\n", result.response);
	else
		fprintf(stderr, "RPC failed: %d\n",
			result.done ? result.status : wait_ret);
	pthread_mutex_unlock(&result.lock);

	tr_client_destroy(client);
	pthread_cond_destroy(&result.cond);
	pthread_mutex_destroy(&result.lock);
	return result.done && result.status == TR_RPC_STATUS_OK ? 0 : 1;
}
