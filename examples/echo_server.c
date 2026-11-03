#define _POSIX_C_SOURCE 200809L
#include "tr/rpc_v2.h"
#include "tr/status.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static volatile sig_atomic_t stopping;

static void on_signal(int signo)
{
	(void)signo;
	stopping = 1;
}

static enum tr_rpc_status echo_handler(struct tr_rpc_v2_call_id call,
                                       const struct tr_rpc_v2_request *request,
                                       const struct tr_rpc_v2_cancel *cancel,
                                       struct tr_rpc_v2_reply *reply, void *arg)
{
	(void)call;
	(void)cancel;
	(void)arg;
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE &&
	    tr_rpc_v2_reply_append(reply, &request->body) != TR_OK)
		return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	/* ONE 与流式共用终局：HALF_CLOSE 的默认完成会发送独立 FINAL。 */
	return TR_RPC_STATUS_OK;
}

/* 只由外部控制线程收敛；AGAIN 不是成功，也不能先销毁回调存储。 */
static int destroy_server(struct tr_server *server)
{
	struct timespec deadline, now;
	const struct timespec pause_time = {0, 1000000L};
	int ret = tr_server_destroy(server);

	if (ret != TR_AGAIN)
		return ret;
	if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
		return TR_ERR_SYS;
	deadline.tv_sec += 5;
	for (;;) {
		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
			return TR_ERR_SYS;
		if (now.tv_sec > deadline.tv_sec ||
		    (now.tv_sec == deadline.tv_sec &&
		     now.tv_nsec >= deadline.tv_nsec))
			return TR_ERR_TIMEOUT;
		if (nanosleep(&pause_time, NULL) != 0 && errno != EINTR)
			return TR_ERR_SYS;
		ret = tr_server_destroy(server);
		if (ret != TR_AGAIN)
			return ret;
	}
}

int main(int argc, char **argv)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_rpc_v2_method method;
	struct timespec pause_time;
	uint16_t port = 9000U;
	uint16_t bound = 0;
	int drain_ret;

	if (argc > 1)
		port = (uint16_t)strtoul(argv[1], NULL, 10);

	tr_server_config_init(&config);

	memset(&method, 0, sizeof(method));
	method.descriptor.service_id = 1U;
	method.descriptor.method_id = 1U;
	method.descriptor.request_cardinality = TR_RPC_ONE;
	method.descriptor.response_cardinality = TR_RPC_ONE;
	method.descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.lane = TR_RPC_LANE_CONTROL;
	method.descriptor.max_request_bytes = 64U * 1024U;
	method.descriptor.max_response_bytes = 64U * 1024U;

	method.work = echo_handler;
	if (tr_server_create(&config, &method, 1, &server) != TR_OK ||
	    tr_server_listen(server, "0.0.0.0", port, &bound) != TR_OK ||
	    tr_server_start(server) != TR_OK) {
		if (destroy_server(server) != TR_OK)
			return 1;
		return 1;
	}

	signal(SIGINT, on_signal);
	signal(SIGTERM, on_signal);
	printf("echo server listening on 0.0.0.0:%u\n", (unsigned)bound);

	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 100000000L;
	while (!stopping)
		nanosleep(&pause_time, NULL);

	drain_ret = tr_server_drain(server, 5000U);
	if (destroy_server(server) != TR_OK)
		return 1;
	return drain_ret == TR_OK ? 0 : 1;
}
