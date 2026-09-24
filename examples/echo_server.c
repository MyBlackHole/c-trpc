#include "tr/server.h"
#include "tr/status.h"

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

static int echo_handler(struct tr_rpc_call_handle call,
			const struct tr_rpc_bytes *request,
			struct tr_rpc_unary_response *response, void *arg)
{
	(void)call;
	(void)arg;
	response->status = TR_RPC_STATUS_OK;
	response->message = *request;
	return TR_OK;
}

int main(int argc, char **argv)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_rpc_method_desc method;
	struct timespec pause_time;
	uint16_t port = 9000U;
	uint16_t bound = 0;

	if (argc > 1)
		port = (uint16_t)strtoul(argv[1], NULL, 10);

	tr_server_config_init(&config);
	if (tr_server_create(&config, &server) != TR_OK)
		return 1;

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

	if (tr_server_register_method(server, &method, echo_handler, NULL) !=
		    TR_OK ||
	    tr_server_listen(server, "0.0.0.0", port, &bound) != TR_OK ||
	    tr_server_start(server) != TR_OK) {
		tr_server_destroy(server);
		return 1;
	}

	signal(SIGINT, on_signal);
	signal(SIGTERM, on_signal);
	printf("echo server listening on 0.0.0.0:%u\n", (unsigned)bound);

	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 100000000L;
	while (!stopping)
		nanosleep(&pause_time, NULL);

	(void)tr_server_drain(server, 5000U);
	tr_server_destroy(server);
	return 0;
}
