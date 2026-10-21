#ifndef TR_SERVER_V2_TEST_FIXTURE_H
#define TR_SERVER_V2_TEST_FIXTURE_H

#include "tr/rpc_v2.h"

static enum tr_rpc_status
tr_test_server_v2_noop_work(struct tr_rpc_v2_call_id call,
                            const struct tr_rpc_v2_request *request,
                            const struct tr_rpc_v2_cancel *cancel,
                            struct tr_rpc_v2_reply *reply, void *arg)
{
	(void)call;
	(void)request;
	(void)cancel;
	(void)reply;
	(void)arg;
	return TR_RPC_STATUS_OK;
}

static int tr_test_server_v2_create(const struct tr_server_config *config,
                                    struct tr_server **out)
{
	struct tr_rpc_v2_method method = {0};

	method.descriptor.service_id = 1U;
	method.descriptor.method_id = 1U;
	method.descriptor.request_cardinality = TR_RPC_ONE;
	method.descriptor.response_cardinality = TR_RPC_MANY;
	method.descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.lane = TR_RPC_LANE_CONTROL;
	method.descriptor.max_request_bytes = 1U;
	method.descriptor.max_response_bytes = 1U;
	method.work = tr_test_server_v2_noop_work;
	return tr_server_create(config, &method, 1U, out);
}

#endif
