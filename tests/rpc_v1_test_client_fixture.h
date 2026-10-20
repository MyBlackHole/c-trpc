#ifndef TR_RPC_V1_TEST_CLIENT_FIXTURE_H
#define TR_RPC_V1_TEST_CLIENT_FIXTURE_H

#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "../src/facade_tuning_internal.h"
#include "tr/client.h"

/*
 * Test-only raw V1 RPC peer for exercising the still-retained V1 Server and
 * Endpoint. This deliberately bypasses the removed tr_client V1 facade.
 */
struct rpc_v1_test_client {
	struct tr_client_config config;
	struct tr_facade_tuning tuning;
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *endpoint;
	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_message_pool_ready;
	int reassembly_pool_ready;
};

int rpc_v1_test_client_create(const struct tr_client_config *config,
                              const struct tr_facade_tuning *tuning,
                              struct rpc_v1_test_client **out);
int rpc_v1_test_client_connect(struct rpc_v1_test_client *client,
                               const char *ipv4_address, uint16_t port);
int rpc_v1_test_client_destroy(struct rpc_v1_test_client *client);

#endif
