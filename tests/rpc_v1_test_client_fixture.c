#define _POSIX_C_SOURCE 200809L
#include "rpc_v1_test_client_fixture.h"

#include "../src/execution/deadline_internal.h"
#include "../src/execution/buffer_internal.h"
#include "../src/facade_policy_internal.h"
#include "../src/facade_tuning_internal.h"
#include "../src/io/socket.h"
#include "../src/io/socket_internal.h"
#include "tr/status.h"

#include <poll.h>
#include <stdlib.h>
#include <string.h>

static void rpc_v1_test_client_normalize_config(struct tr_client_config *config)
{
	struct tr_client_config defaults;

	tr_client_config_init(&defaults);
#define COPY_DEFAULT(field)                                                    \
	do {                                                                   \
		if (config->field == 0U)                                       \
			config->field = defaults.field;                        \
	} while (0)
	COPY_DEFAULT(connect_timeout_ms);
	COPY_DEFAULT(reconnect_initial_delay_ms);
	COPY_DEFAULT(reconnect_max_delay_ms);
#undef COPY_DEFAULT

#define COPY_LIMIT(field)                                                      \
	do {                                                                   \
		if (config->limits.field == 0U)                                \
			config->limits.field = defaults.limits.field;          \
	} while (0)
	COPY_LIMIT(max_streams);
	COPY_LIMIT(max_methods);
	COPY_LIMIT(max_calls);
	COPY_LIMIT(max_frame_payload_bytes);
	COPY_LIMIT(max_message_bytes);
	COPY_LIMIT(initial_window_bytes);
	COPY_LIMIT(window_update_threshold_bytes);
#undef COPY_LIMIT
}

static int rpc_v1_test_client_connect_fd(struct rpc_v1_test_client *client,
                                         const char *address, uint16_t port,
                                         int *out_fd)
{
	struct tr_deadline deadline;
	int fd = -1;
	int ret;

	ret = tr_deadline_init_ms(&deadline, client->config.connect_timeout_ms);
	if (ret != TR_OK)
		return ret;
	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_IN_PROGRESS) {
		ret = tr_deadline_poll_fd(fd, POLLOUT, &deadline, 0);
		if (ret == TR_OK)
			ret = tr_tcp_finish_connect(fd);
	}
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}
	if (tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(fd, 1);
		if (ret != TR_OK) {
			tr_socket_close(&fd);
			return ret;
		}
	}
	*out_fd = fd;
	return TR_OK;
}

int rpc_v1_test_client_create(const struct tr_client_config *config,
                              const struct tr_facade_tuning *tuning,
                              struct rpc_v1_test_client **out)
{
	struct rpc_v1_test_client *client;
	struct tr_reactor_config reactor_config = {0};
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	client = calloc(1U, sizeof(*client));
	if (!client)
		return TR_ERR_NOMEM;
	if (config)
		client->config = *config;
	else
		tr_client_config_init(&client->config);
	rpc_v1_test_client_normalize_config(&client->config);
	if (tuning)
		client->tuning = *tuning;
	else
		tr_facade_tuning_init(&client->tuning);
	tr_facade_tuning_normalize(&client->tuning);
	if (!tr_tcp_nodelay_policy_valid(client->config.tcp_nodelay) ||
	    (client->tuning.observability_flags &
	     ~TR_OBSERVABILITY_VALID_FLAGS) ||
	    client->config.limits.max_message_bytes <
	            client->config.limits.max_frame_payload_bytes ||
	    client->config.connection_groups.max_data_connections != 0U ||
	    client->config.connection_groups.max_active_transfers != 0U ||
	    client->config.connection_groups.on_transfer_ready ||
	    client->config.connection_groups.callback_arg ||
	    client->config.enable_reconnect) {
		free(client);
		return TR_ERR_INVALID;
	}

	ret = tr_buffer_pool_init_dynamic(
	        &client->rpc_message_pool,
	        client->tuning.rpc_message_pool_count,
	        client->config.limits.max_message_bytes);
	if (ret != TR_OK)
		goto fail;
	client->rpc_message_pool_ready = 1;
	ret = tr_buffer_pool_init_dynamic(
	        &client->reassembly_pool, client->tuning.reassembly_pool_count,
	        client->config.limits.max_message_bytes);
	if (ret != TR_OK)
		goto fail;
	client->reassembly_pool_ready = 1;

	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = client->tuning.command_capacity;
	reactor_config.tx_item_capacity = client->tuning.tx_item_capacity;
	reactor_config.control_tx_item_capacity =
	        client->tuning.control_tx_item_capacity;
	reactor_config.rx_buffer_count = client->tuning.rx_buffer_count;
	reactor_config.rx_buffer_size =
	        client->config.limits.max_frame_payload_bytes;
	reactor_config.max_payload_len =
	        client->config.limits.max_frame_payload_bytes;
	reactor_config.rx_budget_bytes =
	        client->config.limits.max_frame_payload_bytes > UINT32_MAX / 4U
	                ? UINT32_MAX
	                : client->config.limits.max_frame_payload_bytes * 4U;
	reactor_config.tx_budget_bytes = reactor_config.rx_budget_bytes;
	reactor_config.observability_flags = client->tuning.observability_flags;
	ret = tr_reactor_create(&reactor_config, NULL, NULL, NULL,
	                        &client->reactor);
	if (ret != TR_OK)
		goto fail;
	ret = tr_reactor_start(client->reactor);
	if (ret != TR_OK)
		goto fail;
	*out = client;
	return TR_OK;

fail:
	if (client->reactor) {
		(void)tr_reactor_stop(client->reactor);
		(void)tr_reactor_destroy(client->reactor);
	}
	if (client->reassembly_pool_ready)
		(void)tr_buffer_pool_destroy(&client->reassembly_pool);
	if (client->rpc_message_pool_ready)
		(void)tr_buffer_pool_destroy(&client->rpc_message_pool);
	free(client);
	return ret;
}

int rpc_v1_test_client_connect(struct rpc_v1_test_client *client,
                               const char *ipv4_address, uint16_t port)
{
	struct tr_channel_config channel_config = {0};
	struct tr_rpc_endpoint_config rpc_config = {0};
	struct tr_conn_handle connection = {0};
	struct tr_channel_keepalive_config keepalive = {0};
	int fd = -1;
	int ret;

	if (!client || !ipv4_address || port == 0U || !client->reactor ||
	    client->channel || client->endpoint)
		return TR_ERR_INVALID;
	ret = rpc_v1_test_client_connect_fd(client, ipv4_address, port, &fd);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_adopt_fd(client->reactor, fd, &connection);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}
	fd = -1;

	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = client->config.limits.max_streams;
	channel_config.initial_window_bytes =
	        client->config.limits.initial_window_bytes;
	channel_config.window_update_threshold_bytes =
	        client->config.limits.window_update_threshold_bytes;
	channel_config.max_message_bytes =
	        client->config.limits.max_message_bytes;
	channel_config.reassembly_pool = &client->reassembly_pool;
	ret = tr_channel_create(&channel_config, connection, connection, NULL,
	                        NULL, NULL, NULL, &client->channel);
	if (ret != TR_OK)
		return ret;
	ret = tr_channel_wait_ready(client->channel, TR_LANE_CONTROL,
	                            client->config.connect_timeout_ms);
	if (ret != TR_OK)
		return ret;

	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = client->config.limits.max_methods;
	rpc_config.max_calls = client->config.limits.max_calls;
	rpc_config.message_pool = &client->rpc_message_pool;
	rpc_config.executor_queue_capacity =
	        client->tuning.executor_queue_capacity;
	rpc_config.executor_threads = client->tuning.executor_threads;
	rpc_config.executor_continuation_reserve =
	        client->tuning.executor_continuation_reserve;
	rpc_config.observability_flags = client->tuning.observability_flags;
	rpc_config.interceptor = client->config.interceptor;
	ret = tr_rpc_endpoint_create(client->channel, &rpc_config,
	                             &client->endpoint);
	if (ret != TR_OK)
		return ret;
	if (client->config.keepalive_interval_ms != 0U) {
		keepalive.interval_ms = client->config.keepalive_interval_ms;
		keepalive.timeout_ms = client->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(client->channel, &keepalive);
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

int rpc_v1_test_client_destroy(struct rpc_v1_test_client *client)
{
	int ret;

	if (!client)
		return TR_OK;
	if (client->endpoint) {
		ret = tr_rpc_endpoint_destroy(client->endpoint);
		if (ret != TR_OK)
			return ret;
		client->endpoint = NULL;
	}
	if (client->channel) {
		ret = tr_channel_destroy(client->channel);
		if (ret != TR_OK)
			return ret;
		client->channel = NULL;
	}
	if (client->reactor) {
		ret = tr_reactor_stop(client->reactor);
		if (ret != TR_OK)
			return ret;
		ret = tr_reactor_destroy(client->reactor);
		if (ret != TR_OK)
			return ret;
		client->reactor = NULL;
	}
	if (client->reassembly_pool_ready) {
		ret = tr_buffer_pool_destroy(&client->reassembly_pool);
		if (ret != TR_OK)
			return ret;
		client->reassembly_pool_ready = 0;
	}
	if (client->rpc_message_pool_ready) {
		ret = tr_buffer_pool_destroy(&client->rpc_message_pool);
		if (ret != TR_OK)
			return ret;
		client->rpc_message_pool_ready = 0;
	}
	free(client);
	return TR_OK;
}
