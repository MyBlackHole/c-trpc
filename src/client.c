#include "tr/client.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "tr/buffer.h"
#include "tr/channel.h"
#include "tr/reactor.h"
#include "tr/rpc_wire.h"
#include "tr/socket.h"
#include "tr/status.h"

struct tr_client {
	struct tr_client_config config;

	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
	struct tr_conn_handle connection;

	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_pool_ready;
	int reassembly_pool_ready;

	int connected;
};

static uint64_t tr_client_now_ms(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000) +
	       (uint64_t)ts.tv_nsec / UINT64_C(1000000);
}

static void tr_client_pause_ms(uint32_t ms)
{
	struct timespec ts;

	ts.tv_sec = (time_t)(ms / 1000U);
	ts.tv_nsec = (long)(ms % 1000U) * 1000000L;
	while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
		;
}

static void tr_client_normalize_config(struct tr_client_config *config)
{
	struct tr_client_config defaults;

	tr_client_config_init(&defaults);
#define TR_CLIENT_DEFAULT_FIELD(field)                    \
	do {                                              \
		if ((config)->field == 0)                 \
			(config)->field = defaults.field; \
	} while (0)
	TR_CLIENT_DEFAULT_FIELD(connect_timeout_ms);
	TR_CLIENT_DEFAULT_FIELD(reconnect_initial_delay_ms);
	TR_CLIENT_DEFAULT_FIELD(reconnect_max_delay_ms);
#undef TR_CLIENT_DEFAULT_FIELD

	{
		struct tr_facade_limits *l = &config->limits;
		const struct tr_facade_limits *d = &defaults.limits;
#define TR_LIMIT_DEFAULT(field)              \
	do {                                 \
		if (l->field == 0)           \
			l->field = d->field; \
	} while (0)
		TR_LIMIT_DEFAULT(max_streams);
		TR_LIMIT_DEFAULT(max_methods);
		TR_LIMIT_DEFAULT(max_calls);
		TR_LIMIT_DEFAULT(max_frame_payload_bytes);
		TR_LIMIT_DEFAULT(max_message_bytes);
		TR_LIMIT_DEFAULT(initial_window_bytes);
		TR_LIMIT_DEFAULT(window_update_threshold_bytes);
		TR_LIMIT_DEFAULT(command_capacity);
		TR_LIMIT_DEFAULT(tx_item_capacity);
		TR_LIMIT_DEFAULT(control_tx_item_capacity);
		TR_LIMIT_DEFAULT(rx_buffer_count);
		TR_LIMIT_DEFAULT(rpc_message_pool_count);
		TR_LIMIT_DEFAULT(rpc_message_buffer_bytes);
		TR_LIMIT_DEFAULT(reassembly_pool_count);
		TR_LIMIT_DEFAULT(executor_threads);
		TR_LIMIT_DEFAULT(executor_queue_capacity);
#undef TR_LIMIT_DEFAULT
	}
}

void tr_client_config_init(struct tr_client_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	tr_facade_limits_init(&config->limits);
	config->connect_timeout_ms = 5000U;
	config->keepalive_interval_ms = 30000U;
	config->keepalive_timeout_ms = 10000U;
	config->enable_reconnect = 0;
	config->reconnect_initial_delay_ms = 200U;
	config->reconnect_max_delay_ms = 10000U;
}

static int tr_client_connect_fd(const char *address, uint16_t port,
				uint32_t timeout_ms, int *out_fd)
{
	struct pollfd pfd;
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!out_fd)
		return TR_ERR_INVALID;
	*out_fd = -1;

	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_OK) {
		*out_fd = tr_fd_take(&fd);
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLOUT;

	do {
		ret = poll(&pfd, 1, (int)timeout_ms);
	} while (ret < 0 && errno == EINTR);

	if (ret == 0)
		return TR_ERR_TIMEOUT;
	if (ret < 0)
		return TR_ERR_SYS;

	ret = tr_tcp_finish_connect(fd);
	if (ret != TR_OK)
		return ret;

	*out_fd = tr_fd_take(&fd);
	return TR_OK;
}

int tr_client_create(const struct tr_client_config *config,
		     struct tr_client **out)
{
	struct tr_client_config effective;
	struct tr_reactor_config reactor_config;
	struct tr_client *client;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (config)
		effective = *config;
	else
		tr_client_config_init(&effective);
	tr_client_normalize_config(&effective);

	if (effective.limits.max_message_bytes <
		    effective.limits.max_frame_payload_bytes ||
	    effective.limits.rpc_message_buffer_bytes < TR_RPC_WIRE_HEADER_SIZE)
		return TR_ERR_INVALID;

	client = (struct tr_client *)calloc(1, sizeof(*client));
	if (!client)
		return TR_ERR_NOMEM;
	client->config = effective;

	ret = tr_buffer_pool_init(&client->rpc_message_pool,
				  effective.limits.rpc_message_pool_count,
				  effective.limits.rpc_message_buffer_bytes);
	if (ret != TR_OK)
		goto fail;
	client->rpc_pool_ready = 1;

	ret = tr_buffer_pool_init(&client->reassembly_pool,
				  effective.limits.reassembly_pool_count,
				  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		goto fail;
	client->reassembly_pool_ready = 1;

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = effective.limits.command_capacity;
	reactor_config.tx_item_capacity = effective.limits.tx_item_capacity;
	reactor_config.control_tx_item_capacity =
		effective.limits.control_tx_item_capacity;
	reactor_config.rx_buffer_count = effective.limits.rx_buffer_count;
	reactor_config.rx_buffer_size =
		effective.limits.max_frame_payload_bytes;
	reactor_config.max_payload_len =
		effective.limits.max_frame_payload_bytes;
	reactor_config.rx_budget_bytes =
		effective.limits.max_frame_payload_bytes > UINT32_MAX / 4U ?
			UINT32_MAX :
			effective.limits.max_frame_payload_bytes * 4U;
	reactor_config.tx_budget_bytes = reactor_config.rx_budget_bytes;

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
	if (client->reactor)
		tr_reactor_destroy(client->reactor);
	if (client->reassembly_pool_ready)
		tr_buffer_pool_destroy(&client->reassembly_pool);
	if (client->rpc_pool_ready)
		tr_buffer_pool_destroy(&client->rpc_message_pool);
	free(client);
	return ret;
}

static void tr_client_reset_session(struct tr_client *client)
{
	if (!client)
		return;

	if (client->connection.reactor)
		(void)tr_reactor_close(client->connection);

	if (client->rpc) {
		tr_rpc_endpoint_destroy(client->rpc);
		client->rpc = NULL;
	}
	if (client->channel) {
		tr_channel_destroy(client->channel);
		client->channel = NULL;
	}

	memset(&client->connection, 0, sizeof(client->connection));
	client->connected = 0;
}

int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port)
{
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_channel_keepalive_config keepalive_config;
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!client || !ipv4_address || port == 0)
		return TR_ERR_INVALID;
	if (client->channel || client->rpc)
		return TR_ERR_STATE;

	ret = tr_client_connect_fd(ipv4_address, port,
				   client->config.connect_timeout_ms, &fd);
	if (ret != TR_OK)
		return ret;

	ret = tr_reactor_adopt_fd(client->reactor, fd, &client->connection);
	if (ret != TR_OK)
		return ret;
	(void)tr_fd_take(&fd);

	memset(&channel_config, 0, sizeof(channel_config));
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

	ret = tr_channel_create(&channel_config, client->connection,
				client->connection, NULL, NULL, NULL, NULL,
				&client->channel);
	if (ret != TR_OK)
		goto fail_session;

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = client->config.limits.max_methods;
	rpc_config.max_calls = client->config.limits.max_calls;
	rpc_config.message_pool = &client->rpc_message_pool;
	rpc_config.executor_threads = client->config.limits.executor_threads;
	rpc_config.executor_queue_capacity =
		client->config.limits.executor_queue_capacity;

	ret = tr_rpc_endpoint_create(client->channel, &rpc_config,
				     &client->rpc);
	if (ret != TR_OK)
		goto fail_session;

	/*
	 * Complete the initial HELLO handshake before starting maintenance
	 * threads. This keeps failed connect attempts rollback-safe: the original
	 * connection cannot be replaced by reconnect while teardown is running.
	 */
	ret = tr_client_wait_ready(client, client->config.connect_timeout_ms);
	if (ret != TR_OK)
		goto fail_session;

	if (client->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			client->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			client->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(client->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			goto fail_session;
	}

	if (client->config.enable_reconnect) {
		memset(&reconnect_config, 0, sizeof(reconnect_config));
		reconnect_config.ipv4_address = ipv4_address;
		reconnect_config.control_port = port;
		reconnect_config.bulk_port = port;
		reconnect_config.initial_delay_ms =
			client->config.reconnect_initial_delay_ms;
		reconnect_config.max_delay_ms =
			client->config.reconnect_max_delay_ms;
		reconnect_config.connect_timeout_ms =
			client->config.connect_timeout_ms;
		ret = tr_channel_enable_client_reconnect(client->channel,
							 &reconnect_config);
		if (ret != TR_OK)
			goto fail_session;
	}

	client->connected = 1;
	return TR_OK;

fail_session:
	tr_client_reset_session(client);
	return ret;
}

int tr_client_wait_ready(struct tr_client *client, uint32_t timeout_ms)
{
	uint64_t start;

	if (!client || !client->channel)
		return TR_ERR_STATE;

	start = tr_client_now_ms();
	for (;;) {
		enum tr_channel_lane_state state;
		int ret = tr_channel_get_lane_state(client->channel,
						    TR_LANE_CONTROL, &state);
		if (ret != TR_OK)
			return ret;
		if (state == TR_CHANNEL_LANE_UP)
			return TR_OK;
		if (timeout_ms != 0 && tr_client_now_ms() - start >= timeout_ms)
			return TR_ERR_TIMEOUT;
		tr_client_pause_ms(1U);
	}
}

int tr_client_register_method(struct tr_client *client,
			      const struct tr_rpc_method_desc *method)
{
	if (!client || !client->rpc)
		return TR_ERR_STATE;
	return tr_rpc_register_method(client->rpc, method, NULL, NULL);
}

int tr_client_unary_call_ex(struct tr_client *client, uint32_t service_id,
			    uint32_t method_id,
			    const struct tr_rpc_bytes *request,
			    const struct tr_rpc_call_options *options,
			    tr_rpc_unary_result_cb result_cb, void *result_arg,
			    struct tr_rpc_call_handle *out)
{
	if (!client || !client->rpc)
		return TR_ERR_STATE;
	return tr_rpc_unary_call_ex(client->rpc, service_id, method_id, request,
				    options, result_cb, result_arg, out);
}

int tr_client_unary_call(struct tr_client *client, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out)
{
	return tr_client_unary_call_ex(client, service_id, method_id, request,
				       NULL, result_cb, result_arg, out);
}

int tr_client_call_start_ex(struct tr_client *client, uint32_t service_id,
			    uint32_t method_id,
			    const struct tr_rpc_call_options *options,
			    const struct tr_rpc_call_callbacks *callbacks,
			    struct tr_rpc_call_handle *out)
{
	if (!client || !client->rpc)
		return TR_ERR_STATE;
	return tr_rpc_call_start_ex(client->rpc, service_id, method_id, options,
				    callbacks, out);
}

int tr_client_call_start(struct tr_client *client, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out)
{
	return tr_client_call_start_ex(client, service_id, method_id, NULL,
				       callbacks, out);
}

int tr_client_begin_drain(struct tr_client *client)
{
	if (!client || !client->channel)
		return TR_ERR_STATE;
	return tr_channel_begin_drain(client->channel);
}

int tr_client_wait_drained(struct tr_client *client, uint32_t timeout_ms)
{
	if (!client || !client->channel)
		return TR_ERR_STATE;
	return tr_channel_wait_drained(client->channel, timeout_ms);
}

int tr_client_get_channel_stats(struct tr_client *client,
				struct tr_channel_stats *out)
{
	if (!client || !client->channel)
		return TR_ERR_STATE;
	return tr_channel_get_stats(client->channel, out);
}

int tr_client_get_rpc_stats(struct tr_client *client,
			    struct tr_rpc_endpoint_stats *out)
{
	if (!client || !client->rpc)
		return TR_ERR_STATE;
	return tr_rpc_endpoint_get_stats(client->rpc, out);
}

void tr_client_destroy(struct tr_client *client)
{
	if (!client)
		return;

	if (client->channel) {
		(void)tr_channel_begin_drain(client->channel);
		(void)tr_channel_wait_drained(client->channel, 1000U);
	}

	if (client->reactor)
		(void)tr_reactor_stop(client->reactor);

	if (client->rpc)
		tr_rpc_endpoint_destroy(client->rpc);
	if (client->channel)
		tr_channel_destroy(client->channel);
	if (client->reactor)
		tr_reactor_destroy(client->reactor);

	if (client->reassembly_pool_ready)
		tr_buffer_pool_destroy(&client->reassembly_pool);
	if (client->rpc_pool_ready)
		tr_buffer_pool_destroy(&client->rpc_message_pool);

	free(client);
}
