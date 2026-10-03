#include "tr/client.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#include "tr/buffer.h"
#include "tr/channel.h"
#include "tr/reactor.h"
#include "tr/rpc_wire.h"
#include "tr/socket.h"
#include "tr/status.h"
#include "tr/transport.h"
#include "channel_internal.h"
#include "facade_diagnostics_internal.h"
#include "pipeline_control_wire_internal.h"
#include "pipeline_route_internal.h"
#include "reactor_internal.h"
#include "rpc_internal.h"
#include "runtime_internal.h"
#include "socket_internal.h"

#define TR_CLIENT_CONNECTION_GROUP_CONTROL_BUFFERS 4U

struct tr_client {
	struct tr_client_config config;

	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
	struct tr_conn_handle connection;

	struct tr_conn_handle connection_group;
	struct tr_pipeline_route_preface connection_group_route;
	struct tr_buffer_pool connection_group_control_pool;
	uint32_t connection_group_generation;
	int connection_group_control_pool_ready;

	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_pool_ready;
	int reassembly_pool_ready;

	int connected;
};

TR_DEFINE_PTR_OWNERSHIP(tr_client_owner, struct tr_client, tr_client_destroy)

static struct tr_reactor *tr_client_reactor(struct tr_client *client)
{
	return client ? tr_runtime_shard_reactor(client->shard) : NULL;
}

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
	config->tcp_nodelay = TR_TCP_NODELAY_DEFAULT;
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

static int tr_client_send_all_fd(int fd, const uint8_t *data, size_t len,
				 uint32_t timeout_ms)
{
	uint64_t start = tr_client_now_ms();

	if (fd < 0 || (len != 0U && !data))
		return TR_ERR_INVALID;

	while (len != 0U) {
		struct pollfd pfd;
		ssize_t n;
		int wait_ms;
		int ret;

		n = send(fd, data, len, MSG_NOSIGNAL);
		if (n > 0) {
			data += (size_t)n;
			len -= (size_t)n;
			continue;
		}
		if (n < 0 && errno == EINTR)
			continue;
		if (n >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
			return TR_ERR_SYS;

		if (timeout_ms == 0U) {
			wait_ms = -1;
		} else {
			uint64_t elapsed = tr_client_now_ms() - start;

			if (elapsed >= timeout_ms)
				return TR_ERR_TIMEOUT;
			wait_ms = (int)(timeout_ms - elapsed);
		}

		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = fd;
		pfd.events = POLLOUT;
		do {
			ret = poll(&pfd, 1, wait_ms);
		} while (ret < 0 && errno == EINTR);
		if (ret == 0)
			return TR_ERR_TIMEOUT;
		if (ret < 0)
			return TR_ERR_SYS;
	}
	return TR_OK;
}

static uint32_t
tr_client_connection_group_next_generation(struct tr_client *client)
{
	client->connection_group_generation++;
	if (client->connection_group_generation == 0U)
		client->connection_group_generation = 1U;
	return client->connection_group_generation;
}

static int
tr_client_connection_group_control_pool_ensure(struct tr_client *client)
{
	int ret;

	if (client->connection_group_control_pool_ready)
		return TR_OK;
	ret = tr_buffer_pool_init(
		&client->connection_group_control_pool,
		TR_CLIENT_CONNECTION_GROUP_CONTROL_BUFFERS,
		TR_PIPELINE_CONTROL_WIRE_SIZE);
	if (ret != TR_OK)
		return ret;
	client->connection_group_control_pool_ready = 1;
	return TR_OK;
}

static int tr_client_connection_group_message_matches(
	const struct tr_client *client,
	const struct tr_pipeline_control_wire_message *message)
{
	const struct tr_pipeline_route_preface *route;

	if (!client || !message)
		return 0;
	route = &client->connection_group_route;
	return message->owner_shard_id == route->owner_shard_id &&
	       message->pipeline_id == route->pipeline_id &&
	       message->epoch == route->epoch;
}

static int tr_client_connection_group_cancel_offer_on_owner(
	struct tr_client *client, struct tr_conn_handle connection,
	const struct tr_pipeline_control_wire_message *offer,
	uint64_t message_id)
{
	struct tr_pipeline_control_wire_message cancel;
	struct tr_buffer *buffer = NULL;
	int ret;

	ret = tr_buffer_acquire(
		&client->connection_group_control_pool,
		TR_PIPELINE_CONTROL_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	cancel = *offer;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	cancel.stream_id = 0U;
	ret = tr_pipeline_control_wire_encode(buffer->data, &cancel);
	if (ret != TR_OK)
		goto fail;
	buffer->len = TR_PIPELINE_CONTROL_WIRE_SIZE;

	ret = tr_reactor_send(
		connection, TR_FRAME_PIPELINE_CONTROL, 0U, 0U, message_id, buffer);
	if (ret == TR_OK)
		return TR_OK;

fail:
	tr_buffer_release(buffer);
	return ret;
}

static enum tr_frame_disposition tr_client_connection_group_control_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_client *client = (struct tr_client *)arg;
	struct tr_pipeline_control_wire_message message;
	int ret;

	if (!client || !frame ||
	    frame->header.type != TR_FRAME_PIPELINE_CONTROL ||
	    frame->header.flags != 0U || frame->header.stream_id != 0U ||
	    !frame->payload ||
	    frame->payload->len != TR_PIPELINE_CONTROL_WIRE_SIZE) {
		(void)tr_reactor_abort_on_owner(connection, TR_ERR_BAD_TYPE);
		return TR_FRAME_RELEASE;
	}

	memset(&message, 0, sizeof(message));
	ret = tr_pipeline_control_wire_decode(
		frame->payload->data, frame->payload->len, &message);
	if (ret != TR_OK ||
	    !tr_client_connection_group_message_matches(client, &message)) {
		if (ret == TR_OK)
			ret = TR_ERR_STALE;
		(void)tr_reactor_abort_on_owner(connection, ret);
		return TR_FRAME_RELEASE;
	}

	/*
	 * This P3 slice establishes only the public CONTROL lifecycle. Do not leak
	 * DATA index/generation to applications: return unused reservations to the
	 * Server until the next slice can establish the matching DATA lane.
	 */
	if (message.type == TR_PIPELINE_CONTROL_DATA_OFFER) {
		ret = tr_client_connection_group_cancel_offer_on_owner(
			client, connection, &message, frame->header.message_id);
		if (ret == TR_OK)
			return TR_FRAME_RELEASE;
		if (ret >= 0)
			ret = TR_ERR_STATE;
	} else {
		ret = TR_ERR_BAD_TYPE;
	}

	(void)tr_reactor_abort_on_owner(connection, ret);
	return TR_FRAME_RELEASE;
}

struct tr_client_connection_group_adopt_request {
	struct tr_client *client;
	int fd;
	int fd_consumed;
	struct tr_conn_handle connection;
};

static int tr_client_connection_group_adopt_on_owner(void *arg)
{
	struct tr_client_connection_group_adopt_request *request =
		(struct tr_client_connection_group_adopt_request *)arg;
	struct tr_reactor *reactor = tr_client_reactor(request->client);
	int ret;

	ret = tr_reactor_adopt_fd(reactor, request->fd, &request->connection);
	if (ret != TR_OK)
		return ret;
	request->fd_consumed = 1;

	ret = tr_reactor_set_handler(
		request->connection, tr_client_connection_group_control_frame,
		NULL, request->client);
	if (ret != TR_OK) {
		(void)tr_reactor_close_on_owner(request->connection);
		memset(&request->connection, 0, sizeof(request->connection));
	}
	return ret;
}

struct tr_client_connection_group_close_request {
	struct tr_conn_handle connection;
};

static int tr_client_connection_group_close_on_owner(void *arg)
{
	struct tr_client_connection_group_close_request *request =
		(struct tr_client_connection_group_close_request *)arg;

	return tr_reactor_close_on_owner(request->connection);
}

int tr_client_create(const struct tr_client_config *config,
		     struct tr_client **out)
{
	struct tr_client_config effective;
	struct tr_runtime_config runtime_config;
	struct tr_runtime_shard_config shard_config;
	struct tr_reactor_config *reactor_config;
	struct tr_client *client TR_AUTO(tr_client_owner_cleanup) = NULL;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (config)
		effective = *config;
	else
		tr_client_config_init(&effective);
	tr_client_normalize_config(&effective);

	if (!tr_tcp_nodelay_policy_valid(effective.tcp_nodelay) ||
	    effective.limits.max_message_bytes <
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
		return ret;
	client->rpc_pool_ready = 1;

	ret = tr_buffer_pool_init(&client->reassembly_pool,
				  effective.limits.reassembly_pool_count,
				  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		return ret;
	client->reassembly_pool_ready = 1;

	memset(&runtime_config, 0, sizeof(runtime_config));
	memset(&shard_config, 0, sizeof(shard_config));
	runtime_config.shard_count = 1U;
	runtime_config.shards = &shard_config;
	reactor_config = &shard_config.reactor;
	reactor_config->max_connections = 4U;
	reactor_config->command_capacity = effective.limits.command_capacity;
	reactor_config->tx_item_capacity = effective.limits.tx_item_capacity;
	reactor_config->control_tx_item_capacity =
		effective.limits.control_tx_item_capacity;
	reactor_config->rx_buffer_count = effective.limits.rx_buffer_count;
	reactor_config->rx_buffer_size =
		effective.limits.max_frame_payload_bytes;
	reactor_config->max_payload_len =
		effective.limits.max_frame_payload_bytes;
	reactor_config->rx_budget_bytes =
		effective.limits.max_frame_payload_bytes > UINT32_MAX / 4U ?
			UINT32_MAX :
			effective.limits.max_frame_payload_bytes * 4U;
	reactor_config->tx_budget_bytes = reactor_config->rx_budget_bytes;
	reactor_config->observability_flags =
		effective.limits.observability_flags;

	ret = tr_runtime_create(&runtime_config, &client->runtime);
	if (ret != TR_OK)
		return ret;
	client->shard = tr_runtime_shard_at(client->runtime, 0U);
	if (!client->shard)
		return TR_ERR_STATE;
	ret = tr_runtime_start(client->runtime);
	if (ret != TR_OK)
		return ret;

	*out = tr_client_owner_take(&client);
	return TR_OK;
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

struct tr_client_session_guard {
	struct tr_client *client;
	int armed;
};

static void
tr_client_session_guard_cleanup(struct tr_client_session_guard *guard)
{
	if (guard && guard->armed && guard->client)
		tr_client_reset_session(guard->client);
}

int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port)
{
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_client_session_guard session
		TR_AUTO(tr_client_session_guard_cleanup) = { client, 0 };
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

	if (tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(fd, 1);
		if (ret != TR_OK)
			return ret;
	}

	ret = tr_reactor_adopt_fd(tr_client_reactor(client), fd, &client->connection);
	if (ret != TR_OK)
		return ret;
	(void)tr_fd_take(&fd);
	session.armed = 1;

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
		return ret;

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = client->config.limits.max_methods;
	rpc_config.max_calls = client->config.limits.max_calls;
	rpc_config.message_pool = &client->rpc_message_pool;
	rpc_config.executor_threads = client->config.limits.executor_threads;
	rpc_config.executor_queue_capacity =
		client->config.limits.executor_queue_capacity;
	rpc_config.observability_flags =
		client->config.limits.observability_flags;

	ret = tr_rpc_endpoint_create_with_executor_group(
		client->channel, &rpc_config, NULL, &client->rpc);
	if (ret != TR_OK)
		return ret;

	/*
	 * 先完成初始 HELLO handshake，再允许后续 reconnect policy 接管。
	 * 这样 connect 失败时 rollback 不会与 connection replacement 竞态。
	 */
	ret = tr_client_wait_ready(client, client->config.connect_timeout_ms);
	if (ret != TR_OK)
		return ret;

	if (client->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			client->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			client->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(client->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			return ret;
	}

	if (client->config.enable_reconnect) {
		ret = tr_channel_set_reconnect_tcp_nodelay(
			client->channel,
			tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay));
		if (ret != TR_OK)
			return ret;

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
			return ret;
	}

	client->connected = 1;
	session.armed = 0;
	return TR_OK;
}

int tr_client_connection_group_connect(
	struct tr_client *client, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *group)
{
	struct tr_pipeline_route_preface route;
	struct tr_client_connection_group_adopt_request request;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!client || !ipv4_address || !group || port == 0U ||
	    group->group_id == 0U || group->epoch == 0U)
		return TR_ERR_INVALID;
	if (client->connection_group.reactor)
		return TR_ERR_STATE;

	ret = tr_client_connection_group_control_pool_ensure(client);
	if (ret != TR_OK)
		return ret;

	ret = tr_client_connect_fd(
		ipv4_address, port, client->config.connect_timeout_ms, &fd);
	if (ret != TR_OK)
		return ret;

	if (tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(fd, 1);
		if (ret != TR_OK)
			return ret;
	}

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	/*
	 * The current Server facade binds the listener to internal shard 0.
	 * Keep that routing detail inside Transport; it is not public identity.
	 */
	route.owner_shard_id = 0U;
	route.pipeline_id = group->group_id;
	route.epoch = group->epoch;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation =
		tr_client_connection_group_next_generation(client);

	ret = tr_pipeline_route_preface_encode(raw, &route);
	if (ret != TR_OK)
		return ret;
	ret = tr_client_send_all_fd(
		fd, raw, sizeof(raw), client->config.connect_timeout_ms);
	if (ret != TR_OK)
		return ret;

	/*
	 * Publish immutable route identity before the owner installs the handler.
	 * The owner callback adopts the fd and installs the CONTROL frame handler
	 * in one Reactor turn, so Server frames already buffered after TRR1 cannot
	 * run through a NULL/default handler window.
	 */
	client->connection_group_route = route;
	memset(&request, 0, sizeof(request));
	request.client = client;
	request.fd = fd;
	ret = tr_reactor_call(
		tr_client_reactor(client),
		tr_client_connection_group_adopt_on_owner, &request);
	if (request.fd_consumed)
		(void)tr_fd_take(&fd);
	if (ret != TR_OK) {
		memset(&client->connection_group_route, 0,
		       sizeof(client->connection_group_route));
		return ret;
	}

	client->connection_group = request.connection;
	return TR_OK;
}

int tr_client_connection_group_close(struct tr_client *client)
{
	struct tr_client_connection_group_close_request request;
	int ret;

	if (!client)
		return TR_ERR_INVALID;
	if (!client->connection_group.reactor)
		return TR_ERR_STATE;

	request.connection = client->connection_group;
	ret = tr_reactor_call(
		client->connection_group.reactor,
		tr_client_connection_group_close_on_owner, &request);
	if (ret == TR_ERR_STALE || ret == TR_ERR_CLOSED)
		ret = TR_OK;
	if (ret != TR_OK)
		return ret;

	memset(&client->connection_group, 0, sizeof(client->connection_group));
	memset(&client->connection_group_route, 0,
	       sizeof(client->connection_group_route));
	return TR_OK;
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

	if (client->connection_group.reactor)
		(void)tr_client_connection_group_close(client);

	if (client->runtime)
		(void)tr_runtime_stop(client->runtime);

	if (client->rpc)
		tr_rpc_endpoint_destroy(client->rpc);
	if (client->channel)
		tr_channel_destroy(client->channel);
	if (client->runtime)
		tr_runtime_destroy(client->runtime);
	if (client->reassembly_pool_ready)
		tr_buffer_pool_destroy(&client->reassembly_pool);
	if (client->rpc_pool_ready)
		tr_buffer_pool_destroy(&client->rpc_message_pool);
	if (client->connection_group_control_pool_ready)
		tr_buffer_pool_destroy(&client->connection_group_control_pool);

	free(client);
}
