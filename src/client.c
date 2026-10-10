#include "tr/client.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "execution/buffer.h"
#include "execution/deadline_internal.h"
#include "transport/channel/channel.h"
#include "execution/reactor.h"
#include "execution/reactor_internal.h"
#include "rpc/rpc_wire.h"
#include "io/socket.h"
#include "tr/status.h"
#include "execution/buffer_internal.h"
#include "transport/channel/channel_internal.h"
#include "transport/group/client_group_internal.h"
#include "facade_diagnostics_internal.h"
#include "facade_policy_internal.h"
#include "facade_tuning_internal.h"
#include "rpc/rpc_internal.h"
#include "runtime/runtime_internal.h"
#include "io/socket_internal.h"


static int tr_client_blocking_lifecycle_context(void)
{
	/*
	 * Blocking lifecycle waits must run on an external control thread.
	 * Reactor owner callbacks must return so I/O/timers can advance; RPC
	 * workers must return so the current Call can publish its completion.
	 */
	return tr_reactor_in_owner_context() || tr_rpc_in_worker_context();
}

struct tr_client {
	struct tr_client_config config;
	struct tr_facade_tuning tuning;

	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
	struct tr_conn_handle connection;

	struct tr_client_group *connection_group;

	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_pool_ready;
	int reassembly_pool_ready;

	int connected;
};

static int tr_client_create_rollback(
	struct tr_client **out, struct tr_client *client, int cause)
{
	int rollback_ret;

	if (!out || !client)
		return cause;

	/*
	 * constructor 失败后显式执行 terminal rollback，不能把 fallible destroy
	 * 藏在 void cleanup。rollback 成功保持传统的 *out==NULL；rollback 自身
	 * 失败时把 partial Client ownership 留在 *out，调用方可继续 destroy。
	 */
	rollback_ret = tr_client_destroy(client);
	if (rollback_ret == TR_OK) {
		*out = NULL;
		return cause;
	}

	*out = client;
	return rollback_ret;
}

static struct tr_reactor *tr_client_reactor(struct tr_client *client)
{
	return client ? tr_runtime_shard_reactor(client->shard) : NULL;
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
#undef TR_LIMIT_DEFAULT
	}

	/*
	 * Connection Group transfer concurrency is a dedicated semantic budget.
	 * Preserve the historical behavior by inheriting max_streams when callers
	 * enable DATA lanes without choosing an explicit transfer limit.
	 */
	if (config->connection_groups.max_data_connections != 0U &&
	    config->connection_groups.max_active_transfers == 0U)
		config->connection_groups.max_active_transfers =
			config->limits.max_streams;
}

void tr_client_config_init(struct tr_client_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	tr_facade_limits_init(&config->limits);
	tr_connection_group_client_config_init(&config->connection_groups);
	config->connect_timeout_ms = 5000U;
	config->tcp_nodelay = TR_TCP_NODELAY_DEFAULT;
	config->keepalive_interval_ms = 30000U;
	config->keepalive_timeout_ms = 10000U;
	config->enable_reconnect = 0;
	config->reconnect_initial_delay_ms = 200U;
	config->reconnect_max_delay_ms = 10000U;
}

static int tr_client_connect_fd(const char *address, uint16_t port,
				const struct tr_deadline *deadline, int *out_fd)
{
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!deadline || !out_fd)
		return TR_ERR_INVALID;
	*out_fd = -1;

	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_OK) {
		*out_fd = tr_fd_take(&fd);
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	/* EINTR、短 poll 和超大毫秒数不能重新获得完整预算。 */
	ret = tr_deadline_poll_fd(fd, POLLOUT, deadline, 0);
	if (ret != TR_OK)
		return ret;

	ret = tr_tcp_finish_connect(fd);
	if (ret != TR_OK)
		return ret;

	*out_fd = tr_fd_take(&fd);
	return TR_OK;
}

int tr_client_create_with_tuning(
	const struct tr_client_config *config,
	const struct tr_facade_tuning *tuning,
	struct tr_client **out)
{
	struct tr_client_config effective;
	struct tr_facade_tuning effective_tuning;
	struct tr_runtime_config runtime_config;
	struct tr_runtime_shard_config shard_config;
	struct tr_reactor_config *reactor_config;
	struct tr_client *client = NULL;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;

	if (config)
		effective = *config;
	else
		tr_client_config_init(&effective);
	tr_client_normalize_config(&effective);

	if (tuning)
		effective_tuning = *tuning;
	else
		tr_facade_tuning_init(&effective_tuning);
	tr_facade_tuning_normalize(&effective_tuning);

	if (!tr_tcp_nodelay_policy_valid(effective.tcp_nodelay) ||
	    (effective_tuning.observability_flags &
	     ~TR_OBSERVABILITY_VALID_FLAGS) ||
	    effective.limits.max_message_bytes <
		    effective.limits.max_frame_payload_bytes)
		return TR_ERR_INVALID;

	client = (struct tr_client *)calloc(1, sizeof(*client));
	if (!client)
		return TR_ERR_NOMEM;
	client->config = effective;
	client->tuning = effective_tuning;

	ret = tr_buffer_pool_init_dynamic(&client->rpc_message_pool,
					  effective_tuning.rpc_message_pool_count,
					  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		return tr_client_create_rollback(out, client, ret);
	client->rpc_pool_ready = 1;

	ret = tr_buffer_pool_init(&client->reassembly_pool,
				  effective_tuning.reassembly_pool_count,
				  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		return tr_client_create_rollback(out, client, ret);
	client->reassembly_pool_ready = 1;

	memset(&runtime_config, 0, sizeof(runtime_config));
	memset(&shard_config, 0, sizeof(shard_config));
	runtime_config.shard_count = 1U;
	runtime_config.shards = &shard_config;
	reactor_config = &shard_config.reactor;
	if (effective.connection_groups.max_data_connections > UINT32_MAX - 2U)
		return tr_client_create_rollback(
			out, client, TR_ERR_INVALID);
	reactor_config->max_connections =
		effective.connection_groups.max_data_connections + 2U;
	if (reactor_config->max_connections < 4U)
		reactor_config->max_connections = 4U;
	reactor_config->command_capacity = effective_tuning.command_capacity;
	reactor_config->tx_item_capacity = effective_tuning.tx_item_capacity;
	reactor_config->control_tx_item_capacity =
		effective_tuning.control_tx_item_capacity;
	reactor_config->rx_buffer_count = effective_tuning.rx_buffer_count;
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
		effective_tuning.observability_flags;

	ret = tr_runtime_create(&runtime_config, &client->runtime);
	if (ret != TR_OK)
		return tr_client_create_rollback(out, client, ret);
	client->shard = tr_runtime_shard_at(client->runtime, 0U);
	if (!client->shard)
		return tr_client_create_rollback(out, client, TR_ERR_STATE);
	ret = tr_runtime_start(client->runtime);
	if (ret != TR_OK)
		return tr_client_create_rollback(out, client, ret);

	/*
	 * Connection Group engine is created lazily on first public Group use.
	 * A normal RPC-only Client therefore pays no Group heap/pool/timer cost.
	 */
	*out = client;
	return TR_OK;
}

int tr_client_create(const struct tr_client_config *config,
		     struct tr_client **out)
{
	return tr_client_create_with_tuning(config, NULL, out);
}

struct tr_client_close_connection_request {
	struct tr_conn_handle connection;
};

static int tr_client_close_connection_on_owner(void *arg)
{
	struct tr_client_close_connection_request *request =
		(struct tr_client_close_connection_request *)arg;
	int ret;

	ret = tr_reactor_close_on_owner(request->connection);
	return ret == TR_ERR_STALE ? TR_OK : ret;
}

static int tr_client_close_connection_sync(struct tr_conn_handle connection)
{
	struct tr_client_close_connection_request request;

	if (!connection.reactor)
		return TR_OK;
	request.connection = connection;
	return tr_reactor_call(
		connection.reactor, tr_client_close_connection_on_owner, &request);
}

static int tr_client_reset_session(struct tr_client *client)
{
	if (!client)
		return TR_OK;

	if (client->connection.reactor) {
		int ret = tr_client_close_connection_sync(client->connection);

		if (ret != TR_OK)
			return ret;
	}

	if (client->rpc) {
		int ret = tr_rpc_endpoint_destroy(client->rpc);

		if (ret != TR_OK)
			return ret;
		client->rpc = NULL;
	}
	if (client->channel) {
		int ret = tr_channel_destroy(client->channel);

		if (ret != TR_OK)
			return ret;
		client->channel = NULL;
	}

	memset(&client->connection, 0, sizeof(client->connection));
	client->connected = 0;
	return TR_OK;
}

static int tr_client_connect_rollback(
	struct tr_client *client, int cause)
{
	int rollback_ret;

	/*
	 * fd 被 Reactor 接管以后，后续失败已经跨过异步 publication 边界。
	 * rollback 必须显式返回状态：只有 reset_session() 真正收敛，才允许
	 * 把原 connect 错误返回给调用方；rollback 自身失败时生命周期错误优先。
	 */
	rollback_ret = tr_client_reset_session(client);
	return rollback_ret != TR_OK ? rollback_ret : cause;
}

int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port)
{
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_deadline connect_deadline;
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!client || !ipv4_address || port == 0)
		return TR_ERR_INVALID;
	if (client->channel || client->rpc ||
	    tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;

	/* TCP connect 和 HELLO 握手共享同一个绝对截止时间。 */
	ret = tr_deadline_init_ms(&connect_deadline,
				  client->config.connect_timeout_ms);
	if (ret != TR_OK)
		return ret;
	ret = tr_client_connect_fd(ipv4_address, port, &connect_deadline, &fd);
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
		goto rollback;

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = client->config.limits.max_methods;
	rpc_config.max_calls = client->config.limits.max_calls;
	rpc_config.message_pool = &client->rpc_message_pool;
	rpc_config.executor_threads = client->tuning.executor_threads;
	rpc_config.executor_queue_capacity =
		client->tuning.executor_queue_capacity;
	rpc_config.observability_flags =
		client->tuning.observability_flags;
	rpc_config.interceptor = client->config.interceptor;

	ret = tr_rpc_endpoint_create_with_executor_group(
		client->channel, &rpc_config, NULL, &client->rpc);
	if (ret != TR_OK)
		goto rollback;

	/*
	 * 先完成初始 HELLO handshake，再允许后续 reconnect policy 接管。
	 * 这样 connect 失败时 rollback 不会与 connection replacement 竞态。
	 */
	ret = tr_channel_wait_ready_deadline(
		client->channel, TR_LANE_CONTROL, &connect_deadline);
	if (ret != TR_OK)
		goto rollback;

	if (client->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			client->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			client->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(client->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			goto rollback;
	}

	if (client->config.enable_reconnect) {
		ret = tr_channel_set_reconnect_tcp_nodelay(
			client->channel,
			tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay));
		if (ret != TR_OK)
			goto rollback;

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
			goto rollback;
	}

	client->connected = 1;
	return TR_OK;

rollback:
	return tr_client_connect_rollback(client, ret);
}

static int tr_client_connection_group_ensure(struct tr_client *client)
{
	struct tr_client_group_config group_config;
	int ret;

	if (!client)
		return TR_ERR_INVALID;
	if (client->connection_group)
		return TR_OK;

	memset(&group_config, 0, sizeof(group_config));
	group_config.owner = tr_client_reactor(client);
	group_config.max_data_connections =
		client->config.connection_groups.max_data_connections;
	group_config.max_transfers =
		client->config.connection_groups.max_active_transfers;
	group_config.max_message_bytes =
		client->config.limits.max_message_bytes;
	group_config.max_frame_payload_bytes =
		client->config.limits.max_frame_payload_bytes;
	group_config.connect_timeout_ms = client->config.connect_timeout_ms;
	group_config.tcp_nodelay =
		tr_tcp_nodelay_policy_enabled(client->config.tcp_nodelay);
	group_config.on_transfer_ready =
		client->config.connection_groups.on_transfer_ready;
	group_config.callback_arg =
		client->config.connection_groups.callback_arg;

	ret = tr_client_group_create(
		&group_config, &client->connection_group);
	return ret;
}

int tr_client_connection_group_connect(
	struct tr_client *client, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *group)
{
	int ret;

	if (!client || !ipv4_address || !group || port == 0U ||
	    group->group_id == 0U || group->epoch == 0U)
		return TR_ERR_INVALID;
	ret = tr_client_connection_group_ensure(client);
	if (ret != TR_OK)
		return ret;
	return tr_client_group_connect(
		client->connection_group, ipv4_address, port, group);
}

int tr_client_connection_group_close(struct tr_client *client)
{
	if (!client)
		return TR_ERR_INVALID;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_close(client->connection_group);
}

int tr_client_connection_group_begin_drain(struct tr_client *client)
{
	if (!client)
		return TR_ERR_INVALID;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_begin_drain(client->connection_group);
}

int tr_client_connection_group_get_stats(
	struct tr_client *client,
	struct tr_connection_group_client_stats *out)
{
	if (!client || !out)
		return TR_ERR_INVALID;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_get_stats(client->connection_group, out);
}

int tr_client_connection_group_wait_drained(
	struct tr_client *client, uint32_t timeout_ms)
{
	if (!client)
		return TR_ERR_INVALID;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!client->connection_group)
		return TR_ERR_STATE;

	return tr_client_group_wait_drained(
		client->connection_group, timeout_ms);
}

int tr_client_connection_group_release_transfer(
	struct tr_client *client, uint32_t stream_id)
{
	if (!client || stream_id == 0U)
		return TR_ERR_INVALID;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_release_transfer(
		client->connection_group, stream_id);
}

int tr_client_connection_group_send(
	struct tr_client *client, uint32_t stream_id, uint64_t message_id,
	const struct tr_transport_bytes *bytes)
{
	if (!client || stream_id == 0U || !bytes ||
	    (bytes->len != 0U && !bytes->data))
		return TR_ERR_INVALID;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_send(
		client->connection_group, stream_id, message_id, bytes);
}

int tr_client_wait_ready(struct tr_client *client, uint32_t timeout_ms)
{
	if (!client)
		return TR_ERR_STATE;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!client->channel)
		return TR_ERR_STATE;

	return tr_channel_wait_ready(
		client->channel, TR_LANE_CONTROL, timeout_ms);
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
	if (!client)
		return TR_ERR_STATE;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!client->channel)
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

int tr_client_get_rpc_semantic_stats(
	struct tr_client *client, struct tr_rpc_semantic_stats *out)
{
	if (!client || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));
	if (!client->rpc)
		return TR_OK;
	return tr_rpc_endpoint_get_semantic_stats(client->rpc, out);
}

int tr_client_destroy(struct tr_client *client)
{
	int ret;

	if (!client)
		return TR_OK;

	/*
	 * destroy is an external terminal operation. Calling it from a Reactor
	 * callback or RPC worker would make synchronous teardown wait for itself.
	 * The public contract forbids that context; defensively leave ownership
	 * unchanged instead of partially destroying the Client.
	 */
	if (tr_reactor_in_owner_context() || tr_rpc_in_worker_context())
		return TR_ERR_STATE;

	/*
	 * destroy 是纯 terminal teardown，不内置 graceful-drain policy 或魔法超时。
	 * 需要优雅排空时，调用方必须在此之前显式调用 begin_drain/wait_drained。
	 *
	 * 所有 callback source 与上层 owner barrier 仍必须在 Runtime stop 前收敛。
	 */
	if (client->connection_group) {
		ret = tr_client_group_destroy(client->connection_group);
		if (ret != TR_OK)
			return ret;
		client->connection_group = NULL;
	}

	if (client->rpc) {
		ret = tr_rpc_endpoint_destroy(client->rpc);
		if (ret != TR_OK)
			return ret;
		client->rpc = NULL;
	}
	if (client->channel) {
		ret = tr_channel_destroy(client->channel);
		if (ret != TR_OK)
			return ret;
		client->channel = NULL;
	}

	if (client->runtime) {
		ret = tr_runtime_stop(client->runtime);
		/*
		 * A failed lifecycle barrier means ownership has not converged.
		 * Keep the remaining Client/Runtime storage alive rather than freeing
		 * memory that an execution thread may still reference.
		 */
		if (ret != TR_OK)
			return ret;
	}

	/*
	 * Pools may borrow RuntimeShard accounting in later phases. Release them
	 * after protocol users are gone but before destroying their resource owner.
	 */
	if (client->reassembly_pool_ready) {
		ret = tr_buffer_pool_destroy(&client->reassembly_pool);
		if (ret != TR_OK)
			return ret;
		client->reassembly_pool_ready = 0;
	}
	if (client->rpc_pool_ready) {
		ret = tr_buffer_pool_destroy(&client->rpc_message_pool);
		if (ret != TR_OK)
			return ret;
		client->rpc_pool_ready = 0;
	}

	if (client->runtime) {
		ret = tr_runtime_destroy(client->runtime);
		if (ret != TR_OK)
			return ret;
		client->runtime = NULL;
		client->shard = NULL;
	}

	free(client);
	return TR_OK;
}
