#define _POSIX_C_SOURCE 200809L
#include "execution/worker_context.h"
#include "tr/client.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "execution/buffer.h"
#include "execution/deadline_internal.h"
#include "transport/channel/channel.h"
#include "execution/reactor.h"
#include "execution/reactor_internal.h"
#include "execution/task_executor.h"
#include "rpc/rpc_record.h"
#include "io/socket.h"
#include "io/tls_config.h"
#include "tr/status.h"
#include "execution/buffer_internal.h"
#include "transport/channel/channel_internal.h"
#include "transport/group/client_group_internal.h"
#include "facade_diagnostics_internal.h"
#include "facade_policy_internal.h"
#include "facade_tuning_internal.h"
#include "runtime/runtime_internal.h"
#include "io/socket_internal.h"
#include "client_record_internal.h"
#include "rpc/rpc_record_dispatch.h"
#include "rpc/rpc_v2_internal.h"

static int tr_client_blocking_lifecycle_context(void)
{
	/* 阻塞的生命周期等待必须在外部控制线程运行。
	 * 反应器所有者回调必须返回，I/O 与定时器才能推进；
	 * RPC 工作线程必须返回，当前 Call 才能发布完成结果。
	 */
	return tr_reactor_in_owner_context() ||
	       tr_execution_in_worker_context() ||
	       tr_rpc_record_client_in_callback() ||
	       tr_rpc_record_dispatch_in_callback();
}

struct tr_client {
	struct tr_client_config config;
	struct tr_facade_tuning tuning;

	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_channel *channel;
	struct tr_conn_handle connection;
	struct tr_rpc_record_client *record_rpc;
	struct tr_client_record_config record_config;
	struct tr_rpc_method_desc *record_methods;
	size_t record_method_count;
	size_t record_method_bytes;
	int record_mode;
	int transport_only;
	int record_sdk_mode;
	int record_connect_failed;
	_Atomic int record_stopping;
	int destroy_active;
	/* owner 只发布谓词；外部等待不读取调用槽或回调中的可变字段。 */
	pthread_mutex_t record_drain_lock;
	pthread_cond_t record_drain_cond;
	int record_drain_lock_ready, record_drain_cond_ready;
	int record_draining, record_drained;

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

	/* 构造函数失败后显式执行终止回滚，不能把可能失败的销毁
	 * 藏在无返回值清理中。回滚成功保持传统的 *out==NULL；回滚自身
	 * 失败时把部分构造的 Client 所有权留在 *out，调用方可继续销毁。
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

static int tr_client_record_wait_init(struct tr_client *client)
{
	pthread_condattr_t attr;
	int ret;
	if (pthread_mutex_init(&client->record_drain_lock, NULL) != 0)
		return TR_ERR_SYS;
	client->record_drain_lock_ready = 1;
	if (pthread_condattr_init(&attr) != 0)
		return TR_ERR_SYS;
	ret = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	if (ret == 0)
		ret = pthread_cond_init(&client->record_drain_cond, &attr);
	if (ret == 0)
		client->record_drain_cond_ready = 1;
	int cleanup_ret = pthread_condattr_destroy(&attr);
	return ret == 0 && cleanup_ret == 0 ? TR_OK : TR_ERR_SYS;
}

static void tr_client_record_progress(bool drained, void *arg)
{
	struct tr_client *client = arg;
	pthread_mutex_lock(&client->record_drain_lock);
	if (client->record_drained != (int)drained) {
		client->record_drained = drained;
		pthread_cond_broadcast(&client->record_drain_cond);
	}
	pthread_mutex_unlock(&client->record_drain_lock);
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

	/* 连接组传输并发度是专用的语义预算。
	 * 调用者启用 DATA 通道而未选择显式传输上限时，
	 * 通过继承 max_streams 保留历史行为。
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
	config->tls.mode = TR_TLS_DISABLED;
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

static int
tr_client_record_config_valid(const struct tr_client_config *config,
                              const struct tr_client_record_config *record,
                              const struct tr_rpc_method_desc *methods,
                              size_t method_count)
{
	const struct tr_connection_group_client_config *groups =
	        &config->connection_groups;
	uint64_t window = config->limits.initial_window_bytes;
	uint64_t record_limit = config->limits.max_message_bytes;

	/* 未通告的已消费信用最多为 threshold-1。整记录原子准入要求
	 * 剩余窗口仍容纳传输允许的最大记录，不能只按未协商的方法限额算。
	 */
	if (record_limit > window)
		record_limit = window;

	if (config->enable_reconnect || groups->max_data_connections ||
	    groups->max_active_transfers || groups->on_transfer_ready ||
	    groups->callback_arg)
		return TR_ERR_UNSUPPORTED;
	if (!methods || !method_count ||
	    method_count > config->limits.max_methods ||
	    method_count > SIZE_MAX / sizeof(*methods) ||
	    !record->memory_budget_bytes || !record->calls.capacity ||
	    record->calls.capacity > config->limits.max_calls ||
	    record->calls.capacity > config->limits.max_streams ||
	    !record->calls.progress_limit ||
	    record->calls.progress_period_ns < UINT64_C(1000000) ||
	    record->calls.progress_period_ns > UINT64_C(1000000000) ||
	    config->limits.max_message_bytes <
	            TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES ||
	    config->limits.initial_window_bytes <
	            TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES ||
	    config->limits.window_update_threshold_bytes >
	            window - record_limit + 1)
		return TR_ERR_INVALID;
	for (size_t i = 0; i < method_count; ++i) {
		const struct tr_rpc_method_desc *method = &methods[i];
		struct tr_rpc_record_state state;

		if (!method->request_codec_id || !method->response_codec_id ||
		    method->lane > TR_RPC_LANE_BULK ||
		    (uint64_t)method->max_request_bytes +
		                    TR_RPC_RECORD_HEADER_SIZE >
		            config->limits.max_message_bytes ||
		    (uint64_t)method->max_response_bytes +
		                    TR_RPC_RECORD_HEADER_SIZE >
		            config->limits.max_message_bytes ||
		    (uint64_t)method->max_request_bytes +
		                    TR_RPC_RECORD_HEADER_SIZE >
		            config->limits.initial_window_bytes ||
		    (uint64_t)method->max_response_bytes +
		                    TR_RPC_RECORD_HEADER_SIZE >
		            config->limits.initial_window_bytes ||
		    tr_rpc_record_state_init(
		            &state, method->request_cardinality,
		            method->response_cardinality) != TR_OK)
			return TR_ERR_INVALID;
		for (size_t j = 0; j < i; ++j)
			if (methods[j].service_id == method->service_id &&
			    methods[j].method_id == method->method_id)
				return TR_ERR_INVALID;
	}
	return TR_OK;
}

static int
tr_client_create_internal(const struct tr_client_config *config,
                          const struct tr_facade_tuning *tuning,
                          const struct tr_client_record_config *record,
                          const struct tr_rpc_method_desc *methods,
                          size_t method_count, int public_v2,
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
	ret = tr_tls_client_config_validate(&effective.tls, 0);
	if (ret != TR_OK)
		return ret;

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
	if (record) {
		ret = tr_client_record_config_valid(&effective, record, methods,
		                                    method_count);
		if (ret != TR_OK)
			return ret;
	}

	client = (struct tr_client *)calloc(1, sizeof(*client));
	if (!client)
		return TR_ERR_NOMEM;
	client->config = effective;
	client->tuning = effective_tuning;
	atomic_init(&client->record_stopping, 0);
	if (record) {
		client->record_mode = 1;
		client->record_sdk_mode = public_v2;
		client->record_config = *record;
		ret = tr_client_record_wait_init(client);
		if (ret != TR_OK)
			return tr_client_create_rollback(out, client, ret);
	} else {
		client->transport_only = 1;
	}

	memset(&runtime_config, 0, sizeof(runtime_config));
	memset(&shard_config, 0, sizeof(shard_config));
	runtime_config.shard_count = 1U;
	runtime_config.shards = &shard_config;
	if (record)
		shard_config.memory_budget_bytes = record->memory_budget_bytes;
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
	if (record) {
		struct tr_memory_budget *budget =
		        tr_runtime_shard_memory_budget(client->shard);
		size_t bytes = method_count * sizeof(*methods);

		ret = tr_buffer_pool_init_budgeted(
		        &client->reassembly_pool,
		        effective_tuning.reassembly_pool_count,
		        effective.limits.max_message_bytes, budget);
		if (ret != TR_OK)
			return tr_client_create_rollback(out, client, ret);
		client->reassembly_pool_ready = 1;
		if (public_v2) {
			ret = tr_buffer_pool_init_dynamic_budgeted(
			        &client->rpc_message_pool,
			        effective_tuning.rpc_message_pool_count,
			        effective.limits.max_message_bytes, budget);
			if (ret != TR_OK)
				return tr_client_create_rollback(out, client,
				                                 ret);
			client->rpc_pool_ready = 1;
		}
		ret = tr_memory_budget_reserve(budget, bytes);
		if (ret != TR_OK)
			return tr_client_create_rollback(out, client, ret);
		client->record_method_bytes = bytes;
		client->record_methods = malloc(bytes);
		if (!client->record_methods)
			return tr_client_create_rollback(out, client,
			                                 TR_ERR_NOMEM);
		memcpy(client->record_methods, methods, bytes);
		client->record_method_count = method_count;
	}
	ret = tr_runtime_start(client->runtime);
	if (ret != TR_OK)
		return tr_client_create_rollback(out, client, ret);

	/* 连接组引擎在首次使用公开 Group 接口时才创建。
	 * 因此仅使用 RPC 的普通客户端不承担 Group 堆、池或定时器成本。
	 */
	*out = client;
	return TR_OK;
}

int tr_client_create_record(const struct tr_client_config *config,
                            const struct tr_facade_tuning *tuning,
                            const struct tr_client_record_config *record,
                            const struct tr_rpc_method_desc *methods,
                            size_t method_count, struct tr_client **out)
{
	if (!record) {
		if (out)
			*out = NULL;
		return TR_ERR_INVALID;
	}
	return tr_client_create_internal(config, tuning, record, methods,
	                                 method_count, 0, out);
}

int tr_client_create(const struct tr_client_config *config,
                     const struct tr_rpc_method_desc *methods,
                     size_t method_count, struct tr_client **out)
{
	struct tr_client_config effective;
	struct tr_facade_tuning tuning;
	struct tr_client_record_config record = {0};
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
	if (!methods || !method_count ||
	    method_count > effective.limits.max_methods ||
	    method_count > SIZE_MAX / sizeof(*methods))
		return TR_ERR_INVALID;
	/* 公共上限描述正文。内部为控制记录留出的空间不能放宽此约束。 */
	for (size_t i = 0; i < method_count; ++i)
		if (methods[i].max_request_bytes >
		            effective.limits.max_message_bytes ||
		    methods[i].max_response_bytes >
		            effective.limits.max_message_bytes ||
		    methods[i].max_request_bytes >
		            effective.limits.initial_window_bytes ||
		    methods[i].max_response_bytes >
		            effective.limits.initial_window_bytes)
			return TR_ERR_INVALID;
	ret = tr_rpc_v2_prepare_limits(&effective.limits);
	if (ret != TR_OK)
		return ret;
	tr_facade_tuning_init(&tuning);
	ret = tr_rpc_v2_memory_limit(&effective.limits, &tuning, 1,
	                             &record.memory_budget_bytes);
	if (ret != TR_OK)
		return ret;
	record.calls.capacity = effective.limits.max_calls;
	if (record.calls.capacity > effective.limits.max_streams)
		record.calls.capacity = effective.limits.max_streams;
	record.calls.progress_limit = 64;
	record.calls.progress_period_ns = 1000000;
	return tr_client_create_internal(&effective, &tuning, &record, methods,
	                                 method_count, 1, out);
}

int tr_client_create_transport(const struct tr_client_config *config,
                               struct tr_client **out)
{
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (config && config->enable_reconnect)
		return TR_ERR_UNSUPPORTED;
	ret = tr_client_create_internal(config, NULL, NULL, NULL, 0, 0, out);
	return ret;
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

static int tr_client_record_close_on_owner(void *arg)
{
	struct tr_client *client = arg;

	return tr_rpc_record_client_close_on_owner(&client->record_rpc);
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
	if (client->record_rpc) {
		int ret = tr_reactor_call(tr_client_reactor(client),
		                          tr_client_record_close_on_owner,
		                          client);

		if (ret != TR_OK)
			return ret;
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

	/* fd 被反应器接管以后，后续失败已经跨过异步发布边界。
	 * 回滚必须显式返回状态：只有 reset_session() 真正收敛，才允许
	 * 把原连接错误返回给调用方；回滚自身失败时生命周期错误优先。
	 */
	rollback_ret = tr_client_reset_session(client);
	if (rollback_ret != TR_OK)
		client->record_connect_failed = 1;
	return rollback_ret != TR_OK ? rollback_ret : cause;
}

struct tr_client_record_assemble_request {
	struct tr_client *client;
	const struct tr_channel_config *config;
	int *fd;
};

static int tr_client_record_assemble_on_owner(void *arg)
{
	struct tr_client_record_assemble_request *request = arg;
	struct tr_client *client = request->client;
	int ret;

	/* 接管与发布不得分成两个 owner 命令：对端可能在其间关闭，
	 * 导致后续 Channel 绑定失效连接，或向尚未安装 RPC 的层交付事件。
	 */
	ret = tr_reactor_adopt_fd(tr_client_reactor(client), *request->fd,
	                          &client->connection);
	if (ret != TR_OK)
		return ret;
	(void)tr_fd_take(request->fd);
	ret = tr_channel_create_deferred(request->config, client->connection,
	                                 client->connection, NULL, NULL, NULL,
	                                 NULL, &client->channel);
	if (ret != TR_OK)
		return ret;
	ret = tr_rpc_record_client_create_on_owner(
	        client->channel, client->record_methods,
	        client->record_method_count, &client->record_config.calls,
	        tr_runtime_shard_memory_budget(client->shard),
	        &client->record_rpc);
	if (ret != TR_OK)
		return ret;
	if (client->record_sdk_mode) {
		ret = tr_rpc_record_client_set_send_pool_on_owner(
		        client->record_rpc, &client->rpc_message_pool);
		if (ret != TR_OK)
			return ret;
	}
	ret = tr_rpc_record_client_set_progress_notify_on_owner(
	        client->record_rpc, tr_client_record_progress, client);
	if (ret != TR_OK)
		return ret;
	ret = tr_rpc_record_client_start_on_owner(client->record_rpc);
	if (ret != TR_OK)
		return ret;
	return tr_channel_start(client->channel);
}

int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port)
{
	struct tr_channel_config channel_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_deadline connect_deadline;
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	if (!client || !ipv4_address || port == 0)
		return TR_ERR_INVALID;
	if (client->transport_only)
		return TR_ERR_UNSUPPORTED;
	if (client->channel || client->record_rpc ||
	    client->record_connect_failed ||
	    atomic_load_explicit(&client->record_stopping,
	                         memory_order_acquire) ||
	    tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;

	/* TCP 连接和 HELLO 握手共享同一个绝对截止时间。 */
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
	channel_config.feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET;
	channel_config.required_feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET;
	channel_config.memory_budget =
	        tr_runtime_shard_memory_budget(client->shard);

	{
		struct tr_client_record_assemble_request request = {
		        client, &channel_config, &fd};

		ret = tr_reactor_call(tr_client_reactor(client),
		                      tr_client_record_assemble_on_owner,
		                      &request);
	}
	if (ret != TR_OK)
		goto rollback;

	/* 连接与初始 HELLO 握手共享同一期限；只有完整 V2 装配就绪后才返回。 */
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
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
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
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_close(client->connection_group);
}

int tr_client_connection_group_begin_drain(struct tr_client *client)
{
	if (!client)
		return TR_ERR_INVALID;
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
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
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
	if (!client->connection_group)
		return TR_ERR_STATE;
	return tr_client_group_get_stats(client->connection_group, out);
}

int tr_client_connection_group_wait_drained(
	struct tr_client *client, uint32_t timeout_ms)
{
	if (!client)
		return TR_ERR_INVALID;
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
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
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
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
	if (client->record_mode)
		return TR_ERR_UNSUPPORTED;
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
static int tr_client_record_sync(struct tr_rpc_record_client *client,
                                 int (*fn)(void *arg), void *arg)
{
	struct tr_reactor *owner;

	if (!client)
		return TR_ERR_INVALID;
	owner = tr_rpc_record_client_reactor(client);
	if (!owner)
		return TR_ERR_STATE;
	if ((tr_reactor_in_owner_context() ||
	     tr_rpc_record_client_in_callback() ||
	     tr_rpc_record_dispatch_in_callback()) &&
	    !tr_reactor_in_owner_context_for(owner))
		return TR_ERR_STATE;
	return tr_reactor_call(owner, fn, arg);
}

struct tr_client_record_start_request {
	struct tr_rpc_record_client *client;
	uint32_t service_id;
	uint32_t method_id;
	const struct tr_rpc_call_options *options;
	const struct tr_rpc_record_client_callbacks *callbacks;
	struct tr_rpc_record_client_handle *out;
};

static int tr_client_record_start_on_owner(void *arg)
{
	struct tr_client_record_start_request *request = arg;

	return tr_rpc_record_client_call_start_on_owner(
	        request->client, request->service_id, request->method_id,
	        request->options, request->callbacks, request->out);
}

int tr_client_record_call_start(
        struct tr_client *client, uint32_t service_id, uint32_t method_id,
        const struct tr_rpc_call_options *options,
        const struct tr_rpc_record_client_callbacks *callbacks,
        struct tr_rpc_record_client_handle *out)
{
	struct tr_client_record_start_request request;

	if (!client)
		return TR_ERR_INVALID;
	if (!client->record_mode)
		return TR_ERR_UNSUPPORTED;
	if (atomic_load_explicit(&client->record_stopping,
	                         memory_order_acquire) ||
	    client->record_connect_failed || !client->connected ||
	    !client->record_rpc)
		return TR_ERR_STATE;
	request = (struct tr_client_record_start_request){
	        client->record_rpc, service_id, method_id, options,
	        callbacks,          out};
	return tr_client_record_sync(client->record_rpc,
	                             tr_client_record_start_on_owner, &request);
}

struct tr_client_v2_start_request {
	struct tr_rpc_record_client *client;
	uint32_t service_id, method_id;
	const struct tr_rpc_call_options *options;
	const struct tr_rpc_v2_client_callbacks *callbacks;
	struct tr_rpc_v2_call *out;
	const struct tr_rpc_bytes *unary_request;
};

static int tr_client_v2_start_on_owner(void *arg)
{
	struct tr_client_v2_start_request *request = arg;
	return tr_rpc_record_client_call_start_public_on_owner(
	        request->client, request->service_id, request->method_id,
	        request->options, request->callbacks, request->out,
	        request->unary_request);
}

static int tr_client_v2_start(
        struct tr_client *client, uint32_t service_id, uint32_t method_id,
        const struct tr_rpc_call_options *options,
        const struct tr_rpc_v2_client_callbacks *callbacks,
        struct tr_rpc_v2_call *out, const struct tr_rpc_bytes *unary_request)
{
	if (!client)
		return TR_ERR_INVALID;
	if (!client->record_sdk_mode)
		return TR_ERR_UNSUPPORTED;
	if (atomic_load_explicit(&client->record_stopping,
	                         memory_order_acquire) ||
	    client->record_connect_failed || !client->connected ||
	    !client->record_rpc)
		return TR_ERR_STATE;
	struct tr_client_v2_start_request request = {
	        client->record_rpc, service_id, method_id,    options,
	        callbacks,          out,        unary_request};
	return tr_client_record_sync(client->record_rpc,
	                             tr_client_v2_start_on_owner, &request);
}

int tr_client_call_start(struct tr_client *client, uint32_t service_id,
                         uint32_t method_id,
                         const struct tr_rpc_call_options *options,
                         const struct tr_rpc_v2_client_callbacks *callbacks,
                         struct tr_rpc_v2_call *out)
{
	return tr_client_v2_start(client, service_id, method_id, options,
	                          callbacks, out, NULL);
}

int tr_client_unary_call(struct tr_client *client, uint32_t service_id,
                         uint32_t method_id, const struct tr_rpc_bytes *request,
                         const struct tr_rpc_call_options *options,
                         const struct tr_rpc_v2_client_callbacks *callbacks,
                         struct tr_rpc_v2_call *out)
{
	if (!request || (request->len && !request->data))
		return TR_ERR_INVALID;
	return tr_client_v2_start(client, service_id, method_id, options,
	                          callbacks, out, request);
}

enum tr_client_record_operation {
	TR_CLIENT_RECORD_SEND,
	TR_CLIENT_RECORD_SEND_BYTES,
	TR_CLIENT_RECORD_HALF_CLOSE,
	TR_CLIENT_RECORD_CANCEL
};

struct tr_client_record_operation_request {
	struct tr_rpc_record_client_handle handle;
	struct tr_buffer **body;
	enum tr_client_record_operation operation;
	const struct tr_rpc_bytes *message;
};

static int tr_client_record_operation_on_owner(void *arg)
{
	struct tr_client_record_operation_request *request = arg;

	switch (request->operation) {
	case TR_CLIENT_RECORD_SEND:
		return tr_rpc_record_client_send_on_owner(request->handle,
		                                          request->body);
	case TR_CLIENT_RECORD_SEND_BYTES:
		return tr_rpc_record_client_send_bytes_on_owner(
		        request->handle, request->message);
	case TR_CLIENT_RECORD_HALF_CLOSE:
		return tr_rpc_record_client_half_close_on_owner(
		        request->handle);
	case TR_CLIENT_RECORD_CANCEL:
		return tr_rpc_record_client_cancel_on_owner(request->handle);
	}
	return TR_ERR_INVALID;
}

int tr_client_record_send(struct tr_rpc_record_client_handle handle,
                          struct tr_buffer **body)
{
	struct tr_client_record_operation_request request = {
	        .handle = handle,
	        .body = body,
	        .operation = TR_CLIENT_RECORD_SEND};

	return tr_client_record_sync(
	        handle.client, tr_client_record_operation_on_owner, &request);
}

int tr_client_record_half_close(struct tr_rpc_record_client_handle handle)
{
	struct tr_client_record_operation_request request = {
	        .handle = handle, .operation = TR_CLIENT_RECORD_HALF_CLOSE};

	return tr_client_record_sync(
	        handle.client, tr_client_record_operation_on_owner, &request);
}

int tr_client_record_cancel(struct tr_rpc_record_client_handle handle)
{
	struct tr_client_record_operation_request request = {
	        .handle = handle, .operation = TR_CLIENT_RECORD_CANCEL};

	return tr_client_record_sync(
	        handle.client, tr_client_record_operation_on_owner, &request);
}

int tr_rpc_v2_call_send(struct tr_rpc_v2_call call,
                        const struct tr_rpc_bytes *message)
{
	struct tr_rpc_record_client_handle handle =
	        tr_rpc_record_client_private_handle(call);
	struct tr_client_record_operation_request request = {
	        .handle = handle,
	        .operation = TR_CLIENT_RECORD_SEND_BYTES,
	        .message = message};
	return tr_client_record_sync(
	        handle.client, tr_client_record_operation_on_owner, &request);
}

int tr_rpc_v2_call_half_close(struct tr_rpc_v2_call call)
{
	return tr_client_record_half_close(
	        tr_rpc_record_client_private_handle(call));
}

int tr_rpc_v2_call_cancel(struct tr_rpc_v2_call call)
{
	return tr_client_record_cancel(
	        tr_rpc_record_client_private_handle(call));
}

static int tr_client_record_begin_drain_on_owner(void *arg)
{
	struct tr_client *client = arg;
	/* 回调中也可发布排空意图。先关闭 RPC 准入，再由同一 owner
	 * 事务冻结 Channel；静默发布要等当前用户回调和绑定操作返回。 */
	pthread_mutex_lock(&client->record_drain_lock);
	if (!client->record_draining) {
		client->record_draining = 1;
		client->record_drained = 0;
	}
	pthread_mutex_unlock(&client->record_drain_lock);
	int ret = tr_rpc_record_client_begin_drain_on_owner(client->record_rpc);
	return ret == TR_OK ? tr_channel_begin_drain(client->channel) : ret;
}

int tr_client_begin_drain(struct tr_client *client)
{
	if (!client || !client->channel)
		return TR_ERR_STATE;
	if (atomic_load_explicit(&client->record_stopping,
	                         memory_order_acquire) ||
	    client->record_connect_failed)
		return TR_ERR_STATE;
	if (client->record_mode)
		return tr_client_record_sync(
		        client->record_rpc,
		        tr_client_record_begin_drain_on_owner, client);
	return tr_channel_begin_drain(client->channel);
}

static int tr_client_record_wait_drained(struct tr_client *client,
                                         const struct tr_deadline *deadline)
{
	int ret = TR_OK;
	pthread_mutex_lock(&client->record_drain_lock);
	if (!client->record_draining) {
		ret = TR_ERR_STATE;
		goto out;
	}
	while (!client->record_drained) {
		if (!deadline) {
			ret = TR_AGAIN;
			break;
		}
		int wait_ret = pthread_cond_timedwait(
		        &client->record_drain_cond, &client->record_drain_lock,
		        &deadline->absolute);
		if (wait_ret != 0) {
			ret = wait_ret == ETIMEDOUT ? TR_AGAIN : TR_ERR_SYS;
			break;
		}
	}
out:
	pthread_mutex_unlock(&client->record_drain_lock);
	return ret;
}

int tr_client_wait_drained(struct tr_client *client, uint32_t timeout_ms)
{
	if (!client)
		return TR_ERR_STATE;
	if (tr_client_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!client->channel)
		return TR_ERR_STATE;
	if (client->record_mode) {
		struct tr_deadline deadline;
		const struct tr_deadline *wait_deadline = NULL;
		int ret;
		if (atomic_load_explicit(&client->record_stopping,
		                         memory_order_acquire) ||
		    client->record_connect_failed)
			return TR_ERR_STATE;
		if (timeout_ms) {
			ret = tr_deadline_init_ms(&deadline, timeout_ms);
			if (ret != TR_OK)
				return ret;
			wait_deadline = &deadline;
		}
		ret = tr_client_record_wait_drained(client, wait_deadline);
		return ret == TR_OK ? tr_channel_wait_drained_deadline(
		                              client->channel, wait_deadline)
		                    : ret;
	}
	return tr_channel_wait_drained(client->channel, timeout_ms);
}

int tr_client_get_channel_stats(struct tr_client *client,
				struct tr_channel_stats *out)
{
	if (!client || !client->channel)
		return TR_ERR_STATE;
	return tr_channel_get_stats(client->channel, out);
}

struct tr_client_record_semantic_request {
	struct tr_rpc_record_client *client;
	struct tr_rpc_semantic_stats *out;
};

static int tr_client_record_semantic_on_owner(void *arg)
{
	struct tr_client_record_semantic_request *request = arg;
	return tr_rpc_record_client_get_semantic_stats_on_owner(request->client,
	                                                        request->out);
}

int tr_client_get_rpc_semantic_stats(
	struct tr_client *client, struct tr_rpc_semantic_stats *out)
{
	if (!client || !out)
		return TR_ERR_INVALID;
	if (!client->record_mode) {
		*out = (struct tr_rpc_semantic_stats){0};
		return TR_OK;
	}
	struct tr_reactor *owner = tr_client_reactor(client);
	if (atomic_load_explicit(&client->record_stopping,
	                         memory_order_acquire) ||
	    client->record_connect_failed || !owner)
		return TR_ERR_STATE;
	/* 同一 Client 的 LIVE 回调可在 owner 上直接读取；工作线程、
	 * 停止清理域及其他 owner 不能同步等待此 owner 的进度。 */
	if (tr_client_blocking_lifecycle_context() &&
	    !tr_reactor_in_owner_context_for(owner))
		return TR_ERR_STATE;
	if (!client->record_rpc) {
		*out = (struct tr_rpc_semantic_stats){0};
		return TR_OK;
	}
	struct tr_client_record_semantic_request request = {client->record_rpc,
	                                                    out};
	return tr_reactor_call(owner, tr_client_record_semantic_on_owner,
	                       &request);
}

static int tr_client_destroy_internal(struct tr_client *client)
{
	int ret;

	if (client->record_mode) {
		atomic_store_explicit(&client->record_stopping, 1,
		                      memory_order_release);
		/* 父 owner 必须先 join，才能从清理域交付残留终局。
		 * 包括 fatal exit 与 OPEN 尚未提交时的已受理责任。
		 */
		if (client->runtime) {
			ret = tr_runtime_stop(client->runtime);
			if (ret != TR_OK)
				return ret;
		}
		if (client->record_rpc) {
			ret = tr_rpc_record_client_reap_stopped(
			        &client->record_rpc);
			if (ret != TR_OK)
				return ret;
		}
	}

	/* 销毁是纯终止拆除，不内置优雅排空策略或隐含超时。
	 * 需要优雅排空时，调用方必须在此之前显式调用 begin_drain/wait_drained。
	 */
	if (client->connection_group) {
		ret = tr_client_group_destroy(client->connection_group);
		if (ret != TR_OK)
			return ret;
		client->connection_group = NULL;
	}

	if (client->channel) {
		ret = tr_channel_destroy(client->channel);
		if (ret != TR_OK)
			return ret;
		client->channel = NULL;
	}

	if (client->runtime && !client->record_mode) {
		ret = tr_runtime_stop(client->runtime);
		/* 生命周期屏障失败意味着所有权尚未收敛。
		 * 保持剩余 Client/Runtime 存储存活。
		 * 不释放执行线程可能仍引用的内存。
		 */
		if (ret != TR_OK)
			return ret;
	}

	/* 池在后续阶段可能借用 RuntimeShard 记账。
	 * 在协议使用方离开后、销毁其资源所有者前释放池。
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
	if (client->record_method_bytes) {
		struct tr_memory_budget *budget;
		size_t bytes = client->record_method_bytes;

		/* 构造只有在分片已绑定后才预约方法表。缺少预算所有者
		 * 时不能跳过归还责任，亦不能继续释放 Runtime 外壳。
		 */
		if (!client->runtime || !client->shard)
			return TR_ERR_STATE;
		budget = tr_runtime_shard_memory_budget(client->shard);
		ret = tr_memory_budget_release(budget, bytes);
		if (ret != TR_OK)
			return ret;
		free(client->record_methods);
		client->record_methods = NULL;
		client->record_method_bytes = 0;
		client->record_method_count = 0;
	}

	/* 生命周期由外部串行化；宿主和父 owner 静默后再拆通知同步原语。
	 * 失败保留剩余原语和 Client，销毁重试不会再次销毁已完成的部分。 */
	if (client->record_drain_cond_ready) {
		if (pthread_cond_destroy(&client->record_drain_cond) != 0)
			return TR_ERR_SYS;
		client->record_drain_cond_ready = 0;
	}
	if (client->record_drain_lock_ready) {
		if (pthread_mutex_destroy(&client->record_drain_lock) != 0)
			return TR_ERR_SYS;
		client->record_drain_lock_ready = 0;
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

int tr_client_destroy(struct tr_client *client)
{
	int ret;

	if (!client)
		return TR_OK;
	if (tr_client_blocking_lifecycle_context() || client->destroy_active)
		return TR_ERR_STATE;
	client->destroy_active = 1;
	ret = tr_client_destroy_internal(client);
	if (ret != TR_OK)
		client->destroy_active = 0;
	return ret;
}
