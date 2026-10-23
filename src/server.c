#define _POSIX_C_SOURCE 200809L
#include "execution/worker_context.h"
#include "tr/server.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "execution/buffer.h"
#include "execution/deadline_internal.h"
#include "transport/channel/channel.h"
#include "execution/buffer_internal.h"
#include "transport/channel/channel_internal.h"
#include "facade_diagnostics_internal.h"
#include "facade_policy_internal.h"
#include "facade_tuning_internal.h"
#include "execution/reactor.h"
#include "execution/reactor_internal.h"
#include "io/socket.h"
#include "io/tls_config.h"
#include "io/tls_openssl.h"
#include "io/tls_provider_operation_internal.h"
#include "memory_budget.h"
#include "tr/status.h"
#include "runtime/runtime_internal.h"
#include "io/socket_internal.h"
#include "observability_internal.h"
#include "transport/group/pipeline_listener_internal.h"
#include "server_record_internal.h"
#include "rpc/rpc_record_client.h"
#include "rpc/rpc_record_stats.h"
#include "rpc/rpc_record_producer.h"
#include "rpc/rpc_v2_internal.h"

#define TR_SERVER_ACCEPT_BATCH 16U

/* 对端协议对象由 Server 拥有，Runtime 只管理公共传输前缀与存储。
 * 相同槽位复用时必须清空整个扩展（而不只清空公共前缀）。
 */
struct tr_server_peer {
	struct tr_runtime_peer transport;
	struct tr_rpc_record_endpoint *record_rpc;
	int record_closing;
};

_Static_assert(offsetof(struct tr_server_peer, transport) == 0,
               "Server peer transport prefix must start at offset zero");
_Static_assert(_Alignof(struct tr_server_peer) ==
                       _Alignof(struct tr_runtime_peer),
               "Server peer extension must preserve Runtime alignment");

static struct tr_server_peer *tr_server_peer_state(struct tr_runtime_peer *peer)
{
	return (struct tr_server_peer *)peer;
}

enum tr_server_mode {
	TR_SERVER_MODE_RPC_V2 = 1,
	TR_SERVER_MODE_TRANSPORT_ONLY = 2
};

struct tr_server;

struct tr_server_shard {
	struct tr_server *server;
	struct tr_runtime_shard *runtime;
	struct tr_tls_openssl_profile *tls_profile;
	struct tr_buffer_pool reassembly_pool;
	int reassembly_pool_ready;
	int peer_events_enabled;
	struct tr_reactor_timer_handle handshake_timer;
	int handshake_timer_registered;
	struct tr_task_executor *record_tasks;
	struct tr_rpc_record_producer_domain *record_producers;
	struct tr_reactor_timer_handle record_cleanup_timer;
	int record_cleanup_timer_registered;
	/* 只在 owner 或父线程 join 后修改，排空时与对端事件一起冻结。 */
	int record_cleanup_enabled;
	int record_tasks_retiring;
	/* observing 仅由 owner 修改；其余谓词在 finalizer_lock 下发布/读取。 */
	int record_drain_observing;
	int record_drained, record_drain_status;
	struct tr_server_channel_stats retired_channel_stats;
	/* 所属 owner 直接记账，寿命长于全部 peer。退役无需搬移快照；
	 * 停止后的写入只发生在父线程 join 且统计入口关闭的清理域。 */
	struct tr_rpc_record_semantic_counters record_semantic;
};

static int tr_server_blocking_lifecycle_context(void)
{
	/* 同步排空等待需要反应器所有者与 RPC 执行器都能推进。
	 * 从任一执行域进入等待，都会等待当前回调或任务
	 * 返回后才可能取得的进展。
	 */
	return tr_reactor_in_owner_context() ||
	       tr_execution_in_worker_context() ||
	       tr_rpc_record_client_in_callback() ||
	       tr_rpc_record_dispatch_in_callback();
}

struct tr_server {
	struct tr_server_config config;
	enum tr_server_mode mode;
	struct tr_tls_openssl_profile *tls_profile;
	struct tr_tls_openssl_profile *tls_recovery_profile;
	char *tls_client_ca_file_copy;
	struct tr_tls_provider_operation_table tls_operations;
	pthread_mutex_t tls_provider_lock;
	struct tr_tls_provider_operation tls_credentials_operation;
	struct tr_reactor_timer_handle tls_credentials_timer;
	uint64_t tls_credentials_deadline_ns;
	enum tr_tls_rotation_policy tls_credentials_policy;
	int tls_operations_ready;
	int tls_provider_lock_ready;
	int tls_credentials_active;
	int tls_credentials_start_active;
	int tls_credentials_cancel_notified;
	int tls_credentials_timer_registered;

	struct tr_runtime *runtime;
	struct tr_server_shard *shards;
	/* 已绑定 Runtime、允许销毁路径遍历的分片前缀。 */
	uint32_t shard_count;

	struct tr_server_record_config record;
	/* 普通 V2 全 Server 的共享上限；分片保留本地账目。 */
	struct tr_memory_budget memory_budget;
	struct tr_rpc_dispatch_method *record_methods;
	size_t record_method_count;
	size_t record_method_bytes;
	int record_start_attempted;

	struct tr_pipeline_listener *connection_group_listener;

	pthread_mutex_t finalizer_lock;
	pthread_cond_t record_drain_cond;
	int record_drain_cond_ready;
	int started;
	_Atomic int tls_rotation_failed;
	_Atomic int tls_update_active;
	int gracefully_drained;
	/* 只有构造完成并发布后才允许读取跨分片统计；终止销毁开始后关闭。 */
	int published;
};

TR_DEFINE_PTR_OWNERSHIP(tr_server_mem, struct tr_server, free)

static uint64_t tr_server_tls_credentials_deadline_tick(void *arg,
                                                        uint64_t now_ns);
static int tr_server_monotonic_now_ns(uint64_t *out_now_ns);

static char *tr_server_copy_optional_string(const char *value)
{
	size_t length;
	char *copy;

	if (!value)
		return NULL;
	length = strlen(value);
	if (length == SIZE_MAX)
		return NULL;
	copy = malloc(length + 1U);
	if (copy)
		memcpy(copy, value, length + 1U);
	return copy;
}

static int tr_server_create_rollback(
	struct tr_server **out, struct tr_server *server, int cause)
{
	int rollback_ret;

	if (!out || !server)
		return cause;

	/* 服务端构造函数不再依赖可能失败的无返回值所有者清理。正常回滚
	 * 完整收敛时保持 *out==NULL；若终止清理自身失败，则把部分构造的
	 * Server 所有权留给调用方，后续显式 tr_server_destroy() 可继续重试。
	 */
	rollback_ret = tr_server_destroy(server);
	if (rollback_ret == TR_OK) {
		*out = NULL;
		return cause;
	}

	*out = server;
	return rollback_ret;
}

static struct tr_reactor *
tr_server_shard_reactor(struct tr_server_shard *shard)
{
	return shard ? tr_runtime_shard_reactor(shard->runtime) : NULL;
}

static int tr_server_shard_listener_fd(struct tr_server_shard *shard)
{
	return shard ? tr_runtime_shard_listener_fd(shard->runtime) : -1;
}

static uint32_t tr_server_shard_peer_capacity(struct tr_server_shard *shard)
{
	return shard ? tr_runtime_shard_peer_capacity(shard->runtime) : 0U;
}

static struct tr_runtime_peer *
tr_server_shard_peer_at(struct tr_server_shard *shard, uint32_t slot)
{
	return shard ? tr_runtime_shard_peer_at(shard->runtime, slot) : NULL;
}

void tr_connection_group_server_config_init(
	struct tr_connection_group_server_config *config)
{
	if (!config)
		return;
	memset(config, 0, sizeof(*config));
}

static int tr_server_connection_group_config_valid(
	const struct tr_connection_group_server_config *config)
{
	if (!config)
		return 0;
	if (config->max_groups == 0U)
		return config->max_connections == 0U &&
		       config->max_data_connections_per_group == 0U &&
		       config->max_streams_per_group == 0U &&
		       config->authorize == NULL &&
		       config->authorize_async == NULL &&
		       config->authorize_cancel == NULL;

	return config->max_connections >= config->max_groups &&
	       config->max_data_connections_per_group != 0U &&
	       config->max_streams_per_group != 0U &&
	       (config->authorize != NULL || config->authorize_async != NULL) &&
	       ((config->authorize_async == NULL) ==
	        (config->authorize_cancel == NULL));
}

static int tr_server_connection_group_authorize(
        const struct tr_connection_group_authorization *authorization,
        void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;

	if (!server || !authorization)
		return TR_ERR_INVALID;
	if (!server->config.connection_groups.authorize)
		return TR_OK;

	return server->config.connection_groups.authorize(
	        authorization, server->config.connection_groups.callback_arg);
}

static enum tr_frame_disposition tr_server_connection_group_data_frame(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;
	struct tr_connection_group_message message;
	enum tr_connection_group_message_disposition disposition;

	(void)connection;
	if (!server || !route || !frame ||
	    frame->header.type != TR_FRAME_DATA ||
	    !server->config.connection_groups.on_message)
		return TR_FRAME_RELEASE;

	memset(&message, 0, sizeof(message));
	message.group.group_id = route->pipeline_id;
	message.group.epoch = route->epoch;
	message.stream_id = frame->header.stream_id;
	message.message_id = frame->header.message_id;
	if (frame->header.flags & TR_FRAME_F_FIRST)
		message.flags |= TR_CONNECTION_GROUP_DATA_FIRST;
	if (frame->header.flags & TR_FRAME_F_LAST)
		message.flags |= TR_CONNECTION_GROUP_DATA_LAST;
	message._private[1] = 1U;
	if (frame->payload) {
		message.bytes.data = frame->payload->data;
		message.bytes.len = frame->payload->len;
		message._private[0] = (uintptr_t)frame->payload;
	}

	disposition = server->config.connection_groups.on_message(
		&message, server->config.connection_groups.callback_arg);
	if (disposition == TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP &&
	    frame->payload)
		return TR_FRAME_TAKE_OWNERSHIP;
	return TR_FRAME_RELEASE;
}

static void tr_server_connection_group_data_event(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;
	struct tr_connection_group_id group;
	enum tr_connection_group_data_event public_event;

	(void)connection;
	if (!server || !route || !server->config.connection_groups.on_data_event)
		return;

	if (event == TR_CONN_EVENT_CLOSED)
		public_event = TR_CONNECTION_GROUP_DATA_CLOSED;
	else if (event == TR_CONN_EVENT_ERROR)
		public_event = TR_CONNECTION_GROUP_DATA_ERROR;
	else
		return;

	group.group_id = route->pipeline_id;
	group.epoch = route->epoch;
	server->config.connection_groups.on_data_event(
		&group, public_event, status,
		server->config.connection_groups.callback_arg);
}

int tr_connection_group_message_release(
	struct tr_connection_group_message *message)
{
	struct tr_buffer *buffer;

	if (!message || message->_private[1] != 1U)
		return TR_ERR_INVALID;

	buffer = (struct tr_buffer *)(uintptr_t)message->_private[0];
	message->_private[0] = 0U;
	message->_private[1] = 0U;
	message->bytes.data = NULL;
	message->bytes.len = 0U;
	if (buffer)
		tr_buffer_release(buffer);
	return TR_OK;
}

static void tr_server_merge_channel_stats(
	struct tr_server_channel_stats *dst,
	const struct tr_channel_stats *src, int current)
{
	if (!dst || !src)
		return;

	if (current)
		dst->active_streams_current += src->active_streams;
	dst->streams_opened += src->streams_opened;
	dst->streams_closed += src->streams_closed;
	dst->stream_errors += src->stream_errors;
	dst->messages_tx += src->messages_tx;
	dst->messages_rx += src->messages_rx;
	dst->bytes_tx += src->bytes_tx;
	dst->bytes_rx += src->bytes_rx;
	dst->window_updates_tx += src->window_updates_tx;
	dst->window_updates_rx += src->window_updates_rx;
	dst->reconnect_attempts += src->reconnect_attempts;
	dst->reconnect_successes += src->reconnect_successes;
	dst->keepalive_pings_sent += src->keepalive_pings_sent;
	dst->keepalive_pongs_received += src->keepalive_pongs_received;
	dst->keepalive_timeouts += src->keepalive_timeouts;
}

static void tr_server_sum_reactor_work(struct tr_reactor_work *dst,
				       const struct tr_reactor_work *src)
{
	dst->commands += src->commands;
	dst->completions += src->completions;
	dst->timer_callbacks += src->timer_callbacks;
	dst->rx_bytes += src->rx_bytes;
	dst->tx_bytes += src->tx_bytes;
	dst->rx_dispatches += src->rx_dispatches;
	dst->tx_dispatches += src->tx_dispatches;
}

static void tr_server_max_reactor_work(struct tr_reactor_work *dst,
				       const struct tr_reactor_work *src)
{
	if (src->commands > dst->commands)
		dst->commands = src->commands;
	if (src->completions > dst->completions)
		dst->completions = src->completions;
	if (src->timer_callbacks > dst->timer_callbacks)
		dst->timer_callbacks = src->timer_callbacks;
	if (src->rx_bytes > dst->rx_bytes)
		dst->rx_bytes = src->rx_bytes;
	if (src->tx_bytes > dst->tx_bytes)
		dst->tx_bytes = src->tx_bytes;
	if (src->rx_dispatches > dst->rx_dispatches)
		dst->rx_dispatches = src->rx_dispatches;
	if (src->tx_dispatches > dst->tx_dispatches)
		dst->tx_dispatches = src->tx_dispatches;
}

static void tr_server_merge_queue_observation(
	struct tr_queue_observation *dst,
	const struct tr_queue_observation *src)
{
	dst->capacity += src->capacity;
	dst->current += src->current;
	dst->peak += src->peak;
	dst->full_events += src->full_events;
}

static void tr_server_merge_pool_observation(
	struct tr_pool_observation *dst,
	const struct tr_pool_observation *src)
{
	dst->capacity += src->capacity;
	dst->current += src->current;
	dst->peak += src->peak;
	dst->exhausted_events += src->exhausted_events;
}

static void tr_server_merge_command_observation(
	struct tr_reactor_command_observation *dst,
	const struct tr_reactor_command_observation *src)
{
	dst->enqueued += src->enqueued;
	dst->full_events += src->full_events;
}

static void tr_server_merge_reactor_stats(struct tr_reactor_stats *dst,
					  const struct tr_reactor_stats *src)
{
	dst->turns += src->turns;
	tr_server_sum_reactor_work(&dst->limits, &src->limits);
	tr_server_sum_reactor_work(&dst->total, &src->total);
	tr_server_max_reactor_work(&dst->max_per_turn, &src->max_per_turn);
	tr_server_sum_reactor_work(&dst->budget_hits, &src->budget_hits);
	dst->epoll_polls += src->epoll_polls;
	dst->epoll_waits += src->epoll_waits;
	if (src->timer_lateness_ns_max > dst->timer_lateness_ns_max)
		dst->timer_lateness_ns_max = src->timer_lateness_ns_max;
	dst->shutdown_completions += src->shutdown_completions;

	tr_server_merge_queue_observation(&dst->command_queue,
					  &src->command_queue);
	tr_server_merge_queue_observation(&dst->completion_queue,
					  &src->completion_queue);
	tr_server_merge_command_observation(&dst->command_send,
					    &src->command_send);
	tr_server_merge_command_observation(&dst->command_resume_rx,
					    &src->command_resume_rx);
	tr_server_merge_command_observation(&dst->command_call,
					    &src->command_call);
	tr_server_merge_command_observation(&dst->command_other,
					    &src->command_other);

	tr_server_merge_pool_observation(&dst->rx_buffer_pool,
					 &src->rx_buffer_pool);
	tr_server_merge_pool_observation(&dst->tx_item_pool,
					 &src->tx_item_pool);
	tr_server_merge_pool_observation(&dst->control_tx_item_pool,
					 &src->control_tx_item_pool);

	dst->observability_flags |= src->observability_flags;
	dst->busy_ns += src->busy_ns;
	dst->poll_ns += src->poll_ns;
	tr_merge_latency_histogram(&dst->turn_busy_ns, &src->turn_busy_ns);
}

static void tr_server_normalize_config(struct tr_server_config *config)
{
	struct tr_server_config defaults;
	struct tr_facade_limits *l;
	const struct tr_facade_limits *d;

	tr_server_config_init(&defaults);
	if (config->shard_count == 0)
		config->shard_count = defaults.shard_count;
	if (config->max_peers == 0)
		config->max_peers = defaults.max_peers;
	if (config->listen_backlog <= 0)
		config->listen_backlog = defaults.listen_backlog;
	if (config->handshake_timeout_ms == 0U)
		config->handshake_timeout_ms = defaults.handshake_timeout_ms;

	l = &config->limits;
	d = &defaults.limits;
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
	TR_LIMIT_DEFAULT(max_pending_tls_operations);
	TR_LIMIT_DEFAULT(max_tls_credential_bundle_bytes);
#undef TR_LIMIT_DEFAULT
}

void tr_server_config_init(struct tr_server_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	tr_facade_limits_init(&config->limits);
	tr_connection_group_server_config_init(&config->connection_groups);
	config->shard_count = 1U;
	config->max_peers = 64U;
	config->listen_backlog = 128;
	config->tcp_nodelay = TR_TCP_NODELAY_DEFAULT;
	config->keepalive_interval_ms = 30000U;
	config->keepalive_timeout_ms = 10000U;
	config->handshake_timeout_ms = 10000U;
	config->tls.mode = TR_TLS_DISABLED;
}

static uint32_t tr_server_budget_share(uint32_t total,
				      uint32_t shard_count,
				      uint32_t shard_index)
{
	uint32_t base = total / shard_count;
	uint32_t remainder = total % shard_count;

	return base + (shard_index < remainder ? 1U : 0U);
}

static int
tr_server_budget_supports_shards(const struct tr_server_config *config,
                                 const struct tr_facade_tuning *tuning,
                                 enum tr_server_mode mode)
{
	uint32_t shards;

	if (!config || !tuning || config->shard_count == 0U)
		return 0;
	shards = config->shard_count;

	return config->max_peers >= shards &&
	       (uint32_t)config->listen_backlog >= shards &&
	       tuning->command_capacity >= shards &&
	       tuning->tx_item_capacity >= shards &&
	       tuning->control_tx_item_capacity >= shards &&
	       tuning->rx_buffer_count >= shards &&
	       (mode != TR_SERVER_MODE_RPC_V2 ||
	        tuning->reassembly_pool_count >= shards);
}

static uint64_t tr_server_record_budget_share(uint64_t total, uint32_t shards,
                                              uint32_t index)
{
	return total / shards + (index < total % shards ? 1U : 0U);
}

static int
tr_server_record_config_valid(const struct tr_server_config *config,
                              const struct tr_server_record_config *record,
                              const struct tr_rpc_dispatch_method *methods,
                              size_t method_count)
{
	const struct tr_rpc_record_endpoint_config *endpoint =
	        &record->endpoint;
	const struct tr_connection_group_server_config *groups =
	        &config->connection_groups;
	uint64_t window = config->limits.initial_window_bytes;
	uint64_t record_limit = config->limits.max_message_bytes;

	/* 方法限额不在线协商，按传输允许上界约束信用更新阈值，
	 * 保证不足 threshold 的已消费字节不会卡住下一条完整记录。
	 */
	if (record_limit > window)
		record_limit = window;

	if (groups->max_groups || groups->max_connections ||
	    groups->max_data_connections_per_group ||
	    groups->max_streams_per_group || groups->authorize ||
	    groups->on_message || groups->on_data_event || groups->callback_arg)
		return TR_ERR_UNSUPPORTED;
	if (!methods || !method_count ||
	    method_count > config->limits.max_methods ||
	    method_count > SIZE_MAX / sizeof(*methods) ||
	    record->memory_budget_bytes < config->shard_count ||
	    record->tasks.workers < config->shard_count ||
	    record->tasks.workers > record->tasks.capacity ||
	    record->tasks.capacity >= UINT32_MAX ||
	    !endpoint->dispatch.capacity ||
	    endpoint->dispatch.capacity > config->limits.max_calls ||
	    endpoint->dispatch.capacity > config->limits.max_streams ||
	    !endpoint->dispatch.max_pending_per_call ||
	    !endpoint->dispatch.max_replies_per_event ||
	    !endpoint->completion_limit || !endpoint->progress_limit ||
	    endpoint->progress_period_ns < 1000000 ||
	    endpoint->progress_period_ns > 1000000000 ||
	    !endpoint->final_wait_ns ||
	    config->limits.max_message_bytes <
	            TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES ||
	    config->limits.initial_window_bytes <
	            TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES ||
	    config->limits.window_update_threshold_bytes >
	            window - record_limit + 1)
		return TR_ERR_INVALID;
	for (size_t i = 0; i < method_count; ++i) {
		const struct tr_rpc_method_desc *method =
		        &methods[i].descriptor;
		struct tr_rpc_record_state state;

		/* 复用记录基数校验；codec 与方法编号仍是 V2 不透明数字。 */
		if ((methods[i].work == NULL) == (methods[i].v2_work == NULL) ||
		    (methods[i].v2_terminal && !methods[i].v2_work) ||
		    !method->request_codec_id || !method->response_codec_id ||
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
			if (methods[j].descriptor.service_id ==
			            method->service_id &&
			    methods[j].descriptor.method_id ==
			            method->method_id)
				return TR_ERR_INVALID;
	}
	return TR_OK;
}

static void tr_server_tuning_ensure_shard_minimums(
	struct tr_facade_tuning *tuning, uint32_t shard_count)
{
	if (!tuning || shard_count == 0U)
		return;

#define TR_TUNING_AT_LEAST_SHARDS(field)       \
	do {                                   \
		if (tuning->field < shard_count) \
			tuning->field = shard_count; \
	} while (0)
	TR_TUNING_AT_LEAST_SHARDS(command_capacity);
	TR_TUNING_AT_LEAST_SHARDS(tx_item_capacity);
	TR_TUNING_AT_LEAST_SHARDS(control_tx_item_capacity);
	TR_TUNING_AT_LEAST_SHARDS(rx_buffer_count);
	TR_TUNING_AT_LEAST_SHARDS(rpc_message_pool_count);
	TR_TUNING_AT_LEAST_SHARDS(reassembly_pool_count);
#undef TR_TUNING_AT_LEAST_SHARDS
}

static int tr_server_peer_disconnected(struct tr_runtime_peer *peer)
{
	enum tr_connection_state state;
	int ret;

	if (!peer->used || !peer->connection.reactor)
		return 0;

	ret = tr_reactor_get_connection_state(peer->connection, &state);
	if (ret == TR_ERR_STALE)
		return 1;
	if (ret != TR_OK)
		return 0;

	return state == TR_CONN_FREE || state == TR_CONN_CLOSED ||
	       state == TR_CONN_ERROR;
}

static void
tr_server_note_peer_added_owner(struct tr_server_shard *shard)
{
	tr_runtime_shard_peer_note_added(shard->runtime);
}

static int tr_server_destroy_peer(struct tr_runtime_peer *peer)
{
	struct tr_server_peer *state;

	if (!peer)
		return TR_OK;
	state = tr_server_peer_state(peer);
	/* V2 共享执行器的完成必须先由分片整体清理，不能逐对端抢先交付。 */
	if (state->record_rpc)
		return TR_ERR_STATE;
	if (peer->channel) {
		int ret = tr_channel_destroy(peer->channel);

		if (ret != TR_OK)
			return ret;
		peer->channel = NULL;
	}

	memset(peer, 0, sizeof(struct tr_server_peer));
	return TR_OK;
}

static void tr_server_record_notify(void *arg)
{
	/* 执行器锁内只唤醒既有 eventfd；定时器保障通知写失败后的进度。 */
	tr_runtime_shard_signal_peer_event(arg);
}

static void tr_server_record_terminal(struct tr_stream_handle stream,
                                      struct tr_rpc_endpoint_result result,
                                      void *arg)
{
	(void)stream;
	(void)result;
	tr_runtime_shard_signal_peer_event(arg);
}

static void tr_server_record_progress(bool endpoint_drained, void *arg)
{
	struct tr_server_shard *shard = arg;
	struct tr_server *server = shard->server;
	int drained = 1, status = TR_OK;
	(void)endpoint_drained;
	if (!shard->record_drain_observing)
		return;
	/* 此时对端表已冻结，查询只在所属 owner 执行。共享执行器的
	 * outstanding 不等同于本分片终局已通知，不能由 worker 直接发布。 */
	for (uint32_t i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		bool quiet;
		struct tr_server_peer *state = tr_server_peer_state(peer);
		if (!peer || !peer->used || !state->record_rpc)
			continue;
		int ret = tr_rpc_record_endpoint_is_drained_on_owner(
		        state->record_rpc, &quiet);
		if (ret != TR_OK) {
			status = ret;
			drained = 0;
			break;
		}
		if (!quiet)
			drained = 0;
	}
	pthread_mutex_lock(&server->finalizer_lock);
	if (shard->record_drained != drained ||
	    shard->record_drain_status != status) {
		shard->record_drained = drained;
		shard->record_drain_status = status;
		pthread_cond_broadcast(&server->record_drain_cond);
	}
	pthread_mutex_unlock(&server->finalizer_lock);
}

static int tr_server_record_watch_drain_on_owner(void *arg)
{
	struct tr_server_shard *shard = arg;
	shard->record_drain_observing = 1;
	for (uint32_t i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		struct tr_server_peer *state = tr_server_peer_state(peer);
		if (!peer || !peer->used || !state->record_rpc)
			continue;
		int ret = tr_rpc_record_endpoint_set_progress_notify_on_owner(
		        state->record_rpc, tr_server_record_progress, shard);
		if (ret != TR_OK)
			return ret;
	}
	/* 空分片也须发布；注册与随后检查共享同一锁，完成先到不会失唤醒。 */
	tr_server_record_progress(false, shard);
	return TR_OK;
}

static int tr_server_record_wait_drained(struct tr_server *server,
                                         const struct tr_deadline *deadline)
{
	int ret = TR_OK;
	pthread_mutex_lock(&server->finalizer_lock);
	for (;;) {
		int drained = 1;
		for (uint32_t i = 0; i < server->shard_count; ++i) {
			struct tr_server_shard *shard = &server->shards[i];
			if (shard->record_drain_status != TR_OK) {
				ret = shard->record_drain_status;
				goto out;
			}
			if (!shard->record_drained)
				drained = 0;
		}
		if (drained)
			break;
		if (!deadline) {
			ret = TR_AGAIN;
			break;
		}
		int wait_ret = pthread_cond_timedwait(
		        &server->record_drain_cond, &server->finalizer_lock,
		        &deadline->absolute);
		if (wait_ret != 0) {
			ret = wait_ret == ETIMEDOUT ? TR_AGAIN : TR_ERR_SYS;
			break;
		}
	}
out:
	pthread_mutex_unlock(&server->finalizer_lock);
	return ret;
}

static void
tr_server_record_detach_peers_on_owner(struct tr_server_shard *shard)
{
	if (!shard->record_cleanup_enabled)
		return;
	for (uint32_t i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		struct tr_channel_stats stats;
		int ret;
		struct tr_server_peer *state = tr_server_peer_state(peer);

		if (!peer || !peer->used)
			continue;
		if (state->record_closing && peer->connection.reactor) {
			ret = tr_reactor_close_on_owner(peer->connection);
			if (ret != TR_OK && ret != TR_ERR_STALE)
				continue;
		} else if (!tr_server_peer_disconnected(peer)) {
			continue;
		}
		/* 工作或解绑尚未静默时保留原槽位，不能制造第二条退役队列。 */
		if (state->record_rpc) {
			ret = tr_rpc_record_endpoint_close_on_owner(
			        &state->record_rpc);
			if (ret != TR_OK)
				continue;
		}
		if (peer->channel) {
			ret = tr_channel_detach_for_finalize(peer->channel);
			if (ret != TR_OK)
				continue;
			if (tr_channel_get_stats(peer->channel, &stats) ==
			    TR_OK)
				tr_server_merge_channel_stats(
				        &shard->retired_channel_stats, &stats,
				        0);
			tr_channel_finalize_detached(peer->channel);
		}
		memset(peer, 0, sizeof(struct tr_server_peer));
		tr_runtime_shard_peer_note_removed_for_reap(shard->runtime);
		tr_runtime_shard_peer_note_reaped(shard->runtime);
	}
}

static uint64_t tr_server_record_cleanup_tick(void *arg, uint64_t now_ns)
{
	struct tr_server_shard *shard = arg;
	uint64_t period = shard->server->record.endpoint.progress_period_ns;

	if (!shard->record_cleanup_enabled)
		return 0;
	tr_server_record_detach_peers_on_owner(shard);
	return period > UINT64_MAX - now_ns ? UINT64_MAX : now_ns + period;
}

static int tr_server_record_start_on_owner(void *arg)
{
	struct tr_server_shard *shard = arg;
	struct tr_server *server = shard->server;
	uint32_t id = tr_runtime_shard_id(shard->runtime);
	struct tr_task_executor_config tasks = {
	        .workers = (size_t)tr_server_record_budget_share(
	                server->record.tasks.workers, server->shard_count, id),
	        .capacity = (size_t)tr_server_record_budget_share(
	                server->record.tasks.capacity, server->shard_count,
	                id)};
	struct timespec now;
	uint64_t deadline;
	int ret;

	/* 生产者租约跨本分片的对端退役共享；应用保留已关闭句柄也不能
	 * 通过重新连接绕过有限配额。域本身与执行器使用同一字节预算。
	 */
	ret = tr_rpc_record_producer_domain_create(
	        tasks.capacity, tr_runtime_shard_memory_budget(shard->runtime),
	        &shard->record_producers);
	if (ret != TR_OK)
		return ret;
	ret = tr_task_executor_create(
	        &tasks, tr_runtime_shard_memory_budget(shard->runtime),
	        &shard->record_tasks);
	if (ret != TR_OK)
		return ret;
	ret = tr_task_executor_set_notify(
	        shard->record_tasks, tr_server_record_notify, shard->runtime);
	if (ret == TR_OK)
		ret = tr_task_executor_start(shard->record_tasks);
	if (ret != TR_OK)
		return ret;
	ret = tr_reactor_timer_register(tr_server_shard_reactor(shard),
	                                tr_server_record_cleanup_tick, shard,
	                                &shard->record_cleanup_timer);
	if (ret != TR_OK)
		return ret;
	shard->record_cleanup_timer_registered = 1;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		return TR_ERR_SYS;
	deadline = (uint64_t)now.tv_sec * 1000000000U + (uint64_t)now.tv_nsec;
	if (server->record.endpoint.progress_period_ns > UINT64_MAX - deadline)
		return TR_ERR_STATE;
	deadline += server->record.endpoint.progress_period_ns;
	ret = tr_reactor_timer_arm(shard->record_cleanup_timer, deadline);
	if (ret == TR_OK)
		shard->record_cleanup_enabled = 1;
	return ret;
}

static int tr_server_record_pause_cleanup(void *arg)
{
	struct tr_server_shard *shard = arg;

	/* call_or_stopped 的已停止分支持有父锁，此处不得重入其他父 API。 */
	shard->record_cleanup_enabled = 0;
	return TR_OK;
}

static int tr_server_record_reap_shard(struct tr_server_shard *shard)
{
	struct tr_task_executor_stats stats;
	int result = TR_OK;
	int ret;
	size_t delivered;

	if (shard->record_tasks_retiring)
		return shard->record_tasks
		               ? tr_task_executor_destroy(&shard->record_tasks)
		               : TR_OK;
	/* 首轮必须覆盖所有共享此执行器的端点。AGAIN 已置清理域；任何
	 * 其他失败都禁止后续交付，避免非 owner 执行尚未切换语义的 done。 */
	for (uint32_t i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		struct tr_server_peer *state = tr_server_peer_state(peer);

		if (!peer || !state->record_rpc)
			continue;
		ret = tr_rpc_record_endpoint_reap_stopped(&state->record_rpc);
		if (ret != TR_OK && ret != TR_AGAIN && result == TR_OK)
			result = ret;
	}
	if (result != TR_OK)
		return result;
	if (shard->record_tasks) {
		ret = tr_task_executor_stop(shard->record_tasks,
		                            TR_TASK_CANCEL);
		if (ret == TR_OK)
			ret = tr_task_executor_adopt_cleanup(
			        shard->record_tasks);
		if (ret == TR_OK)
			ret = tr_task_executor_join(shard->record_tasks);
		if (ret != TR_OK)
			return ret;
		ret = tr_task_executor_dispatch(
		        shard->record_tasks,
		        shard->server->record.endpoint.completion_limit,
		        &delivered);
		if (ret != TR_OK)
			return ret;
		ret = tr_task_executor_get_stats(shard->record_tasks, &stats);
		if (ret != TR_OK)
			return ret;
		if (stats.outstanding)
			return TR_AGAIN;
	}
	for (uint32_t i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		struct tr_server_peer *state = tr_server_peer_state(peer);

		if (!peer || !state->record_rpc)
			continue;
		ret = tr_rpc_record_endpoint_reap_stopped(&state->record_rpc);
		if (ret != TR_OK && (result == TR_OK ||
		                     (result == TR_AGAIN && ret != TR_AGAIN)))
			result = ret;
	}
	if (result != TR_OK || !shard->record_tasks)
		return result;
	ret = tr_task_executor_set_notify(shard->record_tasks, NULL, NULL);
	if (ret != TR_OK)
		return ret;
	/* destroy 的同步原语失败后只允许再次 destroy，不能回到普通 API。 */
	shard->record_tasks_retiring = 1;
	return tr_task_executor_destroy(&shard->record_tasks);
}

static int tr_server_record_reap_all(struct tr_server *server)
{
	int result = TR_OK;

	for (uint32_t i = 0; i < server->shard_count; ++i) {
		int ret = tr_server_record_reap_shard(&server->shards[i]);

		if (ret != TR_OK && (result == TR_OK ||
		                     (result == TR_AGAIN && ret != TR_AGAIN)))
			result = ret;
	}
	return result;
}

static void tr_server_detach_disconnected_peers_on_owner(
	struct tr_server_shard *shard)
{
	if (shard->server->mode != TR_SERVER_MODE_RPC_V2)
		return;
	tr_server_record_detach_peers_on_owner(shard);
}

static void tr_server_on_peer_lifecycle_event(int fd, uint32_t events,
					      void *arg)
{
	struct tr_server_shard *shard = (struct tr_server_shard *)arg;

	(void)fd;
	if (!shard || !(events & EPOLLIN))
		return;

	tr_runtime_shard_drain_peer_event(shard->runtime);
	tr_server_detach_disconnected_peers_on_owner(shard);
}

static int tr_server_disable_peer_events(struct tr_server_shard *shard)
{
	int ret;

	if (!shard)
		return TR_ERR_INVALID;
	if (shard->server->mode == TR_SERVER_MODE_RPC_V2 &&
	    (shard->record_cleanup_enabled ||
	     shard->record_cleanup_timer_registered)) {
		ret = tr_reactor_call_or_stopped(tr_server_shard_reactor(shard),
		                                 tr_server_record_pause_cleanup,
		                                 shard);
		if (ret != TR_OK)
			return ret;
		if (shard->record_cleanup_timer_registered) {
			ret = tr_reactor_timer_unregister(
			        shard->record_cleanup_timer);
			if (ret != TR_OK)
				return ret;
			shard->record_cleanup_timer_registered = 0;
		}
	}
	if (!shard->peer_events_enabled)
		return TR_OK;

	ret = tr_runtime_shard_disable_peer_events(shard->runtime);
	if (ret != TR_OK)
		return ret;
	shard->peer_events_enabled = 0;
	return TR_OK;
}

static void tr_server_signal_peer_cleanup(struct tr_server_shard *shard)
{
	if (shard)
		tr_runtime_shard_signal_peer_event(shard->runtime);
}

/* 每分片复用一个有界定时器，只扫描当前对端槽位，不保留旧代次指针。 */
static uint64_t tr_server_handshake_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_server_shard *shard = (struct tr_server_shard *)arg;
	uint64_t next = 0U;
	uint32_t i;

	for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
			tr_server_shard_peer_at(shard, i);
		uint64_t deadline;
		int ret;

		if (!peer || !peer->used || peer->ready ||
		    tr_server_peer_disconnected(peer))
			continue;
		deadline = tr_deadline_to_ns(&peer->handshake_deadline);
		if (deadline <= now_ns) {
			ret = tr_reactor_abort_on_owner(peer->connection,
							TR_ERR_TIMEOUT);
			if (ret == TR_OK || ret == TR_ERR_STALE) {
				tr_server_signal_peer_cleanup(shard);
				continue;
			}
			/* 退役失败仍保留租约，下个所有者处理轮次重试。 */
			deadline = now_ns == UINT64_MAX ? now_ns : now_ns + 1U;
		}
		if (next == 0U || deadline < next)
			next = deadline;
	}
	return next;
}

static int tr_server_arm_handshake_on_owner(struct tr_server_shard *shard,
					    struct tr_runtime_peer *peer)
{
	uint64_t next = tr_deadline_to_ns(&peer->handshake_deadline);
	uint32_t i;
	int ret;

	if (!shard->handshake_timer_registered) {
		ret = tr_reactor_timer_register(tr_server_shard_reactor(shard),
						tr_server_handshake_timer_main,
						shard, &shard->handshake_timer);
		if (ret != TR_OK)
			return ret;
		shard->handshake_timer_registered = 1;
	}
	for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *pending =
			tr_server_shard_peer_at(shard, i);
		uint64_t deadline;

		if (!pending || !pending->used || pending->ready ||
		    tr_server_peer_disconnected(pending))
			continue;
		deadline = tr_deadline_to_ns(&pending->handshake_deadline);
		if (deadline < next)
			next = deadline;
	}
	/* 设置定时失败不得发布无唤醒保障的新连接；旧定时器状态保持不变。 */
	return tr_reactor_timer_arm(shard->handshake_timer, next);
}

static void tr_server_on_channel_lifecycle(struct tr_channel *channel,
					   enum tr_channel_event event,
					   int status, void *arg)
{
	struct tr_server_shard *shard = (struct tr_server_shard *)arg;

	(void)status;
	if (event == TR_CHANNEL_EVENT_CONTROL_UP) {
		uint32_t i;

		for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
			struct tr_runtime_peer *peer =
				tr_server_shard_peer_at(shard, i);
			int expired;
			int ret;

			if (!peer || peer->channel != channel || peer->ready)
				continue;
			/* 接收事件与到期定时器同轮就绪时，晚到的 HELLO
			 * 不能越过租约。 */
			ret = tr_deadline_expired(&peer->handshake_deadline,
						  &expired);
			if (ret != TR_OK || expired) {
				(void)tr_reactor_abort_on_owner(
					peer->connection,
					ret != TR_OK ? ret : TR_ERR_TIMEOUT);
				tr_server_signal_peer_cleanup(shard);
				return;
			}
			peer->ready = 1;
			tr_runtime_shard_peer_note_ready(shard->runtime);
			break;
		}
	}
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN)
		tr_server_signal_peer_cleanup(shard);
}

static int tr_server_adopt_peer_rollback(
	struct tr_server_shard *shard, struct tr_runtime_peer *peer, int cause)
{
	int close_ret = TR_OK;
	struct tr_server_peer *state;

	if (!shard || !peer)
		return cause;
	state = tr_server_peer_state(peer);
	if (shard->server->mode == TR_SERVER_MODE_RPC_V2)
		state->record_closing = 1;

	/* tr_server_adopt_peer() 只在分片反应器所有者回调中执行。
	 * 回滚不再把 CLOSE 重新排队，而是在当前所有者处理轮次直接退役精确
	 * 连接；因此不受命令队列容量影响，也不会留下 ACTIVE 的部分对端
	 * 等待一个永远没有提交成功的关闭命令。
	 */
	if (peer->connection.reactor) {
		close_ret = tr_reactor_close_on_owner(peer->connection);
		if (close_ret == TR_ERR_STALE)
			close_ret = TR_OK;
	}

	/* 一旦 Channel/Endpoint 发布已经建立，不能在当前所有者回调中
	 * 同步销毁。先发布部分对端，延迟到后续分片生命周期处理轮次做
	 * 解绑与收尾；连接已在上面完成同步退役。
	 */
	if (peer->channel || state->record_rpc ||
	    (shard->server->mode == TR_SERVER_MODE_RPC_V2 &&
	     close_ret != TR_OK && peer->connection.reactor)) {
		peer->used = 1;
		tr_server_note_peer_added_owner(shard);
		tr_server_signal_peer_cleanup(shard);
	} else {
		memset(peer, 0, sizeof(struct tr_server_peer));
	}

	return close_ret != TR_OK ? close_ret : cause;
}

static int tr_server_adopt_peer(struct tr_server_shard *shard, int fd)
{
	struct tr_server *server = shard->server;
	struct tr_channel_config channel_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_runtime_peer *peer;
	struct tr_server_peer *state;
	uint32_t slot;
	int owned_fd TR_AUTO(tr_fd_cleanup) = fd;
	int ret;

	{
		struct tr_runtime_peer_stats peer_stats;

		tr_runtime_shard_peer_stats(shard->runtime, &peer_stats);
		if (peer_stats.current >= peer_stats.capacity) {
			tr_runtime_shard_peer_note_capacity_rejection(
				shard->runtime);
			return TR_AGAIN;
		}
	}

	for (slot = 0; slot < tr_server_shard_peer_capacity(shard); ++slot) {
		peer = tr_server_shard_peer_at(shard, slot);
		if (peer && !peer->used)
			break;
	}
	if (slot == tr_server_shard_peer_capacity(shard)) {
		tr_runtime_shard_peer_note_capacity_rejection(shard->runtime);
		return TR_AGAIN;
	}

	peer = tr_server_shard_peer_at(shard, slot);
	state = tr_server_peer_state(peer);
	memset(peer, 0, sizeof(struct tr_server_peer));
	ret = tr_deadline_init_ms(&peer->handshake_deadline,
				  server->config.handshake_timeout_ms);
	if (ret != TR_OK)
		return ret;
	ret = tr_server_arm_handshake_on_owner(shard, peer);
	if (ret != TR_OK)
		return ret;
	if (tr_tcp_nodelay_policy_enabled(server->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(owned_fd, 1);
		if (ret != TR_OK)
			goto rollback;
	}

	if (shard->tls_profile) {
		ret = tr_reactor_adopt_tls_fd_on_owner(
		        tr_server_shard_reactor(shard), owned_fd,
		        shard->tls_profile,
		        tr_deadline_to_ns(&peer->handshake_deadline),
		        &peer->connection);
	} else {
		ret = tr_reactor_adopt_fd(tr_server_shard_reactor(shard),
		                          owned_fd, &peer->connection);
	}
	if (ret != TR_OK)
		goto rollback;
	(void)tr_fd_take(&owned_fd);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = server->config.limits.max_streams;
	channel_config.initial_window_bytes =
		server->config.limits.initial_window_bytes;
	channel_config.window_update_threshold_bytes =
		server->config.limits.window_update_threshold_bytes;
	channel_config.max_message_bytes =
		server->config.limits.max_message_bytes;
	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		channel_config.reassembly_pool = &shard->reassembly_pool;
		channel_config.memory_budget =
		        tr_runtime_shard_memory_budget(shard->runtime);
		channel_config.feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET;
		channel_config.required_feature_bits =
		        TR_CHANNEL_FEATURE_STREAM_RESET;
	}

	/* 服务端必须先完成 RPC Endpoint/Method 安装，再启动 HELLO。
	 * 否则客户端可能先观察到 Channel UP，并在服务端服务就绪前
	 * 发送首条 RPC，导致消息被尚未安装上层处理函数的 Channel 消费。
	 */
	ret = tr_channel_create_deferred(&channel_config, peer->connection,
					 peer->connection, NULL, NULL, NULL, NULL,
					 &peer->channel);
	if (ret != TR_OK)
		goto rollback;

	ret = tr_channel_set_lifecycle_observer(
		peer->channel, tr_server_on_channel_lifecycle, shard);
	if (ret != TR_OK)
		goto rollback;

	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		ret = tr_rpc_record_endpoint_create_on_owner(
		        peer->channel, shard->record_tasks,
		        server->record_methods, server->record_method_count,
		        &server->record.endpoint, tr_server_record_terminal,
		        shard->runtime,
		        tr_runtime_shard_memory_budget(shard->runtime),
		        &state->record_rpc);
		if (ret == TR_OK)
			ret = tr_rpc_record_endpoint_bind_semantic_stats_on_owner(
			        state->record_rpc, &shard->record_semantic);
		if (ret == TR_OK)
			ret = tr_rpc_record_endpoint_bind_producers_on_owner(
			        state->record_rpc, shard->record_producers);
		if (ret == TR_OK)
			ret = tr_rpc_record_endpoint_start_on_owner(
			        state->record_rpc);
	}
	if (ret != TR_OK)
		goto rollback;

	ret = tr_channel_start(peer->channel);
	if (ret != TR_OK)
		goto rollback;

	if (server->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			server->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			server->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(peer->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			goto rollback;
	}

	peer->used = 1;
	tr_server_note_peer_added_owner(shard);
	/* 覆盖连接在此对端进入所有者表可见范围前已变为 DOWN 的竞争。
	 * 分片 eventfd 合并该重试。
	 */
	tr_server_signal_peer_cleanup(shard);
	return TR_OK;

rollback:
	return tr_server_adopt_peer_rollback(shard, peer, ret);
}

static void tr_server_on_listener_ready(int listener, uint32_t events,
					void *arg)
{
	struct tr_server_shard *shard = (struct tr_server_shard *)arg;
	uint32_t accepted = 0U;

	if (!shard || !(events & EPOLLIN))
		return;

	while (accepted < TR_SERVER_ACCEPT_BATCH) {
		int fd = -1;
		int ret = tr_tcp_accept(listener, &fd);

		if (ret == TR_AGAIN)
			break;
		if (ret != TR_OK)
			break;

		/* 在分片反应器所有者上运行。adopt_fd 走所有者快速路径，
		 * 使 Channel/RPC 设置立即看到 ACTIVE 连接。
		 */
		(void)tr_server_adopt_peer(shard, fd);
		accepted++;
	}
}

static int
tr_server_create_internal(enum tr_server_mode mode,
                          const struct tr_server_config *config,
                          const struct tr_facade_tuning *tuning,
                          const struct tr_server_record_config *record,
                          const struct tr_rpc_dispatch_method *record_methods,
                          size_t record_method_count, struct tr_server **out)
{
	struct tr_server_config effective;
	struct tr_facade_tuning effective_tuning;
	struct tr_runtime_config runtime_config;
	struct tr_runtime_shard_config *shard_configs = NULL;
	struct tr_server *server_mem TR_AUTO(tr_server_mem_cleanup) = NULL;
	struct tr_server *server = NULL;
	int ret;
	int credentials_provider_enabled;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if ((mode != TR_SERVER_MODE_RPC_V2 &&
	     mode != TR_SERVER_MODE_TRANSPORT_ONLY) ||
	    ((mode == TR_SERVER_MODE_RPC_V2) != (record != NULL)))
		return TR_ERR_INVALID;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;

	if (config)
		effective = *config;
	else
		tr_server_config_init(&effective);
	tr_server_normalize_config(&effective);
	ret = tr_tls_credentials_provider_validate(
	        &effective.tls_credentials_provider, effective.tls.mode);
	if (ret != TR_OK)
		return ret;
	credentials_provider_enabled =
	        effective.tls_credentials_provider.fetch != NULL;
	ret = credentials_provider_enabled && !effective.tls.certificate_file &&
	                      !effective.tls.private_key_file
	              ? tr_tls_server_config_validate_for_pem(
	                        &effective.tls,
	                        mode == TR_SERVER_MODE_RPC_V2 ||
	                                effective.connection_groups
	                                                .max_groups != 0U)
	              : tr_tls_server_config_validate(
	                        &effective.tls,
	                        mode == TR_SERVER_MODE_RPC_V2 ||
	                                effective.connection_groups
	                                                .max_groups != 0U);
	if (ret != TR_OK)
		return ret;
	if (effective.tls.mode == TR_TLS_REQUIRED &&
	    effective.connection_groups.max_groups != 0U) {
		if (mode != TR_SERVER_MODE_TRANSPORT_ONLY)
			return TR_ERR_UNSUPPORTED;
		if (effective.tls.client_auth != TR_TLS_CLIENT_AUTH_REQUIRED)
			return TR_ERR_INVALID;
	}
	if (effective.tls.mode == TR_TLS_REQUIRED &&
	    mode == TR_SERVER_MODE_TRANSPORT_ONLY &&
	    effective.connection_groups.max_groups == 0U)
		return TR_ERR_UNSUPPORTED;

	if (tuning)
		effective_tuning = *tuning;
	else
		tr_facade_tuning_init(&effective_tuning);
	tr_facade_tuning_normalize(&effective_tuning);
	if (!tuning)
		tr_server_tuning_ensure_shard_minimums(
			&effective_tuning, effective.shard_count);
	if (mode == TR_SERVER_MODE_RPC_V2) {
		ret = tr_server_record_config_valid(&effective, record,
		                                    record_methods,
		                                    record_method_count);
		if (ret != TR_OK)
			return ret;
	}

	if (!tr_tcp_nodelay_policy_valid(effective.tcp_nodelay) ||
	    (effective_tuning.observability_flags &
	     ~TR_OBSERVABILITY_VALID_FLAGS) ||
	    effective.limits.max_message_bytes <
	            effective.limits.max_frame_payload_bytes ||
	    effective.max_peers == 0 ||
	    effective.max_peers > (UINT32_MAX - 4U) / 2U ||
	    !tr_server_connection_group_config_valid(
	            &effective.connection_groups) ||
	    effective.connection_groups.max_connections >
	            UINT32_MAX - effective.max_peers - 4U ||
	    !tr_server_budget_supports_shards(&effective, &effective_tuning,
	                                      mode))
		return TR_ERR_INVALID;

	server_mem = (struct tr_server *)calloc(1, sizeof(*server_mem));
	if (!server_mem)
		return TR_ERR_NOMEM;
	server_mem->config = effective;
	server_mem->mode = mode;
	if (record) {
		server_mem->record = *record;
		tr_memory_budget_init(&server_mem->memory_budget,
		                      record->memory_budget_bytes);
	}

	if (pthread_mutex_init(&server_mem->finalizer_lock, NULL) != 0)
		return TR_ERR_INVALID;
	server = tr_server_mem_take(&server_mem);
	atomic_init(&server->tls_rotation_failed, 0);
	atomic_init(&server->tls_update_active, 0);
	server->tls_client_ca_file_copy =
	        tr_server_copy_optional_string(effective.tls.client_ca_file);
	if (effective.tls.client_ca_file && !server->tls_client_ca_file_copy)
		return tr_server_create_rollback(out, server, TR_ERR_NOMEM);
	server->config.tls.client_ca_file = server->tls_client_ca_file_copy;
	if (credentials_provider_enabled) {
		if (pthread_mutex_init(&server->tls_provider_lock, NULL) != 0)
			return tr_server_create_rollback(out, server,
			                                 TR_ERR_SYS);
		server->tls_provider_lock_ready = 1;
	}
	if (effective.tls.mode == TR_TLS_REQUIRED &&
	    (effective.tls.certificate_file || effective.tls.private_key_file ||
	     !credentials_provider_enabled)) {
		ret = tr_tls_openssl_server_profile_create(
		        &effective.tls, &server->tls_profile);
		if (ret != TR_OK)
			return tr_server_create_rollback(out, server, ret);
	}
	server->config.tls.certificate_file = NULL;
	server->config.tls.private_key_file = NULL;
	if (mode == TR_SERVER_MODE_RPC_V2) {
		pthread_condattr_t attr;
		if (pthread_condattr_init(&attr) != 0)
			return tr_server_create_rollback(out, server,
			                                 TR_ERR_SYS);
		ret = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
		if (ret == 0)
			ret = pthread_cond_init(&server->record_drain_cond,
			                        &attr);
		if (ret == 0)
			server->record_drain_cond_ready = 1;
		int cleanup_ret = pthread_condattr_destroy(&attr);
		if (ret != 0 || cleanup_ret != 0)
			return tr_server_create_rollback(out, server,
			                                 TR_ERR_SYS);
	}

	memset(&runtime_config, 0, sizeof(runtime_config));
	shard_configs = (struct tr_runtime_shard_config *)calloc(
		effective.shard_count, sizeof(*shard_configs));
	if (!shard_configs)
		return tr_server_create_rollback(out, server, TR_ERR_NOMEM);

	runtime_config.shard_count = effective.shard_count;
	runtime_config.shards = shard_configs;

	{
		uint32_t i;

		for (i = 0; i < effective.shard_count; ++i) {
			struct tr_runtime_shard_config *shard_config =
				&shard_configs[i];
			struct tr_reactor_config *reactor_config =
				&shard_config->reactor;
			uint32_t peer_capacity = tr_server_budget_share(
				effective.max_peers, effective.shard_count, i);

			shard_config->peer_capacity = peer_capacity;
			shard_config->peer_stride =
			        sizeof(struct tr_server_peer);
			if (mode == TR_SERVER_MODE_RPC_V2) {
				shard_config->memory_budget_bytes =
				        record->memory_budget_bytes;
				shard_config->memory_budget_parent =
				        &server->memory_budget;
			}

			reactor_config->max_connections =
				peer_capacity + 4U +
				(i == 0U ?
				 effective.connection_groups.max_connections :
				 0U);
			reactor_config->command_capacity =
				tr_server_budget_share(
					effective_tuning.command_capacity,
					effective.shard_count, i);
			reactor_config->tx_item_capacity =
				tr_server_budget_share(
					effective_tuning.tx_item_capacity,
					effective.shard_count, i);
			reactor_config->control_tx_item_capacity =
				tr_server_budget_share(
					effective_tuning.control_tx_item_capacity,
					effective.shard_count, i);
			reactor_config->rx_buffer_count =
				tr_server_budget_share(
					effective_tuning.rx_buffer_count,
					effective.shard_count, i);
			reactor_config->rx_buffer_size =
				effective.limits.max_frame_payload_bytes;
			reactor_config->max_payload_len =
				effective.limits.max_frame_payload_bytes;
			reactor_config->rx_budget_bytes =
				effective.limits.max_frame_payload_bytes >
						UINT32_MAX / 4U ?
					UINT32_MAX :
					effective.limits.max_frame_payload_bytes *
						4U;
			reactor_config->tx_budget_bytes =
				reactor_config->rx_budget_bytes;
			reactor_config->observability_flags =
				effective_tuning.observability_flags;
		}
	}

	ret = tr_runtime_create(&runtime_config, &server->runtime);
	free(shard_configs);
	shard_configs = NULL;
	if (ret != TR_OK)
		return tr_server_create_rollback(out, server, ret);

	{
		uint32_t shard_count = tr_runtime_shard_count(server->runtime);
		uint32_t i;

		/* 分配成功前不发布可遍历数量，回滚仍可安全处理空数组。 */
		server->shards = (struct tr_server_shard *)calloc(
			shard_count, sizeof(*server->shards));
		if (!server->shards)
			return tr_server_create_rollback(out, server, TR_ERR_NOMEM);

		for (i = 0; i < shard_count; ++i) {
			struct tr_server_shard *server_shard =
				&server->shards[i];

			server_shard->server = server;
			server_shard->runtime =
			        tr_runtime_shard_at(server->runtime, i);
			if (!server_shard->runtime)
				return tr_server_create_rollback(
					out, server, TR_ERR_STATE);

			/* 先发布有效 Runtime 绑定，再获取分片资源。后续失败时，
			 * 当前分片由各 pool_ready 标志精确回收；尚未绑定的尾部
			 * 条目不进入销毁和收尾器等待路径。
			 */
			server->shard_count = i + 1U;
			if (server->tls_profile) {
				ret = tr_tls_openssl_profile_retain(
				        server->tls_profile);
				if (ret != TR_OK)
					return tr_server_create_rollback(
					        out, server, ret);
				server_shard->tls_profile = server->tls_profile;
			}

			if (mode == TR_SERVER_MODE_RPC_V2) {
				ret = tr_buffer_pool_init_budgeted(
				        &server_shard->reassembly_pool,
				        tr_server_budget_share(
				                effective_tuning
				                        .reassembly_pool_count,
				                effective.shard_count, i),
				        effective.limits.max_message_bytes,
				        tr_runtime_shard_memory_budget(
				                server_shard->runtime));
				if (ret != TR_OK)
					return tr_server_create_rollback(
					        out, server, ret);
				server_shard->reassembly_pool_ready = 1;
			}
		}
	}
	if (credentials_provider_enabled ||
	    effective.connection_groups.authorize_async) {
		ret = tr_tls_provider_operation_table_init(
		        &server->tls_operations,
		        effective.limits.max_pending_tls_operations,
		        tr_runtime_shard_memory_budget(
		                server->shards[0].runtime));
		if (ret != TR_OK)
			return tr_server_create_rollback(out, server, ret);
		server->tls_operations_ready = 1;
		if (credentials_provider_enabled) {
			ret = tr_reactor_timer_register(
			        tr_server_shard_reactor(&server->shards[0]),
			        tr_server_tls_credentials_deadline_tick, server,
			        &server->tls_credentials_timer);
			if (ret != TR_OK)
				return tr_server_create_rollback(out, server,
				                                 ret);
			server->tls_credentials_timer_registered = 1;
		}
	}
	if (mode == TR_SERVER_MODE_RPC_V2) {
		struct tr_memory_budget *budget =
		        tr_runtime_shard_memory_budget(
		                server->shards[0].runtime);
		size_t bytes = record_method_count * sizeof(*record_methods);

		ret = tr_memory_budget_reserve(budget, bytes);
		if (ret != TR_OK)
			return tr_server_create_rollback(out, server, ret);
		server->record_method_bytes = bytes;
		server->record_methods = malloc(bytes);
		if (!server->record_methods)
			return tr_server_create_rollback(out, server,
			                                 TR_ERR_NOMEM);
		memcpy(server->record_methods, record_methods, bytes);
		server->record_method_count = record_method_count;
	}

	if (effective.connection_groups.max_groups != 0U) {
		struct tr_pipeline_listener_config group_config;
		struct tr_server_shard *owner_shard = &server->shards[0];

		memset(&group_config, 0, sizeof(group_config));
		group_config.owner = tr_server_shard_reactor(owner_shard);
		group_config.owner_shard_id =
			tr_runtime_shard_id(owner_shard->runtime);
		group_config.pipeline_capacity =
			effective.connection_groups.max_groups;
		group_config.connection_capacity =
			effective.connection_groups.max_connections;
		group_config.handshake_timeout_ms =
			effective.handshake_timeout_ms;
		group_config.data_capacity_per_pipeline =
			effective.connection_groups.max_data_connections_per_group;
		group_config.stream_affinity_capacity_per_pipeline =
			effective.connection_groups.max_streams_per_group;
		/* 将 CONTROL 负载缓冲调优保留在内部。组连接总预算
		 * 为并发控制发送提供保守的有界默认值，
		 * 无需向 SDK 添加实现调节项。
		 */
		group_config.control_message_count =
			effective.connection_groups.max_connections;
		group_config.authorization_capacity =
		        effective.limits.max_pending_tls_operations <
		                        effective.connection_groups
		                                .max_connections
		                ? effective.limits.max_pending_tls_operations
		                : effective.connection_groups.max_connections;
		group_config.tls_profile = owner_shard->tls_profile;
		group_config.authorization_operations =
		        effective.connection_groups.authorize_async
		                ? &server->tls_operations
		                : NULL;
		group_config.authorize_control =
			tr_server_connection_group_authorize;
		group_config.authorize_arg = server;
		group_config.authorize_async =
		        effective.connection_groups.authorize_async;
		group_config.authorize_cancel =
		        effective.connection_groups.authorize_cancel;
		group_config.authorize_async_arg =
		        effective.connection_groups.callback_arg;
		group_config.data_frame_cb =
			tr_server_connection_group_data_frame;
		group_config.data_event_cb =
			tr_server_connection_group_data_event;
		group_config.data_callback_arg = server;

		ret = tr_pipeline_listener_create(
			&group_config, &server->connection_group_listener);
		if (ret != TR_OK)
			return tr_server_create_rollback(out, server, ret);
	}

	/* 全部分片和可选监听器均已构造完成后才发布可读状态。 */
	server->published = 1;
	*out = server;
	return TR_OK;
}

int tr_server_create_record(const struct tr_server_config *config,
                            const struct tr_facade_tuning *tuning,
                            const struct tr_server_record_config *record,
                            const struct tr_rpc_dispatch_method *methods,
                            size_t method_count, struct tr_server **out)
{
	if (!record) {
		if (out)
			*out = NULL;
		return TR_ERR_INVALID;
	}
	return tr_server_create_internal(TR_SERVER_MODE_RPC_V2, config, tuning,
	                                 record, methods, method_count, out);
}

int tr_server_create_transport(const struct tr_server_config *config,
                               struct tr_server **out)
{
	struct tr_facade_tuning tuning;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	tr_facade_tuning_init(&tuning);
	if (config)
		tr_server_tuning_ensure_shard_minimums(&tuning,
		                                       config->shard_count);
	else
		tr_server_tuning_ensure_shard_minimums(&tuning, 1U);
	return tr_server_create_internal(TR_SERVER_MODE_TRANSPORT_ONLY, config,
	                                 &tuning, NULL, NULL, 0U, out);
}

struct tr_server_tls_install_request {
	struct tr_server *server;
	struct tr_server_shard *shard;
	struct tr_tls_openssl_profile *profile;
};

static int tr_server_tls_install_on_owner(void *arg)
{
	struct tr_server_tls_install_request *request = arg;
	struct tr_server_shard *shard;
	struct tr_tls_openssl_profile *old_profile;
	int ret;

	if (!request || !request->server || !request->shard ||
	    request->shard->server != request->server)
		return TR_ERR_INVALID;
	shard = request->shard;
	if (shard->tls_profile == request->profile)
		return TR_OK;
	if (request->profile) {
		ret = tr_tls_openssl_profile_retain(request->profile);
		if (ret != TR_OK)
			return ret;
	}
	if (request->server->connection_group_listener &&
	    tr_runtime_shard_id(shard->runtime) == 0U) {
		ret = tr_pipeline_listener_set_tls_profile_on_owner(
		        request->server->connection_group_listener,
		        request->profile);
		if (ret != TR_OK) {
			tr_tls_openssl_profile_release(request->profile);
			return ret;
		}
	}
	old_profile = shard->tls_profile;
	shard->tls_profile = request->profile;
	tr_tls_openssl_profile_release(old_profile);
	return TR_OK;
}

static int tr_server_tls_install_all(struct tr_server *server,
                                     struct tr_tls_openssl_profile *profile,
                                     int reverse)
{
	uint32_t offset;

	for (offset = 0U; offset < server->shard_count; ++offset) {
		uint32_t index;
		if (reverse) {
			index = server->shard_count - offset - 1U;
		} else if (server->connection_group_listener &&
		           server->shard_count > 1U) {
			/* Listener 更新留在最后，前面 shard 的部分安装在失败时
			 * 走真实回滚路径。 */
			index = offset + 1U < server->shard_count ? offset + 1U
			                                          : 0U;
		} else {
			index = offset;
		}
		struct tr_server_shard *shard = &server->shards[index];
		struct tr_server_tls_install_request request = {server, shard,
		                                                profile};
		int ret = tr_reactor_call_or_stopped(
		        tr_server_shard_reactor(shard),
		        tr_server_tls_install_on_owner, &request);
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

static int tr_server_tls_gate_all(struct tr_server *server, int paused)
{
	uint32_t i;
	int result = TR_OK;

	for (i = 0U; i < server->shard_count; ++i) {
		int ret = tr_reactor_tls_admission_set(
		        tr_server_shard_reactor(&server->shards[i]), paused);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	return result;
}

static int tr_server_tls_close_generation_all(struct tr_server *server,
                                              uint64_t generation)
{
	uint32_t i;
	int result = TR_OK;

	for (i = 0U; i < server->shard_count; ++i) {
		int ret = tr_reactor_tls_close_generation(
		        tr_server_shard_reactor(&server->shards[i]),
		        generation);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	return result;
}

static int tr_server_tls_close_before_generation_all(struct tr_server *server,
                                                     uint64_t generation)
{
	uint32_t i;
	int result = TR_OK;

	for (i = 0U; i < server->shard_count; ++i) {
		int ret = tr_reactor_tls_close_before_generation(
		        tr_server_shard_reactor(&server->shards[i]),
		        generation);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	return result;
}

static void tr_server_tls_rotation_mark_failed(struct tr_server *server)
{
	atomic_store_explicit(&server->tls_rotation_failed, 1,
	                      memory_order_release);
	server->published = 0;
}

/* candidate 的初始引用在本函数内转交给 Server，失败回滚收敛时释放。 */
static int tr_server_tls_publish_candidate_impl(
        struct tr_server *server, struct tr_tls_openssl_profile *candidate,
        enum tr_tls_rotation_policy policy, uint64_t deadline_ns,
        uint64_t *out_generation)
{
	struct tr_tls_openssl_profile *old_profile;
	uint64_t candidate_generation = 1U;
	int ret;
	int rollback_ret;

	if (!server || !candidate ||
	    (policy != TR_TLS_ROTATE_GRACEFUL &&
	     policy != TR_TLS_ROTATE_REVOKE_OLD)) {
		tr_tls_openssl_profile_release(candidate);
		return TR_ERR_INVALID;
	}
	if (tr_server_blocking_lifecycle_context() ||
	    atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire) ||
	    !server->published || !server->runtime || !server->shards ||
	    server->shard_count == 0U) {
		tr_tls_openssl_profile_release(candidate);
		return TR_ERR_STATE;
	}
	old_profile = server->tls_profile;
	if (old_profile) {
		ret = tr_tls_openssl_profile_next_generation(
		        old_profile, &candidate_generation);
		if (ret != TR_OK) {
			tr_tls_openssl_profile_release(candidate);
			return ret;
		}
	}
	ret = tr_tls_openssl_profile_set_generation(candidate,
	                                            candidate_generation);
	if (ret != TR_OK) {
		tr_tls_openssl_profile_release(candidate);
		return ret;
	}
	ret = tr_server_tls_gate_all(server, 1);
	if (ret != TR_OK) {
		rollback_ret = tr_server_tls_gate_all(server, 0);
		if (rollback_ret != TR_OK)
			tr_server_tls_rotation_mark_failed(server);
		tr_tls_openssl_profile_release(candidate);
		return ret;
	}
	ret = tr_server_tls_install_all(server, candidate, 0);
	if (ret != TR_OK) {
		rollback_ret =
		        tr_server_tls_install_all(server, old_profile, 1);
		if (rollback_ret != TR_OK) {
			tr_server_tls_rotation_mark_failed(server);
			tr_tls_openssl_profile_release(candidate);
			return TR_ERR_STATE;
		}
		rollback_ret = tr_server_tls_gate_all(server, 0);
		if (rollback_ret != TR_OK)
			tr_server_tls_rotation_mark_failed(server);
		tr_tls_openssl_profile_release(candidate);
		return rollback_ret == TR_OK ? ret : TR_ERR_STATE;
	}
	if (deadline_ns != 0U) {
		uint64_t now_ns;
		ret = tr_server_monotonic_now_ns(&now_ns);
		if (ret != TR_OK || now_ns >= deadline_ns) {
			if (ret == TR_OK)
				ret = TR_ERR_TIMEOUT;
			rollback_ret = tr_server_tls_install_all(
			        server, old_profile, 1);
			if (rollback_ret == TR_OK)
				rollback_ret =
				        tr_server_tls_gate_all(server, 0);
			if (rollback_ret != TR_OK) {
				tr_server_tls_rotation_mark_failed(server);
				tr_tls_openssl_profile_release(candidate);
				return TR_ERR_STATE;
			}
			tr_tls_openssl_profile_release(candidate);
			return ret;
		}
	}

	server->tls_profile = candidate;
	server->tls_recovery_profile = old_profile;
	if (policy == TR_TLS_ROTATE_REVOKE_OLD) {
		ret = tr_server_tls_close_before_generation_all(
		        server, candidate_generation);
		if (ret != TR_OK) {
			tr_server_tls_rotation_mark_failed(server);
			return TR_ERR_STATE;
		}
	}
	ret = tr_server_tls_gate_all(server, 0);
	if (ret == TR_OK) {
		server->tls_recovery_profile = NULL;
		tr_tls_openssl_profile_release(old_profile);
		if (out_generation)
			*out_generation = candidate_generation;
		return TR_OK;
	}

	rollback_ret = tr_server_tls_gate_all(server, 1);
	if (rollback_ret == TR_OK)
		rollback_ret = tr_server_tls_close_generation_all(
		        server, candidate_generation);
	if (rollback_ret == TR_OK)
		rollback_ret =
		        tr_server_tls_install_all(server, old_profile, 1);
	if (rollback_ret != TR_OK) {
		tr_server_tls_rotation_mark_failed(server);
		return TR_ERR_STATE;
	}
	server->tls_profile = old_profile;
	server->tls_recovery_profile = NULL;
	tr_tls_openssl_profile_release(candidate);
	rollback_ret = tr_server_tls_gate_all(server, 0);
	if (rollback_ret != TR_OK) {
		tr_server_tls_rotation_mark_failed(server);
		return TR_ERR_STATE;
	}
	return ret;
}

static int tr_server_tls_update_profile_impl(
        struct tr_server *server, const struct tr_tls_server_config *config,
        enum tr_tls_rotation_policy policy, uint64_t *out_generation)
{
	struct tr_tls_openssl_profile *candidate = NULL;
	int ret;

	if (!server || !config ||
	    (policy != TR_TLS_ROTATE_GRACEFUL &&
	     policy != TR_TLS_ROTATE_REVOKE_OLD))
		return TR_ERR_INVALID;
	if (tr_server_blocking_lifecycle_context() ||
	    atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->tls_profile || !server->published || !server->runtime ||
	    !server->shards || server->shard_count == 0U)
		return TR_ERR_STATE;
	if (config->mode != TR_TLS_REQUIRED ||
	    (server->mode == TR_SERVER_MODE_TRANSPORT_ONLY &&
	     server->config.connection_groups.max_groups != 0U &&
	     config->client_auth != TR_TLS_CLIENT_AUTH_REQUIRED))
		return TR_ERR_INVALID;
	ret = tr_tls_openssl_server_profile_create(config, &candidate);
	if (ret != TR_OK)
		return ret;
	return tr_server_tls_publish_candidate_impl(server, candidate, policy,
	                                            0U, out_generation);
}

int tr_server_tls_update_profile(struct tr_server *server,
                                 const struct tr_tls_server_config *config,
                                 enum tr_tls_rotation_policy policy,
                                 uint64_t *out_generation)
{
	int ret;

	if (!server)
		return TR_ERR_INVALID;
	if (atomic_exchange_explicit(&server->tls_update_active, 1,
	                             memory_order_acquire))
		return TR_AGAIN;
	ret = tr_server_tls_update_profile_impl(server, config, policy,
	                                        out_generation);
	atomic_store_explicit(&server->tls_update_active, 0,
	                      memory_order_release);
	return ret;
}

static int tr_server_tls_operation_equal(struct tr_tls_provider_operation left,
                                         struct tr_tls_provider_operation right)
{
	return left.instance == right.instance &&
	       left.generation == right.generation && left.slot == right.slot &&
	       left.kind == right.kind;
}

static int tr_server_monotonic_now_ns(uint64_t *out_now_ns)
{
	struct timespec now;
	uint64_t seconds;

	if (!out_now_ns || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
	    now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1000000000L)
		return TR_ERR_SYS;
	seconds = (uint64_t)now.tv_sec;
	if (seconds > UINT64_MAX / UINT64_C(1000000000))
		return TR_ERR_STATE;
	seconds *= UINT64_C(1000000000);
	if ((uint64_t)now.tv_nsec > UINT64_MAX - seconds)
		return TR_ERR_STATE;
	*out_now_ns = seconds + (uint64_t)now.tv_nsec;
	return TR_OK;
}

static uint64_t tr_server_tls_credentials_deadline_tick(void *arg,
                                                        uint64_t now_ns)
{
	struct tr_server *server = arg;
	struct tr_tls_provider_operation operation = {0};
	tr_tls_credentials_cancel_cb cancel_cb = NULL;
	void *provider_arg = NULL;
	int notify = 0;

	if (!server || !server->tls_provider_lock_ready)
		return 0U;
	if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
		return now_ns == UINT64_MAX ? now_ns : now_ns + 1U;
	if (server->tls_credentials_active &&
	    now_ns >= server->tls_credentials_deadline_ns) {
		operation = server->tls_credentials_operation;
		if (tr_tls_provider_operation_request_cancel(
		            &server->tls_operations, operation, TR_ERR_TIMEOUT,
		            &notify) == TR_OK &&
		    notify) {
			if (server->tls_credentials_start_active) {
				server->tls_credentials_cancel_notified = 2;
			} else if (!server->tls_credentials_cancel_notified) {
				server->tls_credentials_cancel_notified = 1;
				cancel_cb =
				        server->config.tls_credentials_provider
				                .cancel;
				provider_arg =
				        server->config.tls_credentials_provider
				                .arg;
			}
		}
	}
	(void)pthread_mutex_unlock(&server->tls_provider_lock);
	if (cancel_cb)
		cancel_cb(operation, provider_arg);
	return 0U;
}

static int
tr_server_tls_credentials_retire(struct tr_server *server,
                                 struct tr_tls_provider_operation operation)
{
	int ret = tr_tls_provider_operation_retire(&server->tls_operations,
	                                           operation);
	if (ret != TR_OK)
		return ret;
	if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	if (server->tls_credentials_active &&
	    tr_server_tls_operation_equal(server->tls_credentials_operation,
	                                  operation)) {
		server->tls_credentials_active = 0;
		server->tls_credentials_start_active = 0;
		server->tls_credentials_cancel_notified = 0;
		server->tls_credentials_deadline_ns = 0U;
	}
	if (pthread_mutex_unlock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	return TR_OK;
}

int tr_server_tls_credentials_start(
        struct tr_server *server, uint32_t timeout_ms,
        enum tr_tls_rotation_policy policy,
        struct tr_tls_provider_operation *out_operation)
{
	struct tr_tls_credential_request request;
	struct tr_tls_provider_operation operation;
	tr_tls_credentials_fetch_cb fetch_cb;
	tr_tls_credentials_cancel_cb cancel_cb = NULL;
	void *provider_arg;
	int notify_cancel = 0;
	int reserved = 0;
	int ret;

	if (!server || !out_operation || timeout_ms == 0U ||
	    (policy != TR_TLS_ROTATE_GRACEFUL &&
	     policy != TR_TLS_ROTATE_REVOKE_OLD))
		return TR_ERR_INVALID;
	if (!server->config.tls_credentials_provider.fetch ||
	    !server->tls_operations_ready || !server->tls_provider_lock_ready)
		return TR_ERR_UNSUPPORTED;
	if (!server->published ||
	    atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (atomic_load_explicit(&server->tls_update_active,
	                         memory_order_acquire))
		return TR_AGAIN;
	{
		struct tr_deadline deadline;
		ret = tr_deadline_init_ms(&deadline, timeout_ms);
		if (ret != TR_OK)
			return ret;
		request.deadline_monotonic_ns = tr_deadline_to_ns(&deadline);
	}
	request.rotation_policy = policy;
	if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	if (server->tls_credentials_active) {
		(void)pthread_mutex_unlock(&server->tls_provider_lock);
		return TR_AGAIN;
	}
	ret = tr_tls_provider_operation_reserve(
	        &server->tls_operations, TR_TLS_PROVIDER_OPERATION_CREDENTIALS,
	        server->config.limits.max_tls_credential_bundle_bytes,
	        &operation);
	if (ret == TR_OK) {
		reserved = 1;
		ret = tr_tls_provider_operation_accept(&server->tls_operations,
		                                       operation);
	}
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&server->tls_provider_lock);
		if (reserved) {
			int ignored_notify = 0;
			(void)tr_tls_provider_operation_request_cancel(
			        &server->tls_operations, operation,
			        TR_ERR_CANCELLED, &ignored_notify);
			(void)tr_tls_provider_operation_retire(
			        &server->tls_operations, operation);
		}
		return ret;
	}
	server->tls_credentials_operation = operation;
	server->tls_credentials_deadline_ns = request.deadline_monotonic_ns;
	server->tls_credentials_policy = policy;
	server->tls_credentials_active = 1;
	server->tls_credentials_start_active = 1;
	server->tls_credentials_cancel_notified = 0;
	fetch_cb = server->config.tls_credentials_provider.fetch;
	provider_arg = server->config.tls_credentials_provider.arg;
	if (pthread_mutex_unlock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	ret = tr_reactor_timer_arm(server->tls_credentials_timer,
	                           request.deadline_monotonic_ns);
	if (ret != TR_OK) {
		(void)tr_tls_provider_operation_complete(
		        &server->tls_operations, operation, ret, NULL, 0U);
		(void)tr_server_tls_credentials_retire(server, operation);
		return ret;
	}
	fetch_cb(&request, operation, provider_arg);
	if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	server->tls_credentials_start_active = 0;
	if (server->tls_credentials_active &&
	    tr_server_tls_operation_equal(server->tls_credentials_operation,
	                                  operation) &&
	    server->tls_credentials_cancel_notified == 2) {
		server->tls_credentials_cancel_notified = 1;
		cancel_cb = server->config.tls_credentials_provider.cancel;
		notify_cancel = 1;
	}
	if (pthread_mutex_unlock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	*out_operation = operation;
	if (notify_cancel)
		cancel_cb(operation, provider_arg);
	return TR_OK;
}

int tr_server_tls_credentials_complete(
        struct tr_server *server, struct tr_tls_provider_operation operation,
        int status, const struct tr_tls_credential_bundle *bundle)
{
	if (!server || !server->tls_operations_ready)
		return TR_ERR_INVALID;
	return tr_tls_provider_operation_complete_credentials(
	        &server->tls_operations, operation, status, bundle);
}

static int tr_server_tls_credentials_request_cancel(
        struct tr_server *server, struct tr_tls_provider_operation operation,
        int reason)
{
	tr_tls_credentials_cancel_cb cancel_cb = NULL;
	void *provider_arg = NULL;
	int notify = 0;
	int ret;

	if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	if (!server->tls_credentials_active ||
	    !tr_server_tls_operation_equal(server->tls_credentials_operation,
	                                   operation)) {
		(void)pthread_mutex_unlock(&server->tls_provider_lock);
		return TR_ERR_STALE;
	}
	ret = tr_tls_provider_operation_request_cancel(
	        &server->tls_operations, operation, reason, &notify);
	if (ret == TR_OK && notify) {
		if (server->tls_credentials_start_active) {
			server->tls_credentials_cancel_notified = 2;
		} else if (!server->tls_credentials_cancel_notified) {
			server->tls_credentials_cancel_notified = 1;
			cancel_cb =
			        server->config.tls_credentials_provider.cancel;
			provider_arg =
			        server->config.tls_credentials_provider.arg;
		}
	}
	if (pthread_mutex_unlock(&server->tls_provider_lock) != 0)
		return TR_ERR_SYS;
	if (cancel_cb)
		cancel_cb(operation, provider_arg);
	return ret;
}

static int tr_server_tls_credentials_expire_if_due(
        struct tr_server *server, struct tr_tls_provider_operation operation,
        uint64_t deadline_ns)
{
	uint64_t now_ns;
	int ret = tr_server_monotonic_now_ns(&now_ns);

	if (ret != TR_OK || now_ns < deadline_ns)
		return ret;
	return tr_server_tls_credentials_request_cancel(server, operation,
	                                                TR_ERR_TIMEOUT);
}

int tr_server_tls_credentials_cancel(struct tr_server *server,
                                     struct tr_tls_provider_operation operation)
{
	if (!server)
		return TR_ERR_INVALID;
	if (!server->config.tls_credentials_provider.fetch)
		return TR_ERR_UNSUPPORTED;
	return tr_server_tls_credentials_request_cancel(server, operation,
	                                                TR_ERR_CANCELLED);
}

int tr_server_tls_credentials_poll(struct tr_server *server,
                                   struct tr_tls_provider_operation operation,
                                   struct tr_tls_credential_result *out_result)
{
	struct tr_tls_credential_bundle bundle = {0};
	struct tr_tls_credential_result result = {0};
	struct tr_tls_openssl_profile *candidate = NULL;
	struct tr_tls_server_config profile_config;
	uint64_t deadline_ns;
	uint64_t now_ns;
	int provider_status;
	int ret;

	if (!server || !out_result)
		return TR_ERR_INVALID;
	if (!server->config.tls_credentials_provider.fetch)
		return TR_ERR_UNSUPPORTED;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (atomic_exchange_explicit(&server->tls_update_active, 1,
	                             memory_order_acquire))
		return TR_AGAIN;
	if (pthread_mutex_lock(&server->tls_provider_lock) != 0) {
		atomic_store_explicit(&server->tls_update_active, 0,
		                      memory_order_release);
		return TR_ERR_SYS;
	}
	if (!server->tls_credentials_active ||
	    !tr_server_tls_operation_equal(server->tls_credentials_operation,
	                                   operation)) {
		(void)pthread_mutex_unlock(&server->tls_provider_lock);
		atomic_store_explicit(&server->tls_update_active, 0,
		                      memory_order_release);
		return TR_ERR_STALE;
	}
	if (server->tls_credentials_start_active) {
		(void)pthread_mutex_unlock(&server->tls_provider_lock);
		atomic_store_explicit(&server->tls_update_active, 0,
		                      memory_order_release);
		return TR_AGAIN;
	}
	deadline_ns = server->tls_credentials_deadline_ns;
	(void)pthread_mutex_unlock(&server->tls_provider_lock);
	ret = tr_server_tls_credentials_expire_if_due(server, operation,
	                                              deadline_ns);
	if (ret != TR_OK && ret != TR_ERR_STALE) {
		atomic_store_explicit(&server->tls_update_active, 0,
		                      memory_order_release);
		return ret;
	}
	ret = tr_tls_provider_operation_credentials_result(
	        &server->tls_operations, operation, &provider_status, &bundle);
	if (ret != TR_OK) {
		if (ret == TR_AGAIN) {
			int expire_ret =
			        tr_server_tls_credentials_expire_if_due(
			                server, operation, deadline_ns);
			if (expire_ret != TR_OK && expire_ret != TR_ERR_STALE)
				ret = expire_ret;
		}
		atomic_store_explicit(&server->tls_update_active, 0,
		                      memory_order_release);
		return ret;
	}
	result.status = provider_status;
	if (provider_status == TR_OK) {
		ret = tr_server_monotonic_now_ns(&now_ns);
		if (ret != TR_OK) {
			result.status = ret;
		} else if (now_ns >= deadline_ns) {
			result.status = TR_ERR_TIMEOUT;
		} else {
			profile_config = server->config.tls;
			profile_config.certificate_file = NULL;
			profile_config.private_key_file = NULL;
			ret = tr_tls_openssl_server_profile_create_from_pem(
			        &profile_config, &bundle,
			        server->config.limits
			                .max_tls_credential_bundle_bytes,
			        &candidate);
			if (ret != TR_OK) {
				result.status = ret;
			} else {
				ret = tr_server_monotonic_now_ns(&now_ns);
				if (ret != TR_OK || now_ns >= deadline_ns) {
					tr_tls_openssl_profile_release(
					        candidate);
					candidate = NULL;
					result.status =
					        ret != TR_OK ? ret
					                     : TR_ERR_TIMEOUT;
				} else {
					result.status = tr_server_tls_publish_candidate_impl(
					        server, candidate,
					        server->tls_credentials_policy,
					        deadline_ns,
					        &result.profile_generation);
					candidate = NULL;
				}
			}
		}
	}
	ret = tr_server_tls_credentials_retire(server, operation);
	if (ret == TR_OK) {
		*out_result = result;
		ret = TR_OK;
	}
	atomic_store_explicit(&server->tls_update_active, 0,
	                      memory_order_release);
	return ret;
}

int tr_server_create(const struct tr_server_config *config,
                     const struct tr_rpc_v2_method *methods,
                     size_t method_count, struct tr_server **out)
{
	struct tr_server_config effective;
	struct tr_facade_tuning tuning;
	struct tr_server_record_config record = {0};
	struct tr_rpc_dispatch_method *frozen;
	size_t max_reply = 0;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (config)
		effective = *config;
	else
		tr_server_config_init(&effective);
	tr_server_normalize_config(&effective);
	if (!methods || !method_count ||
	    method_count > effective.limits.max_methods ||
	    method_count > SIZE_MAX / sizeof(*frozen))
		return TR_ERR_INVALID;
	for (size_t i = 0; i < method_count; ++i) {
		const struct tr_rpc_method_desc *method =
		        &methods[i].descriptor;
		/* 公共大小限制描述业务正文。控制记录的内部最小空间
		 * 不能反过来扩大应用声明的方法正文准入上限。 */
		if (!methods[i].work ||
		    method->max_request_bytes >
		            effective.limits.max_message_bytes ||
		    method->max_response_bytes >
		            effective.limits.max_message_bytes ||
		    method->max_request_bytes >
		            effective.limits.initial_window_bytes ||
		    method->max_response_bytes >
		            effective.limits.initial_window_bytes)
			return TR_ERR_INVALID;
		if (method->max_response_bytes > max_reply)
			max_reply = method->max_response_bytes;
	}
	if (max_reply > SIZE_MAX / 16U)
		return TR_ERR_BAD_LENGTH;
	ret = tr_rpc_v2_prepare_limits(&effective.limits);
	if (ret != TR_OK)
		return ret;
	tr_facade_tuning_init(&tuning);
	tr_server_tuning_ensure_shard_minimums(&tuning, effective.shard_count);
	ret = tr_rpc_v2_memory_limit(&effective.limits, &tuning,
	                             effective.shard_count,
	                             &record.memory_budget_bytes);
	if (ret != TR_OK)
		return ret;
	/* 一个回调生成有限回复批次；网络背压只保留该批次，不会
	 * 重新执行工作函数。池与任务容量由门面推导，不暴露实现布局。 */
	record.endpoint = (struct tr_rpc_record_endpoint_config){
	        .dispatch = {.capacity = effective.limits.max_calls <
	                                                 effective.limits
	                                                         .max_streams
	                                         ? effective.limits.max_calls
	                                         : effective.limits.max_streams,
	                     .max_pending_per_call = 16U,
	                     .max_replies_per_event = 16U,
	                     .max_reply_bytes = max_reply * 16U},
	        .completion_limit = 64U,
	        .progress_limit = 64U,
	        .progress_period_ns = UINT64_C(1000000),
	        .final_wait_ns = UINT64_C(1000000000)};
	record.tasks.workers =
	        effective.shard_count > 4U ? effective.shard_count : 4U;
	record.tasks.capacity =
	        record.tasks.workers > 256U ? record.tasks.workers : 256U;
	frozen = calloc(method_count, sizeof(*frozen));
	if (!frozen)
		return TR_ERR_NOMEM;
	for (size_t i = 0; i < method_count; ++i) {
		frozen[i].descriptor = methods[i].descriptor;
		frozen[i].arg = methods[i].arg;
		frozen[i].v2_work = methods[i].work;
		frozen[i].v2_terminal = methods[i].terminal;
	}
	/* create_internal 复制并记账。临时表无回调参数所有权；即使
	 * 回滚失败把部分 Server 交还调用者，也不再借用此数组。 */
	ret = tr_server_create_internal(TR_SERVER_MODE_RPC_V2, &effective,
	                                &tuning, &record, frozen, method_count,
	                                out);
	free(frozen);
	return ret;
}

int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port)
{
	uint16_t bound_port = port;
	uint32_t i;
	int reuse_port;
	int ret;

	if (!server || !ipv4_address || server->shard_count == 0U)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (server->mode == TR_SERVER_MODE_TRANSPORT_ONLY)
		return TR_ERR_UNSUPPORTED;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->published)
		return TR_ERR_STATE;
	if (server->config.tls.mode == TR_TLS_REQUIRED && !server->tls_profile)
		return TR_AGAIN;
	if (server->started)
		return TR_ERR_STATE;

	for (i = 0; i < server->shard_count; ++i)
		if (tr_server_shard_listener_fd(&server->shards[i]) >= 0)
			return TR_ERR_STATE;

	reuse_port = server->shard_count > 1U;
	for (i = 0; i < server->shard_count; ++i) {
		struct tr_server_shard *shard = &server->shards[i];
		uint16_t actual_port = 0U;
		uint32_t backlog = tr_server_budget_share(
			(uint32_t)server->config.listen_backlog,
			server->shard_count, i);

		ret = tr_runtime_shard_listen_ipv4_ex(
			shard->runtime, ipv4_address,
			i == 0U ? port : bound_port, (int)backlog,
			reuse_port, &actual_port);
		if (ret != TR_OK)
			goto rollback;

		if (i == 0U)
			bound_port = actual_port;
		else if (actual_port != bound_port) {
			ret = TR_ERR_STATE;
			goto rollback;
		}
	}

	if (out_bound_port)
		*out_bound_port = bound_port;
	return TR_OK;

rollback:
	{
		uint32_t j;
		int rollback_ret = TR_OK;

		for (j = 0; j < server->shard_count; ++j) {
			int close_ret;

			if (tr_server_shard_listener_fd(&server->shards[j]) < 0)
				continue;
			close_ret = tr_runtime_shard_close_listener(
				server->shards[j].runtime);
			if (close_ret != TR_OK && rollback_ret == TR_OK)
				rollback_ret = close_ret;
		}
		if (rollback_ret != TR_OK)
			return rollback_ret;
	}
	return ret;
}

int tr_server_connection_group_listen(
	struct tr_server *server, const char *ipv4_address, uint16_t port,
	int backlog, uint16_t *out_bound_port)
{
	if (!server || !ipv4_address || backlog <= 0)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener)
		return TR_ERR_STATE;
	if (server->config.tls.mode == TR_TLS_REQUIRED && !server->tls_profile)
		return TR_AGAIN;
	if (server->started)
		return TR_ERR_STATE;

	return tr_pipeline_listener_listen_ipv4(
		server->connection_group_listener, ipv4_address, port, backlog,
		out_bound_port);
}

static int tr_server_connection_group_stop_internal(struct tr_server *server)
{
	if (!server || !server->connection_group_listener)
		return TR_OK;
	return tr_pipeline_listener_stop(server->connection_group_listener);
}

int tr_server_connection_group_stop(struct tr_server *server)
{
	if (!server)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener)
		return TR_ERR_STATE;
	return tr_server_connection_group_stop_internal(server);
}

int tr_server_connection_group_authorize_complete(
        struct tr_server *server, struct tr_tls_provider_operation operation,
        int status, enum tr_connection_group_authorization_decision decision)
{
	if (!server)
		return TR_ERR_INVALID;
	if (!server->connection_group_listener ||
	    !server->config.connection_groups.authorize_async)
		return TR_ERR_UNSUPPORTED;
	return tr_pipeline_listener_authorize_complete(
	        server->connection_group_listener, operation, status, decision);
}

int tr_server_connection_group_begin_drain(struct tr_server *server)
{
	if (!server)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener || !server->started)
		return TR_ERR_STATE;
	return tr_pipeline_listener_begin_drain(
		server->connection_group_listener);
}

int tr_server_connection_group_get_stats(
	struct tr_server *server,
	struct tr_connection_group_server_stats *out)
{
	struct tr_pipeline_listener_stats internal;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener)
		return TR_ERR_STATE;

	memset(&internal, 0, sizeof(internal));
	ret = tr_pipeline_listener_get_stats(
		server->connection_group_listener, &internal);
	if (ret != TR_OK)
		return ret;

	memset(out, 0, sizeof(*out));
	out->draining = internal.draining;
	out->groups_current = internal.pipelines_current;
	out->groups_peak = internal.pipelines_peak;
	out->connections_current = internal.connections_current;
	out->connections_peak = internal.connections_peak;
	out->data_connections_current =
		internal.data_connections_current;
	out->active_transfers = internal.active_transfers;
	out->control_accepts = internal.control_accepts;
	out->data_accepts = internal.data_accepts;
	out->route_rejections = internal.route_rejections;
	out->capacity_rejections = internal.capacity_rejections;
	return TR_OK;
}

int tr_server_connection_group_wait_drained(
	struct tr_server *server, uint32_t timeout_ms)
{
	if (!server)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->connection_group_listener || !server->started)
		return TR_ERR_STATE;

	return tr_pipeline_listener_wait_drained(
		server->connection_group_listener, timeout_ms);
}

int tr_server_connection_group_send_data_offer(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint64_t message_id)
{
	if (!server || group_id == 0U || epoch == 0U)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener || !server->started)
		return TR_ERR_STATE;

	return tr_pipeline_listener_send_data_offer(
		server->connection_group_listener, group_id, epoch,
		message_id, NULL);
}

int tr_server_connection_group_send_transfer_ready(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint32_t stream_id, uint64_t message_id)
{
	if (!server || group_id == 0U || epoch == 0U || stream_id == 0U)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener || !server->started)
		return TR_ERR_STATE;

	return tr_pipeline_listener_send_transfer_ready(
		server->connection_group_listener, group_id, epoch,
		stream_id, message_id);
}

int tr_server_connection_group_release_transfer(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint32_t stream_id)
{
	if (!server || group_id == 0U || epoch == 0U || stream_id == 0U)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (!server->connection_group_listener || !server->started)
		return TR_ERR_STATE;

	return tr_pipeline_listener_release_transfer(
		server->connection_group_listener, group_id, epoch, stream_id);
}

int tr_server_start(struct tr_server *server)
{
	uint32_t rpc_listener_count = 0U;
	uint32_t i;
	int group_listening;
	int ret;

	if (!server || server->shard_count == 0U)
		return TR_ERR_STATE;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->published || (server->mode == TR_SERVER_MODE_RPC_V2 &&
	                           server->record_start_attempted))
		return TR_ERR_STATE;
	if (server->config.tls.mode == TR_TLS_REQUIRED && !server->tls_profile)
		return TR_AGAIN;
	if (server->started)
		return TR_ERR_STATE;

	for (i = 0; i < server->shard_count; ++i)
		if (tr_server_shard_listener_fd(&server->shards[i]) >= 0)
			rpc_listener_count++;
	if (rpc_listener_count != 0U &&
	    rpc_listener_count != server->shard_count)
		return TR_ERR_STATE;

	group_listening =
		server->connection_group_listener &&
		tr_pipeline_listener_bound_port(server->connection_group_listener) != 0U;
	if (rpc_listener_count == 0U && !group_listening)
		return TR_ERR_STATE;

	if (server->mode == TR_SERVER_MODE_RPC_V2)
		server->record_start_attempted = 1;
	ret = tr_runtime_start(server->runtime);
	if (ret != TR_OK) {
		if (server->mode == TR_SERVER_MODE_RPC_V2)
			server->published = 0;
		return ret;
	}
	/* 所有分片的任务所有者和重试源就绪后才开放任何 TCP 接纳源。 */
	if (server->mode == TR_SERVER_MODE_RPC_V2)
		for (i = 0; i < server->shard_count; ++i) {
			ret = tr_runtime_shard_call(
			        server->shards[i].runtime,
			        tr_server_record_start_on_owner,
			        &server->shards[i]);
			if (ret != TR_OK)
				goto rollback_events;
		}

	for (i = 0; i < server->shard_count; ++i) {
		struct tr_server_shard *shard = &server->shards[i];

		ret = tr_runtime_shard_enable_peer_events(
			shard->runtime, tr_server_on_peer_lifecycle_event, shard);
		if (ret != TR_OK)
			goto rollback_events;
		shard->peer_events_enabled = 1;

		if (tr_server_shard_listener_fd(shard) >= 0) {
			ret = tr_runtime_shard_enable_listener_events(
				shard->runtime, tr_server_on_listener_ready, shard);
			if (ret != TR_OK)
				goto rollback_events;
		}
	}

	server->started = 1;
	return TR_OK;

rollback_events: {
	uint32_t j;
	int rollback_ret = TR_OK;

	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		/* 某个 owner 可能已致命退出。先 stop/join，再执行允许
		 * 已停止父对象的解绑；不能被 CLOSED 卡在 join 之前。 */
		server->published = 0;
		rollback_ret = tr_runtime_stop(server->runtime);
		if (rollback_ret != TR_OK)
			return rollback_ret;
	}

	for (j = 0; j < server->shard_count; ++j) {
		int cleanup_ret;

		if (tr_server_shard_listener_fd(&server->shards[j]) >= 0) {
			cleanup_ret = tr_runtime_shard_disable_listener_events(
			        server->shards[j].runtime);
			if (cleanup_ret != TR_OK && rollback_ret == TR_OK)
				rollback_ret = cleanup_ret;
		}

		cleanup_ret = tr_server_disable_peer_events(&server->shards[j]);
		if (cleanup_ret != TR_OK && rollback_ret == TR_OK)
			rollback_ret = cleanup_ret;
	}

	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		int cleanup_ret = tr_server_record_reap_all(server);

		if (cleanup_ret != TR_OK &&
		    (rollback_ret == TR_OK ||
		     (rollback_ret == TR_AGAIN && cleanup_ret != TR_AGAIN)))
			rollback_ret = cleanup_ret;
	} else {
		int stop_ret = tr_runtime_stop(server->runtime);
		if (stop_ret != TR_OK && rollback_ret == TR_OK)
			rollback_ret = stop_ret;
	}
	if (rollback_ret != TR_OK)
		return rollback_ret;
}
	return ret;
}

static int tr_server_stop_accepting(struct tr_server *server)
{
	uint32_t i;
	int ret;

	if (!server)
		return TR_ERR_INVALID;

	ret = tr_server_connection_group_stop_internal(server);
	if (ret != TR_OK)
		return ret;

	for (i = 0; i < server->shard_count; ++i) {
		struct tr_server_shard *shard = &server->shards[i];

		if (tr_server_shard_listener_fd(shard) < 0)
			continue;
		ret = tr_runtime_shard_close_listener(shard->runtime);
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

int tr_server_drain(struct tr_server *server, uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	const struct tr_deadline *wait_deadline = NULL;
	uint32_t shard_index;
	int final = TR_OK;

	if (!server)
		return TR_ERR_STATE;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;

	/* 在首次生命周期修改前拒绝。处理函数等待自己的流，
	 * 或所有者回调等待反应器进展，都会等待自身；
	 * 若检测到此错误前先停止监听器，还会使服务端处于部分排空状态。
	 */
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->started)
		return TR_ERR_STATE;

	{
		int ret = tr_server_stop_accepting(server);
		if (ret != TR_OK)
			return ret;
	}
	for (shard_index = 0; shard_index < server->shard_count; ++shard_index) {
		int ret = tr_server_disable_peer_events(
			&server->shards[shard_index]);
		if (ret != TR_OK)
			return ret;
	}

	/* 监听器与对端事件注销都是同步所有者屏障。二者返回后，
	 * 接受路径不能再发布新对端，生命周期事件也不能解绑或清空对端槽位，
	 * 因此对端表在此外部排空阶段被冻结。
	 *
	 * GOAWAY 使用 Connection 内嵌的生命周期发送槽位。因此，
	 * 普通控制发送池或命令环形队列容量耗尽不会使准入失败；
	 * 无需门面轮询或重试循环。
	 */
	for (shard_index = 0; shard_index < server->shard_count; ++shard_index) {
		struct tr_server_shard *shard = &server->shards[shard_index];
		uint32_t i;

		for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
			struct tr_runtime_peer *peer =
				tr_server_shard_peer_at(shard, i);
			int ret;

			if (!peer || !peer->used)
				continue;
			ret = tr_channel_begin_drain(peer->channel);
			if (ret != TR_OK && final == TR_OK)
				final = ret;
		}
	}

	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			struct tr_server_shard *shard =
			        &server->shards[shard_index];
			int ret = tr_reactor_call(
			        tr_server_shard_reactor(shard),
			        tr_server_record_watch_drain_on_owner, shard);
			if (ret != TR_OK)
				return ret;
		}
	}

	/* 所有 GOAWAY 准入已完成，排空等待从这里开始计时一次。
	 * 非零超时的每个 Channel 共用相同的 CLOCK_MONOTONIC 绝对期限，
	 * 不再按对端舍入或重建剩余毫秒预算。0ms 仍逐个立即检查。
	 */
	if (timeout_ms != 0U) {
		int ret = tr_deadline_init_ms(&deadline, timeout_ms);

		if (ret != TR_OK)
			return ret;
		wait_deadline = &deadline;
	}
	if (server->mode == TR_SERVER_MODE_RPC_V2) {
		/* RESET 可先归还全部网络槽，原 worker、待交付完成和终局
		 * 回调仍须真正静默。先等本层，再以同一期限等网络和 TX。 */
		int ret = tr_server_record_wait_drained(server, wait_deadline);
		if (ret != TR_OK)
			return ret;
	}
	for (shard_index = 0; shard_index < server->shard_count; ++shard_index) {
		struct tr_server_shard *shard = &server->shards[shard_index];
		uint32_t i;

		for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
			struct tr_runtime_peer *peer =
				tr_server_shard_peer_at(shard, i);
			int ret;

			if (!peer || !peer->used)
				continue;
			if (wait_deadline) {
				int expired;

				ret = tr_deadline_expired(wait_deadline,
							  &expired);
				if (ret != TR_OK)
					return ret;
				if (expired)
					return TR_ERR_TIMEOUT;
			}
			ret = tr_channel_wait_drained_deadline(
				peer->channel, wait_deadline);
			if (ret != TR_OK && final == TR_OK)
				final = ret;
		}
	}

	if (final == TR_OK)
		server->gracefully_drained = 1;
	return final;
}

/* 仅归档累计字段；当前资源量指标来自存活对端。 */
static void tr_server_collect_retired_stats(struct tr_server_shard *shard,
                                            struct tr_server_stats *stats)
{
	stats->channel.streams_opened +=
	        shard->retired_channel_stats.streams_opened;
	stats->channel.streams_closed +=
	        shard->retired_channel_stats.streams_closed;
	stats->channel.stream_errors +=
	        shard->retired_channel_stats.stream_errors;
	stats->channel.messages_tx += shard->retired_channel_stats.messages_tx;
	stats->channel.messages_rx += shard->retired_channel_stats.messages_rx;
	stats->channel.bytes_tx += shard->retired_channel_stats.bytes_tx;
	stats->channel.bytes_rx += shard->retired_channel_stats.bytes_rx;
	stats->channel.window_updates_tx +=
	        shard->retired_channel_stats.window_updates_tx;
	stats->channel.window_updates_rx +=
	        shard->retired_channel_stats.window_updates_rx;
	stats->channel.reconnect_attempts +=
	        shard->retired_channel_stats.reconnect_attempts;
	stats->channel.reconnect_successes +=
	        shard->retired_channel_stats.reconnect_successes;
	stats->channel.keepalive_pings_sent +=
	        shard->retired_channel_stats.keepalive_pings_sent;
	stats->channel.keepalive_pongs_received +=
	        shard->retired_channel_stats.keepalive_pongs_received;
	stats->channel.keepalive_timeouts +=
	        shard->retired_channel_stats.keepalive_timeouts;
}

struct tr_server_stats_owner_request {
	struct tr_server_shard *shard;
	struct tr_server_stats *stats;
};

static int tr_server_collect_peer_stats_on_owner(void *arg)
{
	struct tr_server_stats_owner_request *request =
		(struct tr_server_stats_owner_request *)arg;
	struct tr_server_shard *shard = request->shard;
	struct tr_server_stats *stats = request->stats;
	struct tr_runtime_peer_stats peer_stats;
	uint32_t i;

	/* 对端槽位与所有者侧计数器是单写者反应器状态。
	 * 外部统计调用者同步进入每个分片所有者。
	 */
	pthread_mutex_lock(&shard->server->finalizer_lock);
	tr_server_collect_retired_stats(shard, stats);
	tr_runtime_shard_peer_stats(shard->runtime, &peer_stats);
	pthread_mutex_unlock(&shard->server->finalizer_lock);
	stats->max_peers += peer_stats.capacity;
	stats->peers_current += peer_stats.current;
	stats->peers_peak += peer_stats.peak;
	stats->peers_reaping_current += peer_stats.reaping_current;
	stats->peers_ready_total += peer_stats.ready_total;
	stats->peers_reaped_total += peer_stats.reaped_total;
	stats->peer_capacity_rejections += peer_stats.capacity_rejections;

	for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer =
		        tr_server_shard_peer_at(shard, i);
		struct tr_channel_stats channel_stats;
		struct tr_server_peer *state = tr_server_peer_state(peer);

		if (!peer || !peer->used)
			continue;

		if (peer->ready && state->record_rpc && peer->channel &&
		    !tr_server_peer_disconnected(peer))
			stats->peers_ready_current++;

		if (state->record_rpc)
			stats->rpc.endpoints_current++;
		if (peer->channel &&
		    tr_channel_get_stats(peer->channel, &channel_stats) == TR_OK)
			tr_server_merge_channel_stats(&stats->channel,
						      &channel_stats, 1);
	}
	if (shard->record_tasks) {
		struct tr_task_executor_stats tasks;
		int ret =
		        tr_task_executor_get_stats(shard->record_tasks, &tasks);

		if (ret != TR_OK)
			return ret;
		/* 共享资源只按分片计一次，不为每个对端重复累加容量。 */
		stats->rpc.executor_queue_capacity_current += tasks.capacity;
		stats->rpc.executor_queued_tasks_current += tasks.queued;
		stats->rpc.executor_running_tasks_current += tasks.running;
	}
	if (shard->server->mode == TR_SERVER_MODE_RPC_V2) {
		struct tr_rpc_semantic_stats semantic;
		int ret = tr_rpc_record_semantic_stats_read(
		        &shard->record_semantic, &semantic);
		if (ret != TR_OK)
			return ret;
		return tr_rpc_record_semantic_stats_merge(&stats->rpc.semantic,
		                                          &semantic);
	}

	return TR_OK;
}

int tr_server_get_stats(struct tr_server *server, struct tr_server_stats *out)
{
	struct tr_server_stats stats;
	uint32_t i;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;
	if (atomic_load_explicit(&server->tls_rotation_failed,
	                         memory_order_acquire))
		return TR_ERR_STATE;
	/* 回滚失败时 *out 可能保留部分构造对象；调用方只能继续销毁，
	 * 不允许统计遍历未发布或已经开始终止拆除的分片数组。
	 */
	if (!server->published || !server->runtime || !server->shards ||
	    server->shard_count == 0U)
		return TR_ERR_STATE;

	memset(&stats, 0, sizeof(stats));
	stats.shard_count = server->shard_count;

	if (server->mode == TR_SERVER_MODE_RPC_V2)
		stats.rpc.executor_threads =
		        (uint32_t)server->record.tasks.workers;

	for (i = 0; i < server->shard_count; ++i) {
		struct tr_server_stats_owner_request request;
		struct tr_server_shard *shard = &server->shards[i];
		struct tr_reactor_stats reactor_stats;
		struct tr_pool_observation pool_stats;

		if (!shard->runtime)
			return TR_ERR_STATE;
		memset(&reactor_stats, 0, sizeof(reactor_stats));
		ret = tr_reactor_get_stats(tr_server_shard_reactor(shard),
					   &reactor_stats);
		if (ret != TR_OK)
			return ret;
		tr_server_merge_reactor_stats(&stats.reactor, &reactor_stats);

		if (shard->reassembly_pool_ready) {
			memset(&pool_stats, 0, sizeof(pool_stats));
			ret = tr_buffer_pool_get_stats(&shard->reassembly_pool,
			                               &pool_stats);
			if (ret != TR_OK)
				return ret;
			tr_server_merge_pool_observation(&stats.reassembly_pool,
			                                 &pool_stats);
		}

		request.shard = shard;
		request.stats = &stats;
		ret = tr_runtime_shard_call(
			shard->runtime, tr_server_collect_peer_stats_on_owner,
			&request);
		if (ret != TR_OK)
			return ret;
	}

	*out = stats;
	return TR_OK;
}

struct tr_server_record_semantic_request {
	struct tr_server_shard *shard;
	struct tr_rpc_semantic_stats *out;
};

static int tr_server_record_semantic_on_owner(void *arg)
{
	struct tr_server_record_semantic_request *request = arg;
	/* 构造后的未启动分片由 call_or_stopped 的父锁串行化；运行中
	 * 只在 owner 复制值，不采集队列、资源池或工作线程诊断。 */
	return tr_rpc_record_semantic_stats_read(
	        &request->shard->record_semantic, request->out);
}

static int tr_server_record_semantic_stats(struct tr_server *server,
                                           struct tr_rpc_semantic_stats *out)
{
	struct tr_rpc_semantic_stats total = {0};
	/* destroy 可先关闭 published，再等待已有工作退出。被禁止的
	 * 回调查询必须只读 TLS 就返回，不能先触碰生命周期可变字段。 */
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->published || !server->runtime || !server->shards ||
	    !server->shard_count)
		return TR_ERR_STATE;
	/* 全 Server 快照需要依次进入所有分片。统一限制到外部控制线程，
	 * 不随 shard_count 改变回调合同；任一失败保持调用方 out 不变。 */
	for (uint32_t i = 0; i < server->shard_count; ++i) {
		struct tr_rpc_semantic_stats part;
		struct tr_server_shard *shard = &server->shards[i];
		struct tr_server_record_semantic_request request = {shard,
		                                                    &part};
		if (!shard->runtime)
			return TR_ERR_STATE;
		int ret = tr_reactor_call_or_stopped(
		        tr_server_shard_reactor(shard),
		        tr_server_record_semantic_on_owner, &request);
		if (ret == TR_OK)
			ret = tr_rpc_record_semantic_stats_merge(&total, &part);
		if (ret != TR_OK)
			return ret;
	}
	*out = total;
	return TR_OK;
}

int tr_server_get_rpc_semantic_stats(
	struct tr_server *server, struct tr_rpc_semantic_stats *out)
{
	struct tr_server_stats stats;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;
	if (server->mode == TR_SERVER_MODE_RPC_V2)
		return tr_server_record_semantic_stats(server, out);

	memset(&stats, 0, sizeof(stats));
	ret = tr_server_get_stats(server, &stats);
	if (ret != TR_OK)
		return ret;

	*out = stats.rpc.semantic;
	return TR_OK;
}

int tr_server_get_connection_group_stats_internal(
	struct tr_server *server, struct tr_pipeline_listener_stats *out)
{
	if (!server || !out)
		return TR_ERR_INVALID;
	if (!server->connection_group_listener)
		return TR_ERR_STATE;
	return tr_pipeline_listener_get_stats(
		server->connection_group_listener, out);
}

static int tr_server_tls_credentials_drain_for_destroy(struct tr_server *server)
{
	struct tr_tls_provider_operation operation;
	struct tr_tls_credential_bundle bundle = {0};
	int provider_status;
	int active;
	int start_active;
	int ret;

	if (server->tls_provider_lock_ready) {
		if (pthread_mutex_lock(&server->tls_provider_lock) != 0)
			return TR_ERR_SYS;
		active = server->tls_credentials_active;
		start_active = server->tls_credentials_start_active;
		operation = server->tls_credentials_operation;
		if (pthread_mutex_unlock(&server->tls_provider_lock) != 0)
			return TR_ERR_SYS;
		if (active) {
			ret = tr_server_tls_credentials_request_cancel(
			        server, operation, TR_ERR_CANCELLED);
			if (ret != TR_OK && ret != TR_ERR_STALE)
				return ret;
			if (start_active)
				return TR_AGAIN;
			ret = tr_tls_provider_operation_credentials_result(
			        &server->tls_operations, operation,
			        &provider_status, &bundle);
			if (ret == TR_AGAIN)
				return TR_AGAIN;
			if (ret != TR_OK)
				return ret;
			ret = tr_server_tls_credentials_retire(server,
			                                       operation);
			if (ret != TR_OK)
				return ret;
		}
	}
	if (server->tls_credentials_timer_registered) {
		ret = tr_reactor_timer_unregister(
		        server->tls_credentials_timer);
		if (ret != TR_OK)
			return ret;
		server->tls_credentials_timer_registered = 0;
	}
	return TR_OK;
}

int tr_server_destroy(struct tr_server *server)
{
	uint32_t shard_index;
	int ret;

	if (!server)
		return TR_OK;

	/* 服务端拆除会等待反应器与执行器工作线程退出并静止。
	 * 绝不能从拆除进展所依赖的执行上下文开始拆除。
	 */
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	ret = tr_server_tls_credentials_drain_for_destroy(server);
	if (ret != TR_OK)
		return ret;

	/* 销毁即使稍后需要重试也不再允许读取跨分片统计。 */
	server->published = 0;
	if (server->tls_profile && server->gracefully_drained &&
	    server->runtime && server->shards) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			struct tr_server_shard *shard =
			        &server->shards[shard_index];
			for (uint32_t i = 0;
			     i < tr_server_shard_peer_capacity(shard); ++i) {
				struct tr_runtime_peer *peer =
				        tr_server_shard_peer_at(shard, i);
				int close_ret;

				if (!peer || !peer->used ||
				    !peer->connection.reactor)
					continue;
				close_ret = tr_reactor_close_graceful(
				        peer->connection);
				/* tr_runtime_stop() may stop the owner and
				 * still return TR_AGAIN while accepted work
				 * drains. A destroy retry sees CLOSED because
				 * that owner no longer accepts commands; the
				 * close-notify barrier completed before the
				 * first stop attempt. */
				if (close_ret != TR_OK &&
				    close_ret != TR_ERR_STALE &&
				    close_ret != TR_ERR_CLOSED)
					return close_ret;
			}
		}
	}
	if (server->mode == TR_SERVER_MODE_RPC_V2 && server->runtime) {
		/* 终止拆除先 join 父 owner；异常退出的父循环也走同一路径。
		 * 在此之前任何 CLOSED 都不是可以释放回调参数的静默证明。 */
		ret = tr_runtime_stop(server->runtime);
		if (ret != TR_OK)
			return ret;
		server->started = 0;
	}
	if (server->shards) {
		ret = tr_server_stop_accepting(server);
		if (ret != TR_OK)
			return ret;
	}
	if (server->connection_group_listener) {
		ret = tr_pipeline_listener_stop(
			server->connection_group_listener);

		/* 服务端销毁是终局生命周期边界。监听器停止成功后才允许
		 * 释放其注册表与会话内存；失败表示内部拆除不变量
		 * 未收敛，继续释放 Runtime 反而可能制造释放后使用。
		 */
		if (ret != TR_OK)
			return ret;
		ret = tr_pipeline_listener_destroy(
			server->connection_group_listener);
		if (ret != TR_OK)
			return ret;
		server->connection_group_listener = NULL;
	}
	if (server->shards)
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			struct tr_server_shard *shard =
				&server->shards[shard_index];

			/* 监听器解绑已同步关闭注册入口；跨过定时器屏障才可释放分片。
			 */
			if (shard->handshake_timer_registered) {
				ret = tr_reactor_timer_unregister(
					shard->handshake_timer);
				if (ret != TR_OK)
					return ret;
				shard->handshake_timer_registered = 0;
			}
			ret = tr_server_disable_peer_events(shard);
			if (ret != TR_OK)
				return ret;
		}

	if (server->runtime && server->started) {
		ret = tr_runtime_stop(server->runtime);
		/* 除非每个分片所有者都已跨过停止屏障，否则不要销毁对端或
		 * Runtime 存储。
		 * 线程回收失败后继续释放会把生命周期错误变成释放后使用。
		 */
		if (ret != TR_OK)
			return ret;
		server->started = 0;
	}
	if (server->mode == TR_SERVER_MODE_RPC_V2 && server->shards) {
		ret = tr_server_record_reap_all(server);
		if (ret != TR_OK)
			return ret;
	}

	if (server->shards) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			struct tr_server_shard *shard =
				&server->shards[shard_index];
			uint32_t i;

			for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
				struct tr_runtime_peer *peer =
					tr_server_shard_peer_at(shard, i);
				struct tr_server_peer *state =
				        tr_server_peer_state(peer);

				if (!peer || (!peer->used && !peer->channel &&
				              !state->record_rpc))
					continue;
				{
					ret = tr_server_destroy_peer(peer);
					if (ret != TR_OK)
						return ret;
				}
			}
		}
	}

	/* 所有 Endpoint 和传输引用已经退役。剩余 producer 只由应用
	 * 持有，或正在结束锁外复制；先切断其预算依赖再销毁 Runtime。
	 * 域关闭不等待应用归还句柄，晚 release 只修改独立计费存储。
	 */
	if (server->mode == TR_SERVER_MODE_RPC_V2 && server->shards) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			ret = tr_rpc_record_producer_domain_close(
			        &server->shards[shard_index].record_producers);
			if (ret != TR_OK)
				return ret;
		}
	}

	/* 分片池是预算使用方；RuntimeShard 拥有预算本身。
	 * 销毁 Runtime 或预算所有者前，释放每个池的预留额度。
	 */
	if (server->shards) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			struct tr_server_shard *shard =
				&server->shards[shard_index];

			if (shard->reassembly_pool_ready) {
				int ret = tr_buffer_pool_destroy(
					&shard->reassembly_pool);
				if (ret != TR_OK)
					return ret;
				shard->reassembly_pool_ready = 0;
			}
		}
	}
	if (server->shards) {
		for (shard_index = 0; shard_index < server->shard_count;
		     ++shard_index) {
			tr_tls_openssl_profile_release(
			        server->shards[shard_index].tls_profile);
			server->shards[shard_index].tls_profile = NULL;
		}
	}

	if (server->record_method_bytes) {
		struct tr_memory_budget *budget;
		size_t bytes = server->record_method_bytes;

		/* 方法表预留只在分片绑定后发布；回滚必须保留有效预算
		 * 所有者，不能在缺失该依赖时跳过尚未归还的额度。
		 */
		if (!server->shards || server->shard_count == 0U ||
		    !server->shards[0].runtime)
			return TR_ERR_STATE;
		budget = tr_runtime_shard_memory_budget(
		        server->shards[0].runtime);
		ret = tr_memory_budget_release(budget, bytes);
		if (ret != TR_OK)
			return ret;
		free(server->record_methods);
		server->record_methods = NULL;
		server->record_method_bytes = 0;
		server->record_method_count = 0;
	}
	if (server->tls_operations_ready) {
		ret = tr_tls_provider_operation_table_destroy(
		        &server->tls_operations);
		if (ret != TR_OK)
			return ret;
		server->tls_operations_ready = 0;
	}

	if (server->record_drain_cond_ready) {
		if (pthread_cond_destroy(&server->record_drain_cond) != 0)
			return TR_ERR_SYS;
		server->record_drain_cond_ready = 0;
	}
	if (server->runtime) {
		ret = tr_runtime_destroy(server->runtime);
		if (ret != TR_OK)
			return ret;
		server->runtime = NULL;
	}
	if (server->tls_profile) {
		tr_tls_openssl_profile_release(server->tls_profile);
		server->tls_profile = NULL;
	}
	if (server->tls_recovery_profile) {
		tr_tls_openssl_profile_release(server->tls_recovery_profile);
		server->tls_recovery_profile = NULL;
	}
	if (server->tls_provider_lock_ready) {
		if (pthread_mutex_destroy(&server->tls_provider_lock) != 0)
			return TR_ERR_SYS;
		server->tls_provider_lock_ready = 0;
	}
	if (server->mode == TR_SERVER_MODE_RPC_V2 &&
	    atomic_load_explicit(&server->memory_budget.current_bytes,
	                         memory_order_acquire) != 0U)
		return TR_ERR_STATE;

	free(server->shards);
	server->shards = NULL;
	server->shard_count = 0U;

	free(server->tls_client_ca_file_copy);
	server->tls_client_ca_file_copy = NULL;
	pthread_mutex_destroy(&server->finalizer_lock);
	free(server);
	return TR_OK;
}
