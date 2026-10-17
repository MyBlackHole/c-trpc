#include "tr/server.h"

#include <assert.h>
#include <pthread.h>
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
#include "rpc/rpc_wire.h"
#include "io/socket.h"
#include "tr/status.h"
#include "rpc/rpc_internal.h"
#include "runtime/runtime_internal.h"
#include "io/socket_internal.h"
#include "observability_internal.h"
#include "transport/group/pipeline_listener_internal.h"
#include "server_record_internal.h"
#include "rpc/rpc_record_client.h"

#define TR_SERVER_ACCEPT_BATCH 16U

enum tr_server_method_kind {
	TR_SERVER_METHOD_UNARY = 1,
	TR_SERVER_METHOD_STREAM = 2
};

struct tr_server_method {
	int used;
	enum tr_server_method_kind kind;
	struct tr_rpc_method_desc desc;
	tr_rpc_unary_handler unary_handler;
	struct tr_rpc_stream_handlers stream_handlers;
	void *handler_arg;
};

struct tr_server;
struct tr_server_detached_peer;

struct tr_server_shard {
	struct tr_server *server;
	struct tr_runtime_shard *runtime;
	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	uint32_t executor_threads;
	int rpc_pool_ready;
	int reassembly_pool_ready;
	int peer_events_enabled;
	struct tr_reactor_timer_handle handshake_timer;
	int handshake_timer_registered;
	struct tr_task_executor *record_tasks;
	struct tr_reactor_timer_handle record_cleanup_timer;
	int record_cleanup_timer_registered;
	/* 只在 owner 或父线程 join 后修改，排空时与对端事件一起冻结。 */
	int record_cleanup_enabled;
	int record_tasks_retiring;
	struct tr_server_detached_peer *detached_peers;
	struct tr_server_channel_stats retired_channel_stats;
	struct tr_server_rpc_stats retired_rpc_stats;
};

struct tr_server_detached_peer {
	struct tr_server_detached_peer *next;
	struct tr_rpc_endpoint_stats rpc_stats;
	struct tr_channel_stats channel_stats;
	struct tr_server_shard *shard;
	struct tr_channel *channel;
};

static int tr_server_blocking_lifecycle_context(void)
{
	/* 同步排空等待需要反应器所有者与 RPC 执行器都能推进。
	 * 从任一执行域进入等待，都会等待当前回调或任务
	 * 返回后才可能取得的进展。
	 */
	return tr_reactor_in_owner_context() || tr_rpc_in_worker_context() ||
	       tr_task_in_worker_context() ||
	       tr_rpc_record_client_in_callback();
}

struct tr_server {
	struct tr_server_config config;
	struct tr_facade_tuning tuning;

	struct tr_runtime *runtime;
	struct tr_server_shard *shards;
	/* 已绑定 Runtime、允许销毁路径遍历的分片前缀。 */
	uint32_t shard_count;

	struct tr_server_method *methods;
	uint32_t method_count;
	struct tr_server_record_config record;
	struct tr_rpc_dispatch_method *record_methods;
	size_t record_method_count;
	size_t record_method_bytes;
	int record_mode;
	int record_start_attempted;

	struct tr_pipeline_listener *connection_group_listener;

	pthread_mutex_t finalizer_lock;
	pthread_cond_t finalizer_cond;
	int finalizer_cond_ready;
	int started;
	/* 只有构造完成并发布后才允许读取跨分片统计；终止销毁开始后关闭。 */
	int published;
};

TR_DEFINE_PTR_OWNERSHIP(tr_server_mem, struct tr_server, free)

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

static struct tr_rpc_executor_group *
tr_server_shard_rpc_executor(struct tr_server_shard *shard)
{
	return shard ? tr_runtime_shard_rpc_executor(shard->runtime) : NULL;
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
		       config->max_streams_per_group == 0U;

	return config->max_connections >= config->max_groups &&
	       config->max_data_connections_per_group != 0U &&
	       config->max_streams_per_group != 0U &&
	       config->authorize != NULL;
}

static int tr_server_connection_group_authorize(
	const struct tr_pipeline_route_preface *route, void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;
	struct tr_connection_group_id group;

	if (!server || !route || !server->config.connection_groups.authorize)
		return TR_ERR_INVALID;

	group.group_id = route->pipeline_id;
	group.epoch = route->epoch;
	return server->config.connection_groups.authorize(
		&group, server->config.connection_groups.callback_arg);
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

static void tr_server_merge_rpc_semantic_stats(
	struct tr_rpc_semantic_stats *dst,
	const struct tr_rpc_semantic_stats *src)
{
	uint32_t i;

	if (!dst || !src)
		return;

	dst->calls_started += src->calls_started;
	dst->calls_finished += src->calls_finished;
	dst->calls_inflight += src->calls_inflight;
	for (i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		dst->final_status[i] += src->final_status[i];
}

static void tr_server_merge_rpc_stats(
	struct tr_server_rpc_stats *dst,
	const struct tr_rpc_endpoint_stats *src, int current)
{
	if (!dst || !src)
		return;

	if (src->executor_threads > dst->executor_threads)
		dst->executor_threads = src->executor_threads;
	if (current) {
		dst->endpoints_current++;
		dst->opening_calls_current += src->opening_calls;
		dst->active_calls_current += src->active_calls;
		dst->terminal_calls_current += src->terminal_calls;
		dst->executor_queued_tasks_current += src->executor_queued_tasks;
		dst->executor_running_tasks_current += src->executor_running_tasks;
		dst->executor_queue_capacity_current +=
			src->executor_queue_capacity;
		dst->executor_ready_calls_current += src->executor_ready_calls;
	}
	if (src->executor_queue.peak >
	    dst->executor_queue_peak_max_per_endpoint)
		dst->executor_queue_peak_max_per_endpoint =
			src->executor_queue.peak;
	if (src->executor_ready_calls_peak >
	    dst->executor_ready_calls_peak_max_per_endpoint)
		dst->executor_ready_calls_peak_max_per_endpoint =
			src->executor_ready_calls_peak;

	dst->executor_enqueued_tasks += src->executor_enqueued_tasks;
	dst->executor_taken_tasks += src->executor_taken_tasks;
	dst->executor_admission_limit_hits +=
		src->executor_admission_limit_hits;
	dst->executor_hard_full_events += src->executor_hard_full_events;
	tr_merge_latency_histogram(&dst->executor_queue_wait_ns,
				   &src->executor_queue_wait_ns);
	tr_merge_latency_histogram(&dst->executor_handler_ns,
				   &src->executor_handler_ns);

	dst->calls_started += src->calls_started;
	dst->calls_completed += src->calls_completed;
	dst->calls_cancelled += src->calls_cancelled;
	dst->calls_deadline_exceeded += src->calls_deadline_exceeded;
	tr_server_merge_rpc_semantic_stats(&dst->semantic, &src->semantic);
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
                                 int record_mode)
{
	uint32_t shards;

	if (!config || !tuning || config->shard_count == 0U)
		return 0;
	shards = config->shard_count;

	return config->max_peers >= shards &&
	       (uint32_t)config->listen_backlog >= shards &&
	       (record_mode || tuning->executor_threads >= shards) &&
	       tuning->command_capacity >= shards &&
	       tuning->tx_item_capacity >= shards &&
	       tuning->control_tx_item_capacity >= shards &&
	       tuning->rx_buffer_count >= shards &&
	       (record_mode || tuning->rpc_message_pool_count >= shards) &&
	       tuning->reassembly_pool_count >= shards;
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

	if (config->interceptor.fn || config->interceptor.arg ||
	    groups->max_groups || groups->max_connections ||
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
		if (!methods[i].work || !method->request_codec_id ||
		    !method->response_codec_id ||
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
	TR_TUNING_AT_LEAST_SHARDS(executor_threads);
#undef TR_TUNING_AT_LEAST_SHARDS
}

static int tr_server_register_methods_on_peer(struct tr_server *server,
					      struct tr_runtime_peer *peer)
{
	uint32_t i;

	for (i = 0; i < server->method_count; ++i) {
		struct tr_server_method *m = &server->methods[i];
		int ret;

		if (!m->used)
			continue;
		if (m->kind == TR_SERVER_METHOD_UNARY) {
			ret = tr_rpc_register_method(peer->rpc, &m->desc,
						     m->unary_handler,
						     m->handler_arg);
		} else {
			ret = tr_rpc_register_stream_method(peer->rpc, &m->desc,
							    &m->stream_handlers,
							    m->handler_arg);
		}
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

static int tr_server_peer_handle_equal(const struct tr_runtime_peer *a,
				       const struct tr_runtime_peer *b)
{
	return a->connection.reactor == b->connection.reactor &&
	       a->connection.slot == b->connection.slot &&
	       a->connection.generation == b->connection.generation;
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
	if (!peer)
		return TR_OK;
	/* V2 共享执行器的完成必须先由分片整体清理，不能逐对端抢先交付。 */
	if (peer->record_rpc)
		return TR_ERR_STATE;

	if (peer->rpc) {
		int ret = tr_rpc_endpoint_destroy(peer->rpc);

		if (ret != TR_OK)
			return ret;
		peer->rpc = NULL;
	}
	if (peer->channel) {
		int ret = tr_channel_destroy(peer->channel);

		if (ret != TR_OK)
			return ret;
		peer->channel = NULL;
	}

	free(peer->finalize_ctx);
	peer->finalize_ctx = NULL;
	memset(peer, 0, sizeof(*peer));
	return TR_OK;
}

static void tr_server_finish_detached_peer(
	struct tr_server_detached_peer *detached,
	const struct tr_rpc_endpoint_stats *rpc_stats)
{
	struct tr_server_shard *shard;
	struct tr_server *server;
	struct tr_channel_stats channel_stats;
	struct tr_server_detached_peer **link;
	int have_channel_stats = 0;

	if (!detached)
		return;
	shard = detached->shard;
	server = shard ? shard->server : NULL;
	if (!server || !shard) {
		free(detached);
		return;
	}

	memset(&channel_stats, 0, sizeof(channel_stats));
	if (detached->channel) {
		if (tr_channel_get_stats(detached->channel, &channel_stats) == TR_OK)
			have_channel_stats = 1;
		tr_channel_finalize_detached(detached->channel);
		detached->channel = NULL;
	}

	pthread_mutex_lock(&server->finalizer_lock);
	for (link = &shard->detached_peers; *link; link = &(*link)->next) {
		if (*link == detached) {
			*link = detached->next;
			break;
		}
	}
	if (rpc_stats)
		tr_server_merge_rpc_stats(&shard->retired_rpc_stats, rpc_stats,
		                          0);
	if (have_channel_stats)
		tr_server_merge_channel_stats(&shard->retired_channel_stats,
		                              &channel_stats, 0);
	tr_runtime_shard_peer_note_reaped(shard->runtime);
	pthread_cond_broadcast(&server->finalizer_cond);
	pthread_mutex_unlock(&server->finalizer_lock);

	/* 分片的退役 Endpoint 预算用尽时，断开的对端仍可能占据槽位。
	 * 释放一个退役对象会重新开放解绑准入；唤醒所有者以重试这些槽位。
	 */
	tr_runtime_shard_signal_peer_event(shard->runtime);
	free(detached);
}

static void tr_server_on_endpoint_finalized(
	const struct tr_rpc_endpoint_stats *rpc_stats, void *arg)
{
	tr_server_finish_detached_peer(
		(struct tr_server_detached_peer *)arg, rpc_stats);
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

		if (!peer || !peer->used)
			continue;
		if (peer->record_closing && peer->connection.reactor) {
			ret = tr_reactor_close_on_owner(peer->connection);
			if (ret != TR_OK && ret != TR_ERR_STALE)
				continue;
		} else if (!tr_server_peer_disconnected(peer)) {
			continue;
		}
		/* 工作或解绑尚未静默时保留原槽位，不能制造第二条退役队列。 */
		if (peer->record_rpc) {
			ret = tr_rpc_record_endpoint_close_on_owner(
			        &peer->record_rpc);
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
		memset(peer, 0, sizeof(*peer));
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

		if (!peer || !peer->record_rpc)
			continue;
		ret = tr_rpc_record_endpoint_reap_stopped(&peer->record_rpc);
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

		if (!peer || !peer->record_rpc)
			continue;
		ret = tr_rpc_record_endpoint_reap_stopped(&peer->record_rpc);
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
	uint32_t i;

	if (shard->server->record_mode) {
		tr_server_record_detach_peers_on_owner(shard);
		return;
	}

	for (i = 0; i < tr_server_shard_peer_capacity(shard); ++i) {
		struct tr_runtime_peer *peer = tr_server_shard_peer_at(shard, i);
		struct tr_runtime_peer snapshot;
		struct tr_server_detached_peer *detached;
		int ret;

		if (!peer)
			continue;

		if (!peer->used)
			continue;
		snapshot = *peer;

		if (!tr_server_peer_disconnected(&snapshot))
			continue;

		detached =
			(struct tr_server_detached_peer *)snapshot.finalize_ctx;
		if (!detached)
			continue;

		/* 对端表槽位生命周期与退役 Endpoint
		 * 生命周期分离，但二者都必须有界。
		 * 所有退役槽位均在使用时，保持此已断开对端的发布状态，
		 * 等收尾器通知分片生命周期 eventfd 后重试。
		 */
		if (snapshot.rpc &&
		    tr_runtime_shard_peer_reaping_at_capacity(shard->runtime))
			break;

		/* 在 Channel DOWN 通知返回后于反应器所有者上运行。
		 * 先移除所有所有者可见的回调与定时器源。
		 */
		if (snapshot.rpc) {
			ret = tr_rpc_endpoint_detach_for_finalize(snapshot.rpc);
			if (ret != TR_OK)
				continue;
		}
		if (snapshot.channel) {
			ret = tr_channel_detach_for_finalize(snapshot.channel);
			if (ret != TR_OK)
				continue;
		}

		if (!peer->used ||
		    !tr_server_peer_handle_equal(peer, &snapshot) ||
		    peer->channel != snapshot.channel ||
		    peer->rpc != snapshot.rpc ||
		    peer->finalize_ctx != snapshot.finalize_ctx)
			continue;

		if (snapshot.rpc) {
			ret = tr_rpc_endpoint_arm_detached_finalizer(
				snapshot.rpc, tr_server_on_endpoint_finalized,
				detached);
			if (ret != TR_OK)
				continue;
		}

		detached->shard = shard;
		detached->channel = snapshot.channel;
		/* 保留所有者最后快照，直到归档快照替换它。
		 * 不在所有者引用释放后保留 Endpoint 指针。
		 */
		if (snapshot.rpc)
			(void)tr_rpc_endpoint_get_stats(snapshot.rpc,
			                                &detached->rpc_stats);
		if (snapshot.channel)
			(void)tr_channel_get_stats(snapshot.channel,
			                           &detached->channel_stats);
		pthread_mutex_lock(&shard->server->finalizer_lock);
		detached->next = shard->detached_peers;
		shard->detached_peers = detached;
		pthread_mutex_unlock(&shard->server->finalizer_lock);

		/* 槽位生命周期独立于旧 Endpoint 工作线程引用。
		 * 一旦所有者解绑完成，就立即清空槽位，使新对端可以复用，
		 * 即使已解绑 Endpoint 仍在排空。
		 */
		memset(peer, 0, sizeof(*peer));
		tr_runtime_shard_peer_note_removed_for_reap(shard->runtime);

		if (snapshot.rpc)
			tr_rpc_endpoint_release_detached_owner(snapshot.rpc);
		else
			tr_server_finish_detached_peer(detached, NULL);
	}
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

static void tr_server_wait_peer_finalizers(struct tr_server *server)
{
	pthread_mutex_lock(&server->finalizer_lock);
	for (;;) {
		uint32_t pending = 0U;
		uint32_t i;

		for (i = 0; i < server->shard_count; ++i) {
			struct tr_runtime_peer_stats stats;

			tr_runtime_shard_peer_stats(
				server->shards[i].runtime, &stats);
			pending += stats.reaping_current;
		}
		if (pending == 0U)
			break;
		pthread_cond_wait(&server->finalizer_cond,
				  &server->finalizer_lock);
	}
	pthread_mutex_unlock(&server->finalizer_lock);
}

static int tr_server_disable_peer_events(struct tr_server_shard *shard)
{
	int ret;

	if (!shard)
		return TR_ERR_INVALID;
	if (shard->server->record_mode &&
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

	if (!shard || !peer)
		return cause;
	if (shard->server->record_mode)
		peer->record_closing = 1;

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

	/* 一旦 Channel/RPC 发布已经建立，不能在当前所有者回调中
	 * 同步销毁。先发布部分对端，延迟到后续分片生命周期处理轮次做
	 * 解绑与收尾；连接已在上面完成同步退役。
	 */
	if (peer->channel || peer->rpc || peer->record_rpc ||
	    (shard->server->record_mode && close_ret != TR_OK &&
	     peer->connection.reactor)) {
		peer->used = 1;
		tr_server_note_peer_added_owner(shard);
		tr_server_signal_peer_cleanup(shard);
	} else {
		free(peer->finalize_ctx);
		memset(peer, 0, sizeof(*peer));
	}

	return close_ret != TR_OK ? close_ret : cause;
}

static int tr_server_adopt_peer(struct tr_server_shard *shard, int fd)
{
	struct tr_server *server = shard->server;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_runtime_peer *peer;
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
	memset(peer, 0, sizeof(*peer));
	ret = tr_deadline_init_ms(&peer->handshake_deadline,
				  server->config.handshake_timeout_ms);
	if (ret != TR_OK)
		return ret;
	ret = tr_server_arm_handshake_on_owner(shard, peer);
	if (ret != TR_OK)
		return ret;
	if (!server->record_mode) {
		peer->finalize_ctx =
		        calloc(1, sizeof(struct tr_server_detached_peer));
		if (!peer->finalize_ctx)
			return TR_ERR_NOMEM;
		((struct tr_server_detached_peer *)peer->finalize_ctx)->shard =
		        shard;
	}

	if (tr_tcp_nodelay_policy_enabled(server->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(owned_fd, 1);
		if (ret != TR_OK)
			goto rollback;
	}

	ret = tr_reactor_adopt_fd(tr_server_shard_reactor(shard), owned_fd,
				  &peer->connection);
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
	channel_config.reassembly_pool = &shard->reassembly_pool;

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

	if (server->record_mode) {
		ret = tr_rpc_record_endpoint_create_on_owner(
		        peer->channel, shard->record_tasks,
		        server->record_methods, server->record_method_count,
		        &server->record.endpoint, tr_server_record_terminal,
		        shard->runtime,
		        tr_runtime_shard_memory_budget(shard->runtime),
		        &peer->record_rpc);
		if (ret == TR_OK)
			ret = tr_rpc_record_endpoint_start_on_owner(
			        peer->record_rpc);
	} else {
		memset(&rpc_config, 0, sizeof(rpc_config));
		rpc_config.role = TR_RPC_SERVER;
		rpc_config.max_methods = server->config.limits.max_methods;
		rpc_config.max_calls = server->config.limits.max_calls;
		rpc_config.message_pool = &shard->rpc_message_pool;
		rpc_config.executor_threads = shard->executor_threads;
		rpc_config.executor_queue_capacity =
		        server->tuning.executor_queue_capacity;
		rpc_config.executor_continuation_reserve =
		        server->tuning.executor_continuation_reserve;
		rpc_config.observability_flags =
		        server->tuning.observability_flags;
		rpc_config.interceptor = server->config.interceptor;

		ret = tr_rpc_endpoint_create_with_executor_group(
		        peer->channel, &rpc_config,
		        tr_server_shard_rpc_executor(shard), &peer->rpc);
		if (ret == TR_OK)
			ret = tr_server_register_methods_on_peer(server, peer);
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
tr_server_create_internal(const struct tr_server_config *config,
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

	if (tuning)
		effective_tuning = *tuning;
	else
		tr_facade_tuning_init(&effective_tuning);
	tr_facade_tuning_normalize(&effective_tuning);
	if (!tuning)
		tr_server_tuning_ensure_shard_minimums(
			&effective_tuning, effective.shard_count);
	if (record) {
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
	    (!record && effective_tuning.executor_continuation_reserve != 0 &&
	     effective_tuning.executor_continuation_reserve >=
	             (effective_tuning.executor_queue_capacity < 16U
	                      ? 16U
	                      : effective_tuning.executor_queue_capacity)) ||
	    effective.max_peers == 0 ||
	    effective.max_peers > (UINT32_MAX - 4U) / 2U ||
	    !tr_server_connection_group_config_valid(
	            &effective.connection_groups) ||
	    effective.connection_groups.max_connections >
	            UINT32_MAX - effective.max_peers - 4U ||
	    !tr_server_budget_supports_shards(&effective, &effective_tuning,
	                                      record != NULL))
		return TR_ERR_INVALID;

	server_mem = (struct tr_server *)calloc(1, sizeof(*server_mem));
	if (!server_mem)
		return TR_ERR_NOMEM;
	server_mem->config = effective;
	server_mem->tuning = effective_tuning;
	server_mem->record_mode = record != NULL;
	if (record)
		server_mem->record = *record;

	if (pthread_mutex_init(&server_mem->finalizer_lock, NULL) != 0)
		return TR_ERR_INVALID;
	server = tr_server_mem_take(&server_mem);
	if (pthread_cond_init(&server->finalizer_cond, NULL) != 0)
		return tr_server_create_rollback(out, server, TR_ERR_SYS);
	server->finalizer_cond_ready = 1;

	if (!record) {
		server->methods = (struct tr_server_method *)calloc(
		        effective.limits.max_methods, sizeof(*server->methods));
		if (!server->methods)
			return tr_server_create_rollback(out, server,
			                                 TR_ERR_NOMEM);
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
			if (record) {
				shard_config->memory_budget_bytes =
				        tr_server_record_budget_share(
				                record->memory_budget_bytes,
				                effective.shard_count, i);
			} else {
				shard_config->rpc_executor.endpoint_capacity =
				        peer_capacity;
				shard_config->rpc_executor
				        .max_calls_per_endpoint =
				        effective.limits.max_calls;
				shard_config->rpc_executor
				        .thread_count = tr_server_budget_share(
				        effective_tuning.executor_threads,
				        effective.shard_count, i);
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
			server_shard->executor_threads =
				tr_server_budget_share(
					effective_tuning.executor_threads,
					effective.shard_count, i);
			if (!server_shard->runtime)
				return tr_server_create_rollback(
					out, server, TR_ERR_STATE);

			/* 先发布有效 Runtime 绑定，再获取分片资源。后续失败时，
			 * 当前分片由各 pool_ready 标志精确回收；尚未绑定的尾部
			 * 条目不进入销毁和收尾器等待路径。
			 */
			server->shard_count = i + 1U;

			if (!record) {
				ret = tr_buffer_pool_init_dynamic_budgeted(
				        &server_shard->rpc_message_pool,
				        tr_server_budget_share(
				                effective_tuning
				                        .rpc_message_pool_count,
				                effective.shard_count, i),
				        effective.limits.max_message_bytes,
				        tr_runtime_shard_memory_budget(
				                server_shard->runtime));
				if (ret != TR_OK)
					return tr_server_create_rollback(
					        out, server, ret);
				server_shard->rpc_pool_ready = 1;
			}

			ret = tr_buffer_pool_init_budgeted(
				&server_shard->reassembly_pool,
				tr_server_budget_share(
					effective_tuning.reassembly_pool_count,
					effective.shard_count, i),
				effective.limits.max_message_bytes,
				tr_runtime_shard_memory_budget(
					server_shard->runtime));
			if (ret != TR_OK)
				return tr_server_create_rollback(out, server, ret);
			server_shard->reassembly_pool_ready = 1;
		}
	}
	if (record) {
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
		group_config.authorize_control =
			tr_server_connection_group_authorize;
		group_config.authorize_arg = server;
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

int tr_server_create_with_tuning(const struct tr_server_config *config,
                                 const struct tr_facade_tuning *tuning,
                                 struct tr_server **out)
{
	return tr_server_create_internal(config, tuning, NULL, NULL, 0, out);
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
	return tr_server_create_internal(config, tuning, record, methods,
	                                 method_count, out);
}

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out)
{
	return tr_server_create_with_tuning(config, NULL, out);
}

static int tr_server_validate_method(const struct tr_rpc_method_desc *method,
				     int unary)
{
	if (!method || method->service_id == 0U || method->method_id == 0U ||
	    (method->request_cardinality != TR_RPC_ONE &&
	     method->request_cardinality != TR_RPC_MANY) ||
	    (method->response_cardinality != TR_RPC_ONE &&
	     method->response_cardinality != TR_RPC_MANY) ||
	    method->request_codec_id != TR_RPC_CODEC_RAW ||
	    method->response_codec_id != TR_RPC_CODEC_RAW ||
	    (method->lane != TR_LANE_CONTROL && method->lane != TR_LANE_BULK) ||
	    method->max_request_bytes == 0U || method->max_response_bytes == 0U)
		return TR_ERR_INVALID;

	if (unary && (method->request_cardinality != TR_RPC_ONE ||
		      method->response_cardinality != TR_RPC_ONE))
		return TR_ERR_INVALID;
	return TR_OK;
}

static int tr_server_method_exists(struct tr_server *server,
				   uint32_t service_id, uint32_t method_id)
{
	uint32_t i;

	for (i = 0; i < server->method_count; ++i)
		if (server->methods[i].used &&
		    server->methods[i].desc.service_id == service_id &&
		    server->methods[i].desc.method_id == method_id)
			return 1;
	return 0;
}

int tr_server_register_method(struct tr_server *server,
			      const struct tr_rpc_method_desc *method,
			      tr_rpc_unary_handler handler, void *handler_arg)
{
	struct tr_server_method *entry;

	if (server && server->record_mode)
		return TR_ERR_UNSUPPORTED;
	if (!server || !handler ||
	    tr_server_validate_method(method, 1) != TR_OK)
		return TR_ERR_INVALID;
	if (server->started)
		return TR_ERR_STATE;
	if (server->method_count >= server->config.limits.max_methods)
		return TR_AGAIN;
	if (tr_server_method_exists(server, method->service_id,
				    method->method_id))
		return TR_ERR_STATE;

	entry = &server->methods[server->method_count++];
	memset(entry, 0, sizeof(*entry));
	entry->used = 1;
	entry->kind = TR_SERVER_METHOD_UNARY;
	entry->desc = *method;
	entry->unary_handler = handler;
	entry->handler_arg = handler_arg;
	return TR_OK;
}

int tr_server_register_stream_method(
	struct tr_server *server, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg)
{
	struct tr_server_method *entry;

	if (server && server->record_mode)
		return TR_ERR_UNSUPPORTED;
	if (!server || !handlers ||
	    tr_server_validate_method(method, 0) != TR_OK)
		return TR_ERR_INVALID;
	if (server->started)
		return TR_ERR_STATE;
	if (server->method_count >= server->config.limits.max_methods)
		return TR_AGAIN;
	if (tr_server_method_exists(server, method->service_id,
				    method->method_id))
		return TR_ERR_STATE;

	entry = &server->methods[server->method_count++];
	memset(entry, 0, sizeof(*entry));
	entry->used = 1;
	entry->kind = TR_SERVER_METHOD_STREAM;
	entry->desc = *method;
	entry->stream_handlers = *handlers;
	entry->handler_arg = handler_arg;
	return TR_OK;
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
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->published)
		return TR_ERR_STATE;
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
	if (!server->connection_group_listener)
		return TR_ERR_STATE;
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
	if (!server->connection_group_listener)
		return TR_ERR_STATE;
	return tr_server_connection_group_stop_internal(server);
}

int tr_server_connection_group_begin_drain(struct tr_server *server)
{
	if (!server)
		return TR_ERR_INVALID;
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
	if (tr_server_blocking_lifecycle_context())
		return TR_ERR_STATE;
	if (!server->published ||
	    (server->record_mode && server->record_start_attempted))
		return TR_ERR_STATE;
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

	if (server->record_mode)
		server->record_start_attempted = 1;
	ret = tr_runtime_start(server->runtime);
	if (ret != TR_OK) {
		if (server->record_mode)
			server->published = 0;
		return ret;
	}
	/* 所有分片的任务所有者和重试源就绪后才开放任何 TCP 接纳源。 */
	if (server->record_mode)
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

	if (server->record_mode) {
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

	if (server->record_mode) {
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

	return final;
}

/* 仅归档累计字段；当前资源量指标来自存活对端。 */
static void tr_server_collect_retired_stats(struct tr_server_shard *shard,
                                            struct tr_server_stats *stats)
{
	struct tr_server_detached_peer *detached;

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
	if (shard->retired_rpc_stats.executor_threads >
	    stats->rpc.executor_threads)
		stats->rpc.executor_threads =
		        shard->retired_rpc_stats.executor_threads;
	if (shard->retired_rpc_stats.executor_queue_peak_max_per_endpoint >
	    stats->rpc.executor_queue_peak_max_per_endpoint)
		stats->rpc.executor_queue_peak_max_per_endpoint =
		        shard->retired_rpc_stats
		                .executor_queue_peak_max_per_endpoint;
	if (shard->retired_rpc_stats
	            .executor_ready_calls_peak_max_per_endpoint >
	    stats->rpc.executor_ready_calls_peak_max_per_endpoint)
		stats->rpc.executor_ready_calls_peak_max_per_endpoint =
		        shard->retired_rpc_stats
		                .executor_ready_calls_peak_max_per_endpoint;
	stats->rpc.executor_enqueued_tasks +=
	        shard->retired_rpc_stats.executor_enqueued_tasks;
	stats->rpc.executor_taken_tasks +=
	        shard->retired_rpc_stats.executor_taken_tasks;
	stats->rpc.executor_admission_limit_hits +=
	        shard->retired_rpc_stats.executor_admission_limit_hits;
	stats->rpc.executor_hard_full_events +=
	        shard->retired_rpc_stats.executor_hard_full_events;
	tr_merge_latency_histogram(
	        &stats->rpc.executor_queue_wait_ns,
	        &shard->retired_rpc_stats.executor_queue_wait_ns);
	tr_merge_latency_histogram(
	        &stats->rpc.executor_handler_ns,
	        &shard->retired_rpc_stats.executor_handler_ns);
	stats->rpc.calls_started += shard->retired_rpc_stats.calls_started;
	stats->rpc.calls_completed += shard->retired_rpc_stats.calls_completed;
	stats->rpc.calls_cancelled += shard->retired_rpc_stats.calls_cancelled;
	stats->rpc.calls_deadline_exceeded +=
	        shard->retired_rpc_stats.calls_deadline_exceeded;
	tr_server_merge_rpc_semantic_stats(&stats->rpc.semantic,
	                                   &shard->retired_rpc_stats.semantic);

	for (detached = shard->detached_peers; detached;
	     detached = detached->next) {
		tr_server_merge_rpc_stats(&stats->rpc, &detached->rpc_stats, 0);
		tr_server_merge_channel_stats(&stats->channel,
		                              &detached->channel_stats, 0);
	}
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
		struct tr_rpc_endpoint_stats rpc_stats;
		struct tr_channel_stats channel_stats;

		if (!peer || !peer->used)
			continue;

		if (peer->ready && (peer->rpc || peer->record_rpc) &&
		    peer->channel && !tr_server_peer_disconnected(peer))
			stats->peers_ready_current++;

		if (peer->rpc &&
		    tr_rpc_endpoint_get_stats(peer->rpc, &rpc_stats) == TR_OK)
			tr_server_merge_rpc_stats(&stats->rpc, &rpc_stats, 1);
		if (peer->record_rpc)
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

	return TR_OK;
}

int tr_server_get_stats(struct tr_server *server, struct tr_server_stats *out)
{
	struct tr_server_stats stats;
	uint32_t i;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;
	/* 回滚失败时 *out 可能保留部分构造对象；调用方只能继续销毁，
	 * 不允许统计遍历未发布或已经开始终止拆除的分片数组。
	 */
	if (!server->published || !server->runtime || !server->shards ||
	    server->shard_count == 0U)
		return TR_ERR_STATE;

	memset(&stats, 0, sizeof(stats));
	stats.shard_count = server->shard_count;

	stats.rpc.executor_threads =
	        server->record_mode ? (uint32_t)server->record.tasks.workers
	                            : server->tuning.executor_threads;

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

		if (shard->rpc_pool_ready) {
			memset(&pool_stats, 0, sizeof(pool_stats));
			ret = tr_buffer_pool_get_stats(&shard->rpc_message_pool,
			                               &pool_stats);
			if (ret != TR_OK)
				return ret;
			tr_server_merge_pool_observation(
			        &stats.rpc_message_pool, &pool_stats);
		}

		memset(&pool_stats, 0, sizeof(pool_stats));
		ret = tr_buffer_pool_get_stats(&shard->reassembly_pool,
					       &pool_stats);
		if (ret != TR_OK)
			return ret;
		tr_server_merge_pool_observation(&stats.reassembly_pool,
						 &pool_stats);

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

int tr_server_get_rpc_semantic_stats(
	struct tr_server *server, struct tr_rpc_semantic_stats *out)
{
	struct tr_server_stats stats;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;
	if (server->record_mode)
		return TR_ERR_UNSUPPORTED;

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

	/* 销毁即使稍后需要重试也不再允许读取跨分片统计。 */
	server->published = 0;
	if (server->record_mode && server->runtime) {
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
	if (server->record_mode && server->shards) {
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

				if (!peer || (!peer->used && !peer->channel &&
				              !peer->rpc && !peer->record_rpc))
					continue;
				{
					ret = tr_server_destroy_peer(peer);
					if (ret != TR_OK)
						return ret;
				}
			}
		}
	}

	tr_server_wait_peer_finalizers(server);

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
			if (shard->rpc_pool_ready) {
				int ret = tr_buffer_pool_destroy(
					&shard->rpc_message_pool);
				if (ret != TR_OK)
					return ret;
				shard->rpc_pool_ready = 0;
			}
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
		free(server->record_methods);
		server->record_methods = NULL;
		server->record_method_bytes = 0;
		server->record_method_count = 0;
		ret = tr_memory_budget_release(budget, bytes);
		if (ret != TR_OK)
			return ret;
	}

	if (server->runtime) {
		ret = tr_runtime_destroy(server->runtime);
		if (ret != TR_OK)
			return ret;
		server->runtime = NULL;
	}

	free(server->shards);
	server->shards = NULL;
	server->shard_count = 0U;

	free(server->methods);
	if (server->finalizer_cond_ready)
		pthread_cond_destroy(&server->finalizer_cond);
	pthread_mutex_destroy(&server->finalizer_lock);
	free(server);
	return TR_OK;
}
