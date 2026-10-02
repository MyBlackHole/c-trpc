#include "tr/server.h"

#include <errno.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "tr/buffer.h"
#include "tr/channel.h"
#include "channel_internal.h"
#include "tr/reactor.h"
#include "tr/rpc_wire.h"
#include "tr/socket.h"
#include "tr/status.h"
#include "rpc_internal.h"
#include "runtime_internal.h"
#include "socket_internal.h"
#include "observability_internal.h"

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

struct tr_server {
	struct tr_server_config config;

	struct tr_runtime *runtime;
	struct tr_runtime_shard *shard;
	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_pool_ready;
	int reassembly_pool_ready;

	struct tr_server_method *methods;
	uint32_t method_count;

	struct tr_server_channel_stats retired_channel_stats;
	struct tr_server_rpc_stats retired_rpc_stats;

	pthread_mutex_t lock;
	pthread_cond_t finalizer_cond;
	int finalizer_cond_ready;
	int started;
	int peer_events_enabled;
};

TR_DEFINE_PTR_OWNERSHIP(tr_server_mem, struct tr_server, free)
TR_DEFINE_PTR_OWNERSHIP(tr_server_owner, struct tr_server, tr_server_destroy)

static struct tr_reactor *tr_server_reactor(struct tr_server *server)
{
	return server ? tr_runtime_shard_reactor(server->shard) : NULL;
}

static struct tr_rpc_executor_group *
tr_server_rpc_executor(struct tr_server *server)
{
	return server ? tr_runtime_shard_rpc_executor(server->shard) : NULL;
}

static int tr_server_listener_fd(struct tr_server *server)
{
	return server ? tr_runtime_shard_listener_fd(server->shard) : -1;
}

static uint32_t tr_server_peer_capacity(struct tr_server *server)
{
	return server ? tr_runtime_shard_peer_capacity(server->shard) : 0U;
}

static struct tr_runtime_peer *
tr_server_peer_at(struct tr_server *server, uint32_t slot)
{
	return server ? tr_runtime_shard_peer_at(server->shard, slot) : NULL;
}

static uint64_t tr_server_now_ms(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000) +
	       (uint64_t)ts.tv_nsec / UINT64_C(1000000);
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
}

static void tr_server_normalize_config(struct tr_server_config *config)
{
	struct tr_server_config defaults;
	struct tr_facade_limits *l;
	const struct tr_facade_limits *d;

	tr_server_config_init(&defaults);
	if (config->max_peers == 0)
		config->max_peers = defaults.max_peers;
	if (config->listen_backlog <= 0)
		config->listen_backlog = defaults.listen_backlog;

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

void tr_server_config_init(struct tr_server_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	tr_facade_limits_init(&config->limits);
	config->max_peers = 64U;
	config->listen_backlog = 128;
	config->tcp_nodelay = TR_TCP_NODELAY_DEFAULT;
	config->keepalive_interval_ms = 30000U;
	config->keepalive_timeout_ms = 10000U;
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

static void tr_server_note_peer_added_locked(struct tr_server *server)
{
	tr_runtime_shard_peer_note_added(server->shard);
}

static void tr_server_finish_finalizing_peer(
	struct tr_server *server, struct tr_runtime_peer *peer,
	const struct tr_rpc_endpoint_stats *rpc_stats)
{
	struct tr_channel_stats channel_stats;
	int have_channel_stats = 0;

	memset(&channel_stats, 0, sizeof(channel_stats));
	if (peer->channel) {
		if (tr_channel_get_stats(peer->channel, &channel_stats) == TR_OK)
			have_channel_stats = 1;
		tr_channel_finalize_detached(peer->channel);
	}

	pthread_mutex_lock(&server->lock);
	if (rpc_stats)
		tr_server_merge_rpc_stats(&server->retired_rpc_stats,
					  rpc_stats, 0);
	if (have_channel_stats)
		tr_server_merge_channel_stats(&server->retired_channel_stats,
					      &channel_stats, 0);
	memset(peer, 0, sizeof(*peer));
	tr_runtime_shard_peer_note_reaped(server->shard);
	pthread_cond_broadcast(&server->finalizer_cond);
	pthread_mutex_unlock(&server->lock);
}

static void tr_server_on_endpoint_finalized(
	const struct tr_rpc_endpoint_stats *rpc_stats, void *arg)
{
	struct tr_runtime_peer *peer = (struct tr_runtime_peer *)arg;
	struct tr_server *server;

	if (!peer)
		return;
	server = (struct tr_server *)peer->owner_arg;
	if (!server)
		return;

	tr_server_finish_finalizing_peer(server, peer, rpc_stats);
}

static void tr_server_finalize_channel_only_peer(
	struct tr_server *server, struct tr_runtime_peer *peer)
{
	tr_server_finish_finalizing_peer(server, peer, NULL);
}

static void tr_server_detach_disconnected_peers_on_owner(
	struct tr_server *server)
{
	uint32_t i;

	for (i = 0; i < tr_server_peer_capacity(server); ++i) {
		struct tr_runtime_peer *peer = tr_server_peer_at(server, i);
		struct tr_runtime_peer snapshot;
		int ret;

		if (!peer)
			continue;

		pthread_mutex_lock(&server->lock);
		if (!peer->used || peer->finalizing) {
			pthread_mutex_unlock(&server->lock);
			continue;
		}
		snapshot = *peer;
		pthread_mutex_unlock(&server->lock);

		if (!tr_server_peer_disconnected(&snapshot))
			continue;

		/*
		 * This function runs on the Reactor owner after the Channel DOWN
		 * callback has returned. Detach all owner-visible protocol sources
		 * before changing peer-table ownership.
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

		pthread_mutex_lock(&server->lock);
		if (!peer->used || peer->finalizing ||
		    !tr_server_peer_handle_equal(peer, &snapshot) ||
		    peer->channel != snapshot.channel ||
		    peer->rpc != snapshot.rpc) {
			pthread_mutex_unlock(&server->lock);
			continue;
		}

		/*
		 * Arm the Endpoint finalizer before ownership leaves the table, but
		 * do not release its owner ref yet: a zero-worker Endpoint could
		 * otherwise finalize synchronously before peer state is published.
		 */
		if (snapshot.rpc) {
			ret = tr_rpc_endpoint_arm_detached_finalizer(
				snapshot.rpc, tr_server_on_endpoint_finalized, peer);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&server->lock);
				continue;
			}
		}

		peer->used = 0;
		peer->finalizing = 1;
		peer->owner_arg = server;
		tr_runtime_shard_peer_note_removed_for_reap(server->shard);
		pthread_mutex_unlock(&server->lock);

		if (snapshot.rpc) {
			/*
			 * Last-ref finalization may run synchronously here or later on
			 * the worker/completion thread that releases the final task ref.
			 */
			tr_rpc_endpoint_release_detached_owner(snapshot.rpc);
		} else {
			tr_server_finalize_channel_only_peer(server, peer);
		}
	}
}

static void tr_server_on_peer_lifecycle_event(int fd, uint32_t events,
					      void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;

	(void)fd;
	if (!server || !(events & EPOLLIN))
		return;

	tr_runtime_shard_drain_peer_event(server->shard);
	tr_server_detach_disconnected_peers_on_owner(server);
}

static void tr_server_wait_peer_finalizers(struct tr_server *server)
{
	struct tr_runtime_peer_stats stats;

	pthread_mutex_lock(&server->lock);
	for (;;) {
		tr_runtime_shard_peer_stats(server->shard, &stats);
		if (stats.reaping_current == 0U)
			break;
		pthread_cond_wait(&server->finalizer_cond, &server->lock);
	}
	pthread_mutex_unlock(&server->lock);
}

static void tr_server_disable_peer_events(struct tr_server *server)
{
	if (!server || !server->peer_events_enabled)
		return;

	(void)tr_runtime_shard_disable_peer_events(server->shard);
	server->peer_events_enabled = 0;
}

static void tr_server_signal_peer_cleanup(struct tr_server *server)
{
	if (server)
		tr_runtime_shard_signal_peer_event(server->shard);
}

static void tr_server_on_channel_lifecycle(struct tr_channel *channel,
					   enum tr_channel_event event,
					   int status, void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;

	(void)channel;
	(void)status;
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN)
		tr_server_signal_peer_cleanup(server);
}

struct tr_server_peer_guard {
	struct tr_server *server;
	struct tr_runtime_peer *peer;
	int armed;
};

static void tr_server_peer_guard_cleanup(struct tr_server_peer_guard *guard)
{
	struct tr_runtime_peer *peer;

	if (!guard || !guard->armed || !guard->server || !guard->peer)
		return;
	peer = guard->peer;

	if (peer->connection.reactor)
		(void)tr_reactor_close(peer->connection);

	/*
	 * 一旦 Channel/RPC 状态已经建立，Reactor callback 就可能观察过它们。
	 * 此时失败不能直接 free；必须先发布 partial peer，再通过
	 * shard peer event 延后到当前 owner callback 返回后执行 detach。
	 */
	if (peer->channel || peer->rpc) {
		pthread_mutex_lock(&guard->server->lock);
		peer->used = 1;
		tr_server_note_peer_added_locked(guard->server);
		pthread_mutex_unlock(&guard->server->lock);
		tr_server_signal_peer_cleanup(guard->server);
	} else {
		memset(&peer->connection, 0, sizeof(peer->connection));
	}
}

static int tr_server_adopt_peer(struct tr_server *server, int fd)
{
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_runtime_peer *peer;
	struct tr_server_peer_guard peer_guard
		TR_AUTO(tr_server_peer_guard_cleanup) = { server, NULL, 0 };
	uint32_t slot;
	int owned_fd TR_AUTO(tr_fd_cleanup) = fd;
	int ret;

	pthread_mutex_lock(&server->lock);
	{
		struct tr_runtime_peer_stats peer_stats;

		tr_runtime_shard_peer_stats(server->shard, &peer_stats);
		if (peer_stats.current + peer_stats.reaping_current >=
		    peer_stats.capacity) {
			tr_runtime_shard_peer_note_capacity_rejection(server->shard);
			pthread_mutex_unlock(&server->lock);
			return TR_AGAIN;
		}
	}

	for (slot = 0; slot < tr_server_peer_capacity(server); ++slot) {
		peer = tr_server_peer_at(server, slot);
		if (peer && !peer->used && !peer->finalizing)
			break;
	}
	if (slot == tr_server_peer_capacity(server)) {
		tr_runtime_shard_peer_note_capacity_rejection(server->shard);
		pthread_mutex_unlock(&server->lock);
		return TR_AGAIN;
	}

	peer = tr_server_peer_at(server, slot);
	memset(peer, 0, sizeof(*peer));
	peer_guard.peer = peer;
	pthread_mutex_unlock(&server->lock);

	if (tr_tcp_nodelay_policy_enabled(server->config.tcp_nodelay)) {
		ret = tr_tcp_set_nodelay(owned_fd, 1);
		if (ret != TR_OK)
			return ret;
	}

	ret = tr_reactor_adopt_fd(tr_server_reactor(server), owned_fd,
				  &peer->connection);
	if (ret != TR_OK)
		return ret;
	(void)tr_fd_take(&owned_fd);
	peer_guard.armed = 1;

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
	channel_config.reassembly_pool = &server->reassembly_pool;

	/*
	 * Server 必须先完成 RPC Endpoint/Method 安装，再启动 HELLO。
	 * 否则 Client 可能先观察到 Channel UP，并在 Server service ready 前
	 * 发送首条 RPC，导致消息被尚未安装上层 handler 的 Channel 消费。
	 */
	ret = tr_channel_create_deferred(&channel_config, peer->connection,
					 peer->connection, NULL, NULL, NULL, NULL,
					 &peer->channel);
	if (ret != TR_OK)
		return ret;

	ret = tr_channel_set_lifecycle_observer(
		peer->channel, tr_server_on_channel_lifecycle, server);
	if (ret != TR_OK)
		return ret;

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_SERVER;
	rpc_config.max_methods = server->config.limits.max_methods;
	rpc_config.max_calls = server->config.limits.max_calls;
	rpc_config.message_pool = &server->rpc_message_pool;
	rpc_config.executor_threads = server->config.limits.executor_threads;
	rpc_config.executor_queue_capacity =
		server->config.limits.executor_queue_capacity;
	rpc_config.executor_continuation_reserve =
		server->config.limits.executor_continuation_reserve;
	rpc_config.observability_flags =
		server->config.limits.observability_flags;

	ret = tr_rpc_endpoint_create_with_executor_group(
		peer->channel, &rpc_config, tr_server_rpc_executor(server),
		&peer->rpc);
	if (ret != TR_OK)
		return ret;

	ret = tr_server_register_methods_on_peer(server, peer);
	if (ret != TR_OK)
		return ret;

	ret = tr_channel_start(peer->channel);
	if (ret != TR_OK)
		return ret;

	if (server->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			server->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			server->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(peer->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			return ret;
	}

	pthread_mutex_lock(&server->lock);
	peer->used = 1;
	tr_server_note_peer_added_locked(server);
	tr_runtime_shard_peer_note_ready(server->shard);
	pthread_mutex_unlock(&server->lock);
	/*
	 * Cover the race where the connection went DOWN before this peer became
	 * visible to the reaper. The signal is coalesced by the shard eventfd.
	 */
	tr_server_signal_peer_cleanup(server);
	peer_guard.armed = 0;
	return TR_OK;
}

static void tr_server_on_listener_ready(int listener, uint32_t events,
					void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;
	uint32_t accepted = 0U;

	if (!server || !(events & EPOLLIN))
		return;

	while (accepted < TR_SERVER_ACCEPT_BATCH) {
		int fd = -1;
		int ret = tr_tcp_accept(listener, &fd);

		if (ret == TR_AGAIN)
			break;
		if (ret != TR_OK)
			break;

		/*
		 * Runs on the Reactor owner. adopt_fd takes the owner fast path so
		 * Channel/RPC setup sees an ACTIVE connection immediately.
		 *
		 * Bound each listener callback so an accept flood cannot monopolize
		 * one Reactor turn. Level-triggered epoll will report the listener
		 * again while backlog remains.
		 */
		(void)tr_server_adopt_peer(server, fd);
		accepted++;
	}
}

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out)
{
	struct tr_server_config effective;
	struct tr_runtime_config runtime_config;
	struct tr_reactor_config *reactor_config;
	struct tr_server *server_mem TR_AUTO(tr_server_mem_cleanup) = NULL;
	struct tr_server *server TR_AUTO(tr_server_owner_cleanup) = NULL;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (config)
		effective = *config;
	else
		tr_server_config_init(&effective);
	tr_server_normalize_config(&effective);

	if (!tr_tcp_nodelay_policy_valid(effective.tcp_nodelay) ||
	    effective.limits.max_message_bytes <
		    effective.limits.max_frame_payload_bytes ||
	    effective.limits.rpc_message_buffer_bytes <
		    TR_RPC_WIRE_HEADER_SIZE ||
	    (effective.limits.executor_continuation_reserve != 0 &&
	     effective.limits.executor_continuation_reserve >=
		     (effective.limits.executor_queue_capacity < 16U ?
			      16U :
			      effective.limits.executor_queue_capacity)) ||
	    effective.max_peers == 0 ||
	    effective.max_peers > (UINT32_MAX - 4U) / 2U)
		return TR_ERR_INVALID;

	server_mem = (struct tr_server *)calloc(1, sizeof(*server_mem));
	if (!server_mem)
		return TR_ERR_NOMEM;
	server_mem->config = effective;

	if (pthread_mutex_init(&server_mem->lock, NULL) != 0)
		return TR_ERR_INVALID;
	server = tr_server_mem_take(&server_mem);
	if (pthread_cond_init(&server->finalizer_cond, NULL) != 0)
		return TR_ERR_SYS;
	server->finalizer_cond_ready = 1;

	server->methods = (struct tr_server_method *)calloc(
		effective.limits.max_methods, sizeof(*server->methods));
	if (!server->methods) {
		ret = TR_ERR_NOMEM;
		return ret;
	}

	ret = tr_buffer_pool_init(&server->rpc_message_pool,
				  effective.limits.rpc_message_pool_count,
				  effective.limits.rpc_message_buffer_bytes);
	if (ret != TR_OK)
		return ret;
	server->rpc_pool_ready = 1;

	ret = tr_buffer_pool_init(&server->reassembly_pool,
				  effective.limits.reassembly_pool_count,
				  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		return ret;
	server->reassembly_pool_ready = 1;

	memset(&runtime_config, 0, sizeof(runtime_config));
	runtime_config.shard_count = 1U;
	runtime_config.peer_capacity = effective.max_peers;
	runtime_config.rpc_executor.endpoint_capacity = effective.max_peers;
	runtime_config.rpc_executor.max_calls_per_endpoint =
		effective.limits.max_calls;
	runtime_config.rpc_executor.thread_count =
		effective.limits.executor_threads;
	reactor_config = &runtime_config.reactor;
	reactor_config->max_connections = effective.max_peers + 4U;
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

	ret = tr_runtime_create(&runtime_config, &server->runtime);
	if (ret != TR_OK)
		return ret;
	server->shard = tr_runtime_shard_at(server->runtime, 0U);
	if (!server->shard)
		return TR_ERR_STATE;

	*out = tr_server_owner_take(&server);
	return TR_OK;
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
	if (!server || !ipv4_address)
		return TR_ERR_INVALID;
	if (server->started || tr_server_listener_fd(server) >= 0)
		return TR_ERR_STATE;

	return tr_runtime_shard_listen_ipv4(
		server->shard, ipv4_address, port, server->config.listen_backlog,
		out_bound_port);
}

int tr_server_start(struct tr_server *server)
{
	int ret;

	if (!server || tr_server_listener_fd(server) < 0)
		return TR_ERR_STATE;
	if (server->started)
		return TR_ERR_STATE;

	ret = tr_runtime_start(server->runtime);
	if (ret != TR_OK)
		return ret;

	ret = tr_runtime_shard_enable_peer_events(
		server->shard, tr_server_on_peer_lifecycle_event, server);
	if (ret != TR_OK) {
		(void)tr_runtime_stop(server->runtime);
		return ret;
	}
	server->peer_events_enabled = 1;

	ret = tr_runtime_shard_enable_listener_events(
		server->shard, tr_server_on_listener_ready, server);
	if (ret != TR_OK) {
		tr_server_disable_peer_events(server);
		(void)tr_runtime_stop(server->runtime);
		return ret;
	}

	server->started = 1;
	return TR_OK;
}

static void tr_server_stop_accepting(struct tr_server *server)
{
	(void)tr_runtime_shard_disable_listener_events(server->shard);
	tr_runtime_shard_close_listener(server->shard);
}

int tr_server_drain(struct tr_server *server, uint32_t timeout_ms)
{
	uint64_t start;
	uint32_t i;
	int final = TR_OK;

	if (!server || !server->started)
		return TR_ERR_STATE;

	tr_server_stop_accepting(server);
	tr_server_disable_peer_events(server);

	for (i = 0; i < tr_server_peer_capacity(server); ++i) {
		struct tr_runtime_peer *peer = tr_server_peer_at(server, i);

		if (peer && peer->used) {
			int ret = tr_channel_begin_drain(peer->channel);

			if (ret != TR_OK && ret != TR_AGAIN && final == TR_OK)
				final = ret;
		}
	}

	start = tr_server_now_ms();
	for (i = 0; i < tr_server_peer_capacity(server); ++i) {
		struct tr_runtime_peer *peer = tr_server_peer_at(server, i);
		uint32_t remaining = timeout_ms;
		int ret;

		if (!peer || !peer->used)
			continue;
		if (timeout_ms != 0) {
			uint64_t elapsed = tr_server_now_ms() - start;
			if (elapsed >= timeout_ms)
				return TR_ERR_TIMEOUT;
			remaining = (uint32_t)(timeout_ms - elapsed);
		}
		ret = tr_channel_wait_drained(peer->channel, remaining);
		if (ret != TR_OK && final == TR_OK)
			final = ret;
	}

	return final;
}

int tr_server_get_stats(struct tr_server *server, struct tr_server_stats *out)
{
	struct tr_server_stats stats;
	uint32_t i;
	int ret;

	if (!server || !out)
		return TR_ERR_INVALID;

	memset(&stats, 0, sizeof(stats));
	ret = tr_reactor_get_stats(tr_server_reactor(server), &stats.reactor);
	if (ret != TR_OK)
		return ret;
	ret = tr_buffer_pool_get_stats(&server->rpc_message_pool,
				       &stats.rpc_message_pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_buffer_pool_get_stats(&server->reassembly_pool,
				       &stats.reassembly_pool);
	if (ret != TR_OK)
		return ret;

	pthread_mutex_lock(&server->lock);
	{
		struct tr_runtime_peer_stats peer_stats;

		tr_runtime_shard_peer_stats(server->shard, &peer_stats);
		stats.max_peers = peer_stats.capacity;
		stats.peers_current = peer_stats.current;
		stats.peers_peak = peer_stats.peak;
		stats.peers_reaping_current = peer_stats.reaping_current;
		stats.peers_ready_total = peer_stats.ready_total;
		stats.peers_reaped_total = peer_stats.reaped_total;
		stats.peer_capacity_rejections = peer_stats.capacity_rejections;
	}
	stats.channel = server->retired_channel_stats;
	stats.rpc = server->retired_rpc_stats;
	if (stats.rpc.executor_threads == 0)
		stats.rpc.executor_threads =
			server->config.limits.executor_threads;

	for (i = 0; i < tr_server_peer_capacity(server); ++i) {
		struct tr_runtime_peer *peer = tr_server_peer_at(server, i);
		struct tr_rpc_endpoint_stats rpc_stats;
		struct tr_channel_stats channel_stats;

		if (!peer->used)
			continue;

		if (peer->rpc && peer->channel)
			stats.peers_ready_current++;

		if (peer->rpc &&
		    tr_rpc_endpoint_get_stats(peer->rpc, &rpc_stats) == TR_OK)
			tr_server_merge_rpc_stats(&stats.rpc, &rpc_stats, 1);
		if (peer->channel &&
		    tr_channel_get_stats(peer->channel, &channel_stats) == TR_OK)
			tr_server_merge_channel_stats(&stats.channel,
						      &channel_stats, 1);
	}
	pthread_mutex_unlock(&server->lock);

	*out = stats;
	return TR_OK;
}

void tr_server_destroy(struct tr_server *server)
{
	uint32_t i;

	if (!server)
		return;

	if (tr_server_listener_fd(server) >= 0)
		tr_server_stop_accepting(server);
	tr_server_disable_peer_events(server);

	if (server->runtime && server->started)
		(void)tr_runtime_stop(server->runtime);

	for (i = 0; i < tr_server_peer_capacity(server); ++i) {
		struct tr_runtime_peer *peer = tr_server_peer_at(server, i);

		if (!peer || peer->finalizing ||
		    (!peer->used && !peer->channel && !peer->rpc))
			continue;
		tr_server_destroy_peer(server, peer, 0);
	}

	tr_server_wait_peer_finalizers(server);

	if (server->runtime)
		tr_runtime_destroy(server->runtime);
	if (server->reassembly_pool_ready)
		tr_buffer_pool_destroy(&server->reassembly_pool);
	if (server->rpc_pool_ready)
		tr_buffer_pool_destroy(&server->rpc_message_pool);

	free(server->methods);
	if (server->finalizer_cond_ready)
		pthread_cond_destroy(&server->finalizer_cond);
	pthread_mutex_destroy(&server->lock);
	free(server);
}
