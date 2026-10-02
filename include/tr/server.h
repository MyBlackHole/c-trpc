#ifndef TR_SERVER_H
#define TR_SERVER_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_server;

struct tr_server_config {
	struct tr_facade_limits limits;

	/* Server 内独立 Reactor/resource shard 数；0 由 init/default 归一化为 1。 */
	uint32_t shard_count;

	/* 所有 shard 合计允许同时保留的最大 peer 对象数。 */
	uint32_t max_peers;
	int listen_backlog;

	/*
	 * DEFAULT/ENABLED 对每个已接受的 facade TCP peer 启用 TCP_NODELAY；
	 * DISABLED 保留内核默认 Nagle 行为。
	 */
	enum tr_tcp_nodelay_policy tcp_nodelay;

	/* 0 表示禁用 Transport keepalive。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;
};

void tr_server_config_init(struct tr_server_config *config);

struct tr_server_channel_stats {
	uint64_t active_streams_current;

	uint64_t streams_opened;
	uint64_t streams_closed;
	uint64_t stream_errors;
	uint64_t messages_tx;
	uint64_t messages_rx;
	uint64_t bytes_tx;
	uint64_t bytes_rx;
	uint64_t window_updates_tx;
	uint64_t window_updates_rx;
	uint64_t reconnect_attempts;
	uint64_t reconnect_successes;
	uint64_t keepalive_pings_sent;
	uint64_t keepalive_pongs_received;
	uint64_t keepalive_timeouts;
};

struct tr_server_rpc_stats {
	uint32_t executor_threads;
	uint32_t endpoints_current;

	uint64_t opening_calls_current;
	uint64_t active_calls_current;
	uint64_t terminal_calls_current;
	uint64_t executor_queued_tasks_current;
	uint64_t executor_running_tasks_current;
	uint64_t executor_queue_capacity_current;
	uint64_t executor_ready_calls_current;

	/* Largest historical value observed on any one peer Endpoint. */
	uint32_t executor_queue_peak_max_per_endpoint;
	uint32_t executor_ready_calls_peak_max_per_endpoint;

	uint64_t executor_enqueued_tasks;
	uint64_t executor_taken_tasks;
	uint64_t executor_admission_limit_hits;
	uint64_t executor_hard_full_events;
	struct tr_latency_histogram executor_queue_wait_ns;
	struct tr_latency_histogram executor_handler_ns;

	uint64_t calls_started;
	uint64_t calls_completed;
	uint64_t calls_cancelled;
	uint64_t calls_deadline_exceeded;
};

struct tr_server_stats {
	uint32_t shard_count;
	uint32_t max_peers;
	uint32_t peers_current;
	uint32_t peers_peak;
	uint32_t peers_ready_current;
	uint32_t peers_reaping_current;

	uint64_t peers_ready_total;
	uint64_t peers_reaped_total;
	uint64_t peer_capacity_rejections;

	struct tr_reactor_stats reactor;
	struct tr_pool_observation rpc_message_pool;
	struct tr_pool_observation reassembly_pool;
	struct tr_server_channel_stats channel;
	struct tr_server_rpc_stats rpc;
};

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out);

/* V1 有意限制只能在 server start 之前注册方法。 */
int tr_server_register_method(struct tr_server *server,
			      const struct tr_rpc_method_desc *method,
			      tr_rpc_unary_handler handler, void *handler_arg);

int tr_server_register_stream_method(
	struct tr_server *server, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

/* V1 facade 仅接受数字 IPv4 address；port=0 表示申请临时端口。 */
int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port);

int tr_server_start(struct tr_server *server);

/* 停止接收新 peer，发送 GOAWAY，并等待已有 Stream 结束。 */
int tr_server_drain(struct tr_server *server, uint32_t timeout_ms);

/*
 * Structured facade snapshot. Individual component snapshots are coherent, but
 * counters may advance while the cross-component snapshot is being collected.
 * Lifetime RPC/Channel counters include peers already finalized after owner
 * detach; current fields cover peers retained at snapshot time.
 */
int tr_server_get_stats(struct tr_server *server, struct tr_server_stats *out);

void tr_server_destroy(struct tr_server *server);

#ifdef __cplusplus
}
#endif

#endif
