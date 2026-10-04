#ifndef TR_FACADE_DIAGNOSTICS_INTERNAL_H
#define TR_FACADE_DIAGNOSTICS_INTERNAL_H

#include "tr/channel.h"
#include "tr/client.h"
#include "tr/reactor.h"
#include "tr/server.h"
#include "rpc/rpc_internal.h"
#include "pipeline_listener_internal.h"

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

	struct tr_rpc_semantic_stats semantic;
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

int tr_server_get_stats(struct tr_server *server, struct tr_server_stats *out);
int tr_server_get_connection_group_stats_internal(
	struct tr_server *server, struct tr_pipeline_listener_stats *out);

int tr_client_get_channel_stats(struct tr_client *client,
				struct tr_channel_stats *out);
int tr_client_get_rpc_stats(struct tr_client *client,
			    struct tr_rpc_endpoint_stats *out);

#endif
