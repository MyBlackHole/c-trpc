#ifndef TR_RUNTIME_INTERNAL_H
#define TR_RUNTIME_INTERNAL_H

#include <stdint.h>

#include "../execution/reactor.h"
#include "../memory_budget.h"

struct tr_runtime;
struct tr_runtime_shard;
struct tr_rpc_executor_group;
struct tr_channel;
struct tr_rpc_endpoint;

struct tr_runtime_peer {
	int used;
	void *finalize_ctx;
	struct tr_conn_handle connection;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
};

struct tr_runtime_peer_stats {
	uint32_t capacity;
	uint32_t current;
	uint32_t peak;
	uint32_t reaping_current;
	uint64_t ready_total;
	uint64_t reaped_total;
	uint64_t capacity_rejections;
};

/*
 * Runtime owns one independent resource domain per shard. Configuration is
 * explicit per shard so enabling N shards never multiplies a Server-wide
 * budget implicitly.
 */
struct tr_runtime_rpc_executor_config {
	uint32_t endpoint_capacity;
	uint32_t max_calls_per_endpoint;
	uint32_t thread_count;
};

struct tr_runtime_shard_config {
	struct tr_reactor_config reactor;
	uint32_t peer_capacity;

	/*
	 * Internal Phase-7 shard memory budget. 0 means accounting-only/unbounded
	 * until all major shard-local consumers participate in this capability.
	 */
	uint64_t memory_budget_bytes;

	struct tr_runtime_rpc_executor_config rpc_executor;
};

struct tr_runtime_config {
	uint32_t shard_count;
	const struct tr_runtime_shard_config *shards;
};

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out);
int tr_runtime_start(struct tr_runtime *runtime);
int tr_runtime_stop(struct tr_runtime *runtime);
int tr_runtime_destroy(struct tr_runtime *runtime);

uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime);
struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index);
uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard);
struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard);
int tr_runtime_shard_call(struct tr_runtime_shard *shard,
			  int (*fn)(void *arg), void *arg);
struct tr_rpc_executor_group *
tr_runtime_shard_rpc_executor(const struct tr_runtime_shard *shard);

/*
 * Internal shard memory-budget capability. Consumers must reserve before
 * allocating budgeted bytes and release the exact reservation on teardown.
 */
struct tr_memory_budget *
tr_runtime_shard_memory_budget(struct tr_runtime_shard *shard);
void tr_runtime_shard_memory_stats(
	const struct tr_runtime_shard *shard,
	struct tr_memory_budget_stats *out);

/*
 * Listener lifetime is shard-owned. The listener event source is registered
 * with that shard's Reactor; close first unregisters the owner event source,
 * then closes the fd.
 */
int tr_runtime_shard_listen_ipv4(struct tr_runtime_shard *shard,
				 const char *address, uint16_t port,
				 int backlog, uint16_t *out_bound_port);
int tr_runtime_shard_listen_ipv4_ex(struct tr_runtime_shard *shard,
				    const char *address, uint16_t port,
				    int backlog, int reuse_port,
				    uint16_t *out_bound_port);
int tr_runtime_shard_listener_fd(const struct tr_runtime_shard *shard);
uint16_t tr_runtime_shard_bound_port(const struct tr_runtime_shard *shard);
typedef void (*tr_runtime_listener_cb)(int fd, uint32_t events, void *arg);

int tr_runtime_shard_enable_listener_events(struct tr_runtime_shard *shard,
					    tr_runtime_listener_cb callback,
					    void *arg);
int tr_runtime_shard_disable_listener_events(struct tr_runtime_shard *shard);
void tr_runtime_shard_close_listener(struct tr_runtime_shard *shard);

/*
 * Peer storage is shard-owned. Accept, publish, lifecycle detach and live
 * snapshot all run on the Reactor owner. Runtime owns storage/counters; only
 * detached-finalizer retirement counters may be updated off-owner.
 */
uint32_t tr_runtime_shard_peer_capacity(const struct tr_runtime_shard *shard);
struct tr_runtime_peer *
tr_runtime_shard_peer_at(struct tr_runtime_shard *shard, uint32_t slot);
void tr_runtime_shard_peer_note_added(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_ready(struct tr_runtime_shard *shard);
int tr_runtime_shard_peer_reaping_at_capacity(
	const struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_removed_for_reap(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_reaped(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_capacity_rejection(
	struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_stats(const struct tr_runtime_shard *shard,
				 struct tr_runtime_peer_stats *out);

/*
 * Shard-local deferred peer lifecycle event source. Channel callbacks signal
 * it from the Reactor owner; epoll dispatches it on a later Reactor turn so
 * shared-connection DOWN notifications finish before detach begins.
 */
typedef void (*tr_runtime_peer_event_cb)(int fd, uint32_t events, void *arg);

int tr_runtime_shard_peer_event_fd(const struct tr_runtime_shard *shard);
int tr_runtime_shard_enable_peer_events(struct tr_runtime_shard *shard,
					tr_runtime_peer_event_cb callback,
					void *arg);
int tr_runtime_shard_disable_peer_events(struct tr_runtime_shard *shard);
void tr_runtime_shard_signal_peer_event(struct tr_runtime_shard *shard);
void tr_runtime_shard_drain_peer_event(struct tr_runtime_shard *shard);

#endif
