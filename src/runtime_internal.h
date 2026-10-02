#ifndef TR_RUNTIME_INTERNAL_H
#define TR_RUNTIME_INTERNAL_H

#include <stdint.h>

#include "tr/reactor.h"

struct tr_runtime;
struct tr_runtime_shard;
struct tr_rpc_executor_group;

/*
 * Phase-3 runtime ownership seam.
 *
 * The first implementation deliberately accepts only shard_count == 1.
 * Keeping the count explicit makes the ownership boundary real without
 * prematurely introducing multi-Reactor listener/routing semantics.
 */
struct tr_runtime_rpc_executor_config {
	uint32_t endpoint_capacity;
	uint32_t max_calls_per_endpoint;
	uint32_t thread_count;
};

struct tr_runtime_config {
	uint32_t shard_count;
	struct tr_reactor_config reactor;
	struct tr_runtime_rpc_executor_config rpc_executor;
};

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out);
int tr_runtime_start(struct tr_runtime *runtime);
int tr_runtime_stop(struct tr_runtime *runtime);
void tr_runtime_destroy(struct tr_runtime *runtime);

uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime);
struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index);
uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard);
struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard);
struct tr_rpc_executor_group *
tr_runtime_shard_rpc_executor(const struct tr_runtime_shard *shard);

/*
 * Phase-3 listener ownership: the shard owns the listening fd even while the
 * legacy central accept thread still borrows it to accept connections.
 *
 * A borrower must be quiesced/joined before close. close_listener() is a
 * lifetime operation, not a concurrent cancellation primitive; closing while
 * another thread still uses the numeric fd could race with fd reuse.
 */
int tr_runtime_shard_listen_ipv4(struct tr_runtime_shard *shard,
				 const char *address, uint16_t port,
				 int backlog, uint16_t *out_bound_port);
int tr_runtime_shard_listener_fd(const struct tr_runtime_shard *shard);
uint16_t tr_runtime_shard_bound_port(const struct tr_runtime_shard *shard);
void tr_runtime_shard_close_listener(struct tr_runtime_shard *shard);

#endif
