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

#endif
