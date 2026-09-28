#ifndef TR_RPC_INTERNAL_H
#define TR_RPC_INTERNAL_H

#include <stdint.h>

#include "tr/rpc.h"

/* Stable Endpoint-lifetime Method hash shared with collision tests. */
static inline uint64_t tr_rpc_method_hash(uint32_t service_id,
					  uint32_t method_id)
{
	uint64_t value = ((uint64_t)service_id << 32) | (uint64_t)method_id;

	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	value ^= value >> 31;
	return value;
}

struct tr_rpc_executor_group;
int tr_rpc_executor_group_create(uint32_t endpoint_capacity,
				 uint32_t max_calls_per_endpoint,
				 uint32_t thread_count,
				 struct tr_rpc_executor_group **out);
void tr_rpc_executor_group_destroy(struct tr_rpc_executor_group *group);

int tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel, const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group, struct tr_rpc_endpoint **out);

/*
 * Server reaping path: synchronously quiesce/drain the Endpoint, snapshot its
 * final counters, then release it. stats may be NULL.
 */
void tr_rpc_endpoint_destroy_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats);

/* Internal deterministic diagnostics for the bounded Call deadline heap. */
int tr_rpc_deadline_heap_snapshot(struct tr_rpc_endpoint *endpoint,
				  uint32_t *count,
				  struct tr_rpc_call_handle *root,
				  uint64_t *root_deadline_ns);

#endif
