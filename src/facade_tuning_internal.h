#ifndef TR_FACADE_TUNING_INTERNAL_H
#define TR_FACADE_TUNING_INTERNAL_H

#include <stdint.h>

struct tr_client;
struct tr_client_config;
struct tr_server;
struct tr_server_config;
struct tr_facade_limits;

/*
 * Repository-internal implementation tuning.
 *
 * These capacities describe current Reactor/pool implementation details, not
 * application protocol semantics. They intentionally stay out of the installed
 * SDK. Zero fields are normalized to internal defaults.
 *
 * Server values are aggregate budgets split deterministically across shards.
 */
struct tr_facade_tuning {
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;
	uint32_t rx_buffer_count;

	uint32_t rpc_message_pool_count;
	uint32_t reassembly_pool_count;

	/*
	 * Executor layout is implementation tuning, not stable application
	 * semantics. Server executor_threads is an aggregate shard budget;
	 * executor_queue_capacity is per Endpoint; continuation reserve is
	 * Server-only and uses the same bounded node pool.
	 */
	uint32_t executor_threads;
	uint32_t executor_queue_capacity;
	uint32_t executor_continuation_reserve;
};

void tr_facade_tuning_init(struct tr_facade_tuning *tuning);
void tr_facade_tuning_normalize(
	struct tr_facade_tuning *tuning,
	const struct tr_facade_limits *limits);

int tr_client_create_with_tuning(
	const struct tr_client_config *config,
	const struct tr_facade_tuning *tuning,
	struct tr_client **out);

int tr_server_create_with_tuning(
	const struct tr_server_config *config,
	const struct tr_facade_tuning *tuning,
	struct tr_server **out);

#endif
