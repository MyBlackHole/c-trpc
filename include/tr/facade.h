#ifndef TR_FACADE_H
#define TR_FACADE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Shared high-level runtime limits used by the Client/Server facades.
 * Zero-valued fields are filled by tr_facade_limits_init().
 *
 * V1 facade intentionally exposes only the shared-connection mode. The lower
 * level Channel API still supports split CONTROL/BULK connections.
 */
struct tr_facade_limits {
	uint32_t max_streams;
	uint32_t max_methods;
	uint32_t max_calls;

	uint32_t max_frame_payload_bytes;
	uint32_t max_message_bytes;

	uint64_t initial_window_bytes;
	uint64_t window_update_threshold_bytes;

	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;
	uint32_t rx_buffer_count;

	uint32_t rpc_message_pool_count;
	uint32_t rpc_message_buffer_bytes;
	uint32_t reassembly_pool_count;

	uint32_t executor_threads;
	uint32_t executor_queue_capacity;
};

void tr_facade_limits_init(struct tr_facade_limits *limits);

#ifdef __cplusplus
}
#endif

#endif
