#ifndef TR_FACADE_H
#define TR_FACADE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Client/Server facade 共用的高层 runtime limits。
 * 值为 0 的字段由 tr_facade_limits_init() 填入默认值。
 *
 * V1 facade 有意只暴露 shared-connection 模式；
 * 更底层的 Channel API 仍支持 split CONTROL/BULK connection。
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

	/*
	 * Client：worker 由 Client RPC Endpoint 自己拥有。
	 * Server：所有已接收 peer 的 RPC Endpoint 共用一个 worker pool。
	 */
	uint32_t executor_threads;
	/* 每个 Endpoint 独立的有界 task 容量。 */
	uint32_t executor_queue_capacity;
};

void tr_facade_limits_init(struct tr_facade_limits *limits);

#ifdef __cplusplus
}
#endif

#endif
