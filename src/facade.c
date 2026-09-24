#include "tr/facade.h"

#include <string.h>

void tr_facade_limits_init(struct tr_facade_limits *limits)
{
	if (!limits)
		return;

	memset(limits, 0, sizeof(*limits));
	limits->max_streams = 128U;
	limits->max_methods = 64U;
	limits->max_calls = 256U;

	limits->max_frame_payload_bytes = 256U * 1024U;
	limits->max_message_bytes = 4U * 1024U * 1024U;

	limits->initial_window_bytes = 8U * 1024U * 1024U;
	limits->window_update_threshold_bytes = 512U * 1024U;

	limits->command_capacity = 1024U;
	limits->tx_item_capacity = 512U;
	limits->control_tx_item_capacity = 128U;
	limits->rx_buffer_count = 64U;

	limits->rpc_message_pool_count = 64U;
	limits->rpc_message_buffer_bytes = 256U * 1024U;
	limits->reassembly_pool_count = 8U;

	limits->executor_threads = 4U;
	limits->executor_queue_capacity = 1024U;
}
