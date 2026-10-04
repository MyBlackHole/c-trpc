#include "tr/facade.h"
#include "facade_tuning_internal.h"

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

}


void tr_facade_tuning_init(struct tr_facade_tuning *tuning)
{
	if (!tuning)
		return;

	memset(tuning, 0, sizeof(*tuning));
	tuning->command_capacity = 1024U;
	tuning->tx_item_capacity = 512U;
	tuning->control_tx_item_capacity = 128U;
	tuning->rx_buffer_count = 64U;
	tuning->rpc_message_pool_count = 64U;
	tuning->reassembly_pool_count = 8U;
	tuning->rpc_send_bytes_limit = UINT64_C(16) * 1024U * 1024U;
	tuning->executor_threads = 4U;
	tuning->executor_queue_capacity = 1024U;
	tuning->executor_continuation_reserve = 0U;
	tuning->observability_flags = 0U;
}

void tr_facade_tuning_normalize(struct tr_facade_tuning *tuning)
{
	struct tr_facade_tuning defaults;

	if (!tuning)
		return;
	tr_facade_tuning_init(&defaults);

#define TR_TUNING_DEFAULT(field)                        \
	do {                                            \
		if (tuning->field == 0U)                \
			tuning->field = defaults.field; \
	} while (0)
	TR_TUNING_DEFAULT(command_capacity);
	TR_TUNING_DEFAULT(tx_item_capacity);
	TR_TUNING_DEFAULT(control_tx_item_capacity);
	TR_TUNING_DEFAULT(rx_buffer_count);
	TR_TUNING_DEFAULT(rpc_message_pool_count);
	TR_TUNING_DEFAULT(reassembly_pool_count);
	TR_TUNING_DEFAULT(rpc_send_bytes_limit);
	TR_TUNING_DEFAULT(executor_threads);
	TR_TUNING_DEFAULT(executor_queue_capacity);
#undef TR_TUNING_DEFAULT
}
