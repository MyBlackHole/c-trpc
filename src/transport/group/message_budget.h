#ifndef TR_TRANSPORT_GROUP_MESSAGE_BUDGET_H
#define TR_TRANSPORT_GROUP_MESSAGE_BUDGET_H

#include <stdint.h>

#include "tr/status.h"

#define TR_GROUP_MESSAGE_WORKSPACE_BYTES (1024U * 1024U)
#define TR_GROUP_MESSAGE_WINDOW_LOG 20U
#define TR_GROUP_MESSAGE_TASK_OVERHEAD_BYTES (64U * 1024U)
#define TR_GROUP_MESSAGE_RX_SLOT_OVERHEAD_BYTES 256U
#define TR_GROUP_MESSAGE_JOB_SLOT_OVERHEAD_BYTES 256U

static inline int tr_group_message_budget_add(uint64_t *total, uint64_t bytes)
{
	if (!total || *total > UINT64_MAX - bytes)
		return TR_ERR_BAD_LENGTH;
	*total += bytes;
	return TR_OK;
}

static inline int tr_group_message_budget_mul(uint64_t count, uint64_t bytes,
                                              uint64_t *out)
{
	if (!out || (count != 0U && bytes > UINT64_MAX / count))
		return TR_ERR_BAD_LENGTH;
	*out = count * bytes;
	return TR_OK;
}

/* The hard ceiling covers one source Message, one maximum encoded Message,
 * a bounded Zstandard workspace, and bounded task metadata per accepted job.
 * Receivers additionally reserve one full encoded builder for every active
 * transfer slot while queued/running transforms use the task budget.
 */
static inline int tr_group_message_budget_required(
        uint32_t max_message_bytes, uint32_t max_encoded_message_bytes,
        uint64_t receive_builder_slots, uint32_t task_capacity,
        uint64_t reactor_bytes, uint64_t peer_bytes, uint64_t *out)
{
	uint64_t per_task = 0U;
	uint64_t task_bytes;
	uint64_t builder_bytes;
	uint64_t total = reactor_bytes;
	int ret;

	if (!out || max_message_bytes == 0U ||
	    max_encoded_message_bytes < 17U || task_capacity == 0U)
		return TR_ERR_INVALID;
	ret = tr_group_message_budget_add(&per_task, max_message_bytes);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(&per_task,
		                                  max_encoded_message_bytes);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(
		        &per_task, TR_GROUP_MESSAGE_WORKSPACE_BYTES);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(
		        &per_task, TR_GROUP_MESSAGE_TASK_OVERHEAD_BYTES);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(
		        &per_task, TR_GROUP_MESSAGE_JOB_SLOT_OVERHEAD_BYTES);
	if (ret != TR_OK)
		return ret;
	ret = tr_group_message_budget_mul(task_capacity, per_task, &task_bytes);
	if (ret != TR_OK)
		return ret;
	ret = tr_group_message_budget_mul(
	        receive_builder_slots,
	        (uint64_t)max_encoded_message_bytes +
	                TR_GROUP_MESSAGE_RX_SLOT_OVERHEAD_BYTES,
	        &builder_bytes);
	if (ret != TR_OK)
		return ret;
	ret = tr_group_message_budget_add(&total, peer_bytes);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(&total, task_bytes);
	if (ret == TR_OK)
		ret = tr_group_message_budget_add(&total, builder_bytes);
	if (ret != TR_OK)
		return ret;
	*out = total;
	return TR_OK;
}

#endif
