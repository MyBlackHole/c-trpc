#include "legacy_rpc_execution.h"

#include "../rpc/rpc_internal.h"
#include "tr/status.h"

/* 句柄始终存放原始 group 指针，向旧类型转换后才允许解引用。
 * 无额外堆分配、引用计数、生命周期门禁或调度线程。
 */
static struct tr_rpc_executor_group *tr_runtime_legacy_rpc_execution_raw(
        struct tr_runtime_legacy_rpc_execution *execution)
{
	return (struct tr_rpc_executor_group *)execution;
}

int tr_runtime_legacy_rpc_execution_create(
        uint32_t endpoint_capacity, uint32_t max_calls_per_endpoint,
        uint32_t thread_count, struct tr_runtime_legacy_rpc_execution **out)
{
	struct tr_rpc_executor_group *group = NULL;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	ret = tr_rpc_executor_group_create(endpoint_capacity,
	                                   max_calls_per_endpoint, thread_count,
	                                   &group);
	if (ret == TR_OK)
		*out = (struct tr_runtime_legacy_rpc_execution *)group;
	return ret;
}

int tr_runtime_legacy_rpc_execution_can_start(
        struct tr_runtime_legacy_rpc_execution *execution)
{
	return tr_rpc_executor_group_can_start(
	        tr_runtime_legacy_rpc_execution_raw(execution));
}

int tr_runtime_legacy_rpc_execution_start(
        struct tr_runtime_legacy_rpc_execution *execution)
{
	return tr_rpc_executor_group_start(
	        tr_runtime_legacy_rpc_execution_raw(execution));
}

int tr_runtime_legacy_rpc_execution_stop(
        struct tr_runtime_legacy_rpc_execution *execution)
{
	return tr_rpc_executor_group_stop(
	        tr_runtime_legacy_rpc_execution_raw(execution));
}

int tr_runtime_legacy_rpc_execution_destroy(
        struct tr_runtime_legacy_rpc_execution **execution)
{
	int ret;

	if (!execution)
		return TR_ERR_INVALID;
	ret = tr_rpc_executor_group_destroy_checked(
	        tr_runtime_legacy_rpc_execution_raw(*execution));
	if (ret == TR_OK)
		*execution = NULL;
	return ret;
}

struct tr_rpc_executor_group *tr_runtime_legacy_rpc_execution_group(
        struct tr_runtime_legacy_rpc_execution *execution)
{
	return tr_runtime_legacy_rpc_execution_raw(execution);
}
