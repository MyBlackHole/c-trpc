#ifndef TR_RUNTIME_LEGACY_RPC_EXECUTION_H
#define TR_RUNTIME_LEGACY_RPC_EXECUTION_H

#include <stdint.h>

struct tr_rpc_executor_group;
struct tr_runtime_legacy_rpc_execution;

/* V1 专用适配边界；默认 V2 不构造本执行域。
 * 不额外分配包装对象：不透明指针只在本模块内转换回原执行器组。
 * Runtime 拥有该能力直到 destroy 成功。失败保留能力以供再次回收。
 */
int tr_runtime_legacy_rpc_execution_create(
	uint32_t endpoint_capacity, uint32_t max_calls_per_endpoint,
	uint32_t thread_count, struct tr_runtime_legacy_rpc_execution **out);
int tr_runtime_legacy_rpc_execution_can_start(
	struct tr_runtime_legacy_rpc_execution *execution);
int tr_runtime_legacy_rpc_execution_start(
	struct tr_runtime_legacy_rpc_execution *execution);
int tr_runtime_legacy_rpc_execution_stop(
	struct tr_runtime_legacy_rpc_execution *execution);
int tr_runtime_legacy_rpc_execution_destroy(
	struct tr_runtime_legacy_rpc_execution **execution);

/* 仅给尚未迁移的旧 Endpoint 借用，不能据此接管或释放组所有权。 */
struct tr_rpc_executor_group *tr_runtime_legacy_rpc_execution_group(
	struct tr_runtime_legacy_rpc_execution *execution);

#endif
