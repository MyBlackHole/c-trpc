#ifndef TR_FACADE_TUNING_INTERNAL_H
#define TR_FACADE_TUNING_INTERNAL_H

#include <stdint.h>

struct tr_client;
struct tr_client_config;
struct tr_server;
struct tr_server_config;

/*
 * 仓库内部的实现层调优参数。
 *
 * 这些容量描述当前 Reactor/资源池的实现细节，而不是应用协议语义。
 * 它们有意不进入已安装 SDK；取值为 0 的字段会归一化为内部默认值。
 *
 * Server 侧数值是聚合预算，并按确定性规则拆分到各 shard。
 */
struct tr_facade_tuning {
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;
	uint32_t rx_buffer_count;

	uint32_t rpc_message_pool_count;
	uint32_t reassembly_pool_count;

	/*
	 * Executor 布局属于实现层调优，不是稳定的应用
	 * 语义。Server 的 executor_threads 是 shard 聚合预算；
	 * executor_queue_capacity 按 Endpoint 计算；continuation reserve
	 * 仅用于 Server，并使用同一个有界节点池。
	 */
	uint32_t executor_threads;
	uint32_t executor_queue_capacity;
	uint32_t executor_continuation_reserve;

	/*
	 * 内部诊断开销策略。TR_OBSERVABILITY_TIMING 会启用
	 * Reactor/RPC 热调度路径中的单调时钟采样。
	 */
	uint32_t observability_flags;
};

void tr_facade_tuning_init(struct tr_facade_tuning *tuning);
void tr_facade_tuning_normalize(struct tr_facade_tuning *tuning);

int tr_client_create_with_tuning(
	const struct tr_client_config *config,
	const struct tr_facade_tuning *tuning,
	struct tr_client **out);

int tr_server_create_with_tuning(
	const struct tr_server_config *config,
	const struct tr_facade_tuning *tuning,
	struct tr_server **out);

#endif
