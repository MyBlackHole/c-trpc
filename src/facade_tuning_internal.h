#ifndef TR_FACADE_TUNING_INTERNAL_H
#define TR_FACADE_TUNING_INTERNAL_H

#include <stdint.h>

struct tr_client;
struct tr_client_config;
struct tr_server;
struct tr_server_config;

/* 仓库内部实现调优。
 *
 * 这些容量描述当前反应器与池的实现细节，不是应用协议语义。
 * 它们有意不包含在已安装的 SDK 中。零字段会归一化为内部默认值。
 *
 * 服务端值是按确定性方式分摊到各分片的总预算。
 */
struct tr_facade_tuning {
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;
	uint32_t rx_buffer_count;

	uint32_t rpc_message_pool_count;
	uint32_t reassembly_pool_count;

	/* 执行器布局属于实现调优，不是稳定的应用语义。
	 * 服务端 executor_threads 是各分片的总预算；
	 * executor_queue_capacity 按 Endpoint 计；延续任务预留
	 * 仅用于服务端，并使用同一个有界节点池。
	 */
	uint32_t executor_threads;
	uint32_t executor_queue_capacity;
	uint32_t executor_continuation_reserve;

	/* 内部诊断成本策略。TR_OBSERVABILITY_TIMING 启用
	 * 反应器与 RPC 热调度路径上的单调时钟采样。
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
