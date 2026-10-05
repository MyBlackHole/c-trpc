#ifndef TR_FACADE_TUNING_INTERNAL_H
#define TR_FACADE_TUNING_INTERNAL_H

#include <stdint.h>

struct tr_client;
struct tr_client_config;
struct tr_server;
struct tr_server_config;

/*
 * 仓库内部实现调优。
 *
 * 这些容量描述当前 Reactor/资源池实现细节，不代表应用协议语义。
 * 它们有意不进入已安装 SDK；值为 0 的字段归一化为内部默认值。
 *
 * Server 侧的值是聚合预算，并按确定性规则拆分到各分片。
 */
struct tr_facade_tuning {
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;
	uint32_t rx_buffer_count;

	uint32_t rpc_message_pool_count;
	uint32_t reassembly_pool_count;

	/*
	 * 执行器布局属于实现调优，不是稳定应用语义。
	 * Server executor_threads 是分片聚合预算；
	 * executor_queue_capacity 按 Endpoint 生效；
	 * 续处理预留仅 Server 使用，并与普通任务共用同一个有界节点池。
	 */
	uint32_t executor_threads;
	uint32_t executor_queue_capacity;
	uint32_t executor_continuation_reserve;

	/*
	 * 内部诊断成本策略。
	 * TR_OBSERVABILITY_TIMING 会在 Reactor/RPC 热调度路径启用单调时钟采样。
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
