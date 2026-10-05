#ifndef TR_FACADE_H
#define TR_FACADE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 高层 TCP 发送策略。
 * DEFAULT 有意解析为 ENABLED，使零初始化的门面配置保持低延迟 RPC 默认行为。
 * DISABLED 保持 Linux TCP 的 Nagle 策略不变。
 */
enum tr_tcp_nodelay_policy {
	TR_TCP_NODELAY_DEFAULT = 0,
	TR_TCP_NODELAY_ENABLED = 1,
	TR_TCP_NODELAY_DISABLED = 2
};

/*
 * Client/Server facade 共用的高层 semantic limits。
 * 值为 0 的字段由 tr_facade_limits_init() 填入默认值。
 *
 * Reactor、资源池、执行器和诊断等实现资源不会在这里暴露；
 * 门面拥有的运行时调优参数保持内部化。
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
};

void tr_facade_limits_init(struct tr_facade_limits *limits);

#ifdef __cplusplus
}
#endif

#endif
