#ifndef TR_FACADE_H
#define TR_FACADE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * High-level TCP send policy. DEFAULT deliberately resolves to ENABLED so
 * zero-initialized facade configs retain the low-latency RPC default.
 * DISABLED leaves Linux TCP's Nagle policy unchanged.
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
 * Reactor/pool/executor/diagnostic implementation resources are deliberately
 * not represented here; facade-owned runtime tuning stays internal.
 *
 * V1 facade 有意只暴露 shared-connection 模式；
 * 更底层的 Channel API 仍支持 split CONTROL/BULK connection。
 */
struct tr_facade_limits {
	uint32_t max_streams;
	uint32_t max_methods;
	uint32_t max_calls;

	uint32_t max_frame_payload_bytes;
	/*
	 * Maximum application RPC message payload. RPC header/metadata overhead is
	 * derived internally and does not consume this semantic budget.
	 */
	uint32_t max_message_bytes;

	uint64_t initial_window_bytes;
	uint64_t window_update_threshold_bytes;
};

void tr_facade_limits_init(struct tr_facade_limits *limits);

#ifdef __cplusplus
}
#endif

#endif
