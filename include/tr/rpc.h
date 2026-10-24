#ifndef TR_RPC_H
#define TR_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "tr/rpc_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/* RPC V2 共用的状态、方法与元数据模型；调用能力见 tr/rpc_v2.h。 */

#define TR_RPC_METADATA_MAX_BYTES 512U
#define TR_RPC_METADATA_MAX_KEY_LEN 63U

/* RPC 级流量类别。取值有意与当前 Transport 通道保持一致。 */
#define TR_RPC_LANE_CONTROL 0U
#define TR_RPC_LANE_BULK 1U

/* Message 算法编号与每条 MESSAGE 选择一致；NONE 是隐式默认值，掩码只
 * 表示显式允许的可选算法。未设置时，方法只允许未压缩消息。压缩会暴露
 * 正文长度和时序；TLS 不隐藏这些信息。不要把秘密与攻击者可控字节放入
 * 同一条压缩正文。 */
enum tr_rpc_compression {
	TR_RPC_COMPRESSION_NONE = 0,
	TR_RPC_COMPRESSION_ZSTD = 1
};

#define TR_RPC_COMPRESSION_MASK_ZSTD (1U << TR_RPC_COMPRESSION_ZSTD)

struct tr_rpc_metadata {
	const char *key;
	const void *value;
	uint16_t value_len;
};

struct tr_rpc_call_options {
	/* 从 Call 创建时刻开始计算的相对截止时间；0 表示禁用截止时间。 */
	uint32_t timeout_ms;

	/* 仅附加到首条出站 RPC 消息的初始元数据。 */
	const struct tr_rpc_metadata *metadata;
	uint16_t metadata_count;

	/* 仅用于 tr_client_unary_call 的单条请求正文；call_start 必须为 NONE。
	 * 零初始化保留原有未压缩行为。 */
	enum tr_rpc_compression request_compression;
};

/* 方法请求与响应的消息基数。 */
enum tr_rpc_cardinality { TR_RPC_NONE = 0, TR_RPC_ONE = 1, TR_RPC_MANY = 2 };

enum tr_rpc_status {
	TR_RPC_STATUS_OK = 0,
	TR_RPC_STATUS_CANCELLED = 1,
	TR_RPC_STATUS_UNKNOWN = 2,
	TR_RPC_STATUS_INVALID_ARGUMENT = 3,
	TR_RPC_STATUS_DEADLINE_EXCEEDED = 4,
	TR_RPC_STATUS_NOT_FOUND = 5,
	TR_RPC_STATUS_ALREADY_EXISTS = 6,
	TR_RPC_STATUS_PERMISSION_DENIED = 7,
	TR_RPC_STATUS_RESOURCE_EXHAUSTED = 8,
	TR_RPC_STATUS_FAILED_PRECONDITION = 9,
	TR_RPC_STATUS_ABORTED = 10,
	TR_RPC_STATUS_OUT_OF_RANGE = 11,
	TR_RPC_STATUS_UNIMPLEMENTED = 12,
	TR_RPC_STATUS_INTERNAL = 13,
	TR_RPC_STATUS_UNAVAILABLE = 14,
	TR_RPC_STATUS_DATA_LOSS = 15,
	TR_RPC_STATUS_UNAUTHENTICATED = 16
};

#define TR_RPC_STATUS_COUNT 17U

/*
 * 稳定且与布局无关的 V2 RPC 生命周期快照。
 *
 * 客户端开始：本地 Call 已被接受并发布能力，外部 API 将返回 TR_OK；
 * V2 回调可能早于外部 start 返回，计数在发布能力时已经生效。
 * 服务端开始：完整合法 OPEN 匹配方法并通过期限准入，包含随后发生的
 * 工作分配或执行器拒绝；未知方法、无效或不完整 OPEN 不计入调用数。
 *
 * 当此端点提交最终的应用可见 RPC 结果时，恰好记录一次结束。
 * V2 在调用唯一 terminal 回调之前计数；服务端仍须先等待工作返回和
 * 完成交付，无 terminal 函数时在相同提交点计数。该计数不证明回调
 * 已返回、对端收到回复或传输资源已收敛。
 * final_status[] 以枚举 tr_rpc_status 为索引，其总和等于 calls_finished。
 *
 * calls_inflight 是快照计量值（开始数减结束数），不是 Runtime 槽位、
 * 执行器或传输层计数。
 * V2 无法用 uint64_t 表示累计值时读取返回 TR_ERR_BAD_LENGTH，
 * 不回绕、不返回截断快照，也不因此拒绝正常 RPC 调用。
 */
struct tr_rpc_semantic_stats {
	uint64_t calls_started;
	uint64_t calls_finished;
	uint64_t calls_inflight;
	uint64_t final_status[TR_RPC_STATUS_COUNT];
};

struct tr_rpc_method_desc {
	uint32_t service_id;
	uint32_t method_id;

	enum tr_rpc_cardinality request_cardinality;
	enum tr_rpc_cardinality response_cardinality;

	uint32_t request_codec_id;
	uint32_t response_codec_id;

	/* TR_RPC_LANE_CONTROL 或 TR_RPC_LANE_BULK。 */
	uint32_t lane;

	/* 这是单条消息上限，不是整个 Stream 的累计上限。 */
	uint32_t max_request_bytes;
	uint32_t max_response_bytes;

	/* 本地方法策略：请求、响应方向分别列出接收/发送可显式选择的算法。
	 * NONE 隐式允许；零掩码保持旧方法的未压缩行为。 */
	uint32_t request_compression_algorithms;
	uint32_t response_compression_algorithms;
};

#ifdef __cplusplus
}
#endif

#endif
