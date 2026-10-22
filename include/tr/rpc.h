#ifndef TR_RPC_H
#define TR_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "tr/rpc_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 公共 RPC 状态、方法与元数据模型，以及尚待删除的 V1 低层类型。
 * 普通门面现只构造 V2，其业务能力见 tr/rpc_v2.h；下文标为 V1 的
 * 句柄/续处理接口不能用于默认构造得到的实例。
 *
 * 此处有意不声明 Endpoint/Channel/Executor 的构造接口；
 * 这些属于内部引擎的职责。
 */

#define TR_RPC_METADATA_MAX_BYTES 512U
#define TR_RPC_METADATA_MAX_KEY_LEN 63U

/* RPC 级流量类别。取值有意与当前 Transport 通道保持一致。 */
#define TR_RPC_LANE_CONTROL 0U
#define TR_RPC_LANE_BULK 1U

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
};

/*
 * V1 可执行 Method 使用 ONE/MANY。
 * 注意：当前线协议没有方法开启消息的独立封装，因此客户端 MANY 请求在
 * close_send() 前至少要成功发送一条 REQUEST；真正的零消息流式调用属于
 * 未来的方法开启扩展。
 */
enum tr_rpc_cardinality { TR_RPC_NONE = 0, TR_RPC_ONE = 1, TR_RPC_MANY = 2 };

/*
 * Call 上下文快照。
 *
 * 这是协议标识/生命周期的只读快照，不暴露 Endpoint/Stream/槽位。
 * deadline_remaining_ms 只在 has_deadline != 0 时有效；它是读取瞬间的相对值。
 */
struct tr_rpc_context {
	uint32_t service_id;
	uint32_t method_id;
	enum tr_rpc_cardinality request_cardinality;
	enum tr_rpc_cardinality response_cardinality;
	uint64_t deadline_remaining_ms;
	int has_deadline;
	int cancelled;
	int cancel_status;
};

struct tr_rpc_call_handle;

enum tr_rpc_interceptor_phase {
	TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL = 1,
	TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER = 2,
	TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER = 3,
	TR_RPC_INTERCEPTOR_CLIENT_POST_CALL = 4
};

/*
 * V1 拦截器：
 *
 * - 在所属 Reactor 所有者线程上同步执行，必须短小且非阻塞；
 * - 回调内只允许使用 Call 上下文/元数据 API，不应 send/finish/cancel；
 * - SERVER_PRE_HANDLER 返回 OK 表示继续，返回合法非 OK RPC 状态表示在
 *   应用处理器运行前拒绝该 Call；
 * - V1 忽略其他阶段的返回值，建议返回 OK。
 *
 * 钩子运行时不会持有 endpoint->lock，因此上下文/元数据 API 可安全重入。
 */
typedef int (*tr_rpc_interceptor_fn)(
	struct tr_rpc_call_handle call, enum tr_rpc_interceptor_phase phase,
	int status, void *arg);

struct tr_rpc_interceptor {
	tr_rpc_interceptor_fn fn;
	void *arg;
};

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
 * 稳定且与布局无关的 RPC 生命周期快照。
 *
 * 客户端开始：本地 Call 已被接受并发布能力，外部 API 将返回 TR_OK；
 * V2 回调可能早于外部 start 返回，计数在发布能力时已经生效。
 * V1 服务端开始：已注册 Method 的首条合法 REQUEST 被接受
 * （包括随后被拦截器或准入机制拒绝的情况）。V2 服务端开始：完整合法
 * OPEN 匹配方法并通过期限准入，包含随后发生的工作分配或执行器拒绝；
 * 未知方法、无效或不完整 OPEN 不计入服务端调用数。
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

/*
 * 不透明的固定大小 Call 能力句柄。
 *
 * 应用可以按值复制/传递整个值，但不得检查或修改 _private。
 * 零初始化的值无效。其表示有意不暴露 Endpoint/槽位/代次等引擎标识。
 *
 * 这是借用的能力句柄，并非所有权引用：复制它不会延长
 * Client/Server/Endpoint 生命周期。不得与所有者销毁操作并发使用，
 * 也不得在所属门面销毁后使用。Call 的终止回调/处理器收尾完成后，
 * 后续异步继续执行时对该句柄的使用必须视为无效或陈旧。
 */
#define TR_RPC_CALL_PRIVATE_WORDS 2U
struct tr_rpc_call_handle {
	uint64_t _private[TR_RPC_CALL_PRIVATE_WORDS];
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
};

struct tr_rpc_unary_response {
	int status;
	struct tr_rpc_bytes message;
};

typedef int (*tr_rpc_unary_handler)(struct tr_rpc_call_handle call,
				    const struct tr_rpc_bytes *request,
				    struct tr_rpc_unary_response *response,
				    void *arg);
typedef void (*tr_rpc_unary_result_cb)(struct tr_rpc_call_handle call,
				       int status,
				       const struct tr_rpc_bytes *response,
				       void *arg);

/*
 * 流式接收描述符。
 *
 * bytes 是应用唯一可见的载荷视图。_private 是固定大小的释放能力句柄，
 * 仅供 tr_rpc_message_release() 使用；它保留零复制的 TAKE_OWNERSHIP
 * 路径，而不暴露 Buffer/Stream/Channel 类型。
 *
 * 返回 TR_RPC_MESSAGE_TAKE_OWNERSHIP 会将 RX 载荷及内部用于维持对端
 * 生命周期的持有关系一并转移给应用。保留的描述符在对端断开后仍可安全
 * 释放，且必须恰好释放一次。转移该所有权时应原样复制整个描述符；
 * 不得释放多份副本。保留消息可能延迟对端拆除，因此应用必须先释放这些
 * 消息，才能期待 Client/Server 销毁完成。
 */
#define TR_RPC_MESSAGE_PRIVATE_WORDS 4U
struct tr_rpc_message {
	struct tr_rpc_bytes bytes;
	uintptr_t _private[TR_RPC_MESSAGE_PRIVATE_WORDS];
};

enum tr_rpc_message_disposition {
	TR_RPC_MESSAGE_RELEASE = 0,
	TR_RPC_MESSAGE_TAKE_OWNERSHIP = 1
};

enum tr_rpc_call_event {
	TR_RPC_CALL_EVENT_OPENED = 1,
	TR_RPC_CALL_EVENT_WRITABLE = 2,
	/*
	 * 对端在最终 STATUS 之前提前关闭响应方向时上报。
	 * 正常 STATUS 之后的传输层关闭不重复上报该事件。
	 */
	TR_RPC_CALL_EVENT_REMOTE_CLOSED = 3,
	/* 应用终止屏障；正常 Call 中它之后不再有其他事件。 */
	TR_RPC_CALL_EVENT_FINISHED = 4,
	/* 最终 STATUS 之前的异常终止。 */
	TR_RPC_CALL_EVENT_ERROR = 5
};

typedef enum tr_rpc_message_disposition (*tr_rpc_message_cb)(
	struct tr_rpc_call_handle call, const struct tr_rpc_message *message,
	void *arg);
typedef void (*tr_rpc_call_event_cb)(struct tr_rpc_call_handle call,
				     enum tr_rpc_call_event event, int status,
				     void *arg);

struct tr_rpc_call_callbacks {
	tr_rpc_message_cb on_message;
	tr_rpc_call_event_cb on_event;
	void *arg;
};

typedef int (*tr_rpc_stream_open_handler)(struct tr_rpc_call_handle call,
					  void *arg);
typedef enum tr_rpc_message_disposition (*tr_rpc_stream_message_handler)(
	struct tr_rpc_call_handle call, const struct tr_rpc_message *message,
	void *arg);
typedef void (*tr_rpc_stream_half_close_handler)(struct tr_rpc_call_handle call,
						 void *arg);
typedef void (*tr_rpc_stream_writable_handler)(struct tr_rpc_call_handle call,
					       void *arg);
typedef void (*tr_rpc_stream_close_handler)(struct tr_rpc_call_handle call,
					    int status, void *arg);

struct tr_rpc_stream_handlers {
	tr_rpc_stream_open_handler on_open;
	tr_rpc_stream_message_handler on_message;
	tr_rpc_stream_half_close_handler on_half_close;
	tr_rpc_stream_writable_handler on_writable;
	tr_rpc_stream_close_handler on_close;
};

/*
 * 应用 Call 续处理 API。
 *
 * send()：Client 发送 REQUEST；Server 发送 RESPONSE。
 *
 * close_send()：仅用于客户端请求方向的半关闭。V1 ONE 必须已经发送
 * 恰好 1 条 REQUEST；MANY 必须已经发送至少 1 条 REQUEST。
 *
 * finish()：仅用于服务端流式 Call，发送最终 STATUS 并结束响应
 * 方向。Server 不应使用 close_send() 绕过 STATUS。
 */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

/*
 * 初始元数据：
 * - set_metadata() 必须在本方向第一条业务消息封装发送前调用；
 * - get_peer_metadata() 读取对端首个 REQUEST/RESPONSE 的初始元数据。
 */
int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/*
 * V1 尾部元数据：
 * - 仅服务端流式 Call 可在最终 STATUS 提交前设置；
 * - 客户端在 FINISHED 回调内及 Call 能力句柄仍有效期间读取；
 * - V1 一元调用没有独立 STATUS 消息封装，因此不支持尾部元数据。
 */
int tr_rpc_call_set_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	void *value, uint16_t *value_len);

/* 读取 Call 标识/截止时间/取消状态的所有者一致快照。 */
int tr_rpc_call_get_context(struct tr_rpc_call_handle call,
			    struct tr_rpc_context *out);

/* 释放保留的 TR_RPC_MESSAGE_TAKE_OWNERSHIP 描述符。 */
int tr_rpc_message_release(struct tr_rpc_message *message);

#ifdef __cplusplus
}
#endif

#endif
