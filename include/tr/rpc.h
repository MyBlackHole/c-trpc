#ifndef TR_RPC_H
#define TR_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "tr/rpc_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 面向应用的 RPC 契约。
 *
 * 此处有意不声明 Endpoint、Channel 和 Executor 的构造接口；
 * 这些属于内部引擎职责。
 */

#define TR_RPC_METADATA_MAX_BYTES 512U
#define TR_RPC_METADATA_MAX_KEY_LEN 63U

/* RPC 层流量类别；取值有意与当前 Transport 通道保持一致。 */
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

	/* 仅附加到首个向外发送 RPC 消息的初始元数据。 */
	const struct tr_rpc_metadata *metadata;
	uint16_t metadata_count;
};

/*
 * V1 可执行 Method 使用 ONE/MANY。
 * 注意：当前线协议没有独立的 Method-open 信封，因此 Client 的 MANY 请求在
 * close_send() 前至少要成功发送一条 REQUEST；真正的零消息流式调用留给
 * 后续 Method-open 扩展。
 */
enum tr_rpc_cardinality { TR_RPC_NONE = 0, TR_RPC_ONE = 1, TR_RPC_MANY = 2 };

/*
 * Call 上下文快照。
 *
 * 这是协议标识与生命周期的只读快照，不暴露 Endpoint、Stream 或槽位。
 * deadline_remaining_ms 仅在 has_deadline != 0 时有效；它表示读取瞬间的相对值。
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
 * - 在所属 Reactor 所有者上同步执行，必须短小且非阻塞；
 * - 回调内只允许使用 Call 上下文/元数据 API，不应执行 send/finish/cancel；
 * - SERVER_PRE_HANDLER 返回 OK 表示继续，返回合法的非 OK RPC 状态表示在
 *   应用处理器运行前拒绝该 Call；
 * - V1 忽略其他阶段的返回值，建议返回 OK。
 *
 * 钩子运行时不会持有 endpoint->lock，因此上下文/元数据 API 可以安全重入。
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
 * 稳定且与内存布局无关的 RPC 生命周期快照。
 *
 * Client 侧 started：本地 Call API 返回 TR_OK 并发布了 Call 能力。
 * Server 侧 started：已注册 Method 接受了第一条合法 REQUEST
 * （包括随后被拦截器或准入逻辑拒绝的情况）。
 *
 * 当该 Endpoint 提交最终、应用可见的 RPC 结果时，finished 恰好记录一次。
 * final_status[] 使用 enum tr_rpc_status 作为索引，其总和等于 calls_finished。
 *
 * calls_inflight 是 started - finished 的快照计量值，
 * 不是 Runtime 槽位、Executor 或 Transport 数量。
 */
struct tr_rpc_semantic_stats {
	uint64_t calls_started;
	uint64_t calls_finished;
	uint64_t calls_inflight;
	uint64_t final_status[TR_RPC_STATUS_COUNT];
};

/*
 * 不透明的固定大小 Call 能力。
 *
 * 应用可以按值复制或传递整个对象，但不得读取或修改 _private。
 * 全零初始化的对象无效；其表示形式有意不暴露 Endpoint、槽位或代次等引擎标识。
 *
 * 这是借用能力而不是所有权引用：复制它不会延长 Client、Server 或 Endpoint
 * 的生命周期。不得与所属对象的销毁并发使用，也不得在所属门面销毁后继续使用。
 * Call 的终止回调或处理器闭包完成后，后续继续使用必须视为无效或过期。
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

	/* 取 TR_RPC_LANE_CONTROL 或 TR_RPC_LANE_BULK。 */
	uint32_t lane;

	/* 这是单条消息的上限，不是整个 Stream 的累计上限。 */
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
 * bytes 是应用唯一可见的载荷视图。_private 是固定的释放能力，
 * 仅供 tr_rpc_message_release() 使用；它在不暴露 Buffer、Stream、Channel
 * 类型的前提下保留零拷贝 TAKE_OWNERSHIP 路径。
 *
 * 返回 TR_RPC_MESSAGE_TAKE_OWNERSHIP 会同时把接收载荷和内部对端生命周期
 * 固定引用转移给应用。即使对端已经断开，保留的描述符仍可安全释放，
 * 但必须且只能释放一次。转移该所有权时应原样复制整个描述符，
 * 不得释放它的多个副本。保留消息可能延迟对端清理，因此应用必须先释放这些
 * 消息，再等待 Client/Server 销毁完成。
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
	 * 对端在 final STATUS 之前提前关闭响应方向时上报。
	 * 正常 STATUS 后的 Transport 关闭不会重复上报该事件。
	 */
	TR_RPC_CALL_EVENT_REMOTE_CLOSED = 3,
	/* 应用侧终止屏障；正常 Call 中它之后不再有其他事件。 */
	TR_RPC_CALL_EVENT_FINISHED = 4,
	/* final STATUS 之前的异常终止事件。 */
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
 * 应用侧 Call 继续操作 API。
 *
 * send()：Client 发送 REQUEST；Server 发送 RESPONSE。
 *
 * close_send()：仅用于 Client 请求方向的半关闭。V1 ONE 必须已经恰好发送
 * 1 条 REQUEST；MANY 必须已经发送至少 1 条 REQUEST。
 *
 * finish()：仅用于 Server 流式 Call，发送 final STATUS 并结束响应方向。
 * Server 不应使用 close_send() 绕过 STATUS。
 */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

/*
 * 初始元数据：
 * - set_metadata() 必须在本方向第一条业务信封发送前调用；
 * - get_peer_metadata() 读取对端首个 REQUEST/RESPONSE 携带的初始元数据。
 */
int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/*
 * V1 尾随元数据：
 * - 仅 Server 流式 Call 可在 final STATUS 提交前设置；
 * - Client 可在 FINISHED 回调内以及 Call 能力仍存活期间读取；
 * - Unary V1 没有独立 STATUS 信封，因此不支持尾随元数据。
 */
int tr_rpc_call_set_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	void *value, uint16_t *value_len);

/* 读取 Call 标识、截止时间和取消状态的所有者一致性快照。 */
int tr_rpc_call_get_context(struct tr_rpc_call_handle call,
			    struct tr_rpc_context *out);

/* 释放通过 TR_RPC_MESSAGE_TAKE_OWNERSHIP 保留的描述符。 */
int tr_rpc_message_release(struct tr_rpc_message *message);

#ifdef __cplusplus
}
#endif

#endif
