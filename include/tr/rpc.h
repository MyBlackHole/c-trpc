#ifndef TR_RPC_H
#define TR_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "tr/rpc_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Application-facing RPC contract.
 *
 * Endpoint/Channel/Executor construction is intentionally not declared here;
 * those are internal engine concerns.
 */

#define TR_RPC_METADATA_MAX_BYTES 512U
#define TR_RPC_METADATA_MAX_KEY_LEN 63U

/* RPC-level traffic class. Values intentionally match the current Transport lanes. */
#define TR_RPC_LANE_CONTROL 0U
#define TR_RPC_LANE_BULK 1U

struct tr_rpc_metadata {
	const char *key;
	const void *value;
	uint16_t value_len;
};

struct tr_rpc_call_options {
	/* 从 Call 创建时刻开始计算的相对 deadline；0 表示禁用 deadline。 */
	uint32_t timeout_ms;

	/* 仅附加到首个 outbound RPC message 的初始 metadata。 */
	const struct tr_rpc_metadata *metadata;
	uint16_t metadata_count;
};

/*
 * V1 executable Method 使用 ONE/MANY。
 * 注意：当前 wire 没有独立 Method-open envelope，因此 Client request MANY 在
 * close_send() 前至少要成功发送一条 REQUEST；真正 0-message streaming 属于
 * future Method-open 扩展。
 */
enum tr_rpc_cardinality { TR_RPC_NONE = 0, TR_RPC_ONE = 1, TR_RPC_MANY = 2 };

/*
 * Call context snapshot。
 *
 * 这是 protocol identity/lifecycle 的只读快照，不暴露 Endpoint/Stream/slot。
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
 * V1 Interceptor：
 *
 * - 在所属 Reactor owner 上同步执行，必须短小且非阻塞；
 * - callback 内只允许使用 Call Context/metadata API，不应 send/finish/cancel；
 * - SERVER_PRE_HANDLER 返回 OK 表示继续，返回合法非 OK RPC status 表示在
 *   application handler 运行前拒绝该 Call；
 * - 其他 phase 的返回值 V1 忽略，建议返回 OK。
 *
 * hook 运行时不会持有 endpoint->lock，因此 Context/metadata API 可安全重入。
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
 * Stable, layout-independent RPC lifecycle snapshot.
 *
 * Client started: the local Call API returned TR_OK and published a Call
 * capability. Server started: the first valid REQUEST was accepted for a
 * registered Method (including a later interceptor/admission rejection).
 *
 * finished is recorded exactly once when this endpoint commits the final
 * application-visible RPC outcome. final_status[] is indexed by enum
 * tr_rpc_status and its sum equals calls_finished.
 *
 * calls_inflight is a snapshot gauge (started - finished), not a Runtime slot,
 * executor or transport count.
 */
struct tr_rpc_semantic_stats {
	uint64_t calls_started;
	uint64_t calls_finished;
	uint64_t calls_inflight;
	uint64_t final_status[TR_RPC_STATUS_COUNT];
};

/*
 * Opaque fixed-size Call capability.
 *
 * Applications may copy/pass the whole value by value, but must not inspect or
 * modify _private.  A zero-initialized value is invalid.  The representation
 * deliberately does not expose Endpoint/slot/generation engine identity.
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

	/* TR_RPC_LANE_CONTROL or TR_RPC_LANE_BULK. */
	uint32_t lane;

	/* 这是单条 message 上限，不是整个 Stream 的累计上限。 */
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
 * Streaming receive descriptor.
 *
 * bytes is the only application-visible payload view.  _private is a fixed
 * release capability used only by tr_rpc_message_release(); it keeps the
 * zero-copy TAKE_OWNERSHIP path without exposing Buffer/Stream/Channel types.
 *
 * Returning TR_RPC_MESSAGE_TAKE_OWNERSHIP transfers both the RX payload and
 * the internal peer-lifetime pin to the application.  The retained descriptor
 * remains safe to release after peer disconnect and must be released exactly
 * once.  Copy the whole descriptor unchanged when moving that ownership; do
 * not release multiple copies.  Retained messages may delay peer teardown, so
 * applications must release them before expecting Client/Server destroy to
 * complete.
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
	 * Peer 在 final STATUS 之前提前关闭 response half 时上报。
	 * 正常 STATUS 后的 transport close 不重复上报该事件。
	 */
	TR_RPC_CALL_EVENT_REMOTE_CLOSED = 3,
	/* Application terminal barrier；正常 Call 中它之后不再有其他事件。 */
	TR_RPC_CALL_EVENT_FINISHED = 4,
	/* final STATUS 之前的异常 terminal。 */
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
 * Application Call continuation API.
 *
 * send(): Client 发送 REQUEST；Server 发送 RESPONSE。
 *
 * close_send(): 仅用于 Client request-side half-close。V1 ONE 必须已经发送
 * exactly 1 条 REQUEST；MANY 必须已经发送至少 1 条 REQUEST。
 *
 * finish(): 仅用于 Server streaming Call，发送 final STATUS 并结束 response
 * side。Server 不应使用 close_send() 绕过 STATUS。
 */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

/*
 * Initial metadata:
 * - set_metadata() 必须在本方向第一条业务 envelope 发送前调用；
 * - get_peer_metadata() 读取 peer 首个 REQUEST/RESPONSE 的 initial metadata。
 */
int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/*
 * Trailing metadata V1：
 * - 仅 Server streaming Call 可在 final STATUS 提交前设置；
 * - Client 在 FINISHED callback 内及 Call capability 尚存活期间读取；
 * - Unary V1 没有独立 STATUS envelope，因此不支持 trailers。
 */
int tr_rpc_call_set_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_trailing_metadata(
	struct tr_rpc_call_handle call, const char *key,
	void *value, uint16_t *value_len);

/* 读取 Call identity/deadline/cancellation 的 owner-consistent snapshot。 */
int tr_rpc_call_get_context(struct tr_rpc_call_handle call,
			    struct tr_rpc_context *out);

/* Release a retained TR_RPC_MESSAGE_TAKE_OWNERSHIP descriptor. */
int tr_rpc_message_release(struct tr_rpc_message *message);

#ifdef __cplusplus
}
#endif

#endif
