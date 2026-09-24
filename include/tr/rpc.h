#ifndef TR_RPC_H
#define TR_RPC_H

#include <stddef.h>
#include <stdint.h>

#include "tr/buffer.h"
#include "tr/channel.h"
#include "tr/rpc_codec.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_rpc_endpoint;

#define TR_RPC_METADATA_MAX_BYTES 512U
#define TR_RPC_METADATA_MAX_KEY_LEN 63U

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

enum tr_rpc_role { TR_RPC_CLIENT = 1, TR_RPC_SERVER = 2 };

enum tr_rpc_cardinality { TR_RPC_NONE = 0, TR_RPC_ONE = 1, TR_RPC_MANY = 2 };

enum tr_rpc_status {
	TR_RPC_STATUS_OK = 0,
	TR_RPC_STATUS_CANCELLED = 1,
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

struct tr_rpc_call_handle {
	struct tr_rpc_endpoint *endpoint;
	uint32_t slot;
	uint32_t generation;
};

struct tr_rpc_method_desc {
	uint32_t service_id;
	uint32_t method_id;

	enum tr_rpc_cardinality request_cardinality;
	enum tr_rpc_cardinality response_cardinality;

	uint32_t request_codec_id;
	uint32_t response_codec_id;

	enum tr_lane lane;

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
 * Streaming 接收消息。bytes 只是 storage 内部的一段视图。
 * callback 如果返回 TAKE_OWNERSHIP，必须保存这个小型 descriptor，
 * 并在使用结束后调用 tr_rpc_message_release() 归还资源。
 */
struct tr_rpc_message {
	struct tr_rpc_bytes bytes;
	struct tr_buffer *storage;
	struct tr_stream_handle stream;
};

enum tr_rpc_message_disposition {
	TR_RPC_MESSAGE_RELEASE = 0,
	TR_RPC_MESSAGE_TAKE_OWNERSHIP = 1
};

enum tr_rpc_call_event {
	TR_RPC_CALL_EVENT_OPENED = 1,
	TR_RPC_CALL_EVENT_WRITABLE = 2,
	TR_RPC_CALL_EVENT_REMOTE_CLOSED = 3,
	TR_RPC_CALL_EVENT_FINISHED = 4,
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

struct tr_rpc_endpoint_config {
	enum tr_rpc_role role;
	uint32_t max_methods;
	uint32_t max_calls;

	/* 用于复制 RPC envelope 和小型快路径 header 的 buffer pool。 */
	struct tr_buffer_pool *message_pool;

	/*
     * RPC executor 的有界 task 容量。0 表示根据 max_calls 选择默认值。
     * 该容量由该 executor 的所有 worker 共享。
     */
	uint32_t executor_queue_capacity;

	/*
     * executor worker 数量。0 表示选择一个不超过 max_calls 的小型默认值。
     * 不同 Call 可以并行执行；同一个 Call 的 callback 必须严格串行，
     * 并保持 enqueue 顺序。
     */
	uint32_t executor_threads;
};

struct tr_rpc_endpoint_stats {
	uint32_t max_methods;
	uint32_t registered_methods;
	uint32_t max_calls;
	uint32_t opening_calls;
	uint32_t active_calls;
	uint32_t terminal_calls;

	uint32_t executor_threads;
	uint32_t executor_queued_tasks;
	uint32_t executor_running_tasks;

	uint64_t calls_started;
	uint64_t calls_completed;
	uint64_t calls_cancelled;
	uint64_t calls_deadline_exceeded;
};

int tr_rpc_endpoint_create(struct tr_channel *channel,
			   const struct tr_rpc_endpoint_config *config,
			   struct tr_rpc_endpoint **out);

/*
 * 仅当外部用户已经停止创建新工作后调用。
 * 销毁是同步的：函数返回前会完成 Channel callback quiescence，并等待
 * executor task 的强引用全部排空。因此 Endpoint 借用的 Channel 可以在
 * 本函数返回后立即销毁。
 */
void tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint);

/*
 * 注册 Method Descriptor。
 * Client 侧 unary_handler 必须为 NULL，可注册 NONE/ONE/MANY 形状；
 * Server 侧非 NULL unary_handler 仅允许 ONE -> ONE。
 * TR_RPC_NONE 预留给未来的 method-open envelope；V1 可执行方法的双向
 * cardinality 只使用 ONE 或 MANY。
 */
int tr_rpc_register_method(struct tr_rpc_endpoint *endpoint,
			   const struct tr_rpc_method_desc *method,
			   tr_rpc_unary_handler unary_handler,
			   void *handler_arg);

/* 注册非 Unary 的 Server 方法；callback 在线程池 RPC executor 中执行。 */
int tr_rpc_register_stream_method(struct tr_rpc_endpoint *endpoint,
				  const struct tr_rpc_method_desc *method,
				  const struct tr_rpc_stream_handlers *handlers,
				  void *handler_arg);

/* 兼容现有 ONE -> ONE 使用方式的便利接口。 */
int tr_rpc_unary_call_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 const struct tr_rpc_call_options *options,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out);

int tr_rpc_unary_call(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id, const struct tr_rpc_bytes *request,
		      tr_rpc_unary_result_cb result_cb, void *result_arg,
		      struct tr_rpc_call_handle *out);

/* 为 ONE/MANY streaming 形状启动一个 Client Call。 */
int tr_rpc_call_start_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_options *options,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out);

int tr_rpc_call_start(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id,
		      const struct tr_rpc_call_callbacks *callbacks,
		      struct tr_rpc_call_handle *out);

/* 把小消息复制进 Endpoint pool；调用方输入的 ownership 不发生变化。 */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);

/*
 * RAW bulk 快路径：只复制 32-byte RPC envelope，payload 作为第二个 slice
 * 直接交给 Transport。
 * 所有权：TR_OK 时 payload ownership 转移给 Transport；失败时仍归调用方。
 */
int tr_rpc_call_send_buffer(struct tr_rpc_call_handle call,
			    struct tr_buffer *payload);

/* half-close 本地发送方向。 */
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);

/* Server 发送最终 STATUS envelope，然后 half-close 本地输出方向。 */
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);

/*
 * cooperative cancellation：本地 Call 立即以 CANCELLED 结束；
 * 如果 peer 已经观察到该 Call，则尽力发送 CANCEL control envelope。
 * cancellation 只终止协议/回调生命周期，不代表回滚已经发生的业务副作用。
 */
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);

/*
 * 供应用 callback 查询 cancellation/deadline 状态：
 * 已取消返回 1，仍 active 返回 0，handle stale/参数非法返回负的 tr_status。
 */
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

/*
 * initial metadata 有大小上限，并且每个方向只随第一条 outbound RPC message
 * 发送。key 仅允许小写 ASCII [a-z0-9_.-]。
 * metadata 只能在该方向首条消息完成编码之前设置。
 */
int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);

/* 复制一个 peer metadata 值；*value_len 输入时是容量，返回时是实际长度。 */
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/* 释放通过 TR_RPC_MESSAGE_TAKE_OWNERSHIP 保留下来的 message。 */
int tr_rpc_message_release(struct tr_rpc_message *message);

/* 重试 pending Unary response、最终本地 close 以及 Transport control 工作。 */
int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint);

int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out);

#ifdef __cplusplus
}
#endif

#endif
