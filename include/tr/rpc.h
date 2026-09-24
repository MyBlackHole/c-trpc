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
	/* Relative deadline from call creation; 0 disables the deadline. */
	uint32_t timeout_ms;

	/* Initial metadata attached to the first outbound RPC message. */
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

	/* Per-message limits, not whole-stream limits. */
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
 * Streaming receive message. bytes is a view inside storage. If a callback
 * returns TAKE_OWNERSHIP it must copy this small descriptor and later call
 * tr_rpc_message_release().
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

	/* Pool used for copied RPC envelopes and small fast-path headers. */
	struct tr_buffer_pool *message_pool;

	/*
     * Bounded RPC executor task capacity. 0 selects a default based on
     * max_calls. Capacity is shared by all executor workers.
     */
	uint32_t executor_queue_capacity;

	/*
     * Executor worker count. 0 selects a small default capped by max_calls.
     * Different Calls may execute in parallel, while callbacks belonging to
     * one Call are always executed serially and in enqueue order.
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

/* Call only after external users have stopped creating new work. */
void tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint);

/*
 * Registers a descriptor. On a client unary_handler must be NULL and any
 * NONE/ONE/MANY shape is accepted. On a server a non-NULL unary handler is
 * valid only for ONE -> ONE methods. TR_RPC_NONE is reserved for a future
 * method-open envelope; V1 executable methods use ONE or MANY in both
 * directions.
 */
int tr_rpc_register_method(struct tr_rpc_endpoint *endpoint,
			   const struct tr_rpc_method_desc *method,
			   tr_rpc_unary_handler unary_handler,
			   void *handler_arg);

/* Registers a non-Unary server method. Callbacks execute on the RPC executor. */
int tr_rpc_register_stream_method(struct tr_rpc_endpoint *endpoint,
				  const struct tr_rpc_method_desc *method,
				  const struct tr_rpc_stream_handlers *handlers,
				  void *handler_arg);

/* Backward-compatible ONE -> ONE convenience API. */
int tr_rpc_unary_call_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 const struct tr_rpc_call_options *options,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out);

int tr_rpc_unary_call(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id, const struct tr_rpc_bytes *request,
		      tr_rpc_unary_result_cb result_cb, void *result_arg,
		      struct tr_rpc_call_handle *out);

/* Starts a client Call for ONE/MANY streaming shapes. */
int tr_rpc_call_start_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_options *options,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out);

int tr_rpc_call_start(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id,
		      const struct tr_rpc_call_callbacks *callbacks,
		      struct tr_rpc_call_handle *out);

/* Copies a small message into the endpoint pool. No caller ownership changes. */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);

/*
 * RAW bulk fast path. Only a 32-byte RPC envelope is copied; payload is passed
 * to Transport as a second slice. On TR_OK payload ownership transfers to the
 * transport; on failure the caller retains payload.
 */
int tr_rpc_call_send_buffer(struct tr_rpc_call_handle call,
			    struct tr_buffer *payload);

/* Half-closes the local message direction. */
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);

/* Server final status: sends a STATUS envelope then half-closes local output. */
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);

/*
 * Cooperative cancellation. The local Call completes immediately with
 * CANCELLED while a best-effort CANCEL control envelope is propagated to the
 * peer when that peer has already observed the Call. Cancellation never
 * implies rollback of application side effects.
 */
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);

/*
 * Query cancellation/deadline state from application callbacks. Returns 1 if
 * cancelled, 0 if still active, or a negative tr_status on stale/invalid input.
 */
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

/*
 * Initial metadata is bounded and sent only with the first outbound RPC
 * message in each direction. Keys are lowercase ASCII [a-z0-9_.-].
 * Metadata may be set only before that first message is encoded.
 */
int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);

/* Copies one peer metadata value. *value_len is input capacity/output size. */
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/* Release a message retained by TR_RPC_MESSAGE_TAKE_OWNERSHIP. */
int tr_rpc_message_release(struct tr_rpc_message *message);

/* Retry pending Unary responses / final local close and Transport control work. */
int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint);

int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out);

#ifdef __cplusplus
}
#endif

#endif
