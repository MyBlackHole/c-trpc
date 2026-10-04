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
 * Applications must copy the whole descriptor unchanged when retaining it.
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

/* Application Call continuation API. */
int tr_rpc_call_send(struct tr_rpc_call_handle call,
		     const struct tr_rpc_bytes *message);
int tr_rpc_call_close_send(struct tr_rpc_call_handle call);
int tr_rpc_call_finish(struct tr_rpc_call_handle call, int status);
int tr_rpc_call_cancel(struct tr_rpc_call_handle call);
int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle call, int *status_out);

int tr_rpc_call_set_metadata(struct tr_rpc_call_handle call, const char *key,
			     const void *value, uint16_t value_len);
int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle call,
				  const char *key, void *value,
				  uint16_t *value_len);

/* Release a retained TR_RPC_MESSAGE_TAKE_OWNERSHIP descriptor. */
int tr_rpc_message_release(struct tr_rpc_message *message);

#ifdef __cplusplus
}
#endif

#endif
