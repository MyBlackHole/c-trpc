#define _GNU_SOURCE
#include "tr/rpc.h"

#include "tr/rpc_wire.h"
#include "tr/status.h"
#include "tr/endian.h"
#include "tr/guard.h"
#include "tr/refcount.h"
#include "rpc_internal.h"
#include "channel_internal.h"
#include "reactor_internal.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum tr_rpc_call_state {
	TR_RPC_CALL_FREE = 0,
	TR_RPC_CALL_OPENING,
	TR_RPC_CALL_ACTIVE,
	TR_RPC_CALL_TERMINAL
};

enum tr_rpc_handler_kind {
	TR_RPC_HANDLER_NONE = 0,
	TR_RPC_HANDLER_UNARY,
	TR_RPC_HANDLER_STREAM
};

enum tr_rpc_task_type {
	TR_RPC_TASK_SERVER_UNARY = 1,
	TR_RPC_TASK_SERVER_STREAM_MESSAGE,
	TR_RPC_TASK_SERVER_HALF_CLOSE,
	TR_RPC_TASK_SERVER_WRITABLE,
	TR_RPC_TASK_SERVER_CLOSE,
	TR_RPC_TASK_CLIENT_UNARY_RESULT,
	TR_RPC_TASK_CLIENT_MESSAGE,
	TR_RPC_TASK_CLIENT_EVENT
};

struct tr_rpc_method_entry {
	int used;
	enum tr_rpc_handler_kind handler_kind;
	struct tr_rpc_method_desc desc;

	tr_rpc_unary_handler unary_handler;
	struct tr_rpc_stream_handlers stream_handlers;
	void *handler_arg;
};

struct tr_rpc_call_slot {
	uint32_t generation;
	enum tr_rpc_call_state state;

	struct tr_stream_handle stream;
	struct tr_rpc_method_entry *method;

	uint32_t tx_count;
	uint32_t rx_count;
	uint32_t task_refs;

	int is_unary;
	int local_closed;
	int remote_closed;
	int final_status_seen;
	int final_status_sent;
	int final_status;

	int cancelled;
	int cancel_status;
	int terminal_notified;
	uint64_t deadline_ns;

	uint8_t local_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t local_metadata_len;
	uint8_t peer_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t peer_metadata_len;

	struct tr_buffer *pending_tx;
	struct tr_buffer *pending_control;
	int need_local_close;
	int response_received;
	int result_delivered;

	tr_rpc_unary_result_cb result_cb;
	void *result_arg;

	struct tr_rpc_call_callbacks callbacks;
};

struct tr_rpc_task {
	uint16_t type;
	uint16_t first_message;
	int status;
	enum tr_rpc_call_event event;
	struct tr_rpc_call_handle call;
	struct tr_buffer *payload;

	/*
	 * Worker execution snapshot.  Mutable Call/Endpoint state stays owned by
	 * the Reactor; workers only consume these copied capabilities.
	 */
	struct tr_stream_handle stream;
	union {
		struct {
			tr_rpc_unary_handler handler;
			void *handler_arg;
			struct tr_rpc_method_desc method;
		} server_unary;
		struct {
			struct tr_rpc_stream_handlers handlers;
			void *handler_arg;
		} server_stream;
		struct {
			tr_rpc_unary_result_cb result_cb;
			void *result_arg;
		} client_unary;
		struct {
			struct tr_rpc_call_callbacks callbacks;
		} client_stream;
	} u;
};

/*
 * Unary worker 只产生业务结果；wire metadata、Call 状态和发送动作必须回到
 * Reactor owner thread 后再处理。
 */
struct tr_rpc_unary_completion {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;
	int status;
	uint32_t response_len;
	uint8_t response[];
};

struct tr_rpc_task_completion {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;
};

#define TR_RPC_EXEC_NONE UINT32_MAX

struct tr_rpc_executor_node {
	struct tr_rpc_task task;
	uint32_t next;
};

struct tr_rpc_executor_callq {
	uint32_t generation;
	uint32_t head;
	uint32_t tail;
	uint32_t queued_count;
	int ready;
	int running;
	int cancelled;
};

struct tr_rpc_executor_group;

struct tr_rpc_executor {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t *threads;
	uint32_t thread_count;

	struct tr_rpc_executor_node *nodes;
	struct tr_rpc_executor_callq *callq;
	uint32_t *ready_calls;

	uint32_t capacity;
	uint32_t free_head;
	uint32_t queued_count;
	uint32_t running_count;

	uint32_t ready_capacity;
	uint32_t ready_head;
	uint32_t ready_tail;
	uint32_t ready_count;

	struct tr_rpc_executor_group *group;
	int group_enqueued;
	int stopping;
	uint32_t started_threads;
};

struct tr_rpc_executor_group {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t *threads;
	struct tr_rpc_endpoint **ready_endpoints;

	uint32_t capacity;
	uint32_t head;
	uint32_t tail;
	uint32_t count;

	uint32_t thread_count;
	uint32_t started_threads;
	int stopping;
};

TR_DEFINE_PTR_OWNERSHIP(tr_rpc_group_mem, struct tr_rpc_executor_group, free)
TR_DEFINE_PTR_OWNERSHIP(tr_rpc_thread_array, pthread_t, free)
TR_DEFINE_PTR_OWNERSHIP(tr_rpc_endpoint_array, struct tr_rpc_endpoint *, free)
TR_DEFINE_PTR_OWNERSHIP(tr_rpc_group_owner, struct tr_rpc_executor_group,
			tr_rpc_executor_group_destroy)

struct tr_rpc_endpoint {
	pthread_mutex_t lock;
	pthread_cond_t ref_cond;
	int deadline_stopping;
	struct tr_reactor_timer_handle deadline_timer;
	int deadline_timer_registered;

	struct tr_channel *channel;
	struct tr_rpc_endpoint_config config;

	struct tr_rpc_method_entry *methods;
	struct tr_rpc_call_slot *calls;

	struct tr_rpc_executor executor;
	struct tr_refcount refs;

	uint64_t stat_calls_started;
	uint64_t stat_calls_completed;
	uint64_t stat_calls_cancelled;
	uint64_t stat_calls_deadline_exceeded;
};

#define TR_RPC_METADATA_RESERVED_TIMEOUT ":timeout-ms"
#define TR_RPC_METADATA_RESERVED_TIMEOUT_LEN 11U
#define TR_RPC_METADATA_TLV_HEADER_SIZE 3U
#define TR_RPC_DEADLINE_METADATA_BYTES     \
	(TR_RPC_METADATA_TLV_HEADER_SIZE + \
	 TR_RPC_METADATA_RESERVED_TIMEOUT_LEN + 8U)

static int tr_rpc_cancel_internal(struct tr_rpc_call_handle handle, int status);
static void tr_rpc_deadline_changed_locked(struct tr_rpc_endpoint *endpoint);
static int tr_rpc_endpoint_get(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_endpoint_put(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_endpoint_release(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_executor_complete_task(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle);

static uint64_t tr_rpc_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static uint64_t tr_rpc_timeout_deadline_ns(uint32_t timeout_ms)
{
	uint64_t now;
	uint64_t delta;

	if (timeout_ms == 0)
		return 0;
	now = tr_rpc_now_ns();
	delta = (uint64_t)timeout_ms * UINT64_C(1000000);
	if (UINT64_MAX - now < delta)
		return UINT64_MAX;
	return now + delta;
}

/*
 * Mutable RPC protocol state is applied by the Channel's Reactor owner.
 * endpoint->lock remains as a transition lock until the remaining application
 * control-plane APIs have also moved behind owner commands.
 */
static int tr_rpc_owner_call(struct tr_rpc_endpoint *endpoint,
			     int (*fn)(void *arg), void *arg)
{
	struct tr_reactor *reactor;

	if (!endpoint || !fn)
		return TR_ERR_INVALID;

	reactor = tr_channel_reactor(endpoint->channel);
	if (!reactor)
		return TR_ERR_STATE;
	return tr_reactor_call(reactor, fn, arg);
}

static int tr_rpc_metadata_key_valid(const char *key, size_t *len_out)
{
	size_t i;
	size_t len;

	if (!key)
		return 0;
	len = strlen(key);
	if (len == 0 || len > TR_RPC_METADATA_MAX_KEY_LEN || key[0] == ':')
		return 0;

	for (i = 0; i < len; ++i) {
		unsigned char c = (unsigned char)key[i];
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
		      c == '_' || c == '.' || c == '-'))
			return 0;
	}

	if (len_out)
		*len_out = len;
	return 1;
}

static int tr_rpc_metadata_add_raw(uint8_t *dst, uint16_t *len_io,
				   const char *key, size_t key_len,
				   const void *value, uint16_t value_len,
				   int allow_reserved)
{
	uint32_t need;
	uint32_t off;
	uint32_t pos;

	if (!dst || !len_io || !key || key_len == 0 || key_len > UINT8_MAX ||
	    (value_len != 0 && !value))
		return TR_ERR_INVALID;
	if (!allow_reserved && key[0] == ':')
		return TR_ERR_INVALID;

	/* 拒绝重复 key，保证 metadata lookup 语义确定且唯一。 */
	off = 0;
	while (off < *len_io) {
		uint8_t old_key_len;
		uint16_t old_value_len;
		uint32_t entry_len;

		if (*len_io - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_STATE;
		old_key_len = dst[off];
		old_value_len = tr_get_le16(dst + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)old_key_len + (uint32_t)old_value_len;
		if (entry_len > (uint32_t)(*len_io - off))
			return TR_ERR_STATE;
		if (old_key_len == key_len &&
		    memcmp(dst + off + TR_RPC_METADATA_TLV_HEADER_SIZE, key,
			   key_len) == 0)
			return TR_ERR_STATE;
		off += entry_len;
	}

	need = TR_RPC_METADATA_TLV_HEADER_SIZE + (uint32_t)key_len + value_len;
	if ((uint32_t)*len_io + need > TR_RPC_METADATA_MAX_BYTES)
		return TR_ERR_BAD_LENGTH;

	pos = *len_io;
	dst[pos] = (uint8_t)key_len;
	tr_put_le16(dst + pos + 1U, value_len);
	memcpy(dst + pos + TR_RPC_METADATA_TLV_HEADER_SIZE, key, key_len);
	if (value_len)
		memcpy(dst + pos + TR_RPC_METADATA_TLV_HEADER_SIZE + key_len,
		       value, value_len);
	*len_io = (uint16_t)(pos + need);
	return TR_OK;
}

static int tr_rpc_metadata_find_raw(const uint8_t *data, uint16_t len,
				    const char *key, size_t key_len,
				    const uint8_t **value, uint16_t *value_len)
{
	uint32_t off = 0;

	if (!data || !key || !value || !value_len)
		return TR_ERR_INVALID;

	while (off < len) {
		uint8_t item_key_len;
		uint16_t item_value_len;
		uint32_t entry_len;
		const uint8_t *item_key;

		if (len - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_BAD_LENGTH;
		item_key_len = data[off];
		item_value_len = tr_get_le16(data + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)item_key_len + item_value_len;
		if (entry_len > (uint32_t)(len - off) || item_key_len == 0)
			return TR_ERR_BAD_LENGTH;
		item_key = data + off + TR_RPC_METADATA_TLV_HEADER_SIZE;
		if (item_key_len == key_len &&
		    memcmp(item_key, key, key_len) == 0) {
			*value = item_key + item_key_len;
			*value_len = item_value_len;
			return TR_OK;
		}
		off += entry_len;
	}
	return TR_ERR_STALE;
}

static int tr_rpc_metadata_validate_raw(const uint8_t *data, uint16_t len)
{
	uint32_t off = 0;

	if (len != 0 && !data)
		return TR_ERR_INVALID;
	while (off < len) {
		uint8_t key_len;
		uint16_t value_len;
		uint32_t entry_len;

		if (len - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_BAD_LENGTH;
		key_len = data[off];
		value_len = tr_get_le16(data + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)key_len + value_len;
		if (key_len == 0 || entry_len > (uint32_t)(len - off))
			return TR_ERR_BAD_LENGTH;
		off += entry_len;
	}
	return TR_OK;
}

static int
tr_rpc_apply_options_locked(struct tr_rpc_endpoint *endpoint,
			    struct tr_rpc_call_slot *call,
			    const struct tr_rpc_call_options *options,
			    uint64_t deadline_ns)
{
	uint16_t i;
	int ret;

	if (!options)
		return TR_OK;

	for (i = 0; i < options->metadata_count; ++i) {
		const struct tr_rpc_metadata *item = &options->metadata[i];
		size_t key_len;

		if (!tr_rpc_metadata_key_valid(item->key, &key_len))
			return TR_ERR_INVALID;
		ret = tr_rpc_metadata_add_raw(call->local_metadata,
					      &call->local_metadata_len,
					      item->key, key_len, item->value,
					      item->value_len, 0);
		if (ret != TR_OK)
			return ret;
	}

	if (options->timeout_ms) {
		if ((uint32_t)call->local_metadata_len +
			    TR_RPC_DEADLINE_METADATA_BYTES >
		    TR_RPC_METADATA_MAX_BYTES)
			return TR_ERR_BAD_LENGTH;
		call->deadline_ns = deadline_ns;
		if (call->deadline_ns == 0)
			return TR_ERR_SYS;
		tr_rpc_deadline_changed_locked(endpoint);
	}
	return TR_OK;
}

static int tr_rpc_build_outbound_metadata_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call,
	uint16_t wire_type, uint8_t out[TR_RPC_METADATA_MAX_BYTES],
	uint16_t *out_len)
{
	uint16_t len;

	if (!endpoint || !call || !out || !out_len)
		return TR_ERR_INVALID;

	len = 0;
	if (call->tx_count == 0 && wire_type != TR_RPC_WIRE_CANCEL) {
		len = call->local_metadata_len;
		if (len)
			memcpy(out, call->local_metadata, len);
	}

	if (endpoint->config.role == TR_RPC_CLIENT &&
	    wire_type == TR_RPC_WIRE_REQUEST && call->tx_count == 0 &&
	    call->deadline_ns != 0) {
		uint64_t now = tr_rpc_now_ns();
		uint64_t remaining_ns;
		uint64_t remaining_ms;
		uint8_t value[8];
		int ret;

		if (now == 0)
			return TR_ERR_SYS;
		remaining_ns =
			call->deadline_ns > now ? call->deadline_ns - now : 0;
		remaining_ms =
			(remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
		if (remaining_ms == 0)
			remaining_ms = 1;
		tr_put_le64(value, remaining_ms);
		ret = tr_rpc_metadata_add_raw(
			out, &len, TR_RPC_METADATA_RESERVED_TIMEOUT,
			TR_RPC_METADATA_RESERVED_TIMEOUT_LEN, value,
			sizeof(value), 1);
		if (ret != TR_OK)
			return ret;
	}

	*out_len = len;
	return TR_OK;
}

static int tr_rpc_import_peer_metadata_locked(struct tr_rpc_endpoint *endpoint,
					      struct tr_rpc_call_slot *call,
					      uint16_t wire_type,
					      const uint8_t *metadata,
					      uint16_t metadata_len)
{
	uint32_t off = 0;
	int ret;

	if (!endpoint || !call)
		return TR_ERR_INVALID;
	ret = tr_rpc_metadata_validate_raw(metadata, metadata_len);
	if (ret != TR_OK)
		return ret;

	call->peer_metadata_len = 0;
	while (off < metadata_len) {
		uint8_t key_len = metadata[off];
		uint16_t value_len = tr_get_le16(metadata + off + 1U);
		const uint8_t *key =
			metadata + off + TR_RPC_METADATA_TLV_HEADER_SIZE;
		const uint8_t *value = key + key_len;
		uint32_t entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
				     (uint32_t)key_len + value_len;

		if (key_len == TR_RPC_METADATA_RESERVED_TIMEOUT_LEN &&
		    memcmp(key, TR_RPC_METADATA_RESERVED_TIMEOUT,
			   TR_RPC_METADATA_RESERVED_TIMEOUT_LEN) == 0) {
			if (endpoint->config.role != TR_RPC_SERVER ||
			    wire_type != TR_RPC_WIRE_REQUEST || value_len != 8U)
				return TR_ERR_BAD_LENGTH;
			{
				uint64_t timeout_ms = tr_get_le64(value);
				uint64_t now = tr_rpc_now_ns();
				uint64_t delta;

				if (timeout_ms == 0 || now == 0)
					return TR_ERR_BAD_LENGTH;
				if (timeout_ms > UINT64_MAX / UINT64_C(1000000))
					call->deadline_ns = UINT64_MAX;
				else {
					delta = timeout_ms * UINT64_C(1000000);
					call->deadline_ns =
						UINT64_MAX - now < delta ?
							UINT64_MAX :
							now + delta;
				}
				tr_rpc_deadline_changed_locked(endpoint);
			}
		} else {
			uint16_t new_len = call->peer_metadata_len;
			int add_ret;
			char key_copy[TR_RPC_METADATA_MAX_KEY_LEN + 1U];

			if (key_len > TR_RPC_METADATA_MAX_KEY_LEN)
				return TR_ERR_BAD_LENGTH;
			memcpy(key_copy, key, key_len);
			key_copy[key_len] = '\0';
			if (!tr_rpc_metadata_key_valid(key_copy, NULL))
				return TR_ERR_BAD_LENGTH;
			add_ret = tr_rpc_metadata_add_raw(call->peer_metadata,
							  &new_len, key_copy,
							  key_len, value,
							  value_len, 0);
			if (add_ret != TR_OK)
				return add_ret;
			call->peer_metadata_len = new_len;
		}
		off += entry_len;
	}
	return TR_OK;
}

static int tr_rpc_stream_equal(struct tr_stream_handle a,
			       struct tr_stream_handle b)
{
	return a.channel == b.channel && a.slot == b.slot &&
	       a.generation == b.generation;
}

static struct tr_rpc_call_handle
tr_rpc_make_call_handle(struct tr_rpc_endpoint *endpoint, uint32_t slot,
			const struct tr_rpc_call_slot *call)
{
	struct tr_rpc_call_handle handle;

	handle.endpoint = endpoint;
	handle.slot = slot;
	handle.generation = call->generation;
	return handle;
}

static struct tr_rpc_method_entry *
tr_rpc_find_method_locked(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			  uint32_t method_id)
{
	uint32_t i;

	for (i = 0; i < endpoint->config.max_methods; ++i) {
		struct tr_rpc_method_entry *entry = &endpoint->methods[i];
		if (entry->used && entry->desc.service_id == service_id &&
		    entry->desc.method_id == method_id)
			return entry;
	}
	return NULL;
}

static struct tr_rpc_call_slot *
tr_rpc_find_call_by_stream_locked(struct tr_rpc_endpoint *endpoint,
				  struct tr_stream_handle stream,
				  uint32_t *slot_out)
{
	uint32_t i;

	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		if (call->state != TR_RPC_CALL_FREE &&
		    tr_rpc_stream_equal(call->stream, stream)) {
			if (slot_out)
				*slot_out = i;
			return call;
		}
	}
	return NULL;
}

static struct tr_rpc_call_slot *
tr_rpc_lookup_call_handle_locked(struct tr_rpc_call_handle handle)
{
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;

	if (!endpoint || handle.slot >= endpoint->config.max_calls)
		return NULL;

	call = &endpoint->calls[handle.slot];
	if (call->state == TR_RPC_CALL_FREE ||
	    call->generation != handle.generation)
		return NULL;
	return call;
}

static int tr_rpc_allocate_call_locked(struct tr_rpc_endpoint *endpoint,
				       struct tr_rpc_call_slot **out,
				       uint32_t *slot_out)
{
	uint32_t i;

	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		uint32_t generation;

		if (call->state != TR_RPC_CALL_FREE)
			continue;

		generation = call->generation + 1U;
		if (generation == 0)
			generation = 1U;

		memset(call, 0, sizeof(*call));
		call->generation = generation;
		endpoint->stat_calls_started++;
		if (out)
			*out = call;
		if (slot_out)
			*slot_out = i;
		return TR_OK;
	}
	return TR_AGAIN;
}

static void tr_rpc_free_call_locked(struct tr_rpc_call_slot *call)
{
	uint32_t generation;

	if (!call)
		return;

	generation = call->generation;
	if (call->pending_tx)
		tr_buffer_release(call->pending_tx);
	if (call->pending_control)
		tr_buffer_release(call->pending_control);
	memset(call, 0, sizeof(*call));
	call->generation = generation;
}

static void tr_rpc_maybe_free_call_locked(struct tr_rpc_call_slot *call)
{
	if (call && call->state == TR_RPC_CALL_TERMINAL &&
	    call->task_refs == 0 && !call->pending_control &&
	    !call->need_local_close)
		tr_rpc_free_call_locked(call);
}

static enum tr_rpc_cardinality
tr_rpc_outbound_cardinality(const struct tr_rpc_endpoint *endpoint,
			    const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->request_cardinality :
		       method->response_cardinality;
}

static uint32_t tr_rpc_outbound_codec(const struct tr_rpc_endpoint *endpoint,
				      const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->request_codec_id :
		       method->response_codec_id;
}

static uint32_t tr_rpc_outbound_limit(const struct tr_rpc_endpoint *endpoint,
				      const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->max_request_bytes :
		       method->max_response_bytes;
}

static uint16_t
tr_rpc_outbound_wire_type(const struct tr_rpc_endpoint *endpoint)
{
	return endpoint->config.role == TR_RPC_CLIENT ? TR_RPC_WIRE_REQUEST :
							TR_RPC_WIRE_RESPONSE;
}

static int tr_rpc_validate_cardinality(enum tr_rpc_cardinality cardinality,
				       uint32_t count)
{
	if (cardinality == TR_RPC_NONE)
		return TR_ERR_STATE;
	if (cardinality == TR_RPC_ONE && count != 0)
		return TR_ERR_STATE;
	if (cardinality != TR_RPC_ONE && cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	return TR_OK;
}

static int tr_rpc_encode_message(struct tr_rpc_endpoint *endpoint,
				 struct tr_rpc_call_slot *call, uint16_t type,
				 const struct tr_rpc_method_desc *method,
				 uint32_t codec_id, int status,
				 const struct tr_rpc_bytes *message,
				 struct tr_buffer **out)
{
	struct tr_rpc_wire_header header;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t metadata_len = 0;
	uint32_t encoded = 0;
	uint32_t metadata_wire_len = 0;
	uint32_t payload_off;
	uint32_t total;
	int ret;

	if (!endpoint || !call || !method || !message || !out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (codec_id != TR_RPC_CODEC_RAW)
		return TR_ERR_BAD_TYPE;
	ret = tr_rpc_build_outbound_metadata_locked(endpoint, call, type,
						    metadata, &metadata_len);
	if (ret != TR_OK)
		return ret;
	if (metadata_len)
		metadata_wire_len =
			TR_RPC_WIRE_METADATA_PREFIX_SIZE + metadata_len;
	if (message->len >
	    UINT32_MAX - TR_RPC_WIRE_HEADER_SIZE - metadata_wire_len)
		return TR_ERR_BAD_LENGTH;
	total = TR_RPC_WIRE_HEADER_SIZE + metadata_wire_len + message->len;

	ret = tr_buffer_acquire(endpoint->config.message_pool, total, &buffer);
	if (ret != TR_OK)
		return ret;

	memset(&header, 0, sizeof(header));
	header.version = TR_RPC_WIRE_VERSION;
	header.type = type;
	header.service_id = method->service_id;
	header.method_id = method->method_id;
	header.codec_id = codec_id;
	header.flags = metadata_len ? TR_RPC_WIRE_F_METADATA : 0;
	header.status = status;
	header.payload_len = message->len;

	ret = tr_rpc_wire_encode(buffer->data, &header);
	if (ret != TR_OK)
		return ret;

	payload_off = TR_RPC_WIRE_HEADER_SIZE;
	if (metadata_len) {
		tr_put_le16(buffer->data + payload_off, metadata_len);
		memcpy(buffer->data + payload_off +
			       TR_RPC_WIRE_METADATA_PREFIX_SIZE,
		       metadata, metadata_len);
		payload_off += metadata_wire_len;
	}

	ret = tr_rpc_raw_encode(message, buffer->data + payload_off,
				buffer->capacity - payload_off, &encoded);
	if (ret != TR_OK || encoded != message->len) {
		if (ret == TR_OK)
			ret = TR_ERR_STATE;
		return ret;
	}

	buffer->len = total;
	*out = tr_buffer_take(&buffer);
	return TR_OK;
}

static int tr_rpc_encode_header_buffer(struct tr_rpc_endpoint *endpoint,
				       struct tr_rpc_call_slot *call,
				       uint16_t type,
				       const struct tr_rpc_method_desc *method,
				       uint32_t codec_id, int status,
				       uint32_t payload_len,
				       struct tr_buffer **out)
{
	struct tr_rpc_wire_header header;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t metadata_len = 0;
	uint32_t metadata_wire_len = 0;
	uint32_t total;
	int ret;

	if (!endpoint || !call || !method || !out)
		return TR_ERR_INVALID;
	*out = NULL;

	ret = tr_rpc_build_outbound_metadata_locked(endpoint, call, type,
						    metadata, &metadata_len);
	if (ret != TR_OK)
		return ret;
	if (metadata_len)
		metadata_wire_len =
			TR_RPC_WIRE_METADATA_PREFIX_SIZE + metadata_len;
	total = TR_RPC_WIRE_HEADER_SIZE + metadata_wire_len;

	ret = tr_buffer_acquire(endpoint->config.message_pool, total, &buffer);
	if (ret != TR_OK)
		return ret;

	memset(&header, 0, sizeof(header));
	header.version = TR_RPC_WIRE_VERSION;
	header.type = type;
	header.service_id = method->service_id;
	header.method_id = method->method_id;
	header.codec_id = codec_id;
	header.flags = metadata_len ? TR_RPC_WIRE_F_METADATA : 0;
	header.status = status;
	header.payload_len = payload_len;

	ret = tr_rpc_wire_encode(buffer->data, &header);
	if (ret != TR_OK)
		return ret;

	if (metadata_len) {
		tr_put_le16(buffer->data + TR_RPC_WIRE_HEADER_SIZE,
			    metadata_len);
		memcpy(buffer->data + TR_RPC_WIRE_HEADER_SIZE +
			       TR_RPC_WIRE_METADATA_PREFIX_SIZE,
		       metadata, metadata_len);
	}

	buffer->len = total;
	*out = tr_buffer_take(&buffer);
	return TR_OK;
}

static int tr_rpc_encode_control_locked(struct tr_rpc_endpoint *endpoint,
					struct tr_rpc_call_slot *call,
					uint16_t type, int status,
					struct tr_buffer **out)
{
	struct tr_rpc_method_desc method;

	if (!endpoint || !call || !call->method || !out)
		return TR_ERR_INVALID;
	method = call->method->desc;
	return tr_rpc_encode_header_buffer(endpoint, call, type, &method, 0,
					   status, 0, out);
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_try_unary_send_locked(struct tr_rpc_endpoint *endpoint,
					struct tr_rpc_call_slot *call)
{
	int ret = TR_OK;

	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL)
		return TR_ERR_CLOSED;
	if (call->pending_tx) {
		ret = tr_stream_send(call->stream, call->pending_tx);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&call->pending_tx);
			call->tx_count++;
			call->need_local_close = 1;
			call->state = TR_RPC_CALL_ACTIVE;
			if (endpoint->config.role == TR_RPC_SERVER)
				call->deadline_ns = 0;
		} else {
			return ret;
		}
	}

	if (call->need_local_close) {
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			call->need_local_close = 0;
			call->local_closed = 1;
			if (endpoint->config.role == TR_RPC_SERVER &&
			    call->remote_closed) {
				call->state = TR_RPC_CALL_TERMINAL;
				call->deadline_ns = 0;
				tr_rpc_deadline_changed_locked(endpoint);
				tr_rpc_maybe_free_call_locked(call);
			}
			return TR_OK;
		}
	}
	return ret;
}

static int tr_rpc_executor_ready_push_locked(struct tr_rpc_executor *executor,
					     uint32_t slot)
{
	if (executor->ready_count == executor->ready_capacity)
		return TR_ERR_STATE;

	executor->ready_calls[executor->ready_tail] = slot;
	executor->ready_tail =
		(executor->ready_tail + 1U) % executor->ready_capacity;
	executor->ready_count++;
	return TR_OK;
}

static void
tr_rpc_executor_ready_undo_push_locked(struct tr_rpc_executor *executor,
				       uint32_t slot)
{
	uint32_t tail;

	if (executor->ready_count == 0 || executor->ready_capacity == 0)
		return;

	tail = (executor->ready_tail + executor->ready_capacity - 1U) %
	       executor->ready_capacity;
	if (executor->ready_calls[tail] != slot)
		return;

	executor->ready_tail = tail;
	executor->ready_count--;
}

static int tr_rpc_executor_group_enqueue(struct tr_rpc_executor_group *group,
					 struct tr_rpc_endpoint *endpoint)
{
	int ret = TR_OK;

	if (!group || !endpoint)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&group->lock);
	if (group->stopping)
		ret = TR_ERR_CLOSED;
	else if (group->count == group->capacity)
		ret = TR_ERR_STATE;
	else {
		group->ready_endpoints[group->tail] = endpoint;
		group->tail = (group->tail + 1U) % group->capacity;
		group->count++;
		pthread_cond_signal(&group->cond);
	}
	pthread_mutex_unlock(&group->lock);
	return ret;
}

static int tr_rpc_executor_push(struct tr_rpc_endpoint *endpoint,
				const struct tr_rpc_task *task)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	struct tr_rpc_executor_callq *callq;
	struct tr_rpc_executor_node *node;
	uint32_t node_index;
	int ret = TR_OK;

	if (!task || task->call.slot >= endpoint->config.max_calls)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&executor->lock);
	if (executor->stopping || executor->free_head == TR_RPC_EXEC_NONE) {
		ret = executor->stopping ? TR_ERR_CLOSED : TR_AGAIN;
		goto out;
	}

	callq = &executor->callq[task->call.slot];
	if (callq->generation != task->call.generation) {
		if (callq->running || callq->ready ||
		    callq->head != TR_RPC_EXEC_NONE) {
			ret = TR_ERR_STATE;
			goto out;
		}
		callq->generation = task->call.generation;
		callq->head = TR_RPC_EXEC_NONE;
		callq->tail = TR_RPC_EXEC_NONE;
		callq->queued_count = 0;
		callq->cancelled = 0;
	}

	node_index = executor->free_head;
	node = &executor->nodes[node_index];
	executor->free_head = node->next;
	node->task = *task;
	node->next = TR_RPC_EXEC_NONE;

	if (callq->tail != TR_RPC_EXEC_NONE)
		executor->nodes[callq->tail].next = node_index;
	else
		callq->head = node_index;
	callq->tail = node_index;
	callq->queued_count++;
	executor->queued_count++;

	if (!callq->running && !callq->ready) {
		ret = tr_rpc_executor_ready_push_locked(executor,
							task->call.slot);
		if (ret != TR_OK) {
			uint32_t cur = callq->head;
			uint32_t prev = TR_RPC_EXEC_NONE;

			while (cur != TR_RPC_EXEC_NONE && cur != node_index) {
				prev = cur;
				cur = executor->nodes[cur].next;
			}
			if (cur == node_index) {
				if (prev != TR_RPC_EXEC_NONE)
					executor->nodes[prev].next =
						TR_RPC_EXEC_NONE;
				else
					callq->head = TR_RPC_EXEC_NONE;
				callq->tail = prev;
			}
			node->next = executor->free_head;
			executor->free_head = node_index;
			if (callq->queued_count != 0)
				callq->queued_count--;
			executor->queued_count--;
			goto out;
		}
		callq->ready = 1;
		if (executor->group) {
			if (!executor->group_enqueued) {
				ret = tr_rpc_executor_group_enqueue(
					executor->group, endpoint);
				if (ret != TR_OK) {
					callq->ready = 0;
					tr_rpc_executor_ready_undo_push_locked(
						executor, task->call.slot);
					{
						uint32_t cur = callq->head;
						uint32_t prev = TR_RPC_EXEC_NONE;

						while (cur != TR_RPC_EXEC_NONE &&
						       cur != node_index) {
							prev = cur;
							cur = executor->nodes[cur].next;
						}
						if (cur == node_index) {
							if (prev != TR_RPC_EXEC_NONE)
								executor->nodes[prev].next =
									TR_RPC_EXEC_NONE;
							else
								callq->head =
									TR_RPC_EXEC_NONE;
							callq->tail = prev;
						}
					}
					node->next = executor->free_head;
					executor->free_head = node_index;
					if (callq->queued_count != 0)
						callq->queued_count--;
					if (executor->queued_count != 0)
						executor->queued_count--;
					goto out;
				}
				executor->group_enqueued = 1;
			}
		} else {
			pthread_cond_signal(&executor->cond);
		}
	}

out:
	pthread_mutex_unlock(&executor->lock);
	return ret;
}

/*
 * endpoint->lock 必须已经持有。
 *
 * Task 在入队前复制 worker 所需的全部 callback/handler/stream capability。
 * 入队之后 worker 不再回读 mutable Call/Endpoint protocol state。
 */
static int tr_rpc_prepare_task_snapshot_locked(
	struct tr_rpc_call_slot *call, struct tr_rpc_task *task)
{
	if (!call || !task)
		return TR_ERR_INVALID;

	task->stream = call->stream;

	switch (task->type) {
	case TR_RPC_TASK_SERVER_UNARY:
		if (!call->method ||
		    call->method->handler_kind != TR_RPC_HANDLER_UNARY)
			return TR_ERR_STATE;
		task->u.server_unary.handler = call->method->unary_handler;
		task->u.server_unary.handler_arg = call->method->handler_arg;
		task->u.server_unary.method = call->method->desc;
		break;

	case TR_RPC_TASK_SERVER_STREAM_MESSAGE:
	case TR_RPC_TASK_SERVER_HALF_CLOSE:
	case TR_RPC_TASK_SERVER_WRITABLE:
	case TR_RPC_TASK_SERVER_CLOSE:
		if (!call->method ||
		    call->method->handler_kind != TR_RPC_HANDLER_STREAM)
			return TR_ERR_STATE;
		task->u.server_stream.handlers = call->method->stream_handlers;
		task->u.server_stream.handler_arg = call->method->handler_arg;
		break;

	case TR_RPC_TASK_CLIENT_UNARY_RESULT:
		task->u.client_unary.result_cb = call->result_cb;
		task->u.client_unary.result_arg = call->result_arg;
		break;

	case TR_RPC_TASK_CLIENT_MESSAGE:
	case TR_RPC_TASK_CLIENT_EVENT:
		task->u.client_stream.callbacks = call->callbacks;
		break;

	default:
		return TR_ERR_INVALID;
	}

	return TR_OK;
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_queue_task_locked(struct tr_rpc_endpoint *endpoint,
				    struct tr_rpc_call_slot *call,
				    const struct tr_rpc_task *task)
{
	struct tr_rpc_task snapshot;
	int ret;

	if (!task)
		return TR_ERR_INVALID;
	snapshot = *task;
	ret = tr_rpc_prepare_task_snapshot_locked(call, &snapshot);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_endpoint_get(endpoint);
	if (ret != TR_OK)
		return ret;

	call->task_refs++;
	ret = tr_rpc_executor_push(endpoint, &snapshot);
	if (ret != TR_OK) {
		call->task_refs--;
		/*
		 * 当前仍持有 endpoint->lock，且 owner reference 仍然存活，
		 * 因此这里的 rollback put 不可能成为最后一次 put。
		 */
		(void)tr_refcount_put(&endpoint->refs);
	}
	return ret;
}

static void tr_rpc_task_done(struct tr_rpc_endpoint *endpoint,
			     struct tr_rpc_call_handle handle)
{
	int last;

	pthread_mutex_lock(&endpoint->lock);
	if (handle.slot < endpoint->config.max_calls) {
		struct tr_rpc_call_slot *call = &endpoint->calls[handle.slot];
		if (call->state != TR_RPC_CALL_FREE &&
		    call->generation == handle.generation) {
			if (call->task_refs != 0)
				call->task_refs--;
			tr_rpc_maybe_free_call_locked(call);
		}
	}

	/*
	 * 在持有 endpoint->lock 时释放 task 持有的 Endpoint 强引用。
	 * destroy 会在同一把锁/条件变量上等待，再释放 owner reference，
	 * 因此这里完成 signal 之前 Endpoint 不可能被提前 free。
	 */
	last = tr_refcount_put(&endpoint->refs);
	if (last == 0 && tr_refcount_read(&endpoint->refs) == 1U)
		pthread_cond_broadcast(&endpoint->ref_cond);
	pthread_mutex_unlock(&endpoint->lock);

	if (last == 1)
		tr_rpc_endpoint_release(endpoint);
}

struct tr_rpc_stream_payload_release {
	struct tr_stream_handle stream;
	struct tr_buffer *payload;
	int executed;
};

static int tr_rpc_stream_release_payload_on_owner(void *arg)
{
	struct tr_rpc_stream_payload_release *request =
		(struct tr_rpc_stream_payload_release *)arg;

	request->executed = 1;
	return tr_stream_release_payload(request->stream, request->payload);
}

/*
 * RX credit belongs to Stream protocol state, so worker threads return payload
 * ownership to the Reactor owner instead of mutating Stream under channel->lock.
 * If the Reactor can no longer accept commands, it cannot consume the payload;
 * release the buffer locally so shutdown cannot leak memory.
 */
static int tr_rpc_release_stream_payload(struct tr_stream_handle stream,
					 struct tr_buffer *payload)
{
	struct tr_rpc_stream_payload_release request;
	struct tr_reactor *reactor;
	int ret;

	if (!payload)
		return TR_ERR_INVALID;
	if (!stream.channel) {
		tr_buffer_release(payload);
		return TR_ERR_STALE;
	}

	reactor = tr_channel_reactor(stream.channel);
	if (!reactor) {
		tr_buffer_release(payload);
		return TR_ERR_STATE;
	}

	request.stream = stream;
	request.payload = payload;
	request.executed = 0;
	ret = tr_reactor_call(reactor, tr_rpc_stream_release_payload_on_owner,
			      &request);
	if (!request.executed)
		tr_buffer_release(payload);
	return ret;
}

struct tr_rpc_stream_close_request {
	struct tr_stream_handle stream;
};

static int tr_rpc_stream_close_on_owner(void *arg)
{
	struct tr_rpc_stream_close_request *request =
		(struct tr_rpc_stream_close_request *)arg;

	return tr_stream_close(request->stream);
}

static int tr_rpc_close_stream(struct tr_stream_handle stream)
{
	struct tr_rpc_stream_close_request request;
	struct tr_reactor *reactor;

	if (!stream.channel)
		return TR_ERR_STALE;
	reactor = tr_channel_reactor(stream.channel);
	if (!reactor)
		return TR_ERR_STATE;

	request.stream = stream;
	return tr_reactor_call(reactor, tr_rpc_stream_close_on_owner, &request);
}

static void tr_rpc_release_task_payload(struct tr_rpc_task *task)
{
	if (!task || !task->payload)
		return;

	(void)tr_rpc_release_stream_payload(task->stream,
					    tr_buffer_take(&task->payload));
}

static int tr_rpc_decode_task_message(struct tr_rpc_task *task,
				      struct tr_rpc_message *message,
				      struct tr_rpc_wire_header *wire)
{
	const uint8_t *metadata = NULL;
	const uint8_t *body = NULL;
	uint16_t metadata_len = 0;
	int ret;

	if (!task->payload || !message || !wire)
		return TR_ERR_INVALID;

	ret = tr_rpc_wire_decode_ex(task->payload->data, task->payload->len,
				    wire, &metadata, &metadata_len, &body);
	if (ret != TR_OK)
		return ret;
	(void)metadata;
	(void)metadata_len;

	if (wire->codec_id != TR_RPC_CODEC_RAW)
		return TR_ERR_BAD_TYPE;

	memset(message, 0, sizeof(*message));
	message->bytes.data = body;
	message->bytes.len = wire->payload_len;
	message->storage = task->payload;
	return TR_OK;
}

static void tr_rpc_finish_task_on_owner(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle call)
{
	tr_rpc_executor_complete_task(endpoint, call);
	tr_rpc_task_done(endpoint, call);
}

static void tr_rpc_apply_task_completion(void *arg)
{
	struct tr_rpc_task_completion *completion =
		(struct tr_rpc_task_completion *)arg;
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;

	if (!completion)
		return;

	endpoint = completion->endpoint;
	call = completion->call;
	free(completion);
	tr_rpc_finish_task_on_owner(endpoint, call);
}

static int tr_rpc_apply_task_completion_sync(void *arg)
{
	struct tr_rpc_task_completion *completion =
		(struct tr_rpc_task_completion *)arg;

	if (!completion || !completion->endpoint)
		return TR_ERR_INVALID;
	tr_rpc_finish_task_on_owner(completion->endpoint, completion->call);
	return TR_OK;
}

static int tr_rpc_defer_task_completion(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle call)
{
	struct tr_rpc_task_completion *completion;
	struct tr_reactor *reactor;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;

	completion =
		(struct tr_rpc_task_completion *)malloc(sizeof(*completion));
	reactor = tr_channel_reactor(endpoint->channel);
	if (!completion) {
		/*
		 * OOM 不能迫使 worker 回退修改 Call。同步 owner-call 使用栈上
		 * request，不需要额外分配，并保持同一生命周期规则。
		 */
		struct tr_rpc_task_completion sync_completion;

		sync_completion.endpoint = endpoint;
		sync_completion.call = call;
		return tr_reactor_call(reactor,
				       tr_rpc_apply_task_completion_sync,
				       &sync_completion);
	}
	completion->endpoint = endpoint;
	completion->call = call;

	ret = tr_reactor_complete(reactor, tr_rpc_apply_task_completion,
			      completion);
	if (ret != TR_OK)
		free(completion);
	return ret;
}

static void tr_rpc_apply_unary_completion(void *arg)
{
	struct tr_rpc_unary_completion *completion =
		(struct tr_rpc_unary_completion *)arg;
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_buffer *response_buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes response;
	int ret = TR_OK;

	if (!completion)
		return;

	endpoint = completion->endpoint;
	response.data = completion->response_len ? completion->response : NULL;
	response.len = completion->response_len;

	/*
	 * 这里由 Reactor owner thread 执行。endpoint->lock 暂时保留用于兼容
	 * 仍可从 application 线程进入的旧 API；后续 ownership
	 * 收敛后再缩减这把锁，而不是在本阶段直接替换成 atomic。
	 */
	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(completion->call);
	if (call && call->method && call->is_unary && !call->cancelled &&
	    !call->pending_tx) {
		ret = tr_rpc_encode_message(
			endpoint, call, TR_RPC_WIRE_RESPONSE,
			&call->method->desc, call->method->desc.response_codec_id,
			completion->status, &response, &response_buffer);
		if (ret == TR_OK) {
			call->pending_tx = tr_buffer_take(&response_buffer);
			(void)tr_rpc_try_unary_send_locked(endpoint, call);
		}
	}
	pthread_mutex_unlock(&endpoint->lock);

	/*
	 * completion 接管了原 task 的 Endpoint 强引用和 task_refs；
	 * 必须在 owner thread 最后归还，避免 Call 在 completion 应用前复用。
	 */
	{
		struct tr_rpc_call_handle call_handle = completion->call;

		free(completion);
		/*
		 * per-call executor serialization 必须覆盖 completion apply；
		 * 只有 owner 已应用本次结果后，才允许同一 Call 的下一项任务运行。
		 */
		tr_rpc_finish_task_on_owner(endpoint, call_handle);
	}
}

static int tr_rpc_prepare_unary_completion(
	struct tr_rpc_endpoint *endpoint, const struct tr_rpc_task *task,
	int status, const struct tr_rpc_bytes *response,
	struct tr_rpc_unary_completion **out)
{
	struct tr_rpc_unary_completion *completion;
	size_t size;

	if (!endpoint || !task || !response || !out ||
	    (response->len != 0 && !response->data))
		return TR_ERR_INVALID;
	*out = NULL;
#if SIZE_MAX <= UINT32_MAX
	if (response->len > (uint32_t)(SIZE_MAX - sizeof(*completion)))
		return TR_ERR_BAD_LENGTH;
#endif

	size = sizeof(*completion) + (size_t)response->len;
	completion = (struct tr_rpc_unary_completion *)malloc(size);
	if (!completion)
		return TR_ERR_NOMEM;

	completion->endpoint = endpoint;
	completion->call = task->call;
	completion->status = status;
	completion->response_len = response->len;
	if (response->len)
		memcpy(completion->response, response->data, response->len);

	*out = completion;
	return TR_OK;
}

static int tr_rpc_submit_unary_completion(
	struct tr_rpc_endpoint *endpoint,
	struct tr_rpc_unary_completion *completion)
{
	struct tr_reactor *reactor;

	if (!endpoint || !completion)
		return TR_ERR_INVALID;

	reactor = tr_channel_reactor(endpoint->channel);
	return tr_reactor_complete(reactor, tr_rpc_apply_unary_completion,
			       completion);
}

static int tr_rpc_executor_run_server_unary(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_task *task)
{
	struct tr_rpc_message message;
	struct tr_rpc_wire_header wire;
	struct tr_rpc_unary_response response;
	struct tr_rpc_bytes completion_response;
	struct tr_rpc_unary_completion *completion = NULL;
	tr_rpc_unary_handler handler = task->u.server_unary.handler;
	int ret;
	int deferred = 0;

	if (!handler || tr_rpc_decode_task_message(task, &message, &wire) != TR_OK) {
		(void)tr_rpc_close_stream(task->stream);
		goto out;
	}

	message.stream = task->stream;
	memset(&response, 0, sizeof(response));
	response.status = TR_RPC_STATUS_INTERNAL;

	ret = handler(task->call, &message.bytes, &response,
		      task->u.server_unary.handler_arg);
	if (ret != TR_OK)
		response.status = TR_RPC_STATUS_INTERNAL;

	if (response.message.len >
		    task->u.server_unary.method.max_response_bytes ||
	    (response.message.len != 0 && !response.message.data)) {
		response.status = TR_RPC_STATUS_INTERNAL;
		response.message.data = NULL;
		response.message.len = 0;
	}

	completion_response = response.message;
	ret = tr_rpc_prepare_unary_completion(
		endpoint, task, response.status, &completion_response,
		&completion);
	if (ret != TR_OK)
		goto out;

out:
	/*
	 * 先结束 worker 对 request payload 的所有访问，再提交会释放 task
	 * strong-ref 的 completion。否则 Reactor 可能先执行 completion，
	 * teardown 随后越过仍在归还 RX buffer 的 worker。
	 */
	tr_rpc_release_task_payload(task);

	if (completion) {
		ret = tr_rpc_submit_unary_completion(endpoint, completion);
		if (ret == TR_OK) {
			deferred = 1;
			completion = NULL;
		} else {
			free(completion);
			completion = NULL;
			(void)tr_rpc_close_stream(task->stream);
		}
	}
	return deferred;
}

static void tr_rpc_executor_mark_cancelled(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor;

	if (!endpoint || handle.slot >= endpoint->config.max_calls)
		return;

	executor = &endpoint->executor;
	pthread_mutex_lock(&executor->lock);
	if (executor->callq[handle.slot].generation == handle.generation)
		executor->callq[handle.slot].cancelled = 1;
	pthread_mutex_unlock(&executor->lock);
}

static int tr_rpc_executor_task_cancelled(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor;
	int cancelled = 0;

	if (!endpoint || handle.slot >= endpoint->config.max_calls)
		return 1;

	executor = &endpoint->executor;
	pthread_mutex_lock(&executor->lock);
	if (executor->callq[handle.slot].generation == handle.generation)
		cancelled = executor->callq[handle.slot].cancelled;
	pthread_mutex_unlock(&executor->lock);
	return cancelled;
}

static int tr_rpc_executor_run_task(struct tr_rpc_endpoint *endpoint,
				    struct tr_rpc_task *task)
{
	if (task->type == TR_RPC_TASK_SERVER_UNARY)
		return tr_rpc_executor_run_server_unary(endpoint, task);

	/*
	 * Cancellation is executor scheduling metadata, not a worker read of
	 * mutable RPC Call state.  Close/result/event notifications must still run.
	 */
	if (task->type != TR_RPC_TASK_SERVER_CLOSE &&
	    task->type != TR_RPC_TASK_CLIENT_UNARY_RESULT &&
	    task->type != TR_RPC_TASK_CLIENT_EVENT &&
	    tr_rpc_executor_task_cancelled(endpoint, task->call)) {
		tr_rpc_release_task_payload(task);
		return 0;
	}

	switch (task->type) {
	case TR_RPC_TASK_SERVER_STREAM_MESSAGE: {
		struct tr_rpc_message message;
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_stream_handlers *handlers =
			&task->u.server_stream.handlers;
		void *handler_arg = task->u.server_stream.handler_arg;
		enum tr_rpc_message_disposition disposition =
			TR_RPC_MESSAGE_RELEASE;
		int ret = TR_OK;

		if (tr_rpc_decode_task_message(task, &message, &wire) !=
		    TR_OK) {
			(void)tr_rpc_call_finish(task->call,
						 TR_RPC_STATUS_INTERNAL);
			break;
		}
		message.stream = task->stream;

		if (task->first_message && handlers->on_open)
			ret = handlers->on_open(task->call, handler_arg);
		if (ret == TR_OK && handlers->on_message)
			disposition = handlers->on_message(task->call, &message,
							    handler_arg);
		if (ret != TR_OK)
			(void)tr_rpc_call_finish(task->call,
						 TR_RPC_STATUS_INTERNAL);

		if (disposition == TR_RPC_MESSAGE_TAKE_OWNERSHIP)
			(void)tr_buffer_take(&task->payload);
		break;
	}

	case TR_RPC_TASK_SERVER_HALF_CLOSE:
		if (task->u.server_stream.handlers.on_half_close)
			task->u.server_stream.handlers.on_half_close(
				task->call, task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_SERVER_WRITABLE:
		if (task->u.server_stream.handlers.on_writable)
			task->u.server_stream.handlers.on_writable(
				task->call, task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_SERVER_CLOSE:
		if (task->u.server_stream.handlers.on_close)
			task->u.server_stream.handlers.on_close(
				task->call, task->status,
				task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_CLIENT_UNARY_RESULT: {
		struct tr_rpc_message message;
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_bytes *bytes = NULL;
		int status = task->status;

		if (task->payload) {
			if (tr_rpc_decode_task_message(task, &message, &wire) ==
			    TR_OK) {
				message.stream = task->stream;
				bytes = &message.bytes;
				status = wire.status;
			} else {
				status = TR_RPC_STATUS_INTERNAL;
			}
		}
		if (task->u.client_unary.result_cb)
			task->u.client_unary.result_cb(
				task->call, status, bytes,
				task->u.client_unary.result_arg);
		break;
	}

	case TR_RPC_TASK_CLIENT_MESSAGE: {
		struct tr_rpc_message message;
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_call_callbacks *callbacks =
			&task->u.client_stream.callbacks;
		enum tr_rpc_message_disposition disposition =
			TR_RPC_MESSAGE_RELEASE;

		if (tr_rpc_decode_task_message(task, &message, &wire) ==
		    TR_OK) {
			message.stream = task->stream;
			if (callbacks->on_message)
				disposition = callbacks->on_message(
					task->call, &message, callbacks->arg);
		}
		if (disposition == TR_RPC_MESSAGE_TAKE_OWNERSHIP)
			(void)tr_buffer_take(&task->payload);
		break;
	}

	case TR_RPC_TASK_CLIENT_EVENT:
		if (task->u.client_stream.callbacks.on_event)
			task->u.client_stream.callbacks.on_event(
				task->call, task->event, task->status,
				task->u.client_stream.callbacks.arg);
		break;

	default:
		break;
	}

	tr_rpc_release_task_payload(task);
	return 0;
}

static int tr_rpc_executor_take(struct tr_rpc_endpoint *endpoint,
				struct tr_rpc_task *task, int wait)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	struct tr_rpc_executor_callq *callq;
	struct tr_rpc_executor_node *node;
	uint32_t slot;
	uint32_t node_index;

	pthread_mutex_lock(&executor->lock);
	if (executor->group && !wait)
		executor->group_enqueued = 0;

	while (wait && executor->ready_count == 0 && !executor->stopping)
		pthread_cond_wait(&executor->cond, &executor->lock);

	if (executor->ready_count == 0) {
		int ret = executor->stopping ? 0 : -1;
		pthread_mutex_unlock(&executor->lock);
		return ret;
	}

	slot = executor->ready_calls[executor->ready_head];
	executor->ready_head =
		(executor->ready_head + 1U) % executor->ready_capacity;
	executor->ready_count--;

	callq = &executor->callq[slot];
	callq->ready = 0;
	node_index = callq->head;
	if (node_index == TR_RPC_EXEC_NONE || callq->running) {
		pthread_mutex_unlock(&executor->lock);
		return -1;
	}

	node = &executor->nodes[node_index];
	*task = node->task;
	callq->head = node->next;
	if (callq->head == TR_RPC_EXEC_NONE)
		callq->tail = TR_RPC_EXEC_NONE;

	node->next = executor->free_head;
	executor->free_head = node_index;
	if (callq->queued_count != 0)
		callq->queued_count--;
	if (executor->queued_count != 0)
		executor->queued_count--;
	callq->running = 1;
	executor->running_count++;

	/*
	 * shared worker 已消费该 Endpoint 的 wake token。
	 * 如果该 Endpoint 上还有其他 ready Call，则在释放 executor lock 之前
	 * 再发布一个 replacement token，让其他 shared worker 可以并行处理。
	 */
	if (executor->group && executor->ready_count != 0 &&
	    !executor->group_enqueued) {
		if (tr_rpc_executor_group_enqueue(executor->group, endpoint) ==
		    TR_OK)
			executor->group_enqueued = 1;
	}

	pthread_mutex_unlock(&executor->lock);
	return 1;
}

static void tr_rpc_executor_complete_task(struct tr_rpc_endpoint *endpoint,
					  struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor = &endpoint->executor;

	pthread_mutex_lock(&executor->lock);
	if (handle.slot < endpoint->config.max_calls) {
		struct tr_rpc_executor_callq *callq =
			&executor->callq[handle.slot];

		if (callq->generation == handle.generation && callq->running) {
			callq->running = 0;
			if (executor->running_count != 0)
				executor->running_count--;

			if (callq->head != TR_RPC_EXEC_NONE && !callq->ready) {
				if (tr_rpc_executor_ready_push_locked(
					    executor, handle.slot) == TR_OK) {
					callq->ready = 1;
					if (executor->group) {
						if (!executor->group_enqueued &&
						    tr_rpc_executor_group_enqueue(
							    executor->group,
							    endpoint) == TR_OK)
							executor->group_enqueued = 1;
					} else {
						pthread_cond_signal(&executor->cond);
					}
				}
			}
		}
	}
	pthread_mutex_unlock(&executor->lock);
}

static void *tr_rpc_executor_main(void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;

	for (;;) {
		struct tr_rpc_task task;
		int ret = tr_rpc_executor_take(endpoint, &task, 1);

		if (ret == 0)
			break;
		if (ret < 0)
			continue;

		{
			int task_done_deferred =
				tr_rpc_executor_run_task(endpoint, &task);
			if (!task_done_deferred &&
			    tr_rpc_defer_task_completion(endpoint, task.call) !=
				    TR_OK) {
				/*
				 * Reactor 已停止、queue 满或 OOM 时必须在 worker
				 * 侧完成 rollback/finalize，不能泄漏 task strong-ref。
				 * 正常运行路径统一由 Reactor owner 完成。
				 */
				tr_rpc_executor_complete_task(endpoint, task.call);
				tr_rpc_task_done(endpoint, task.call);
			}
		}
	}

	return NULL;
}

static struct tr_rpc_endpoint *
tr_rpc_executor_group_take(struct tr_rpc_executor_group *group)
{
	struct tr_rpc_endpoint *endpoint;

	pthread_mutex_lock(&group->lock);
	while (group->count == 0 && !group->stopping)
		pthread_cond_wait(&group->cond, &group->lock);
	if (group->count == 0 && group->stopping) {
		pthread_mutex_unlock(&group->lock);
		return NULL;
	}

	endpoint = group->ready_endpoints[group->head];
	group->ready_endpoints[group->head] = NULL;
	group->head = (group->head + 1U) % group->capacity;
	group->count--;
	pthread_mutex_unlock(&group->lock);
	return endpoint;
}

static void *tr_rpc_executor_group_main(void *arg)
{
	struct tr_rpc_executor_group *group =
		(struct tr_rpc_executor_group *)arg;

	for (;;) {
		struct tr_rpc_endpoint *endpoint =
			tr_rpc_executor_group_take(group);
		struct tr_rpc_task task;
		int ret;

		if (!endpoint)
			break;

		ret = tr_rpc_executor_take(endpoint, &task, 0);
		if (ret <= 0)
			continue;

		{
			int task_done_deferred =
				tr_rpc_executor_run_task(endpoint, &task);
			if (!task_done_deferred &&
			    tr_rpc_defer_task_completion(endpoint, task.call) !=
				    TR_OK) {
				/*
				 * Reactor 已停止、queue 满或 OOM 时必须在 worker
				 * 侧完成 rollback/finalize，不能泄漏 task strong-ref。
				 * 正常运行路径统一由 Reactor owner 完成。
				 */
				tr_rpc_executor_complete_task(endpoint, task.call);
				tr_rpc_task_done(endpoint, task.call);
			}
		}
	}

	return NULL;
}

static void tr_rpc_executor_cleanup(struct tr_rpc_executor *executor)
{
	if (!executor)
		return;

	free(executor->ready_calls);
	executor->ready_calls = NULL;
	free(executor->callq);
	executor->callq = NULL;
	free(executor->nodes);
	executor->nodes = NULL;
	free(executor->threads);
	executor->threads = NULL;
}

static void tr_rpc_executor_destroy(struct tr_rpc_endpoint *endpoint);

struct tr_rpc_executor_init_guard {
	struct tr_rpc_endpoint *endpoint;
	int armed;
};

static void
tr_rpc_executor_init_guard_cleanup(struct tr_rpc_executor_init_guard *guard)
{
	if (guard && guard->armed && guard->endpoint)
		tr_rpc_executor_destroy(guard->endpoint);
}

static int tr_rpc_executor_init(struct tr_rpc_endpoint *endpoint,
				uint32_t capacity,
				struct tr_rpc_executor_group *group)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	struct tr_rpc_executor_init_guard guard
		TR_AUTO(tr_rpc_executor_init_guard_cleanup) = { endpoint, 0 };
	uint32_t thread_count;
	uint32_t i;

	memset(executor, 0, sizeof(*executor));
	if (capacity == 0)
		capacity = endpoint->config.max_calls * 4U + 16U;
	if (capacity < 16U)
		capacity = 16U;

	thread_count = endpoint->config.executor_threads;
	if (thread_count == 0)
		thread_count = endpoint->config.max_calls < 4U ?
				       endpoint->config.max_calls :
				       4U;
	if (thread_count == 0)
		thread_count = 1U;
	if (thread_count > endpoint->config.max_calls)
		thread_count = endpoint->config.max_calls;

	if (pthread_mutex_init(&executor->lock, NULL) != 0)
		return TR_ERR_INVALID;
	if (pthread_cond_init(&executor->cond, NULL) != 0) {
		pthread_mutex_destroy(&executor->lock);
		return TR_ERR_INVALID;
	}
	guard.armed = 1;

	executor->group = group;
	if (!group)
		executor->threads =
			(pthread_t *)calloc(thread_count, sizeof(*executor->threads));
	executor->nodes = (struct tr_rpc_executor_node *)calloc(
		capacity, sizeof(*executor->nodes));
	executor->callq = (struct tr_rpc_executor_callq *)calloc(
		endpoint->config.max_calls, sizeof(*executor->callq));
	executor->ready_calls = (uint32_t *)calloc(
		endpoint->config.max_calls, sizeof(*executor->ready_calls));
	if ((!group && !executor->threads) || !executor->nodes ||
	    !executor->callq || !executor->ready_calls)
		return TR_ERR_NOMEM;

	executor->capacity = capacity;
	executor->thread_count = group ? group->thread_count : thread_count;
	executor->ready_capacity = endpoint->config.max_calls;
	executor->free_head = capacity ? 0U : TR_RPC_EXEC_NONE;

	for (i = 0; i < capacity; ++i)
		executor->nodes[i].next =
			(i + 1U < capacity) ? i + 1U : TR_RPC_EXEC_NONE;
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		executor->callq[i].head = TR_RPC_EXEC_NONE;
		executor->callq[i].tail = TR_RPC_EXEC_NONE;
	}

	if (!group) {
		for (i = 0; i < thread_count; ++i) {
			if (pthread_create(&executor->threads[i], NULL,
					   tr_rpc_executor_main, endpoint) != 0)
				return TR_ERR_SYS;
			executor->started_threads++;
		}
	}

	guard.armed = 0;
	return TR_OK;
}

static void tr_rpc_executor_shutdown(struct tr_rpc_endpoint *endpoint)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	uint32_t i;

	pthread_mutex_lock(&executor->lock);
	if (!executor->stopping) {
		executor->stopping = 1;
		pthread_cond_broadcast(&executor->cond);
	}
	pthread_mutex_unlock(&executor->lock);

	/*
	 * standalone Endpoint 自己拥有 worker，因此 owner 最后一次 put 之前
	 * 必须先 join 全部 worker。
	 * Server 的 shared worker 由 executor group 拥有，只需要异步排空
	 * 已经存在的 task reference。
	 */
	if (!executor->group) {
		for (i = 0; i < executor->started_threads; ++i)
			(void)pthread_join(executor->threads[i], NULL);
		executor->started_threads = 0;
	}
}

static void tr_rpc_executor_release(struct tr_rpc_endpoint *endpoint)
{
	struct tr_rpc_executor *executor = &endpoint->executor;

	tr_rpc_executor_cleanup(executor);
	pthread_cond_destroy(&executor->cond);
	pthread_mutex_destroy(&executor->lock);
}

static void tr_rpc_executor_destroy(struct tr_rpc_endpoint *endpoint)
{
	tr_rpc_executor_shutdown(endpoint);
	tr_rpc_executor_release(endpoint);
}

int tr_rpc_executor_group_create(uint32_t endpoint_capacity,
				 uint32_t max_calls_per_endpoint,
				 uint32_t thread_count,
				 struct tr_rpc_executor_group **out)
{
	struct tr_rpc_executor_group *group_mem
		TR_AUTO(tr_rpc_group_mem_cleanup) = NULL;
	struct tr_rpc_executor_group *group
		TR_AUTO(tr_rpc_group_owner_cleanup) = NULL;
	pthread_t *threads TR_AUTO(tr_rpc_thread_array_cleanup) = NULL;
	struct tr_rpc_endpoint **ready_endpoints
		TR_AUTO(tr_rpc_endpoint_array_cleanup) = NULL;
	uint64_t total_calls;
	uint32_t i;

	if (!out || endpoint_capacity == 0 || max_calls_per_endpoint == 0 ||
	    endpoint_capacity == UINT32_MAX)
		return TR_ERR_INVALID;
	*out = NULL;

	total_calls = (uint64_t)endpoint_capacity * max_calls_per_endpoint;
	if (total_calls == 0)
		return TR_ERR_INVALID;

	if (thread_count == 0)
		thread_count = total_calls < 4U ? (uint32_t)total_calls : 4U;
	if ((uint64_t)thread_count > total_calls)
		thread_count = (uint32_t)total_calls;
	if (thread_count == 0)
		thread_count = 1U;

	group_mem =
		(struct tr_rpc_executor_group *)calloc(1, sizeof(*group_mem));
	if (!group_mem)
		return TR_ERR_NOMEM;

	threads = (pthread_t *)calloc(thread_count, sizeof(*threads));
	/*
	 * Server reaper 会先从 peer table 移除旧 peer，再销毁旧 Endpoint。
	 * 因此 retiring Endpoint 可能和 max_peers 个 live Endpoint 短暂重叠。
	 * ready queue 额外保留一个 slot，用来容纳这个有界重叠窗口。
	 */
	ready_endpoints = (struct tr_rpc_endpoint **)calloc(
		endpoint_capacity + 1U, sizeof(*ready_endpoints));
	if (!threads || !ready_endpoints)
		return TR_ERR_NOMEM;

	if (pthread_mutex_init(&group_mem->lock, NULL) != 0)
		return TR_ERR_INVALID;
	if (pthread_cond_init(&group_mem->cond, NULL) != 0) {
		pthread_mutex_destroy(&group_mem->lock);
		return TR_ERR_INVALID;
	}

	group_mem->threads = tr_rpc_thread_array_take(&threads);
	group_mem->ready_endpoints =
		tr_rpc_endpoint_array_take(&ready_endpoints);
	group_mem->capacity = endpoint_capacity + 1U;
	group_mem->thread_count = thread_count;
	group = tr_rpc_group_mem_take(&group_mem);

	for (i = 0; i < thread_count; ++i) {
		if (pthread_create(&group->threads[i], NULL,
				   tr_rpc_executor_group_main, group) != 0)
			return TR_ERR_SYS;
		group->started_threads++;
	}

	*out = tr_rpc_group_owner_take(&group);
	return TR_OK;
}

void tr_rpc_executor_group_destroy(struct tr_rpc_executor_group *group)
{
	uint32_t i;

	if (!group)
		return;

	pthread_mutex_lock(&group->lock);
	group->stopping = 1;
	pthread_cond_broadcast(&group->cond);
	pthread_mutex_unlock(&group->lock);

	for (i = 0; i < group->started_threads; ++i)
		(void)pthread_join(group->threads[i], NULL);

	free(group->ready_endpoints);
	free(group->threads);
	pthread_cond_destroy(&group->cond);
	pthread_mutex_destroy(&group->lock);
	free(group);
}

static int tr_rpc_queue_client_event_locked(struct tr_rpc_endpoint *endpoint,
					    uint32_t slot,
					    struct tr_rpc_call_slot *call,
					    enum tr_rpc_call_event event,
					    int status)
{
	struct tr_rpc_task task;

	memset(&task, 0, sizeof(task));
	task.type = TR_RPC_TASK_CLIENT_EVENT;
	task.call = tr_rpc_make_call_handle(endpoint, slot, call);
	task.event = event;
	task.status = status;
	return tr_rpc_queue_task_locked(endpoint, call, &task);
}

static int tr_rpc_queue_unary_failure_locked(struct tr_rpc_endpoint *endpoint,
					     uint32_t slot,
					     struct tr_rpc_call_slot *call,
					     int status)
{
	struct tr_rpc_task task;

	if (call->result_delivered)
		return TR_OK;

	memset(&task, 0, sizeof(task));
	task.type = TR_RPC_TASK_CLIENT_UNARY_RESULT;
	task.call = tr_rpc_make_call_handle(endpoint, slot, call);
	task.status = status;
	call->result_delivered = 1;
	return tr_rpc_queue_task_locked(endpoint, call, &task);
}

static int tr_rpc_notify_terminal_locked(struct tr_rpc_endpoint *endpoint,
					 uint32_t slot,
					 struct tr_rpc_call_slot *call,
					 int status)
{
	struct tr_rpc_task task;
	int ret = TR_OK;

	if (call->terminal_notified)
		return TR_OK;

	if (endpoint->config.role == TR_RPC_CLIENT) {
		if (call->is_unary)
			ret = tr_rpc_queue_unary_failure_locked(endpoint, slot,
								call, status);
		else
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_FINISHED, status);
	} else if (!call->is_unary && call->method &&
		   call->method->handler_kind == TR_RPC_HANDLER_STREAM) {
		memset(&task, 0, sizeof(task));
		task.type = TR_RPC_TASK_SERVER_CLOSE;
		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		task.status = status;
		ret = tr_rpc_queue_task_locked(endpoint, call, &task);
	}

	if (ret == TR_OK) {
		call->terminal_notified = 1;
		endpoint->stat_calls_completed++;
	}
	return ret;
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_try_cancel_send_locked(struct tr_rpc_endpoint *endpoint,
					 struct tr_rpc_call_slot *call)
{
	int ret = TR_OK;

	(void)endpoint;
	if (call->pending_control) {
		ret = tr_stream_send(call->stream, call->pending_control);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&call->pending_control);
			call->need_local_close = 1;
		} else if (ret != TR_AGAIN) {
			tr_buffer_release(tr_buffer_take(&call->pending_control));
			call->need_local_close = 1;
		} else {
			return ret;
		}
	}

	if (call->need_local_close && !call->local_closed) {
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			call->need_local_close = 0;
			call->local_closed = 1;
			ret = TR_OK;
		}
	}
	return ret;
}

struct tr_rpc_cancel_request {
	struct tr_rpc_call_handle handle;
	int status;
};

static int tr_rpc_cancel_on_owner(void *arg)
{
	struct tr_rpc_cancel_request *request =
		(struct tr_rpc_cancel_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	int status = request->status;
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_buffer *control TR_AUTO(tr_buffer_cleanup) = NULL;
	int peer_visible;
	int ret = TR_OK;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_STALE;
	}
	if (call->cancelled) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_OK;
	}
	if (call->state == TR_RPC_CALL_TERMINAL) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_CLOSED;
	}

	call->cancelled = 1;
	call->cancel_status = status;
	tr_rpc_executor_mark_cancelled(endpoint, handle);
	if (status == TR_RPC_STATUS_DEADLINE_EXCEEDED)
		endpoint->stat_calls_deadline_exceeded++;
	else
		endpoint->stat_calls_cancelled++;
	call->deadline_ns = 0;
	call->final_status_seen = 1;
	call->final_status = status;
	call->state = TR_RPC_CALL_TERMINAL;
	(void)tr_rpc_notify_terminal_locked(endpoint, handle.slot, call,
					    status);

	/*
	 * Client peer 在第一条 REQUEST 真正发送前并不知道该 streaming Call。
	 * Server Call 按定义一定来自已经收到的 REQUEST，因此天然对 peer 可见。
	 */
	peer_visible = endpoint->config.role == TR_RPC_SERVER ||
		       call->tx_count != 0;
	if (peer_visible && !call->local_closed && call->method) {
		ret = tr_rpc_encode_control_locked(
			endpoint, call, TR_RPC_WIRE_CANCEL, status, &control);
		if (ret == TR_OK)
			call->pending_control = tr_buffer_take(&control);
	}

	call->need_local_close = 1;
	(void)tr_rpc_try_cancel_send_locked(endpoint, call);
	tr_rpc_maybe_free_call_locked(call);
	tr_rpc_deadline_changed_locked(endpoint);
	pthread_mutex_unlock(&endpoint->lock);

	return TR_OK;
}

static int tr_rpc_cancel_internal(struct tr_rpc_call_handle handle, int status)
{
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_cancel_request request;

	if (!endpoint || (status != TR_RPC_STATUS_CANCELLED &&
			  status != TR_RPC_STATUS_DEADLINE_EXCEEDED))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status = status;
	return tr_rpc_owner_call(endpoint, tr_rpc_cancel_on_owner, &request);
}
static uint64_t
tr_rpc_deadline_earliest_locked(const struct tr_rpc_endpoint *endpoint,
				uint64_t now_ns, uint32_t *expired_slot)
{
	uint64_t earliest = 0;
	uint32_t i;

	if (expired_slot)
		*expired_slot = UINT32_MAX;

	for (i = 0; i < endpoint->config.max_calls; ++i) {
		const struct tr_rpc_call_slot *call = &endpoint->calls[i];

		if (call->state == TR_RPC_CALL_FREE ||
		    call->state == TR_RPC_CALL_TERMINAL ||
		    call->deadline_ns == 0)
			continue;

		if (now_ns != 0 && call->deadline_ns <= now_ns) {
			if (expired_slot)
				*expired_slot = i;
			return call->deadline_ns;
		}
		if (earliest == 0 || call->deadline_ns < earliest)
			earliest = call->deadline_ns;
	}

	return earliest;
}

/*
 * endpoint->lock 必须已经持有，并且调用方必须处于 Endpoint 所属 Reactor
 * owner thread。Call deadline 的所有正常变更路径已经 owner 化，因此这里可
 * 直接更新 Reactor-local timer，不再经过 shared maintenance/thread。
 */
static void tr_rpc_deadline_changed_locked(struct tr_rpc_endpoint *endpoint)
{
	uint64_t earliest;

	if (endpoint->deadline_stopping ||
	    !endpoint->deadline_timer_registered)
		return;

	earliest = tr_rpc_deadline_earliest_locked(endpoint, 0, NULL);
	(void)tr_reactor_timer_arm(endpoint->deadline_timer, earliest);
}

static uint64_t tr_rpc_deadline_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	uint32_t expired_slot = UINT32_MAX;
	uint64_t earliest;
	struct tr_rpc_call_handle handle;
	struct tr_rpc_cancel_request request;

	if (now_ns == 0)
		now_ns = tr_rpc_now_ns();

	pthread_mutex_lock(&endpoint->lock);
	if (endpoint->deadline_stopping) {
		pthread_mutex_unlock(&endpoint->lock);
		return 0;
	}

	earliest = tr_rpc_deadline_earliest_locked(endpoint, now_ns,
						   &expired_slot);
	if (expired_slot == UINT32_MAX) {
		pthread_mutex_unlock(&endpoint->lock);
		return earliest;
	}

	{
		struct tr_rpc_call_slot *call = &endpoint->calls[expired_slot];

		handle = tr_rpc_make_call_handle(endpoint, expired_slot, call);
		/*
		 * 先从 deadline scan 中移除，避免 cancellation 失败时同一
		 * owner turn 反复命中。cancel_on_owner() 会重新计算下一条。
		 */
		call->deadline_ns = 0;
	}
	pthread_mutex_unlock(&endpoint->lock);

	/*
	 * Timer callback 本身就在 Reactor owner thread，直接调用 owner
	 * implementation，避免再走同步 owner-call。cancel_on_owner() 内部的
	 * deadline_changed_locked() 会显式 re-arm；timer queue 的 version
	 * 规则保证该显式 arm 不会被本 callback 的返回值覆盖。
	 */
	request.handle = handle;
	request.status = TR_RPC_STATUS_DEADLINE_EXCEEDED;
	(void)tr_rpc_cancel_on_owner(&request);
	return 0;
}

static int tr_rpc_deadline_init(struct tr_rpc_endpoint *endpoint)
{
	struct tr_reactor *reactor;
	int ret;

	reactor = tr_channel_reactor(endpoint->channel);
	if (!reactor)
		return TR_ERR_STATE;

	endpoint->deadline_stopping = 0;
	ret = tr_reactor_timer_register(reactor, tr_rpc_deadline_timer_main,
					 endpoint, &endpoint->deadline_timer);
	if (ret != TR_OK)
		return ret;
	endpoint->deadline_timer_registered = 1;
	return TR_OK;
}

static void tr_rpc_deadline_destroy(struct tr_rpc_endpoint *endpoint)
{
	struct tr_reactor_timer_handle timer;

	if (!endpoint->deadline_timer_registered)
		return;

	pthread_mutex_lock(&endpoint->lock);
	endpoint->deadline_stopping = 1;
	timer = endpoint->deadline_timer;
	endpoint->deadline_timer_registered = 0;
	memset(&endpoint->deadline_timer, 0, sizeof(endpoint->deadline_timer));
	pthread_mutex_unlock(&endpoint->lock);

	/*
	 * unregister 是 owner-serialized 的同步 barrier：返回后 timer callback
	 * 不会再取得 Endpoint，从而允许后续 executor/ref teardown 安全释放。
	 */
	(void)tr_reactor_timer_unregister(timer);
}

static enum tr_stream_data_disposition
tr_rpc_on_data(struct tr_stream_handle stream, uint64_t message_id,
	       struct tr_buffer *payload, void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	struct tr_rpc_wire_header wire;
	const uint8_t *metadata = NULL;
	const uint8_t *body = NULL;
	uint16_t metadata_len = 0;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_task task;
	uint32_t slot = 0;
	int first_message = 0;
	int ret;

	(void)message_id;
	if (!payload)
		return TR_STREAM_DATA_RELEASE;

	ret = tr_rpc_wire_decode_ex(payload->data, payload->len, &wire,
				    &metadata, &metadata_len, &body);
	if (ret != TR_OK) {
		(void)tr_stream_close(stream);
		return TR_STREAM_DATA_RELEASE;
	}
	(void)body;

	memset(&task, 0, sizeof(task));
	task.payload = payload;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_find_call_by_stream_locked(endpoint, stream, &slot);

	if (wire.type == TR_RPC_WIRE_REQUEST) {
		enum tr_rpc_cardinality cardinality;

		if (endpoint->config.role != TR_RPC_SERVER ||
		    wire.status != 0) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (!call) {
			method = tr_rpc_find_method_locked(
				endpoint, wire.service_id, wire.method_id);
			if (!method ||
			    method->handler_kind == TR_RPC_HANDLER_NONE ||
			    tr_rpc_allocate_call_locked(endpoint, &call,
							&slot) != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}

			call->state = TR_RPC_CALL_ACTIVE;
			call->stream = stream;
			call->method = method;
			call->is_unary = method->handler_kind ==
					 TR_RPC_HANDLER_UNARY;
			first_message = 1;
		} else {
			method = call->method;
		}

		if (!method || call->cancelled ||
		    wire.service_id != method->desc.service_id ||
		    wire.method_id != method->desc.method_id ||
		    wire.codec_id != method->desc.request_codec_id ||
		    wire.codec_id != TR_RPC_CODEC_RAW ||
		    wire.payload_len > method->desc.max_request_bytes ||
		    (!first_message && metadata_len != 0)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (first_message) {
			ret = tr_rpc_import_peer_metadata_locked(endpoint, call,
								 wire.type,
								 metadata,
								 metadata_len);
			if (ret != TR_OK) {
				tr_rpc_free_call_locked(call);
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		cardinality = method->desc.request_cardinality;
		if (tr_rpc_validate_cardinality(cardinality, call->rx_count) !=
		    TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		call->rx_count++;

		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		task.first_message = (uint16_t)first_message;
		task.type = call->is_unary ? TR_RPC_TASK_SERVER_UNARY :
					     TR_RPC_TASK_SERVER_STREAM_MESSAGE;
		ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK) {
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}

	if (wire.type == TR_RPC_WIRE_RESPONSE) {
		enum tr_rpc_cardinality cardinality;
		int first_response;

		if (endpoint->config.role != TR_RPC_CLIENT || !call ||
		    !call->method || call->cancelled) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		method = call->method;
		first_response = call->rx_count == 0;
		if (wire.service_id != method->desc.service_id ||
		    wire.method_id != method->desc.method_id ||
		    wire.codec_id != method->desc.response_codec_id ||
		    wire.codec_id != TR_RPC_CODEC_RAW ||
		    wire.payload_len > method->desc.max_response_bytes ||
		    (!first_response && metadata_len != 0)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (first_response) {
			ret = tr_rpc_import_peer_metadata_locked(endpoint, call,
								 wire.type,
								 metadata,
								 metadata_len);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		cardinality = method->desc.response_cardinality;
		if (tr_rpc_validate_cardinality(cardinality, call->rx_count) !=
		    TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		call->rx_count++;

		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		if (call->is_unary) {
			if (call->response_received || call->result_delivered) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			call->response_received = 1;
			call->result_delivered = 1;
			call->deadline_ns = 0;
			task.type = TR_RPC_TASK_CLIENT_UNARY_RESULT;
		} else {
			if (wire.status != TR_RPC_STATUS_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			task.type = TR_RPC_TASK_CLIENT_MESSAGE;
		}

		ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK) {
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}

	if (wire.type == TR_RPC_WIRE_STATUS) {
		int first_response;

		if (endpoint->config.role != TR_RPC_CLIENT || !call ||
		    call->is_unary || wire.payload_len != 0 ||
		    call->final_status_seen) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		first_response = call->rx_count == 0;
		if (!first_response && metadata_len != 0) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		if (first_response) {
			ret = tr_rpc_import_peer_metadata_locked(endpoint, call,
								 wire.type,
								 metadata,
								 metadata_len);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		call->final_status_seen = 1;
		call->final_status = wire.status;
		call->deadline_ns = 0;
		ret = tr_rpc_queue_client_event_locked(
			endpoint, slot, call, TR_RPC_CALL_EVENT_FINISHED,
			wire.status);
		if (ret == TR_OK)
			call->terminal_notified = 1;
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK)
			(void)tr_stream_close(stream);
		return TR_STREAM_DATA_RELEASE;
	}

	if (wire.type == TR_RPC_WIRE_CANCEL) {
		int cancel_status = wire.status;

		if (!call || !call->method || metadata_len != 0 ||
		    wire.payload_len != 0 ||
		    wire.service_id != call->method->desc.service_id ||
		    wire.method_id != call->method->desc.method_id ||
		    (cancel_status != TR_RPC_STATUS_CANCELLED &&
		     cancel_status != TR_RPC_STATUS_DEADLINE_EXCEEDED)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (!call->cancelled) {
			call->cancelled = 1;
			call->cancel_status = cancel_status;
			tr_rpc_executor_mark_cancelled(
				endpoint,
				tr_rpc_make_call_handle(endpoint, slot, call));
			call->deadline_ns = 0;
			call->final_status_seen = 1;
			call->final_status = cancel_status;
			call->state = TR_RPC_CALL_TERMINAL;
			(void)tr_rpc_notify_terminal_locked(
				endpoint, slot, call, cancel_status);
		}
		call->need_local_close = 1;
		(void)tr_rpc_try_cancel_send_locked(endpoint, call);
		tr_rpc_maybe_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return TR_STREAM_DATA_RELEASE;
	}

	pthread_mutex_unlock(&endpoint->lock);
	(void)tr_stream_close(stream);
	return TR_STREAM_DATA_RELEASE;
}

static void tr_rpc_on_stream_event(struct tr_stream_handle stream,
				   enum tr_stream_event event, int status,
				   void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_task task;
	uint32_t slot = 0;
	int ret = TR_OK;

	memset(&task, 0, sizeof(task));
	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_find_call_by_stream_locked(endpoint, stream, &slot);

	if (!call) {
		pthread_mutex_unlock(&endpoint->lock);
		return;
	}

	if (event == TR_STREAM_EVENT_OPENED) {
		if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
			(void)tr_rpc_try_cancel_send_locked(endpoint, call);
			tr_rpc_maybe_free_call_locked(call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		call->state = TR_RPC_CALL_ACTIVE;
		if (call->is_unary && endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call, TR_RPC_CALL_EVENT_OPENED,
				TR_RPC_STATUS_OK);
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_WRITABLE) {
		if (call->cancelled || call->pending_control) {
			ret = tr_rpc_try_cancel_send_locked(endpoint, call);
			tr_rpc_maybe_free_call_locked(call);
		} else if (call->is_unary)
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_WRITABLE, TR_RPC_STATUS_OK);
		else if (call->method &&
			 call->method->handler_kind == TR_RPC_HANDLER_STREAM) {
			task.type = TR_RPC_TASK_SERVER_WRITABLE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		}
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		call->remote_closed = 1;
		if (call->cancelled) {
			tr_rpc_maybe_free_call_locked(call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		if (!call->is_unary && endpoint->config.role == TR_RPC_SERVER) {
			task.type = TR_RPC_TASK_SERVER_HALF_CLOSE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_CLIENT) {
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_REMOTE_CLOSED,
				TR_RPC_STATUS_OK);
		}
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_ERROR || event == TR_STREAM_EVENT_CLOSED) {
		int terminal_status = status == TR_OK ?
					      TR_RPC_STATUS_OK :
					      TR_RPC_STATUS_UNAVAILABLE;

		call->deadline_ns = 0;
		if (call->cancelled) {
			if (call->pending_control) {
				tr_buffer_release(call->pending_control);
				call->pending_control = NULL;
			}
			call->need_local_close = 0;
			call->local_closed = 1;
			call->state = TR_RPC_CALL_TERMINAL;
			tr_rpc_maybe_free_call_locked(call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}

		if (call->is_unary && endpoint->config.role == TR_RPC_CLIENT &&
		    !call->result_delivered) {
			if (tr_rpc_queue_unary_failure_locked(
				    endpoint, slot, call,
				    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
				call->terminal_notified = 1;
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_CLIENT &&
			   !call->final_status_seen) {
			if (tr_rpc_queue_client_event_locked(
				    endpoint, slot, call,
				    TR_RPC_CALL_EVENT_ERROR,
				    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
				call->terminal_notified = 1;
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_SERVER &&
			   call->method &&
			   call->method->handler_kind ==
				   TR_RPC_HANDLER_STREAM &&
			   !call->terminal_notified) {
			task.type = TR_RPC_TASK_SERVER_CLOSE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			task.status = terminal_status;
			if (tr_rpc_queue_task_locked(endpoint, call, &task) ==
			    TR_OK)
				call->terminal_notified = 1;
		}

		call->state = TR_RPC_CALL_TERMINAL;
		tr_rpc_maybe_free_call_locked(call);
		tr_rpc_deadline_changed_locked(endpoint);
		pthread_mutex_unlock(&endpoint->lock);
		return;
	}

	pthread_mutex_unlock(&endpoint->lock);
}

static void tr_rpc_on_channel_event(struct tr_channel *channel,
				    enum tr_channel_event event, int status,
				    void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	enum tr_lane failed_lane;
	uint32_t i;

	(void)channel;
	(void)status;
	if (event != TR_CHANNEL_EVENT_CONTROL_DOWN &&
	    event != TR_CHANNEL_EVENT_BULK_DOWN)
		return;
	failed_lane = event == TR_CHANNEL_EVENT_BULK_DOWN ? TR_LANE_BULK :
							    TR_LANE_CONTROL;

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		struct tr_rpc_task task;

		if (call->state == TR_RPC_CALL_FREE || !call->method ||
		    call->method->desc.lane != failed_lane)
			continue;

		call->deadline_ns = 0;
		call->state = TR_RPC_CALL_TERMINAL;
		call->need_local_close = 0;
		call->local_closed = 1;
		if (call->pending_control) {
			tr_buffer_release(call->pending_control);
			call->pending_control = NULL;
		}

		if (endpoint->config.role == TR_RPC_CLIENT) {
			if (call->is_unary) {
				if (tr_rpc_queue_unary_failure_locked(
					    endpoint, i, call,
					    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
					call->terminal_notified = 1;
			} else if (!call->terminal_notified) {
				if (tr_rpc_queue_client_event_locked(
					    endpoint, i, call,
					    TR_RPC_CALL_EVENT_ERROR,
					    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
					call->terminal_notified = 1;
			}
		} else if (!call->is_unary &&
			   call->method->handler_kind ==
				   TR_RPC_HANDLER_STREAM &&
			   !call->terminal_notified) {
			memset(&task, 0, sizeof(task));
			task.type = TR_RPC_TASK_SERVER_CLOSE;
			task.call = tr_rpc_make_call_handle(endpoint, i, call);
			task.status = TR_RPC_STATUS_UNAVAILABLE;
			if (tr_rpc_queue_task_locked(endpoint, call, &task) ==
			    TR_OK)
				call->terminal_notified = 1;
		}

		tr_rpc_maybe_free_call_locked(call);
	}
	tr_rpc_deadline_changed_locked(endpoint);
	pthread_mutex_unlock(&endpoint->lock);
}

struct tr_rpc_endpoint_build {
	struct tr_rpc_endpoint *endpoint;
	int lock_ready;
	int ref_cond_ready;
	int deadline_ready;
	int executor_ready;
	int handler_installed;
};

static void tr_rpc_endpoint_build_cleanup(struct tr_rpc_endpoint_build *build)
{
	struct tr_rpc_endpoint *endpoint;

	if (!build || !build->endpoint)
		return;
	endpoint = build->endpoint;

	if (build->handler_installed) {
		(void)tr_channel_set_handler(endpoint->channel, NULL, NULL,
					     NULL, NULL);
		(void)tr_channel_quiesce(endpoint->channel);
	}
	if (build->deadline_ready)
		tr_rpc_deadline_destroy(endpoint);
	if (build->executor_ready)
		tr_rpc_executor_destroy(endpoint);

	free(endpoint->calls);
	free(endpoint->methods);
	if (build->ref_cond_ready)
		pthread_cond_destroy(&endpoint->ref_cond);
	if (build->lock_ready)
		pthread_mutex_destroy(&endpoint->lock);
	free(endpoint);
	build->endpoint = NULL;
}

int tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel, const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group, struct tr_rpc_endpoint **out)
{
	struct tr_rpc_endpoint_build build
		TR_AUTO(tr_rpc_endpoint_build_cleanup) = { 0 };
	struct tr_rpc_endpoint *endpoint;
	int ret;

	if (!channel || !config || !out || !config->message_pool ||
	    (config->role != TR_RPC_CLIENT && config->role != TR_RPC_SERVER) ||
	    config->max_methods == 0 || config->max_calls == 0)
		return TR_ERR_INVALID;

	*out = NULL;
	endpoint = (struct tr_rpc_endpoint *)calloc(1, sizeof(*endpoint));
	if (!endpoint)
		return TR_ERR_NOMEM;
	build.endpoint = endpoint;

	if (pthread_mutex_init(&endpoint->lock, NULL) != 0)
		return TR_ERR_INVALID;
	build.lock_ready = 1;
	if (pthread_cond_init(&endpoint->ref_cond, NULL) != 0)
		return TR_ERR_INVALID;
	build.ref_cond_ready = 1;
	if (tr_refcount_init(&endpoint->refs, 1U) != TR_OK)
		return TR_ERR_STATE;

	endpoint->methods = (struct tr_rpc_method_entry *)calloc(
		config->max_methods, sizeof(*endpoint->methods));
	endpoint->calls = (struct tr_rpc_call_slot *)calloc(
		config->max_calls, sizeof(*endpoint->calls));
	if (!endpoint->methods || !endpoint->calls)
		return TR_ERR_NOMEM;

	endpoint->channel = channel;
	endpoint->config = *config;

	ret = tr_rpc_deadline_init(endpoint);
	if (ret != TR_OK)
		return ret;
	build.deadline_ready = 1;

	ret = tr_rpc_executor_init(endpoint, config->executor_queue_capacity,
				   group);
	if (ret != TR_OK)
		return ret;
	build.executor_ready = 1;

	ret = tr_channel_set_handler(channel, tr_rpc_on_data,
				     tr_rpc_on_stream_event,
				     tr_rpc_on_channel_event, endpoint);
	if (ret != TR_OK)
		return ret;
	build.handler_installed = 1;

	*out = endpoint;
	build.endpoint = NULL;
	return TR_OK;
}

int tr_rpc_endpoint_create(struct tr_channel *channel,
			   const struct tr_rpc_endpoint_config *config,
			   struct tr_rpc_endpoint **out)
{
	return tr_rpc_endpoint_create_with_executor_group(
		channel, config, NULL, out);
}

static int tr_rpc_endpoint_get(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	return tr_refcount_get(&endpoint->refs);
}

static void tr_rpc_endpoint_wait_owner_only(struct tr_rpc_endpoint *endpoint)
{
	pthread_mutex_lock(&endpoint->lock);
	while (tr_refcount_read(&endpoint->refs) != 1U)
		pthread_cond_wait(&endpoint->ref_cond, &endpoint->lock);
	pthread_mutex_unlock(&endpoint->lock);
}

static void tr_rpc_endpoint_release(struct tr_rpc_endpoint *endpoint)
{
	uint32_t i;

	if (!endpoint)
		return;

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i)
		tr_rpc_free_call_locked(&endpoint->calls[i]);
	pthread_mutex_unlock(&endpoint->lock);

	tr_rpc_executor_release(endpoint);
	free(endpoint->calls);
	free(endpoint->methods);
	pthread_cond_destroy(&endpoint->ref_cond);
	pthread_mutex_destroy(&endpoint->lock);
	free(endpoint);
}

static void tr_rpc_endpoint_put(struct tr_rpc_endpoint *endpoint)
{
	int last;

	if (!endpoint)
		return;

	last = tr_refcount_put(&endpoint->refs);
	if (last == 1)
		tr_rpc_endpoint_release(endpoint);
}

void tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return;

	/*
	 * 先关闭所有可能产生新 Endpoint user 的来源，再等待已有 task reference
	 * 全部排空。Endpoint 只是借用 Channel，因此 destructor 必须保持同步：
	 * 本函数返回后调用方可以立即安全销毁 Channel。
	 */
	(void)tr_channel_set_handler(endpoint->channel, NULL, NULL, NULL, NULL);
	(void)tr_channel_quiesce(endpoint->channel);
	tr_rpc_deadline_destroy(endpoint);
	tr_rpc_executor_shutdown(endpoint);
	tr_rpc_endpoint_wait_owner_only(endpoint);

	/* 释放 owner 创建时持有的初始强引用；此处必须是最后一次 put。 */
	tr_rpc_endpoint_put(endpoint);
}

static int tr_rpc_validate_method(const struct tr_rpc_method_desc *method)
{
	if (!method || method->service_id == 0 || method->method_id == 0 ||
	    (method->lane != TR_LANE_CONTROL && method->lane != TR_LANE_BULK) ||
	    method->request_codec_id != TR_RPC_CODEC_RAW ||
	    method->response_codec_id != TR_RPC_CODEC_RAW ||
	    method->max_request_bytes == 0 || method->max_response_bytes == 0)
		return TR_ERR_INVALID;

	if (method->request_cardinality != TR_RPC_ONE &&
	    method->request_cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	if (method->response_cardinality != TR_RPC_ONE &&
	    method->response_cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	return TR_OK;
}

static int tr_rpc_register_method_internal(
	struct tr_rpc_endpoint *endpoint,
	const struct tr_rpc_method_desc *method, enum tr_rpc_handler_kind kind,
	tr_rpc_unary_handler unary_handler,
	const struct tr_rpc_stream_handlers *stream_handlers, void *handler_arg)
{
	uint32_t i;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;
	ret = tr_rpc_validate_method(method);
	if (ret != TR_OK)
		return ret;

	if (kind == TR_RPC_HANDLER_UNARY &&
	    (method->request_cardinality != TR_RPC_ONE ||
	     method->response_cardinality != TR_RPC_ONE || !unary_handler))
		return TR_ERR_INVALID;
	if (kind == TR_RPC_HANDLER_STREAM && !stream_handlers)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&endpoint->lock);
	if (tr_rpc_find_method_locked(endpoint, method->service_id,
				      method->method_id)) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_STATE;
	}

	for (i = 0; i < endpoint->config.max_methods; ++i) {
		struct tr_rpc_method_entry *entry = &endpoint->methods[i];
		if (entry->used)
			continue;

		entry->used = 1;
		entry->handler_kind = kind;
		entry->desc = *method;
		entry->unary_handler = unary_handler;
		if (stream_handlers)
			entry->stream_handlers = *stream_handlers;
		entry->handler_arg = handler_arg;
		pthread_mutex_unlock(&endpoint->lock);
		return TR_OK;
	}

	pthread_mutex_unlock(&endpoint->lock);
	return TR_AGAIN;
}

int tr_rpc_register_method(struct tr_rpc_endpoint *endpoint,
			   const struct tr_rpc_method_desc *method,
			   tr_rpc_unary_handler unary_handler,
			   void *handler_arg)
{
	enum tr_rpc_handler_kind kind = TR_RPC_HANDLER_NONE;

	if (!endpoint)
		return TR_ERR_INVALID;

	if (endpoint->config.role == TR_RPC_SERVER) {
		if (!unary_handler)
			return TR_ERR_INVALID;
		kind = TR_RPC_HANDLER_UNARY;
	} else if (unary_handler) {
		return TR_ERR_INVALID;
	}

	return tr_rpc_register_method_internal(
		endpoint, method, kind, unary_handler, NULL, handler_arg);
}

int tr_rpc_register_stream_method(struct tr_rpc_endpoint *endpoint,
				  const struct tr_rpc_method_desc *method,
				  const struct tr_rpc_stream_handlers *handlers,
				  void *handler_arg)
{
	if (!endpoint || endpoint->config.role != TR_RPC_SERVER)
		return TR_ERR_INVALID;

	return tr_rpc_register_method_internal(endpoint, method,
					       TR_RPC_HANDLER_STREAM, NULL,
					       handlers, handler_arg);
}

struct tr_rpc_unary_call_owner_request {
	struct tr_rpc_endpoint *endpoint;
	uint32_t service_id;
	uint32_t method_id;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_options options;
	int has_options;
	uint64_t deadline_ns;
	tr_rpc_unary_result_cb result_cb;
	void *result_arg;
	struct tr_rpc_call_handle *out;
};

static int tr_rpc_unary_call_on_owner(void *arg)
{
	struct tr_rpc_unary_call_owner_request *request =
		(struct tr_rpc_unary_call_owner_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_call_handle handle;
	struct tr_buffer *request_buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	const struct tr_rpc_call_options *options =
		request->has_options ? &request->options : NULL;
	uint32_t slot;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	method = tr_rpc_find_method_locked(endpoint, request->service_id,
					   request->method_id);
	if (!method || method->desc.request_cardinality != TR_RPC_ONE ||
	    method->desc.response_cardinality != TR_RPC_ONE ||
	    request->request.len > method->desc.max_request_bytes) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_INVALID;
	}

	ret = tr_rpc_allocate_call_locked(endpoint, &call, &slot);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	call->state = TR_RPC_CALL_OPENING;
	call->method = method;
	call->is_unary = 1;
	call->result_cb = request->result_cb;
	call->result_arg = request->result_arg;

	ret = tr_rpc_apply_options_locked(endpoint, call, options,
					  request->deadline_ns);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_REQUEST,
				    &method->desc,
				    method->desc.request_codec_id,
				    TR_RPC_STATUS_OK, &request->request,
				    &request_buffer);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}
	call->pending_tx = tr_buffer_take(&request_buffer);
	handle = tr_rpc_make_call_handle(endpoint, slot, call);

	ret = tr_stream_open(endpoint->channel, method->desc.lane,
			     &call->stream);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	*request->out = handle;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

int tr_rpc_unary_call_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 const struct tr_rpc_call_options *options,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out)
{
	struct tr_rpc_unary_call_owner_request owner_request;

	if (!endpoint || !request || !out ||
	    endpoint->config.role != TR_RPC_CLIENT ||
	    (request->len != 0 && !request->data) ||
	    (options && options->metadata_count != 0 && !options->metadata))
		return TR_ERR_INVALID;

	memset(&owner_request, 0, sizeof(owner_request));
	owner_request.endpoint = endpoint;
	owner_request.service_id = service_id;
	owner_request.method_id = method_id;
	owner_request.request = *request;
	owner_request.result_cb = result_cb;
	owner_request.result_arg = result_arg;
	owner_request.out = out;
	if (options) {
		owner_request.options = *options;
		owner_request.has_options = 1;
		if (options->timeout_ms)
			owner_request.deadline_ns =
				tr_rpc_timeout_deadline_ns(options->timeout_ms);
	}

	return tr_rpc_owner_call(endpoint, tr_rpc_unary_call_on_owner,
				 &owner_request);
}

int tr_rpc_unary_call(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id, const struct tr_rpc_bytes *request,
		      tr_rpc_unary_result_cb result_cb, void *result_arg,
		      struct tr_rpc_call_handle *out)
{
	return tr_rpc_unary_call_ex(endpoint, service_id, method_id, request,
				    NULL, result_cb, result_arg, out);
}

struct tr_rpc_call_start_owner_request {
	struct tr_rpc_endpoint *endpoint;
	uint32_t service_id;
	uint32_t method_id;
	struct tr_rpc_call_options options;
	int has_options;
	uint64_t deadline_ns;
	struct tr_rpc_call_callbacks callbacks;
	int has_callbacks;
	struct tr_rpc_call_handle *out;
};

static int tr_rpc_call_start_on_owner(void *arg)
{
	struct tr_rpc_call_start_owner_request *request =
		(struct tr_rpc_call_start_owner_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_call_handle handle;
	const struct tr_rpc_call_options *options =
		request->has_options ? &request->options : NULL;
	uint32_t slot;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	method = tr_rpc_find_method_locked(endpoint, request->service_id,
					   request->method_id);
	if (!method) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_INVALID;
	}

	ret = tr_rpc_allocate_call_locked(endpoint, &call, &slot);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	call->state = TR_RPC_CALL_OPENING;
	call->method = method;
	call->is_unary = 0;
	if (request->has_callbacks)
		call->callbacks = request->callbacks;

	ret = tr_rpc_apply_options_locked(endpoint, call, options,
					  request->deadline_ns);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	handle = tr_rpc_make_call_handle(endpoint, slot, call);
	ret = tr_stream_open(endpoint->channel, method->desc.lane,
			     &call->stream);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	*request->out = handle;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

int tr_rpc_call_start_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_options *options,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out)
{
	struct tr_rpc_call_start_owner_request owner_request;

	if (!endpoint || !out || endpoint->config.role != TR_RPC_CLIENT ||
	    (options && options->metadata_count != 0 && !options->metadata))
		return TR_ERR_INVALID;

	memset(&owner_request, 0, sizeof(owner_request));
	owner_request.endpoint = endpoint;
	owner_request.service_id = service_id;
	owner_request.method_id = method_id;
	owner_request.out = out;
	if (options) {
		owner_request.options = *options;
		owner_request.has_options = 1;
		if (options->timeout_ms)
			owner_request.deadline_ns =
				tr_rpc_timeout_deadline_ns(options->timeout_ms);
	}
	if (callbacks) {
		owner_request.callbacks = *callbacks;
		owner_request.has_callbacks = 1;
	}

	return tr_rpc_owner_call(endpoint, tr_rpc_call_start_on_owner,
				 &owner_request);
}

int tr_rpc_call_start(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id,
		      const struct tr_rpc_call_callbacks *callbacks,
		      struct tr_rpc_call_handle *out)
{
	return tr_rpc_call_start_ex(endpoint, service_id, method_id, NULL,
				    callbacks, out);
}

static int tr_rpc_prepare_send_locked(struct tr_rpc_call_handle handle,
				      struct tr_rpc_call_slot **call_out,
				      struct tr_rpc_method_desc *method_out,
				      uint16_t *wire_type_out,
				      uint32_t *codec_out, uint32_t *limit_out)
{
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	enum tr_rpc_cardinality cardinality;

	if (!endpoint)
		return TR_ERR_INVALID;

	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary)
		return TR_ERR_STALE;
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL ||
	    call->final_status_seen || call->final_status_sent ||
	    call->local_closed)
		return TR_ERR_CLOSED;
	if (call->state != TR_RPC_CALL_ACTIVE)
		return TR_AGAIN;

	cardinality =
		tr_rpc_outbound_cardinality(endpoint, &call->method->desc);
	if (tr_rpc_validate_cardinality(cardinality, call->tx_count) != TR_OK)
		return TR_ERR_STATE;

	*call_out = call;
	*method_out = call->method->desc;
	*wire_type_out = tr_rpc_outbound_wire_type(endpoint);
	*codec_out = tr_rpc_outbound_codec(endpoint, &call->method->desc);
	*limit_out = tr_rpc_outbound_limit(endpoint, &call->method->desc);
	return TR_OK;
}

struct tr_rpc_send_request {
	struct tr_rpc_call_handle handle;
	const struct tr_rpc_bytes *message;
};

static int tr_rpc_call_send_on_owner(void *arg)
{
	struct tr_rpc_send_request *request = (struct tr_rpc_send_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	const struct tr_rpc_bytes *message = request->message;
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	uint16_t wire_type;
	uint32_t codec_id;
	uint32_t limit;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	ret = tr_rpc_prepare_send_locked(handle, &call, &method, &wire_type,
					 &codec_id, &limit);
	if (ret == TR_OK && message->len > limit)
		ret = TR_ERR_BAD_LENGTH;
	if (ret == TR_OK)
		ret = tr_rpc_encode_message(endpoint, call, wire_type, &method,
					    codec_id, TR_RPC_STATUS_OK, message,
					    &encoded);
	if (ret == TR_OK) {
		ret = tr_stream_send(call->stream, encoded);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&encoded);
			call->tx_count++;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_send(struct tr_rpc_call_handle handle,
		     const struct tr_rpc_bytes *message)
{
	struct tr_rpc_send_request request;

	if (!handle.endpoint || !message ||
	    (message->len != 0 && !message->data))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.message = message;
	return tr_rpc_owner_call(handle.endpoint, tr_rpc_call_send_on_owner,
				 &request);
}

struct tr_rpc_send_buffer_request {
	struct tr_rpc_call_handle handle;
	struct tr_buffer *payload;
};

static int tr_rpc_call_send_buffer_on_owner(void *arg)
{
	struct tr_rpc_send_buffer_request *request =
		(struct tr_rpc_send_buffer_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	struct tr_buffer *payload = request->payload;
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *header TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_buffer *parts[2];
	uint16_t wire_type;
	uint32_t codec_id;
	uint32_t limit;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	ret = tr_rpc_prepare_send_locked(handle, &call, &method, &wire_type,
					 &codec_id, &limit);
	if (ret == TR_OK && payload->len > limit)
		ret = TR_ERR_BAD_LENGTH;
	if (ret == TR_OK && codec_id != TR_RPC_CODEC_RAW)
		ret = TR_ERR_BAD_TYPE;
	if (ret == TR_OK)
		ret = tr_rpc_encode_header_buffer(
			endpoint, call, wire_type, &method, codec_id,
			TR_RPC_STATUS_OK, payload->len, &header);
	if (ret == TR_OK) {
		parts[0] = header;
		parts[1] = payload;
		ret = tr_stream_sendv(call->stream, parts, 2);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&header);
			call->tx_count++;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_send_buffer(struct tr_rpc_call_handle handle,
			    struct tr_buffer *payload)
{
	struct tr_rpc_send_buffer_request request;

	if (!handle.endpoint || !payload || payload->len == 0)
		return TR_ERR_INVALID;

	request.handle = handle;
	request.payload = payload;
	return tr_rpc_owner_call(handle.endpoint,
				 tr_rpc_call_send_buffer_on_owner, &request);
}

struct tr_rpc_close_send_request {
	struct tr_rpc_call_handle handle;
};

static int tr_rpc_call_close_send_on_owner(void *arg)
{
	struct tr_rpc_close_send_request *request =
		(struct tr_rpc_close_send_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	enum tr_rpc_cardinality cardinality;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary || call->local_closed) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}

	cardinality =
		tr_rpc_outbound_cardinality(endpoint, &call->method->desc);
	if (cardinality == TR_RPC_ONE && call->tx_count != 1U) {
		ret = TR_ERR_STATE;
		goto out;
	}
	if (cardinality == TR_RPC_NONE && call->tx_count != 0U) {
		ret = TR_ERR_STATE;
		goto out;
	}

	ret = tr_stream_close(call->stream);
	if (ret == TR_OK || ret == TR_ERR_CLOSED) {
		call->local_closed = 1;
		ret = TR_OK;
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_close_send(struct tr_rpc_call_handle handle)
{
	struct tr_rpc_close_send_request request;

	if (!handle.endpoint)
		return TR_ERR_INVALID;
	request.handle = handle;
	return tr_rpc_owner_call(handle.endpoint,
				 tr_rpc_call_close_send_on_owner, &request);
}

struct tr_rpc_finish_request {
	struct tr_rpc_call_handle handle;
	int status;
};

static int tr_rpc_call_finish_on_owner(void *arg)
{
	struct tr_rpc_finish_request *request =
		(struct tr_rpc_finish_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	int status = request->status;
	struct tr_rpc_endpoint *endpoint = handle.endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty;
	enum tr_rpc_cardinality cardinality;
	int ret;

	empty.data = NULL;
	empty.len = 0;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary ||
	    call->final_status_sent) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}

	cardinality = call->method->desc.response_cardinality;
	if (status == TR_RPC_STATUS_OK && cardinality == TR_RPC_ONE &&
	    call->tx_count != 1U) {
		ret = TR_ERR_STATE;
		goto out;
	}

	method = call->method->desc;
	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_STATUS, &method,
				    method.response_codec_id, status, &empty,
				    &buffer);
	if (ret == TR_OK) {
		ret = tr_stream_send(call->stream, buffer);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&buffer);
			call->final_status_sent = 1;
			call->final_status = status;
			call->deadline_ns = 0;
			tr_rpc_deadline_changed_locked(endpoint);
			ret = tr_stream_close(call->stream);
			if (ret == TR_OK || ret == TR_ERR_CLOSED) {
				call->local_closed = 1;
				call->need_local_close = 0;
				if (call->remote_closed) {
					call->state = TR_RPC_CALL_TERMINAL;
					(void)tr_rpc_notify_terminal_locked(
						endpoint, handle.slot, call,
						status);
					tr_rpc_maybe_free_call_locked(call);
				}
				ret = TR_OK;
			} else if (ret == TR_AGAIN) {
				/* STATUS 已提交给 Transport，close 后续由内部 retry 完成。 */
				call->need_local_close = 1;
				ret = TR_OK;
			}
		}
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_finish(struct tr_rpc_call_handle handle, int status)
{
	struct tr_rpc_finish_request request;

	if (!handle.endpoint ||
	    handle.endpoint->config.role != TR_RPC_SERVER)
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status = status;
	return tr_rpc_owner_call(handle.endpoint, tr_rpc_call_finish_on_owner,
				 &request);
}

int tr_rpc_call_cancel(struct tr_rpc_call_handle handle)
{
	return tr_rpc_cancel_internal(handle, TR_RPC_STATUS_CANCELLED);
}

struct tr_rpc_cancel_query {
	struct tr_rpc_call_handle handle;
	int *status_out;
};

static int tr_rpc_call_is_cancelled_on_owner(void *arg)
{
	struct tr_rpc_cancel_query *request =
		(struct tr_rpc_cancel_query *)arg;
	struct tr_rpc_endpoint *endpoint = request->handle.endpoint;
	struct tr_rpc_call_slot *call;
	int cancelled;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}

	cancelled = call->cancelled;
	if (request->status_out)
		*request->status_out = cancelled ? call->cancel_status :
						   TR_RPC_STATUS_OK;
	ret = cancelled ? 1 : 0;

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle handle, int *status_out)
{
	struct tr_rpc_cancel_query request;

	if (!handle.endpoint)
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status_out = status_out;
	return tr_rpc_owner_call(handle.endpoint,
				 tr_rpc_call_is_cancelled_on_owner, &request);
}

struct tr_rpc_set_metadata_request {
	struct tr_rpc_call_handle handle;
	const char *key;
	const void *value;
	uint16_t value_len;
	size_t key_len;
};

static int tr_rpc_call_set_metadata_on_owner(void *arg)
{
	struct tr_rpc_set_metadata_request *request =
		(struct tr_rpc_set_metadata_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->handle.endpoint;
	struct tr_rpc_call_slot *call;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}
	if (call->tx_count != 0 || call->pending_tx != NULL ||
	    call->pending_control != NULL) {
		ret = TR_ERR_STATE;
		goto out;
	}

	if (call->deadline_ns != 0 &&
	    (uint32_t)call->local_metadata_len +
			    TR_RPC_METADATA_TLV_HEADER_SIZE + request->key_len +
			    request->value_len +
			    TR_RPC_DEADLINE_METADATA_BYTES >
		    TR_RPC_METADATA_MAX_BYTES) {
		ret = TR_ERR_BAD_LENGTH;
		goto out;
	}

	ret = tr_rpc_metadata_add_raw(
		call->local_metadata, &call->local_metadata_len,
		request->key, request->key_len, request->value,
		request->value_len, 0);

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_set_metadata(struct tr_rpc_call_handle handle, const char *key,
			     const void *value, uint16_t value_len)
{
	struct tr_rpc_set_metadata_request request;
	size_t key_len;

	if (!handle.endpoint || !tr_rpc_metadata_key_valid(key, &key_len) ||
	    (value_len != 0 && !value))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.key = key;
	request.value = value;
	request.value_len = value_len;
	request.key_len = key_len;
	return tr_rpc_owner_call(handle.endpoint,
				 tr_rpc_call_set_metadata_on_owner, &request);
}

struct tr_rpc_get_metadata_request {
	struct tr_rpc_call_handle handle;
	const char *key;
	void *value;
	uint16_t *value_len;
	size_t key_len;
};

static int tr_rpc_call_get_peer_metadata_on_owner(void *arg)
{
	struct tr_rpc_get_metadata_request *request =
		(struct tr_rpc_get_metadata_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->handle.endpoint;
	struct tr_rpc_call_slot *call;
	const uint8_t *found = NULL;
	uint16_t found_len = 0;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}

	ret = tr_rpc_metadata_find_raw(
		call->peer_metadata, call->peer_metadata_len,
		request->key, request->key_len, &found, &found_len);
	if (ret == TR_OK) {
		if (!request->value) {
			*request->value_len = found_len;
		} else if (*request->value_len < found_len) {
			*request->value_len = found_len;
			ret = TR_ERR_BAD_LENGTH;
		} else {
			if (found_len)
				memcpy(request->value, found, found_len);
			*request->value_len = found_len;
		}
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle handle,
				  const char *key, void *value,
				  uint16_t *value_len)
{
	struct tr_rpc_get_metadata_request request;
	size_t key_len;

	if (!handle.endpoint || !value_len ||
	    !tr_rpc_metadata_key_valid(key, &key_len))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.key = key;
	request.value = value;
	request.value_len = value_len;
	request.key_len = key_len;
	return tr_rpc_owner_call(handle.endpoint,
				 tr_rpc_call_get_peer_metadata_on_owner,
				 &request);
}

int tr_rpc_message_release(struct tr_rpc_message *message)
{
	struct tr_buffer *storage;
	int ret;

	if (!message || !message->storage)
		return TR_ERR_INVALID;

	storage = message->storage;
	message->storage = NULL;
	ret = tr_rpc_release_stream_payload(message->stream, storage);
	memset(message, 0, sizeof(*message));
	return ret;
}

static int tr_rpc_endpoint_flush_on_owner(void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	uint32_t i;
	int result;

	result = tr_channel_flush(endpoint->channel);

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		int ret = TR_OK;

		if (call->state == TR_RPC_CALL_FREE)
			continue;

		if (call->is_unary &&
		    (call->pending_tx || call->need_local_close))
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (!call->is_unary && call->need_local_close) {
			ret = tr_stream_close(call->stream);
			if (ret == TR_OK || ret == TR_ERR_CLOSED) {
				call->need_local_close = 0;
				call->local_closed = 1;
				ret = TR_OK;
			}
		}

		if (ret != TR_OK && ret != TR_AGAIN && result == TR_OK)
			result = ret;
		else if (ret == TR_AGAIN && result == TR_OK)
			result = TR_AGAIN;
	}
	pthread_mutex_unlock(&endpoint->lock);
	return result;
}

int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	return tr_rpc_owner_call(endpoint, tr_rpc_endpoint_flush_on_owner,
				 endpoint);
}

int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out)
{
	uint32_t i;

	if (!endpoint || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));

	pthread_mutex_lock(&endpoint->lock);
	out->max_methods = endpoint->config.max_methods;
	out->max_calls = endpoint->config.max_calls;
	out->calls_started = endpoint->stat_calls_started;
	out->calls_completed = endpoint->stat_calls_completed;
	out->calls_cancelled = endpoint->stat_calls_cancelled;
	out->calls_deadline_exceeded = endpoint->stat_calls_deadline_exceeded;

	for (i = 0; i < endpoint->config.max_methods; ++i)
		if (endpoint->methods[i].used)
			out->registered_methods++;

	for (i = 0; i < endpoint->config.max_calls; ++i) {
		switch (endpoint->calls[i].state) {
		case TR_RPC_CALL_OPENING:
			out->opening_calls++;
			break;
		case TR_RPC_CALL_ACTIVE:
			out->active_calls++;
			break;
		case TR_RPC_CALL_TERMINAL:
			out->terminal_calls++;
			break;
		default:
			break;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);

	pthread_mutex_lock(&endpoint->executor.lock);
	out->executor_threads = endpoint->executor.thread_count;
	out->executor_queued_tasks = endpoint->executor.queued_count;
	out->executor_running_tasks = endpoint->executor.running_count;
	pthread_mutex_unlock(&endpoint->executor.lock);

	return TR_OK;
}
