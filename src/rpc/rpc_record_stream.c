#include "rpc_record_stream.h"
#include "rpc_record_transport_internal.h"
#include "../endian.h"
#include "../message/builder.h"
#include "../memory_budget.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../transport/protocol/message_envelope.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

struct tr_rpc_record_rx_item;
struct tr_rpc_record_tx_item;

struct tr_rpc_record_stream {
	struct tr_stream_handle stream;
	struct tr_reactor *reactor;
	struct tr_memory_budget *budget;
	struct tr_rpc_record_stream_config config;
	struct tr_rpc_record_stream_callbacks callbacks;
	struct tr_rpc_record_state state;
	struct tr_rpc_record_limits tx_limits;
	struct tr_rpc_record_limits tx_compressed_limits;
	struct tr_rpc_record_limits rx_limits;
	struct tr_rpc_record_state rx_reserved_state;
	struct tr_rpc_record_rx_item *rx_head;
	struct tr_rpc_record_rx_item *rx_tail;
	struct tr_rpc_record_tx_item *tx_pending;
	enum tr_stream_event rx_pending_event;
	int rx_pending_status;
	uint32_t tx_encoded_body_limit;
	uint32_t channel_max_message_bytes;
	size_t tx_workspace_bytes;
	unsigned tx_window_log;
	unsigned tx_compression_enabled;
	size_t rx_workspace_bytes;
	unsigned rx_window_log;
	uint8_t header[TR_RPC_RECORD_HEADER_SIZE];
	size_t header_used;
	uint8_t *partial;
	size_t partial_used;
	size_t partial_size;
	unsigned receiving;
	unsigned sending;
	unsigned callback_depth;
	unsigned terminal_notified;
};

struct tr_rpc_record_rx_item {
	struct tr_rpc_record_stream *call;
	struct tr_rpc_record_rx_item *next;
	struct tr_rpc_record_state next_state;
	struct tr_rpc_record_header header;
	struct tr_message decoded;
	struct tr_task_handle task;
	uint8_t *body;
	size_t item_bytes;
	size_t body_bytes;
	uint32_t metadata_size;
	enum tr_message_transform_outcome outcome;
	unsigned transform_active;
	unsigned ready;
	unsigned owner_stopped;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
};

struct tr_rpc_record_tx_item {
	struct tr_rpc_record_stream *call;
	struct tr_rpc_record_header header;
	struct tr_message encoded;
	struct tr_task_handle task;
	size_t item_bytes;
	size_t prefix_size;
	size_t total_size;
	size_t offset;
	uint32_t metadata_size;
	unsigned transform_active;
	unsigned ready;
	unsigned owner_stopped;
	unsigned reset_requested;
	uint8_t prefix[TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES];
};

/* 位于独立 TX 对象中；发送接受之后不再引用 Call，最后释放可能发生在
 * Call 已销毁之后，所以预算域必须由上层保持存活。
 */
struct tr_rpc_record_prefix {
	struct tr_buffer buffer;
	struct tr_memory_budget *budget;
	size_t bytes;
	uint8_t data[];
};

static int tr_record_owner(const struct tr_rpc_record_stream *call)
{
	if (!call)
		return TR_ERR_INVALID;
	return tr_reactor_in_owner_context_for(call->reactor) ? TR_OK
	                                                      : TR_ERR_STATE;
}

static int tr_record_same_stream(const struct tr_rpc_record_stream *call,
                                 struct tr_stream_handle stream)
{
	return stream.channel == call->stream.channel &&
	       stream.slot == call->stream.slot &&
	       stream.generation == call->stream.generation;
}

static enum tr_rpc_record_direction
tr_record_receive_direction(const struct tr_rpc_record_stream *call)
{
	return call->config.send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	               ? TR_RPC_RECORD_RESPONSE_DIRECTION
	               : TR_RPC_RECORD_REQUEST_DIRECTION;
}

static void tr_record_clear_partial(struct tr_rpc_record_stream *call)
{
	free(call->partial);
	if (call->partial_size)
		(void)tr_memory_budget_release(call->budget,
		                               call->partial_size);
	call->partial = NULL;
	call->partial_size = call->partial_used = call->header_used = 0;
}

static void tr_record_notify_terminal(struct tr_rpc_record_stream *call,
                                      enum tr_rpc_record_delivery delivery)
{
	if (!call->state.terminal || call->terminal_notified ||
	    call->receiving || call->sending || call->callback_depth ||
	    call->rx_head || call->tx_pending)
		return;
	call->terminal_notified = 1;
	++call->callback_depth;
	call->callbacks.terminal(call,
	                         (enum tr_rpc_status)call->state.final_status,
	                         delivery, call->callbacks.arg);
	--call->callback_depth;
}

/* 512字节上限使条目数及重复键检查的工作量有常量上界。不使用strlen读取
 * 对端字节；冒号保留项不再混入V2自定义元数据，期限有独立记录字段。
 */
static int tr_record_metadata_valid(const uint8_t *data, uint32_t size)
{
	size_t offset = 0;
	if (size > TR_RPC_METADATA_MAX_BYTES || (size && !data))
		return TR_ERR_BAD_LENGTH;
	while (offset < size) {
		size_t key_size, value_size, entry_size;
		if (size - offset < 3)
			return TR_ERR_BAD_LENGTH;
		key_size = data[offset];
		value_size = tr_get_le16(data + offset + 1);
		entry_size = 3 + key_size + value_size;
		if (!key_size || key_size > TR_RPC_METADATA_MAX_KEY_LEN ||
		    entry_size > size - offset)
			return TR_ERR_BAD_LENGTH;
		for (size_t i = 0; i < key_size; ++i) {
			uint8_t ch = data[offset + 3 + i];
			if (!((ch >= 'a' && ch <= 'z') ||
			      (ch >= '0' && ch <= '9') || ch == '_' ||
			      ch == '.' || ch == '-'))
				return TR_ERR_BAD_FLAGS;
		}
		for (size_t prior = 0; prior < offset;) {
			size_t n = data[prior];
			if (n == key_size &&
			    !memcmp(data + prior + 3, data + offset + 3, n))
				return TR_ERR_BAD_FLAGS;
			prior += 3 + n + tr_get_le16(data + prior + 1);
		}
		offset += entry_size;
	}
	return TR_OK;
}

static int tr_record_preflight(const struct tr_rpc_record_stream *call,
                               const struct tr_rpc_record_state *base,
                               enum tr_rpc_record_direction direction,
                               const struct tr_rpc_record_header *header,
                               struct tr_rpc_record_state *next)
{
	const struct tr_rpc_method_desc *method = &call->config.method;
	if (header->kind == TR_RPC_RECORD_OPEN &&
	    (header->service_id != method->service_id ||
	     header->method_id != method->method_id))
		return TR_ERR_BAD_TYPE;
	if (header->kind == TR_RPC_RECORD_MESSAGE) {
		uint32_t codec = direction == TR_RPC_RECORD_REQUEST_DIRECTION
		                         ? method->request_codec_id
		                         : method->response_codec_id;
		uint32_t limit = direction == TR_RPC_RECORD_REQUEST_DIRECTION
		                         ? method->max_request_bytes
		                         : method->max_response_bytes;
		if (header->codec_id != codec)
			return TR_ERR_BAD_TYPE;
		if (header->decoded_size > limit)
			return TR_ERR_BAD_LENGTH;
	}
	*next = *base;
	return tr_rpc_record_state_apply(next, direction, header);
}

/* 一个方向的接收变换可能尚未完成时，另一个方向仍可提交记录。合并已接受
 * 的发送事件时只复制发送方向字段，不能把排队中的接收快照提前写入真实状态。
 */
static void
tr_record_state_merge_direction(struct tr_rpc_record_state *destination,
                                const struct tr_rpc_record_state *source,
                                enum tr_rpc_record_direction direction)
{
	if (direction == TR_RPC_RECORD_REQUEST_DIRECTION) {
		destination->request_messages = source->request_messages;
		destination->request_closed = source->request_closed;
	} else {
		destination->response_headers = source->response_headers;
		destination->response_messages = source->response_messages;
	}
	if (source->opened)
		destination->opened = 1;
	if (source->terminal) {
		destination->terminal = 1;
		destination->final_status = source->final_status;
	}
}

int tr_rpc_record_stream_create_on_owner(
        struct tr_stream_handle stream,
        const struct tr_rpc_record_stream_config *config,
        const struct tr_rpc_record_stream_callbacks *callbacks,
        struct tr_memory_budget *budget, struct tr_rpc_record_stream **out)
{
	struct tr_rpc_record_stream *call;
	struct tr_rpc_record_state state;
	struct tr_channel_message_limits message_limits;
	struct tr_stream_diagnostics stream_state;
	struct tr_reactor *reactor;
	uint32_t tx_size, rx_size, rx_encoded_size;
	uint32_t send_policy;
	uint32_t receive_algorithms = TR_RPC_RECORD_ACCEPT_NONE;
	int ret;
	if (!stream.channel || !config || !callbacks || !callbacks->received ||
	    !callbacks->terminal || !budget || !budget->limit_bytes || !out)
		return TR_ERR_INVALID;
	if (*out)
		return TR_ERR_STATE;
	reactor = tr_channel_reactor(stream.channel);
	if (!tr_reactor_in_owner_context_for(reactor))
		return TR_ERR_STATE;
	if ((config->send_direction != TR_RPC_RECORD_REQUEST_DIRECTION &&
	     config->send_direction != TR_RPC_RECORD_RESPONSE_DIRECTION) ||
	    !config->method.request_codec_id ||
	    !config->method.response_codec_id ||
	    (config->method.request_compression_algorithms &
	     ~TR_RPC_COMPRESSION_MASK_ZSTD) ||
	    (config->method.response_compression_algorithms &
	     ~TR_RPC_COMPRESSION_MASK_ZSTD))
		return TR_ERR_INVALID;
	ret = tr_rpc_record_state_init(&state,
	                               config->method.request_cardinality,
	                               config->method.response_cardinality);
	if (ret != TR_OK)
		return ret;
	ret = tr_stream_get_diagnostics(stream, &stream_state);
	if (ret != TR_OK)
		return ret;
	if ((uint32_t)stream_state.lane != config->method.lane)
		return TR_ERR_BAD_TYPE;
	ret = tr_channel_message_limits_on_owner(
	        stream.channel, stream_state.lane, &message_limits);
	if (ret != TR_OK)
		return ret;
	ret = tr_memory_budget_reserve(budget, sizeof(*call));
	if (ret != TR_OK)
		return ret;
	call = calloc(1, sizeof(*call));
	if (!call) {
		(void)tr_memory_budget_release(budget, sizeof(*call));
		return TR_ERR_NOMEM;
	}
	call->stream = stream;
	call->reactor = reactor;
	call->budget = budget;
	call->config = *config;
	call->callbacks = *callbacks;
	call->state = state;
	call->rx_reserved_state = state;
	tx_size = config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                  ? config->method.max_request_bytes
	                  : config->method.max_response_bytes;
	rx_size = config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                  ? config->method.max_response_bytes
	                  : config->method.max_request_bytes;
	rx_encoded_size = rx_size;
	send_policy = config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                      ? config->method.request_compression_algorithms
	                      : config->method.response_compression_algorithms;
	uint32_t receive_policy =
	        config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                ? config->method.response_compression_algorithms
	                : config->method.request_compression_algorithms;
	if ((receive_policy & TR_RPC_COMPRESSION_MASK_ZSTD) &&
	    (message_limits.local_receive_algorithms &
	     TR_MESSAGE_ALGORITHM_ZSTD) &&
	    message_limits.local_max_encoded_wire_bytes >
	            TR_MESSAGE_ENVELOPE_WIRE_SIZE) {
		uint32_t encoded_body_limit =
		        message_limits.local_max_encoded_wire_bytes -
		        TR_MESSAGE_ENVELOPE_WIRE_SIZE;
		if (encoded_body_limit > rx_encoded_size)
			rx_encoded_size = encoded_body_limit;
		receive_algorithms |= TR_RPC_RECORD_ACCEPT_ZSTD;
		call->rx_workspace_bytes =
		        message_limits.max_compression_workspace_bytes;
		call->rx_window_log = message_limits.max_compression_window_log;
	}
	call->tx_limits = (struct tr_rpc_record_limits){
	        tx_size, tx_size, TR_RPC_METADATA_MAX_BYTES,
	        TR_RPC_RECORD_ACCEPT_NONE};
	call->tx_compressed_limits = call->tx_limits;
	call->channel_max_message_bytes = message_limits.max_message_bytes;
	call->tx_workspace_bytes =
	        message_limits.max_compression_workspace_bytes;
	call->tx_window_log = message_limits.max_compression_window_log;
	if ((send_policy & TR_RPC_COMPRESSION_MASK_ZSTD) &&
	    message_limits.peer_caps_ready &&
	    (message_limits.peer_receive_algorithms &
	     TR_MESSAGE_ALGORITHM_ZSTD) &&
	    message_limits.peer_max_encoded_wire_bytes >
	            TR_MESSAGE_ENVELOPE_WIRE_SIZE) {
		call->tx_encoded_body_limit =
		        message_limits.peer_max_encoded_wire_bytes -
		        TR_MESSAGE_ENVELOPE_WIRE_SIZE;
		call->tx_compressed_limits.max_encoded_size =
		        call->tx_encoded_body_limit;
		call->tx_compressed_limits.accepted_compression |=
		        TR_RPC_RECORD_ACCEPT_ZSTD;
		call->tx_compression_enabled = 1U;
	}
	call->rx_limits = (struct tr_rpc_record_limits){
	        rx_encoded_size, rx_size, TR_RPC_METADATA_MAX_BYTES,
	        receive_algorithms};
	*out = call;
	return TR_OK;
}

static void tr_record_prefix_release(struct tr_buffer *buffer)
{
	struct tr_rpc_record_prefix *prefix =
	        (struct tr_rpc_record_prefix *)buffer;
	struct tr_memory_budget *budget = prefix->budget;
	size_t bytes = prefix->bytes;
	free(prefix);
	(void)tr_memory_budget_release(budget, bytes);
}

static void tr_record_tx_item_release(struct tr_rpc_record_stream *call,
                                      struct tr_rpc_record_tx_item *item)
{
	if (item->encoded._storage)
		(void)tr_message_release(&item->encoded);
	if (call->tx_pending == item)
		call->tx_pending = NULL;
	size_t item_bytes = item->item_bytes;
	free(item);
	(void)tr_memory_budget_release(call->budget, item_bytes);
}

static void tr_record_tx_cancel(struct tr_rpc_record_stream *call);
static enum tr_rpc_status
tr_record_transform_status(enum tr_message_transform_outcome outcome);

static int tr_record_tx_copy(struct tr_rpc_record_tx_item *item, uint8_t *out,
                             size_t offset, size_t size)
{
	struct tr_message_view encoded;
	size_t prefix_size = item->prefix_size;
	if (tr_message_view(&item->encoded, &encoded) != TR_OK)
		return TR_ERR_STATE;
	while (size) {
		if (offset < prefix_size) {
			size_t amount = prefix_size - offset;
			if (amount > size)
				amount = size;
			memcpy(out, item->prefix + offset, amount);
			offset += amount;
			out += amount;
			size -= amount;
		} else {
			size_t body_offset = offset - prefix_size;
			if (body_offset > encoded.len ||
			    size > encoded.len - body_offset)
				return TR_ERR_BAD_LENGTH;
			memcpy(out, encoded.data + body_offset, size);
			offset += size;
			out += size;
			size = 0;
		}
	}
	return TR_OK;
}

static int tr_record_tx_send_chunk(struct tr_rpc_record_stream *call,
                                   struct tr_rpc_record_tx_item *item,
                                   size_t offset, size_t size)
{
	struct tr_rpc_record_prefix *chunk;
	struct tr_stream_diagnostics diagnostics;
	size_t allocation_size;
	int ret;
	if (!size || size > UINT32_MAX || size > SIZE_MAX - sizeof(*chunk))
		return TR_ERR_BAD_LENGTH;
	allocation_size = sizeof(*chunk) + size;
	ret = tr_memory_budget_reserve(call->budget, allocation_size);
	if (ret != TR_OK)
		return ret;
	chunk = malloc(allocation_size);
	if (!chunk) {
		(void)tr_memory_budget_release(call->budget, allocation_size);
		return TR_ERR_NOMEM;
	}
	chunk->budget = call->budget;
	chunk->bytes = allocation_size;
	chunk->buffer =
	        (struct tr_buffer){.data = chunk->data,
	                           .capacity = (uint32_t)size,
	                           .release_cb = tr_record_prefix_release};
	ret = tr_record_tx_copy(item, chunk->data, offset, size);
	if (ret != TR_OK) {
		tr_buffer_release(&chunk->buffer);
		return ret;
	}
	chunk->buffer.len = (uint32_t)size;
	if (tr_stream_get_diagnostics(call->stream, &diagnostics) != TR_OK) {
		tr_buffer_release(&chunk->buffer);
		return TR_ERR_STALE;
	}
	call->sending = 1;
	ret = tr_stream_send(call->stream, &chunk->buffer);
	call->sending = 0;
	if (ret != TR_OK)
		tr_buffer_release(&chunk->buffer);
	return ret;
}

static int tr_record_tx_pump(struct tr_rpc_record_stream *call)
{
	struct tr_rpc_record_tx_item *item = call->tx_pending;
	struct tr_message_view encoded;
	struct tr_stream_diagnostics diagnostics;
	struct tr_rpc_record_state next;
	size_t total, available, chunk_size;
	int ret;
	if (!item || item->transform_active || !item->ready)
		return TR_OK;
	if (item->owner_stopped)
		return TR_AGAIN;
	if (call->sending)
		return TR_AGAIN;
	if (call->state.terminal) {
		tr_record_tx_cancel(call);
		tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
		return TR_OK;
	}
	/* 先前接受的远端 FINAL 可能排在尚未完成的接收变换之后。
	 * 在验证并提交它之前暂停本方向输出，避免在已终局调用继续写字节。 */
	if (call->rx_head && call->rx_reserved_state.terminal)
		return TR_AGAIN;
	ret = tr_message_view(&item->encoded, &encoded);
	if (ret != TR_OK)
		goto fail_internal;
	if (item->prefix_size > SIZE_MAX - encoded.len)
		goto fail_length;
	total = item->prefix_size + encoded.len;
	item->total_size = total;
	while (item->offset < total) {
		ret = tr_stream_get_diagnostics(call->stream, &diagnostics);
		if (ret != TR_OK)
			goto fail_unavailable;
		if (diagnostics.flow.tx_send_limit <=
		    diagnostics.flow.tx_sent_bytes)
			return TR_AGAIN;
		available = (size_t)(diagnostics.flow.tx_send_limit -
		                     diagnostics.flow.tx_sent_bytes);
		chunk_size = total - item->offset;
		if (chunk_size > available)
			chunk_size = available;
		if (chunk_size > call->channel_max_message_bytes)
			chunk_size = call->channel_max_message_bytes;
		if (!chunk_size)
			return TR_AGAIN;
		ret = tr_record_tx_send_chunk(call, item, item->offset,
		                              chunk_size);
		if (ret == TR_AGAIN)
			return TR_AGAIN;
		if (ret == TR_ERR_NOMEM)
			goto fail_resource;
		if (ret != TR_OK)
			goto fail_unavailable;
		item->offset += chunk_size;
	}
	ret = tr_record_preflight(call, &call->state,
	                          call->config.send_direction, &item->header,
	                          &next);
	if (ret != TR_OK)
		goto fail_internal;
	call->state = next;
	if (call->rx_tail)
		tr_record_state_merge_direction(&call->rx_reserved_state, &next,
		                                call->config.send_direction);
	else
		call->rx_reserved_state = next;
	tr_record_tx_item_release(call, item);
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return TR_OK;

fail_length:
	ret = TR_ERR_BAD_LENGTH;
	goto fail_internal;
fail_resource:
	if (!call->state.terminal)
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	else
		tr_record_tx_cancel(call);
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
fail_unavailable:
	if (!call->state.terminal)
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_UNAVAILABLE);
	else
		tr_record_tx_cancel(call);
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
fail_internal:
	if (!call->state.terminal)
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_INTERNAL);
	else
		tr_record_tx_cancel(call);
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
}

static void tr_record_tx_cancel(struct tr_rpc_record_stream *call)
{
	struct tr_rpc_record_tx_item *item = call->tx_pending;
	if (!item)
		return;
	if (item->offset && !item->reset_requested) {
		item->reset_requested = 1U;
		(void)tr_stream_reset_on_owner(
		        call->stream,
		        call->state.terminal
		                ? tr_rpc_record_reset_reason(
		                          (enum tr_rpc_status)
		                                  call->state.final_status)
		                : TR_ERR_SYS);
	}
	if (item->transform_active) {
		(void)tr_channel_message_cancel_on_owner(call->stream.channel,
		                                         item->task);
		return;
	}
	tr_record_tx_item_release(call, item);
}

static void tr_record_tx_transform_done(void *arg, struct tr_task_handle handle,
                                        struct tr_message_service_result result,
                                        struct tr_message *message)
{
	struct tr_rpc_record_tx_item *item = arg;
	struct tr_rpc_record_stream *call = item->call;
	struct tr_stream_diagnostics stream_state;
	struct tr_message_view encoded;
	int ret;
	item->transform_active = 0;
	if (result.delivery == TR_MESSAGE_SERVICE_OWNER_STOPPED) {
		item->owner_stopped = 1U;
		item->ready = 1U;
		return;
	}
	if (handle.instance != item->task.instance ||
	    handle.generation != item->task.generation ||
	    handle.slot != item->task.slot) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_INTERNAL);
		return;
	}
	/* 取消/RESET 已选终局时，回执只归还 TX 节点并兑现终局。不能再
	 * 访问可能已退役的 Stream，也不能把已选状态重新提交给 abort。 */
	if (call->state.terminal) {
		tr_record_tx_item_release(call, item);
		tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
		return;
	}
	if (tr_stream_get_diagnostics(call->stream, &stream_state) != TR_OK) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_UNAVAILABLE);
		return;
	}
	if (result.outcome != TR_MESSAGE_TRANSFORM_OK) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, tr_record_transform_status(result.outcome));
		return;
	}
	if (call->state.terminal) {
		item->ready = 1U;
		tr_record_tx_pump(call);
		return;
	}
	if (!message || tr_message_clone(message, &item->encoded) != TR_OK) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	ret = tr_message_view(&item->encoded, &encoded);
	if (ret != TR_OK || encoded.len > UINT32_MAX) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, TR_RPC_STATUS_INTERNAL);
		return;
	}
	item->header.encoded_size = (uint32_t)encoded.len;
	ret = tr_rpc_record_encode_header(
	        item->prefix, TR_RPC_RECORD_HEADER_SIZE, &item->header,
	        &call->tx_compressed_limits);
	if (ret != TR_OK) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, ret == TR_ERR_BAD_LENGTH
		                      ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
		                      : TR_RPC_STATUS_INTERNAL);
		return;
	}
	item->prefix_size = TR_RPC_RECORD_HEADER_SIZE + item->metadata_size;
	item->ready = 1U;
	tr_record_tx_pump(call);
}

static int tr_record_tx_item_create(struct tr_rpc_record_stream *call,
                                    const struct tr_rpc_record_header *header,
                                    const uint8_t *metadata,
                                    struct tr_rpc_record_tx_item **out)
{
	struct tr_rpc_record_tx_item *item;
	int ret = tr_memory_budget_reserve(call->budget, sizeof(*item));
	if (ret != TR_OK)
		return ret;
	item = calloc(1, sizeof(*item));
	if (!item) {
		(void)tr_memory_budget_release(call->budget, sizeof(*item));
		return TR_ERR_NOMEM;
	}
	item->call = call;
	item->header = *header;
	item->item_bytes = sizeof(*item);
	item->metadata_size = header->metadata_size;
	if (item->metadata_size)
		memcpy(item->prefix + TR_RPC_RECORD_HEADER_SIZE, metadata,
		       item->metadata_size);
	*out = item;
	return TR_OK;
}

int tr_rpc_record_stream_send_compressed_on_owner(
        struct tr_rpc_record_stream *call,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body)
{
	struct tr_rpc_record_header intent;
	struct tr_rpc_record_state next;
	struct tr_rpc_record_tx_item *item = NULL;
	struct tr_message_builder *builder = NULL;
	struct tr_message input = {0};
	struct tr_buffer *payload = body ? *body : NULL;
	const struct tr_rpc_record_state *base;
	struct tr_message_transform_config config;
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (!header)
		return TR_ERR_INVALID;
	if (call->sending || call->state.terminal)
		return TR_ERR_STATE;
	if (call->tx_pending)
		return TR_AGAIN;
	if (header->kind != TR_RPC_RECORD_MESSAGE ||
	    header->compression != TR_RPC_RECORD_ZSTD || header->encoded_size)
		return TR_ERR_BAD_TYPE;
	if (!call->tx_compression_enabled) {
		return TR_ERR_UNSUPPORTED;
	}
	if ((payload && (payload->len > payload->capacity ||
	                 (payload->len && !payload->data))) ||
	    header->decoded_size != (payload ? payload->len : 0U))
		return TR_ERR_BAD_LENGTH;
	ret = tr_record_metadata_valid(metadata, header->metadata_size);
	if (ret != TR_OK)
		return ret;
	if (call->rx_head && header->kind == TR_RPC_RECORD_FINAL_STATUS)
		return TR_AGAIN;
	intent = *header;
	intent.encoded_size = 1U;
	base = call->rx_tail ? &call->rx_reserved_state : &call->state;
	ret = tr_record_preflight(call, base, call->config.send_direction,
	                          &intent, &next);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_tx_item_create(call, header, metadata, &item);
	if (ret != TR_OK)
		return ret;
	ret = tr_message_builder_create(call->budget, header->decoded_size,
	                                &builder);
	if (ret == TR_OK && header->decoded_size)
		ret = tr_message_builder_append(builder, payload->data,
		                                header->decoded_size);
	if (ret == TR_OK)
		ret = tr_message_builder_publish(&builder, &input);
	config = (struct tr_message_transform_config){
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {call->tx_limits.max_decoded_size,
	                   call->tx_encoded_body_limit},
	        .decoded_size = header->decoded_size,
	        .max_workspace_bytes = call->tx_workspace_bytes,
	        .max_window_log = call->tx_window_log,
	        .compression_level = 3};
	if (ret == TR_OK)
		ret = tr_channel_message_submit_on_owner(
		        call->stream.channel, &input, &config,
		        tr_record_tx_transform_done, item, &item->task);
	if (builder)
		(void)tr_message_builder_cancel(&builder);
	if (input._storage)
		(void)tr_message_release(&input);
	if (ret != TR_OK) {
		tr_record_tx_item_release(call, item);
		return ret;
	}
	item->transform_active = 1U;
	call->tx_pending = item;
	if (body)
		*body = NULL;
	if (payload)
		tr_buffer_release(payload);
	return TR_OK;
}

int tr_rpc_record_stream_send_on_owner(
        struct tr_rpc_record_stream *call,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body)
{
	struct tr_rpc_record_state next;
	struct tr_rpc_record_prefix *prefix;
	const struct tr_rpc_record_state *base;
	struct tr_buffer *payload = body ? *body : NULL;
	struct tr_buffer *slices[2];
	uint8_t wire_header[TR_RPC_RECORD_HEADER_SIZE];
	size_t wire_size, bytes;
	int empty_payload = payload && payload->len == 0;
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (call->sending || call->state.terminal)
		return TR_ERR_STATE;
	if (call->tx_pending)
		return TR_AGAIN;
	ret = tr_rpc_record_encode_header(wire_header, sizeof(wire_header),
	                                  header, &call->tx_limits);
	if (ret != TR_OK)
		return ret;
	if ((payload && (payload->len > payload->capacity ||
	                 (payload->len && !payload->data))) ||
	    header->encoded_size != (payload ? payload->len : 0))
		return TR_ERR_BAD_LENGTH;
	ret = tr_record_metadata_valid(metadata, header->metadata_size);
	if (ret != TR_OK)
		return ret;
	if (call->rx_head && header->kind == TR_RPC_RECORD_FINAL_STATUS)
		return TR_AGAIN;
	base = call->rx_tail ? &call->rx_reserved_state : &call->state;
	ret = tr_record_preflight(call, base, call->config.send_direction,
	                          header, &next);
	if (ret != TR_OK)
		return ret;
	wire_size = TR_RPC_RECORD_HEADER_SIZE + header->metadata_size;
	bytes = sizeof(*prefix) + wire_size;
	ret = tr_memory_budget_reserve(call->budget, bytes);
	if (ret != TR_OK)
		return ret;
	prefix = malloc(bytes);
	if (!prefix) {
		(void)tr_memory_budget_release(call->budget, bytes);
		return TR_ERR_NOMEM;
	}
	prefix->budget = call->budget;
	prefix->bytes = bytes;
	prefix->buffer =
	        (struct tr_buffer){.data = prefix->data,
	                           .capacity = (uint32_t)wire_size,
	                           .len = (uint32_t)wire_size,
	                           .release_cb = tr_record_prefix_release};
	memcpy(prefix->data, wire_header, sizeof(wire_header));
	if (header->metadata_size)
		memcpy(prefix->data + sizeof(wire_header), metadata,
		       header->metadata_size);
	slices[0] = &prefix->buffer;
	slices[1] = payload;
	call->sending = 1;
	ret = tr_stream_sendv(call->stream, slices,
	                      payload && payload->len ? 2U : 1U);
	if (ret == TR_OK) {
		if (call->rx_tail) {
			tr_record_state_merge_direction(
			        &call->state, &next,
			        call->config.send_direction);
			tr_record_state_merge_direction(
			        &call->rx_reserved_state, &next,
			        call->config.send_direction);
		} else {
			call->state = next;
			call->rx_reserved_state = next;
		}
		if (body)
			*body = NULL;
		/* 零字节消息不能省掉语义事件；原零长存储不必交给拒绝零长切片
		 * 的传输API。在清空调用方句柄并提交状态之后才执行释放钩子。
		 */
		if (empty_payload)
			tr_buffer_release(payload);
	} else {
		tr_buffer_release(&prefix->buffer);
	}
	call->sending = 0;
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
}

static void tr_record_rx_item_release(struct tr_rpc_record_stream *call,
                                      struct tr_rpc_record_rx_item *item)
{
	if (item->decoded._storage)
		(void)tr_message_release(&item->decoded);
	free(item->body);
	if (item->body_bytes)
		(void)tr_memory_budget_release(call->budget, item->body_bytes);
	size_t item_bytes = item->item_bytes;
	free(item);
	(void)tr_memory_budget_release(call->budget, item_bytes);
}

static void tr_record_rx_cancel(struct tr_rpc_record_stream *call);

static struct tr_rpc_record_rx_item *
tr_record_rx_item_create(struct tr_rpc_record_stream *call,
                         const struct tr_rpc_record_view *view,
                         const struct tr_rpc_record_state *next)
{
	struct tr_rpc_record_rx_item *item;
	if (tr_memory_budget_reserve(call->budget, sizeof(*item)) != TR_OK)
		return NULL;
	item = calloc(1, sizeof(*item));
	if (!item) {
		(void)tr_memory_budget_release(call->budget, sizeof(*item));
		return NULL;
	}
	item->call = call;
	item->item_bytes = sizeof(*item);
	item->header = view->header;
	item->metadata_size = view->header.metadata_size;
	item->next_state = *next;
	if (item->metadata_size)
		memcpy(item->metadata, view->metadata, item->metadata_size);
	return item;
}

static void tr_record_rx_append(struct tr_rpc_record_stream *call,
                                struct tr_rpc_record_rx_item *item)
{
	if (call->rx_tail)
		call->rx_tail->next = item;
	else
		call->rx_head = item;
	call->rx_tail = item;
	call->rx_reserved_state = item->next_state;
}

static int tr_record_rx_copy_body(struct tr_rpc_record_stream *call,
                                  struct tr_rpc_record_rx_item *item,
                                  const uint8_t *body)
{
	if (!item->header.encoded_size)
		return TR_OK;
	if (!body)
		return TR_ERR_INVALID;
	if (tr_memory_budget_reserve(call->budget, item->header.encoded_size) !=
	    TR_OK)
		return TR_ERR_NOMEM;
	item->body = malloc(item->header.encoded_size);
	if (!item->body) {
		(void)tr_memory_budget_release(call->budget,
		                               item->header.encoded_size);
		return TR_ERR_NOMEM;
	}
	item->body_bytes = item->header.encoded_size;
	memcpy(item->body, body, item->body_bytes);
	return TR_OK;
}

static enum tr_rpc_status
tr_record_transform_status(enum tr_message_transform_outcome outcome)
{
	switch (outcome) {
	case TR_MESSAGE_TRANSFORM_CANCELLED:
		return TR_RPC_STATUS_CANCELLED;
	case TR_MESSAGE_TRANSFORM_LIMIT:
	case TR_MESSAGE_TRANSFORM_NOMEM:
		return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	case TR_MESSAGE_TRANSFORM_BAD_DATA:
		return TR_RPC_STATUS_INVALID_ARGUMENT;
	case TR_MESSAGE_TRANSFORM_INTERNAL:
	default:
		return TR_RPC_STATUS_INTERNAL;
	}
}

static void tr_record_rx_flush(struct tr_rpc_record_stream *call,
                               enum tr_rpc_record_delivery delivery)
{
	while (call->rx_head && call->rx_head->ready) {
		struct tr_rpc_record_rx_item *item = call->rx_head;
		struct tr_rpc_record_state next;
		int ret;
		call->rx_head = item->next;
		if (!call->rx_head)
			call->rx_tail = NULL;
		if (call->state.terminal) {
			tr_record_rx_item_release(call, item);
			continue;
		}
		if (item->owner_stopped ||
		    delivery == TR_RPC_RECORD_OWNER_STOPPED) {
			tr_record_rx_item_release(call, item);
			continue;
		}
		ret = tr_record_preflight(call, &call->state,
		                          tr_record_receive_direction(call),
		                          &item->header, &next);
		if (ret != TR_OK) {
			(void)tr_rpc_record_state_abort(
			        &call->state, TR_RPC_STATUS_INVALID_ARGUMENT);
			tr_record_rx_cancel(call);
			tr_record_rx_item_release(call, item);
			continue;
		}
		struct tr_rpc_record_view view = {
		        .header = item->header,
		        .metadata = item->metadata_size ? item->metadata : NULL,
		        .body = item->body};
		if (item->decoded._storage) {
			struct tr_message_view decoded;
			if (tr_message_view(&item->decoded, &decoded) !=
			    TR_OK) {
				(void)tr_rpc_record_stream_abort_on_owner(
				        call, TR_RPC_STATUS_INTERNAL);
				tr_record_rx_item_release(call, item);
				continue;
			}
			view.body = decoded.data;
		}
		call->state = next;
		unsigned was_receiving = call->receiving;
		call->receiving = 1;
		++call->callback_depth;
		call->callbacks.received(call, &view, call->callbacks.arg);
		--call->callback_depth;
		call->receiving = was_receiving;
		tr_record_rx_item_release(call, item);
	}
	if (!call->rx_head) {
		call->rx_reserved_state = call->state;
		if (call->rx_pending_event) {
			enum tr_stream_event event = call->rx_pending_event;
			int status = call->rx_pending_status;
			call->rx_pending_event = 0;
			if (!call->state.terminal)
				(void)tr_rpc_record_stream_event_on_owner(
				        call, call->stream, event, status);
		}
	}
	if (!call->rx_head && call->tx_pending && call->tx_pending->ready &&
	    !call->state.terminal)
		(void)tr_record_tx_pump(call);
	tr_record_notify_terminal(call, delivery);
}

static void tr_record_rx_transform_done(void *arg, struct tr_task_handle handle,
                                        struct tr_message_service_result result,
                                        struct tr_message *message)
{
	struct tr_rpc_record_rx_item *item = arg;
	struct tr_rpc_record_stream *call = item->call;
	struct tr_stream_diagnostics stream_state;
	item->transform_active = 0;
	if (result.delivery == TR_MESSAGE_SERVICE_OWNER_STOPPED) {
		item->owner_stopped = 1;
		item->ready = 1;
		item->outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
		return;
	}
	if (handle.instance != item->task.instance ||
	    handle.generation != item->task.generation ||
	    handle.slot != item->task.slot) {
		item->outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
		item->ready = 1;
		if (!call->state.terminal)
			(void)tr_rpc_record_stream_abort_on_owner(
			        call, TR_RPC_STATUS_INTERNAL);
		else
			tr_record_rx_flush(call, TR_RPC_RECORD_ON_OWNER);
		return;
	}
	if (tr_stream_get_diagnostics(call->stream, &stream_state) != TR_OK) {
		item->outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
		item->ready = 1;
		if (!call->state.terminal)
			(void)tr_rpc_record_stream_abort_on_owner(
			        call, TR_RPC_STATUS_UNAVAILABLE);
		else
			tr_record_rx_flush(call, TR_RPC_RECORD_ON_OWNER);
		return;
	}
	item->outcome = result.outcome;
	if (result.outcome == TR_MESSAGE_TRANSFORM_OK &&
	    !call->state.terminal) {
		if (!message ||
		    tr_message_clone(message, &item->decoded) != TR_OK)
			item->outcome = TR_MESSAGE_TRANSFORM_NOMEM;
	}
	item->ready = 1;
	if (item->outcome != TR_MESSAGE_TRANSFORM_OK && !call->state.terminal) {
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, tr_record_transform_status(item->outcome));
		return;
	}
	tr_record_rx_flush(call, TR_RPC_RECORD_ON_OWNER);
}

static int tr_record_rx_enqueue(struct tr_rpc_record_stream *call,
                                const struct tr_rpc_record_view *view,
                                const struct tr_rpc_record_state *next)
{
	struct tr_rpc_record_rx_item *item =
	        tr_record_rx_item_create(call, view, next);
	int ret;
	if (!item)
		return TR_ERR_NOMEM;
	if (view->header.compression == TR_RPC_RECORD_ZSTD) {
		struct tr_message_builder *builder = NULL;
		struct tr_message input = {0};
		struct tr_message_transform_config config = {
		        .kind = TR_MESSAGE_ZSTD_DECODE,
		        .limits = {call->rx_limits.max_decoded_size,
		                   call->rx_limits.max_encoded_size},
		        .decoded_size = view->header.decoded_size,
		        .max_workspace_bytes = call->rx_workspace_bytes,
		        .max_window_log = call->rx_window_log};
		ret = tr_message_builder_create(
		        call->budget, view->header.encoded_size, &builder);
		if (ret == TR_OK)
			ret = tr_message_builder_append(
			        builder, view->body, view->header.encoded_size);
		if (ret == TR_OK)
			ret = tr_message_builder_publish(&builder, &input);
		if (ret == TR_OK)
			ret = tr_channel_message_submit_on_owner(
			        call->stream.channel, &input, &config,
			        tr_record_rx_transform_done, item, &item->task);
		if (builder)
			(void)tr_message_builder_cancel(&builder);
		if (input._storage)
			(void)tr_message_release(&input);
		if (ret != TR_OK) {
			tr_record_rx_item_release(call, item);
			return ret == TR_AGAIN ? TR_ERR_NOMEM : ret;
		}
		item->transform_active = 1;
	} else {
		ret = tr_record_rx_copy_body(call, item, view->body);
		if (ret != TR_OK) {
			tr_record_rx_item_release(call, item);
			return ret;
		}
		item->ready = 1;
		item->outcome = TR_MESSAGE_TRANSFORM_OK;
	}
	tr_record_rx_append(call, item);
	tr_record_rx_flush(call, TR_RPC_RECORD_ON_OWNER);
	return TR_OK;
}

static int tr_record_accept(struct tr_rpc_record_stream *call,
                            const uint8_t *bytes, size_t size)
{
	struct tr_rpc_record_view view;
	struct tr_rpc_record_state next;
	const struct tr_rpc_record_state *base =
	        call->rx_tail ? &call->rx_reserved_state : &call->state;
	size_t used;
	int ret = tr_rpc_record_decode(bytes, size, &call->rx_limits, &view,
	                               &used);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_metadata_valid(view.metadata,
	                               view.header.metadata_size);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_preflight(call, base, tr_record_receive_direction(call),
	                          &view.header, &next);
	if (ret != TR_OK)
		return ret;
	if (view.header.compression == TR_RPC_RECORD_ZSTD || call->rx_tail)
		return tr_record_rx_enqueue(call, &view, &next);
	call->state = next;
	++call->callback_depth;
	call->callbacks.received(call, &view, call->callbacks.arg);
	--call->callback_depth;
	return TR_OK;
}

static int tr_record_allocate_partial(struct tr_rpc_record_stream *call,
                                      size_t size)
{
	int ret = tr_memory_budget_reserve(call->budget, size);
	if (ret != TR_OK)
		return TR_ERR_NOMEM;
	call->partial = malloc(size);
	if (!call->partial) {
		(void)tr_memory_budget_release(call->budget, size);
		return TR_ERR_NOMEM;
	}
	call->partial_size = size;
	return TR_OK;
}

int tr_rpc_record_stream_receive_on_owner(struct tr_rpc_record_stream *call,
                                          struct tr_stream_handle stream,
                                          const uint8_t *data, size_t size)
{
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (!tr_record_same_stream(call, stream))
		return TR_ERR_STALE;
	if (!data && size)
		return TR_ERR_INVALID;
	if (call->receiving || call->sending || call->state.terminal)
		return TR_ERR_STATE;
	call->receiving = 1;
	while (size) {
		struct tr_rpc_record_header header;
		struct tr_rpc_record_state next;
		size_t total, amount;
		if (call->state.terminal) {
			ret = TR_ERR_STATE;
			break;
		}
		if (call->partial) {
			amount = call->partial_size - call->partial_used;
			if (amount > size)
				amount = size;
			memcpy(call->partial + call->partial_used, data,
			       amount);
			data += amount;
			size -= amount;
			call->partial_used += amount;
			if (call->partial_used == call->partial_size) {
				ret = tr_record_accept(call, call->partial,
				                       call->partial_size);
				tr_record_clear_partial(call);
				if (ret != TR_OK)
					break;
			}
			continue;
		}
		if (call->header_used) {
			amount = sizeof(call->header) - call->header_used;
			if (amount > size)
				amount = size;
			memcpy(call->header + call->header_used, data, amount);
			data += amount;
			size -= amount;
			call->header_used += amount;
			ret = tr_rpc_record_decode_header(
			        call->header, call->header_used,
			        &call->rx_limits, &header, &total);
		} else {
			ret = tr_rpc_record_decode_header(
			        data, size, &call->rx_limits, &header, &total);
			if (ret == TR_AGAIN) {
				/* 解码器只有不足固定头时才返回AGAIN。 */
				memcpy(call->header, data, size);
				call->header_used = size;
			}
		}
		if (ret == TR_AGAIN) {
			ret = TR_OK;
			break;
		}
		if (ret != TR_OK)
			break;
		/* 在正文预约前验证本地方法及事件顺序。身份授权不由方法ID代替；
		 * 上层可在完整 OPEN 的 received 回调选择拒绝终局。
		 */
		const struct tr_rpc_record_state *base =
		        call->rx_tail ? &call->rx_reserved_state : &call->state;
		ret = tr_record_preflight(call, base,
		                          tr_record_receive_direction(call),
		                          &header, &next);
		if (ret != TR_OK)
			break;
		if (!call->header_used && size >= total) {
			ret = tr_record_accept(call, data, total);
			data += total;
			size -= total;
			if (ret != TR_OK)
				break;
			continue;
		}
		if (call->header_used && total == sizeof(call->header)) {
			ret = tr_record_accept(call, call->header, total);
			call->header_used = 0;
			if (ret != TR_OK)
				break;
			continue;
		}
		ret = tr_record_allocate_partial(call, total);
		if (ret != TR_OK)
			break;
		if (call->header_used) {
			memcpy(call->partial, call->header, call->header_used);
			call->partial_used = call->header_used;
			call->header_used = 0;
		} else {
			memcpy(call->partial, data, size);
			call->partial_used = size;
			size = 0;
		}
	}
	if (ret != TR_OK && !call->state.terminal)
		(void)tr_rpc_record_stream_abort_on_owner(
		        call, ret == TR_ERR_NOMEM
		                      ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
		                      : TR_RPC_STATUS_INVALID_ARGUMENT);
	if (call->state.terminal)
		tr_record_clear_partial(call);
	call->receiving = 0;
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
}

static void tr_record_rx_cancel(struct tr_rpc_record_stream *call)
{
	for (struct tr_rpc_record_rx_item *item = call->rx_head; item;
	     item = item->next) {
		if (item->transform_active)
			(void)tr_channel_message_cancel_on_owner(
			        call->stream.channel, item->task);
	}
}

int tr_rpc_record_stream_abort_on_owner(struct tr_rpc_record_stream *call,
                                        enum tr_rpc_status status)
{
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (call->sending)
		return TR_ERR_STATE;
	ret = tr_rpc_record_state_abort(&call->state, status);
	if (ret == TR_OK) {
		if (!call->receiving)
			tr_record_clear_partial(call);
		tr_record_rx_cancel(call);
		tr_record_tx_cancel(call);
		tr_record_rx_flush(call, TR_RPC_RECORD_ON_OWNER);
		tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	}
	return ret;
}

int tr_rpc_record_stream_event_on_owner(struct tr_rpc_record_stream *call,
                                        struct tr_stream_handle stream,
                                        enum tr_stream_event event, int status)
{
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (!tr_record_same_stream(call, stream))
		return TR_ERR_STALE;
	if (call->receiving || call->sending)
		return TR_ERR_STATE;
	if (call->state.terminal)
		return TR_OK;
	if (event == TR_STREAM_EVENT_OPENED)
		return TR_OK;
	if (event == TR_STREAM_EVENT_WRITABLE) {
		ret = tr_record_tx_pump(call);
		return ret == TR_AGAIN ? TR_OK : ret;
	}
	if (event == TR_STREAM_EVENT_REFUSED && status != TR_ERR_NOMEM)
		return TR_ERR_INVALID;
	if ((event == TR_STREAM_EVENT_RESET ||
	     event == TR_STREAM_EVENT_REFUSED) &&
	    tr_rpc_record_status_from_reset(status) ==
	            (enum tr_rpc_status)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	/* CLOSE 在线上排在此前 DATA 之后；压缩变换尚未提交时，先保留
	 * 正常远端关闭，让此前排队的 RPC 记录决定是否已有合法 FINAL。
	 * RESET/ERROR 仍立即取消变换并按原传输原因终止。 */
	if (call->rx_head && event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		if (!call->rx_pending_event ||
		    call->rx_pending_event == TR_STREAM_EVENT_REMOTE_CLOSED) {
			call->rx_pending_event = event;
			call->rx_pending_status = status;
		}
		return TR_OK;
	}
	if (event == TR_STREAM_EVENT_RESET ||
	    event == TR_STREAM_EVENT_REFUSED) {
		enum tr_rpc_status result =
		        tr_rpc_record_status_from_reset(status);
		/* RESET 取消整个调用，合法的 RPC HALF_CLOSE 不屏蔽它。
		 * state_abort 保留先前已接纳的 FINAL，且缓存只释放一次。 */
		return tr_rpc_record_stream_abort_on_owner(call, result);
	}
	if (event != TR_STREAM_EVENT_REMOTE_CLOSED &&
	    event != TR_STREAM_EVENT_CLOSED && event != TR_STREAM_EVENT_ERROR)
		return TR_ERR_INVALID;
	if (event == TR_STREAM_EVENT_REMOTE_CLOSED && !call->header_used &&
	    !call->partial &&
	    call->config.send_direction == TR_RPC_RECORD_RESPONSE_DIRECTION &&
	    call->state.request_closed)
		return TR_OK;
	return tr_rpc_record_stream_abort_on_owner(
	        call, call->partial || call->header_used
	                      ? TR_RPC_STATUS_DATA_LOSS
	                      : TR_RPC_STATUS_UNAVAILABLE);
}

int tr_rpc_record_stream_snapshot_on_owner(struct tr_rpc_record_stream *call,
                                           struct tr_rpc_record_state *out,
                                           size_t *cached_bytes)
{
	int ret = tr_record_owner(call);
	if (ret != TR_OK)
		return ret;
	if (!out || !cached_bytes)
		return TR_ERR_INVALID;
	*out = call->state;
	*cached_bytes = call->partial_size;
	return TR_OK;
}

static int tr_record_destroy(struct tr_rpc_record_stream **pointer)
{
	struct tr_rpc_record_stream *call = *pointer;
	struct tr_memory_budget *budget = call->budget;
	tr_record_clear_partial(call);
	*pointer = NULL;
	free(call);
	return tr_memory_budget_release(budget, sizeof(*call));
}

int tr_rpc_record_stream_destroy_on_owner(struct tr_rpc_record_stream **pointer)
{
	int ret;
	if (!pointer)
		return TR_ERR_INVALID;
	ret = tr_record_owner(*pointer);
	if (ret != TR_OK)
		return ret;
	if ((*pointer)->receiving || (*pointer)->sending ||
	    (*pointer)->callback_depth || (*pointer)->rx_head ||
	    (*pointer)->tx_pending ||
	    ((*pointer)->state.opened && !(*pointer)->state.terminal))
		return TR_ERR_STATE;
	return tr_record_destroy(pointer);
}

int tr_rpc_record_stream_reap_stopped(struct tr_rpc_record_stream **pointer)
{
	struct tr_reactor_recovery_guard guard = {0};
	struct tr_rpc_record_stream *call;
	int ret;
	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	call = *pointer;
	/* 先取得父循环静默证明，再读取owner字段；不把正在停止当成已静默。 */
	ret = tr_reactor_recovery_acquire(call->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (call->receiving || call->sending || call->callback_depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	for (struct tr_rpc_record_rx_item *item = call->rx_head; item;
	     item = item->next) {
		if (item->transform_active) {
			(void)tr_reactor_recovery_release(&guard);
			return TR_AGAIN;
		}
	}
	if (call->tx_pending && call->tx_pending->transform_active) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_AGAIN;
	}
	if (call->state.opened && !call->state.terminal)
		(void)tr_rpc_record_state_abort(&call->state,
		                                TR_RPC_STATUS_CANCELLED);
	tr_record_clear_partial(call);
	while (call->rx_head) {
		struct tr_rpc_record_rx_item *item = call->rx_head;
		call->rx_head = item->next;
		tr_record_rx_item_release(call, item);
	}
	call->rx_tail = NULL;
	if (call->tx_pending)
		tr_record_tx_item_release(call, call->tx_pending);
	tr_record_notify_terminal(call, TR_RPC_RECORD_OWNER_STOPPED);
	ret = tr_reactor_recovery_release(&guard);
	return ret == TR_OK ? tr_record_destroy(pointer) : ret;
}
