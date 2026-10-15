#include "rpc_record_stream.h"
#include "../endian.h"
#include "../memory_budget.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

struct tr_rpc_record_stream {
	struct tr_stream_handle stream;
	struct tr_reactor *reactor;
	struct tr_memory_budget *budget;
	struct tr_rpc_record_stream_config config;
	struct tr_rpc_record_stream_callbacks callbacks;
	struct tr_rpc_record_state state;
	struct tr_rpc_record_limits tx_limits;
	struct tr_rpc_record_limits rx_limits;
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
	    call->receiving || call->sending || call->callback_depth)
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
	*next = call->state;
	return tr_rpc_record_state_apply(next, direction, header);
}

int tr_rpc_record_stream_create_on_owner(
        struct tr_stream_handle stream,
        const struct tr_rpc_record_stream_config *config,
        const struct tr_rpc_record_stream_callbacks *callbacks,
        struct tr_memory_budget *budget, struct tr_rpc_record_stream **out)
{
	struct tr_rpc_record_stream *call;
	struct tr_rpc_record_state state;
	struct tr_stream_diagnostics stream_state;
	struct tr_reactor *reactor;
	uint32_t tx_size, rx_size;
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
	    !config->method.response_codec_id)
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
	tx_size = config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                  ? config->method.max_request_bytes
	                  : config->method.max_response_bytes;
	rx_size = config->send_direction == TR_RPC_RECORD_REQUEST_DIRECTION
	                  ? config->method.max_response_bytes
	                  : config->method.max_request_bytes;
	call->tx_limits = (struct tr_rpc_record_limits){
	        tx_size, tx_size, TR_RPC_METADATA_MAX_BYTES,
	        TR_RPC_RECORD_ACCEPT_NONE};
	call->rx_limits = (struct tr_rpc_record_limits){
	        rx_size, rx_size, TR_RPC_METADATA_MAX_BYTES,
	        TR_RPC_RECORD_ACCEPT_NONE};
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

int tr_rpc_record_stream_send_on_owner(
        struct tr_rpc_record_stream *call,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body)
{
	struct tr_rpc_record_state next;
	struct tr_rpc_record_prefix *prefix;
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
	ret = tr_record_preflight(call, call->config.send_direction, header,
	                          &next);
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
		call->state = next;
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

static int tr_record_accept(struct tr_rpc_record_stream *call,
                            const uint8_t *bytes, size_t size)
{
	struct tr_rpc_record_view view;
	struct tr_rpc_record_state next;
	size_t used;
	int ret = tr_rpc_record_decode(bytes, size, &call->rx_limits, &view,
	                               &used);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_metadata_valid(view.metadata,
	                               view.header.metadata_size);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_preflight(call, tr_record_receive_direction(call),
	                          &view.header, &next);
	if (ret != TR_OK)
		return ret;
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
		ret = tr_record_preflight(call,
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
		(void)tr_rpc_record_state_abort(
		        &call->state, ret == TR_ERR_NOMEM
		                              ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
		                              : TR_RPC_STATUS_INVALID_ARGUMENT);
	if (call->state.terminal)
		tr_record_clear_partial(call);
	call->receiving = 0;
	tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	return ret;
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
		tr_record_notify_terminal(call, TR_RPC_RECORD_ON_OWNER);
	}
	return ret;
}

int tr_rpc_record_stream_event_on_owner(struct tr_rpc_record_stream *call,
                                        struct tr_stream_handle stream,
                                        enum tr_stream_event event)
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
	if (event == TR_STREAM_EVENT_OPENED ||
	    event == TR_STREAM_EVENT_WRITABLE)
		return TR_OK;
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
	    (*pointer)->callback_depth ||
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
	if (call->state.opened && !call->state.terminal)
		(void)tr_rpc_record_state_abort(&call->state,
		                                TR_RPC_STATUS_CANCELLED);
	tr_record_notify_terminal(call, TR_RPC_RECORD_OWNER_STOPPED);
	ret = tr_reactor_recovery_release(&guard);
	return ret == TR_OK ? tr_record_destroy(pointer) : ret;
}
