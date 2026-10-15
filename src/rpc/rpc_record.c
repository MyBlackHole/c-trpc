#include "rpc_record.h"
#include "../endian.h"
#include "tr/status.h"

#include <string.h>

static int tr_rpc_record_kind_valid(enum tr_rpc_record_kind kind)
{
	return kind >= TR_RPC_RECORD_OPEN && kind <= TR_RPC_RECORD_FINAL_STATUS;
}

static int tr_rpc_record_limits_valid(const struct tr_rpc_record_limits *limits)
{
	const uint32_t supported =
	        TR_RPC_RECORD_ACCEPT_NONE | TR_RPC_RECORD_ACCEPT_ZSTD;

	return limits &&
	       limits->max_metadata_size <= TR_RPC_METADATA_MAX_BYTES &&
	       (limits->accepted_compression & TR_RPC_RECORD_ACCEPT_NONE) &&
	       !(limits->accepted_compression & ~supported);
}

static int tr_rpc_record_validate(const struct tr_rpc_record_header *h,
                                  const struct tr_rpc_record_limits *limits,
                                  size_t *total)
{
	size_t size;

	if (!h || !tr_rpc_record_limits_valid(limits))
		return TR_ERR_INVALID;
	if (!tr_rpc_record_kind_valid(h->kind))
		return TR_ERR_BAD_TYPE;
	if (h->metadata_size > limits->max_metadata_size ||
	    h->encoded_size > limits->max_encoded_size ||
	    h->decoded_size > limits->max_decoded_size)
		return TR_ERR_BAD_LENGTH;
	/* 先减再加；32位size_t不能因长载荷与头相加回绕成可接受的小记录。 */
	size = TR_RPC_RECORD_HEADER_SIZE + (size_t)h->metadata_size;
	if (h->encoded_size > SIZE_MAX - size)
		return TR_ERR_BAD_LENGTH;
	size += h->encoded_size;
	if (h->kind != TR_RPC_RECORD_OPEN &&
	    (h->service_id || h->method_id || h->remaining_timeout_ns))
		return TR_ERR_BAD_FLAGS;
	if (h->kind != TR_RPC_RECORD_FINAL_STATUS && h->status)
		return TR_ERR_BAD_FLAGS;
	if (h->kind != TR_RPC_RECORD_MESSAGE) {
		if (h->codec_id || h->compression || h->encoded_size ||
		    h->decoded_size)
			return TR_ERR_BAD_FLAGS;
		if (h->kind == TR_RPC_RECORD_HALF_CLOSE && h->metadata_size)
			return TR_ERR_BAD_FLAGS;
		if (h->kind == TR_RPC_RECORD_FINAL_STATUS &&
		    h->status >= TR_RPC_STATUS_COUNT)
			return TR_ERR_BAD_TYPE;
	} else {
		if (h->metadata_size)
			return TR_ERR_BAD_FLAGS;
		if (!h->codec_id)
			return TR_ERR_BAD_TYPE;
		/* 在位移前验证算法ID，避免远端值造成未定义位移。 */
		if (h->compression > TR_RPC_RECORD_ZSTD ||
		    !(limits->accepted_compression & (1U << h->compression)))
			return TR_ERR_UNSUPPORTED;
		if ((h->compression == TR_RPC_RECORD_NONE &&
		     h->encoded_size != h->decoded_size) ||
		    (h->compression != TR_RPC_RECORD_NONE && !h->encoded_size))
			return TR_ERR_BAD_LENGTH;
	}
	*total = size;
	return TR_OK;
}

int tr_rpc_record_encode_header(uint8_t *out, size_t capacity,
                                const struct tr_rpc_record_header *header,
                                const struct tr_rpc_record_limits *limits)
{
	uint8_t bytes[TR_RPC_RECORD_HEADER_SIZE] = {'T', 'R', 'P', 'C'};
	size_t total;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	ret = tr_rpc_record_validate(header, limits, &total);
	if (ret != TR_OK)
		return ret;
	if (capacity < sizeof(bytes))
		return TR_ERR_BAD_LENGTH;
	tr_put_le16(bytes + 4, TR_RPC_RECORD_VERSION);
	tr_put_le16(bytes + 6, (uint16_t)header->kind);
	tr_put_le32(bytes + 8, header->service_id);
	tr_put_le32(bytes + 12, header->method_id);
	tr_put_le32(bytes + 16, header->codec_id);
	tr_put_le32(bytes + 20, header->compression);
	tr_put_le32(bytes + 24, header->encoded_size);
	tr_put_le32(bytes + 28, header->decoded_size);
	tr_put_le32(bytes + 32, header->metadata_size);
	tr_put_le32(bytes + 36, header->status);
	tr_put_le64(bytes + 40, header->remaining_timeout_ns);
	memcpy(out, bytes, sizeof(bytes));
	return TR_OK;
}

int tr_rpc_record_decode_header(const uint8_t *data, size_t size,
                                const struct tr_rpc_record_limits *limits,
                                struct tr_rpc_record_header *out,
                                size_t *record_size)
{
	struct tr_rpc_record_header h = {0};
	size_t total;
	int ret;

	if ((!data && size) || !out || !record_size ||
	    !tr_rpc_record_limits_valid(limits))
		return TR_ERR_INVALID;
	if (size < 4)
		return TR_AGAIN;
	if (memcmp(data, "TRPC", 4))
		return TR_ERR_BAD_MAGIC;
	if (size < 6)
		return TR_AGAIN;
	if (tr_get_le16(data + 4) != TR_RPC_RECORD_VERSION)
		return TR_ERR_BAD_VERSION;
	if (size < 8)
		return TR_AGAIN;
	h.kind = (enum tr_rpc_record_kind)tr_get_le16(data + 6);
	if (!tr_rpc_record_kind_valid(h.kind))
		return TR_ERR_BAD_TYPE;
	if (size < TR_RPC_RECORD_HEADER_SIZE)
		return TR_AGAIN;
	h.service_id = tr_get_le32(data + 8);
	h.method_id = tr_get_le32(data + 12);
	h.codec_id = tr_get_le32(data + 16);
	h.compression = tr_get_le32(data + 20);
	h.encoded_size = tr_get_le32(data + 24);
	h.decoded_size = tr_get_le32(data + 28);
	h.metadata_size = tr_get_le32(data + 32);
	h.status = tr_get_le32(data + 36);
	h.remaining_timeout_ns = tr_get_le64(data + 40);
	ret = tr_rpc_record_validate(&h, limits, &total);
	if (ret != TR_OK)
		return ret;
	*out = h;
	*record_size = total;
	return TR_OK;
}

int tr_rpc_record_decode(const uint8_t *data, size_t size,
                         const struct tr_rpc_record_limits *limits,
                         struct tr_rpc_record_view *out, size_t *consumed)
{
	struct tr_rpc_record_view view = {0};
	size_t total;
	int ret;

	if (!out || !consumed)
		return TR_ERR_INVALID;
	ret = tr_rpc_record_decode_header(data, size, limits, &view.header,
	                                  &total);
	if (ret != TR_OK)
		return ret;
	if (size < total)
		return TR_AGAIN;
	if (view.header.metadata_size)
		view.metadata = data + TR_RPC_RECORD_HEADER_SIZE;
	if (view.header.encoded_size)
		view.body = data + TR_RPC_RECORD_HEADER_SIZE +
		            view.header.metadata_size;
	*out = view;
	*consumed = total;
	return TR_OK;
}

static int tr_rpc_record_cardinality_valid(enum tr_rpc_cardinality value)
{
	return value == TR_RPC_ONE || value == TR_RPC_MANY;
}

int tr_rpc_record_state_init(struct tr_rpc_record_state *state,
                             enum tr_rpc_cardinality request,
                             enum tr_rpc_cardinality response)
{
	if (!state || !tr_rpc_record_cardinality_valid(request) ||
	    !tr_rpc_record_cardinality_valid(response))
		return TR_ERR_INVALID;
	*state = (struct tr_rpc_record_state){.request_cardinality = request,
	                                      .response_cardinality = response,
	                                      .initialized = 1};
	return TR_OK;
}

int tr_rpc_record_state_apply(struct tr_rpc_record_state *state,
                              enum tr_rpc_record_direction direction,
                              const struct tr_rpc_record_header *record)
{
	const struct tr_rpc_record_limits shape = {
	        UINT32_MAX, UINT32_MAX, TR_RPC_METADATA_MAX_BYTES,
	        TR_RPC_RECORD_ACCEPT_NONE | TR_RPC_RECORD_ACCEPT_ZSTD};
	struct tr_rpc_record_state next;
	size_t total;
	int ret;

	if (!state || !record || !state->initialized ||
	    (direction != TR_RPC_RECORD_REQUEST_DIRECTION &&
	     direction != TR_RPC_RECORD_RESPONSE_DIRECTION))
		return TR_ERR_INVALID;
	if (state->terminal)
		return TR_ERR_STATE;
	ret = tr_rpc_record_validate(record, &shape, &total);
	if (ret != TR_OK)
		return ret;
	next = *state;
	if (!next.opened) {
		if (record->kind != TR_RPC_RECORD_OPEN ||
		    direction != TR_RPC_RECORD_REQUEST_DIRECTION)
			return TR_ERR_STATE;
		next.opened = 1;
	} else if (direction == TR_RPC_RECORD_REQUEST_DIRECTION) {
		if (next.request_closed)
			return TR_ERR_STATE;
		if (record->kind == TR_RPC_RECORD_MESSAGE) {
			if (next.request_messages == UINT64_MAX ||
			    (next.request_cardinality == TR_RPC_ONE &&
			     next.request_messages != 0))
				return TR_ERR_STATE;
			++next.request_messages;
		} else if (record->kind == TR_RPC_RECORD_HALF_CLOSE) {
			if (next.request_cardinality == TR_RPC_ONE &&
			    next.request_messages != 1)
				return TR_ERR_STATE;
			next.request_closed = 1;
		} else {
			return TR_ERR_STATE;
		}
	} else {
		switch (record->kind) {
		case TR_RPC_RECORD_INITIAL_METADATA:
			if (next.response_headers)
				return TR_ERR_STATE;
			next.response_headers = 1;
			break;
		case TR_RPC_RECORD_MESSAGE:
			if (!next.response_headers ||
			    next.response_messages == UINT64_MAX ||
			    (next.response_cardinality == TR_RPC_ONE &&
			     next.response_messages != 0))
				return TR_ERR_STATE;
			++next.response_messages;
			break;
		case TR_RPC_RECORD_FINAL_STATUS:
			if (record->status == TR_RPC_STATUS_OK &&
			    ((next.request_cardinality == TR_RPC_ONE &&
			      next.request_messages != 1) ||
			     (next.response_cardinality == TR_RPC_ONE &&
			      next.response_messages != 1)))
				return TR_ERR_STATE;
			next.terminal = 1;
			next.final_status = record->status;
			break;
		default:
			return TR_ERR_STATE;
		}
	}
	*state = next;
	return TR_OK;
}

int tr_rpc_record_state_abort(struct tr_rpc_record_state *state,
                              enum tr_rpc_status status)
{
	if (!state || !state->initialized || status <= TR_RPC_STATUS_OK ||
	    status >= (int)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	if (state->terminal)
		return TR_ERR_STATE;
	state->terminal = 1;
	state->final_status = (uint32_t)status;
	return TR_OK;
}
