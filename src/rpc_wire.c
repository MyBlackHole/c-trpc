#include "tr/rpc_wire.h"

#include "tr/endian.h"
#include "tr/status.h"

#include <stddef.h>
#include <string.h>

static int tr_rpc_wire_type_valid(uint16_t type)
{
	return type == TR_RPC_WIRE_REQUEST || type == TR_RPC_WIRE_RESPONSE ||
	       type == TR_RPC_WIRE_CANCEL || type == TR_RPC_WIRE_STATUS;
}

int tr_rpc_wire_encode(uint8_t out[TR_RPC_WIRE_HEADER_SIZE],
		       const struct tr_rpc_wire_header *header)
{
	if (!out || !header || !tr_rpc_wire_type_valid(header->type) ||
	    header->version != TR_RPC_WIRE_VERSION ||
	    (header->flags & ~TR_RPC_WIRE_VALID_FLAGS) != 0)
		return TR_ERR_INVALID;

	out[0] = 'T';
	out[1] = 'R';
	out[2] = 'P';
	out[3] = 'C';
	tr_put_le16(out + TR_RPC_WIRE_OFF_VERSION, header->version);
	tr_put_le16(out + TR_RPC_WIRE_OFF_TYPE, header->type);
	tr_put_le32(out + TR_RPC_WIRE_OFF_SERVICE_ID, header->service_id);
	tr_put_le32(out + TR_RPC_WIRE_OFF_METHOD_ID, header->method_id);
	tr_put_le32(out + TR_RPC_WIRE_OFF_CODEC_ID, header->codec_id);
	tr_put_le32(out + TR_RPC_WIRE_OFF_FLAGS, header->flags);
	tr_put_le32(out + TR_RPC_WIRE_OFF_STATUS, (uint32_t)header->status);
	tr_put_le32(out + TR_RPC_WIRE_OFF_PAYLOAD_LEN, header->payload_len);
	return TR_OK;
}

int tr_rpc_wire_decode_ex(const uint8_t *data, uint32_t len,
			  struct tr_rpc_wire_header *header,
			  const uint8_t **metadata, uint16_t *metadata_len,
			  const uint8_t **payload)
{
	const uint8_t *body;
	uint32_t body_len;
	uint32_t expected;
	uint16_t md_len = 0;

	if (!data || !header || !metadata || !metadata_len || !payload ||
	    len < TR_RPC_WIRE_HEADER_SIZE)
		return TR_ERR_BAD_LENGTH;

	if (data[0] != 'T' || data[1] != 'R' || data[2] != 'P' ||
	    data[3] != 'C')
		return TR_ERR_BAD_MAGIC;

	memset(header, 0, sizeof(*header));
	header->version = tr_get_le16(data + TR_RPC_WIRE_OFF_VERSION);
	header->type = tr_get_le16(data + TR_RPC_WIRE_OFF_TYPE);
	header->service_id = tr_get_le32(data + TR_RPC_WIRE_OFF_SERVICE_ID);
	header->method_id = tr_get_le32(data + TR_RPC_WIRE_OFF_METHOD_ID);
	header->codec_id = tr_get_le32(data + TR_RPC_WIRE_OFF_CODEC_ID);
	header->flags = tr_get_le32(data + TR_RPC_WIRE_OFF_FLAGS);
	header->status = (int32_t)tr_get_le32(data + TR_RPC_WIRE_OFF_STATUS);
	header->payload_len = tr_get_le32(data + TR_RPC_WIRE_OFF_PAYLOAD_LEN);

	if (header->version != TR_RPC_WIRE_VERSION)
		return TR_ERR_BAD_VERSION;
	if (!tr_rpc_wire_type_valid(header->type))
		return TR_ERR_BAD_TYPE;
	if ((header->flags & ~TR_RPC_WIRE_VALID_FLAGS) != 0)
		return TR_ERR_BAD_FLAGS;

	body = data + TR_RPC_WIRE_HEADER_SIZE;
	body_len = len - TR_RPC_WIRE_HEADER_SIZE;
	*metadata = NULL;
	*metadata_len = 0;

	if ((header->flags & TR_RPC_WIRE_F_METADATA) != 0) {
		if (body_len < TR_RPC_WIRE_METADATA_PREFIX_SIZE)
			return TR_ERR_BAD_LENGTH;
		md_len = tr_get_le16(body);
		if (md_len > TR_RPC_WIRE_MAX_METADATA_BYTES)
			return TR_ERR_BAD_LENGTH;
		expected = TR_RPC_WIRE_METADATA_PREFIX_SIZE + (uint32_t)md_len;
		if (expected > body_len ||
		    header->payload_len != body_len - expected)
			return TR_ERR_BAD_LENGTH;
		*metadata = body + TR_RPC_WIRE_METADATA_PREFIX_SIZE;
		*metadata_len = md_len;
		*payload = body + expected;
		return TR_OK;
	}

	if (header->payload_len != body_len)
		return TR_ERR_BAD_LENGTH;
	*payload = body;
	return TR_OK;
}

int tr_rpc_wire_decode(const uint8_t *data, uint32_t len,
		       struct tr_rpc_wire_header *header,
		       const uint8_t **payload)
{
	const uint8_t *metadata;
	uint16_t metadata_len;

	return tr_rpc_wire_decode_ex(data, len, header, &metadata,
				     &metadata_len, payload);
}
