#include "tr/wire.h"

#include "tr/crc32c.h"
#include "tr/endian.h"
#include "tr/status.h"

#include <string.h>

static const uint8_t tr_magic[4] = { 'T', 'R', 'P', '1' };

int tr_frame_type_valid(uint16_t type)
{
	switch (type) {
	case TR_FRAME_DATA:
	case TR_FRAME_STREAM_OPEN:
	case TR_FRAME_STREAM_CLOSE:
	case TR_FRAME_WINDOW_UPDATE:
	case TR_FRAME_PING:
	case TR_FRAME_PONG:
	case TR_FRAME_ERROR:
	case TR_FRAME_HELLO:
	case TR_FRAME_HELLO_ACK:
	case TR_FRAME_GOAWAY:
		return 1;
	default:
		return 0;
	}
}

static uint32_t tr_header_crc(const uint8_t raw[TR_WIRE_HEADER_SIZE])
{
	uint8_t tmp[TR_WIRE_HEADER_SIZE];

	memcpy(tmp, raw, sizeof(tmp));
	memset(tmp + TR_WIRE_OFF_HEADER_CRC, 0, sizeof(uint32_t));
	return tr_crc32c(tmp, sizeof(tmp));
}

int tr_wire_header_encode(uint8_t out[TR_WIRE_HEADER_SIZE],
			  const struct tr_frame_header *header)
{
	uint32_t crc;

	if (!out || !header)
		return TR_ERR_INVALID;

	memset(out, 0, TR_WIRE_HEADER_SIZE);
	memcpy(out + TR_WIRE_OFF_MAGIC, tr_magic, sizeof(tr_magic));

	tr_put_le16(out + TR_WIRE_OFF_VERSION, header->version);
	tr_put_le16(out + TR_WIRE_OFF_TYPE, header->type);
	tr_put_le32(out + TR_WIRE_OFF_FLAGS, header->flags);
	tr_put_le32(out + TR_WIRE_OFF_STREAM_ID, header->stream_id);
	tr_put_le64(out + TR_WIRE_OFF_MESSAGE_ID, header->message_id);
	tr_put_le32(out + TR_WIRE_OFF_PAYLOAD_LEN, header->payload_len);
	tr_put_le32(out + TR_WIRE_OFF_PAYLOAD_CRC, header->payload_crc32c);
	tr_put_le32(out + TR_WIRE_OFF_HEADER_CRC, 0);
	tr_put_le32(out + TR_WIRE_OFF_RESERVED, header->reserved);

	crc = tr_header_crc(out);
	tr_put_le32(out + TR_WIRE_OFF_HEADER_CRC, crc);

	return TR_OK;
}

int tr_wire_header_decode(const uint8_t in[TR_WIRE_HEADER_SIZE],
			  struct tr_frame_header *header)
{
	if (!in || !header)
		return TR_ERR_INVALID;

	memset(header, 0, sizeof(*header));

	header->version = tr_get_le16(in + TR_WIRE_OFF_VERSION);
	header->type = tr_get_le16(in + TR_WIRE_OFF_TYPE);
	header->flags = tr_get_le32(in + TR_WIRE_OFF_FLAGS);
	header->stream_id = tr_get_le32(in + TR_WIRE_OFF_STREAM_ID);
	header->message_id = tr_get_le64(in + TR_WIRE_OFF_MESSAGE_ID);
	header->payload_len = tr_get_le32(in + TR_WIRE_OFF_PAYLOAD_LEN);
	header->payload_crc32c = tr_get_le32(in + TR_WIRE_OFF_PAYLOAD_CRC);
	header->header_crc32c = tr_get_le32(in + TR_WIRE_OFF_HEADER_CRC);
	header->reserved = tr_get_le32(in + TR_WIRE_OFF_RESERVED);

	return TR_OK;
}

int tr_wire_header_validate(const uint8_t raw[TR_WIRE_HEADER_SIZE],
			    const struct tr_frame_header *header,
			    const struct tr_wire_limits *limits)
{
	uint32_t max_payload;

	if (!raw || !header)
		return TR_ERR_INVALID;

	if (memcmp(raw + TR_WIRE_OFF_MAGIC, tr_magic, sizeof(tr_magic)) != 0)
		return TR_ERR_BAD_MAGIC;

	if (tr_header_crc(raw) != header->header_crc32c)
		return TR_ERR_HEADER_CRC;

	if (header->version != TR_WIRE_ENV_VERSION)
		return TR_ERR_BAD_VERSION;

	if (!tr_frame_type_valid(header->type))
		return TR_ERR_BAD_TYPE;

	if (header->flags & ~TR_FRAME_F_KNOWN_MASK)
		return TR_ERR_BAD_FLAGS;

	if (header->reserved != 0)
		return TR_ERR_RESERVED;

	max_payload = limits && limits->max_payload_len ?
			      limits->max_payload_len :
			      TR_WIRE_DEFAULT_MAX_PAYLOAD;

	if (header->payload_len > max_payload)
		return TR_ERR_BAD_LENGTH;

	return TR_OK;
}
