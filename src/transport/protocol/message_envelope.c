#include "message_envelope.h"

#include "../../endian.h"
#include "tr/status.h"

#include <string.h>

static int
tr_message_envelope_validate(const struct tr_message_envelope *envelope)
{
	if (!envelope)
		return TR_ERR_INVALID;
	if (envelope->codec_id != TR_MESSAGE_CODEC_ZSTD)
		return TR_ERR_UNSUPPORTED;
	if (envelope->encoded_body_bytes == 0U ||
	    envelope->encoded_body_bytes >
	            UINT32_MAX - TR_MESSAGE_ENVELOPE_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	return TR_OK;
}

int tr_message_envelope_encode(uint8_t out[TR_MESSAGE_ENVELOPE_WIRE_SIZE],
                               const struct tr_message_envelope *envelope)
{
	int ret;

	if (!out || !envelope)
		return TR_ERR_INVALID;
	ret = tr_message_envelope_validate(envelope);
	if (ret != TR_OK)
		return ret;

	memset(out, 0, TR_MESSAGE_ENVELOPE_WIRE_SIZE);
	tr_put_le16(out, TR_MESSAGE_ENVELOPE_VERSION);
	tr_put_le16(out + 2U, envelope->codec_id);
	tr_put_le32(out + 4U, envelope->encoded_body_bytes);
	tr_put_le32(out + 8U, envelope->decoded_message_bytes);
	return TR_OK;
}

int tr_message_envelope_decode(const uint8_t *data, uint32_t len,
                               struct tr_message_envelope *out)
{
	struct tr_message_envelope decoded;
	int ret;

	if (!data || !out)
		return TR_ERR_INVALID;
	if (len != TR_MESSAGE_ENVELOPE_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	if (tr_get_le16(data) != TR_MESSAGE_ENVELOPE_VERSION)
		return TR_ERR_BAD_VERSION;
	if (tr_get_le32(data + 12U) != 0U)
		return TR_ERR_RESERVED;

	decoded.codec_id = tr_get_le16(data + 2U);
	decoded.encoded_body_bytes = tr_get_le32(data + 4U);
	decoded.decoded_message_bytes = tr_get_le32(data + 8U);
	ret = tr_message_envelope_validate(&decoded);
	if (ret != TR_OK)
		return ret;
	*out = decoded;
	return TR_OK;
}
