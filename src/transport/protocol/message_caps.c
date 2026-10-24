#include "message_caps.h"

#include "../../endian.h"
#include "tr/status.h"

#include <string.h>

static int tr_message_caps_validate(const struct tr_message_caps *caps)
{
	if (!caps)
		return TR_ERR_INVALID;
	if (caps->receive_algorithm_bits & ~TR_MESSAGE_ALGORITHM_KNOWN_MASK)
		return TR_ERR_UNSUPPORTED;
	if (caps->receive_algorithm_bits == 0U)
		return caps->max_encoded_wire_bytes == 0U ? TR_OK
		                                          : TR_ERR_INVALID;
	if (caps->max_encoded_wire_bytes < 17U)
		return TR_ERR_BAD_LENGTH;
	return TR_OK;
}

int tr_message_caps_encode(uint8_t out[TR_MESSAGE_CAPS_WIRE_SIZE],
                           const struct tr_message_caps *caps)
{
	int ret;

	if (!out || !caps)
		return TR_ERR_INVALID;
	ret = tr_message_caps_validate(caps);
	if (ret != TR_OK)
		return ret;

	memset(out, 0, TR_MESSAGE_CAPS_WIRE_SIZE);
	tr_put_le16(out, TR_MESSAGE_CAPS_SCHEMA_VERSION);
	tr_put_le32(out + 4U, caps->receive_algorithm_bits);
	tr_put_le32(out + 8U, caps->max_encoded_wire_bytes);
	return TR_OK;
}

int tr_message_caps_decode(const uint8_t *data, uint32_t len,
                           struct tr_message_caps *out)
{
	struct tr_message_caps decoded;
	int ret;

	if (!data || !out)
		return TR_ERR_INVALID;
	if (len != TR_MESSAGE_CAPS_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	if (tr_get_le16(data) != TR_MESSAGE_CAPS_SCHEMA_VERSION)
		return TR_ERR_BAD_VERSION;
	if (tr_get_le16(data + 2U) != 0U)
		return TR_ERR_RESERVED;

	decoded.receive_algorithm_bits = tr_get_le32(data + 4U);
	decoded.max_encoded_wire_bytes = tr_get_le32(data + 8U);
	ret = tr_message_caps_validate(&decoded);
	if (ret != TR_OK)
		return ret;
	*out = decoded;
	return TR_OK;
}
