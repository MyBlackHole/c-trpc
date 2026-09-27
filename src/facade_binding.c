#include "facade_binding.h"

#include "tr/endian.h"
#include "tr/status.h"

#include <errno.h>
#include <string.h>
#include <sys/random.h>

#define TR_FACADE_BIND_OFF_MAGIC 0U
#define TR_FACADE_BIND_OFF_VERSION 4U
#define TR_FACADE_BIND_OFF_LANE 6U
#define TR_FACADE_BIND_OFF_IDENTITY 8U
#define TR_FACADE_BIND_OFF_RESERVED 24U

static const uint8_t tr_facade_bind_magic[4] = { 'T', 'R', 'B', '1' };

static int tr_facade_binding_lane_valid(uint16_t lane)
{
	return lane == TR_FACADE_BIND_LANE_CONTROL ||
	       lane == TR_FACADE_BIND_LANE_BULK;
}

static int tr_facade_binding_id_zero(const uint8_t id[TR_FACADE_BIND_ID_SIZE])
{
	uint8_t value = 0;
	uint32_t i;

	for (i = 0; i < TR_FACADE_BIND_ID_SIZE; ++i)
		value |= id[i];
	return value == 0;
}

int tr_facade_binding_generate_id(uint8_t out[TR_FACADE_BIND_ID_SIZE])
{
	uint32_t off;

	if (!out)
		return TR_ERR_INVALID;

	do {
		off = 0;
		while (off < TR_FACADE_BIND_ID_SIZE) {
			ssize_t n = getrandom(out + off,
					      TR_FACADE_BIND_ID_SIZE - off, 0);
			if (n < 0) {
				if (errno == EINTR)
					continue;
				memset(out, 0, TR_FACADE_BIND_ID_SIZE);
				return TR_ERR_SYS;
			}
			if (n == 0) {
				memset(out, 0, TR_FACADE_BIND_ID_SIZE);
				return TR_ERR_SYS;
			}
			off += (uint32_t)n;
		}
	} while (tr_facade_binding_id_zero(out));

	return TR_OK;
}

int tr_facade_binding_encode(uint8_t out[TR_FACADE_BIND_WIRE_SIZE],
			     const struct tr_facade_binding *binding)
{
	if (!out || !binding || !tr_facade_binding_lane_valid(binding->lane) ||
	    tr_facade_binding_id_zero(binding->identity))
		return TR_ERR_INVALID;

	memset(out, 0, TR_FACADE_BIND_WIRE_SIZE);
	memcpy(out + TR_FACADE_BIND_OFF_MAGIC, tr_facade_bind_magic,
	       sizeof(tr_facade_bind_magic));
	tr_put_le16(out + TR_FACADE_BIND_OFF_VERSION, TR_FACADE_BIND_VERSION);
	tr_put_le16(out + TR_FACADE_BIND_OFF_LANE, binding->lane);
	memcpy(out + TR_FACADE_BIND_OFF_IDENTITY, binding->identity,
	       TR_FACADE_BIND_ID_SIZE);
	return TR_OK;
}

int tr_facade_binding_decode(const uint8_t *data, uint32_t len,
			     struct tr_facade_binding *binding)
{
	uint16_t lane;

	if (!data || !binding || len != TR_FACADE_BIND_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	if (memcmp(data + TR_FACADE_BIND_OFF_MAGIC, tr_facade_bind_magic,
		   sizeof(tr_facade_bind_magic)) != 0)
		return TR_ERR_BAD_MAGIC;
	if (tr_get_le16(data + TR_FACADE_BIND_OFF_VERSION) !=
	    TR_FACADE_BIND_VERSION)
		return TR_ERR_BAD_VERSION;

	lane = tr_get_le16(data + TR_FACADE_BIND_OFF_LANE);
	if (!tr_facade_binding_lane_valid(lane))
		return TR_ERR_BAD_TYPE;
	if (tr_get_le64(data + TR_FACADE_BIND_OFF_RESERVED) != 0)
		return TR_ERR_RESERVED;

	memset(binding, 0, sizeof(*binding));
	binding->lane = lane;
	memcpy(binding->identity, data + TR_FACADE_BIND_OFF_IDENTITY,
	       TR_FACADE_BIND_ID_SIZE);
	if (tr_facade_binding_id_zero(binding->identity)) {
		memset(binding, 0, sizeof(*binding));
		return TR_ERR_INVALID;
	}
	return TR_OK;
}

int tr_facade_binding_id_equal(const uint8_t a[TR_FACADE_BIND_ID_SIZE],
			       const uint8_t b[TR_FACADE_BIND_ID_SIZE])
{
	uint8_t diff = 0;
	uint32_t i;

	if (!a || !b)
		return 0;
	for (i = 0; i < TR_FACADE_BIND_ID_SIZE; ++i)
		diff |= (uint8_t)(a[i] ^ b[i]);
	return diff == 0;
}
