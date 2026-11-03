#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <string.h>

int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written)
{
	if (!input || !written || (input->len != 0 && (!input->data || !dst)))
		return TR_ERR_INVALID;
	if (input->len > capacity)
		return TR_ERR_BAD_LENGTH;

	if (input->len)
		memcpy(dst, input->data, input->len);
	*written = input->len;
	return TR_OK;
}

int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out)
{
	if (!out || (len != 0 && !src))
		return TR_ERR_INVALID;

	out->data = src;
	out->len = len;
	return TR_OK;
}
