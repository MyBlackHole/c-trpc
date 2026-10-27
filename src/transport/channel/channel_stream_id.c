#include "channel_internal.h"

int tr_channel_local_stream_id_take(uint32_t *next, uint32_t *out)
{
	uint32_t stream_id;

	if (!next || !out)
		return TR_ERR_INVALID;
	stream_id = *next;
	if (stream_id == 0U)
		return TR_ERR_CLOSED;
	*out = stream_id;
	*next = stream_id > UINT32_MAX - 2U ? 0U : stream_id + 2U;
	return TR_OK;
}
