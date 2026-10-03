#include "pipeline_control_wire_internal.h"

#include <string.h>

#include "tr/endian.h"
#include "tr/status.h"

static const uint8_t tr_pipeline_control_wire_magic[4] = {
	'T', 'R', 'C', '1'
};

static int tr_pipeline_control_wire_type_valid(uint16_t type)
{
	return type == TR_PIPELINE_CONTROL_DATA_OFFER ||
	       type == TR_PIPELINE_CONTROL_DATA_CANCEL ||
	       type == TR_PIPELINE_CONTROL_TRANSFER_READY;
}

static int tr_pipeline_control_wire_fields_valid(
	const struct tr_pipeline_control_wire_message *message)
{
	if (!message ||
	    message->version != TR_PIPELINE_CONTROL_WIRE_VERSION ||
	    !tr_pipeline_control_wire_type_valid(message->type) ||
	    message->flags != 0U || message->owner_shard_id == UINT32_MAX ||
	    message->pipeline_id == 0U || message->epoch == 0U ||
	    message->data_index == UINT32_MAX ||
	    message->data_generation == 0U || message->reserved != 0U)
		return 0;

	if (message->type == TR_PIPELINE_CONTROL_TRANSFER_READY)
		return message->stream_id != 0U;

	return message->stream_id == 0U;
}

int tr_pipeline_control_wire_encode(
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE],
	const struct tr_pipeline_control_wire_message *message)
{
	if (!out || !tr_pipeline_control_wire_fields_valid(message))
		return TR_ERR_INVALID;

	memset(out, 0, TR_PIPELINE_CONTROL_WIRE_SIZE);
	memcpy(out + TR_PIPELINE_CONTROL_WIRE_OFF_MAGIC,
	       tr_pipeline_control_wire_magic,
	       sizeof(tr_pipeline_control_wire_magic));
	tr_put_le16(out + TR_PIPELINE_CONTROL_WIRE_OFF_VERSION,
		    message->version);
	tr_put_le16(out + TR_PIPELINE_CONTROL_WIRE_OFF_TYPE, message->type);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_FLAGS, message->flags);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_OWNER_SHARD,
		    message->owner_shard_id);
	tr_put_le64(out + TR_PIPELINE_CONTROL_WIRE_OFF_PIPELINE_ID,
		    message->pipeline_id);
	tr_put_le64(out + TR_PIPELINE_CONTROL_WIRE_OFF_EPOCH, message->epoch);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_STREAM_ID,
		    message->stream_id);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_DATA_INDEX,
		    message->data_index);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_DATA_GENERATION,
		    message->data_generation);
	tr_put_le32(out + TR_PIPELINE_CONTROL_WIRE_OFF_RESERVED,
		    message->reserved);
	return TR_OK;
}

int tr_pipeline_control_wire_decode(
	const uint8_t *data, uint32_t len,
	struct tr_pipeline_control_wire_message *message)
{
	if (!data || !message || len != TR_PIPELINE_CONTROL_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	if (memcmp(data + TR_PIPELINE_CONTROL_WIRE_OFF_MAGIC,
		   tr_pipeline_control_wire_magic,
		   sizeof(tr_pipeline_control_wire_magic)) != 0)
		return TR_ERR_BAD_MAGIC;

	memset(message, 0, sizeof(*message));
	message->version =
		tr_get_le16(data + TR_PIPELINE_CONTROL_WIRE_OFF_VERSION);
	message->type =
		tr_get_le16(data + TR_PIPELINE_CONTROL_WIRE_OFF_TYPE);
	message->flags =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_FLAGS);
	message->owner_shard_id =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_OWNER_SHARD);
	message->pipeline_id =
		tr_get_le64(data + TR_PIPELINE_CONTROL_WIRE_OFF_PIPELINE_ID);
	message->epoch =
		tr_get_le64(data + TR_PIPELINE_CONTROL_WIRE_OFF_EPOCH);
	message->stream_id =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_STREAM_ID);
	message->data_index =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_DATA_INDEX);
	message->data_generation =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_DATA_GENERATION);
	message->reserved =
		tr_get_le32(data + TR_PIPELINE_CONTROL_WIRE_OFF_RESERVED);

	if (message->version != TR_PIPELINE_CONTROL_WIRE_VERSION)
		return TR_ERR_BAD_VERSION;
	if (!tr_pipeline_control_wire_type_valid(message->type))
		return TR_ERR_BAD_TYPE;
	if (message->flags != 0U)
		return TR_ERR_BAD_FLAGS;
	if (message->reserved != 0U)
		return TR_ERR_RESERVED;
	if (!tr_pipeline_control_wire_fields_valid(message))
		return TR_ERR_INVALID;
	return TR_OK;
}

int tr_pipeline_control_wire_data_route(
	const struct tr_pipeline_control_wire_message *message,
	struct tr_pipeline_route_preface *route)
{
	if (!message || !route ||
	    message->type != TR_PIPELINE_CONTROL_DATA_OFFER ||
	    !tr_pipeline_control_wire_fields_valid(message))
		return TR_ERR_INVALID;

	memset(route, 0, sizeof(*route));
	route->version = TR_PIPELINE_ROUTE_VERSION;
	route->role = TR_PIPELINE_ROUTE_DATA;
	route->owner_shard_id = message->owner_shard_id;
	route->pipeline_id = message->pipeline_id;
	route->epoch = message->epoch;
	route->member_index = message->data_index;
	route->member_generation = message->data_generation;
	return TR_OK;
}
