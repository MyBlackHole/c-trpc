#include "pipeline_route_internal.h"

#include <string.h>

#include "../crc32c.h"
#include "../endian.h"
#include "tr/status.h"

static const uint8_t tr_pipeline_route_magic[4] = { 'T', 'R', 'R', '1' };

static uint32_t tr_pipeline_route_header_crc(
	const uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE])
{
	uint8_t tmp[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	memcpy(tmp, raw, sizeof(tmp));
	memset(tmp + TR_PIPELINE_ROUTE_OFF_HEADER_CRC, 0, sizeof(uint32_t));
	return tr_crc32c(tmp, sizeof(tmp));
}

int tr_pipeline_route_preface_encode(
	uint8_t out[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface)
{
	uint32_t crc;

	if (!out || !preface)
		return TR_ERR_INVALID;

	memset(out, 0, TR_PIPELINE_ROUTE_PREFACE_SIZE);
	memcpy(out + TR_PIPELINE_ROUTE_OFF_MAGIC, tr_pipeline_route_magic,
	       sizeof(tr_pipeline_route_magic));
	tr_put_le16(out + TR_PIPELINE_ROUTE_OFF_VERSION, preface->version);
	tr_put_le16(out + TR_PIPELINE_ROUTE_OFF_ROLE, preface->role);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_FLAGS, preface->flags);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_OWNER_SHARD,
		    preface->owner_shard_id);
	tr_put_le64(out + TR_PIPELINE_ROUTE_OFF_PIPELINE_ID,
		    preface->pipeline_id);
	tr_put_le64(out + TR_PIPELINE_ROUTE_OFF_EPOCH, preface->epoch);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_MEMBER_INDEX,
		    preface->member_index);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_MEMBER_GENERATION,
		    preface->member_generation);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_HEADER_CRC, 0U);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_RESERVED, preface->reserved);

	crc = tr_pipeline_route_header_crc(out);
	tr_put_le32(out + TR_PIPELINE_ROUTE_OFF_HEADER_CRC, crc);
	return TR_OK;
}

int tr_pipeline_route_preface_decode(
	const uint8_t in[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	struct tr_pipeline_route_preface *preface)
{
	if (!in || !preface)
		return TR_ERR_INVALID;

	memset(preface, 0, sizeof(*preface));
	preface->version =
		tr_get_le16(in + TR_PIPELINE_ROUTE_OFF_VERSION);
	preface->role = tr_get_le16(in + TR_PIPELINE_ROUTE_OFF_ROLE);
	preface->flags = tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_FLAGS);
	preface->owner_shard_id =
		tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_OWNER_SHARD);
	preface->pipeline_id =
		tr_get_le64(in + TR_PIPELINE_ROUTE_OFF_PIPELINE_ID);
	preface->epoch = tr_get_le64(in + TR_PIPELINE_ROUTE_OFF_EPOCH);
	preface->member_index =
		tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_MEMBER_INDEX);
	preface->member_generation =
		tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_MEMBER_GENERATION);
	preface->header_crc32c =
		tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_HEADER_CRC);
	preface->reserved =
		tr_get_le32(in + TR_PIPELINE_ROUTE_OFF_RESERVED);
	return TR_OK;
}

int tr_pipeline_route_preface_validate_fields(
	const struct tr_pipeline_route_preface *preface)
{
	if (!preface)
		return TR_ERR_INVALID;

	if (preface->version != TR_PIPELINE_ROUTE_VERSION)
		return TR_ERR_BAD_VERSION;

	if (preface->role != TR_PIPELINE_ROUTE_CONTROL &&
	    preface->role != TR_PIPELINE_ROUTE_DATA)
		return TR_ERR_BAD_TYPE;

	if (preface->flags & ~TR_PIPELINE_ROUTE_F_KNOWN_MASK)
		return TR_ERR_BAD_FLAGS;

	if (preface->reserved != 0U)
		return TR_ERR_RESERVED;

	if (preface->pipeline_id == 0U || preface->epoch == 0U ||
	    preface->owner_shard_id == UINT32_MAX ||
	    preface->member_generation == 0U)
		return TR_ERR_INVALID;

	if (preface->role == TR_PIPELINE_ROUTE_CONTROL) {
		if (preface->member_index != TR_PIPELINE_ROUTE_MEMBER_CONTROL)
			return TR_ERR_INVALID;
	} else if (preface->member_index ==
		   TR_PIPELINE_ROUTE_MEMBER_CONTROL) {
		return TR_ERR_INVALID;
	}

	return TR_OK;
}

int tr_pipeline_route_preface_validate(
	const uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface)
{
	if (!raw || !preface)
		return TR_ERR_INVALID;

	if (memcmp(raw + TR_PIPELINE_ROUTE_OFF_MAGIC,
		   tr_pipeline_route_magic,
		   sizeof(tr_pipeline_route_magic)) != 0)
		return TR_ERR_BAD_MAGIC;

	if (tr_pipeline_route_header_crc(raw) != preface->header_crc32c)
		return TR_ERR_HEADER_CRC;

	return tr_pipeline_route_preface_validate_fields(preface);
}

void tr_pipeline_route_parser_init(struct tr_pipeline_route_parser *parser)
{
	if (parser)
		memset(parser, 0, sizeof(*parser));
}

int tr_pipeline_route_parser_feed(
	struct tr_pipeline_route_parser *parser,
	const uint8_t *data, size_t len, size_t *consumed,
	struct tr_pipeline_route_preface *out, int *ready)
{
	struct tr_pipeline_route_preface decoded;
	size_t remaining;
	size_t take;
	int ret;

	if (!parser || !consumed || !out || !ready || (len != 0U && !data))
		return TR_ERR_INVALID;

	*consumed = 0U;
	*ready = 0;
	if (parser->done)
		return TR_ERR_STATE;

	remaining = TR_PIPELINE_ROUTE_PREFACE_SIZE - parser->have;
	take = len < remaining ? len : remaining;
	if (take != 0U) {
		memcpy(parser->raw + parser->have, data, take);
		parser->have += (uint32_t)take;
		*consumed = take;
	}

	if (parser->have != TR_PIPELINE_ROUTE_PREFACE_SIZE)
		return TR_OK;

	/*
	 * A complete preface is a one-shot decision. Invalid routing identity is
	 * connection-fatal; callers must close rather than trying to resynchronize
	 * arbitrary bytes before the normal Transport framing begins.
	 */
	parser->done = 1;
	ret = tr_pipeline_route_preface_decode(parser->raw, &decoded);
	if (ret != TR_OK)
		return ret;
	ret = tr_pipeline_route_preface_validate(parser->raw, &decoded);
	if (ret != TR_OK)
		return ret;

	*out = decoded;
	*ready = 1;
	return TR_OK;
}
