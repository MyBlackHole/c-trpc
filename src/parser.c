#include "tr/parser.h"

#include "tr/crc32c.h"
#include "tr/status.h"

#include <string.h>

static void tr_parser_clear_current(struct tr_parser *parser)
{
	parser->state = TR_PARSER_HEADER;
	parser->header_have = 0;
	memset(&parser->header, 0, sizeof(parser->header));
	parser->payload = NULL;
	parser->payload_have = 0;
	parser->payload_crc_state = tr_crc32c_begin();
}

int tr_parser_init(struct tr_parser *parser, struct tr_buffer_pool *pool,
		   const struct tr_wire_limits *limits)
{
	if (!parser || !pool)
		return TR_ERR_INVALID;

	memset(parser, 0, sizeof(*parser));
	parser->pool = pool;
	parser->limits.max_payload_len = (limits && limits->max_payload_len) ?
						 limits->max_payload_len :
						 TR_WIRE_DEFAULT_MAX_PAYLOAD;

	tr_parser_clear_current(parser);
	return TR_OK;
}

void tr_parser_reset(struct tr_parser *parser)
{
	if (!parser)
		return;

	if (parser->payload)
		tr_buffer_release(parser->payload);

	tr_parser_clear_current(parser);
}

int tr_parser_prepare(struct tr_parser *parser)
{
	int ret;

	if (!parser)
		return TR_ERR_INVALID;

	if (parser->state != TR_PARSER_WAIT_PAYLOAD_BUFFER)
		return TR_OK;

	ret = tr_buffer_acquire(parser->pool, parser->header.payload_len,
				&parser->payload);
	if (ret != TR_OK)
		return ret;

	parser->payload_have = 0;
	parser->payload_crc_state = tr_crc32c_begin();
	parser->state = TR_PARSER_PAYLOAD;
	return TR_OK;
}

void *tr_parser_write_ptr(struct tr_parser *parser)
{
	if (!parser)
		return NULL;

	if (parser->state == TR_PARSER_HEADER)
		return parser->header_buf + parser->header_have;

	if (parser->state == TR_PARSER_PAYLOAD && parser->payload)
		return parser->payload->data + parser->payload_have;

	return NULL;
}

size_t tr_parser_write_len(const struct tr_parser *parser)
{
	if (!parser)
		return 0;

	if (parser->state == TR_PARSER_HEADER)
		return TR_WIRE_HEADER_SIZE - parser->header_have;

	if (parser->state == TR_PARSER_PAYLOAD && parser->payload)
		return parser->header.payload_len - parser->payload_have;

	return 0;
}

static int tr_parser_emit_empty(struct tr_parser *parser,
				struct tr_frame *out_frame)
{
	if (parser->header.payload_crc32c != tr_crc32c(NULL, 0))
		return TR_ERR_PAYLOAD_CRC;

	tr_frame_init(out_frame);
	out_frame->header = parser->header;
	tr_parser_clear_current(parser);
	return TR_FRAME_READY;
}

int tr_parser_produce(struct tr_parser *parser, size_t produced,
		      struct tr_frame *out_frame)
{
	size_t writable;
	int ret;

	if (!parser || !out_frame)
		return TR_ERR_INVALID;

	writable = tr_parser_write_len(parser);
	if (produced == 0 || produced > writable)
		return TR_ERR_INVALID;

	if (parser->state == TR_PARSER_HEADER) {
		parser->header_have += (uint32_t)produced;

		if (parser->header_have != TR_WIRE_HEADER_SIZE)
			return TR_OK;

		ret = tr_wire_header_decode(parser->header_buf,
					    &parser->header);
		if (ret != TR_OK)
			goto fail;

		ret = tr_wire_header_validate(parser->header_buf,
					      &parser->header, &parser->limits);
		if (ret != TR_OK)
			goto fail;

		if (parser->header.payload_len == 0) {
			ret = tr_parser_emit_empty(parser, out_frame);
			if (ret < 0)
				goto fail;
			return ret;
		}

		parser->state = TR_PARSER_WAIT_PAYLOAD_BUFFER;
		ret = tr_parser_prepare(parser);
		if (ret == TR_AGAIN)
			return TR_AGAIN;
		if (ret != TR_OK)
			goto fail;

		return TR_OK;
	}

	if (parser->state == TR_PARSER_PAYLOAD) {
		const uint8_t *new_data =
			parser->payload->data + parser->payload_have;

		parser->payload_crc_state = tr_crc32c_update(
			parser->payload_crc_state, new_data, produced);
		parser->payload_have += (uint32_t)produced;
		parser->payload->len = parser->payload_have;

		if (parser->payload_have != parser->header.payload_len)
			return TR_OK;

		if (tr_crc32c_finish(parser->payload_crc_state) !=
		    parser->header.payload_crc32c) {
			ret = TR_ERR_PAYLOAD_CRC;
			goto fail;
		}

		tr_frame_init(out_frame);
		out_frame->header = parser->header;
		out_frame->payload = parser->payload;

		parser->payload = NULL;
		tr_parser_clear_current(parser);
		return TR_FRAME_READY;
	}

	return TR_ERR_STATE;

fail:
	tr_parser_reset(parser);
	return ret;
}
