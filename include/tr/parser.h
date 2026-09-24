#ifndef TR_PARSER_H
#define TR_PARSER_H

#include <stddef.h>
#include <stdint.h>

#include "tr/buffer.h"
#include "tr/frame.h"
#include "tr/wire.h"

#ifdef __cplusplus
extern "C" {
#endif

enum tr_parser_state {
	TR_PARSER_HEADER = 0,
	TR_PARSER_WAIT_PAYLOAD_BUFFER,
	TR_PARSER_PAYLOAD
};

struct tr_parser {
	enum tr_parser_state state;

	uint8_t header_buf[TR_WIRE_HEADER_SIZE];
	uint32_t header_have;
	struct tr_frame_header header;

	struct tr_buffer *payload;
	uint32_t payload_have;
	uint32_t payload_crc_state;

	struct tr_buffer_pool *pool;
	struct tr_wire_limits limits;
};

int tr_parser_init(struct tr_parser *parser, struct tr_buffer_pool *pool,
		   const struct tr_wire_limits *limits);
void tr_parser_reset(struct tr_parser *parser);

/*
 * Ensures the parser has all resources needed for the next receive.
 * Returns TR_AGAIN when the payload buffer pool is temporarily exhausted.
 */
int tr_parser_prepare(struct tr_parser *parser);

/* Direct-receive API: recv() may write straight into this span. */
void *tr_parser_write_ptr(struct tr_parser *parser);
size_t tr_parser_write_len(const struct tr_parser *parser);

/*
 * Reports bytes written into the span returned by tr_parser_write_ptr().
 * TR_FRAME_READY transfers payload ownership to out_frame.
 */
int tr_parser_produce(struct tr_parser *parser, size_t produced,
		      struct tr_frame *out_frame);

#ifdef __cplusplus
}
#endif

#endif
