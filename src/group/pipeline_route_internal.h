#ifndef TR_PIPELINE_ROUTE_INTERNAL_H
#define TR_PIPELINE_ROUTE_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TR_PIPELINE_ROUTE_PREFACE_SIZE 48U
#define TR_PIPELINE_ROUTE_VERSION 1U
#define TR_PIPELINE_ROUTE_MEMBER_CONTROL UINT32_MAX

#define TR_PIPELINE_ROUTE_OFF_MAGIC 0U
#define TR_PIPELINE_ROUTE_OFF_VERSION 4U
#define TR_PIPELINE_ROUTE_OFF_ROLE 6U
#define TR_PIPELINE_ROUTE_OFF_FLAGS 8U
#define TR_PIPELINE_ROUTE_OFF_OWNER_SHARD 12U
#define TR_PIPELINE_ROUTE_OFF_PIPELINE_ID 16U
#define TR_PIPELINE_ROUTE_OFF_EPOCH 24U
#define TR_PIPELINE_ROUTE_OFF_MEMBER_INDEX 32U
#define TR_PIPELINE_ROUTE_OFF_MEMBER_GENERATION 36U
#define TR_PIPELINE_ROUTE_OFF_HEADER_CRC 40U
#define TR_PIPELINE_ROUTE_OFF_RESERVED 44U

#define TR_PIPELINE_ROUTE_F_KNOWN_MASK 0U

enum tr_pipeline_route_role {
	TR_PIPELINE_ROUTE_CONTROL = 1,
	TR_PIPELINE_ROUTE_DATA = 2
};

/*
 * Fixed routing identity sent before normal Transport framing on a physical
 * connection that joins a Pipeline.
 *
 * member_generation is a Pipeline membership generation issued by the
 * CONTROL-plane. It is deliberately unrelated to Reactor connection slot
 * generation.
 */
struct tr_pipeline_route_preface {
	uint16_t version;
	uint16_t role;
	uint32_t flags;
	uint32_t owner_shard_id;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t member_index;
	uint32_t member_generation;
	uint32_t header_crc32c;
	uint32_t reserved;
};

int tr_pipeline_route_preface_encode(
	uint8_t out[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_decode(
	const uint8_t in[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_validate_fields(
	const struct tr_pipeline_route_preface *preface);
int tr_pipeline_route_preface_validate(
	const uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE],
	const struct tr_pipeline_route_preface *preface);

/*
 * Incremental parser for TCP fragmentation/coalescing.
 *
 * feed() consumes at most the remaining preface bytes. If input also contains
 * bytes for the following Transport HELLO/frame, *consumed stops exactly at
 * TR_PIPELINE_ROUTE_PREFACE_SIZE so the caller can pass the tail onward.
 *
 * Invalid complete prefaces are terminal for this parser; subsequent feed()
 * returns TR_ERR_STATE. *out is changed only for a successfully validated
 * complete preface.
 */
struct tr_pipeline_route_parser {
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint32_t have;
	int done;
};

void tr_pipeline_route_parser_init(struct tr_pipeline_route_parser *parser);
int tr_pipeline_route_parser_feed(
	struct tr_pipeline_route_parser *parser,
	const uint8_t *data, size_t len, size_t *consumed,
	struct tr_pipeline_route_preface *out, int *ready);

#endif
