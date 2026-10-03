#ifndef TR_PIPELINE_CONTROL_WIRE_INTERNAL_H
#define TR_PIPELINE_CONTROL_WIRE_INTERNAL_H

#include <stdint.h>

#include "pipeline_route_internal.h"

#define TR_PIPELINE_CONTROL_WIRE_SIZE 48U
#define TR_PIPELINE_CONTROL_WIRE_VERSION 1U

#define TR_PIPELINE_CONTROL_WIRE_OFF_MAGIC 0U
#define TR_PIPELINE_CONTROL_WIRE_OFF_VERSION 4U
#define TR_PIPELINE_CONTROL_WIRE_OFF_TYPE 6U
#define TR_PIPELINE_CONTROL_WIRE_OFF_FLAGS 8U
#define TR_PIPELINE_CONTROL_WIRE_OFF_OWNER_SHARD 12U
#define TR_PIPELINE_CONTROL_WIRE_OFF_PIPELINE_ID 16U
#define TR_PIPELINE_CONTROL_WIRE_OFF_EPOCH 24U
#define TR_PIPELINE_CONTROL_WIRE_OFF_STREAM_ID 32U
#define TR_PIPELINE_CONTROL_WIRE_OFF_DATA_INDEX 36U
#define TR_PIPELINE_CONTROL_WIRE_OFF_DATA_GENERATION 40U
#define TR_PIPELINE_CONTROL_WIRE_OFF_RESERVED 44U

enum tr_pipeline_control_wire_type {
	TR_PIPELINE_CONTROL_DATA_OFFER = 1,
	TR_PIPELINE_CONTROL_DATA_CANCEL = 2,
	TR_PIPELINE_CONTROL_TRANSFER_READY = 3
};

struct tr_pipeline_control_wire_message {
	uint16_t version;
	uint16_t type;
	uint32_t flags;
	uint32_t owner_shard_id;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t stream_id;
	uint32_t data_index;
	uint32_t data_generation;
	uint32_t reserved;
};

/*
 * Fixed-size Pipeline CONTROL message payload.
 *
 * The outer Transport frame already protects payload bytes with CRC32C. TRC1
 * adds an explicit application magic/version and carries only routing/runtime
 * identity: no Reactor slot/generation ever crosses the wire.
 */
int tr_pipeline_control_wire_encode(
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE],
	const struct tr_pipeline_control_wire_message *message);
int tr_pipeline_control_wire_decode(
	const uint8_t *data, uint32_t len,
	struct tr_pipeline_control_wire_message *message);

/*
 * DATA_OFFER carries exactly the capability needed by a DATA socket TRR1
 * preface. Convert it without exposing or inventing Reactor connection ids.
 */
int tr_pipeline_control_wire_data_route(
	const struct tr_pipeline_control_wire_message *message,
	struct tr_pipeline_route_preface *route);

#endif
