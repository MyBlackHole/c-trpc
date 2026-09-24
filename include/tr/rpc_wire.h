#ifndef TR_RPC_WIRE_H
#define TR_RPC_WIRE_H

#include <stdint.h>

#define TR_RPC_WIRE_HEADER_SIZE 32U
#define TR_RPC_WIRE_VERSION 1U

#define TR_RPC_WIRE_OFF_MAGIC 0U
#define TR_RPC_WIRE_OFF_VERSION 4U
#define TR_RPC_WIRE_OFF_TYPE 6U
#define TR_RPC_WIRE_OFF_SERVICE_ID 8U
#define TR_RPC_WIRE_OFF_METHOD_ID 12U
#define TR_RPC_WIRE_OFF_CODEC_ID 16U
#define TR_RPC_WIRE_OFF_FLAGS 20U
#define TR_RPC_WIRE_OFF_STATUS 24U
#define TR_RPC_WIRE_OFF_PAYLOAD_LEN 28U

/* Metadata is a bounded TLV block prefixed by a little-endian u16 length. */
#define TR_RPC_WIRE_F_METADATA (1U << 0)
#define TR_RPC_WIRE_VALID_FLAGS TR_RPC_WIRE_F_METADATA
#define TR_RPC_WIRE_METADATA_PREFIX_SIZE 2U
#define TR_RPC_WIRE_MAX_METADATA_BYTES 512U

enum tr_rpc_wire_type {
	TR_RPC_WIRE_REQUEST = 1,
	TR_RPC_WIRE_RESPONSE = 2,
	TR_RPC_WIRE_CANCEL = 3,
	TR_RPC_WIRE_STATUS = 4
};

struct tr_rpc_wire_header {
	uint16_t version;
	uint16_t type;
	uint32_t service_id;
	uint32_t method_id;
	uint32_t codec_id;
	uint32_t flags;
	int32_t status;
	/* Application payload length; metadata is not included in this value. */
	uint32_t payload_len;
};

int tr_rpc_wire_encode(uint8_t out[TR_RPC_WIRE_HEADER_SIZE],
		       const struct tr_rpc_wire_header *header);

/* Compatibility decoder. Metadata, if present, is validated and skipped. */
int tr_rpc_wire_decode(const uint8_t *data, uint32_t len,
		       struct tr_rpc_wire_header *header,
		       const uint8_t **payload);

/* Returns validated metadata and application payload views inside data. */
int tr_rpc_wire_decode_ex(const uint8_t *data, uint32_t len,
			  struct tr_rpc_wire_header *header,
			  const uint8_t **metadata, uint16_t *metadata_len,
			  const uint8_t **payload);

#endif
