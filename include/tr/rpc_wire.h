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

/* Metadata 是有界 TLV block，前缀为 little-endian u16 长度。 */
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
	/* application payload 长度；该值不包含 metadata。 */
	uint32_t payload_len;
};

int tr_rpc_wire_encode(uint8_t out[TR_RPC_WIRE_HEADER_SIZE],
		       const struct tr_rpc_wire_header *header);

/* 兼容 decoder；存在 metadata 时先校验，再跳过 metadata。 */
int tr_rpc_wire_decode(const uint8_t *data, uint32_t len,
		       struct tr_rpc_wire_header *header,
		       const uint8_t **payload);

/* 返回 data 内部已经校验过的 metadata 和 application payload view。 */
int tr_rpc_wire_decode_ex(const uint8_t *data, uint32_t len,
			  struct tr_rpc_wire_header *header,
			  const uint8_t **metadata, uint16_t *metadata_len,
			  const uint8_t **payload);

#endif
