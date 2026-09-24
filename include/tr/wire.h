#ifndef TR_WIRE_H
#define TR_WIRE_H

#include <stdint.h>

#define TR_WIRE_HEADER_SIZE 40U
#define TR_WIRE_ENV_VERSION 1U
#define TR_WIRE_DEFAULT_MAX_PAYLOAD (1024U * 1024U)

#define TR_WIRE_OFF_MAGIC 0U
#define TR_WIRE_OFF_VERSION 4U
#define TR_WIRE_OFF_TYPE 6U
#define TR_WIRE_OFF_FLAGS 8U
#define TR_WIRE_OFF_STREAM_ID 12U
#define TR_WIRE_OFF_MESSAGE_ID 16U
#define TR_WIRE_OFF_PAYLOAD_LEN 24U
#define TR_WIRE_OFF_PAYLOAD_CRC 28U
#define TR_WIRE_OFF_HEADER_CRC 32U
#define TR_WIRE_OFF_RESERVED 36U

#define TR_FRAME_F_FIRST (1U << 0)
#define TR_FRAME_F_LAST (1U << 1)
#define TR_FRAME_F_LANE_BULK (1U << 2)
#define TR_FRAME_F_KNOWN_MASK \
	(TR_FRAME_F_FIRST | TR_FRAME_F_LAST | TR_FRAME_F_LANE_BULK)

enum tr_frame_type {
	TR_FRAME_DATA = 1,
	TR_FRAME_STREAM_OPEN = 2,
	TR_FRAME_STREAM_CLOSE = 3,
	TR_FRAME_WINDOW_UPDATE = 4,
	TR_FRAME_PING = 5,
	TR_FRAME_PONG = 6,
	TR_FRAME_ERROR = 7,
	TR_FRAME_HELLO = 8,
	TR_FRAME_HELLO_ACK = 9,
	TR_FRAME_GOAWAY = 10
};

struct tr_frame_header {
	uint16_t version;
	uint16_t type;
	uint32_t flags;
	uint32_t stream_id;
	uint64_t message_id;
	uint32_t payload_len;
	uint32_t payload_crc32c;
	uint32_t header_crc32c;
	uint32_t reserved;
};

struct tr_wire_limits {
	uint32_t max_payload_len;
};

int tr_frame_type_valid(uint16_t type);

int tr_wire_header_encode(uint8_t out[TR_WIRE_HEADER_SIZE],
			  const struct tr_frame_header *header);
int tr_wire_header_decode(const uint8_t in[TR_WIRE_HEADER_SIZE],
			  struct tr_frame_header *header);
int tr_wire_header_validate(const uint8_t raw[TR_WIRE_HEADER_SIZE],
			    const struct tr_frame_header *header,
			    const struct tr_wire_limits *limits);

#endif
