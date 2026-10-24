#ifndef TR_MESSAGE_CAPS_H
#define TR_MESSAGE_CAPS_H

#include <stdint.h>

#define TR_MESSAGE_CAPS_SCHEMA_VERSION 1U
#define TR_MESSAGE_CAPS_WIRE_SIZE 12U

#define TR_MESSAGE_ALGORITHM_ZSTD (UINT32_C(1) << 0)
#define TR_MESSAGE_ALGORITHM_KNOWN_MASK TR_MESSAGE_ALGORITHM_ZSTD

/* 该能力由本端接收方通告；对端据此决定自己的发送算法。 */
struct tr_message_caps {
	uint32_t receive_algorithm_bits;
	uint32_t max_encoded_wire_bytes;
};

int tr_message_caps_encode(uint8_t out[TR_MESSAGE_CAPS_WIRE_SIZE],
                           const struct tr_message_caps *caps);
int tr_message_caps_decode(const uint8_t *data, uint32_t len,
                           struct tr_message_caps *out);

#endif
