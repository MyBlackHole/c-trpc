#ifndef TR_MESSAGE_ENVELOPE_H
#define TR_MESSAGE_ENVELOPE_H

#include <stdint.h>

#define TR_MESSAGE_ENVELOPE_VERSION 1U
#define TR_MESSAGE_ENVELOPE_WIRE_SIZE 16U

#define TR_MESSAGE_CODEC_ZSTD 1U

struct tr_message_envelope {
	uint16_t codec_id;
	uint32_t encoded_body_bytes;
	uint32_t decoded_message_bytes;
};

int tr_message_envelope_encode(uint8_t out[TR_MESSAGE_ENVELOPE_WIRE_SIZE],
                               const struct tr_message_envelope *envelope);
int tr_message_envelope_decode(const uint8_t *data, uint32_t len,
                               struct tr_message_envelope *out);

#endif
