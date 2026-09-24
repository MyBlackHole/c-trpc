#ifndef TR_RPC_CODEC_H
#define TR_RPC_CODEC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RPC_CODEC_RAW 1U

struct tr_rpc_bytes {
	const uint8_t *data;
	uint32_t len;
};

/*
 * RAW is the V1 baseline codec. The copied control-path helper below remains
 * useful for small messages. Bulk RPC can instead use tr_rpc_call_send_buffer(),
 * which emits a small RPC envelope plus the original payload as Transport slices.
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
