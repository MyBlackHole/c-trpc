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
 * RAW 是 V1 baseline codec。
 * 下面的 copied control-path helper 适合小消息；
 * Bulk RPC 可以改用 tr_rpc_call_send_buffer()，把小型 RPC envelope
 * 和原始 payload 作为多个 Transport slice 发送，避免额外大块复制。
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
