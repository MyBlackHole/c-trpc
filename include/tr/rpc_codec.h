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
 * 下面的 copied helper 适合 application-facing 小消息。
 * 内部 Transport engine 可以使用 copy-minimal Buffer fast path，但该能力
 * 不属于 stable RPC application contract。
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
