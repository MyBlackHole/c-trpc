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
 * RAW 是 V1 基线编解码器。
 * 下面采用复制方式的辅助接口适合面向应用的小消息。
 * 内部 Transport 引擎可以使用尽量少复制的 Buffer 快速路径，但该能力
 * 不属于稳定的 RPC 应用契约。
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
