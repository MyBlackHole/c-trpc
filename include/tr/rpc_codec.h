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
 * RAW 是独立的复制式字节工具，不包含 RPC Call、Method 或 Wire 封装。
 * 下列接口在调用返回前完成输入/输出字节复制；它们不表示旧 V1 RPC 协议仍可用。
 * 内部传输路径可以使用尽量少复制的 Buffer 快速路径，但该能力不属于稳定的
 * RPC 应用契约。
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
		      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
		      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
