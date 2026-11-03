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
 * RAW 是独立的字节工具，不包含 RPC Call、Method 或 Wire 封装。
 * tr_rpc_raw_encode() 将输入字节复制到调用方提供的目标缓冲区。
 * tr_rpc_raw_decode() 不分配或复制数据，而是返回指向 src 的借用视图；
 * 调用方必须保证 src 在使用 out->data 期间保持有效。它们不表示旧 V1 RPC
 * 协议仍可用。内部传输路径可以使用 Buffer 快速路径，但不属于稳定的 RPC
 * 应用契约。
 */
int tr_rpc_raw_encode(const struct tr_rpc_bytes *input, uint8_t *dst,
                      uint32_t capacity, uint32_t *written);
int tr_rpc_raw_decode(const uint8_t *src, uint32_t len,
                      struct tr_rpc_bytes *out);

#ifdef __cplusplus
}
#endif

#endif
