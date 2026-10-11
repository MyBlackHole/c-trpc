#ifndef TR_CRC32C_H
#define TR_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/* CRC-32C（Castagnoli），不是 IEEE CRC-32。update() 消耗并返回
 * 原始中间状态；在最后一个分段之后调用一次 finish()。
 * 空更新接受 NULL 且保持状态不变。否则 data 必须
 * 指向至少 len 个可读字节；无需对齐。
 * 后端自动选择，首次并发调用也是安全的。
 */
uint32_t tr_crc32c_begin(void);
uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len);
uint32_t tr_crc32c_finish(uint32_t state);
uint32_t tr_crc32c(const void *data, size_t len);

#endif
