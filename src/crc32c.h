#ifndef TR_CRC32C_H
#define TR_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * 使用 CRC-32C（Castagnoli），不是 IEEE CRC-32。
 * update() 接收并返回原始中间状态；最后一个分段完成后只调用一次 finish()。
 * 空更新允许 data 为 NULL，并保持状态不变；否则 data 必须至少指向 len 字节
 * 可读内存，不要求对齐。后端自动选择，并且并发首次调用是安全的。
 */
uint32_t tr_crc32c_begin(void);
uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len);
uint32_t tr_crc32c_finish(uint32_t state);
uint32_t tr_crc32c(const void *data, size_t len);

#endif
