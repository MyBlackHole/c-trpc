#ifndef TR_CRC32C_H
#define TR_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * 这里实现 CRC-32C（Castagnoli），不是 IEEE CRC-32。
 * update() 接收并返回原始中间状态；最后一个分段处理完成后只调用一次 finish()。
 * 空更新允许传入 NULL，并保持状态不变；否则 data 必须指向至少 len 个可读字节，
 * 不要求内存对齐。后端会自动选择，并且首次并发调用也是安全的。
 */
uint32_t tr_crc32c_begin(void);
uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len);
uint32_t tr_crc32c_finish(uint32_t state);
uint32_t tr_crc32c(const void *data, size_t len);

#endif
