#ifndef TR_CRC32C_H
#define TR_CRC32C_H

#include <stddef.h>
#include <stdint.h>

uint32_t tr_crc32c_begin(void);
uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len);
uint32_t tr_crc32c_finish(uint32_t state);
uint32_t tr_crc32c(const void *data, size_t len);

#endif
