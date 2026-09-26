#ifndef TR_CRC32C_H
#define TR_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * CRC-32C (Castagnoli), not IEEE CRC-32. update() consumes and returns the
 * raw intermediate state; apply finish() once after the final segment.
 * Empty updates accept NULL and leave state unchanged. Otherwise data must
 * point to at least len readable bytes; no alignment is required.
 * Backend selection is automatic and safe for concurrent first calls.
 */
uint32_t tr_crc32c_begin(void);
uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len);
uint32_t tr_crc32c_finish(uint32_t state);
uint32_t tr_crc32c(const void *data, size_t len);

#endif
