#ifndef TR_CRC32C_INTERNAL_H
#define TR_CRC32C_INTERNAL_H

#include "tr/crc32c.h"

/* Private backend entry points for differential tests; not installed in the SDK. */
#if defined(__x86_64__) && !defined(TR_CRC32C_FORCE_PORTABLE)
#define TR_CRC32C_X86_SSE42 1
#else
#define TR_CRC32C_X86_SSE42 0
#endif

uint32_t tr_crc32c_update_portable(uint32_t state, const void *data, size_t len);
int tr_crc32c_sse42_available(void);

#if TR_CRC32C_X86_SSE42
/* Caller must check tr_crc32c_sse42_available() before using this backend. */
uint32_t tr_crc32c_update_sse42(uint32_t state, const void *data, size_t len);
#endif

#endif
