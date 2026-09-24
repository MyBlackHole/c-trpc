#include "tr/crc32c.h"

/* Castagnoli polynomial, reflected representation. */
#define TR_CRC32C_POLY 0x82F63B78U

uint32_t tr_crc32c_begin(void)
{
	return 0xFFFFFFFFU;
}

uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len)
{
	const uint8_t *p = (const uint8_t *)data;
	uint32_t crc = state;
	size_t i;

	for (i = 0; i < len; ++i) {
		unsigned bit;
		crc ^= p[i];

		for (bit = 0; bit < 8; ++bit) {
			uint32_t mask = (uint32_t)-(int32_t)(crc & 1U);
			crc = (crc >> 1) ^ (TR_CRC32C_POLY & mask);
		}
	}

	return crc;
}

uint32_t tr_crc32c_finish(uint32_t state)
{
	return state ^ 0xFFFFFFFFU;
}

uint32_t tr_crc32c(const void *data, size_t len)
{
	return tr_crc32c_finish(tr_crc32c_update(tr_crc32c_begin(), data, len));
}
