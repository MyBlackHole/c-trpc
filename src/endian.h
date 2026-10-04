#ifndef TR_ENDIAN_H
#define TR_ENDIAN_H

#include <stdint.h>

static inline uint16_t tr_get_le16(const void *ptr)
{
	const uint8_t *p = (const uint8_t *)ptr;
	return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

static inline uint32_t tr_get_le32(const void *ptr)
{
	const uint8_t *p = (const uint8_t *)ptr;
	return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
	       ((uint32_t)p[3] << 24);
}

static inline uint64_t tr_get_le64(const void *ptr)
{
	const uint8_t *p = (const uint8_t *)ptr;
	return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
	       ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) |
	       ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) |
	       ((uint64_t)p[7] << 56);
}

static inline void tr_put_le16(void *ptr, uint16_t v)
{
	uint8_t *p = (uint8_t *)ptr;
	p[0] = (uint8_t)v;
	p[1] = (uint8_t)(v >> 8);
}

static inline void tr_put_le32(void *ptr, uint32_t v)
{
	uint8_t *p = (uint8_t *)ptr;
	p[0] = (uint8_t)v;
	p[1] = (uint8_t)(v >> 8);
	p[2] = (uint8_t)(v >> 16);
	p[3] = (uint8_t)(v >> 24);
}

static inline void tr_put_le64(void *ptr, uint64_t v)
{
	uint8_t *p = (uint8_t *)ptr;
	p[0] = (uint8_t)v;
	p[1] = (uint8_t)(v >> 8);
	p[2] = (uint8_t)(v >> 16);
	p[3] = (uint8_t)(v >> 24);
	p[4] = (uint8_t)(v >> 32);
	p[5] = (uint8_t)(v >> 40);
	p[6] = (uint8_t)(v >> 48);
	p[7] = (uint8_t)(v >> 56);
}

#endif
