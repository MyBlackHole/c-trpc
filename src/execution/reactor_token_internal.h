#ifndef TR_REACTOR_TOKEN_INTERNAL_H
#define TR_REACTOR_TOKEN_INTERNAL_H

#include <stdint.h>

/*
 * Reactor event token namespace：
 *
 * connection token:
 *   [ generation:32 ][ slot:32 ]
 *
 * internal auxiliary token:
 *   [ 0xFFFF:16 ][ generation:32 ][ slot:16 ]
 *
 * 因此 connection generation 必须避开 0xFFFFxxxx；aux generation 可以使用
 * 完整非零 uint32_t 空间。wake/listener/peer-event 使用 UINT64_MAX 附近固定
 * token，其低 16 位超出 aux slot capacity，不会被 aux decoder 接受。
 */
#define TR_REACTOR_INTERNAL_TOKEN_PREFIX UINT16_C(0xffff)
#define TR_REACTOR_CONNECTION_GENERATION_MAX UINT32_C(0xfffeffff)

static inline uint32_t
tr_reactor_connection_next_generation(uint32_t generation)
{
	generation++;
	if (generation == 0U ||
	    generation > TR_REACTOR_CONNECTION_GENERATION_MAX)
		generation = 1U;
	return generation;
}

static inline uint32_t
tr_reactor_aux_next_generation(uint32_t generation)
{
	generation++;
	if (generation == 0U)
		generation = 1U;
	return generation;
}

static inline uint64_t
tr_reactor_aux_token(uint32_t slot, uint32_t generation)
{
	return (UINT64_C(0xffff) << 48) |
	       ((uint64_t)generation << 16) |
	       (uint64_t)(slot & UINT32_C(0xffff));
}

static inline int
tr_reactor_aux_token_decode(
	uint64_t token, uint32_t capacity,
	uint32_t *slot, uint32_t *generation)
{
	uint32_t decoded_slot;
	uint32_t decoded_generation;

	if ((uint16_t)(token >> 48) != TR_REACTOR_INTERNAL_TOKEN_PREFIX)
		return 0;

	decoded_slot = (uint32_t)(token & UINT64_C(0xffff));
	decoded_generation = (uint32_t)(token >> 16);
	if (decoded_slot >= capacity || decoded_generation == 0U)
		return 0;

	if (slot)
		*slot = decoded_slot;
	if (generation)
		*generation = decoded_generation;
	return 1;
}

#endif
