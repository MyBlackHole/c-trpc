#ifndef TR_REACTOR_TOKEN_INTERNAL_H
#define TR_REACTOR_TOKEN_INTERNAL_H

#include <stdint.h>

/* 反应器事件令牌命名空间：
 *
 * 连接令牌：
 *   [ generation:32 ][ slot:32 ]
 *
 * 内部辅助令牌：
 *   [ 0xFFFF:16 ][ generation:32 ][ slot:16 ]
 *
 * 因此连接 generation 必须避开 0xFFFFxxxx；辅助 generation 可以使用
 * 完整非零 uint32_t 空间。唤醒、监听器和对端事件使用 UINT64_MAX 附近固定
 * 令牌，其低 16 位超出辅助槽位容量，不会被辅助解码器接受。
 */
#define TR_REACTOR_INTERNAL_TOKEN_PREFIX UINT16_C(0xffff)
#define TR_REACTOR_CONNECTION_GENERATION_MAX UINT32_C(0xfffeffff)

static inline uint32_t
tr_reactor_connection_next_generation(uint32_t generation)
{
	/* 连接代次也是跨模块持有能力凭证的一部分。耗尽后返回无效值，
	 * 真实槽位永久停止复用，不能让旧连接/RESET 墓碑重新匹配新连接。 */
	if (generation >= TR_REACTOR_CONNECTION_GENERATION_MAX)
		return 0U;
	/* 上界保证收窄无损；先用 64 位加法，避免 Clang 14 把递增结果的
	 * 32 位符号宽度错误带入后续 64 位令牌位移。 */
	return (uint32_t)((uint64_t)generation + UINT64_C(1));
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
