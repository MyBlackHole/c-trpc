#include "../src/execution/reactor_token_internal.h"

#include <assert.h>
#include <stdint.h>

#define TEST_AUX_CAPACITY 16U

static uint64_t connection_token(uint32_t slot, uint32_t generation)
{
	return ((uint64_t)generation << 32) | (uint64_t)slot;
}

static void test_connection_generation_skips_internal_namespace(void)
{
	assert(tr_reactor_connection_next_generation(0U) == 1U);
	assert(tr_reactor_connection_next_generation(
		       TR_REACTOR_CONNECTION_GENERATION_MAX - 1U) ==
	       TR_REACTOR_CONNECTION_GENERATION_MAX);
	assert(tr_reactor_connection_next_generation(
		       TR_REACTOR_CONNECTION_GENERATION_MAX) == 1U);

	/* 所有合法 connection token 的高 16 位都不能进入 0xFFFF namespace。 */
	assert((uint16_t)(
		       connection_token(
			       7U, TR_REACTOR_CONNECTION_GENERATION_MAX) >>
		       48) != TR_REACTOR_INTERNAL_TOKEN_PREFIX);
}

static void test_aux_generation_uses_full_uint32_range(void)
{
	assert(tr_reactor_aux_next_generation(0U) == 1U);
	assert(tr_reactor_aux_next_generation(UINT32_MAX - 1U) == UINT32_MAX);
	assert(tr_reactor_aux_next_generation(UINT32_MAX) == 1U);

	/* wrap 后 token 必须变化，旧 UINT32_MAX capability 不能等价于 generation 1。 */
	assert(tr_reactor_aux_token(3U, UINT32_MAX) !=
	       tr_reactor_aux_token(3U, 1U));
}

static void check_aux_roundtrip(uint32_t slot, uint32_t generation)
{
	uint64_t token = tr_reactor_aux_token(slot, generation);
	uint32_t decoded_slot = UINT32_MAX;
	uint32_t decoded_generation = 0U;

	assert(tr_reactor_aux_token_decode(
		       token, TEST_AUX_CAPACITY,
		       &decoded_slot, &decoded_generation));
	assert(decoded_slot == slot);
	assert(decoded_generation == generation);
}

static void test_aux_token_roundtrip_and_special_namespace(void)
{
	uint32_t slot;
	uint32_t generation;

	check_aux_roundtrip(0U, 1U);
	check_aux_roundtrip(15U, UINT32_C(0x12345678));
	check_aux_roundtrip(7U, UINT32_MAX);

	/* generation 0 从来不是有效 capability。 */
	assert(!tr_reactor_aux_token_decode(
		tr_reactor_aux_token(1U, 0U),
		TEST_AUX_CAPACITY, &slot, &generation));

	/* 固定内部 token 的低 16 位不属于 aux slot，因此不会被误解码。 */
	assert(!tr_reactor_aux_token_decode(
		UINT64_MAX, TEST_AUX_CAPACITY, &slot, &generation));
	assert(!tr_reactor_aux_token_decode(
		UINT64_MAX - UINT64_C(1),
		TEST_AUX_CAPACITY, &slot, &generation));
	assert(!tr_reactor_aux_token_decode(
		UINT64_MAX - UINT64_C(2),
		TEST_AUX_CAPACITY, &slot, &generation));

	/* 普通 connection token 的高 16 位不在 aux namespace。 */
	assert(!tr_reactor_aux_token_decode(
		connection_token(4U, TR_REACTOR_CONNECTION_GENERATION_MAX),
		TEST_AUX_CAPACITY, &slot, &generation));

	/* slot 超出真实 aux capacity 也必须 fail closed。 */
	assert(!tr_reactor_aux_token_decode(
		tr_reactor_aux_token(TEST_AUX_CAPACITY, 1U),
		TEST_AUX_CAPACITY, &slot, &generation));
}

int main(void)
{
	test_connection_generation_skips_internal_namespace();
	test_aux_generation_uses_full_uint32_range();
	test_aux_token_roundtrip_and_special_namespace();
	return 0;
}
