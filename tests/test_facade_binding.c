#include "../src/facade_binding.h"

#include "tr/status.h"
#include "tr/wire.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int id_is_zero(const uint8_t id[TR_FACADE_BIND_ID_SIZE])
{
	uint8_t value = 0;
	uint32_t i;

	for (i = 0; i < TR_FACADE_BIND_ID_SIZE; ++i)
		value |= id[i];
	return value == 0;
}

static void fill_id(uint8_t id[TR_FACADE_BIND_ID_SIZE])
{
	uint32_t i;
	for (i = 0; i < TR_FACADE_BIND_ID_SIZE; ++i)
		id[i] = (uint8_t)(i + 1U);
}

static void test_roundtrip(uint16_t lane)
{
	struct tr_facade_binding input;
	struct tr_facade_binding output;
	uint8_t wire[TR_FACADE_BIND_WIRE_SIZE];

	memset(&input, 0, sizeof(input));
	input.lane = lane;
	fill_id(input.identity);

	assert(tr_facade_binding_encode(wire, &input) == TR_OK);
	assert(wire[0] == 'T' && wire[1] == 'R' && wire[2] == 'B' &&
	       wire[3] == '1');
	assert(wire[4] == 1 && wire[5] == 0);
	assert(wire[6] == (uint8_t)lane && wire[7] == 0);
	assert(memcmp(wire + 8, input.identity, TR_FACADE_BIND_ID_SIZE) == 0);
	assert(memcmp(wire + 24, "\0\0\0\0\0\0\0\0", 8) == 0);

	memset(&output, 0, sizeof(output));
	assert(tr_facade_binding_decode(wire, sizeof(wire), &output) == TR_OK);
	assert(output.lane == lane);
	assert(tr_facade_binding_id_equal(input.identity, output.identity));
}

static void test_invalid_fields(void)
{
	struct tr_facade_binding binding;
	struct tr_facade_binding decoded;
	uint8_t wire[TR_FACADE_BIND_WIRE_SIZE];

	memset(&binding, 0, sizeof(binding));
	binding.lane = TR_FACADE_BIND_LANE_CONTROL;
	assert(tr_facade_binding_encode(wire, &binding) == TR_ERR_INVALID);

	fill_id(binding.identity);
	binding.lane = 7;
	assert(tr_facade_binding_encode(wire, &binding) == TR_ERR_INVALID);

	binding.lane = TR_FACADE_BIND_LANE_CONTROL;
	assert(tr_facade_binding_encode(wire, &binding) == TR_OK);
	assert(tr_facade_binding_decode(wire, sizeof(wire) - 1, &decoded) ==
	       TR_ERR_BAD_LENGTH);

	wire[0] = 'X';
	assert(tr_facade_binding_decode(wire, sizeof(wire), &decoded) ==
	       TR_ERR_BAD_MAGIC);
	wire[0] = 'T';

	wire[4] = 2;
	assert(tr_facade_binding_decode(wire, sizeof(wire), &decoded) ==
	       TR_ERR_BAD_VERSION);
	wire[4] = 1;

	wire[6] = 9;
	assert(tr_facade_binding_decode(wire, sizeof(wire), &decoded) ==
	       TR_ERR_BAD_TYPE);
	wire[6] = TR_FACADE_BIND_LANE_CONTROL;

	wire[24] = 1;
	assert(tr_facade_binding_decode(wire, sizeof(wire), &decoded) ==
	       TR_ERR_RESERVED);
	wire[24] = 0;

	memset(wire + 8, 0, TR_FACADE_BIND_ID_SIZE);
	assert(tr_facade_binding_decode(wire, sizeof(wire), &decoded) ==
	       TR_ERR_INVALID);
}

static void test_random_identity(void)
{
	uint8_t id[TR_FACADE_BIND_ID_SIZE];
	uint8_t other[TR_FACADE_BIND_ID_SIZE];

	memset(id, 0, sizeof(id));
	assert(tr_facade_binding_generate_id(id) == TR_OK);
	assert(!id_is_zero(id));
	assert(tr_facade_binding_id_equal(id, id));

	memcpy(other, id, sizeof(other));
	other[TR_FACADE_BIND_ID_SIZE - 1] ^= 1U;
	assert(!tr_facade_binding_id_equal(id, other));
	assert(!tr_facade_binding_id_equal(NULL, other));
}

static void test_bind_frame_type(void)
{
	assert(tr_frame_type_valid(TR_FRAME_BIND));
}

int main(void)
{
	test_roundtrip(TR_FACADE_BIND_LANE_CONTROL);
	test_roundtrip(TR_FACADE_BIND_LANE_BULK);
	test_invalid_fields();
	test_random_identity();
	test_bind_frame_type();
	puts("facade binding tests passed");
	return 0;
}
