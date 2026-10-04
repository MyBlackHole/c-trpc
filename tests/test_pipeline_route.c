#include "../src/group/pipeline_route_internal.h"

#include "tr/endian.h"
#include "tr/status.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static struct tr_pipeline_route_preface control_preface(void)
{
	struct tr_pipeline_route_preface preface;

	memset(&preface, 0, sizeof(preface));
	preface.version = TR_PIPELINE_ROUTE_VERSION;
	preface.role = TR_PIPELINE_ROUTE_CONTROL;
	preface.owner_shard_id = 3U;
	preface.pipeline_id = UINT64_C(0x1122334455667788);
	preface.epoch = UINT64_C(0x0102030405060708);
	preface.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	preface.member_generation = 7U;
	return preface;
}

static struct tr_pipeline_route_preface data_preface(void)
{
	struct tr_pipeline_route_preface preface = control_preface();

	preface.role = TR_PIPELINE_ROUTE_DATA;
	preface.member_index = 5U;
	preface.member_generation = 9U;
	return preface;
}

static void test_route_roundtrip_control(void)
{
	struct tr_pipeline_route_preface in = control_preface();
	struct tr_pipeline_route_preface out;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, &in) == TR_OK);
	assert(raw[TR_PIPELINE_ROUTE_OFF_MAGIC + 0U] == 'T');
	assert(raw[TR_PIPELINE_ROUTE_OFF_MAGIC + 1U] == 'R');
	assert(raw[TR_PIPELINE_ROUTE_OFF_MAGIC + 2U] == 'R');
	assert(raw[TR_PIPELINE_ROUTE_OFF_MAGIC + 3U] == '1');
	assert(raw[TR_PIPELINE_ROUTE_OFF_PIPELINE_ID + 0U] == 0x88U);
	assert(raw[TR_PIPELINE_ROUTE_OFF_PIPELINE_ID + 7U] == 0x11U);
	assert(tr_get_le32(raw + TR_PIPELINE_ROUTE_OFF_MEMBER_INDEX) ==
	       UINT32_MAX);

	memset(&out, 0, sizeof(out));
	assert(tr_pipeline_route_preface_decode(raw, &out) == TR_OK);
	assert(tr_pipeline_route_preface_validate(raw, &out) == TR_OK);

	assert(out.version == in.version);
	assert(out.role == in.role);
	assert(out.flags == 0U);
	assert(out.owner_shard_id == in.owner_shard_id);
	assert(out.pipeline_id == in.pipeline_id);
	assert(out.epoch == in.epoch);
	assert(out.member_index == in.member_index);
	assert(out.member_generation == in.member_generation);
	assert(out.reserved == 0U);
	assert(out.header_crc32c != 0U);
}

static void test_route_roundtrip_data(void)
{
	struct tr_pipeline_route_preface in = data_preface();
	struct tr_pipeline_route_preface out;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, &in) == TR_OK);
	assert(tr_pipeline_route_preface_decode(raw, &out) == TR_OK);
	assert(tr_pipeline_route_preface_validate(raw, &out) == TR_OK);
	assert(out.role == TR_PIPELINE_ROUTE_DATA);
	assert(out.member_index == 5U);
	assert(out.member_generation == 9U);
}

static int encode_decode_validate(struct tr_pipeline_route_preface *preface)
{
	struct tr_pipeline_route_preface decoded;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, preface) == TR_OK);
	assert(tr_pipeline_route_preface_decode(raw, &decoded) == TR_OK);
	return tr_pipeline_route_preface_validate(raw, &decoded);
}

static void test_route_semantic_validation(void)
{
	struct tr_pipeline_route_preface preface;

	preface = control_preface();
	preface.version++;
	assert(encode_decode_validate(&preface) == TR_ERR_BAD_VERSION);

	preface = control_preface();
	preface.role = 99U;
	assert(encode_decode_validate(&preface) == TR_ERR_BAD_TYPE);

	preface = control_preface();
	preface.flags = 1U;
	assert(encode_decode_validate(&preface) == TR_ERR_BAD_FLAGS);

	preface = control_preface();
	preface.reserved = 1U;
	assert(encode_decode_validate(&preface) == TR_ERR_RESERVED);

	preface = control_preface();
	preface.pipeline_id = 0U;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);

	preface = control_preface();
	preface.epoch = 0U;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);

	preface = control_preface();
	preface.owner_shard_id = UINT32_MAX;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);

	preface = control_preface();
	preface.member_generation = 0U;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);

	preface = control_preface();
	preface.member_index = 0U;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);

	preface = data_preface();
	preface.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	assert(encode_decode_validate(&preface) == TR_ERR_INVALID);
}

static void test_route_corruption_validation(void)
{
	struct tr_pipeline_route_preface preface = data_preface();
	struct tr_pipeline_route_preface decoded;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, &preface) == TR_OK);

	raw[TR_PIPELINE_ROUTE_OFF_PIPELINE_ID] ^= 0x40U;
	assert(tr_pipeline_route_preface_decode(raw, &decoded) == TR_OK);
	assert(tr_pipeline_route_preface_validate(raw, &decoded) ==
	       TR_ERR_HEADER_CRC);

	assert(tr_pipeline_route_preface_encode(raw, &preface) == TR_OK);
	raw[TR_PIPELINE_ROUTE_OFF_MAGIC] = 'X';
	assert(tr_pipeline_route_preface_decode(raw, &decoded) == TR_OK);
	/*
	 * Validation intentionally checks magic before CRC so completely unrelated
	 * protocols are rejected as BAD_MAGIC rather than corruption.
	 */
	assert(tr_pipeline_route_preface_validate(raw, &decoded) ==
	       TR_ERR_BAD_MAGIC);
}

static void test_route_incremental_parser(void)
{
	struct tr_pipeline_route_preface in = data_preface();
	struct tr_pipeline_route_preface out;
	struct tr_pipeline_route_parser parser;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t consumed;
	size_t i;
	int ready;

	assert(tr_pipeline_route_preface_encode(raw, &in) == TR_OK);
	tr_pipeline_route_parser_init(&parser);
	memset(&out, 0, sizeof(out));

	for (i = 0; i < TR_PIPELINE_ROUTE_PREFACE_SIZE; ++i) {
		consumed = 99U;
		ready = -1;
		assert(tr_pipeline_route_parser_feed(
			       &parser, raw + i, 1U, &consumed, &out,
			       &ready) == TR_OK);
		assert(consumed == 1U);
		if (i + 1U < TR_PIPELINE_ROUTE_PREFACE_SIZE)
			assert(ready == 0);
		else
			assert(ready == 1);
	}

	assert(out.pipeline_id == in.pipeline_id);
	assert(out.member_index == in.member_index);
	assert(out.member_generation == in.member_generation);

	consumed = 99U;
	ready = -1;
	assert(tr_pipeline_route_parser_feed(
		       &parser, raw, 1U, &consumed, &out, &ready) ==
	       TR_ERR_STATE);
	assert(consumed == 0U);
	assert(ready == 0);
}

static void test_route_parser_preserves_trailing_transport_bytes(void)
{
	struct tr_pipeline_route_preface in = control_preface();
	struct tr_pipeline_route_preface out;
	struct tr_pipeline_route_parser parser;
	uint8_t buffer[TR_PIPELINE_ROUTE_PREFACE_SIZE + 7U];
	static const uint8_t tail[7] = { 'T', 'R', 'P', '1', 0x01, 0x00, 0x08 };
	size_t consumed = 0U;
	int ready = 0;

	assert(tr_pipeline_route_preface_encode(buffer, &in) == TR_OK);
	memcpy(buffer + TR_PIPELINE_ROUTE_PREFACE_SIZE, tail, sizeof(tail));

	tr_pipeline_route_parser_init(&parser);
	assert(tr_pipeline_route_parser_feed(
		       &parser, buffer, sizeof(buffer), &consumed,
		       &out, &ready) == TR_OK);
	assert(ready == 1);
	assert(consumed == TR_PIPELINE_ROUTE_PREFACE_SIZE);
	assert(memcmp(buffer + consumed, tail, sizeof(tail)) == 0);
}

static void test_route_parser_error_is_terminal(void)
{
	struct tr_pipeline_route_preface in = data_preface();
	struct tr_pipeline_route_preface out;
	struct tr_pipeline_route_parser parser;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t consumed;
	int ready;

	assert(tr_pipeline_route_preface_encode(raw, &in) == TR_OK);
	raw[TR_PIPELINE_ROUTE_OFF_EPOCH] ^= 1U;

	tr_pipeline_route_parser_init(&parser);
	assert(tr_pipeline_route_parser_feed(
		       &parser, raw, sizeof(raw), &consumed, &out,
		       &ready) == TR_ERR_HEADER_CRC);
	assert(consumed == sizeof(raw));
	assert(ready == 0);

	assert(tr_pipeline_route_parser_feed(
		       &parser, raw, sizeof(raw), &consumed, &out,
		       &ready) == TR_ERR_STATE);
	assert(consumed == 0U);
}

int main(void)
{
	test_route_roundtrip_control();
	test_route_roundtrip_data();
	test_route_semantic_validation();
	test_route_corruption_validation();
	test_route_incremental_parser();
	test_route_parser_preserves_trailing_transport_bytes();
	test_route_parser_error_is_terminal();
	puts("pipeline routing preface: ok");
	return 0;
}
