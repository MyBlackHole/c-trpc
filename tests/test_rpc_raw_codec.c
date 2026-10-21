#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)

static void test_nonempty_roundtrip(void)
{
	const uint8_t source[] = {0, 0x7f, 0xff, 0x42};
	uint8_t encoded[6];
	struct tr_rpc_bytes input = {source, sizeof(source)};
	struct tr_rpc_bytes decoded = {NULL, 99};
	uint32_t written = 99;

	memset(encoded, 0xa5, sizeof(encoded));
	CHECK(tr_rpc_raw_encode(&input, encoded, sizeof(source), &written) ==
	      TR_OK);
	CHECK(written == sizeof(source));
	CHECK(!memcmp(encoded, source, sizeof(source)));
	CHECK(encoded[4] == 0xa5 && encoded[5] == 0xa5);
	CHECK(tr_rpc_raw_decode(encoded, written, &decoded) == TR_OK);
	CHECK(decoded.data == encoded && decoded.len == sizeof(source));
	CHECK(!memcmp(decoded.data, source, decoded.len));
}

static void test_empty_bytes_allow_null_data(void)
{
	const uint8_t source = 0x5a;
	struct tr_rpc_bytes input = {NULL, 0};
	struct tr_rpc_bytes decoded = {&source, 99};
	uint32_t written = 99;

	CHECK(tr_rpc_raw_encode(&input, NULL, 0, &written) == TR_OK);
	CHECK(written == 0);
	CHECK(tr_rpc_raw_decode(NULL, 0, &decoded) == TR_OK);
	CHECK(decoded.data == NULL && decoded.len == 0);

	input.data = &source;
	written = 99;
	CHECK(tr_rpc_raw_encode(&input, NULL, 0, &written) == TR_OK);
	CHECK(written == 0);
	CHECK(tr_rpc_raw_decode(&source, 0, &decoded) == TR_OK);
	CHECK(decoded.data == &source && decoded.len == 0);
}

static void test_short_capacity_preserves_outputs(void)
{
	const uint8_t source[] = {1, 2, 3, 4};
	uint8_t encoded[4];
	struct tr_rpc_bytes input = {source, sizeof(source)};
	uint32_t written = 99;

	memset(encoded, 0xa5, sizeof(encoded));
	CHECK(tr_rpc_raw_encode(&input, encoded, sizeof(source) - 1,
	                        &written) == TR_ERR_BAD_LENGTH);
	CHECK(written == 99);
	for (size_t i = 0; i < sizeof(encoded); ++i)
		CHECK(encoded[i] == 0xa5);
}

static void test_invalid_arguments_preserve_outputs(void)
{
	const uint8_t source = 0x5a;
	uint8_t encoded = 0xa5;
	struct tr_rpc_bytes input = {&source, 1};
	struct tr_rpc_bytes decoded = {&source, 99};
	uint32_t written = 99;

	CHECK(tr_rpc_raw_encode(NULL, &encoded, 1, &written) ==
	      TR_ERR_INVALID);
	CHECK(written == 99 && encoded == 0xa5);
	CHECK(tr_rpc_raw_encode(&input, &encoded, 1, NULL) == TR_ERR_INVALID);
	CHECK(encoded == 0xa5);
	input.data = NULL;
	CHECK(tr_rpc_raw_encode(&input, &encoded, 1, &written) ==
	      TR_ERR_INVALID);
	CHECK(written == 99 && encoded == 0xa5);
	input.data = &source;
	CHECK(tr_rpc_raw_encode(&input, NULL, 1, &written) ==
	      TR_ERR_INVALID);
	CHECK(written == 99);

	CHECK(tr_rpc_raw_decode(NULL, 1, &decoded) == TR_ERR_INVALID);
	CHECK(decoded.data == &source && decoded.len == 99);
	CHECK(tr_rpc_raw_decode(&source, 1, NULL) == TR_ERR_INVALID);
	CHECK(tr_rpc_raw_decode(NULL, 0, NULL) == TR_ERR_INVALID);
}

int main(void)
{
	test_nonempty_roundtrip();
	test_empty_bytes_allow_null_data();
	test_short_capacity_preserves_outputs();
	test_invalid_arguments_preserve_outputs();
	puts("rpc_raw_codec: ok");
	return 0;
}
