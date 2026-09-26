#include "crc32c_internal.h"

#if TR_CRC32C_X86_SSE42
#include <cpuid.h>
#include <nmmintrin.h>
#include <pthread.h>
#include <string.h>
#endif

/*
 * Reflected Castagnoli polynomial 0x82f63b78. Entry i is obtained by eight
 * steps of: crc = (crc >> 1) ^ (crc & 1 ? polynomial : 0), starting at i.
 * Read-only from process start: no table initialization or allocation.
 */
static const uint32_t tr_crc32c_table[256] = {
	0x00000000U, 0xf26b8303U, 0xe13b70f7U, 0x1350f3f4U,
	0xc79a971fU, 0x35f1141cU, 0x26a1e7e8U, 0xd4ca64ebU,
	0x8ad958cfU, 0x78b2dbccU, 0x6be22838U, 0x9989ab3bU,
	0x4d43cfd0U, 0xbf284cd3U, 0xac78bf27U, 0x5e133c24U,
	0x105ec76fU, 0xe235446cU, 0xf165b798U, 0x030e349bU,
	0xd7c45070U, 0x25afd373U, 0x36ff2087U, 0xc494a384U,
	0x9a879fa0U, 0x68ec1ca3U, 0x7bbcef57U, 0x89d76c54U,
	0x5d1d08bfU, 0xaf768bbcU, 0xbc267848U, 0x4e4dfb4bU,
	0x20bd8edeU, 0xd2d60dddU, 0xc186fe29U, 0x33ed7d2aU,
	0xe72719c1U, 0x154c9ac2U, 0x061c6936U, 0xf477ea35U,
	0xaa64d611U, 0x580f5512U, 0x4b5fa6e6U, 0xb93425e5U,
	0x6dfe410eU, 0x9f95c20dU, 0x8cc531f9U, 0x7eaeb2faU,
	0x30e349b1U, 0xc288cab2U, 0xd1d83946U, 0x23b3ba45U,
	0xf779deaeU, 0x05125dadU, 0x1642ae59U, 0xe4292d5aU,
	0xba3a117eU, 0x4851927dU, 0x5b016189U, 0xa96ae28aU,
	0x7da08661U, 0x8fcb0562U, 0x9c9bf696U, 0x6ef07595U,
	0x417b1dbcU, 0xb3109ebfU, 0xa0406d4bU, 0x522bee48U,
	0x86e18aa3U, 0x748a09a0U, 0x67dafa54U, 0x95b17957U,
	0xcba24573U, 0x39c9c670U, 0x2a993584U, 0xd8f2b687U,
	0x0c38d26cU, 0xfe53516fU, 0xed03a29bU, 0x1f682198U,
	0x5125dad3U, 0xa34e59d0U, 0xb01eaa24U, 0x42752927U,
	0x96bf4dccU, 0x64d4cecfU, 0x77843d3bU, 0x85efbe38U,
	0xdbfc821cU, 0x2997011fU, 0x3ac7f2ebU, 0xc8ac71e8U,
	0x1c661503U, 0xee0d9600U, 0xfd5d65f4U, 0x0f36e6f7U,
	0x61c69362U, 0x93ad1061U, 0x80fde395U, 0x72966096U,
	0xa65c047dU, 0x5437877eU, 0x4767748aU, 0xb50cf789U,
	0xeb1fcbadU, 0x197448aeU, 0x0a24bb5aU, 0xf84f3859U,
	0x2c855cb2U, 0xdeeedfb1U, 0xcdbe2c45U, 0x3fd5af46U,
	0x7198540dU, 0x83f3d70eU, 0x90a324faU, 0x62c8a7f9U,
	0xb602c312U, 0x44694011U, 0x5739b3e5U, 0xa55230e6U,
	0xfb410cc2U, 0x092a8fc1U, 0x1a7a7c35U, 0xe811ff36U,
	0x3cdb9bddU, 0xceb018deU, 0xdde0eb2aU, 0x2f8b6829U,
	0x82f63b78U, 0x709db87bU, 0x63cd4b8fU, 0x91a6c88cU,
	0x456cac67U, 0xb7072f64U, 0xa457dc90U, 0x563c5f93U,
	0x082f63b7U, 0xfa44e0b4U, 0xe9141340U, 0x1b7f9043U,
	0xcfb5f4a8U, 0x3dde77abU, 0x2e8e845fU, 0xdce5075cU,
	0x92a8fc17U, 0x60c37f14U, 0x73938ce0U, 0x81f80fe3U,
	0x55326b08U, 0xa759e80bU, 0xb4091bffU, 0x466298fcU,
	0x1871a4d8U, 0xea1a27dbU, 0xf94ad42fU, 0x0b21572cU,
	0xdfeb33c7U, 0x2d80b0c4U, 0x3ed04330U, 0xccbbc033U,
	0xa24bb5a6U, 0x502036a5U, 0x4370c551U, 0xb11b4652U,
	0x65d122b9U, 0x97baa1baU, 0x84ea524eU, 0x7681d14dU,
	0x2892ed69U, 0xdaf96e6aU, 0xc9a99d9eU, 0x3bc21e9dU,
	0xef087a76U, 0x1d63f975U, 0x0e330a81U, 0xfc588982U,
	0xb21572c9U, 0x407ef1caU, 0x532e023eU, 0xa145813dU,
	0x758fe5d6U, 0x87e466d5U, 0x94b49521U, 0x66df1622U,
	0x38cc2a06U, 0xcaa7a905U, 0xd9f75af1U, 0x2b9cd9f2U,
	0xff56bd19U, 0x0d3d3e1aU, 0x1e6dcdeeU, 0xec064eedU,
	0xc38d26c4U, 0x31e6a5c7U, 0x22b65633U, 0xd0ddd530U,
	0x0417b1dbU, 0xf67c32d8U, 0xe52cc12cU, 0x1747422fU,
	0x49547e0bU, 0xbb3ffd08U, 0xa86f0efcU, 0x5a048dffU,
	0x8ecee914U, 0x7ca56a17U, 0x6ff599e3U, 0x9d9e1ae0U,
	0xd3d3e1abU, 0x21b862a8U, 0x32e8915cU, 0xc083125fU,
	0x144976b4U, 0xe622f5b7U, 0xf5720643U, 0x07198540U,
	0x590ab964U, 0xab613a67U, 0xb831c993U, 0x4a5a4a90U,
	0x9e902e7bU, 0x6cfbad78U, 0x7fab5e8cU, 0x8dc0dd8fU,
	0xe330a81aU, 0x115b2b19U, 0x020bd8edU, 0xf0605beeU,
	0x24aa3f05U, 0xd6c1bc06U, 0xc5914ff2U, 0x37faccf1U,
	0x69e9f0d5U, 0x9b8273d6U, 0x88d28022U, 0x7ab90321U,
	0xae7367caU, 0x5c18e4c9U, 0x4f48173dU, 0xbd23943eU,
	0xf36e6f75U, 0x0105ec76U, 0x12551f82U, 0xe03e9c81U,
	0x34f4f86aU, 0xc69f7b69U, 0xd5cf889dU, 0x27a40b9eU,
	0x79b737baU, 0x8bdcb4b9U, 0x988c474dU, 0x6ae7c44eU,
	0xbe2da0a5U, 0x4c4623a6U, 0x5f16d052U, 0xad7d5351U,
};

uint32_t tr_crc32c_update_portable(uint32_t state, const void *data, size_t len)
{
	const uint8_t *p = data;

	while (len != 0) {
		state = (state >> 8) ^ tr_crc32c_table[(state ^ *p++) & 0xffU];
		len--;
	}
	return state;
}

#if TR_CRC32C_X86_SSE42
/* Keep the dispatcher and the rest of the library at the baseline ISA. */
__attribute__((target("sse4.2")))
uint32_t tr_crc32c_update_sse42(uint32_t state, const void *data, size_t len)
{
	const uint8_t *p = data;

	/* memcpy permits unaligned input without aliasing or out-of-bounds loads. */
	while (len >= sizeof(uint64_t)) {
		uint64_t word;

		memcpy(&word, p, sizeof(word));
		state = (uint32_t)_mm_crc32_u64(state, word);
		p += sizeof(word);
		len -= sizeof(word);
	}
	if (len >= sizeof(uint32_t)) {
		uint32_t word;

		memcpy(&word, p, sizeof(word));
		state = _mm_crc32_u32(state, word);
		p += sizeof(word);
		len -= sizeof(word);
	}
	if (len >= sizeof(uint16_t)) {
		uint16_t word;

		memcpy(&word, p, sizeof(word));
		state = _mm_crc32_u16(state, word);
		p += sizeof(word);
		len -= sizeof(word);
	}
	if (len != 0)
		state = _mm_crc32_u8(state, *p);
	return state;
}
#endif

int tr_crc32c_sse42_available(void)
{
#if TR_CRC32C_X86_SSE42
	unsigned eax, ebx, ecx, edx;

	return __get_cpuid(1U, &eax, &ebx, &ecx, &edx) &&
	       (ecx & bit_SSE4_2) != 0;
#else
	return 0;
#endif
}

#if TR_CRC32C_X86_SSE42
static pthread_once_t tr_crc32c_once = PTHREAD_ONCE_INIT;
static uint32_t (*tr_crc32c_backend)(uint32_t, const void *, size_t) =
	tr_crc32c_update_portable;

static void tr_crc32c_select_backend(void)
{
	if (tr_crc32c_sse42_available())
		tr_crc32c_backend = tr_crc32c_update_sse42;
}
#endif

uint32_t tr_crc32c_begin(void)
{
	return 0xffffffffU;
}

uint32_t tr_crc32c_update(uint32_t state, const void *data, size_t len)
{
	/* Empty updates accept NULL, preserve any raw state, and need no dispatch. */
	if (len == 0)
		return state;
#if TR_CRC32C_X86_SSE42
	/* pthread_once publishes the immutable function pointer to every caller. */
	(void)pthread_once(&tr_crc32c_once, tr_crc32c_select_backend);
	return tr_crc32c_backend(state, data, len);
#else
	return tr_crc32c_update_portable(state, data, len);
#endif
}

uint32_t tr_crc32c_finish(uint32_t state)
{
	return state ^ 0xffffffffU;
}

uint32_t tr_crc32c(const void *data, size_t len)
{
	return tr_crc32c_finish(tr_crc32c_update(tr_crc32c_begin(), data, len));
}
