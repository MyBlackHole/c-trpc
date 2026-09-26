#define _GNU_SOURCE
#include "tr/crc32c.h"
#include "../src/crc32c_internal.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define THREADS 16U
#define RANDOM_CASES 10000U
#define DATA_BYTES 8192U

struct backend {
	const char *name;
	uint32_t (*update)(uint32_t, const void *, size_t);
};

static struct backend backends[3] = {
	{ "auto", tr_crc32c_update },
	{ "portable", tr_crc32c_update_portable }
};
static size_t backend_count = 2U;

/* Independent bitwise oracle, preserving the pre-optimization raw-state API. */
static uint32_t reference(uint32_t state, const void *data, size_t len)
{
	const unsigned char *p = data;
	size_t i;

	for (i = 0; i < len; ++i) {
		unsigned bit;

		state ^= p[i];
		for (bit = 0; bit < 8U; ++bit)
			state = (state >> 1) ^
				((state & 1U) ? UINT32_C(0x82f63b78) : 0U);
	}
	return state;
}

static uint32_t random_next(uint32_t *state)
{
	*state ^= *state << 13;
	*state ^= *state >> 17;
	*state ^= *state << 5;
	return *state;
}

static void check_backends(uint32_t state, const void *data, size_t len)
{
	uint32_t expected = reference(state, data, len);
	size_t i;

	for (i = 0; i < backend_count; ++i) {
		uint32_t actual = backends[i].update(state, data, len);

		if (actual != expected)
			fprintf(stderr, "%s: len=%zu state=%08x expected=%08x actual=%08x\n",
				backends[i].name, len, (unsigned)state,
				(unsigned)expected, (unsigned)actual);
		assert(actual == expected);
	}
}

struct thread_ctx {
	pthread_barrier_t *barrier;
	unsigned index;
};

static void *first_use_thread(void *arg)
{
	const struct thread_ctx *ctx = arg;
	unsigned char data[1024];
	uint32_t seed = UINT32_C(0x5a671fe1) + ctx->index;
	size_t len = 769U + ctx->index;
	unsigned i;
	int ret;

	for (i = 0; i < sizeof(data); ++i)
		data[i] = (unsigned char)random_next(&seed);
	ret = pthread_barrier_wait(ctx->barrier);
	assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
	for (i = 0; i < 256U; ++i) {
		uint32_t state = random_next(&seed);
		uint32_t expected = reference(state, data + ctx->index, len);

		assert(tr_crc32c_update(state, data + ctx->index, len) == expected);
	}
	return NULL;
}

/* Must run before any nonempty use of the public dispatch entry point. */
static void test_concurrent_first_use(void)
{
	pthread_barrier_t barrier;
	pthread_t threads[THREADS];
	struct thread_ctx contexts[THREADS];
	unsigned i;
	int ret;

	assert(pthread_barrier_init(&barrier, NULL, THREADS + 1U) == 0);
	for (i = 0; i < THREADS; ++i) {
		contexts[i].barrier = &barrier;
		contexts[i].index = i;
		assert(pthread_create(&threads[i], NULL, first_use_thread, &contexts[i]) == 0);
	}
	ret = pthread_barrier_wait(&barrier);
	assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
	for (i = 0; i < THREADS; ++i)
		assert(pthread_join(threads[i], NULL) == 0);
	assert(pthread_barrier_destroy(&barrier) == 0);
	puts("concurrent first dispatch (16 threads): ok");
}

static void test_vectors_and_empty(void)
{
	unsigned char data[32];
	unsigned i;

	assert(tr_crc32c_begin() == UINT32_C(0xffffffff));
	assert(tr_crc32c_finish(0U) == UINT32_C(0xffffffff));
	assert(tr_crc32c(NULL, 0U) == 0U);
	assert(tr_crc32c("123456789", 9U) == UINT32_C(0xe3069283));
	memset(data, 0, sizeof(data));
	assert(tr_crc32c(data, sizeof(data)) == UINT32_C(0x8a9136aa));
	memset(data, 0xff, sizeof(data));
	assert(tr_crc32c(data, sizeof(data)) == UINT32_C(0x62a8ab43));
	for (i = 0; i < sizeof(data); ++i)
		data[i] = (unsigned char)i;
	assert(tr_crc32c(data, sizeof(data)) == UINT32_C(0x46dd794e));
	for (i = 0; i < sizeof(data); ++i)
		data[i] = (unsigned char)(31U - i);
	assert(tr_crc32c(data, sizeof(data)) == UINT32_C(0x113fdb5c));
	check_backends(0U, NULL, 0U);
	check_backends(UINT32_MAX, NULL, 0U);
	check_backends(UINT32_C(0x39d48f71), NULL, 0U);
	puts("known answers / empty raw state: ok");
}

static void test_offsets_and_splits(void)
{
	unsigned char data[320];
	size_t offset, len, split, b;

	for (offset = 0; offset < sizeof(data); ++offset)
		data[offset] = (unsigned char)(offset * 37U + 19U);
	for (offset = 0; offset < 64U; ++offset)
		for (len = 0; len <= 256U; ++len)
			check_backends(UINT32_C(0x719ac353), data + offset, len);
	for (offset = 0; offset < 8U; ++offset) {
		for (len = 0; len <= 128U; ++len) {
			uint32_t expected = reference(UINT32_MAX, data + offset, len);

			for (split = 0; split <= len; ++split) {
				for (b = 0; b < backend_count; ++b) {
					uint32_t state = backends[b].update(UINT32_MAX,
						data + offset, split);

					state = backends[b].update(state, NULL, 0U);
					state = backends[b].update(state,
						data + offset + split, len - split);
					assert(state == expected);
				}
			}
		}
	}
	puts("64 alignments / short tails / exhaustive split points: ok");
}

static void test_random_states(void)
{
	unsigned char data[DATA_BYTES + 64U];
	uint32_t seed = UINT32_C(0xf93ce761);
	unsigned i;

	for (i = 0; i < sizeof(data); ++i)
		data[i] = (unsigned char)random_next(&seed);
	for (i = 0; i < RANDOM_CASES; ++i) {
		uint32_t state = random_next(&seed);
		size_t offset = random_next(&seed) % 64U;
		size_t len = random_next(&seed) % (DATA_BYTES + 1U);
		size_t split = random_next(&seed) % (len + 1U);
		uint32_t expected;
		size_t b;

		data[random_next(&seed) % sizeof(data)] ^= (unsigned char)seed;
		expected = reference(state, data + offset, len);
		check_backends(state, data + offset, len);
		for (b = 0; b < backend_count; ++b) {
			uint32_t part = backends[b].update(state, data + offset, split);

			assert(backends[b].update(part, data + offset + split,
						 len - split) == expected);
		}
	}
	puts("10000 randomized contents / lengths / raw states / splits: ok");
}

static void test_large_incremental(void)
{
	const size_t len = 1024U * 1024U + 7U;
	unsigned char *data = malloc(len);
	uint32_t expected;
	size_t i, b;

	assert(data);
	for (i = 0; i < len; ++i)
		data[i] = (unsigned char)(i * 17U + i / 251U);
	expected = reference(UINT32_MAX, data, len);
	assert(tr_crc32c(data, len) == tr_crc32c_finish(expected));
	for (b = 0; b < backend_count; ++b) {
		uint32_t state = UINT32_MAX;
		size_t pos = 0;

		while (pos < len) {
			size_t count = 1U + pos % 127U;

			if (count > len - pos)
				count = len - pos;
			state = backends[b].update(state, data + pos, count);
			pos += count;
		}
		assert(state == expected);
	}
	free(data);
	puts("1 MiB plus tail / many incremental segments: ok");
}

static void test_guard_pages(void)
{
	long page_long = sysconf(_SC_PAGESIZE);
	size_t page, len;
	unsigned char *mapping, *data;

	assert(page_long >= 512);
	page = (size_t)page_long;
	mapping = mmap(NULL, page * 3U, PROT_NONE,
		       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	assert(mapping != MAP_FAILED);
	data = mapping + page;
	assert(mprotect(data, page, PROT_READ | PROT_WRITE) == 0);
	for (len = 0; len < page; ++len)
		data[len] = (unsigned char)(len * 13U);
	assert(mprotect(data, page, PROT_READ) == 0);
	for (len = 0; len <= 257U; ++len) {
		check_backends(UINT32_MAX, data, len);
		/* len=0 even permits a pointer to the inaccessible trailing page. */
		check_backends(UINT32_C(0x1056fa49), data + page - len, len);
	}
	check_backends(UINT32_MAX, data, page);
	check_backends(UINT32_MAX, data + 1, page - 1U);
	assert(munmap(mapping, page * 3U) == 0);
	puts("read-only input / guard pages / exact ends: ok");
}

int main(int argc, char **argv)
{
	int available = tr_crc32c_sse42_available();

	assert(setvbuf(stdout, NULL, _IONBF, 0) == 0);
	if (argc > 2 || (argc == 2 && strcmp(argv[1], "--expect-portable") != 0)) {
		fprintf(stderr, "usage: %s [--expect-portable]\n", argv[0]);
		return EXIT_FAILURE;
	}
	if (argc == 2)
		assert(!available);
	alarm(120U); /* Hang protection, not a performance threshold. */
	test_concurrent_first_use();
#if TR_CRC32C_X86_SSE42
	if (available) {
		backends[backend_count].name = "sse4.2";
		backends[backend_count++].update = tr_crc32c_update_sse42;
	}
#endif
	printf("backends: auto + portable%s\n", available ? " + sse4.2" : " (no SSE4.2)");
	test_vectors_and_empty();
	test_offsets_and_splits();
	test_random_states();
	test_large_incremental();
	test_guard_pages();
	alarm(0U);
	return 0;
}
