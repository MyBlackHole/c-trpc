#define _POSIX_C_SOURCE 200809L
#include "codec.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
static volatile uint64_t sink;
static uint64_t ticks(clockid_t clock)
{
	struct timespec t;
	assert(clock_gettime(clock, &t) == 0);
	return (uint64_t)t.tv_sec * UINT64_C(1000000000) + (uint64_t)t.tv_nsec;
}
static void measure(enum format format, unsigned version, unsigned length,
                    unsigned trial)
{
	uint8_t data[SPIKE_DATA_MAX], encoded[SPIKE_FRAME_MAX],
	        edited[SPIKE_FRAME_MAX];
	memset(data, 0xa5, sizeof(data));
	struct message input = {7U, data, length, (const uint8_t *)"new",
	                        3U, 1};
	size_t size = 0;
	assert(spike_encode(format, version, &input, encoded, sizeof(encoded),
	                    &size) == OUT_OK);
	struct view decoded;
	assert(spike_decode(format, version, encoded, size, 64U, &decoded) ==
	       OUT_OK);
	unsigned iterations = length >= 65536U  ? 2000U
	                      : length >= 4096U ? 8000U
	                                        : 50000U;
	for (unsigned phase = 0; phase < 4U; ++phase) {
		uint64_t wall = ticks(CLOCK_MONOTONIC),
		         cpu = ticks(CLOCK_PROCESS_CPUTIME_ID);
		for (unsigned i = 0; i < iterations; ++i) {
			if (phase == 0U) {
				assert(spike_encode(format, version, &input,
				                    encoded, sizeof(encoded),
				                    &size) == OUT_OK);
				sink += encoded[size - 1U];
			} else if (phase == 1U) {
				assert(spike_decode(format, version, encoded,
				                    size, 64U,
				                    &decoded) == OUT_OK);
				sink += decoded.message.offset;
			} else if (phase == 2U)
				sink += spike_consume(&decoded);
			else {
				assert(spike_edit_offset(&decoded, i, edited,
				                         sizeof(edited)) ==
				       OUT_OK);
				sink += edited[size - 1U];
			}
		}
		cpu = ticks(CLOCK_PROCESS_CPUTIME_ID) - cpu;
		wall = ticks(CLOCK_MONOTONIC) - wall;
		const char *names[] = {"encode", "verify", "consume_all",
		                       "copy_edit"};
		printf("{\"format\":\"%s\",\"version\":%u,\"data_bytes\":%u,"
		       "\"trial\":%u,\"phase\":\"%s\",\"iterations\":%u,\"body_"
		       "bytes\":%zu,\"shared_frame_bytes\":4,\"cpu_ns_per_op\":"
		       "%.3f,\"wall_ns_per_op\":%.3f}\n",
		       format == FORMAT_COMPACT ? "compact_candidate"
		                                : "block_candidate",
		       version, length, trial, names[phase], iterations,
		       size - 4U, (double)cpu / iterations,
		       (double)wall / iterations);
	}
}
int main(int argc, char **argv)
{
	spike_check();
	if (argc == 2 && !strcmp(argv[1], "--check")) {
		puts("{\"type\":\"spike_check\",\"formats\":2,\"writer_"
		     "versions\":2,\"reader_versions\":2,\"length_cases\":6,"
		     "\"truncation_checks\":true,\"bounded_work\":true,"
		     "\"unknown_label_edit_roundtrip\":true}");
		return 0;
	}
	assert(argc == 1);
	const unsigned lengths[] = {0U, 1U, 4096U, 65536U};
	for (unsigned trial = 0; trial < 9U; ++trial)
		for (unsigned version = 1U; version <= 2U; ++version)
			for (unsigned i = 0;
			     i < sizeof(lengths) / sizeof(lengths[0]); ++i)
				for (unsigned order = 0; order < 2U; ++order)
					measure((enum format)((trial + order) %
					                      2U),
					        version, lengths[i], trial);
	return 0;
}
