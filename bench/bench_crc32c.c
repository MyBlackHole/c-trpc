#define _POSIX_C_SOURCE 200809L
#include "tr/crc32c.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define SAMPLE_BYTES (16U * 1024U * 1024U)
#define SAMPLES 5U
#define BUFFER_BYTES (1024U * 1024U)

static volatile uint32_t checksum_sink;

static double now_seconds(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		perror("clock_gettime");
		exit(EXIT_FAILURE);
	}
	return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int compare_double(const void *a, const void *b)
{
	double x = *(const double *)a;
	double y = *(const double *)b;

	return (x > y) - (x < y);
}

int main(void)
{
	static const size_t sizes[] = {64U, 4096U, 65536U, BUFFER_BYTES};
	unsigned char *buffer;
	size_t i;

	if (tr_crc32c("123456789", 9U) != UINT32_C(0xe3069283)) {
		fputs("CRC32C known-answer check failed\n", stderr);
		return EXIT_FAILURE;
	}
	buffer = malloc(BUFFER_BYTES);
	if (!buffer) {
		perror("malloc");
		return EXIT_FAILURE;
	}
	for (i = 0; i < BUFFER_BYTES; ++i)
		buffer[i] = (unsigned char)(i * 17U + 3U);

	puts("# Single-thread cached-buffer CRC microbenchmark, not RPC/network throughput.");
	puts("size_bytes,samples,bytes_per_sample,median_MiB_s,min_MiB_s,max_MiB_s");
	for (i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
		double rates[SAMPLES];
		size_t repeats = SAMPLE_BYTES / sizes[i];
		unsigned sample;

		checksum_sink = tr_crc32c(buffer, sizes[i]);
		for (sample = 0; sample < SAMPLES; ++sample) {
			double begin = now_seconds();
			double elapsed;
			size_t j;

			for (j = 0; j < repeats; ++j)
				checksum_sink = tr_crc32c(buffer, sizes[i]);
			elapsed = now_seconds() - begin;
			if (elapsed <= 0.0) {
				fputs("nonpositive elapsed time\n", stderr);
				free(buffer);
				return EXIT_FAILURE;
			}
			rates[sample] = ((double)repeats * (double)sizes[i]) /
				(1024.0 * 1024.0 * elapsed);
		}
		qsort(rates, SAMPLES, sizeof(rates[0]), compare_double);
		printf("%zu,%u,%zu,%.3f,%.3f,%.3f\n", sizes[i], SAMPLES,
		       repeats * sizes[i], rates[SAMPLES / 2U], rates[0],
		       rates[SAMPLES - 1U]);
	}
	free(buffer);
	return EXIT_SUCCESS;
}
