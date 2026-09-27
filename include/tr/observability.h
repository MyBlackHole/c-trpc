#ifndef TR_OBSERVABILITY_H
#define TR_OBSERVABILITY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Cheap counters/high-water marks are always collected. Timing histograms are
 * opt-in because they require monotonic-clock reads on hot scheduling paths.
 */
#define TR_OBSERVABILITY_TIMING (1U << 0)
#define TR_OBSERVABILITY_VALID_FLAGS TR_OBSERVABILITY_TIMING

#define TR_LATENCY_HISTOGRAM_BUCKETS 64U

/*
 * Fixed log2 nanosecond histogram.
 *
 * bucket[0] contains 0..1 ns.
 * bucket[i] (1 <= i < 63) contains 2^i .. 2^(i+1)-1 ns.
 * bucket[63] contains values >= 2^63 ns.
 */
struct tr_latency_histogram {
	uint64_t samples;
	uint64_t total_ns;
	uint64_t max_ns;
	uint64_t buckets[TR_LATENCY_HISTOGRAM_BUCKETS];
};

/* Common bounded-queue snapshot semantics used across runtime diagnostics. */
struct tr_queue_observation {
	uint32_t capacity;
	uint32_t current;
	uint32_t peak;
	uint64_t full_events;
};

/* Common bounded-pool snapshot semantics. */
struct tr_pool_observation {
	uint32_t capacity;
	uint32_t current;
	uint32_t peak;
	uint64_t exhausted_events;
};

#ifdef __cplusplus
}
#endif

#endif
