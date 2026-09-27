#ifndef TR_OBSERVABILITY_INTERNAL_H
#define TR_OBSERVABILITY_INTERNAL_H

#include "tr/observability.h"

#include <stdint.h>

static inline void tr_observe_high_water_u32(uint32_t *peak, uint32_t value)
{
	if (peak && value > *peak)
		*peak = value;
}

static inline uint32_t tr_observe_log2_bucket_u64(uint64_t value)
{
	if (value <= 1U)
		return 0U;
	return 63U - (uint32_t)__builtin_clzll((unsigned long long)value);
}

static inline void
tr_observe_latency_ns(struct tr_latency_histogram *histogram, uint64_t value)
{
	uint32_t bucket;

	if (!histogram)
		return;

	bucket = tr_observe_log2_bucket_u64(value);
	histogram->samples++;
	histogram->total_ns += value;
	if (value > histogram->max_ns)
		histogram->max_ns = value;
	histogram->buckets[bucket]++;
}

#endif
