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

static inline void
tr_merge_latency_histogram(struct tr_latency_histogram *dst,
			   const struct tr_latency_histogram *src)
{
	uint32_t i;

	if (!dst || !src)
		return;

	dst->samples += src->samples;
	dst->total_ns += src->total_ns;
	if (src->max_ns > dst->max_ns)
		dst->max_ns = src->max_ns;
	for (i = 0; i < TR_LATENCY_HISTOGRAM_BUCKETS; ++i)
		dst->buckets[i] += src->buckets[i];
}

#endif
