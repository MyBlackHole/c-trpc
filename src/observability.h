#ifndef TR_OBSERVABILITY_H
#define TR_OBSERVABILITY_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 低成本计数器和高水位始终采集。
 * 计时直方图按需启用，因为它们需要在热调度路径读取单调时钟。
 */
#define TR_OBSERVABILITY_TIMING (1U << 0)
#define TR_OBSERVABILITY_VALID_FLAGS TR_OBSERVABILITY_TIMING

#define TR_LATENCY_HISTOGRAM_BUCKETS 64U

/*
 * 固定的以 2 为底纳秒直方图。
 *
 * bucket[0] 包含 0..1 ns。
 * bucket[i]（1 <= i < 63）包含 2^i .. 2^(i+1)-1 ns。
 * bucket[63] 包含 >= 2^63 ns 的值。
 */
struct tr_latency_histogram {
	uint64_t samples;
	uint64_t total_ns;
	uint64_t max_ns;
	uint64_t buckets[TR_LATENCY_HISTOGRAM_BUCKETS];
};

/* 运行时诊断统一使用的有界队列快照语义。 */
struct tr_queue_observation {
	uint32_t capacity;
	uint32_t current;
	uint32_t peak;
	uint64_t full_events;
};

/* 统一的有界资源池快照语义。 */
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
