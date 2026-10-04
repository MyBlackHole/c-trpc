#ifndef TR_MEMORY_BUDGET_H
#define TR_MEMORY_BUDGET_H

#include <stdatomic.h>
#include <stdint.h>

#include "tr/status.h"

/*
 * Thread-safe byte budget shared by one RuntimeShard resource domain.
 *
 * limit_bytes == 0 means accounting-only/unbounded. This lets Phase 7 attach
 * consumers incrementally without pretending the stable facade already has a
 * complete shard-memory limit.
 */
struct tr_memory_budget {
	uint64_t limit_bytes;
	_Atomic uint64_t current_bytes;
	_Atomic uint64_t peak_bytes;
	_Atomic uint64_t rejection_events;
};

struct tr_memory_budget_stats {
	uint64_t limit_bytes;
	uint64_t current_bytes;
	uint64_t peak_bytes;
	uint64_t rejection_events;
};

static inline void
tr_memory_budget_init(struct tr_memory_budget *budget, uint64_t limit_bytes)
{
	if (!budget)
		return;

	budget->limit_bytes = limit_bytes;
	atomic_init(&budget->current_bytes, 0U);
	atomic_init(&budget->peak_bytes, 0U);
	atomic_init(&budget->rejection_events, 0U);
}

static inline int
tr_memory_budget_reserve(struct tr_memory_budget *budget, uint64_t bytes)
{
	uint64_t current;
	uint64_t next;
	uint64_t peak;

	if (!budget)
		return TR_ERR_INVALID;
	if (bytes == 0U)
		return TR_OK;

	current = atomic_load_explicit(
		&budget->current_bytes, memory_order_relaxed);
	for (;;) {
		if (bytes > UINT64_MAX - current)
			return TR_ERR_STATE;
		next = current + bytes;
		if (budget->limit_bytes != 0U &&
		    next > budget->limit_bytes) {
			(void)atomic_fetch_add_explicit(
				&budget->rejection_events, 1U,
				memory_order_relaxed);
			return TR_AGAIN;
		}
		if (atomic_compare_exchange_weak_explicit(
			    &budget->current_bytes, &current, next,
			    memory_order_acq_rel, memory_order_relaxed))
			break;
	}

	peak = atomic_load_explicit(&budget->peak_bytes, memory_order_relaxed);
	while (peak < next &&
	       !atomic_compare_exchange_weak_explicit(
		       &budget->peak_bytes, &peak, next,
		       memory_order_relaxed, memory_order_relaxed))
		;
	return TR_OK;
}

static inline int
tr_memory_budget_release(struct tr_memory_budget *budget, uint64_t bytes)
{
	uint64_t current;

	if (!budget)
		return TR_ERR_INVALID;
	if (bytes == 0U)
		return TR_OK;

	current = atomic_load_explicit(
		&budget->current_bytes, memory_order_relaxed);
	for (;;) {
		if (current < bytes)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &budget->current_bytes, &current,
			    current - bytes, memory_order_acq_rel,
			    memory_order_relaxed))
			return TR_OK;
	}
}

static inline void
tr_memory_budget_get_stats(
	const struct tr_memory_budget *budget,
	struct tr_memory_budget_stats *out)
{
	if (!out)
		return;

	out->limit_bytes = 0U;
	out->current_bytes = 0U;
	out->peak_bytes = 0U;
	out->rejection_events = 0U;
	if (!budget)
		return;

	out->limit_bytes = budget->limit_bytes;
	out->current_bytes = atomic_load_explicit(
		&budget->current_bytes, memory_order_relaxed);
	out->peak_bytes = atomic_load_explicit(
		&budget->peak_bytes, memory_order_relaxed);
	out->rejection_events = atomic_load_explicit(
		&budget->rejection_events, memory_order_relaxed);
}

#endif
