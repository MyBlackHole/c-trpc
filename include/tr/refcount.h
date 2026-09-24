#ifndef TR_REFCOUNT_H
#define TR_REFCOUNT_H

#include <stdatomic.h>
#include <stdint.h>

#include "tr/status.h"

struct tr_refcount {
	_Atomic uint32_t value;
};

/*
 * Refcounts are strong references:
 * - zero is dead and must never be resurrected;
 * - UINT32_MAX is reserved so increment cannot wrap;
 * - put() returns 1 only for the transition 1 -> 0.
 */
static inline int tr_refcount_init(struct tr_refcount *ref, uint32_t initial)
{
	if (!ref || initial == 0 || initial == UINT32_MAX)
		return TR_ERR_INVALID;

	atomic_init(&ref->value, initial);
	return TR_OK;
}

static inline uint32_t tr_refcount_read(const struct tr_refcount *ref)
{
	if (!ref)
		return 0;

	return atomic_load_explicit(&ref->value, memory_order_acquire);
}

static inline int tr_refcount_get(struct tr_refcount *ref)
{
	uint32_t old;

	if (!ref)
		return TR_ERR_INVALID;

	old = atomic_load_explicit(&ref->value, memory_order_relaxed);
	for (;;) {
		if (old == 0 || old == UINT32_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &ref->value, &old, old + 1U,
			    memory_order_acquire, memory_order_relaxed))
			return TR_OK;
	}
}

/*
 * Returns 1 when a reference was acquired, 0 when the object is already dead,
 * or a negative tr_status on invalid/overflow state.
 */
static inline int tr_refcount_get_unless_zero(struct tr_refcount *ref)
{
	uint32_t old;

	if (!ref)
		return TR_ERR_INVALID;

	old = atomic_load_explicit(&ref->value, memory_order_relaxed);
	for (;;) {
		if (old == 0)
			return 0;
		if (old == UINT32_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &ref->value, &old, old + 1U,
			    memory_order_acquire, memory_order_relaxed))
			return 1;
	}
}

/*
 * Returns 1 when this was the final reference, 0 when references remain,
 * or a negative tr_status on underflow/invalid state.
 */
static inline int tr_refcount_put(struct tr_refcount *ref)
{
	uint32_t old;

	if (!ref)
		return TR_ERR_INVALID;

	old = atomic_load_explicit(&ref->value, memory_order_relaxed);
	for (;;) {
		if (old == 0)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &ref->value, &old, old - 1U,
			    memory_order_acq_rel, memory_order_relaxed))
			return old == 1U ? 1 : 0;
	}
}

#endif
