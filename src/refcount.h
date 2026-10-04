#ifndef TR_REFCOUNT_H
#define TR_REFCOUNT_H

#include <stdatomic.h>
#include <stdint.h>

#include "tr/status.h"

struct tr_refcount {
	_Atomic uint32_t value;
};

/*
 * refcount 表示强引用（strong reference）：
 * - 0 表示对象已经死亡，禁止从 0 恢复引用；
 * - 接近 UINT32_MAX 时拒绝继续增长，避免计数回绕；
 * - 只有 1 -> 0 的最后一次 put() 返回 1。
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
		if (old == 0 || old >= UINT32_MAX - 1U)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &ref->value, &old, old + 1U,
			    memory_order_acquire, memory_order_relaxed))
			return TR_OK;
	}
}

/*
 * 成功获得强引用时返回 1；对象已经死亡（ref == 0）时返回 0；
 * 参数非法或引用计数接近溢出时返回负的 tr_status。
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
		if (old >= UINT32_MAX - 1U)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
			    &ref->value, &old, old + 1U,
			    memory_order_acquire, memory_order_relaxed))
			return 1;
	}
}

/*
 * 如果释放的是最后一个强引用则返回 1；仍有其他引用时返回 0；
 * underflow 或参数非法时返回负的 tr_status。
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
