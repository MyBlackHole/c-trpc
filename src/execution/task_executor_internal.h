#ifndef TR_TASK_EXECUTOR_INTERNAL_H
#define TR_TASK_EXECUTOR_INTERNAL_H

#include <stdatomic.h>
#include <stdint.h>
#include "tr/status.h"

/* 只由本模块及其白盒测试使用；不以回绕重新授予过期能力。 */
static inline int tr_task_generation_next(uint64_t *value)
{
	if (*value == UINT64_MAX)
		return TR_ERR_STATE;
	++*value;
	return TR_OK;
}

static inline int tr_task_instance_next(_Atomic uint64_t *counter,
                                        uint64_t *out)
{
	uint64_t value = atomic_load_explicit(counter, memory_order_relaxed);

	for (;;) {
		if (value == UINT64_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            counter, &value, value + 1, memory_order_relaxed,
		            memory_order_relaxed)) {
			*out = value + 1;
			return TR_OK;
		}
	}
}

#endif
