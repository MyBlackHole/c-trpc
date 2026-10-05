#ifndef TR_BUFFER_INTERNAL_H
#define TR_BUFFER_INTERNAL_H

#include <stdint.h>
#include "buffer.h"

struct tr_memory_budget;

/*
 * Internal bounded on-demand pool: buffer_count bounds concurrent ownership.
 * Checked-out descriptors grow lazily up to max_buffer_size and retain capacity
 * for steady-state reuse.
 */
int tr_buffer_pool_init_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t buffer_size, struct tr_memory_budget *budget);
int tr_buffer_pool_init_dynamic_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t max_buffer_size, struct tr_memory_budget *budget);

int tr_buffer_pool_init_dynamic(struct tr_buffer_pool *pool,
				uint32_t buffer_count,
				uint32_t max_buffer_size);

#endif
