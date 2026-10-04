#ifndef TR_BUFFER_INTERNAL_H
#define TR_BUFFER_INTERNAL_H

#include <stdint.h>
#include "tr/buffer.h"

/*
 * Internal bounded on-demand pool: buffer_count bounds concurrent ownership.
 * Checked-out descriptors grow lazily up to max_buffer_size and retain capacity
 * for steady-state reuse.
 */
int tr_buffer_pool_init_dynamic(struct tr_buffer_pool *pool,
				uint32_t buffer_count,
				uint32_t max_buffer_size);

#endif
