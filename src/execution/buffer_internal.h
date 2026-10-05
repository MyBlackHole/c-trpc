#ifndef TR_BUFFER_INTERNAL_H
#define TR_BUFFER_INTERNAL_H

#include <stdint.h>
#include "buffer.h"

struct tr_memory_budget;

/*
 * 内部有界按需资源池：buffer_count 限制并发所有权数量。
 * 已取出的描述符按需增长到 max_buffer_size，并保留容量供稳态复用。
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
