#include "builder.h"

#include "storage.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct tr_message_builder {
	struct tr_message_storage storage;
	struct tr_memory_budget *budget;
	uint8_t *data;
	size_t capacity;
	size_t used;
	unsigned metadata_reserved;
	unsigned storage_reserved;
};

static void tr_message_builder_release(void *arg)
{
	struct tr_message_builder *builder = arg;
	struct tr_memory_budget *budget = builder->budget;

	free(builder->data);
	free(builder);
	(void)tr_memory_budget_release(budget,
	                               sizeof(struct tr_message_builder));
}

int tr_message_builder_create(struct tr_memory_budget *budget, size_t capacity,
                              struct tr_message_builder **out)
{
	struct tr_message_builder *builder;
	int ret;

	if (!budget || !budget->limit_bytes || !out)
		return TR_ERR_INVALID;
	if (*out)
		return TR_ERR_STATE;
	if (capacity > UINT64_MAX - sizeof(*builder))
		return TR_ERR_BAD_LENGTH;

	ret = tr_memory_budget_reserve(budget, sizeof(*builder));
	if (ret != TR_OK)
		return ret;
	builder = calloc(1, sizeof(*builder));
	if (!builder) {
		(void)tr_memory_budget_release(budget, sizeof(*builder));
		return TR_ERR_NOMEM;
	}
	builder->budget = budget;
	builder->capacity = capacity;
	builder->metadata_reserved = 1U;
	ret = tr_message_storage_reserve(&builder->storage, budget, capacity);
	if (ret != TR_OK)
		goto fail;
	builder->storage_reserved = 1U;
	/* 空消息仍需要一个稳定的非 NULL backing address；只按0字节
	 * 预约载荷，控制块和最小分配不混进调用方的载荷预算。 */
	builder->data = malloc(capacity ? capacity : 1U);
	if (!builder->data) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	*out = builder;
	return TR_OK;

fail:
	if (builder->storage_reserved)
		(void)tr_message_storage_cancel(&builder->storage);
	free(builder);
	(void)tr_memory_budget_release(budget, sizeof(*builder));
	return ret;
}

int tr_message_builder_append(struct tr_message_builder *builder,
                              const void *data, size_t len)
{
	if (!builder || (len && !data))
		return TR_ERR_INVALID;
	if (len > builder->capacity - builder->used)
		return TR_ERR_BAD_LENGTH;
	if (len)
		memcpy(builder->data + builder->used, data, len);
	builder->used += len;
	return TR_OK;
}

int tr_message_builder_publish(struct tr_message_builder **pointer,
                               struct tr_message *out)
{
	struct tr_message_builder *builder;
	int ret;

	if (!pointer || !out)
		return TR_ERR_INVALID;
	builder = *pointer;
	if (!builder)
		return TR_ERR_STATE;
	if (builder->used != builder->capacity)
		return TR_AGAIN;
	ret = tr_message_storage_publish(
	        &builder->storage, builder->data, builder->used,
	        tr_message_builder_release, builder, out);
	if (ret != TR_OK)
		return ret;
	builder->storage_reserved = 0U;
	builder->metadata_reserved = 0U;
	*pointer = NULL;
	return TR_OK;
}

int tr_message_builder_cancel(struct tr_message_builder **pointer)
{
	struct tr_message_builder *builder;
	int ret;

	if (!pointer)
		return TR_ERR_INVALID;
	builder = *pointer;
	if (!builder)
		return TR_ERR_STATE;
	free(builder->data);
	builder->data = NULL;
	if (builder->storage_reserved) {
		ret = tr_message_storage_cancel(&builder->storage);
		if (ret != TR_OK)
			return ret;
		builder->storage_reserved = 0U;
	}
	if (builder->metadata_reserved) {
		ret = tr_memory_budget_release(builder->budget,
		                               sizeof(*builder));
		if (ret != TR_OK)
			return ret;
		builder->metadata_reserved = 0U;
	}
	free(builder);
	*pointer = NULL;
	return TR_OK;
}
