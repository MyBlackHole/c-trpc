#include "transform.h"
#include "transform_internal.h"
#include "storage.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdatomic.h>
#include <stdlib.h>

/* 取消位与终局提交共用一个原子状态，避免“检查取消后又发布成功”的窗口。 */
enum {
	TR_TRANSFORM_READY = 1U,
	TR_TRANSFORM_RUNNING = 2U,
	TR_TRANSFORM_DONE = 4U,
	TR_TRANSFORM_CANCEL = 8U
};

struct tr_transform_output {
	struct tr_message_storage storage;
	struct tr_memory_budget *budget;
	uint8_t *data;
};

struct tr_message_transform {
	_Atomic unsigned state;
	struct tr_message_transform_config config;
	struct tr_memory_budget *budget;
	struct tr_message input;
	struct tr_message output;
	struct tr_transform_output *reserved_output;
	size_t output_capacity;
	enum tr_message_transform_outcome outcome;
};

static const struct tr_message_limits tr_transform_move_limits = {
        .max_decoded_bytes = SIZE_MAX,
        .max_encoded_bytes = SIZE_MAX,
};

static void tr_transform_output_release(void *arg)
{
	struct tr_transform_output *output = arg;
	struct tr_memory_budget *budget = output->budget;

	free(output->data);
	free(output);
	/* 存储提供方私有元数据单独计费；载荷额度由 Message 在回调后归还。
	 * budget 的存活与不可重置是调用契约，正确的预约不可能在此下溢。
	 */
	(void)tr_memory_budget_release(budget, sizeof(*output));
}

static int tr_transform_output_discard(struct tr_message_transform *operation)
{
	struct tr_transform_output *output = operation->reserved_output;
	struct tr_memory_budget *budget = operation->budget;
	int ret;
	int metadata_ret;

	if (!output)
		return TR_OK;
	operation->reserved_output = NULL;
	free(output->data);
	ret = tr_message_storage_cancel(&output->storage);
	free(output);
	metadata_ret = tr_memory_budget_release(budget, sizeof(*output));
	return ret != TR_OK ? ret : metadata_ret;
}

int tr_message_transform_prepare(
        struct tr_message *input,
        const struct tr_message_transform_config *config,
        struct tr_memory_budget *budget, struct tr_message_transform **out)
{
	struct tr_message_transform *operation = NULL;
	struct tr_transform_output *output = NULL;
	struct tr_message_view view;
	size_t capacity;
	size_t reservation;
	int ret;

	if (!input || !config || !budget || !out || budget->limit_bytes == 0)
		return TR_ERR_INVALID;
	if (*out)
		return TR_ERR_STATE;
	ret = tr_message_view(input, &view);
	if (ret != TR_OK)
		return ret;
	ret = tr_message_zstd_plan(config, view.len, &capacity);
	if (ret != TR_OK)
		return ret;
	if (config->max_workspace_bytes >
	    SIZE_MAX - sizeof(*operation) - sizeof(*output))
		return TR_ERR_BAD_LENGTH;
	reservation = sizeof(*operation) + sizeof(*output) +
	              config->max_workspace_bytes;
	ret = tr_memory_budget_reserve(budget, reservation);
	if (ret != TR_OK)
		return ret;

	operation = calloc(1, sizeof(*operation));
	output = calloc(1, sizeof(*output));
	if (!operation || !output) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	/* malloc(0) 的行为不可用于定义空消息的拥有关系。 */
	ret = tr_message_storage_reserve(&output->storage, budget,
	                                 capacity != 0 ? capacity : 1);
	if (ret != TR_OK)
		goto fail;
	ret = tr_message_none_move(input, &tr_transform_move_limits,
	                           &operation->input);
	if (ret != TR_OK) {
		(void)tr_message_storage_cancel(&output->storage);
		goto fail;
	}
	output->budget = budget;
	operation->config = *config;
	operation->budget = budget;
	operation->reserved_output = output;
	operation->output_capacity = capacity;
	atomic_init(&operation->state, TR_TRANSFORM_READY);
	*out = operation;
	return TR_OK;

fail:
	free(output);
	free(operation);
	(void)tr_memory_budget_release(budget, reservation);
	return ret;
}

static bool tr_transform_cancelled(void *arg)
{
	struct tr_message_transform *operation = arg;

	return (atomic_load_explicit(&operation->state, memory_order_acquire) &
	        TR_TRANSFORM_CANCEL) != 0;
}

static enum tr_message_transform_outcome
tr_transform_execute(struct tr_message_transform *operation)
{
	struct tr_transform_output *output = operation->reserved_output;
	struct tr_message_view view;
	enum tr_message_transform_outcome outcome;
	uint8_t *data;
	size_t length = 0;
	int ret;

	if (tr_transform_cancelled(operation))
		return TR_MESSAGE_TRANSFORM_CANCELLED;
	ret = tr_message_view(&operation->input, &view);
	if (ret != TR_OK)
		return TR_MESSAGE_TRANSFORM_INTERNAL;
	data = malloc(operation->output_capacity != 0
	                      ? operation->output_capacity
	                      : 1);
	if (!data)
		return TR_MESSAGE_TRANSFORM_NOMEM;
	outcome = tr_message_zstd_run(&operation->config, &view, data,
	                              operation->output_capacity, &length,
	                              tr_transform_cancelled, operation);
	if (outcome != TR_MESSAGE_TRANSFORM_OK) {
		free(data);
		return outcome;
	}
	ret = tr_message_storage_publish(&output->storage, data, length,
	                                 tr_transform_output_release, output,
	                                 &operation->output);
	if (ret != TR_OK) {
		free(data);
		return TR_MESSAGE_TRANSFORM_INTERNAL;
	}
	/* publish 不调用回调且尚未对外公布结果；在执行者交出控制前，
	 * 将成功载荷交给独立的存储释放责任。失败载荷始终在本作用域释放。
	 */
	output->data = data;
	operation->reserved_output = NULL;
	return TR_MESSAGE_TRANSFORM_OK;
}

int tr_message_transform_run(struct tr_message_transform *operation)
{
	unsigned state;
	enum tr_message_transform_outcome outcome;
	int ret;

	if (!operation)
		return TR_ERR_INVALID;
	state = atomic_load_explicit(&operation->state, memory_order_acquire);
	for (;;) {
		if ((state & ~TR_TRANSFORM_CANCEL) != TR_TRANSFORM_READY)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &operation->state, &state,
		            TR_TRANSFORM_RUNNING |
		                    (state & TR_TRANSFORM_CANCEL),
		            memory_order_acq_rel, memory_order_acquire))
			break;
	}
	outcome = tr_transform_execute(operation);
	ret = tr_transform_output_discard(operation);
	if (ret != TR_OK)
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	ret = tr_memory_budget_release(operation->budget,
	                               operation->config.max_workspace_bytes);
	if (ret != TR_OK)
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	ret = tr_message_release(&operation->input);
	if (ret != TR_OK)
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;

	operation->outcome = outcome;
	state = TR_TRANSFORM_RUNNING;
	if (atomic_compare_exchange_strong_explicit(
	            &operation->state, &state, TR_TRANSFORM_DONE,
	            memory_order_release, memory_order_relaxed))
		return TR_OK;

	/* 只有 cancel 能使上面的提交失败。先销毁可能已完成的输出，再发布
	 * 取消终局；取消者从不接管清理，因此不存在并发释放或双重完成。
	 */
	if (tr_message_view(&operation->output, &(struct tr_message_view){0}) ==
	    TR_OK)
		(void)tr_message_release(&operation->output);
	operation->outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
	atomic_store_explicit(&operation->state, TR_TRANSFORM_DONE,
	                      memory_order_release);
	return TR_OK;
}

int tr_message_transform_cancel(struct tr_message_transform *operation)
{
	unsigned state;

	if (!operation)
		return TR_ERR_INVALID;
	state = atomic_load_explicit(&operation->state, memory_order_acquire);
	for (;;) {
		if (state == TR_TRANSFORM_DONE)
			return TR_ERR_STATE;
		if ((state & TR_TRANSFORM_CANCEL) != 0)
			return TR_OK;
		if (atomic_compare_exchange_weak_explicit(
		            &operation->state, &state,
		            state | TR_TRANSFORM_CANCEL, memory_order_acq_rel,
		            memory_order_acquire))
			return TR_OK;
	}
}

int tr_message_transform_finish(struct tr_message_transform **operation,
                                enum tr_message_transform_outcome *outcome,
                                struct tr_message *out)
{
	struct tr_message_transform *current;
	struct tr_memory_budget *budget;
	struct tr_message_view view;
	int ret;

	if (!operation || !*operation || !outcome || !out)
		return TR_ERR_INVALID;
	current = *operation;
	if (tr_message_view(out, &view) != TR_ERR_STATE)
		return TR_ERR_STATE;
	if (atomic_load_explicit(&current->state, memory_order_acquire) !=
	    TR_TRANSFORM_DONE)
		return TR_AGAIN;
	if (current->outcome == TR_MESSAGE_TRANSFORM_OK) {
		ret = tr_message_none_move(&current->output,
		                           &tr_transform_move_limits, out);
		if (ret != TR_OK)
			return ret;
	} else if (tr_message_view(&current->output, &view) == TR_OK) {
		(void)tr_message_release(&current->output);
	}
	*outcome = current->outcome;
	budget = current->budget;
	*operation = NULL;
	free(current);
	return tr_memory_budget_release(budget, sizeof(*current));
}
