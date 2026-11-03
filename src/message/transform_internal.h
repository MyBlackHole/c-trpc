#ifndef TR_MESSAGE_TRANSFORM_INTERNAL_H
#define TR_MESSAGE_TRANSFORM_INTERNAL_H

#include <stdbool.h>
#include "transform.h"

/* 仅 Message 实现内部使用；后端不认识操作布局、预算域或执行器。
 * cancel 借用上下文，必须同步、不可失败，不能销毁后端正在借用的对象。
 */
typedef bool (*tr_message_transform_cancel_fn)(void *arg);

int tr_message_zstd_plan(const struct tr_message_transform_config *config,
                         size_t input_size, size_t *output_capacity);

enum tr_message_transform_outcome
tr_message_zstd_run(const struct tr_message_transform_config *config,
                    const struct tr_message_view *input, uint8_t *output,
                    size_t capacity, size_t *output_size,
                    tr_message_transform_cancel_fn cancel, void *arg);

#endif
