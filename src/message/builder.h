#ifndef TR_MESSAGE_BUILDER_H
#define TR_MESSAGE_BUILDER_H

#include <stddef.h>

#include "message.h"

struct tr_memory_budget;
struct tr_message_builder;

/* 预留固定容量（含0字节空消息）并逐段复制数据。只有恰好填满后才能发布成不可变
 * Message。 builder 和载荷容量分别计入 budget；发布后这两项都由 Message 持有。
 * 创建、追加、发布和取消由同一所有者串行调用；发布前不得跨线程访问。
 */
int tr_message_builder_create(struct tr_memory_budget *budget, size_t capacity,
                              struct tr_message_builder **out);
int tr_message_builder_append(struct tr_message_builder *builder,
                              const void *data, size_t len);
int tr_message_builder_publish(struct tr_message_builder **builder,
                               struct tr_message *out);
int tr_message_builder_cancel(struct tr_message_builder **builder);

#endif
