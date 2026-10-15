#ifndef TR_RUNTIME_MESSAGE_INTERNAL_H
#define TR_RUNTIME_MESSAGE_INTERNAL_H

struct tr_runtime_shard;
struct tr_reactor;
struct tr_memory_budget;
struct tr_message_service;
struct tr_message_service_config;

/* 仅 Runtime 与其消息组合适配使用。ops 必须为静态只读对象；Runtime
 * 不引用具体消息实现符号，因此未启用消息服务的 SDK 不强制链接 Zstandard。
 * open 同步发布完整或失败保留的部分对象，返回后不得再改 out；close 只有
 * 完整收敛后才清空对象。两个回调均只在正确 Reactor owner 上执行。
 */
struct tr_runtime_message_ops {
	int (*open_on_owner)(struct tr_reactor *reactor,
	                     const struct tr_message_service_config *config,
	                     struct tr_memory_budget *budget,
	                     struct tr_message_service **out);
	int (*close_on_owner)(struct tr_message_service **service);
};

int tr_runtime_shard_message_open_owned_on_owner(
        struct tr_runtime_shard *shard,
        const struct tr_message_service_config *config,
        const struct tr_runtime_message_ops *ops);
int tr_runtime_shard_message_close_owned_on_owner(
        struct tr_runtime_shard *shard);

/* 仅消息适配在一个 owner turn 内借用，不能发布到外部或直接关闭它。
 * 对外只提供 runtime_message.h 中的操作，不暴露 Runtime 私有布局。
 */
int tr_runtime_shard_message_borrow_on_owner(struct tr_runtime_shard *shard,
                                             struct tr_message_service **out);

#endif
