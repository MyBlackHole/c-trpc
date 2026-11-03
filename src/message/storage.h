#ifndef TR_MESSAGE_STORAGE_H
#define TR_MESSAGE_STORAGE_H

#include "message.h"
#include "../refcount.h"

struct tr_memory_budget;

/* 同步、不可失败地释放该存储。可以释放包含控制块的整个对象，但
 * 不能提前销毁预算域、重新发布该控制块或将真正释放推迟到异步任务。
 * 池保留容量和已有记账缓冲不能直接使用本适配，避免提前返还/重复计费。
 */
typedef void (*tr_message_storage_release_fn)(void *arg);

/* 仅存储提供方包含此头以嵌入控制块；普通消息消费者只用 message.h。
 * 这里不暴露池锁/空闲链，也不冻结布局为公开 ABI。初始化为 {0}；
 * 不得复制/直接修改字段。预约/发布/取消只由提供方串行调用。
 * 成功发布后控制块和原字节必须存活且保持不可变，直到最后释放回调；
 * 原字节可修改的权利也已移交。回调完成之前不得复用控制块。
 */
struct tr_message_storage {
	struct tr_refcount _refs;
	struct tr_memory_budget *_budget;
	uint64_t _capacity;
	tr_message_storage_release_fn _release;
	void *_release_arg;
	unsigned _state;
};

/* 在分配载荷之前预约实际存储容量；本函数不分配、不接管任何载荷。
 * 预算必须非 NULL 并存活到最后 release 返回之后；0 容量合法。
 * 本切片仅记账提供方声明的载荷容量，不承诺控制块、线程或整个 RSS。
 * 预算不足为 TR_AGAIN，失败不改变 storage。不可重复预约。
 */
int tr_message_storage_reserve(struct tr_message_storage *storage,
                               struct tr_memory_budget *budget,
                               size_t capacity);

/* 分配失败或放弃发布时归还预约，不执行载荷释放回调。
 * 提供方必须先释放其已分配的载荷，再 cancel；失败保留预约供诊断/重试。
 */
int tr_message_storage_cancel(struct tr_message_storage *storage);

/* 仅 TR_OK 接管 data 对应存储及其最终释放责任，建立首个消息引用。
 * 失败时不调用回调、不修改 out，提供方仍拥有载荷和预约，可修正后重试
 * 或先释放载荷再 cancel。len 不得大于预约容量；非零容量要求非空 data。
 * out 必须初始化为空。data 必须指向至少 capacity 字节的有效区域。
 * 本函数不启动任务/线程，不在返回前执行完成/释放回调。
 */
int tr_message_storage_publish(struct tr_message_storage *storage,
                               const uint8_t *data, size_t len,
                               tr_message_storage_release_fn release, void *arg,
                               struct tr_message *out);

#endif
