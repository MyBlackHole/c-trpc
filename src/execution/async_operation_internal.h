#ifndef TR_ASYNC_OPERATION_INTERNAL_H
#define TR_ASYNC_OPERATION_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#include "tr/status.h"

struct tr_memory_budget;
struct tr_async_operation_pool;

enum tr_async_operation_kind {
	TR_ASYNC_OPERATION_KIND_RPC_AUTHORIZATION = 1,
	TR_ASYNC_OPERATION_KIND_TLS_PROVIDER = 2,
	TR_ASYNC_OPERATION_KIND_MESSAGE_TRANSFORM = 3
};

/* 弱身份不保持操作存活；代次在同一槽复用时递增且永不回绕。 */
struct tr_async_operation {
	uint64_t instance;
	uint64_t generation;
	uint32_t slot;
};

/* 强引用仅表示当前阶段的唯一交接能力，复制的旧 token 不再有效。 */
struct tr_async_operation_ref {
	uint64_t instance;
	uint64_t generation;
	uint64_t token;
	uint32_t slot;
};

/* 只复制按值身份信息；不保存调用方指针或业务 payload。 */
struct tr_async_operation_spec {
	uint32_t kind;
	uint64_t domain_generation;
	uint64_t object_generation;
	uint64_t deadline_ns;
};

struct tr_async_operation_completion {
	int status;
	/* 小型领域结果按值携带，避免为固定结果额外分配 payload。 */
	uint64_t value;
	void *payload;
	void (*destroy)(void *payload);
};

struct tr_async_operation_stats {
	uint32_t capacity;
	uint32_t reserved;
	uint32_t accepted;
	uint32_t published;
	uint32_t consumed;
	uint32_t generation_exhausted;
	uint64_t reserve_full;
	int accepting;
};

int tr_async_operation_pool_create(uint32_t capacity,
                                   struct tr_memory_budget *budget,
                                   struct tr_async_operation_pool **out);
/* 由完成 owner 在启用池操作前绑定一次；producer publish/cancel 可跨线程。 */
int tr_async_operation_pool_bind_owner(struct tr_async_operation_pool *pool);
/* 仅供已有外部互斥串行化的兼容适配器交接 owner；证明为假时拒绝。 */
int tr_async_operation_pool_handoff_owner(struct tr_async_operation_pool *pool,
                                          int external_serialization_proven);
int tr_async_operation_reserve(struct tr_async_operation_pool *pool,
                               const struct tr_async_operation_spec *spec,
                               struct tr_async_operation *out_operation,
                               struct tr_async_operation_ref *out_producer_ref);
/* 仅撤销与 producer_ref 对应的预留；错配返回 STALE，双方状态和引用不变。 */
int tr_async_operation_reject(struct tr_async_operation_pool *pool,
                              struct tr_async_operation operation,
                              struct tr_async_operation_ref *producer_ref);
int tr_async_operation_accept(struct tr_async_operation_pool *pool,
                              struct tr_async_operation operation,
                              struct tr_async_operation_ref *out_completion_ref,
                              int *out_ready);
/* out_ready == 0 时 out_completion_ref 保持不变；唯一 producer ref
 * 留给工作方，稍后由 publish 交付 completion ref。 */
int tr_async_operation_publish(
        struct tr_async_operation_pool *pool,
        struct tr_async_operation_ref *producer_ref,
        struct tr_async_operation_completion completion,
        struct tr_async_operation_ref *out_completion_ref, int *out_ready);
int tr_async_operation_request_cancel(struct tr_async_operation_pool *pool,
                                      struct tr_async_operation operation,
                                      int reason, int *out_notify);
int tr_async_operation_consume(
        struct tr_async_operation_pool *pool,
        const struct tr_async_operation_ref *completion_ref,
        struct tr_async_operation_spec *out_spec,
        struct tr_async_operation_completion *out_completion,
        int *out_cancelled, int *out_cancel_reason);
int tr_async_operation_retire(struct tr_async_operation_pool *pool,
                              struct tr_async_operation_ref *completion_ref);
int tr_async_operation_pool_close(struct tr_async_operation_pool *pool);
int tr_async_operation_reap_stopped(struct tr_async_operation_pool *pool,
                                    int owner_and_producers_permanently_stopped,
                                    uint32_t *out_reaped);
int tr_async_operation_get_stats(struct tr_async_operation_pool *pool,
                                 struct tr_async_operation_stats *out);
int tr_async_operation_pool_destroy(struct tr_async_operation_pool **pool);

/* 仅测试使用；目标槽必须空闲。 */
int tr_async_operation_test_set_generation(struct tr_async_operation_pool *pool,
                                           uint32_t slot, uint64_t generation);

#endif
