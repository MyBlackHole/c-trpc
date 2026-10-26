#ifndef TR_RPC_V2_INTERNAL_H
#define TR_RPC_V2_INTERNAL_H

#include "tr/rpc_v2.h"

struct tr_facade_tuning;
struct tr_rpc_v2_context_state;

#define TR_RPC_V2_CONTEXT_ACTIVE TR_RPC_STATUS_COUNT

/* 只对已经归一化的公共 V2 配置调用一次。失败不修改输入或 out。 */
int tr_rpc_v2_prepare_limits(struct tr_facade_limits *limits);
int tr_rpc_v2_memory_limit(const struct tr_facade_limits *limits,
                           const struct tr_facade_tuning *tuning,
                           uint32_t shards, uint64_t *out);

/* 只封装已经由记录层验证、并由当前回调借用的元数据。 */
void tr_rpc_v2_metadata_view_init(struct tr_rpc_v2_metadata_view *out,
                                  const uint8_t *data, size_t size);

/* 编码到调用方提供的 512 字节暂存区；失败时不发布 size，暂存区内容
 * 未定义。Client OPEN 与 Server 回复使用同一键名、重复项和长度规则。 */
int tr_rpc_v2_metadata_encode(const struct tr_rpc_metadata *entries,
                              size_t count, uint8_t *out, uint32_t *size);

/* 固定上下文池的内部引用由 Client Call 槽持有到完全退役。 */
int tr_rpc_v2_context_acquire_internal(
        const struct tr_rpc_v2_call_context *context,
        struct tr_rpc_v2_context_state **out, uint64_t *deadline);
void tr_rpc_v2_context_release_internal(
        struct tr_rpc_v2_context_state *context);
int tr_rpc_v2_context_status_internal(struct tr_rpc_v2_context_state *context,
                                      int *status);

#endif
