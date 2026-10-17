#ifndef TR_RPC_V2_INTERNAL_H
#define TR_RPC_V2_INTERNAL_H

#include "tr/rpc_v2.h"

struct tr_facade_tuning;

/* 只对已经归一化的公共 V2 配置调用一次。失败不修改输入或 out。 */
int tr_rpc_v2_prepare_limits(struct tr_facade_limits *limits);
int tr_rpc_v2_memory_limit(const struct tr_facade_limits *limits,
                           const struct tr_facade_tuning *tuning,
                           uint32_t shards, uint64_t *out);

/* 只封装已经由记录层验证、并由当前回调借用的元数据。 */
void tr_rpc_v2_metadata_view_init(struct tr_rpc_v2_metadata_view *out,
                                  const uint8_t *data, size_t size);

#endif
