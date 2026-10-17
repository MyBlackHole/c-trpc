#ifndef TR_RPC_RECORD_STATS_H
#define TR_RPC_RECORD_STATS_H

#include "tr/rpc.h"
#include "tr/status.h"

#include <stdbool.h>
#include <stdint.h>

/*
 * 仅由所属 Reactor 所有者记账；停止后由已排除重启和外部访问的清理线程
 * 接管。可被同 owner 的多个宿主借用，存储必须长于全部宿主及其终局回调。
 * 不包含共享热路径原子、锁或每调用分配；零初始化即为有效空统计。
 *
 * 累计值不可表示时永久标记失败，不改变 RPC 准入或终局行为。不能独立
 * 饱和各字段后仍把不满足生命周期恒等式的快照作为 TR_OK 返回。
 */
struct tr_rpc_record_semantic_counters {
	struct tr_rpc_semantic_stats value;
	bool overflow;
};

static inline void
tr_rpc_record_semantic_start(struct tr_rpc_record_semantic_counters *counters)
{
	if (counters->overflow)
		return;
	if (counters->value.calls_started == UINT64_MAX) {
		counters->overflow = true;
		return;
	}
	++counters->value.calls_started;
}

static inline void
tr_rpc_record_semantic_finish(struct tr_rpc_record_semantic_counters *counters,
                              enum tr_rpc_status status)
{
	uint32_t index = (uint32_t)status;
	if (counters->overflow)
		return;
	if (index >= TR_RPC_STATUS_COUNT)
		index = TR_RPC_STATUS_INTERNAL;
	if (counters->value.calls_finished == UINT64_MAX ||
	    counters->value.final_status[index] == UINT64_MAX) {
		counters->overflow = true;
		return;
	}
	++counters->value.calls_finished;
	++counters->value.final_status[index];
}

/* 调用者保证 owner/清理域独占；此函数本身不取得生命周期或同步租约。
 * inflight 只在快照时由累计值计算，终局后的网络退役不会增加它。
 * 所有失败均保持 out 原文，溢出不允许用看似成功的截断快照掩盖。
 */
static inline int tr_rpc_record_semantic_stats_read(
        const struct tr_rpc_record_semantic_counters *counters,
        struct tr_rpc_semantic_stats *out)
{
	struct tr_rpc_semantic_stats value;
	if (!counters || !out)
		return TR_ERR_INVALID;
	if (counters->overflow ||
	    counters->value.calls_finished > counters->value.calls_started)
		return TR_ERR_BAD_LENGTH;
	value = counters->value;
	value.calls_inflight = value.calls_started - value.calls_finished;
	*out = value;
	return TR_OK;
}

/* 两份输入都是有效的独立快照；先在局部合并所有字段，再一次发布。
 * 任意字段加法不可表示均保持 dst 不变，跨 shard 不返回部分统计。
 */
static inline int
tr_rpc_record_semantic_stats_merge(struct tr_rpc_semantic_stats *dst,
                                   const struct tr_rpc_semantic_stats *src)
{
	struct tr_rpc_semantic_stats value;
	if (!dst || !src)
		return TR_ERR_INVALID;
	value = *dst;
	if (src->calls_started > UINT64_MAX - value.calls_started ||
	    src->calls_finished > UINT64_MAX - value.calls_finished ||
	    src->calls_inflight > UINT64_MAX - value.calls_inflight)
		return TR_ERR_BAD_LENGTH;
	value.calls_started += src->calls_started;
	value.calls_finished += src->calls_finished;
	value.calls_inflight += src->calls_inflight;
	for (uint32_t i = 0; i < TR_RPC_STATUS_COUNT; ++i) {
		if (src->final_status[i] > UINT64_MAX - value.final_status[i])
			return TR_ERR_BAD_LENGTH;
		value.final_status[i] += src->final_status[i];
	}
	*dst = value;
	return TR_OK;
}

#endif
