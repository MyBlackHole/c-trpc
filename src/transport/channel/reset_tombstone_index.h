#ifndef TR_RESET_TOMBSTONE_INDEX_H
#define TR_RESET_TOMBSTONE_INDEX_H

#include "channel.h"

#include <stddef.h>
#include <stdint.h>

/* RESET 的有限精确历史；只有同物理连接、同原因的重复 RESET 可被吸收。
 * DATA/信用没有此豁免；对端 RESET 已是其发送 FIFO 屏障，之后的数据仍是协议错误。
 */
struct tr_stream_reset_tombstone {
	struct tr_conn_handle connection;
	uint32_t stream_id;
	int peer_status;
};

/*
 * 表槽存 ring_slot + 1，0 表示空；流标识从 history[slot] 读取，避免在索引
 * 中重复保存键。capacity 必须是大于 max_streams 的 2 的幂，调用方在创建时
 * 一次性分配并计入预算。分布均匀时查找期望 O(1)，碰撞最坏仍受固定 capacity 限制。
 */
int tr_reset_tombstone_index_find(
	const uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, uint32_t *ring_slot_out, size_t *probes_out);
int tr_reset_tombstone_index_insert(
	uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, uint32_t ring_slot, size_t *probes_out);
/* 删除操作的 probes_out 统计查找、簇校验和 backward-shift 的全部槽位检查。 */
int tr_reset_tombstone_index_remove(
	uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, size_t *probes_out);

/* 同单元测试复用的复合键 home 位置计算。 */
size_t tr_reset_tombstone_index_home(size_t capacity,
					    struct tr_conn_handle connection,
					    uint32_t stream_id);

#endif
