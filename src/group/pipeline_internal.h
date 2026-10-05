#ifndef TR_PIPELINE_INTERNAL_H
#define TR_PIPELINE_INTERNAL_H

#include <stdint.h>

#include "../execution/reactor.h"

struct tr_pipeline;

struct tr_pipeline_config {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t data_capacity;
	uint32_t stream_affinity_capacity;
};

struct tr_pipeline_data_ref {
	uint32_t index;
	uint32_t generation;
};

struct tr_pipeline_transfer_ready {
	uint32_t stream_id;
	struct tr_pipeline_data_ref data;
};

struct tr_pipeline_attached_data {
	struct tr_pipeline_data_ref data;
	struct tr_conn_handle connection;
};

struct tr_pipeline_stats {
	uint32_t owner_shard_id;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t data_capacity;
	uint32_t data_reserved_count;
	uint32_t data_count;
	uint32_t stream_affinity_capacity;
	uint32_t stream_affinity_count;
	int control_bound;
};

/*
 * Pipeline 是单 Reactor owner 的 soft-state connection group。
 *
 * 本层只管理 membership/affinity，不拥有底层 connection fd 生命周期，也不定义
 * wire routing preface。所有 mutable 操作通过 owner Reactor 串行化。
 */
int tr_pipeline_create(const struct tr_pipeline_config *config,
		       struct tr_pipeline **out);
void tr_pipeline_destroy(struct tr_pipeline *pipeline);

struct tr_reactor *tr_pipeline_owner(const struct tr_pipeline *pipeline);
uint32_t tr_pipeline_owner_shard_id(const struct tr_pipeline *pipeline);
uint64_t tr_pipeline_id(const struct tr_pipeline *pipeline);
uint64_t tr_pipeline_epoch(const struct tr_pipeline *pipeline);

int tr_pipeline_set_control(struct tr_pipeline *pipeline,
			    struct tr_conn_handle connection);
int tr_pipeline_clear_control(struct tr_pipeline *pipeline,
			      struct tr_conn_handle expected);
int tr_pipeline_control(struct tr_pipeline *pipeline,
			struct tr_conn_handle *out);

/*
 * CONTROL 控制面成员能力：
 * reserve 在 DATA socket 存在前返回索引/代次；
 * attach 只消费精确匹配的 RESERVED 能力。
 */
int tr_pipeline_reserve_data(struct tr_pipeline *pipeline,
			     struct tr_pipeline_data_ref *out);
int tr_pipeline_cancel_data_reservation(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);

/*
 * 针对某个已签发精确代次的幂等对端取消。
 *
 * 唯一状态变化是 RESERVED -> FREE。
 * 同一代次已经 ATTACHED 或已经 FREE 时，以空操作返回 TR_OK；
 * 代次已经复用时返回 TR_ERR_STALE。
 * 这样对端在 DATA 前导数据移交后无需 attach ACK 也能收敛，
 * 同时避免 ABA 取消风险。
 */
int tr_pipeline_cancel_data_offer(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);

int tr_pipeline_attach_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data,
			    struct tr_conn_handle connection);

/* 兼容辅助接口：原子地预留并附加一个 DATA 成员。 */
int tr_pipeline_add_data(struct tr_pipeline *pipeline,
			 struct tr_conn_handle connection,
			 struct tr_pipeline_data_ref *out);
int tr_pipeline_remove_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data);
int tr_pipeline_data_connection(struct tr_pipeline *pipeline,
				struct tr_pipeline_data_ref data,
				struct tr_conn_handle *out);

/*
 * ATTACHED DATA 成员的所有者一致性快照。
 * 调用方提供有界数组；容量小于当前已附加数量时拒绝请求。
 */
int tr_pipeline_attached_data_snapshot(
	struct tr_pipeline *pipeline, struct tr_pipeline_attached_data *out,
	uint32_t capacity, uint32_t *count_out);

/*
 * Round-robin 只在 live DATA slot 间选择。调用方在新 Stream 建立时 select 一次，
 * 随后必须通过 bind_stream 固定 affinity，禁止 frame-by-frame 重新选择。
 */
int tr_pipeline_select_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref *out);

/*
 * CONTROL 屏障原语：选择一个当前 ATTACHED 的 DATA 成员，
 * 并在所有者上原子绑定 Stream 亲和关系。
 * RESERVED 槽位永远不会参与选择。
 * 返回 TR_OK 表示该令牌可以安全地作为 TRANSFER_READY 发布；
 * 返回 TR_AGAIN 表示当前没有可用的已附加 DATA。
 */
int tr_pipeline_prepare_transfer(
	struct tr_pipeline *pipeline, uint32_t stream_id,
	struct tr_pipeline_transfer_ready *out);

int tr_pipeline_bind_stream(struct tr_pipeline *pipeline, uint32_t stream_id,
			    struct tr_pipeline_data_ref data);
int tr_pipeline_stream_data(struct tr_pipeline *pipeline, uint32_t stream_id,
			    struct tr_pipeline_data_ref *data_out,
			    struct tr_conn_handle *connection_out);
int tr_pipeline_unbind_stream(struct tr_pipeline *pipeline,
			      uint32_t stream_id);

int tr_pipeline_get_stats(struct tr_pipeline *pipeline,
			  struct tr_pipeline_stats *out);

#endif
