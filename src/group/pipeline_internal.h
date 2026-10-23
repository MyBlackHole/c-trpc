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

/* Pipeline 是由单个反应器所有者管理的软状态连接组。
 *
 * 本层只管理成员关系与亲和绑定，不拥有底层连接 fd 生命周期，也不定义
 * 线协议路由前导。所有修改操作通过所属反应器串行化。
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

/* CONTROL 平面的成员关系能力凭证：
 * 预留操作在 DATA 套接字存在前返回索引与代次。
 * 附接只消耗精确匹配的 RESERVED 能力凭证。
 */
int tr_pipeline_reserve_data(struct tr_pipeline *pipeline,
			     struct tr_pipeline_data_ref *out);
/* 仅限所属 Reactor 所有者；由注册表分配并保证不复用的代次。 */
int tr_pipeline_reserve_data_with_generation_on_owner(
        struct tr_pipeline *pipeline, uint32_t generation,
        struct tr_pipeline_data_ref *out);
int tr_pipeline_cancel_data_reservation(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);

/* 针对一个精确签发代次的幂等对端取消。
 *
 * RESERVED -> FREE 是唯一状态修改。同代次已为 ATTACHED 或 FREE 时
 * 返回 TR_OK，不做操作；已复用的代次返回 TR_ERR_STALE。
 * 这允许对端在 DATA 前导交接后收敛，无需附接确认，也不冒 ABA 取消风险。
 */
int tr_pipeline_cancel_data_offer(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);

int tr_pipeline_attach_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data,
			    struct tr_conn_handle connection);
/* 验证 CONTROL 存活且路由代次仍是精确 RESERVED；不修改成员关系。 */
int tr_pipeline_validate_data_reservation_on_owner(
        struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);

/* 兼容辅助函数：原子地预留并附接一个 DATA 成员关系。 */
int tr_pipeline_add_data(struct tr_pipeline *pipeline,
			 struct tr_conn_handle connection,
			 struct tr_pipeline_data_ref *out);
int tr_pipeline_remove_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data);
int tr_pipeline_data_connection(struct tr_pipeline *pipeline,
				struct tr_pipeline_data_ref data,
				struct tr_conn_handle *out);

/* 所有者侧一致的 ATTACHED DATA 成员关系快照。调用者提供
 * 有界数组；容量小于当前已附接数量时被拒绝。
 */
int tr_pipeline_attached_data_snapshot(
	struct tr_pipeline *pipeline, struct tr_pipeline_attached_data *out,
	uint32_t capacity, uint32_t *count_out);

/* 轮询只在存活 DATA 槽位间选择。调用方在新流建立时选择一次，
 * 随后必须通过 bind_stream 固定亲和绑定，禁止逐帧重新选择。
 */
int tr_pipeline_select_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref *out);

/* CONTROL 屏障原语：在所有者上原子地选择一个当前 ATTACHED DATA 成员关系
 * 并绑定流亲和关系。RESERVED 槽位绝无资格。TR_OK 表示返回令牌
 * 可安全通告为 TRANSFER_READY；TR_AGAIN 表示当前没有已附接的 DATA。
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
