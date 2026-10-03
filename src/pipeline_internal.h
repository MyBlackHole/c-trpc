#ifndef TR_PIPELINE_INTERNAL_H
#define TR_PIPELINE_INTERNAL_H

#include <stdint.h>

#include "tr/reactor.h"

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
 * CONTROL-plane membership capability:
 * reserve returns an index/generation before a DATA socket exists.
 * attach consumes only the exact RESERVED capability.
 */
int tr_pipeline_reserve_data(struct tr_pipeline *pipeline,
			     struct tr_pipeline_data_ref *out);
int tr_pipeline_cancel_data_reservation(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data);
int tr_pipeline_attach_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data,
			    struct tr_conn_handle connection);

/* Compatibility helper: atomically reserve + attach one DATA membership. */
int tr_pipeline_add_data(struct tr_pipeline *pipeline,
			 struct tr_conn_handle connection,
			 struct tr_pipeline_data_ref *out);
int tr_pipeline_remove_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data);
int tr_pipeline_data_connection(struct tr_pipeline *pipeline,
				struct tr_pipeline_data_ref data,
				struct tr_conn_handle *out);

/*
 * Owner-coherent snapshot of ATTACHED DATA memberships. Callers provide a
 * bounded array; capacity smaller than the current attached count is rejected.
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
 * CONTROL barrier primitive: select one currently ATTACHED DATA membership and
 * bind stream affinity atomically on the owner. RESERVED slots are never
 * eligible. TR_OK means the returned token is safe to advertise as
 * TRANSFER_READY; TR_AGAIN means no attached DATA is currently available.
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
