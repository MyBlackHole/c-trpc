#ifndef TR_PIPELINE_LISTENER_INTERNAL_H
#define TR_PIPELINE_LISTENER_INTERNAL_H

#include <stdint.h>

#include "../../group/pipeline_route_internal.h"
#include "tr/reactor.h"

struct tr_pipeline_listener;

typedef int (*tr_pipeline_listener_authorize_control_cb)(
	const struct tr_pipeline_route_preface *route, void *arg);

typedef enum tr_frame_disposition (*tr_pipeline_listener_data_frame_cb)(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg);

typedef void (*tr_pipeline_listener_data_event_cb)(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg);

struct tr_pipeline_listener_config {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;

	uint32_t pipeline_capacity;
	uint32_t connection_capacity;
	uint32_t data_capacity_per_pipeline;
	uint32_t stream_affinity_capacity_per_pipeline;
	uint32_t control_message_count;

	/*
	 * 必须提供的 CONTROL admission/fencing hook。
	 * listener 绝不能把客户端自报的 TRR1 CONTROL identity 直接视为授权结果。
	 */
	tr_pipeline_listener_authorize_control_cb authorize_control;
	void *authorize_arg;

	tr_pipeline_listener_data_frame_cb data_frame_cb;
	tr_pipeline_listener_data_event_cb data_event_cb;
	void *data_callback_arg;
};

struct tr_pipeline_listener_stats {
	uint32_t pipeline_capacity;
	uint32_t pipelines_current;
	uint32_t pipelines_peak;
	uint32_t connection_capacity;
	uint32_t connections_current;
	uint32_t connections_peak;
	uint32_t data_connections_current;
	uint64_t active_transfers;
	uint32_t draining;
	uint64_t control_accepts;
	uint64_t data_accepts;
	uint64_t route_rejections;
	uint64_t capacity_rejections;
};

/*
 * 一个 listener 只属于一个 Reactor/shard owner，并拥有该 shard 的 Pipeline
 * registry 与 bounded CONTROL message buffer 资源。
 */
int tr_pipeline_listener_create(
	const struct tr_pipeline_listener_config *config,
	struct tr_pipeline_listener **out);
int tr_pipeline_listener_listen_ipv4(
	struct tr_pipeline_listener *listener, const char *address,
	uint16_t port, int backlog, uint16_t *out_bound_port);
int tr_pipeline_listener_begin_drain(struct tr_pipeline_listener *listener);

/*
 * stop() 负责关闭 admission 并把所有 connection/session 收敛到静止状态。
 * destroy() 只释放已经 stop/quiesce 的对象，不隐式执行可能失败的状态推进。
 */
int tr_pipeline_listener_stop(struct tr_pipeline_listener *listener);
void tr_pipeline_listener_destroy(struct tr_pipeline_listener *listener);

uint16_t tr_pipeline_listener_bound_port(
	const struct tr_pipeline_listener *listener);

int tr_pipeline_listener_send_data_offer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out);
int tr_pipeline_listener_send_transfer_ready(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id, uint64_t message_id);
int tr_pipeline_listener_release_transfer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id);

int tr_pipeline_listener_get_stats(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_stats *out);

#endif
