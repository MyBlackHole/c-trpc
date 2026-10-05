#ifndef TR_PIPELINE_CONTROL_TRANSPORT_INTERNAL_H
#define TR_PIPELINE_CONTROL_TRANSPORT_INTERNAL_H

#include <stdint.h>

#include "../../group/pipeline_control_internal.h"
#include "../../group/pipeline_route_internal.h"
#include "../../execution/buffer.h"
#include "../../execution/reactor.h"

struct tr_pipeline_control_transport;

typedef void (*tr_pipeline_control_transport_closing_cb)(void *arg);
typedef void (*tr_pipeline_control_transport_closed_cb)(
	uint64_t pipeline_id, uint64_t epoch, int teardown_status, void *arg);

struct tr_pipeline_control_transport_config {
	struct tr_pipeline_control *control;
	struct tr_conn_handle connection;
	struct tr_buffer_pool *message_pool;
	struct tr_pipeline_route_preface control_route;
	tr_pipeline_control_transport_closing_cb closing_cb;
	tr_pipeline_control_transport_closed_cb closed_cb;
	void *closed_arg;
};

/*
 * 为一个 CONTROL 连接安装真实的 TRP1 帧处理器。
 * 返回 TR_OK 后，control 的所有权转移给 Transport。
 */
int tr_pipeline_control_transport_create(
	const struct tr_pipeline_control_transport_config *config,
	struct tr_pipeline_control_transport **out);

/*
 * Server 侧 CONTROL 发送接口。这些 API 保证 Pipeline 状态转换与
 * 线协议提交相对于失败路径保持原子性：如果入队失败，新建的
 * 预留或亲和关系会回滚。
 */
int tr_pipeline_control_transport_send_data_offer(
	struct tr_pipeline_control_transport *transport, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out);
int tr_pipeline_control_transport_send_transfer_ready(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id,
	uint64_t message_id);
int tr_pipeline_control_transport_release_transfer(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id);

/*
 * 显式终止一个 CONTROL transport。该操作在 connection 的 Reactor owner 上
 * 执行；第一次调用先关闭 admission，随后完成 Pipeline fatal teardown。
 * 如果前一次 connection event 已进入 closing 但 teardown 未完成，可安全重试。
 */
int tr_pipeline_control_transport_abort(
	struct tr_pipeline_control_transport *transport);

int tr_pipeline_control_transport_get_stats(
	struct tr_pipeline_control_transport *transport,
	struct tr_pipeline_stats *out);

uint64_t tr_pipeline_control_transport_pipeline_id(
	const struct tr_pipeline_control_transport *transport);
uint64_t tr_pipeline_control_transport_epoch(
	const struct tr_pipeline_control_transport *transport);

#endif
