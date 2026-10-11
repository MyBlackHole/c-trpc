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

/* 为一个 CONTROL 连接安装真实的 TRP1 帧处理函数。
 * TR_OK 时 control 的所有权转移给传输层。
 */
int tr_pipeline_control_transport_create(
	const struct tr_pipeline_control_transport_config *config,
	struct tr_pipeline_control_transport **out);

/* 服务端 CONTROL 发送。这些 API 使 Pipeline 状态转换与
 * 线协议提交相对于失败保持原子性：若入队失败，新预留或亲和绑定会回滚。
 */
int tr_pipeline_control_transport_send_data_offer(
	struct tr_pipeline_control_transport *transport, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out);
int tr_pipeline_control_transport_send_transfer_ready(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id,
	uint64_t message_id);
int tr_pipeline_control_transport_release_transfer(
	struct tr_pipeline_control_transport *transport, uint32_t stream_id);

/* 收敛一个已经进入终止路径的 CONTROL 传输对象。该操作在连接的
 * 反应器所有者上执行；第一次调用先关闭准入，随后完成 Pipeline 致命
 * 拆除。如果前一次连接事件已进入 closing 但拆除未完成，可安全重试。
 *
 * 调用方必须先确保 CONTROL 连接已关闭或正在其关闭回调中，因成功
 * 返回会释放传输回调的 callback_arg。本接口当前只供监听器终止拆除
 * 在完成连接关闭屏障后重试残留会话。
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
