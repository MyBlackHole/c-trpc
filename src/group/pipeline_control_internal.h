#ifndef TR_PIPELINE_CONTROL_INTERNAL_H
#define TR_PIPELINE_CONTROL_INTERNAL_H

#include <stdint.h>

#include "pipeline_internal.h"
#include "pipeline_registry_internal.h"
#include "pipeline_route_internal.h"
#include "pipeline_control_wire_internal.h"

struct tr_pipeline_control;

struct tr_pipeline_control_config {
	struct tr_pipeline_registry *registry;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t data_capacity;
	uint32_t stream_affinity_capacity;
};

struct tr_pipeline_data_offer {
	struct tr_pipeline_data_ref data;
	struct tr_pipeline_route_preface route;
};

/* 一个 CONTROL 会话所有权对应一个已注册的 Pipeline 运行期对象。
 *
 * 创建：
 *   创建 Pipeline -> 绑定 CONTROL -> 注册到分片本地注册表
 *
 * 关闭：
 *   只有 ATTACHED DATA 和流亲和绑定都已清空后才允许优雅关闭。
 *   清除 CONTROL 会取消尚未附接的 RESERVED 能力凭证，并与注册表
 *   注销在同一个反应器所有者事务内完成。
 */
int tr_pipeline_control_create(
	const struct tr_pipeline_control_config *config,
	struct tr_conn_handle control_connection,
	struct tr_pipeline_control **out);

int tr_pipeline_control_reserve_data(
	struct tr_pipeline_control *control,
	struct tr_pipeline_data_offer *out);
int tr_pipeline_control_cancel_data(
	struct tr_pipeline_control *control,
	const struct tr_pipeline_data_offer *offer);

int tr_pipeline_control_prepare_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id,
	struct tr_pipeline_transfer_ready *out);
int tr_pipeline_control_release_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id);
int tr_pipeline_control_get_stats(
	struct tr_pipeline_control *control,
	struct tr_pipeline_stats *out);

/* CONTROL 线协议适配器把运行期状态转换与线协议标识编码收敛在同一层。
 * 预留或准备时如果固定长度消息编码失败，会回滚本次新建的软状态。
 */
int tr_pipeline_control_reserve_data_wire(
	struct tr_pipeline_control *control,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);
/* 同一所有者事务内返回刚创建的精确 DATA 能力凭证，供传输层
 * 在线协议入队失败时执行精确回滚，避免再从刚编码的线协议反向解析。
 */
int tr_pipeline_control_reserve_data_wire_ex(
	struct tr_pipeline_control *control,
	struct tr_pipeline_data_offer *offer_out,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);
int tr_pipeline_control_cancel_data_wire(
	struct tr_pipeline_control *control, const uint8_t *data, uint32_t len);
int tr_pipeline_control_prepare_transfer_wire(
	struct tr_pipeline_control *control, uint32_t stream_id,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);

/* 致命 CONTROL 拆除：
 * - 先使全部 ATTACHED DATA 成员关系与对应流亲和绑定失效；
 * - 取消 RESERVED 能力凭证；
 * - 从分片本地注册表注销 Pipeline；
 * - 最后由反应器所有者同步关闭 DATA 套接字并释放 Pipeline 软状态。
 */
int tr_pipeline_control_abort(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control);

int tr_pipeline_control_close(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control);

#endif
