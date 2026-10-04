#ifndef TR_PIPELINE_CONTROL_INTERNAL_H
#define TR_PIPELINE_CONTROL_INTERNAL_H

#include <stdint.h>

#include "group/pipeline_internal.h"
#include "group/pipeline_registry_internal.h"
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

/*
 * 一个 CONTROL session ownership 对应一个已注册的 Pipeline 运行期对象。
 *
 * create：
 *   create Pipeline -> bind CONTROL -> register 到 shard-local registry
 *
 * close：
 *   只有 ATTACHED DATA 和 Stream affinity 都已清空后才允许 graceful close。
 *   CONTROL clear 会取消尚未 attach 的 RESERVED capability，并与 registry
 *   unregister 在同一个 Reactor owner transaction 内完成。
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

/*
 * CONTROL wire adapter 把运行期状态转换与 wire identity 编码收敛在同一层。
 * reserve/prepare 如果固定长度消息编码失败，会回滚本次新建的 soft-state。
 */
int tr_pipeline_control_reserve_data_wire(
	struct tr_pipeline_control *control,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);
int tr_pipeline_control_cancel_data_wire(
	struct tr_pipeline_control *control, const uint8_t *data, uint32_t len);
int tr_pipeline_control_prepare_transfer_wire(
	struct tr_pipeline_control *control, uint32_t stream_id,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE]);

/*
 * Fatal CONTROL teardown：
 * - 先使全部 ATTACHED DATA membership 与对应 Stream affinity 失效；
 * - 取消 RESERVED capability；
 * - 从 shard-local registry 注销 Pipeline；
 * - 最后由 Reactor owner 同步关闭 DATA socket 并释放 Pipeline soft-state。
 */
int tr_pipeline_control_abort(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control);

int tr_pipeline_control_close(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control);

#endif
