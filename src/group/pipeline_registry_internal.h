#ifndef TR_PIPELINE_REGISTRY_INTERNAL_H
#define TR_PIPELINE_REGISTRY_INTERNAL_H

#include <stdint.h>

#include "pipeline_internal.h"
#include "pipeline_route_internal.h"

struct tr_pipeline_registry;

struct tr_pipeline_registry_config {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;
	uint32_t capacity;
};

struct tr_pipeline_registry_stats {
	uint32_t owner_shard_id;
	uint32_t capacity;
	uint32_t count;
};

/*
 * Registry 是 bounded shard-local index，不拥有 Pipeline lifetime。
 * Pipeline 必须先 unregister 再 destroy；Pipeline 裸指针不得逃逸 owner domain。
 * 唯一允许长期保存的内部 pointer capability，是 exact DATA attach 后返回给
 * owner-local ingress handler 的绑定，并且不能超过对应 membership 生命周期。
 */
int tr_pipeline_registry_create(
	const struct tr_pipeline_registry_config *config,
	struct tr_pipeline_registry **out);
void tr_pipeline_registry_destroy(struct tr_pipeline_registry *registry);

struct tr_reactor *
tr_pipeline_registry_owner(const struct tr_pipeline_registry *registry);
uint32_t
tr_pipeline_registry_owner_shard_id(const struct tr_pipeline_registry *registry);

int tr_pipeline_registry_register(struct tr_pipeline_registry *registry,
				  struct tr_pipeline *pipeline);
int tr_pipeline_registry_unregister(struct tr_pipeline_registry *registry,
				    struct tr_pipeline *pipeline);

/*
 * 在一个 owner turn 内关闭 Pipeline CONTROL 并注销 registry entry。
 *
 * 该操作先验证 exact registry identity、CONTROL 存在以及不存在 ATTACHED DATA /
 * Stream affinity；随后进入不可逆 commit：clear CONTROL、取消 RESERVED capability、
 * 摘除 registry entry。成功后调用方可以安全 destroy Pipeline。
 */
int tr_pipeline_registry_close_control(
	struct tr_pipeline_registry *registry, struct tr_pipeline *pipeline,
	struct tr_conn_handle expected_control);

/*
 * DATA route attach 只消费 generation 精确匹配的 RESERVED capability。
 * routing preface 在进入本层前已经完成 magic/CRC 校验；本 API 仍会独立校验
 * semantic fields 与 registry identity。
 */
int tr_pipeline_registry_attach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out);

/*
 * Owner-only ingress attach 版本。除消费 exact DATA reservation 外，还返回
 * DATA ingress handler 使用的 owner-local Pipeline capability。该指针不得逃逸
 * owner domain，也不得超过对应 exact DATA membership 生命周期。
 */
int tr_pipeline_registry_attach_data_route_local_on_owner(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out,
	struct tr_pipeline **pipeline_out);

/*
 * Cancel one exact RESERVED route after DATA establishment fails before
 * membership becomes live. Already ATTACHED/stale/reused capabilities are not
 * affected.
 */
int tr_pipeline_registry_cancel_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface);

int tr_pipeline_registry_detach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle expected_connection);

int tr_pipeline_registry_get_stats(
	struct tr_pipeline_registry *registry,
	struct tr_pipeline_registry_stats *out);

#endif
