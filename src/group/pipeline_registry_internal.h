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

/* 为成员代次计算下一个非零值；UINT32_MAX 后明确拒绝而不回绕。 */
int tr_pipeline_registry_member_generation_after(uint32_t current,
                                                 uint32_t *out);

/* 注册表是有界的分片本地索引，不拥有 Pipeline 生命周期。
 * Pipeline 必须先注销再销毁；Pipeline 裸指针不得逃逸所有者域。
 * 唯一允许长期保存的内部指针能力凭证，是精确 DATA 附接后返回给
 * 所有者本地入口处理函数的绑定，并且不能超过对应成员关系生命周期。
 */
int tr_pipeline_registry_create(
	const struct tr_pipeline_registry_config *config,
	struct tr_pipeline_registry **out);
void tr_pipeline_registry_destroy(struct tr_pipeline_registry *registry);

struct tr_reactor *
tr_pipeline_registry_owner(const struct tr_pipeline_registry *registry);
uint32_t
tr_pipeline_registry_owner_shard_id(const struct tr_pipeline_registry *registry);

/* 仅限注册表所属 Reactor 所有者；成功后代次在该注册表内不复用。 */
int tr_pipeline_registry_next_member_generation_on_owner(
        struct tr_pipeline_registry *registry, uint32_t *out);

int tr_pipeline_registry_register(struct tr_pipeline_registry *registry,
				  struct tr_pipeline *pipeline);

/* CONTROL 发布事务。注册表重复与容量检查发生在 Pipeline 修改前；
 * 一旦 CONTROL 绑定成功，注册表插入就是不会失败的所有者本地提交。
 * 这避免了仅为在注册失败时回滚 CONTROL 而进行第二次可能失败的所有者调用。
 */
int tr_pipeline_registry_register_control(
	struct tr_pipeline_registry *registry, struct tr_pipeline *pipeline,
	struct tr_conn_handle control_connection);

int tr_pipeline_registry_unregister(struct tr_pipeline_registry *registry,
				    struct tr_pipeline *pipeline);

/* 在一个所有者处理轮次内关闭 Pipeline CONTROL 并注销注册表条目。
 *
 * 该操作先验证精确注册表标识、CONTROL 存在以及不存在 ATTACHED DATA 或
 * 流亲和绑定；随后进入不可逆提交：清除 CONTROL、取消 RESERVED 能力凭证、
 * 摘除注册表条目。成功后调用方可以安全销毁 Pipeline。
 */
int tr_pipeline_registry_close_control(
	struct tr_pipeline_registry *registry, struct tr_pipeline *pipeline,
	struct tr_conn_handle expected_control);

/* DATA 路由附接只消耗 generation 精确匹配的 RESERVED 能力凭证。
 * 路由前导在进入本层前已经完成魔数与 CRC 校验；本 API 仍会独立校验
 * 语义字段与注册表标识。
 */
int tr_pipeline_registry_attach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out);

/* 仅限所有者的入口附接版本。除消耗精确 DATA 预留外，还返回
 * DATA 入口处理函数使用的所有者本地 Pipeline 能力凭证。该指针不得逃逸
 * 所有者域，也不得超过对应精确 DATA 成员关系的生命周期。
 */
int tr_pipeline_registry_attach_data_route_local_on_owner(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out,
	struct tr_pipeline **pipeline_out);

/* 只读验证精确 route/member 代次；供身份授权先于附接执行。 */
int tr_pipeline_registry_validate_data_route_on_owner(
        struct tr_pipeline_registry *registry,
        const struct tr_pipeline_route_preface *preface);

/* DATA 建立失败、成员关系尚未生效时，取消一个精确的 RESERVED 路由。
 * 已 ATTACHED、陈旧或已复用的能力凭证不受影响。
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
