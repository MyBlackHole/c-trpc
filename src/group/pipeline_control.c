#include "pipeline_control_internal.h"

#include <stdlib.h>
#include <string.h>

#include "../execution/reactor_internal.h"
#include "tr/status.h"

struct tr_pipeline_control {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline *pipeline;
	struct tr_pipeline_attached_data *abort_data;
	uint32_t data_capacity;
};

static int tr_pipeline_control_offer_matches(
	struct tr_pipeline_control *control,
	const struct tr_pipeline_data_offer *offer)
{
	const struct tr_pipeline_route_preface *route;

	if (!control || !control->pipeline || !offer)
		return 0;
	route = &offer->route;

	if (tr_pipeline_route_preface_validate_fields(route) != TR_OK)
		return 0;
	if (route->role != TR_PIPELINE_ROUTE_DATA)
		return 0;

	return route->owner_shard_id ==
		       tr_pipeline_owner_shard_id(control->pipeline) &&
	       route->pipeline_id == tr_pipeline_id(control->pipeline) &&
	       route->epoch == tr_pipeline_epoch(control->pipeline) &&
	       route->member_index == offer->data.index &&
	       route->member_generation == offer->data.generation;
}

int tr_pipeline_control_create(
	const struct tr_pipeline_control_config *config,
	struct tr_conn_handle control_connection,
	struct tr_pipeline_control **out)
{
	struct tr_pipeline_control *control;
	struct tr_pipeline_config pipeline_config;
	struct tr_reactor *owner;
	uint32_t owner_shard_id;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (!config || !config->registry || config->pipeline_id == 0U ||
	    config->epoch == 0U || config->data_capacity == 0U ||
	    config->stream_affinity_capacity == 0U)
		return TR_ERR_INVALID;

	owner = tr_pipeline_registry_owner(config->registry);
	owner_shard_id =
		tr_pipeline_registry_owner_shard_id(config->registry);
	if (!owner || owner_shard_id == UINT32_MAX ||
	    control_connection.reactor != owner)
		return TR_ERR_INVALID;

	control = (struct tr_pipeline_control *)calloc(1, sizeof(*control));
	if (!control)
		return TR_ERR_NOMEM;
	control->abort_data = (struct tr_pipeline_attached_data *)calloc(
		config->data_capacity, sizeof(*control->abort_data));
	if (!control->abort_data) {
		free(control);
		return TR_ERR_NOMEM;
	}
	control->data_capacity = config->data_capacity;

	memset(&pipeline_config, 0, sizeof(pipeline_config));
	pipeline_config.owner = owner;
	pipeline_config.owner_shard_id = owner_shard_id;
	pipeline_config.pipeline_id = config->pipeline_id;
	pipeline_config.epoch = config->epoch;
	pipeline_config.data_capacity = config->data_capacity;
	pipeline_config.stream_affinity_capacity =
		config->stream_affinity_capacity;

	ret = tr_pipeline_create(&pipeline_config, &control->pipeline);
	if (ret != TR_OK)
		goto fail;

	/* CONTROL 绑定与注册表发布必须属于同一个所有者事务。
	 * 注册表辅助函数会在任何 Pipeline 修改之前完成重复与容量
	 * 校验；成功绑定后只剩不会失败的所有者本地注册表提交，因此这里
	 * 不再需要第二个可能失败的 clear_control() 回滚。
	 */
	ret = tr_pipeline_registry_register_control(
		config->registry, control->pipeline, control_connection);
	if (ret != TR_OK)
		goto fail_pipeline;

	control->registry = config->registry;
	*out = control;
	return TR_OK;

fail_pipeline:
	tr_pipeline_destroy(control->pipeline);
fail:
	free(control->abort_data);
	free(control);
	return ret;
}

int tr_pipeline_control_reserve_data(
	struct tr_pipeline_control *control,
	struct tr_pipeline_data_offer *out)
{
	struct tr_pipeline_data_ref data;
	struct tr_pipeline_route_preface route;
	int ret;

	if (!control || !control->pipeline || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));
	memset(&data, 0, sizeof(data));
	ret = tr_pipeline_reserve_data(control->pipeline, &data);
	if (ret != TR_OK)
		return ret;

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_DATA;
	route.owner_shard_id =
		tr_pipeline_owner_shard_id(control->pipeline);
	route.pipeline_id = tr_pipeline_id(control->pipeline);
	route.epoch = tr_pipeline_epoch(control->pipeline);
	route.member_index = data.index;
	route.member_generation = data.generation;

	out->data = data;
	out->route = route;
	return TR_OK;
}

int tr_pipeline_control_cancel_data(
	struct tr_pipeline_control *control,
	const struct tr_pipeline_data_offer *offer)
{
	if (!tr_pipeline_control_offer_matches(control, offer))
		return TR_ERR_STALE;

	return tr_pipeline_cancel_data_offer(
		control->pipeline, offer->data);
}

int tr_pipeline_control_prepare_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id,
	struct tr_pipeline_transfer_ready *out)
{
	if (!control || !control->pipeline)
		return TR_ERR_INVALID;

	return tr_pipeline_prepare_transfer(
		control->pipeline, stream_id, out);
}

int tr_pipeline_control_release_transfer(
	struct tr_pipeline_control *control, uint32_t stream_id)
{
	if (!control || !control->pipeline)
		return TR_ERR_INVALID;

	return tr_pipeline_unbind_stream(control->pipeline, stream_id);
}

int tr_pipeline_control_get_stats(
	struct tr_pipeline_control *control,
	struct tr_pipeline_stats *out)
{
	if (!control || !control->pipeline || !out)
		return TR_ERR_INVALID;
	return tr_pipeline_get_stats(control->pipeline, out);
}

static void tr_pipeline_control_wire_identity(
	struct tr_pipeline_control *control,
	struct tr_pipeline_control_wire_message *message)
{
	message->version = TR_PIPELINE_CONTROL_WIRE_VERSION;
	message->owner_shard_id =
		tr_pipeline_owner_shard_id(control->pipeline);
	message->pipeline_id = tr_pipeline_id(control->pipeline);
	message->epoch = tr_pipeline_epoch(control->pipeline);
}

struct tr_pipeline_control_reserve_wire_request {
	struct tr_pipeline_control *control;
	struct tr_pipeline_data_offer *offer_out;
	uint8_t *out;
};

static int tr_pipeline_control_reserve_data_wire_on_owner(void *arg)
{
	struct tr_pipeline_control_reserve_wire_request *request =
		(struct tr_pipeline_control_reserve_wire_request *)arg;
	struct tr_pipeline_control *control = request->control;
	struct tr_pipeline_control_wire_message message;
	struct tr_pipeline_data_offer offer;
	int rollback_ret;
	int ret;

	memset(&offer, 0, sizeof(offer));
	ret = tr_pipeline_control_reserve_data(control, &offer);
	if (ret != TR_OK)
		return ret;

	memset(&message, 0, sizeof(message));
	tr_pipeline_control_wire_identity(control, &message);
	message.type = TR_PIPELINE_CONTROL_DATA_OFFER;
	message.data_index = offer.data.index;
	message.data_generation = offer.data.generation;

	ret = tr_pipeline_control_wire_encode(request->out, &message);
	if (ret != TR_OK) {
		/* 预留与编码都在同一个所有者处理轮次。这里回滚精确
		 * 代次，不允许反应器停止插入两个状态转换之间。
		 */
		rollback_ret = tr_pipeline_control_cancel_data(control, &offer);
		return rollback_ret != TR_OK ? rollback_ret : ret;
	}

	if (request->offer_out)
		*request->offer_out = offer;
	return TR_OK;
}

int tr_pipeline_control_reserve_data_wire_ex(
	struct tr_pipeline_control *control,
	struct tr_pipeline_data_offer *offer_out,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE])
{
	struct tr_pipeline_control_reserve_wire_request request;

	if (!control || !control->pipeline || !out)
		return TR_ERR_INVALID;
	memset(out, 0, TR_PIPELINE_CONTROL_WIRE_SIZE);
	if (offer_out)
		memset(offer_out, 0, sizeof(*offer_out));

	request.control = control;
	request.offer_out = offer_out;
	request.out = out;
	return tr_reactor_call(
		tr_pipeline_owner(control->pipeline),
		tr_pipeline_control_reserve_data_wire_on_owner, &request);
}

int tr_pipeline_control_reserve_data_wire(
	struct tr_pipeline_control *control,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE])
{
	return tr_pipeline_control_reserve_data_wire_ex(control, NULL, out);
}

int tr_pipeline_control_cancel_data_wire(
	struct tr_pipeline_control *control, const uint8_t *data, uint32_t len)
{
	struct tr_pipeline_control_wire_message message;
	struct tr_pipeline_data_offer offer;
	int ret;

	if (!control || !control->pipeline)
		return TR_ERR_INVALID;

	memset(&message, 0, sizeof(message));
	ret = tr_pipeline_control_wire_decode(data, len, &message);
	if (ret != TR_OK)
		return ret;
	if (message.type != TR_PIPELINE_CONTROL_DATA_CANCEL)
		return TR_ERR_BAD_TYPE;

	memset(&offer, 0, sizeof(offer));
	offer.data.index = message.data_index;
	offer.data.generation = message.data_generation;
	offer.route.version = TR_PIPELINE_ROUTE_VERSION;
	offer.route.role = TR_PIPELINE_ROUTE_DATA;
	offer.route.owner_shard_id = message.owner_shard_id;
	offer.route.pipeline_id = message.pipeline_id;
	offer.route.epoch = message.epoch;
	offer.route.member_index = message.data_index;
	offer.route.member_generation = message.data_generation;

	return tr_pipeline_control_cancel_data(control, &offer);
}

struct tr_pipeline_control_prepare_wire_request {
	struct tr_pipeline_control *control;
	uint32_t stream_id;
	uint8_t *out;
};

static int tr_pipeline_control_prepare_transfer_wire_on_owner(void *arg)
{
	struct tr_pipeline_control_prepare_wire_request *request =
		(struct tr_pipeline_control_prepare_wire_request *)arg;
	struct tr_pipeline_control *control = request->control;
	struct tr_pipeline_control_wire_message message;
	struct tr_pipeline_transfer_ready ready;
	int rollback_ret;
	int ret;

	memset(&ready, 0, sizeof(ready));
	ret = tr_pipeline_control_prepare_transfer(
		control, request->stream_id, &ready);
	if (ret != TR_OK)
		return ret;

	memset(&message, 0, sizeof(message));
	tr_pipeline_control_wire_identity(control, &message);
	message.type = TR_PIPELINE_CONTROL_TRANSFER_READY;
	message.stream_id = ready.stream_id;
	message.data_index = ready.data.index;
	message.data_generation = ready.data.generation;

	ret = tr_pipeline_control_wire_encode(request->out, &message);
	if (ret != TR_OK) {
		rollback_ret = tr_pipeline_control_release_transfer(
			control, request->stream_id);
		return rollback_ret != TR_OK ? rollback_ret : ret;
	}
	return TR_OK;
}

int tr_pipeline_control_prepare_transfer_wire(
	struct tr_pipeline_control *control, uint32_t stream_id,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE])
{
	struct tr_pipeline_control_prepare_wire_request request;

	if (!control || !control->pipeline || !out || stream_id == 0U)
		return TR_ERR_INVALID;
	memset(out, 0, TR_PIPELINE_CONTROL_WIRE_SIZE);

	request.control = control;
	request.stream_id = stream_id;
	request.out = out;
	return tr_reactor_call(
		tr_pipeline_owner(control->pipeline),
		tr_pipeline_control_prepare_transfer_wire_on_owner, &request);
}

struct tr_pipeline_control_teardown_request {
	struct tr_pipeline_control *control;
	struct tr_conn_handle expected_control;
};

static int tr_pipeline_control_abort_on_owner(void *arg)
{
	struct tr_pipeline_control_teardown_request *request =
		(struct tr_pipeline_control_teardown_request *)arg;
	struct tr_pipeline_control *control = request->control;
	uint32_t count = 0U;
	uint32_t i;
	int ret;

	/* 致命拆除必须在一个反应器所有者处理轮次内完成。先取得 ATTACHED
	 * 成员关系的一致快照，再逐个失效；remove_data() 同时清除指向对应
	 * 代次的流亲和绑定。
	 */
	ret = tr_pipeline_attached_data_snapshot(
		control->pipeline, control->abort_data,
		control->data_capacity, &count);
	if (ret != TR_OK)
		return ret;

	for (i = 0; i < count; ++i) {
		ret = tr_pipeline_remove_data(
			control->pipeline, control->abort_data[i].data);
		if (ret != TR_OK)
			return ret;
	}

	/* 注册表辅助函数把“清除 CONTROL、取消 RESERVED、注销”
	 * 合并为同一个所有者侧提交。成功后 Pipeline 已经不可被路由
	 * 或 CONTROL 再次发现，可以安全释放软状态。
	 */
	ret = tr_pipeline_registry_close_control(
		control->registry, control->pipeline,
		request->expected_control);
	if (ret != TR_OK)
		return ret;

	tr_pipeline_destroy(control->pipeline);
	control->pipeline = NULL;
	control->registry = NULL;

	/* 成员关系已经先于套接字回调失效。STALE 仅表示关闭回调
	 * 已经先一步回收该精确连接代次，不需要恢复任何状态。
	 */
	for (i = 0; i < count; ++i) {
		ret = tr_reactor_close_on_owner(
			control->abort_data[i].connection);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			continue;
	}

	free(control->abort_data);
	control->abort_data = NULL;
	free(control);
	return TR_OK;
}

int tr_pipeline_control_abort(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control)
{
	struct tr_pipeline_control_teardown_request request;
	struct tr_reactor *owner;

	if (!control || !control->pipeline || !control->registry ||
	    !control->abort_data)
		return TR_ERR_INVALID;

	owner = tr_pipeline_registry_owner(control->registry);
	if (!owner || expected_control.reactor != owner)
		return TR_ERR_INVALID;

	request.control = control;
	request.expected_control = expected_control;
	return tr_reactor_call(
		owner, tr_pipeline_control_abort_on_owner, &request);
}

static int tr_pipeline_control_close_on_owner(void *arg)
{
	struct tr_pipeline_control_teardown_request *request =
		(struct tr_pipeline_control_teardown_request *)arg;
	struct tr_pipeline_control *control = request->control;
	int ret;

	/* 优雅关闭不允许 ATTACHED DATA 或流亲和绑定存活；
	 * 注册表辅助函数在任何不可逆修改之前完成这项校验。RESERVED 能力凭证
	 * 属于 CONTROL，可在提交时一并取消。
	 */
	ret = tr_pipeline_registry_close_control(
		control->registry, control->pipeline,
		request->expected_control);
	if (ret != TR_OK)
		return ret;

	tr_pipeline_destroy(control->pipeline);
	control->pipeline = NULL;
	control->registry = NULL;
	free(control->abort_data);
	control->abort_data = NULL;
	free(control);
	return TR_OK;
}

int tr_pipeline_control_close(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control)
{
	struct tr_pipeline_control_teardown_request request;
	struct tr_reactor *owner;

	if (!control || !control->pipeline || !control->registry)
		return TR_ERR_INVALID;

	owner = tr_pipeline_registry_owner(control->registry);
	if (!owner || expected_control.reactor != owner)
		return TR_ERR_INVALID;

	request.control = control;
	request.expected_control = expected_control;
	return tr_reactor_call(
		owner, tr_pipeline_control_close_on_owner, &request);
}
