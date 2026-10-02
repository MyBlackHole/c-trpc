#include "pipeline_control_internal.h"

#include <stdlib.h>
#include <string.h>

#include "tr/status.h"

struct tr_pipeline_control {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline *pipeline;
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

	ret = tr_pipeline_set_control(control->pipeline, control_connection);
	if (ret != TR_OK)
		goto fail_pipeline;

	ret = tr_pipeline_registry_register(
		config->registry, control->pipeline);
	if (ret != TR_OK)
		goto fail_control;

	control->registry = config->registry;
	*out = control;
	return TR_OK;

fail_control:
	(void)tr_pipeline_clear_control(
		control->pipeline, control_connection);
fail_pipeline:
	tr_pipeline_destroy(control->pipeline);
fail:
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

	return tr_pipeline_cancel_data_reservation(
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

int tr_pipeline_control_close(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control)
{
	struct tr_pipeline_stats stats;
	int ret;

	if (!control || !control->pipeline || !control->registry)
		return TR_ERR_INVALID;

	memset(&stats, 0, sizeof(stats));
	ret = tr_pipeline_get_stats(control->pipeline, &stats);
	if (ret != TR_OK)
		return ret;

	/*
	 * Attached DATA and Stream affinity have external socket/transfer
	 * lifetimes and must quiesce explicitly. RESERVED capabilities are owned
	 * solely by CONTROL and are cancelled by clear_control().
	 */
	if (stats.data_count != 0U ||
	    stats.stream_affinity_count != 0U)
		return TR_ERR_STATE;

	ret = tr_pipeline_clear_control(
		control->pipeline, expected_control);
	if (ret != TR_OK)
		return ret;

	ret = tr_pipeline_registry_unregister(
		control->registry, control->pipeline);
	if (ret != TR_OK) {
		/*
		 * Keep the session usable if registry state unexpectedly prevented
		 * unregister after CONTROL clear.
		 */
		(void)tr_pipeline_set_control(
			control->pipeline, expected_control);
		return ret;
	}

	tr_pipeline_destroy(control->pipeline);
	control->pipeline = NULL;
	control->registry = NULL;
	free(control);
	return TR_OK;
}
