#include "pipeline_control_internal.h"

#include <stdlib.h>
#include <string.h>

#include "reactor_internal.h"
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

int tr_pipeline_control_reserve_data_wire(
	struct tr_pipeline_control *control,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE])
{
	struct tr_pipeline_control_wire_message message;
	struct tr_pipeline_data_offer offer;
	int ret;

	if (!control || !control->pipeline || !out)
		return TR_ERR_INVALID;
	memset(out, 0, TR_PIPELINE_CONTROL_WIRE_SIZE);
	memset(&offer, 0, sizeof(offer));

	ret = tr_pipeline_control_reserve_data(control, &offer);
	if (ret != TR_OK)
		return ret;

	memset(&message, 0, sizeof(message));
	tr_pipeline_control_wire_identity(control, &message);
	message.type = TR_PIPELINE_CONTROL_DATA_OFFER;
	message.data_index = offer.data.index;
	message.data_generation = offer.data.generation;

	ret = tr_pipeline_control_wire_encode(out, &message);
	if (ret != TR_OK)
		(void)tr_pipeline_control_cancel_data(control, &offer);
	return ret;
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

int tr_pipeline_control_prepare_transfer_wire(
	struct tr_pipeline_control *control, uint32_t stream_id,
	uint8_t out[TR_PIPELINE_CONTROL_WIRE_SIZE])
{
	struct tr_pipeline_control_wire_message message;
	struct tr_pipeline_transfer_ready ready;
	int ret;

	if (!control || !control->pipeline || !out || stream_id == 0U)
		return TR_ERR_INVALID;
	memset(out, 0, TR_PIPELINE_CONTROL_WIRE_SIZE);
	memset(&ready, 0, sizeof(ready));

	ret = tr_pipeline_control_prepare_transfer(control, stream_id, &ready);
	if (ret != TR_OK)
		return ret;

	memset(&message, 0, sizeof(message));
	tr_pipeline_control_wire_identity(control, &message);
	message.type = TR_PIPELINE_CONTROL_TRANSFER_READY;
	message.stream_id = ready.stream_id;
	message.data_index = ready.data.index;
	message.data_generation = ready.data.generation;

	ret = tr_pipeline_control_wire_encode(out, &message);
	if (ret != TR_OK)
		(void)tr_pipeline_control_release_transfer(control, stream_id);
	return ret;
}

int tr_pipeline_control_abort(
	struct tr_pipeline_control *control,
	struct tr_conn_handle expected_control)
{
	uint32_t count = 0U;
	uint32_t i;
	int ret;

	if (!control || !control->pipeline || !control->registry ||
	    !control->abort_data)
		return TR_ERR_INVALID;

	ret = tr_pipeline_attached_data_snapshot(
		control->pipeline, control->abort_data,
		control->data_capacity, &count);
	if (ret != TR_OK)
		return ret;

	/*
	 * Snapshot and removals execute in the same owner turn. Nothing can replace
	 * one of these capabilities between the two operations.
	 */
	for (i = 0; i < count; ++i) {
		ret = tr_pipeline_remove_data(
			control->pipeline, control->abort_data[i].data);
		if (ret != TR_OK)
			return ret;
	}

	ret = tr_pipeline_clear_control(
		control->pipeline, expected_control);
	if (ret != TR_OK)
		return ret;

	ret = tr_pipeline_registry_unregister(
		control->registry, control->pipeline);
	if (ret != TR_OK) {
		(void)tr_pipeline_set_control(
			control->pipeline, expected_control);
		return ret;
	}

	tr_pipeline_destroy(control->pipeline);
	control->pipeline = NULL;
	control->registry = NULL;

	/*
	 * Membership is already unreachable before socket callbacks run. Every
	 * handle came from the owner-coherent ATTACHED snapshot, so owner-close is
	 * expected to succeed; STALE only means a callback retired it first.
	 */
	for (i = 0; i < count; ++i) {
		ret = tr_reactor_close_on_owner(
			control->abort_data[i].connection);
		if (ret != TR_OK && ret != TR_ERR_STALE) {
			/* Runtime object is already fenced; never resurrect it. */
			continue;
		}
	}

	free(control->abort_data);
	control->abort_data = NULL;
	free(control);
	return TR_OK;
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
	free(control->abort_data);
	control->abort_data = NULL;
	free(control);
	return TR_OK;
}
