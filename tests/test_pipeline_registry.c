#include "../src/pipeline_internal.h"
#include "../src/pipeline_registry_internal.h"
#include "../src/pipeline_route_internal.h"

#include "tr/reactor.h"
#include "tr/status.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static struct tr_conn_handle fake_connection(struct tr_reactor *reactor,
					     uint32_t slot,
					     uint32_t generation)
{
	struct tr_conn_handle handle;

	memset(&handle, 0, sizeof(handle));
	handle.reactor = reactor;
	handle.slot = slot;
	handle.generation = generation;
	return handle;
}

static struct tr_pipeline *make_pipeline(struct tr_reactor *owner,
					 uint32_t shard_id,
					 uint64_t pipeline_id,
					 uint64_t epoch)
{
	struct tr_pipeline_config config;
	struct tr_pipeline *pipeline = NULL;

	memset(&config, 0, sizeof(config));
	config.owner = owner;
	config.owner_shard_id = shard_id;
	config.pipeline_id = pipeline_id;
	config.epoch = epoch;
	config.data_capacity = 2U;
	config.stream_affinity_capacity = 4U;
	assert(tr_pipeline_create(&config, &pipeline) == TR_OK);
	assert(pipeline != NULL);
	return pipeline;
}

static struct tr_pipeline_route_preface data_route(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data)
{
	struct tr_pipeline_route_preface preface;

	memset(&preface, 0, sizeof(preface));
	preface.version = TR_PIPELINE_ROUTE_VERSION;
	preface.role = TR_PIPELINE_ROUTE_DATA;
	preface.owner_shard_id = tr_pipeline_owner_shard_id(pipeline);
	preface.pipeline_id = tr_pipeline_id(pipeline);
	preface.epoch = tr_pipeline_epoch(pipeline);
	preface.member_index = data.index;
	preface.member_generation = data.generation;
	return preface;
}

static void test_registry_and_data_route_attach(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_reactor *other = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_registry_stats registry_stats;
	struct tr_pipeline *pipeline;
	struct tr_pipeline *same_id_new_epoch;
	struct tr_pipeline *wrong_shard;
	struct tr_pipeline_data_ref reserved;
	struct tr_pipeline_data_ref attached_data;
	struct tr_pipeline_data_ref cancelled;
	struct tr_pipeline_stats pipeline_stats;
	struct tr_pipeline_route_preface preface;
	struct tr_pipeline_route_preface wrong;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle data_connection;
	struct tr_conn_handle other_connection;
	struct tr_conn_handle observed;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &other) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);
	assert(tr_reactor_start(other) == TR_OK);

	pipeline = make_pipeline(owner, 3U, UINT64_C(0x1001), UINT64_C(9));
	same_id_new_epoch =
		make_pipeline(owner, 3U, UINT64_C(0x1001), UINT64_C(10));
	wrong_shard =
		make_pipeline(owner, 4U, UINT64_C(0x2002), UINT64_C(1));

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 3U;
	registry_config.capacity = 2U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);

	assert(tr_pipeline_registry_register(registry, pipeline) == TR_OK);
	assert(tr_pipeline_registry_register(registry, pipeline) == TR_ERR_STATE);
	/* pipeline_id is unique while the old epoch remains registered. */
	assert(tr_pipeline_registry_register(registry, same_id_new_epoch) ==
	       TR_ERR_STATE);
	assert(tr_pipeline_registry_register(registry, wrong_shard) ==
	       TR_ERR_INVALID);

	control_connection = fake_connection(owner, 10U, 1U);
	assert(tr_pipeline_set_control(pipeline, control_connection) == TR_OK);

	assert(tr_pipeline_reserve_data(pipeline, &reserved) == TR_OK);
	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_get_stats(pipeline, &pipeline_stats) == TR_OK);
	assert(pipeline_stats.data_reserved_count == 1U);
	assert(pipeline_stats.data_count == 0U);

	preface = data_route(pipeline, reserved);
	data_connection = fake_connection(owner, 20U, 1U);
	other_connection = fake_connection(other, 20U, 1U);

	/*
	 * Every mismatch must leave the reservation intact. The final exact route
	 * therefore still attaches successfully using the original generation.
	 */
	wrong = preface;
	wrong.owner_shard_id++;
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &wrong, data_connection, NULL) ==
	       TR_ERR_STALE);

	wrong = preface;
	wrong.epoch++;
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &wrong, data_connection, NULL) ==
	       TR_ERR_STALE);

	wrong = preface;
	wrong.member_generation++;
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &wrong, data_connection, NULL) ==
	       TR_ERR_STALE);

	wrong = preface;
	wrong.role = TR_PIPELINE_ROUTE_CONTROL;
	wrong.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &wrong, data_connection, NULL) ==
	       TR_ERR_BAD_TYPE);

	assert(tr_pipeline_registry_attach_data_route(
		       registry, &preface, other_connection, NULL) ==
	       TR_ERR_INVALID);

	assert(tr_pipeline_registry_attach_data_route(
		       registry, &preface, data_connection,
		       &attached_data) == TR_OK);
	assert(attached_data.index == reserved.index);
	assert(attached_data.generation == reserved.generation);
	assert(tr_pipeline_data_connection(
		       pipeline, attached_data, &observed) == TR_OK);
	assert(observed.reactor == data_connection.reactor);
	assert(observed.slot == data_connection.slot);
	assert(observed.generation == data_connection.generation);

	/* Reservation was consumed exactly once. */
	{
		int duplicate_ret = tr_pipeline_registry_attach_data_route(
			registry, &preface, data_connection, NULL);

		assert(duplicate_ret == TR_ERR_STATE ||
		       duplicate_ret == TR_ERR_STALE);
	}

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_get_stats(pipeline, &pipeline_stats) == TR_OK);
	assert(pipeline_stats.data_reserved_count == 0U);
	assert(pipeline_stats.data_count == 1U);

	/* Cancelled capabilities remain stale and cannot attach later. */
	assert(tr_pipeline_reserve_data(pipeline, &cancelled) == TR_OK);
	assert(tr_pipeline_cancel_data_reservation(pipeline, cancelled) == TR_OK);
	wrong = data_route(pipeline, cancelled);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &wrong,
		       fake_connection(owner, 21U, 1U), NULL) ==
	       TR_ERR_STALE);

	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) == TR_OK);
	assert(registry_stats.owner_shard_id == 3U);
	assert(registry_stats.capacity == 2U);
	assert(registry_stats.count == 1U);

	/* Active CONTROL/DATA membership fences registry lifetime. */
	assert(tr_pipeline_registry_unregister(registry, pipeline) ==
	       TR_ERR_STATE);
	assert(tr_pipeline_remove_data(pipeline, attached_data) == TR_OK);
	assert(tr_pipeline_clear_control(pipeline, control_connection) == TR_OK);
	assert(tr_pipeline_registry_unregister(registry, pipeline) == TR_OK);
	assert(tr_pipeline_registry_unregister(registry, pipeline) == TR_ERR_STALE);
	/* New epoch may claim the same pipeline_id only after old unregister. */
	assert(tr_pipeline_registry_register(registry, same_id_new_epoch) == TR_OK);
	assert(tr_pipeline_registry_unregister(
		       registry, same_id_new_epoch) == TR_OK);

	tr_pipeline_registry_destroy(registry);
	tr_pipeline_destroy(wrong_shard);
	tr_pipeline_destroy(same_id_new_epoch);
	tr_pipeline_destroy(pipeline);

	assert(tr_reactor_stop(other) == TR_OK);
	assert(tr_reactor_stop(owner) == TR_OK);
	tr_reactor_destroy(other);
	tr_reactor_destroy(owner);
}

static void test_registry_validation(void)
{
	struct tr_pipeline_registry_config config;
	struct tr_pipeline_registry *registry =
		(struct tr_pipeline_registry *)(uintptr_t)1U;

	memset(&config, 0, sizeof(config));
	assert(tr_pipeline_registry_create(&config, &registry) ==
	       TR_ERR_INVALID);
	assert(registry == NULL);
}

int main(void)
{
	test_registry_validation();
	test_registry_and_data_route_attach();
	puts("pipeline registry/reservation routing: ok");
	return 0;
}
