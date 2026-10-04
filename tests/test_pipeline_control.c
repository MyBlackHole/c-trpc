#include "../src/group/pipeline_control_internal.h"
#include "../src/group/pipeline_registry_internal.h"

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

static void test_pipeline_control_session(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline_registry_stats registry_stats;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control *duplicate = NULL;
	struct tr_pipeline_data_offer offer0;
	struct tr_pipeline_data_offer offer1;
	struct tr_pipeline_data_offer stale;
	struct tr_pipeline_transfer_ready ready0;
	struct tr_pipeline_transfer_ready ready1;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle data0;
	struct tr_conn_handle data1;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = owner;
	registry_config.owner_shard_id = 2U;
	registry_config.capacity = 2U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = registry;
	control_config.pipeline_id = UINT64_C(0x6001);
	control_config.epoch = UINT64_C(41);
	control_config.data_capacity = 2U;
	control_config.stream_affinity_capacity = 4U;

	control_connection = fake_connection(owner, 10U, 1U);
	assert(tr_pipeline_control_create(
		       &control_config, control_connection, &control) == TR_OK);
	assert(control != NULL);

	/* Registry uniqueness fences a second runtime epoch/session with same id. */
	{
		struct tr_pipeline_control_config duplicate_config = control_config;

		duplicate_config.epoch++;
		assert(tr_pipeline_control_create(
			       &duplicate_config, control_connection,
			       &duplicate) == TR_ERR_STATE);
		assert(duplicate == NULL);
	}

	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) == TR_OK);
	assert(registry_stats.count == 1U);

	/*
	 * A reservation is an offer only. TRANSFER_READY is forbidden until an
	 * exact DATA connection has consumed the capability.
	 */
	memset(&offer0, 0, sizeof(offer0));
	assert(tr_pipeline_control_reserve_data(control, &offer0) == TR_OK);
	assert(offer0.route.version == TR_PIPELINE_ROUTE_VERSION);
	assert(offer0.route.role == TR_PIPELINE_ROUTE_DATA);
	assert(offer0.route.owner_shard_id == 2U);
	assert(offer0.route.pipeline_id == UINT64_C(0x6001));
	assert(offer0.route.epoch == UINT64_C(41));
	assert(offer0.route.member_index == offer0.data.index);
	assert(offer0.route.member_generation == offer0.data.generation);

	memset(&ready0, 0, sizeof(ready0));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready0) == TR_AGAIN);

	/* A forged cancellation must not consume the real capability. */
	stale = offer0;
	stale.data.generation++;
	stale.route.member_generation++;
	assert(tr_pipeline_control_cancel_data(control, &stale) ==
	       TR_ERR_STALE);

	/*
	 * Keep offer0 RESERVED while offer1 becomes ATTACHED. TRANSFER_READY must
	 * skip the reservation even if round-robin encounters that slot first.
	 */
	memset(&offer1, 0, sizeof(offer1));
	assert(tr_pipeline_control_reserve_data(control, &offer1) == TR_OK);
	data1 = fake_connection(owner, 21U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &offer1.route, data1, NULL) == TR_OK);

	/*
	 * Exact peer cancellation is idempotent across handoff ambiguity.
	 * Once this generation is ATTACHED it is a successful no-op and must not
	 * retire the live membership.
	 */
	assert(tr_pipeline_control_cancel_data(control, &offer1) == TR_OK);

	memset(&ready0, 0, sizeof(ready0));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready0) == TR_OK);
	assert(ready0.stream_id == 1001U);
	assert(ready0.data.index == offer1.data.index);
	assert(ready0.data.generation == offer1.data.generation);

	/* Stream affinity is established exactly once. */
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1001U, &ready1) == TR_ERR_STATE);

	/* Attach the previously RESERVED offer0; next stream can now use it. */
	data0 = fake_connection(owner, 20U, 1U);
	assert(tr_pipeline_registry_attach_data_route(
		       registry, &offer0.route, data0, NULL) == TR_OK);

	memset(&ready1, 0, sizeof(ready1));
	assert(tr_pipeline_control_prepare_transfer(
		       control, 1002U, &ready1) == TR_OK);
	assert(ready1.stream_id == 1002U);
	assert(ready1.data.index == offer0.data.index);
	assert(ready1.data.generation == offer0.data.generation);

	/* CONTROL cannot disappear while attached DATA/Stream lifetime remains. */
	assert(tr_pipeline_control_close(control, control_connection) ==
	       TR_ERR_STATE);

	assert(tr_pipeline_control_release_transfer(control, 1001U) == TR_OK);
	assert(tr_pipeline_control_release_transfer(control, 1002U) == TR_OK);
	assert(tr_pipeline_control_release_transfer(control, 1002U) ==
	       TR_ERR_STALE);

	assert(tr_pipeline_registry_detach_data_route(
		       registry, &offer0.route, data0) == TR_OK);
	assert(tr_pipeline_registry_detach_data_route(
		       registry, &offer1.route, data1) == TR_OK);

	/* Same exact generation already FREE is also an idempotent no-op. */
	assert(tr_pipeline_control_cancel_data(control, &offer1) == TR_OK);
	stale = offer1;
	stale.data.generation++;
	stale.route.member_generation++;
	assert(tr_pipeline_control_cancel_data(control, &stale) ==
	       TR_ERR_STALE);

	/*
	 * RESERVED capability 属于 CONTROL。错误 CONTROL generation 的 close
	 * 必须在 commit 前失败，不能先取消 reservation 再尝试恢复 CONTROL。
	 */
	assert(tr_pipeline_control_reserve_data(control, &offer0) == TR_OK);
	{
		struct tr_conn_handle stale_control = control_connection;
		struct tr_pipeline_stats before_close;

		stale_control.generation++;
		assert(tr_pipeline_control_close(control, stale_control) ==
		       TR_ERR_STALE);
		memset(&before_close, 0, sizeof(before_close));
		assert(tr_pipeline_control_get_stats(control, &before_close) ==
		       TR_OK);
		assert(before_close.control_bound == 1);
		assert(before_close.data_reserved_count == 1U);
	}
	assert(tr_pipeline_control_close(control, control_connection) == TR_OK);
	control = NULL;

	memset(&registry_stats, 0, sizeof(registry_stats));
	assert(tr_pipeline_registry_get_stats(registry, &registry_stats) == TR_OK);
	assert(registry_stats.count == 0U);

	tr_pipeline_registry_destroy(registry);
	assert(tr_reactor_stop(owner) == TR_OK);
	tr_reactor_destroy(owner);
}

static void test_pipeline_control_validation(void)
{
	struct tr_pipeline_control_config config;
	struct tr_pipeline_control *control =
		(struct tr_pipeline_control *)(uintptr_t)1U;
	struct tr_conn_handle connection;

	memset(&config, 0, sizeof(config));
	memset(&connection, 0, sizeof(connection));
	assert(tr_pipeline_control_create(
		       &config, connection, &control) == TR_ERR_INVALID);
	assert(control == NULL);
}

int main(void)
{
	test_pipeline_control_validation();
	test_pipeline_control_session();
	puts("pipeline control/transfer-ready: ok");
	return 0;
}
