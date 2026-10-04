#include "../src/pipeline_internal.h"

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

static int conn_equal(struct tr_conn_handle a, struct tr_conn_handle b)
{
	return a.reactor == b.reactor && a.slot == b.slot &&
	       a.generation == b.generation;
}

static void test_pipeline_connection_group_and_affinity(void)
{
	struct tr_reactor *owner = NULL;
	struct tr_reactor *other = NULL;
	struct tr_pipeline_config config;
	struct tr_pipeline *pipeline = NULL;
	struct tr_pipeline_data_ref data0;
	struct tr_pipeline_data_ref data1;
	struct tr_pipeline_data_ref reused;
	struct tr_pipeline_data_ref selected;
	struct tr_pipeline_data_ref affinity;
	struct tr_pipeline_stats stats;
	struct tr_conn_handle control;
	struct tr_conn_handle wrong_control;
	struct tr_conn_handle data_conn0;
	struct tr_conn_handle data_conn1;
	struct tr_conn_handle data_conn2;
	struct tr_conn_handle connection;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &owner) == TR_OK);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &other) == TR_OK);
	assert(tr_reactor_start(owner) == TR_OK);
	assert(tr_reactor_start(other) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = owner;
	config.owner_shard_id = 3U;
	config.pipeline_id = UINT64_C(0x1234);
	config.epoch = UINT64_C(7);
	config.data_capacity = 2U;
	config.stream_affinity_capacity = 4U;
	assert(tr_pipeline_create(&config, &pipeline) == TR_OK);
	assert(pipeline != NULL);
	assert(tr_pipeline_owner(pipeline) == owner);
	assert(tr_pipeline_owner_shard_id(pipeline) == 3U);
	assert(tr_pipeline_id(pipeline) == UINT64_C(0x1234));
	assert(tr_pipeline_epoch(pipeline) == UINT64_C(7));

	control = fake_connection(owner, 10U, 1U);
	wrong_control = fake_connection(other, 10U, 1U);
	assert(tr_pipeline_set_control(pipeline, wrong_control) == TR_ERR_INVALID);
	assert(tr_pipeline_set_control(pipeline, control) == TR_OK);
	assert(tr_pipeline_set_control(pipeline, control) == TR_ERR_STATE);
	assert(tr_pipeline_add_data(pipeline, control, &data0) == TR_ERR_STATE);
	assert(tr_pipeline_control(pipeline, &connection) == TR_OK);
	assert(conn_equal(connection, control));

	/* Stale clear must not detach the current CONTROL membership. */
	assert(tr_pipeline_clear_control(
		       pipeline, fake_connection(owner, 10U, 2U)) ==
	       TR_ERR_STALE);
	assert(tr_pipeline_control(pipeline, &connection) == TR_OK);
	assert(conn_equal(connection, control));

	data_conn0 = fake_connection(owner, 20U, 3U);
	data_conn1 = fake_connection(owner, 21U, 4U);
	data_conn2 = fake_connection(owner, 22U, 5U);

	/* RESERVED membership is not selectable until the exact capability attaches. */
	assert(tr_pipeline_reserve_data(pipeline, &reused) == TR_OK);
	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_get_stats(pipeline, &stats) == TR_OK);
	assert(stats.data_reserved_count == 1U);
	assert(stats.data_count == 0U);
	assert(tr_pipeline_select_data(pipeline, &selected) == TR_AGAIN);
	assert(tr_pipeline_cancel_data_reservation(pipeline, reused) == TR_OK);
	assert(tr_pipeline_cancel_data_reservation(pipeline, reused) ==
	       TR_ERR_STALE);

	assert(tr_pipeline_add_data(
		       pipeline, fake_connection(other, 20U, 3U), &data0) ==
	       TR_ERR_INVALID);
	assert(tr_pipeline_add_data(pipeline, data_conn0, &data0) == TR_OK);
	assert(tr_pipeline_add_data(pipeline, data_conn0, &reused) == TR_ERR_STATE);
	assert(tr_pipeline_add_data(pipeline, data_conn1, &data1) == TR_OK);
	assert(data0.index != data1.index);
	assert(data0.generation != 0U);
	assert(data1.generation != 0U);
	assert(tr_pipeline_add_data(pipeline, data_conn2, &reused) == TR_AGAIN);

	/* Selection is round-robin across current live DATA slots. */
	assert(tr_pipeline_select_data(pipeline, &selected) == TR_OK);
	assert(selected.index == data0.index);
	assert(selected.generation == data0.generation);
	assert(tr_pipeline_select_data(pipeline, &selected) == TR_OK);
	assert(selected.index == data1.index);
	assert(selected.generation == data1.generation);
	assert(tr_pipeline_select_data(pipeline, &selected) == TR_OK);
	assert(selected.index == data0.index);

	/*
	 * Stream affinity is immutable for the Stream lifetime: select once,
	 * bind once, and all later lookups return the same DATA generation.
	 */
	assert(tr_pipeline_bind_stream(pipeline, 101U, data0) == TR_OK);
	assert(tr_pipeline_bind_stream(pipeline, 102U, data1) == TR_OK);
	assert(tr_pipeline_bind_stream(pipeline, 101U, data1) == TR_ERR_STATE);

	assert(tr_pipeline_stream_data(
		       pipeline, 101U, &affinity, &connection) == TR_OK);
	assert(affinity.index == data0.index);
	assert(affinity.generation == data0.generation);
	assert(conn_equal(connection, data_conn0));

	assert(tr_pipeline_stream_data(
		       pipeline, 102U, &affinity, &connection) == TR_OK);
	assert(affinity.index == data1.index);
	assert(affinity.generation == data1.generation);
	assert(conn_equal(connection, data_conn1));

	/*
	 * DATA failure invalidates all Stream affinity for that exact slot
	 * generation. Reusing the same index must not resurrect old Streams.
	 */
	assert(tr_pipeline_remove_data(pipeline, data0) == TR_OK);
	assert(tr_pipeline_remove_data(pipeline, data0) == TR_ERR_STALE);
	assert(tr_pipeline_stream_data(
		       pipeline, 101U, &affinity, &connection) == TR_ERR_STALE);
	assert(tr_pipeline_stream_data(
		       pipeline, 102U, &affinity, &connection) == TR_OK);

	assert(tr_pipeline_add_data(pipeline, data_conn2, &reused) == TR_OK);
	assert(reused.index == data0.index);
	assert(reused.generation != data0.generation);
	assert(tr_pipeline_stream_data(
		       pipeline, 101U, &affinity, &connection) == TR_ERR_STALE);
	assert(tr_pipeline_bind_stream(pipeline, 103U, reused) == TR_OK);
	assert(tr_pipeline_stream_data(
		       pipeline, 103U, &affinity, &connection) == TR_OK);
	assert(conn_equal(connection, data_conn2));

	assert(tr_pipeline_unbind_stream(pipeline, 102U) == TR_OK);
	assert(tr_pipeline_unbind_stream(pipeline, 102U) == TR_ERR_STALE);

	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_get_stats(pipeline, &stats) == TR_OK);
	assert(stats.owner_shard_id == 3U);
	assert(stats.pipeline_id == UINT64_C(0x1234));
	assert(stats.epoch == UINT64_C(7));
	assert(stats.control_bound == 1);
	assert(stats.data_reserved_count == 0U);
	assert(stats.data_capacity == 2U);
	assert(stats.data_count == 2U);
	assert(stats.stream_affinity_capacity == 4U);
	assert(stats.stream_affinity_count == 1U);

	/* CONTROL loss invalidates capabilities that were only RESERVED. */
	assert(tr_pipeline_remove_data(pipeline, reused) == TR_OK);
	assert(tr_pipeline_reserve_data(pipeline, &reused) == TR_OK);
	assert(tr_pipeline_clear_control(pipeline, control) == TR_OK);
	assert(tr_pipeline_control(pipeline, &connection) == TR_ERR_STALE);
	assert(tr_pipeline_attach_data(pipeline, reused, data_conn2) ==
	       TR_ERR_STATE);
	assert(tr_pipeline_cancel_data_reservation(pipeline, reused) ==
	       TR_ERR_STALE);

	/*
	 * destroy 只释放已经 quiesce 的 Pipeline soft-state。DATA membership
	 * 不由 Pipeline 拥有，测试必须先显式解除最后一个 ATTACHED generation。
	 */
	assert(tr_pipeline_remove_data(pipeline, data1) == TR_OK);
	tr_pipeline_destroy(pipeline);
	assert(tr_reactor_stop(other) == TR_OK);
	assert(tr_reactor_stop(owner) == TR_OK);
	tr_reactor_destroy(other);
	tr_reactor_destroy(owner);
}

static void test_pipeline_validation(void)
{
	struct tr_pipeline_config config;
	struct tr_pipeline *pipeline = (struct tr_pipeline *)(uintptr_t)1U;

	memset(&config, 0, sizeof(config));
	assert(tr_pipeline_create(&config, &pipeline) == TR_ERR_INVALID);
	assert(pipeline == NULL);
}

int main(void)
{
	test_pipeline_validation();
	test_pipeline_connection_group_and_affinity();
	puts("pipeline connection-group/affinity: ok");
	return 0;
}
