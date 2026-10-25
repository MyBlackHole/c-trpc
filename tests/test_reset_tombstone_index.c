#include "../src/transport/channel/channel_internal.h"
#include "../src/transport/channel/reset_tombstone_index.h"
#include "tr/status.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static void colliding_stream_ids(struct tr_conn_handle connection,
				 uint32_t home, uint32_t mask, uint32_t out[3])
{
	uint32_t found = 0U;

	for (uint32_t candidate = 1U; found < 3U; ++candidate) {
		if ((tr_reset_tombstone_index_home(8U, connection,
		                                  candidate) & mask) == home)
			out[found++] = candidate;
	}
}

static void test_wrapped_collision_cluster_delete_and_reuse(void)
{
	uint32_t slots[8] = {0U};
	struct tr_stream_reset_tombstone history[4] = {0};
	const struct tr_conn_handle connection = {NULL, 3U, 7U};
	uint32_t ids[3];
	uint32_t reuse_ids[3];
	uint32_t slot = UINT32_MAX;
	size_t probes = 0U;

	/* Home 7 forces a collision cluster to wrap through slot zero. */
	colliding_stream_ids(connection, 7U, 7U, ids);
	for (uint32_t i = 0U; i < 3U; ++i) {
		history[i].connection = connection;
		history[i].stream_id = ids[i];
		assert(tr_reset_tombstone_index_insert(
		               slots, 8U, history, 4U, connection, ids[i], i,
		               &probes) == TR_OK);
		assert(probes == (size_t)i + 1U);
	}

	assert(slots[7] == 1U && slots[0] == 2U && slots[1] == 3U);
	assert(tr_reset_tombstone_index_remove(
		               slots, 8U, history, 4U, connection, ids[0],
		               &probes) == TR_OK);
	assert(tr_reset_tombstone_index_find(
		               slots, 8U, history, 4U, connection, ids[0], &slot,
		               &probes) == TR_ERR_STALE);
	assert(tr_reset_tombstone_index_find(
		               slots, 8U, history, 4U, connection, ids[1], &slot,
		               &probes) == TR_OK);
	assert(slot == 1U && probes == 1U);
	assert(tr_reset_tombstone_index_find(
		               slots, 8U, history, 4U, connection, ids[2], &slot,
		               &probes) == TR_OK);
	assert(slot == 2U && probes == 2U);

	/* Reusing the vacated FIFO slot must not leave the old key reachable. */
	colliding_stream_ids(connection, 3U, 7U, reuse_ids);
	history[0].connection = connection;
	history[0].stream_id = reuse_ids[0];
	assert(tr_reset_tombstone_index_insert(
	               slots, 8U, history, 4U, connection, reuse_ids[0], 0U,
	               &probes) == TR_OK);
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 4U, connection, reuse_ids[0], &slot,
	               &probes) == TR_OK);
	assert(slot == 0U);
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 4U, connection, ids[1], &slot,
	               &probes) == TR_OK);
	assert(slot == 1U);
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 4U, connection, ids[2], &slot,
	               &probes) == TR_OK);
	assert(slot == 2U);
}

static void test_duplicate_insert_and_invalid_slots(void)
{
	uint32_t slots[4] = {0U};
	uint32_t undersized_slots[2] = {0U};
	struct tr_stream_reset_tombstone history[2] = {0};
	const struct tr_conn_handle connection = {NULL, 0U, 1U};
	uint32_t slot = UINT32_MAX;
	size_t probes = 0U;

	history[0].stream_id = 17U;
	history[0].connection = connection;
	assert(tr_reset_tombstone_index_insert(
	               slots, 4U, history, 2U, connection, 17U, 0U,
	               &probes) == TR_OK);
	assert(tr_reset_tombstone_index_insert(
	               slots, 4U, history, 2U, connection, 17U, 0U,
	               &probes) == TR_ERR_STATE);
	assert(tr_reset_tombstone_index_insert(
	               slots, 4U, history, 2U, connection, 17U, 2U,
	               &probes) == TR_ERR_INVALID);
	assert(tr_reset_tombstone_index_find(
	               slots, 4U, history, 2U, connection, 18U, &slot,
	               &probes) == TR_ERR_STALE);
	assert(slot == UINT32_MAX);

	/* A table with no guaranteed empty slot is rejected before use. */
	assert(tr_reset_tombstone_index_insert(
	               undersized_slots, 2U, history, 2U, connection, 17U, 0U,
	               &probes) == TR_ERR_INVALID);
}

static void test_same_stream_id_on_distinct_connections(void)
{
	uint32_t slots[8] = {0U};
	struct tr_stream_reset_tombstone history[2] = {0};
	const struct tr_conn_handle connection_a = {NULL, 1U, 3U};
	const struct tr_conn_handle connection_b = {NULL, 2U, 4U};
	size_t probes = 0U;

	history[0].connection = connection_a;
	history[0].stream_id = 1U;
	history[1].connection = connection_b;
	history[1].stream_id = 1U;
	assert(tr_reset_tombstone_index_insert(
	               slots, 8U, history, 2U, connection_a, 1U, 0U,
	               &probes) == TR_OK);
	/* Split CONTROL/BULK have independent peer ID frontiers. */
	assert(tr_reset_tombstone_index_insert(
	               slots, 8U, history, 2U, connection_b, 1U, 1U,
	               &probes) == TR_OK);
	uint32_t slot = UINT32_MAX;
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 2U, connection_a, 1U, &slot,
	               &probes) == TR_OK);
	assert(slot == 0U);
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 2U, connection_b, 1U, &slot,
	               &probes) == TR_OK);
	assert(slot == 1U);
	assert(tr_reset_tombstone_index_remove(
	               slots, 8U, history, 2U, connection_a, 1U,
	               &probes) == TR_OK);
	assert(tr_reset_tombstone_index_find(
	               slots, 8U, history, 2U, connection_b, 1U, &slot,
	               &probes) == TR_OK);
	assert(slot == 1U);
}

static void test_average_probe_bound_for_large_history(void)
{
	const uint32_t count = 40000U;
	size_t capacity = 1U;
	struct tr_stream_reset_tombstone *history;
	uint32_t *slots;
	const struct tr_conn_handle connection = {NULL, 5U, 9U};
	uint64_t total_probes = 0U;

	while (capacity < (size_t)count * 2U)
		capacity <<= 1U;
	history = calloc(count, sizeof(*history));
	slots = calloc(capacity, sizeof(*slots));
	assert(history && slots);

	for (uint32_t i = 0U; i < count; ++i) {
		history[i].connection = connection;
		history[i].stream_id = 2U * i + 1U;
		size_t probes = 0U;
		assert(tr_reset_tombstone_index_insert(
		               slots, capacity, history, count,
		               connection, history[i].stream_id, i,
		               &probes) == TR_OK);
		total_probes += probes;
	}
	for (uint32_t i = 0U; i < count; ++i) {
		uint32_t slot = UINT32_MAX;
		size_t probes = 0U;
		assert(tr_reset_tombstone_index_find(
		               slots, capacity, history, count,
		               connection, history[i].stream_id, &slot,
		               &probes) == TR_OK);
		assert(slot == i);
		total_probes += probes;
	}

	/* 固定 40k 历史下总探测数随条目数线性增长，而非逐项全表扫描。 */
	assert(total_probes <= (uint64_t)count * 8U);
	free(slots);
	free(history);
}

static void test_adversarial_collision_fifo_turnover(void)
{
	enum { collision_streams = 256U, collision_capacity = 512U };
	const uint32_t max_streams = collision_streams;
	const size_t capacity = collision_capacity;
	const uint32_t id_count = max_streams * 2U;
	uint32_t ids[collision_streams * 2U];
	uint32_t resident_ids[collision_streams];
	uint32_t resident_slots[collision_streams];
	uint32_t evicted_ids[collision_streams];
	uint32_t slots[collision_capacity] = {0U};
	struct tr_stream_reset_tombstone history[collision_streams] = {0};
	const struct tr_conn_handle connection = {NULL, 11U, 17U};
	size_t home;

	/* 所有键故意映射到同一 home bucket，构成确定的线性探测簇。 */
	home = tr_reset_tombstone_index_home(capacity, connection, 1U);
	{
		uint32_t found = 0U;
		for (uint32_t candidate = 1U; found < id_count; ++candidate) {
			if (tr_reset_tombstone_index_home(
			            capacity, connection, candidate) == home)
				ids[found++] = candidate;
		}
	}

	for (uint32_t i = 0U; i < max_streams; ++i) {
		size_t probes = 0U;
		assert(tr_reset_tombstone_index_home(
		               capacity, connection, ids[i]) == home);
		history[i].connection = connection;
		history[i].stream_id = ids[i];
		assert(tr_reset_tombstone_index_insert(
		               slots, capacity, history, max_streams,
		               connection, ids[i], i, &probes) == TR_OK);
		assert(probes == (size_t)i + 1U);
		resident_ids[i] = ids[i];
		resident_slots[i] = i;
	}

	/* 在 50% 占用下反复移除簇首并复用同一墓碑环槽。 */
	for (uint32_t turn = 0U; turn < max_streams; ++turn) {
		uint32_t recycled_slot = resident_slots[0];
		uint32_t stale_slot = UINT32_MAX;
		size_t probes = 0U;
		evicted_ids[turn] = resident_ids[0];
		assert(tr_reset_tombstone_index_remove(
		               slots, capacity, history, max_streams,
		               connection, resident_ids[0], &probes) == TR_OK);
		assert(probes == 1U);
		assert(tr_reset_tombstone_index_find(
		               slots, capacity, history, max_streams,
		               connection, resident_ids[0], &stale_slot,
		               &probes) == TR_ERR_STALE);
		assert(probes == (size_t)max_streams);

		history[recycled_slot].connection = connection;
		history[recycled_slot].stream_id = ids[max_streams + turn];
		assert(tr_reset_tombstone_index_insert(
		               slots, capacity, history, max_streams, connection,
		               ids[max_streams + turn], recycled_slot,
		               &probes) == TR_OK);
		assert(probes == (size_t)max_streams);

		for (uint32_t i = 0U; i + 1U < max_streams; ++i) {
			resident_ids[i] = resident_ids[i + 1U];
			resident_slots[i] = resident_slots[i + 1U];
		}
		resident_ids[max_streams - 1U] = ids[max_streams + turn];
		resident_slots[max_streams - 1U] = recycled_slot;
	}

	for (uint32_t i = 0U; i < max_streams; ++i) {
		uint32_t slot = UINT32_MAX;
		size_t probes = 0U;
		assert(tr_reset_tombstone_index_find(
		               slots, capacity, history, max_streams,
		               connection, resident_ids[i], &slot,
		               &probes) == TR_OK);
		assert(slot == resident_slots[i]);
		assert(probes == (size_t)i + 1U);
		assert(tr_reset_tombstone_index_find(
		               slots, capacity, history, max_streams,
		               connection, evicted_ids[i], &slot,
		               &probes) == TR_ERR_STALE);
		assert(probes == (size_t)max_streams + 1U);
	}
}

int main(void)
{
	test_wrapped_collision_cluster_delete_and_reuse();
	test_duplicate_insert_and_invalid_slots();
	test_same_stream_id_on_distinct_connections();
	test_average_probe_bound_for_large_history();
	test_adversarial_collision_fifo_turnover();
	puts("reset_tombstone_index: collision FIFO turnover, split identity, deletion, reuse, 40k probes passed");
	return 0;
}
