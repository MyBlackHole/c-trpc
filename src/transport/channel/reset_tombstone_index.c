#include "reset_tombstone_index.h"

#include "channel_internal.h"
#include "tr/status.h"

static int tr_reset_tombstone_index_valid(
	const uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams)
{
	return slots && capacity && !(capacity & (capacity - 1U)) && history &&
	       max_streams && capacity > (size_t)max_streams;
}

static int tr_reset_tombstone_connection_equal(
	struct tr_conn_handle a, struct tr_conn_handle b)
{
	return a.reactor == b.reactor && a.slot == b.slot &&
	       a.generation == b.generation;
}

size_t tr_reset_tombstone_index_home(size_t capacity,
					    struct tr_conn_handle connection,
					    uint32_t stream_id)
{
	uint64_t value = ((uint64_t)connection.slot << 32) |
	                 connection.generation;
	value ^= (uint64_t)(uintptr_t)connection.reactor;
	value ^= (uint64_t)tr_channel_stream_id_hash(stream_id) *
	         UINT64_C(0x9e3779b97f4a7c15);
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	value ^= value >> 31;
	return (size_t)value & (capacity - 1U);
}

static int tr_reset_tombstone_index_find_position(
	const uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, size_t *position_out, size_t *probes_out)
{
	size_t mask = capacity - 1U;
	size_t position = tr_reset_tombstone_index_home(
		capacity, connection, stream_id);

	if (probes_out)
		*probes_out = 0U;
	for (size_t probes = 0U; probes < capacity; ++probes) {
		uint32_t encoded_slot = slots[position];
		if (probes_out)
			*probes_out = probes + 1U;
		if (!encoded_slot)
			return TR_ERR_STALE;
		uint32_t ring_slot = encoded_slot - 1U;
		if (ring_slot >= max_streams || !history[ring_slot].stream_id)
			return TR_ERR_STATE;
		if (history[ring_slot].stream_id == stream_id &&
		    tr_reset_tombstone_connection_equal(
		            history[ring_slot].connection, connection)) {
			*position_out = position;
			return TR_OK;
		}
		position = (position + 1U) & mask;
	}
	return TR_ERR_STALE;
}

int tr_reset_tombstone_index_find(
	const uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, uint32_t *ring_slot_out, size_t *probes_out)
{
	size_t position;
	int ret;

	if (probes_out)
		*probes_out = 0U;
	if (!tr_reset_tombstone_index_valid(slots, capacity, history,
					    max_streams) ||
	    !stream_id || !ring_slot_out)
		return TR_ERR_INVALID;
	ret = tr_reset_tombstone_index_find_position(
		slots, capacity, history, max_streams, connection, stream_id,
		&position, probes_out);
	if (ret == TR_OK)
		*ring_slot_out = slots[position] - 1U;
	return ret;
}

int tr_reset_tombstone_index_insert(
	uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, uint32_t ring_slot, size_t *probes_out)
{
	size_t mask;
	size_t position;

	if (probes_out)
		*probes_out = 0U;
	if (!tr_reset_tombstone_index_valid(slots, capacity, history,
					    max_streams) ||
	    !stream_id || ring_slot >= max_streams ||
	    history[ring_slot].stream_id != stream_id)
		return TR_ERR_INVALID;
	mask = capacity - 1U;
	position = tr_reset_tombstone_index_home(capacity, connection,
						 stream_id);
	for (size_t probes = 0U; probes < capacity; ++probes) {
		uint32_t encoded_slot = slots[position];
		if (probes_out)
			*probes_out = probes + 1U;
		if (!encoded_slot) {
			slots[position] = ring_slot + 1U;
			return TR_OK;
		}
		uint32_t occupied_slot = encoded_slot - 1U;
		if (occupied_slot >= max_streams ||
		    !history[occupied_slot].stream_id)
			return TR_ERR_STATE;
		if (history[occupied_slot].stream_id == stream_id &&
		    tr_reset_tombstone_connection_equal(
		            history[occupied_slot].connection, connection))
			return TR_ERR_STATE;
		position = (position + 1U) & mask;
	}
	return TR_AGAIN;
}

int tr_reset_tombstone_index_remove(
	uint32_t *slots, size_t capacity,
	const struct tr_stream_reset_tombstone *history,
	uint32_t max_streams, struct tr_conn_handle connection,
	uint32_t stream_id, size_t *probes_out)
{
	size_t hole;
	size_t mask;
	size_t validation_checks = 0U;
	int ret;

	if (probes_out)
		*probes_out = 0U;
	if (!tr_reset_tombstone_index_valid(slots, capacity, history,
					    max_streams) ||
	    !stream_id)
		return TR_ERR_INVALID;
	ret = tr_reset_tombstone_index_find_position(
		slots, capacity, history, max_streams, connection, stream_id,
		&hole, probes_out);
	if (ret != TR_OK)
		return ret;
	mask = capacity - 1U;

	/* 先验证整段探测簇，避免损坏槽位导致删除只完成一半。 */
	{
		size_t scan = (hole + 1U) & mask;
		size_t scanned = 0U;
		while (scanned < capacity) {
			uint32_t encoded_slot = slots[scan];
			if (!encoded_slot)
				break;
			uint32_t ring_slot = encoded_slot - 1U;
			if (ring_slot >= max_streams ||
			    !history[ring_slot].stream_id) {
				if (probes_out)
					*probes_out += scanned + 1U;
				return TR_ERR_STATE;
			}
			scan = (scan + 1U) & mask;
			scanned++;
		}
		if (scanned == capacity) {
			if (probes_out)
				*probes_out += scanned;
			return TR_ERR_STATE;
		}
		validation_checks = scanned + 1U;
	}

	{
		size_t scan_start = (hole + 1U) & mask;
		size_t scan = scan_start;
		while (slots[scan]) {
			uint32_t encoded_slot = slots[scan];
			uint32_t ring_slot = encoded_slot - 1U;
			uint32_t key = history[ring_slot].stream_id;
			size_t home = tr_reset_tombstone_index_home(
				capacity, history[ring_slot].connection, key);
			size_t scan_distance = (scan - home) & mask;
			size_t hole_distance = (hole - home) & mask;

			if (hole_distance < scan_distance) {
				slots[hole] = slots[scan];
				hole = scan;
			}
			scan = (scan + 1U) & mask;
		}
		if (probes_out)
			*probes_out += validation_checks +
			               ((scan - scan_start) & mask) + 1U;
	}
	slots[hole] = 0U;
	return TR_OK;
}
