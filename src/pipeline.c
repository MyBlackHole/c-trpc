#include "pipeline_internal.h"

#include <stdlib.h>
#include <string.h>

#include "reactor_internal.h"
#include "tr/status.h"

enum tr_pipeline_affinity_state {
	TR_PIPELINE_AFFINITY_EMPTY = 0,
	TR_PIPELINE_AFFINITY_USED = 1,
	TR_PIPELINE_AFFINITY_TOMBSTONE = 2
};

struct tr_pipeline_data_slot {
	struct tr_conn_handle connection;
	uint32_t generation;
	int used;
};

struct tr_pipeline_affinity_entry {
	uint32_t stream_id;
	struct tr_pipeline_data_ref data;
	enum tr_pipeline_affinity_state state;
};

struct tr_pipeline {
	struct tr_reactor *owner;
	uint64_t pipeline_id;

	int control_bound;
	struct tr_conn_handle control;

	struct tr_pipeline_data_slot *data_slots;
	uint32_t data_capacity;
	uint32_t data_count;
	uint32_t next_data_index;

	struct tr_pipeline_affinity_entry *affinities;
	uint32_t affinity_capacity;
	uint32_t affinity_count;
};

static int tr_pipeline_conn_equal(struct tr_conn_handle a,
				  struct tr_conn_handle b)
{
	return a.reactor == b.reactor && a.slot == b.slot &&
	       a.generation == b.generation;
}

static int tr_pipeline_connection_valid(const struct tr_pipeline *pipeline,
					struct tr_conn_handle connection)
{
	return pipeline && connection.reactor == pipeline->owner;
}

static uint32_t tr_pipeline_stream_hash(uint32_t stream_id)
{
	uint32_t value = stream_id;

	value ^= value >> 16;
	value *= UINT32_C(0x7feb352d);
	value ^= value >> 15;
	value *= UINT32_C(0x846ca68b);
	value ^= value >> 16;
	return value;
}

static int tr_pipeline_data_ref_valid(const struct tr_pipeline *pipeline,
				      struct tr_pipeline_data_ref data)
{
	const struct tr_pipeline_data_slot *slot;

	if (!pipeline || data.index >= pipeline->data_capacity ||
	    data.generation == 0U)
		return 0;

	slot = &pipeline->data_slots[data.index];
	return slot->used && slot->generation == data.generation;
}

static uint32_t tr_pipeline_next_generation(uint32_t generation)
{
	generation++;
	if (generation == 0U)
		generation = 1U;
	return generation;
}

int tr_pipeline_create(const struct tr_pipeline_config *config,
		       struct tr_pipeline **out)
{
	struct tr_pipeline *pipeline;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (!config || !config->owner || config->pipeline_id == 0U ||
	    config->data_capacity == 0U ||
	    config->stream_affinity_capacity == 0U)
		return TR_ERR_INVALID;

	pipeline = (struct tr_pipeline *)calloc(1, sizeof(*pipeline));
	if (!pipeline)
		return TR_ERR_NOMEM;

	pipeline->data_slots = (struct tr_pipeline_data_slot *)calloc(
		config->data_capacity, sizeof(*pipeline->data_slots));
	pipeline->affinities = (struct tr_pipeline_affinity_entry *)calloc(
		config->stream_affinity_capacity, sizeof(*pipeline->affinities));
	if (!pipeline->data_slots || !pipeline->affinities) {
		free(pipeline->affinities);
		free(pipeline->data_slots);
		free(pipeline);
		return TR_ERR_NOMEM;
	}

	pipeline->owner = config->owner;
	pipeline->pipeline_id = config->pipeline_id;
	pipeline->data_capacity = config->data_capacity;
	pipeline->affinity_capacity = config->stream_affinity_capacity;
	*out = pipeline;
	return TR_OK;
}

void tr_pipeline_destroy(struct tr_pipeline *pipeline)
{
	if (!pipeline)
		return;

	free(pipeline->affinities);
	free(pipeline->data_slots);
	free(pipeline);
}

struct tr_reactor *tr_pipeline_owner(const struct tr_pipeline *pipeline)
{
	return pipeline ? pipeline->owner : NULL;
}

uint64_t tr_pipeline_id(const struct tr_pipeline *pipeline)
{
	return pipeline ? pipeline->pipeline_id : 0U;
}

struct tr_pipeline_control_request {
	struct tr_pipeline *pipeline;
	struct tr_conn_handle connection;
};

static int tr_pipeline_set_control_on_owner(void *arg)
{
	struct tr_pipeline_control_request *request =
		(struct tr_pipeline_control_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;

	if (!tr_pipeline_connection_valid(pipeline, request->connection))
		return TR_ERR_INVALID;
	if (pipeline->control_bound)
		return TR_ERR_STATE;

	pipeline->control = request->connection;
	pipeline->control_bound = 1;
	return TR_OK;
}

int tr_pipeline_set_control(struct tr_pipeline *pipeline,
			    struct tr_conn_handle connection)
{
	struct tr_pipeline_control_request request;

	if (!pipeline)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.connection = connection;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_set_control_on_owner, &request);
}

static int tr_pipeline_clear_control_on_owner(void *arg)
{
	struct tr_pipeline_control_request *request =
		(struct tr_pipeline_control_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;

	if (!pipeline->control_bound ||
	    !tr_pipeline_conn_equal(pipeline->control, request->connection))
		return TR_ERR_STALE;

	memset(&pipeline->control, 0, sizeof(pipeline->control));
	pipeline->control_bound = 0;
	return TR_OK;
}

int tr_pipeline_clear_control(struct tr_pipeline *pipeline,
			      struct tr_conn_handle expected)
{
	struct tr_pipeline_control_request request;

	if (!pipeline)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.connection = expected;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_clear_control_on_owner, &request);
}

struct tr_pipeline_control_get_request {
	struct tr_pipeline *pipeline;
	struct tr_conn_handle *out;
};

static int tr_pipeline_control_on_owner(void *arg)
{
	struct tr_pipeline_control_get_request *request =
		(struct tr_pipeline_control_get_request *)arg;

	if (!request->pipeline->control_bound)
		return TR_ERR_STALE;
	*request->out = request->pipeline->control;
	return TR_OK;
}

int tr_pipeline_control(struct tr_pipeline *pipeline,
			struct tr_conn_handle *out)
{
	struct tr_pipeline_control_get_request request;

	if (!pipeline || !out)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.out = out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_control_on_owner, &request);
}

struct tr_pipeline_data_add_request {
	struct tr_pipeline *pipeline;
	struct tr_conn_handle connection;
	struct tr_pipeline_data_ref *out;
};

static int tr_pipeline_add_data_on_owner(void *arg)
{
	struct tr_pipeline_data_add_request *request =
		(struct tr_pipeline_data_add_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	uint32_t i;

	if (!tr_pipeline_connection_valid(pipeline, request->connection))
		return TR_ERR_INVALID;
	if (pipeline->data_count == pipeline->data_capacity)
		return TR_AGAIN;

	for (i = 0; i < pipeline->data_capacity; ++i) {
		struct tr_pipeline_data_slot *slot = &pipeline->data_slots[i];

		if (slot->used)
			continue;
		slot->generation =
			tr_pipeline_next_generation(slot->generation);
		slot->connection = request->connection;
		slot->used = 1;
		pipeline->data_count++;
		request->out->index = i;
		request->out->generation = slot->generation;
		return TR_OK;
	}

	return TR_AGAIN;
}

int tr_pipeline_add_data(struct tr_pipeline *pipeline,
			 struct tr_conn_handle connection,
			 struct tr_pipeline_data_ref *out)
{
	struct tr_pipeline_data_add_request request;

	if (!pipeline || !out)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.connection = connection;
	request.out = out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_add_data_on_owner, &request);
}

struct tr_pipeline_data_request {
	struct tr_pipeline *pipeline;
	struct tr_pipeline_data_ref data;
	struct tr_conn_handle *connection_out;
};

static int tr_pipeline_remove_data_on_owner(void *arg)
{
	struct tr_pipeline_data_request *request =
		(struct tr_pipeline_data_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	struct tr_pipeline_data_slot *slot;
	uint32_t i;

	if (!tr_pipeline_data_ref_valid(pipeline, request->data))
		return TR_ERR_STALE;

	slot = &pipeline->data_slots[request->data.index];
	slot->used = 0;
	memset(&slot->connection, 0, sizeof(slot->connection));
	pipeline->data_count--;

	/*
	 * Stream 生命周期不能跨 DATA replacement 存活。删除 connection 时直接
	 * 使所有指向该 slot generation 的 affinity 失效。
	 */
	for (i = 0; i < pipeline->affinity_capacity; ++i) {
		struct tr_pipeline_affinity_entry *entry =
			&pipeline->affinities[i];

		if (entry->state != TR_PIPELINE_AFFINITY_USED ||
		    entry->data.index != request->data.index ||
		    entry->data.generation != request->data.generation)
			continue;
		entry->state = TR_PIPELINE_AFFINITY_TOMBSTONE;
		pipeline->affinity_count--;
	}

	return TR_OK;
}

int tr_pipeline_remove_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref data)
{
	struct tr_pipeline_data_request request;

	if (!pipeline)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.pipeline = pipeline;
	request.data = data;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_remove_data_on_owner, &request);
}

static int tr_pipeline_data_connection_on_owner(void *arg)
{
	struct tr_pipeline_data_request *request =
		(struct tr_pipeline_data_request *)arg;

	if (!tr_pipeline_data_ref_valid(request->pipeline, request->data))
		return TR_ERR_STALE;

	*request->connection_out =
		request->pipeline->data_slots[request->data.index].connection;
	return TR_OK;
}

int tr_pipeline_data_connection(struct tr_pipeline *pipeline,
				struct tr_pipeline_data_ref data,
				struct tr_conn_handle *out)
{
	struct tr_pipeline_data_request request;

	if (!pipeline || !out)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.data = data;
	request.connection_out = out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_data_connection_on_owner, &request);
}

struct tr_pipeline_select_request {
	struct tr_pipeline *pipeline;
	struct tr_pipeline_data_ref *out;
};

static int tr_pipeline_select_data_on_owner(void *arg)
{
	struct tr_pipeline_select_request *request =
		(struct tr_pipeline_select_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	uint32_t offset;

	if (pipeline->data_count == 0U)
		return TR_AGAIN;

	for (offset = 0; offset < pipeline->data_capacity; ++offset) {
		uint32_t index =
			(pipeline->next_data_index + offset) %
			pipeline->data_capacity;
		struct tr_pipeline_data_slot *slot =
			&pipeline->data_slots[index];

		if (!slot->used)
			continue;
		request->out->index = index;
		request->out->generation = slot->generation;
		pipeline->next_data_index =
			(index + 1U) % pipeline->data_capacity;
		return TR_OK;
	}

	return TR_AGAIN;
}

int tr_pipeline_select_data(struct tr_pipeline *pipeline,
			    struct tr_pipeline_data_ref *out)
{
	struct tr_pipeline_select_request request;

	if (!pipeline || !out)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.out = out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_select_data_on_owner, &request);
}

static int tr_pipeline_affinity_find(struct tr_pipeline *pipeline,
				     uint32_t stream_id,
				     uint32_t *slot_out,
				     uint32_t *insert_out)
{
	uint32_t start = tr_pipeline_stream_hash(stream_id) %
			 pipeline->affinity_capacity;
	uint32_t tombstone = UINT32_MAX;
	uint32_t offset;

	for (offset = 0; offset < pipeline->affinity_capacity; ++offset) {
		uint32_t index = (start + offset) % pipeline->affinity_capacity;
		struct tr_pipeline_affinity_entry *entry =
			&pipeline->affinities[index];

		if (entry->state == TR_PIPELINE_AFFINITY_USED) {
			if (entry->stream_id == stream_id) {
				if (slot_out)
					*slot_out = index;
				return 1;
			}
			continue;
		}
		if (entry->state == TR_PIPELINE_AFFINITY_TOMBSTONE) {
			if (tombstone == UINT32_MAX)
				tombstone = index;
			continue;
		}

		if (insert_out)
			*insert_out =
				tombstone != UINT32_MAX ? tombstone : index;
		return 0;
	}

	if (insert_out)
		*insert_out = tombstone;
	return 0;
}

struct tr_pipeline_affinity_request {
	struct tr_pipeline *pipeline;
	uint32_t stream_id;
	struct tr_pipeline_data_ref data;
	struct tr_pipeline_data_ref *data_out;
	struct tr_conn_handle *connection_out;
};

static int tr_pipeline_bind_stream_on_owner(void *arg)
{
	struct tr_pipeline_affinity_request *request =
		(struct tr_pipeline_affinity_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	struct tr_pipeline_affinity_entry *entry;
	uint32_t existing = UINT32_MAX;
	uint32_t insert = UINT32_MAX;

	if (request->stream_id == 0U)
		return TR_ERR_INVALID;
	if (!tr_pipeline_data_ref_valid(pipeline, request->data))
		return TR_ERR_STALE;
	if (tr_pipeline_affinity_find(pipeline, request->stream_id,
				      &existing, &insert))
		return TR_ERR_STATE;
	if (insert == UINT32_MAX)
		return TR_AGAIN;

	entry = &pipeline->affinities[insert];
	entry->stream_id = request->stream_id;
	entry->data = request->data;
	entry->state = TR_PIPELINE_AFFINITY_USED;
	pipeline->affinity_count++;
	return TR_OK;
}

int tr_pipeline_bind_stream(struct tr_pipeline *pipeline, uint32_t stream_id,
			    struct tr_pipeline_data_ref data)
{
	struct tr_pipeline_affinity_request request;

	if (!pipeline)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.pipeline = pipeline;
	request.stream_id = stream_id;
	request.data = data;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_bind_stream_on_owner, &request);
}

static int tr_pipeline_stream_data_on_owner(void *arg)
{
	struct tr_pipeline_affinity_request *request =
		(struct tr_pipeline_affinity_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	struct tr_pipeline_affinity_entry *entry;
	uint32_t slot = UINT32_MAX;

	if (request->stream_id == 0U)
		return TR_ERR_INVALID;
	if (!tr_pipeline_affinity_find(pipeline, request->stream_id,
				       &slot, NULL))
		return TR_ERR_STALE;

	entry = &pipeline->affinities[slot];
	if (!tr_pipeline_data_ref_valid(pipeline, entry->data)) {
		entry->state = TR_PIPELINE_AFFINITY_TOMBSTONE;
		pipeline->affinity_count--;
		return TR_ERR_STALE;
	}

	if (request->data_out)
		*request->data_out = entry->data;
	if (request->connection_out)
		*request->connection_out =
			pipeline->data_slots[entry->data.index].connection;
	return TR_OK;
}

int tr_pipeline_stream_data(struct tr_pipeline *pipeline, uint32_t stream_id,
			    struct tr_pipeline_data_ref *data_out,
			    struct tr_conn_handle *connection_out)
{
	struct tr_pipeline_affinity_request request;

	if (!pipeline || (!data_out && !connection_out))
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.pipeline = pipeline;
	request.stream_id = stream_id;
	request.data_out = data_out;
	request.connection_out = connection_out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_stream_data_on_owner, &request);
}

static int tr_pipeline_unbind_stream_on_owner(void *arg)
{
	struct tr_pipeline_affinity_request *request =
		(struct tr_pipeline_affinity_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;
	uint32_t slot = UINT32_MAX;

	if (request->stream_id == 0U)
		return TR_ERR_INVALID;
	if (!tr_pipeline_affinity_find(pipeline, request->stream_id,
				       &slot, NULL))
		return TR_ERR_STALE;

	pipeline->affinities[slot].state = TR_PIPELINE_AFFINITY_TOMBSTONE;
	pipeline->affinity_count--;
	return TR_OK;
}

int tr_pipeline_unbind_stream(struct tr_pipeline *pipeline,
			      uint32_t stream_id)
{
	struct tr_pipeline_affinity_request request;

	if (!pipeline)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.pipeline = pipeline;
	request.stream_id = stream_id;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_unbind_stream_on_owner, &request);
}

struct tr_pipeline_stats_request {
	struct tr_pipeline *pipeline;
	struct tr_pipeline_stats *out;
};

static int tr_pipeline_get_stats_on_owner(void *arg)
{
	struct tr_pipeline_stats_request *request =
		(struct tr_pipeline_stats_request *)arg;
	struct tr_pipeline *pipeline = request->pipeline;

	memset(request->out, 0, sizeof(*request->out));
	request->out->pipeline_id = pipeline->pipeline_id;
	request->out->data_capacity = pipeline->data_capacity;
	request->out->data_count = pipeline->data_count;
	request->out->stream_affinity_capacity = pipeline->affinity_capacity;
	request->out->stream_affinity_count = pipeline->affinity_count;
	request->out->control_bound = pipeline->control_bound;
	return TR_OK;
}

int tr_pipeline_get_stats(struct tr_pipeline *pipeline,
			  struct tr_pipeline_stats *out)
{
	struct tr_pipeline_stats_request request;

	if (!pipeline || !out)
		return TR_ERR_INVALID;
	request.pipeline = pipeline;
	request.out = out;
	return tr_reactor_call(pipeline->owner,
			       tr_pipeline_get_stats_on_owner, &request);
}
