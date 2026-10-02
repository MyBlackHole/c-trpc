#include "pipeline_registry_internal.h"

#include <stdlib.h>
#include <string.h>

#include "reactor_internal.h"
#include "tr/status.h"

enum tr_pipeline_registry_entry_state {
	TR_PIPELINE_REGISTRY_EMPTY = 0,
	TR_PIPELINE_REGISTRY_USED = 1,
	TR_PIPELINE_REGISTRY_TOMBSTONE = 2
};

struct tr_pipeline_registry_entry {
	uint64_t pipeline_id;
	struct tr_pipeline *pipeline;
	enum tr_pipeline_registry_entry_state state;
};

struct tr_pipeline_registry {
	struct tr_reactor *owner;
	uint32_t owner_shard_id;
	struct tr_pipeline_registry_entry *entries;
	uint32_t capacity;
	uint32_t count;
};

static uint32_t tr_pipeline_registry_hash(uint64_t pipeline_id)
{
	uint64_t value = pipeline_id;

	value ^= value >> 33;
	value *= UINT64_C(0xff51afd7ed558ccd);
	value ^= value >> 33;
	value *= UINT64_C(0xc4ceb9fe1a85ec53);
	value ^= value >> 33;
	return (uint32_t)(value ^ (value >> 32));
}

static int tr_pipeline_registry_find(
	struct tr_pipeline_registry *registry, uint64_t pipeline_id,
	uint32_t *found_out, uint32_t *insert_out)
{
	uint32_t start;
	uint32_t tombstone = UINT32_MAX;
	uint32_t offset;

	start = tr_pipeline_registry_hash(pipeline_id) % registry->capacity;
	for (offset = 0; offset < registry->capacity; ++offset) {
		uint32_t index = (start + offset) % registry->capacity;
		struct tr_pipeline_registry_entry *entry =
			&registry->entries[index];

		if (entry->state == TR_PIPELINE_REGISTRY_USED) {
			if (entry->pipeline_id == pipeline_id) {
				if (found_out)
					*found_out = index;
				return 1;
			}
			continue;
		}

		if (entry->state == TR_PIPELINE_REGISTRY_TOMBSTONE) {
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

int tr_pipeline_registry_create(
	const struct tr_pipeline_registry_config *config,
	struct tr_pipeline_registry **out)
{
	struct tr_pipeline_registry *registry;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (!config || !config->owner ||
	    config->owner_shard_id == UINT32_MAX ||
	    config->capacity == 0U)
		return TR_ERR_INVALID;

	registry = (struct tr_pipeline_registry *)calloc(1, sizeof(*registry));
	if (!registry)
		return TR_ERR_NOMEM;

	registry->entries = (struct tr_pipeline_registry_entry *)calloc(
		config->capacity, sizeof(*registry->entries));
	if (!registry->entries) {
		free(registry);
		return TR_ERR_NOMEM;
	}

	registry->owner = config->owner;
	registry->owner_shard_id = config->owner_shard_id;
	registry->capacity = config->capacity;
	*out = registry;
	return TR_OK;
}

void tr_pipeline_registry_destroy(struct tr_pipeline_registry *registry)
{
	if (!registry)
		return;

	free(registry->entries);
	free(registry);
}

struct tr_pipeline_registry_pipeline_request {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline *pipeline;
};

static int tr_pipeline_registry_register_on_owner(void *arg)
{
	struct tr_pipeline_registry_pipeline_request *request =
		(struct tr_pipeline_registry_pipeline_request *)arg;
	struct tr_pipeline_registry *registry = request->registry;
	struct tr_pipeline *pipeline = request->pipeline;
	struct tr_pipeline_registry_entry *entry;
	uint32_t found = UINT32_MAX;
	uint32_t insert = UINT32_MAX;

	if (tr_pipeline_owner(pipeline) != registry->owner ||
	    tr_pipeline_owner_shard_id(pipeline) !=
		    registry->owner_shard_id)
		return TR_ERR_INVALID;

	if (tr_pipeline_registry_find(registry, tr_pipeline_id(pipeline),
				      &found, &insert))
		return TR_ERR_STATE;
	if (registry->count == registry->capacity || insert == UINT32_MAX)
		return TR_AGAIN;

	entry = &registry->entries[insert];
	entry->pipeline_id = tr_pipeline_id(pipeline);
	entry->pipeline = pipeline;
	entry->state = TR_PIPELINE_REGISTRY_USED;
	registry->count++;
	return TR_OK;
}

int tr_pipeline_registry_register(struct tr_pipeline_registry *registry,
				  struct tr_pipeline *pipeline)
{
	struct tr_pipeline_registry_pipeline_request request;

	if (!registry || !pipeline)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.registry = registry;
	request.pipeline = pipeline;
	return tr_reactor_call(registry->owner,
			       tr_pipeline_registry_register_on_owner, &request);
}

static int tr_pipeline_registry_unregister_on_owner(void *arg)
{
	struct tr_pipeline_registry_pipeline_request *request =
		(struct tr_pipeline_registry_pipeline_request *)arg;
	struct tr_pipeline_registry *registry = request->registry;
	uint32_t found = UINT32_MAX;

	if (!tr_pipeline_registry_find(registry,
				       tr_pipeline_id(request->pipeline),
				       &found, NULL))
		return TR_ERR_STALE;
	if (registry->entries[found].pipeline != request->pipeline)
		return TR_ERR_STALE;

	registry->entries[found].pipeline = NULL;
	registry->entries[found].pipeline_id = 0U;
	registry->entries[found].state = TR_PIPELINE_REGISTRY_TOMBSTONE;
	registry->count--;
	return TR_OK;
}

int tr_pipeline_registry_unregister(struct tr_pipeline_registry *registry,
				    struct tr_pipeline *pipeline)
{
	struct tr_pipeline_registry_pipeline_request request;

	if (!registry || !pipeline)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.registry = registry;
	request.pipeline = pipeline;
	return tr_reactor_call(registry->owner,
			       tr_pipeline_registry_unregister_on_owner, &request);
}

struct tr_pipeline_registry_attach_request {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline_route_preface preface;
	struct tr_conn_handle connection;
	struct tr_pipeline_data_ref *data_out;
};

static int tr_pipeline_registry_attach_data_route_on_owner(void *arg)
{
	struct tr_pipeline_registry_attach_request *request =
		(struct tr_pipeline_registry_attach_request *)arg;
	struct tr_pipeline_registry *registry = request->registry;
	struct tr_pipeline *pipeline;
	struct tr_pipeline_data_ref data;
	uint32_t found = UINT32_MAX;
	int ret;

	ret = tr_pipeline_route_preface_validate_fields(&request->preface);
	if (ret != TR_OK)
		return ret;
	if (request->preface.role != TR_PIPELINE_ROUTE_DATA)
		return TR_ERR_BAD_TYPE;
	if (request->preface.owner_shard_id != registry->owner_shard_id)
		return TR_ERR_STALE;
	if (request->connection.reactor != registry->owner)
		return TR_ERR_INVALID;

	if (!tr_pipeline_registry_find(registry,
				       request->preface.pipeline_id,
				       &found, NULL))
		return TR_ERR_STALE;
	pipeline = registry->entries[found].pipeline;
	if (!pipeline)
		return TR_ERR_STALE;

	if (tr_pipeline_owner(pipeline) != registry->owner ||
	    tr_pipeline_owner_shard_id(pipeline) != registry->owner_shard_id ||
	    tr_pipeline_epoch(pipeline) != request->preface.epoch)
		return TR_ERR_STALE;

	data.index = request->preface.member_index;
	data.generation = request->preface.member_generation;
	ret = tr_pipeline_attach_data(pipeline, data, request->connection);
	if (ret != TR_OK)
		return ret;

	if (request->data_out)
		*request->data_out = data;
	return TR_OK;
}

int tr_pipeline_registry_attach_data_route(
	struct tr_pipeline_registry *registry,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection,
	struct tr_pipeline_data_ref *data_out)
{
	struct tr_pipeline_registry_attach_request request;

	if (!registry || !preface)
		return TR_ERR_INVALID;
	if (data_out)
		memset(data_out, 0, sizeof(*data_out));

	memset(&request, 0, sizeof(request));
	request.registry = registry;
	request.preface = *preface;
	request.connection = connection;
	request.data_out = data_out;
	return tr_reactor_call(
		registry->owner,
		tr_pipeline_registry_attach_data_route_on_owner, &request);
}

struct tr_pipeline_registry_stats_request {
	struct tr_pipeline_registry *registry;
	struct tr_pipeline_registry_stats *out;
};

static int tr_pipeline_registry_get_stats_on_owner(void *arg)
{
	struct tr_pipeline_registry_stats_request *request =
		(struct tr_pipeline_registry_stats_request *)arg;

	request->out->owner_shard_id = request->registry->owner_shard_id;
	request->out->capacity = request->registry->capacity;
	request->out->count = request->registry->count;
	return TR_OK;
}

int tr_pipeline_registry_get_stats(
	struct tr_pipeline_registry *registry,
	struct tr_pipeline_registry_stats *out)
{
	struct tr_pipeline_registry_stats_request request;

	if (!registry || !out)
		return TR_ERR_INVALID;
	memset(out, 0, sizeof(*out));
	request.registry = registry;
	request.out = out;
	return tr_reactor_call(registry->owner,
			       tr_pipeline_registry_get_stats_on_owner, &request);
}
