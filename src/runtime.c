#include "runtime_internal.h"

#include <stdlib.h>

#include "tr/status.h"

#define TR_RUNTIME_PHASE3_SHARDS 1U

struct tr_runtime_shard {
	uint32_t shard_id;
	struct tr_reactor *reactor;
	int started;
};

struct tr_runtime {
	uint32_t shard_count;
	struct tr_runtime_shard *shards;
	int started;
};

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out)
{
	struct tr_runtime *runtime;
	uint32_t i;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || config->shard_count != TR_RUNTIME_PHASE3_SHARDS)
		return TR_ERR_INVALID;

	runtime = (struct tr_runtime *)calloc(1, sizeof(*runtime));
	if (!runtime)
		return TR_ERR_NOMEM;

	runtime->shards = (struct tr_runtime_shard *)calloc(
		config->shard_count, sizeof(*runtime->shards));
	if (!runtime->shards) {
		free(runtime);
		return TR_ERR_NOMEM;
	}
	runtime->shard_count = config->shard_count;

	for (i = 0; i < runtime->shard_count; ++i) {
		struct tr_runtime_shard *shard = &runtime->shards[i];
		int ret;

		shard->shard_id = i;
		ret = tr_reactor_create(&config->reactor, NULL, NULL, NULL,
					&shard->reactor);
		if (ret != TR_OK) {
			while (i != 0U) {
				--i;
				tr_reactor_destroy(runtime->shards[i].reactor);
			}
			free(runtime->shards);
			free(runtime);
			return ret;
		}
	}

	*out = runtime;
	return TR_OK;
}

int tr_runtime_start(struct tr_runtime *runtime)
{
	uint32_t i;

	if (!runtime)
		return TR_ERR_INVALID;
	if (runtime->started)
		return TR_ERR_STATE;

	for (i = 0; i < runtime->shard_count; ++i) {
		struct tr_runtime_shard *shard = &runtime->shards[i];
		int ret;

		if (shard->started)
			continue;
		ret = tr_reactor_start(shard->reactor);
		if (ret != TR_OK) {
			while (i != 0U) {
				struct tr_runtime_shard *started;

				--i;
				started = &runtime->shards[i];
				if (started->started) {
					(void)tr_reactor_stop(started->reactor);
					started->started = 0;
				}
			}
			return ret;
		}
		shard->started = 1;
	}

	runtime->started = 1;
	return TR_OK;
}

int tr_runtime_stop(struct tr_runtime *runtime)
{
	uint32_t i;
	int result = TR_OK;

	if (!runtime)
		return TR_ERR_INVALID;
	if (!runtime->started)
		return TR_OK;

	i = runtime->shard_count;
	while (i != 0U) {
		struct tr_runtime_shard *shard;
		int ret;

		--i;
		shard = &runtime->shards[i];
		if (!shard->started)
			continue;
		ret = tr_reactor_stop(shard->reactor);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
		if (ret == TR_OK)
			shard->started = 0;
	}

	if (result == TR_OK)
		runtime->started = 0;
	return result;
}

void tr_runtime_destroy(struct tr_runtime *runtime)
{
	uint32_t i;

	if (!runtime)
		return;

	(void)tr_runtime_stop(runtime);
	for (i = 0; i < runtime->shard_count; ++i)
		tr_reactor_destroy(runtime->shards[i].reactor);
	free(runtime->shards);
	free(runtime);
}

uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime)
{
	return runtime ? runtime->shard_count : 0U;
}

struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index)
{
	if (!runtime || index >= runtime->shard_count)
		return NULL;
	return &runtime->shards[index];
}

uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard)
{
	return shard ? shard->shard_id : UINT32_MAX;
}

struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard)
{
	return shard ? shard->reactor : NULL;
}
