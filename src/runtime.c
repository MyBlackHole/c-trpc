#include "runtime_internal.h"

#include <stdlib.h>

#include "tr/socket.h"
#include "tr/status.h"
#include "rpc_internal.h"

#define TR_RUNTIME_PHASE3_SHARDS 1U

struct tr_runtime_shard {
	uint32_t shard_id;
	struct tr_reactor *reactor;
	struct tr_rpc_executor_group *rpc_executor;

	struct tr_runtime_peer *peers;
	uint32_t peer_capacity;
	uint32_t peer_count;
	uint32_t peer_count_peak;
	uint32_t peer_reaping_count;
	uint64_t peers_ready_total;
	uint64_t peers_reaped_total;
	uint64_t peer_capacity_rejections;

	int listen_fd;
	uint16_t bound_port;
	int started;
};

struct tr_runtime {
	uint32_t shard_count;
	struct tr_runtime_shard *shards;
	int started;
};

static void tr_runtime_shard_release(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	tr_runtime_shard_close_listener(shard);
	if (shard->rpc_executor) {
		tr_rpc_executor_group_destroy(shard->rpc_executor);
		shard->rpc_executor = NULL;
	}
	if (shard->reactor) {
		tr_reactor_destroy(shard->reactor);
		shard->reactor = NULL;
	}
	free(shard->peers);
	shard->peers = NULL;
	shard->peer_capacity = 0U;
}

static int
tr_runtime_rpc_executor_config_valid(const struct tr_runtime_config *config)
{
	const struct tr_runtime_rpc_executor_config *executor =
		&config->rpc_executor;

	if (executor->endpoint_capacity == 0U)
		return executor->max_calls_per_endpoint == 0U &&
		       executor->thread_count == 0U;
	return executor->max_calls_per_endpoint != 0U;
}

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out)
{
	struct tr_runtime *runtime;
	uint32_t i;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || config->shard_count != TR_RUNTIME_PHASE3_SHARDS ||
	    !tr_runtime_rpc_executor_config_valid(config))
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
		shard->listen_fd = -1;
		shard->peer_capacity = config->peer_capacity;
		if (shard->peer_capacity != 0U) {
			shard->peers = (struct tr_runtime_peer *)calloc(
				shard->peer_capacity, sizeof(*shard->peers));
			if (!shard->peers)
				ret = TR_ERR_NOMEM;
			else
				ret = TR_OK;
		} else {
			ret = TR_OK;
		}
		if (ret == TR_OK)
			ret = tr_reactor_create(&config->reactor, NULL, NULL, NULL,
						&shard->reactor);
		if (ret == TR_OK && config->rpc_executor.endpoint_capacity != 0U)
			ret = tr_rpc_executor_group_create(
				config->rpc_executor.endpoint_capacity,
				config->rpc_executor.max_calls_per_endpoint,
				config->rpc_executor.thread_count,
				&shard->rpc_executor);
		if (ret != TR_OK) {
			tr_runtime_shard_release(shard);
			while (i != 0U) {
				--i;
				tr_runtime_shard_release(&runtime->shards[i]);
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
		tr_runtime_shard_release(&runtime->shards[i]);
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

struct tr_rpc_executor_group *
tr_runtime_shard_rpc_executor(const struct tr_runtime_shard *shard)
{
	return shard ? shard->rpc_executor : NULL;
}

int tr_runtime_shard_listen_ipv4(struct tr_runtime_shard *shard,
				 const char *address, uint16_t port,
				 int backlog, uint16_t *out_bound_port)
{
	int fd = -1;
	uint16_t bound = 0;
	int ret;

	if (!shard || !address || backlog <= 0)
		return TR_ERR_INVALID;
	if (shard->listen_fd >= 0)
		return TR_ERR_STATE;

	ret = tr_tcp_listen_ipv4(address, port, backlog, &fd, &bound);
	if (ret != TR_OK)
		return ret;

	shard->listen_fd = fd;
	shard->bound_port = bound;
	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

int tr_runtime_shard_listener_fd(const struct tr_runtime_shard *shard)
{
	return shard ? shard->listen_fd : -1;
}

uint16_t tr_runtime_shard_bound_port(const struct tr_runtime_shard *shard)
{
	return shard ? shard->bound_port : 0U;
}

void tr_runtime_shard_close_listener(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	tr_socket_close(&shard->listen_fd);
	shard->bound_port = 0U;
}

uint32_t tr_runtime_shard_peer_capacity(const struct tr_runtime_shard *shard)
{
	return shard ? shard->peer_capacity : 0U;
}

struct tr_runtime_peer *
tr_runtime_shard_peer_at(struct tr_runtime_shard *shard, uint32_t slot)
{
	if (!shard || slot >= shard->peer_capacity)
		return NULL;
	return &shard->peers[slot];
}

void tr_runtime_shard_peer_note_added(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	shard->peer_count++;
	if (shard->peer_count > shard->peer_count_peak)
		shard->peer_count_peak = shard->peer_count;
}

void tr_runtime_shard_peer_note_ready(struct tr_runtime_shard *shard)
{
	if (shard)
		shard->peers_ready_total++;
}

void tr_runtime_shard_peer_note_removed_for_reap(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	if (shard->peer_count != 0U)
		shard->peer_count--;
	shard->peer_reaping_count++;
}

void tr_runtime_shard_peer_note_reaped(struct tr_runtime_shard *shard)
{
	if (!shard)
		return;

	if (shard->peer_reaping_count != 0U)
		shard->peer_reaping_count--;
	shard->peers_reaped_total++;
}

void tr_runtime_shard_peer_note_capacity_rejection(
	struct tr_runtime_shard *shard)
{
	if (shard)
		shard->peer_capacity_rejections++;
}

void tr_runtime_shard_peer_stats(const struct tr_runtime_shard *shard,
				 struct tr_runtime_peer_stats *out)
{
	if (!out)
		return;

	memset(out, 0, sizeof(*out));
	if (!shard)
		return;

	out->capacity = shard->peer_capacity;
	out->current = shard->peer_count;
	out->peak = shard->peer_count_peak;
	out->reaping_current = shard->peer_reaping_count;
	out->ready_total = shard->peers_ready_total;
	out->reaped_total = shard->peers_reaped_total;
	out->capacity_rejections = shard->peer_capacity_rejections;
}
