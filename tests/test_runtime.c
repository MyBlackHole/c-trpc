#include "../src/runtime_internal.h"

#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void test_runtime_single_shard_identity(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	assert(tr_runtime_shard_count(runtime) == 1U);

	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_id(shard) == 0U);
	assert(tr_runtime_shard_reactor(shard) != NULL);
	assert(tr_runtime_shard_rpc_executor(shard) == NULL);
	assert(tr_runtime_shard_at(runtime, 1U) == NULL);

	tr_runtime_destroy(runtime);
}

static void test_runtime_rejects_multi_shard_before_phase4(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = (struct tr_runtime *)(uintptr_t)1U;

	memset(&config, 0, sizeof(config));
	config.shard_count = 2U;

	assert(tr_runtime_create(&config, &runtime) == TR_ERR_INVALID);
	assert(runtime == NULL);
}

static void test_runtime_shard_rpc_executor_ownership(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;
	config.rpc_executor.endpoint_capacity = 4U;
	config.rpc_executor.max_calls_per_endpoint = 8U;
	config.rpc_executor.thread_count = 2U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_rpc_executor(shard) != NULL);
	tr_runtime_destroy(runtime);

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;
	config.rpc_executor.thread_count = 1U;
	assert(tr_runtime_create(&config, &runtime) == TR_ERR_INVALID);
	assert(runtime == NULL);
}

static void test_runtime_shard_listener_ownership(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	uint16_t bound = 0;
	int listener;

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_listener_fd(shard) == -1);
	assert(tr_runtime_shard_bound_port(shard) == 0U);

	assert(tr_runtime_shard_listen_ipv4(
		       shard, "127.0.0.1", 0U, 8, &bound) == TR_OK);
	assert(bound != 0U);
	listener = tr_runtime_shard_listener_fd(shard);
	assert(listener >= 0);
	assert(tr_runtime_shard_bound_port(shard) == bound);

	assert(tr_runtime_shard_listen_ipv4(
		       shard, "127.0.0.1", 0U, 8, NULL) == TR_ERR_STATE);

	tr_runtime_shard_close_listener(shard);
	assert(tr_runtime_shard_listener_fd(shard) == -1);
	assert(tr_runtime_shard_bound_port(shard) == 0U);

	/* Re-open proves explicit close fully releases the shard-owned resource. */
	assert(tr_runtime_shard_listen_ipv4(
		       shard, "127.0.0.1", 0U, 8, &bound) == TR_OK);
	listener = tr_runtime_shard_listener_fd(shard);
	assert(listener >= 0);

	tr_runtime_destroy(runtime);

	/* Runtime destruction is the final owner and must close a live listener. */
	errno = 0;
	assert(fcntl(listener, F_GETFD) == -1);
	assert(errno == EBADF);
}

static void test_runtime_shard_peer_resources(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct tr_runtime_peer *peer0;
	struct tr_runtime_peer *peer1;
	struct tr_runtime_peer_stats stats;

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;
	config.peer_capacity = 3U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_peer_capacity(shard) == 3U);
	assert(tr_runtime_shard_peer_at(shard, 3U) == NULL);

	peer0 = tr_runtime_shard_peer_at(shard, 0U);
	peer1 = tr_runtime_shard_peer_at(shard, 1U);
	assert(peer0 != NULL && peer1 != NULL);
	assert(!peer0->used && !peer1->used);

	tr_runtime_shard_peer_stats(shard, &stats);
	assert(stats.capacity == 3U);
	assert(stats.current == 0U);
	assert(stats.peak == 0U);
	assert(stats.reaping_current == 0U);
	assert(stats.ready_total == 0U);
	assert(stats.reaped_total == 0U);
	assert(stats.capacity_rejections == 0U);

	peer0->used = 1;
	tr_runtime_shard_peer_note_added(shard);
	tr_runtime_shard_peer_note_ready(shard);
	peer1->used = 1;
	tr_runtime_shard_peer_note_added(shard);
	tr_runtime_shard_peer_note_capacity_rejection(shard);

	tr_runtime_shard_peer_stats(shard, &stats);
	assert(stats.current == 2U);
	assert(stats.peak == 2U);
	assert(stats.ready_total == 1U);
	assert(stats.capacity_rejections == 1U);

	memset(peer0, 0, sizeof(*peer0));
	tr_runtime_shard_peer_note_removed_for_reap(shard);
	tr_runtime_shard_peer_stats(shard, &stats);
	assert(stats.current == 1U);
	assert(stats.reaping_current == 1U);
	tr_runtime_shard_peer_note_reaped(shard);

	memset(peer1, 0, sizeof(*peer1));
	tr_runtime_shard_peer_note_removed_for_reap(shard);
	tr_runtime_shard_peer_note_reaped(shard);

	tr_runtime_shard_peer_stats(shard, &stats);
	assert(stats.current == 0U);
	assert(stats.peak == 2U);
	assert(stats.reaping_current == 0U);
	assert(stats.reaped_total == 2U);

	tr_runtime_destroy(runtime);
}

static void test_runtime_lifecycle(void)
{
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;

	memset(&config, 0, sizeof(config));
	config.shard_count = 1U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(tr_runtime_stop(runtime) == TR_OK);
	assert(tr_runtime_start(runtime) == TR_OK);
	assert(tr_runtime_start(runtime) == TR_ERR_STATE);
	assert(tr_runtime_stop(runtime) == TR_OK);
	assert(tr_runtime_stop(runtime) == TR_OK);
	tr_runtime_destroy(runtime);
}

#define RUN_TEST(fn) do { fn(); puts(#fn ": ok"); } while (0)

int main(void)
{
	RUN_TEST(test_runtime_single_shard_identity);
	RUN_TEST(test_runtime_rejects_multi_shard_before_phase4);
	RUN_TEST(test_runtime_shard_rpc_executor_ownership);
	RUN_TEST(test_runtime_shard_listener_ownership);
	RUN_TEST(test_runtime_shard_peer_resources);
	RUN_TEST(test_runtime_lifecycle);
	return 0;
}
