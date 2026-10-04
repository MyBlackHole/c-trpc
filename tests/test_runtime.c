#include "../src/runtime/runtime_internal.h"

#include "tr/status.h"
#include "../src/io/socket.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void runtime_test_config_init(
	struct tr_runtime_config *config,
	struct tr_runtime_shard_config *shards, uint32_t shard_count)
{
	memset(config, 0, sizeof(*config));
	memset(shards, 0, sizeof(*shards) * shard_count);
	config->shard_count = shard_count;
	config->shards = shards;
}

static void test_runtime_single_shard_identity(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;

	runtime_test_config_init(&config, &shard_config, 1U);

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

static void test_runtime_multi_shard_identity(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[3];
	struct tr_runtime *runtime = NULL;
	uint32_t i;

	runtime_test_config_init(&config, shards, 3U);
	shards[0].peer_capacity = 2U;
	shards[1].peer_capacity = 3U;
	shards[2].peer_capacity = 4U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	assert(runtime != NULL);
	assert(tr_runtime_shard_count(runtime) == 3U);

	for (i = 0; i < 3U; ++i) {
		struct tr_runtime_shard *shard =
			tr_runtime_shard_at(runtime, i);

		assert(shard != NULL);
		assert(tr_runtime_shard_id(shard) == i);
		assert(tr_runtime_shard_reactor(shard) != NULL);
		assert(tr_runtime_shard_peer_capacity(shard) == i + 2U);
	}
	assert(tr_runtime_shard_at(runtime, 3U) == NULL);

	tr_runtime_destroy(runtime);
}

static void test_runtime_rejects_invalid_shard_config(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard;
	struct tr_runtime *runtime = (struct tr_runtime *)(uintptr_t)1U;

	memset(&config, 0, sizeof(config));
	assert(tr_runtime_create(&config, &runtime) == TR_ERR_INVALID);
	assert(runtime == NULL);

	runtime_test_config_init(&config, &shard, 1U);
	config.shards = NULL;
	runtime = (struct tr_runtime *)(uintptr_t)1U;
	assert(tr_runtime_create(&config, &runtime) == TR_ERR_INVALID);
	assert(runtime == NULL);
}

static void test_runtime_reuseport_listener_group(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shards[2];
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard0;
	struct tr_runtime_shard *shard1;
	uint16_t port0 = 0U;
	uint16_t port1 = 0U;

	runtime_test_config_init(&config, shards, 2U);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard0 = tr_runtime_shard_at(runtime, 0U);
	shard1 = tr_runtime_shard_at(runtime, 1U);
	assert(shard0 != NULL && shard1 != NULL);

	assert(tr_runtime_shard_listen_ipv4_ex(
		       shard0, "127.0.0.1", 0U, 8, 1, &port0) == TR_OK);
	assert(port0 != 0U);
	assert(tr_runtime_shard_listen_ipv4_ex(
		       shard1, "127.0.0.1", port0, 8, 1, &port1) == TR_OK);
	assert(port1 == port0);
	assert(tr_runtime_shard_listener_fd(shard0) >= 0);
	assert(tr_runtime_shard_listener_fd(shard1) >= 0);

	tr_runtime_destroy(runtime);
}

static void test_runtime_shard_rpc_executor_ownership(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;

	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.rpc_executor.endpoint_capacity = 4U;
	shard_config.rpc_executor.max_calls_per_endpoint = 8U;
	shard_config.rpc_executor.thread_count = 2U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_rpc_executor(shard) != NULL);
	tr_runtime_destroy(runtime);

	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.rpc_executor.thread_count = 1U;
	assert(tr_runtime_create(&config, &runtime) == TR_ERR_INVALID);
	assert(runtime == NULL);
}

static void test_runtime_shard_listener_ownership(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	uint16_t bound = 0;
	int listener;

	runtime_test_config_init(&config, &shard_config, 1U);

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

struct runtime_listener_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned accepted;
};

static void runtime_listener_test_cb(int listener, uint32_t events, void *arg)
{
	struct runtime_listener_test_ctx *ctx =
		(struct runtime_listener_test_ctx *)arg;

	if (!(events & EPOLLIN))
		return;

	for (;;) {
		int fd = -1;
		int ret = tr_tcp_accept(listener, &fd);

		if (ret == TR_AGAIN)
			break;
		if (ret != TR_OK)
			break;

		tr_socket_close(&fd);
		pthread_mutex_lock(&ctx->lock);
		ctx->accepted++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}
}

static void test_runtime_shard_listener_events(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct runtime_listener_test_ctx ctx;
	struct timespec deadline;
	struct pollfd pfd;
	uint16_t bound = 0;
	int client_fd = -1;
	int ret;

	runtime_test_config_init(&config, &shard_config, 1U);
	memset(&ctx, 0, sizeof(ctx));

	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	assert(tr_runtime_shard_listen_ipv4(
		       shard, "127.0.0.1", 0U, 8, &bound) == TR_OK);
	assert(bound != 0U);
	assert(tr_runtime_start(runtime) == TR_OK);
	assert(tr_runtime_shard_enable_listener_events(
		       shard, runtime_listener_test_cb, &ctx) == TR_OK);

	ret = tr_tcp_connect_ipv4("127.0.0.1", bound, &client_fd);
	assert(ret == TR_OK || ret == TR_IN_PROGRESS);
	if (ret == TR_IN_PROGRESS) {
		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = client_fd;
		pfd.events = POLLOUT;
		do {
			ret = poll(&pfd, 1U, 10000);
		} while (ret < 0 && errno == EINTR);
		assert(ret == 1);
		assert(tr_tcp_finish_connect(client_fd) == TR_OK);
	}

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.accepted == 0U)
		assert(pthread_cond_timedwait(&ctx.cond, &ctx.lock,
					     &deadline) == 0);
	assert(ctx.accepted == 1U);
	pthread_mutex_unlock(&ctx.lock);

	/*
	 * unregister is synchronous with the owner callback; once it returns the
	 * callback cannot still be using the shard listener.
	 */
	assert(tr_runtime_shard_disable_listener_events(shard) == TR_OK);
	tr_socket_close(&client_fd);
	assert(tr_runtime_stop(runtime) == TR_OK);
	tr_runtime_destroy(runtime);
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
}

static void test_runtime_shard_peer_resources(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct tr_runtime_peer *peer0;
	struct tr_runtime_peer *peer1;
	struct tr_runtime_peer_stats stats;

	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.peer_capacity = 3U;

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

static void test_runtime_shard_peer_event_source(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct pollfd pfd;
	int event_fd;

	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.peer_capacity = 2U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	event_fd = tr_runtime_shard_peer_event_fd(shard);
	assert(event_fd >= 0);

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = event_fd;
	pfd.events = POLLIN;
	assert(poll(&pfd, 1U, 0) == 0);

	tr_runtime_shard_signal_peer_event(shard);
	tr_runtime_shard_signal_peer_event(shard);
	assert(poll(&pfd, 1U, 0) == 1);
	assert(pfd.revents & POLLIN);

	/* Multiple producers coalesce into one readable lifecycle event source. */
	tr_runtime_shard_drain_peer_event(shard);
	pfd.revents = 0;
	assert(poll(&pfd, 1U, 0) == 0);

	tr_runtime_destroy(runtime);

	errno = 0;
	assert(fcntl(event_fd, F_GETFD) == -1);
	assert(errno == EBADF);
}

struct runtime_peer_event_dispatch_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_runtime_shard *shard;
	unsigned calls;
};

static void runtime_peer_event_dispatch_cb(int fd, uint32_t events, void *arg)
{
	struct runtime_peer_event_dispatch_ctx *ctx =
		(struct runtime_peer_event_dispatch_ctx *)arg;

	(void)fd;
	if (!(events & EPOLLIN))
		return;

	tr_runtime_shard_drain_peer_event(ctx->shard);
	pthread_mutex_lock(&ctx->lock);
	ctx->calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_runtime_shard_peer_event_dispatch(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct runtime_peer_event_dispatch_ctx ctx;
	struct timespec deadline;

	runtime_test_config_init(&config, &shard_config, 1U);
	memset(&ctx, 0, sizeof(ctx));
	shard_config.peer_capacity = 1U;

	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	ctx.shard = shard;
	assert(tr_runtime_start(runtime) == TR_OK);
	assert(tr_runtime_shard_enable_peer_events(
		       shard, runtime_peer_event_dispatch_cb, &ctx) == TR_OK);

	tr_runtime_shard_signal_peer_event(shard);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.calls == 0U)
		assert(pthread_cond_timedwait(&ctx.cond, &ctx.lock,
					     &deadline) == 0);
	assert(ctx.calls == 1U);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_runtime_shard_disable_peer_events(shard) == TR_OK);
	assert(tr_runtime_stop(runtime) == TR_OK);
	tr_runtime_destroy(runtime);
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
}

static void test_runtime_shard_memory_budget(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;
	struct tr_runtime_shard *shard;
	struct tr_memory_budget *budget;
	struct tr_memory_budget_stats stats;
	uint64_t peer_bytes =
		UINT64_C(2) * (uint64_t)sizeof(struct tr_runtime_peer);

	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.peer_capacity = 2U;
	shard_config.memory_budget_bytes = peer_bytes + 16U;

	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	assert(shard != NULL);
	budget = tr_runtime_shard_memory_budget(shard);
	assert(budget != NULL);

	memset(&stats, 0, sizeof(stats));
	tr_runtime_shard_memory_stats(shard, &stats);
	assert(stats.limit_bytes == peer_bytes + 16U);
	assert(stats.current_bytes == peer_bytes);
	assert(stats.peak_bytes == peer_bytes);
	assert(stats.rejection_events == 0U);

	assert(tr_memory_budget_reserve(budget, 8U) == TR_OK);
	tr_runtime_shard_memory_stats(shard, &stats);
	assert(stats.current_bytes == peer_bytes + 8U);
	assert(stats.peak_bytes == peer_bytes + 8U);

	assert(tr_memory_budget_release(budget, 8U) == TR_OK);
	assert(tr_memory_budget_release(budget, peer_bytes + 1U) ==
	       TR_ERR_STATE);
	assert(tr_memory_budget_reserve(budget, 17U) == TR_AGAIN);
	tr_runtime_shard_memory_stats(shard, &stats);
	assert(stats.current_bytes == peer_bytes);
	assert(stats.peak_bytes == peer_bytes + 8U);
	assert(stats.rejection_events == 1U);
	tr_runtime_destroy(runtime);

	runtime = (struct tr_runtime *)(uintptr_t)1U;
	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.peer_capacity = 2U;
	shard_config.memory_budget_bytes = peer_bytes - 1U;
	assert(tr_runtime_create(&config, &runtime) == TR_AGAIN);
	assert(runtime == NULL);

	/* 0 is accounting-only/unbounded during the Phase-7 migration. */
	runtime_test_config_init(&config, &shard_config, 1U);
	shard_config.peer_capacity = 1U;
	assert(tr_runtime_create(&config, &runtime) == TR_OK);
	shard = tr_runtime_shard_at(runtime, 0U);
	tr_runtime_shard_memory_stats(shard, &stats);
	assert(stats.limit_bytes == 0U);
	assert(stats.current_bytes ==
	       (uint64_t)sizeof(struct tr_runtime_peer));
	assert(stats.peak_bytes == stats.current_bytes);
	tr_runtime_destroy(runtime);
}

static void test_runtime_lifecycle(void)
{
	struct tr_runtime_config config;
	struct tr_runtime_shard_config shard_config;
	struct tr_runtime *runtime = NULL;

	runtime_test_config_init(&config, &shard_config, 1U);

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
	RUN_TEST(test_runtime_multi_shard_identity);
	RUN_TEST(test_runtime_rejects_invalid_shard_config);
	RUN_TEST(test_runtime_reuseport_listener_group);
	RUN_TEST(test_runtime_shard_rpc_executor_ownership);
	RUN_TEST(test_runtime_shard_listener_ownership);
	RUN_TEST(test_runtime_shard_listener_events);
	RUN_TEST(test_runtime_shard_peer_resources);
	RUN_TEST(test_runtime_shard_peer_event_source);
	RUN_TEST(test_runtime_shard_peer_event_dispatch);
	RUN_TEST(test_runtime_shard_memory_budget);
	RUN_TEST(test_runtime_lifecycle);
	return 0;
}
