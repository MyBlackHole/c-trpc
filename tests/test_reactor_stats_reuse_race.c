#define _GNU_SOURCE

#include "../src/execution/reactor.h"
#include "tr/status.h"

#include <assert.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define REUSE_CYCLES 5000U
#define READER_THREADS 4U

struct test_context {
	struct tr_reactor *reactor;
	pthread_mutex_t handle_lock;
	struct tr_conn_handle handle;
	atomic_int stop_readers;
	atomic_int reader_error;
	atomic_uint_fast64_t stats_reads;
};

static void *stats_reader(void *arg)
{
	struct test_context *ctx = arg;

	while (!atomic_load_explicit(&ctx->stop_readers, memory_order_relaxed)) {
		struct tr_conn_handle handle;
		struct tr_connection_stats stats;
		int ret;

		assert(pthread_mutex_lock(&ctx->handle_lock) == 0);
		handle = ctx->handle;
		assert(pthread_mutex_unlock(&ctx->handle_lock) == 0);

		ret = tr_reactor_get_connection_stats(handle, &stats);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			atomic_store_explicit(&ctx->reader_error, ret,
					      memory_order_relaxed);
		atomic_fetch_add_explicit(&ctx->stats_reads, 1U,
					  memory_order_relaxed);
	}

	return NULL;
}

static int make_connection(struct tr_reactor *reactor,
			   struct tr_conn_handle *handle, int *peer_fd)
{
	int sockets[2];
	int ret;

	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) != 0)
		return TR_ERR_SYS;
	ret = tr_reactor_adopt_fd(reactor, sockets[0], handle);
	if (ret != TR_OK) {
		close(sockets[0]);
		close(sockets[1]);
		return ret;
	}
	*peer_fd = sockets[1];
	return TR_OK;
}

static void wait_for_free(struct tr_conn_handle handle)
{
	struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000L};
	unsigned attempts;

	for (attempts = 0U; attempts < 10000U; ++attempts) {
		enum tr_connection_state state;
		int ret = tr_reactor_get_connection_state(handle, &state);

		if (ret == TR_OK && state == TR_CONN_FREE)
			return;
		assert(ret == TR_OK || ret == TR_ERR_STALE);
		assert(nanosleep(&pause, NULL) == 0);
	}

	fputs("等待连接槽位释放超时\n", stderr);
	assert(0);
}

int main(void)
{
	struct tr_reactor_config config;
	struct test_context ctx;
	pthread_t readers[READER_THREADS];
	struct tr_conn_handle current;
	int peer_fd;
	unsigned i;

	memset(&config, 0, sizeof(config));
	config.max_connections = 1U;
	memset(&ctx, 0, sizeof(ctx));
	atomic_init(&ctx.stop_readers, 0);
	atomic_init(&ctx.reader_error, TR_OK);
	atomic_init(&ctx.stats_reads, 0U);
	assert(pthread_mutex_init(&ctx.handle_lock, NULL) == 0);
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &ctx.reactor) == TR_OK);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(make_connection(ctx.reactor, &current, &peer_fd) == TR_OK);
	assert(current.slot == 0U);
	assert(pthread_mutex_lock(&ctx.handle_lock) == 0);
	ctx.handle = current;
	assert(pthread_mutex_unlock(&ctx.handle_lock) == 0);

	for (i = 0U; i < READER_THREADS; ++i)
		assert(pthread_create(&readers[i], NULL, stats_reader, &ctx) == 0);
	while (atomic_load_explicit(&ctx.stats_reads, memory_order_relaxed) < 1000U)
		(void)sched_yield();

	for (i = 0U; i < REUSE_CYCLES; ++i) {
		assert(tr_reactor_close(current) == TR_OK);
		wait_for_free(current);
		close(peer_fd);
		assert(make_connection(ctx.reactor, &current, &peer_fd) == TR_OK);
		assert(current.slot == 0U);
		assert(pthread_mutex_lock(&ctx.handle_lock) == 0);
		ctx.handle = current;
		assert(pthread_mutex_unlock(&ctx.handle_lock) == 0);
	}

	atomic_store_explicit(&ctx.stop_readers, 1, memory_order_relaxed);
	for (i = 0U; i < READER_THREADS; ++i)
		assert(pthread_join(readers[i], NULL) == 0);
	assert(atomic_load_explicit(&ctx.reader_error, memory_order_relaxed) == TR_OK);
	assert(atomic_load_explicit(&ctx.stats_reads, memory_order_relaxed) >
	       (uint64_t)REUSE_CYCLES);

	assert(tr_reactor_close(current) == TR_OK);
	wait_for_free(current);
	close(peer_fd);
	assert(tr_reactor_stop(ctx.reactor) == TR_OK);
	assert(tr_reactor_destroy(ctx.reactor) == TR_OK);
	assert(pthread_mutex_destroy(&ctx.handle_lock) == 0);
	return 0;
}
