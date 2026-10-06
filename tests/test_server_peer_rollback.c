#define _GNU_SOURCE
#include "tr/server.h"
#include "tr/status.h"

#include "../src/execution/reactor_internal.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/transport/channel/channel_internal.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static atomic_int fail_observer_once;
static atomic_uint observer_attempts;
static atomic_uint owner_close_attempts;
static atomic_uint queued_close_attempts;

static pthread_mutex_t probe_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t probe_cond = PTHREAD_COND_INITIALIZER;

int __real_tr_channel_set_lifecycle_observer(
	struct tr_channel *channel, tr_channel_event_cb event_cb,
	void *callback_arg);
int __real_tr_reactor_close_on_owner(struct tr_conn_handle connection);
int __real_tr_reactor_close(struct tr_conn_handle connection);

int __wrap_tr_channel_set_lifecycle_observer(
	struct tr_channel *channel, tr_channel_event_cb event_cb,
	void *callback_arg)
{
	atomic_fetch_add(&observer_attempts, 1U);
	if (atomic_exchange(&fail_observer_once, 0))
		return TR_ERR_BAD_LENGTH;
	return __real_tr_channel_set_lifecycle_observer(
		channel, event_cb, callback_arg);
}

int __wrap_tr_reactor_close_on_owner(struct tr_conn_handle connection)
{
	int ret;

	ret = __real_tr_reactor_close_on_owner(connection);
	atomic_fetch_add(&owner_close_attempts, 1U);

	pthread_mutex_lock(&probe_lock);
	pthread_cond_broadcast(&probe_cond);
	pthread_mutex_unlock(&probe_lock);
	return ret;
}

int __wrap_tr_reactor_close(struct tr_conn_handle connection)
{
	atomic_fetch_add(&queued_close_attempts, 1U);
	return __real_tr_reactor_close(connection);
}

static void small_server_config(struct tr_server_config *config)
{
	tr_server_config_init(config);
	config->shard_count = 1U;
	config->max_peers = 1U;
	config->listen_backlog = 4;
	config->keepalive_interval_ms = 0U;
	config->limits.max_streams = 4U;
	config->limits.max_methods = 2U;
	config->limits.max_calls = 4U;
	config->limits.max_frame_payload_bytes = 1024U;
	config->limits.max_message_bytes = 4096U;
	config->limits.initial_window_bytes = 4096U;
	config->limits.window_update_threshold_bytes = 1024U;
}

static int connect_loopback(uint16_t port)
{
	struct sockaddr_in address;
	int fd;

	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);

	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

	assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	return fd;
}

static void wait_for_owner_close(void)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&probe_lock);
	while (atomic_load(&owner_close_attempts) == 0U) {
		int ret = pthread_cond_timedwait(
			&probe_cond, &probe_lock, &deadline);

		if (ret == ETIMEDOUT)
			break;
		assert(ret == 0);
	}
	pthread_mutex_unlock(&probe_lock);
	assert(atomic_load(&owner_close_attempts) != 0U);
}

static void wait_for_peer_reap(struct tr_server *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_server_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 0U && stats.peers_reaped_total >= 1U)
			return;
		sched_yield();
	}
	assert(!"partial peer was not reaped");
}

int main(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	uint16_t port = 0U;
	int client_fd;

	small_server_config(&config);
	atomic_store(&fail_observer_once, 1);
	atomic_store(&observer_attempts, 0U);
	atomic_store(&owner_close_attempts, 0U);
	atomic_store(&queued_close_attempts, 0U);

	assert(tr_server_create(&config, &server) == TR_OK);
	assert(server != NULL);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	client_fd = connect_loopback(port);

	/*
	 * observer publication 在 peer adopt 中故意失败。rollback 必须在当前
	 * Reactor owner turn 调用 close_on_owner()，不能再向 command queue
	 * 提交普通 close。
	 */
	wait_for_owner_close();
	assert(atomic_load(&observer_attempts) == 1U);
	assert(atomic_load(&owner_close_attempts) == 1U);
	assert(atomic_load(&queued_close_attempts) == 0U);

	/*
	 * Channel 已经存在，因此 rollback 会发布 partial peer 并通过 shard
	 * lifecycle event 延迟 detach。最终 peer slot 与 detached storage 都必须
	 * 被回收，不得留下 ACTIVE/used 残留。
	 */
	wait_for_peer_reap(server);

	assert(close(client_fd) == 0);
	assert(tr_server_destroy(server) == TR_OK);
	return 0;
}
