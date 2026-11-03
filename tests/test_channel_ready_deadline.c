#define _GNU_SOURCE
#include "../src/transport/channel/channel_internal.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

/*
 * 只推进调用线程的单调时钟；真实 Reactor/HELLO 与其他线程不受影响。
 * 在取得真实 Channel 锁后推进时间，确定性重现“锁前未到期、锁后已到期”。
 */
struct deadline_probe {
	struct timespec now;
	struct timespec after_lock;
	unsigned int lock_calls;
	unsigned int clock_calls;
	int fail_after_lock;
};

static _Thread_local struct deadline_probe *active_probe;

int __real_clock_gettime(clockid_t clock, struct timespec *time);
int __real_pthread_mutex_lock(pthread_mutex_t *mutex);

int __wrap_clock_gettime(clockid_t clock, struct timespec *time)
{
	struct deadline_probe *probe = active_probe;

	if (!probe || clock != CLOCK_MONOTONIC)
		return __real_clock_gettime(clock, time);
	probe->clock_calls++;
	if (probe->fail_after_lock && probe->lock_calls != 0U) {
		errno = EIO;
		return -1;
	}
	*time = probe->now;
	return 0;
}

int __wrap_pthread_mutex_lock(pthread_mutex_t *mutex)
{
	struct deadline_probe *probe = active_probe;
	int ret = __real_pthread_mutex_lock(mutex);

	if (probe && ret == 0) {
		assert(probe->lock_calls == 0U);
		probe->lock_calls++;
		probe->now = probe->after_lock;
	}
	return ret;
}

struct fixture {
	struct tr_reactor *reactor;
	struct tr_channel *client;
	struct tr_channel *server;
};

static void fixture_init(struct fixture *fixture)
{
	struct tr_channel_config config = {0};
	struct tr_conn_handle client_connection, server_connection;
	int sockets[2];

	memset(fixture, 0, sizeof(*fixture));
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			 0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL,
				 &fixture->reactor) == TR_OK);
	assert(tr_reactor_start(fixture->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(fixture->reactor, sockets[0],
				  &client_connection) == TR_OK);
	assert(tr_reactor_adopt_fd(fixture->reactor, sockets[1],
				  &server_connection) == TR_OK);
	config.role = TR_CHANNEL_SERVER;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(&config, server_connection,
			server_connection, NULL, NULL, NULL, NULL,
			&fixture->server) == TR_OK);
	config.role = TR_CHANNEL_CLIENT;
	assert(tr_channel_create(&config, client_connection, client_connection,
			NULL, NULL, NULL, NULL, &fixture->client) == TR_OK);
	assert(tr_channel_start(fixture->server) == TR_OK);
	assert(tr_channel_wait_ready(fixture->client, TR_LANE_CONTROL,
				     5000U) == TR_OK);
	assert(tr_channel_wait_ready(fixture->server, TR_LANE_CONTROL,
				     5000U) == TR_OK);
}

static void test_ready_at_lock_boundary(struct fixture *fixture,
		enum tr_lane lane, long after_lock_ns, int fail_after_lock,
		int expected)
{
	struct deadline_probe probe = {
		.now = { .tv_sec = 100, .tv_nsec = 0 },
		.after_lock = { .tv_sec = 100, .tv_nsec = after_lock_ns },
		.fail_after_lock = fail_after_lock
	};
	const struct tr_deadline deadline = {
		.absolute = { .tv_sec = 100, .tv_nsec = 10000000L },
		.infinite = 0
	};
	int ret;

	active_probe = &probe;
	ret = tr_channel_wait_ready_deadline(fixture->client, lane, &deadline);
	active_probe = NULL;
	if (ret != expected)
		fprintf(stderr, "lane=%d after_lock=%ld fail=%d: got %d, expected %d\n",
			lane, after_lock_ns, fail_after_lock, ret, expected);
	assert(ret == expected);
	assert(probe.lock_calls == 1U);
	assert(probe.clock_calls == 2U);

	/* 每条返回路径都必须释放锁；随后正常就绪查询与销毁仍能继续。 */
	assert(tr_channel_wait_ready(fixture->client, lane, 0U) == TR_OK);
}

static void test_expired_before_lock(struct fixture *fixture)
{
	struct deadline_probe probe = {
		.now = { .tv_sec = 100, .tv_nsec = 10000000L }
	};
	const struct tr_deadline deadline = {
		.absolute = { .tv_sec = 100, .tv_nsec = 10000000L },
		.infinite = 0
	};
	int ret;

	active_probe = &probe;
	ret = tr_channel_wait_ready_deadline(fixture->client,
					    TR_LANE_CONTROL, &deadline);
	active_probe = NULL;
	assert(ret == TR_ERR_TIMEOUT);
	assert(probe.lock_calls == 0U);
	assert(probe.clock_calls == 1U);
}

static void test_zero_wait_has_no_clock_dependency(struct fixture *fixture)
{
	struct deadline_probe probe = { .fail_after_lock = 1 };
	int ret;

	active_probe = &probe;
	ret = tr_channel_wait_ready(fixture->client, TR_LANE_CONTROL, 0U);
	active_probe = NULL;
	assert(ret == TR_OK);
	assert(probe.lock_calls == 1U);
	assert(probe.clock_calls == 0U);
}

int main(void)
{
	struct fixture fixture;
	enum tr_lane lane;

	fixture_init(&fixture);
	for (lane = TR_LANE_CONTROL; lane <= TR_LANE_BULK; lane++) {
		/* 先运行修复前必败的精确到期路径，再覆盖两侧边界和时钟错误。 */
		test_ready_at_lock_boundary(&fixture, lane, 10000000L, 0,
					    TR_ERR_TIMEOUT);
		test_ready_at_lock_boundary(&fixture, lane, 10000001L, 0,
					    TR_ERR_TIMEOUT);
		test_ready_at_lock_boundary(&fixture, lane, 9999999L, 0, TR_OK);
		test_ready_at_lock_boundary(&fixture, lane, 0L, 1, TR_ERR_SYS);
	}
	test_expired_before_lock(&fixture);
	test_zero_wait_has_no_clock_dependency(&fixture);
	assert(tr_channel_destroy(fixture.client) == TR_OK);
	assert(tr_channel_destroy(fixture.server) == TR_OK);
	assert(tr_reactor_stop(fixture.reactor) == TR_OK);
	assert(tr_reactor_destroy(fixture.reactor) == TR_OK);
	puts("Channel ready deadline lock-boundary: ok");
	return 0;
}
