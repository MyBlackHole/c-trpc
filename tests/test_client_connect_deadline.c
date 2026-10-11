#define _GNU_SOURCE
#include "tr/client.h"
#include "tr/status.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel_internal.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#define TEST_TIMEOUT_MS 100U

struct connect_probe {
	struct tr_reactor *peer_reactor;
	struct tr_channel *peer_channel;
	struct timespec now;
	unsigned int poll_calls;
	unsigned int finish_calls;
	unsigned int hello_waits;
	unsigned int interrupts;
	uint32_t tcp_elapsed_ms;
	int fail_clock;
	int client_fd;
};

static _Thread_local struct connect_probe *active_probe;

int __real_clock_gettime(clockid_t clock, struct timespec *time);
int __real_poll(struct pollfd *fds, nfds_t count, int timeout);
int __real_tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out);
int __real_tr_tcp_finish_connect(int fd);
int __real_pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex,
				 const struct timespec *deadline);

int __wrap_clock_gettime(clockid_t clock, struct timespec *time)
{
	struct connect_probe *probe = active_probe;

	if (!probe || clock != CLOCK_MONOTONIC)
		return __real_clock_gettime(clock, time);
	if (probe->fail_clock) {
		errno = EIO;
		return -1;
	}
	*time = probe->now;
	return 0;
}

int __wrap_tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out)
{
	struct connect_probe *probe = active_probe;
	struct tr_channel_config config = {0};
	struct tr_conn_handle peer_connection;
	int sockets[2];

	if (!probe)
		return __real_tr_tcp_connect_ipv4(address, port, out);

	/* 使用真实连接和延迟启动的服务端 Channel，使 HELLO 必然未完成。 */
	active_probe = NULL;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			 0, sockets) == 0);
	assert(tr_reactor_adopt_fd(probe->peer_reactor, sockets[1],
				  &peer_connection) == TR_OK);
	config.role = TR_CHANNEL_SERVER;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(&config, peer_connection,
			peer_connection, NULL, NULL, NULL, NULL,
			&probe->peer_channel) == TR_OK);
	active_probe = probe;
	probe->client_fd = sockets[0];
	*out = sockets[0];
	return TR_IN_PROGRESS;
}

int __wrap_tr_tcp_finish_connect(int fd)
{
	struct connect_probe *probe = active_probe;

	if (!probe)
		return __real_tr_tcp_finish_connect(fd);
	assert(fd == probe->client_fd);
	probe->finish_calls++;
	return TR_OK;
}

int __wrap_poll(struct pollfd *fds, nfds_t count, int timeout)
{
	struct connect_probe *probe = active_probe;
	uint32_t elapsed_ms;

	if (!probe)
		return __real_poll(fds, count, timeout);
	assert(count == 1U && fds[0].fd == probe->client_fd);
	assert(fds[0].events == POLLOUT);
	elapsed_ms = (uint32_t)(probe->now.tv_nsec / 1000000L);
	assert(timeout == (int)(TEST_TIMEOUT_MS - elapsed_ms));
	probe->poll_calls++;
	if (probe->interrupts != 0U) {
		probe->interrupts--;
		probe->now.tv_nsec += 10000000L;
		errno = EINTR;
		return -1;
	}
	probe->now.tv_nsec = (long)probe->tcp_elapsed_ms * 1000000L;
	fds[0].revents = POLLOUT;
	return 1;
}

/* Fortify 构建下 poll 可能转换为此入口；两者共享同一脚本。 */
int __wrap___poll_chk(struct pollfd *fds, nfds_t count, int timeout,
		     size_t bytes)
{
	assert(count <= bytes / sizeof(*fds));
	return __wrap_poll(fds, count, timeout);
}

int __wrap_pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex,
				 const struct timespec *deadline)
{
	struct connect_probe *probe = active_probe;

	if (!probe)
		return __real_pthread_cond_timedwait(cond, mutex, deadline);
	assert(cond != NULL && mutex != NULL);
	assert(probe->finish_calls == 1U);
	assert(probe->now.tv_sec == 100);
	assert(probe->now.tv_nsec == (long)probe->tcp_elapsed_ms * 1000000L);
	/* TCP 已消耗 80ms，真实 HELLO 等待只能继承原 100ms 绝对期限。 */
	assert(deadline->tv_sec == 100);
	assert(deadline->tv_nsec == (long)TEST_TIMEOUT_MS * 1000000L);
	probe->hello_waits++;
	probe->now = *deadline;
	return ETIMEDOUT;
}

static void run_case(struct tr_client *client, uint32_t tcp_elapsed_ms,
		     unsigned int interrupts, int fail_clock)
{
	struct connect_probe probe = {
		.now = { .tv_sec = 100, .tv_nsec = 0 },
		.interrupts = interrupts,
		.tcp_elapsed_ms = tcp_elapsed_ms,
		.fail_clock = fail_clock,
		.client_fd = -1
	};
	int ret;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL,
				 &probe.peer_reactor) == TR_OK);
	assert(tr_reactor_start(probe.peer_reactor) == TR_OK);
	active_probe = &probe;
	ret = tr_client_connect(client, "127.0.0.1", 12345U);
	active_probe = NULL;
	assert(ret == (fail_clock ? TR_ERR_SYS : TR_ERR_TIMEOUT));
	if (fail_clock) {
		assert(probe.client_fd == -1);
		assert(probe.poll_calls == 0U && probe.hello_waits == 0U);
	} else {
		unsigned int reached_hello = tcp_elapsed_ms < TEST_TIMEOUT_MS;

		assert(probe.poll_calls == interrupts + 1U);
		assert(probe.finish_calls == reached_hello);
		assert(probe.hello_waits == reached_hello);
		/* 无论 TCP 阶段还是 HELLO 阶段超时，Client 都已归还 fd。 */
		errno = 0;
		assert(fcntl(probe.client_fd, F_GETFD) == -1 && errno == EBADF);
		assert(tr_channel_destroy(probe.peer_channel) == TR_OK);
	}
	assert(tr_reactor_stop(probe.peer_reactor) == TR_OK);
	assert(tr_reactor_destroy(probe.peer_reactor) == TR_OK);
}

int main(void)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;

	tr_client_config_init(&config);
	config.connect_timeout_ms = TEST_TIMEOUT_MS;
	config.keepalive_interval_ms = 0U;
	config.tcp_nodelay = TR_TCP_NODELAY_DISABLED;
	assert(tr_client_create(&config, &client) == TR_OK);
	/* 同一个 Client 重复连接，证明各失败路径没有留下部分会话。 */
	run_case(client, 80U, 0U, 0);
	run_case(client, 80U, 3U, 0);
	run_case(client, 100U, 0U, 0);
	run_case(client, 0U, 0U, 1);
	assert(tr_client_destroy(client) == TR_OK);
	puts("Client TCP/HELLO shared deadline and rollback: ok");
	return 0;
}
