#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/connector_internal.h"
#include "../src/io/socket.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_TIMEOUT_MS 20U
#define TEST_START_NS UINT64_C(1000000000)
#define TEST_DELAY_NS (UINT64_C(1000000) * TEST_TIMEOUT_MS)

enum progress_mode {
	PROGRESS_INTERRUPTS,
	PROGRESS_PARTIAL,
	PROGRESS_FINAL_SEND,
	PROGRESS_WRITABLE,
	PROGRESS_EAGAIN,
	PROGRESS_DETACH_RETRY
};

struct deadline_probe {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_connector *connector;
	enum progress_mode mode;
	uint64_t now_ns;
	uint64_t elapsed_ns;
	uint64_t event_token;
	unsigned send_calls;
	unsigned finish_calls;
	unsigned clock_calls;
	unsigned unregister_calls;
	unsigned callbacks;
	int fd;
	int peer;
	int fail_initial_clock;
	int fail_progress_clock;
	int fail_clock;
	int wait_writable;
	int status;
	int destroy_ret;
};

/* 仅执行本探针的所有者线程观察虚拟单调时钟。 */
static _Thread_local struct deadline_probe *active_probe;

int __real_clock_gettime(clockid_t clock, struct timespec *time);
ssize_t __real_send(int fd, const void *data, size_t len, int flags);
int __real_epoll_wait(int fd, struct epoll_event *events, int count, int timeout);
int __real_epoll_ctl(int fd, int operation, int target, struct epoll_event *event);
int __real_tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out);
int __real_tr_tcp_finish_connect(int fd);
int __real_tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd);

int __wrap_clock_gettime(clockid_t clock, struct timespec *time)
{
	struct deadline_probe *probe = active_probe;

	if (!probe || clock != CLOCK_MONOTONIC)
		return __real_clock_gettime(clock, time);
	probe->clock_calls++;
	if (probe->fail_clock) {
		errno = EIO;
		return -1;
	}
	time->tv_sec = (time_t)(probe->now_ns / UINT64_C(1000000000));
	time->tv_nsec = (long)(probe->now_ns % UINT64_C(1000000000));
	return 0;
}

int __wrap_tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out)
{
	struct deadline_probe *probe = active_probe;
	int pair[2];

	if (!probe)
		return __real_tr_tcp_connect_ipv4(address, port, out);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			 0, pair) == 0);
	probe->fd = pair[0];
	probe->peer = pair[1];
	*out = pair[0];
	if (probe->mode == PROGRESS_WRITABLE ||
	    probe->mode == PROGRESS_DETACH_RETRY) {
		probe->wait_writable = 1;
		return TR_IN_PROGRESS;
	}
	return TR_OK;
}

int __wrap_tr_tcp_finish_connect(int fd)
{
	struct deadline_probe *probe = active_probe;

	if (!probe || fd != probe->fd)
		return __real_tr_tcp_finish_connect(fd);
	probe->finish_calls++;
	return TR_OK;
}

ssize_t __wrap_send(int fd, const void *data, size_t len, int flags)
{
	struct deadline_probe *probe = active_probe;

	if (!probe || fd != probe->fd)
		return __real_send(fd, data, len, flags);
	probe->send_calls++;
	if (probe->mode == PROGRESS_INTERRUPTS && probe->send_calls <= 16U) {
		/* 限定故障脚本次数，让错误基线明确失败而不是永久挂起。 */
		probe->now_ns += UINT64_C(5000000);
		errno = EINTR;
		return -1;
	}
	if (probe->mode == PROGRESS_PARTIAL) {
		probe->now_ns += UINT64_C(10000000);
		return __real_send(fd, data, 1U, flags);
	}
	if (probe->mode == PROGRESS_EAGAIN && probe->send_calls == 1U) {
		probe->wait_writable = 1;
		errno = EAGAIN;
		return -1;
	}
	if (probe->mode == PROGRESS_FINAL_SEND || probe->mode == PROGRESS_EAGAIN) {
		probe->now_ns = TEST_START_NS + probe->elapsed_ns;
		probe->fail_clock = probe->fail_progress_clock;
	}
	return __real_send(fd, data, len, flags);
}

int __wrap_epoll_ctl(int fd, int operation, int target, struct epoll_event *event)
{
	struct deadline_probe *probe = active_probe;
	int ret = __real_epoll_ctl(fd, operation, target, event);

	if (ret == 0 && probe && target == probe->fd &&
	    operation == EPOLL_CTL_ADD)
		probe->event_token = event->data.u64;
	return ret;
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int count, int timeout)
{
	struct deadline_probe *probe = active_probe;
	int ret = __real_epoll_wait(fd, events, count, timeout);
	int i;

	for (i = 0; i < ret && probe && probe->wait_writable; ++i) {
		if (events[i].data.u64 != probe->event_token)
			continue;
		/* 真实 epoll 返回 Connector 令牌，在分派前推进时钟。 */
		probe->wait_writable = 0;
		probe->now_ns = TEST_START_NS + probe->elapsed_ns;
		probe->fail_clock = probe->fail_progress_clock;
	}
	return ret;
}

int __wrap_tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd)
{
	struct deadline_probe *probe = active_probe;

	if (probe && fd == probe->fd &&
	    probe->mode == PROGRESS_DETACH_RETRY &&
	    probe->unregister_calls++ == 0U) {
		/* 到期前已锁存的成功不得因清理重试而改变。 */
		probe->now_ns = TEST_START_NS + TEST_DELAY_NS + 1U;
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_aux_event_unregister(reactor, fd);
}

static void deadline_complete(int status, int fd, uint64_t deadline_ns,
                              void *arg)
{
	struct deadline_probe *probe = arg;
	int destroy_ret;

	assert(active_probe == probe);
	if (probe->fail_initial_clock)
		assert(deadline_ns == 0U);
	else
		assert(deadline_ns == TEST_START_NS + TEST_DELAY_NS);
	if (status == TR_OK)
		assert(fd == probe->fd);
	else
		assert(fd == -1);
	tr_socket_close(&fd);
	/* 所有路径都必须支持完成回调内销毁 Connector。 */
	destroy_ret = tr_connector_destroy(probe->connector);
	active_probe = NULL;
	pthread_mutex_lock(&probe->lock);
	probe->status = status;
	probe->destroy_ret = destroy_ret;
	if (destroy_ret == TR_OK)
		probe->connector = NULL;
	probe->callbacks++;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
}

static int start_on_owner(void *arg)
{
	static const uint8_t preface[] = { 0x43U, 0x54U, 0x52U, 0x50U };
	struct deadline_probe *probe = arg;
	int ret;

	assert(active_probe == NULL);
	active_probe = probe;
	probe->fail_clock = probe->fail_initial_clock;
	ret = tr_connector_start(probe->connector, "127.0.0.1", 1U,
				 preface, (probe->mode == PROGRESS_WRITABLE ||
					   probe->mode == PROGRESS_DETACH_RETRY) ?
					  0U : (uint32_t)sizeof(preface));
	if (ret != TR_OK)
		active_probe = NULL;
	return ret;
}

static void run_deadline_case(enum progress_mode mode, uint64_t elapsed_ns,
			     int fail_initial_clock, int fail_progress_clock,
			     int expected_status, unsigned expected_sends,
			     unsigned expected_finishes)
{
	struct tr_reactor_config reactor_config = {0};
	struct tr_connector_config connector_config = {0};
	struct tr_reactor *reactor = NULL;
	struct deadline_probe probe = {0};
	struct timespec deadline;
	int ret = 0;

	probe.mode = mode;
	probe.elapsed_ns = elapsed_ns;
	probe.now_ns = TEST_START_NS;
	probe.fd = -1;
	probe.peer = -1;
	probe.fail_initial_clock = fail_initial_clock;
	probe.fail_progress_clock = fail_progress_clock;
	assert(pthread_mutex_init(&probe.lock, NULL) == 0);
	assert(pthread_cond_init(&probe.cond, NULL) == 0);
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	connector_config.owner = reactor;
	connector_config.timeout_ms = TEST_TIMEOUT_MS;
	connector_config.complete_cb = deadline_complete;
	connector_config.callback_arg = &probe;
	assert(tr_connector_create(&connector_config, &probe.connector) == TR_OK);
	assert(tr_reactor_call(reactor, start_on_owner, &probe) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe.lock);
	while (probe.callbacks == 0U && ret == 0)
		ret = pthread_cond_timedwait(&probe.cond, &probe.lock, &deadline);
	if (probe.callbacks != 1U || probe.status != expected_status)
		fprintf(stderr, "Connector mode=%d elapsed=%llu: callbacks=%u "
			"status=%d expected=%d\n", mode,
			(unsigned long long)elapsed_ns, probe.callbacks,
			probe.status, expected_status);
	assert(probe.callbacks == 1U);
	assert(probe.status == expected_status);
	assert(probe.destroy_ret == TR_OK && probe.connector == NULL);
	assert(probe.send_calls == expected_sends);
	assert(probe.finish_calls == expected_finishes);
	assert(probe.clock_calls != 0U);
	if (mode == PROGRESS_DETACH_RETRY)
		assert(probe.unregister_calls == 2U);
	pthread_mutex_unlock(&probe.lock);
	assert(fcntl(probe.fd, F_GETFD) == -1 && errno == EBADF);
	assert(close(probe.peer) == 0);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(pthread_cond_destroy(&probe.cond) == 0);
	assert(pthread_mutex_destroy(&probe.lock) == 0);
}

int main(void)
{
	run_deadline_case(PROGRESS_INTERRUPTS, 0U, 0, 0,
			  TR_ERR_TIMEOUT, 4U, 0U);
	run_deadline_case(PROGRESS_PARTIAL, 0U, 0, 0,
			  TR_ERR_TIMEOUT, 2U, 0U);
	run_deadline_case(PROGRESS_FINAL_SEND, TEST_DELAY_NS - 1U, 0, 0,
			  TR_OK, 1U, 0U);
	run_deadline_case(PROGRESS_FINAL_SEND, TEST_DELAY_NS, 0, 0,
			  TR_ERR_TIMEOUT, 1U, 0U);
	run_deadline_case(PROGRESS_FINAL_SEND, TEST_DELAY_NS + 1U, 0, 0,
			  TR_ERR_TIMEOUT, 1U, 0U);
	run_deadline_case(PROGRESS_FINAL_SEND, 0U, 1, 0,
			  TR_ERR_SYS, 0U, 0U);
	run_deadline_case(PROGRESS_FINAL_SEND, TEST_DELAY_NS - 1U, 0, 1,
			  TR_ERR_SYS, 1U, 0U);
	run_deadline_case(PROGRESS_WRITABLE, TEST_DELAY_NS - 1U, 0, 0,
			  TR_OK, 0U, 1U);
	run_deadline_case(PROGRESS_WRITABLE, TEST_DELAY_NS, 0, 0,
			  TR_ERR_TIMEOUT, 0U, 0U);
	run_deadline_case(PROGRESS_WRITABLE, TEST_DELAY_NS + 1U, 0, 0,
			  TR_ERR_TIMEOUT, 0U, 0U);
	run_deadline_case(PROGRESS_WRITABLE, TEST_DELAY_NS - 1U, 0, 1,
			  TR_ERR_SYS, 0U, 0U);
	run_deadline_case(PROGRESS_EAGAIN, TEST_DELAY_NS - 1U, 0, 0,
			  TR_OK, 2U, 0U);
	run_deadline_case(PROGRESS_EAGAIN, TEST_DELAY_NS, 0, 0,
			  TR_ERR_TIMEOUT, 1U, 0U);
	run_deadline_case(PROGRESS_DETACH_RETRY, TEST_DELAY_NS - 1U, 0, 0,
			  TR_OK, 0U, 1U);
	return 0;
}
