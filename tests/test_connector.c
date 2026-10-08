#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/connector_internal.h"
#include "../src/io/socket.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

struct connector_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_connector *connector;
	int callbacks;
	int status;
	int destroy_ret;
	int pause_unregister_failure;
	int unregister_failure_seen;
	int allow_unregister_failure_return;
	unsigned unregister_completed;
	int cancel_on_unregister_failure;
	int nested_cancel_status;
};

enum fake_connect_mode {
	FAKE_CONNECT_DISABLED = 0,
	FAKE_CONNECT_PIPE,
	FAKE_CONNECT_FULL_SOCKET
};

static atomic_int fake_connect_mode;
static atomic_int fail_aux_unregister_once;
static atomic_uint aux_unregister_attempts;
static _Atomic(struct connector_test_ctx *) unregister_test_ctx;
static int fake_connect_peer = -1;

int __real_tr_tcp_connect_ipv4(
	const char *address, uint16_t port, int *out_fd);
int __real_tr_reactor_aux_event_unregister(
	struct tr_reactor *reactor, int fd);

int __wrap_tr_tcp_connect_ipv4(
	const char *address, uint16_t port, int *out_fd)
{
	int mode = atomic_load(&fake_connect_mode);

	if (mode == FAKE_CONNECT_DISABLED)
		return __real_tr_tcp_connect_ipv4(address, port, out_fd);

	(void)address;
	(void)port;
	assert(out_fd != NULL);
	if (mode == FAKE_CONNECT_PIPE) {
		int pipefd[2];

		assert(pipe2(pipefd, O_NONBLOCK | O_CLOEXEC) == 0);
		*out_fd = pipefd[0];
		fake_connect_peer = pipefd[1];
		return TR_IN_PROGRESS;
	}
	if (mode == FAKE_CONNECT_FULL_SOCKET) {
		char filler[4096] = {0};
		int socketfd[2];

		assert(socketpair(
			       AF_UNIX,
			       SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			       0, socketfd) == 0);
		for (;;) {
			ssize_t n = send(
				socketfd[0], filler, sizeof(filler),
				MSG_DONTWAIT | MSG_NOSIGNAL);

			if (n > 0)
				continue;
			if (n < 0 && errno == EINTR)
				continue;
			assert(n < 0 &&
			       (errno == EAGAIN || errno == EWOULDBLOCK));
			break;
		}
		for (;;) {
			struct pollfd writable = {
				.fd = socketfd[0],
				.events = POLLOUT,
				.revents = 0
			};
			int poll_ret = poll(&writable, 1U, 0);

			assert(poll_ret >= 0);
			if (poll_ret == 0 || !(writable.revents & POLLOUT))
				break;
			for (;;) {
				ssize_t n = send(
					socketfd[0], filler, 1U,
					MSG_DONTWAIT | MSG_NOSIGNAL);

				if (n > 0)
					break;
				if (n < 0 && errno == EINTR)
					continue;
				assert(n < 0 &&
				       (errno == EAGAIN || errno == EWOULDBLOCK));
				break;
			}
		}
		*out_fd = socketfd[0];
		fake_connect_peer = socketfd[1];
		return TR_OK;
	}
	assert(0 && "未知的测试连接模式");
	return TR_ERR_STATE;
}

static void record_unregister_completion(
	struct connector_test_ctx *ctx)
{
	if (!ctx)
		return;
	pthread_mutex_lock(&ctx->lock);
	ctx->unregister_completed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

int __wrap_tr_reactor_aux_event_unregister(
	struct tr_reactor *reactor, int fd)
{
	struct connector_test_ctx *ctx;
	int cancel_on_failure = 0;
	int ret;

	atomic_fetch_add(&aux_unregister_attempts, 1U);
	ctx = atomic_load(&unregister_test_ctx);
	if (atomic_exchange(&fail_aux_unregister_once, 0)) {
		if (ctx) {
			pthread_mutex_lock(&ctx->lock);
			ctx->unregister_failure_seen = 1;
			pthread_cond_broadcast(&ctx->cond);
			while (ctx->pause_unregister_failure &&
			       !ctx->allow_unregister_failure_return)
				pthread_cond_wait(&ctx->cond, &ctx->lock);
			cancel_on_failure =
				ctx->cancel_on_unregister_failure;
			pthread_mutex_unlock(&ctx->lock);
		}
		if (cancel_on_failure) {
			int cancel_status = tr_connector_cancel(ctx->connector);

			pthread_mutex_lock(&ctx->lock);
			ctx->nested_cancel_status = cancel_status;
			pthread_cond_broadcast(&ctx->cond);
			pthread_mutex_unlock(&ctx->lock);
		}
		record_unregister_completion(ctx);
		return TR_ERR_SYS;
	}
	ret = __real_tr_reactor_aux_event_unregister(reactor, fd);
	record_unregister_completion(ctx);
	return ret;
}

static void connector_destroy_from_completion(int status, int fd, void *arg)
{
	struct connector_test_ctx *ctx =
		(struct connector_test_ctx *)arg;
	int destroy_ret;

	tr_socket_close(&fd);
	destroy_ret = tr_connector_destroy(ctx->connector);

	pthread_mutex_lock(&ctx->lock);
	ctx->status = status;
	ctx->destroy_ret = destroy_ret;
	if (destroy_ret == TR_OK)
		ctx->connector = NULL;
	ctx->callbacks++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_connector_callback(struct connector_test_ctx *ctx)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (ctx->callbacks == 0 && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->callbacks == 1);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_unregister_failure(struct connector_test_ctx *ctx)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (!ctx->unregister_failure_seen && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->unregister_failure_seen);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_unregister_count(
	struct connector_test_ctx *ctx, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (ctx->unregister_completed < target && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->unregister_completed >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void connector_record_completion(int status, int fd, void *arg)
{
	struct connector_test_ctx *ctx =
		(struct connector_test_ctx *)arg;

	tr_socket_close(&fd);
	pthread_mutex_lock(&ctx->lock);
	ctx->status = status;
	ctx->callbacks++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void drain_fake_connect_peer(void)
{
	char buffer[4096];

	assert(fake_connect_peer >= 0);
	for (;;) {
		ssize_t n = recv(fake_connect_peer, buffer, sizeof(buffer), 0);

		if (n > 0)
			continue;
		if (n < 0 && errno == EINTR)
			continue;
		if (n == 0)
			return;
		assert(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
		return;
	}
}

static int connector_owner_barrier(void *arg)
{
	(void)arg;
	return TR_OK;
}

static void close_fake_connect_peer(void)
{
	if (fake_connect_peer >= 0) {
		assert(close(fake_connect_peer) == 0);
		fake_connect_peer = -1;
	}
}

static void test_completion_may_destroy_connector(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;
	uint16_t port = 0U;
	int listener = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_tcp_listen_ipv4(
		       "127.0.0.1", 0U, 8, &listener, &port) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 5000U;
	connector_config.tcp_nodelay = 1;
	connector_config.complete_cb = connector_destroy_from_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	/*
	 * completion 可能在 tr_connector_start() 内同步执行，也可能稍后由 EPOLLOUT 触发。
	 * 两种情况下 callback 都可能在 Connector 调用栈退出前销毁对象，因此之后不能再解引用。
	 */
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", port, NULL, 0U) == TR_OK);
	wait_connector_callback(&ctx);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status == TR_OK);
	assert(ctx.destroy_ret == TR_OK);
	assert(ctx.connector == NULL);
	pthread_mutex_unlock(&ctx.lock);

	tr_socket_close(&listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void connector_noop_completion(int status, int fd, void *arg)
{
	(void)status;
	(void)arg;
	tr_socket_close(&fd);
}

static void test_inactive_cancel_and_destroy_are_idempotent(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct tr_connector *connector = NULL;

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 1000U;
	connector_config.complete_cb = connector_noop_completion;
	assert(tr_connector_create(&connector_config, &connector) == TR_OK);

	assert(tr_connector_cancel(connector) == TR_OK);
	assert(tr_connector_cancel(connector) == TR_OK);
	assert(tr_connector_destroy(connector) == TR_OK);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static void test_timeout_detach_failure_retries(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 20U;
	connector_config.complete_cb = connector_destroy_from_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	atomic_store(&fake_connect_mode, FAKE_CONNECT_PIPE);
	atomic_store(&fail_aux_unregister_once, 1);
	atomic_store(&aux_unregister_attempts, 0U);

	/*
	 * fake connect 使用 pipe read-end，并只监听 EPOLLOUT，因此连接不会产生
	 * progress event，只能由 timeout 驱动。第一次 timeout detach 注入失败后，
	 * timer 必须自动重排；第二次成功后 completion 只能发布一次。
	 */
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", 1U, NULL, 0U) == TR_OK);
	wait_connector_callback(&ctx);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status == TR_ERR_TIMEOUT);
	assert(ctx.destroy_ret == TR_OK);
	assert(ctx.callbacks == 1);
	assert(ctx.connector == NULL);
	pthread_mutex_unlock(&ctx.lock);
	assert(atomic_load(&aux_unregister_attempts) == 2U);
	assert(atomic_load(&fail_aux_unregister_once) == 0);

	atomic_store(&fake_connect_mode, FAKE_CONNECT_DISABLED);
	close_fake_connect_peer();

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_timeout_status_survives_writable_event(void)
{
	static const uint8_t preface[] = {0x43U, 0x54U, 0x52U, 0x50U};
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	ctx.pause_unregister_failure = 1;

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 30U;
	connector_config.complete_cb = connector_record_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	atomic_store(&unregister_test_ctx, &ctx);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_FULL_SOCKET);
	atomic_store(&fail_aux_unregister_once, 1);
	atomic_store(&aux_unregister_attempts, 0U);
	/* 真实 socketpair 先被填满，确保 preface 发送进入 EPOLLOUT 等待。 */
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", 1U,
		       preface, (uint32_t)sizeof(preface)) == TR_OK);
	wait_unregister_failure(&ctx);

	/*
	 * 暂停在第一次 detach 失败处，先排空对端令 fd 可写，再放行 owner。
	 * 如事件回调仍推进状态机，这里会错误地以 TR_OK 覆盖超时结果。
	 */
	drain_fake_connect_peer();
	pthread_mutex_lock(&ctx.lock);
	ctx.allow_unregister_failure_return = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_connector_callback(&ctx);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status == TR_ERR_TIMEOUT);
	assert(ctx.callbacks == 1);
	pthread_mutex_unlock(&ctx.lock);
	assert(atomic_load(&aux_unregister_attempts) == 2U);
	assert(tr_connector_destroy(ctx.connector) == TR_OK);
	ctx.connector = NULL;

	atomic_store(&unregister_test_ctx, NULL);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_DISABLED);
	close_fake_connect_peer();
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_completed_timeout_rejects_late_cancel(void)
{
	static const uint8_t preface[] = {0x43U, 0x54U, 0x52U, 0x50U};
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	ctx.cancel_on_unregister_failure = 1;

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 30U;
	connector_config.complete_cb = connector_record_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	atomic_store(&unregister_test_ctx, &ctx);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_FULL_SOCKET);
	atomic_store(&fail_aux_unregister_once, 1);
	atomic_store(&aux_unregister_attempts, 0U);
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", 1U,
		       preface, (uint32_t)sizeof(preface)) == TR_OK);
	wait_connector_callback(&ctx);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status == TR_ERR_TIMEOUT);
	assert(ctx.callbacks == 1);
	assert(ctx.nested_cancel_status == TR_ERR_STATE);
	pthread_mutex_unlock(&ctx.lock);
	assert(atomic_load(&aux_unregister_attempts) == 2U);
	assert(tr_connector_destroy(ctx.connector) == TR_OK);
	ctx.connector = NULL;

	atomic_store(&unregister_test_ctx, NULL);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_DISABLED);
	close_fake_connect_peer();
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_cancel_wins_over_later_writable_event(void)
{
	static const uint8_t preface[] = {0x43U, 0x54U, 0x52U, 0x50U};
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 5000U;
	connector_config.complete_cb = connector_record_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	atomic_store(&unregister_test_ctx, &ctx);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_FULL_SOCKET);
	atomic_store(&fail_aux_unregister_once, 1);
	atomic_store(&aux_unregister_attempts, 0U);
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", 1U,
		       preface, (uint32_t)sizeof(preface)) == TR_OK);
	assert(tr_connector_cancel(ctx.connector) == TR_ERR_SYS);

	/* 取消首次提交后才令 fd 可写，后续事件只能重试取消清理。 */
	drain_fake_connect_peer();
	wait_unregister_count(&ctx, 2U);
	assert(tr_reactor_call(
		       reactor, connector_owner_barrier, NULL) == TR_OK);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.callbacks == 0);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_connector_destroy(ctx.connector) == TR_OK);
	ctx.connector = NULL;

	atomic_store(&unregister_test_ctx, NULL);
	atomic_store(&fake_connect_mode, FAKE_CONNECT_DISABLED);
	close_fake_connect_peer();
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

int main(void)
{
	test_completion_may_destroy_connector();
	test_inactive_cancel_and_destroy_are_idempotent();
	test_timeout_detach_failure_retries();
	test_timeout_status_survives_writable_event();
	test_completed_timeout_rejects_late_cancel();
	test_cancel_wins_over_later_writable_event();
	return 0;
}
