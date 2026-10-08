#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

enum epoll_del_fault_mode {
	EPOLL_DEL_FAULT_NONE = 0,
	EPOLL_DEL_FAULT_EIO
};

static enum epoll_del_fault_mode epoll_del_fault;
static int epoll_del_fault_fd = -1;
static int observed_epoll_fd = -1;
static int observed_del_errno;
static unsigned callback_hits;

int __real_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event);

int __wrap_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event)
{
	enum epoll_del_fault_mode fault;
	int ret;

	if (op == EPOLL_CTL_ADD)
		observed_epoll_fd = epfd;

	if (op == EPOLL_CTL_DEL && fd == epoll_del_fault_fd &&
	    epoll_del_fault != EPOLL_DEL_FAULT_NONE) {
		fault = epoll_del_fault;
		epoll_del_fault = EPOLL_DEL_FAULT_NONE;
		epoll_del_fault_fd = -1;

		assert(fault == EPOLL_DEL_FAULT_EIO);
		errno = EIO;
		return -1;
	}

	ret = __real_epoll_ctl(epfd, op, fd, event);
	if (op == EPOLL_CTL_DEL && ret < 0)
		observed_del_errno = errno;
	return ret;
}

static void inject_del_fault(int fd, enum epoll_del_fault_mode fault)
{
	assert(epoll_del_fault == EPOLL_DEL_FAULT_NONE);
	assert(fd >= 0);
	epoll_del_fault_fd = fd;
	epoll_del_fault = fault;
}

static int make_event_fd(void)
{
	int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

	assert(fd >= 0);
	return fd;
}

static void source_callback(int fd, uint32_t events, void *arg)
{
	unsigned *hits = (unsigned *)arg;

	(void)fd;
	(void)events;
	if (hits)
		(*hits)++;
}

static int teardown_mark(void *arg)
{
	unsigned *count = (unsigned *)arg;

	(*count)++;
	return TR_OK;
}

static int publish_fail(void *arg)
{
	(void)arg;
	return TR_ERR_STATE;
}

static void test_listener_detach_failure_preserves_publication(
	struct tr_reactor *reactor)
{
	unsigned teardown_calls = 0U;
	int fd = make_event_fd();
	int other = make_event_fd();

	assert(tr_reactor_listener_register(
		       reactor, fd, source_callback, &callback_hits) == TR_OK);

	inject_del_fault(fd, EPOLL_DEL_FAULT_EIO);
	assert(tr_reactor_listener_unregister_call(
		       reactor, source_callback, &callback_hits,
		       teardown_mark, &teardown_calls) == TR_ERR_SYS);
	assert(teardown_calls == 0U);

	/* 解绑失败时必须保留原回调发布。 */
	assert(tr_reactor_listener_register(
		       reactor, other, source_callback, &callback_hits) ==
	       TR_ERR_STATE);

	assert(tr_reactor_listener_unregister_call(
		       reactor, source_callback, &callback_hits,
		       teardown_mark, &teardown_calls) == TR_OK);
	assert(teardown_calls == 1U);

	assert(tr_reactor_listener_register(
		       reactor, other, source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_listener_unregister(reactor, other) == TR_OK);

	assert(close(fd) == 0);
	assert(close(other) == 0);
}

static void test_peer_detach_failure_preserves_publication(
	struct tr_reactor *reactor)
{
	int fd = make_event_fd();
	int other = make_event_fd();

	assert(tr_reactor_peer_event_register(
		       reactor, fd, source_callback, &callback_hits) == TR_OK);

	inject_del_fault(fd, EPOLL_DEL_FAULT_EIO);
	assert(tr_reactor_peer_event_unregister(reactor, fd) == TR_ERR_SYS);

	assert(tr_reactor_peer_event_register(
		       reactor, other, source_callback, &callback_hits) ==
	       TR_ERR_STATE);

	assert(tr_reactor_peer_event_unregister(reactor, fd) == TR_OK);
	assert(tr_reactor_peer_event_register(
		       reactor, other, source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_peer_event_unregister(reactor, other) == TR_OK);

	assert(close(fd) == 0);
	assert(close(other) == 0);
}

static void test_aux_detach_failure_preserves_publication(
	struct tr_reactor *reactor)
{
	int fd = make_event_fd();

	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) == TR_OK);

	inject_del_fault(fd, EPOLL_DEL_FAULT_EIO);
	assert(tr_reactor_aux_event_unregister(reactor, fd) == TR_ERR_SYS);

	/* 原 fd 仍处于发布状态，因此拒绝重复注册。 */
	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) ==
	       TR_ERR_STATE);

	assert(tr_reactor_aux_event_unregister(reactor, fd) == TR_OK);
	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_aux_event_unregister(reactor, fd) == TR_OK);

	assert(close(fd) == 0);
}

enum fd_identity_case {
	FD_IDENTITY_CLOSED = 0,
	FD_IDENTITY_DEV_NULL,
	FD_IDENTITY_PIPE
};

struct fd_identity_callback_context {
	struct tr_reactor *reactor;
	pthread_mutex_t lock;
	pthread_cond_t condition;
	int retained_fd;
	int callback_fd;
	int unregister_status;
	int unregister_errno;
	ssize_t drain_result;
	unsigned callback_count;
};

static void fd_identity_callback(int fd, uint32_t events, void *arg)
{
	struct fd_identity_callback_context *context =
		(struct fd_identity_callback_context *)arg;
	uint64_t value;
	int unregister_status;
	int unregister_errno;
	ssize_t drain_result;

	(void)events;
	unregister_status = tr_reactor_aux_event_unregister(context->reactor, fd);
	unregister_errno = errno;
	do {
		drain_result = read(context->retained_fd, &value, sizeof(value));
	} while (drain_result < 0 && errno == EINTR);

	pthread_mutex_lock(&context->lock);
	context->callback_fd = fd;
	context->unregister_status = unregister_status;
	context->unregister_errno = unregister_errno;
	context->drain_result = drain_result;
	context->callback_count++;
	pthread_cond_signal(&context->condition);
	pthread_mutex_unlock(&context->lock);
}

static void test_lost_fd_identity_does_not_confirm_detach(
	enum fd_identity_case identity_case)
{
	struct tr_reactor *reactor = NULL;
	struct fd_identity_callback_context context;
	struct epoll_event event;
	struct timespec deadline;
	uint64_t one = 1U;
	uint64_t old_token;
	int registered_fd = make_event_fd();
	int retained_fd = dup(registered_fd);
	int replacement_fd = -1;
	int replacement_peer = -1;
	int expected_errno;
	int event_count;
	int wait_result;

	assert(retained_fd >= 0);
	memset(&context, 0, sizeof(context));
	context.retained_fd = retained_fd;
	assert(pthread_mutex_init(&context.lock, NULL) == 0);
	assert(pthread_cond_init(&context.condition, NULL) == 0);
	if (identity_case == FD_IDENTITY_DEV_NULL) {
		replacement_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
		assert(replacement_fd >= 0);
		expected_errno = EPERM;
	} else if (identity_case == FD_IDENTITY_PIPE) {
		int pipe_fds[2];

		assert(pipe(pipe_fds) == 0);
		replacement_fd = pipe_fds[0];
		replacement_peer = pipe_fds[1];
		expected_errno = ENOENT;
	} else {
		expected_errno = EBADF;
	}

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	context.reactor = reactor;
	assert(tr_reactor_aux_event_register(
		       reactor, registered_fd, EPOLLIN,
		       fd_identity_callback, &context) == TR_OK);
	assert(observed_epoll_fd >= 0);

	/* 保留旧 open file description 的 dup，再关闭并可选复用原 fd 数值。 */
	assert(close(registered_fd) == 0);
	if (replacement_fd >= 0) {
		assert(dup2(replacement_fd, registered_fd) == registered_fd);
		assert(close(replacement_fd) == 0);
		replacement_fd = -1;
	}
	assert(write(retained_fd, &one, sizeof(one)) == (ssize_t)sizeof(one));
	event_count = epoll_wait(observed_epoll_fd, &event, 1, 100);
	assert(event_count == 1);
	old_token = event.data.u64;

	observed_del_errno = 0;
	assert(tr_reactor_aux_event_unregister(reactor, registered_fd) ==
	       TR_ERR_SYS);
	assert(observed_del_errno == expected_errno);

	/* 失败的 DEL 没有证明旧注册已移除，原 callback publication 必须保留。 */
	assert(tr_reactor_aux_event_register(
		       reactor, registered_fd, EPOLLIN,
		       source_callback, &callback_hits) == TR_ERR_STATE);
	event_count = epoll_wait(observed_epoll_fd, &event, 1, 100);
	assert(event_count == 1);
	assert(event.data.u64 == old_token);

	/* 旧事件仍能进入 Reactor；失败的解绑必须继续保活原 callback arg。 */
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	assert(pthread_mutex_lock(&context.lock) == 0);
	while (context.callback_count == 0U) {
		wait_result = pthread_cond_timedwait(
			&context.condition, &context.lock, &deadline);
		assert(wait_result == 0);
	}
	assert(context.callback_count == 1U);
	assert(context.callback_fd == registered_fd);
	assert(context.unregister_status == TR_ERR_SYS);
	assert(context.unregister_errno == expected_errno);
	assert(context.drain_result == (ssize_t)sizeof(one));
	assert(pthread_mutex_unlock(&context.lock) == 0);

	assert(tr_reactor_aux_event_register(
		       reactor, registered_fd, EPOLLIN,
		       source_callback, &callback_hits) == TR_ERR_STATE);

	/* 销毁 Reactor 关闭 epoll fd 后，才释放源对象和 callback arg。 */
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(pthread_cond_destroy(&context.condition) == 0);
	assert(pthread_mutex_destroy(&context.lock) == 0);
	assert(close(retained_fd) == 0);
	if (replacement_peer >= 0)
		assert(close(replacement_peer) == 0);
	if (identity_case != FD_IDENTITY_CLOSED)
		assert(close(registered_fd) == 0);
}

static void test_register_publish_reports_rollback_barrier_failure(
	struct tr_reactor *reactor)
{
	int fd = make_event_fd();
	int other = make_event_fd();

	inject_del_fault(fd, EPOLL_DEL_FAULT_EIO);
	assert(tr_reactor_listener_register_publish(
		       reactor, fd, source_callback, &callback_hits,
		       publish_fail, NULL) == TR_ERR_SYS);

	/* 回滚屏障失败时保留发布状态，不能伪装为事务已完整撤销。 */
	assert(tr_reactor_listener_register(
		       reactor, other, source_callback, &callback_hits) ==
	       TR_ERR_STATE);
	assert(tr_reactor_listener_unregister(reactor, fd) == TR_OK);

	assert(close(fd) == 0);
	assert(close(other) == 0);
}

int main(void)
{
	struct tr_reactor *reactor = NULL;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(reactor != NULL);

	test_listener_detach_failure_preserves_publication(reactor);
	test_peer_detach_failure_preserves_publication(reactor);
	test_aux_detach_failure_preserves_publication(reactor);
	test_register_publish_reports_rollback_barrier_failure(reactor);

	assert(callback_hits == 0U);
	assert(epoll_del_fault == EPOLL_DEL_FAULT_NONE);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	test_lost_fd_identity_does_not_confirm_detach(FD_IDENTITY_CLOSED);
	test_lost_fd_identity_does_not_confirm_detach(FD_IDENTITY_DEV_NULL);
	test_lost_fd_identity_does_not_confirm_detach(FD_IDENTITY_PIPE);
	return 0;
}
