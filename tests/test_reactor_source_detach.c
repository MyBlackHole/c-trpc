#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

enum epoll_del_fault_mode {
	EPOLL_DEL_FAULT_NONE = 0,
	EPOLL_DEL_FAULT_EIO,
	EPOLL_DEL_FAULT_REMOVE_THEN_ENOENT
};

static enum epoll_del_fault_mode epoll_del_fault;
static int epoll_del_fault_fd = -1;
static unsigned callback_hits;

int __real_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event);

int __wrap_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event)
{
	enum epoll_del_fault_mode fault;

	if (op != EPOLL_CTL_DEL || fd != epoll_del_fault_fd ||
	    epoll_del_fault == EPOLL_DEL_FAULT_NONE)
		return __real_epoll_ctl(epfd, op, fd, event);

	fault = epoll_del_fault;
	epoll_del_fault = EPOLL_DEL_FAULT_NONE;
	epoll_del_fault_fd = -1;

	if (fault == EPOLL_DEL_FAULT_REMOVE_THEN_ENOENT) {
		assert(__real_epoll_ctl(epfd, op, fd, event) == 0);
		errno = ENOENT;
		return -1;
	}

	assert(fault == EPOLL_DEL_FAULT_EIO);
	errno = EIO;
	return -1;
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

	/* Failed detach must keep the exact callback publication occupied. */
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

	/* Exact fd remains published, so duplicate registration is rejected. */
	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) ==
	       TR_ERR_STATE);

	assert(tr_reactor_aux_event_unregister(reactor, fd) == TR_OK);
	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_aux_event_unregister(reactor, fd) == TR_OK);

	assert(close(fd) == 0);
}

static void test_confirmed_absent_enoent_clears_publication(
	struct tr_reactor *reactor)
{
	int fd = make_event_fd();

	assert(tr_reactor_listener_register(
		       reactor, fd, source_callback, &callback_hits) == TR_OK);

	/*
	 * Remove the real kernel registration first, then report ENOENT. This
	 * models the only safe ENOENT interpretation: the old source is absent.
	 */
	inject_del_fault(fd, EPOLL_DEL_FAULT_REMOVE_THEN_ENOENT);
	assert(tr_reactor_listener_unregister(reactor, fd) == TR_OK);

	assert(tr_reactor_listener_register(
		       reactor, fd, source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_listener_unregister(reactor, fd) == TR_OK);
	assert(close(fd) == 0);
}

static void test_closed_fd_is_confirmed_absent(struct tr_reactor *reactor)
{
	int fd = make_event_fd();
	int old_fd = fd;
	int replacement;

	assert(tr_reactor_aux_event_register(
		       reactor, fd, EPOLLIN, source_callback, &callback_hits) == TR_OK);

	/*
	 * close() automatically removes the open file description from epoll.
	 * The subsequent DEL sees EBADF, which is still a proven detached source.
	 */
	assert(close(fd) == 0);
	fd = -1;
	assert(tr_reactor_aux_event_unregister(reactor, old_fd) == TR_OK);

	replacement = make_event_fd();
	assert(tr_reactor_aux_event_register(
		       reactor, replacement, EPOLLIN,
		       source_callback, &callback_hits) == TR_OK);
	assert(tr_reactor_aux_event_unregister(reactor, replacement) == TR_OK);
	assert(close(replacement) == 0);
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

	/*
	 * The rollback barrier failed, so publication must remain visible rather
	 * than pretending that the failed transaction is fully unwound.
	 */
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
	test_confirmed_absent_enoent_clears_publication(reactor);
	test_closed_fd_is_confirmed_absent(reactor);
	test_register_publish_reports_rollback_barrier_failure(reactor);

	assert(callback_hits == 0U);
	assert(epoll_del_fault == EPOLL_DEL_FAULT_NONE);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	return 0;
}
