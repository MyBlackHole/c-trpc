#ifndef TR_REACTOR_INTERNAL_H
#define TR_REACTOR_INTERNAL_H

#include <stddef.h>
#include "tr/reactor.h"

/*
 * 提交一个 worker completion 回 Reactor owner。
 *
 * fn 必须是非阻塞的短任务，禁止在回调里执行磁盘、网络、KMS 等阻塞工作。
 * TR_OK 表示 arg 的生命周期责任已经转移给 bounded completion queue；
 * 提交失败时仍由调用方负责。
 */
int tr_reactor_complete(struct tr_reactor *reactor, void (*fn)(void *arg),
			void *arg);

/*
 * 在 Reactor owner thread 执行一个短小、非阻塞的同步操作，并把 fn 的
 * 返回值传回调用方。owner thread 内调用时直接执行，避免自等待。
 *
 * arg 只在本函数返回前被访问，因此调用方可以安全传递栈上 request。
 */
int tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *arg),
		    void *arg);

/*
 * Optional raw connection preface gate. The Reactor reads exactly byte_count
 * bytes before allowing normal TRP1 parser input, so bytes following the
 * preface remain in the socket receive queue and are never over-read.
 *
 * feed runs on the owner Reactor and must be short/non-blocking. On the final
 * fragment it must set *done = 1 after completing any routing/handler setup.
 * release is called exactly once on success or connection close/error.
 */
typedef int (*tr_reactor_preface_feed_cb)(
	struct tr_conn_handle connection, const uint8_t *data, size_t len,
	int *done, void *arg);
typedef void (*tr_reactor_preface_release_cb)(void *arg);

struct tr_reactor_preface_handler {
	uint32_t byte_count;
	tr_reactor_preface_feed_cb feed;
	tr_reactor_preface_release_cb release;
	void *arg;
};

int tr_reactor_adopt_fd_prefaced_on_owner(
	struct tr_reactor *reactor, int fd,
	const struct tr_reactor_preface_handler *preface,
	struct tr_conn_handle *out);

/*
 * Owner-only immediate close helpers for compound owner state transitions.
 * Unlike the public close/abort APIs, these do not enqueue a command: the
 * connection is retired before the caller continues mutating related state.
 */
int tr_reactor_close_on_owner(struct tr_conn_handle connection);
int tr_reactor_abort_on_owner(struct tr_conn_handle connection, int status);

typedef void (*tr_reactor_listener_cb)(int fd, uint32_t events, void *arg);
typedef void (*tr_reactor_peer_event_cb)(int fd, uint32_t events, void *arg);

/*
 * Register one listener/event source in the Reactor epoll set. The callback
 * runs on the Reactor owner and must be short/non-blocking.
 *
 * register/unregister are synchronous lifecycle barriers. unregister returns
 * only after an in-flight callback has completed.
 */
int tr_reactor_listener_register(struct tr_reactor *reactor, int fd,
				 tr_reactor_listener_cb callback, void *arg);
int tr_reactor_listener_unregister(struct tr_reactor *reactor, int fd);

int tr_reactor_peer_event_register(struct tr_reactor *reactor, int fd,
				   tr_reactor_peer_event_cb callback, void *arg);
int tr_reactor_peer_event_unregister(struct tr_reactor *reactor, int fd);

typedef uint64_t (*tr_reactor_timer_cb)(void *arg, uint64_t now_ns);

struct tr_reactor_timer_handle {
	struct tr_reactor *reactor;
	uint32_t slot;
	uint32_t generation;
};

/*
 * Reactor-local timer capability. register/arm/unregister 都串行化到 owner；
 * callback 在 Reactor thread 上执行，必须短小且非阻塞。
 */
int tr_reactor_timer_register(struct tr_reactor *reactor,
			      tr_reactor_timer_cb callback, void *arg,
			      struct tr_reactor_timer_handle *out);
int tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			 uint64_t deadline_ns);
int tr_reactor_timer_unregister(struct tr_reactor_timer_handle handle);

#endif
