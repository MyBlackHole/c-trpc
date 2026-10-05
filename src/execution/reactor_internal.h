#ifndef TR_REACTOR_INTERNAL_H
#define TR_REACTOR_INTERNAL_H

#include <stddef.h>
#include "reactor.h"

struct tr_memory_budget;

/*
 * 提交一个 worker completion 回 Reactor owner。
 *
 * fn 必须是非阻塞的短任务，禁止在回调里执行磁盘、网络、KMS 等阻塞工作。
 * 跨线程提交遇到 bounded completion queue 满时会在 queue-local condition 上
 * 等待容量，不做 sched_yield 自旋，也不允许 worker 绕过 owner。
 *
 * TR_OK 表示 arg 的生命周期责任已经转移给 completion queue/owner；
 * stop/close 或 wait primitive 失败时 ownership 仍由调用方负责。
 */
int tr_reactor_complete(struct tr_reactor *reactor, void (*fn)(void *arg),
			void *arg);

/*
 * 运行中：与 tr_reactor_call() 相同，由 owner 执行。
 * 完全 stopped：在 ctl_lock 下由调用线程直接执行 fn；此时不存在 owner callback。
 * 正在 stop（started && !accepting）：返回 TR_ERR_CLOSED，不允许 caller 越过
 * teardown barrier 直接修改 owner state。
 *
 * 仅用于 teardown/config publication 这类“stopped 时直接修改也安全”的内部状态；
 * fn 不得在 stopped direct path 中重新进入 Reactor lifecycle API。
 */
int tr_reactor_call_or_stopped(
	struct tr_reactor *reactor, int (*fn)(void *arg), void *arg);

/*
 * 在 Reactor owner thread 执行一个短小、非阻塞的同步操作，并把 fn 的
 * 返回值传回调用方。owner thread 内调用时直接执行，避免自等待。
 *
 * arg 只在本函数返回前被访问，因此调用方可以安全传递栈上 request。
 */
int tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *arg),
		    void *arg);

/* Internal resource-domain capability; NULL for standalone/unbudgeted Reactor. */
struct tr_memory_budget *
tr_reactor_memory_budget(struct tr_reactor *reactor);

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

typedef void (*tr_reactor_aux_event_cb)(int fd, uint32_t events, void *arg);

/*
 * Bounded auxiliary fd event sources，供 owner-local 状态机使用，例如多个并发的
 * nonblocking connect。每个 registration 使用独立 generation token，slot 复用后
 * stale epoll event 会被安全丢弃。
 *
 * Reactor 只观察 fd，不取得 fd ownership，也不会主动 close。
 * events 只接受 EPOLLIN/EPOLLOUT；ERR/HUP 始终自动加入。
 */
int tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
				  uint32_t events,
				  tr_reactor_aux_event_cb callback, void *arg);
int tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd);

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
