#ifndef TR_REACTOR_INTERNAL_H
#define TR_REACTOR_INTERNAL_H

#include <stddef.h>
#include "reactor.h"

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

/*
 * 仅当当前线程正在 Reactor 所有者循环中执行时为真。
 * 生命周期 stop/destroy 绝不能等待或释放自己的所有者线程。
 */
int tr_reactor_in_owner_context(void);

/*
 * 可选的原始连接前导数据门控。
 * Reactor 在允许普通 TRP1 解析器输入前精确读取 byte_count 个字节，
 * 因此前导数据之后的字节会保留在 socket 接收队列中，不会被过度读取。
 *
 * feed 在 Reactor 所有者上运行，必须短小且非阻塞。
 * 最后一个分片完成全部路由/处理器设置后必须设置 *done = 1。
 * 无论成功还是连接关闭/出错，release 都恰好调用一次。
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
 * 仅限所有者使用的立即关闭辅助接口，用于复合所有者状态转换。
 * 与公开 close/abort API 不同，这些接口不会入队命令：
 * 在调用方继续修改相关状态前，连接已经完成退役。
 */
int tr_reactor_close_on_owner(struct tr_conn_handle connection);
int tr_reactor_abort_on_owner(struct tr_conn_handle connection, int status);

typedef void (*tr_reactor_listener_cb)(int fd, uint32_t events, void *arg);
typedef void (*tr_reactor_peer_event_cb)(int fd, uint32_t events, void *arg);

/*
 * 在 Reactor 的 epoll 集合中注册一个 listener/事件源。
 * 回调运行在 Reactor 所有者上，必须短小且非阻塞。
 *
 * register/unregister 是同步生命周期屏障；
 * unregister 只有在正在执行的回调完成后才返回。
 */
int tr_reactor_listener_register(struct tr_reactor *reactor, int fd,
				 tr_reactor_listener_cb callback, void *arg);
int tr_reactor_listener_unregister(struct tr_reactor *reactor, int fd);

int tr_reactor_peer_event_register(struct tr_reactor *reactor, int fd,
				   tr_reactor_peer_event_cb callback, void *arg);
int tr_reactor_peer_event_unregister(struct tr_reactor *reactor, int fd);

typedef void (*tr_reactor_aux_event_cb)(int fd, uint32_t events, void *arg);

/*
 * 有界辅助 fd 事件源，供所有者本地状态机使用，例如多个并发的非阻塞连接。
 * 每次注册使用独立代次令牌；槽位复用后，过期的 epoll 事件会被安全丢弃。
 *
 * Reactor 只观察 fd，不取得 fd 所有权，也不会主动关闭。
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
 * Reactor 本地定时器能力。register/arm/unregister 都串行化到所有者；
 * 回调在 Reactor 线程上执行，必须短小且非阻塞。
 */
int tr_reactor_timer_register(struct tr_reactor *reactor,
			      tr_reactor_timer_cb callback, void *arg,
			      struct tr_reactor_timer_handle *out);
int tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			 uint64_t deadline_ns);
int tr_reactor_timer_unregister(struct tr_reactor_timer_handle handle);

#endif
