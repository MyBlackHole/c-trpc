#ifndef TR_REACTOR_INTERNAL_H
#define TR_REACTOR_INTERNAL_H

#include <stddef.h>
#include "reactor.h"
#include "tr/peer_identity.h"

struct tr_peer_identity_snapshot;

/* 提交一个工作线程完成项回反应器所有者。
 *
 * fn 必须是非阻塞的短任务，禁止在回调里执行磁盘、网络、KMS 等阻塞工作。
 * 跨线程提交遇到有界完成队列满时会在队列本地条件变量上
 * 等待容量，不做 sched_yield 自旋，也不允许工作线程绕过所有者。
 *
 * TR_OK 表示 arg 的生命周期责任已经转移给完成队列或所有者；
 * 停止、关闭或等待原语失败时所有权仍由调用方负责。
 */
int tr_reactor_complete(struct tr_reactor *reactor, void (*fn)(void *arg),
			void *arg);

/* 运行中：与 tr_reactor_call() 相同，由所有者执行。
 * 完全停止：在 ctl_lock 下由调用线程直接执行 fn；此时不存在所有者回调。
 * 正在停止（started && !accepting）：返回 TR_ERR_CLOSED，不允许调用者越过
 * 拆除屏障直接修改所有者状态。
 *
 * 仅用于拆除或配置发布这类“停止时直接修改也安全”的内部状态；
 * fn 不得在已停止的直接路径中重新进入反应器生命周期 API。
 */
int tr_reactor_call_or_stopped(
	struct tr_reactor *reactor, int (*fn)(void *arg), void *arg);

/* 在反应器所有者线程执行一个短小、非阻塞的同步操作，并把 fn 的
 * 返回值传回调用方。所有者线程内调用时直接执行，避免自等待。
 * owner 回调等待另一 Reactor 时，若该等待会闭合跨 owner 等待环，返回
 * TR_ERR_STATE，且不把 fn 提交到目标队列。
 *
 * arg 只在本函数返回前被访问，因此调用方可以安全传递栈上请求。
 */
int tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *arg),
		    void *arg);

/* Compute the fixed owner memory charged when constructing a Reactor with
 * this configuration. Applies the same defaults and overflow checks as the
 * constructor, without reserving from a budget or creating resources.
 */
int tr_reactor_memory_requirement(const struct tr_reactor_config *config,
                                  uint64_t *out);

/* TLS 快照轮换屏障；准入门禁仅阻止新的 TLS adopt，不影响现有会话。 */
int tr_reactor_tls_admission_set(struct tr_reactor *reactor, int paused);
int tr_reactor_tls_close_generation(struct tr_reactor *reactor,
                                    uint64_t generation);
int tr_reactor_tls_close_before_generation(struct tr_reactor *reactor,
                                           uint64_t generation);
int tr_reactor_tls_generation_on_owner(struct tr_conn_handle connection,
                                       uint64_t *out_generation);
int tr_reactor_tls_profile_retain_on_owner(
        struct tr_conn_handle connection,
        struct tr_tls_openssl_profile **out_profile);

/* 仅当当前线程在反应器所有者循环内执行时为真。
 * 生命周期停止或销毁绝不能等待或释放其自身所有者线程。
 */
int tr_reactor_in_owner_context(void);

/* 可选的原始连接前导门禁。反应器在允许正常 TRP1 解析器输入前，
 * 精确读取 byte_count 字节，因此前导之后的字节保留在套接字接收队列，
 * 绝不超读。
 *
 * feed 在所属反应器上运行，必须短小且非阻塞。在最后一个分片中，
 * 它必须在完成路由或处理函数设置后设置 *done = 1。最后一片可返回
 * TR_IN_PROGRESS 暂停普通读取；之后必须在同一 owner 调用 resume。
 * 成功或连接关闭、出错时，release 恰好调用一次。
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
int tr_reactor_adopt_tls_fd_prefaced_on_owner(
        struct tr_reactor *reactor, int fd,
        struct tr_tls_openssl_profile *profile, uint64_t handshake_deadline_ns,
        const struct tr_reactor_preface_handler *preface,
        struct tr_conn_handle *out);
/* 仅限所属 Reactor owner。仅完整前导返回 TR_IN_PROGRESS 后可恢复；
 * TR_OK 释放前导并恢复读取，负状态关闭精确连接并释放前导。 */
int tr_reactor_preface_resume_on_owner(struct tr_conn_handle connection,
                                       int status);

/* TLS Client 用的认证门禁：认证回调成功后，才通过 TLS 发出原始 route
 * preface；回调拿到的身份视图只在本次调用期间借用有效。 */
typedef int (*tr_reactor_tls_authenticated_cb)(
        struct tr_conn_handle connection,
        const struct tr_peer_identity_view *peer_identity, void *arg);
typedef int (*tr_reactor_tls_preface_sent_cb)(struct tr_conn_handle connection,
                                              void *arg);

int tr_reactor_adopt_tls_fd_outbound_prefaced_on_owner(
        struct tr_reactor *reactor, int fd,
        struct tr_tls_openssl_profile *profile, uint64_t handshake_deadline_ns,
        const uint8_t *preface, uint32_t preface_len,
        tr_reactor_tls_authenticated_cb authenticated_cb,
        void *authenticated_arg, tr_reactor_tls_preface_sent_cb preface_sent_cb,
        void *preface_sent_arg, struct tr_conn_handle *out);

/* 仅限所有者的立即关闭辅助函数，用于复合所有者状态转换。
 * 与公开关闭或中止 API 不同，这些函数不将命令入队：
 * 调用者继续修改相关状态前，连接已经退役。
 */
int tr_reactor_close_on_owner(struct tr_conn_handle connection);
int tr_reactor_abort_on_owner(struct tr_conn_handle connection, int status);
/* 仅由显式协议排空后的外部生命周期线程调用；TLS close_notify 完成或
 * 有界关闭失败后返回，普通非 TLS 连接不使用此辅助函数。owner 回调调用时，
 * 若整个关闭等待会闭合跨 owner 等待环，则在提交关闭命令前返回 TR_ERR_STATE。 */
int tr_reactor_close_graceful(struct tr_conn_handle connection);

/* 协议层对精确连接代次施加/解除接收背压，不等待命令容量。
 * 与缓冲池 RESUME_RX 独立；释放只调度接收，不在本次调用中分派帧。
 * 只允许该反应器所有者调用，连接退役后自动清除。 */
int tr_reactor_set_rx_hold_on_owner(struct tr_conn_handle connection, int held);

/* 仅限所有者的终止控制发送。GOAWAY 使用一个 Connection 内嵌的
 * 仅含头部发送槽位，独立于普通控制发送池或命令容量。
 */
int tr_reactor_send_goaway_on_owner(struct tr_conn_handle connection,
                                    uint32_t last_accepted_peer_stream_id);

/* 所有者登记一次发送排空观察，不分配发送项或命令。TR_OK 固定当前命令
 * 前沿；此前命令全部处理、该精确连接的 TX 队列及两个终止预留都为空后，
 * 由原 event_cb 收到一次 TX_DRAINED。重复待办登记不延后前沿。连接关闭
 * 则由原 CLOSED/ERROR 完成通知，不再另发 TX_DRAINED。
 *
 * 此事件只证明本机发送资源已归还及字节已交给内核，不证明对端已收到。
 * 使用者须先停止新的业务发送并保持处理器发布，直到事件或显式解绑。
 * 回调不会在本次登记栈内执行，不建立新线程或轮询定时器。
 */
int tr_reactor_notify_tx_drained_on_owner(struct tr_conn_handle connection);

/* 所有者登记/取消一次控制发送容量观察。每连接固定一个等待项；池和命令
 * 容量可用时在调度点通过既有 event_cb 发送 CONTROL_TX_READY，先摘除再
 * 回调。通知不预留空位，竞争失败者须重新登记。重复登记幂等且不改变 FIFO。
 * 回调共用 TX 分派预算；关闭或替换处理器自动取消，不会在本函数内回调。
 * 使用者必须在释放回调参数之前取消观察或成功解绑处理器。 */
int tr_reactor_watch_control_tx_on_owner(struct tr_conn_handle connection,
                                         int enabled);

/* 所有者登记/取消一次 DATA 发送项池观察。每连接固定一个等待项；容量恢复后
 * 在有界调度点通过原 event_cb 收到一次 DATA_TX_READY。通知先摘队列且不预留项；
 * 与普通 TX 和控制池通知共用每轮 TX 分派预算。关闭或替换处理器自动取消。 */
int tr_reactor_watch_data_tx_on_owner(struct tr_conn_handle connection,
                                      int enabled);

/* Channel 压缩分帧器在所属 owner 上发送一个不可再分的 DATA 帧。
 * FIRST/LAST 由调用方维护整个逻辑消息的边界；成功时接管 payload。
 * pool_exhausted 可为 NULL；非 NULL 时先清零，仅在 DATA TX 项池耗尽返回
 * TR_AGAIN 时设为 1，其余失败原因保持为 0。 */
int tr_reactor_send_data_frame_on_owner(struct tr_conn_handle connection,
                                        uint32_t flags, uint32_t stream_id,
                                        uint64_t message_id,
                                        struct tr_buffer *payload,
                                        int *pool_exhausted);

/* 单流终止的专用内嵌发送槽，与 GOAWAY 和普通控制池均独立。
 * 仅所有者可调用；TR_OK 接受精确连接代次/流/原因的 FIFO 屏障，
 * 相同在途项幂等，其他流返回 AGAIN。帧前保留全部已接受命令和 DATA。
 */
int tr_reactor_send_stream_reset_on_owner(struct tr_conn_handle connection,
                                          uint32_t stream_id, int status);

/* 未受理 OPEN 的单向拒绝，与 RESET 共用上述固定预留和 FIFO。
 * 不消耗流槽、普通控制池或命令容量；原因固定为资源不足。 */
int tr_reactor_send_stream_refused_on_owner(struct tr_conn_handle connection,
                                            uint32_t stream_id);

typedef void (*tr_reactor_listener_cb)(int fd, uint32_t events, void *arg);
typedef void (*tr_reactor_peer_event_cb)(int fd, uint32_t events, void *arg);

/* 在反应器 epoll 集中注册一个监听器事件源。回调由所有者执行，必须短小且非阻塞。
 *
 * fd 由调用方持有，反应器只借用它。调用方必须保持注册时的 fd 数值和
 * 打开文件描述不变，直到注销返回 TR_OK；dup 可以存在，但
 * 不能因此提前关闭或复用注册用的 fd。
 *
 * 注册与注销是同步生命周期屏障。只有 EPOLL_CTL_DEL 成功才会清除
 * callback/arg 发布。DEL 的任何错误都返回错误并保留发布；在重试成功或
 * 反应器销毁前，调用方必须保持回调参数有效。
 */
int tr_reactor_listener_register(struct tr_reactor *reactor, int fd,
				 tr_reactor_listener_cb callback, void *arg);

/* 将监听器事件源注册与调用者的所有者状态发布合并成一个生命周期事务。
 *
 * 运行中：两步在同一个反应器所有者处理轮次完成；
 * 完全停止：两步在 ctl_lock 下完成，start() 不能插入其间；
 * 正在停止：返回 TR_ERR_CLOSED，不执行任何发布。
 *
 * publish 在事件源已加入 epoll、但尚不可能分派的串行化区间执行。
 * publish 返回失败时反应器会先撤销事件源注册；若撤销屏障本身失败，
 * 函数返回该生命周期错误并保留事件源发布，调用方不得提前释放 callback_arg，
 * 必须先重试注销。此时 on_retained 会在相同串行化区间执行，用于让调用方
 * 记录仍由反应器发布的资源所有权；out_retained 会报告错误返回时事件源是否仍
 * 发布。没有提供 on_retained 的调用方仍必须根据 out_retained 自行保留资源。
 * publish 必须短小、非阻塞；已停止的直接路径中不得重入反应器生命周期 API。
 */
int tr_reactor_listener_register_publish(
	struct tr_reactor *reactor, int fd,
	tr_reactor_listener_cb callback, void *arg,
	int (*publish)(void *arg), void *publish_arg,
	void (*on_retained)(void *arg), void *retained_arg,
	int *out_retained);

/* 将监听器事件源解绑与调用者拆除状态合并为一个生命周期事务。
 *
 * callback/arg 用于确认当前反应器监听器确实属于调用方；事件源已不存在时
 * fn 仍会执行，因此拆除可以保持幂等。运行中由所有者执行；完全停止
 * 时在 ctl_lock 下直接执行；正在停止时返回 TR_ERR_CLOSED。
 *
 * 与 register_publish 不同，fn 失败不会恢复已经撤销的事件源：拆除一旦跨过
 * 准入屏障就不可逆。fn 必须短小、非阻塞，且已停止的直接路径中
 * 不得重入反应器生命周期 API。
 */
int tr_reactor_listener_unregister_call(
	struct tr_reactor *reactor,
	tr_reactor_listener_cb callback, void *arg,
	int (*fn)(void *arg), void *fn_arg);

int tr_reactor_listener_unregister(struct tr_reactor *reactor, int fd);

/* 对端事件源遵循与监听器相同的借用 fd 和注销生命周期契约。 */
int tr_reactor_peer_event_register(struct tr_reactor *reactor, int fd,
				   tr_reactor_peer_event_cb callback, void *arg);
int tr_reactor_peer_event_unregister(struct tr_reactor *reactor, int fd);

typedef void (*tr_reactor_aux_event_cb)(int fd, uint32_t events, void *arg);

/* 有界辅助 fd 事件源，供所有者本地状态机使用，例如多个并发的
 * 非阻塞连接。每个注册使用独立代次令牌，槽位复用后
 * 陈旧 epoll 事件会被安全丢弃。
 *
 * 反应器只借用 fd，不取得所有权，也不会主动关闭。调用方必须保持
 * 注册时的 fd 数值和打开文件描述不变，直到注销返回 TR_OK；
 * dup 可以存在，但不能提前关闭或复用注册用的 fd。
 * 注销的 EPOLL_CTL_DEL 失败时保留 callback/arg 发布，调用方须保持 arg
 * 有效，直到重试成功或反应器销毁。
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

/* 反应器本地定时器能力。注册、设置定时与注销都串行化到所有者；
 * 回调在反应器线程上执行，必须短小且非阻塞。
 */
int tr_reactor_timer_register(struct tr_reactor *reactor,
			      tr_reactor_timer_cb callback, void *arg,
			      struct tr_reactor_timer_handle *out);
int tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			 uint64_t deadline_ns);
int tr_reactor_timer_unregister(struct tr_reactor_timer_handle handle);

/* 精确检查所属反应器，不能仅用“当前是某个 owner”代替。 */
int tr_reactor_in_owner_context_for(const struct tr_reactor *reactor);

/* 返回所属连接的借用身份快照；TLS 尚未完成认证时返回 TR_AGAIN。
 * 仅可在该连接 Reactor owner 上调用，调用方须 retain 后跨越 owner 回调。
 */
int tr_reactor_peer_identity_snapshot_on_owner(
        struct tr_conn_handle connection,
        struct tr_peer_identity_snapshot **out);

/* 库内停止回收租约，零初始化、不可复制。成功取得要求 stop/join 已成功，
 * 并阻止 start/destroy，直到 release。允许相同租约重试；不延长 Reactor
 * 存储寿命，不允许并发外层生命周期/释放。回收线程不被伪装为协议 owner。
 * 子源必须成功解绑、通知者/回调已静默后才能释放租约。 */
struct tr_reactor_recovery_guard {
	struct tr_reactor *reactor;
};
int tr_reactor_recovery_acquire(struct tr_reactor *reactor,
                                struct tr_reactor_recovery_guard *guard);
int tr_reactor_recovery_release(struct tr_reactor_recovery_guard *guard);

#endif
