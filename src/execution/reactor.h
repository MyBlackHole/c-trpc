#ifndef TR_REACTOR_H
#define TR_REACTOR_H

#include <stdint.h>

#include "buffer.h"
#include "../transport/protocol/frame.h"
#include "../observability.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_reactor;
struct tr_memory_budget;

struct tr_reactor_limits {
	uint32_t max_payload_len;
};

#define TR_REACTOR_MAX_TX_SLICES 4U

struct tr_conn_handle {
	struct tr_reactor *reactor;
	uint32_t slot;
	uint32_t generation;
};

enum tr_connection_state {
	TR_CONN_FREE = 0,
	TR_CONN_RESERVED,
	TR_CONN_ACTIVE,
	TR_CONN_CLOSED,
	TR_CONN_ERROR
};

struct tr_connection_stats {
	enum tr_connection_state state;

	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t rx_frames;
	uint64_t tx_frames;
	uint64_t recv_eagain;
	uint64_t send_eagain;
	uint64_t rx_pauses;

	uint64_t last_rx_activity_ns;
	uint64_t last_tx_activity_ns;

	uint32_t tx_queued_items;
	int rx_paused;
	int tx_wait_writable;
};

/* 工作单位：出队条目、定时器回调、线协议字节与就绪访问次数。 */
struct tr_reactor_work {
	uint64_t commands;
	uint64_t completions;
	uint64_t timer_callbacks;
	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t rx_dispatches;
	uint64_t tx_dispatches;
};

struct tr_reactor_command_observation {
	uint64_t enqueued;
	uint64_t full_events;
};

struct tr_reactor_stats {
	uint64_t turns;
	struct tr_reactor_work limits;
	struct tr_reactor_work total;
	struct tr_reactor_work max_per_turn;
	/* 达到限制的轮次数，不证明仍有更多工作。 */
	struct tr_reactor_work budget_hits;
	/* 零或非零超时的 epoll 调用次数，不是测得的睡眠时间。 */
	uint64_t epoll_polls;
	uint64_t epoll_waits;
	/* 在定时器批次分派时采样最早到期定时器的延迟。 */
	uint64_t timer_lateness_ns_max;
	/* STOP 排空不受普通轮次限制，也不计入 total.completions。 */
	uint64_t shutdown_completions;

	/* 有界队列压力快照。
	 * 命令 full_events：生产者首次遇到满环形队列的压力事件；异步命令
	 * 可能立即返回 TR_AGAIN，同步所有者请求或 STOP 则进入容量等待。
	 * 完成项 full_events：生产者遇到满环形队列并进入容量等待的事件。
	 */
	struct tr_queue_observation command_queue;
	struct tr_queue_observation completion_queue;

	/* 命令队列压力的生产者侧归属统计。 */
	struct tr_reactor_command_observation command_send;
	struct tr_reactor_command_observation command_resume_rx;
	struct tr_reactor_command_observation command_call;
	struct tr_reactor_command_observation command_other;

	/* 有界传输池。 */
	struct tr_pool_observation rx_buffer_pool;
	struct tr_pool_observation tx_item_pool;
	struct tr_pool_observation control_tx_item_pool;

	/* 仅在启用 TR_OBSERVABILITY_TIMING 时填充。 */
	uint32_t observability_flags;
	uint64_t busy_ns;
	uint64_t poll_ns;
	struct tr_latency_histogram turn_busy_ns;
};

enum tr_frame_disposition { TR_FRAME_RELEASE = 0, TR_FRAME_TAKE_OWNERSHIP = 1 };

enum tr_connection_event { TR_CONN_EVENT_CLOSED = 1, TR_CONN_EVENT_ERROR = 2 };

typedef enum tr_frame_disposition (*tr_reactor_frame_cb)(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg);

typedef void (*tr_reactor_event_cb)(struct tr_conn_handle connection,
				    enum tr_connection_event event, int status,
				    void *arg);

struct tr_reactor_config {
	uint32_t max_connections;
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;

	uint32_t rx_buffer_count;
	uint32_t rx_buffer_size;
	uint32_t max_payload_len;

	/* 每个反应器轮次的线协议字节总上限，由所有连接共享。 */
	uint32_t rx_budget_bytes;
	uint32_t tx_budget_bytes;

	/* TR_OBSERVABILITY_* 标志；计时需要显式启用，以保护热路径。 */
	uint32_t observability_flags;

	/* 内部 RuntimeShard 记账所有者；NULL 保留独立模式。 */
	struct tr_memory_budget *memory_budget;
};

int tr_reactor_create(const struct tr_reactor_config *config,
		      tr_reactor_frame_cb frame_cb,
		      tr_reactor_event_cb event_cb, void *callback_arg,
		      struct tr_reactor **out);

/* 开启一个新的反应器运行轮次。
 *
 * create() 后以及每次成功 stop() 后反应器都处于非运行状态；同一个反应器
 * 可以再次启动。每次启动都重新开放命令与完成项准入代次，
 * 旧轮次中被停止操作唤醒的等待者不得进入新轮次。
 *
 * 停止会关闭并回收所有存活连接，因此旧 tr_conn_handle 不跨轮次保持
 * 存活。反应器本地已注册的定时器、监听器或辅助事件源属于反应器对象本身，
 * 未显式注销时会保留，并在下一轮次所有者完整发布后恢复分派。
 */
int tr_reactor_start(struct tr_reactor *reactor);

/* 所有权：返回 TR_OK 后 fd 由反应器接管；失败时仍由调用方拥有。 */
int tr_reactor_adopt_fd(struct tr_reactor *reactor, int fd,
			struct tr_conn_handle *out);

/* 所有权：
 * - TR_OK：payload 所有权转移给反应器；
 * - 其他返回值：payload 仍由调用方拥有。
 * payload 可以为 NULL。
 */
int tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
		    uint32_t flags, uint32_t stream_id, uint64_t message_id,
		    struct tr_buffer *payload);

/* 分散聚集发送接口，用于上层零拷贝或最少拷贝快速路径。
 * TR_OK 时所有缓冲区的所有权都转移给反应器；失败时全部仍归调用方。
 * 同一个缓冲区指针禁止在 payloads 中重复出现。
 */
int tr_reactor_sendv(struct tr_conn_handle connection, uint16_t type,
		     uint32_t flags, uint32_t stream_id, uint64_t message_id,
		     struct tr_buffer *const *payloads, uint32_t payload_count);

/* 带单条消息 DATA 帧负载上限的发送接口，供 Channel 完成能力协商后使用。
 * max_frame_payload_len 必须非 0，且不能超过所属反应器的配置上限。
 */
int tr_reactor_sendv_limited(struct tr_conn_handle connection, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id,
			     struct tr_buffer *const *payloads,
			     uint32_t payload_count,
			     uint32_t max_frame_payload_len);

/* 替换一个存活连接的回调，主要供 Channel 等上层使用。
 * 传入 NULL 可禁用对应回调。
 *
 * 外部线程调用时，本函数同步提交 SET_HANDLER 命令，并等待反应器所有者
 * 线程应用 frame_cb/event_cb/callback_arg 这一组状态后再返回。
 * 如果已经位于反应器所有者线程，则直接修改当前连接。
 */
int tr_reactor_set_handler(struct tr_conn_handle connection,
			   tr_reactor_frame_cb frame_cb,
			   tr_reactor_event_cb event_cb, void *callback_arg);

/* 等待反应器线程完成调用前已经进入执行阶段的回调和命令。
 * 这是生命周期静止屏障：通常先禁用回调，再调用本函数，
 * 最后才允许释放回调所有者的状态。
 *
 * 禁止从反应器线程自身调用，否则会形成自等待。
 * 如果所有者在屏障执行前因致命 epoll 错误退出，返回 TR_ERR_SYS，
 * 而不是错误地报告静止已成立；仅被队列关闭拒绝的请求返回 TR_ERR_CLOSED。
 */
int tr_reactor_quiesce(struct tr_reactor *reactor);

/* 获取当前槽位状态快照，用于连接替换和诊断。 */
int tr_reactor_get_connection_state(struct tr_conn_handle connection,
				    enum tr_connection_state *out);

/* 无锁读取连接热路径计数器快照，仅用于诊断或监控。 */
int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out);

/* 自创建以来已完成轮次的一致快照，不含当前轮次与嵌套直接所有者调用。
 * 运行中的反应器通过同步所有者调用串行化读取；所有者回调直接读取。
 * 启动前和停止已回收线程后也有效。与停止竞争时可能返回 TR_ERR_CLOSED；
 * 失败保持 *out 不变。调用者必须保持 reactor 存活。
 */
int tr_reactor_get_stats(struct tr_reactor *reactor, struct tr_reactor_stats *out);

/* 获取反应器不可变的线协议限制快照，供上层能力协商使用。 */
int tr_reactor_get_limits(struct tr_reactor *reactor,
			  struct tr_reactor_limits *out);

int tr_reactor_resume_rx(struct tr_conn_handle connection);
int tr_reactor_close(struct tr_conn_handle connection);
/* 主动使存活连接失败，并通过连接回调上报 status。 */
int tr_reactor_abort(struct tr_conn_handle connection, int status);

/* 关闭当前运行轮次并等待所有者线程退出。成功后可再次 start()。
 * 停止是外部同步生命周期屏障，禁止从反应器所有者回调内调用。
 *
 * STOP 命令一旦被接受，后续即使 pthread_join() 暂时失败，也不会重新提交
 * 第二个 STOP。对象保持调用者拥有的终止状态；再次 stop()/destroy()
 * 会重试同一所有者线程的回收屏障，直到成功后才允许开始新轮次。
 *
 * epoll_wait 发生非 EINTR 的致命错误时，所有者自行关闭两个队列的准入，
 * 让已接受但未执行的同步请求返回 TR_ERR_SYS，归还其余命令所有权，
 * 执行已接受的完成项，并以 TR_CONN_EVENT_ERROR/TR_ERR_SYS 通知连接。
 * 这一异常退出同样进入仅回收线程阶段；stop() 负责等待并回收线程，不会
 * 向已失去消费者的队列再次发送 STOP。完成 stop() 后仍可重新 start()。
 */
int tr_reactor_stop(struct tr_reactor *reactor);
int tr_reactor_destroy(struct tr_reactor *reactor);

#ifdef __cplusplus
}
#endif

#endif
