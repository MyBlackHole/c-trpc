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

/* 工作量单位：出队条目、定时器回调、线协议字节数以及就绪项访问次数。 */
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
	/* 达到限制的轮次；它不证明当时一定还有剩余工作。 */
	struct tr_reactor_work budget_hits;
	/* timeout 为 0/非 0 的 epoll 调用次数，不表示实际睡眠时长。 */
	uint64_t epoll_polls;
	uint64_t epoll_waits;
	/* 分发定时器批次时采样最早到期定时器的延迟。 */
	uint64_t timer_lateness_ns_max;
	/* STOP 排空不计入普通轮次限制，也不计入 total.completions。 */
	uint64_t shutdown_completions;

	/*
	 * 有界队列压力快照。
	 * command full_events：生产者首次遇到环形队列已满的压力事件；异步命令
	 * 可能立即返回 TR_AGAIN，同步所有者请求/STOP 则进入容量等待。
	 * completion full_events：生产者遇到环形队列已满并进入容量等待的事件。
	 */
	struct tr_queue_observation command_queue;
	struct tr_queue_observation completion_queue;

	/* 命令队列压力的生产者侧归因统计。 */
	struct tr_reactor_command_observation command_send;
	struct tr_reactor_command_observation command_resume_rx;
	struct tr_reactor_command_observation command_call;
	struct tr_reactor_command_observation command_other;

	/* 有界 Transport 资源池。 */
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

	/* 每个 Reactor 轮次的聚合线协议字节上限，由全部连接共享。 */
	uint32_t rx_budget_bytes;
	uint32_t tx_budget_bytes;

	/* TR_OBSERVABILITY_* 标志；计时能力需要显式启用，以保护热路径。 */
	uint32_t observability_flags;

	/* 内部 RuntimeShard 记账所有者；NULL 表示保持独立运行模式。 */
	struct tr_memory_budget *memory_budget;
};

int tr_reactor_create(const struct tr_reactor_config *config,
		      tr_reactor_frame_cb frame_cb,
		      tr_reactor_event_cb event_cb, void *callback_arg,
		      struct tr_reactor **out);
int tr_reactor_start(struct tr_reactor *reactor);

/* 所有权：返回 TR_OK 后 fd 由 Reactor 接管；失败时仍由调用方拥有。 */
int tr_reactor_adopt_fd(struct tr_reactor *reactor, int fd,
			struct tr_conn_handle *out);

/*
 * 所有权：
 * - TR_OK：载荷所有权转移给 Reactor；
 * - 其他返回值：载荷仍由调用方拥有。
 * 载荷可以为 NULL。
 */
int tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
		    uint32_t flags, uint32_t stream_id, uint64_t message_id,
		    struct tr_buffer *payload);

/*
 * 分散/聚集发送接口，用于上层零拷贝或最少拷贝快路径。
 * 返回 TR_OK 时所有 Buffer 的所有权都转移给 Reactor；失败时全部仍归调用方。
 * 同一个 Buffer 指针禁止在 payloads 中重复出现。
 */
int tr_reactor_sendv(struct tr_conn_handle connection, uint16_t type,
		     uint32_t flags, uint32_t stream_id, uint64_t message_id,
		     struct tr_buffer *const *payloads, uint32_t payload_count);

/*
 * 带单条消息 DATA 帧载荷上限的发送接口，供 Channel 完成能力协商后使用。
 * max_frame_payload_len 必须非 0，且不能超过所属 Reactor 的配置上限。
 */
int tr_reactor_sendv_limited(struct tr_conn_handle connection, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id,
			     struct tr_buffer *const *payloads,
			     uint32_t payload_count,
			     uint32_t max_frame_payload_len);

/*
 * 替换一个存活连接的回调，主要供 Channel 等上层使用。
 * 传入 NULL 可禁用对应回调。
 *
 * 外部线程调用时，本函数同步提交 SET_HANDLER 命令，
 * 并等待 Reactor 所有者线程应用 frame_cb/event_cb/callback_arg 这一组状态后再返回。
 * 如果已经位于 Reactor 所有者线程，则直接修改当前连接。
 */
int tr_reactor_set_handler(struct tr_conn_handle connection,
			   tr_reactor_frame_cb frame_cb,
			   tr_reactor_event_cb event_cb, void *callback_arg);

/*
 * 等待 Reactor 线程完成调用前已经进入执行阶段的回调和命令。
 * 这是生命周期静止屏障：通常先禁用回调，再调用本函数，
 * 最后才允许释放回调所有者的状态。
 *
 * 禁止从 Reactor 线程自身调用，否则会形成自等待。
 */
int tr_reactor_quiesce(struct tr_reactor *reactor);

/* 获取当前槽位状态快照，用于连接替换和诊断。 */
int tr_reactor_get_connection_state(struct tr_conn_handle connection,
				    enum tr_connection_state *out);

/* 无锁读取连接热路径计数器快照，仅用于诊断和监控。 */
int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out);

/*
 * 获取自创建以来已经完成轮次的一致性快照，不包含当前轮次和嵌套的直接所有者调用。
 * Reactor 运行时通过同步所有者调用串行化读取；所有者回调中直接读取。
 * 在启动前以及 stop 已完成 join 后同样有效。与 stop 竞争时可能返回 TR_ERR_CLOSED；
 * 失败时保持 *out 不变。调用期间调用方必须保证 Reactor 仍然存活。
 */
int tr_reactor_get_stats(struct tr_reactor *reactor, struct tr_reactor_stats *out);

/* 获取 Reactor 不可变的线协议限制快照，供上层能力协商使用。 */
int tr_reactor_get_limits(struct tr_reactor *reactor,
			  struct tr_reactor_limits *out);

int tr_reactor_resume_rx(struct tr_conn_handle connection);
int tr_reactor_close(struct tr_conn_handle connection);
/* 主动使存活连接失败，并通过连接回调上报状态。 */
int tr_reactor_abort(struct tr_conn_handle connection, int status);

int tr_reactor_stop(struct tr_reactor *reactor);
void tr_reactor_destroy(struct tr_reactor *reactor);

#ifdef __cplusplus
}
#endif

#endif
