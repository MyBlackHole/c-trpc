#ifndef TR_REACTOR_H
#define TR_REACTOR_H

#include <stdint.h>

#include "tr/buffer.h"
#include "tr/frame.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_reactor;

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

/* Work units: dequeued entries, timer callbacks, wire bytes and ready visits. */
struct tr_reactor_work {
	uint64_t commands;
	uint64_t completions;
	uint64_t timer_callbacks;
	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t rx_dispatches;
	uint64_t tx_dispatches;
};

struct tr_reactor_stats {
	uint64_t turns;
	struct tr_reactor_work limits;
	struct tr_reactor_work total;
	struct tr_reactor_work max_per_turn;
	/* Turns reaching a limit, not proof that more work remained. */
	struct tr_reactor_work budget_hits;
	/* epoll calls with zero / nonzero timeout, not measured sleep time. */
	uint64_t epoll_polls;
	uint64_t epoll_waits;
	/* Oldest due timer's lateness sampled at timer batch dispatch. */
	uint64_t timer_lateness_ns_max;
	/* STOP drain is outside normal turn limits and total.completions. */
	uint64_t shutdown_completions;
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

	/* Aggregate wire-byte limits per Reactor turn, shared by all connections. */
	uint32_t rx_budget_bytes;
	uint32_t tx_budget_bytes;
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
 * - TR_OK：payload ownership 转移给 Reactor；
 * - 其他返回值：payload 仍由调用方拥有。
 * payload 可以为 NULL。
 */
int tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
		    uint32_t flags, uint32_t stream_id, uint64_t message_id,
		    struct tr_buffer *payload);

/*
 * scatter/gather 发送接口，用于上层 zero-copy / copy-minimal 快路径。
 * TR_OK 时所有 buffer 的 ownership 都转移给 Reactor；失败时全部仍归调用方。
 * 同一个 buffer 指针禁止在 payloads 中重复出现。
 */
int tr_reactor_sendv(struct tr_conn_handle connection, uint16_t type,
		     uint32_t flags, uint32_t stream_id, uint64_t message_id,
		     struct tr_buffer *const *payloads, uint32_t payload_count);

/*
 * 带单条消息 DATA frame payload 上限的发送接口，供 Channel 完成能力协商后使用。
 * max_frame_payload_len 必须非 0，且不能超过所属 Reactor 的配置上限。
 */
int tr_reactor_sendv_limited(struct tr_conn_handle connection, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id,
			     struct tr_buffer *const *payloads,
			     uint32_t payload_count,
			     uint32_t max_frame_payload_len);

/*
 * 替换一个存活 connection 的回调，主要供 Channel 等上层使用。
 * 传入 NULL 可禁用对应回调。
 *
 * 外部线程调用时，本函数同步提交 SET_HANDLER command，并等待 Reactor owner
 * thread 应用 frame_cb/event_cb/callback_arg 这一组状态后再返回。
 * 如果已经位于 Reactor owner thread，则直接修改当前 connection。
 */
int tr_reactor_set_handler(struct tr_conn_handle connection,
			   tr_reactor_frame_cb frame_cb,
			   tr_reactor_event_cb event_cb, void *callback_arg);

/*
 * 等待 Reactor thread 完成调用前已经进入执行阶段的 callback 和 command。
 * 这是生命周期屏障（quiescence barrier）：通常先禁用 callback，再调用本函数，
 * 最后才允许释放 callback owner 的状态。
 *
 * 禁止从 Reactor thread 自身调用，否则会形成自等待。
 */
int tr_reactor_quiesce(struct tr_reactor *reactor);

/* 获取当前 slot state 快照，用于 connection replacement 和诊断。 */
int tr_reactor_get_connection_state(struct tr_conn_handle connection,
				    enum tr_connection_state *out);

/* 无锁读取 connection 热路径计数器快照，仅用于诊断/监控。 */
int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out);

/*
 * Coherent snapshot of completed turns since creation, excluding the current
 * turn and nested direct owner calls. Running reactors serialize the read via
 * synchronous owner call; owner callbacks read directly. Also valid before
 * start and after stop has joined. A race with stop may return TR_ERR_CLOSED;
 * failures leave *out unchanged. The caller must keep reactor alive.
 */
int tr_reactor_get_stats(struct tr_reactor *reactor, struct tr_reactor_stats *out);

/* 获取 Reactor 不可变 wire limits 快照，供上层能力协商使用。 */
int tr_reactor_get_limits(struct tr_reactor *reactor,
			  struct tr_reactor_limits *out);

int tr_reactor_resume_rx(struct tr_conn_handle connection);
int tr_reactor_close(struct tr_conn_handle connection);
/* 主动使存活 connection 失败，并通过 connection callback 上报 status。 */
int tr_reactor_abort(struct tr_conn_handle connection, int status);

int tr_reactor_stop(struct tr_reactor *reactor);
void tr_reactor_destroy(struct tr_reactor *reactor);

#ifdef __cplusplus
}
#endif

#endif
