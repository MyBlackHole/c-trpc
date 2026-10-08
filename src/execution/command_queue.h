#ifndef TR_COMMAND_QUEUE_H
#define TR_COMMAND_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum tr_command_type {
	TR_CMD_ADOPT_FD = 1,
	TR_CMD_SEND,
	TR_CMD_RESUME_RX,
	TR_CMD_CLOSE,
	TR_CMD_ABORT,
	TR_CMD_SET_HANDLER,
	TR_CMD_QUIESCE,
	TR_CMD_CALL,
	TR_CMD_STOP
};

struct tr_tx_item;
struct tr_reactor_sync;
struct tr_reactor_handler_request;

struct tr_command {
	uint16_t type;
	uint16_t reserved;
	uint32_t slot;
	uint32_t generation;
	/* Queue-assigned FIFO sequence; 0 means not yet enqueued. */
	uint64_t sequence;

	union {
		struct {
			int fd;
		} adopt;

		struct {
			struct tr_tx_item *item;
		} send;

		struct {
			int status;
		} abort;

		struct {
			struct tr_reactor_handler_request *request;
		} handler;

		struct {
			struct tr_reactor_sync *sync;
		} quiesce;


		struct {
			int (*fn)(void *arg);
			void *arg;
			struct tr_reactor_sync *sync;
		} call;
	} u;
};

struct tr_command_queue {
	pthread_mutex_t lock;
	pthread_cond_t not_full;
	struct tr_command *items;
	uint32_t capacity;
	uint32_t head;
	uint32_t tail;
	uint32_t count;
	uint32_t peak_count;
	uint64_t full_events;
	uint64_t next_sequence;
	uint64_t wait_generation;
	uint32_t waiters;
	int wait_accepting;

	/* Producer-side attribution, updated under the existing queue lock. */
	uint64_t pushed_send;
	uint64_t full_send;
	uint64_t pushed_resume_rx;
	uint64_t full_resume_rx;
	uint64_t pushed_call;
	uint64_t full_call;
	uint64_t pushed_other;
	uint64_t full_other;

	int wake_pending;

	/*
	 * 固定的停止专用槽位，不占普通 ring 的 capacity/count/peak_count。
	 * STOP 仅在普通 ring 排空后弹出；stop_closed 保持到下一 epoch open，
	 * 防止已经弹出 STOP 后又接受新命令。所有字段均由 lock 保护。
	 */
	struct tr_command stop_command;
	int stop_pending;
	int stop_closed;
};

int tr_command_queue_init(struct tr_command_queue *queue, uint32_t capacity);
void tr_command_queue_destroy(struct tr_command_queue *queue);

/*
 * 当调用方需要唤醒 Reactor eventfd 时设置 need_wake。
 * TR_AGAIN 表示有界 queue 已满，此时 command ownership 没有发生转移。
 */
int tr_command_queue_push(struct tr_command_queue *queue,
			  const struct tr_command *command, int *need_wake);

/*
 * 同步 owner request 的 capacity wait admission。
 *
 * close 控制 push_wait() waiter，普通 push() 在 STOP 提交前仍为有界立即入队。
 * STOP 提交会关闭整个 epoch；消费 STOP 后，open 才重开普通及等待准入。
 * generation 由生命周期 owner 在 ctl_lock 保护下取样，防止 stop
 * 关闭 waiter 后旧 request 重新混入新的 admission epoch。
 */
int tr_command_queue_wait_open(struct tr_command_queue *queue);
void tr_command_queue_wait_close(struct tr_command_queue *queue);
uint64_t tr_command_queue_wait_generation(struct tr_command_queue *queue);

/*
 * TR_OK：command ownership 已转移给 queue。
 * TR_ERR_CLOSED：expected_generation 已关闭/失效，ownership 未转移。
 * queue 满时在 not_full 上睡眠，不做 sched_yield 自旋。
 */
int tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t expected_generation, int *need_wake);

/*
 * 仅供 STOP 使用的固定容量保留通道，不分配内存、不等待普通 ring 空位。
 * 保留原内部函数名，但此入口已经不再是 capacity waiter：不能在持有
 * Reactor ctl_lock 时等待 owner 消费，因为 owner 回调自身也可能需要该锁。
 *
 * 在 queue->lock 下关闭本 epoch 的新命令，STOP 排在所有已接受命令之后；
 * pop_batch 先取普通 ring，再取 STOP，且两者共用原 batch budget。
 * 非 STOP 返回 TR_ERR_INVALID；同 epoch 重复提交返回 TR_ERR_STATE。
 * 只有上一 STOP 已弹出，wait_open 才能开启下一 epoch。
 */
int tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake);

/*
 * 锁内判断普通 ring 与 STOP 保留槽。返回 1 表示当前无 pending command；
 * NULL 返回 0，让调用方保守地走排队路径。
 */
int tr_command_queue_is_empty(struct tr_command_queue *queue);

/*
 * Snapshot the greatest FIFO sequence assigned so far. Used by owner-local
 * deferred lifecycle work to wait only for commands that were already admitted
 * at its linearization point.
 */
uint64_t tr_command_queue_last_sequence(struct tr_command_queue *queue);

/*
 * 64-bit modular sequence comparison. A lifecycle barrier can never remain
 * pending across 2^63 admitted commands, so half-range ordering is unambiguous.
 * Sequence 0 is the pre-first-command sentinel and is always already satisfied.
 */
static inline int tr_command_sequence_after_eq(
	uint64_t current, uint64_t target)
{
	if (target == 0U)
		return 1;
	return ((current - target) & (UINT64_C(1) << 63)) == 0U;
}

/* 返回实际复制到 out 的 command 数量。 */
size_t tr_command_queue_pop_batch(struct tr_command_queue *queue,
				  struct tr_command *out, size_t max_commands);

#ifdef __cplusplus
}
#endif

#endif
