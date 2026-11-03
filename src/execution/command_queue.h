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
	/* 队列分配的 FIFO 序号；0 表示尚未入队。 */
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

	/* 生产者侧归属统计，在现有队列锁下更新。 */
	uint64_t pushed_send;
	uint64_t full_send;
	uint64_t pushed_resume_rx;
	uint64_t full_resume_rx;
	uint64_t pushed_call;
	uint64_t full_call;
	uint64_t pushed_other;
	uint64_t full_other;

	int wake_pending;

	/* 固定的停止专用槽位，不占普通环形队列的 capacity/count/peak_count。
	 * STOP 仅在普通环形队列排空后弹出；stop_closed 保持到下一轮次开放，
	 * 防止已经弹出 STOP 后又接受新命令。所有字段均由 lock 保护。
	 */
	struct tr_command stop_command;
	int stop_pending;
	int stop_closed;
};

int tr_command_queue_init(struct tr_command_queue *queue, uint32_t capacity);
void tr_command_queue_destroy(struct tr_command_queue *queue);

/* 当调用方需要唤醒反应器 eventfd 时设置 need_wake。
 * TR_AGAIN 表示有界队列已满，此时命令所有权没有发生转移。
 */
int tr_command_queue_push(struct tr_command_queue *queue,
			  const struct tr_command *command, int *need_wake);

/* 同步所有者请求的容量等待准入。
 *
 * 关闭控制 push_wait() 等待者，普通 push() 在 STOP 提交前仍为有界立即入队。
 * STOP 提交会关闭整个轮次；消费 STOP 后，开放操作才重开普通及等待准入。
 * generation 由生命周期所有者在 ctl_lock 保护下取样，防止停止操作
 * 关闭等待者后旧请求重新混入新的准入轮次。
 */
int tr_command_queue_wait_open(struct tr_command_queue *queue);
void tr_command_queue_wait_close(struct tr_command_queue *queue);
uint64_t tr_command_queue_wait_generation(struct tr_command_queue *queue);

/* TR_OK：命令所有权已转移给队列。
 * TR_ERR_CLOSED：expected_generation 已关闭或失效，所有权未转移。
 * 队列满时在 not_full 上睡眠，不做 sched_yield 自旋。
 */
int tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t expected_generation, int *need_wake);

/* 仅供 STOP 使用的固定容量保留通道，不分配内存、不等待普通环形队列空位。
 * 保留原内部函数名，但此入口已经不再是容量等待者：不能在持有
 * 反应器 ctl_lock 时等待所有者消费，因为所有者回调自身也可能需要该锁。
 *
 * 在 queue->lock 下关闭本轮次的新命令，STOP 排在所有已接受命令之后；
 * pop_batch 先取普通环形队列，再取 STOP，且两者共用原批次预算。
 * 非 STOP 返回 TR_ERR_INVALID；同轮次重复提交返回 TR_ERR_STATE。
 * 只有上一 STOP 已弹出，wait_open 才能开启下一轮次。
 */
int tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake);

/* 锁内判断普通环形队列与 STOP 保留槽。返回 1 表示当前无待处理命令；
 * NULL 返回 0，让调用方保守地走排队路径。
 */
int tr_command_queue_is_empty(struct tr_command_queue *queue);

/* 对目前已分配的最大 FIFO 序号取快照。供所有者本地的延后生命周期工作使用，
 * 仅等待其线性化点之前已获准的命令。
 */
uint64_t tr_command_queue_last_sequence(struct tr_command_queue *queue);

/* 64 位模序号比较。生命周期屏障绝不会跨越 2^63 个获准命令而仍未完成，
 * 因此半范围排序无歧义。序号 0 是首条命令之前的哨兵值，始终视为已满足。
 */
static inline int tr_command_sequence_after_eq(
	uint64_t current, uint64_t target)
{
	if (target == 0U)
		return 1;
	return ((current - target) & (UINT64_C(1) << 63)) == 0U;
}

/* 返回实际复制到 out 的命令数量。 */
size_t tr_command_queue_pop_batch(struct tr_command_queue *queue,
				  struct tr_command *out, size_t max_commands);

#ifdef __cplusplus
}
#endif

#endif
