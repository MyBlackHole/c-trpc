#ifndef TR_TASK_EXECUTOR_H
#define TR_TASK_EXECUTOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct tr_task_executor;
struct tr_task_cancel;
struct tr_memory_budget;

/* 值类型弱能力，不延长执行器寿命。实例与槽位代次都必须相符。
 * 不得修改字段或从旧能力构造新能力；旧实例销毁后仍须有外部生命周期保护。
 */
struct tr_task_handle {
	uint64_t instance;
	uint64_t generation;
	uint32_t slot;
};

/* work_status 是工作函数的原始返回值，不直接代表应用可观察的成功。
 * cancelled 表示取消在执行器终局提交前获胜；完成方必须清理原始结果，
 * 不得在 cancelled 时将可能已生成的成功载荷继续向上交付。
 */
struct tr_task_result {
	int work_status;
	bool cancelled;
};

typedef int (*tr_task_work_fn)(void *arg, const struct tr_task_cancel *cancel);
typedef void (*tr_task_done_fn)(void *arg, struct tr_task_handle handle,
                                struct tr_task_result result);

struct tr_task_spec {
	tr_task_work_fn work;
	tr_task_done_fn done;
	void *arg;
};

struct tr_task_executor_config {
	size_t workers;
	/* 覆盖排队、执行、等待交付和正在回调的全部已接受工作。 */
	size_t capacity;
};

enum tr_task_stop_mode { TR_TASK_DRAIN, TR_TASK_CANCEL };

struct tr_task_executor_stats {
	size_t capacity;
	size_t outstanding;
	size_t queued;
	size_t running;
	size_t completed;
	size_t delivering;
	size_t workers_alive;
	bool accepting;
};

/* 创建者是完成交付及生命周期所有者；预算必须有非零硬上限，存活到
 * destroy 返回。创建前预约执行器、槽位与线程句柄数组，不计 pthread
 * 内部分配/线程栈/RSS；任务载荷由上层分别记账。create 不创建线程。
 * 仅 TR_OK 写 *out（原值须为 NULL）；失败不发布半对象、不留下预约。
 */
int tr_task_executor_create(const struct tr_task_executor_config *config,
                            struct tr_memory_budget *budget,
                            struct tr_task_executor **out);

/* 仅创建者调用一次。全部线程建立后才开放提交；部分启动失败返回
 * TR_ERR_SYS，但 *executor 仍拥有已建线程，已停止准入。必须 join/destroy，
 * 不得释放或重启它。失败前没有任务被接受，不隐藏 pthread_join 失败。
 */
int tr_task_executor_start(struct tr_task_executor *executor);

/* 库内完成通知适配。仅创建者在 start 之前设置，或在 join 且全部完成
 * 已交付后清除。通知在完成入队之后、执行器锁内同步调用，必须短小、
 * 不阻塞、不分配，不得重新调用执行器/应用代码；仅用于唤醒事件源。
 * 通知不是结果所有权转移，也不是一条消息一个事件；失败不能丢弃完成。
 * arg 必须存活至 join 后清除通知或销毁执行器，外部 fd 的解绑另行负责。
 */
typedef void (*tr_task_notify_fn)(void *arg);
int tr_task_executor_set_notify(struct tr_task_executor *executor,
                                tr_task_notify_fn notify, void *arg);

/* 成功时复制 spec，接管一次 work 和一次 done 的执行责任；失败不调用
 * 用户代码、不接管 arg、不改 out。提交本身不分配、不等待队列容量。
 * 满载返回 TR_AGAIN；停机返回 TR_ERR_CLOSED；代次耗尽明确失败不回绕。
 * work 在工作线程恰好执行一次，即使启动前已经取消，仍须按取消令牌
 * 收敛此前已接受的上层操作。禁止 pthread_exit/longjmp/外部 pthread_cancel。
 * work 不得等待依赖本执行器/所有者继续推进的工作；取消是协作式。
 * done 仅在创建者显式 dispatch 内执行，允许释放 arg；引擎其后不读取 arg。
 * 非所有者提交时，done 可能在 submit 返回之前于所有者线程执行；全部
 * 回调参数须事先准备。out 只由提交者访问，与 arg 的生存期存储分开。
 */
int tr_task_executor_submit(struct tr_task_executor *executor,
                            const struct tr_task_spec *spec,
                            struct tr_task_handle *out);

/* 可跨线程调用。取消不释放任务、不交付回调；提交终局后返回 STATE，
 * 过期/其他实例能力返回 STALE。令牌只在 work 调用期间借用，不得保留。
 */
int tr_task_executor_cancel(struct tr_task_executor *executor,
                            struct tr_task_handle handle);
bool tr_task_cancel_requested(const struct tr_task_cancel *cancel);

/* 仅创建者，非阻塞交付最多 limit 个完成；limit 必须非零。回调在锁外，
 * 同一个执行器不可递归 dispatch/join/destroy。可在回调里 submit/stop，
 * 但当前槽位直到回调返回才归还。完成次序是完成入队次序，不是提交次序。
 */
int tr_task_executor_dispatch(struct tr_task_executor *executor, size_t limit,
                              size_t *delivered);

/* 可跨线程停止准入；DRAIN 可升级 CANCEL，不能降级。取消仅覆盖未提交
 * 终局的任务，不改写已完成结果。所有已接受任务仍有一次 done 交付责任。
 */
int tr_task_executor_stop(struct tr_task_executor *executor,
                          enum tr_task_stop_mode mode);

/* 仅创建者且不在回调中：必须先 stop。工作仍未退出时返回 TR_AGAIN，
 * 不等待用户 work；全部工作退出后逐个 pthread_join，成功的不会重复 join。
 * join 失败保留对象与剩余线程以供重试。成功只证明工作线程静默，完成队列
 * 仍须 dispatch。pthread 线程尾部回收可能短暂等待，不承诺硬实时上限。
 */
int tr_task_executor_join(struct tr_task_executor *executor);
int tr_task_executor_get_stats(struct tr_task_executor *executor,
                               struct tr_task_executor_stats *out);

/* 仅创建者，外部须排除所有并发 API/能力使用。仅未启动或已 join 且全部
 * done 返回时销毁。仍有任务返回 AGAIN；未停止/join 返回 STATE。
 * 同步原语销毁失败保留指针，此后只允许重试 destroy。成功清空指针并归还
 * 预分配字节。不能因用户 work 不合作而提前释放仍被使用的内存。
 */
int tr_task_executor_destroy(struct tr_task_executor **executor);

#endif
