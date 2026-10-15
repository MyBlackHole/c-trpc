#ifndef TR_MESSAGE_SERVICE_H
#define TR_MESSAGE_SERVICE_H

#include "../message/transform.h"
#include "../execution/task_executor.h"

struct tr_reactor;
struct tr_message_service;

struct tr_message_service_config {
	struct tr_task_executor_config tasks;
	/* 每次事件/后备定时交付的最大完成数，不是单回调 CPU 时间上限。 */
	size_t dispatch_limit;
	/* 后备进度检查周期，1 ms..1 s。通知写失败不能遗失已接受的完成。 */
	uint64_t watchdog_ns;
};

struct tr_message_service_stats {
	struct tr_task_executor_stats tasks;
	size_t held_jobs;
	/* 首个通知 I/O errno，锁存至销毁；非零时拒绝新消息并取消在途任务。 */
	int notification_error;
	bool accepting;
	bool event_attached;
	bool timer_attached;
};

/* 回调仅在所属 Reactor 的所有者上执行，必须短小、非阻塞。
 * message 只在回调内借用；成功时可 clone/none_move 留存，否则由服务释放。
 * 失败时为空句柄。arg 可在回调内释放；不得直接复制拥有型消息结构。
 * 取消也必须交付一次回调，取消返回不允许提前销毁 arg。父所有者退出后，
 * 尚未交付的责任由 reap_stopped 的显式 abandon 接管；不会在其他线程
 * 调用这个正常完成函数，也不会把未交付的成功结果改到回收线程发布。
 */
typedef void (*tr_message_service_done_fn)(
        void *arg, struct tr_task_handle handle,
        enum tr_message_transform_outcome outcome, struct tr_message *message);

/* 库内组合模块，不是稳定 SDK。所有 on_owner API 必须在传入 Reactor 的
 * 当前所有者调用（跨线程使用 tr_reactor_call），不是任意其他 Reactor。
 * create 不注册事件、不启动线程；只在成功时发布 *out，原值须为 NULL。
 * service、执行器和变换使用同一硬预算；输入已有预算不重复计费。
 * budget 必须存活到所有输出引用释放。正常先关闭服务再停止父 Reactor；
 * 父所有者先退出时，成功 stop/join 后用 reap_stopped 回收，父对象须保留。
 */
int tr_message_service_create_on_owner(
        struct tr_reactor *reactor,
        const struct tr_message_service_config *config,
        struct tr_memory_budget *budget, struct tr_message_service **out);

/* 仅尝试一次。任一步失败都停止准入，保留对象及已注册源/已建线程；调用方
 * 必须 close_on_owner 重试清理，不得直接 free。源注册后在同一 owner turn
 * 完成状态发布，不存在事件先到却尚未登记 fd 所有权的跨线程窗口。
 */
int tr_message_service_start_on_owner(struct tr_message_service *service);

/* 原子接受一个消息变换：仅 TR_OK 清空 input、写 handle、承担一次完成责任。
 * 正常由 done 交付；父所有者退出后，尚未交付部分由显式 abandon 清理。
 * 先在原引用的克隆上准备变换，执行器拒绝时回滚克隆及预约，保留 input。
 * 消费者不需自行处理两个接受点。接受不是网络发送或远端业务成功。
 * 仅 Zstandard 显式变换经过此服务；none 继续使用无任务的直接移交路径。
 */
int tr_message_service_submit_on_owner(
        struct tr_message_service *service, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);

/* 可以跨线程，但调用者须保证 service 存活并与 close 排除并发；弱能力
 * 不保护已释放对象。任务取消在每个后端检查点可见，不从取消线程释放操作。
 */
int tr_message_service_cancel(struct tr_message_service *service,
                              struct tr_task_handle handle);
int tr_message_service_stop_on_owner(struct tr_message_service *service,
                                     enum tr_task_stop_mode mode);
int tr_message_service_get_stats_on_owner(struct tr_message_service *service,
                                          struct tr_message_service_stats *out);

/* 自动停止准入并有界交付完成。尚未静默返回 AGAIN，不阻塞等待业务工作。
 * 回调/提交的重入 close 返回 STATE。先 join、交付全部完成，再逐个注销
 * 辅助源/定时器，最后清除通知并销毁执行器、关闭 fd。DEL 或 timer 注销
 * 失败保留源、fd 和回调参数，调用方须重试；父所有者先退出后改用下述
 * reap_stopped，但不能直接销毁父对象绕过发布和任务责任。
 * 完全清理后清空指针；最终 close/预算错误也可能清空指针，不得重复释放。
 * 这是父 Reactor 正在运行时的有序关闭；接管成功后不再接受 on_owner 操作。
 */
int tr_message_service_close_on_owner(struct tr_message_service **service);

/* 父所有者消失后的明确终局：只接受原提交 arg 和任务能力，不交付载荷。
 * 对每条尚未 done 的任务恰好调用一次；输入和私有输出已收敛后才调用，
 * 允许释放 arg。可以记录 OWNER_LOST/取消并释放上层责任，不能冒充成功。
 * 回调在 reap_stopped 的调用线程、所有内部锁之外同步执行，必须短小；
 * 禁止重入回收或启动/销毁父 Reactor。此函数指针只借用到本次调用返回。
 */
typedef void (*tr_message_service_abandon_fn)(void *arg,
                                              struct tr_task_handle handle);

/* 仅在父 Reactor 的 tr_reactor_stop 成功（包括其 pthread_join）后调用。
 * 该入口以 call_or_stopped 核验：运行态返回 STATE，停止中/未成功 join
 * 返回 CLOSED，不因为 accepting 已关闭或线程看似退出就越过静默屏障。
 * 不允许从任何 Reactor 所有者调用；首次接管后仅同一回收线程可以重试。
 *
 * 调用方是父生命周期协调者，必须持有其外部独占权：从 stop 开始直到本
 * 函数清空 service，禁止父 start/destroy、其他服务 API、第二回收者及
 * 其他触及该服务的控制操作并发。父对象和预算域必须继续存活。这不是
 * 自动父子注册/引用机制，不支持父 Reactor 已销毁后的悬空指针回收。
 *
 * 接管取消在途任务并撤销通知，逐个解绑 fd/定时器；每次最多清理配置的
 * dispatch_limit 个结果，然后尝试 join。未结束返回 AGAIN，不强制终止
 * 工作者；解绑/join/destroy 失败保留对象和进度。已完成但未交付的成功
 * 输出也销毁，全部剩余 arg 只走 abandon，不调用原 done。已交付并被
 * 应用保留的输出不受影响。接管不可逆，失败不恢复正常准入。
 *
 * 成功清空指针；close/预算异常也可能在资源实际销毁后清空指针，遵循与
 * close_on_owner 相同的最终返回契约。失败返回时 abandon 不会在后台调用，
 * 下一次重试可以继续回收；不能因当前回调数为零就遗忘尚在执行的任务。
 */
int tr_message_service_reap_stopped(struct tr_message_service **service,
                                    tr_message_service_abandon_fn abandon);

#endif
