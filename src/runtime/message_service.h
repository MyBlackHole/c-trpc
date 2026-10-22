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

enum tr_message_service_delivery {
	TR_MESSAGE_SERVICE_ON_OWNER,
	TR_MESSAGE_SERVICE_OWNER_STOPPED
};

struct tr_message_service_result {
	enum tr_message_transform_outcome outcome;
	enum tr_message_service_delivery delivery;
};

/* 正常回调在所属 Reactor 所有者执行；父 owner 成功 join 后的异常回收
 * 在显式 reaper 调用线程执行，delivery 为 OWNER_STOPPED，强制取消且
 * 不交付载荷。此分支只做本地终止/引用释放，不得操作旧协议状态或发起
 * 网络发送。两种路径合计恰好一次，均须短小、非阻塞。
 * message 只在回调内借用；成功时可 clone/none_move 留存，否则由服务释放。
 * 失败时为空句柄。arg 可在回调内释放；不得直接复制拥有型消息结构。
 * 取消也必须交付一次回调，取消返回不允许提前销毁 arg。
 */
typedef void (*tr_message_service_done_fn)(
        void *arg, struct tr_task_handle handle,
        struct tr_message_service_result result, struct tr_message *message);

/* 库内组合模块，不是稳定 SDK。所有 on_owner API 必须在传入 Reactor 的
 * 当前所有者调用（跨线程使用 tr_reactor_call），不是任意其他 Reactor。
 * create 不注册事件、不启动线程；只在成功时发布 *out，原值须为 NULL。
 * service、执行器和变换使用同一硬预算；输入已有预算不重复计费。
 * budget 必须存活到所有输出引用释放。优先正常关闭服务再停止父循环；
 * 若父循环先退出，须 stop/join 成功后显式 reap，不能直接销毁父对象。
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

/* 原子接受一个消息变换：仅 TR_OK 清空 input、写 handle、承担一次 done。
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
 * 失败保留源、fd 和回调参数，调用方须重试，不能关闭父 Reactor 绕过责任。
 * 完全清理后清空指针；最终 close/预算错误也可能清空指针，不得重复释放。
 * 这是父 Reactor 存活期间的有序关闭；停止后的回收使用 reap_stopped。
 */
int tr_message_service_close_on_owner(struct tr_message_service **service);

/* 父 Reactor 的 stop/join 成功之后，由串行化的生命周期控制线程重试。
 * 取得回收租约后阻止父重启/销毁；取消所有未终局工作，完成回调明确标记
 * OWNER_STOPPED，不交付成功载荷。工作尚未静默返回 AGAIN，解绑失败保留
 * 对象/fd/租约。全部责任结束才释放租约及服务。不得与其他 service API
 * 或外层生命周期并发；回调中重入及任何工作/owner 线程调用均拒绝。
 * 本函数不停止或销毁父对象，不可用它绕过失败的 pthread_join。 */
int tr_message_service_reap_stopped(struct tr_message_service **service);

#endif
