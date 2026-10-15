#ifndef TR_RPC_RECORD_DISPATCH_H
#define TR_RPC_RECORD_DISPATCH_H

#include "rpc_record_router.h"
#include "../execution/task_executor.h"

struct tr_rpc_record_dispatch;
struct tr_rpc_record_reply;

/* 弱能力，不能自行改写或在派发器销毁后使用。业务工作函数只取得该
 * 调用的数值身份，不取得路由器/Stream/可变调用结构。 */
struct tr_rpc_dispatch_id {
	uint64_t generation;
	uint32_t slot;
};
struct tr_rpc_dispatch_handle {
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_rpc_dispatch_id id;
};

/* 在工作线程按每调用 FIFO 执行。view 的正文/元数据是派发器持有的副本，
 * 只借用到函数返回。不同调用可并发访问同一 arg，arg 须自行同步并存活
 * 到派发器完全销毁。取消是协作式，不允许退出线程或等待所属 Reactor。
 *
 * 每个 OPEN/MESSAGE/HALF_CLOSE 最多执行一次；尚未开始就被取消的业务
 * 回调可以不执行，但已接受的执行器任务仍由完成路径收敛。非OK返回值
 * 丢弃本次生成的正文并尝试发送错误 FINAL；HALF_CLOSE 默认选择最终OK。
 * 不支持从回调返回后继续使用 reply；长期异步生产者另需后续持有接口。
 */
typedef enum tr_rpc_status (*tr_rpc_record_work_fn)(
        struct tr_rpc_dispatch_id id, const struct tr_rpc_record_view *view,
        const struct tr_task_cancel *cancel, struct tr_rpc_record_reply *reply,
        void *arg);

struct tr_rpc_dispatch_method {
	struct tr_rpc_method_desc descriptor;
	tr_rpc_record_work_fn work;
	void *arg;
};
struct tr_rpc_record_dispatch_config {
	uint32_t capacity;
	/* 含排队、已投递、已完成待发送的全部事件；超限终止该调用而非
	 * 返回可重放已吸收网络字节的AGAIN。另受共享字节预算限制。 */
	uint32_t max_pending_per_call;
	uint32_t max_replies_per_event;
	size_t max_reply_bytes;
};
struct tr_rpc_record_dispatch_stats {
	size_t active_calls;
	size_t held_events;
	size_t submitted_calls;
	size_t reply_calls;
	size_t terminal_calls;
};

/* 工作函数内追加一个响应消息（零长度仍是一条）。先预约精确存储再
 * 复制，受方法大小、条数及单事件总字节限制。任何追加失败锁存错误，
 * 不能忽略失败后返回OK而交付被截断的成功批次。不承担任意用户堆内存。
 */
int tr_rpc_record_reply_append(struct tr_rpc_record_reply *reply,
                               const void *data, size_t size);
/* 选择本次回复的最终状态，重复选择返回STATE；不允许随后再追加正文。
 * 非OK终局丢弃本次正文。所有记录仍由原绑定验证基数及发送接受点。
 */
int tr_rpc_record_reply_finish(struct tr_rpc_record_reply *reply,
                               enum tr_rpc_status status);

typedef void (*tr_rpc_dispatch_terminal_fn)(
        struct tr_rpc_dispatch_handle handle, struct tr_stream_handle stream,
        enum tr_rpc_status status, enum tr_rpc_record_delivery delivery,
        bool final_accepted, void *arg);

/* 库内服务端派发适配；不创建线程/事件源，不接管executor或Channel。
 * executor 的完成所有者必须是该 Reactor，正常运行时外层必须持续驱动
 * executor dispatch，再调用本模块 pump（含WRITABLE/资源压力解除）。
 * 可以共享同owner的执行器；不能在notify的执行器锁内调用pump。
 * 方法表复制，参数指针借用；外层转发OPENED/数据/流事件，RX信用仍归外层。
 *
 * terminal 在该调用所有任务完成清理后至多一次。final_accepted 仅说明
 * FINAL已被传输接受，不说明已写入或对端已收到；路由拒绝/本地取消仍可能
 * 是未通知对端的本地终局。回调可做资源清理/快照，不可重入生命周期操作。
 */
int tr_rpc_record_dispatch_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_dispatch_config *config,
        tr_rpc_dispatch_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_dispatch **out);
int tr_rpc_record_dispatch_admit_on_owner(
        struct tr_rpc_record_dispatch *dispatch, struct tr_stream_handle stream,
        struct tr_rpc_dispatch_handle *out);
int tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *dispatch, struct tr_stream_handle stream,
        const uint8_t *data, size_t size);
int tr_rpc_record_dispatch_event_on_owner(
        struct tr_rpc_record_dispatch *dispatch, struct tr_stream_handle stream,
        enum tr_stream_event event);

/* 每次最多访问 min(limit,capacity) 个调用，轮转推进；每调用最多发一条
 * 记录或投递一次任务。背压只保留结果并重试发送，绝不重跑业务函数。
 * pump 的成功不保证所有待办已结束；外层事件循环须保证公平重复推进。
 */
int tr_rpc_record_dispatch_pump_on_owner(
        struct tr_rpc_record_dispatch *dispatch, size_t limit);
int tr_rpc_record_dispatch_abort_on_owner(struct tr_rpc_dispatch_handle handle,
                                          enum tr_rpc_status status);
int tr_rpc_record_dispatch_get_stats_on_owner(
        struct tr_rpc_record_dispatch *dispatch,
        struct tr_rpc_record_dispatch_stats *out);
/* 先移除外层流路由；仅终局已通知、全部事件已释放后退役，失败保持句柄。 */
int tr_rpc_record_dispatch_retire_on_owner(
        struct tr_rpc_dispatch_handle handle);

/* 销毁停止准入并本地取消路由；在途任务存在返回AGAIN，保留派发器和
 * 参数，外层继续交付执行器完成再重试。不停止/销毁借用的执行器，不自动
 * close/RESET流；TX正文及前缀可能比派发器存活更久，预算域须继续有效。
 */
int tr_rpc_record_dispatch_destroy_on_owner(struct tr_rpc_record_dispatch **p);
/* 父Reactor已join，外层全程排除重启/销毁和并发调用。首次调用置为清理
 * 域并请求取消，任务未交付返回AGAIN。随后由执行器所有者负责stop/join、
 * adopt_cleanup与dispatch；再重试本接口。先进入清理域，才能在非原owner
 * 交付该派发器的任务完成。最终复用路由恢复租约，OWNER_STOPPED只清理，
 * 不交付成功回复、不访问已失效Stream。共享执行器的停止由外层统一协调。
 */
int tr_rpc_record_dispatch_reap_stopped(struct tr_rpc_record_dispatch **p);

#endif
