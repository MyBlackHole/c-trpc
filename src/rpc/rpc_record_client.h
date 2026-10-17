#ifndef TR_RPC_RECORD_CLIENT_H
#define TR_RPC_RECORD_CLIENT_H

#include "rpc_record_stream.h"

#include <stdbool.h>

struct tr_rpc_record_client;

struct tr_rpc_record_client_config {
	uint32_t capacity;
	size_t progress_limit;
	/* 单个既有 Reactor 定时器推进 OPEN、关闭、共享 TX 压力和期限。
	 * 显式周期 1ms..1s；轮转受 progress_limit 限制，不承诺实时抢占。 */
	uint64_t progress_period_ns;
};

/* 借用宿主寿命；复制不增加引用。64 位代次在同一宿主内永不回绕。
 * Client 首次销毁尝试开始后即禁止使用，即使收尾失败仍保留 Client。 */
struct tr_rpc_record_client_handle {
	struct tr_rpc_record_client *client;
	uint64_t generation;
	uint32_t slot;
};

typedef void (*tr_rpc_record_client_received_fn)(
        struct tr_rpc_record_client_handle handle,
        const struct tr_rpc_record_view *view, void *arg);
typedef void (*tr_rpc_record_client_writable_fn)(
        struct tr_rpc_record_client_handle handle, void *arg);
typedef void (*tr_rpc_record_client_terminal_fn)(
        struct tr_rpc_record_client_handle handle, enum tr_rpc_status status,
        enum tr_rpc_record_delivery delivery, void *arg);

struct tr_rpc_record_client_callbacks {
	/* 必需。view 仅借用到返回；零长度 MESSAGE 也交付一次。所有正常
	 * 回调在所属 owner 上执行，必须短小且非阻塞。可 send/half_close/
	 * cancel，不可递归 start、接收、关闭或销毁宿主。 */
	tr_rpc_record_client_received_fn received;
	/* 可选。OPEN 被本地传输接受后通知初次可写；之后仅是重试提示，
	 * 不是保留信用或保证下一次发送成功。AGAIN 的正文不由宿主保存。 */
	tr_rpc_record_client_writable_fn writable;
	/* 必需。成功受理后恰好一次，包括 OPEN 尚未接受时停止的调用。
	 * 延后到当前应用回调退出；可释放 arg。OWNER_STOPPED 表示在已
	 * join 的外部清理线程交付；保留已选终局，尚未终局者选 CANCELLED。
	 * 该回调域不允许重入 Client 生命周期。回调不表示槽已退役；在
	 * Channel 接管 RESET 或关闭之前仍保留固定槽，不再访问用户 arg。 */
	tr_rpc_record_client_terminal_fn terminal;
	void *arg;
};

/* 库内 V2 Client 宿主，只装配专用 deferred Channel，不是另一套 Client。
 * Channel、创建时已验证冻结的方法表和非零硬预算均借用；没有线程池、
 * 业务正文队列或第二套 RPC 状态机。create 不发布回调源，失败不改 out。
 * start 部分失败保留对象及已发布责任，必须 close/reap 重试收尾。
 */
int tr_rpc_record_client_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_method_desc *methods,
        size_t method_count, const struct tr_rpc_record_client_config *config,
        struct tr_memory_budget *budget, struct tr_rpc_record_client **out);
int tr_rpc_record_client_start_on_owner(struct tr_rpc_record_client *client);

/* 门面排空桥。通知只在 owner 的最外层进度边界、全部用户回调返回后
 * 发布本层静默状态；未发送的 OPEN、未报告终局及尚未退役的绑定都为
 * busy，不包含 Channel 持有的网络确认和 TX。观察者只发布状态，不得
 * 重入本宿主。设置不推进业务；begin_drain 后立即发布或延至当前回调
 * 返回，随后仅在谓词变化时通知。NULL 注销，停止域回收不调用观察者。
 */
typedef void (*tr_rpc_record_client_progress_fn)(bool drained, void *arg);
int tr_rpc_record_client_set_progress_notify_on_owner(
        struct tr_rpc_record_client *client,
        tr_rpc_record_client_progress_fn notify, void *arg);
/* 单调关闭新 CALL 准入，已有调用继续推进；允许从用户回调内发布意图。
 * 外层须在同一 owner 事务发布 Channel GOAWAY，网络等待由外层承担。 */
int tr_rpc_record_client_begin_drain_on_owner(
        struct tr_rpc_record_client *client);

/* 不延长对象寿命，只供既有 Client 将弱句柄路由到精确所属 Reactor。
 * in_callback 是当前线程的回调域查询，不读取其他 owner 的可变字段。 */
struct tr_reactor *
tr_rpc_record_client_reactor(struct tr_rpc_record_client *client);
int tr_rpc_record_client_in_callback(void);

/* 仅 TR_OK 写 out 并接管一次终局责任。OPEN 等待 ACK/信用也是已受理；
 * 失败不回调用户，已被传输接受的内部收尾仍占一个固定槽直到可回收。
 * 元数据有界复制；timeout_ms 是真实本地单调期限，OPEN 重试携带剩余值。
 * 服务端执行期限不由此保证；本轮 Client 不支持自动重连。回调可能先于
 * 外层同步 API 返回，arg 应预先准备，out 存储不能依赖 arg 的寿命。
 */
int tr_rpc_record_client_call_start_on_owner(
        struct tr_rpc_record_client *client, uint32_t service_id,
        uint32_t method_id, const struct tr_rpc_call_options *options,
        const struct tr_rpc_record_client_callbacks *callbacks,
        struct tr_rpc_record_client_handle *out);

/* 仅 TR_OK 接管并清空 *body，失败保留正文及本次发送语义。允许 NULL
 * 正文发送一条空 MESSAGE；不隐式排队。OPEN 尚未接受返回 AGAIN。
 * 首次 OPENED 冻结对端初始窗口，超过该窗口的整条记录明确拒绝；当前
 * 剩余信用不足仍返回 AGAIN。本入口未实现任意 L>W 的发送活性。
 * 成功终局/取消后的句柄拒绝新操作，安全退役后返回 STALE。
 */
int tr_rpc_record_client_send_on_owner(
        struct tr_rpc_record_client_handle handle, struct tr_buffer **body);
int tr_rpc_record_client_half_close_on_owner(
        struct tr_rpc_record_client_handle handle);
/* 本地选择 CANCELLED；本地超时/错误共用 RESET 退役路径，RPC HALF_CLOSE
 * 不屏蔽整调用取消。先通知唯一终局，安全边界上将发送/确认责任交给
 * Channel，只有其返回 TR_OK 或精确流已关闭后才移除 RPC 路由/绑定。
 * 失败保留原责任并由进度定时器重试，不重放业务，不重解析晚 FINAL。
 * 已接纳 peer FINAL 的调用沿普通 CLOSE 完成；CLOSE 一经接受不再主动
 * 改为 RESET。Channel 独立持有确认状态，不合作 peer 可占有界网络槽到
 * 断连或销毁。取消不抢占远端线程，也不撤销已发生的业务副作用。
 */
int tr_rpc_record_client_cancel_on_owner(
        struct tr_rpc_record_client_handle handle);
int tr_rpc_record_client_pump_on_owner(struct tr_rpc_record_client *client);

/* close 用于仍在 owner 上的装配回滚：停止受理并取消调用；待 Channel
 * 接管 RESET、普通 STREAM_CLOSE 被接受或精确流死亡后释放绑定，再
 * 解绑 handler/注销 timer。网络控制状态不引用已退役的 RPC 存储。
 * AGAIN 或硬错误保留未解除的责任，须继续驱动并重试；晚到旧流字节
 * 仅释放，不匹配新的代次。成功后原 Channel 应销毁，不换绑新宿主。
 */
int tr_rpc_record_client_close_on_owner(struct tr_rpc_record_client **client);
/* 父 Reactor 已 stop/join，外部控制线程串行重试；不发停止后的网络帧。
 * 先交付全部残留终局，再拆除发布源。失败保留对象及剩余责任。TX 前缀
 * 和应用已提交的 buffer 可长于宿主存活，预算必须保持到 Channel 释放。
 */
int tr_rpc_record_client_reap_stopped(struct tr_rpc_record_client **client);

#endif
