#ifndef TR_RPC_RECORD_ENDPOINT_H
#define TR_RPC_RECORD_ENDPOINT_H

#include "rpc_record_dispatch.h"

struct tr_rpc_record_endpoint;
struct tr_rpc_record_semantic_counters;

#define TR_RPC_RECORD_OPEN_TIMEOUT_DEFAULT_NS UINT64_C(10000000000)

struct tr_rpc_record_endpoint_config {
	struct tr_rpc_record_dispatch_config dispatch;
	size_t completion_limit;
	size_t progress_limit;
	/* 单个已有 Reactor 定时器同时推进期限检查、完成、发送和资源压力恢复。
	 * 显式周期 1ms..1s；不承诺零轮询成本或单回调的 CPU 时间上限。 */
	uint64_t progress_period_ns;
	/* 本地终局选择后等待 FINAL 接受的最长时间。到期停止重试正文，
	 * 转向预留 RESET 路径；不是业务执行期限，也不保证实时抢占。
	 * 业务尚未退出的本地终局可直接 RESET，通知仍等待真实退出。 */
	uint64_t final_wait_ns;
	/* transport OPENED 到完整有效 RPC OPEN 的本机总期限。0 采用固定
	 * 10s 默认值；非0为有限纳秒预算，分段字节和重试均不能续期。
	 * 到期直接 RESET；对端不确认时网络槽仍由 Channel 有界保留。 */
	uint64_t open_timeout_ns;
};

struct tr_rpc_endpoint_result {
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	bool final_accepted;
};

typedef void (*tr_rpc_endpoint_terminal_fn)(
        struct tr_stream_handle stream, struct tr_rpc_endpoint_result result,
        void *arg);

typedef void (*tr_rpc_endpoint_progress_fn)(bool drained, void *arg);

/* 库内 V2 服务端宿主。只能绑定专用且尚未开始
 * 交付流事件的 Channel（例如 deferred Channel）。start 安装其唯一处理器，
 * 不保存/恢复未知的旧处理器。Channel、executor、预算皆借用；外层必须在
 * 本宿主关闭后再销毁它们，禁止并发替换处理器。executor 的完成所有者须为
 * 此 Reactor；宿主会 dispatch 共享执行器，不负责创建、停止或销毁工作池。
 *
 * create 不发布事件源；普通失败保持 *out 为 NULL。若内部派发器的失败
 * 清理也未完成，则 *out 接收待清理宿主，调用方须 close 或 stopped reap。
 * start 任一步失败都保留
 * 对象及已注册定时器，须 close 重试。用户终局只在业务任务静默后通知一次；
 * final_accepted 不是网络送达证明。回调可以查询，不得重入接收/关闭。
 */
int tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_endpoint_config *config,
        tr_rpc_endpoint_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_endpoint **out);
int tr_rpc_record_endpoint_start_on_owner(struct tr_rpc_record_endpoint *ep);

/* 只在 start 前绑定同 owner 的累计记账。借用对象必须长于所有绑定宿主，
 * 包括停止后任务完成交付；关闭宿主不会清空或再次归档此累计值。 */
int tr_rpc_record_endpoint_bind_semantic_stats_on_owner(
        struct tr_rpc_record_endpoint *ep,
        struct tr_rpc_record_semantic_counters *counters);
/* 首次 start 前一次借用宿主的共享 producer 域；所有绑定 Endpoint/TX
 * 退出后宿主才可关闭该域，不能在对端退役时重置有限句柄配额。 */
int tr_rpc_record_endpoint_bind_producers_on_owner(
        struct tr_rpc_record_endpoint *ep,
        struct tr_rpc_record_producer_domain *domain);

/* 同时由数据、可写及定时器驱动；每次工作量受显式数量配置限制。
 * 重入错误以预分配槽位形成有界待处理队列，先于业务完成/发送处理；
 * 事件预算用尽时保留队列至下一处理轮次，不能让成功终局越过既知错误。
 * 本地拒绝自动尝试错误 FINAL，接受后再关流；期限耗尽转 RESET。发送
 * 失败保留同一帧，不能因为重试再次执行业务。已选终局但任务尚未退出时，
 * 通过只读结果先停止网络；终局通知和业务释放仍等原任务完成。
 * RESET 接管后 Channel 独立处理确认和晚帧，不需要 RPC 保留第二条路由。
 */
int tr_rpc_record_endpoint_pump_on_owner(struct tr_rpc_record_endpoint *ep);

/* 只读所有者快照。正常宿主须已退役全部绑定；停止对象可保留固定槽，
 * 但必须已交付所有任务和终局，且终止责任已经移交 Channel。回调/进度
 * 栈或待处理事件尚未退出时返回 TR_OK 和 false，不把局部静默当成完成。
 * 不等待已转交 Channel 的网络确认或 Reactor 的 TX 引用。
 */
int tr_rpc_record_endpoint_is_drained_on_owner(
        struct tr_rpc_record_endpoint *ep, bool *out);
/* 注册立即发布一次，此后仅在静默谓词变化时发布。通知只在安全最外层
 * owner 边界执行，回调可查询，不能重入 pump/close/start 或修改观察者。
 * NULL 注销且不回调；arg 借用到注销或正常 close 的最后通知返回。
 * stopped reap 只注销，不在清理线程伪装正常 owner 进度通知。
 */
int tr_rpc_record_endpoint_set_progress_notify_on_owner(
        struct tr_rpc_record_endpoint *ep, tr_rpc_endpoint_progress_fn fn,
        void *arg);

/* 停止新准入，取消在途业务，交付已接受任务并尝试终止流。任务未静默、
 * 终止尚未接受时返回 AGAIN；外层必须继续驱动共享执行器和 Reactor。
 * 先解绑 Channel 处理器，再注销定时器。失败保持对象及剩余发布可重试。
 * 成功后原 Channel 应销毁，不可带着未被对端确认关闭的旧流换绑新宿主。
 * TX帧有独立预算引用，可能长于宿主存活；预算域须等它们释放完再销毁。
 */
int tr_rpc_record_endpoint_close_on_owner(struct tr_rpc_record_endpoint **ep);

/* 父线程已经 join 且外部排除重启/销毁。首次调用置清理域并取消任务，
 * 未完成返回AGAIN；随后由外层 stop/join/adopt_cleanup/dispatch 共享执行器，
 * 再重试。复用原恢复租约，不在错误线程冒充正常完成，不发送停止后的网络帧。
 */
int tr_rpc_record_endpoint_reap_stopped(struct tr_rpc_record_endpoint **ep);

#endif
