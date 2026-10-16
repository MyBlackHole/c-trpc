#ifndef TR_RPC_RECORD_ENDPOINT_H
#define TR_RPC_RECORD_ENDPOINT_H

#include "rpc_record_dispatch.h"

struct tr_rpc_record_endpoint;

struct tr_rpc_record_endpoint_config {
	struct tr_rpc_record_dispatch_config dispatch;
	size_t completion_limit;
	size_t progress_limit;
	/* 单个已有 Reactor 定时器同时推进完成、发送和资源压力恢复。
	 * 显式周期 1ms..1s；不承诺零轮询成本或单回调的 CPU 时间上限。 */
	uint64_t progress_period_ns;
	/* 本地终局选择后等待 FINAL 接受的最长时间。到期停止重试正文，
	 * 转而只重试传输半关闭；不是业务执行期限，也不保证实时抢占。 */
	uint64_t final_wait_ns;
};

struct tr_rpc_endpoint_result {
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	bool final_accepted;
};

typedef void (*tr_rpc_endpoint_terminal_fn)(
        struct tr_stream_handle stream, struct tr_rpc_endpoint_result result,
        void *arg);

/* 库内 V2 服务端宿主，尚未替换旧公开 Endpoint。只能绑定专用且尚未开始
 * 交付流事件的 Channel（例如 deferred Channel）。start 安装其唯一处理器，
 * 不保存/恢复未知的旧处理器。Channel、executor、预算皆借用；外层必须在
 * 本宿主关闭后再销毁它们，禁止并发替换处理器。executor 的完成所有者须为
 * 此 Reactor；宿主会 dispatch 共享执行器，不负责创建、停止或销毁工作池。
 *
 * create 不发布事件源，失败保持 *out 为 NULL。start 任一步失败都保留
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

/* 同时由数据、可写及定时器驱动；每次工作量受显式数量配置限制。
 * 本地拒绝自动尝试错误 FINAL，接受后再关流。发送失败保留同一帧，不能
 * 因为重试再次执行业务。到期或不可用时可只关流，因此不能承诺错误必达。
 */
int tr_rpc_record_endpoint_pump_on_owner(struct tr_rpc_record_endpoint *ep);

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
