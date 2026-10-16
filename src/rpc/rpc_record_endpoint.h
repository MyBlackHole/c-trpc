#ifndef TR_RPC_RECORD_ENDPOINT_H
#define TR_RPC_RECORD_ENDPOINT_H

#include "rpc_record_dispatch.h"

struct tr_rpc_record_endpoint;

struct tr_rpc_record_endpoint_config {
	struct tr_rpc_record_dispatch_config dispatch;
	/* 每轮执行器完成交付和调用推进各自的条数上限。 */
	size_t work_limit;
	/* 共享执行器不允许被此端点覆盖通知钩子。定时器保证完成/资源恢复
	 * 的自动推进，1 ms..1 s；其周期成本须单独测量。 */
	uint64_t progress_interval_ns;
	/* 本地终局后的 FINAL 发送等待期限。超时不假报发送成功，只尝试
	 * 关闭发送方向；CLOSE 本身的容量背压仍须恢复后才能被接受。 */
	uint64_t final_timeout_ns;
};

struct tr_rpc_record_endpoint_result {
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	bool final_accepted;
	bool close_accepted;
	/* 精确RPC错误无法发送时保留该事实；0不代表对端已收到或执行。 */
	int transport_error;
};

typedef void (*tr_rpc_endpoint_terminal_fn)(
        struct tr_stream_handle stream,
        struct tr_rpc_record_endpoint_result result, void *arg);

/* 库内V2服务端组合；显式独占一个空闲Channel的上层handler，不与V1
 * Endpoint/其他协议混装。调用方须拥有替换handler的权限，并保证从
 * create到start之间没有其他handler安装或流打开；不恢复旧handler。
 * 物理连接失效后端点关闭新流准入；不得在端点存活时替换其连接。
 * Channel、Reactor、共享executor和方法参数均为借用，必须晚于端点回收。
 * executor完成owner须匹配Reactor；可以已启动，端点不创建/停止其线程。
 * create只分配和创建派发器，成功才发布out；start安装一个定时器及
 * Channel回调。start失败后对象仍由调用方持有，必须close或reap重试。
 *
 * 为每个传输槽预留独立FINAL头及控制块，业务预算耗尽时不再为拒绝
 * 分配内存。该存储由引用保护，能越过端点销毁存活至最后TX释放。
 */
int tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_endpoint_config *config,
        tr_rpc_endpoint_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_endpoint **out);
int tr_rpc_record_endpoint_start_on_owner(struct tr_rpc_record_endpoint *ep);

/* 用精确Stream身份取消本地调用，错误/旧代次拒绝。终局自动尝试发送
 * 非OK FINAL并半关闭。取消仍须等待业务退出后才交付terminal，不能
 * 因调用成功就释放方法参数。回调中不可重入本端点生命周期操作。
 */
int tr_rpc_record_endpoint_abort_on_owner(struct tr_rpc_record_endpoint *ep,
                                          struct tr_stream_handle stream,
                                          enum tr_rpc_status status);

/* 关闭独占Channel的新流准入，取消调用并继续有界发送终局/关闭。
 * 未静默返回AGAIN，保留对象及源；不停止借用executor。terminal在工作
 * 已收敛、最终发送已接受或明确放弃、半关闭已接受或流失效之后至多一次。
 * FINAL/CLOSE accepted都不是网络写出确认。注销失败不能释放回调参数。
 * 普通关闭不要求恶意对端主动半关闭；父Channel由调用方随后关闭。
 */
int tr_rpc_record_endpoint_close_on_owner(struct tr_rpc_record_endpoint **ep);

/* 父Reactor已join、外部排除重启/销毁/并发调用。首次先解绑端点源并
 * 授权派发器清理域，再返回AGAIN等待共享executor的统一stop/join/
 * adopt_cleanup/dispatch；重试回收端点。不冒充原owner、不发送记录。
 * 不能在首次reap前从其他线程交付此端点的任务完成。
 */
int tr_rpc_record_endpoint_reap_stopped(struct tr_rpc_record_endpoint **ep);

#endif
