#ifndef TR_SERVER_RECORD_INTERNAL_H
#define TR_SERVER_RECORD_INTERNAL_H

#include "tr/server.h"
#include "facade_tuning_internal.h"
#include "rpc/rpc_record_endpoint.h"

/* 库内显式 V2 入口，返回既有 Server；不安装为 SDK，不协商或回退到 V1。
 * endpoint 是每对端的有界记录宿主配置；tasks 与 memory_budget_bytes 是
 * 全 Server 总额，按分片确定分摊，不因增加分片隐式放大。每个分片至少
 * 一个工作线程，所有对端共享该分片的既有 task_executor。
 *
 * 硬预算覆盖已参与 Runtime 记账的 Reactor、对端表、分片池、任务和记录
 * 存储；不等同于进程 RSS、线程栈或尚未迁移的 Channel/Server 元数据。
 * limits.max_message_bytes 是整条传输记录的上限；本阶段要求它和初始
 * 窗口至少容纳 48 字节固定头及最大控制元数据，并容纳每个方法最大
 * 正文加固定头。对端另有自身接收限制；未解决任意 L>W 的发送活性。
 * 更新阈值 T 还须满足 T <= W-min(W,max_message_bytes)+1，W 为初始
 * 窗口，避免已消费但尚未通告的信用卡住下一条完整记录。
 */
struct tr_server_record_config {
	struct tr_rpc_record_endpoint_config endpoint;
	struct tr_task_executor_config tasks;
	uint64_t memory_budget_bytes;
};

/* 方法描述与工作函数表在创建时复制并冻结，arg 借用至 destroy 成功。
 * 业务契约沿用 tr_rpc_record_work_fn：每调用 FIFO、跨调用可并发，reply
 * 仅在当前工作回调内有效；不提供旧 Call 弱句柄或长期异步回复能力。
 *
 * 成功后使用 tr_server_listen/start/drain/destroy。创建失败的所有权沿用
 * tr_server_create；start 仅尝试一次，失败后只允许 destroy 重试。destroy
 * 可以返回 AGAIN，表示任务尚未静默；保留 Server 与全部借用参数后重试。
 * 停止后先 join 父 owner，再在清理域取消/交付任务，不发送迟到成功。
 * 所有生命周期操作必须由外部线程串行调用，业务/owner 回调中拒绝。
 * destroy 及后续重试须在同一外部控制线程执行，保持执行器最终销毁
 * 失败后的完成所有者身份；不能在同步原语部分销毁后重新转移该责任。
 *
 * 不接受 interceptor 或连接组配置；旧方法注册与稳定 RPC 语义统计返回
 * UNSUPPORTED。内部诊断仅报告真实 peer/Channel/池与共享任务资源，V1
 * RPC 生命周期字段无 V2 含义。tuning 的 executor_* 与
 * rpc_message_pool_count 在此入口不使用。当前只支持 NONE；TLS、RPC
 * 截止时间调度、V1 Client 互通和公开默认切换仍未提供。
 */
int tr_server_create_record(const struct tr_server_config *config,
                            const struct tr_facade_tuning *tuning,
                            const struct tr_server_record_config *record,
                            const struct tr_rpc_dispatch_method *methods,
                            size_t method_count, struct tr_server **out);

#endif
