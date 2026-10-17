#ifndef TR_SERVER_RECORD_INTERNAL_H
#define TR_SERVER_RECORD_INTERNAL_H

#include "tr/server.h"
#include "facade_tuning_internal.h"
#include "rpc/rpc_record_endpoint.h"

/* 库内显式 V2 调优入口，返回既有 Server；不安装为 SDK，不协商或回退到 V1。
 * 公开 tr_server_create_v2 在组合根推导本配置，不公开这些资源布局。
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
 * work 与 v2_work 恰一启用；后者复用相同派发状态，并在完整有效 OPEN
 * 准入后承担可选 v2_terminal 的一次通知，通知等待已提交工作实际退出。
 *
 * 成功后使用 tr_server_listen/start/drain/destroy。创建失败的所有权沿用
 * tr_server_create；start 仅尝试一次，失败后只允许 destroy 重试。destroy
 * 可以返回 AGAIN，表示任务尚未静默；保留 Server 与全部借用参数后重试。
 * 停止后先 join 父 owner，再在清理域取消/交付任务，不发送迟到成功。
 * 所有生命周期操作必须由外部线程串行调用，业务/owner 回调中拒绝。
 * destroy 及后续重试须在同一外部控制线程执行，保持执行器最终销毁
 * 失败后的完成所有者身份；不能在同步原语部分销毁后重新转移该责任。
 *
 * drain 先冻结对端表及新流准入，再等原调用的 worker、完成交付、终局
 * 通知和绑定静默，最后等待 Channel 的双 GOAWAY、流与 TX 排空。RESET
 * 已归还网络槽不代表业务已经退出；不合作任务会保留借用存储至等待
 * 超时。所有分片和等待层共享一个单调绝对期限，0ms 仅立即检查。
 * owner 主动发布静默状态并唤醒外部条件变量，不由等待线程轮询任务。
 *
 * 不接受 interceptor 或连接组配置；旧方法注册与稳定 RPC 语义统计返回
 * UNSUPPORTED。内部诊断仅报告真实 peer/Channel/池与共享任务资源，V1
 * RPC 生命周期字段无 V2 含义。tuning 的 executor_* 与
 * rpc_message_pool_count 在此入口不使用。完整有效 OPEN 携带的非零
 * remaining_timeout_ns 从接收时转换为本机单调期限，覆盖任务排队、
 * 业务执行和回复背压；0 表示无限。期限到达取消原任务令牌，可先用
 * RESET 停止网络，待真实任务退出后交付终局，不抢占不合作的业务。
 * 接收对端 RESET 同样取消原任务，不停止其他调用共享的执行器。
 * 分段 OPEN 尚未完整验证
 * 前的等待不计入此期限，错误 FINAL 也不保证送达。
 * 当前只支持 NONE；TLS、V1 Client 互通和公开默认切换仍未提供。
 */
int tr_server_create_record(const struct tr_server_config *config,
                            const struct tr_facade_tuning *tuning,
                            const struct tr_server_record_config *record,
                            const struct tr_rpc_dispatch_method *methods,
                            size_t method_count, struct tr_server **out);

#endif
