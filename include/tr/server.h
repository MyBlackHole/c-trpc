#ifndef TR_SERVER_H
#define TR_SERVER_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"
#include "tr/transport.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_server;

struct tr_server_config {
	struct tr_facade_limits limits;
	struct tr_connection_group_server_config connection_groups;

	/*
	 * Server 内独立的 Reactor/资源分片数；0 由初始化/默认配置归一化为 1。
	 * shard_count > 1 时，每个分片在同一服务端口上使用 SO_REUSEPORT。
	 */
	uint32_t shard_count;

	/*
	 * 所有分片合计的对端表槽位上限。已脱离或正在回收的对端不再占用
	 * 槽位，但每个分片内正在退役的 Endpoint 数同样受该分片槽位容量
	 * 限制，因此槽位复用不会演化成无界对象积压。
	 */
	uint32_t max_peers;
	/* 所有监听器合计的连接等待队列预算，由 Server 确定性拆分到各分片。 */
	int listen_backlog;

	/*
	 * DEFAULT/ENABLED 对每个已接受的门面 TCP 对端启用 TCP_NODELAY；
	 * DISABLED 保留内核默认 Nagle 行为。
	 */
	enum tr_tcp_nodelay_policy tcp_nodelay;

	/* 0 表示禁用 Transport 保活。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;

	/*
	 * 入站 TCP 接受后完成 HELLO 或连接组 TRR1 前导的总期限。
	 * 默认 10000ms；0 也采用默认值，不禁用资源回收。收到部分字节不续期。
	 * 与仅在 Channel 就绪后运行的保活机制相互独立。
	 */
	uint32_t handshake_timeout_ms;
};

void tr_server_config_init(struct tr_server_config *config);

/*
 * 只构造 V2；方法表在构造期间复制冻结，不能为空或零条。
 * 不协商、不回退 V1。此接口取代旧的两参数构造，须重新编译调用者。
 * 业务处理器使用 tr/rpc_v2.h 的统一 OPEN/MESSAGE/HALF_CLOSE 契约。
 * 正常成功时返回 TR_OK，并通过 *out 向调用方转移 Server 所有权。
 * 正常构造失败且回滚完整收敛时，*out 保持 NULL。
 *
 * 如果构造回滚本身失败，函数返回生命周期错误，同时 *out
 * 保留部分构造的 Server 的所有权；调用方不得继续 start/listen/register，
 * 应直接通过 tr_server_destroy() 重试终止收敛。
 */
struct tr_rpc_v2_method;
int tr_server_create(const struct tr_server_config *config,
                     const struct tr_rpc_v2_method *methods,
                     size_t method_count, struct tr_server **out);

/* 仅创建连接组传输门面。使用 connection_group_listen/start 等接口，
 * tr_server_listen 返回 UNSUPPORTED，不能借此打开旧 RPC 监听端口。
 * 复用普通构造的执行域和失败所有权规则。
 */
int tr_server_create_transport(const struct tr_server_config *config,
                               struct tr_server **out);

/* 门面仅接受数字形式的 IPv4 地址；port=0 表示申请临时端口。 */
int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port);

int tr_server_start(struct tr_server *server);

/*
 * 停止接收新对端，发送 GOAWAY，并同步等待已有 Stream 结束。
 *
 * 排空是外部生命周期屏障：从 Reactor 所有者回调或 RPC
 * 处理器/结果/事件回调中调用时返回 TR_ERR_STATE，且不会先执行部分
 * 停止/排空状态修改。回调需要关闭 Server 时应通知外部控制线程。
 */
int tr_server_drain(struct tr_server *server, uint32_t timeout_ms);

/*
 * 获取该 Server 所有存活及已退役对端的稳定聚合 RPC 语义生命周期快照。
 * 构造回滚失败后保留的部分 Server、已经进入终局销毁的 Server
 * 不能再读取跨分片统计，此时返回 TR_ERR_STATE。
 * V1/V2 共用此入口。V2 在外部控制线程同步读取各分片；所有 c-trpc
 * 回调及工作线程均返回 TR_ERR_STATE，避免跨 owner 自等待。
 * 成功构造但尚未 start 的 V2 Server 返回全零。跨分片采集不是全局同一
 * 时刻的原子快照，各字段仍满足调用记账等式。只有 TR_OK 修改 out；
 * V2 累计值或分片求和不能表示时返回 TR_ERR_BAD_LENGTH。
 * 与其他 Server API 一样，调用方不得与 tr_server_destroy() 并发调用。
 */
int tr_server_get_rpc_semantic_stats(
	struct tr_server *server, struct tr_rpc_semantic_stats *out);

/*
 * 同步执行最终所有权释放。
 *
 * 调用 destroy 前，调用方必须停止发起新的 Server API 调用。
 * 不允许从 c-trpc 回调中调用 destroy，包括 Reactor 所有者回调、
 * RPC 处理器/结果/事件回调。必须先通知外部控制线程并从回调返回，
 * 再销毁 Server。
 *
 * destroy 只执行终止拆除，不替代显式 tr_server_drain()。
 *
 * 返回 TR_OK 才表示 Server 存储已释放。任何错误都表示对象仍由调用方拥有，
 * 可能已经关闭部分准入/事件源；调用方解决尚未归还的所有权后可重试。
 *
 * Call 句柄和回调参数不会延长 Server 生命周期。
 */
int tr_server_destroy(struct tr_server *server);

#ifdef __cplusplus
}
#endif

#endif
