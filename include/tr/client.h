#ifndef TR_CLIENT_H
#define TR_CLIENT_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"
#include "tr/tls.h"
#include "tr/transport.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_client;

struct tr_client_config {
	struct tr_facade_limits limits;
	struct tr_connection_group_client_config connection_groups;

	uint32_t connect_timeout_ms;

	/*
	 * DEFAULT/ENABLED 在首次连接及自动重连的 TCP 套接字上启用
	 * TCP_NODELAY；DISABLED 保留内核默认 Nagle 行为。
	 */
	enum tr_tcp_nodelay_policy tcp_nodelay;

	/* 0 表示禁用 Transport 保活。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;

	/* 当前公开工厂要求 enable_reconnect==0，不透明重放。 */
	int enable_reconnect;
	uint32_t reconnect_initial_delay_ms;
	uint32_t reconnect_max_delay_ms;

	struct tr_tls_client_config tls;
};

void tr_client_config_init(struct tr_client_config *config);

/*
 * 只构造 V2；方法表在构造期间复制冻结，不能为空或零条。
 * 不协商、不回退 V1。此接口取代旧的两参数构造，须重新编译调用者。
 * 正文上限、执行域与回调契约见 tr/rpc_v2.h。
 * 正常成功时返回 TR_OK，并通过 *out 向调用方转移 Client 所有权。
 * 正常构造失败且回滚完整收敛时，*out 保持 NULL。
 *
 * 极端情况下，如果构造失败后的终止回滚自身也失败，函数返回该
 * 生命周期错误，同时 *out 保留部分构造的 Client 的所有权；调用方必须
 * 停止继续使用业务 API，并通过 tr_client_destroy() 重试收敛。
 */
int tr_client_create(const struct tr_client_config *config,
                     const struct tr_rpc_method_desc *methods,
                     size_t method_count, struct tr_client **out);

/* 仅创建连接组传输门面，不启用 RPC。使用 connection_group_* 接口，
 * tr_client_connect 返回 UNSUPPORTED；RPC 重连配置也不适用。
 * 复用 config_init/destroy 的参数、执行域与失败所有权规则。
 */
int tr_client_create_transport(const struct tr_client_config *config,
                               struct tr_client **out);

/*
 * 门面仅接受数字形式的 IPv4 地址。
 *
 * fd 被 Reactor 接管后，后续 Channel/RPC/保活/重连任一步失败都会
 * 同步执行会话回滚。只有回滚完整收敛时才返回原连接错误；
 * 回滚自身失败时返回该生命周期错误，并保留 Client 所有权与尚未收敛
 * 的内部对象，调用方应停止新的连接尝试并通过 tr_client_destroy() 重试终止。
 */
int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port);

/*
 * 同步等待连接就绪。
 * 该操作依赖 Reactor 推进，必须从外部控制线程调用；从 Reactor 所有者
 * 回调或 RPC 工作线程回调中调用时返回 TR_ERR_STATE。
 */
int tr_client_wait_ready(struct tr_client *client, uint32_t timeout_ms);

/* 调用能力与回调的完整定义及消息/终局契约见 tr/rpc_v2.h。
 * 方法表已经由 create 复制冻结，不再提供运行时旧方法注册。
 */
struct tr_rpc_v2_call;
struct tr_rpc_v2_client_callbacks;
int tr_client_call_start(struct tr_client *client, uint32_t service_id,
                         uint32_t method_id,
                         const struct tr_rpc_call_options *options,
                         const struct tr_rpc_v2_client_callbacks *callbacks,
                         struct tr_rpc_v2_call *out);
int tr_client_unary_call(struct tr_client *client, uint32_t service_id,
                         uint32_t method_id, const struct tr_rpc_bytes *request,
                         const struct tr_rpc_call_options *options,
                         const struct tr_rpc_v2_client_callbacks *callbacks,
                         struct tr_rpc_v2_call *out);

/*
 * begin_drain() 只发布单调的排空意图，可从回调内调用。
 * wait_drained() 会同步等待现有 Stream 收敛，必须从外部控制线程调用；
 * 从 Reactor 所有者回调或 RPC 工作线程回调中调用时返回
 * TR_ERR_STATE，避免等待当前执行上下文本身完成。
 */
int tr_client_begin_drain(struct tr_client *client);
int tr_client_wait_drained(struct tr_client *client, uint32_t timeout_ms);

/* 获取该 Client 的稳定 RPC 语义生命周期快照，供 RPC V2 调用方读取。
 * V2 普通线程通过同步 owner 调用读取；自身 LIVE 回调可直接读取。
 * RPC 工作线程、其他 owner 及 SHUTDOWN 回调不能同步跨域读取，返回
 * TR_ERR_STATE。尚未连接时为全零；已开始 destroy 或回滚未收敛时拒绝。
 * V2 仅 TR_OK 修改 out，累计值不能表示时返回 TR_ERR_BAD_LENGTH。
 * 调用方不得与 connect/destroy 等外部串行化生命周期操作并发调用。
 */
int tr_client_get_rpc_semantic_stats(
	struct tr_client *client, struct tr_rpc_semantic_stats *out);

/*
 * 同步执行最终所有权释放。
 *
 * 调用 destroy 前，调用方必须停止发起新的 Client API 调用。
 * 不允许从 c-trpc 回调中调用 destroy，包括 Reactor 所有者回调、
 * RPC 处理器/结果/事件回调。此类回调必须先把关闭意图交给外部控制线程，
 * 然后从回调返回。
 *
 * destroy 只执行终止拆除，不隐式执行优雅排空，也没有内部
 * 超时策略。需要优雅关闭时，调用方必须先显式 begin_drain()/wait_drained()。
 *
 * 返回 TR_OK 才表示 Client 存储已释放。任何错误都表示对象仍由调用方拥有，
 * 可能已进入部分终止状态；调用方解决尚未归还的所有权后可重试。
 *
 * Call 句柄不会延长 Client 生命周期；不得与该销毁操作并发使用，
 * 也不得在销毁完成后继续使用。
 */
int tr_client_destroy(struct tr_client *client);

#ifdef __cplusplus
}
#endif

#endif
