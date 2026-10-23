#ifndef TR_TRANSPORT_H
#define TR_TRANSPORT_H

#include <stdint.h>

#include "tr/peer_identity.h"
#include "tr/tls.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_server;
struct tr_client;

struct tr_connection_group_id {
	uint64_t group_id;
	uint64_t epoch;
};

/*
 * 稳定的连接组语义观测。
 *
 * 这些结构有意排除 Reactor 槽位、路由代次、队列占用、解析器资源池
 * 及其他实现诊断信息。
 */
struct tr_connection_group_client_stats {
	struct tr_connection_group_id group;
	uint32_t control_connected;
	uint32_t draining;
	uint32_t data_connections;
	uint32_t active_transfers;
	uint32_t active_transfer_limit;
	uint64_t send_bytes_inflight;
	uint64_t send_bytes_limit;
};

struct tr_connection_group_server_stats {
	uint32_t draining;
	uint32_t groups_current;
	uint32_t groups_peak;
	uint32_t connections_current;
	uint32_t connections_peak;
	uint32_t data_connections_current;
	uint64_t active_transfers;
	uint64_t control_accepts;
	uint64_t data_accepts;
	uint64_t route_rejections;
	uint64_t capacity_rejections;
};

struct tr_transport_bytes {
	const uint8_t *data;
	uint32_t len;
};

#define TR_CONNECTION_GROUP_DATA_FIRST (1U << 0)
#define TR_CONNECTION_GROUP_DATA_LAST (1U << 1)

/*
 * DATA 接收描述符。
 *
 * bytes 在回调期间有效。返回 TAKE_OWNERSHIP 会将底层 RX 缓冲区转移
 * 给应用：原样复制此描述符，并通过 tr_connection_group_message_release()
 * 恰好释放一次。_private 是不透明的释放能力句柄，应用绝不能检查或修改。
 * 所属 tr_server 销毁前，必须释放全部保留消息。
 */
#define TR_CONNECTION_GROUP_MESSAGE_PRIVATE_WORDS 2U
struct tr_connection_group_message {
	struct tr_connection_group_id group;
	uint32_t stream_id;
	uint32_t flags;
	uint64_t message_id;
	struct tr_transport_bytes bytes;
	uintptr_t _private[TR_CONNECTION_GROUP_MESSAGE_PRIVATE_WORDS];
};

enum tr_connection_group_message_disposition {
	TR_CONNECTION_GROUP_MESSAGE_RELEASE = 0,
	TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP = 1
};

enum tr_connection_group_data_event {
	TR_CONNECTION_GROUP_DATA_CLOSED = 1,
	TR_CONNECTION_GROUP_DATA_ERROR = 2
};

/*
 * CONTROL 与 DATA 路由在 TLS 认证后分别调用授权回调。身份 URI 视图只在
 * 当前回调期间借用有效；明文连接的身份认证类型为 NONE。回调在 Server
 * 所属的 I/O 域执行，不得阻塞等待需要同一个 Server/Reactor 推进的工作。
 */
enum tr_connection_group_role {
	TR_CONNECTION_GROUP_ROLE_CONTROL = 1,
	TR_CONNECTION_GROUP_ROLE_DATA = 2
};

enum tr_connection_group_authorization_decision {
	TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW = 1,
	TR_CONNECTION_GROUP_AUTHORIZATION_DENY = 2
};

/* CONTROL 的 control_identity 为 NONE；DATA 使用活动 CONTROL 会话身份。 */
struct tr_connection_group_authorization {
	struct tr_connection_group_id group;
	enum tr_connection_group_role role;
	struct tr_peer_identity_view peer_identity;
	struct tr_peer_identity_view control_identity;
	uint64_t tls_profile_generation;
	uint64_t deadline_monotonic_ns;
};

typedef int (*tr_connection_group_authorize_cb)(
        const struct tr_connection_group_authorization *authorization,
        void *arg);
/* 异步 provider 回调运行在连接组 Reactor owner，必须快速返回且不得阻塞。
 * authorization 与其中的身份视图仅在该回调期间借用；provider 应复制需要的
 * 字段。每个 operation 最终必须调用 authorize_complete 一次。
 */
typedef void (*tr_connection_group_authorize_async_cb)(
        const struct tr_connection_group_authorization *authorization,
        struct tr_tls_provider_operation operation, void *arg);
/* 收到取消通知后，provider 必须停止访问原始请求数据，并通过
 * authorize_complete 确认该 operation；确认前服务端保留其有界槽位。
 */
typedef void (*tr_connection_group_authorize_cancel_cb)(
        struct tr_tls_provider_operation operation, void *arg);

typedef enum tr_connection_group_message_disposition
(*tr_connection_group_message_cb)(
	const struct tr_connection_group_message *message, void *arg);

typedef void (*tr_connection_group_data_event_cb)(
	const struct tr_connection_group_id *group,
	enum tr_connection_group_data_event event, int status, void *arg);

/*
 * 客户端 TRANSFER_READY 通知。
 *
 * 精确的 Stream -> DATA 成员亲和关系安装完成后，回调在 Client 所属
 * 的 I/O 域运行。应用只可见组/流的语义标识；DATA 索引/代次仍属于内部细节。
 *
 * 不得阻塞等待需要同一个 Client/Reactor 所有者推进才能完成的工作。
 */
typedef void (*tr_connection_group_transfer_ready_cb)(
	const struct tr_connection_group_id *group, uint32_t stream_id,
	uint64_t message_id, void *arg);

/*
 * 可选的服务端通用连接组能力。
 *
 * max_groups == 0 时保持禁用，且不预留额外连接槽位。
 * 启用后，所有容量都是显式的语义上限。
 * 当前门面将组监听器绑定到一个内部所有者域；
 * Reactor/分片句柄不会成为公开契约的一部分。
 */
struct tr_connection_group_server_config {
	uint32_t max_groups;
	uint32_t max_connections;
	uint32_t max_data_connections_per_group;
	uint32_t max_streams_per_group;

	tr_connection_group_authorize_cb authorize;
	tr_connection_group_authorize_async_cb authorize_async;
	tr_connection_group_authorize_cancel_cb authorize_cancel;
	tr_connection_group_message_cb on_message;
	tr_connection_group_data_event_cb on_data_event;
	void *callback_arg;
};

void tr_connection_group_server_config_init(
	struct tr_connection_group_server_config *config);

/*
 * 可选的客户端 DATA 通道预算。
 *
 * max_data_connections == 0 时保留仅 CONTROL 的行为：在内部取消
 * DATA_OFFER。非零值启用 DATA 套接字自动建立，并以该值作为语义上限。
 * 路由/成员代次保持内部化，不成为应用能力句柄。
 */
struct tr_connection_group_client_config {
	uint32_t max_data_connections;

	/*
	 * 同时安装的 Stream -> DATA 传输亲和关系的数量上限。
	 * 启用 DATA 通道时，0 继承 tr_client_config.limits.max_streams，
	 * 保留第 7 阶段之前的默认值。这是应用可见的进行中传输数量语义上限，
	 * 不是哈希表容量调节参数。
	 */
	uint32_t max_active_transfers;

	/*
	 * 可选的 READY 回调。为 NULL 时仍会安装亲和关系状态，
	 * 因此应用可以在带外协调 Stream 标识。
	 */
	tr_connection_group_transfer_ready_cb on_transfer_ready;
	void *callback_arg;
};

void tr_connection_group_client_config_init(
	struct tr_connection_group_client_config *config);

/*
 * 客户端通用连接组生命周期。
 *
 * 每个 tr_client 保留一个活动组。connect() 建立 CONTROL
 * TCP 连接，提交内部路由标识，并将套接字所有权转移给 Client 的单一
 * 所有者域。Reactor/分片/成员代次仍属于实现细节。
 *
 * 客户端配置启用 DATA 通道时，在内部消费 DATA_OFFER：Client 在同一
 * 所有者域建立匹配的 DATA 套接字，并提交精确的路由能力句柄，
 * 不暴露索引/代次。max_data_connections == 0 时，在内部取消邀请。
 */
int tr_client_connection_group_connect(
	struct tr_client *client, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *group);
int tr_client_connection_group_close(struct tr_client *client);

/*
 * 客户端连接组优雅排空。
 *
 * begin_drain() 是本地准入屏障：停止建立新邀请的 DATA 通道，且不安装
 * 屏障后观察到的 TRANSFER_READY 消息对应的亲和关系。
 * begin_drain() 前已处于 READY 的传输仍可使用，以便应用工作完成。
 * Server 独立拥有其传输亲和关系，并可能释放先前已发出但迟到的 READY。
 *
 * 不存在活动传输亲和关系、DATA TX 仍拥有的载荷字节或待完成的 DATA
 * 建立操作时，wait_drained() 返回 TR_OK。等待由所有者发布的生命周期
 * 代次事件驱动；它不轮询所有者统计，也不成为第二个协议状态所有者。
 * timeout_ms == 0 表示无限等待。wait_drained() 是外部阻塞式生命周期等待：
 * 从 Reactor 所有者回调或 RPC 工作线程回调调用时返回 TR_ERR_STATE。
 * begin_drain() 仍可从回调中安全调用，以发布排空意图。
 */
int tr_client_connection_group_begin_drain(struct tr_client *client);
int tr_client_connection_group_wait_drained(
	struct tr_client *client, uint32_t timeout_ms);
int tr_client_connection_group_get_stats(
	struct tr_client *client,
	struct tr_connection_group_client_stats *out);

/*
 * 应用层 Stream 生命周期结束后，释放一个客户端逻辑传输亲和关系。
 * 服务端亲和关系由 tr_server_connection_group_release_transfer()
 * 独立释放。
 */
int tr_client_connection_group_release_transfer(
	struct tr_client *client, uint32_t stream_id);

/*
 * 通过客户端已处于 READY 的传输发送一条逻辑 DATA 消息。
 *
 * bytes 仅在本次调用期间借用；返回 TR_OK 时，门面已经把字节复制到
 * 有界的内部发送存储并接管其所有权，因此应用可以立即复用或释放原内存。
 * TR_AGAIN 表示有界发送准入或 Reactor TX 资源池暂时已满；
 * 应用所有权不发生转移，调用方可以稍后重试。
 *
 * 精确 DATA 通道仅根据既有流亲和关系选择。
 * DATA 索引/代次及 FIRST/LAST 分片仍属于内部细节。
 */
int tr_client_connection_group_send(
	struct tr_client *client, uint32_t stream_id, uint64_t message_id,
	const struct tr_transport_bytes *bytes);

/*
 * 组监听器的生命周期由 tr_server 拥有。
 *
 * listen() 在 tr_server_start() 前配置。Server 可以仅启动此组监听器，
 * 不要求同时存在 RPC 监听器。
 * stop() 停止接收新组并关闭当前的软状态组连接。
 * tr_server_drain()/destroy() 同样会停止此监听器。
 */
int tr_server_connection_group_listen(
	struct tr_server *server, const char *ipv4_address, uint16_t port,
	int backlog, uint16_t *out_bound_port);
int tr_server_connection_group_stop(struct tr_server *server);

/*
 * 服务端连接组优雅排空。
 *
 * begin_drain() 停止接收新组并拒绝新建 DATA_OFFER / TRANSFER_READY，
 * 同时保留既有连接及传输流量。release_transfer() 仍可用，以便活动工作静默。
 *
 * 全部既有组连接自然消失后，wait_drained() 返回 TR_OK。
 * 等待由所有者发布的监听器生命周期代次事件驱动；
 * 它不轮询所有者计数器，也不成为第二个状态所有者。
 * timeout_ms == 0 表示无限等待。这是外部阻塞式生命周期等待：
 * 从 Reactor 所有者回调或 RPC 工作线程回调调用时返回 TR_ERR_STATE。
 * begin_drain() 仍可从回调中安全调用；stop() 仍是立即强制关闭操作。
 */
int tr_server_connection_group_begin_drain(struct tr_server *server);
int tr_server_connection_group_wait_drained(
	struct tr_server *server, uint32_t timeout_ms);
int tr_server_connection_group_get_stats(
	struct tr_server *server,
	struct tr_connection_group_server_stats *out);

/* 针对一个已接受组的 CONTROL 平面操作。 */
int tr_server_connection_group_send_data_offer(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint64_t message_id);
int tr_server_connection_group_send_transfer_ready(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint32_t stream_id, uint64_t message_id);
int tr_server_connection_group_release_transfer(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint32_t stream_id);

/*
 * 释放从 on_message 返回 TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP
 * 而保留的消息。
 */
int tr_connection_group_message_release(
	struct tr_connection_group_message *message);

#ifdef __cplusplus
}
#endif

#endif
