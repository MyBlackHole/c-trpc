#ifndef TR_TRANSPORT_H
#define TR_TRANSPORT_H

#include <stdint.h>

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
 * 稳定的 Connection Group 语义观测数据。
 *
 * 这些结构有意排除 Reactor 槽位、路由代次、队列占用、
 * 解析器资源池以及其他实现层诊断信息。
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
 * bytes 仅在回调执行期间有效。返回 TAKE_OWNERSHIP 会把底层接收 Buffer
 * 转移给应用：应原样复制该描述符，并且只使用
 * tr_connection_group_message_release() 释放一次。
 * _private 是不透明的释放能力，应用不得读取或修改。
 * 所有保留消息都必须在所属 tr_server 销毁前释放。
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
 * 回调运行在 Server 所属的 I/O 执行域中。
 * 不得阻塞等待必须依赖同一个 Server/Reactor 才能推进的工作。
 */
typedef int (*tr_connection_group_authorize_cb)(
	const struct tr_connection_group_id *group, void *arg);

typedef enum tr_connection_group_message_disposition
(*tr_connection_group_message_cb)(
	const struct tr_connection_group_message *message, void *arg);

typedef void (*tr_connection_group_data_event_cb)(
	const struct tr_connection_group_id *group,
	enum tr_connection_group_data_event event, int status, void *arg);

/*
 * Client 侧 TRANSFER_READY 通知。
 *
 * 精确的 Stream -> DATA 成员亲和关系安装完成后，
 * 回调在 Client 所属的 I/O 执行域中运行。应用只能看到语义上的
 * group/stream 标识；DATA 索引和代次保持内部化。
 *
 * 不得阻塞等待必须依赖同一个 Client/Reactor 所有者才能推进的工作。
 */
typedef void (*tr_connection_group_transfer_ready_cb)(
	const struct tr_connection_group_id *group, uint32_t stream_id,
	uint64_t message_id, void *arg);

/*
 * 可选的 Server 侧通用 Connection Group 能力。
 *
 * max_groups == 0 表示禁用该能力，并且不预留额外连接槽位。
 * 启用后，所有容量参数都是明确的语义边界。
 * 当前 V1 门面把 Group listener 绑定到一个内部所有者执行域；
 * Reactor 或 shard 句柄都不会成为公开契约的一部分。
 */
struct tr_connection_group_server_config {
	uint32_t max_groups;
	uint32_t max_connections;
	uint32_t max_data_connections_per_group;
	uint32_t max_streams_per_group;

	tr_connection_group_authorize_cb authorize;
	tr_connection_group_message_cb on_message;
	tr_connection_group_data_event_cb on_data_event;
	void *callback_arg;
};

void tr_connection_group_server_config_init(
	struct tr_connection_group_server_config *config);

/*
 * 可选的 Client 侧 DATA 通道预算。
 *
 * max_data_connections == 0 保持仅 CONTROL 的行为：DATA_OFFER 在内部取消。
 * 非零值允许自动建立 DATA socket，数量最多达到该语义边界。
 * 路由和成员代次保持内部化，不会成为应用能力。
 */
struct tr_connection_group_client_config {
	uint32_t max_data_connections;

	/*
	 * 同时安装的 Stream -> DATA 传输亲和关系上限。
	 * 启用 DATA 通道时，0 表示继承 tr_client_config.limits.max_streams，
	 * 以保持第 7 阶段之前的默认行为。这是应用可见的语义并发上限，
	 * 不是哈希表容量调节参数。
	 */
	uint32_t max_active_transfers;

	/*
	 * 可选的 READY 回调。即使这里为 NULL，亲和状态仍会安装，
	 * 因此应用也可以通过带外方式协调 Stream 标识。
	 */
	tr_connection_group_transfer_ready_cb on_transfer_ready;
	void *callback_arg;
};

void tr_connection_group_client_config_init(
	struct tr_connection_group_client_config *config);

/*
 * Client 侧通用 Connection Group 生命周期。
 *
 * V1 为每个 tr_client 保留一个活动 Group。connect() 建立 CONTROL TCP 连接，
 * 提交内部路由标识，并把 socket 所有权转移给 Client 的单一所有者执行域。
 * Reactor、shard 和成员代次仍然属于实现细节。
 *
 * 当 Client 配置启用 DATA 通道时，DATA_OFFER 在内部消费：
 * Client 在同一所有者执行域上建立对应 DATA socket，
 * 并提交精确的路由能力，而不暴露索引或代次。
 * max_data_connections == 0 时，DATA_OFFER 在内部取消。
 */
int tr_client_connection_group_connect(
	struct tr_client *client, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *group);
int tr_client_connection_group_close(struct tr_client *client);

/*
 * Client Group 优雅排空。
 *
 * begin_drain() 是本地准入屏障：它停止建立新收到的 DATA 通道，
 * 并且不再安装屏障之后观察到的 TRANSFER_READY。
 * begin_drain() 之前已经 READY 的传输仍可使用，以便应用完成现有工作。
 * Server 独立拥有自己的传输亲和关系，也可能释放之前已经发出的延迟 READY。
 *
 * 当不存在活动传输亲和关系、DATA 发送路径不再持有载荷字节，
 * 且没有待建立的 DATA 连接时，wait_drained() 返回 TR_OK。
 * timeout_ms == 0 表示无限等待。
 * 不要在 Client 所属 I/O 执行域的回调中调用 wait_drained()。
 */
int tr_client_connection_group_begin_drain(struct tr_client *client);
int tr_client_connection_group_wait_drained(
	struct tr_client *client, uint32_t timeout_ms);
int tr_client_connection_group_get_stats(
	struct tr_client *client,
	struct tr_connection_group_client_stats *out);

/*
 * 应用层 Stream 生命周期结束后，释放一个 Client 侧逻辑传输亲和关系。
 * Server 侧亲和关系由 tr_server_connection_group_release_transfer()
 * 独立释放。
 */
int tr_client_connection_group_release_transfer(
	struct tr_client *client, uint32_t stream_id);

/*
 * 在已经 READY 的 Client 传输上发送一条逻辑 DATA 消息。
 *
 * bytes 只在本次调用期间借用；返回 TR_OK 时，门面已经把数据复制到
 * 有界的内部发送所有权中，因此应用可以立即复用或释放自己的内存。
 * TR_AGAIN 表示有界发送准入失败或 Reactor TX 资源池暂时已满；
 * 此时不会转移应用所有权，调用方可以稍后重试。
 *
 * 精确的 DATA 通道仅根据已有的 Stream 亲和关系选择。
 * DATA 索引、代次以及 FIRST/LAST 分片规则保持内部化。
 */
int tr_client_connection_group_send(
	struct tr_client *client, uint32_t stream_id, uint64_t message_id,
	const struct tr_transport_bytes *bytes);

/*
 * Group listener 的生命周期由 tr_server 拥有。
 *
 * listen() 必须在 tr_server_start() 之前配置。
 * Server 可以只启动该 Group listener，不要求同时存在 RPC listener。
 * stop() 停止接受新的 Group，并关闭当前软状态 Group 连接。
 * tr_server_drain()/destroy() 也会停止该 listener。
 */
int tr_server_connection_group_listen(
	struct tr_server *server, const char *ipv4_address, uint16_t port,
	int backlog, uint16_t *out_bound_port);
int tr_server_connection_group_stop(struct tr_server *server);

/*
 * Server Group 优雅排空。
 *
 * begin_drain() 停止接受新的 Group，并拒绝创建新的 DATA_OFFER /
 * TRANSFER_READY，同时保留已有连接和传输流量。
 * release_transfer() 仍然可用，使活动工作能够自然静止。
 *
 * 所有已有 Group 连接自然消失后，wait_drained() 返回 TR_OK。
 * timeout_ms == 0 表示无限等待；stop() 仍然是立即强制关闭操作。
 */
int tr_server_connection_group_begin_drain(struct tr_server *server);
int tr_server_connection_group_wait_drained(
	struct tr_server *server, uint32_t timeout_ms);
int tr_server_connection_group_get_stats(
	struct tr_server *server,
	struct tr_connection_group_server_stats *out);

/* 针对一个已接受 Group 的 CONTROL 控制面操作。 */
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
 * 释放 on_message 返回 TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP
 * 后保留的消息。
 */
int tr_connection_group_message_release(
	struct tr_connection_group_message *message);

#ifdef __cplusplus
}
#endif

#endif
