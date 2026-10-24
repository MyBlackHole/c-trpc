#ifndef TR_CHANNEL_INTERNAL_H
#define TR_CHANNEL_INTERNAL_H

#include "channel.h"
#include "../../execution/deadline_internal.h"
#include "../../runtime/message_service.h"

struct tr_peer_identity_snapshot;

typedef int (*tr_channel_message_submit_fn)(
        void *context, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);
typedef int (*tr_channel_message_cancel_fn)(void *context,
                                            struct tr_task_handle handle);

/* RPC body adapter uses a value snapshot so per-method transforms cannot
 * retain pointers into Channel's lane-generation capability records. */
struct tr_channel_message_limits {
	uint32_t local_receive_algorithms;
	uint32_t local_max_encoded_wire_bytes;
	uint32_t peer_receive_algorithms;
	uint32_t peer_max_encoded_wire_bytes;
	uint32_t max_message_bytes;
	size_t max_compression_workspace_bytes;
	unsigned max_compression_window_log;
	unsigned peer_caps_ready;
};

/* Channel 创建后所属反应器不再变化，仅供内部所有者路由使用。 */
struct tr_reactor *tr_channel_reactor(struct tr_channel *channel);

/* 所属 Reactor owner 上读取本端变换上限与当前物理 lane 的对端接收许可。
 * 输出是副本；lane 不可用时返回 AGAIN，输出保持不变。 */
int tr_channel_message_limits_on_owner(struct tr_channel *channel,
                                       enum tr_lane lane,
                                       struct tr_channel_message_limits *out);

/* 所属 owner 上取得 CONTROL 连接的借用身份；需 retain 才能跨过当前回调。 */
int tr_channel_peer_identity_snapshot_on_owner(
        struct tr_channel *channel, struct tr_peer_identity_snapshot **out);

/* 仅供所有者处理上层生命周期失败：无需命令准入，同步退役当前通道的
 * 精确连接。回调内联执行；调用时不得持有 Channel 或上层协议锁。
 */
int tr_channel_abort_lane_on_owner(struct tr_channel *channel,
                                  enum tr_lane lane, int status);

/* 返回精确流代次的逻辑通道，供 RPC 入站方法校验。 */
int tr_stream_get_lane_internal(struct tr_stream_handle stream,
                               enum tr_lane *out);

/* 仅供协议终局回收：在 STREAM_CLOSE 前单调停止新增 RX 信用。
 * 必须在 Channel 所有者上调用，精确活代次的 OPENING/OPEN 均可，重复幂等。
 * 不改变双向开闭状态、已广告额度或 TX 信用；后续释放仍记入真实 consumed，
 * 但不再发送/重试 WINDOW_UPDATE。普通半关闭仍须保留接收信用，不调用本函数。
 * NULL 返回 INVALID，非所有者返回 STATE，过期句柄返回 STALE；不转移所有权。
 */
int tr_stream_stop_credit_on_owner(struct tr_stream_handle stream);

/* 调用级双向终止。仅所属 owner、精确活代次及已协商 RESET 能力可用。
 * status 仅允许 CLOSED（取消）、TIMEOUT、NOMEM、STATE、SYS；不接受 RPC
 * 状态。TR_OK 后由 Channel 保持固定状态并承担发送/重试/网络退役责任，
 * 不要求 RESET 已经写入套接字。重复请求幂等，第一次原因不被覆盖。
 *
 * 立即停止新的数据与信用并至多通知一次 RESET；收到对端 FIFO RESET
 * 后以 CLOSED 通知网络静默。上层仍独立等待自己的工作与持有引用退出。
 * 已接受普通本地 STREAM_CLOSE 后不能主动 RESET，返回 CLOSED；RPC
 * HALF_CLOSE 不属于传输半关闭。远端 RESET 仍可终止本地已半关闭的流。
 * 尚未收到初始 WINDOW_UPDATE 的 OPENING 返回 AGAIN 且不改变状态，
 * 上层保留责任，等待准入确认或 GOAWAY 受理边界/连接故障使流退役。
 * 不转移应用载荷所有权；未成功取得维护定时器责任时返回错误且不选终局。
 */
int tr_stream_reset_on_owner(struct tr_stream_handle stream, int status);

/* 有界 Channel 索引与其测试共用的流线协议标识哈希。 */
static inline uint32_t tr_channel_stream_id_hash(uint32_t stream_id)
{
	uint32_t value = stream_id;

	value ^= value >> 16;
	value *= UINT32_C(0x7feb352d);
	value ^= value >> 15;
	value *= UINT32_C(0x846ca68b);
	value ^= value >> 16;
	return value;
}

/* 创建后固定的流槽位数，仅供内部预分配索引使用。 */
uint32_t tr_channel_max_streams(struct tr_channel *channel);

/* 服务端门面使用延后创建，先完成 RPC Endpoint/Method 安装，
 * 再启动 HELLO 握手，避免对端在服务层就绪前发送 RPC 数据。
 * 构造失败可能留下必须由调用方保留并重试销毁的 *out；
 * 和 tr_channel_create() 采用相同的部分对象所有权契约。
 */
int tr_channel_create_deferred(
	const struct tr_channel_config *config,
	struct tr_conn_handle control_connection,
	struct tr_conn_handle bulk_connection,
	tr_stream_data_cb data_cb,
	tr_stream_event_cb stream_event_cb,
	tr_channel_event_cb channel_event_cb,
	void *callback_arg,
	struct tr_channel **out);

/* Deferred Client/Server Channel 在 HELLO 前绑定其 Runtime 分片消息服务入口。
 * 服务反应器必须与 Channel 相同；submit 在 Channel owner 上调用。context 和
 * 服务必须长于 Channel。服务可未启用，此时任务提交返回 CLOSED；不能据此发布
 * 非零接收能力。只能在 deferred Channel 启动握手前绑定一次。
 */
int tr_channel_bind_message_service_on_owner(
        struct tr_channel *channel, struct tr_reactor *service_reactor,
        tr_channel_message_submit_fn submit,
        tr_channel_message_cancel_fn cancel, void *context);

/* 仅 Channel 所有者可调用；参数和所有权语义原样转交给已绑定服务。
 * 未绑定返回 STATE，开始拆除后返回 CLOSED；不持有 Channel 锁调用服务。
 */
int tr_channel_message_submit_on_owner(
        struct tr_channel *channel, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);
int tr_channel_message_cancel_on_owner(struct tr_channel *channel,
                                       struct tr_task_handle task);

/* 仅供门面的套接字策略注入。通过 Channel 所属反应器所有者串行化，
 * 必须在自动重连启动前设置；否则底层 Channel 重连保留
 * 内核默认值。
 */
int tr_channel_set_reconnect_tcp_nodelay(struct tr_channel *channel,
                                         int enabled);

/* 内部生命周期观察者独立于 RPC 使用的上层 Channel 处理函数。
 * 发布与读取都在 Channel 所属反应器所有者上完成，不属于
 * channel->lock 保护域。观察者在普通处理函数之后收到 Channel 事件，
 * 且只能执行短小、非阻塞的通知工作。
 */
int tr_channel_set_lifecycle_observer(struct tr_channel *channel,
				      tr_channel_event_cb event_cb,
				      void *callback_arg);

/* 服务端对端拆除分成两个阶段：
 * - detach_for_finalize()：由所有者串行执行，移除全部反应器或定时器回调
 *   源，使 Channel 不再能被协议分派触达；
 * - finalize_detached()：只做不依赖所有者的内存与资源释放，必须在解绑
 *   成功后才能调用。
 *
 * detach_for_finalize() 是可重试生命周期屏障。处理函数或定时器解绑失败时
 * Channel 继续由调用方拥有，并保留所有尚未确认解除的发布；只有返回
 * TR_OK 才会发布 teardown_detached，并允许进入收尾。
 */
int tr_channel_detach_for_finalize(struct tr_channel *channel);
/* detach_for_finalize() 成功后，所有可失败生命周期屏障已经完成。
 * finalize_detached() 只做不依赖所有者的本地资源释放，语义上不可失败。
 */
void tr_channel_finalize_detached(struct tr_channel *channel);

/* 供客户端连接使用；TCP 与 HELLO 共享上游创建的截止时间。 */
int tr_channel_wait_ready_deadline(
	struct tr_channel *channel, enum tr_lane lane,
	const struct tr_deadline *deadline);

/* 在已启动排空屏障的 Channel 上等待。deadline 为同一操作共享的
 * 有限 CLOCK_MONOTONIC 绝对截止时间；NULL 表示只检查当前状态、不阻塞。
 * 到期时仍返回 TR_AGAIN，与公开 wait_drained 的返回码契约一致。
 */
int tr_channel_wait_drained_deadline(
	struct tr_channel *channel, const struct tr_deadline *deadline);

int tr_channel_start(struct tr_channel *channel);

#endif
