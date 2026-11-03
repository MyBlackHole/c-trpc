#ifndef TR_CHANNEL_H
#define TR_CHANNEL_H

#include <stdint.h>

#include "../../execution/buffer.h"
#include "../../execution/reactor.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_channel;
struct tr_memory_budget;

#define TR_CHANNEL_PROTOCOL_VERSION 3U
#define TR_CHANNEL_LANE_MASK_CONTROL (1U << 0)
#define TR_CHANNEL_LANE_MASK_BULK (1U << 1)
/* 只有显式协商此能力的专用 Channel 才发送或接收单流 RESET。 */
#define TR_CHANNEL_FEATURE_STREAM_RESET UINT64_C(1)

struct tr_channel_capabilities {
	uint16_t protocol_version;
	uint32_t lane_mask;
	uint32_t max_frame_payload_bytes;
	uint32_t max_message_bytes;
	uint64_t feature_bits;
};

/* 逻辑流量类别；映射到哪条物理连接由 Channel 策略决定。 */
enum tr_lane { TR_LANE_CONTROL = 0, TR_LANE_BULK = 1 };

enum tr_channel_role { TR_CHANNEL_CLIENT = 1, TR_CHANNEL_SERVER = 2 };

enum tr_channel_mode {
	TR_CHANNEL_SHARED_CONNECTION = 1,
	TR_CHANNEL_SPLIT_CONNECTIONS = 2
};

enum tr_channel_event {
	TR_CHANNEL_EVENT_CONTROL_DOWN = 1,
	TR_CHANNEL_EVENT_BULK_DOWN = 2,
	TR_CHANNEL_EVENT_CONTROL_UP = 3,
	TR_CHANNEL_EVENT_BULK_UP = 4,
	TR_CHANNEL_EVENT_CONTROL_GOAWAY = 5,
	TR_CHANNEL_EVENT_BULK_GOAWAY = 6,
	/* 自动重连因其所有者定时器失败而无法推进。
	 * 状态码为负；禁用或销毁仍负责回调拆除。
	 */
	TR_CHANNEL_EVENT_CONTROL_RECONNECT_FAILED = 7,
	TR_CHANNEL_EVENT_BULK_RECONNECT_FAILED = 8
};

enum tr_channel_state {
	TR_CHANNEL_RUNNING = 1,
	TR_CHANNEL_DRAINING = 2,
	TR_CHANNEL_DRAINED = 3
};

enum tr_channel_lane_state {
	TR_CHANNEL_LANE_DOWN = 0,
	TR_CHANNEL_LANE_UP = 1,
	TR_CHANNEL_LANE_RECONNECTING = 2,
	TR_CHANNEL_LANE_HANDSHAKING = 3
};

struct tr_channel_reconnect_config {
	/* V1 重连仅支持数字 IPv4 端点；address 会复制到 Channel 内部。 */
	const char *ipv4_address;

	uint16_t control_port;
	/* 0 表示复用 control_port；共享连接模式下忽略该字段。 */
	uint16_t bulk_port;

	uint32_t initial_delay_ms;
	uint32_t max_delay_ms;
	uint32_t connect_timeout_ms;
};

struct tr_channel_keepalive_config {
	uint32_t interval_ms;
	uint32_t timeout_ms;
};

enum tr_stream_event {
	TR_STREAM_EVENT_OPENED = 1,
	TR_STREAM_EVENT_REMOTE_CLOSED = 2,
	TR_STREAM_EVENT_CLOSED = 3,
	TR_STREAM_EVENT_ERROR = 4,
	TR_STREAM_EVENT_WRITABLE = 5,
	/* 双向停止已选定；任务退出与网络静默分别由上层及 CLOSED 证明。 */
	TR_STREAM_EVENT_RESET = 6,
	/* 对端未受理 OPEN，句柄已失效；不回发终止帧，原因是 NOMEM。 */
	TR_STREAM_EVENT_REFUSED = 7
};

enum tr_stream_data_disposition {
	TR_STREAM_DATA_RELEASE = 0,
	TR_STREAM_DATA_TAKE_OWNERSHIP = 1
};

struct tr_stream_handle {
	struct tr_channel *channel;
	uint32_t slot;
	uint32_t generation;
};

struct tr_channel_config {
	enum tr_channel_role role;
	enum tr_channel_mode mode;

	uint32_t max_streams;

	/* 每个流初始授予对端的接收容量，同时限制单条整消息准入长度。
	 * 绝对 WINDOW_UPDATE 归还消费信用，不改变这个固定窗口。 */
	uint64_t initial_window_bytes;

	/* 信用更新的期望批量阈值；实际值不超过 W-min(W,M)+1，
	 * W 为本地初始窗口，M 为协商消息上限，避免残余信用阻塞合法消息。 */
	uint64_t window_update_threshold_bytes;

	/* 可选的接收侧消息重组。
	 * 一个逻辑流消息可以跨多个传输 DATA 帧。
	 * reassembly_pool 为 NULL 时拒绝分片接收，调用方必须保证
	 * 一条消息能放进一个传输帧。
	 * 启用后 max_message_bytes 必须能放进 reassembly_pool 的单个缓冲区。
	 */
	uint32_t max_message_bytes;
	struct tr_buffer_pool *reassembly_pool;

	/* Channel HELLO 协议只支持版本 3；0 使用此默认值。
	 * 与帧 envelope 版本及 RPC Record 版本分别管理，不降级为旧 GOAWAY。 */
	uint16_t min_protocol_version;
	uint16_t max_protocol_version;
	uint64_t feature_bits;
	/* 必需能力必须同时被本地声明和对端协商；缺失时握手失败，
	 * 不能先发布 READY 再在终止路径静默降级。0 保持旧 Channel 配置。 */
	uint64_t required_feature_bits;
	/* 可选：Channel 固定存储计入所属 shard 的共享预算。
	 * 预算对象须存活到 Channel 成功销毁或完成 detached 收尾。 */
	struct tr_memory_budget *memory_budget;
};

typedef enum tr_stream_data_disposition (*tr_stream_data_cb)(
	struct tr_stream_handle stream, uint64_t message_id,
	struct tr_buffer *payload, void *arg);

typedef void (*tr_stream_event_cb)(struct tr_stream_handle stream,
				   enum tr_stream_event event, int status,
				   void *arg);

typedef void (*tr_channel_event_cb)(struct tr_channel *channel,
				    enum tr_channel_event event, int status,
				    void *arg);

/* 两个连接句柄必须属于同一个反应器。
 * 共享模式允许 bulk_connection == control_connection；
 * 分离模式要求两者是不同且存活的句柄。
 *
 * Channel 会为连接安装反应器处理函数。销毁前必须先停止外部使用，
 * 并确保不存在并发回调或等待者；tr_channel_destroy() 内部会关闭等待者
 * 准入、清理处理函数并等待回调静止。
 *
 * 创建成功时 *out 为完整 Channel。创建失败时通常为 NULL；若构造
 * 回滚无法确认已经解绑 Reactor 回调，则错误返回时 *out 仍可非 NULL。
 * 这时调用方独占该部分构造对象，只能执行 tr_channel_destroy()；
 * 销毁失败必须保留指针重试，不得提前释放 Reactor 或 callback_arg。
 *
 * 只有 destroy 返回 TR_OK 才表示 Channel 存储已释放。若仍存在等待者或拆除
 * 屏障无法收敛，返回错误并保持对象存活，调用方不得继续释放所属 Runtime。
 */
int tr_channel_create(const struct tr_channel_config *config,
		      struct tr_conn_handle control_connection,
		      struct tr_conn_handle bulk_connection,
		      tr_stream_data_cb data_cb,
		      tr_stream_event_cb stream_event_cb,
		      tr_channel_event_cb channel_event_cb, void *callback_arg,
		      struct tr_channel **out);

int tr_channel_destroy(struct tr_channel *channel);

/* 替换上层回调，主要用于在已有 Channel 上叠加 RPC。
 *
 * 运行中的 Channel 通过所属反应器所有者同步发布：函数返回 TR_OK 时，
 * 旧回调已不再执行，调用方可以释放旧回调所有者。
 *
 * 反应器已完全停止时允许仅用于拆除的直接发布；正在停止
 * 时返回 TR_ERR_CLOSED，禁止越过所有者拆除屏障修改回调。
 */
int tr_channel_set_handler(struct tr_channel *channel,
			   tr_stream_data_cb data_cb,
			   tr_stream_event_cb stream_event_cb,
			   tr_channel_event_cb channel_event_cb,
			   void *callback_arg);

/* 显式等待 Channel 所属反应器到达当前命令或事件静止点。
 *
 * tr_channel_set_handler() 本身已经是同步所有者发布，不要求调用方再用
 * 本函数等待旧处理函数；本 API 保留给需要独立所有者屏障的其他控制平面场景。
 */
int tr_channel_quiesce(struct tr_channel *channel);

/* 在保留逻辑 Channel 的情况下替换失败的物理连接。
 * 失败通道上的旧流不会跨替换存活，调用方必须重新创建流或 Call。
 * 共享模式替换任一通道等价于同时替换 CONTROL/BULK。
 * 新句柄必须属于该 Channel 的反应器，且已经由反应器接管
 * （状态为 RESERVED 或 ACTIVE）。
 */
int tr_channel_replace_connection(struct tr_channel *channel, enum tr_lane lane,
				  struct tr_conn_handle connection);

/* 启用 V1 客户端自动重连。
 * 连接、退避与超时全部由所属反应器的定时器与非阻塞连接器
 * 状态机推进，不为每个 Channel 创建维护线程。
 *
 * 断开时进行中的流立即失败，不做透明重放；新 TCP 连接
 * 完成后仍通过正常 HELLO 握手恢复新的流或 Call 准入。
 */
int tr_channel_enable_client_reconnect(
	struct tr_channel *channel,
	const struct tr_channel_reconnect_config *config);

int tr_channel_disable_client_reconnect(struct tr_channel *channel);

int tr_channel_enable_keepalive(
	struct tr_channel *channel,
	const struct tr_channel_keepalive_config *config);
int tr_channel_disable_keepalive(struct tr_channel *channel);

int tr_channel_get_lane_state(struct tr_channel *channel, enum tr_lane lane,
			      enum tr_channel_lane_state *out);

/* 等待指定通道完成 HELLO/HELLO_ACK 并进入 UP。该等待只观察 Channel 状态，
 * 不推进握手或重连协议。timeout_ms == 0 表示无限等待。
 * 反应器所有者上下文调用返回 TR_ERR_STATE。
 */
int tr_channel_wait_ready(
	struct tr_channel *channel, enum tr_lane lane, uint32_t timeout_ms);

/* 选定通道进入 UP 后，返回协商得到的出站上限。
 * max_frame_payload_bytes/max_message_bytes 表示对端声明的接收能力；
 * 本地接收上限仍由 Channel/Reactor 配置决定。
 */
int tr_channel_get_capabilities(struct tr_channel *channel, enum tr_lane lane,
				struct tr_channel_capabilities *out);

/* 优雅关闭：建立单调的流准入屏障，并在各就绪物理连接上发送 GOAWAY，
 * 固定该连接最后一个已接纳且已接受初始 WINDOW_UPDATE 的对端 stream_id。
 * 0 表示尚无准入；该边界不是 RPC Method/任务已执行的证明。屏障后禁止
 * 新的本地流，迟到的对端 OPEN 仅验证后丢弃；对端按边界直接退役未获
 * 准入的 OPENING 流，只通知一次 ERROR、TR_ERR_CLOSED，不回发 CLOSE。
 * 已有流继续运行，直到自然关闭或取消。
 *
 * 因此活跃流在 begin_drain() 成功建立屏障后只能保持或减少，
 * DRAINED 不会因为延迟到达的对端 OPEN 再退回 DRAINING。
 *
 * 本操作幂等。GOAWAY 使用 Connection 内嵌的终局生命周期发送槽位，
 * 不与普通控制发送池或命令环形队列容量竞争。
 * 接收 GOAWAY 会在同一物理连接上对称发布本方向的冻结边界；split 模式
 * 不因此关闭另一物理连接的准入。显式 begin_drain 才排空整个 Channel。
 */
int tr_channel_begin_drain(struct tr_channel *channel);

/* 同步等待本地流归零、存活物理连接完成双向 GOAWAY，且 Reactor 确认
 * 此前命令及 TX/终止预留全部释放。等待者由条件变量驱动；既有维护
 * 定时器负责外部最后一次关闭之后的 owner 进展。0ms 为非阻塞检查。
 *
 * 本函数是纯等待操作，不发送 GOAWAY，也不推进 Channel 协议状态。
 * GOAWAY 准入已在 begin_drain() 的所有者事务中完成。
 * 不合作的对端保留有界状态，超时返回 AGAIN，不强制释放仍被借用的对象。
 * 双 GOAWAY 且无流后不再生成 PING/PONG；晚 DATA/CLOSE 仍严格校验。
 *
 * 该进度依赖反应器所有者继续处理接收、发送和关闭或取消回调，因此
 * 所有者上下文调用返回 TR_ERR_STATE。
 *
 * 所有 Channel 状态等待者（包括 wait_ready / wait_drained）都受同一
 * 拆除准入保护。调用方仍必须停止新的外部 API；销毁或解绑
 * 会拒绝在已有等待者尚未退出时释放 Channel。
 */
int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms);

/* DRAINED 使用与 wait_drained 相同的流、双 GOAWAY 与本机 TX 谓词。
 * 发送完成只证明字节交给内核，不证明对端应用已收到响应；RPC worker、
 * 用户持有载荷及其他上层引用须由所属模块另外等待，不能据此销毁它们。
 */
int tr_channel_get_state(struct tr_channel *channel,
			 enum tr_channel_state *out);
uint32_t tr_channel_active_streams(struct tr_channel *channel);

/* 打开一个逻辑字节流；准入成功通过 OPENED 上报。对端无流槽时上报
 * REFUSED/NOMEM，精确句柄已退役且无需回复，连接仍可继续开流。 */
int tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
		   struct tr_stream_handle *out);

/* 一次写入对应一条逻辑流消息。
 * 超过反应器帧负载上限时由传输层自动切成多个 DATA 帧。
 * 所有权：TR_OK 后 payload 所有权转移给反应器，直到全部分片
 * 发送完成；其他返回值下仍归调用方。
 * TR_AGAIN 表示流控或反应器背压。
 * 已准入流上超过对端初始窗口或协商消息上限返回 BAD_LENGTH，
 * 该长度没有信用恢复路径；失败不改变发送字节/消息计数。
 */
int tr_stream_send(struct tr_stream_handle stream, struct tr_buffer *payload);

/* 分散聚集流写入。流控按所有缓冲区长度之和计费。
 * TR_OK 时每个缓冲区的所有权都转移给传输层。
 */
int tr_stream_sendv(struct tr_stream_handle stream,
		    struct tr_buffer *const *payloads, uint32_t payload_count);

/* 半关闭本地发送方向。 */
/* OPENING 尚无对端准入证明时返回 AGAIN，不发送 CLOSE，也不接管重试。
 * 调用方须等待 OPENED 后重试，或由 REFUSED/GOAWAY/连接错误退役句柄。 */
int tr_stream_close(struct tr_stream_handle stream);

/* 释放之前通过 TR_STREAM_DATA_TAKE_OWNERSHIP 保留的 payload，
 * 并把对应字节额度归还给流接收窗口。
 */
int tr_stream_release_payload(struct tr_stream_handle stream,
			      struct tr_buffer *payload);

/* 重试待处理的绝对 WINDOW_UPDATE 控制帧。 */
int tr_channel_flush(struct tr_channel *channel);

/* 获取基于字节的流控状态快照，用于诊断和测试。 */
struct tr_stream_flow_state {
	uint64_t tx_sent_bytes;
	uint64_t tx_send_limit;
	uint64_t rx_received_bytes;
	uint64_t rx_consumed_bytes;
	uint64_t rx_advertised_limit;
};

struct tr_stream_diagnostics {
	enum tr_lane lane;
	uint32_t stream_id;
	int local_open;
	int remote_open;
	uint64_t next_tx_message_id;
	uint64_t next_rx_message_id;
	struct tr_stream_flow_state flow;
};

struct tr_channel_stats {
	enum tr_channel_state state;
	enum tr_channel_lane_state control_lane_state;
	enum tr_channel_lane_state bulk_lane_state;
	uint32_t active_streams;

	uint64_t streams_opened;
	uint64_t streams_closed;
	uint64_t stream_errors;
	uint64_t messages_tx;
	uint64_t messages_rx;
	uint64_t bytes_tx;
	uint64_t bytes_rx;
	uint64_t window_updates_tx;
	uint64_t window_updates_rx;
	uint64_t reconnect_attempts;
	uint64_t reconnect_successes;

	uint64_t keepalive_pings_sent;
	uint64_t keepalive_pongs_received;
	uint64_t keepalive_timeouts;
	uint64_t control_last_rtt_ns;
	uint64_t bulk_last_rtt_ns;

	struct tr_connection_stats control_connection;
	struct tr_connection_stats bulk_connection;
};

int tr_stream_get_flow_state(struct tr_stream_handle stream,
			     struct tr_stream_flow_state *out);

int tr_stream_get_diagnostics(struct tr_stream_handle stream,
			      struct tr_stream_diagnostics *out);

int tr_channel_get_stats(struct tr_channel *channel,
			 struct tr_channel_stats *out);

#ifdef __cplusplus
}
#endif

#endif
