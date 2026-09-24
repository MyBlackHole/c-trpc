#ifndef TR_CHANNEL_H
#define TR_CHANNEL_H

#include <stdint.h>

#include "tr/buffer.h"
#include "tr/reactor.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_channel;

#define TR_CHANNEL_PROTOCOL_VERSION 1U
#define TR_CHANNEL_LANE_MASK_CONTROL (1U << 0)
#define TR_CHANNEL_LANE_MASK_BULK (1U << 1)

struct tr_channel_capabilities {
	uint16_t protocol_version;
	uint32_t lane_mask;
	uint32_t max_frame_payload_bytes;
	uint32_t max_message_bytes;
	uint64_t feature_bits;
};

/* 逻辑 traffic class；映射到哪条物理 connection 由 Channel policy 决定。 */
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
	TR_CHANNEL_EVENT_BULK_GOAWAY = 6
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
	/* V1 reconnect 仅支持数字 IPv4 endpoint；address 会复制到 Channel 内部。 */
	const char *ipv4_address;

	uint16_t control_port;
	/* 0 表示复用 control_port；shared-connection 模式下忽略该字段。 */
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
	TR_STREAM_EVENT_WRITABLE = 5
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

	/* 每个 Stream 初始授予 peer 的接收容量。 */
	uint64_t initial_window_bytes;

	/* 至少释放这么多 credit 后才发送新的绝对 WINDOW_UPDATE。 */
	uint64_t window_update_threshold_bytes;

	/*
     * 可选的接收侧 message reassembly。
     * 一个逻辑 Stream message 可以跨多个 Transport DATA frame。
     * reassembly_pool 为 NULL 时拒绝 fragmented receive，调用方必须保证
     * 一条 message 能放进一个 Transport frame。
     * 启用后 max_message_bytes 必须能放进 reassembly_pool 的单个 buffer。
     */
	uint32_t max_message_bytes;
	struct tr_buffer_pool *reassembly_pool;

	/* Channel protocol capabilities；version 为 0 时默认只支持 V1。 */
	uint16_t min_protocol_version;
	uint16_t max_protocol_version;
	uint64_t feature_bits;
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

/*
 * 两个 connection handle 必须属于同一个 Reactor。
 * shared 模式允许 bulk_connection == control_connection；
 * split 模式要求两者是不同且存活的 handle。
 *
 * Channel 会为 connection 安装 Reactor handler。销毁前必须先停止外部使用，
 * 并确保不存在并发 callback；tr_channel_destroy() 内部会清理 handler 并等待
 * callback quiescence。
 */
int tr_channel_create(const struct tr_channel_config *config,
		      struct tr_conn_handle control_connection,
		      struct tr_conn_handle bulk_connection,
		      tr_stream_data_cb data_cb,
		      tr_stream_event_cb stream_event_cb,
		      tr_channel_event_cb channel_event_cb, void *callback_arg,
		      struct tr_channel **out);

void tr_channel_destroy(struct tr_channel *channel);

/*
 * 替换上层 callback，主要用于在已有 Channel 上叠加 RPC。
 * 旧 callback owner 在 in-flight callback 完成 quiescence 之前不能释放。
 */
int tr_channel_set_handler(struct tr_channel *channel,
			   tr_stream_data_cb data_cb,
			   tr_stream_event_cb stream_event_cb,
			   tr_channel_event_cb channel_event_cb,
			   void *callback_arg);

/*
 * 等待可能已经观察到旧 Channel handler 的 Reactor callback 全部结束。
 * 应在替换/清空上层 handler 之后、释放旧 callback owner 之前调用。
 */
int tr_channel_quiesce(struct tr_channel *channel);

/*
 * 在保留逻辑 Channel 的情况下替换失败的物理 connection。
 * 失败 lane 上的旧 Stream 不会跨 replacement 存活，调用方必须重新创建
 * Stream/Call。shared 模式替换任一 lane 等价于同时替换 CONTROL/BULK。
 * 新 handle 必须属于该 Channel 的 Reactor，且已经由 Reactor 接管
 * （状态为 RESERVED 或 ACTIVE）。
 */
int tr_channel_replace_connection(struct tr_channel *channel, enum tr_lane lane,
				  struct tr_conn_handle connection);

/*
 * 启用 V1 Client 自动 reconnect。
 * 一个轻量 Channel maintenance thread 负责 connect/backoff；socket 被
 * Reactor 接管后，正常 I/O 仍全部由 owner Reactor 执行。
 * disconnect 时 in-flight Stream 立即失败，不做透明 replay。
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

/*
 * 选定 lane 进入 UP 后，返回协商得到的 outbound limits。
 * max_frame_payload_bytes/max_message_bytes 表示 peer 声明的接收能力；
 * 本地接收上限仍由 Channel/Reactor 配置决定。
 */
int tr_channel_get_capabilities(struct tr_channel *channel, enum tr_lane lane,
				struct tr_channel_capabilities *out);

/*
 * graceful shutdown：禁止创建新的本地 Stream，并在 ready lane 上发送
 * GOAWAY。已有 Stream 继续运行，直到自然 close/cancel。
 * 本操作幂等；TR_AGAIN 表示 GOAWAY control frame 暂时无法入队，
 * Channel 仍保持 draining，调用方可以重试。
 */
int tr_channel_begin_drain(struct tr_channel *channel);
int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms);
int tr_channel_get_state(struct tr_channel *channel,
			 enum tr_channel_state *out);
uint32_t tr_channel_active_streams(struct tr_channel *channel);

/* 打开一个逻辑 byte-stream；完成状态通过 OPENED event 上报。 */
int tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
		   struct tr_stream_handle *out);

/*
 * 一次 write 对应一条逻辑 Stream message。
 * 超过 Reactor frame-payload 上限时由 Transport 自动切成多个 DATA frame。
 * 所有权：TR_OK 后 payload ownership 转移给 Reactor，直到全部 fragment
 * 发送完成；其他返回值下仍归调用方。
 * TR_AGAIN 表示 flow-control 或 Reactor backpressure。
 */
int tr_stream_send(struct tr_stream_handle stream, struct tr_buffer *payload);

/*
 * scatter/gather Stream write。flow control 按所有 buffer 长度之和计费。
 * TR_OK 时每个 buffer 的 ownership 都转移给 Transport。
 */
int tr_stream_sendv(struct tr_stream_handle stream,
		    struct tr_buffer *const *payloads, uint32_t payload_count);

/* half-close 本地发送方向。 */
int tr_stream_close(struct tr_stream_handle stream);

/*
 * 释放之前通过 TR_STREAM_DATA_TAKE_OWNERSHIP 保留的 payload，
 * 并把对应字节 credit 归还给 Stream receive window。
 */
int tr_stream_release_payload(struct tr_stream_handle stream,
			      struct tr_buffer *payload);

/* 重试 pending 的绝对 WINDOW_UPDATE control frame。 */
int tr_channel_flush(struct tr_channel *channel);

/* 获取 byte-based flow-control 状态快照，用于诊断和测试。 */
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
