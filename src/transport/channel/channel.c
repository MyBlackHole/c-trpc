#define _GNU_SOURCE
#include "channel.h"
#include "channel_internal.h"
#include "../../io/connector_internal.h"
#include "../../io/socket_internal.h"
#include "../../execution/reactor_internal.h"

#include "tr/status.h"
#include "../protocol/wire.h"
#include "../../io/socket.h"
#include "../../endian.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TR_CHANNEL_HELLO_WIRE_SIZE 32U
#define TR_CHANNEL_PROTOCOL_BUFFER_SIZE 64U
#define TR_STREAM_FREE_NONE UINT32_MAX
/* 合并重试，其上限独立于存活流的数量。 */
#define TR_CHANNEL_WINDOW_RETRY_NS UINT64_C(25000000)

enum tr_stream_slot_state {
	TR_STREAM_SLOT_FREE = 0,
	TR_STREAM_SLOT_OPENING,
	TR_STREAM_SLOT_OPEN,
	TR_STREAM_SLOT_RESETTING
};

struct tr_channel_pending_hello {
	int valid;
	struct tr_conn_handle connection;
	uint8_t payload[TR_CHANNEL_HELLO_WIRE_SIZE];
};

struct tr_stream_index_entry {
	uint32_t stream_id;
	uint32_t slot;
};

/* 仅保留有限个精确双向终止记录，吸收重复 RESET。DATA/信用没有
 * 此豁免；对端 RESET 已是其发送 FIFO 屏障，之后的数据仍是协议错误。 */
struct tr_stream_reset_tombstone {
	struct tr_conn_handle connection;
	uint32_t stream_id;
	int peer_status;
};

/* 每个物理连接独立记录两个方向的受理边界。已见 OPEN 与已受理 OPEN
 * 不能共用前沿：本地排空后仍会收到对端尚未观察 GOAWAY 时发出的 OPEN。
 * 这些帧只推进去重前沿，不再取得本地流或逐流拒绝责任。 */
struct tr_channel_drain {
	uint32_t active_streams;
	uint32_t last_peer_admitted;
	uint32_t last_local_accepted;
	uint32_t local_cutoff;
	uint32_t peer_cutoff;
	int local_started;
	int peer_started;
	int goaway_accepted;
	int tx_wait_requested;
	int tx_drained;
};

struct tr_stream_slot {
	uint32_t generation;
	uint32_t free_next;
	enum tr_stream_slot_state state;
	enum tr_lane lane;
	uint32_t stream_id;

	int local_open;
	int remote_open;
	int opened_notified;

	uint64_t next_tx_message_id;
	uint64_t next_rx_message_id;

	uint64_t tx_sent_bytes;
	uint64_t tx_send_limit;

	uint64_t rx_received_bytes;
	uint64_t rx_consumed_bytes;
	uint64_t rx_advertised_limit;
	uint64_t pending_advertised_limit;
	int rx_credit_stopped;

	struct tr_buffer *rx_reassembly;
	uint64_t rx_reassembly_message_id;
	int reset_status;
	int reset_peer_status;
	int reset_sent;
	int reset_received;
	int reset_notified;
	int reset_fragmented;
	int reset_discard_unordered;
	uint64_t reset_message_bytes;
};

struct tr_channel {
	struct tr_channel_config config;
	pthread_mutex_t lock;
	pthread_cond_t state_cond;
	uint32_t state_waiters;
	int state_wait_closed;

	struct tr_reactor *reactor;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle bulk_connection;

	int control_alive;
	int bulk_alive;
	int control_ready;
	int bulk_ready;
	int handshake_started;
	int handshake_start_error;
	struct tr_channel_pending_hello pending_hello[2];
	int local_draining;
	int control_peer_draining;
	int bulk_peer_draining;
	struct tr_channel_drain drain[2];
	int drain_retry_pending;
	int drain_progressing;
	struct tr_channel_capabilities control_caps;
	struct tr_channel_capabilities bulk_caps;

	uint32_t next_local_stream_id;
	uint32_t free_stream_head;
	uint32_t active_streams;
	struct tr_stream_slot *streams;
	struct tr_stream_index_entry *stream_index;
	size_t stream_index_capacity;
	struct tr_stream_reset_tombstone *reset_tombstones;
	uint32_t reset_tombstone_cursor;
	uint32_t reset_cursor;
	uint32_t peer_stream_frontier[2];
	int reset_retry_pending;
	int reset_progressing;

	tr_stream_data_cb data_cb;
	tr_stream_event_cb stream_event_cb;
	tr_channel_event_cb channel_event_cb;
	void *callback_arg;

	tr_channel_event_cb lifecycle_event_cb;
	void *lifecycle_callback_arg;

	int reconnect_enabled;
	struct tr_connector *reconnect_connector;
	struct tr_reactor_timer_handle reconnect_timer;
	int reconnect_timer_registered;
	enum tr_lane reconnect_lane;

	char reconnect_address[64];
	uint16_t reconnect_control_port;
	uint16_t reconnect_bulk_port;
	uint32_t reconnect_initial_delay_ms;
	uint32_t reconnect_max_delay_ms;
	uint32_t reconnect_connect_timeout_ms;
	int reconnect_tcp_nodelay;
	uint32_t control_reconnect_attempt;
	uint32_t bulk_reconnect_attempt;
	int control_reconnecting;
	int bulk_reconnecting;

	int keepalive_enabled;
	int teardown_detached;
	/* 共享维护定时器：保活与可重试的 WINDOW_UPDATE。 */
	struct tr_reactor_timer_handle keepalive_timer;
	int keepalive_timer_registered;
	int window_retry_pending;
	int window_retry_scheduled;
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;
	uint64_t keepalive_next_ping_id;
	int keepalive_ping_outstanding[2];
	uint64_t keepalive_ping_id[2];
	uint64_t keepalive_ping_sent_ns[2];
	uint64_t keepalive_last_rtt_ns[2];

	uint64_t stat_streams_opened;
	uint64_t stat_streams_closed;
	uint64_t stat_stream_errors;
	uint64_t stat_messages_tx;
	uint64_t stat_messages_rx;
	uint64_t stat_bytes_tx;
	uint64_t stat_bytes_rx;
	uint64_t stat_window_updates_tx;
	uint64_t stat_window_updates_rx;
	uint64_t stat_reconnect_attempts;
	uint64_t stat_reconnect_successes;
	uint64_t stat_keepalive_pings_sent;
	uint64_t stat_keepalive_pongs_received;
	uint64_t stat_keepalive_timeouts;
};

static int tr_conn_equal(struct tr_conn_handle a, struct tr_conn_handle b)
{
	return a.reactor == b.reactor && a.slot == b.slot &&
	       a.generation == b.generation;
}

static uint64_t tr_add_sat_u64(uint64_t a, uint64_t b)
{
	if (UINT64_MAX - a < b)
		return UINT64_MAX;
	return a + b;
}

static uint64_t tr_channel_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static int tr_lane_index(enum tr_lane lane)
{
	return lane == TR_LANE_BULK ? 1 : 0;
}

static struct tr_conn_handle
tr_channel_connection(const struct tr_channel *channel, enum tr_lane lane)
{
	return lane == TR_LANE_BULK ? channel->bulk_connection :
				      channel->control_connection;
}

static int tr_channel_lane_alive(const struct tr_channel *channel,
				 enum tr_lane lane)
{
	if (lane == TR_LANE_BULK)
		return channel->bulk_alive && channel->bulk_ready;
	return channel->control_alive && channel->control_ready;
}

static uint32_t tr_lane_flag(enum tr_lane lane)
{
	return lane == TR_LANE_BULK ? TR_FRAME_F_LANE_BULK : 0U;
}

static uint32_t
tr_channel_connection_lane_mask_locked(const struct tr_channel *channel,
				       struct tr_conn_handle connection)
{
	uint32_t mask = 0;

	if (tr_conn_equal(channel->control_connection, connection))
		mask |= TR_CHANNEL_LANE_MASK_CONTROL;
	if (tr_conn_equal(channel->bulk_connection, connection))
		mask |= TR_CHANNEL_LANE_MASK_BULK;
	return mask;
}

static int
tr_channel_keepalive_index_for_connection(const struct tr_channel *channel,
					  struct tr_conn_handle connection)
{
	if (tr_conn_equal(channel->control_connection, connection))
		return 0;
	if (tr_conn_equal(channel->bulk_connection, connection))
		return 1;
	return -1;
}

static int
tr_channel_connection_alive_locked(const struct tr_channel *channel,
				   struct tr_conn_handle connection)
{
	if (tr_conn_equal(channel->control_connection, connection) &&
	    channel->control_alive)
		return 1;
	if (tr_conn_equal(channel->bulk_connection, connection) &&
	    channel->bulk_alive)
		return 1;
	return 0;
}

static int tr_channel_connection_idle_locked(const struct tr_channel *channel,
                                             struct tr_conn_handle connection)
{
	int idx =
	        tr_channel_keepalive_index_for_connection(channel, connection);
	return idx >= 0 && channel->drain[idx].local_started &&
	       channel->drain[idx].peer_started &&
	       channel->drain[idx].active_streams == 0U;
}

static int tr_channel_drained_locked(const struct tr_channel *channel)
{
	if (!channel->local_draining || channel->active_streams != 0U)
		return 0;
	for (int i = 0; i < 2; ++i) {
		struct tr_conn_handle connection = tr_channel_connection(
		        channel, i == 0 ? TR_LANE_CONTROL : TR_LANE_BULK);
		const struct tr_channel_drain *drain = &channel->drain[i];
		if ((i == 1 &&
		     tr_conn_equal(connection, channel->control_connection)) ||
		    !tr_channel_connection_alive_locked(channel, connection))
			continue;
		if (!drain->local_started || !drain->peer_started ||
		    !drain->goaway_accepted || !drain->tx_drained)
			return 0;
	}
	return 1;
}

static int tr_channel_keepalive_needed_locked(const struct tr_channel *channel)
{
	if (!channel->keepalive_enabled)
		return 0;
	for (int i = 0; i < 2; ++i) {
		struct tr_conn_handle connection = tr_channel_connection(
		        channel, i == 0 ? TR_LANE_CONTROL : TR_LANE_BULK);
		if (i == 1 &&
		    tr_conn_equal(connection, channel->control_connection))
			continue;
		if (!tr_channel_connection_alive_locked(channel, connection)) {
			if (!channel->local_draining &&
			    !channel->drain[i].local_started)
				return 1;
			continue;
		}
		if (!tr_channel_connection_idle_locked(channel, connection))
			return 1;
	}
	return 0;
}

static void tr_channel_keepalive_reset_locked(struct tr_channel *channel,
					      enum tr_lane lane)
{
	int idx = tr_lane_index(lane);

	channel->keepalive_ping_outstanding[idx] = 0;
	channel->keepalive_ping_id[idx] = 0;
	channel->keepalive_ping_sent_ns[idx] = 0;
}

static void tr_channel_keepalive_check_lane(struct tr_channel *channel,
					    enum tr_lane lane)
{
	struct tr_conn_handle connection;
	struct tr_connection_stats stats;
	uint64_t now;
	uint64_t sent_ns;
	uint64_t interval_ns;
	uint64_t timeout_ns;
	uint64_t ping_id;
	int idx = tr_lane_index(lane);
	int outstanding;
	int ready;
	int ret;

	pthread_mutex_lock(&channel->lock);
	if (!channel->keepalive_enabled ||
	    tr_channel_connection_idle_locked(
	            channel, tr_channel_connection(channel, lane)) ||
	    (lane == TR_LANE_BULK && tr_conn_equal(channel->control_connection,
	                                           channel->bulk_connection))) {
		pthread_mutex_unlock(&channel->lock);
		return;
	}
	connection = tr_channel_connection(channel, lane);
	ready = tr_channel_lane_alive(channel, lane);
	outstanding = channel->keepalive_ping_outstanding[idx];
	sent_ns = channel->keepalive_ping_sent_ns[idx];
	interval_ns =
		(uint64_t)channel->keepalive_interval_ms * UINT64_C(1000000);
	timeout_ns =
		(uint64_t)channel->keepalive_timeout_ms * UINT64_C(1000000);
	pthread_mutex_unlock(&channel->lock);

	if (!ready ||
	    tr_reactor_get_connection_stats(connection, &stats) != TR_OK)
		return;

	now = tr_channel_now_ns();
	if (now == 0)
		return;

	if (outstanding) {
		/* 探测发出后的任意对端流量都足以证明连接仍存活。
		 * 在超时边界前继续保留未完成探测，使显式 PONG 仍可提供
		 * 往返时间样本；如果只观察到其他流量，则正常结束探测，
		 * 不把连接判定为失败。
		 */
		if (stats.last_rx_activity_ns > sent_ns) {
			if (now - sent_ns >= timeout_ns) {
				pthread_mutex_lock(&channel->lock);
				if (channel->keepalive_ping_outstanding[idx] &&
				    channel->keepalive_ping_sent_ns[idx] ==
					    sent_ns)
					tr_channel_keepalive_reset_locked(
						channel, lane);
				pthread_mutex_unlock(&channel->lock);
			}
			return;
		}

		if (now - sent_ns >= timeout_ns) {
			pthread_mutex_lock(&channel->lock);
			if (channel->keepalive_ping_outstanding[idx] &&
			    channel->keepalive_ping_sent_ns[idx] == sent_ns) {
				tr_channel_keepalive_reset_locked(channel,
								  lane);
				channel->stat_keepalive_timeouts++;
			} else {
				connection.reactor = NULL;
			}
			pthread_mutex_unlock(&channel->lock);
			if (connection.reactor)
				(void)tr_reactor_abort(connection,
						       TR_ERR_TIMEOUT);
		}
		return;
	}

	if (stats.last_rx_activity_ns != 0 &&
	    now - stats.last_rx_activity_ns < interval_ns)
		return;

	pthread_mutex_lock(&channel->lock);
	if (!channel->keepalive_enabled ||
	    tr_channel_connection_idle_locked(channel, connection) ||
	    !tr_channel_lane_alive(channel, lane) ||
	    channel->keepalive_ping_outstanding[idx] ||
	    !tr_conn_equal(connection, tr_channel_connection(channel, lane))) {
		pthread_mutex_unlock(&channel->lock);
		return;
	}
	ping_id = ++channel->keepalive_next_ping_id;
	if (ping_id == 0)
		ping_id = ++channel->keepalive_next_ping_id;
	channel->keepalive_ping_outstanding[idx] = 1;
	channel->keepalive_ping_id[idx] = ping_id;
	channel->keepalive_ping_sent_ns[idx] = now;
	pthread_mutex_unlock(&channel->lock);

	ret = tr_reactor_send(connection, TR_FRAME_PING, 0, 0, ping_id, NULL);
	pthread_mutex_lock(&channel->lock);
	if (ret == TR_OK) {
		channel->stat_keepalive_pings_sent++;
	} else if (channel->keepalive_ping_outstanding[idx] &&
		   channel->keepalive_ping_id[idx] == ping_id) {
		tr_channel_keepalive_reset_locked(channel, lane);
	}
	pthread_mutex_unlock(&channel->lock);
}

static uint32_t
tr_channel_keepalive_tick_ms_locked(const struct tr_channel *channel)
{
	uint32_t tick_ms = channel->keepalive_interval_ms / 4U;

	if (channel->keepalive_timeout_ms / 4U < tick_ms)
		tick_ms = channel->keepalive_timeout_ms / 4U;
	if (tick_ms < 10U)
		tick_ms = 10U;
	if (tick_ms > 1000U)
		tick_ms = 1000U;
	return tick_ms;
}

static int tr_send_window_update_locked(struct tr_channel *channel,
					struct tr_stream_slot *stream);
static void tr_channel_reset_progress_on_owner(struct tr_channel *channel);
static int tr_channel_drain_progress_on_owner(struct tr_channel *channel);
static int tr_channel_protocol_error(struct tr_channel *channel,
                                     struct tr_conn_handle connection,
                                     int status);

static uint64_t
tr_channel_keepalive_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	uint64_t next_ns = 0;
	uint64_t deadline_ns;
	uint32_t tick_ms = 0;
	uint32_t i;
	int enabled;
	int retry_pending;

	tr_channel_reset_progress_on_owner(channel);

	/* 既有维护定时器同时推进信用/RESET 重试及排空。
	 * 失败的 WINDOW_UPDATE 无需另一接收事件或应用刷新即可重试。
	 * 只扫描当前流槽位：旧 stream_id/generation 不能在槽位复用后存活。
	 */
	pthread_mutex_lock(&channel->lock);
	channel->window_retry_scheduled = 0;
	if (channel->window_retry_pending) {
		channel->window_retry_pending = 0;
		for (i = 0; i < channel->config.max_streams; ++i) {
			struct tr_stream_slot *stream = &channel->streams[i];
			int ret;

			if (stream->state == TR_STREAM_SLOT_FREE ||
			    stream->pending_advertised_limit == 0)
				continue;

			ret = tr_send_window_update_locked(channel, stream);
			if (ret != TR_OK && ret != TR_ERR_CLOSED &&
			    ret != TR_ERR_STALE)
				channel->window_retry_pending = 1;
		}
	}
	enabled = tr_channel_keepalive_needed_locked(channel);
	pthread_mutex_unlock(&channel->lock);

	if (enabled) {
		tr_channel_keepalive_check_lane(channel, TR_LANE_CONTROL);
		tr_channel_keepalive_check_lane(channel, TR_LANE_BULK);
	}
	(void)tr_channel_drain_progress_on_owner(channel);

	pthread_mutex_lock(&channel->lock);
	enabled = tr_channel_keepalive_needed_locked(channel);
	tick_ms = enabled ? tr_channel_keepalive_tick_ms_locked(channel) : 0U;
	retry_pending = channel->window_retry_pending ||
	                channel->reset_retry_pending ||
	                channel->drain_retry_pending;
	if (retry_pending)
		channel->window_retry_scheduled = 1;
	pthread_mutex_unlock(&channel->lock);

	if (!enabled && !retry_pending)
		return 0;
	if (now_ns == 0)
		now_ns = tr_channel_now_ns();
	if (now_ns == 0) {
		/* CLOCK_MONOTONIC 不可用时不设置定时器。 */
		pthread_mutex_lock(&channel->lock);
		channel->window_retry_scheduled = 0;
		pthread_mutex_unlock(&channel->lock);
		return 0;
	}
	if (enabled)
		next_ns = tr_add_sat_u64(
			now_ns, (uint64_t)tick_ms * UINT64_C(1000000));
	if (retry_pending) {
		deadline_ns =
			tr_add_sat_u64(now_ns, TR_CHANNEL_WINDOW_RETRY_NS);
		if (next_ns == 0 || deadline_ns < next_ns)
			next_ns = deadline_ns;
	}
	return next_ns;
}

static uint32_t tr_channel_local_max_message(const struct tr_channel *channel,
					     uint32_t max_frame_payload)
{
	if (channel->config.max_message_bytes)
		return channel->config.max_message_bytes;
	return max_frame_payload;
}

static void tr_channel_encode_hello(uint8_t out[TR_CHANNEL_HELLO_WIRE_SIZE],
				    uint16_t min_version, uint16_t max_version,
				    uint32_t lane_mask,
				    uint32_t max_frame_payload,
				    uint32_t max_message_bytes,
				    uint64_t feature_bits)
{
	memset(out, 0, TR_CHANNEL_HELLO_WIRE_SIZE);
	tr_put_le16(out + 0, min_version);
	tr_put_le16(out + 2, max_version);
	tr_put_le32(out + 4, lane_mask);
	tr_put_le32(out + 8, max_frame_payload);
	tr_put_le32(out + 12, max_message_bytes);
	tr_put_le64(out + 16, feature_bits);
}

static void
tr_channel_encode_hello_ack(uint8_t out[TR_CHANNEL_HELLO_WIRE_SIZE],
			    const struct tr_channel_capabilities *caps)
{
	memset(out, 0, TR_CHANNEL_HELLO_WIRE_SIZE);
	tr_put_le16(out + 0, caps->protocol_version);
	tr_put_le32(out + 4, caps->lane_mask);
	tr_put_le32(out + 8, caps->max_frame_payload_bytes);
	tr_put_le32(out + 12, caps->max_message_bytes);
	tr_put_le64(out + 16, caps->feature_bits);
}

static int tr_channel_decode_hello(const uint8_t *data, uint32_t len,
				   uint16_t *min_version, uint16_t *max_version,
				   uint32_t *lane_mask,
				   uint32_t *max_frame_payload,
				   uint32_t *max_message_bytes,
				   uint64_t *feature_bits)
{
	if (!data || len != TR_CHANNEL_HELLO_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;

	*min_version = tr_get_le16(data + 0);
	*max_version = tr_get_le16(data + 2);
	*lane_mask = tr_get_le32(data + 4);
	*max_frame_payload = tr_get_le32(data + 8);
	*max_message_bytes = tr_get_le32(data + 12);
	*feature_bits = tr_get_le64(data + 16);

	if (tr_get_le64(data + 24) != 0)
		return TR_ERR_RESERVED;
	return TR_OK;
}

static int tr_channel_decode_hello_ack(const uint8_t *data, uint32_t len,
				       struct tr_channel_capabilities *caps)
{
	if (!data || !caps || len != TR_CHANNEL_HELLO_WIRE_SIZE)
		return TR_ERR_BAD_LENGTH;
	if (tr_get_le16(data + 2) != 0 || tr_get_le64(data + 24) != 0)
		return TR_ERR_RESERVED;

	memset(caps, 0, sizeof(*caps));
	caps->protocol_version = tr_get_le16(data + 0);
	caps->lane_mask = tr_get_le32(data + 4);
	caps->max_frame_payload_bytes = tr_get_le32(data + 8);
	caps->max_message_bytes = tr_get_le32(data + 12);
	caps->feature_bits = tr_get_le64(data + 16);
	return TR_OK;
}

static int tr_channel_local_capabilities(struct tr_channel *channel,
			      struct tr_conn_handle connection,
			      struct tr_channel_capabilities *caps)
{
	struct tr_reactor_limits limits;
	int ret;

	ret = tr_reactor_get_limits(channel->reactor, &limits);
	if (ret != TR_OK)
		return ret;

	memset(caps, 0, sizeof(*caps));
	caps->protocol_version = channel->config.max_protocol_version;
	pthread_mutex_lock(&channel->lock);
	caps->lane_mask =
		tr_channel_connection_lane_mask_locked(channel, connection);
	pthread_mutex_unlock(&channel->lock);
	caps->max_frame_payload_bytes = limits.max_payload_len;
	caps->max_message_bytes =
		tr_channel_local_max_message(channel, limits.max_payload_len);
	caps->feature_bits = channel->config.feature_bits;
	if (caps->lane_mask == 0 || caps->max_frame_payload_bytes == 0 ||
	    caps->max_message_bytes == 0)
		return TR_ERR_STATE;
	return TR_OK;
}

struct tr_channel_protocol_buffer {
	struct tr_buffer buffer;
	uint8_t storage[TR_CHANNEL_PROTOCOL_BUFFER_SIZE];
};

static void tr_channel_protocol_buffer_release(struct tr_buffer *buffer)
{
	free((struct tr_channel_protocol_buffer *)buffer);
}

static int tr_channel_protocol_buffer_acquire(
	uint32_t size, struct tr_buffer **out)
{
	struct tr_channel_protocol_buffer *owned;

	if (!out || size == 0U || size > TR_CHANNEL_PROTOCOL_BUFFER_SIZE)
		return TR_ERR_INVALID;
	*out = NULL;

	owned = (struct tr_channel_protocol_buffer *)calloc(1, sizeof(*owned));
	if (!owned)
		return TR_ERR_NOMEM;

	owned->buffer.data = owned->storage;
	owned->buffer.capacity = TR_CHANNEL_PROTOCOL_BUFFER_SIZE;
	owned->buffer.release_cb = tr_channel_protocol_buffer_release;
	*out = &owned->buffer;
	return TR_OK;
}

static int tr_channel_send_hello(struct tr_channel *channel,
				 struct tr_conn_handle connection)
{
	struct tr_channel_capabilities local;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	int ret;

	ret = tr_channel_local_capabilities(channel, connection, &local);
	if (ret != TR_OK)
		return ret;
	ret = tr_channel_protocol_buffer_acquire(
		TR_CHANNEL_HELLO_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	tr_channel_encode_hello(buffer->data,
				channel->config.min_protocol_version,
				channel->config.max_protocol_version,
				local.lane_mask, local.max_frame_payload_bytes,
				local.max_message_bytes, local.feature_bits);
	buffer->len = TR_CHANNEL_HELLO_WIRE_SIZE;
	ret = tr_reactor_send(connection, TR_FRAME_HELLO, 0, 0, 0, buffer);
	if (ret == TR_OK)
		(void)tr_buffer_take(&buffer);
	return ret;
}

static int
tr_channel_normalize_create_hello_error(struct tr_channel *channel,
					 struct tr_conn_handle connection,
					 int ret)
{
	uint32_t mask;
	int still_alive = 0;

	if (ret == TR_OK)
		return TR_OK;
	if (ret != TR_ERR_STALE && ret != TR_ERR_CLOSED)
		return ret;

	/* Reactor 所有者回调在当前轮次内串行化，无须递归等待静默；
	 * 外部调用则必须先确认回调静默，才能把 STALE/CLOSED
	 * 规范化为异步连接 DOWN。静默失败不可伪装成握手成功。
	 */
	if (!tr_reactor_in_owner_context_for(channel->reactor)) {
		int fence = tr_reactor_quiesce(channel->reactor);

		if (fence != TR_OK)
			return fence;
	}

	pthread_mutex_lock(&channel->lock);
	mask = tr_channel_connection_lane_mask_locked(channel, connection);
	if ((mask & TR_CHANNEL_LANE_MASK_CONTROL) && channel->control_alive)
		still_alive = 1;
	if ((mask & TR_CHANNEL_LANE_MASK_BULK) && channel->bulk_alive)
		still_alive = 1;
	pthread_mutex_unlock(&channel->lock);

	return still_alive ? ret : TR_OK;
}

static int tr_channel_send_hello_ack(
	struct tr_conn_handle connection,
	const struct tr_channel_capabilities *caps)
{
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	int ret;

	ret = tr_channel_protocol_buffer_acquire(
		TR_CHANNEL_HELLO_WIRE_SIZE, &buffer);
	if (ret != TR_OK)
		return ret;

	tr_channel_encode_hello_ack(buffer->data, caps);
	buffer->len = TR_CHANNEL_HELLO_WIRE_SIZE;
	ret = tr_reactor_send(connection, TR_FRAME_HELLO_ACK, 0, 0, 0, buffer);
	if (ret == TR_OK)
		(void)tr_buffer_take(&buffer);
	return ret;
}

static void
tr_channel_set_caps_locked(struct tr_channel *channel, uint32_t lane_mask,
			   const struct tr_channel_capabilities *caps,
			   int *control_up, int *bulk_up)
{
	if ((lane_mask & TR_CHANNEL_LANE_MASK_CONTROL) &&
	    !channel->control_ready) {
		channel->control_caps = *caps;
		channel->control_ready = 1;
		*control_up = 1;
	}
	if ((lane_mask & TR_CHANNEL_LANE_MASK_BULK) && !channel->bulk_ready) {
		channel->bulk_caps = *caps;
		channel->bulk_ready = 1;
		*bulk_up = 1;
	}
	if (*control_up || *bulk_up)
		pthread_cond_broadcast(&channel->state_cond);
}

static uint32_t
tr_channel_lane_max_frame_locked(const struct tr_channel *channel,
				 enum tr_lane lane)
{
	const struct tr_channel_capabilities *caps =
		lane == TR_LANE_BULK ? &channel->bulk_caps :
				       &channel->control_caps;
	return caps->max_frame_payload_bytes;
}

static uint32_t
tr_channel_lane_max_message_locked(const struct tr_channel *channel,
				   enum tr_lane lane)
{
	const struct tr_channel_capabilities *caps =
		lane == TR_LANE_BULK ? &channel->bulk_caps :
				       &channel->control_caps;
	return caps->max_message_bytes;
}

static int tr_channel_connection_ready_locked(const struct tr_channel *channel,
					      struct tr_conn_handle connection)
{
	uint32_t mask = tr_channel_connection_lane_mask_locked(channel, connection);

	if (mask == 0)
		return 0;
	if ((mask & TR_CHANNEL_LANE_MASK_CONTROL) && !channel->control_ready)
		return 0;
	if ((mask & TR_CHANNEL_LANE_MASK_BULK) && !channel->bulk_ready)
		return 0;
	return 1;
}

static uint32_t
tr_channel_active_streams_locked(const struct tr_channel *channel)
{
	return channel->active_streams;
}

static int tr_channel_drain_progress_on_owner(struct tr_channel *channel)
{
	int result = TR_OK;

	pthread_mutex_lock(&channel->lock);
	if (channel->drain_progressing || channel->teardown_detached) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	channel->drain_progressing = 1;
	pthread_mutex_unlock(&channel->lock);

	for (int i = 0; i < 2; ++i) {
		struct tr_conn_handle connection;
		struct tr_channel_drain *drain;
		uint32_t cutoff;
		int send_goaway;
		int request_tx = 0;
		int ret = TR_OK;

		pthread_mutex_lock(&channel->lock);
		connection = tr_channel_connection(
		        channel, i == 0 ? TR_LANE_CONTROL : TR_LANE_BULK);
		drain = &channel->drain[i];
		if ((i == 1 &&
		     tr_conn_equal(connection, channel->control_connection)) ||
		    !drain->local_started ||
		    !tr_channel_connection_alive_locked(channel, connection)) {
			pthread_mutex_unlock(&channel->lock);
			continue;
		}
		cutoff = drain->local_cutoff;
		send_goaway =
		        !drain->goaway_accepted &&
		        tr_channel_connection_ready_locked(channel, connection);
		pthread_mutex_unlock(&channel->lock);
		if (send_goaway) {
			ret = tr_reactor_send_goaway_on_owner(connection,
			                                      cutoff);
			pthread_mutex_lock(&channel->lock);
			if (ret == TR_OK)
				drain->goaway_accepted = 1;
			pthread_mutex_unlock(&channel->lock);
		}
		if (ret == TR_OK) {
			pthread_mutex_lock(&channel->lock);
			if (drain->goaway_accepted && drain->peer_started &&
			    drain->active_streams == 0U &&
			    !drain->tx_wait_requested &&
			    tr_channel_connection_alive_locked(channel,
			                                       connection)) {
				/* 先发布等待责任，再调用可能内联通知的 owner
				 * 能力。 无流与双 GOAWAY 后不会再产生新的应用
				 * DATA/CLOSE。 */
				drain->tx_wait_requested = 1;
				request_tx = 1;
			}
			pthread_mutex_unlock(&channel->lock);
			if (request_tx)
				ret = tr_reactor_notify_tx_drained_on_owner(
				        connection);
		}
		if (ret != TR_OK) {
			/* 终局责任不能静默丢失。失败时同步退役精确连接；
			 * 对端/发送故障仍按原连接 ERROR 通知，不伪装完成事件。
			 */
			if (result == TR_OK)
				result = ret;
			(void)tr_channel_protocol_error(channel, connection,
			                                ret);
		}
	}

	pthread_mutex_lock(&channel->lock);
	channel->drain_retry_pending = 0;
	for (int i = 0; i < 2; ++i) {
		struct tr_conn_handle connection = tr_channel_connection(
		        channel, i == 0 ? TR_LANE_CONTROL : TR_LANE_BULK);
		struct tr_channel_drain *drain = &channel->drain[i];
		if ((i == 1 &&
		     tr_conn_equal(connection, channel->control_connection)) ||
		    !tr_channel_connection_alive_locked(channel, connection))
			continue;
		if (drain->local_started && !drain->tx_drained)
			channel->drain_retry_pending = 1;
	}
	channel->drain_progressing = 0;
	if (tr_channel_drained_locked(channel))
		pthread_cond_broadcast(&channel->state_cond);
	pthread_mutex_unlock(&channel->lock);
	return result;
}

static int tr_channel_send_pending_goaway_on_owner(void *arg)
{
	return tr_channel_drain_progress_on_owner(arg);
}

static int tr_stream_id_is_local(const struct tr_channel *channel,
				 uint32_t stream_id)
{
	if (stream_id == 0)
		return 0;
	if (channel->config.role == TR_CHANNEL_CLIENT)
		return (stream_id & 1U) != 0;
	return (stream_id & 1U) == 0;
}

static size_t tr_stream_index_capacity_for(uint32_t max_streams)
{
	size_t capacity = 1U;
	size_t target;

	if (max_streams == 0U)
		return 0U;
#if SIZE_MAX <= UINT32_MAX
	if (max_streams > (uint32_t)(SIZE_MAX / 2U))
		return 0U;
#endif
	target = (size_t)max_streams * 2U;
	while (capacity < target) {
		if (capacity > SIZE_MAX / 2U)
			return 0U;
		capacity <<= 1U;
	}
	return capacity;
}

static size_t
tr_stream_index_home(const struct tr_channel *channel, uint32_t stream_id)
{
	return (size_t)tr_channel_stream_id_hash(stream_id) &
	       (channel->stream_index_capacity - 1U);
}

static int tr_stream_index_insert_locked(struct tr_channel *channel,
					 uint32_t stream_id, uint32_t slot)
{
	size_t mask;
	size_t pos;
	size_t probes;

	if (!channel || !channel->stream_index || stream_id == 0U ||
	    slot >= channel->config.max_streams ||
	    channel->stream_index_capacity == 0U)
		return TR_ERR_INVALID;

	mask = channel->stream_index_capacity - 1U;
	pos = tr_stream_index_home(channel, stream_id);
	for (probes = 0; probes < channel->stream_index_capacity; ++probes) {
		struct tr_stream_index_entry *entry =
			&channel->stream_index[pos];

		if (entry->stream_id == 0U) {
			entry->stream_id = stream_id;
			entry->slot = slot;
			return TR_OK;
		}
		if (entry->stream_id == stream_id)
			return TR_ERR_STATE;
		pos = (pos + 1U) & mask;
	}

	return TR_AGAIN;
}

static int tr_stream_index_remove_locked(struct tr_channel *channel,
					 uint32_t stream_id)
{
	size_t mask;
	size_t hole;
	size_t scan;
	size_t probes;

	if (!channel || !channel->stream_index || stream_id == 0U ||
	    channel->stream_index_capacity == 0U)
		return TR_ERR_INVALID;

	mask = channel->stream_index_capacity - 1U;
	hole = tr_stream_index_home(channel, stream_id);
	for (probes = 0; probes < channel->stream_index_capacity; ++probes) {
		if (channel->stream_index[hole].stream_id == 0U)
			return TR_ERR_STALE;
		if (channel->stream_index[hole].stream_id == stream_id)
			break;
		hole = (hole + 1U) & mask;
	}
	if (probes == channel->stream_index_capacity)
		return TR_ERR_STALE;

	scan = (hole + 1U) & mask;
	while (channel->stream_index[scan].stream_id != 0U) {
		size_t home = tr_stream_index_home(
			channel, channel->stream_index[scan].stream_id);
		size_t scan_distance = (scan - home) & mask;
		size_t hole_distance = (hole - home) & mask;

		if (hole_distance < scan_distance) {
			channel->stream_index[hole] =
				channel->stream_index[scan];
			hole = scan;
		}
		scan = (scan + 1U) & mask;
	}

	memset(&channel->stream_index[hole], 0,
	       sizeof(channel->stream_index[hole]));
	return TR_OK;
}

static struct tr_stream_slot *tr_stream_lookup_id(struct tr_channel *channel,
						  uint32_t stream_id,
						  uint32_t *slot_out)
{
	size_t mask;
	size_t pos;
	size_t probes;

	if (!channel || !channel->stream_index || stream_id == 0U ||
	    channel->stream_index_capacity == 0U)
		return NULL;

	mask = channel->stream_index_capacity - 1U;
	pos = tr_stream_index_home(channel, stream_id);
	for (probes = 0; probes < channel->stream_index_capacity; ++probes) {
		struct tr_stream_index_entry *entry =
			&channel->stream_index[pos];
		struct tr_stream_slot *stream;

		if (entry->stream_id == 0U)
			return NULL;
		if (entry->stream_id != stream_id) {
			pos = (pos + 1U) & mask;
			continue;
		}
		if (entry->slot >= channel->config.max_streams)
			return NULL;

		stream = &channel->streams[entry->slot];
		if (stream->state == TR_STREAM_SLOT_FREE ||
		    stream->stream_id != stream_id)
			return NULL;
		if (slot_out)
			*slot_out = entry->slot;
		return stream;
	}
	return NULL;
}

static struct tr_stream_slot *
tr_stream_lookup_handle(struct tr_stream_handle handle)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;

	if (!channel || handle.slot >= channel->config.max_streams)
		return NULL;

	stream = &channel->streams[handle.slot];
	if (stream->state == TR_STREAM_SLOT_FREE ||
	    stream->generation != handle.generation)
		return NULL;
	return stream;
}

static struct tr_stream_handle
tr_stream_make_handle(struct tr_channel *channel, uint32_t slot,
		      const struct tr_stream_slot *stream)
{
	struct tr_stream_handle handle;
	handle.channel = channel;
	handle.slot = slot;
	handle.generation = stream->generation;
	return handle;
}

static void tr_stream_free_locked(struct tr_channel *channel, uint32_t slot)
{
	struct tr_stream_slot *stream;
	uint32_t generation;

	if (!channel || slot >= channel->config.max_streams)
		return;
	stream = &channel->streams[slot];
	if (stream->state == TR_STREAM_SLOT_FREE)
		return;

	{
		int idx = channel->config.mode == TR_CHANNEL_SHARED_CONNECTION
		                  ? 0
		                  : tr_lane_index(stream->lane);
		assert(channel->drain[idx].active_streams != 0U);
		channel->drain[idx].active_streams--;
	}
	generation = stream->generation;
	(void)tr_stream_index_remove_locked(channel, stream->stream_id);
	if (stream->rx_reassembly)
		tr_buffer_release(tr_buffer_take(&stream->rx_reassembly));
	memset(stream, 0, sizeof(*stream));
	stream->generation = generation;
	/* 旧句柄永远不能在第 2^32 次复用后重新有效。耗尽代次的槽
	 * 保持静默，不再回到空闲链；其余槽和同连接调用仍可前进。 */
	stream->free_next = TR_STREAM_FREE_NONE;
	if (generation != UINT32_MAX) {
		stream->free_next = channel->free_stream_head;
		channel->free_stream_head = slot;
	}
	if (channel->active_streams != 0) {
		channel->active_streams--;
		if (channel->active_streams == 0)
			pthread_cond_broadcast(&channel->state_cond);
	}
}

static int tr_stream_connection_matches(const struct tr_channel *channel,
					const struct tr_stream_slot *stream,
					struct tr_conn_handle connection)
{
	return tr_conn_equal(tr_channel_connection(channel, stream->lane),
			     connection);
}

static int tr_send_window_update_locked(struct tr_channel *channel,
					struct tr_stream_slot *stream)
{
	struct tr_conn_handle connection;
	uint64_t target;
	int ret;

	if (stream->rx_credit_stopped) {
		stream->pending_advertised_limit = 0;
		return TR_OK;
	}
	target = tr_add_sat_u64(stream->rx_consumed_bytes,
				channel->config.initial_window_bytes);
	if (target <= stream->rx_advertised_limit) {
		stream->pending_advertised_limit = 0;
		return TR_OK;
	}

	if (target - stream->rx_advertised_limit <
		    channel->config.window_update_threshold_bytes &&
	    stream->pending_advertised_limit == 0)
		return TR_OK;

	if (target > stream->pending_advertised_limit)
		stream->pending_advertised_limit = target;

	connection = tr_channel_connection(channel, stream->lane);
	if (!tr_channel_lane_alive(channel, stream->lane))
		return TR_ERR_CLOSED;

	ret = tr_reactor_send(connection, TR_FRAME_WINDOW_UPDATE, 0,
			      stream->stream_id,
			      stream->pending_advertised_limit, NULL);
	if (ret == TR_OK) {
		stream->rx_advertised_limit = stream->pending_advertised_limit;
		stream->pending_advertised_limit = 0;
		channel->stat_window_updates_tx++;
	}
	return ret;
}

/* 调用者必须在释放 channel->lock 后触发维护定时器。
 * 持有此锁时进行反应器同步所有者调用可能使接收路径死锁。
 */
static int tr_channel_schedule_window_retry_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_reactor_timer_handle timer;
	uint64_t now_ns;
	int need_arm;
	int ret;

	pthread_mutex_lock(&channel->lock);
	need_arm = channel->window_retry_pending &&
		   !channel->window_retry_scheduled &&
		   channel->keepalive_timer_registered &&
		   !channel->teardown_detached;
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);
	if (!need_arm)
		return TR_OK;

	now_ns = tr_channel_now_ns();
	if (now_ns == 0)
		return TR_ERR_SYS;
	ret = tr_reactor_timer_arm(
		timer, tr_add_sat_u64(now_ns, TR_CHANNEL_WINDOW_RETRY_NS));
	if (ret == TR_OK) {
		pthread_mutex_lock(&channel->lock);
		channel->window_retry_scheduled = 1;
		pthread_mutex_unlock(&channel->lock);
	}
	return ret;
}

static int tr_channel_schedule_window_retry(struct tr_channel *channel)
{
	return tr_reactor_call(channel->reactor,
			       tr_channel_schedule_window_retry_on_owner, channel);
}

/* 先取得可重试责任，再提交本地 RESET。这里只使用已注册的维护定时器，
 * 不发布新事件源；失败时尚未改变流，调用方可以保留原关闭责任重试。 */
static int tr_channel_reset_timer_on_owner(struct tr_channel *channel)
{
	struct tr_reactor_timer_handle timer;
	uint64_t now;
	int ret;

	pthread_mutex_lock(&channel->lock);
	if (!channel->keepalive_timer_registered ||
	    channel->teardown_detached) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (channel->window_retry_scheduled) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);
	now = tr_channel_now_ns();
	if (!now)
		return TR_ERR_SYS;
	ret = tr_reactor_timer_arm(
	        timer, tr_add_sat_u64(now, TR_CHANNEL_WINDOW_RETRY_NS));
	if (ret == TR_OK) {
		pthread_mutex_lock(&channel->lock);
		channel->window_retry_scheduled = 1;
		pthread_mutex_unlock(&channel->lock);
	}
	return ret;
}

static int tr_stream_reset_supported_locked(const struct tr_channel *channel,
                                            enum tr_lane lane)
{
	const struct tr_channel_capabilities *caps =
	        lane == TR_LANE_BULK ? &channel->bulk_caps
	                             : &channel->control_caps;
	return (caps->feature_bits & TR_CHANNEL_FEATURE_STREAM_RESET) != 0;
}

static void tr_stream_reset_begin_locked(struct tr_channel *channel,
                                         struct tr_stream_slot *stream,
                                         int status)
{
	stream->reset_status = status;
	stream->reset_fragmented = stream->rx_reassembly != NULL;
	stream->reset_message_bytes =
	        stream->rx_reassembly ? stream->rx_reassembly->len : 0;
	if (stream->rx_reassembly) {
		stream->rx_consumed_bytes += stream->rx_reassembly->len;
		tr_buffer_release(tr_buffer_take(&stream->rx_reassembly));
	}
	stream->state = TR_STREAM_SLOT_RESETTING;
	stream->local_open = 0;
	stream->rx_credit_stopped = 1;
	stream->pending_advertised_limit = 0;
	channel->reset_retry_pending = 1;
}

static int tr_stream_consume_locked(struct tr_channel *channel,
				    struct tr_stream_slot *stream,
				    uint64_t bytes)
{
	uint64_t available =
		stream->rx_received_bytes - stream->rx_consumed_bytes;
	int ret;

	if (bytes > available)
		bytes = available;
	stream->rx_consumed_bytes += bytes;
	ret = tr_send_window_update_locked(channel, stream);
	if (ret == TR_AGAIN) {
		channel->window_retry_pending = 1;
		return 1;
	}
	return 0;
}

static void tr_stream_notify(struct tr_channel *channel,
			     struct tr_stream_handle handle,
			     enum tr_stream_event event, int status)
{
	tr_stream_event_cb cb = channel->stream_event_cb;
	void *cb_arg = channel->callback_arg;

	/* 上层处理函数的发布与读取都在反应器所有者上完成。
	 * 回调调用前取快照，允许回调自己同步替换后续处理函数，
	 * 但不影响当前事件。
	 */
	if (cb)
		cb(handle, event, status, cb_arg);
}

static void tr_channel_notify(struct tr_channel *channel,
			      enum tr_channel_event event, int status)
{
	tr_channel_event_cb cb = channel->channel_event_cb;
	tr_channel_event_cb lifecycle_cb = channel->lifecycle_event_cb;
	void *cb_arg = channel->callback_arg;
	void *lifecycle_arg = channel->lifecycle_callback_arg;

	/* 两组回调都是所有者发布。先取快照再调用普通处理函数，
	 * 保持当前事件的观察者集合稳定；回调内替换只影响后续事件。
	 */
	if (cb)
		cb(channel, event, status, cb_arg);
	if (lifecycle_cb)
		lifecycle_cb(channel, event, status, lifecycle_arg);
}

static void tr_channel_fail_lane_streams(struct tr_channel *channel,
					 enum tr_lane lane, int status)
{
	uint32_t i;

	for (i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_handle handle;
		int notify = 0;

		pthread_mutex_lock(&channel->lock);
		if (channel->streams[i].state != TR_STREAM_SLOT_FREE &&
		    channel->streams[i].lane == lane) {
			handle = tr_stream_make_handle(channel, i,
						       &channel->streams[i]);
			tr_stream_free_locked(channel, i);
			channel->stat_stream_errors++;
			notify = 1;
		}
		pthread_mutex_unlock(&channel->lock);

		if (notify)
			tr_stream_notify(channel, handle, TR_STREAM_EVENT_ERROR,
					 status);
	}
}

static uint32_t tr_reconnect_delay_ms(uint32_t initial_ms, uint32_t max_ms,
				      uint32_t attempt)
{
	uint64_t delay = initial_ms;

	while (attempt != 0 && delay < max_ms) {
		delay *= 2U;
		if (delay > max_ms)
			delay = max_ms;
		attempt--;
	}
	return (uint32_t)delay;
}

static int tr_stream_allocate_locked(struct tr_channel *channel,
				     uint32_t stream_id, enum tr_lane lane,
				     enum tr_stream_slot_state state,
				     struct tr_stream_handle *out)
{
	struct tr_stream_slot *stream;
	uint32_t generation;
	uint32_t slot = channel->free_stream_head;

	if (slot == TR_STREAM_FREE_NONE)
		return TR_AGAIN;

	stream = &channel->streams[slot];
	channel->free_stream_head = stream->free_next;
	assert(stream->generation != UINT32_MAX);
	generation = stream->generation + 1U;

	memset(stream, 0, sizeof(*stream));
	stream->generation = generation;
	stream->free_next = TR_STREAM_FREE_NONE;
	stream->state = state;
	stream->lane = lane;
	stream->stream_id = stream_id;
	stream->local_open = 1;
	stream->remote_open = 1;
	stream->next_tx_message_id = 1;
	stream->next_rx_message_id = 1;
	stream->rx_advertised_limit =
		channel->config.initial_window_bytes;

	{
		int ret = tr_stream_index_insert_locked(channel, stream_id, slot);

		if (ret != TR_OK) {
			memset(stream, 0, sizeof(*stream));
			stream->generation = generation;
			stream->free_next = TR_STREAM_FREE_NONE;
			if (generation != UINT32_MAX) {
				stream->free_next = channel->free_stream_head;
				channel->free_stream_head = slot;
			}
			return ret;
		}
	}

	if (out)
		*out = tr_stream_make_handle(channel, slot, stream);
	channel->active_streams++;
	channel->drain[channel->config.mode == TR_CHANNEL_SHARED_CONNECTION
	                       ? 0
	                       : tr_lane_index(lane)]
	        .active_streams++;
	channel->stat_streams_opened++;
	return TR_OK;
}

static int tr_channel_protocol_error(struct tr_channel *channel,
				     struct tr_conn_handle connection,
				     int status)
{
	int ret;

	(void)channel;
	/* 帧分派与延后 HELLO 重放都在反应器所有者上运行。
	 * 协议失败必须立即退役精确连接代次：tr_reactor_close() 会竞争有界
	 * 命令容量，可能返回 TR_AGAIN，使畸形对端继续存活。
	 * 通过错误事件通知 Channel，而不是报告优雅关闭。
	 *
	 * 某些内部准入失败为 TR_AGAIN；尽管此处这种失败是终止性的，
	 * 中止操作仍要求负状态码。
	 * 调用者进入此辅助函数前已释放 channel->lock，因此
	 * 同步连接事件可以安全地退役其流槽位。
	 */
	ret = tr_reactor_abort_on_owner(connection,
				      status < 0 ? status : TR_ERR_STATE);
	if (ret != TR_OK && ret != TR_ERR_STALE)
		return ret;
	return status;
}

static void tr_channel_reset_progress_on_owner(struct tr_channel *channel)
{
	uint32_t start;

	pthread_mutex_lock(&channel->lock);
	if (channel->reset_progressing || channel->teardown_detached) {
		pthread_mutex_unlock(&channel->lock);
		return;
	}
	channel->reset_progressing = 1;
	channel->reset_retry_pending = 0;
	start = channel->reset_cursor;
	pthread_mutex_unlock(&channel->lock);
	for (uint32_t visited = 0; visited < channel->config.max_streams;
	     ++visited) {
		uint32_t slot = (uint32_t)(((uint64_t)start + visited) %
		                           channel->config.max_streams);
		struct tr_stream_slot *stream;
		struct tr_stream_handle handle = {0};
		struct tr_conn_handle connection = {0};
		int closed = 0;
		int ret = TR_OK;

		pthread_mutex_lock(&channel->lock);
		stream = &channel->streams[slot];
		if (stream->state != TR_STREAM_SLOT_RESETTING) {
			pthread_mutex_unlock(&channel->lock);
			continue;
		}
		connection = tr_channel_connection(channel, stream->lane);
		if (!stream->reset_sent) {
			ret = tr_reactor_send_stream_reset_on_owner(
			        connection, stream->stream_id,
			        stream->reset_status);
			if (ret == TR_OK) {
				stream->reset_sent = 1;
				/* 固定预留每次只服务一条流；下一次从成功者之后
				 * 开始，低编号流不能持续抢占确认或主动终止。 */
				channel->reset_cursor =
				        slot + 1U == channel->config.max_streams
				                ? 0
				                : slot + 1U;
			} else if (ret == TR_AGAIN) {
				channel->reset_retry_pending = 1;
			}
		}
		if (stream->reset_sent && stream->reset_received &&
		    stream->reset_notified) {
			struct tr_stream_reset_tombstone *retired =
			        &channel->reset_tombstones
			                 [channel->reset_tombstone_cursor];
			*retired = (struct tr_stream_reset_tombstone){
			        connection, stream->stream_id,
			        stream->reset_peer_status};
			channel->reset_tombstone_cursor++;
			if (channel->reset_tombstone_cursor ==
			    channel->config.max_streams)
				channel->reset_tombstone_cursor = 0;
			handle = tr_stream_make_handle(channel, slot, stream);
			tr_stream_free_locked(channel, slot);
			channel->stat_streams_closed++;
			closed = 1;
		}
		pthread_mutex_unlock(&channel->lock);
		if (closed)
			tr_stream_notify(channel, handle,
			                 TR_STREAM_EVENT_CLOSED, TR_OK);
		else if (ret != TR_OK && ret != TR_AGAIN)
			(void)tr_channel_protocol_error(channel, connection,
			                                ret);
	}
	pthread_mutex_lock(&channel->lock);
	channel->reset_progressing = 0;
	pthread_mutex_unlock(&channel->lock);
}

static int tr_channel_handle_stream_reset(struct tr_channel *channel,
                                          struct tr_conn_handle connection,
                                          const struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	uint32_t slot;
	int notify = 0;
	int status;
	int ret;

	if (frame->header.flags || frame->payload ||
	    frame->header.payload_len || !frame->header.stream_id ||
	    frame->header.message_id > INT32_MAX ||
	    !tr_wire_stream_reset_status_valid(-(int)frame->header.message_id))
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_INVALID);
	status = -(int)frame->header.message_id;
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (!stream) {
		for (uint32_t i = 0; i < channel->config.max_streams; ++i) {
			const struct tr_stream_reset_tombstone *retired =
			        &channel->reset_tombstones[i];
			if (retired->stream_id == frame->header.stream_id &&
			    retired->peer_status == status &&
			    tr_conn_equal(retired->connection, connection)) {
				pthread_mutex_unlock(&channel->lock);
				return TR_OK;
			}
		}
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_STATE);
	}
	if (!tr_stream_connection_matches(channel, stream, connection) ||
	    !tr_stream_reset_supported_locked(channel, stream->lane) ||
	    (stream->reset_received && stream->reset_peer_status != status)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_STATE);
	}
	handle = tr_stream_make_handle(channel, slot, stream);
	pthread_mutex_unlock(&channel->lock);
	ret = tr_channel_reset_timer_on_owner(channel);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	if (stream->state != TR_STREAM_SLOT_RESETTING)
		tr_stream_reset_begin_locked(channel, stream, status);
	/* 相向主动 RESET 的原因可以不同；只约束同一发送方向的重复。 */
	stream->reset_peer_status = status;
	stream->reset_received = 1;
	stream->remote_open = 0;
	if (!stream->reset_notified) {
		stream->reset_notified = 1;
		notify = 1;
	}
	status = stream->reset_status;
	pthread_mutex_unlock(&channel->lock);
	if (notify)
		tr_stream_notify(channel, handle, TR_STREAM_EVENT_RESET,
		                 status);
	tr_channel_reset_progress_on_owner(channel);
	return TR_OK;
}

static int tr_channel_handle_hello(struct tr_channel *channel,
				   struct tr_conn_handle connection,
				   const struct tr_frame *frame)
{
	struct tr_channel_capabilities local;
	struct tr_channel_capabilities negotiated;
	struct tr_channel_capabilities ack_caps;
	uint16_t peer_min;
	uint16_t peer_max;
	uint16_t low;
	uint16_t high;
	uint32_t peer_lane_mask;
	uint32_t peer_max_frame;
	uint32_t peer_max_message;
	uint64_t peer_features;
	uint32_t expected_lane_mask;
	int control_up = 0;
	int bulk_up = 0;
	int ret;

	if (!frame->payload || frame->header.stream_id != 0 ||
	    frame->header.message_id != 0 || frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);

	ret = tr_channel_decode_hello(frame->payload->data, frame->payload->len,
				      &peer_min, &peer_max, &peer_lane_mask,
				      &peer_max_frame, &peer_max_message,
				      &peer_features);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);

	ret = tr_channel_local_capabilities(channel, connection, &local);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);

	expected_lane_mask = local.lane_mask;
	if (peer_min == 0 || peer_max == 0 || peer_min > peer_max ||
	    peer_lane_mask != expected_lane_mask || peer_max_frame == 0 ||
	    peer_max_message == 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_UNSUPPORTED);

	low = peer_min > channel->config.min_protocol_version ?
		      peer_min :
		      channel->config.min_protocol_version;
	high = peer_max < channel->config.max_protocol_version ?
		       peer_max :
		       channel->config.max_protocol_version;
	if (low > high)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_VERSION);

	memset(&negotiated, 0, sizeof(negotiated));
	negotiated.protocol_version = high;
	negotiated.lane_mask = expected_lane_mask;
	/* 对端 HELLO 声明的是对端接收能力，也就是我们的发送上限。 */
	negotiated.max_frame_payload_bytes =
		peer_max_frame < local.max_frame_payload_bytes ?
			peer_max_frame :
			local.max_frame_payload_bytes;
	negotiated.max_message_bytes = peer_max_message;
	negotiated.feature_bits = peer_features & local.feature_bits;

	if (negotiated.max_frame_payload_bytes == 0 ||
	    negotiated.max_message_bytes == 0 ||
	    (negotiated.feature_bits & channel->config.required_feature_bits) !=
	            channel->config.required_feature_bits)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_UNSUPPORTED);
	pthread_mutex_lock(&channel->lock);
	int connection_index =
	        tr_channel_keepalive_index_for_connection(channel, connection);
	int after_goaway = connection_index >= 0 &&
	                   channel->drain[connection_index].goaway_accepted;
	pthread_mutex_unlock(&channel->lock);
	if (after_goaway)
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_STATE);

	/* ACK 把本地接收上限返回给对端。 */
	ack_caps = local;
	ack_caps.protocol_version = high;
	ack_caps.feature_bits = negotiated.feature_bits;
	ret = tr_channel_send_hello_ack(connection, &ack_caps);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);

	pthread_mutex_lock(&channel->lock);
	tr_channel_set_caps_locked(channel, expected_lane_mask, &negotiated,
				   &control_up, &bulk_up);
	pthread_mutex_unlock(&channel->lock);

	if (control_up)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_CONTROL_UP, TR_OK);
	if (bulk_up)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_BULK_UP, TR_OK);
	(void)tr_channel_send_pending_goaway_on_owner(channel);
	return TR_OK;
}

struct tr_channel_pending_hello_request {
	struct tr_channel *channel;
	struct tr_channel_pending_hello hello;
};

static int
tr_channel_process_pending_hello_on_owner(void *arg)
{
	struct tr_channel_pending_hello_request *request =
		(struct tr_channel_pending_hello_request *)arg;
	struct tr_buffer payload;
	struct tr_frame frame;
	int alive;

	if (!request || !request->channel || !request->hello.valid)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&request->channel->lock);
	alive = tr_channel_connection_alive_locked(
		request->channel, request->hello.connection);
	pthread_mutex_unlock(&request->channel->lock);
	if (!alive)
		return TR_OK;

	memset(&payload, 0, sizeof(payload));
	payload.data = request->hello.payload;
	payload.capacity = TR_CHANNEL_HELLO_WIRE_SIZE;
	payload.len = TR_CHANNEL_HELLO_WIRE_SIZE;

	memset(&frame, 0, sizeof(frame));
	frame.header.version = TR_WIRE_ENV_VERSION;
	frame.header.type = TR_FRAME_HELLO;
	frame.header.payload_len = TR_CHANNEL_HELLO_WIRE_SIZE;
	frame.payload = &payload;

	/* 协议错误已经关闭连接。延后重放与正常反应器帧分派一致，
	 * 不会同步地将处理函数结果呈现给 Channel 创建者。
	 */
	(void)tr_channel_handle_hello(request->channel,
				      request->hello.connection, &frame);
	return TR_OK;
}

static int
tr_channel_defer_hello_before_start(struct tr_channel *channel,
				    struct tr_conn_handle connection,
				    const struct tr_frame *frame)
{
	int idx;

	if (!channel || !frame)
		return 0;

	pthread_mutex_lock(&channel->lock);
	if (channel->handshake_started) {
		pthread_mutex_unlock(&channel->lock);
		return 0;
	}

	idx = tr_channel_keepalive_index_for_connection(channel, connection);
	if (idx < 0 || !frame->payload ||
	    frame->header.stream_id != 0 || frame->header.message_id != 0 ||
	    frame->header.flags != 0 ||
	    frame->payload->len != TR_CHANNEL_HELLO_WIRE_SIZE) {
		pthread_mutex_unlock(&channel->lock);
		(void)tr_channel_protocol_error(channel, connection,
						TR_ERR_STATE);
		return 1;
	}

	channel->pending_hello[idx].valid = 1;
	channel->pending_hello[idx].connection = connection;
	memcpy(channel->pending_hello[idx].payload, frame->payload->data,
	       TR_CHANNEL_HELLO_WIRE_SIZE);
	pthread_mutex_unlock(&channel->lock);
	return 1;
}

static int tr_channel_handle_hello_ack(struct tr_channel *channel,
				       struct tr_conn_handle connection,
				       const struct tr_frame *frame)
{
	struct tr_channel_capabilities local;
	struct tr_channel_capabilities caps;
	uint32_t expected_lane_mask;
	int control_up = 0;
	int bulk_up = 0;
	int ret;

	if (!frame->payload || frame->header.stream_id != 0 ||
	    frame->header.message_id != 0 || frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);

	ret = tr_channel_decode_hello_ack(frame->payload->data,
					  frame->payload->len, &caps);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);
	ret = tr_channel_local_capabilities(channel, connection, &local);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);

	expected_lane_mask = local.lane_mask;
	if (caps.protocol_version < channel->config.min_protocol_version ||
	    caps.protocol_version > channel->config.max_protocol_version ||
	    caps.lane_mask != expected_lane_mask ||
	    caps.max_frame_payload_bytes == 0 || caps.max_message_bytes == 0 ||
	    (caps.feature_bits & ~local.feature_bits) != 0 ||
	    (caps.feature_bits & channel->config.required_feature_bits) !=
	            channel->config.required_feature_bits)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_UNSUPPORTED);

	if (caps.max_frame_payload_bytes > local.max_frame_payload_bytes)
		caps.max_frame_payload_bytes = local.max_frame_payload_bytes;

	pthread_mutex_lock(&channel->lock);
	tr_channel_set_caps_locked(channel, expected_lane_mask, &caps,
				   &control_up, &bulk_up);
	pthread_mutex_unlock(&channel->lock);

	if (control_up)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_CONTROL_UP, TR_OK);
	if (bulk_up)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_BULK_UP, TR_OK);
	(void)tr_channel_send_pending_goaway_on_owner(channel);
	return TR_OK;
}

static int tr_channel_handle_goaway(struct tr_channel *channel,
				    struct tr_conn_handle connection,
				    const struct tr_frame *frame)
{
	uint32_t mask;
	uint32_t cutoff;
	int idx;
	int ret;
	int control_event = 0;
	int bulk_event = 0;

	if (frame->payload || frame->header.payload_len != 0 ||
	    frame->header.stream_id != 0 ||
	    frame->header.message_id > UINT32_MAX || frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	cutoff = (uint32_t)frame->header.message_id;

	pthread_mutex_lock(&channel->lock);
	idx = tr_channel_keepalive_index_for_connection(channel, connection);
	if (idx < 0 || (cutoff && !tr_stream_id_is_local(channel, cutoff)) ||
	    cutoff != channel->drain[idx].last_local_accepted ||
	    (channel->drain[idx].peer_started &&
	     cutoff != channel->drain[idx].peer_cutoff)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_STATE);
	}
	if (channel->drain[idx].peer_started) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_drain_progress_on_owner(channel);
	}
	/* 接收方在初始 WINDOW_UPDATE 获得发送责任后才推进受理边界，
	 * GOAWAY 的固定命令前沿与 TX FIFO 保留该 WU 为前驱。因此 cutoff
	 * 必须恰好等于本连接已确认的持久高水位，不能否定历史准入，也不能
	 * 声称尚未确认、另一连接或未发布空洞中的 ID 已被准入。
	 * 同一 FIFO 内被边界覆盖的更早 OPEN 也不能仍缺初始 WU。 */
	for (uint32_t i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_slot *stream = &channel->streams[i];
		if (stream->state != TR_STREAM_SLOT_FREE &&
		    tr_stream_connection_matches(channel, stream, connection) &&
		    tr_stream_id_is_local(channel, stream->stream_id) &&
		    ((stream->stream_id > cutoff &&
		      stream->state != TR_STREAM_SLOT_OPENING) ||
		     (stream->stream_id <= cutoff &&
		      stream->state == TR_STREAM_SLOT_OPENING))) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
			                                 TR_ERR_STATE);
		}
	}
	pthread_mutex_unlock(&channel->lock);
	ret = tr_channel_reset_timer_on_owner(channel);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);

	pthread_mutex_lock(&channel->lock);
	channel->drain[idx].peer_started = 1;
	channel->drain[idx].peer_cutoff = cutoff;
	if (!channel->drain[idx].local_started) {
		channel->drain[idx].local_started = 1;
		channel->drain[idx].local_cutoff =
		        channel->drain[idx].last_peer_admitted;
	}
	channel->drain_retry_pending = 1;
	mask = tr_channel_connection_lane_mask_locked(channel, connection);
	if ((mask & TR_CHANNEL_LANE_MASK_CONTROL) &&
	    !channel->control_peer_draining) {
		channel->control_peer_draining = 1;
		control_event = 1;
	}
	if ((mask & TR_CHANNEL_LANE_MASK_BULK) &&
	    !channel->bulk_peer_draining) {
		channel->bulk_peer_draining = 1;
		bulk_event = 1;
	}
	pthread_mutex_unlock(&channel->lock);
	ret = tr_channel_drain_progress_on_owner(channel);
	if (ret != TR_OK)
		return ret;

	for (uint32_t i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_handle handle = {0};
		pthread_mutex_lock(&channel->lock);
		struct tr_stream_slot *stream = &channel->streams[i];
		if (stream->state == TR_STREAM_SLOT_OPENING &&
		    tr_stream_connection_matches(channel, stream, connection) &&
		    tr_stream_id_is_local(channel, stream->stream_id) &&
		    stream->stream_id > cutoff) {
			handle = tr_stream_make_handle(channel, i, stream);
			tr_stream_free_locked(channel, i);
			channel->stat_stream_errors++;
			channel->stat_streams_closed++;
		}
		pthread_mutex_unlock(&channel->lock);
		if (handle.channel) {
			/* 拒绝本身已证明对端没有取得流；没有 CLOSE/RESET 回包，
			 * 也不留下逐流拒绝表。先失效精确代次再通知上层。 */
			tr_stream_notify(channel, handle, TR_STREAM_EVENT_ERROR,
			                 TR_ERR_CLOSED);
		}
	}

	if (control_event)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_CONTROL_GOAWAY,
				  TR_OK);
	if (bulk_event)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_BULK_GOAWAY, TR_OK);
	return tr_channel_drain_progress_on_owner(channel);
}

static int tr_channel_handle_stream_open(struct tr_channel *channel,
					 struct tr_conn_handle connection,
					 const struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	enum tr_lane lane;
	uint32_t slot = 0;
	int connection_index;
	int ret;

	if (frame->payload || frame->header.payload_len != 0 ||
	    (frame->header.flags & ~(uint32_t)TR_FRAME_F_LANE_BULK) != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	lane = (frame->header.flags & TR_FRAME_F_LANE_BULK) ? TR_LANE_BULK :
							      TR_LANE_CONTROL;

	if (!frame->header.stream_id ||
	    !tr_conn_equal(tr_channel_connection(channel, lane), connection) ||
	    tr_stream_id_is_local(channel, frame->header.stream_id))
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);

	pthread_mutex_lock(&channel->lock);
	connection_index =
	        tr_channel_keepalive_index_for_connection(channel, connection);
	/* 同一物理连接上的 wire ID 不重复。拆分连接各自有
	 * FIFO 前沿，不能用另一个 TCP 先到的较大 ID 拒绝合法 OPEN。 */
	if (connection_index < 0 ||
	    channel->drain[connection_index].peer_started ||
	    frame->header.stream_id <=
	            channel->peer_stream_frontier[connection_index]) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_STATE);
	}
	if (tr_stream_lookup_id(channel, frame->header.stream_id, NULL)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}
	channel->peer_stream_frontier[connection_index] =
	        frame->header.stream_id;

	/* begin_drain() 是单向准入屏障。对端可能在收到我们的 GOAWAY 前
	 * 刚发出 STREAM_OPEN；若在 local_draining 可见后接受该延迟帧，
	 * active_streams 可能在 wait_drained() 已观察到零后再次增加。
	 *
	 * 拒绝时不分配本地流或发送逐流 CLOSE。GOAWAY 的冻结边界直接
	 * 终结对端未获初始 WINDOW_UPDATE 的槽位；对端自己的 GOAWAY
	 * 则是最后一个可能的 OPEN 的 FIFO 屏障。未确认流禁止发送 CLOSE，
	 * 因而无需吸收未知 ID 或保留无界拒绝表。
	 */
	if (channel->local_draining ||
	    channel->drain[connection_index].local_started) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}

	ret = tr_stream_allocate_locked(channel, frame->header.stream_id, lane,
					TR_STREAM_SLOT_OPEN, &handle);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&channel->lock);
		/* 没有槽就没有可重试的拒绝责任；不能静默遗留对端 OPENING。
		 * 本阶段不增加第二个拒绝表，明确使此精确连接失败。 */
		return tr_channel_protocol_error(channel, connection,
		                                 ret == TR_AGAIN ? TR_ERR_NOMEM
		                                                 : ret);
	}

	stream = &channel->streams[handle.slot];
	slot = handle.slot;
	stream->tx_send_limit = frame->header.message_id;
	stream->opened_notified = 1;

	ret = tr_reactor_send(tr_channel_connection(channel, lane),
			      TR_FRAME_WINDOW_UPDATE, 0, stream->stream_id,
			      stream->rx_advertised_limit, NULL);
	if (ret != TR_OK) {
		tr_stream_free_locked(channel, slot);
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection, ret);
	}
	channel->stat_window_updates_tx++;
	channel->drain[connection_index].last_peer_admitted = stream->stream_id;

	handle = tr_stream_make_handle(channel, slot, stream);
	pthread_mutex_unlock(&channel->lock);
	tr_stream_notify(channel, handle, TR_STREAM_EVENT_OPENED, TR_OK);
	return TR_OK;
}

static int tr_channel_handle_window_update(struct tr_channel *channel,
					   struct tr_conn_handle connection,
					   const struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	int notify_opened = 0;
	int notify_writable = 0;
	uint64_t old_limit;
	uint32_t slot;

	if (frame->payload || frame->header.payload_len != 0 ||
	    frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (!stream ||
	    !tr_stream_connection_matches(channel, stream, connection) ||
	    frame->header.message_id < stream->tx_send_limit ||
	    frame->header.message_id < stream->tx_sent_bytes) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}

	old_limit = stream->tx_send_limit;
	stream->tx_send_limit = frame->header.message_id;
	if (tr_stream_id_is_local(channel, stream->stream_id)) {
		int idx = tr_channel_keepalive_index_for_connection(channel,
		                                                    connection);
		assert(idx >= 0);
		if (stream->stream_id > channel->drain[idx].last_local_accepted)
			channel->drain[idx].last_local_accepted =
			        stream->stream_id;
	}
	channel->stat_window_updates_rx++;
	if (stream->state == TR_STREAM_SLOT_RESETTING) {
		int received = stream->reset_received;
		pthread_mutex_unlock(&channel->lock);
		return received ? tr_channel_protocol_error(channel, connection,
		                                            TR_ERR_STATE)
		                : TR_OK;
	}
	if (stream->state == TR_STREAM_SLOT_OPENING) {
		stream->state = TR_STREAM_SLOT_OPEN;
		if (!stream->opened_notified) {
			stream->opened_notified = 1;
			notify_opened = 1;
		}
	} else if (stream->tx_send_limit > old_limit) {
		notify_writable = 1;
	}
	handle = tr_stream_make_handle(channel, slot, stream);
	pthread_mutex_unlock(&channel->lock);

	if (notify_opened)
		tr_stream_notify(channel, handle, TR_STREAM_EVENT_OPENED,
				 TR_OK);
	else if (notify_writable)
		tr_stream_notify(channel, handle, TR_STREAM_EVENT_WRITABLE,
				 TR_OK);
	return TR_OK;
}

/* RESETTING 只保留常数状态，不再预约重组缓冲区、交付上层或补信用。
 * 主动取消及资源失败仍校验原有消息序号与分片；已因消息语义失步而终止
 * 的流不能再对续片重复施加失效的逻辑规则，但仍只吸收精确连接/流、
 * 有效线帧且在冻结额度内的字节。对端 RESET/CLOSE 屏障后不再接收 DATA。 */
static int tr_stream_reset_discard_locked(struct tr_channel *channel,
                                          struct tr_stream_slot *stream,
                                          const struct tr_frame *frame,
                                          uint32_t payload_len)
{
	uint32_t flags = frame->header.flags;
	int first = (flags & TR_FRAME_F_FIRST) != 0;
	int last = (flags & TR_FRAME_F_LAST) != 0;
	uint64_t total;

	if (stream->reset_received || !stream->remote_open)
		return TR_ERR_STATE;
	if (UINT64_MAX - stream->rx_received_bytes < payload_len ||
	    stream->rx_received_bytes + payload_len >
	            stream->rx_advertised_limit)
		return TR_ERR_BAD_LENGTH;
	if (!stream->reset_discard_unordered) {
		if (!frame->header.message_id ||
		    frame->header.message_id != stream->next_rx_message_id ||
		    (first && stream->reset_fragmented) ||
		    (!first && !stream->reset_fragmented))
			return TR_ERR_STATE;
		total = first ? payload_len
		              : stream->reset_message_bytes + payload_len;
		if ((channel->config.max_message_bytes &&
		     total > channel->config.max_message_bytes) ||
		    (!(first && last) && (!channel->config.reassembly_pool ||
		                          !channel->config.max_message_bytes)))
			return TR_ERR_BAD_LENGTH;
		stream->reset_fragmented = !last;
		stream->reset_message_bytes = last ? 0 : total;
		if (last)
			stream->next_rx_message_id++;
	}
	stream->rx_received_bytes += payload_len;
	stream->rx_consumed_bytes += payload_len;
	return TR_OK;
}

/* 仅在帧边界、流身份及 advertised credit 已可信时隔离接收失败。
 * 本方向已接受普通 CLOSE 后不能再追加主动 RESET；未协商能力或无法
 * 取得可靠重试责任时也只能中止精确连接，不能静默丢弃终止责任。 */
static int tr_channel_fail_stream_receive_on_owner(
        struct tr_channel *channel, struct tr_conn_handle connection,
        struct tr_stream_handle handle, const struct tr_frame *frame,
        uint32_t payload_len, int status, int unordered)
{
	struct tr_stream_slot *stream;
	int ret;

	ret = tr_channel_reset_timer_on_owner(channel);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream || stream->state != TR_STREAM_SLOT_OPEN ||
	    !stream->local_open ||
	    !tr_stream_reset_supported_locked(channel, stream->lane)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection, status);
	}
	tr_stream_reset_begin_locked(channel, stream, status);
	stream->reset_discard_unordered = unordered;
	ret = tr_stream_reset_discard_locked(channel, stream, frame,
	                                     payload_len);
	if (ret == TR_OK)
		stream->reset_notified = 1;
	pthread_mutex_unlock(&channel->lock);
	if (ret != TR_OK)
		return tr_channel_protocol_error(channel, connection, ret);
	tr_stream_notify(channel, handle, TR_STREAM_EVENT_RESET, status);
	tr_channel_reset_progress_on_owner(channel);
	return TR_OK;
}

static int tr_channel_handle_data(struct tr_channel *channel,
				  struct tr_conn_handle connection,
				  struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	struct tr_buffer *deliver = NULL;
	enum tr_stream_data_disposition disposition = TR_STREAM_DATA_RELEASE;
	tr_stream_data_cb data_cb;
	void *cb_arg;
	uint32_t slot;
	uint32_t flags;
	uint32_t payload_len = frame->payload ? frame->payload->len : 0U;
	uint32_t logical_len = payload_len;
	uint64_t new_received;
	int fragmented;
	int complete;
	int unordered = 1;
	int ret = TR_OK;

	flags = frame->header.flags;
	if ((flags & ~(TR_FRAME_F_FIRST | TR_FRAME_F_LAST)) != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);
	if (payload_len != frame->header.payload_len)
		return tr_channel_protocol_error(channel, connection,
		                                 TR_ERR_BAD_LENGTH);

	fragmented = (flags & (TR_FRAME_F_FIRST | TR_FRAME_F_LAST)) !=
		     (TR_FRAME_F_FIRST | TR_FRAME_F_LAST);
	complete = (flags & TR_FRAME_F_LAST) != 0;

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (stream && stream->state == TR_STREAM_SLOT_RESETTING &&
	    tr_stream_connection_matches(channel, stream, connection)) {
		ret = tr_stream_reset_discard_locked(channel, stream, frame,
		                                     payload_len);
		pthread_mutex_unlock(&channel->lock);
		return ret == TR_OK ? TR_OK
		                    : tr_channel_protocol_error(
		                              channel, connection, ret);
	}
	if (!stream || stream->state != TR_STREAM_SLOT_OPEN ||
	    !stream->remote_open ||
	    !tr_stream_connection_matches(channel, stream, connection)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}

	if (UINT64_MAX - stream->rx_received_bytes < payload_len) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_LENGTH);
	}
	new_received = stream->rx_received_bytes + payload_len;
	if (new_received > stream->rx_advertised_limit) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}
	if (!frame->header.message_id ||
	    frame->header.message_id != stream->next_rx_message_id) {
		ret = TR_ERR_STATE;
		goto fail_stream;
	}

	/* 单帧消息保持零拷贝。
	 * 分片消息只复制一次到有界的 Channel 重组缓冲区，
	 * 之后传输帧缓冲区可以立即归还反应器接收池。
	 */
	if (!fragmented) {
		if (stream->rx_reassembly ||
		    (channel->config.max_message_bytes != 0 &&
		     payload_len > channel->config.max_message_bytes)) {
			ret = TR_ERR_STATE;
			goto fail_stream;
		}

		stream->rx_received_bytes = new_received;
		stream->next_rx_message_id++;
		deliver = frame->payload;
	} else if (flags & TR_FRAME_F_FIRST) {
		if (stream->rx_reassembly || !channel->config.reassembly_pool ||
		    channel->config.max_message_bytes == 0 ||
		    payload_len > channel->config.max_message_bytes) {
			ret = TR_ERR_STATE;
			goto fail_stream;
		}

		ret = tr_buffer_acquire(channel->config.reassembly_pool,
					channel->config.max_message_bytes,
					&stream->rx_reassembly);
		if (ret != TR_OK) {
			ret = ret == TR_AGAIN || ret == TR_ERR_NOMEM
			              ? TR_ERR_NOMEM
			              : TR_ERR_SYS;
			unordered = 0;
			goto fail_stream;
		}

		if (payload_len != 0)
			memcpy(stream->rx_reassembly->data,
			       frame->payload->data, payload_len);
		stream->rx_reassembly->len = payload_len;
		stream->rx_reassembly_message_id = frame->header.message_id;
		stream->rx_received_bytes = new_received;
	} else {
		uint64_t combined_len;

		if (!stream->rx_reassembly ||
		    stream->rx_reassembly_message_id !=
			    frame->header.message_id) {
			ret = TR_ERR_STATE;
			goto fail_stream;
		}

		combined_len =
			(uint64_t)stream->rx_reassembly->len + payload_len;
		if (combined_len > channel->config.max_message_bytes ||
		    combined_len > stream->rx_reassembly->capacity) {
			ret = TR_ERR_STATE;
			goto fail_stream;
		}

		if (payload_len != 0)
			memcpy(stream->rx_reassembly->data +
				       stream->rx_reassembly->len,
			       frame->payload->data, payload_len);
		stream->rx_reassembly->len = (uint32_t)combined_len;
		stream->rx_received_bytes = new_received;
	}

	if (fragmented && complete) {
		if (!stream->rx_reassembly ||
		    stream->rx_reassembly_message_id !=
			    frame->header.message_id) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 TR_ERR_STATE);
		}

		deliver = tr_buffer_take(&stream->rx_reassembly);
		logical_len = deliver->len;
		stream->rx_reassembly_message_id = 0;
		stream->next_rx_message_id++;
	} else if (fragmented) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}

	channel->stat_messages_rx++;
	channel->stat_bytes_rx += logical_len;

	handle = tr_stream_make_handle(channel, slot, stream);
	data_cb = channel->data_cb;
	cb_arg = channel->callback_arg;
	pthread_mutex_unlock(&channel->lock);

	if (data_cb)
		disposition = data_cb(handle, frame->header.message_id, deliver,
				      cb_arg);

	if (disposition == TR_STREAM_DATA_TAKE_OWNERSHIP) {
		if (!fragmented)
			(void)tr_buffer_take(&frame->payload);
		return TR_OK;
	}

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (stream)
		ret = tr_stream_consume_locked(channel, stream, logical_len);
	pthread_mutex_unlock(&channel->lock);
	if (ret)
		(void)tr_channel_schedule_window_retry(channel);

	if (fragmented && deliver)
		tr_buffer_release(deliver);
	return TR_OK;

fail_stream:
	handle = tr_stream_make_handle(channel, slot, stream);
	pthread_mutex_unlock(&channel->lock);
	return tr_channel_fail_stream_receive_on_owner(
	        channel, connection, handle, frame, payload_len, ret,
	        unordered);
}

static int tr_channel_handle_stream_close(struct tr_channel *channel,
					  struct tr_conn_handle connection,
					  const struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	enum tr_stream_event event = TR_STREAM_EVENT_REMOTE_CLOSED;
	uint32_t slot;
	uint32_t generation;
	int free_after = 0;

	if (frame->payload || frame->header.payload_len != 0 ||
	    frame->header.flags != 0 || frame->header.stream_id == 0 ||
	    frame->header.message_id != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (!stream || stream->state == TR_STREAM_SLOT_OPENING) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}
	if (!tr_stream_connection_matches(channel, stream, connection)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}
	if (stream->state == TR_STREAM_SLOT_RESETTING) {
		int received = stream->reset_received;
		stream->remote_open = 0;
		pthread_mutex_unlock(&channel->lock);
		/* 普通 CLOSE 可能先于对端的响应 RESET 已在 FIFO 中。
		 * 它只结束旧半侧，不取代双向 RESET 的静默屏障。 */
		return received ? tr_channel_protocol_error(channel, connection,
		                                            TR_ERR_STATE)
		                : TR_OK;
	}

	stream->remote_open = 0;
	handle = tr_stream_make_handle(channel, slot, stream);
	generation = stream->generation;
	if (!stream->local_open) {
		event = TR_STREAM_EVENT_CLOSED;
		free_after = 1;
	}
	pthread_mutex_unlock(&channel->lock);

	tr_stream_notify(channel, handle, event, TR_OK);

	if (free_after) {
		pthread_mutex_lock(&channel->lock);
		stream = &channel->streams[slot];
		if (stream->generation == generation && !stream->local_open &&
		    !stream->remote_open) {
			tr_stream_free_locked(channel, slot);
			channel->stat_streams_closed++;
		}
		pthread_mutex_unlock(&channel->lock);
	}
	return tr_channel_drain_progress_on_owner(channel);
}

static enum tr_frame_disposition
tr_channel_on_frame(struct tr_conn_handle connection, struct tr_frame *frame,
		    void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	int ret = TR_OK;

	/* 延后 Channel 创建会在上层就绪前安装传输处理函数。
	 * 过早的对端 HELLO 不得回复 ACK 或发布通道 UP，
	 * 否则对端可以向未绑定的 Channel 发送应用 DATA。
	 */
	if (frame->header.type == TR_FRAME_HELLO &&
	    tr_channel_defer_hello_before_start(channel, connection, frame))
		return TR_FRAME_RELEASE;

	if (frame->header.type != TR_FRAME_HELLO &&
	    frame->header.type != TR_FRAME_HELLO_ACK) {
		int ready;
		pthread_mutex_lock(&channel->lock);
		ready = tr_channel_connection_ready_locked(channel, connection);
		pthread_mutex_unlock(&channel->lock);
		if (!ready) {
			(void)tr_channel_protocol_error(channel, connection,
							TR_ERR_STATE);
			return TR_FRAME_RELEASE;
		}
	}

	switch (frame->header.type) {
	case TR_FRAME_HELLO:
		ret = tr_channel_handle_hello(channel, connection, frame);
		break;
	case TR_FRAME_HELLO_ACK:
		ret = tr_channel_handle_hello_ack(channel, connection, frame);
		break;
	case TR_FRAME_GOAWAY:
		ret = tr_channel_handle_goaway(channel, connection, frame);
		break;
	case TR_FRAME_STREAM_OPEN:
		ret = tr_channel_handle_stream_open(channel, connection, frame);
		break;
	case TR_FRAME_WINDOW_UPDATE:
		ret = tr_channel_handle_window_update(channel, connection,
						      frame);
		break;
	case TR_FRAME_STREAM_CLOSE:
		ret = tr_channel_handle_stream_close(channel, connection,
						     frame);
		break;
	case TR_FRAME_STREAM_RESET:
		ret = tr_channel_handle_stream_reset(channel, connection,
		                                     frame);
		break;
	case TR_FRAME_DATA:
		ret = tr_channel_handle_data(channel, connection, frame);
		break;
	case TR_FRAME_PING: {
		if (frame->payload || frame->header.payload_len != 0 ||
		    frame->header.flags != 0 || frame->header.stream_id != 0) {
			ret = tr_channel_protocol_error(channel, connection,
			                                TR_ERR_STATE);
			break;
		}
		pthread_mutex_lock(&channel->lock);
		int idle =
		        tr_channel_connection_idle_locked(channel, connection);
		pthread_mutex_unlock(&channel->lock);
		/* 双 GOAWAY 且无流后停止生成保活回包，使发送排空成为
		 * 稳定边界；这不放宽 DATA、CLOSE 或 RESET 的身份验证。 */
		if (!idle)
			(void)tr_reactor_send(connection, TR_FRAME_PONG, 0, 0,
			                      frame->header.message_id, NULL);
		break;
	}
	case TR_FRAME_PONG: {
		uint64_t now = tr_channel_now_ns();
		int idx;

		pthread_mutex_lock(&channel->lock);
		idx = tr_channel_keepalive_index_for_connection(channel,
								connection);
		if (idx >= 0 && channel->keepalive_ping_outstanding[idx] &&
		    channel->keepalive_ping_id[idx] ==
			    frame->header.message_id) {
			if (now >= channel->keepalive_ping_sent_ns[idx])
				channel->keepalive_last_rtt_ns[idx] =
					now -
					channel->keepalive_ping_sent_ns[idx];
			tr_channel_keepalive_reset_locked(
				channel,
				idx == 0 ? TR_LANE_CONTROL : TR_LANE_BULK);
			channel->stat_keepalive_pongs_received++;
		}
		pthread_mutex_unlock(&channel->lock);
		break;
	}
	default:
		ret = tr_channel_protocol_error(channel, connection,
						TR_ERR_BAD_TYPE);
		break;
	}

	(void)ret;
	(void)tr_channel_drain_progress_on_owner(channel);
	return TR_FRAME_RELEASE;
}

static int tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel);

static void tr_channel_on_connection_event(struct tr_conn_handle connection,
					   enum tr_connection_event event,
					   int status, void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	int control_down = 0;
	int bulk_down = 0;
	int stream_status = status == TR_OK ? TR_ERR_CLOSED : status;

	pthread_mutex_lock(&channel->lock);
	if (event == TR_CONN_EVENT_TX_DRAINED) {
		int idx = tr_channel_keepalive_index_for_connection(channel,
		                                                    connection);
		if (idx >= 0 && channel->drain[idx].tx_wait_requested &&
		    channel->drain[idx].goaway_accepted &&
		    tr_channel_connection_alive_locked(channel, connection) &&
		    tr_channel_connection_idle_locked(channel, connection)) {
			channel->drain[idx].tx_drained = 1;
			pthread_cond_broadcast(&channel->state_cond);
		}
		pthread_mutex_unlock(&channel->lock);
		return;
	}
	if (tr_conn_equal(channel->control_connection, connection) &&
	    channel->control_alive) {
		channel->control_alive = 0;
		channel->control_ready = 0;
		memset(&channel->control_caps, 0,
		       sizeof(channel->control_caps));
		channel->control_reconnecting = 0;
		tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
		control_down = 1;
	}
	if (tr_conn_equal(channel->bulk_connection, connection) &&
	    channel->bulk_alive) {
		channel->bulk_alive = 0;
		channel->bulk_ready = 0;
		memset(&channel->bulk_caps, 0, sizeof(channel->bulk_caps));
		channel->bulk_reconnecting = 0;
		tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
		bulk_down = 1;
	}
	if (control_down || bulk_down) {
		int i;
		for (i = 0; i < 2; ++i)
			if (channel->pending_hello[i].valid &&
			    tr_conn_equal(channel->pending_hello[i].connection,
					  connection))
				memset(&channel->pending_hello[i], 0,
				       sizeof(channel->pending_hello[i]));
		pthread_cond_broadcast(&channel->state_cond);
	}
	pthread_mutex_unlock(&channel->lock);

	/* 传输连接替换不保留旧流的字节状态。 */
	if (control_down)
		tr_channel_fail_lane_streams(channel, TR_LANE_CONTROL,
					     stream_status);
	if (bulk_down)
		tr_channel_fail_lane_streams(channel, TR_LANE_BULK,
					     stream_status);
	(void)tr_channel_drain_progress_on_owner(channel);

	if (control_down)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_CONTROL_DOWN,
				  stream_status);
	if (bulk_down)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_BULK_DOWN,
				  stream_status);

	if (control_down || bulk_down)
		tr_channel_reconnect_schedule_on_owner(channel);
}

static int tr_channel_reconnect_pick_locked(struct tr_channel *channel,
					    enum tr_lane *lane, uint16_t *port,
					    uint32_t *attempt)
{
	if (!channel->reconnect_enabled || channel->local_draining)
		return 0;

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		if ((!channel->control_alive || !channel->bulk_alive) &&
		    !channel->control_peer_draining &&
		    !channel->bulk_peer_draining &&
		    !channel->control_reconnecting &&
		    !channel->bulk_reconnecting) {
			channel->control_reconnecting = 1;
			channel->bulk_reconnecting = 1;
			*lane = TR_LANE_CONTROL;
			*port = channel->reconnect_control_port;
			*attempt = channel->control_reconnect_attempt;
			return 1;
		}
		return 0;
	}

	/* 一个 Channel 拥有一个重连定时器和一个 Connector。分离模式下，
	 * 当第一条通道的延迟或进行中尝试仍拥有这些共享事件源时，
	 * 不要让第二条通道替换 reconnect_lane。
	 * 完成后会在释放所有权后调度另一条已断开的通道。
	 */
	if (channel->control_reconnecting || channel->bulk_reconnecting)
		return 0;

	/* 尝试失败后优先选择另一条通道。单个 Connector 仍仅由一条通道拥有，
	 * 但另一通道仍断开时，任何一个失败对端都不能独占全部重试机会。
	 */
	if (channel->reconnect_lane == TR_LANE_CONTROL &&
	    channel->control_reconnect_attempt != 0U &&
	    !channel->bulk_alive && !channel->bulk_peer_draining) {
		channel->bulk_reconnecting = 1;
		*lane = TR_LANE_BULK;
		*port = channel->reconnect_bulk_port;
		*attempt = channel->bulk_reconnect_attempt;
		return 1;
	}

	if (channel->reconnect_lane == TR_LANE_BULK &&
	    channel->bulk_reconnect_attempt != 0U &&
	    !channel->control_alive && !channel->control_peer_draining) {
		channel->control_reconnecting = 1;
		*lane = TR_LANE_CONTROL;
		*port = channel->reconnect_control_port;
		*attempt = channel->control_reconnect_attempt;
		return 1;
	}

	if (!channel->control_alive && !channel->control_peer_draining &&
	    !channel->control_reconnecting) {
		channel->control_reconnecting = 1;
		*lane = TR_LANE_CONTROL;
		*port = channel->reconnect_control_port;
		*attempt = channel->control_reconnect_attempt;
		return 1;
	}
	if (!channel->bulk_alive && !channel->bulk_peer_draining &&
	    !channel->bulk_reconnecting) {
		channel->bulk_reconnecting = 1;
		*lane = TR_LANE_BULK;
		*port = channel->reconnect_bulk_port;
		*attempt = channel->bulk_reconnect_attempt;
		return 1;
	}
	return 0;
}

static int tr_channel_lane_still_down_locked(const struct tr_channel *channel,
					     enum tr_lane lane)
{
	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION)
		return !channel->control_alive || !channel->bulk_alive;
	return lane == TR_LANE_BULK ? !channel->bulk_alive :
				      !channel->control_alive;
}

static int tr_channel_reconnect_attempt_active_locked(
	const struct tr_channel *channel, enum tr_lane lane)
{
	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION)
		return channel->control_reconnecting &&
		       channel->bulk_reconnecting &&
		       channel->reconnect_lane == TR_LANE_CONTROL;
	return channel->reconnect_lane == lane &&
	       (lane == TR_LANE_BULK ? channel->bulk_reconnecting :
				       channel->control_reconnecting);
}

static void tr_channel_reconnect_clear_attempt_locked(
	struct tr_channel *channel, enum tr_lane lane)
{
	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		channel->control_reconnecting = 0;
		channel->bulk_reconnecting = 0;
	} else if (lane == TR_LANE_BULK) {
		channel->bulk_reconnecting = 0;
	} else {
		channel->control_reconnecting = 0;
	}
}

static void tr_channel_reconnect_finish_attempt(
	struct tr_channel *channel, enum tr_lane lane, int success)
{
	pthread_mutex_lock(&channel->lock);
	channel->stat_reconnect_attempts++;
	if (success)
		channel->stat_reconnect_successes++;

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		channel->control_reconnecting = 0;
		channel->bulk_reconnecting = 0;
		if (success) {
			channel->control_reconnect_attempt = 0;
			channel->bulk_reconnect_attempt = 0;
		} else if (!channel->control_alive || !channel->bulk_alive) {
			if (channel->control_reconnect_attempt != UINT32_MAX)
				channel->control_reconnect_attempt++;
			channel->bulk_reconnect_attempt =
				channel->control_reconnect_attempt;
		}
	} else if (lane == TR_LANE_BULK) {
		channel->bulk_reconnecting = 0;
		if (success)
			channel->bulk_reconnect_attempt = 0;
		else if (!channel->bulk_alive &&
			 channel->bulk_reconnect_attempt != UINT32_MAX)
			channel->bulk_reconnect_attempt++;
	} else {
		channel->control_reconnecting = 0;
		if (success)
			channel->control_reconnect_attempt = 0;
		else if (!channel->control_alive &&
			 channel->control_reconnect_attempt != UINT32_MAX)
			channel->control_reconnect_attempt++;
	}
	pthread_mutex_unlock(&channel->lock);
}

static int tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel);

static void tr_channel_reconnect_connector_complete(
	int status, int fd, void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_conn_handle connection;
	enum tr_lane lane;
	int success = 0;
	int ret;

	if (!channel) {
		tr_socket_close(&fd);
		return;
	}

	pthread_mutex_lock(&channel->lock);
	lane = channel->reconnect_lane;
	if (!channel->reconnect_enabled ||
	    !tr_channel_reconnect_attempt_active_locked(channel, lane) ||
	    !tr_channel_lane_still_down_locked(channel, lane)) {
		tr_channel_reconnect_clear_attempt_locked(channel, lane);
		pthread_mutex_unlock(&channel->lock);
		tr_socket_close(&fd);
		tr_channel_reconnect_schedule_on_owner(channel);
		return;
	}
	pthread_mutex_unlock(&channel->lock);

	if (status == TR_OK) {
		memset(&connection, 0, sizeof(connection));
		ret = tr_reactor_adopt_fd(channel->reactor, fd, &connection);
		if (ret == TR_OK) {
			fd = -1;
			ret = tr_channel_replace_connection(
				channel, lane, connection);
			if (ret != TR_OK)
				(void)tr_reactor_close_on_owner(connection);
		}
		if (ret == TR_OK)
			success = 1;
	}
	tr_socket_close(&fd);

	tr_channel_reconnect_finish_attempt(channel, lane, success);
	tr_channel_reconnect_schedule_on_owner(channel);
}

static uint64_t tr_channel_reconnect_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	enum tr_lane lane;
	uint16_t port;
	int ret;
	int valid;

	(void)now_ns;
	pthread_mutex_lock(&channel->lock);
	lane = channel->reconnect_lane;
	valid = channel->reconnect_enabled &&
		tr_channel_reconnect_attempt_active_locked(channel, lane) &&
		tr_channel_lane_still_down_locked(channel, lane);
	port = lane == TR_LANE_BULK ?
		       channel->reconnect_bulk_port :
		       channel->reconnect_control_port;
	if (!valid)
		tr_channel_reconnect_clear_attempt_locked(channel, lane);
	pthread_mutex_unlock(&channel->lock);

	if (!valid) {
		tr_channel_reconnect_schedule_on_owner(channel);
		return 0U;
	}

	ret = tr_connector_start(
		channel->reconnect_connector, channel->reconnect_address,
		port, NULL, 0U);
	if (ret != TR_OK) {
		int still_active;

		/* connector 在进入活跃状态后失败时会同步或异步调用完成回调。
		 * 只有启动在发布 connector
		 * 所有权前失败，本次尝试才仍由这里收敛。
		 */
		pthread_mutex_lock(&channel->lock);
		still_active =
			tr_channel_reconnect_attempt_active_locked(channel, lane);
		pthread_mutex_unlock(&channel->lock);
		if (still_active) {
			tr_channel_reconnect_finish_attempt(channel, lane, 0);
			tr_channel_reconnect_schedule_on_owner(channel);
		}
	}
	return 0U;
}

static int tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel)
{
	enum tr_lane lane = TR_LANE_CONTROL;
	uint16_t port = 0U;
	uint32_t attempt = 0U;
	uint32_t delay_ms;
	uint64_t now_ns;
	uint64_t delay_ns;
	uint64_t deadline_ns;
	int picked;
	int ret;

	pthread_mutex_lock(&channel->lock);
	picked = tr_channel_reconnect_pick_locked(
		channel, &lane, &port, &attempt);
	if (picked)
		channel->reconnect_lane = lane;
	pthread_mutex_unlock(&channel->lock);
	if (!picked)
		return TR_OK;

	(void)port;
	delay_ms = tr_reconnect_delay_ms(
		channel->reconnect_initial_delay_ms,
		channel->reconnect_max_delay_ms, attempt);
	now_ns = tr_channel_now_ns();
	if (now_ns == 0U) {
		ret = TR_ERR_SYS;
		goto arm_failed;
	}
	delay_ns = (uint64_t)delay_ms * UINT64_C(1000000);
	deadline_ns = UINT64_MAX - now_ns < delay_ns ?
			      UINT64_MAX : now_ns + delay_ns;
	ret = tr_reactor_timer_arm(channel->reconnect_timer, deadline_ns);
	if (ret == TR_OK)
		return TR_OK;

arm_failed:
	/* 没有设置定时器，因此没有未来的所有者回调来推进本次尝试。
	 * 保守拒绝，而不是静默留下已启用但永久空闲的重连。
	 * 保留定时器与 Connector 所有权，供正常禁用或销毁拆除屏障处理。
	 */
	tr_channel_reconnect_finish_attempt(channel, lane, 0);
	pthread_mutex_lock(&channel->lock);
	channel->reconnect_enabled = 0;
	pthread_mutex_unlock(&channel->lock);
	tr_channel_notify(channel,
		lane == TR_LANE_BULK ?
			TR_CHANNEL_EVENT_BULK_RECONNECT_FAILED :
			TR_CHANNEL_EVENT_CONTROL_RECONNECT_FAILED,
		ret < 0 ? ret : TR_ERR_STATE);
	return ret;
}

static void tr_channel_default_config(struct tr_channel_config *config)
{
	if (config->min_protocol_version == 0)
		config->min_protocol_version = TR_CHANNEL_PROTOCOL_VERSION;
	if (config->max_protocol_version == 0)
		config->max_protocol_version = TR_CHANNEL_PROTOCOL_VERSION;
	if (config->max_streams == 0)
		config->max_streams = 128U;
	if (config->initial_window_bytes == 0)
		config->initial_window_bytes = 4U * 1024U * 1024U;
	if (config->window_update_threshold_bytes == 0)
		config->window_update_threshold_bytes =
			config->initial_window_bytes / 2U;
	if (config->window_update_threshold_bytes == 0)
		config->window_update_threshold_bytes = 1U;
	if (config->reassembly_pool && config->max_message_bytes == 0)
		config->max_message_bytes =
			config->reassembly_pool->buffer_size;
}


int tr_channel_start(struct tr_channel *channel)
{
	struct tr_channel_pending_hello_request pending[2];
	struct tr_conn_handle control_connection;
	struct tr_conn_handle bulk_connection;
	int split;
	int i;
	int ret;

	if (!channel)
		return TR_ERR_INVALID;

	memset(pending, 0, sizeof(pending));
	pthread_mutex_lock(&channel->lock);
	if (channel->handshake_started) {
		ret = channel->handshake_start_error;
		pthread_mutex_unlock(&channel->lock);
		return ret;
	}

	/* 延后创建的使用方调用 start() 前，上层处理函数已安装。
	 * 在接收延后所用的同一锁下发布该就绪状态，并将已收到的 HELLO
	 * 移入栈拥有的重放请求。
	 */
	channel->handshake_started = 1;
	/* 在发送和重放完成前，同步调用者不得观察到虚假的成功。 */
	channel->handshake_start_error = TR_AGAIN;
	for (i = 0; i < 2; ++i) {
		if (!channel->pending_hello[i].valid)
			continue;
		pending[i].channel = channel;
		pending[i].hello = channel->pending_hello[i];
		memset(&channel->pending_hello[i], 0,
		       sizeof(channel->pending_hello[i]));
	}
	control_connection = channel->control_connection;
	bulk_connection = channel->bulk_connection;
	split = !tr_conn_equal(control_connection, bulk_connection);
	pthread_mutex_unlock(&channel->lock);

	/* 在反应器所有者上重放过早的对端 HELLO。只有此后，
	 * ACK 或通道 UP 才能允许对端继续 STREAM_OPEN/DATA，且上层已经绑定。
	 */
	for (i = 0; i < 2; ++i) {
		if (!pending[i].hello.valid)
			continue;
		ret = tr_reactor_call(channel->reactor,
				      tr_channel_process_pending_hello_on_owner,
				      &pending[i]);
		if (ret != TR_OK)
			goto failed;
	}

	ret = tr_channel_send_hello(channel, control_connection);
	ret = tr_channel_normalize_create_hello_error(
		channel, control_connection, ret);
	if (ret != TR_OK)
		goto failed;

	if (split) {
		ret = tr_channel_send_hello(channel, bulk_connection);
		ret = tr_channel_normalize_create_hello_error(
			channel, bulk_connection, ret);
		if (ret != TR_OK)
			goto failed;
	}

	pthread_mutex_lock(&channel->lock);
	channel->handshake_start_error = TR_OK;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;

failed:
	/* 部分 HELLO 可能已经获准发送，不能重新发送造成重复握手。
	 * 因此本对象的启动错误是粘性的，调用方必须关闭并重建。
	 * 不允许下次 start() 在握手未完成时假装成功。
	 */
	pthread_mutex_lock(&channel->lock);
	channel->handshake_start_error = ret;
	pthread_mutex_unlock(&channel->lock);
	return ret;
}

struct tr_channel_build {
	struct tr_channel *channel;
	int lock_ready;
	int state_cond_ready;
};

struct tr_channel_create_publish_request {
	struct tr_channel *channel;
	int start_handshake;
	int control_handler_installed;
	int bulk_handler_installed;
	/* 构造失败但仍有 Reactor 回调引用时，移交部分对象的销毁责任。 */
	int needs_recovery;
};

static int tr_channel_create_connection_active(
	struct tr_conn_handle connection)
{
	enum tr_connection_state state;
	int ret;

	ret = tr_reactor_get_connection_state(connection, &state);
	if (ret != TR_OK)
		return ret;
	return state == TR_CONN_ACTIVE ? TR_OK : TR_ERR_STALE;
}

static int tr_channel_rollback(struct tr_channel_create_publish_request *req)
{
	struct tr_channel *channel = req->channel;
	int control_handler_installed = req->control_handler_installed;
	int bulk_handler_installed = req->bulk_handler_installed;
	struct tr_conn_handle control = channel->control_connection;
	struct tr_conn_handle bulk = channel->bulk_connection;
	int split = !tr_conn_equal(control, bulk);
	int control_safe = !control_handler_installed;
	int bulk_safe = !bulk_handler_installed;
	int ret;
	int final = TR_OK;

	/* 先解绑 callback_arg，再尝试关闭精确连接。两条路径只要有一条
	 * 确认成功，就不会再从该连接回调进入此 Channel。
	 * 两个屏障均失败时，不能让词法清理释放仍被 Reactor 引用的对象。
	 */
	if (control_handler_installed) {
		ret = tr_reactor_set_handler(control, NULL, NULL, NULL);
		if (ret == TR_OK || ret == TR_ERR_STALE)
			control_safe = 1;
		else if (final == TR_OK)
			final = ret;
	}
	if (bulk_handler_installed) {
		ret = tr_reactor_set_handler(bulk, NULL, NULL, NULL);
		if (ret == TR_OK || ret == TR_ERR_STALE)
			bulk_safe = 1;
		else if (final == TR_OK)
			final = ret;
	}

	/* 即使解绑失败仍尝试关闭，且保留第一次回滚错误。
	 * 关闭成功可替代解绑成为强静默证明；关闭失败但已解绑也安全。
	 */
	if (control_handler_installed || bulk_handler_installed) {
		ret = tr_reactor_close_on_owner(control);
		if (ret == TR_OK || ret == TR_ERR_STALE)
			control_safe = 1;
		else if (final == TR_OK)
			final = ret;
		if (split) {
			ret = tr_reactor_close_on_owner(bulk);
			if (ret == TR_OK || ret == TR_ERR_STALE)
				bulk_safe = 1;
			else if (final == TR_OK)
				final = ret;
		}
	}

	req->needs_recovery = !control_safe || !bulk_safe;
	return final;
}

static int tr_channel_create_publish_on_owner(void *arg)
{
	struct tr_channel_create_publish_request *request =
		(struct tr_channel_create_publish_request *)arg;
	struct tr_channel *channel = request->channel;
	struct tr_conn_handle control = channel->control_connection;
	struct tr_conn_handle bulk = channel->bulk_connection;
	int split = !tr_conn_equal(control, bulk);
	int control_handler_installed = 0;
	int bulk_handler_installed = 0;
	int rollback_ret;
	int ret;

	/* 先完成全部连接能力凭证预检，再开始任何回调发布。
	 * 同一所有者处理轮次内不会有 epoll 分派插入，因此预检成功后处理函数
	 * 发布不会因为连接替换与本事务竞争而失效。
	 */
	ret = tr_channel_create_connection_active(control);
	if (ret != TR_OK)
		return ret;
	if (split) {
		ret = tr_channel_create_connection_active(bulk);
		if (ret != TR_OK)
			return ret;
	}

	ret = tr_reactor_set_handler(
		control, tr_channel_on_frame,
		tr_channel_on_connection_event, channel);
	if (ret != TR_OK)
		return ret;
	control_handler_installed = 1;

	if (split) {
		ret = tr_reactor_set_handler(
			bulk, tr_channel_on_frame,
			tr_channel_on_connection_event, channel);
		if (ret != TR_OK)
			goto rollback;
		bulk_handler_installed = 1;
	}

	/* HELLO 准入仍属于构造事务。所有者快速路径只把发送项挂到
	 * 连接队列，不在这里执行套接字发送，因此失败仍可在本轮次
	 * 内撤销处理函数并退役精确连接。
	 */
	if (request->start_handshake) {
		ret = tr_channel_start(channel);
		if (ret != TR_OK)
			goto rollback;
	}

	/* 保活定时器最后发布。注册成功后本事务已经没有后续可失败步骤，
	 * 因而构造函数错误路径永远不需要 timer_unregister 回滚。
	 */
	ret = tr_reactor_timer_register(
		channel->reactor, tr_channel_keepalive_timer_main,
		channel, &channel->keepalive_timer);
	if (ret != TR_OK)
		goto rollback;
	channel->keepalive_timer_registered = 1;
	return TR_OK;

rollback:
	request->control_handler_installed = control_handler_installed;
	request->bulk_handler_installed = bulk_handler_installed;
	rollback_ret = tr_channel_rollback(request);
	if (request->needs_recovery && rollback_ret == TR_OK)
		return TR_ERR_STATE;
	return rollback_ret != TR_OK ? rollback_ret : ret;
}

static void tr_channel_build_cleanup(struct tr_channel_build *build)
{
	struct tr_channel *channel;

	if (!build || !build->channel)
		return;
	channel = build->channel;

	/* TR_AUTO 这里只管理未发布的词法作用域资源。所有反应器处理函数、
	 * 定时器、连接生命周期边都必须在 create_publish 所有者事务
	 * 内先完成收敛，清理本身不得执行任何可失败的异步拆除。
	 */
	if (channel->streams) {
		uint32_t i;
		for (i = 0; i < channel->config.max_streams; ++i)
			if (channel->streams[i].rx_reassembly)
				tr_buffer_release(channel->streams[i].rx_reassembly);
	}
	free(channel->stream_index);
	free(channel->streams);
	if (build->state_cond_ready)
		pthread_cond_destroy(&channel->state_cond);
	if (build->lock_ready)
		pthread_mutex_destroy(&channel->lock);
	free(channel);
	build->channel = NULL;
}

struct tr_reactor *tr_channel_reactor(struct tr_channel *channel)
{
	return channel ? channel->reactor : NULL;
}

int tr_channel_abort_lane_on_owner(struct tr_channel *channel,
                                  enum tr_lane lane, int status)
{
	struct tr_conn_handle connection;

	if (!channel || (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK) ||
	    status >= 0)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&channel->lock);
	connection = lane == TR_LANE_CONTROL ? channel->control_connection :
					     channel->bulk_connection;
	pthread_mutex_unlock(&channel->lock);
	return tr_reactor_abort_on_owner(connection, status);
}

uint32_t tr_channel_max_streams(struct tr_channel *channel)
{
	return channel ? channel->config.max_streams : 0U;
}

static int tr_channel_create_common(
		      const struct tr_channel_config *config,
		      struct tr_conn_handle control_connection,
		      struct tr_conn_handle bulk_connection,
		      tr_stream_data_cb data_cb,
		      tr_stream_event_cb stream_event_cb,
		      tr_channel_event_cb channel_event_cb, void *callback_arg,
		      int start_handshake,
		      struct tr_channel **out)
{
	struct tr_channel_build build TR_AUTO(tr_channel_build_cleanup) = { 0 };
	struct tr_channel *channel;
	int ret;

	if (!out || !control_connection.reactor ||
	    control_connection.reactor != bulk_connection.reactor)
		return TR_ERR_INVALID;

	*out = NULL;
	channel = (struct tr_channel *)calloc(1, sizeof(*channel));
	if (!channel)
		return TR_ERR_NOMEM;
	build.channel = channel;

	if (config)
		channel->config = *config;
	tr_channel_default_config(&channel->config);

	if ((channel->config.role != TR_CHANNEL_CLIENT &&
	     channel->config.role != TR_CHANNEL_SERVER) ||
	    (channel->config.mode != TR_CHANNEL_SHARED_CONNECTION &&
	     channel->config.mode != TR_CHANNEL_SPLIT_CONNECTIONS) ||
	    channel->config.min_protocol_version !=
	            TR_CHANNEL_PROTOCOL_VERSION ||
	    channel->config.max_protocol_version !=
	            TR_CHANNEL_PROTOCOL_VERSION ||
	    (channel->config.required_feature_bits &
	     ~channel->config.feature_bits))
		return TR_ERR_INVALID;

	if (channel->config.reassembly_pool &&
	    (channel->config.max_message_bytes == 0 ||
	     channel->config.max_message_bytes >
		     channel->config.reassembly_pool->buffer_size))
		return TR_ERR_INVALID;

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		if (!tr_conn_equal(control_connection, bulk_connection))
			return TR_ERR_INVALID;
	} else if (tr_conn_equal(control_connection, bulk_connection)) {
		return TR_ERR_INVALID;
	}

	channel->reactor = control_connection.reactor;
	channel->control_connection = control_connection;
	channel->bulk_connection = bulk_connection;

	if (pthread_mutex_init(&channel->lock, NULL) != 0)
		return TR_ERR_INVALID;
	build.lock_ready = 1;
	{
		pthread_condattr_t attr;

		if (pthread_condattr_init(&attr) != 0)
			return TR_ERR_SYS;
		if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
			pthread_condattr_destroy(&attr);
			return TR_ERR_SYS;
		}
		if (pthread_cond_init(&channel->state_cond, &attr) != 0) {
			pthread_condattr_destroy(&attr);
			return TR_ERR_SYS;
		}
		pthread_condattr_destroy(&attr);
		build.state_cond_ready = 1;
	}
	/* 槽与有限重复窗口同一次有界分配；后续 RESET 不分配存储。 */
	channel->streams = (struct tr_stream_slot *)calloc(
	        channel->config.max_streams,
	        sizeof(*channel->streams) + sizeof(*channel->reset_tombstones));
	channel->stream_index_capacity =
		tr_stream_index_capacity_for(channel->config.max_streams);
	if (channel->stream_index_capacity == 0U)
		return TR_ERR_INVALID;
	channel->stream_index = (struct tr_stream_index_entry *)calloc(
		channel->stream_index_capacity, sizeof(*channel->stream_index));
	if (!channel->streams || !channel->stream_index)
		return TR_ERR_NOMEM;
	channel->reset_tombstones =
	        (struct tr_stream_reset_tombstone
	                 *)(channel->streams + channel->config.max_streams);
	{
		uint32_t i;

		channel->free_stream_head = 0U;
		for (i = 0; i < channel->config.max_streams; ++i)
			channel->streams[i].free_next =
				(i + 1U < channel->config.max_streams) ?
					i + 1U : TR_STREAM_FREE_NONE;
	}

	channel->control_alive = 1;
	channel->bulk_alive = 1;
	channel->control_ready = 0;
	channel->bulk_ready = 0;
	channel->next_local_stream_id =
		channel->config.role == TR_CHANNEL_CLIENT ? 1U : 2U;
	channel->data_cb = data_cb;
	channel->stream_event_cb = stream_event_cb;
	channel->channel_event_cb = channel_event_cb;
	channel->callback_arg = callback_arg;

	{
		struct tr_channel_create_publish_request request;

		request = (struct tr_channel_create_publish_request){0};
		request.channel = channel;
		request.start_handshake = start_handshake;
		ret = tr_reactor_call(
			channel->reactor, tr_channel_create_publish_on_owner,
			&request);
		if (ret != TR_OK) {
			/* 异步回调仍可能持有 Channel。即使创建失败，
			 * 调用方也须通过 destroy 重试完整解绑，不得隐式 free。
			 */
			if (request.needs_recovery) {
				*out = channel;
				build.channel = NULL;
			}
			return ret;
		}
	}

	*out = channel;
	build.channel = NULL;
	return TR_OK;
}

int tr_channel_create(const struct tr_channel_config *config,
		      struct tr_conn_handle control_connection,
		      struct tr_conn_handle bulk_connection,
		      tr_stream_data_cb data_cb,
		      tr_stream_event_cb stream_event_cb,
		      tr_channel_event_cb channel_event_cb, void *callback_arg,
		      struct tr_channel **out)
{
	return tr_channel_create_common(config, control_connection,
					bulk_connection, data_cb, stream_event_cb,
					channel_event_cb, callback_arg, 1, out);
}

int tr_channel_create_deferred(
	const struct tr_channel_config *config,
	struct tr_conn_handle control_connection,
	struct tr_conn_handle bulk_connection,
	tr_stream_data_cb data_cb,
	tr_stream_event_cb stream_event_cb,
	tr_channel_event_cb channel_event_cb,
	void *callback_arg,
	struct tr_channel **out)
{
	return tr_channel_create_common(config, control_connection,
					bulk_connection, data_cb, stream_event_cb,
					channel_event_cb, callback_arg, 0, out);
}

static int tr_channel_detach_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
	struct tr_reactor_timer_handle timer;
	int timer_registered;
	int split;
	int ret;

	if (!channel)
		return TR_ERR_INVALID;
	if (channel->config.role != TR_CHANNEL_SERVER)
		return TR_ERR_STATE;

	pthread_mutex_lock(&channel->lock);
	if (channel->teardown_detached) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}

	/* 解绑所有权转移必须在对端仍对所有者可见时跨过每个可能失败的生命周期屏障。
	 * 收尾器没有天然的重试队列，因此不能在转移后才发现存活的排空等待者。
	 */
	channel->state_wait_closed = 1;
	if (channel->state_waiters != 0U) {
		pthread_cond_broadcast(&channel->state_cond);
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}

	channel->local_draining = 1;
	channel->keepalive_enabled = 0;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);

	control = channel->control_connection;
	bulk = channel->bulk_connection;
	split = !tr_conn_equal(control, bulk);
	timer_registered = channel->keepalive_timer_registered;
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);

	/* 处理函数发布是回调生命周期屏障。TR_ERR_STALE 能证明
	 * 这个精确连接代次已经退役；其他失败都必须保留
	 * Channel 所有权，并在收尾前重试收敛。
	 */
	ret = tr_reactor_set_handler(control, NULL, NULL, NULL);
	if (ret != TR_OK && ret != TR_ERR_STALE)
		return ret;
	if (split) {
		ret = tr_reactor_set_handler(bulk, NULL, NULL, NULL);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;
	}

	/* 定时器发布必须保持到注销被确认成功。即使处理函数已经
	 * 解绑，定时器屏障失败仍然可重试：Channel 存储保持存活，
	 * 保活准入已关闭，下一次调用继续重试精确定时器屏障，
	 * 不能假装定时器源已经消失。
	 */
	if (timer_registered) {
		ret = tr_reactor_timer_unregister(timer);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;

		pthread_mutex_lock(&channel->lock);
		channel->keepalive_timer_registered = 0;
		memset(&channel->keepalive_timer, 0,
		       sizeof(channel->keepalive_timer));
		pthread_mutex_unlock(&channel->lock);
	}

	/* 所有所有者可见的回调或定时器源已经完成解绑。此时才允许
	 * 清除上层回调发布，并发布已解绑所有权。
	 */
	channel->data_cb = NULL;
	channel->stream_event_cb = NULL;
	channel->channel_event_cb = NULL;
	channel->callback_arg = NULL;
	channel->lifecycle_event_cb = NULL;
	channel->lifecycle_callback_arg = NULL;

	pthread_mutex_lock(&channel->lock);
	channel->teardown_detached = 1;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_detach_for_finalize(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;
	return tr_reactor_call(channel->reactor, tr_channel_detach_on_owner,
			       channel);
}

static int tr_channel_client_reconnect_present(
	struct tr_channel *channel);

static int tr_channel_close_wait_admission(struct tr_channel *channel)
{
	int ret = TR_OK;

	pthread_mutex_lock(&channel->lock);
	channel->state_wait_closed = 1;
	if (channel->state_waiters != 0U) {
		pthread_cond_broadcast(&channel->state_cond);
		ret = TR_ERR_STATE;
	}
	pthread_mutex_unlock(&channel->lock);
	return ret;
}

void tr_channel_finalize_detached(struct tr_channel *channel)
{
	if (!channel)
		return;

#ifndef NDEBUG
	assert(channel->teardown_detached);
	assert(!channel->keepalive_timer_registered);
	assert(channel->state_wait_closed);
	assert(channel->state_waiters == 0U);
#endif

	/* 所有可能失败的所有者与等待者屏障都已在解绑所有权转移前完成。
	 * 现在收尾只是纯内存与资源释放。
	 */
	if (channel->streams) {
		uint32_t i;

		for (i = 0; i < channel->config.max_streams; ++i)
			if (channel->streams[i].rx_reassembly)
				tr_buffer_release(
					channel->streams[i].rx_reassembly);
	}
	free(channel->stream_index);
	free(channel->streams);
	(void)pthread_cond_destroy(&channel->state_cond);
	pthread_mutex_destroy(&channel->lock);
	free(channel);
}

int tr_channel_destroy(struct tr_channel *channel)
{
	int control_handler_ret;
	int bulk_handler_ret = TR_OK;
	int ret;

	if (!channel)
		return TR_OK;

	ret = tr_channel_close_wait_admission(channel);
	if (ret != TR_OK)
		return ret;

	/* 释放事件源可引用的任何存储前先拆除事件源。客户端
	 * 拥有重连连接器与定时器状态；服务端 Channel 从不启用该
	 * 事件源，可以在其反应器已停止后收尾。
	 */
	ret = tr_channel_disable_keepalive(channel);
	if (ret != TR_OK)
		return ret;

	if (channel->config.role == TR_CHANNEL_CLIENT &&
	    tr_channel_client_reconnect_present(channel)) {
		/* 只有没有剩余重连事件源时，已停止反应器才是有效拆除域。
		 * 正常客户端门面拆除在运行时停止前禁用重连。若底层调用者在
		 * 连接器或定时器所有权仍发布时停止反应器，则保守拒绝，
		 * 而不是绕过所有者解绑。
		 */
		ret = tr_channel_disable_client_reconnect(channel);
		if (ret != TR_OK)
			return ret;
	}

	control_handler_ret = tr_reactor_set_handler(
		channel->control_connection, NULL, NULL, NULL);
	if (!tr_conn_equal(channel->control_connection,
			   channel->bulk_connection))
		bulk_handler_ret = tr_reactor_set_handler(
			channel->bulk_connection, NULL, NULL, NULL);

	/* quiesce 区分“完全停止”与“正在停止”：
	 * - 已停止：TR_OK，即使 SET_HANDLER 返回 CLOSED，也不会再有未来回调；
	 * - 停止中：TR_ERR_CLOSED，所有权尚未收敛，因此保守拒绝。
	 */
	ret = tr_reactor_quiesce(channel->reactor);
	if (ret != TR_OK)
		return ret;

	if (control_handler_ret != TR_OK &&
	    control_handler_ret != TR_ERR_STALE &&
	    control_handler_ret != TR_ERR_CLOSED)
		return control_handler_ret;
	if (bulk_handler_ret != TR_OK &&
	    bulk_handler_ret != TR_ERR_STALE &&
	    bulk_handler_ret != TR_ERR_CLOSED)
		return bulk_handler_ret;

	/* 上方已同步解除保活定时。在 Channel 存储之前释放其定时器槽位，
	 * 使定时器队列不再保留 callback_arg。
	 */
	if (channel->keepalive_timer_registered) {
		ret = tr_reactor_timer_unregister(channel->keepalive_timer);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;
		channel->keepalive_timer_registered = 0;
		memset(&channel->keepalive_timer, 0,
		       sizeof(channel->keepalive_timer));
	}

	if (channel->streams) {
		uint32_t i;

		for (i = 0; i < channel->config.max_streams; ++i)
			if (channel->streams[i].rx_reassembly)
				tr_buffer_release(
					channel->streams[i].rx_reassembly);
	}
	free(channel->stream_index);
	free(channel->streams);
	(void)pthread_cond_destroy(&channel->state_cond);
	pthread_mutex_destroy(&channel->lock);
	free(channel);
	return TR_OK;
}

struct tr_channel_lifecycle_observer_request {
	struct tr_channel *channel;
	tr_channel_event_cb event_cb;
	void *callback_arg;
};

static int tr_channel_set_lifecycle_observer_on_owner(void *arg)
{
	struct tr_channel_lifecycle_observer_request *request =
		(struct tr_channel_lifecycle_observer_request *)arg;

	request->channel->lifecycle_event_cb = request->event_cb;
	request->channel->lifecycle_callback_arg = request->callback_arg;
	return TR_OK;
}

int tr_channel_set_lifecycle_observer(struct tr_channel *channel,
				      tr_channel_event_cb event_cb,
				      void *callback_arg)
{
	struct tr_channel_lifecycle_observer_request request;

	if (!channel)
		return TR_ERR_INVALID;

	request.channel = channel;
	request.event_cb = event_cb;
	request.callback_arg = callback_arg;
	return tr_reactor_call(
		channel->reactor,
		tr_channel_set_lifecycle_observer_on_owner, &request);
}

struct tr_channel_handler_request {
	struct tr_channel *channel;
	tr_stream_data_cb data_cb;
	tr_stream_event_cb stream_event_cb;
	tr_channel_event_cb channel_event_cb;
	void *callback_arg;
};

static int tr_channel_set_handler_on_owner(void *arg)
{
	struct tr_channel_handler_request *request =
		(struct tr_channel_handler_request *)arg;
	struct tr_channel *channel = request->channel;

	channel->data_cb = request->data_cb;
	channel->stream_event_cb = request->stream_event_cb;
	channel->channel_event_cb = request->channel_event_cb;
	channel->callback_arg = request->callback_arg;
	return TR_OK;
}

int tr_channel_set_handler(struct tr_channel *channel,
			   tr_stream_data_cb data_cb,
			   tr_stream_event_cb stream_event_cb,
			   tr_channel_event_cb channel_event_cb,
			   void *callback_arg)
{
	struct tr_channel_handler_request request;

	if (!channel)
		return TR_ERR_INVALID;

	request.channel = channel;
	request.data_cb = data_cb;
	request.stream_event_cb = stream_event_cb;
	request.channel_event_cb = channel_event_cb;
	request.callback_arg = callback_arg;
	return tr_reactor_call_or_stopped(
		channel->reactor, tr_channel_set_handler_on_owner, &request);
}

int tr_channel_quiesce(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;
	return tr_reactor_quiesce(channel->reactor);
}

struct tr_channel_replace_request {
	struct tr_channel *channel;
	enum tr_lane lane;
	struct tr_conn_handle connection;
};

/* 替换附接必须相对于反应器 I/O 回调保持原子性。
 *
 * 如果处理函数在 Channel 记录新通道映射前可见，已可读的对端 HELLO
 * 可能立即运行并观察到 lane_mask == 0，错误地把健康替换连接变为协议错误。
 *
 * 在一个所有者操作中执行验证、处理函数安装与通道发布，
 * 使任何连接回调都不能插入此转换。
 */
static int tr_channel_replace_connection_on_owner(void *arg)
{
	struct tr_channel_replace_request *request =
		(struct tr_channel_replace_request *)arg;
	struct tr_channel *channel = request->channel;
	struct tr_conn_handle connection = request->connection;
	enum tr_lane lane = request->lane;
	enum tr_connection_state connection_state;
	int ret;

	ret = tr_reactor_get_connection_state(connection, &connection_state);
	if (ret != TR_OK)
		return ret;
	if (connection_state != TR_CONN_RESERVED &&
	    connection_state != TR_CONN_ACTIVE)
		return TR_ERR_CLOSED;

	pthread_mutex_lock(&channel->lock);
	if (channel->local_draining ||
	    (lane == TR_LANE_CONTROL && channel->control_peer_draining) ||
	    (lane == TR_LANE_BULK && channel->bulk_peer_draining)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		if (channel->control_alive || channel->bulk_alive) {
			pthread_mutex_unlock(&channel->lock);
			return TR_ERR_STATE;
		}
	} else if (lane == TR_LANE_BULK) {
		if (channel->bulk_alive ||
		    tr_conn_equal(connection, channel->control_connection)) {
			pthread_mutex_unlock(&channel->lock);
			return TR_ERR_STATE;
		}
	} else {
		if (channel->control_alive ||
		    tr_conn_equal(connection, channel->bulk_connection)) {
			pthread_mutex_unlock(&channel->lock);
			return TR_ERR_STATE;
		}
	}

	/* tr_reactor_call() 在此保证所有者上下文；因此 set_handler
	 * 直接更新连接，不能在此转换中途分派 I/O 回调。
	 */
	ret = tr_reactor_set_handler(connection, tr_channel_on_frame,
				     tr_channel_on_connection_event, channel);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&channel->lock);
		return ret;
	}

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		channel->control_connection = connection;
		channel->bulk_connection = connection;
		channel->peer_stream_frontier[0] = 0;
		channel->peer_stream_frontier[1] = 0;
		memset(channel->drain, 0, sizeof(channel->drain));
		channel->control_alive = 1;
		channel->bulk_alive = 1;
		channel->control_ready = 0;
		channel->bulk_ready = 0;
		memset(&channel->control_caps, 0,
		       sizeof(channel->control_caps));
		memset(&channel->bulk_caps, 0, sizeof(channel->bulk_caps));
		channel->control_reconnecting = 0;
		channel->bulk_reconnecting = 0;
		channel->control_reconnect_attempt = 0;
		channel->bulk_reconnect_attempt = 0;
	} else if (lane == TR_LANE_BULK) {
		channel->bulk_connection = connection;
		channel->peer_stream_frontier[1] = 0;
		memset(&channel->drain[1], 0, sizeof(channel->drain[1]));
		channel->bulk_alive = 1;
		channel->bulk_ready = 0;
		memset(&channel->bulk_caps, 0, sizeof(channel->bulk_caps));
		channel->bulk_reconnecting = 0;
		channel->bulk_reconnect_attempt = 0;
	} else {
		channel->control_connection = connection;
		channel->peer_stream_frontier[0] = 0;
		memset(&channel->drain[0], 0, sizeof(channel->drain[0]));
		channel->control_alive = 1;
		channel->control_ready = 0;
		memset(&channel->control_caps, 0,
		       sizeof(channel->control_caps));
		channel->control_reconnecting = 0;
		channel->control_reconnect_attempt = 0;
	}
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_replace_connection(struct tr_channel *channel, enum tr_lane lane,
				  struct tr_conn_handle connection)
{
	struct tr_channel_replace_request request;
	int ret;

	if (!channel || !connection.reactor ||
	    connection.reactor != channel->reactor ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;

	request.channel = channel;
	request.lane = lane;
	request.connection = connection;
	ret = tr_reactor_call(channel->reactor,
				      tr_channel_replace_connection_on_owner, &request);
	if (ret != TR_OK)
		return ret;

	ret = tr_channel_send_hello(channel, connection);
	if (ret != TR_OK) {
		(void)tr_reactor_close(connection);
		return ret;
	}
	return TR_OK;
}

struct tr_channel_reconnect_nodelay_request {
	struct tr_channel *channel;
	int enabled;
};

static int tr_channel_set_reconnect_tcp_nodelay_on_owner(void *arg)
{
	struct tr_channel_reconnect_nodelay_request *request =
		(struct tr_channel_reconnect_nodelay_request *)arg;
	struct tr_channel *channel = request->channel;

	pthread_mutex_lock(&channel->lock);
	if (channel->reconnect_enabled || channel->reconnect_connector ||
	    channel->reconnect_timer_registered) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	channel->reconnect_tcp_nodelay = request->enabled;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_set_reconnect_tcp_nodelay(struct tr_channel *channel,
					    int enabled)
{
	struct tr_channel_reconnect_nodelay_request request;

	if (!channel || (enabled != 0 && enabled != 1))
		return TR_ERR_INVALID;

	request.channel = channel;
	request.enabled = enabled;
	return tr_reactor_call(
		channel->reactor,
		tr_channel_set_reconnect_tcp_nodelay_on_owner, &request);
}

struct tr_channel_reconnect_enable_request {
	struct tr_channel *channel;
	char address[64];
	uint16_t control_port;
	uint16_t bulk_port;
	uint32_t initial_delay_ms;
	uint32_t max_delay_ms;
	uint32_t connect_timeout_ms;
};

static int tr_channel_disable_client_reconnect_on_owner(void *arg);

static int tr_channel_enable_client_reconnect_on_owner(void *arg)
{
	struct tr_channel_reconnect_enable_request *request =
		(struct tr_channel_reconnect_enable_request *)arg;
	struct tr_channel *channel = request->channel;
	struct tr_connector_config connector_config;
	struct tr_connector *connector = NULL;
	struct tr_reactor_timer_handle timer;
	int ret;

	pthread_mutex_lock(&channel->lock);
	if (channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (channel->reconnect_enabled || channel->reconnect_connector ||
	    channel->reconnect_timer_registered) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = channel->reactor;
	connector_config.timeout_ms = request->connect_timeout_ms;
	connector_config.tcp_nodelay = channel->reconnect_tcp_nodelay;
	pthread_mutex_unlock(&channel->lock);
	connector_config.complete_cb = tr_channel_reconnect_connector_complete;
	connector_config.callback_arg = channel;
	ret = tr_connector_create(&connector_config, &connector);
	if (ret != TR_OK)
		return ret;

	memset(&timer, 0, sizeof(timer));
	ret = tr_reactor_timer_register(
		channel->reactor, tr_channel_reconnect_timer_main,
		channel, &timer);
	if (ret != TR_OK) {
		int destroy_ret = tr_connector_destroy(connector);

		/* 回滚失败意味着连接器回调源所有权未收敛；
		 * 报告该错误，而不是假装仅为启用失败。
		 */
		if (destroy_ret != TR_OK)
			return destroy_ret;
		return ret;
	}

	pthread_mutex_lock(&channel->lock);
	memcpy(channel->reconnect_address, request->address,
	       sizeof(channel->reconnect_address));
	channel->reconnect_control_port = request->control_port;
	channel->reconnect_bulk_port = request->bulk_port;
	channel->reconnect_initial_delay_ms = request->initial_delay_ms;
	channel->reconnect_max_delay_ms = request->max_delay_ms;
	channel->reconnect_connect_timeout_ms = request->connect_timeout_ms;
	channel->reconnect_connector = connector;
	channel->reconnect_timer = timer;
	channel->reconnect_timer_registered = 1;
	channel->reconnect_enabled = 1;
	channel->control_reconnect_attempt = 0;
	channel->bulk_reconnect_attempt = 0;
	channel->control_reconnecting = 0;
	channel->bulk_reconnecting = 0;
	channel->reconnect_lane = TR_LANE_CONTROL;
	pthread_mutex_unlock(&channel->lock);

	ret = tr_channel_reconnect_schedule_on_owner(channel);
	if (ret != TR_OK) {
		int cleanup_ret =
			tr_channel_disable_client_reconnect_on_owner(channel);

		return cleanup_ret == TR_OK ? ret : cleanup_ret;
	}
	return TR_OK;
}

int tr_channel_enable_client_reconnect(
	struct tr_channel *channel,
	const struct tr_channel_reconnect_config *config)
{
	struct tr_channel_reconnect_enable_request request;
	size_t address_len;
	uint32_t initial_delay;
	uint32_t max_delay;
	uint32_t connect_timeout;

	if (!channel || !config || !config->ipv4_address ||
	    config->control_port == 0U ||
	    channel->config.role != TR_CHANNEL_CLIENT)
		return TR_ERR_INVALID;

	address_len = strlen(config->ipv4_address);
	if (address_len == 0U || address_len >= sizeof(request.address))
		return TR_ERR_INVALID;
	initial_delay = config->initial_delay_ms ?
			config->initial_delay_ms : 200U;
	max_delay = config->max_delay_ms ? config->max_delay_ms : 10000U;
	connect_timeout = config->connect_timeout_ms ?
			  config->connect_timeout_ms : 5000U;
	if (max_delay < initial_delay)
		return TR_ERR_INVALID;

	memset(&request, 0, sizeof(request));
	request.channel = channel;
	memcpy(request.address, config->ipv4_address, address_len + 1U);
	request.control_port = config->control_port;
	request.bulk_port = config->bulk_port ?
			    config->bulk_port : config->control_port;
	request.initial_delay_ms = initial_delay;
	request.max_delay_ms = max_delay;
	request.connect_timeout_ms = connect_timeout;
	return tr_reactor_call(
		channel->reactor,
		tr_channel_enable_client_reconnect_on_owner, &request);
}

static int tr_channel_disable_client_reconnect_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_connector *connector;
	struct tr_reactor_timer_handle timer;
	int timer_registered;
	int ret;

	/* 先关闭调度准入。已发布的事件源句柄保持完整，直到各自的
	 * 拆除屏障成功，因此失败可重试，也不会使 callback_arg 所有权失管。
	 */
	pthread_mutex_lock(&channel->lock);
	channel->reconnect_enabled = 0;
	connector = channel->reconnect_connector;
	timer = channel->reconnect_timer;
	timer_registered = channel->reconnect_timer_registered;
	pthread_mutex_unlock(&channel->lock);

	if (timer_registered) {
		ret = tr_reactor_timer_arm(timer, 0U);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;

		ret = tr_reactor_timer_unregister(timer);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;

		pthread_mutex_lock(&channel->lock);
		if (channel->reconnect_timer_registered &&
		    channel->reconnect_timer.reactor == timer.reactor &&
		    channel->reconnect_timer.slot == timer.slot &&
		    channel->reconnect_timer.generation == timer.generation) {
			channel->reconnect_timer_registered = 0;
			memset(&channel->reconnect_timer, 0,
			       sizeof(channel->reconnect_timer));
		}
		pthread_mutex_unlock(&channel->lock);
	}

	if (connector) {
		ret = tr_connector_destroy(connector);
		if (ret != TR_OK)
			return ret;

		pthread_mutex_lock(&channel->lock);
		if (channel->reconnect_connector == connector)
			channel->reconnect_connector = NULL;
		pthread_mutex_unlock(&channel->lock);
	}

	pthread_mutex_lock(&channel->lock);
	channel->control_reconnecting = 0;
	channel->bulk_reconnecting = 0;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

static int tr_channel_client_reconnect_present(
	struct tr_channel *channel)
{
	int present;

	pthread_mutex_lock(&channel->lock);
	present = channel->reconnect_enabled ||
		channel->reconnect_connector != NULL ||
		channel->reconnect_timer_registered ||
		channel->control_reconnecting ||
		channel->bulk_reconnecting;
	pthread_mutex_unlock(&channel->lock);
	return present;
}

int tr_channel_disable_client_reconnect(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;
	return tr_reactor_call(
		channel->reactor,
		tr_channel_disable_client_reconnect_on_owner, channel);
}

int tr_channel_enable_keepalive(struct tr_channel *channel,
				const struct tr_channel_keepalive_config *config)
{
	struct tr_reactor_timer_handle timer;
	uint64_t now_ns;
	int ret;

	if (!channel || !config || config->interval_ms == 0 ||
	    config->timeout_ms == 0)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (channel->keepalive_enabled ||
	    !channel->keepalive_timer_registered) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}

	channel->keepalive_interval_ms = config->interval_ms;
	channel->keepalive_timeout_ms = config->timeout_ms;
	channel->keepalive_enabled = 1;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);

	now_ns = tr_channel_now_ns();
	if (now_ns == 0)
		ret = TR_ERR_SYS;
	else
		ret = tr_reactor_timer_arm(timer, now_ns);
	if (ret == TR_OK)
		return TR_OK;

	pthread_mutex_lock(&channel->lock);
	channel->keepalive_enabled = 0;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
	pthread_mutex_unlock(&channel->lock);
	return ret;
}

/* 此定时器也负责 WINDOW_UPDATE 重试。禁用保活绝不能抹除
 * 待处理重试唯一的进展来源；与反应器所有者串行化该决策，
 * 而不是使用所有者外部的陈旧快照。
 */
static int tr_channel_disarm_keepalive_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_reactor_timer_handle timer;
	int retry_scheduled;
	int registered;

	pthread_mutex_lock(&channel->lock);
	registered = channel->keepalive_timer_registered;
	retry_scheduled = channel->window_retry_scheduled;
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);
	if (!registered || retry_scheduled)
		return TR_OK;
	return tr_reactor_timer_arm(timer, 0);
}

int tr_channel_disable_keepalive(struct tr_channel *channel)
{
	int ret;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	channel->keepalive_enabled = 0;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
	pthread_mutex_unlock(&channel->lock);

	/* 持有 channel->lock 时不要与所有者同步。 */
	ret = tr_reactor_call(channel->reactor,
			      tr_channel_disarm_keepalive_on_owner, channel);
	if (ret == TR_ERR_CLOSED)
		return TR_OK;
	return ret;
}

int tr_channel_get_lane_state(struct tr_channel *channel, enum tr_lane lane,
				      enum tr_channel_lane_state *out)
{
	if (!channel || !out ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (lane == TR_LANE_BULK) {
		if (channel->bulk_alive && channel->bulk_ready)
			*out = TR_CHANNEL_LANE_UP;
		else if (channel->bulk_alive)
			*out = TR_CHANNEL_LANE_HANDSHAKING;
		else if (channel->bulk_reconnecting)
			*out = TR_CHANNEL_LANE_RECONNECTING;
		else
			*out = TR_CHANNEL_LANE_DOWN;
	} else {
		if (channel->control_alive && channel->control_ready)
			*out = TR_CHANNEL_LANE_UP;
		else if (channel->control_alive)
			*out = TR_CHANNEL_LANE_HANDSHAKING;
		else if (channel->control_reconnecting)
			*out = TR_CHANNEL_LANE_RECONNECTING;
		else
			*out = TR_CHANNEL_LANE_DOWN;
	}
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_get_capabilities(struct tr_channel *channel, enum tr_lane lane,
				struct tr_channel_capabilities *out)
{
	if (!channel || !out ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (!tr_channel_lane_alive(channel, lane)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}
	*out = lane == TR_LANE_BULK ? channel->bulk_caps :
				      channel->control_caps;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

uint32_t tr_channel_active_streams(struct tr_channel *channel)
{
	uint32_t count;

	if (!channel)
		return 0;
	pthread_mutex_lock(&channel->lock);
	count = tr_channel_active_streams_locked(channel);
	pthread_mutex_unlock(&channel->lock);
	return count;
}

int tr_channel_get_state(struct tr_channel *channel, enum tr_channel_state *out)
{
	if (!channel || !out)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&channel->lock);
	if (!channel->local_draining)
		*out = TR_CHANNEL_RUNNING;
	else if (tr_channel_drained_locked(channel))
		*out = TR_CHANNEL_DRAINED;
	else
		*out = TR_CHANNEL_DRAINING;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

static int tr_channel_begin_drain_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	int ret;
	int progress_ret;

	pthread_mutex_lock(&channel->lock);
	if (channel->state_wait_closed || channel->teardown_detached) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (tr_channel_drained_locked(channel)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	pthread_mutex_unlock(&channel->lock);
	/* 外部最后一次 CLOSE 可在持锁状态释放流，不能在那里同步调用 owner。
	 * 先取得既有维护定时器的责任，由后续 25ms 维护轮次观察该次释放；
	 * 该周期不承诺所有者被阻塞或调度延迟时的实时上限。 */
	ret = tr_channel_reset_timer_on_owner(channel);
	if (ret != TR_OK)
		return ret;

	/* 排空屏障与禁用重连必须在同一个所有者处理轮次内完成。
	 * 否则应用线程可能在“禁用重连”返回后、设置
	 * local_draining 前重新启用重连。
	 */
	pthread_mutex_lock(&channel->lock);
	channel->local_draining = 1;
	for (int i = 0; i < 2; ++i) {
		struct tr_channel_drain *drain = &channel->drain[i];
		if (!drain->local_started) {
			drain->local_started = 1;
			drain->local_cutoff = drain->last_peer_admitted;
		}
	}
	channel->drain_retry_pending = 1;
	pthread_mutex_unlock(&channel->lock);

	ret = tr_channel_disable_client_reconnect_on_owner(channel);
	progress_ret = tr_channel_drain_progress_on_owner(channel);
	return ret != TR_OK ? ret : progress_ret;
}

int tr_channel_wait_ready_deadline(
	struct tr_channel *channel, enum tr_lane lane,
	const struct tr_deadline *deadline)
{
	int timed;
	int result = TR_OK;
	int expired;
	int ret;

	if (!channel || !deadline ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	timed = !deadline->infinite;
	if (timed) {
		ret = tr_deadline_expired(deadline, &expired);
		if (ret != TR_OK)
			return ret;
		if (expired)
			return TR_ERR_TIMEOUT;
	}

	pthread_mutex_lock(&channel->lock);
	if (channel->state_wait_closed) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if ((lane == TR_LANE_CONTROL && channel->control_ready) ||
	    (lane == TR_LANE_BULK && channel->bulk_ready)) {
		/* 等待锁可能耗尽期限；就绪成功必须在持锁观察时再次判定。 */
		if (timed) {
			result = tr_deadline_expired(deadline, &expired);
			if (result == TR_OK && expired)
				result = TR_ERR_TIMEOUT;
		}
		pthread_mutex_unlock(&channel->lock);
		return result;
	}
	if (channel->state_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	channel->state_waiters++;

	for (;;) {
		int ready = lane == TR_LANE_CONTROL ?
			channel->control_ready : channel->bulk_ready;

		/* 不能因另一阶段或已到期后的 READY 事件重新获得时间。
		 * 同时保留关闭状态和条件变量原有的所有权屏障。
		 */
		if (timed) {
			ret = tr_deadline_expired(deadline, &expired);
			if (ret != TR_OK) {
				result = ret;
				break;
			}
			if (expired) {
				result = TR_ERR_TIMEOUT;
				break;
			}
		}
		if (ready)
			break;
		if (channel->state_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}

		if (timed)
			ret = pthread_cond_timedwait(
				&channel->state_cond, &channel->lock,
				&deadline->absolute);
		else
			ret = pthread_cond_wait(
				&channel->state_cond, &channel->lock);

		if (ret == 0)
			continue;
		if (timed && ret == ETIMEDOUT) {
			result = TR_ERR_TIMEOUT;
			break;
		}
		result = TR_ERR_SYS;
		break;
	}

	assert(channel->state_waiters != 0U);
	channel->state_waiters--;
	pthread_mutex_unlock(&channel->lock);
	return result;
}

int tr_channel_wait_ready(
	struct tr_channel *channel, enum tr_lane lane, uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	int ret;

	if (!channel ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	if (timeout_ms == 0U) {
		tr_deadline_init_infinite(&deadline);
	} else {
		ret = tr_deadline_init_ms(&deadline, timeout_ms);
		if (ret != TR_OK)
			return ret;
	}
	return tr_channel_wait_ready_deadline(channel, lane, &deadline);
}

int tr_channel_begin_drain(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;

	return tr_reactor_call(
		channel->reactor, tr_channel_begin_drain_on_owner, channel);
}

int tr_channel_wait_drained_deadline(
	struct tr_channel *channel, const struct tr_deadline *deadline)
{
	int timed = deadline != NULL;
	int result = TR_OK;

	if (!channel || (deadline && deadline->infinite))
		return TR_ERR_INVALID;

	/* wait_drained() 只是等待队列观察者，不驱动 Channel 协议。
	 * GOAWAY 准入或重试属于反应器所有者上的 begin_drain()。
	 * 所有者如果在这里等待，将阻塞用于排空的接收、发送或关闭回调。
	 */
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	pthread_mutex_lock(&channel->lock);
	if (channel->state_wait_closed) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (!channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	if (tr_channel_drained_locked(channel)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	if (!timed) {
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}
	if (channel->state_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	channel->state_waiters++;

	while (!tr_channel_drained_locked(channel)) {
		int ret;

		if (channel->state_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}
		/* 使用调用方传入的原始绝对期限。多个对端不能分别取整后
		 * 重建相对超时，也不能延长同一服务端排空的预算。
		 */
		ret = pthread_cond_timedwait(
			&channel->state_cond, &channel->lock,
			&deadline->absolute);

		if (ret == 0)
			continue;
		if (ret == ETIMEDOUT) {
			result = TR_AGAIN;
			break;
		}
		result = TR_ERR_SYS;
		break;
	}

	assert(channel->state_waiters != 0U);
	channel->state_waiters--;
	pthread_mutex_unlock(&channel->lock);
	return result;
}

int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	int ret;

	if (!channel)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	/* 公开接口的 0ms 语义始终是立即检查，不是无限等待。 */
	if (timeout_ms == 0U)
		return tr_channel_wait_drained_deadline(channel, NULL);

	ret = tr_deadline_init_ms(&deadline, timeout_ms);
	if (ret != TR_OK)
		return ret;
	return tr_channel_wait_drained_deadline(channel, &deadline);
}

int tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
		   struct tr_stream_handle *out)
{
	struct tr_stream_handle handle;
	struct tr_stream_slot *stream;
	struct tr_conn_handle connection;
	uint32_t stream_id;
	int ret;

	if (!channel || !out ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (channel->local_draining ||
	    (lane == TR_LANE_BULK ? channel->bulk_peer_draining :
				    channel->control_peer_draining)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (!tr_channel_lane_alive(channel, lane)) {
		int physical_alive = lane == TR_LANE_BULK ?
					     channel->bulk_alive :
					     channel->control_alive;
		pthread_mutex_unlock(&channel->lock);
		return physical_alive ? TR_AGAIN : TR_ERR_CLOSED;
	}

	stream_id = channel->next_local_stream_id;
	if (stream_id == 0) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	/* 连接上的线协议标识不回绕。失败可以留下空洞，不能重用
	 * 曾经发布过的标识，使迟到 RESET 有机会命中新调用。 */
	channel->next_local_stream_id =
	        stream_id > UINT32_MAX - 2U ? 0 : stream_id + 2U;

	if (tr_stream_lookup_id(channel, stream_id, NULL)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}

	ret = tr_stream_allocate_locked(channel, stream_id, lane,
					TR_STREAM_SLOT_OPENING, &handle);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&channel->lock);
		return ret;
	}

	stream = &channel->streams[handle.slot];
	connection = tr_channel_connection(channel, lane);
	ret = tr_reactor_send(connection, TR_FRAME_STREAM_OPEN,
			      tr_lane_flag(lane), stream_id,
			      stream->rx_advertised_limit, NULL);
	if (ret != TR_OK) {
		tr_stream_free_locked(channel, handle.slot);
		pthread_mutex_unlock(&channel->lock);
		return ret;
	}
	*out = handle;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_stream_sendv(struct tr_stream_handle handle,
		    struct tr_buffer *const *payloads, uint32_t payload_count)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;
	struct tr_conn_handle connection;
	uint64_t payload_len = 0;
	uint64_t end;
	uint64_t message_id;
	uint32_t i;
	int ret;

	if (!channel || !payloads || payload_count == 0 ||
	    payload_count > TR_REACTOR_MAX_TX_SLICES)
		return TR_ERR_INVALID;

	for (i = 0; i < payload_count; ++i) {
		if (!payloads[i] || payloads[i]->len == 0)
			return TR_ERR_INVALID;
		if (UINT64_MAX - payload_len < payloads[i]->len)
			return TR_ERR_BAD_LENGTH;
		payload_len += payloads[i]->len;
	}

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	if (stream->state == TR_STREAM_SLOT_RESETTING) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (stream->state != TR_STREAM_SLOT_OPEN || !stream->local_open) {
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}
	if (!tr_channel_lane_alive(channel, stream->lane)) {
		int physical_alive = stream->lane == TR_LANE_BULK ?
					     channel->bulk_alive :
					     channel->control_alive;
		pthread_mutex_unlock(&channel->lock);
		return physical_alive ? TR_AGAIN : TR_ERR_CLOSED;
	}
	if (payload_len >
	    tr_channel_lane_max_message_locked(channel, stream->lane)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_BAD_LENGTH;
	}

	if (UINT64_MAX - stream->tx_sent_bytes < payload_len) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_BAD_LENGTH;
	}
	end = stream->tx_sent_bytes + payload_len;
	if (end > stream->tx_send_limit) {
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}

	message_id = stream->next_tx_message_id;
	connection = tr_channel_connection(channel, stream->lane);
	ret = tr_reactor_sendv_limited(
		connection, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
		stream->stream_id, message_id, payloads, payload_count,
		tr_channel_lane_max_frame_locked(channel, stream->lane));
	if (ret == TR_OK) {
		stream->tx_sent_bytes = end;
		stream->next_tx_message_id++;
		channel->stat_messages_tx++;
		channel->stat_bytes_tx += payload_len;
	}
	pthread_mutex_unlock(&channel->lock);
	return ret;
}

int tr_stream_send(struct tr_stream_handle handle, struct tr_buffer *payload)
{
	struct tr_buffer *payloads[1];

	if (!payload)
		return TR_ERR_INVALID;
	payloads[0] = payload;
	return tr_stream_sendv(handle, payloads, 1);
}

int tr_stream_stop_credit_on_owner(struct tr_stream_handle handle)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;

	if (!channel)
		return TR_ERR_INVALID;
	if (!tr_reactor_in_owner_context_for(channel->reactor))
		return TR_ERR_STATE;
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	/* 已排队的信用仍先于后续 CLOSE；不撤销对端已经取得的发送额度。
	 * 延迟 DATA 或保留负载仍可释放并计入 consumed，但不得在 CLOSE 后
	 * 产生新的 WINDOW_UPDATE。普通半关闭不调用此终局能力。 */
	stream->rx_credit_stopped = 1;
	stream->pending_advertised_limit = 0;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_stream_reset_on_owner(struct tr_stream_handle handle, int status)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;
	int ret;

	if (!channel || !tr_wire_stream_reset_status_valid(status))
		return TR_ERR_INVALID;
	if (!tr_reactor_in_owner_context_for(channel->reactor))
		return TR_ERR_STATE;
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	if (stream->state == TR_STREAM_SLOT_RESETTING) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	if (!stream->local_open) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (stream->state == TR_STREAM_SLOT_OPENING) {
		/* 对端尚未确认有槽；它可能用 GOAWAY 受理边界拒绝 OPEN。
		 * 不能向尚未取得责任的对端发送 RESET。 */
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}
	if (!tr_stream_reset_supported_locked(channel, stream->lane)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_UNSUPPORTED;
	}
	pthread_mutex_unlock(&channel->lock);
	ret = tr_channel_reset_timer_on_owner(channel);
	if (ret != TR_OK)
		return ret;
	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	/* 取得定时器责任时曾释放锁；并发普通 CLOSE 仍拥有其已接受的
	 * 线上顺序，不能在重新加锁后追加主动 RESET。 */
	if (!stream->local_open) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	tr_stream_reset_begin_locked(channel, stream, status);
	stream->reset_notified = 1;
	pthread_mutex_unlock(&channel->lock);
	tr_stream_notify(channel, handle, TR_STREAM_EVENT_RESET, status);
	tr_channel_reset_progress_on_owner(channel);
	return TR_OK;
}

int tr_stream_close(struct tr_stream_handle handle)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;
	struct tr_conn_handle connection;
	uint32_t slot;
	uint32_t generation;
	int free_now = 0;
	int ret;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}
	if (stream->state == TR_STREAM_SLOT_OPENING) {
		/* 未获初始信用就不向对端发送 CLOSE，避免它拒绝 OPEN 后
		 * 还必须持有逐流记录来辨认迟到关闭。调用方保留重试责任。 */
		pthread_mutex_unlock(&channel->lock);
		return TR_AGAIN;
	}
	if (stream->state == TR_STREAM_SLOT_RESETTING) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	if (!stream->local_open) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}

	connection = tr_channel_connection(channel, stream->lane);
	ret = tr_reactor_send(connection, TR_FRAME_STREAM_CLOSE, 0,
			      stream->stream_id, 0, NULL);
	if (ret == TR_OK) {
		stream->local_open = 0;
		if (!stream->remote_open) {
			slot = handle.slot;
			generation = stream->generation;
			free_now = 1;
		}
	}
	pthread_mutex_unlock(&channel->lock);

	if (free_now) {
		pthread_mutex_lock(&channel->lock);
		stream = &channel->streams[slot];
		if (stream->generation == generation && !stream->local_open &&
		    !stream->remote_open) {
			tr_stream_free_locked(channel, slot);
			channel->stat_streams_closed++;
		}
		pthread_mutex_unlock(&channel->lock);
	}
	return ret;
}

int tr_stream_release_payload(struct tr_stream_handle handle,
			      struct tr_buffer *payload)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;
	uint32_t len;
	int ret = TR_OK;
	int kick_retry = 0;

	if (!payload)
		return TR_ERR_INVALID;

	len = payload->len;
	if (!channel) {
		tr_buffer_release(payload);
		return TR_ERR_STALE;
	}

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (stream) {
		uint64_t before = stream->rx_advertised_limit;
		kick_retry = tr_stream_consume_locked(channel, stream, len);
		if (stream->pending_advertised_limit != 0 &&
		    stream->rx_advertised_limit == before)
			ret = TR_AGAIN;
	} else {
		ret = TR_ERR_STALE;
	}
	pthread_mutex_unlock(&channel->lock);
	if (kick_retry) {
		int arm_ret = tr_channel_schedule_window_retry(channel);
		if (arm_ret != TR_OK && ret == TR_OK)
			ret = arm_ret;
	}

	tr_buffer_release(payload);
	return ret;
}

int tr_channel_flush(struct tr_channel *channel)
{
	uint32_t i;
	int result = TR_OK;
	int kick_retry = 0;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	for (i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_slot *stream = &channel->streams[i];
		int ret;

		if (stream->state == TR_STREAM_SLOT_FREE ||
		    stream->pending_advertised_limit == 0)
			continue;

		ret = tr_send_window_update_locked(channel, stream);
		if (ret == TR_AGAIN) {
			channel->window_retry_pending = 1;
			kick_retry = 1;
		}
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	pthread_mutex_unlock(&channel->lock);
	if (kick_retry) {
		int ret = tr_channel_schedule_window_retry(channel);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}

	{
		int ret = tr_reactor_call(
			channel->reactor,
			tr_channel_send_pending_goaway_on_owner, channel);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	return result;
}

int tr_stream_get_flow_state(struct tr_stream_handle handle,
			     struct tr_stream_flow_state *out)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;

	if (!channel || !out)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}

	out->tx_sent_bytes = stream->tx_sent_bytes;
	out->tx_send_limit = stream->tx_send_limit;
	out->rx_received_bytes = stream->rx_received_bytes;
	out->rx_consumed_bytes = stream->rx_consumed_bytes;
	out->rx_advertised_limit = stream->rx_advertised_limit;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_stream_get_lane_internal(struct tr_stream_handle handle,
                               enum tr_lane *out)
{
	struct tr_stream_slot *stream;

	if (!handle.channel || !out)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&handle.channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (stream)
		*out = stream->lane;
	pthread_mutex_unlock(&handle.channel->lock);
	return stream ? TR_OK : TR_ERR_STALE;
}

int tr_stream_get_diagnostics(struct tr_stream_handle handle,
			      struct tr_stream_diagnostics *out)
{
	struct tr_channel *channel = handle.channel;
	struct tr_stream_slot *stream;

	if (!channel || !out)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_handle(handle);
	if (!stream) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STALE;
	}

	memset(out, 0, sizeof(*out));
	out->lane = stream->lane;
	out->stream_id = stream->stream_id;
	out->local_open = stream->local_open;
	out->remote_open = stream->remote_open;
	out->next_tx_message_id = stream->next_tx_message_id;
	out->next_rx_message_id = stream->next_rx_message_id;
	out->flow.tx_sent_bytes = stream->tx_sent_bytes;
	out->flow.tx_send_limit = stream->tx_send_limit;
	out->flow.rx_received_bytes = stream->rx_received_bytes;
	out->flow.rx_consumed_bytes = stream->rx_consumed_bytes;
	out->flow.rx_advertised_limit = stream->rx_advertised_limit;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_get_stats(struct tr_channel *channel,
			 struct tr_channel_stats *out)
{
	struct tr_conn_handle control_connection;
	struct tr_conn_handle bulk_connection;
	uint32_t active;

	if (!channel || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));
	pthread_mutex_lock(&channel->lock);
	active = tr_channel_active_streams_locked(channel);
	out->active_streams = active;
	if (!channel->local_draining)
		out->state = TR_CHANNEL_RUNNING;
	else if (tr_channel_drained_locked(channel))
		out->state = TR_CHANNEL_DRAINED;
	else
		out->state = TR_CHANNEL_DRAINING;

	if (channel->control_alive && channel->control_ready)
		out->control_lane_state = TR_CHANNEL_LANE_UP;
	else if (channel->control_alive)
		out->control_lane_state = TR_CHANNEL_LANE_HANDSHAKING;
	else if (channel->control_reconnecting)
		out->control_lane_state = TR_CHANNEL_LANE_RECONNECTING;
	else
		out->control_lane_state = TR_CHANNEL_LANE_DOWN;

	if (channel->bulk_alive && channel->bulk_ready)
		out->bulk_lane_state = TR_CHANNEL_LANE_UP;
	else if (channel->bulk_alive)
		out->bulk_lane_state = TR_CHANNEL_LANE_HANDSHAKING;
	else if (channel->bulk_reconnecting)
		out->bulk_lane_state = TR_CHANNEL_LANE_RECONNECTING;
	else
		out->bulk_lane_state = TR_CHANNEL_LANE_DOWN;

	out->streams_opened = channel->stat_streams_opened;
	out->streams_closed = channel->stat_streams_closed;
	out->stream_errors = channel->stat_stream_errors;
	out->messages_tx = channel->stat_messages_tx;
	out->messages_rx = channel->stat_messages_rx;
	out->bytes_tx = channel->stat_bytes_tx;
	out->bytes_rx = channel->stat_bytes_rx;
	out->window_updates_tx = channel->stat_window_updates_tx;
	out->window_updates_rx = channel->stat_window_updates_rx;
	out->reconnect_attempts = channel->stat_reconnect_attempts;
	out->reconnect_successes = channel->stat_reconnect_successes;
	out->keepalive_pings_sent = channel->stat_keepalive_pings_sent;
	out->keepalive_pongs_received = channel->stat_keepalive_pongs_received;
	out->keepalive_timeouts = channel->stat_keepalive_timeouts;
	out->control_last_rtt_ns = channel->keepalive_last_rtt_ns[0];
	out->bulk_last_rtt_ns = channel->keepalive_last_rtt_ns[1];
	control_connection = channel->control_connection;
	bulk_connection = channel->bulk_connection;
	pthread_mutex_unlock(&channel->lock);

	(void)tr_reactor_get_connection_stats(control_connection,
					      &out->control_connection);
	(void)tr_reactor_get_connection_stats(bulk_connection,
					      &out->bulk_connection);
	return TR_OK;
}
