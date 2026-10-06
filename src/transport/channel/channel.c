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

enum tr_stream_slot_state {
	TR_STREAM_SLOT_FREE = 0,
	TR_STREAM_SLOT_OPENING,
	TR_STREAM_SLOT_OPEN
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

	struct tr_buffer *rx_reassembly;
	uint64_t rx_reassembly_message_id;
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
	struct tr_channel_pending_hello pending_hello[2];
	int local_draining;
	int control_peer_draining;
	int bulk_peer_draining;
	int control_goaway_sent;
	int bulk_goaway_sent;
	struct tr_channel_capabilities control_caps;
	struct tr_channel_capabilities bulk_caps;

	uint32_t next_local_stream_id;
	uint32_t free_stream_head;
	uint32_t active_streams;
	struct tr_stream_slot *streams;
	struct tr_stream_index_entry *stream_index;
	size_t stream_index_capacity;

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
	struct tr_reactor_timer_handle keepalive_timer;
	int keepalive_timer_registered;
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
		/*
         * probe 发出后的任意 peer traffic 都足以证明 connection 仍存活。
         * 在 timeout 边界前继续保留 outstanding probe，使显式 PONG 仍可提供
         * RTT sample；如果只观察到其他 traffic，则正常结束 probe，
         * 不把 connection 判定为失败。
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

static uint64_t
tr_channel_keepalive_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	uint32_t tick_ms;
	int enabled;

	/*
	 * Reactor-local callback: all network actions stay on the owning event loop.
	 * Channel lock is retained as a transition lock for public snapshot APIs.
	 */
	pthread_mutex_lock(&channel->lock);
	enabled = channel->keepalive_enabled;
	pthread_mutex_unlock(&channel->lock);
	if (!enabled)
		return 0;

	tr_channel_keepalive_check_lane(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_check_lane(channel, TR_LANE_BULK);

	pthread_mutex_lock(&channel->lock);
	enabled = channel->keepalive_enabled;
	tick_ms = enabled ? tr_channel_keepalive_tick_ms_locked(channel) : 0U;
	pthread_mutex_unlock(&channel->lock);

	if (!enabled)
		return 0;
	if (now_ns == 0)
		now_ns = tr_channel_now_ns();
	if (now_ns == 0)
		return 0;
	return tr_add_sat_u64(
		now_ns, (uint64_t)tick_ms * UINT64_C(1000000));
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

	/*
	 * handler 已经安装后，peer 可能在 Channel create 返回前完成协议判定
	 * 并关闭 connection。create 的既有契约是“构造成功，随后通过 lane DOWN
	 * 表达协商失败”。先跨过 Reactor quiescence barrier，确保可能的 close
	 * callback 已完成，再根据 Channel 自身状态判断是否属于该异步关闭。
	 */
	(void)tr_reactor_quiesce(channel->reactor);

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

static int tr_channel_send_pending_goaway_on_owner(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle bulk_connection;
	int send_control = 0;
	int send_bulk = 0;
	int ret = TR_OK;
	int tmp;

	pthread_mutex_lock(&channel->lock);
	if (!channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}

	control_connection = channel->control_connection;
	bulk_connection = channel->bulk_connection;
	if (channel->control_alive && channel->control_ready &&
	    !channel->control_goaway_sent)
		send_control = 1;
	if (channel->bulk_alive && channel->bulk_ready &&
	    !channel->bulk_goaway_sent)
		send_bulk = 1;
	pthread_mutex_unlock(&channel->lock);

	if (send_control) {
		tmp = tr_reactor_send_goaway_on_owner(control_connection);
		if (tmp == TR_OK) {
			pthread_mutex_lock(&channel->lock);
			if (tr_conn_equal(channel->control_connection,
					  control_connection))
				channel->control_goaway_sent = 1;
			if (tr_conn_equal(channel->bulk_connection,
					  control_connection))
				channel->bulk_goaway_sent = 1;
			pthread_mutex_unlock(&channel->lock);
		} else {
			ret = tmp;
		}
	}

	if (send_bulk && !tr_conn_equal(bulk_connection, control_connection)) {
		tmp = tr_reactor_send_goaway_on_owner(bulk_connection);
		if (tmp == TR_OK) {
			pthread_mutex_lock(&channel->lock);
			if (tr_conn_equal(channel->bulk_connection,
					  bulk_connection))
				channel->bulk_goaway_sent = 1;
			pthread_mutex_unlock(&channel->lock);
		} else if (ret == TR_OK) {
			ret = tmp;
		}
	}

	return ret;
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

	generation = stream->generation;
	(void)tr_stream_index_remove_locked(channel, stream->stream_id);
	if (stream->rx_reassembly)
		tr_buffer_release(tr_buffer_take(&stream->rx_reassembly));
	memset(stream, 0, sizeof(*stream));
	stream->generation = generation;
	stream->free_next = channel->free_stream_head;
	channel->free_stream_head = slot;
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

static void tr_stream_consume_locked(struct tr_channel *channel,
				     struct tr_stream_slot *stream,
				     uint64_t bytes)
{
	uint64_t available =
		stream->rx_received_bytes - stream->rx_consumed_bytes;

	if (bytes > available)
		bytes = available;
	stream->rx_consumed_bytes += bytes;
	(void)tr_send_window_update_locked(channel, stream);
}

static void tr_stream_notify(struct tr_channel *channel,
			     struct tr_stream_handle handle,
			     enum tr_stream_event event, int status)
{
	tr_stream_event_cb cb = channel->stream_event_cb;
	void *cb_arg = channel->callback_arg;

	/*
	 * upper-layer handler publication/read 都在 Reactor owner 上完成。
	 * callback 调用前 snapshot，允许 callback 自己同步替换后续 handler，
	 * 但不影响当前 event。
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

	/*
	 * 两组 callback 都是 owner publication。先 snapshot 再调用 normal handler，
	 * 保持当前 event 的 observer 集合稳定；callback 内替换只影响后续 event。
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
	generation = stream->generation + 1U;
	if (generation == 0)
		generation = 1U;

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
			stream->free_next = channel->free_stream_head;
			channel->free_stream_head = slot;
			return ret;
		}
	}

	if (out)
		*out = tr_stream_make_handle(channel, slot, stream);
	channel->active_streams++;
	channel->stat_streams_opened++;
	return TR_OK;
}

static int tr_channel_protocol_error(struct tr_channel *channel,
				     struct tr_conn_handle connection,
				     int status)
{
	(void)channel;
	(void)tr_reactor_close(connection);
	return status;
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
	/* Peer HELLO 声明的是 peer 的接收能力，也就是我们的 TX limits。 */
	negotiated.max_frame_payload_bytes =
		peer_max_frame < local.max_frame_payload_bytes ?
			peer_max_frame :
			local.max_frame_payload_bytes;
	negotiated.max_message_bytes = peer_max_message;
	negotiated.feature_bits = peer_features & local.feature_bits;

	if (negotiated.max_frame_payload_bytes == 0 ||
	    negotiated.max_message_bytes == 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_UNSUPPORTED);

	/* ACK 把本地 receive limits 返回给 peer。 */
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

	/*
	 * Protocol errors already close the connection. Deferred replay mirrors
	 * normal Reactor frame dispatch, where the handler result is not surfaced
	 * synchronously to the Channel creator.
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
	    (caps.feature_bits & ~local.feature_bits) != 0)
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
	int control_event = 0;
	int bulk_event = 0;

	if (frame->payload || frame->header.payload_len != 0 ||
	    frame->header.stream_id != 0 || frame->header.message_id != 0 ||
	    frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);

	pthread_mutex_lock(&channel->lock);
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

	if (control_event)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_CONTROL_GOAWAY,
				  TR_OK);
	if (bulk_event)
		tr_channel_notify(channel, TR_CHANNEL_EVENT_BULK_GOAWAY, TR_OK);
	return TR_OK;
}

static int tr_channel_handle_stream_open(struct tr_channel *channel,
					 struct tr_conn_handle connection,
					 const struct tr_frame *frame)
{
	struct tr_stream_slot *stream;
	struct tr_stream_handle handle;
	enum tr_lane lane;
	uint32_t slot = 0;
	int ret;

	if (frame->payload || frame->header.payload_len != 0 ||
	    (frame->header.flags & ~(uint32_t)TR_FRAME_F_LANE_BULK) != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	lane = (frame->header.flags & TR_FRAME_F_LANE_BULK) ? TR_LANE_BULK :
							      TR_LANE_CONTROL;

	if (!tr_conn_equal(tr_channel_connection(channel, lane), connection) ||
	    tr_stream_id_is_local(channel, frame->header.stream_id))
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);

	pthread_mutex_lock(&channel->lock);
	if (tr_stream_lookup_id(channel, frame->header.stream_id, NULL)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}

	/*
	 * begin_drain() is a monotonic admission barrier. A peer may have sent
	 * STREAM_OPEN just before receiving our GOAWAY; accepting that delayed frame
	 * after local_draining became visible would make active_streams increase
	 * again after wait_drained() had observed zero.
	 *
	 * Reject without allocating local Stream state. The peer already owns its
	 * initiating Stream slot, so STREAM_CLOSE gives it an ordinary remote-close
	 * signal. If even this bounded control frame cannot be admitted, close the
	 * connection rather than leave the two endpoints with inconsistent Stream
	 * ownership.
	 */
	if (channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		ret = tr_reactor_send(connection, TR_FRAME_STREAM_CLOSE, 0,
				      frame->header.stream_id, 0, NULL);
		if (ret != TR_OK)
			(void)tr_reactor_close_on_owner(connection);
		return ret;
	}

	ret = tr_stream_allocate_locked(channel, frame->header.stream_id, lane,
					TR_STREAM_SLOT_OPEN, &handle);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&channel->lock);
		return ret;
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
		return ret;
	}
	channel->stat_window_updates_tx++;

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
	channel->stat_window_updates_rx++;
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
	int ret = TR_OK;

	flags = frame->header.flags;
	if ((flags & ~(TR_FRAME_F_FIRST | TR_FRAME_F_LAST)) != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	fragmented = (flags & (TR_FRAME_F_FIRST | TR_FRAME_F_LAST)) !=
		     (TR_FRAME_F_FIRST | TR_FRAME_F_LAST);
	complete = (flags & TR_FRAME_F_LAST) != 0;

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (!stream || stream->state != TR_STREAM_SLOT_OPEN ||
	    !stream->remote_open ||
	    !tr_stream_connection_matches(channel, stream, connection) ||
	    frame->header.message_id != stream->next_rx_message_id) {
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

	/*
     * 单 frame message 保持 zero-copy。
     * fragmented message 只复制一次到有界的 Channel reassembly buffer，
     * 之后 Transport frame buffer 可以立即归还 Reactor RX pool。
     */
	if (!fragmented) {
		if (stream->rx_reassembly ||
		    (channel->config.max_message_bytes != 0 &&
		     payload_len > channel->config.max_message_bytes)) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 TR_ERR_BAD_LENGTH);
		}

		stream->rx_received_bytes = new_received;
		stream->next_rx_message_id++;
		deliver = frame->payload;
	} else if (flags & TR_FRAME_F_FIRST) {
		if (stream->rx_reassembly || !channel->config.reassembly_pool ||
		    channel->config.max_message_bytes == 0 ||
		    payload_len > channel->config.max_message_bytes) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 TR_ERR_BAD_LENGTH);
		}

		ret = tr_buffer_acquire(channel->config.reassembly_pool,
					channel->config.max_message_bytes,
					&stream->rx_reassembly);
		if (ret != TR_OK) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 ret);
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
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 TR_ERR_STATE);
		}

		combined_len =
			(uint64_t)stream->rx_reassembly->len + payload_len;
		if (combined_len > channel->config.max_message_bytes ||
		    combined_len > stream->rx_reassembly->capacity) {
			pthread_mutex_unlock(&channel->lock);
			return tr_channel_protocol_error(channel, connection,
							 TR_ERR_BAD_LENGTH);
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
		tr_stream_consume_locked(channel, stream, logical_len);
	pthread_mutex_unlock(&channel->lock);

	if (fragmented && deliver)
		tr_buffer_release(deliver);
	return TR_OK;
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
	    frame->header.flags != 0)
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_BAD_FLAGS);

	pthread_mutex_lock(&channel->lock);
	stream = tr_stream_lookup_id(channel, frame->header.stream_id, &slot);
	if (!stream) {
		/*
		 * A STREAM_OPEN rejected after our drain barrier has no local Stream
		 * slot. Its initiator may still answer our rejection with STREAM_CLOSE
		 * when it closes its own half. Absorb only peer-owned ids while draining;
		 * unknown locally-owned ids remain protocol errors.
		 */
		if (channel->local_draining &&
		    !tr_stream_id_is_local(channel, frame->header.stream_id)) {
			pthread_mutex_unlock(&channel->lock);
			return TR_OK;
		}
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
	}
	if (!tr_stream_connection_matches(channel, stream, connection)) {
		pthread_mutex_unlock(&channel->lock);
		return tr_channel_protocol_error(channel, connection,
						 TR_ERR_STATE);
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
	return TR_OK;
}

static enum tr_frame_disposition
tr_channel_on_frame(struct tr_conn_handle connection, struct tr_frame *frame,
		    void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	int ret = TR_OK;

	/*
	 * Deferred Channel creation installs the transport handler before upper
	 * layers are ready. An early peer HELLO must not ACK or publish lane UP,
	 * otherwise the peer can send application DATA into an unbound Channel.
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
	case TR_FRAME_DATA:
		ret = tr_channel_handle_data(channel, connection, frame);
		break;
	case TR_FRAME_PING:
		(void)tr_reactor_send(connection, TR_FRAME_PONG, 0, 0,
				      frame->header.message_id, NULL);
		break;
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
	return TR_FRAME_RELEASE;
}

static void tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel);

static void tr_channel_on_connection_event(struct tr_conn_handle connection,
					   enum tr_connection_event event,
					   int status, void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;
	int control_down = 0;
	int bulk_down = 0;
	int stream_status = status == TR_OK ? TR_ERR_CLOSED : status;

	(void)event;
	pthread_mutex_lock(&channel->lock);
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

	/* Transport connection replacement 不保留旧 Stream 的 byte state。 */
	if (control_down)
		tr_channel_fail_lane_streams(channel, TR_LANE_CONTROL,
					     stream_status);
	if (bulk_down)
		tr_channel_fail_lane_streams(channel, TR_LANE_BULK,
					     stream_status);

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

static void tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel);

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

		/*
		 * connector 在进入 active 后失败时会同步/异步调用 completion。
		 * 只有 start 在发布 connector ownership 前失败，attempt 才仍由这里收敛。
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

static void tr_channel_reconnect_schedule_on_owner(struct tr_channel *channel)
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
		return;

	(void)port;
	delay_ms = tr_reconnect_delay_ms(
		channel->reconnect_initial_delay_ms,
		channel->reconnect_max_delay_ms, attempt);
	now_ns = tr_channel_now_ns();
	if (now_ns == 0U) {
		tr_channel_reconnect_finish_attempt(channel, lane, 0);
		return;
	}
	delay_ns = (uint64_t)delay_ms * UINT64_C(1000000);
	deadline_ns = UINT64_MAX - now_ns < delay_ns ?
			      UINT64_MAX : now_ns + delay_ns;
	ret = tr_reactor_timer_arm(channel->reconnect_timer, deadline_ns);
	if (ret != TR_OK)
		tr_channel_reconnect_finish_attempt(channel, lane, 0);
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
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}

	/*
	 * Upper-layer handlers are installed before deferred users call start().
	 * Publish that readiness under the same lock used by RX deferral, and move
	 * any already-received HELLO into stack-owned replay requests.
	 */
	channel->handshake_started = 1;
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

	/*
	 * Replay early peer HELLO on the Reactor owner. Only after this point can
	 * ACK/lane-UP let the peer proceed to STREAM_OPEN/DATA, and the upper layer
	 * is already bound.
	 */
	for (i = 0; i < 2; ++i) {
		if (!pending[i].hello.valid)
			continue;
		ret = tr_reactor_call(channel->reactor,
				      tr_channel_process_pending_hello_on_owner,
				      &pending[i]);
		if (ret != TR_OK)
			return ret;
	}

	ret = tr_channel_send_hello(channel, control_connection);
	ret = tr_channel_normalize_create_hello_error(
		channel, control_connection, ret);
	if (ret != TR_OK)
		return ret;

	if (split) {
		ret = tr_channel_send_hello(channel, bulk_connection);
		ret = tr_channel_normalize_create_hello_error(
			channel, bulk_connection, ret);
		if (ret != TR_OK)
			return ret;
	}

	return TR_OK;
}

struct tr_channel_build {
	struct tr_channel *channel;
	int lock_ready;
	int state_cond_ready;
	int keepalive_timer_ready;
	int control_handler_installed;
	int bulk_handler_installed;
};

struct tr_channel_build_close_request {
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
	int split;
};

static int tr_channel_build_close_on_owner(void *arg)
{
	struct tr_channel_build_close_request *request =
		(struct tr_channel_build_close_request *)arg;
	int final = TR_OK;
	int ret;

	ret = tr_reactor_close_on_owner(request->control);
	if (ret != TR_OK && ret != TR_ERR_STALE && ret != TR_ERR_CLOSED)
		final = ret;

	if (request->split) {
		ret = tr_reactor_close_on_owner(request->bulk);
		if (ret != TR_OK && ret != TR_ERR_STALE &&
		    ret != TR_ERR_CLOSED && final == TR_OK)
			final = ret;
	}
	return final;
}

static void tr_channel_build_cleanup(struct tr_channel_build *build)
{
	struct tr_channel *channel;
	int handlers_removed = 0;

	if (!build || !build->channel)
		return;
	channel = build->channel;

	if (build->control_handler_installed) {
		(void)tr_reactor_set_handler(channel->control_connection, NULL,
					     NULL, NULL);
		handlers_removed = 1;
	}
	if (build->bulk_handler_installed) {
		(void)tr_reactor_set_handler(channel->bulk_connection, NULL,
					     NULL, NULL);
		handlers_removed = 1;
	}
	if (handlers_removed && channel->reactor) {
		struct tr_channel_build_close_request request;
		int ret;

		request.control = channel->control_connection;
		request.bulk = channel->bulk_connection;
		request.split = !tr_conn_equal(
			request.control, request.bulk);
		ret = tr_reactor_call(
			channel->reactor, tr_channel_build_close_on_owner,
			&request);
#ifndef NDEBUG
		assert(ret == TR_OK || ret == TR_ERR_CLOSED);
#endif
		if (ret != TR_OK && ret != TR_ERR_CLOSED)
			return;
	}

	if (channel->streams) {
		uint32_t i;
		for (i = 0; i < channel->config.max_streams; ++i)
			if (channel->streams[i].rx_reassembly)
				tr_buffer_release(channel->streams[i].rx_reassembly);
	}
	free(channel->stream_index);
	free(channel->streams);
	if (build->keepalive_timer_ready)
		(void)tr_reactor_timer_unregister(channel->keepalive_timer);
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
	    channel->config.min_protocol_version == 0 ||
	    channel->config.max_protocol_version == 0 ||
	    channel->config.min_protocol_version >
		    channel->config.max_protocol_version)
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
	ret = tr_reactor_timer_register(channel->reactor,
					tr_channel_keepalive_timer_main,
					channel, &channel->keepalive_timer);
	if (ret != TR_OK)
		return ret;
	channel->keepalive_timer_registered = 1;
	build.keepalive_timer_ready = 1;

	channel->streams = (struct tr_stream_slot *)calloc(
		channel->config.max_streams, sizeof(*channel->streams));
	channel->stream_index_capacity =
		tr_stream_index_capacity_for(channel->config.max_streams);
	if (channel->stream_index_capacity == 0U)
		return TR_ERR_INVALID;
	channel->stream_index = (struct tr_stream_index_entry *)calloc(
		channel->stream_index_capacity, sizeof(*channel->stream_index));
	if (!channel->streams || !channel->stream_index)
		return TR_ERR_NOMEM;
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

	ret = tr_reactor_set_handler(control_connection, tr_channel_on_frame,
				     tr_channel_on_connection_event, channel);
	if (ret != TR_OK)
		return ret;
	build.control_handler_installed = 1;

	if (!tr_conn_equal(control_connection, bulk_connection)) {
		ret = tr_reactor_set_handler(bulk_connection,
					     tr_channel_on_frame,
					     tr_channel_on_connection_event,
					     channel);
		if (ret != TR_OK)
			return ret;
		build.bulk_handler_installed = 1;
	}

	if (start_handshake) {
		ret = tr_channel_start(channel);
		if (ret != TR_OK)
			return ret;
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

	/*
	 * Detached ownership transfer must cross every fallible lifetime barrier
	 * while the peer is still owner-visible. Finalizers have no natural retry
	 * queue, so they must not discover a live drain waiter after transfer.
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

	/*
	 * handler publication 是 callback 生命周期屏障。TR_ERR_STALE 能证明
	 * 这个 exact connection generation 已经退休；其他失败都必须保留
	 * Channel ownership，并在 finalization 前重试收敛。
	 */
	ret = tr_reactor_set_handler(control, NULL, NULL, NULL);
	if (ret != TR_OK && ret != TR_ERR_STALE)
		return ret;
	if (split) {
		ret = tr_reactor_set_handler(bulk, NULL, NULL, NULL);
		if (ret != TR_OK && ret != TR_ERR_STALE)
			return ret;
	}

	/*
	 * timer publication 必须保持到 unregister 被确认成功。即使 handler 已经
	 * detach，timer barrier 失败仍然可重试：Channel storage 保持存活，
	 * keepalive admission 已关闭，下一次调用继续重试 exact timer barrier，
	 * 不能假装 timer source 已经消失。
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

	/*
	 * 所有 owner-visible callback/timer source 已经完成 detach。此时才允许
	 * 清除上层 callback publication，并发布 detached ownership。
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

int tr_channel_finalize_detached(struct tr_channel *channel)
{
	if (!channel)
		return TR_OK;

#ifndef NDEBUG
	assert(channel->teardown_detached);
	assert(!channel->keepalive_timer_registered);
	assert(channel->state_wait_closed);
	assert(channel->state_waiters == 0U);
#endif

	/*
	 * All fallible owner/waiter barriers were completed before detached
	 * ownership transfer. Finalization is now pure memory/resource release.
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
	return TR_OK;
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

	/*
	 * Teardown sources before freeing any storage they can reference. A Client
	 * owns reconnect connector/timer state; Server Channels never enable that
	 * source and may be finalized after their Reactor has already stopped.
	 */
	ret = tr_channel_disable_keepalive(channel);
	if (ret != TR_OK)
		return ret;

	if (channel->config.role == TR_CHANNEL_CLIENT &&
	    tr_channel_client_reconnect_present(channel)) {
		/*
		 * A stopped Reactor is a valid teardown domain only when no reconnect
		 * source remains. Normal Client facade teardown disables reconnect before
		 * Runtime stop. If a low-level caller stops Reactor with connector/timer
		 * ownership still published, fail closed rather than bypass owner detach.
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

	/*
	 * quiesce distinguishes "fully stopped" from "currently stopping":
	 * - stopped: TR_OK, there can be no future callback even if SET_HANDLER
	 *   returned CLOSED;
	 * - stopping: TR_ERR_CLOSED, ownership has not converged yet, so fail closed.
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

	/*
	 * keepalive has been synchronously disarmed above. Release its timer slot
	 * before Channel storage so the timer queue no longer retains callback_arg.
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

/*
 * Replacement attachment must be atomic with respect to Reactor I/O callbacks.
 *
 * If the handler becomes visible before Channel records the new lane mapping,
 * an already-readable peer HELLO can run immediately and observe lane_mask == 0,
 * incorrectly turning a healthy replacement connection into a protocol error.
 *
 * Run validation, handler installation and lane publication in one owner
 * operation so no connection callback can interleave with the transition.
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

	/*
	 * tr_reactor_call() guarantees owner context here; set_handler therefore
	 * updates the connection directly and cannot dispatch an I/O callback in
	 * the middle of this transition.
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
		channel->bulk_alive = 1;
		channel->bulk_ready = 0;
		memset(&channel->bulk_caps, 0, sizeof(channel->bulk_caps));
		channel->bulk_reconnecting = 0;
		channel->bulk_reconnect_attempt = 0;
	} else {
		channel->control_connection = connection;
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

		/*
		 * Rollback failure means connector callback-source ownership did not
		 * converge; report that error instead of pretending enable merely failed.
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

	tr_channel_reconnect_schedule_on_owner(channel);
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

	/*
	 * First close scheduling admission. Published source handles remain intact
	 * until their individual teardown barriers succeed, so a failure is
	 * retryable and cannot orphan callback_arg ownership.
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

int tr_channel_disable_keepalive(struct tr_channel *channel)
{
	struct tr_reactor_timer_handle timer;
	int registered;
	int ret;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	channel->keepalive_enabled = 0;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
	registered = channel->keepalive_timer_registered;
	timer = channel->keepalive_timer;
	pthread_mutex_unlock(&channel->lock);

	if (!registered)
		return TR_OK;

	/*
	 * Never wait for the Reactor while holding channel->lock. arm(0) is also a
	 * synchronization barrier while running; after Reactor stop it directly
	 * disarms the owner queue under ctl_lock.
	 */
	ret = tr_reactor_timer_arm(timer, 0);
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
	uint32_t active;

	if (!channel || !out)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&channel->lock);
	active = tr_channel_active_streams_locked(channel);
	if (!channel->local_draining)
		*out = TR_CHANNEL_RUNNING;
	else if (active == 0)
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

	/*
	 * drain barrier 与 reconnect disable 必须在同一个 owner turn 内完成。
	 * 否则 application thread 可能在“disable reconnect”返回后、设置
	 * local_draining 前重新 enable reconnect。
	 */
	pthread_mutex_lock(&channel->lock);
	channel->local_draining = 1;
	pthread_mutex_unlock(&channel->lock);

	ret = tr_channel_disable_client_reconnect_on_owner(channel);
	if (ret != TR_OK)
		return ret;
	return tr_channel_send_pending_goaway_on_owner(channel);
}

int tr_channel_wait_ready(
	struct tr_channel *channel, enum tr_lane lane, uint32_t timeout_ms)
{
	struct timespec deadline;
	int timed = timeout_ms != 0U;
	int result = TR_OK;

	if (!channel ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	if (timed) {
		uint64_t deadline_ns;

		if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
			return TR_ERR_SYS;
		deadline_ns = tr_add_sat_u64(
			(uint64_t)deadline.tv_sec * UINT64_C(1000000000) +
				(uint64_t)deadline.tv_nsec,
			(uint64_t)timeout_ms * UINT64_C(1000000));
		deadline.tv_sec =
			(time_t)(deadline_ns / UINT64_C(1000000000));
		deadline.tv_nsec =
			(long)(deadline_ns % UINT64_C(1000000000));
	}

	pthread_mutex_lock(&channel->lock);
	if (channel->state_wait_closed) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if ((lane == TR_LANE_CONTROL && channel->control_ready) ||
	    (lane == TR_LANE_BULK && channel->bulk_ready)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_OK;
	}
	if (channel->state_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	channel->state_waiters++;

	for (;;) {
		int ready = lane == TR_LANE_CONTROL ?
			channel->control_ready : channel->bulk_ready;
		int ret;

		if (ready)
			break;
		if (channel->state_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}

		if (timed)
			ret = pthread_cond_timedwait(
				&channel->state_cond, &channel->lock, &deadline);
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

int tr_channel_begin_drain(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;

	return tr_reactor_call(
		channel->reactor, tr_channel_begin_drain_on_owner, channel);
}

int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms)
{
	struct timespec deadline;
	int timed = timeout_ms != 0U;
	int result = TR_OK;

	if (!channel)
		return TR_ERR_INVALID;

	/*
	 * wait_drained() is a pure waitqueue-style observer. It must not advance
	 * Channel protocol state: GOAWAY admission/retry belongs to begin_drain()
	 * on the Reactor owner. Waiting from the owner would stall the RX/TX and
	 * close/cancel callbacks that can make active_streams reach zero.
	 */
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	if (timed) {
		uint64_t deadline_ns;

		if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
			return TR_ERR_SYS;
		deadline_ns = tr_add_sat_u64(
			(uint64_t)deadline.tv_sec * UINT64_C(1000000000) +
				(uint64_t)deadline.tv_nsec,
			(uint64_t)timeout_ms * UINT64_C(1000000));
		deadline.tv_sec =
			(time_t)(deadline_ns / UINT64_C(1000000000));
		deadline.tv_nsec =
			(long)(deadline_ns % UINT64_C(1000000000));
	}

	pthread_mutex_lock(&channel->lock);
	if (channel->state_wait_closed) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (!channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	if (channel->active_streams == 0U) {
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

	while (channel->active_streams != 0U) {
		int ret;

		if (channel->state_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}
		ret = pthread_cond_timedwait(
			&channel->state_cond, &channel->lock, &deadline);

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
	channel->next_local_stream_id += 2U;
	if (channel->next_local_stream_id == 0)
		channel->next_local_stream_id =
			channel->config.role == TR_CHANNEL_CLIENT ? 1U : 2U;

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
		tr_stream_consume_locked(channel, stream, len);
		if (stream->pending_advertised_limit != 0 &&
		    stream->rx_advertised_limit == before)
			ret = TR_AGAIN;
	} else {
		ret = TR_ERR_STALE;
	}
	pthread_mutex_unlock(&channel->lock);

	tr_buffer_release(payload);
	return ret;
}

int tr_channel_flush(struct tr_channel *channel)
{
	uint32_t i;
	int result = TR_OK;

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
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	pthread_mutex_unlock(&channel->lock);

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
	else if (active == 0)
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
