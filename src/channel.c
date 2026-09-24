#include "tr/channel.h"

#include "tr/status.h"
#include "tr/wire.h"
#include "tr/socket.h"
#include "tr/endian.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TR_CHANNEL_HELLO_WIRE_SIZE 32U
#define TR_CHANNEL_PROTOCOL_BUFFER_COUNT 8U
#define TR_CHANNEL_PROTOCOL_BUFFER_SIZE 64U

enum tr_stream_slot_state {
	TR_STREAM_SLOT_FREE = 0,
	TR_STREAM_SLOT_OPENING,
	TR_STREAM_SLOT_OPEN
};

struct tr_stream_slot {
	uint32_t generation;
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

	struct tr_reactor *reactor;
	struct tr_conn_handle control_connection;
	struct tr_conn_handle bulk_connection;

	int control_alive;
	int bulk_alive;
	int control_ready;
	int bulk_ready;
	int local_draining;
	int control_peer_draining;
	int bulk_peer_draining;
	int control_goaway_sent;
	int bulk_goaway_sent;
	struct tr_channel_capabilities control_caps;
	struct tr_channel_capabilities bulk_caps;
	struct tr_buffer_pool protocol_pool;

	uint32_t next_local_stream_id;
	struct tr_stream_slot *streams;

	tr_stream_data_cb data_cb;
	tr_stream_event_cb stream_event_cb;
	tr_channel_event_cb channel_event_cb;
	void *callback_arg;

	pthread_cond_t reconnect_cond;
	pthread_t reconnect_thread;
	int reconnect_thread_started;
	int reconnect_enabled;
	int reconnect_stop;

	char reconnect_address[64];
	uint16_t reconnect_control_port;
	uint16_t reconnect_bulk_port;
	uint32_t reconnect_initial_delay_ms;
	uint32_t reconnect_max_delay_ms;
	uint32_t reconnect_connect_timeout_ms;
	uint32_t control_reconnect_attempt;
	uint32_t bulk_reconnect_attempt;
	int control_reconnecting;
	int bulk_reconnecting;

	pthread_cond_t keepalive_cond;
	pthread_t keepalive_thread;
	int keepalive_thread_started;
	int keepalive_enabled;
	int keepalive_stop;
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
tr_channel_connection_lane_mask(const struct tr_channel *channel,
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

static void tr_channel_keepalive_reset_locked(struct tr_channel *channel,
					      enum tr_lane lane)
{
	int idx = tr_lane_index(lane);

	channel->keepalive_ping_outstanding[idx] = 0;
	channel->keepalive_ping_id[idx] = 0;
	channel->keepalive_ping_sent_ns[idx] = 0;
}

static void tr_channel_timespec_after_ms(struct timespec *ts, uint32_t ms)
{
	uint64_t nsec;

	(void)clock_gettime(CLOCK_REALTIME, ts);
	nsec = (uint64_t)ts->tv_nsec +
	       (uint64_t)(ms % 1000U) * UINT64_C(1000000);
	ts->tv_sec +=
		(time_t)(ms / 1000U + (uint32_t)(nsec / UINT64_C(1000000000)));
	ts->tv_nsec = (long)(nsec % UINT64_C(1000000000));
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
	if (!channel->keepalive_enabled || channel->keepalive_stop ||
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
         * Any post-probe peer traffic is sufficient proof of liveness. Keep
         * the probe outstanding until its timeout boundary so an explicit
         * PONG can still contribute an RTT sample; if only other traffic was
         * observed, simply retire the probe without failing the connection.
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

static void *tr_channel_keepalive_thread_main(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;

	for (;;) {
		uint32_t tick_ms;
		struct timespec deadline;

		pthread_mutex_lock(&channel->lock);
		if (channel->keepalive_stop) {
			pthread_mutex_unlock(&channel->lock);
			break;
		}
		tick_ms = channel->keepalive_interval_ms / 4U;
		if (channel->keepalive_timeout_ms / 4U < tick_ms)
			tick_ms = channel->keepalive_timeout_ms / 4U;
		if (tick_ms < 10U)
			tick_ms = 10U;
		if (tick_ms > 1000U)
			tick_ms = 1000U;
		pthread_mutex_unlock(&channel->lock);

		tr_channel_keepalive_check_lane(channel, TR_LANE_CONTROL);
		tr_channel_keepalive_check_lane(channel, TR_LANE_BULK);

		pthread_mutex_lock(&channel->lock);
		if (!channel->keepalive_stop) {
			tr_channel_timespec_after_ms(&deadline, tick_ms);
			(void)pthread_cond_timedwait(&channel->keepalive_cond,
						     &channel->lock, &deadline);
		}
		pthread_mutex_unlock(&channel->lock);
	}
	return NULL;
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

static int tr_channel_local_capabilities(const struct tr_channel *channel,
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
	caps->lane_mask = tr_channel_connection_lane_mask(channel, connection);
	caps->max_frame_payload_bytes = limits.max_payload_len;
	caps->max_message_bytes =
		tr_channel_local_max_message(channel, limits.max_payload_len);
	caps->feature_bits = channel->config.feature_bits;
	if (caps->lane_mask == 0 || caps->max_frame_payload_bytes == 0 ||
	    caps->max_message_bytes == 0)
		return TR_ERR_STATE;
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
	ret = tr_buffer_acquire(&channel->protocol_pool,
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

static int tr_channel_send_hello_ack(struct tr_channel *channel,
				     struct tr_conn_handle connection,
				     const struct tr_channel_capabilities *caps)
{
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	int ret;

	ret = tr_buffer_acquire(&channel->protocol_pool,
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
	uint32_t mask = tr_channel_connection_lane_mask(channel, connection);

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
	uint32_t i;
	uint32_t count = 0;

	for (i = 0; i < channel->config.max_streams; ++i)
		if (channel->streams[i].state != TR_STREAM_SLOT_FREE)
			count++;
	return count;
}

static int tr_channel_send_pending_goaway(struct tr_channel *channel)
{
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
		tmp = tr_reactor_send(control_connection, TR_FRAME_GOAWAY, 0, 0,
				      0, NULL);
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
		tmp = tr_reactor_send(bulk_connection, TR_FRAME_GOAWAY, 0, 0, 0,
				      NULL);
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

static struct tr_stream_slot *tr_stream_lookup_id(struct tr_channel *channel,
						  uint32_t stream_id,
						  uint32_t *slot_out)
{
	uint32_t i;

	for (i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_slot *stream = &channel->streams[i];
		if (stream->state != TR_STREAM_SLOT_FREE &&
		    stream->stream_id == stream_id) {
			if (slot_out)
				*slot_out = i;
			return stream;
		}
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

static void tr_stream_reset_slot(struct tr_stream_slot *stream)
{
	uint32_t generation;

	if (!stream)
		return;

	generation = stream->generation;
	if (stream->rx_reassembly) {
		tr_buffer_release(stream->rx_reassembly);
		stream->rx_reassembly = NULL;
	}
	memset(stream, 0, sizeof(*stream));
	stream->generation = generation;
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
	tr_stream_event_cb cb;
	void *cb_arg;

	pthread_mutex_lock(&channel->lock);
	cb = channel->stream_event_cb;
	cb_arg = channel->callback_arg;
	pthread_mutex_unlock(&channel->lock);

	if (cb)
		cb(handle, event, status, cb_arg);
}

static void tr_channel_notify(struct tr_channel *channel,
			      enum tr_channel_event event, int status)
{
	tr_channel_event_cb cb;
	void *cb_arg;

	pthread_mutex_lock(&channel->lock);
	cb = channel->channel_event_cb;
	cb_arg = channel->callback_arg;
	pthread_mutex_unlock(&channel->lock);

	if (cb)
		cb(channel, event, status, cb_arg);
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
			tr_stream_reset_slot(&channel->streams[i]);
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

static void tr_timespec_add_ms(struct timespec *ts, uint32_t delay_ms)
{
	ts->tv_sec += (time_t)(delay_ms / 1000U);
	ts->tv_nsec += (long)(delay_ms % 1000U) * 1000000L;
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec++;
		ts->tv_nsec -= 1000000000L;
	}
}

static int tr_channel_connect_ipv4_timeout(const char *address, uint16_t port,
					   uint32_t timeout_ms, int *out_fd)
{
	struct pollfd pfd;
	int fd = -1;
	int ret;

	ret = tr_tcp_connect_ipv4(address, port, &fd);
	if (ret == TR_OK) {
		*out_fd = fd;
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLOUT;

	do {
		ret = poll(&pfd, 1, (int)timeout_ms);
	} while (ret < 0 && errno == EINTR);

	if (ret <= 0) {
		tr_socket_close(&fd);
		return ret == 0 ? TR_AGAIN : TR_ERR_SYS;
	}

	ret = tr_tcp_finish_connect(fd);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}

	*out_fd = fd;
	return TR_OK;
}

static int tr_stream_allocate_locked(struct tr_channel *channel,
				     uint32_t stream_id, enum tr_lane lane,
				     enum tr_stream_slot_state state,
				     struct tr_stream_handle *out)
{
	uint32_t i;

	for (i = 0; i < channel->config.max_streams; ++i) {
		struct tr_stream_slot *stream = &channel->streams[i];
		uint32_t generation;

		if (stream->state != TR_STREAM_SLOT_FREE)
			continue;

		generation = stream->generation + 1U;
		if (generation == 0)
			generation = 1U;

		memset(stream, 0, sizeof(*stream));
		stream->generation = generation;
		stream->state = state;
		stream->lane = lane;
		stream->stream_id = stream_id;
		stream->local_open = 1;
		stream->remote_open = 1;
		stream->next_tx_message_id = 1;
		stream->next_rx_message_id = 1;
		stream->rx_advertised_limit =
			channel->config.initial_window_bytes;

		if (out)
			*out = tr_stream_make_handle(channel, i, stream);
		channel->stat_streams_opened++;
		return TR_OK;
	}

	return TR_AGAIN;
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
	/* Peer HELLO advertises what that peer can receive: our TX limits. */
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

	/* ACK advertises our receive limits back to the peer. */
	ack_caps = local;
	ack_caps.protocol_version = high;
	ack_caps.feature_bits = negotiated.feature_bits;
	ret = tr_channel_send_hello_ack(channel, connection, &ack_caps);
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
	(void)tr_channel_send_pending_goaway(channel);
	return TR_OK;
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
	(void)tr_channel_send_pending_goaway(channel);
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
	mask = tr_channel_connection_lane_mask(channel, connection);
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
		tr_stream_reset_slot(stream);
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
     * A single-frame message stays zero-copy. Fragmented messages are copied
     * exactly once into a bounded Channel reassembly buffer; transport frame
     * buffers can then return to the Reactor RX pool immediately.
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

		deliver = stream->rx_reassembly;
		logical_len = deliver->len;
		stream->rx_reassembly = NULL;
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
			frame->payload = NULL;
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
	if (!stream ||
	    !tr_stream_connection_matches(channel, stream, connection)) {
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
			tr_stream_reset_slot(stream);
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
	if (control_down || bulk_down)
		pthread_cond_broadcast(&channel->reconnect_cond);
	pthread_mutex_unlock(&channel->lock);

	/* A transport replacement never preserves stream byte state. */
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
}

static int tr_channel_reconnect_pick_locked(struct tr_channel *channel,
					    enum tr_lane *lane, uint16_t *port,
					    uint32_t *attempt)
{
	if (!channel->reconnect_enabled || channel->reconnect_stop ||
	    channel->local_draining)
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

static void tr_channel_reconnect_finish_attempt(struct tr_channel *channel,
						enum tr_lane lane, int success)
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
	pthread_cond_broadcast(&channel->reconnect_cond);
	pthread_mutex_unlock(&channel->lock);
}

static void *tr_channel_reconnect_thread_main(void *arg)
{
	struct tr_channel *channel = (struct tr_channel *)arg;

	for (;;) {
		enum tr_lane lane = TR_LANE_CONTROL;
		uint16_t port = 0;
		uint32_t attempt = 0;
		uint32_t delay_ms;
		struct timespec deadline;
		int fd TR_AUTO(tr_fd_cleanup) = -1;
		int ret;
		struct tr_conn_handle connection;

		pthread_mutex_lock(&channel->lock);
		while (!channel->reconnect_stop &&
		       !tr_channel_reconnect_pick_locked(channel, &lane, &port,
							 &attempt))
			pthread_cond_wait(&channel->reconnect_cond,
					  &channel->lock);

		if (channel->reconnect_stop) {
			pthread_mutex_unlock(&channel->lock);
			break;
		}

		delay_ms = tr_reconnect_delay_ms(
			channel->reconnect_initial_delay_ms,
			channel->reconnect_max_delay_ms, attempt);
		if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
			if (channel->config.mode ==
			    TR_CHANNEL_SHARED_CONNECTION) {
				channel->control_reconnecting = 0;
				channel->bulk_reconnecting = 0;
			} else if (lane == TR_LANE_BULK) {
				channel->bulk_reconnecting = 0;
			} else {
				channel->control_reconnecting = 0;
			}
			pthread_mutex_unlock(&channel->lock);
			continue;
		}
		tr_timespec_add_ms(&deadline, delay_ms);

		while (!channel->reconnect_stop &&
		       tr_channel_lane_still_down_locked(channel, lane)) {
			ret = pthread_cond_timedwait(&channel->reconnect_cond,
						     &channel->lock, &deadline);
			if (ret == ETIMEDOUT)
				break;
		}

		if (channel->reconnect_stop) {
			pthread_mutex_unlock(&channel->lock);
			break;
		}
		if (!tr_channel_lane_still_down_locked(channel, lane)) {
			if (channel->config.mode ==
			    TR_CHANNEL_SHARED_CONNECTION) {
				channel->control_reconnecting = 0;
				channel->bulk_reconnecting = 0;
			} else if (lane == TR_LANE_BULK) {
				channel->bulk_reconnecting = 0;
			} else {
				channel->control_reconnecting = 0;
			}
			pthread_mutex_unlock(&channel->lock);
			continue;
		}
		pthread_mutex_unlock(&channel->lock);

		ret = tr_channel_connect_ipv4_timeout(
			channel->reconnect_address, port,
			channel->reconnect_connect_timeout_ms, &fd);
		if (ret == TR_OK) {
			ret = tr_reactor_adopt_fd(channel->reactor, fd,
						  &connection);
			if (ret == TR_OK) {
				(void)tr_fd_take(&fd);
				ret = tr_channel_replace_connection(
					channel, lane, connection);
				if (ret != TR_OK)
					(void)tr_reactor_close(connection);
			}
		}
		tr_channel_reconnect_finish_attempt(channel, lane,
						    ret == TR_OK);
	}

	return NULL;
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

int tr_channel_create(const struct tr_channel_config *config,
		      struct tr_conn_handle control_connection,
		      struct tr_conn_handle bulk_connection,
		      tr_stream_data_cb data_cb,
		      tr_stream_event_cb stream_event_cb,
		      tr_channel_event_cb channel_event_cb, void *callback_arg,
		      struct tr_channel **out)
{
	struct tr_channel *channel;
	int protocol_pool_ready = 0;
	int ret;

	if (!out || !control_connection.reactor ||
	    control_connection.reactor != bulk_connection.reactor)
		return TR_ERR_INVALID;

	*out = NULL;
	channel = (struct tr_channel *)calloc(1, sizeof(*channel));
	if (!channel)
		return TR_ERR_NOMEM;

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
		    channel->config.max_protocol_version) {
		free(channel);
		return TR_ERR_INVALID;
	}

	if (channel->config.reassembly_pool &&
	    (channel->config.max_message_bytes == 0 ||
	     channel->config.max_message_bytes >
		     channel->config.reassembly_pool->buffer_size)) {
		free(channel);
		return TR_ERR_INVALID;
	}

	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		if (!tr_conn_equal(control_connection, bulk_connection)) {
			free(channel);
			return TR_ERR_INVALID;
		}
	} else if (tr_conn_equal(control_connection, bulk_connection)) {
		free(channel);
		return TR_ERR_INVALID;
	}

	if (pthread_mutex_init(&channel->lock, NULL) != 0) {
		free(channel);
		return TR_ERR_INVALID;
	}
	if (pthread_cond_init(&channel->reconnect_cond, NULL) != 0) {
		pthread_mutex_destroy(&channel->lock);
		free(channel);
		return TR_ERR_INVALID;
	}
	if (pthread_cond_init(&channel->keepalive_cond, NULL) != 0) {
		pthread_cond_destroy(&channel->reconnect_cond);
		pthread_mutex_destroy(&channel->lock);
		free(channel);
		return TR_ERR_INVALID;
	}

	channel->streams = (struct tr_stream_slot *)calloc(
		channel->config.max_streams, sizeof(*channel->streams));
	if (!channel->streams) {
		pthread_cond_destroy(&channel->keepalive_cond);
		pthread_cond_destroy(&channel->reconnect_cond);
		pthread_mutex_destroy(&channel->lock);
		free(channel);
		return TR_ERR_NOMEM;
	}

	ret = tr_buffer_pool_init(&channel->protocol_pool,
				  TR_CHANNEL_PROTOCOL_BUFFER_COUNT,
				  TR_CHANNEL_PROTOCOL_BUFFER_SIZE);
	if (ret != TR_OK) {
		free(channel->streams);
		pthread_cond_destroy(&channel->keepalive_cond);
		pthread_cond_destroy(&channel->reconnect_cond);
		pthread_mutex_destroy(&channel->lock);
		free(channel);
		return ret;
	}
	protocol_pool_ready = 1;

	channel->reactor = control_connection.reactor;
	channel->control_connection = control_connection;
	channel->bulk_connection = bulk_connection;
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
		goto fail;

	if (!tr_conn_equal(control_connection, bulk_connection)) {
		ret = tr_reactor_set_handler(bulk_connection,
					     tr_channel_on_frame,
					     tr_channel_on_connection_event,
					     channel);
		if (ret != TR_OK) {
			(void)tr_reactor_set_handler(control_connection, NULL,
						     NULL, NULL);
			goto fail;
		}
	}

	ret = tr_channel_send_hello(channel, control_connection);
	if (ret != TR_OK) {
		(void)tr_reactor_set_handler(control_connection, NULL, NULL,
					     NULL);
		if (!tr_conn_equal(control_connection, bulk_connection))
			(void)tr_reactor_set_handler(bulk_connection, NULL,
						     NULL, NULL);
		goto fail;
	}
	if (!tr_conn_equal(control_connection, bulk_connection)) {
		ret = tr_channel_send_hello(channel, bulk_connection);
		if (ret != TR_OK) {
			(void)tr_reactor_set_handler(control_connection, NULL,
						     NULL, NULL);
			(void)tr_reactor_set_handler(bulk_connection, NULL,
						     NULL, NULL);
			goto fail;
		}
	}

	*out = channel;
	return TR_OK;

fail:
	if (protocol_pool_ready)
		tr_buffer_pool_destroy(&channel->protocol_pool);
	free(channel->streams);
	pthread_cond_destroy(&channel->keepalive_cond);
	pthread_cond_destroy(&channel->reconnect_cond);
	pthread_mutex_destroy(&channel->lock);
	free(channel);
	return ret;
}

void tr_channel_destroy(struct tr_channel *channel)
{
	if (!channel)
		return;

	(void)tr_channel_disable_keepalive(channel);
	(void)tr_channel_disable_client_reconnect(channel);

	(void)tr_reactor_set_handler(channel->control_connection, NULL, NULL,
				     NULL);
	if (!tr_conn_equal(channel->control_connection,
			   channel->bulk_connection))
		(void)tr_reactor_set_handler(channel->bulk_connection, NULL,
					     NULL, NULL);

	/*
	 * Stop new reactor callbacks from acquiring this Channel, then wait for
	 * any callback that already copied callback_arg to return.
	 */
	(void)tr_reactor_quiesce(channel->reactor);

	if (channel->streams) {
		uint32_t i;
		for (i = 0; i < channel->config.max_streams; ++i)
			if (channel->streams[i].rx_reassembly)
				tr_buffer_release(
					channel->streams[i].rx_reassembly);
	}
	free(channel->streams);
	tr_buffer_pool_destroy(&channel->protocol_pool);
	pthread_cond_destroy(&channel->keepalive_cond);
	pthread_cond_destroy(&channel->reconnect_cond);
	pthread_mutex_destroy(&channel->lock);
	free(channel);
}

int tr_channel_set_handler(struct tr_channel *channel,
			   tr_stream_data_cb data_cb,
			   tr_stream_event_cb stream_event_cb,
			   tr_channel_event_cb channel_event_cb,
			   void *callback_arg)
{
	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	channel->data_cb = data_cb;
	channel->stream_event_cb = stream_event_cb;
	channel->channel_event_cb = channel_event_cb;
	channel->callback_arg = callback_arg;
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_quiesce(struct tr_channel *channel)
{
	if (!channel)
		return TR_ERR_INVALID;
	return tr_reactor_quiesce(channel->reactor);
}

int tr_channel_replace_connection(struct tr_channel *channel, enum tr_lane lane,
				  struct tr_conn_handle connection)
{
	enum tr_connection_state connection_state;
	int ret;

	if (!channel || !connection.reactor ||
	    connection.reactor != channel->reactor ||
	    (lane != TR_LANE_CONTROL && lane != TR_LANE_BULK))
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (channel->local_draining ||
	    (lane == TR_LANE_CONTROL && channel->control_peer_draining) ||
	    (lane == TR_LANE_BULK && channel->bulk_peer_draining)) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	pthread_mutex_unlock(&channel->lock);

	ret = tr_reactor_set_handler(connection, tr_channel_on_frame,
				     tr_channel_on_connection_event, channel);
	if (ret != TR_OK)
		return ret;

	pthread_mutex_lock(&channel->lock);
	if (channel->config.mode == TR_CHANNEL_SHARED_CONNECTION) {
		if (channel->control_alive || channel->bulk_alive) {
			pthread_mutex_unlock(&channel->lock);
			(void)tr_reactor_set_handler(connection, NULL, NULL,
						     NULL);
			return TR_ERR_STATE;
		}
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
		if (channel->bulk_alive ||
		    tr_conn_equal(connection, channel->control_connection)) {
			pthread_mutex_unlock(&channel->lock);
			(void)tr_reactor_set_handler(connection, NULL, NULL,
						     NULL);
			return TR_ERR_STATE;
		}
		channel->bulk_connection = connection;
		channel->bulk_alive = 1;
		channel->bulk_ready = 0;
		memset(&channel->bulk_caps, 0, sizeof(channel->bulk_caps));
		channel->bulk_reconnecting = 0;
		channel->bulk_reconnect_attempt = 0;
	} else {
		if (channel->control_alive ||
		    tr_conn_equal(connection, channel->bulk_connection)) {
			pthread_mutex_unlock(&channel->lock);
			(void)tr_reactor_set_handler(connection, NULL, NULL,
						     NULL);
			return TR_ERR_STATE;
		}
		channel->control_connection = connection;
		channel->control_alive = 1;
		channel->control_ready = 0;
		memset(&channel->control_caps, 0,
		       sizeof(channel->control_caps));
		channel->control_reconnecting = 0;
		channel->control_reconnect_attempt = 0;
	}
	pthread_cond_broadcast(&channel->reconnect_cond);
	pthread_mutex_unlock(&channel->lock);

	ret = tr_reactor_get_connection_state(connection, &connection_state);
	if (ret != TR_OK || (connection_state != TR_CONN_RESERVED &&
			     connection_state != TR_CONN_ACTIVE)) {
		tr_channel_on_connection_event(connection, TR_CONN_EVENT_ERROR,
					       TR_ERR_CLOSED, channel);
		return ret == TR_OK ? TR_ERR_CLOSED : ret;
	}

	ret = tr_channel_send_hello(channel, connection);
	if (ret != TR_OK) {
		(void)tr_reactor_close(connection);
		return ret;
	}
	return TR_OK;
}

int tr_channel_enable_client_reconnect(
	struct tr_channel *channel,
	const struct tr_channel_reconnect_config *config)
{
	size_t address_len;
	uint32_t initial_delay;
	uint32_t max_delay;
	uint32_t connect_timeout;
	int error;

	if (!channel || !config || !config->ipv4_address ||
	    config->control_port == 0 ||
	    channel->config.role != TR_CHANNEL_CLIENT)
		return TR_ERR_INVALID;

	address_len = strlen(config->ipv4_address);
	if (address_len == 0 ||
	    address_len >= sizeof(channel->reconnect_address))
		return TR_ERR_INVALID;

	initial_delay = config->initial_delay_ms ? config->initial_delay_ms :
						   200U;
	max_delay = config->max_delay_ms ? config->max_delay_ms : 10000U;
	connect_timeout =
		config->connect_timeout_ms ? config->connect_timeout_ms : 5000U;
	if (max_delay < initial_delay)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_CLOSED;
	}
	if (channel->reconnect_thread_started || channel->reconnect_enabled) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}

	memcpy(channel->reconnect_address, config->ipv4_address,
	       address_len + 1U);
	channel->reconnect_control_port = config->control_port;
	channel->reconnect_bulk_port = config->bulk_port ? config->bulk_port :
							   config->control_port;
	channel->reconnect_initial_delay_ms = initial_delay;
	channel->reconnect_max_delay_ms = max_delay;
	channel->reconnect_connect_timeout_ms = connect_timeout;
	channel->reconnect_stop = 0;
	channel->reconnect_enabled = 1;
	channel->control_reconnect_attempt = 0;
	channel->bulk_reconnect_attempt = 0;
	channel->control_reconnecting = 0;
	channel->bulk_reconnecting = 0;

	error = pthread_create(&channel->reconnect_thread, NULL,
			       tr_channel_reconnect_thread_main, channel);
	if (error != 0) {
		channel->reconnect_enabled = 0;
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_SYS;
	}
	channel->reconnect_thread_started = 1;
	pthread_cond_broadcast(&channel->reconnect_cond);
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_disable_client_reconnect(struct tr_channel *channel)
{
	pthread_t thread;
	int join_thread = 0;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	channel->reconnect_enabled = 0;
	channel->reconnect_stop = 1;
	pthread_cond_broadcast(&channel->reconnect_cond);
	if (channel->reconnect_thread_started) {
		thread = channel->reconnect_thread;
		if (pthread_equal(pthread_self(), thread)) {
			/* The maintenance thread will observe reconnect_stop and exit. */
			pthread_mutex_unlock(&channel->lock);
			return TR_OK;
		}
		channel->reconnect_thread_started = 0;
		join_thread = 1;
	}
	pthread_mutex_unlock(&channel->lock);

	if (join_thread && pthread_join(thread, NULL) != 0)
		return TR_ERR_SYS;
	return TR_OK;
}

int tr_channel_enable_keepalive(struct tr_channel *channel,
				const struct tr_channel_keepalive_config *config)
{
	int error;

	if (!channel || !config || config->interval_ms == 0 ||
	    config->timeout_ms == 0)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (channel->keepalive_thread_started || channel->keepalive_enabled) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}

	channel->keepalive_interval_ms = config->interval_ms;
	channel->keepalive_timeout_ms = config->timeout_ms;
	channel->keepalive_stop = 0;
	channel->keepalive_enabled = 1;
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);

	error = pthread_create(&channel->keepalive_thread, NULL,
			       tr_channel_keepalive_thread_main, channel);
	if (error != 0) {
		channel->keepalive_enabled = 0;
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_SYS;
	}
	channel->keepalive_thread_started = 1;
	pthread_cond_broadcast(&channel->keepalive_cond);
	pthread_mutex_unlock(&channel->lock);
	return TR_OK;
}

int tr_channel_disable_keepalive(struct tr_channel *channel)
{
	pthread_t thread;
	int join_thread = 0;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	channel->keepalive_enabled = 0;
	channel->keepalive_stop = 1;
	pthread_cond_broadcast(&channel->keepalive_cond);
	if (channel->keepalive_thread_started) {
		thread = channel->keepalive_thread;
		if (pthread_equal(pthread_self(), thread)) {
			pthread_mutex_unlock(&channel->lock);
			return TR_OK;
		}
		channel->keepalive_thread_started = 0;
		join_thread = 1;
	}
	tr_channel_keepalive_reset_locked(channel, TR_LANE_CONTROL);
	tr_channel_keepalive_reset_locked(channel, TR_LANE_BULK);
	pthread_mutex_unlock(&channel->lock);

	if (join_thread && pthread_join(thread, NULL) != 0)
		return TR_ERR_SYS;
	return TR_OK;
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

int tr_channel_begin_drain(struct tr_channel *channel)
{
	int ret;

	if (!channel)
		return TR_ERR_INVALID;

	(void)tr_channel_disable_client_reconnect(channel);

	pthread_mutex_lock(&channel->lock);
	channel->local_draining = 1;
	pthread_cond_broadcast(&channel->reconnect_cond);
	pthread_mutex_unlock(&channel->lock);

	ret = tr_channel_send_pending_goaway(channel);
	return ret;
}

int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms)
{
	uint64_t deadline_ns = 0;

	if (!channel)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&channel->lock);
	if (!channel->local_draining) {
		pthread_mutex_unlock(&channel->lock);
		return TR_ERR_STATE;
	}
	pthread_mutex_unlock(&channel->lock);

	if (timeout_ms != 0) {
		struct timespec now;
		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
			return TR_ERR_SYS;
		deadline_ns = (uint64_t)now.tv_sec * UINT64_C(1000000000) +
			      (uint64_t)now.tv_nsec +
			      (uint64_t)timeout_ms * UINT64_C(1000000);
	}

	for (;;) {
		struct timespec pause_time;
		uint32_t active;

		pthread_mutex_lock(&channel->lock);
		active = tr_channel_active_streams_locked(channel);
		pthread_mutex_unlock(&channel->lock);
		if (active == 0)
			return TR_OK;
		if (timeout_ms == 0)
			return TR_AGAIN;

		{
			struct timespec now;
			uint64_t now_ns;
			if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
				return TR_ERR_SYS;
			now_ns = (uint64_t)now.tv_sec * UINT64_C(1000000000) +
				 (uint64_t)now.tv_nsec;
			if (now_ns >= deadline_ns)
				return TR_AGAIN;
		}

		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 1000000L;
		nanosleep(&pause_time, NULL);
		(void)tr_channel_send_pending_goaway(channel);
	}
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
		tr_stream_reset_slot(stream);
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
			tr_stream_reset_slot(stream);
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
		int ret = tr_channel_send_pending_goaway(channel);
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
