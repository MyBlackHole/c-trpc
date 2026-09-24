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

/* Logical traffic class. Physical connection mapping is a Channel policy. */
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
	/* V1 reconnect supports numeric IPv4 endpoints. The address is copied. */
	const char *ipv4_address;

	uint16_t control_port;
	/* 0 means control_port. Ignored in shared-connection mode. */
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

	/* Initial receive capacity granted to the peer for each Stream. */
	uint64_t initial_window_bytes;

	/* Absolute WINDOW_UPDATE is emitted after at least this much credit frees. */
	uint64_t window_update_threshold_bytes;

	/*
     * Optional receive-side message reassembly. A logical Stream message may
     * span multiple transport DATA frames. If reassembly_pool is NULL,
     * fragmented receive is rejected and callers must keep each message within
     * one transport frame. When enabled, max_message_bytes must fit in one
     * buffer from reassembly_pool.
     */
	uint32_t max_message_bytes;
	struct tr_buffer_pool *reassembly_pool;

	/* Channel protocol capabilities. Zero versions default to V1 only. */
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
 * The two connection handles must belong to the same reactor. In shared mode
 * bulk_connection may be equal to control_connection; in split mode they must
 * be different live handles.
 *
 * The Channel installs per-connection reactor handlers. Destroy the Channel
 * only after the caller has stopped using it and no callback is concurrently
 * executing (normally after the owning reactor is stopped).
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
 * Replaces upper-layer callbacks. Intended for layering RPC above an existing
 * Channel. Do not destroy the previous callback owner until in-flight callbacks
 * have quiesced.
 */
int tr_channel_set_handler(struct tr_channel *channel,
			   tr_stream_data_cb data_cb,
			   tr_stream_event_cb stream_event_cb,
			   tr_channel_event_cb channel_event_cb,
			   void *callback_arg);

/*
 * Wait until reactor callbacks that may have observed a previous Channel
 * handler have completed. Call after replacing/clearing an upper-layer
 * handler and before freeing the old callback owner.
 */
int tr_channel_quiesce(struct tr_channel *channel);

/*
 * Replaces a failed physical connection while keeping the logical Channel.
 * Existing Streams on the failed lane never survive replacement; callers must
 * open new Streams/Calls. In shared mode replacing either lane replaces both.
 * The new handle must belong to the Channel reactor and must already be owned
 * by that reactor (RESERVED or ACTIVE).
 */
int tr_channel_replace_connection(struct tr_channel *channel, enum tr_lane lane,
				  struct tr_conn_handle connection);

/*
 * Enables V1 client-side automatic reconnect. A small Channel maintenance
 * thread performs connect/backoff work; socket I/O after adoption remains on
 * the owner reactor. In-flight Streams fail immediately on disconnect and are
 * not replayed.
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
 * Returns the outbound limits negotiated for the selected lane after it reaches
 * UP. max_frame_payload_bytes/max_message_bytes describe what the peer has
 * advertised it can receive; local receive bounds remain in Channel/Reactor
 * configuration.
 */
int tr_channel_get_capabilities(struct tr_channel *channel, enum tr_lane lane,
				struct tr_channel_capabilities *out);

/*
 * Graceful shutdown: blocks creation of new local Streams and sends GOAWAY on
 * ready lanes. Existing Streams continue until naturally closed/cancelled.
 * The operation is idempotent; TR_AGAIN means a GOAWAY control frame could not
 * yet be queued and the caller may retry while the Channel remains draining.
 */
int tr_channel_begin_drain(struct tr_channel *channel);
int tr_channel_wait_drained(struct tr_channel *channel, uint32_t timeout_ms);
int tr_channel_get_state(struct tr_channel *channel,
			 enum tr_channel_state *out);
uint32_t tr_channel_active_streams(struct tr_channel *channel);

/* Opens a logical byte-stream. Completion is reported by OPENED. */
int tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
		   struct tr_stream_handle *out);

/*
 * One write is one logical Stream message. Transport fragments it into DATA
 * frames when it exceeds the reactor frame-payload limit. On TR_OK payload
 * ownership transfers to the reactor until every fragment has been sent.
 * Otherwise the caller retains ownership. TR_AGAIN means flow-control or
 * reactor backpressure.
 */
int tr_stream_send(struct tr_stream_handle stream, struct tr_buffer *payload);

/*
 * Scatter/gather Stream write. Flow control accounts for the sum of buffer
 * lengths. On TR_OK ownership of every buffer transfers to Transport.
 */
int tr_stream_sendv(struct tr_stream_handle stream,
		    struct tr_buffer *const *payloads, uint32_t payload_count);

/* Half-closes the local sending direction. */
int tr_stream_close(struct tr_stream_handle stream);

/*
 * Releases a payload previously taken by TR_STREAM_DATA_TAKE_OWNERSHIP and
 * returns its bytes to the Stream receive window.
 */
int tr_stream_release_payload(struct tr_stream_handle stream,
			      struct tr_buffer *payload);

/* Retries pending absolute WINDOW_UPDATE control frames. */
int tr_channel_flush(struct tr_channel *channel);

/* Snapshot of byte-based flow-control state for diagnostics/tests. */
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
