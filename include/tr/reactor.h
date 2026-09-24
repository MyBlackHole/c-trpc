#ifndef TR_REACTOR_H
#define TR_REACTOR_H

#include <stdint.h>

#include "tr/buffer.h"
#include "tr/frame.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_reactor;

struct tr_reactor_limits {
	uint32_t max_payload_len;
};

#define TR_REACTOR_MAX_TX_SLICES 4U

struct tr_conn_handle {
	struct tr_reactor *reactor;
	uint32_t slot;
	uint32_t generation;
};

enum tr_connection_state {
	TR_CONN_FREE = 0,
	TR_CONN_RESERVED,
	TR_CONN_ACTIVE,
	TR_CONN_CLOSED,
	TR_CONN_ERROR
};

struct tr_connection_stats {
	enum tr_connection_state state;

	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t rx_frames;
	uint64_t tx_frames;
	uint64_t recv_eagain;
	uint64_t send_eagain;
	uint64_t rx_pauses;

	uint64_t last_rx_activity_ns;
	uint64_t last_tx_activity_ns;

	uint32_t tx_queued_items;
	int rx_paused;
	int tx_wait_writable;
};

enum tr_frame_disposition { TR_FRAME_RELEASE = 0, TR_FRAME_TAKE_OWNERSHIP = 1 };

enum tr_connection_event { TR_CONN_EVENT_CLOSED = 1, TR_CONN_EVENT_ERROR = 2 };

typedef enum tr_frame_disposition (*tr_reactor_frame_cb)(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg);

typedef void (*tr_reactor_event_cb)(struct tr_conn_handle connection,
				    enum tr_connection_event event, int status,
				    void *arg);

struct tr_reactor_config {
	uint32_t max_connections;
	uint32_t command_capacity;
	uint32_t tx_item_capacity;
	uint32_t control_tx_item_capacity;

	uint32_t rx_buffer_count;
	uint32_t rx_buffer_size;
	uint32_t max_payload_len;

	uint32_t rx_budget_bytes;
	uint32_t tx_budget_bytes;
};

int tr_reactor_create(const struct tr_reactor_config *config,
		      tr_reactor_frame_cb frame_cb,
		      tr_reactor_event_cb event_cb, void *callback_arg,
		      struct tr_reactor **out);
int tr_reactor_start(struct tr_reactor *reactor);

/* On TR_OK the reactor owns fd. */
int tr_reactor_adopt_fd(struct tr_reactor *reactor, int fd,
			struct tr_conn_handle *out);

/*
 * On TR_OK payload ownership transfers to the reactor. On any other return
 * value ownership remains with the caller. A NULL payload is valid.
 */
int tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
		    uint32_t flags, uint32_t stream_id, uint64_t message_id,
		    struct tr_buffer *payload);

/*
 * Scatter/gather variant used by upper-layer zero/copy-minimal fast paths.
 * On TR_OK ownership of every buffer transfers to the reactor. On failure the
 * caller retains all buffers. The same buffer pointer must not appear twice.
 */
int tr_reactor_sendv(struct tr_conn_handle connection, uint16_t type,
		     uint32_t flags, uint32_t stream_id, uint64_t message_id,
		     struct tr_buffer *const *payloads, uint32_t payload_count);

/*
 * Variant with an explicit per-message DATA frame payload ceiling. This is
 * used after Channel capability negotiation. max_frame_payload_len must be
 * nonzero and no larger than the owning reactor's configured limit.
 */
int tr_reactor_sendv_limited(struct tr_conn_handle connection, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id,
			     struct tr_buffer *const *payloads,
			     uint32_t payload_count,
			     uint32_t max_frame_payload_len);

/*
 * Replaces the callbacks used for one live connection. This is intended for
 * higher layers such as Channel. Passing NULL callbacks disables that
 * callback for the connection. The callback argument is copied atomically
 * with the callback pointers under the reactor slot lock.
 */
int tr_reactor_set_handler(struct tr_conn_handle connection,
			   tr_reactor_frame_cb frame_cb,
			   tr_reactor_event_cb event_cb, void *callback_arg);

/* Snapshot current slot state for replacement/diagnostics. */
int tr_reactor_get_connection_state(struct tr_conn_handle connection,
				    enum tr_connection_state *out);

/* Lock-free snapshot of hot-path connection counters for diagnostics. */
int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out);

/* Snapshot immutable reactor wire limits for upper-layer negotiation. */
int tr_reactor_get_limits(struct tr_reactor *reactor,
			  struct tr_reactor_limits *out);

int tr_reactor_resume_rx(struct tr_conn_handle connection);
int tr_reactor_close(struct tr_conn_handle connection);
/* Fails a live connection and reports status through the connection callback. */
int tr_reactor_abort(struct tr_conn_handle connection, int status);

int tr_reactor_stop(struct tr_reactor *reactor);
void tr_reactor_destroy(struct tr_reactor *reactor);

#ifdef __cplusplus
}
#endif

#endif
