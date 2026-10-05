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
 * Stable semantic Connection Group observations.
 *
 * These structures intentionally exclude Reactor slots, route generations,
 * queue occupancy, parser pools and other implementation diagnostics.
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
 * DATA receive descriptor.
 *
 * bytes is valid for the callback duration. Returning TAKE_OWNERSHIP transfers
 * the underlying RX buffer to the application: copy this descriptor unchanged
 * and release it exactly once with tr_connection_group_message_release().
 * _private is an opaque release capability and must never be inspected or
 * modified by applications. All retained messages must be released before the
 * owning tr_server is destroyed.
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
 * Callbacks execute on the Server's owning I/O domain. They must not block on
 * work that requires that same Server/Reactor to make progress.
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
 * Client-side TRANSFER_READY notification.
 *
 * The callback runs on the Client's owning I/O domain after the exact
 * Stream -> DATA membership affinity has been installed. Applications see only
 * semantic group/stream identity; DATA index/generation remain internal.
 *
 * It must not block on work that requires the same Client/Reactor owner to
 * make progress.
 */
typedef void (*tr_connection_group_transfer_ready_cb)(
	const struct tr_connection_group_id *group, uint32_t stream_id,
	uint64_t message_id, void *arg);

/*
 * Optional Server-side generic Connection Group capability.
 *
 * max_groups == 0 keeps the capability disabled and reserves no extra
 * connection slots. When enabled, all capacities are explicit semantic bounds.
 * The current V1 facade binds the group listener to one internal owner domain;
 * no Reactor/shard handle becomes part of the public contract.
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
 * Optional Client-side DATA-lane budget.
 *
 * max_data_connections == 0 preserves the CONTROL-only behavior: DATA_OFFER
 * is cancelled internally. A non-zero value enables automatic DATA socket
 * establishment up to this semantic bound. Routing/member generations remain
 * internal and do not become application capabilities.
 */
struct tr_connection_group_client_config {
	uint32_t max_data_connections;

	/*
	 * Maximum simultaneously installed Stream -> DATA transfer affinities.
	 * 0 inherits tr_client_config.limits.max_streams when DATA lanes are
	 * enabled, preserving the pre-Phase-7 default. This is a semantic
	 * application-visible inflight bound, not a hash-table capacity knob.
	 */
	uint32_t max_active_transfers;

	/*
	 * Optional READY callback. Affinity state is still installed when this is
	 * NULL, so applications may coordinate Stream identity out of band.
	 */
	tr_connection_group_transfer_ready_cb on_transfer_ready;
	void *callback_arg;
};

void tr_connection_group_client_config_init(
	struct tr_connection_group_client_config *config);

/*
 * Client-side generic Connection Group lifecycle.
 *
 * V1 keeps one active group per tr_client. connect() establishes the CONTROL
 * TCP connection, submits the internal routing identity and transfers socket
 * ownership to the Client's single owner domain. Reactor/shard/member
 * generations remain implementation details.
 *
 * When client config enables DATA lanes, DATA_OFFER is consumed internally:
 * the Client establishes the matching DATA socket on the same owner domain and
 * submits the exact route capability without exposing index/generation.
 * With max_data_connections == 0, offers are cancelled internally.
 */
int tr_client_connection_group_connect(
	struct tr_client *client, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *group);
int tr_client_connection_group_close(struct tr_client *client);

/*
 * Graceful Client Group drain.
 *
 * begin_drain() is a local admission barrier: it stops establishing newly
 * offered DATA lanes and does not install TRANSFER_READY messages observed
 * after the barrier. Transfers already READY before begin_drain() remain usable
 * so application work can finish. The Server owns its transfer affinity
 * independently and may release a late READY it had already issued.
 *
 * wait_drained() returns TR_OK when there are no active transfer affinities,
 * no payload bytes still owned by DATA TX, and no pending DATA establishment.
 * The wait is event-driven from owner-published lifecycle generations; it does
 * not poll owner stats or become a second protocol-state owner.
 * timeout_ms == 0 waits indefinitely. wait_drained() is an external blocking
 * lifecycle wait: Reactor-owner callbacks and RPC worker callbacks receive
 * TR_ERR_STATE. begin_drain() remains callback-safe for publishing drain intent.
 */
int tr_client_connection_group_begin_drain(struct tr_client *client);
int tr_client_connection_group_wait_drained(
	struct tr_client *client, uint32_t timeout_ms);
int tr_client_connection_group_get_stats(
	struct tr_client *client,
	struct tr_connection_group_client_stats *out);

/*
 * Release one Client-side logical transfer affinity after application-level
 * Stream lifetime ends. The Server-side affinity is released independently by
 * tr_server_connection_group_release_transfer().
 */
int tr_client_connection_group_release_transfer(
	struct tr_client *client, uint32_t stream_id);

/*
 * Send one logical DATA message on an already READY Client transfer.
 *
 * bytes is borrowed for the duration of the call only; on TR_OK the facade has
 * copied it into bounded internal send ownership, so the application may reuse
 * or free its memory immediately. TR_AGAIN means bounded send admission or the
 * Reactor TX pool is temporarily full; no application ownership is transferred
 * and the caller may retry later.
 *
 * The exact DATA lane is selected only from the existing stream affinity.
 * DATA index/generation and FIRST/LAST fragmentation remain internal.
 */
int tr_client_connection_group_send(
	struct tr_client *client, uint32_t stream_id, uint64_t message_id,
	const struct tr_transport_bytes *bytes);

/*
 * Group listener lifecycle is owned by tr_server.
 *
 * listen() is configured before tr_server_start(). A Server may start with
 * only this group listener; an RPC listener is not required.
 * stop() stops accepting new groups and closes current soft-state group
 * connections. tr_server_drain()/destroy() also stop this listener.
 */
int tr_server_connection_group_listen(
	struct tr_server *server, const char *ipv4_address, uint16_t port,
	int backlog, uint16_t *out_bound_port);
int tr_server_connection_group_stop(struct tr_server *server);

/*
 * Graceful Server Group drain.
 *
 * begin_drain() stops accepting new Groups and rejects new DATA_OFFER /
 * TRANSFER_READY creation while preserving existing connections and transfer
 * traffic. release_transfer() remains available so active work can quiesce.
 *
 * wait_drained() returns TR_OK after all existing Group connections disappear
 * naturally. The wait is event-driven from owner-published Listener lifecycle
 * generations; it does not poll owner counters or become a second state owner.
 * timeout_ms == 0 waits indefinitely. It is an external blocking lifecycle wait:
 * Reactor-owner callbacks and RPC worker callbacks receive TR_ERR_STATE.
 * begin_drain() remains callback-safe; stop() remains the immediate force-close
 * operation.
 */
int tr_server_connection_group_begin_drain(struct tr_server *server);
int tr_server_connection_group_wait_drained(
	struct tr_server *server, uint32_t timeout_ms);
int tr_server_connection_group_get_stats(
	struct tr_server *server,
	struct tr_connection_group_server_stats *out);

/* CONTROL-plane operations for one already accepted group. */
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
 * Release a message retained by returning
 * TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP from on_message.
 */
int tr_connection_group_message_release(
	struct tr_connection_group_message *message);

#ifdef __cplusplus
}
#endif

#endif
