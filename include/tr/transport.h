#ifndef TR_TRANSPORT_H
#define TR_TRANSPORT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct tr_server;

struct tr_connection_group_id {
	uint64_t group_id;
	uint64_t epoch;
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
 * modified by applications.
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
	uint32_t control_message_count;

	tr_connection_group_authorize_cb authorize;
	tr_connection_group_message_cb on_message;
	tr_connection_group_data_event_cb on_data_event;
	void *callback_arg;
};

void tr_connection_group_server_config_init(
	struct tr_connection_group_server_config *config);

/*
 * Group listener lifecycle is owned by tr_server.
 *
 * listen() is configured before tr_server_start(), like tr_server_listen().
 * stop() stops accepting new groups and closes current soft-state group
 * connections. tr_server_drain()/destroy() also stop this listener.
 */
int tr_server_connection_group_listen(
	struct tr_server *server, const char *ipv4_address, uint16_t port,
	int backlog, uint16_t *out_bound_port);
int tr_server_connection_group_stop(struct tr_server *server);

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
