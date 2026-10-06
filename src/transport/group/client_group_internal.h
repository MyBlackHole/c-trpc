#ifndef TR_CLIENT_GROUP_INTERNAL_H
#define TR_CLIENT_GROUP_INTERNAL_H

#include <stdint.h>

#include "../../execution/reactor.h"
#include "tr/transport.h"

struct tr_client_group;

struct tr_client_group_config {
	struct tr_reactor *owner;
	uint32_t max_data_connections;
	uint32_t max_transfers;
	uint32_t max_message_bytes;
	uint32_t max_frame_payload_bytes;
	uint32_t connect_timeout_ms;
	int tcp_nodelay;
	tr_connection_group_transfer_ready_cb on_transfer_ready;
	void *callback_arg;
};

/*
 * 正常失败时 *out 保持 NULL。若 constructor rollback 自身失败，返回该
 * lifecycle error，并通过非空 *out 保留 partial Group ownership；调用方
 * 只能继续 tr_client_group_destroy() 重试收敛。
 */
int tr_client_group_create(const struct tr_client_group_config *config,
			   struct tr_client_group **out);
int tr_client_group_destroy(struct tr_client_group *group);

int tr_client_group_connect(
	struct tr_client_group *group, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *id);
int tr_client_group_close(struct tr_client_group *group);
int tr_client_group_begin_drain(struct tr_client_group *group);
int tr_client_group_wait_drained(
	struct tr_client_group *group, uint32_t timeout_ms);
int tr_client_group_get_stats(
	struct tr_client_group *group,
	struct tr_connection_group_client_stats *out);
int tr_client_group_release_transfer(
	struct tr_client_group *group, uint32_t stream_id);
int tr_client_group_send(
	struct tr_client_group *group, uint32_t stream_id,
	uint64_t message_id, const struct tr_transport_bytes *bytes);

#endif
