#ifndef TR_CLIENT_GROUP_INTERNAL_H
#define TR_CLIENT_GROUP_INTERNAL_H

#include <stdint.h>

#include "../../execution/reactor.h"
#include "../../runtime/message_service.h"
#include "tr/transport.h"

struct tr_memory_budget;

struct tr_client_group;
struct tr_tls_openssl_profile;

struct tr_client_group_config {
	struct tr_reactor *owner;
	struct tr_tls_openssl_profile *tls_profile;
	uint32_t max_data_connections;
	uint32_t max_transfers;
	uint32_t max_message_bytes;
	uint32_t send_compression_algorithms;
	uint32_t max_encoded_message_bytes;
	uint32_t max_frame_payload_bytes;
	uint32_t connect_timeout_ms;
	int tcp_nodelay;
	int (*message_submit)(void *context, struct tr_message *input,
	                      const struct tr_message_transform_config *config,
	                      tr_message_service_done_fn done, void *arg,
	                      struct tr_task_handle *handle);
	int (*message_cancel)(void *context, struct tr_task_handle handle);
	void *message_context;
	struct tr_memory_budget *message_budget;
	tr_connection_group_transfer_ready_cb on_transfer_ready;
	void *callback_arg;
};

/* 正常失败时 *out 保持 NULL。若构造回滚自身失败，返回该
 * 生命周期错误，并通过非空 *out 保留部分构造的 Group 所有权；调用方
 * 只能继续 tr_client_group_destroy() 重试收敛。
 */
int tr_client_group_create(const struct tr_client_group_config *config,
			   struct tr_client_group **out);
int tr_client_group_destroy(struct tr_client_group *group);
int tr_client_group_schedule_tls_profile(
        struct tr_client_group *group, struct tr_tls_openssl_profile *profile);

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
int tr_client_group_send_with_compression(
	struct tr_client_group *group, uint32_t stream_id,
	uint64_t message_id, const struct tr_transport_bytes *bytes,
	enum tr_connection_group_compression compression);

#endif
