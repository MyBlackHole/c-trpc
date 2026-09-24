#ifndef TR_CLIENT_H
#define TR_CLIENT_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_client;

struct tr_client_config {
	struct tr_facade_limits limits;

	uint32_t connect_timeout_ms;

	/* 0 表示禁用 Transport keepalive。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;

	/* 可选的 connection-level reconnect；in-flight Call 永远不会透明 replay。 */
	int enable_reconnect;
	uint32_t reconnect_initial_delay_ms;
	uint32_t reconnect_max_delay_ms;
};

void tr_client_config_init(struct tr_client_config *config);

int tr_client_create(const struct tr_client_config *config,
		     struct tr_client **out);

/* V1 facade 仅接受数字 IPv4 address。 */
int tr_client_connect(struct tr_client *client, const char *ipv4_address,
		      uint16_t port);

int tr_client_wait_ready(struct tr_client *client, uint32_t timeout_ms);

int tr_client_register_method(struct tr_client *client,
			      const struct tr_rpc_method_desc *method);

int tr_client_unary_call_ex(struct tr_client *client, uint32_t service_id,
			    uint32_t method_id,
			    const struct tr_rpc_bytes *request,
			    const struct tr_rpc_call_options *options,
			    tr_rpc_unary_result_cb result_cb, void *result_arg,
			    struct tr_rpc_call_handle *out);

int tr_client_unary_call(struct tr_client *client, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out);

int tr_client_call_start_ex(struct tr_client *client, uint32_t service_id,
			    uint32_t method_id,
			    const struct tr_rpc_call_options *options,
			    const struct tr_rpc_call_callbacks *callbacks,
			    struct tr_rpc_call_handle *out);

int tr_client_call_start(struct tr_client *client, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out);

int tr_client_begin_drain(struct tr_client *client);
int tr_client_wait_drained(struct tr_client *client, uint32_t timeout_ms);

int tr_client_get_channel_stats(struct tr_client *client,
				struct tr_channel_stats *out);
int tr_client_get_rpc_stats(struct tr_client *client,
			    struct tr_rpc_endpoint_stats *out);

void tr_client_destroy(struct tr_client *client);

#ifdef __cplusplus
}
#endif

#endif
