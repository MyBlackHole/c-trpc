#ifndef TR_CLIENT_H
#define TR_CLIENT_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"
#include "tr/transport.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_client;

struct tr_client_config {
	struct tr_facade_limits limits;
	struct tr_connection_group_client_config connection_groups;

	uint32_t connect_timeout_ms;

	/*
	 * DEFAULT/ENABLED 在首次连接及自动 reconnect 的 TCP socket 上启用
	 * TCP_NODELAY；DISABLED 保留内核默认 Nagle 行为。
	 */
	enum tr_tcp_nodelay_policy tcp_nodelay;

	/* 0 表示禁用 Transport keepalive。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;

	/* RPC Call-level owner interceptor；fn==NULL 表示禁用。 */
	struct tr_rpc_interceptor interceptor;

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

/* Stable RPC semantic lifecycle snapshot for this Client. */
int tr_client_get_rpc_semantic_stats(
	struct tr_client *client, struct tr_rpc_semantic_stats *out);

/*
 * Exclusive terminal operation.
 *
 * The caller must own the final external Client lifetime: no concurrent public
 * API call may still use client. Do not call destroy from c-trpc RPC
 * handlers/result callbacks/interceptors or Transport callbacks running on this
 * Client's worker/Reactor. Unsafe self-destroy is refused; use drain/shutdown
 * signaling from the callback and perform destroy later from an external
 * owner thread.
 */
void tr_client_destroy(struct tr_client *client);

#ifdef __cplusplus
}
#endif

#endif
