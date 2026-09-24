#ifndef TR_SERVER_H
#define TR_SERVER_H

#include <stdint.h>

#include "tr/facade.h"
#include "tr/rpc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct tr_server;

struct tr_server_config {
	struct tr_facade_limits limits;

	/* V1 Server facade 允许同时保留的最大 peer 对象数。 */
	uint32_t max_peers;
	int listen_backlog;

	/* 0 表示禁用 Transport keepalive。 */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;
};

void tr_server_config_init(struct tr_server_config *config);

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out);

/* V1 有意限制只能在 server start 之前注册方法。 */
int tr_server_register_method(struct tr_server *server,
			      const struct tr_rpc_method_desc *method,
			      tr_rpc_unary_handler handler, void *handler_arg);

int tr_server_register_stream_method(
	struct tr_server *server, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

/* V1 facade 仅接受数字 IPv4 address；port=0 表示申请临时端口。 */
int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port);

int tr_server_start(struct tr_server *server);

/* 停止接收新 peer，发送 GOAWAY，并等待已有 Stream 结束。 */
int tr_server_drain(struct tr_server *server, uint32_t timeout_ms);

void tr_server_destroy(struct tr_server *server);

#ifdef __cplusplus
}
#endif

#endif
