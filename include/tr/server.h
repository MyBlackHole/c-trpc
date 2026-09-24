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

	/* Maximum accepted peer objects retained by the V1 server facade. */
	uint32_t max_peers;
	int listen_backlog;

	/* 0 disables Transport keepalive. */
	uint32_t keepalive_interval_ms;
	uint32_t keepalive_timeout_ms;
};

void tr_server_config_init(struct tr_server_config *config);

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out);

/* Registration is intentionally restricted to the pre-start phase in V1. */
int tr_server_register_method(struct tr_server *server,
			      const struct tr_rpc_method_desc *method,
			      tr_rpc_unary_handler handler, void *handler_arg);

int tr_server_register_stream_method(
	struct tr_server *server, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

/* V1 facade accepts a numeric IPv4 address. port=0 requests an ephemeral port. */
int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port);

int tr_server_start(struct tr_server *server);

/* Stops accepting new peers, sends GOAWAY and waits for existing Streams. */
int tr_server_drain(struct tr_server *server, uint32_t timeout_ms);

void tr_server_destroy(struct tr_server *server);

#ifdef __cplusplus
}
#endif

#endif
