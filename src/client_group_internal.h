#ifndef TR_CLIENT_GROUP_INTERNAL_H
#define TR_CLIENT_GROUP_INTERNAL_H

#include <stdint.h>

#include "tr/reactor.h"
#include "tr/transport.h"

struct tr_client_group;

struct tr_client_group_config {
	struct tr_reactor *owner;
	uint32_t max_data_connections;
	uint32_t connect_timeout_ms;
	int tcp_nodelay;
};

int tr_client_group_create(const struct tr_client_group_config *config,
			   struct tr_client_group **out);
void tr_client_group_destroy(struct tr_client_group *group);

int tr_client_group_connect(
	struct tr_client_group *group, const char *ipv4_address, uint16_t port,
	const struct tr_connection_group_id *id);
int tr_client_group_close(struct tr_client_group *group);

#endif
