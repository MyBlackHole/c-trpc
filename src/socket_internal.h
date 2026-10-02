#ifndef TR_SOCKET_INTERNAL_H
#define TR_SOCKET_INTERNAL_H

#include "tr/facade.h"

int tr_tcp_set_nodelay(int fd, int enabled);
int tr_tcp_listen_ipv4_ex(const char *address, uint16_t port, int backlog,
			  int reuse_port, int *out_fd,
			  uint16_t *out_bound_port);

static inline int
tr_tcp_nodelay_policy_valid(enum tr_tcp_nodelay_policy policy)
{
	return policy == TR_TCP_NODELAY_DEFAULT ||
	       policy == TR_TCP_NODELAY_ENABLED ||
	       policy == TR_TCP_NODELAY_DISABLED;
}

static inline int
tr_tcp_nodelay_policy_enabled(enum tr_tcp_nodelay_policy policy)
{
	return policy != TR_TCP_NODELAY_DISABLED;
}

#endif
