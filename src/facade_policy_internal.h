#ifndef TR_FACADE_POLICY_INTERNAL_H
#define TR_FACADE_POLICY_INTERNAL_H

#include "tr/facade.h"

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
