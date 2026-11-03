#ifndef TR_SOCKET_INTERNAL_H
#define TR_SOCKET_INTERNAL_H

#include <stdint.h>

int tr_tcp_set_nodelay(int fd, int enabled);
int tr_tcp_listen_ipv4_ex(const char *address, uint16_t port, int backlog,
                          int reuse_port, int *out_fd,
                          uint16_t *out_bound_port);

#endif
