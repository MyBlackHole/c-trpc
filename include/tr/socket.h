#ifndef TR_SOCKET_H
#define TR_SOCKET_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int tr_tcp_listen_ipv4(const char *address, uint16_t port, int backlog,
		       int *out_fd, uint16_t *out_bound_port);

/* Returns TR_OK or TR_IN_PROGRESS. */
int tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out_fd);

int tr_tcp_finish_connect(int fd);
int tr_tcp_accept(int listen_fd, int *out_fd);
void tr_socket_close(int *fd);

#ifdef __cplusplus
}
#endif

#endif
