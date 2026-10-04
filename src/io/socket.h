#ifndef TR_SOCKET_H
#define TR_SOCKET_H

#include <stdint.h>

#include "tr/cleanup.h"

#ifdef __cplusplus
extern "C" {
#endif

int tr_tcp_listen_ipv4(const char *address, uint16_t port, int backlog,
		       int *out_fd, uint16_t *out_bound_port);

/* 返回 TR_OK 或 TR_IN_PROGRESS；成功拿到的 fd 由调用方拥有。 */
int tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out_fd);

int tr_tcp_finish_connect(int fd);
int tr_tcp_accept(int listen_fd, int *out_fd);
void tr_socket_close(int *fd);

/* 作用域自动管理的 fd；-1 表示已经 disarm，不再拥有资源。 */
static inline void tr_fd_cleanup(int *fd)
{
	tr_socket_close(fd);
}

static inline int tr_fd_take(int *fd)
{
	int value = -1;

	if (fd) {
		value = *fd;
		*fd = -1;
	}
	return value;
}

#ifdef __cplusplus
}
#endif

#endif
