#define _GNU_SOURCE
#include "tr/socket.h"
#include "tr/status.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stddef.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int tr_set_nonblock_cloexec(int fd)
{
	int flags;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return TR_ERR_SYS;
	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
		return TR_ERR_SYS;

	flags = fcntl(fd, F_GETFD, 0);
	if (flags < 0)
		return TR_ERR_SYS;
	if (fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0)
		return TR_ERR_SYS;

	return TR_OK;
}

static int tr_socket_ipv4(void)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (fd < 0)
		return -1;

	if (tr_set_nonblock_cloexec(fd) != TR_OK) {
		close(fd);
		return -1;
	}

	return fd;
}

int tr_tcp_listen_ipv4(const char *address, uint16_t port, int backlog,
		       int *out_fd, uint16_t *out_bound_port)
{
	struct sockaddr_in addr;
	socklen_t addr_len;
	int fd;
	int one = 1;

	if (!address || !out_fd || backlog <= 0)
		return TR_ERR_INVALID;

	*out_fd = -1;
	fd = tr_socket_ipv4();
	if (fd < 0)
		return TR_ERR_SYS;

	(void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);

	if (inet_pton(AF_INET, address, &addr.sin_addr) != 1) {
		close(fd);
		return TR_ERR_INVALID;
	}

	if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		close(fd);
		return TR_ERR_SYS;
	}

	if (listen(fd, backlog) < 0) {
		close(fd);
		return TR_ERR_SYS;
	}

	if (out_bound_port) {
		addr_len = sizeof(addr);
		if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) < 0) {
			close(fd);
			return TR_ERR_SYS;
		}
		*out_bound_port = ntohs(addr.sin_port);
	}

	*out_fd = fd;
	return TR_OK;
}

int tr_tcp_connect_ipv4(const char *address, uint16_t port, int *out_fd)
{
	struct sockaddr_in addr;
	int fd;

	if (!address || !out_fd)
		return TR_ERR_INVALID;

	*out_fd = -1;
	fd = tr_socket_ipv4();
	if (fd < 0)
		return TR_ERR_SYS;

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);

	if (inet_pton(AF_INET, address, &addr.sin_addr) != 1) {
		close(fd);
		return TR_ERR_INVALID;
	}

	if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
		*out_fd = fd;
		return TR_OK;
	}

	if (errno == EINPROGRESS) {
		*out_fd = fd;
		return TR_IN_PROGRESS;
	}

	close(fd);
	return TR_ERR_SYS;
}

int tr_tcp_finish_connect(int fd)
{
	int error = 0;
	socklen_t error_len = sizeof(error);

	if (fd < 0)
		return TR_ERR_INVALID;

	if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &error_len) < 0)
		return TR_ERR_SYS;

	if (error != 0) {
		errno = error;
		return TR_ERR_SYS;
	}

	return TR_OK;
}

int tr_tcp_accept(int listen_fd, int *out_fd)
{
	int fd;

	if (listen_fd < 0 || !out_fd)
		return TR_ERR_INVALID;

	*out_fd = -1;

#ifdef SOCK_NONBLOCK
	fd = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
	if (fd >= 0) {
		*out_fd = fd;
		return TR_OK;
	}

	if (errno != ENOSYS) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return TR_AGAIN;
		if (errno == EINTR)
			return TR_AGAIN;
		return TR_ERR_SYS;
	}
#endif

	fd = accept(listen_fd, NULL, NULL);
	if (fd < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
			return TR_AGAIN;
		return TR_ERR_SYS;
	}

	if (tr_set_nonblock_cloexec(fd) != TR_OK) {
		close(fd);
		return TR_ERR_SYS;
	}

	*out_fd = fd;
	return TR_OK;
}

void tr_socket_close(int *fd)
{
	if (!fd || *fd < 0)
		return;

	close(*fd);
	*fd = -1;
}
