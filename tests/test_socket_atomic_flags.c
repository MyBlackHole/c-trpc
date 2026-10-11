#define _GNU_SOURCE
#include "io/socket.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

int __real_socket(int domain, int type, int protocol);
static unsigned int checked;

/* 在系统调用返回边界检查，避免库内随后执行的 fcntl 掩盖继承窗口。 */
int __wrap_socket(int domain, int type, int protocol)
{
	int fd = __real_socket(domain, type, protocol);
	char number[32];
	pid_t child;
	int status;

	if (fd < 0)
		return fd;
	snprintf(number, sizeof(number), "%d", fd);
	child = fork();
	assert(child >= 0);
	if (child == 0) {
		execl("/proc/self/exe", "test_socket_atomic_flags", "child",
		      number, (char *)NULL);
		_exit(111);
	}
	assert(waitpid(child, &status, 0) == child);
	assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	assert((fcntl(fd, F_GETFD) & FD_CLOEXEC) != 0);
	assert((fcntl(fd, F_GETFL) & O_NONBLOCK) != 0);
	checked++;
	return fd;
}

int main(int argc, char **argv)
{
	int listener = -1;
	int client = -1;
	int ret;
	uint16_t port;

	if (argc == 3) {
		errno = 0;
		return fcntl(atoi(argv[2]), F_GETFD) == -1 && errno == EBADF
		               ? 0
		               : 42;
	}
	assert(tr_tcp_listen_ipv4("127.0.0.1", 0, 4, &listener, &port) ==
	       TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, &client);
	assert(ret == TR_OK || ret == TR_IN_PROGRESS);
	assert(checked == 2);
	tr_socket_close(&client);
	tr_socket_close(&listener);
	puts("listen/connect sockets: atomic nonblocking and no exec "
	     "inheritance");
	return 0;
}
