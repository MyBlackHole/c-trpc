#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <pthread.h>
#include <spawn.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../src/io/byte_endpoint.h"
#include "../src/io/socket.h"
#include "../src/io/tls_openssl.h"
#include "../src/memory_budget.h"
#include "tls_test_identity.h"

extern char **environ;

union ledger_header {
	max_align_t alignment;
	struct {
		size_t bytes;
		uint64_t magic;
	} value;
};
#define LEDGER_MAGIC UINT64_C(0x746c736d656d6f72)
#define PAIRS_MAX 16U
#define PAYLOAD_BYTES (64U * 1024U)
#define CHUNK_BYTES 16384U
#define FAULT_PROFILE_METHOD UINT32_MAX
#define FAULT_SESSION_WRAPPER (UINT32_MAX - 1U)
#define FAULT_BIO_USERDATA (UINT32_MAX - 2U)
#define PROFILE_CHURN (UINT32_MAX - 3U)

/* 备注：只有独立 exec 子进程安装钩子。头部保证 malloc 对齐，账本只计
 * OpenSSL 请求字节；头部、libc 元数据、RSS 和内核缓冲分别处理。 */
static pthread_mutex_t ledger_lock = PTHREAD_MUTEX_INITIALIZER;
static struct {
	size_t live_bytes, peak_bytes, live_blocks;
	uint64_t attempts, fail_at, failures;
} ledger;
static unsigned fail_next_calloc, fail_next_method, library_failures;
void *__real_calloc(size_t count, size_t size);
BIO_METHOD *__real_BIO_meth_new(int type, const char *name);
void *__wrap_calloc(size_t count, size_t size)
{
	if (fail_next_calloc && --fail_next_calloc == 0U) {
		++library_failures;
		return NULL;
	}
	return __real_calloc(count, size);
}
BIO_METHOD *__wrap_BIO_meth_new(int type, const char *name)
{
	if (fail_next_method) {
		fail_next_method = 0U;
		++library_failures;
		return NULL;
	}
	return __real_BIO_meth_new(type, name);
}

static int ledger_attempt(void)
{
	assert(pthread_mutex_lock(&ledger_lock) == 0);
	assert(ledger.attempts != UINT64_MAX);
	++ledger.attempts;
	int fail = ledger.fail_at && ledger.attempts == ledger.fail_at;
	ledger.failures += (unsigned)fail;
	assert(pthread_mutex_unlock(&ledger_lock) == 0);
	return fail;
}
static void ledger_resize(size_t old_size, size_t new_size, int delta)
{
	assert(pthread_mutex_lock(&ledger_lock) == 0);
	assert(ledger.live_bytes >= old_size);
	assert(new_size <= SIZE_MAX - (ledger.live_bytes - old_size));
	ledger.live_bytes = ledger.live_bytes - old_size + new_size;
	if (delta > 0) {
		assert(ledger.live_blocks != SIZE_MAX);
		++ledger.live_blocks;
	} else if (delta < 0) {
		assert(ledger.live_blocks);
		--ledger.live_blocks;
	}
	if (ledger.live_bytes > ledger.peak_bytes)
		ledger.peak_bytes = ledger.live_bytes;
	assert(pthread_mutex_unlock(&ledger_lock) == 0);
}
static void *ledger_malloc(size_t size, const char *file, int line)
{
	union ledger_header *header;
	(void)file;
	(void)line;
	if (size > SIZE_MAX - sizeof(*header) || ledger_attempt())
		return NULL;
	header = malloc(sizeof(*header) + size);
	if (!header)
		return NULL;
	header->value.bytes = size;
	header->value.magic = LEDGER_MAGIC;
	ledger_resize(0, size, 1);
	return header + 1;
}
static void ledger_free(void *pointer, const char *file, int line)
{
	(void)file;
	(void)line;
	if (!pointer)
		return;
	union ledger_header *header = (union ledger_header *)pointer - 1;
	assert(header->value.magic == LEDGER_MAGIC);
	size_t size = header->value.bytes;
	header->value.magic = 0;
	free(header);
	ledger_resize(size, 0, -1);
}
static void *ledger_realloc(void *pointer, size_t size, const char *file,
                            int line)
{
	if (!pointer)
		return ledger_malloc(size, file, line);
	if (!size) {
		ledger_free(pointer, file, line);
		return NULL;
	}
	union ledger_header *header = (union ledger_header *)pointer - 1;
	assert(header->value.magic == LEDGER_MAGIC);
	size_t old_size = header->value.bytes;
	if (size > SIZE_MAX - sizeof(*header) || ledger_attempt())
		return NULL;
	union ledger_header *next = realloc(header, sizeof(*header) + size);
	if (!next)
		return NULL;
	next->value.bytes = size;
	ledger_resize(old_size, size, 0);
	return next + 1;
}
static void ledger_selftest(void)
{
	void *pointer = OPENSSL_malloc(17U);
	assert(pointer && (uintptr_t)pointer % _Alignof(max_align_t) == 0U);
	assert(ledger.live_bytes == 17U && ledger.live_blocks == 1U);
	pointer = OPENSSL_realloc(pointer, 65U);
	assert(pointer && ledger.live_bytes == 65U && ledger.live_blocks == 1U);
	ledger.fail_at = ledger.attempts + 1U;
	assert(ledger_realloc(pointer, 129U, __FILE__, __LINE__) == NULL);
	ledger.fail_at = 0U;
	assert(ledger.live_bytes == 65U && ledger.live_blocks == 1U);
	OPENSSL_clear_free(pointer, 65U);
	assert(!ledger.live_bytes && !ledger.live_blocks);
	ledger.failures = 0U;
}
static uint64_t now_ns(void)
{
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}
static void tcp_pair(int fds[2])
{
	int listener = -1;
	uint16_t port;
	assert(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) ==
	       TR_OK);
	int ret = tr_tcp_connect_ipv4("127.0.0.1", port, &fds[0]);
	assert(ret == TR_OK || ret == TR_IN_PROGRESS);
	if (ret == TR_IN_PROGRESS) {
		struct pollfd pending = {.fd = fds[0], .events = POLLOUT};
		assert(poll(&pending, 1, 5000) == 1);
		assert(tr_tcp_finish_connect(fds[0]) == TR_OK);
	}
	while ((ret = tr_tcp_accept(listener, &fds[1])) == TR_AGAIN) {
		struct pollfd pending = {.fd = listener, .events = POLLIN};
		assert(poll(&pending, 1, 5000) == 1);
	}
	assert(ret == TR_OK && close(listener) == 0);
}
struct connection {
	int fd;
	struct tr_tls_openssl_session *session;
	int authenticated;
	size_t sent, received;
	uint8_t input[CHUNK_BYTES];
};
static size_t socket_capacity(int fd)
{
	int send_size, receive_size;
	socklen_t length = sizeof(int);
	assert(getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_size, &length) == 0);
	length = sizeof(int);
	assert(getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &receive_size, &length) ==
	       0);
	assert(send_size > 0 && receive_size > 0);
	return (size_t)send_size + (size_t)receive_size;
}
static void transfer(struct connection *connections, unsigned count)
{
	static uint8_t payload[CHUNK_BYTES];
	const struct tr_byte_endpoint_driver *driver = tr_tls_openssl_driver();
	struct pollfd pending[2U * PAIRS_MAX];
	uint64_t deadline = now_ns() + UINT64_C(10000000000);
	memset(payload, 0x59, sizeof(payload));
	for (;;) {
		unsigned done = 0;
		int progress = 0;
		for (unsigned i = 0; i < count; ++i) {
			struct connection *c = &connections[i];
			uint32_t wait = 0U, events = 0U;
			int ret;
			if (!c->authenticated) {
				ret = driver->handshake(c->session, &wait);
				assert(ret == TR_OK || ret == TR_AGAIN);
				if (ret == TR_OK) {
					c->authenticated = 1;
					progress = 1;
				} else
					events |= wait;
			}
			if (c->authenticated && c->sent < PAYLOAD_BYTES) {
				size_t written = 0U;
				ret = driver->write(c->session, payload,
				                    sizeof(payload), &written,
				                    &wait);
				assert(ret == TR_OK || ret == TR_AGAIN);
				if (ret == TR_OK) {
					assert(written &&
					       written <= sizeof(payload));
					c->sent += written;
					assert(c->sent <= PAYLOAD_BYTES);
					progress = 1;
				} else
					events |= wait;
			}
			if (c->authenticated && c->received < PAYLOAD_BYTES) {
				size_t received = 0U;
				ret = driver->read(c->session, c->input,
				                   sizeof(c->input), &received,
				                   &wait);
				assert(ret == TR_OK || ret == TR_AGAIN);
				if (ret == TR_OK) {
					assert(received &&
					       received <= sizeof(c->input));
					for (size_t j = 0; j < received; ++j)
						assert(c->input[j] == 0x59);
					c->received += received;
					assert(c->received <= PAYLOAD_BYTES);
					progress = 1;
				} else
					events |= wait;
			}
			done += c->sent == PAYLOAD_BYTES &&
			        c->received == PAYLOAD_BYTES;
			pending[i] = (struct pollfd){
			        .fd = c->fd,
			        .events =
			                (short)(((events &
			                          TR_BYTE_ENDPOINT_EVENT_READ)
			                                 ? POLLIN
			                                 : 0) |
			                        ((events &
			                          TR_BYTE_ENDPOINT_EVENT_WRITE)
			                                 ? POLLOUT
			                                 : 0))};
		}
		if (done == count)
			return;
		assert(now_ns() < deadline);
		if (!progress) {
			int ret = poll(pending, count, 10);
			assert(ret >= 0 || errno == EINTR);
		}
	}
}
static int make_client_profile(char **argv,
                               struct tr_tls_openssl_profile **client)
{
	struct tr_tls_client_config cc = {.mode = TR_TLS_REQUIRED,
	                                  .ca_file = argv[4],
	                                  .expected_peer_name =
	                                          "server.example.test",
	                                  .client_certificate_file = argv[7],
	                                  .client_private_key_file = argv[8]};
	return tr_tls_openssl_client_profile_create(&cc, client);
}
static void make_profiles(char **argv, struct tr_tls_openssl_profile **client,
                          struct tr_tls_openssl_profile **server)
{
	struct tr_tls_server_config sc = {.mode = TR_TLS_REQUIRED,
	                                  .certificate_file = argv[5],
	                                  .private_key_file = argv[6],
	                                  .client_ca_file = argv[4],
	                                  .client_auth =
	                                          TR_TLS_CLIENT_AUTH_REQUIRED};
	assert(make_client_profile(argv, client) == TR_OK);
	assert(tr_tls_openssl_server_profile_create(&sc, server) == TR_OK);
}
static int run_child(unsigned pairs, unsigned fail_at, char **argv)
{
	struct tr_tls_openssl_profile *client = NULL, *server = NULL;
	struct tr_memory_budget budget;
	const struct tr_byte_endpoint_driver *driver = tr_tls_openssl_driver();
	assert(pairs && pairs <= PAIRS_MAX);
	assert(CRYPTO_set_mem_functions(ledger_malloc, ledger_realloc,
	                                ledger_free) == 1);
	ledger_selftest();
	assert(OPENSSL_init_ssl(OPENSSL_INIT_NO_ATEXIT, NULL) == 1);
	if (fail_at == PROFILE_CHURN) {
		/* 类型编号属于 BIO 类型而非 profile，不得随凭证轮换耗尽。 */
		for (unsigned i = 0; i < 160U; ++i) {
			assert(make_client_profile(argv, &client) == TR_OK);
			tr_tls_openssl_profile_release(client);
		}
		ERR_clear_error();
		OPENSSL_cleanup();
		printf("tls-ledger: phase=profile-churn profiles=160 "
		       "retained_bytes=%zu retained_blocks=%zu\n",
		       ledger.live_bytes, ledger.live_blocks);
		assert(!ledger.live_bytes && !ledger.live_blocks);
		return 0;
	}
	if (fail_at == FAULT_PROFILE_METHOD) {
		fail_next_method = 1U;
		assert(make_client_profile(argv, &client) == TR_ERR_NOMEM);
		assert(client == NULL && library_failures == 1U);
		assert(make_client_profile(argv, &client) == TR_OK);
		tr_tls_openssl_profile_release(client);
		ERR_clear_error();
		OPENSSL_cleanup();
		printf("tls-ledger: phase=profile-method-failure "
		       "retained_bytes=%zu retained_blocks=%zu\n",
		       ledger.live_bytes, ledger.live_blocks);
		assert(!ledger.live_bytes && !ledger.live_blocks);
		return 0;
	}
	make_profiles(argv, &client, &server);
	tr_memory_budget_init(&budget, UINT64_C(1048576));
	size_t profile_bytes = ledger.live_bytes;
	ledger.peak_bytes = profile_bytes;
	size_t socket_bytes = 0U;
	if (fail_at) {
		int fds[2];
		struct tr_tls_openssl_session *session = NULL;
		tcp_pair(fds);
		if (fail_at == FAULT_SESSION_WRAPPER)
			fail_next_calloc = 1U;
		else if (fail_at == FAULT_BIO_USERDATA)
			fail_next_calloc = 2U;
		else
			ledger.fail_at = ledger.attempts + fail_at;
		int ret = tr_tls_openssl_session_create(client, fds[0], &budget,
		                                        &session);
		ledger.fail_at = 0U;
		assert(fail_at == FAULT_SESSION_WRAPPER ||
		                       fail_at == FAULT_BIO_USERDATA
		               ? library_failures == 1U
		               : ledger.failures == 1U);
		if (ret == TR_OK) {
			assert(session);
			driver->release(session);
		} else
			assert(ret == TR_ERR_NOMEM && session == NULL);
		assert(fcntl(fds[0], F_GETFD) >= 0 &&
		       atomic_load(&budget.current_bytes) == 0U);
		/* 备注：一次失败不允许污染后续会话构造。 */
		session = NULL;
		assert(tr_tls_openssl_session_create(client, fds[0], &budget,
		                                     &session) == TR_OK);
		driver->release(session);
		assert(close(fds[0]) == 0 && close(fds[1]) == 0);
	} else {
		struct connection connections[2U * PAIRS_MAX] = {0};
		for (unsigned i = 0; i < pairs; ++i) {
			int fds[2];
			tcp_pair(fds);
			for (unsigned j = 0; j < 2U; ++j) {
				struct connection *c = &connections[2U * i + j];
				int size = 4096;
				c->fd = fds[j];
				assert(setsockopt(c->fd, SOL_SOCKET, SO_SNDBUF,
				                  &size, sizeof(size)) == 0);
				assert(tr_tls_openssl_session_create(
				               j ? server : client, c->fd,
				               &budget, &c->session) == TR_OK);
				socket_bytes += socket_capacity(c->fd);
			}
		}
		/* 备注：先归还调用方的 profile 引用，活动 SSL/BIO 仍须有效。 */
		tr_tls_openssl_profile_release(client);
		tr_tls_openssl_profile_release(server);
		client = server = NULL;
		transfer(connections, 2U * pairs);
		for (unsigned i = 0; i < 2U * pairs; ++i) {
			driver->release(connections[i].session);
			assert(fcntl(connections[i].fd, F_GETFD) >= 0 &&
			       close(connections[i].fd) == 0);
		}
	}
	assert(atomic_load(&budget.current_bytes) == 0U);
	size_t peak_bytes = ledger.peak_bytes;
	tr_tls_openssl_profile_release(client);
	tr_tls_openssl_profile_release(server);
	ERR_clear_error();
	OPENSSL_cleanup();
	const char *fault_case =
	        fail_at == FAULT_SESSION_WRAPPER ? "session_wrapper"
	        : fail_at == FAULT_BIO_USERDATA  ? "bio_userdata"
	        : fail_at                        ? "openssl_allocation"
	                                         : "none";
	printf("tls-ledger: pairs=%u fault_case=%s fail_at=%u "
	       "profile_bytes=%zu "
	       "peak_bytes=%zu retained_bytes=%zu "
	       "retained_blocks=%zu socket_capacity_bytes=%zu "
	       "managed_peak_bytes=%" PRIu64 " alloc_attempts=%" PRIu64
	       " failures=%" PRIu64 " library_failures=%u\n",
	       pairs, fault_case, fail_at >= FAULT_BIO_USERDATA ? 0U : fail_at,
	       profile_bytes, peak_bytes, ledger.live_bytes, ledger.live_blocks,
	       socket_bytes, atomic_load(&budget.peak_bytes), ledger.attempts,
	       ledger.failures, library_failures);
	fflush(stdout);
	assert(!ledger.live_bytes && !ledger.live_blocks);
	return 0;
}
int main(int argc, char **argv)
{
	if (argc == 9 && !strcmp(argv[1], "--child"))
		return run_child((unsigned)strtoul(argv[2], NULL, 10),
		                 (unsigned)strtoul(argv[3], NULL, 10), argv);
	assert(argc == 1);
	struct tls_test_identity identity;
	assert(tls_test_identity_init(&identity, "tls-resource-ledger") == 0);
	const unsigned scenarios[][2] = {{1, 0},
	                                 {4, 0},
	                                 {16, 0},
	                                 {1, 1},
	                                 {1, 2},
	                                 {1, 3},
	                                 {1, 4},
	                                 {1, 8},
	                                 {1, FAULT_SESSION_WRAPPER},
	                                 {1, FAULT_BIO_USERDATA},
	                                 {1, FAULT_PROFILE_METHOD},
	                                 {1, PROFILE_CHURN}};
	int failed = 0;
	for (unsigned i = 0; i < sizeof(scenarios) / sizeof(scenarios[0]);
	     ++i) {
		char pairs[16], fault[16];
		assert(snprintf(pairs, sizeof(pairs), "%u", scenarios[i][0]) >
		       0);
		assert(snprintf(fault, sizeof(fault), "%u", scenarios[i][1]) >
		       0);
		char *arguments[] = {argv[0],
		                     "--child",
		                     pairs,
		                     fault,
		                     identity.ca_file,
		                     identity.server_certificate_file,
		                     identity.server_private_key_file,
		                     identity.client_certificate_file,
		                     identity.client_private_key_file,
		                     NULL};
		pid_t child;
		int status;
		assert(posix_spawn(&child, argv[0], NULL, NULL, arguments,
		                   environ) == 0);
		pid_t waited;
		do {
			waited = waitpid(child, &status, 0);
		} while (waited < 0 && errno == EINTR);
		assert(waited == child);
		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
			failed = 1;
	}
	tls_test_identity_cleanup(&identity);
	assert(!failed);
	puts("tls-ledger: process cleanup and allocation rollback passed");
	return 0;
}
