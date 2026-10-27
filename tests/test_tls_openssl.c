#define _GNU_SOURCE
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../include/tr/status.h"
#include "../src/io/byte_endpoint.h"
#include "../src/io/peer_identity_internal.h"
#include "../src/io/tls_openssl.h"
#include "tls_test_identity.h"

static unsigned int tls_test_cert_list_limit_set_count;
static long tls_test_cert_list_limit_last_value;

long __real_SSL_CTX_ctrl(SSL_CTX *context, int command, long value,
                         void *pointer);

long __wrap_SSL_CTX_ctrl(SSL_CTX *context, int command, long value,
                         void *pointer)
{
	if (command == SSL_CTRL_SET_MAX_CERT_LIST) {
		tls_test_cert_list_limit_set_count++;
		tls_test_cert_list_limit_last_value = value;
	}
	return __real_SSL_CTX_ctrl(context, command, value, pointer);
}

struct tls_test_pair {
	int client_fd;
	int server_fd;
	struct tr_tls_openssl_profile *client_profile;
	struct tr_tls_openssl_profile *server_profile;
	struct tr_tls_openssl_session *client_session;
	struct tr_tls_openssl_session *server_session;
	struct tr_byte_endpoint client_endpoint;
	struct tr_byte_endpoint server_endpoint;
};

struct tls_test_peer {
	int fd;
	char session_file[TR_TLS_TEST_PATH_SIZE];
	struct tr_tls_openssl_profile *profile;
	struct tr_tls_openssl_session *session;
	struct tr_byte_endpoint endpoint;
};

static struct tls_test_identity test_identity;
static struct tls_test_pair test_pair = {
        .client_fd = -1,
        .server_fd = -1,
};

static void tls_test_read_file(const char *path, uint8_t **out,
                               size_t *out_length);
static void tls_test_encrypt_private_key(const char *path, const char *password,
                                         uint8_t **out, size_t *out_length);

static void cleanup_test_identity(void)
{
	tls_test_identity_cleanup(&test_identity);
}

static uint64_t tls_test_now_ns(void)
{
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}

static short tls_test_poll_events(uint32_t events)
{
	short result = 0;
	if (events & TR_BYTE_ENDPOINT_EVENT_READ)
		result |= POLLIN;
	if (events & TR_BYTE_ENDPOINT_EVENT_WRITE)
		result |= POLLOUT;
	return result;
}

static uint32_t tls_test_ready_events(short events)
{
	uint32_t result = 0U;
	if (events & (POLLIN | POLLPRI | POLLERR | POLLHUP | POLLNVAL))
		result |= TR_BYTE_ENDPOINT_EVENT_READ;
	if (events & (POLLOUT | POLLERR | POLLHUP | POLLNVAL))
		result |= TR_BYTE_ENDPOINT_EVENT_WRITE;
	return result;
}

static int tls_test_poll_pair(uint32_t *client_ready, uint32_t *server_ready)
{
	struct pollfd fds[2] = {
	        {.fd = test_pair.client_fd,
	         .events = tls_test_poll_events(tr_byte_endpoint_interest(
	                 &test_pair.client_endpoint))},
	        {.fd = test_pair.server_fd,
	         .events = tls_test_poll_events(tr_byte_endpoint_interest(
	                 &test_pair.server_endpoint))},
	};
	int result;
	do {
		result = poll(fds, 2U, 20);
	} while (result < 0 && errno == EINTR);
	if (result < 0)
		return TR_ERR_SYS;
	*client_ready = tls_test_ready_events(fds[0].revents);
	*server_ready = tls_test_ready_events(fds[1].revents);
	return TR_OK;
}

static int
tls_test_pair_open_with_mtls(const char *client_certificate_file,
                             const char *client_private_key_file,
                             enum tr_tls_client_auth_policy client_auth,
                             const char *client_ca_file)
{
	struct tr_tls_client_config client_config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	        .client_certificate_file = client_certificate_file,
	        .client_private_key_file = client_private_key_file,
	};
	struct tr_tls_server_config server_config = {
	        .mode = TR_TLS_REQUIRED,
	        .certificate_file = test_identity.server_certificate_file,
	        .private_key_file = test_identity.server_private_key_file,
	        .client_ca_file = client_ca_file,
	        .client_auth = client_auth,
	};
	const struct tr_byte_endpoint_driver *driver = tr_tls_openssl_driver();
	uint64_t deadline = tls_test_now_ns() + UINT64_C(5000000000);
	int fds[2];

	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
	               fds) != 0)
		return TR_ERR_SYS;
	test_pair.client_fd = fds[0];
	test_pair.server_fd = fds[1];
	if (tr_tls_openssl_client_profile_create(
	            &client_config, &test_pair.client_profile) != TR_OK ||
	    tr_tls_openssl_server_profile_create(
	            &server_config, &test_pair.server_profile) != TR_OK ||
	    tr_tls_openssl_session_create(test_pair.client_profile,
	                                  test_pair.client_fd,
	                                  &test_pair.client_session) != TR_OK ||
	    tr_tls_openssl_session_create(test_pair.server_profile,
	                                  test_pair.server_fd,
	                                  &test_pair.server_session) != TR_OK)
		return TR_ERR_SYS;
	if (tr_byte_endpoint_init(&test_pair.client_endpoint,
	                          TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                          test_pair.client_fd, deadline, driver,
	                          test_pair.client_session) != TR_OK)
		return TR_ERR_SYS;
	test_pair.client_session = NULL;
	if (tr_byte_endpoint_init(&test_pair.server_endpoint,
	                          TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                          test_pair.server_fd, deadline, driver,
	                          test_pair.server_session) != TR_OK)
		return TR_ERR_SYS;
	test_pair.server_session = NULL;
	if (tr_byte_endpoint_event_bind(&test_pair.client_endpoint) != TR_OK ||
	    tr_byte_endpoint_event_bind(&test_pair.server_endpoint) != TR_OK)
		return TR_ERR_SYS;
	tr_tls_openssl_profile_release(test_pair.client_profile);
	test_pair.client_profile = NULL;
	tr_tls_openssl_profile_release(test_pair.server_profile);
	test_pair.server_profile = NULL;
	return TR_OK;
}

static int tls_test_pair_open(void)
{
	return tls_test_pair_open_with_mtls(NULL, NULL, TR_TLS_CLIENT_AUTH_NONE,
	                                    NULL);
}

static int tls_test_handshake_pair(void)
{
	for (unsigned attempt = 0U; attempt < 1000U; ++attempt) {
		uint64_t now = tls_test_now_ns();
		uint32_t client_ready;
		uint32_t server_ready;
		int status;
		if (test_pair.client_endpoint.state ==
		    TR_BYTE_ENDPOINT_NEGOTIATING) {
			status = tr_byte_endpoint_step(
			        &test_pair.client_endpoint, 0U, now);
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (test_pair.server_endpoint.state ==
		    TR_BYTE_ENDPOINT_NEGOTIATING) {
			status = tr_byte_endpoint_step(
			        &test_pair.server_endpoint, 0U, now);
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (test_pair.client_endpoint.state ==
		            TR_BYTE_ENDPOINT_AUTHENTICATED &&
		    test_pair.server_endpoint.state ==
		            TR_BYTE_ENDPOINT_AUTHENTICATED)
			return TR_OK;
		status = tls_test_poll_pair(&client_ready, &server_ready);
		if (status != TR_OK)
			return status;
		if (test_pair.client_endpoint.state ==
		    TR_BYTE_ENDPOINT_NEGOTIATING) {
			status = tr_byte_endpoint_step(
			        &test_pair.client_endpoint, client_ready,
			        tls_test_now_ns());
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (test_pair.server_endpoint.state ==
		    TR_BYTE_ENDPOINT_NEGOTIATING) {
			status = tr_byte_endpoint_step(
			        &test_pair.server_endpoint, server_ready,
			        tls_test_now_ns());
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
	}
	return TR_ERR_TIMEOUT;
}

static int tls_test_write_once(struct tr_byte_endpoint *endpoint, int fd,
                               const void *buffer, size_t length,
                               size_t *written)
{
	uint32_t wait_events = 0U;
	uint32_t ready_events = 0U;
	int status;
	for (unsigned attempt = 0U; attempt < 100U; ++attempt) {
		status = tr_byte_endpoint_write(endpoint, buffer, length,
		                                ready_events, written,
		                                &wait_events);
		if (status != TR_AGAIN)
			return status;
		struct pollfd fd_state = {
		        .fd = fd,
		        .events = tls_test_poll_events(wait_events),
		};
		int polled;
		do {
			polled = poll(&fd_state, 1U, 200);
		} while (polled < 0 && errno == EINTR);
		if (polled <= 0)
			return polled == 0 ? TR_ERR_TIMEOUT : TR_ERR_SYS;
		ready_events = tls_test_ready_events(fd_state.revents);
	}
	return TR_ERR_TIMEOUT;
}

static int tls_test_read_once(struct tr_byte_endpoint *endpoint, int fd,
                              void *buffer, size_t capacity, size_t *read_bytes)
{
	uint32_t wait_events = 0U;
	uint32_t ready_events = 0U;
	int status;
	for (unsigned attempt = 0U; attempt < 100U; ++attempt) {
		status = tr_byte_endpoint_read(endpoint, buffer, capacity,
		                               ready_events, read_bytes,
		                               &wait_events);
		if (status != TR_AGAIN)
			return status;
		struct pollfd fd_state = {
		        .fd = fd,
		        .events = tls_test_poll_events(wait_events),
		};
		int polled;
		do {
			polled = poll(&fd_state, 1U, 200);
		} while (polled < 0 && errno == EINTR);
		if (polled <= 0)
			return polled == 0 ? TR_ERR_TIMEOUT : TR_ERR_SYS;
		ready_events = tls_test_ready_events(fd_state.revents);
	}
	return TR_ERR_TIMEOUT;
}

static size_t tls_test_fill_send_buffer(int fd)
{
	unsigned char bytes[4096];
	size_t total = 0U;
	memset(bytes, 0xa5, sizeof(bytes));
	for (;;) {
		ssize_t result = send(fd, bytes, sizeof(bytes),
		                      MSG_DONTWAIT | MSG_NOSIGNAL);
		if (result > 0) {
			assert(total <= SIZE_MAX - (size_t)result);
			total += (size_t)result;
			continue;
		}
		if (result < 0 && errno == EINTR)
			continue;
		assert(result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
		break;
	}
	assert(total > 0U);
	return total;
}

static void tls_test_drain_raw_bytes(int fd, size_t length)
{
	unsigned char bytes[4096];
	while (length != 0U) {
		ssize_t result =
		        recv(fd, bytes,
		             length < sizeof(bytes) ? length : sizeof(bytes),
		             MSG_DONTWAIT);
		if (result > 0) {
			length -= (size_t)result;
			continue;
		}
		if (result < 0 && errno == EINTR)
			continue;
		if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			struct pollfd fd_state = {.fd = fd, .events = POLLIN};
			int polled;
			do {
				polled = poll(&fd_state, 1U, 200);
			} while (polled < 0 && errno == EINTR);
			assert(polled > 0);
			continue;
		}
		assert(0 && "raw socket drain failed");
	}
}

static int tls_test_clean_close(void)
{
	uint64_t deadline = tls_test_now_ns() + UINT64_C(5000000000);
	int status = tr_byte_endpoint_begin_close(&test_pair.client_endpoint,
	                                          TR_OK, deadline);
	int server_peer_closed = 0;
	if (status != TR_OK)
		return status;
	for (unsigned attempt = 0U; attempt < 1000U; ++attempt) {
		uint64_t now = tls_test_now_ns();
		uint32_t client_ready;
		uint32_t server_ready;
		if (!test_pair.client_endpoint.shutdown_done) {
			status = tr_byte_endpoint_step(
			        &test_pair.client_endpoint, 0U, now);
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (!server_peer_closed) {
			size_t ignored_bytes = 0U;
			uint32_t wait_events = 0U;
			unsigned char ignored[64];
			status = tr_byte_endpoint_read(
			        &test_pair.server_endpoint, ignored,
			        sizeof(ignored), 0U, &ignored_bytes,
			        &wait_events);
			if (status == TR_ERR_CLOSED) {
				server_peer_closed = 1;
				status = tr_byte_endpoint_begin_close(
				        &test_pair.server_endpoint, TR_OK,
				        deadline);
				if (status != TR_OK)
					return status;
			} else if (status != TR_AGAIN) {
				return status;
			}
		}
		if (server_peer_closed &&
		    !test_pair.server_endpoint.shutdown_done) {
			status = tr_byte_endpoint_step(
			        &test_pair.server_endpoint, 0U, now);
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (test_pair.client_endpoint.shutdown_done &&
		    test_pair.server_endpoint.shutdown_done)
			return TR_OK;
		status = tls_test_poll_pair(&client_ready, &server_ready);
		if (status != TR_OK)
			return status;
		if (!test_pair.client_endpoint.shutdown_done) {
			status = tr_byte_endpoint_step(
			        &test_pair.client_endpoint, client_ready,
			        tls_test_now_ns());
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
		if (server_peer_closed &&
		    !test_pair.server_endpoint.shutdown_done) {
			status = tr_byte_endpoint_step(
			        &test_pair.server_endpoint, server_ready,
			        tls_test_now_ns());
			if (status < 0 && status != TR_AGAIN)
				return status;
		}
	}
	return TR_ERR_TIMEOUT;
}

static void tls_test_pair_cleanup(void)
{
	const struct tr_byte_endpoint_driver *driver = tr_tls_openssl_driver();
	struct tr_byte_endpoint *endpoints[2] = {
	        &test_pair.client_endpoint,
	        &test_pair.server_endpoint,
	};
	for (size_t index = 0U; index < 2U; ++index) {
		struct tr_byte_endpoint *endpoint = endpoints[index];
		if (endpoint->state == TR_BYTE_ENDPOINT_NEGOTIATING ||
		    endpoint->state == TR_BYTE_ENDPOINT_AUTHENTICATED ||
		    endpoint->state == TR_BYTE_ENDPOINT_CLOSING) {
			(void)tr_byte_endpoint_abort(endpoint, TR_ERR_CLOSED);
			if (endpoint->events_bound)
				(void)tr_byte_endpoint_event_unbind(endpoint);
			(void)tr_byte_endpoint_release(endpoint);
		}
	}
	if (test_pair.client_session && driver)
		driver->release(test_pair.client_session);
	if (test_pair.server_session && driver)
		driver->release(test_pair.server_session);
	tr_tls_openssl_profile_release(test_pair.client_profile);
	tr_tls_openssl_profile_release(test_pair.server_profile);
	if (test_pair.client_fd >= 0)
		(void)close(test_pair.client_fd);
	if (test_pair.server_fd >= 0)
		(void)close(test_pair.server_fd);
	memset(&test_pair, 0, sizeof(test_pair));
	test_pair.client_fd = -1;
	test_pair.server_fd = -1;
}

static int
tls_test_pair_open_profiles(struct tr_tls_openssl_profile *client_profile,
                            struct tr_tls_openssl_profile *server_profile)
{
	const struct tr_byte_endpoint_driver *driver = tr_tls_openssl_driver();
	uint64_t deadline = tls_test_now_ns() + UINT64_C(5000000000);
	int fds[2];

	if (!client_profile || !server_profile)
		return TR_ERR_INVALID;
	if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
	               fds) != 0)
		return TR_ERR_SYS;
	test_pair.client_fd = fds[0];
	test_pair.server_fd = fds[1];
	test_pair.client_profile = client_profile;
	test_pair.server_profile = server_profile;
	if (tr_tls_openssl_session_create(client_profile, test_pair.client_fd,
	                                  &test_pair.client_session) != TR_OK ||
	    tr_tls_openssl_session_create(server_profile, test_pair.server_fd,
	                                  &test_pair.server_session) != TR_OK)
		return TR_ERR_SYS;
	if (tr_byte_endpoint_init(&test_pair.client_endpoint,
	                          TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                          test_pair.client_fd, deadline, driver,
	                          test_pair.client_session) != TR_OK)
		return TR_ERR_SYS;
	test_pair.client_session = NULL;
	if (tr_byte_endpoint_init(&test_pair.server_endpoint,
	                          TR_BYTE_ENDPOINT_TLS_REQUIRED,
	                          test_pair.server_fd, deadline, driver,
	                          test_pair.server_session) != TR_OK)
		return TR_ERR_SYS;
	test_pair.server_session = NULL;
	if (tr_byte_endpoint_event_bind(&test_pair.client_endpoint) != TR_OK ||
	    tr_byte_endpoint_event_bind(&test_pair.server_endpoint) != TR_OK)
		return TR_ERR_SYS;
	tr_tls_openssl_profile_release(test_pair.client_profile);
	test_pair.client_profile = NULL;
	tr_tls_openssl_profile_release(test_pair.server_profile);
	test_pair.server_profile = NULL;
	return TR_OK;
}

static void test_in_memory_pem_profiles(void)
{
	const uint32_t bundle_limit = 4U * 1024U * 1024U;
	struct tr_tls_client_config client_config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_server_config server_config = {
	        .mode = TR_TLS_REQUIRED,
	        .client_ca_file = test_identity.ca_file,
	        .client_auth = TR_TLS_CLIENT_AUTH_REQUIRED,
	};
	struct tr_tls_openssl_profile *profile =
	        (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	uint8_t *client_certificate = NULL;
	uint8_t *client_key = NULL;
	uint8_t *server_certificate = NULL;
	uint8_t *server_key = NULL;
	uint8_t *encrypted_key = NULL;
	uint8_t *trailing_certificate = NULL;
	struct tr_tls_credential_bundle client_bundle;
	struct tr_tls_credential_bundle server_bundle;
	size_t client_certificate_length;
	size_t client_key_length;
	size_t server_certificate_length;
	size_t server_key_length;
	size_t encrypted_key_length;

	tls_test_read_file(test_identity.client_certificate_file,
	                   &client_certificate, &client_certificate_length);
	tls_test_read_file(test_identity.client_private_key_file, &client_key,
	                   &client_key_length);
	tls_test_read_file(test_identity.server_certificate_file,
	                   &server_certificate, &server_certificate_length);
	tls_test_read_file(test_identity.server_private_key_file, &server_key,
	                   &server_key_length);
	client_bundle = (struct tr_tls_credential_bundle){
	        .certificate_chain_pem =
	                {
	                        .data = client_certificate,
	                        .len = client_certificate_length,
	                },
	        .private_key_pem =
	                {
	                        .data = client_key,
	                        .len = client_key_length,
	                },
	};
	server_bundle = (struct tr_tls_credential_bundle){
	        .certificate_chain_pem =
	                {
	                        .data = server_certificate,
	                        .len = server_certificate_length,
	                },
	        .private_key_pem =
	                {
	                        .data = server_key,
	                        .len = server_key_length,
	                },
	};
	assert(tr_tls_openssl_client_profile_create_from_pem_with_peer_identity(
	               &client_config, &client_bundle, bundle_limit,
	               &profile) == TR_OK);
	assert(profile != NULL);
	struct tr_tls_openssl_profile *client_profile = profile;
	profile = NULL;
	assert(tr_tls_openssl_server_profile_create_from_pem(
	               &server_config, &server_bundle, bundle_limit,
	               &profile) == TR_OK);
	assert(profile != NULL);
	assert(tr_tls_openssl_profile_peer_identity_enabled(profile));
	struct tr_tls_openssl_profile *server_profile = profile;
	profile = NULL;
	assert(tls_test_pair_open_profiles(client_profile, server_profile) ==
	       TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	tls_test_pair_cleanup();

	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	struct tr_tls_credential_bundle invalid_bundle = client_bundle;
	invalid_bundle.certificate_chain_pem.data = NULL;
	invalid_bundle.certificate_chain_pem.len = 0U;
	assert(tr_tls_openssl_client_profile_create_from_pem(
	               &client_config, &invalid_bundle, bundle_limit,
	               &profile) < 0);
	assert(profile == NULL);

	tls_test_read_file(test_identity.mismatched_private_key_file,
	                   &encrypted_key, &encrypted_key_length);
	invalid_bundle = client_bundle;
	invalid_bundle.private_key_pem.data = encrypted_key;
	invalid_bundle.private_key_pem.len = encrypted_key_length;
	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	assert(tr_tls_openssl_client_profile_create_from_pem(
	               &client_config, &invalid_bundle, bundle_limit,
	               &profile) < 0);
	assert(profile == NULL);
	OPENSSL_cleanse(encrypted_key, encrypted_key_length);
	free(encrypted_key);
	encrypted_key = NULL;

	tls_test_encrypt_private_key(test_identity.client_private_key_file,
	                             "pem-test-secret", &encrypted_key,
	                             &encrypted_key_length);
	invalid_bundle = client_bundle;
	invalid_bundle.private_key_pem.data = encrypted_key;
	invalid_bundle.private_key_pem.len = encrypted_key_length;
	invalid_bundle.private_key_password.data = (const uint8_t *)"incorrect";
	invalid_bundle.private_key_password.len = strlen("incorrect");
	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	assert(tr_tls_openssl_client_profile_create_from_pem(
	               &client_config, &invalid_bundle, bundle_limit,
	               &profile) < 0);
	assert(profile == NULL);
	OPENSSL_cleanse(encrypted_key, encrypted_key_length);
	free(encrypted_key);
	encrypted_key = NULL;

	trailing_certificate = malloc(client_certificate_length + 8U);
	assert(trailing_certificate != NULL);
	memcpy(trailing_certificate, client_certificate,
	       client_certificate_length);
	memcpy(trailing_certificate + client_certificate_length, "\ninvalid",
	       8U);
	invalid_bundle = client_bundle;
	invalid_bundle.certificate_chain_pem.data = trailing_certificate;
	invalid_bundle.certificate_chain_pem.len =
	        client_certificate_length + 8U;
	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	assert(tr_tls_openssl_client_profile_create_from_pem(
	               &client_config, &invalid_bundle, bundle_limit,
	               &profile) < 0);
	assert(profile == NULL);
	free(trailing_certificate);
	trailing_certificate = NULL;

	invalid_bundle = client_bundle;
	invalid_bundle.certificate_chain_pem.len = (size_t)bundle_limit + 1U;
	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	assert(tr_tls_openssl_client_profile_create_from_pem(
	               &client_config, &invalid_bundle, bundle_limit,
	               &profile) == TR_ERR_BAD_LENGTH);
	assert(profile == NULL);

	OPENSSL_cleanse(client_key, client_key_length);
	OPENSSL_cleanse(server_key, server_key_length);
	free(client_certificate);
	free(client_key);
	free(server_certificate);
	free(server_key);
}

static void tls_test_read_file(const char *path, uint8_t **out,
                               size_t *out_length)
{
	FILE *file;
	long length;
	uint8_t *data;

	assert(path != NULL);
	assert(out != NULL);
	assert(out_length != NULL);
	*out = NULL;
	*out_length = 0U;
	file = fopen(path, "rb");
	assert(file != NULL);
	assert(fseek(file, 0L, SEEK_END) == 0);
	length = ftell(file);
	assert(length > 0L);
	assert(fseek(file, 0L, SEEK_SET) == 0);
	data = malloc((size_t)length);
	assert(data != NULL);
	assert(fread(data, 1U, (size_t)length, file) == (size_t)length);
	assert(fclose(file) == 0);
	*out = data;
	*out_length = (size_t)length;
}

static void tls_test_encrypt_private_key(const char *path, const char *password,
                                         uint8_t **out, size_t *out_length)
{
	BIO *input = BIO_new_file(path, "rb");
	BIO *output = NULL;
	EVP_PKEY *key = NULL;
	char *view = NULL;
	long length;

	assert(out != NULL);
	assert(out_length != NULL);
	*out = NULL;
	*out_length = 0U;
	assert(input != NULL);
	key = PEM_read_bio_PrivateKey(input, NULL, NULL, NULL);
	assert(key != NULL);
	output = BIO_new(BIO_s_mem());
	assert(output != NULL);
	assert(PEM_write_bio_PrivateKey(output, key, EVP_aes_256_cbc(),
	                                (unsigned char *)password,
	                                (int)strlen(password), NULL,
	                                NULL) == 1);
	length = BIO_get_mem_data(output, &view);
	assert(length > 0L);
	*out = malloc((size_t)length);
	assert(*out != NULL);
	memcpy(*out, view, (size_t)length);
	*out_length = (size_t)length;
	OPENSSL_cleanse(view, (size_t)length);
	BIO_free(output);
	BIO_free(input);
	EVP_PKEY_free(key);
}

static void test_nonblocking_tls_pair_authenticates_and_preserves_fds(void)
{
	unsigned char payload[] = {0x43U, 0x54U, 0x52U, 0x50U, 0x43U};
	unsigned char other_payload[] = {0x44U, 0x45U};
	unsigned char received[sizeof(payload)];
	size_t written = 0U;
	size_t read_bytes = 0U;
	size_t queued_raw_bytes;
	uint32_t wait_events = 0U;
	int send_buffer_size = 4096;
	assert(tls_test_pair_open() == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(test_pair.client_endpoint.state ==
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(test_pair.server_endpoint.state ==
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_byte_endpoint_read(&test_pair.server_endpoint, received,
	                             sizeof(received), 0U, &read_bytes,
	                             &wait_events) == TR_AGAIN);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_READ);
	assert(tr_byte_endpoint_read(&test_pair.server_endpoint, received,
	                             sizeof(received),
	                             TR_BYTE_ENDPOINT_EVENT_WRITE, &read_bytes,
	                             &wait_events) == TR_AGAIN);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_READ);
	assert(setsockopt(test_pair.client_fd, SOL_SOCKET, SO_SNDBUF,
	                  &send_buffer_size, sizeof(send_buffer_size)) == 0);
	queued_raw_bytes = tls_test_fill_send_buffer(test_pair.client_fd);
	assert(tr_byte_endpoint_write(&test_pair.client_endpoint, payload,
	                              sizeof(payload), 0U, &written,
	                              &wait_events) == TR_AGAIN);
	assert(written == 0U);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_WRITE);
	assert(tr_byte_endpoint_write(&test_pair.client_endpoint, payload,
	                              sizeof(payload),
	                              TR_BYTE_ENDPOINT_EVENT_READ, &written,
	                              &wait_events) == TR_AGAIN);
	assert(wait_events == TR_BYTE_ENDPOINT_EVENT_WRITE);
	assert(tr_byte_endpoint_write(&test_pair.client_endpoint, other_payload,
	                              sizeof(other_payload),
	                              TR_BYTE_ENDPOINT_EVENT_WRITE, &written,
	                              &wait_events) == TR_ERR_STATE);
	tls_test_drain_raw_bytes(test_pair.server_fd, queued_raw_bytes);
	assert(tls_test_write_once(&test_pair.client_endpoint,
	                           test_pair.client_fd, payload,
	                           sizeof(payload), &written) == TR_OK);
	assert(written == sizeof(payload));
	assert(tls_test_read_once(&test_pair.server_endpoint,
	                          test_pair.server_fd, received,
	                          sizeof(received), &read_bytes) == TR_OK);
	assert(read_bytes == sizeof(payload));
	assert(memcmp(received, payload, sizeof(payload)) == 0);
	assert(tls_test_clean_close() == TR_OK);
	assert(tr_byte_endpoint_event_unbind(&test_pair.client_endpoint) ==
	       TR_OK);
	assert(tr_byte_endpoint_event_unbind(&test_pair.server_endpoint) ==
	       TR_OK);
	assert(tr_byte_endpoint_release(&test_pair.client_endpoint) == TR_OK);
	assert(tr_byte_endpoint_release(&test_pair.server_endpoint) == TR_OK);
	assert(fcntl(test_pair.client_fd, F_GETFD) >= 0);
	assert(fcntl(test_pair.server_fd, F_GETFD) >= 0);
	assert(close(test_pair.client_fd) == 0);
	assert(close(test_pair.server_fd) == 0);
	test_pair.client_fd = -1;
	test_pair.server_fd = -1;
}

static void test_required_client_auth_rejects_missing_certificate(void)
{
	assert(tls_test_pair_open_with_mtls(NULL, NULL,
	                                    TR_TLS_CLIENT_AUTH_REQUIRED,
	                                    test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() != TR_OK);
	assert(test_pair.server_endpoint.state !=
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	tls_test_pair_cleanup();
}

static void test_required_client_auth_accepts_valid_certificate(void)
{
	static const uint8_t client_uri[] = "spiffe://c-trpc.example/client";
	static const uint8_t server_uri[] = "spiffe://c-trpc.example/server";
	struct tr_peer_identity_snapshot *identity = NULL;
	struct tr_peer_identity_view view;
	assert(tls_test_pair_open_with_mtls(
	               test_identity.client_certificate_file,
	               test_identity.client_private_key_file,
	               TR_TLS_CLIENT_AUTH_REQUIRED,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(test_pair.client_endpoint.state ==
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(test_pair.server_endpoint.state ==
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.server_endpoint.driver_arg, 1, &identity) ==
	       TR_OK);
	assert(tr_peer_identity_snapshot_view(identity, &view) == TR_OK);
	assert(view.authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE);
	assert(view.uri_san_len == sizeof(client_uri) - 1U);
	assert(memcmp(view.uri_san, client_uri, sizeof(client_uri) - 1U) == 0);
	tr_peer_identity_snapshot_release(identity);
	identity = NULL;
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.client_endpoint.driver_arg, 1, &identity) ==
	       TR_OK);
	assert(tr_peer_identity_snapshot_view(identity, &view) == TR_OK);
	assert(view.uri_san_len == sizeof(server_uri) - 1U);
	assert(memcmp(view.uri_san, server_uri, sizeof(server_uri) - 1U) == 0);
	tr_peer_identity_snapshot_release(identity);
	tls_test_pair_cleanup();
}

static void test_optional_client_auth_accepts_missing_certificate(void)
{
	struct tr_peer_identity_snapshot *identity =
	        (struct tr_peer_identity_snapshot *)(uintptr_t)1U;
	assert(tls_test_pair_open_with_mtls(NULL, NULL,
	                                    TR_TLS_CLIENT_AUTH_OPTIONAL,
	                                    test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(test_pair.server_endpoint.state ==
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.server_endpoint.driver_arg, 0, &identity) ==
	       TR_OK);
	assert(identity == NULL);
	tls_test_pair_cleanup();
}

static void test_optional_client_auth_validates_present_certificate(void)
{
	static const uint8_t client_uri[] = "spiffe://c-trpc.example/client";
	struct tr_peer_identity_snapshot *identity = NULL;
	struct tr_peer_identity_view view;
	assert(tls_test_pair_open_with_mtls(
	               test_identity.client_certificate_file,
	               test_identity.client_private_key_file,
	               TR_TLS_CLIENT_AUTH_OPTIONAL,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.server_endpoint.driver_arg, 0, &identity) ==
	       TR_OK);
	assert(identity != NULL);
	assert(tr_peer_identity_snapshot_view(identity, &view) == TR_OK);
	assert(view.uri_san_len == sizeof(client_uri) - 1U);
	assert(memcmp(view.uri_san, client_uri, sizeof(client_uri) - 1U) == 0);
	tr_peer_identity_snapshot_release(identity);
	tls_test_pair_cleanup();
}

static void test_rotated_client_key_keeps_stable_identity(void)
{
	static const uint8_t client_uri[] = "spiffe://c-trpc.example/client";
	struct tr_peer_identity_snapshot *identity = NULL;
	struct tr_peer_identity_view view;
	assert(tls_test_pair_open_with_mtls(
	               test_identity.rotated_client_certificate_file,
	               test_identity.rotated_client_private_key_file,
	               TR_TLS_CLIENT_AUTH_REQUIRED,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.server_endpoint.driver_arg, 1, &identity) ==
	       TR_OK);
	assert(tr_peer_identity_snapshot_view(identity, &view) == TR_OK);
	assert(view.uri_san_len == sizeof(client_uri) - 1U);
	assert(memcmp(view.uri_san, client_uri, sizeof(client_uri) - 1U) == 0);
	tr_peer_identity_snapshot_release(identity);
	tls_test_pair_cleanup();
}

static void test_required_client_auth_rejects_wrong_ca(void)
{
	assert(tls_test_pair_open_with_mtls(
	               test_identity.wrong_client_ca_certificate_file,
	               test_identity.wrong_client_ca_private_key_file,
	               TR_TLS_CLIENT_AUTH_REQUIRED,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() != TR_OK);
	assert(test_pair.server_endpoint.state !=
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	tls_test_pair_cleanup();
}

static void test_optional_client_auth_still_rejects_wrong_ca(void)
{
	assert(tls_test_pair_open_with_mtls(
	               test_identity.wrong_client_ca_certificate_file,
	               test_identity.wrong_client_ca_private_key_file,
	               TR_TLS_CLIENT_AUTH_OPTIONAL,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() != TR_OK);
	assert(test_pair.server_endpoint.state !=
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	tls_test_pair_cleanup();
}

static void tls_test_expect_client_identity_error(const char *certificate_file,
                                                  const char *private_key_file,
                                                  int expected_status)
{
	struct tr_peer_identity_snapshot *identity =
	        (struct tr_peer_identity_snapshot *)(uintptr_t)1U;
	assert(tls_test_pair_open_with_mtls(certificate_file, private_key_file,
	                                    TR_TLS_CLIENT_AUTH_REQUIRED,
	                                    test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(tr_tls_openssl_session_peer_identity(
	               test_pair.server_endpoint.driver_arg, 1, &identity) ==
	       expected_status);
	assert(identity == NULL);
	tls_test_pair_cleanup();
}

static void test_client_identity_requires_one_bounded_uri_san(void)
{
	tls_test_expect_client_identity_error(
	        test_identity.client_no_uri_certificate_file,
	        test_identity.client_private_key_file, TR_ERR_INVALID);
	tls_test_expect_client_identity_error(
	        test_identity.client_duplicate_uri_certificate_file,
	        test_identity.client_private_key_file, TR_ERR_INVALID);
	tls_test_expect_client_identity_error(
	        test_identity.client_empty_uri_certificate_file,
	        test_identity.client_private_key_file, TR_ERR_INVALID);
	tls_test_expect_client_identity_error(
	        test_identity.client_oversized_uri_certificate_file,
	        test_identity.client_private_key_file, TR_ERR_BAD_LENGTH);
	tls_test_expect_client_identity_error(
	        test_identity.client_control_uri_certificate_file,
	        test_identity.client_private_key_file, TR_ERR_INVALID);
}

static void test_client_auth_rejects_server_only_eku(void)
{
	assert(tls_test_pair_open_with_mtls(
	               test_identity.client_wrong_eku_certificate_file,
	               test_identity.client_private_key_file,
	               TR_TLS_CLIENT_AUTH_REQUIRED,
	               test_identity.ca_file) == TR_OK);
	assert(tls_test_handshake_pair() != TR_OK);
	assert(test_pair.server_endpoint.state !=
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	tls_test_pair_cleanup();
}

static void test_handshake_maps_want_write_then_want_read(void)
{
	int send_buffer_size = 4096;
	size_t queued_raw_bytes;
	int status;
	assert(tls_test_pair_open() == TR_OK);
	assert(setsockopt(test_pair.client_fd, SOL_SOCKET, SO_SNDBUF,
	                  &send_buffer_size, sizeof(send_buffer_size)) == 0);
	queued_raw_bytes = tls_test_fill_send_buffer(test_pair.client_fd);
	status = tr_byte_endpoint_step(&test_pair.client_endpoint, 0U,
	                               tls_test_now_ns());
	assert(status == TR_AGAIN);
	assert(test_pair.client_endpoint.handshake_wait ==
	       TR_BYTE_ENDPOINT_EVENT_WRITE);
	tls_test_drain_raw_bytes(test_pair.server_fd, queued_raw_bytes);
	status = tr_byte_endpoint_step(&test_pair.client_endpoint,
	                               TR_BYTE_ENDPOINT_EVENT_WRITE,
	                               tls_test_now_ns());
	assert(status == TR_AGAIN);
	assert(test_pair.client_endpoint.handshake_wait ==
	       TR_BYTE_ENDPOINT_EVENT_READ);
	tls_test_pair_cleanup();
}

static void test_blocking_socket_is_rejected_without_taking_ownership(void)
{
	struct tr_tls_client_config config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_openssl_profile *profile = NULL;
	struct tr_tls_openssl_session *session = NULL;
	int fds[2];
	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_OK);
	assert((fcntl(fds[0], F_GETFL) & O_NONBLOCK) == 0);
	assert(tr_tls_openssl_session_create(profile, fds[0], &session) ==
	       TR_ERR_INVALID);
	assert(session == NULL);
	assert(fcntl(fds[0], F_GETFD) >= 0);
	assert(close(fds[0]) == 0);
	assert(close(fds[1]) == 0);
	tr_tls_openssl_profile_release(profile);
}

static void test_profiles_pin_peer_certificate_chain_limit(void)
{
	struct tr_tls_client_config client_config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_server_config server_config = {
	        .mode = TR_TLS_REQUIRED,
	        .certificate_file = test_identity.server_certificate_file,
	        .private_key_file = test_identity.server_private_key_file,
	};
	struct tr_tls_openssl_profile *client_profile = NULL;
	struct tr_tls_openssl_profile *server_profile = NULL;
	unsigned int before = tls_test_cert_list_limit_set_count;

	assert(tr_tls_openssl_client_profile_create(&client_config,
	                                            &client_profile) == TR_OK);
	assert(tr_tls_openssl_server_profile_create(&server_config,
	                                            &server_profile) == TR_OK);
	assert(tls_test_cert_list_limit_set_count == before + 2U);
	assert(tls_test_cert_list_limit_last_value ==
	       TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES);
	tr_tls_openssl_profile_release(client_profile);
	tr_tls_openssl_profile_release(server_profile);
}

static X509 *tls_test_large_server_certificate(void)
{
	static const long extension_bytes =
	        TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES + 4096L;
	BIO *certificate_file =
	        BIO_new_file(test_identity.server_certificate_file, "rb");
	BIO *key_file = BIO_new_file(test_identity.ca_private_key_file, "rb");
	X509 *source = NULL;
	X509 *certificate = NULL;
	EVP_PKEY *ca_key = NULL;
	ASN1_OBJECT *object = NULL;
	ASN1_OCTET_STRING *value = NULL;
	X509_EXTENSION *extension = NULL;
	unsigned char *extension_data = NULL;
	int encoded_bytes;

	assert(certificate_file != NULL && key_file != NULL);
	source = PEM_read_bio_X509(certificate_file, NULL, NULL, NULL);
	ca_key = PEM_read_bio_PrivateKey(key_file, NULL, NULL, NULL);
	assert(source != NULL && ca_key != NULL);
	certificate = X509_dup(source);
	assert(certificate != NULL);
	object = OBJ_txt2obj("1.3.6.1.4.1.55555.1", 1);
	value = ASN1_OCTET_STRING_new();
	extension_data = OPENSSL_malloc((size_t)extension_bytes);
	assert(object != NULL && value != NULL && extension_data != NULL);
	memset(extension_data, 0x5a, (size_t)extension_bytes);
	assert(ASN1_OCTET_STRING_set(value, extension_data,
	                             (int)extension_bytes) == 1);
	extension = X509_EXTENSION_create_by_OBJ(NULL, object, 0, value);
	assert(extension != NULL &&
	       X509_add_ext(certificate, extension, -1) == 1);
	assert(X509_sign(certificate, ca_key, EVP_sha256()) > 0);
	encoded_bytes = i2d_X509(certificate, NULL);
	assert(encoded_bytes > TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES);
	OPENSSL_cleanse(extension_data, (size_t)extension_bytes);
	OPENSSL_free(extension_data);
	X509_EXTENSION_free(extension);
	ASN1_OCTET_STRING_free(value);
	ASN1_OBJECT_free(object);
	EVP_PKEY_free(ca_key);
	X509_free(source);
	BIO_free(key_file);
	BIO_free(certificate_file);
	return certificate;
}

static void test_oversized_peer_certificate_chain_is_rejected(void)
{
	char certificate_path[TR_TLS_TEST_PATH_SIZE];
	struct tr_tls_client_config client_config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_server_config server_config = {
	        .mode = TR_TLS_REQUIRED,
	        .private_key_file = test_identity.server_private_key_file,
	};
	struct tr_tls_openssl_profile *client_profile = NULL;
	struct tr_tls_openssl_profile *server_profile = NULL;
	X509 *certificate;
	BIO *output;
	int written;

	written = snprintf(certificate_path, sizeof(certificate_path), "%s/%s",
	                   test_identity.directory, "oversized-server.pem");
	assert(written > 0 && (size_t)written < sizeof(certificate_path));
	certificate = tls_test_large_server_certificate();
	output = BIO_new_file(certificate_path, "wb");
	assert(output != NULL && PEM_write_bio_X509(output, certificate) == 1);
	BIO_free(output);
	X509_free(certificate);
	server_config.certificate_file = certificate_path;

	assert(tr_tls_openssl_client_profile_create(&client_config,
	                                            &client_profile) == TR_OK);
	assert(tr_tls_openssl_server_profile_create(&server_config,
	                                            &server_profile) == TR_OK);
	assert(tls_test_pair_open_profiles(client_profile, server_profile) ==
	       TR_OK);
	assert(SSL_get_max_cert_list(tr_tls_openssl_session_ssl_for_test(
	               test_pair.client_endpoint.driver_arg)) ==
	       TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES);
	assert(SSL_get_max_cert_list(tr_tls_openssl_session_ssl_for_test(
	               test_pair.server_endpoint.driver_arg)) ==
	       TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES);
	ERR_clear_error();
	assert(tls_test_handshake_pair() != TR_OK);
	{
		unsigned long error = ERR_peek_last_error();
		assert(error != 0UL && ERR_GET_LIB(error) == ERR_LIB_SSL &&
		       ERR_GET_REASON(error) == SSL_R_EXCESSIVE_MESSAGE_SIZE);
	}
	assert(test_pair.client_endpoint.state !=
	       TR_BYTE_ENDPOINT_AUTHENTICATED);
	tls_test_pair_cleanup();
	assert(unlink(certificate_path) == 0);
}

static void test_profile_generation_overflow_is_rejected(void)
{
	struct tr_tls_client_config config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_openssl_profile *profile = NULL;
	uint64_t generation = UINT64_C(0xfeedface);

	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_OK);
	assert(tr_tls_openssl_profile_generation(profile) == 1U);
	assert(tr_tls_openssl_profile_next_generation(profile, &generation) ==
	       TR_OK);
	assert(generation == 2U);
	assert(tr_tls_openssl_profile_set_generation(profile, UINT64_MAX) ==
	       TR_OK);
	generation = UINT64_C(0xfeedface);
	assert(tr_tls_openssl_profile_next_generation(profile, &generation) ==
	       TR_ERR_STATE);
	assert(generation == UINT64_C(0xfeedface));
	assert(tr_tls_openssl_profile_retain(profile) == TR_OK);
	assert(tr_tls_openssl_profile_set_generation(profile, 2U) ==
	       TR_ERR_STATE);
	tr_tls_openssl_profile_release(profile);
	tr_tls_openssl_profile_release(profile);
}

static void test_unexpected_eof_is_not_close_notify(void)
{
	unsigned char received[32];
	size_t read_bytes = 0U;
	int status;
	assert(tls_test_pair_open() == TR_OK);
	assert(tls_test_handshake_pair() == TR_OK);
	assert(close(test_pair.server_fd) == 0);
	test_pair.server_fd = -1;
	status = tls_test_read_once(&test_pair.client_endpoint,
	                            test_pair.client_fd, received,
	                            sizeof(received), &read_bytes);
	assert(status < 0);
	assert(status != TR_ERR_CLOSED);
	assert(test_pair.client_endpoint.state == TR_BYTE_ENDPOINT_CLOSING);
	assert(test_pair.client_endpoint.terminal_status == status);
	tls_test_pair_cleanup();
}

static uint32_t tls_test_peer_events(short events)
{
	return tls_test_ready_events(events);
}

static int tls_test_peer_wait(struct tls_test_peer *peer, uint32_t events,
                              uint64_t deadline, uint32_t *ready)
{
	struct pollfd descriptor = {
	        .fd = peer->fd,
	        .events = tls_test_poll_events(events),
	};
	for (;;) {
		uint64_t now = tls_test_now_ns();
		uint64_t remaining;
		int timeout_ms;
		int result;
		if (now >= deadline)
			return TR_ERR_TIMEOUT;
		remaining = deadline - now;
		timeout_ms = (int)((remaining + UINT64_C(999999)) /
		                   UINT64_C(1000000));
		if (timeout_ms > 1000)
			timeout_ms = 1000;
		descriptor.revents = 0;
		result = poll(&descriptor, 1U, timeout_ms);
		if (result > 0) {
			*ready = tls_test_peer_events(descriptor.revents);
			return TR_OK;
		}
		if (result == 0)
			continue;
		if (errno != EINTR)
			return TR_ERR_SYS;
	}
}

static int tls_test_peer_accept(int listener_fd, uint64_t deadline)
{
	for (;;) {
		int fd = accept4(listener_fd, NULL, NULL,
		                 SOCK_NONBLOCK | SOCK_CLOEXEC);
		if (fd >= 0)
			return fd;
		if (errno == EINTR)
			continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK)
			return -1;
		{
			struct pollfd descriptor = {.fd = listener_fd,
			                            .events = POLLIN};
			int result = poll(&descriptor, 1U, 100);
			if (result < 0 && errno != EINTR)
				return -1;
			if (tls_test_now_ns() >= deadline)
				return -1;
		}
	}
}

static int tls_test_peer_connect(unsigned short port, uint64_t deadline)
{
	struct sockaddr_in address = {
	        .sin_family = AF_INET,
	        .sin_port = htons(port),
	};
	int fd;
	int result;
	int socket_error = 0;
	socklen_t socket_error_size = sizeof(socket_error);
	if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1)
		return -1;
	fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
	if (fd < 0)
		return -1;
	result =
	        connect(fd, (const struct sockaddr *)&address, sizeof(address));
	if (result != 0 && errno != EINPROGRESS) {
		(void)close(fd);
		return -1;
	}
	if (result != 0) {
		struct pollfd descriptor = {.fd = fd, .events = POLLOUT};
		do {
			result = poll(&descriptor, 1U, 100);
		} while (result < 0 && errno == EINTR);
		if (result <= 0 || tls_test_now_ns() >= deadline ||
		    getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error,
		               &socket_error_size) != 0 ||
		    socket_error != 0) {
			(void)close(fd);
			return -1;
		}
	}
	return fd;
}

static int tls_test_path_from_directory(char *output, size_t capacity,
                                        const char *directory, const char *name)
{
	int written = snprintf(output, capacity, "%s/%s", directory, name);
	return written > 0 && (size_t)written < capacity;
}

static const char *tls_test_server_certificate_name(const char *case_name)
{
	if (strcmp(case_name, "wrong-dns") == 0)
		return "wrong-dns.pem";
	if (strcmp(case_name, "wrong-ip") == 0)
		return "wrong-ip.pem";
	if (strcmp(case_name, "cn-only") == 0)
		return "common-name-only.pem";
	if (strcmp(case_name, "expired") == 0)
		return "expired.pem";
	if (strcmp(case_name, "client-eku") == 0)
		return "client-only.pem";
	return "server.pem";
}

static int tls_test_peer_profile_create(const char *role, const char *case_name,
                                        const char *directory,
                                        struct tr_tls_openssl_profile **out)
{
	char ca_file[TR_TLS_TEST_PATH_SIZE];
	char certificate_file[TR_TLS_TEST_PATH_SIZE];
	char private_key_file[TR_TLS_TEST_PATH_SIZE];
	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	struct tr_tls_client_config client_config = {
	        .mode = TR_TLS_REQUIRED,
	        .expected_peer_name =
	                strcmp(case_name, "ip") == 0 ||
	                                strcmp(case_name, "wrong-ip") == 0
	                        ? "127.0.0.1"
	                        : "server.example.test",
	        .sni_name = strcmp(case_name, "sni") == 0
	                            ? "routing.example.test"
	                            : NULL,
	};
	struct tr_tls_server_config server_config = {
	        .mode = TR_TLS_REQUIRED,
	};
	if (strcmp(role, "client") == 0) {
		const char *ca_name = strcmp(case_name, "unknown-ca") == 0
		                              ? "alternate-ca.pem"
		                              : "ca.pem";
		if (!tls_test_path_from_directory(ca_file, sizeof(ca_file),
		                                  directory, ca_name))
			return TR_ERR_INVALID;
		client_config.ca_file = ca_file;
		return tr_tls_openssl_client_profile_create(&client_config,
		                                            out);
	}
	if (strcmp(role, "server") == 0) {
		if (!tls_test_path_from_directory(
		            certificate_file, sizeof(certificate_file),
		            directory,
		            tls_test_server_certificate_name(case_name)) ||
		    !tls_test_path_from_directory(private_key_file,
		                                  sizeof(private_key_file),
		                                  directory, "server-key.pem"))
			return TR_ERR_INVALID;
		server_config.certificate_file = certificate_file;
		server_config.private_key_file = private_key_file;
		return tr_tls_openssl_server_profile_create(&server_config,
		                                            out);
	}
	return TR_ERR_INVALID;
}

static int tls_test_peer_open(struct tls_test_peer *peer, const char *role,
                              const char *case_name, const char *directory,
                              int fd, uint64_t deadline,
                              struct tr_tls_openssl_profile *shared_profile)
{
	struct tr_tls_openssl_profile *profile = shared_profile;
	int owns_profile = 0;
	int status;
	memset(peer, 0, sizeof(*peer));
	peer->fd = fd;
	if (!tls_test_path_from_directory(peer->session_file,
	                                  sizeof(peer->session_file), directory,
	                                  "resumption-session.der"))
		return TR_ERR_INVALID;
	if (!profile) {
		status = tls_test_peer_profile_create(role, case_name,
		                                      directory, &profile);
		if (status != TR_OK)
			return status;
		owns_profile = 1;
		peer->profile = profile;
	}
	status = tr_tls_openssl_session_create(profile, fd, &peer->session);
	if (status != TR_OK)
		return status;
	status = tr_byte_endpoint_init(
	        &peer->endpoint, TR_BYTE_ENDPOINT_TLS_REQUIRED, fd, deadline,
	        tr_tls_openssl_driver(), peer->session);
	if (status != TR_OK)
		return status;
	peer->session = NULL;
	status = tr_byte_endpoint_event_bind(&peer->endpoint);
	if (status != TR_OK)
		return status;
	if (owns_profile) {
		tr_tls_openssl_profile_release(peer->profile);
		peer->profile = NULL;
	}
	return TR_OK;
}

static int tls_test_peer_handshake(struct tls_test_peer *peer,
                                   uint64_t deadline)
{
	uint32_t ready = 0U;
	for (;;) {
		int status = tr_byte_endpoint_step(&peer->endpoint, ready,
		                                   tls_test_now_ns());
		ready = 0U;
		if (status != TR_AGAIN)
			return status;
		status = tls_test_peer_wait(
		        peer, tr_byte_endpoint_interest(&peer->endpoint),
		        deadline, &ready);
		if (status != TR_OK)
			return status;
	}
}

static int tls_test_peer_write_all(struct tls_test_peer *peer,
                                   const unsigned char *bytes, size_t length,
                                   uint64_t deadline)
{
	size_t offset = 0U;
	while (offset < length) {
		uint32_t ready = 0U;
		for (;;) {
			size_t written = 0U;
			uint32_t wait_events = 0U;
			int status = tr_byte_endpoint_write(
			        &peer->endpoint, bytes + offset,
			        length - offset, ready, &written, &wait_events);
			ready = 0U;
			if (status == TR_OK) {
				offset += written;
				break;
			}
			if (status != TR_AGAIN)
				return status;
			status = tls_test_peer_wait(peer, wait_events, deadline,
			                            &ready);
			if (status != TR_OK)
				return status;
		}
	}
	return TR_OK;
}

static int tls_test_peer_read_all(struct tls_test_peer *peer,
                                  unsigned char *bytes, size_t length,
                                  uint64_t deadline)
{
	size_t offset = 0U;
	while (offset < length) {
		uint32_t ready = 0U;
		for (;;) {
			size_t read_bytes = 0U;
			uint32_t wait_events = 0U;
			int status = tr_byte_endpoint_read(
			        &peer->endpoint, bytes + offset,
			        length - offset, ready, &read_bytes,
			        &wait_events);
			ready = 0U;
			if (status == TR_OK) {
				offset += read_bytes;
				break;
			}
			if (status != TR_AGAIN)
				return status;
			status = tls_test_peer_wait(peer, wait_events, deadline,
			                            &ready);
			if (status != TR_OK)
				return status;
		}
	}
	return TR_OK;
}

static int tls_test_write_resumption_session(struct tls_test_peer *peer)
{
	SSL *ssl = tr_tls_openssl_session_ssl_for_test(
	        (struct tr_tls_openssl_session *)peer->endpoint.driver_arg);
	SSL_SESSION *session = NULL;
	unsigned char *encoded = NULL;
	int encoded_length;
	int fd = -1;
	size_t offset = 0U;
	int status = -1;
	if (!ssl)
		return -1;
	session = SSL_get1_session(ssl);
	if (!session || SSL_SESSION_has_ticket(session) != 1 ||
	    SSL_SESSION_get_max_early_data(session) != 0U) {
		fprintf(stderr,
		        "TLS ticket missing: session=%p has_ticket=%d "
		        "max_early=%u\n",
		        (void *)session,
		        session ? SSL_SESSION_has_ticket(session) : -1,
		        session ? SSL_SESSION_get_max_early_data(session) : 0U);
		goto cleanup;
	}
	encoded_length = i2d_SSL_SESSION(session, &encoded);
	if (encoded_length <= 0 || !encoded) {
		fprintf(stderr, "TLS session DER encoding failed: length=%d\n",
		        encoded_length);
		goto cleanup;
	}
	fd = open(peer->session_file, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
	          S_IRUSR | S_IWUSR);
	if (fd < 0) {
		fprintf(stderr, "TLS session file open failed: errno=%d\n",
		        errno);
		goto cleanup;
	}
	while (offset < (size_t)encoded_length) {
		ssize_t written = write(fd, encoded + offset,
		                        (size_t)encoded_length - offset);
		if (written > 0) {
			offset += (size_t)written;
			continue;
		}
		if (written < 0 && errno == EINTR)
			continue;
		fprintf(stderr, "TLS session file write failed: errno=%d\n",
		        errno);
		goto cleanup;
	}
	if (close(fd) != 0) {
		fd = -1;
		goto cleanup;
	}
	fd = -1;
	status = 0;

cleanup:
	if (fd >= 0)
		(void)close(fd);
	OPENSSL_free(encoded);
	SSL_SESSION_free(session);
	return status;
}

static SSL_SESSION *tls_test_read_resumption_session(const char *session_file)
{
	struct stat info;
	unsigned char *encoded = NULL;
	const unsigned char *cursor;
	SSL_SESSION *session = NULL;
	int fd = -1;
	size_t offset = 0U;
	if (stat(session_file, &info) != 0 || info.st_size <= 0 ||
	    (uintmax_t)info.st_size > UINT32_MAX)
		return NULL;
	encoded = (unsigned char *)malloc((size_t)info.st_size);
	if (!encoded)
		return NULL;
	fd = open(session_file, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		goto cleanup;
	while (offset < (size_t)info.st_size) {
		ssize_t read_bytes = read(fd, encoded + offset,
		                          (size_t)info.st_size - offset);
		if (read_bytes > 0) {
			offset += (size_t)read_bytes;
			continue;
		}
		if (read_bytes < 0 && errno == EINTR)
			continue;
		goto cleanup;
	}
	cursor = encoded;
	session = d2i_SSL_SESSION(NULL, &cursor, (long)info.st_size);
	if (!session || cursor != encoded + info.st_size) {
		SSL_SESSION_free(session);
		session = NULL;
	}

cleanup:
	if (fd >= 0)
		(void)close(fd);
	free(encoded);
	return session;
}

static int tls_test_peer_send_early_data(struct tls_test_peer *peer,
                                         uint64_t deadline)
{
	static const unsigned char marker[] = "tls-b-forbidden-early-data";
	SSL *ssl = tr_tls_openssl_session_ssl_for_test(
	        (struct tr_tls_openssl_session *)peer->endpoint.driver_arg);
	SSL_SESSION *session =
	        tls_test_read_resumption_session(peer->session_file);
	size_t offset = 0U;
	if (!ssl || !session || SSL_SESSION_has_ticket(session) != 1 ||
	    SSL_SESSION_get_max_early_data(session) != 0U ||
	    SSL_SESSION_set_max_early_data(session, sizeof(marker) - 1U) != 1 ||
	    SSL_set_session(ssl, session) != 1) {
		SSL_SESSION_free(session);
		return TR_ERR_INVALID;
	}
	SSL_SESSION_free(session);
	while (offset < sizeof(marker) - 1U) {
		uint32_t wait_events = 0U;
		int operation;
		int error;
		size_t written = 0U;
		ERR_clear_error();
		operation = SSL_write_early_data(ssl, marker + offset,
		                                 sizeof(marker) - 1U - offset,
		                                 &written);
		if (operation == 1) {
			if (written == 0U ||
			    written > sizeof(marker) - 1U - offset)
				return TR_ERR_STATE;
			offset += written;
			continue;
		}
		error = SSL_get_error(ssl, operation);
		if (error == SSL_ERROR_WANT_READ)
			wait_events = TR_BYTE_ENDPOINT_EVENT_READ;
		else if (error == SSL_ERROR_WANT_WRITE)
			wait_events = TR_BYTE_ENDPOINT_EVENT_WRITE;
		else
			return TR_ERR_SYS;
		{
			uint32_t ready;
			int status = tls_test_peer_wait(peer, wait_events,
			                                deadline, &ready);
			if (status != TR_OK)
				return status;
		}
	}
	return TR_OK;
}

static int tls_test_peer_shutdown(struct tls_test_peer *peer,
                                  uint64_t deadline);
static int tls_test_peer_wait_for_close(struct tls_test_peer *peer,
                                        uint64_t deadline);
static void tls_test_peer_cleanup(struct tls_test_peer *peer);

static int tls_test_early_client(const char *directory, unsigned short port)
{
	static const unsigned char application_marker[] =
	        "c-trpc tls-b authenticated application marker";
	static const unsigned char reply_marker[] = "tls-b acknowledged";
	struct tr_tls_openssl_profile *profile = NULL;
	int status = tls_test_peer_profile_create("client", "early", directory,
	                                          &profile);
	if (status != TR_OK)
		return 20;
	for (unsigned connection = 0U; connection < 2U; ++connection) {
		struct tls_test_peer peer = {.fd = -1};
		uint64_t deadline = tls_test_now_ns() + UINT64_C(8000000000);
		int fd = tls_test_peer_connect(port, deadline);
		if (fd < 0) {
			status = TR_ERR_SYS;
			break;
		}
		status = tls_test_peer_open(&peer, "client", "early", directory,
		                            fd, deadline, profile);
		if (status == TR_OK && connection == 0U) {
			status = tls_test_peer_handshake(&peer, deadline);
			if (status == TR_OK)
				status = tls_test_peer_write_all(
				        &peer, application_marker,
				        sizeof(application_marker) - 1U,
				        deadline);
			if (status == TR_OK) {
				unsigned char reply[sizeof(reply_marker) - 1U];
				status = tls_test_peer_read_all(
				        &peer, reply, sizeof(reply), deadline);
				if (status == TR_OK &&
				    memcmp(reply, reply_marker,
				           sizeof(reply)) != 0)
					status = TR_ERR_STATE;
			}
			if (status == TR_OK &&
			    tls_test_write_resumption_session(&peer) != 0)
				status = TR_ERR_STATE;
			if (status == TR_OK)
				status =
				        tls_test_peer_shutdown(&peer, deadline);
		} else if (status == TR_OK) {
			status = tls_test_peer_send_early_data(&peer, deadline);
			if (status == TR_OK) {
				status = tls_test_peer_handshake(&peer,
				                                 deadline);
			}
			if (status == TR_OK) {
				SSL *ssl = tr_tls_openssl_session_ssl_for_test(
				        (struct tr_tls_openssl_session *)
				                peer.endpoint.driver_arg);
				if (!ssl || SSL_session_reused(ssl) != 1 ||
				    SSL_get_early_data_status(ssl) !=
				            SSL_EARLY_DATA_REJECTED)
					status = TR_ERR_STATE;
			}
		}
		tls_test_peer_cleanup(&peer);
		if (status != TR_OK)
			break;
	}
	tr_tls_openssl_profile_release(profile);
	return status == TR_OK ? 0 : 21;
}

static int tls_test_early_server(const char *directory, int listener_fd)
{
	static const unsigned char application_marker[] =
	        "c-trpc tls-b authenticated application marker";
	static const unsigned char reply_marker[] = "tls-b acknowledged";
	struct tr_tls_openssl_profile *profile = NULL;
	int status = tls_test_peer_profile_create("server", "early", directory,
	                                          &profile);
	if (status != TR_OK)
		return 30;
	for (unsigned connection = 0U; connection < 2U; ++connection) {
		struct tls_test_peer peer = {.fd = -1};
		uint64_t deadline = tls_test_now_ns() + UINT64_C(8000000000);
		int fd = tls_test_peer_accept(listener_fd, deadline);
		if (fd < 0) {
			status = TR_ERR_TIMEOUT;
			break;
		}
		status = tls_test_peer_open(&peer, "server", "early", directory,
		                            fd, deadline, profile);
		if (status == TR_OK)
			status = tls_test_peer_handshake(&peer, deadline);
		if (status == TR_OK && connection == 0U) {
			unsigned char received[sizeof(application_marker) - 1U];
			status = tls_test_peer_read_all(
			        &peer, received, sizeof(received), deadline);
			if (status == TR_OK &&
			    memcmp(received, application_marker,
			           sizeof(received)) != 0)
				status = TR_ERR_STATE;
			if (status == TR_OK)
				status = tls_test_peer_write_all(
				        &peer, reply_marker,
				        sizeof(reply_marker) - 1U, deadline);
			if (status == TR_OK)
				status = tls_test_peer_wait_for_close(&peer,
				                                      deadline);
		} else if (status != TR_OK && connection == 1U) {
			if (status != TR_ERR_TIMEOUT &&
			    peer.endpoint.state == TR_BYTE_ENDPOINT_CLOSING &&
			    peer.endpoint.terminal_status < 0)
				status = TR_OK;
		} else if (status == TR_OK) {
			/* A server that authenticates must still never expose
			 * 0-RTT bytes. */
			static const unsigned char early_marker[] =
			        "tls-b-forbidden-early-data";
			unsigned char received[sizeof(early_marker) - 1U];
			status = tls_test_peer_read_all(
			        &peer, received, sizeof(received),
			        tls_test_now_ns() + UINT64_C(3000000000));
			if (status == TR_OK)
				status = TR_ERR_STATE;
			else if (status == TR_ERR_SYS ||
			         status == TR_ERR_CLOSED)
				status = TR_OK;
		}
		tls_test_peer_cleanup(&peer);
		if (status != TR_OK)
			break;
	}
	tr_tls_openssl_profile_release(profile);
	return status == TR_OK ? 0 : 31;
}

static int tls_test_peer_shutdown(struct tls_test_peer *peer, uint64_t deadline)
{
	int status =
	        tr_byte_endpoint_begin_close(&peer->endpoint, TR_OK, deadline);
	uint32_t ready = 0U;
	if (status != TR_OK)
		return status;
	while (!peer->endpoint.shutdown_done) {
		status = tr_byte_endpoint_step(&peer->endpoint, ready,
		                               tls_test_now_ns());
		ready = 0U;
		if (status < 0 && status != TR_AGAIN)
			return status;
		if (peer->endpoint.shutdown_done)
			break;
		status = tls_test_peer_wait(
		        peer, tr_byte_endpoint_interest(&peer->endpoint),
		        deadline, &ready);
		if (status != TR_OK)
			return status;
	}
	return TR_OK;
}

static int tls_test_peer_wait_for_close(struct tls_test_peer *peer,
                                        uint64_t deadline)
{
	unsigned char byte;
	for (;;) {
		uint32_t ready = 0U;
		size_t read_bytes = 0U;
		uint32_t wait_events = 0U;
		int status = tr_byte_endpoint_read(&peer->endpoint, &byte,
		                                   sizeof(byte), ready,
		                                   &read_bytes, &wait_events);
		if (status == TR_ERR_CLOSED)
			return tls_test_peer_shutdown(peer, deadline);
		if (status == TR_OK)
			return TR_ERR_STATE;
		if (status != TR_AGAIN)
			return status;
		status =
		        tls_test_peer_wait(peer, wait_events, deadline, &ready);
		if (status != TR_OK)
			return status;
		/* read() must retry the same storage after WANT. */
		for (;;) {
			status = tr_byte_endpoint_read(
			        &peer->endpoint, &byte, sizeof(byte), ready,
			        &read_bytes, &wait_events);
			if (status == TR_ERR_CLOSED)
				return tls_test_peer_shutdown(peer, deadline);
			if (status == TR_OK)
				return TR_ERR_STATE;
			if (status != TR_AGAIN)
				return status;
			status = tls_test_peer_wait(peer, wait_events, deadline,
			                            &ready);
			if (status != TR_OK)
				return status;
		}
	}
}

static void tls_test_peer_cleanup(struct tls_test_peer *peer)
{
	if (peer->endpoint.state == TR_BYTE_ENDPOINT_NEGOTIATING ||
	    peer->endpoint.state == TR_BYTE_ENDPOINT_AUTHENTICATED ||
	    (peer->endpoint.state == TR_BYTE_ENDPOINT_CLOSING &&
	     !peer->endpoint.shutdown_done))
		(void)tr_byte_endpoint_abort(&peer->endpoint, TR_ERR_CLOSED);
	if (peer->endpoint.events_bound)
		(void)tr_byte_endpoint_event_unbind(&peer->endpoint);
	if (peer->endpoint.state == TR_BYTE_ENDPOINT_DETACHED)
		(void)tr_byte_endpoint_release(&peer->endpoint);
	if (peer->session)
		tr_tls_openssl_driver()->release(peer->session);
	tr_tls_openssl_profile_release(peer->profile);
	if (peer->fd >= 0)
		(void)close(peer->fd);
	memset(peer, 0, sizeof(*peer));
	peer->fd = -1;
}

static int tls_test_raw_tls12_client(unsigned short port, uint64_t deadline)
{
	SSL_CTX *context = NULL;
	SSL *ssl = NULL;
	int fd = tls_test_peer_connect(port, deadline);
	int result = 1;
	if (fd < 0)
		return -1;
	context = SSL_CTX_new(TLS_client_method());
	if (!context ||
	    SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1 ||
	    SSL_CTX_set_max_proto_version(context, TLS1_2_VERSION) != 1)
		goto cleanup;
	SSL_CTX_set_verify(context, SSL_VERIFY_NONE, NULL);
	ssl = SSL_new(context);
	if (!ssl || SSL_set_fd(ssl, fd) != 1)
		goto cleanup;
	SSL_set_connect_state(ssl);
	for (;;) {
		int operation;
		int error;
		short events;
		struct pollfd descriptor;
		ERR_clear_error();
		operation = SSL_connect(ssl);
		if (operation == 1) {
			result = 1;
			break;
		}
		error = SSL_get_error(ssl, operation);
		if (error != SSL_ERROR_WANT_READ &&
		    error != SSL_ERROR_WANT_WRITE) {
			result = 0;
			break;
		}
		events = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
		descriptor.fd = fd;
		descriptor.events = events;
		descriptor.revents = 0;
		if (poll(&descriptor, 1U, 100) < 0 && errno != EINTR)
			goto cleanup;
		if (tls_test_now_ns() >= deadline)
			goto cleanup;
	}

cleanup:
	SSL_free(ssl);
	SSL_CTX_free(context);
	(void)close(fd);
	return result;
}

static int tls_test_peer_child(const char *role, const char *case_name,
                               unsigned short port, int listener_fd,
                               const char *directory)
{
	static const unsigned char application_marker[] =
	        "c-trpc tls-b authenticated application marker";
	static const unsigned char reply_marker[] = "tls-b acknowledged";
	struct tls_test_peer peer = {.fd = -1};
	uint64_t deadline = tls_test_now_ns() + UINT64_C(8000000000);
	int fd = -1;
	int status;
	int is_client = strcmp(role, "client") == 0;
	int is_positive = strcmp(case_name, "dns") == 0 ||
	                  strcmp(case_name, "ip") == 0 ||
	                  strcmp(case_name, "sni") == 0;

	if (strcmp(case_name, "early") == 0)
		return is_client
		               ? tls_test_early_client(directory, port)
		               : tls_test_early_server(directory, listener_fd);

	if (is_client && strcmp(case_name, "plaintext") == 0) {
		static const unsigned char plaintext[] =
		        "TRPC-PLAINTEXT-DOWNGRADE-MARKER";
		fd = tls_test_peer_connect(port, deadline);
		if (fd < 0)
			return 1;
		status = (int)send(fd, plaintext, sizeof(plaintext) - 1U,
		                   MSG_NOSIGNAL);
		(void)close(fd);
		return status == (int)(sizeof(plaintext) - 1U) ? 0 : 1;
	}
	if (is_client && strcmp(case_name, "eof") == 0) {
		fd = tls_test_peer_connect(port, deadline);
		if (fd < 0)
			return 1;
		(void)shutdown(fd, SHUT_WR);
		(void)close(fd);
		return 0;
	}
	if (is_client && strcmp(case_name, "tls12") == 0)
		return tls_test_raw_tls12_client(port, deadline) == 0 ? 0 : 1;

	if (is_client) {
		fd = tls_test_peer_connect(port, deadline);
	} else {
		fd = tls_test_peer_accept(listener_fd, deadline);
	}
	if (fd < 0)
		return 2;
	status = tls_test_peer_open(&peer, role, case_name, directory, fd,
	                            deadline, NULL);
	if (status != TR_OK) {
		tls_test_peer_cleanup(&peer);
		return 3;
	}
	status = tls_test_peer_handshake(&peer, deadline);
	if (!is_positive) {
		if (is_client) {
			if (status != TR_OK && status != TR_ERR_TIMEOUT) {
				tls_test_peer_cleanup(&peer);
				return 0;
			}
			if (status == TR_OK)
				(void)tls_test_peer_write_all(
				        &peer, application_marker,
				        sizeof(application_marker) - 1U,
				        deadline);
			tls_test_peer_cleanup(&peer);
			return 4;
		}
		if (status != TR_OK) {
			tls_test_peer_cleanup(&peer);
			return status == TR_ERR_TIMEOUT ? 5 : 0;
		}
		{
			unsigned char received[sizeof(application_marker) - 1U];
			status = tls_test_peer_read_all(
			        &peer, received, sizeof(received),
			        tls_test_now_ns() + UINT64_C(3000000000));
		}
		tls_test_peer_cleanup(&peer);
		return status == TR_OK || status == TR_ERR_TIMEOUT ? 6 : 0;
	}
	if (status != TR_OK) {
		tls_test_peer_cleanup(&peer);
		return 7;
	}
	if (is_client) {
		unsigned char reply[sizeof(reply_marker) - 1U];
		status = tls_test_peer_write_all(
		        &peer, application_marker,
		        sizeof(application_marker) - 1U, deadline);
		if (status == TR_OK)
			status = tls_test_peer_read_all(
			        &peer, reply, sizeof(reply), deadline);
		if (status != TR_OK)
			fprintf(stderr,
			        "TLS TCP client/%s exchange status %d\n",
			        case_name, status);
		if (status == TR_OK &&
		    memcmp(reply, reply_marker, sizeof(reply)) != 0) {
			fprintf(stderr, "TLS reply mismatch for %s: %.*s\n",
			        case_name, (int)sizeof(reply),
			        (const char *)reply);
			status = TR_ERR_STATE;
		}
		if (status != TR_OK)
			fprintf(stderr, "TLS TCP client/%s save/status %d\n",
			        case_name, status);
		if (status == TR_OK)
			status = tls_test_peer_shutdown(&peer, deadline);
	} else {
		unsigned char received[sizeof(application_marker) - 1U];
		status = tls_test_peer_read_all(&peer, received,
		                                sizeof(received), deadline);
		if (status == TR_OK &&
		    memcmp(received, application_marker, sizeof(received)) != 0)
			status = TR_ERR_STATE;
		if (status == TR_OK)
			status = tls_test_peer_write_all(
			        &peer, reply_marker, sizeof(reply_marker) - 1U,
			        deadline);
		if (status == TR_OK)
			status = tls_test_peer_wait_for_close(&peer, deadline);
	}
	tls_test_peer_cleanup(&peer);
	if (status != TR_OK)
		fprintf(stderr, "TLS TCP %s/%s failed with status %d\n", role,
		        case_name, status);
	return status == TR_OK ? 0 : 8;
}

static int tls_test_peer_child_args(int argc, char **argv)
{
	char *end = NULL;
	unsigned long port;
	long listener_fd;
	if (argc != 7 || strcmp(argv[1], "--tls-peer") != 0)
		return -1;
	port = strtoul(argv[4], &end, 10);
	if (!end || *end != '\0' || port == 0U || port > 65535U)
		return 9;
	listener_fd = strtol(argv[5], &end, 10);
	if (!end || *end != '\0' || listener_fd < -1 || listener_fd > INT32_MAX)
		return 9;
	return tls_test_peer_child(argv[2], argv[3], (unsigned short)port,
	                           (int)listener_fd, argv[6]);
}

static pid_t tls_test_spawn_peer(const char *executable, const char *role,
                                 const char *case_name, unsigned short port,
                                 int listener_fd, const char *directory)
{
	char port_text[16];
	char fd_text[16];
	pid_t child;
	(void)snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);
	(void)snprintf(fd_text, sizeof(fd_text), "%d", listener_fd);
	child = fork();
	if (child != 0)
		return child;
	execl(executable, executable, "--tls-peer", role, case_name, port_text,
	      fd_text, directory, (char *)NULL);
	_exit(127);
}

static int tls_test_wait_children(pid_t server_pid, pid_t client_pid,
                                  uint64_t deadline)
{
	int server_status = 0;
	int client_status = 0;
	int server_done = 0;
	int client_done = 0;
	for (;;) {
		pid_t result;
		if (!server_done) {
			result = waitpid(server_pid, &server_status, WNOHANG);
			if (result == server_pid)
				server_done = 1;
			else if (result < 0 && errno != EINTR)
				goto fail;
		}
		if (!client_done) {
			result = waitpid(client_pid, &client_status, WNOHANG);
			if (result == client_pid)
				client_done = 1;
			else if (result < 0 && errno != EINTR)
				goto fail;
		}
		if (server_done && client_done) {
			if (WIFEXITED(server_status) &&
			    WEXITSTATUS(server_status) == 0 &&
			    WIFEXITED(client_status) &&
			    WEXITSTATUS(client_status) == 0)
				return 0;
			fprintf(stderr,
			        "TLS TCP child failure: server=%s%d "
			        "client=%s%d\n",
			        WIFEXITED(server_status) ? "exit " : "signal ",
			        WIFEXITED(server_status)
			                ? WEXITSTATUS(server_status)
			                : WTERMSIG(server_status),
			        WIFEXITED(client_status) ? "exit " : "signal ",
			        WIFEXITED(client_status)
			                ? WEXITSTATUS(client_status)
			                : WTERMSIG(client_status));
			return -1;
		}
		if (tls_test_now_ns() >= deadline)
			goto fail;
		{
			struct timespec pause = {.tv_sec = 0,
			                         .tv_nsec = 10000000L};
			(void)nanosleep(&pause, NULL);
		}
	}

fail:
	if (!server_done)
		(void)kill(server_pid, SIGKILL);
	if (!client_done)
		(void)kill(client_pid, SIGKILL);
	if (!server_done)
		(void)waitpid(server_pid, &server_status, 0);
	if (!client_done)
		(void)waitpid(client_pid, &client_status, 0);
	return -1;
}

static int tls_test_tcp_case(const char *case_name)
{
	struct sockaddr_in address = {
	        .sin_family = AF_INET,
	        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
	};
	char executable[4096];
	socklen_t address_length = sizeof(address);
	int listener_fd = -1;
	int descriptor_flags;
	ssize_t executable_length;
	pid_t server_pid = -1;
	pid_t client_pid = -1;
	int status = -1;

	listener_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (listener_fd < 0 ||
	    bind(listener_fd, (const struct sockaddr *)&address,
	         sizeof(address)) != 0 ||
	    listen(listener_fd, 4) != 0 ||
	    getsockname(listener_fd, (struct sockaddr *)&address,
	                &address_length) != 0)
		goto cleanup;
	executable_length =
	        readlink("/proc/self/exe", executable, sizeof(executable) - 1U);
	if (executable_length <= 0 ||
	    (size_t)executable_length >= sizeof(executable) - 1U)
		goto cleanup;
	executable[executable_length] = '\0';
	server_pid = tls_test_spawn_peer(executable, "server", case_name,
	                                 ntohs(address.sin_port), listener_fd,
	                                 test_identity.directory);
	if (server_pid < 0)
		goto cleanup;
	descriptor_flags = fcntl(listener_fd, F_GETFD);
	if (descriptor_flags < 0 ||
	    fcntl(listener_fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0)
		goto cleanup;
	client_pid = tls_test_spawn_peer(executable, "client", case_name,
	                                 ntohs(address.sin_port), -1,
	                                 test_identity.directory);
	if (client_pid < 0)
		goto cleanup;
	(void)close(listener_fd);
	listener_fd = -1;
	status = tls_test_wait_children(server_pid, client_pid,
	                                tls_test_now_ns() +
	                                        UINT64_C(15000000000));
	server_pid = -1;
	client_pid = -1;

cleanup:
	if (listener_fd >= 0)
		(void)close(listener_fd);
	if (server_pid > 0)
		(void)kill(server_pid, SIGKILL);
	if (client_pid > 0)
		(void)kill(client_pid, SIGKILL);
	if (server_pid > 0)
		(void)waitpid(server_pid, NULL, 0);
	if (client_pid > 0)
		(void)waitpid(client_pid, NULL, 0);
	return status;
}

static void test_cross_process_tls13_authentication_and_fail_closed(void)
{
	static const char *const cases[] = {
	        "dns",        "ip",       "sni",       "unknown-ca",
	        "wrong-dns",  "wrong-ip", "cn-only",   "expired",
	        "client-eku", "tls12",    "plaintext", "eof",
	};
	for (size_t index = 0U; index < sizeof(cases) / sizeof(cases[0]);
	     ++index)
		assert(tls_test_tcp_case(cases[index]) == 0);
	assert(tls_test_tcp_case("early") == 0);
}

int main(int argc, char **argv)
{
	int child_result = tls_test_peer_child_args(argc, argv);
	if (child_result >= 0)
		return child_result;
	atexit(cleanup_test_identity);
	atexit(tls_test_pair_cleanup);
	assert(tls_test_identity_init(&test_identity, "tls-b-endpoint") == 0);
	test_in_memory_pem_profiles();
	test_blocking_socket_is_rejected_without_taking_ownership();
	test_profiles_pin_peer_certificate_chain_limit();
	test_oversized_peer_certificate_chain_is_rejected();
	test_profile_generation_overflow_is_rejected();
	test_handshake_maps_want_write_then_want_read();
	test_required_client_auth_rejects_missing_certificate();
	test_required_client_auth_accepts_valid_certificate();
	test_optional_client_auth_accepts_missing_certificate();
	test_optional_client_auth_validates_present_certificate();
	test_rotated_client_key_keeps_stable_identity();
	test_required_client_auth_rejects_wrong_ca();
	test_optional_client_auth_still_rejects_wrong_ca();
	test_client_identity_requires_one_bounded_uri_san();
	test_client_auth_rejects_server_only_eku();
	test_nonblocking_tls_pair_authenticates_and_preserves_fds();
	test_unexpected_eof_is_not_close_notify();
	test_cross_process_tls13_authentication_and_fail_closed();
	return 0;
}
