#define _POSIX_C_SOURCE 200809L

#include <tr/client.h>
#include <tr/rpc_codec.h>
#include <tr/rpc_v2.h>
#include <tr/trpc.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <gnutls/gnutls.h>
#include <pthread.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "tls_test_identity.h"

extern char **environ;

#define TEST_WAIT_MS 12000U
#define TEST_CONNECT_TIMEOUT_MS 3000U
#define TEST_SERVICE_ID 0x234U
#define TEST_METHOD_ID 1U
static const uint8_t test_rpc_payload[] = {0x43U, 0x54U, 0x52U, 0x50U};

struct test_state {
	struct tr_server *server;
	struct tr_server *rpc_server;
	struct tls_test_identity identity;
	uint16_t port;
	uint16_t rpc_port;
};

struct test_rpc_result {
	atomic_int message_count;
	atomic_int response_matches;
	atomic_int terminal;
	atomic_int status;
};

static enum tr_rpc_status test_work(struct tr_rpc_v2_call_id call,
                                    const struct tr_rpc_v2_request *request,
                                    const struct tr_rpc_v2_cancel *cancel,
                                    struct tr_rpc_v2_reply *reply, void *arg)
{
	(void)call;
	(void)request;
	(void)cancel;
	(void)reply;
	(void)arg;
	return TR_RPC_STATUS_OK;
}

static enum tr_rpc_status test_rpc_work(struct tr_rpc_v2_call_id call,
                                        const struct tr_rpc_v2_request *request,
                                        const struct tr_rpc_v2_cancel *cancel,
                                        struct tr_rpc_v2_reply *reply,
                                        void *arg)
{
	(void)call;
	(void)cancel;
	(void)arg;
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE &&
	    tr_rpc_v2_reply_append(reply, &request->body) != TR_OK)
		return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	return TR_RPC_STATUS_OK;
}

static void test_rpc_client_message(struct tr_rpc_v2_call call,
                                    const struct tr_rpc_bytes *body, void *arg)
{
	struct test_rpc_result *result = arg;
	(void)call;
	atomic_store_explicit(&result->response_matches,
	                      body && body->data &&
	                              body->len == sizeof(test_rpc_payload) &&
	                              memcmp(body->data, test_rpc_payload,
	                                     sizeof(test_rpc_payload)) == 0,
	                      memory_order_relaxed);
	atomic_fetch_add_explicit(&result->message_count, 1,
	                          memory_order_relaxed);
}

static void test_rpc_client_terminal(struct tr_rpc_v2_call call,
                                     enum tr_rpc_status status,
                                     enum tr_rpc_v2_delivery delivery,
                                     void *arg)
{
	struct test_rpc_result *result = arg;
	(void)call;
	(void)delivery;
	atomic_store_explicit(&result->status, (int)status,
	                      memory_order_relaxed);
	atomic_store_explicit(&result->terminal, 1, memory_order_release);
}

static void test_state_init(struct test_state *state)
{
	struct tr_rpc_v2_method method;
	struct tr_rpc_method_desc descriptor;
	struct tr_server_config config;
	struct tr_rpc_v2_method rpc_method;
	struct tr_rpc_method_desc rpc_descriptor;

	memset(state, 0, sizeof(*state));
	assert(tls_test_identity_init(&state->identity, "tls-gnutls-interop") ==
	       0);
	memset(&method, 0, sizeof(method));
	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.service_id = TEST_SERVICE_ID;
	descriptor.method_id = TEST_METHOD_ID;
	descriptor.request_cardinality = TR_RPC_ONE;
	descriptor.response_cardinality = TR_RPC_ONE;
	descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	descriptor.lane = TR_RPC_LANE_CONTROL;
	descriptor.max_request_bytes = 1U;
	descriptor.max_response_bytes = 1U;
	method.descriptor = descriptor;
	method.work = test_work;

	tr_server_config_init(&config);
	config.max_peers = 4U;
	config.handshake_timeout_ms = 5000U;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.certificate_file = state->identity.server_certificate_file;
	config.tls.private_key_file = state->identity.server_private_key_file;
	config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	config.tls.client_ca_file = state->identity.ca_file;
	assert(tr_server_create(&config, &method, 1U, &state->server) == TR_OK);
	assert(tr_server_listen(state->server, "127.0.0.1", 0, &state->port) ==
	       TR_OK);
	assert(tr_server_start(state->server) == TR_OK);

	memset(&rpc_method, 0, sizeof(rpc_method));
	memset(&rpc_descriptor, 0, sizeof(rpc_descriptor));
	rpc_descriptor.service_id = TEST_SERVICE_ID;
	rpc_descriptor.method_id = TEST_METHOD_ID;
	rpc_descriptor.request_cardinality = TR_RPC_ONE;
	rpc_descriptor.response_cardinality = TR_RPC_ONE;
	rpc_descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	rpc_descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	rpc_descriptor.lane = TR_RPC_LANE_CONTROL;
	rpc_descriptor.max_request_bytes = sizeof(test_rpc_payload);
	rpc_descriptor.max_response_bytes = sizeof(test_rpc_payload);
	rpc_method.descriptor = rpc_descriptor;
	rpc_method.work = test_rpc_work;
	tr_server_config_init(&config);
	config.max_peers = 4U;
	assert(tr_server_create(&config, &rpc_method, 1U, &state->rpc_server) ==
	       TR_OK);
	assert(tr_server_listen(state->rpc_server, "127.0.0.1", 0,
	                        &state->rpc_port) == TR_OK);
	assert(tr_server_start(state->rpc_server) == TR_OK);
}

static int child_wait(pid_t child, int *out_status)
{
	struct timespec start;
	assert(clock_gettime(CLOCK_MONOTONIC, &start) == 0);
	for (;;) {
		int status;
		pid_t result = waitpid(child, &status, WNOHANG);
		struct timespec now;
		uint64_t elapsed_ms;
		if (result == child) {
			*out_status = status;
			return 0;
		}
		if (result < 0 && errno != EINTR)
			return -1;
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		elapsed_ms = (uint64_t)(now.tv_sec - start.tv_sec) * 1000U;
		if (now.tv_nsec >= start.tv_nsec)
			elapsed_ms += (uint64_t)(now.tv_nsec - start.tv_nsec) /
			              1000000U;
		else
			elapsed_ms -= (uint64_t)(start.tv_nsec - now.tv_nsec) /
			              1000000U;
		if (elapsed_ms >= TEST_WAIT_MS) {
			(void)kill(child, SIGKILL);
			while (waitpid(child, &status, 0) < 0 &&
			       errno == EINTR) {
			}
			return -2;
		}
		{
			const struct timespec pause = {0, 10000000L};
			while (nanosleep(&pause, NULL) != 0 && errno == EINTR) {
			}
		}
	}
}

static int run_gnutls_client(const struct test_state *state,
                             const char *client_certificate,
                             const char *client_private_key,
                             const char *verify_hostname)
{
	posix_spawn_file_actions_t actions;
	char port[16];
	char *arguments[20];
	pid_t child;
	int argument_count = 0;
	int status;
	int result;

	result = snprintf(port, sizeof(port), "%u", (unsigned)state->port);
	assert(result > 0 && (size_t)result < sizeof(port));
	arguments[argument_count++] = (char *)"gnutls-cli";
	arguments[argument_count++] = (char *)"--priority";
	arguments[argument_count++] = (char *)"NORMAL:-VERS-ALL:+VERS-TLS1.3";
	arguments[argument_count++] = (char *)"--port";
	arguments[argument_count++] = port;
	arguments[argument_count++] = (char *)"--x509cafile";
	arguments[argument_count++] = (char *)state->identity.ca_file;
	arguments[argument_count++] = (char *)"--verify-hostname";
	arguments[argument_count++] = (char *)verify_hostname;
	arguments[argument_count++] = (char *)"--sni-hostname";
	arguments[argument_count++] = (char *)"server.example.test";
	if (client_certificate && client_private_key) {
		arguments[argument_count++] = (char *)"--x509certfile";
		arguments[argument_count++] = (char *)client_certificate;
		arguments[argument_count++] = (char *)"--x509keyfile";
		arguments[argument_count++] = (char *)client_private_key;
	}
	arguments[argument_count++] = (char *)"127.0.0.1";
	arguments[argument_count] = NULL;

	assert(posix_spawn_file_actions_init(&actions) == 0);
	assert(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO,
	                                        "/dev/null", O_RDONLY, 0) == 0);
	assert(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO,
	                                        "/dev/null", O_WRONLY, 0) == 0);
	assert(posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO,
	                                        STDERR_FILENO) == 0);
	result = posix_spawnp(&child, arguments[0], &actions, NULL, arguments,
	                      environ);
	assert(posix_spawn_file_actions_destroy(&actions) == 0);
	if (result != 0) {
		fprintf(stderr, "tls-gnutls: cannot start gnutls-cli: %s\n",
		        strerror(result));
		return -1;
	}
	result = child_wait(child, &status);
	if (result != 0) {
		fprintf(stderr, "tls-gnutls: gnutls-cli wait failed: %d\n",
		        result);
		return -1;
	}
	if (!WIFEXITED(status))
		return -1;
	return WEXITSTATUS(status);
}

struct test_gnutls_server {
	const struct test_state *test;
	int listener_fd;
	uint16_t port;
	uint16_t backend_port;
	int result;
	int handshake_complete;
	int handshake_error;
	int client_certificate_verified;
	size_t client_plaintext_bytes;
	size_t backend_plaintext_bytes;
};

static int test_gnutls_send_exact(gnutls_session_t session,
                                  const uint8_t *buffer, size_t length)
{
	size_t sent = 0U;
	while (sent < length) {
		ssize_t amount = gnutls_record_send(session, buffer + sent,
		                                    length - sent);
		if (amount <= 0)
			return -1;
		sent += (size_t)amount;
	}
	return 0;
}

static int test_socket_send_exact(int fd, const uint8_t *buffer, size_t length)
{
	size_t sent = 0U;
	while (sent < length) {
		ssize_t amount =
		        send(fd, buffer + sent, length - sent, MSG_NOSIGNAL);
		if (amount < 0 && errno == EINTR)
			continue;
		if (amount <= 0)
			return -1;
		sent += (size_t)amount;
	}
	return 0;
}

static int test_gnutls_relay(struct test_gnutls_server *server,
                             gnutls_session_t session, int connection_fd,
                             int backend_fd)
{
	struct pollfd descriptors[2] = {
	        {.fd = connection_fd, .events = POLLIN},
	        {.fd = backend_fd, .events = POLLIN},
	};
	uint8_t buffer[16384];
	int client_eof = 0;
	int backend_eof = 0;

	while (!client_eof || !backend_eof) {
		int poll_result;
		descriptors[0].fd = client_eof ? -1 : connection_fd;
		descriptors[1].fd = backend_eof ? -1 : backend_fd;
		poll_result = poll(descriptors, 2U, (int)TEST_WAIT_MS);
		if (poll_result < 0 && errno == EINTR)
			continue;
		if (poll_result <= 0)
			return -1;

		if (!client_eof &&
		    (descriptors[0].revents & (POLLIN | POLLHUP | POLLERR))) {
			ssize_t amount = gnutls_record_recv(session, buffer,
			                                    sizeof(buffer));
			if (amount > 0) {
				server->client_plaintext_bytes +=
				        (size_t)amount;
				if (test_socket_send_exact(backend_fd, buffer,
				                           (size_t)amount) != 0)
					return -1;
			} else if (amount == 0 ||
			           amount == GNUTLS_E_PREMATURE_TERMINATION) {
				client_eof = 1;
				(void)shutdown(backend_fd, SHUT_WR);
			} else if (amount != GNUTLS_E_AGAIN &&
			           amount != GNUTLS_E_INTERRUPTED) {
				return -1;
			}
		}

		if (!backend_eof &&
		    (descriptors[1].revents & (POLLIN | POLLHUP | POLLERR))) {
			ssize_t amount =
			        recv(backend_fd, buffer, sizeof(buffer), 0);
			if (amount > 0) {
				server->backend_plaintext_bytes +=
				        (size_t)amount;
				if (test_gnutls_send_exact(session, buffer,
				                           (size_t)amount) != 0)
					return -1;
			} else if (amount == 0) {
				backend_eof = 1;
				(void)gnutls_bye(session, GNUTLS_SHUT_WR);
			} else if (errno != EINTR) {
				return -1;
			}
		}
	}
	return 0;
}

static void *test_gnutls_server_thread(void *arg)
{
	struct test_gnutls_server *server = arg;
	gnutls_certificate_credentials_t credentials = NULL;
	gnutls_session_t session = NULL;
	struct timeval timeout = {TEST_WAIT_MS / 1000U,
	                          (TEST_WAIT_MS % 1000U) * 1000U};
	struct sockaddr_in peer_address;
	socklen_t peer_length = sizeof(peer_address);
	unsigned int verification_status = 0U;
	int connection_fd = -1;
	int backend_fd = -1;
	int result = TR_ERR_SYS;

	connection_fd = accept(server->listener_fd,
	                       (struct sockaddr *)&peer_address, &peer_length);
	(void)close(server->listener_fd);
	server->listener_fd = -1;
	if (connection_fd < 0)
		goto done;
	(void)setsockopt(connection_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
	                 sizeof(timeout));
	(void)setsockopt(connection_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
	                 sizeof(timeout));
	if (gnutls_certificate_allocate_credentials(&credentials) < 0 ||
	    gnutls_certificate_set_x509_key_file(
	            credentials, server->test->identity.server_certificate_file,
	            server->test->identity.server_private_key_file,
	            GNUTLS_X509_FMT_PEM) < 0 ||
	    gnutls_certificate_set_x509_trust_file(
	            credentials, server->test->identity.ca_file,
	            GNUTLS_X509_FMT_PEM) <= 0 ||
	    gnutls_init(&session, GNUTLS_SERVER) < 0 ||
	    gnutls_priority_set_direct(session, "NORMAL:-VERS-ALL:+VERS-TLS1.3",
	                               NULL) < 0 ||
	    gnutls_credentials_set(session, GNUTLS_CRD_CERTIFICATE,
	                           credentials) < 0)
		goto done;
	gnutls_certificate_server_set_request(session, GNUTLS_CERT_REQUIRE);
	gnutls_transport_set_int(session, connection_fd);
	server->handshake_error = gnutls_handshake(session);
	if (server->handshake_error < 0)
		goto done;
	server->handshake_complete = 1;
	if (gnutls_certificate_verify_peers3(session, NULL,
	                                     &verification_status) < 0 ||
	    verification_status != 0U)
		goto done;
	server->client_certificate_verified = 1;
	backend_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (backend_fd < 0)
		goto done;
	(void)setsockopt(backend_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
	                 sizeof(timeout));
	(void)setsockopt(backend_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
	                 sizeof(timeout));
	memset(&peer_address, 0, sizeof(peer_address));
	peer_address.sin_family = AF_INET;
	peer_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	peer_address.sin_port = htons(server->backend_port);
	if (connect(backend_fd, (struct sockaddr *)&peer_address,
	            sizeof(peer_address)) != 0)
		goto done;
	result = test_gnutls_relay(server, session, connection_fd,
	                           backend_fd) == 0
	                 ? TR_OK
	                 : TR_ERR_SYS;

done:
	server->result = result;
	if (session)
		gnutls_deinit(session);
	if (credentials)
		gnutls_certificate_free_credentials(credentials);
	if (connection_fd >= 0)
		(void)close(connection_fd);
	if (backend_fd >= 0)
		(void)close(backend_fd);
	return NULL;
}

static int test_gnutls_server_listen(struct test_gnutls_server *server,
                                     const struct test_state *test)
{
	struct sockaddr_in address;
	socklen_t address_length = sizeof(address);
	server->listener_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server->listener_fd < 0)
		return -1;
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0;
	if (bind(server->listener_fd, (struct sockaddr *)&address,
	         sizeof(address)) != 0 ||
	    listen(server->listener_fd, 1) != 0 ||
	    getsockname(server->listener_fd, (struct sockaddr *)&address,
	                &address_length) != 0) {
		(void)close(server->listener_fd);
		server->listener_fd = -1;
		return -1;
	}
	server->test = test;
	server->port = ntohs(address.sin_port);
	server->backend_port = test->rpc_port;
	server->result = TR_ERR_STATE;
	return server->port != 0U ? 0 : -1;
}

static int test_tr_client_to_gnutls_server(const struct test_state *state,
                                           const char *expected_peer_name,
                                           int include_client_cert)
{
	struct test_gnutls_server server;
	struct tr_client_config config;
	struct tr_rpc_method_desc method;
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_call_options options;
	struct tr_rpc_bytes request;
	struct tr_rpc_v2_call call;
	struct test_rpc_result rpc_result;
	struct tr_client *client = NULL;
	pthread_t thread;
	int result;
	int rpc_completed = 0;
	unsigned attempt;

	memset(&method, 0, sizeof(method));
	method.service_id = TEST_SERVICE_ID;
	method.method_id = TEST_METHOD_ID;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_RPC_LANE_CONTROL;
	method.max_request_bytes = sizeof(test_rpc_payload);
	method.max_response_bytes = sizeof(test_rpc_payload);
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = test_rpc_client_message;
	callbacks.terminal = test_rpc_client_terminal;
	callbacks.arg = &rpc_result;
	memset(&options, 0, sizeof(options));
	options.timeout_ms = TEST_CONNECT_TIMEOUT_MS;
	request.data = test_rpc_payload;
	request.len = sizeof(test_rpc_payload);
	atomic_init(&rpc_result.message_count, 0);
	atomic_init(&rpc_result.response_matches, 0);
	atomic_init(&rpc_result.terminal, 0);
	atomic_init(&rpc_result.status, -1);
	tr_client_config_init(&config);
	config.connect_timeout_ms = TEST_CONNECT_TIMEOUT_MS;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.ca_file = state->identity.ca_file;
	config.tls.expected_peer_name = expected_peer_name;
	config.tls.sni_name = "server.example.test";
	if (include_client_cert) {
		config.tls.client_certificate_file =
		        state->identity.client_certificate_file;
		config.tls.client_private_key_file =
		        state->identity.client_private_key_file;
	}
	assert(tr_client_create(&config, &method, 1U, &client) == TR_OK);
	memset(&server, 0, sizeof(server));
	assert(test_gnutls_server_listen(&server, state) == 0);
	assert(pthread_create(&thread, NULL, test_gnutls_server_thread,
	                      &server) == 0);
	result = tr_client_connect(client, "127.0.0.1", server.port);
	if (result == TR_OK) {
		if (tr_client_unary_call(client, TEST_SERVICE_ID,
		                         TEST_METHOD_ID, &request, &options,
		                         &callbacks, &call) == TR_OK) {
			for (attempt = 0U; attempt < TEST_WAIT_MS; ++attempt) {
				const struct timespec pause = {0, 1000000L};
				if (atomic_load_explicit(&rpc_result.terminal,
				                         memory_order_acquire))
					break;
				assert(nanosleep(&pause, NULL) == 0);
			}
			if (attempt < TEST_WAIT_MS &&
			    atomic_load_explicit(&rpc_result.status,
			                         memory_order_relaxed) ==
			            TR_RPC_STATUS_OK &&
			    atomic_load_explicit(&rpc_result.message_count,
			                         memory_order_relaxed) == 1 &&
			    atomic_load_explicit(&rpc_result.response_matches,
			                         memory_order_relaxed))
				rpc_completed = 1;
		}
	}
	{
		int destroy_result = TR_AGAIN;
		for (attempt = 0U;
		     attempt < TEST_WAIT_MS && (destroy_result == TR_AGAIN ||
		                                destroy_result == TR_ERR_STATE);
		     ++attempt) {
			const struct timespec pause = {0, 1000000L};
			destroy_result = tr_client_destroy(client);
			if (destroy_result == TR_AGAIN ||
			    destroy_result == TR_ERR_STATE)
				assert(nanosleep(&pause, NULL) == 0);
		}
		assert(destroy_result == TR_OK);
	}
	assert(pthread_join(thread, NULL) == 0);
	if (server.listener_fd >= 0)
		(void)close(server.listener_fd);
	if (result == TR_OK) {
		assert(server.result == TR_OK);
		assert(server.handshake_complete);
		assert(server.client_certificate_verified);
		assert(server.client_plaintext_bytes > 0U);
		assert(server.backend_plaintext_bytes > 0U);
	} else {
		assert(!server.handshake_complete);
		assert(server.client_plaintext_bytes == 0U);
		assert(server.backend_plaintext_bytes == 0U);
		assert(server.handshake_error < 0);
		if (!include_client_cert)
			assert(server.handshake_error ==
			       GNUTLS_E_CERTIFICATE_REQUIRED);
	}
	if (result == TR_OK && !rpc_completed)
		result = TR_ERR_SYS;
	return result;
}

static void test_server_destroy(struct test_state *state)
{
	unsigned attempt;
	int result = TR_AGAIN;
	for (attempt = 0U; attempt < TEST_WAIT_MS &&
	                   (result == TR_AGAIN || result == TR_ERR_STATE);
	     ++attempt) {
		const struct timespec pause = {0, 1000000L};
		result = tr_server_destroy(state->rpc_server);
		if (result == TR_AGAIN || result == TR_ERR_STATE)
			assert(nanosleep(&pause, NULL) == 0);
	}
	assert(result == TR_OK);
	state->rpc_server = NULL;
	result = TR_AGAIN;
	for (attempt = 0U; attempt < TEST_WAIT_MS && result == TR_AGAIN;
	     ++attempt) {
		const struct timespec pause = {0, 1000000L};
		result = tr_server_destroy(state->server);
		if (result == TR_AGAIN)
			assert(nanosleep(&pause, NULL) == 0);
	}
	assert(result == TR_OK);
	state->server = NULL;
	tls_test_identity_cleanup(&state->identity);
}

int main(void)
{
	struct test_state state;
	int result;
	assert(gnutls_global_init() == 0);
	test_state_init(&state);

	result = run_gnutls_client(
	        &state, state.identity.client_certificate_file,
	        state.identity.client_private_key_file, "server.example.test");
	if (result != 0)
		fprintf(stderr, "tls-gnutls: trusted TLS 1.3 mTLS failed: %d\n",
		        result);
	assert(result == 0);
	puts("tls-gnutls: trusted TLS 1.3 mTLS accepted");

	result = run_gnutls_client(&state, NULL, NULL, "server.example.test");
	assert(result > 0);
	puts("tls-gnutls: missing client certificate rejected");

	result = run_gnutls_client(
	        &state, state.identity.client_certificate_file,
	        state.identity.client_private_key_file, "wrong.example.test");
	assert(result > 0);
	puts("tls-gnutls: wrong server hostname rejected");

	result = test_tr_client_to_gnutls_server(&state, "server.example.test",
	                                         1);
	if (result != TR_OK)
		fprintf(stderr,
		        "tls-gnutls: c-trpc client to GnuTLS server failed: "
		        "%d\n",
		        result);
	assert(result == TR_OK);
	puts("tls-gnutls: c-trpc unary echo passed through GnuTLS TLS 1.3 "
	     "mTLS");

	result = test_tr_client_to_gnutls_server(&state, "wrong.example.test",
	                                         1);
	if (result != TR_ERR_CLOSED)
		fprintf(stderr, "tls-gnutls: wrong hostname returned %d\n",
		        result);
	assert(result == TR_ERR_CLOSED);
	puts("tls-gnutls: c-trpc client rejected wrong GnuTLS server hostname");

	result = test_tr_client_to_gnutls_server(&state, "server.example.test",
	                                         0);
	if (result != TR_ERR_CLOSED)
		fprintf(stderr, "tls-gnutls: missing client cert returned %d\n",
		        result);
	assert(result == TR_ERR_CLOSED);
	puts("tls-gnutls: GnuTLS server rejected c-trpc client without "
	     "certificate");

	test_server_destroy(&state);
	gnutls_global_deinit();
	return 0;
}
