#define _POSIX_C_SOURCE 200809L

#include <tr/trpc.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "tls_test_identity.h"

extern char **environ;

#define TEST_WAIT_MS 12000U
#define TEST_SERVICE_ID 0x234U
#define TEST_METHOD_ID 1U

struct test_state {
	struct tr_server *server;
	struct tls_test_identity identity;
	uint16_t port;
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

static void test_state_init(struct test_state *state)
{
	struct tr_rpc_v2_method method;
	struct tr_rpc_method_desc descriptor;
	struct tr_server_config config;

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

static void test_server_destroy(struct test_state *state)
{
	unsigned attempt;
	int result = TR_AGAIN;
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

	test_server_destroy(&state);
	return 0;
}
