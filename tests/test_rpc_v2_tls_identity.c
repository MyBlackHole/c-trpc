#define _POSIX_C_SOURCE 200809L

#include <tr/client.h>
#include <tr/peer_identity.h>
#include <tr/rpc_v2.h>
#include <tr/server.h>
#include <tr/status.h>

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "tls_test_identity.h"

#define TEST_SERVICE_ID 0x234U
#define TEST_METHOD_ID 1U

struct tr_tls_openssl_profile;
struct tr_runtime;

static _Atomic unsigned int tls_profile_retain_call_count;
static _Atomic unsigned int tls_profile_retain_fail_call_a;
static _Atomic unsigned int tls_profile_retain_fail_call_b;
static _Atomic int tls_runtime_destroy_fail_once;

int __real_tr_tls_openssl_profile_retain(
        struct tr_tls_openssl_profile *profile);
int __real_tr_runtime_destroy(struct tr_runtime *runtime);

int __wrap_tr_tls_openssl_profile_retain(struct tr_tls_openssl_profile *profile)
{
	unsigned int call =
	        atomic_fetch_add_explicit(&tls_profile_retain_call_count, 1U,
	                                  memory_order_relaxed) +
	        1U;
	unsigned int fail_call_a = atomic_load_explicit(
	        &tls_profile_retain_fail_call_a, memory_order_relaxed);
	unsigned int fail_call_b = atomic_load_explicit(
	        &tls_profile_retain_fail_call_b, memory_order_relaxed);

	if ((fail_call_a != 0U && call == fail_call_a) ||
	    (fail_call_b != 0U && call == fail_call_b))
		return TR_ERR_NOMEM;
	return __real_tr_tls_openssl_profile_retain(profile);
}

int __wrap_tr_runtime_destroy(struct tr_runtime *runtime)
{
	if (atomic_exchange_explicit(&tls_runtime_destroy_fail_once, 0,
	                             memory_order_relaxed))
		return TR_AGAIN;
	return __real_tr_runtime_destroy(runtime);
}

static void tls_profile_retain_fault_set(unsigned int fail_call_a,
                                         unsigned int fail_call_b)
{
	atomic_store_explicit(&tls_profile_retain_call_count, 0U,
	                      memory_order_relaxed);
	atomic_store_explicit(&tls_profile_retain_fail_call_a, fail_call_a,
	                      memory_order_relaxed);
	atomic_store_explicit(&tls_profile_retain_fail_call_b, fail_call_b,
	                      memory_order_relaxed);
}

static enum tr_rpc_status
tls_rotation_unused_work(struct tr_rpc_v2_call_id call,
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

struct identity_test_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct tls_test_identity certificates;
	struct tr_server *server;
	struct tr_client *client;
	uint16_t port;
	unsigned work_count[3];
	unsigned server_terminals;
	unsigned client_messages;
	unsigned client_terminals;
	enum tr_rpc_status server_status;
	enum tr_rpc_status client_status;
	int block_open;
	int worker_entered;
	int release_worker;
	int expect_certificate_identity;
	int expect_fake_metadata_identity;
	_Atomic int error;
};

struct test_tls_credentials_provider {
	struct tr_tls_provider_operation operation;
	unsigned fetch_count;
	unsigned cancel_count;
};

struct test_pem_bytes {
	uint8_t *data;
	size_t length;
};

static const uint8_t expected_client_uri[] = "spiffe://c-trpc.example/client";
static const uint8_t fake_metadata_identity[] = "spiffe://forged.invalid";
static const uint8_t request_data[] = {0x43U, 0x54U, 0x52U, 0x50U};

static enum tr_rpc_status
test_server_work(struct tr_rpc_v2_call_id call,
                 const struct tr_rpc_v2_request *request,
                 const struct tr_rpc_v2_cancel *cancel,
                 struct tr_rpc_v2_reply *reply, void *arg)
{
	struct identity_test_state *state = arg;
	struct tr_rpc_bytes metadata_value = {0};
	(void)call;
	(void)cancel;
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		pthread_mutex_lock(&state->lock);
		if (state->block_open) {
			state->block_open = 0;
			state->worker_entered = 1;
			pthread_cond_broadcast(&state->changed);
			while (!state->release_worker)
				pthread_cond_wait(&state->changed,
				                  &state->lock);
		}
		pthread_mutex_unlock(&state->lock);
	}
	if (state->expect_certificate_identity) {
		if (request->peer_identity.authentication !=
		            TR_PEER_IDENTITY_TLS_CERTIFICATE ||
		    !request->peer_identity.uri_san ||
		    request->peer_identity.uri_san_len !=
		            sizeof(expected_client_uri) - 1U ||
		    memcmp(request->peer_identity.uri_san, expected_client_uri,
		           sizeof(expected_client_uri) - 1U) != 0) {
			atomic_store_explicit(&state->error, 1,
			                      memory_order_relaxed);
			return TR_RPC_STATUS_INTERNAL;
		}
	} else if (request->peer_identity.authentication !=
	                   TR_PEER_IDENTITY_NONE ||
	           request->peer_identity.uri_san ||
	           request->peer_identity.uri_san_len != 0U) {
		atomic_store_explicit(&state->error, 1, memory_order_relaxed);
		return TR_RPC_STATUS_INTERNAL;
	}
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		if (state->expect_fake_metadata_identity) {
			if (tr_rpc_v2_metadata_find(&request->metadata,
			                            "peer_identity",
			                            &metadata_value) != TR_OK ||
			    metadata_value.len !=
			            sizeof(fake_metadata_identity) - 1U ||
			    memcmp(metadata_value.data, fake_metadata_identity,
			           sizeof(fake_metadata_identity) - 1U) != 0) {
				atomic_store_explicit(&state->error, 1,
				                      memory_order_relaxed);
				return TR_RPC_STATUS_INTERNAL;
			}
		} else if (tr_rpc_v2_metadata_count(&request->metadata) != 0U) {
			atomic_store_explicit(&state->error, 1,
			                      memory_order_relaxed);
			return TR_RPC_STATUS_INTERNAL;
		}
	}
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE &&
	    tr_rpc_v2_reply_append(reply, &request->body) != TR_OK) {
		atomic_store_explicit(&state->error, 1, memory_order_relaxed);
		return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	}
	if ((unsigned)request->kind >= 3U) {
		atomic_store_explicit(&state->error, 1, memory_order_relaxed);
		return TR_RPC_STATUS_INTERNAL;
	}
	pthread_mutex_lock(&state->lock);
	state->work_count[(unsigned)request->kind]++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
	return TR_RPC_STATUS_OK;
}

static void test_server_terminal(struct tr_rpc_v2_call_id call,
                                 enum tr_rpc_status status,
                                 enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct identity_test_state *state = arg;
	(void)call;
	(void)delivery;
	pthread_mutex_lock(&state->lock);
	state->server_status = status;
	state->server_terminals++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static void test_client_message(struct tr_rpc_v2_call call,
                                const struct tr_rpc_bytes *body, void *arg)
{
	struct identity_test_state *state = arg;
	(void)call;
	if (body->len != sizeof(request_data) ||
	    memcmp(body->data, request_data, sizeof(request_data)) != 0)
		atomic_store_explicit(&state->error, 1, memory_order_relaxed);
	pthread_mutex_lock(&state->lock);
	state->client_messages++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static void test_client_terminal(struct tr_rpc_v2_call call,
                                 enum tr_rpc_status status,
                                 enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct identity_test_state *state = arg;
	(void)call;
	(void)delivery;
	pthread_mutex_lock(&state->lock);
	state->client_status = status;
	state->client_terminals++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_terminal(struct identity_test_state *state)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&state->lock);
	while (state->client_terminals == 0U || state->server_terminals == 0U) {
		int result = pthread_cond_timedwait(&state->changed,
		                                    &state->lock, &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_terminal_count(struct identity_test_state *state,
                                    unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&state->lock);
	while (state->client_terminals < expected ||
	       state->server_terminals < expected) {
		int result = pthread_cond_timedwait(&state->changed,
		                                    &state->lock, &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_client_terminal_count(struct identity_test_state *state,
                                           unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&state->lock);
	while (state->client_terminals < expected) {
		int result = pthread_cond_timedwait(&state->changed,
		                                    &state->lock, &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_terminal_counts(struct identity_test_state *state,
                                     unsigned client_expected,
                                     unsigned server_expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&state->lock);
	while (state->client_terminals < client_expected ||
	       state->server_terminals < server_expected) {
		int result = pthread_cond_timedwait(&state->changed,
		                                    &state->lock, &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_worker_entry(struct identity_test_state *state)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&state->lock);
	while (!state->worker_entered) {
		int result = pthread_cond_timedwait(&state->changed,
		                                    &state->lock, &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void release_blocked_worker(struct identity_test_state *state)
{
	pthread_mutex_lock(&state->lock);
	state->release_worker = 1;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static void capture_server_tls_credentials_fetch(
        const struct tr_tls_credential_request *request,
        struct tr_tls_provider_operation operation, void *arg)
{
	struct test_tls_credentials_provider *provider = arg;
	assert(request != NULL);
	assert(request->deadline_monotonic_ns != 0U);
	provider->operation = operation;
	provider->fetch_count++;
}

static void
count_server_tls_credentials_cancel(struct tr_tls_provider_operation operation,
                                    void *arg)
{
	struct test_tls_credentials_provider *provider = arg;
	assert(operation.instance == provider->operation.instance);
	assert(operation.generation == provider->operation.generation);
	assert(operation.slot == provider->operation.slot);
	provider->cancel_count++;
}

static struct test_pem_bytes read_test_pem(const char *path)
{
	struct test_pem_bytes result = {0};
	FILE *file = fopen(path, "rb");
	long length;
	assert(file != NULL);
	assert(fseek(file, 0L, SEEK_END) == 0);
	length = ftell(file);
	assert(length > 0L);
	assert(fseek(file, 0L, SEEK_SET) == 0);
	result.data = malloc((size_t)length);
	assert(result.data != NULL);
	result.length = (size_t)length;
	assert(fread(result.data, 1U, result.length, file) == result.length);
	assert(fclose(file) == 0);
	return result;
}

static void install_server_tls_credentials(
        struct tr_server *server,
        struct test_tls_credentials_provider *provider,
        const char *certificate_path, const char *private_key_path,
        unsigned expected_fetch_count, uint64_t expected_generation)
{
	struct test_pem_bytes certificate = read_test_pem(certificate_path);
	struct test_pem_bytes private_key = read_test_pem(private_key_path);
	struct tr_tls_credential_bundle bundle = {
	        .certificate_chain_pem = {certificate.data, certificate.length},
	        .private_key_pem = {private_key.data, private_key.length},
	};
	struct tr_tls_provider_operation operation = {0};
	struct tr_tls_credential_result result = {0};
	assert(tr_server_tls_credentials_start(server, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(provider->fetch_count == expected_fetch_count);
	assert(operation.instance == provider->operation.instance);
	assert(operation.generation == provider->operation.generation);
	assert(operation.slot == provider->operation.slot);
	assert(tr_server_tls_credentials_complete(server, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_server_tls_credentials_poll(server, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_OK);
	assert(result.profile_generation == expected_generation);
	free(private_key.data);
	free(certificate.data);
}

static void identity_test_cleanup(struct identity_test_state *state)
{
	if (state->client) {
		assert(tr_client_destroy(state->client) == TR_OK);
		state->client = NULL;
	}
	if (state->server) {
		int result;
		do {
			result = tr_server_destroy(state->server);
			if (result == TR_AGAIN) {
				struct timespec pause = {0, 1000000L};
				assert(nanosleep(&pause, NULL) == 0);
			}
		} while (result == TR_AGAIN);
		assert(result == TR_OK);
		state->server = NULL;
	}
	tls_test_identity_cleanup(&state->certificates);
	pthread_cond_destroy(&state->changed);
	pthread_mutex_destroy(&state->lock);
}

static void test_tls_rpc_work_receives_verified_connection_identity(void)
{
	struct identity_test_state state;
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = test_client_message,
	        .terminal = test_client_terminal,
	        .arg = &state,
	};
	struct tr_rpc_metadata metadata = {
	        .key = "peer_identity",
	        .value = (const void *)fake_metadata_identity,
	        .value_len = sizeof(fake_metadata_identity) - 1U,
	};
	struct tr_rpc_call_options options = {
	        .timeout_ms = 5000U,
	        .metadata = &metadata,
	        .metadata_count = 1U,
	};
	struct tr_rpc_bytes request = {
	        .data = request_data,
	        .len = sizeof(request_data),
	};
	struct tr_rpc_v2_call call;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	state.expect_certificate_identity = 1;
	state.expect_fake_metadata_identity = 1;
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates,
	                              "rpc-v2-tls-identity") == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file =
	        state.certificates.server_certificate_file;
	server_config.tls.private_key_file =
	        state.certificates.server_private_key_file;
	server_config.tls.client_ca_file = state.certificates.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	assert(tr_server_listen(state.server, "127.0.0.1", 0, &state.port) ==
	       TR_OK);
	assert(tr_server_start(state.server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        state.certificates.client_certificate_file;
	client_config.tls.client_private_key_file =
	        state.certificates.client_private_key_file;
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", state.port) ==
	       TR_OK);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	wait_for_terminal(&state);
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);
	assert(state.client_messages == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_OPEN] == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_MESSAGE] == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_HALF_CLOSE] == 1U);
	identity_test_cleanup(&state);
}

static void test_call_keeps_identity_after_connection_closes(void)
{
	struct identity_test_state state;
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = test_client_message,
	        .terminal = test_client_terminal,
	        .arg = &state,
	};
	struct tr_rpc_metadata metadata = {
	        .key = "peer_identity",
	        .value = (const void *)fake_metadata_identity,
	        .value_len = sizeof(fake_metadata_identity) - 1U,
	};
	struct tr_rpc_call_options options = {
	        .timeout_ms = 5000U,
	        .metadata = &metadata,
	        .metadata_count = 1U,
	};
	struct tr_rpc_bytes request = {
	        .data = request_data,
	        .len = sizeof(request_data),
	};
	struct tr_rpc_v2_call call;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	state.expect_certificate_identity = 1;
	state.expect_fake_metadata_identity = 1;
	state.block_open = 1;
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates,
	                              "rpc-v2-tls-identity-close") == 0);
	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file =
	        state.certificates.server_certificate_file;
	server_config.tls.private_key_file =
	        state.certificates.server_private_key_file;
	server_config.tls.client_ca_file = state.certificates.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	assert(tr_server_listen(state.server, "127.0.0.1", 0, &state.port) ==
	       TR_OK);
	assert(tr_server_start(state.server) == TR_OK);
	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        state.certificates.client_certificate_file;
	client_config.tls.client_private_key_file =
	        state.certificates.client_private_key_file;
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", state.port) ==
	       TR_OK);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	wait_for_worker_entry(&state);
	assert(tr_client_destroy(state.client) == TR_OK);
	state.client = NULL;
	{
		struct timespec pause = {0, 100000000L};
		assert(nanosleep(&pause, NULL) == 0);
	}
	release_blocked_worker(&state);
	{
		struct timespec deadline;
		assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
		deadline.tv_sec += 15;
		pthread_mutex_lock(&state.lock);
		while (state.server_terminals == 0U) {
			int result = pthread_cond_timedwait(
			        &state.changed, &state.lock, &deadline);
			assert(result == 0);
		}
		pthread_mutex_unlock(&state.lock);
	}
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);
	assert(state.work_count[TR_RPC_V2_REQUEST_OPEN] == 1U);
	identity_test_cleanup(&state);
}

static void test_invalid_client_identity_never_reaches_rpc_work(void)
{
	struct identity_test_state state;
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_server_config server_config;
	struct tr_client_config client_config;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates,
	                              "rpc-v2-tls-identity-invalid") == 0);
	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file =
	        state.certificates.server_certificate_file;
	server_config.tls.private_key_file =
	        state.certificates.server_private_key_file;
	server_config.tls.client_ca_file = state.certificates.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	assert(tr_server_listen(state.server, "127.0.0.1", 0, &state.port) ==
	       TR_OK);
	assert(tr_server_start(state.server) == TR_OK);
	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        state.certificates.client_no_uri_certificate_file;
	client_config.tls.client_private_key_file =
	        state.certificates.client_private_key_file;
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", state.port) !=
	       TR_OK);
	pthread_mutex_lock(&state.lock);
	assert(state.work_count[TR_RPC_V2_REQUEST_OPEN] == 0U);
	assert(state.work_count[TR_RPC_V2_REQUEST_MESSAGE] == 0U);
	assert(state.work_count[TR_RPC_V2_REQUEST_HALF_CLOSE] == 0U);
	pthread_mutex_unlock(&state.lock);
	identity_test_cleanup(&state);
}

static void test_tls_without_client_certificate_yields_none(
        enum tr_tls_client_auth_policy client_auth, const char *fixture_suffix)
{
	struct identity_test_state state;
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = test_client_message,
	        .terminal = test_client_terminal,
	        .arg = &state,
	};
	struct tr_rpc_call_options options = {.timeout_ms = 5000U};
	struct tr_rpc_bytes request = {
	        .data = request_data,
	        .len = sizeof(request_data),
	};
	struct tr_rpc_v2_call call;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates, fixture_suffix) ==
	       0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file =
	        state.certificates.server_certificate_file;
	server_config.tls.private_key_file =
	        state.certificates.server_private_key_file;
	server_config.tls.client_auth = client_auth;
	if (client_auth != TR_TLS_CLIENT_AUTH_NONE)
		server_config.tls.client_ca_file = state.certificates.ca_file;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	assert(tr_server_listen(state.server, "127.0.0.1", 0, &state.port) ==
	       TR_OK);
	assert(tr_server_start(state.server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", state.port) ==
	       TR_OK);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	wait_for_terminal(&state);
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);
	assert(state.client_messages == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_OPEN] == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_MESSAGE] == 1U);
	assert(state.work_count[TR_RPC_V2_REQUEST_HALF_CLOSE] == 1U);
	identity_test_cleanup(&state);
}

static void test_server_provider_rotation_updates_tls_listener(void)
{
	struct identity_test_state state;
	struct tls_test_identity rotated_certificates;
	struct test_tls_credentials_provider provider = {0};
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = test_client_message,
	        .terminal = test_client_terminal,
	        .arg = &state,
	};
	struct tr_rpc_call_options options = {.timeout_ms = 5000U};
	struct tr_rpc_bytes request = {
	        .data = request_data,
	        .len = sizeof(request_data),
	};
	struct tr_rpc_v2_call call;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_client_config rotated_client_config;
	struct tr_client *rotated_client = NULL;
	unsigned expected_calls = 0U;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	state.expect_certificate_identity = 1;
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates,
	                              "rpc-v2-tls-provider-listener-old") == 0);
	assert(tls_test_identity_init(&rotated_certificates,
	                              "rpc-v2-tls-provider-listener-new") == 0);

	tr_server_config_init(&server_config);
	server_config.shard_count = 2U;
	server_config.max_peers = 4U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.client_ca_file = state.certificates.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server_config.tls_credentials_provider.fetch =
	        capture_server_tls_credentials_fetch;
	server_config.tls_credentials_provider.cancel =
	        count_server_tls_credentials_cancel;
	server_config.tls_credentials_provider.arg = &provider;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	install_server_tls_credentials(
	        state.server, &provider,
	        state.certificates.server_certificate_file,
	        state.certificates.server_private_key_file, 1U, 1U);
	assert(tr_server_listen(state.server, "127.0.0.1", 0U, &state.port) ==
	       TR_OK);
	assert(tr_server_start(state.server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        state.certificates.client_certificate_file;
	client_config.tls.client_private_key_file =
	        state.certificates.client_private_key_file;
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", state.port) ==
	       TR_OK);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	expected_calls++;
	wait_for_terminal_counts(&state, expected_calls, expected_calls);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);

	/* 新 CA 签发的凭证只能用于新 listener handshake；旧连接仍可排空。 */
	install_server_tls_credentials(
	        state.server, &provider,
	        rotated_certificates.server_certificate_file,
	        rotated_certificates.server_private_key_file, 2U, 2U);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	expected_calls++;
	wait_for_terminal_counts(&state, expected_calls, expected_calls);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);

	rotated_client_config = client_config;
	rotated_client_config.tls.ca_file = rotated_certificates.ca_file;
	assert(tr_client_create(&rotated_client_config, &descriptor, 1U,
	                        &rotated_client) == TR_OK);
	assert(tr_client_connect(rotated_client, "127.0.0.1", state.port) ==
	       TR_OK);
	assert(tr_client_unary_call(rotated_client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	expected_calls++;
	wait_for_terminal_counts(&state, expected_calls, expected_calls);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);
	assert(state.client_messages == expected_calls);
	assert(state.work_count[TR_RPC_V2_REQUEST_OPEN] == expected_calls);
	assert(state.work_count[TR_RPC_V2_REQUEST_MESSAGE] == expected_calls);
	assert(state.work_count[TR_RPC_V2_REQUEST_HALF_CLOSE] ==
	       expected_calls);
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);
	assert(provider.fetch_count == 2U);
	assert(provider.cancel_count == 0U);

	assert(tr_client_destroy(rotated_client) == TR_OK);
	identity_test_cleanup(&state);
	tls_test_identity_cleanup(&rotated_certificates);
}

static void test_tls_profile_rotation_generation_and_failed_candidate(void)
{
	struct identity_test_state state;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_client_config invalid_client_config;
	struct tr_server_config invalid_server_config;
	struct tr_client *second_client = NULL;
	struct tr_rpc_method_desc descriptor = {
	        .service_id = TEST_SERVICE_ID,
	        .method_id = TEST_METHOD_ID,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = sizeof(request_data),
	        .max_response_bytes = sizeof(request_data),
	};
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = test_server_work,
	        .terminal = test_server_terminal,
	        .arg = &state,
	};
	struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = test_client_message,
	        .terminal = test_client_terminal,
	        .arg = &state,
	};
	struct tr_rpc_call_options options = {.timeout_ms = 5000U};
	struct tr_rpc_bytes request = {
	        .data = request_data,
	        .len = sizeof(request_data),
	};
	struct tr_rpc_v2_call call;
	uint16_t port = 0U;
	uint64_t generation;
	unsigned expected_client_terminals = 1U;
	int ret;

	memset(&state, 0, sizeof(state));
	atomic_init(&state.error, 0);
	state.expect_certificate_identity = 1;
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attributes;
		assert(pthread_condattr_init(&attributes) == 0);
		assert(pthread_condattr_setclock(&attributes,
		                                 CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attributes) == 0);
		assert(pthread_condattr_destroy(&attributes) == 0);
	}
	assert(tls_test_identity_init(&state.certificates,
	                              "rpc-v2-tls-profile-rotation") == 0);
	tr_server_config_init(&server_config);
	server_config.shard_count = 2U;
	server_config.max_peers = 4U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file =
	        state.certificates.server_certificate_file;
	server_config.tls.private_key_file =
	        state.certificates.server_private_key_file;
	server_config.tls.client_ca_file = state.certificates.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	assert(tr_server_create(&server_config, &method, 1U, &state.server) ==
	       TR_OK);
	assert(tr_server_listen(state.server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(tr_server_start(state.server) == TR_OK);

	invalid_server_config = server_config;
	invalid_server_config.tls.certificate_file =
	        "missing-rotation-cert.pem";
	generation = UINT64_C(0xfeedface);
	assert(tr_server_tls_update_profile(
	               state.server, &invalid_server_config.tls,
	               TR_TLS_ROTATE_GRACEFUL, &generation) == TR_ERR_INVALID);
	assert(generation == UINT64_C(0xfeedface));

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = state.certificates.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        state.certificates.client_certificate_file;
	client_config.tls.client_private_key_file =
	        state.certificates.client_private_key_file;
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &state.client) == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1", port) == TR_OK);
	invalid_client_config = client_config;
	invalid_client_config.tls.ca_file = "missing-rotation-ca.pem";
	generation = UINT64_C(0xfeedface);
	assert(tr_client_tls_update_profile(
	               state.client, &invalid_client_config.tls,
	               TR_TLS_ROTATE_GRACEFUL, &generation) == TR_ERR_INVALID);
	assert(generation == UINT64_C(0xfeedface));
	generation = 0U;
	assert(tr_client_tls_update_profile(state.client, &client_config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	generation = 0U;
	assert(tr_server_tls_update_profile(state.server, &server_config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	wait_for_terminal_count(&state, 1U);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);

	generation = 0U;
	assert(tr_server_tls_update_profile(state.server, &server_config.tls,
	                                    TR_TLS_ROTATE_REVOKE_OLD,
	                                    &generation) == TR_OK);
	assert(generation == 3U);
	ret = tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                           TEST_METHOD_ID, &request, &options,
	                           &callbacks, &call);
	if (ret == TR_OK) {
		wait_for_client_terminal_count(&state,
		                               ++expected_client_terminals);
		assert(state.client_status != TR_RPC_STATUS_OK);
	} else
		assert(ret == TR_ERR_STATE || ret == TR_ERR_CLOSED);
	assert(tr_client_create(&client_config, &descriptor, 1U,
	                        &second_client) == TR_OK);
	assert(tr_client_connect(second_client, "127.0.0.1", port) == TR_OK);
	generation = 0U;
	assert(tr_client_tls_update_profile(second_client, &client_config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	assert(tr_client_unary_call(second_client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, &options,
	                            &callbacks, &call) == TR_OK);
	wait_for_terminal_counts(&state, ++expected_client_terminals, 2U);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_status == TR_RPC_STATUS_OK);
	assert(atomic_load_explicit(&state.error, memory_order_relaxed) == 0);
	generation = 0U;
	assert(tr_client_tls_update_profile(second_client, &client_config.tls,
	                                    TR_TLS_ROTATE_REVOKE_OLD,
	                                    &generation) == TR_OK);
	assert(generation == 3U);
	ret = tr_client_unary_call(second_client, TEST_SERVICE_ID,
	                           TEST_METHOD_ID, &request, &options,
	                           &callbacks, &call);
	if (ret == TR_OK) {
		wait_for_client_terminal_count(&state,
		                               ++expected_client_terminals);
		assert(state.client_status != TR_RPC_STATUS_OK);
	} else
		assert(ret == TR_ERR_STATE || ret == TR_ERR_CLOSED);

	assert(tr_client_destroy(second_client) == TR_OK);
	identity_test_cleanup(&state);
}

static void test_tls_profile_install_and_rollback_faults(void)
{
	struct tls_test_identity identity;
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_rpc_v2_method method = {
	        .descriptor =
	                {
	                        .service_id = TEST_SERVICE_ID,
	                        .method_id = TEST_METHOD_ID,
	                        .request_cardinality = TR_RPC_ONE,
	                        .response_cardinality = TR_RPC_ONE,
	                        .request_codec_id = TR_RPC_CODEC_RAW,
	                        .response_codec_id = TR_RPC_CODEC_RAW,
	                        .lane = TR_RPC_LANE_CONTROL,
	                        .max_request_bytes = sizeof(request_data),
	                        .max_response_bytes = sizeof(request_data),
	                },
	        .work = tls_rotation_unused_work,
	};
	uint64_t generation;

	assert(tls_test_identity_init(&identity,
	                              "rpc-v2-tls-install-rollback") == 0);
	tr_server_config_init(&config);
	config.shard_count = 2U;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.certificate_file = identity.server_certificate_file;
	config.tls.private_key_file = identity.server_private_key_file;
	assert(tr_server_create(&config, &method, 1U, &server) == TR_OK);

	/* 第 0 个 shard 安装候选后，第 1 个 shard 的 retain 失败；反向回滚
	 * 必须恢复已部分安装的 shard，且不能发布新 generation。 */
	tls_profile_retain_fault_set(2U, 0U);
	generation = UINT64_C(0xfeedface);
	assert(tr_server_tls_update_profile(server, &config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_ERR_NOMEM);
	assert(atomic_load_explicit(&tls_profile_retain_call_count,
	                            memory_order_relaxed) == 3U);
	assert(generation == UINT64_C(0xfeedface));
	tls_profile_retain_fault_set(0U, 0U);
	generation = 0U;
	assert(tr_server_tls_update_profile(server, &config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	assert(tr_server_destroy(server) == TR_OK);
	server = NULL;
	tls_test_identity_cleanup(&identity);

	assert(tls_test_identity_init(&identity,
	                              "rpc-v2-tls-rollback-failure") == 0);
	tr_server_config_init(&config);
	config.shard_count = 2U;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.certificate_file = identity.server_certificate_file;
	config.tls.private_key_file = identity.server_private_key_file;
	assert(tr_server_create(&config, &method, 1U, &server) == TR_OK);

	/* 第 2 次 retain 令安装失败，第 3 次令 reverse rollback 失败；Server
	 * 必须 fail-closed，generation 输出不变，并保留销毁所有权。 */
	tls_profile_retain_fault_set(2U, 3U);
	generation = UINT64_C(0xfeedface);
	assert(tr_server_tls_update_profile(server, &config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_ERR_STATE);
	assert(atomic_load_explicit(&tls_profile_retain_call_count,
	                            memory_order_relaxed) == 3U);
	assert(generation == UINT64_C(0xfeedface));
	assert(tr_server_tls_update_profile(server, &config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_ERR_STATE);
	tls_profile_retain_fault_set(0U, 0U);
	atomic_store_explicit(&tls_runtime_destroy_fail_once, 1,
	                      memory_order_relaxed);
	assert(tr_server_destroy(server) == TR_AGAIN);
	assert(tr_server_destroy(server) == TR_OK);
	server = NULL;
	tls_test_identity_cleanup(&identity);
}

int main(void)
{
	test_tls_rpc_work_receives_verified_connection_identity();
	test_call_keeps_identity_after_connection_closes();
	test_invalid_client_identity_never_reaches_rpc_work();
	test_tls_without_client_certificate_yields_none(
	        TR_TLS_CLIENT_AUTH_NONE, "rpc-v2-tls-identity-none");
	test_tls_without_client_certificate_yields_none(
	        TR_TLS_CLIENT_AUTH_OPTIONAL, "rpc-v2-tls-identity-optional");
	test_server_provider_rotation_updates_tls_listener();
	test_tls_profile_rotation_generation_and_failed_candidate();
	test_tls_profile_install_and_rollback_faults();
	return 0;
}
