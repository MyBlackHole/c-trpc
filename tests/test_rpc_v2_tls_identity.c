#define _POSIX_C_SOURCE 200809L

#include <tr/client.h>
#include <tr/peer_identity.h>
#include <tr/rpc_v2.h>
#include <tr/server.h>
#include <tr/status.h>

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "tls_test_identity.h"

#define TEST_SERVICE_ID 0x234U
#define TEST_METHOD_ID 1U

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

int main(void)
{
	test_tls_rpc_work_receives_verified_connection_identity();
	test_call_keeps_identity_after_connection_closes();
	test_invalid_client_identity_never_reaches_rpc_work();
	test_tls_without_client_certificate_yields_none(
	        TR_TLS_CLIENT_AUTH_NONE, "rpc-v2-tls-identity-none");
	test_tls_without_client_certificate_yields_none(
	        TR_TLS_CLIENT_AUTH_OPTIONAL, "rpc-v2-tls-identity-optional");
	return 0;
}
