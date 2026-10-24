#define _POSIX_C_SOURCE 200809L

#include <tr/client.h>
#include <tr/server.h>
#include <tr/status.h>

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <time.h>

#include "tls_test_identity.h"

struct fake_provider {
	struct tr_tls_provider_operation operation;
	struct tr_tls_credential_request request;
	unsigned fetch_count;
	_Atomic unsigned cancel_count;
};

static void provider_fetch(const struct tr_tls_credential_request *request,
                           struct tr_tls_provider_operation operation,
                           void *arg)
{
	struct fake_provider *provider = arg;
	provider->request = *request;
	provider->operation = operation;
	provider->fetch_count++;
}

static void provider_cancel(struct tr_tls_provider_operation operation,
                            void *arg)
{
	struct fake_provider *provider = arg;
	(void)operation;
	(void)atomic_fetch_add_explicit(&provider->cancel_count, 1U,
	                                memory_order_release);
}

struct test_bytes {
	uint8_t *data;
	size_t length;
};

static struct test_bytes read_file(const char *path)
{
	struct test_bytes result = {0};
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

static void free_bytes(struct test_bytes *bytes)
{
	if (!bytes)
		return;
	free(bytes->data);
	bytes->data = NULL;
	bytes->length = 0U;
}

static int
authorize(const struct tr_connection_group_authorization *authorization,
          void *arg)
{
	(void)authorization;
	(void)arg;
	return TR_OK;
}

static void test_provider_only_client_fails_closed(void)
{
	struct tr_client_config config;
	struct tr_client *client = NULL;
	const struct tr_connection_group_id group = {1U, 1U};

	tr_client_config_init(&config);
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.expected_peer_name = "server.example";
	config.connection_groups.max_data_connections = 1U;
	config.tls_credentials_provider.fetch = provider_fetch;
	config.tls_credentials_provider.cancel = provider_cancel;
	assert(tr_client_create_transport(&config, &client) == TR_OK);
	assert(client != NULL);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", 1U,
	                                          &group) == TR_AGAIN);
	assert(tr_client_destroy(client) == TR_OK);
}

static void test_provider_pair_is_required(void)
{
	struct tls_test_identity identity;
	struct tr_client_config client_config;
	struct tr_client *client = NULL;
	struct tr_server_config server_config;
	struct tr_server *server = NULL;

	assert(tls_test_identity_init(&identity, "tls-provider-pair-v2") == 0);
	tr_client_config_init(&client_config);
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.tls_credentials_provider.fetch = provider_fetch;
	assert(tr_client_create_transport(&client_config, &client) ==
	       TR_ERR_INVALID);
	assert(client == NULL);

	tr_server_config_init(&server_config);
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file = identity.server_certificate_file;
	server_config.tls.private_key_file = identity.server_private_key_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server_config.tls.client_ca_file = identity.ca_file;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 1U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 1U;
	server_config.connection_groups.authorize = authorize;
	server_config.tls_credentials_provider.fetch = provider_fetch;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_INVALID);
	assert(server == NULL);
	tls_test_identity_cleanup(&identity);
}

static void test_provider_only_server_does_not_listen_in_plaintext(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	uint16_t bound_port = 99U;

	tr_server_config_init(&config);
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	config.tls.client_ca_file = "unused-before-first-profile.pem";
	config.connection_groups.max_groups = 1U;
	config.connection_groups.max_connections = 1U;
	config.connection_groups.max_data_connections_per_group = 1U;
	config.connection_groups.max_streams_per_group = 1U;
	config.connection_groups.authorize = authorize;
	config.tls_credentials_provider.fetch = provider_fetch;
	config.tls_credentials_provider.cancel = provider_cancel;
	assert(tr_server_create_transport(&config, &server) == TR_OK);
	assert(server != NULL);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 8,
	                                         &bound_port) == TR_AGAIN);
	assert(bound_port == 99U);
	assert(tr_server_destroy(server) == TR_OK);
}

static void test_client_async_credentials_and_cancel(void)
{
	struct tls_test_identity identity;
	struct fake_provider provider = {0};
	struct tr_client_config config;
	struct tr_client *client = NULL;
	struct tr_tls_provider_operation operation = {0};
	struct tr_tls_credential_result result = {77, 99U};
	struct tr_tls_credential_bundle bundle = {0};
	struct test_bytes certificate;
	struct test_bytes private_key;
	struct test_bytes rotated_certificate;
	struct test_bytes rotated_private_key;
	struct test_bytes mismatched_private_key;

	assert(tls_test_identity_init(&identity, "tls-provider-v2") == 0);
	certificate = read_file(identity.client_certificate_file);
	private_key = read_file(identity.client_private_key_file);
	rotated_certificate =
	        read_file(identity.rotated_client_certificate_file);
	rotated_private_key =
	        read_file(identity.rotated_client_private_key_file);
	mismatched_private_key =
	        read_file(identity.mismatched_private_key_file);
	tr_client_config_init(&config);
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.ca_file = identity.ca_file;
	config.tls.expected_peer_name = "server.example";
	config.connection_groups.max_data_connections = 1U;
	config.tls_credentials_provider.fetch = provider_fetch;
	config.tls_credentials_provider.cancel = provider_cancel;
	config.tls_credentials_provider.arg = &provider;
	assert(tr_client_create_transport(&config, &client) == TR_OK);
	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(provider.fetch_count == 1U);
	assert(provider.request.deadline_monotonic_ns != 0U);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_AGAIN);
	assert(result.status == 77 && result.profile_generation == 99U);
	bundle.certificate_chain_pem.data = certificate.data;
	bundle.certificate_chain_pem.len = certificate.length;
	bundle.private_key_pem.data = private_key.data;
	bundle.private_key_pem.len = private_key.length;
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_OK && result.profile_generation == 1U);

	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_REVOKE_OLD,
	                                       &operation) == TR_OK);
	bundle.certificate_chain_pem.data = rotated_certificate.data;
	bundle.certificate_chain_pem.len = rotated_certificate.length;
	bundle.private_key_pem.data = rotated_private_key.data;
	bundle.private_key_pem.len = rotated_private_key.length;
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_OK && result.profile_generation == 2U);

	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	bundle.private_key_pem.data = mismatched_private_key.data;
	bundle.private_key_pem.len = mismatched_private_key.length;
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status < 0 && result.profile_generation == 0U);

	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	bundle.private_key_pem.data = rotated_private_key.data;
	bundle.private_key_pem.len = rotated_private_key.length;
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_OK && result.profile_generation == 3U);

	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(tr_client_tls_credentials_cancel(client, operation) == TR_OK);
	assert(atomic_load_explicit(&provider.cancel_count,
	                            memory_order_acquire) == 1U);
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          NULL) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_ERR_CANCELLED &&
	       result.profile_generation == 0U);

	assert(tr_client_tls_credentials_start(client, 10U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	for (unsigned int attempt = 0U;
	     attempt < 500U && atomic_load_explicit(&provider.cancel_count,
	                                            memory_order_acquire) < 2U;
	     ++attempt) {
		const struct timespec delay = {0, 1000000L};
		(void)nanosleep(&delay, NULL);
	}
	assert(atomic_load_explicit(&provider.cancel_count,
	                            memory_order_acquire) == 2U);
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          NULL) == TR_OK);
	assert(tr_client_tls_credentials_poll(client, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_ERR_TIMEOUT &&
	       result.profile_generation == 0U);

	assert(tr_client_tls_credentials_start(client, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(tr_client_destroy(client) == TR_AGAIN);
	assert(atomic_load_explicit(&provider.cancel_count,
	                            memory_order_acquire) == 3U);
	assert(tr_client_tls_credentials_complete(client, operation, TR_OK,
	                                          NULL) == TR_OK);
	assert(tr_client_destroy(client) == TR_OK);
	free_bytes(&certificate);
	free_bytes(&private_key);
	free_bytes(&rotated_certificate);
	free_bytes(&rotated_private_key);
	free_bytes(&mismatched_private_key);
	tls_test_identity_cleanup(&identity);
}

static void test_server_async_credentials_install_before_listen(void)
{
	struct tls_test_identity identity;
	struct fake_provider provider = {0};
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_tls_provider_operation operation = {0};
	struct tr_tls_credential_result result = {33, 44U};
	struct tr_tls_credential_bundle bundle = {0};
	struct test_bytes certificate;
	struct test_bytes private_key;
	uint16_t bound_port = 99U;

	assert(tls_test_identity_init(&identity, "tls-provider-server-v2") ==
	       0);
	certificate = read_file(identity.server_certificate_file);
	private_key = read_file(identity.server_private_key_file);
	tr_server_config_init(&config);
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	config.tls.client_ca_file = identity.ca_file;
	config.connection_groups.max_groups = 1U;
	config.connection_groups.max_connections = 1U;
	config.connection_groups.max_data_connections_per_group = 1U;
	config.connection_groups.max_streams_per_group = 1U;
	config.connection_groups.authorize = authorize;
	config.tls_credentials_provider.fetch = provider_fetch;
	config.tls_credentials_provider.cancel = provider_cancel;
	config.tls_credentials_provider.arg = &provider;
	assert(tr_server_create_transport(&config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 8,
	                                         &bound_port) == TR_AGAIN);
	assert(bound_port == 99U);
	assert(tr_server_tls_credentials_start(server, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(tr_server_tls_credentials_poll(server, operation, &result) ==
	       TR_AGAIN);
	assert(result.status == 33 && result.profile_generation == 44U);
	bundle.certificate_chain_pem.data = certificate.data;
	bundle.certificate_chain_pem.len = certificate.length;
	bundle.private_key_pem.data = private_key.data;
	bundle.private_key_pem.len = private_key.length;
	assert(tr_server_tls_credentials_complete(server, operation, TR_OK,
	                                          &bundle) == TR_OK);
	assert(tr_server_tls_credentials_poll(server, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_OK && result.profile_generation == 1U);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 8,
	                                         &bound_port) == TR_OK);
	assert(bound_port != 0U);
	assert(tr_server_tls_credentials_start(server, 10U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	{
		const struct timespec delay = {0, 30000000L};
		(void)nanosleep(&delay, NULL);
	}
	result.status = 22;
	result.profile_generation = 55U;
	assert(tr_server_tls_credentials_poll(server, operation, &result) ==
	       TR_AGAIN);
	assert(result.status == 22 && result.profile_generation == 55U);
	assert(atomic_load_explicit(&provider.cancel_count,
	                            memory_order_acquire) == 1U);
	assert(tr_server_tls_credentials_complete(server, operation, TR_OK,
	                                          NULL) == TR_OK);
	assert(tr_server_tls_credentials_poll(server, operation, &result) ==
	       TR_OK);
	assert(result.status == TR_ERR_TIMEOUT &&
	       result.profile_generation == 0U);
	assert(tr_server_tls_credentials_start(server, 5000U,
	                                       TR_TLS_ROTATE_GRACEFUL,
	                                       &operation) == TR_OK);
	assert(tr_server_destroy(server) == TR_AGAIN);
	assert(atomic_load_explicit(&provider.cancel_count,
	                            memory_order_acquire) == 2U);
	assert(tr_server_tls_credentials_complete(server, operation, TR_OK,
	                                          NULL) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	free_bytes(&certificate);
	free_bytes(&private_key);
	tls_test_identity_cleanup(&identity);
}

int main(void)
{
	test_provider_only_client_fails_closed();
	test_provider_pair_is_required();
	test_provider_only_server_does_not_listen_in_plaintext();
	test_client_async_credentials_and_cancel();
	test_server_async_credentials_install_before_listen();
	return 0;
}
