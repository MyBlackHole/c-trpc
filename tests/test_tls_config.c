#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../include/tr/client.h"
#include "../include/tr/rpc_codec.h"
#include "../include/tr/rpc_v2.h"
#include "../include/tr/server.h"
#include "../include/tr/status.h"
#include "../include/tr/tls.h"
#include "../src/io/peer_identity_internal.h"
#include "../src/io/tls_config.h"
#include "../src/io/tls_openssl.h"
#include "../src/memory_budget.h"
#include "tls_test_identity.h"

static struct tls_test_identity test_identity;

static void
test_tls_credentials_fetch(const struct tr_tls_credential_request *request,
                           struct tr_tls_provider_operation operation,
                           void *arg)
{
	(void)request;
	(void)operation;
	(void)arg;
}

static void
test_tls_credentials_cancel(struct tr_tls_provider_operation operation,
                            void *arg)
{
	(void)operation;
	(void)arg;
}

static void test_connection_group_authorize_async(
        const struct tr_connection_group_authorization *authorization,
        struct tr_tls_provider_operation operation, void *arg)
{
	(void)authorization;
	(void)operation;
	(void)arg;
}

static void test_connection_group_authorize_cancel(
        struct tr_tls_provider_operation operation, void *arg)
{
	(void)operation;
	(void)arg;
}

static void cleanup_test_identity(void)
{
	tls_test_identity_cleanup(&test_identity);
}

static enum tr_rpc_status tls_test_work(struct tr_rpc_v2_call_id call,
                                        const struct tr_rpc_v2_request *request,
                                        const struct tr_rpc_v2_cancel *cancel,
                                        struct tr_rpc_v2_reply *reply,
                                        void *arg)
{
	(void)call;
	(void)request;
	(void)cancel;
	(void)reply;
	(void)arg;
	return TR_RPC_STATUS_UNIMPLEMENTED;
}

static struct tr_rpc_method_desc tls_test_method_desc(void)
{
	return (struct tr_rpc_method_desc){
	        .service_id = 1U,
	        .method_id = 1U,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_ONE,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = 1U,
	        .max_response_bytes = 1U,
	};
}

static void test_tls_configs_default_disabled(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_tls_credentials_provider credentials_provider = {
	        .fetch = test_tls_credentials_fetch,
	        .cancel = test_tls_credentials_cancel,
	};

	tr_client_config_init(&client);
	tr_server_config_init(&server);
	assert(client.limits.max_pending_tls_operations == 64U);
	assert(server.limits.max_pending_tls_operations == 64U);
	assert(client.limits.max_tls_credential_bundle_bytes ==
	       4U * 1024U * 1024U);
	assert(server.limits.max_tls_credential_bundle_bytes ==
	       4U * 1024U * 1024U);
	assert(strcmp(tr_status_str(TR_ERR_CANCELLED), "cancelled") == 0);
	assert(client.tls.mode == TR_TLS_DISABLED);
	assert(client.tls.ca_file == NULL);
	assert(client.tls.expected_peer_name == NULL);
	assert(server.tls.mode == TR_TLS_DISABLED);
	assert(server.tls.certificate_file == NULL);
	assert(server.tls.client_auth == TR_TLS_CLIENT_AUTH_NONE);
	assert(client.tls_credentials_provider.fetch == NULL);
	assert(client.tls_credentials_provider.cancel == NULL);
	assert(server.tls_credentials_provider.fetch == NULL);
	assert(server.tls_credentials_provider.cancel == NULL);
	assert(credentials_provider.fetch != NULL);
	assert(credentials_provider.cancel != NULL);
	assert(server.connection_groups.authorize_async == NULL);
	assert(server.connection_groups.authorize_cancel == NULL);
	server.connection_groups.authorize_async =
	        test_connection_group_authorize_async;
	server.connection_groups.authorize_cancel =
	        test_connection_group_authorize_cancel;
	assert(server.connection_groups.authorize_async != NULL);
	assert(server.connection_groups.authorize_cancel != NULL);
}

static void test_tls_disabled_rejects_ignored_fields(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_client *client_out = (struct tr_client *)(uintptr_t)1U;
	struct tr_server *server_out = (struct tr_server *)(uintptr_t)1U;

	tr_client_config_init(&client);
	client.tls.expected_peer_name = "server.example.test";
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_INVALID);
	assert(client_out == NULL);

	tr_server_config_init(&server);
	server.tls.certificate_file = "server.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_INVALID);
	assert(server_out == NULL);
}

static void test_tls_required_requires_identity_material(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_client *client_out = NULL;
	struct tr_server *server_out = NULL;

	tr_client_config_init(&client);
	client.tls.mode = TR_TLS_REQUIRED;
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_INVALID);
	assert(client_out == NULL);

	tr_server_config_init(&server);
	server.tls.mode = TR_TLS_REQUIRED;
	server.tls.certificate_file = "server.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_INVALID);
	assert(server_out == NULL);

	server.tls.certificate_file = NULL;
	server.tls.private_key_file = "server-key.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_INVALID);
	assert(server_out == NULL);
}

static void test_tls_invalid_enums_and_empty_values(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_client *client_out = NULL;
	struct tr_server *server_out = NULL;

	tr_client_config_init(&client);
	client.tls.mode = (enum tr_tls_mode)99;
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_INVALID);
	assert(client_out == NULL);

	client.tls.mode = TR_TLS_REQUIRED;
	client.tls.expected_peer_name = "server.example.test";
	client.tls.ca_file = "";
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_INVALID);
	assert(client_out == NULL);

	tr_server_config_init(&server);
	server.tls.client_auth = (enum tr_tls_client_auth_policy)99;
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_INVALID);
	assert(server_out == NULL);

	server.tls.client_auth = TR_TLS_CLIENT_AUTH_NONE;
	server.tls.mode = TR_TLS_REQUIRED;
	server.tls.certificate_file = "";
	server.tls.private_key_file = "server-key.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_INVALID);
	assert(server_out == NULL);
}

static void test_tls_required_v2_factories_and_transport_boundary(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_rpc_method_desc descriptor = tls_test_method_desc();
	struct tr_rpc_v2_method server_method = {
	        .descriptor = descriptor,
	        .work = tls_test_work,
	};
	struct tr_client *client_out = NULL;
	struct tr_server *server_out = NULL;

	tr_client_config_init(&client);
	client.tls.mode = TR_TLS_REQUIRED;
	client.tls.ca_file = test_identity.ca_file;
	client.tls.expected_peer_name = "server.example.test";
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_INVALID);
	assert(client_out == NULL);
	assert(tr_client_create(&client, &descriptor, 1U, &client_out) ==
	       TR_OK);
	assert(client_out != NULL);
	assert(tr_client_destroy(client_out) == TR_OK);
	client_out = NULL;
	client.tls.client_certificate_file =
	        test_identity.client_certificate_file;
	client.tls.client_private_key_file =
	        test_identity.client_private_key_file;
	assert(tr_client_create_transport(&client, &client_out) == TR_OK);
	assert(client_out != NULL);
	assert(tr_client_destroy(client_out) == TR_OK);
	client_out = NULL;
	assert(tr_client_create(&client, &descriptor, 1U, &client_out) ==
	       TR_OK);
	assert(client_out != NULL);
	assert(tr_client_destroy(client_out) == TR_OK);
	client_out = NULL;

	tr_server_config_init(&server);
	server.tls.mode = TR_TLS_REQUIRED;
	server.tls.certificate_file = test_identity.server_certificate_file;
	server.tls.private_key_file = test_identity.server_private_key_file;
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(server_out == NULL);
	assert(tr_server_create(&server, &server_method, 1U, &server_out) ==
	       TR_OK);
	assert(server_out != NULL);
	assert(tr_server_destroy(server_out) == TR_OK);
	server_out = NULL;
	server.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server.tls.client_ca_file = test_identity.ca_file;
	assert(tr_server_create(&server, &server_method, 1U, &server_out) ==
	       TR_OK);
	assert(server_out != NULL);
	assert(tr_server_destroy(server_out) == TR_OK);
	server_out = NULL;
}

static void test_tls_mtls_config_validation(void)
{
	struct tr_tls_client_config client = {
	        .mode = TR_TLS_REQUIRED,
	        .expected_peer_name = "server.example.test",
	        .client_certificate_file = "client.pem",
	        .client_private_key_file = "client-key.pem",
	};
	struct tr_tls_server_config server = {
	        .mode = TR_TLS_REQUIRED,
	        .certificate_file = "server.pem",
	        .private_key_file = "server-key.pem",
	        .client_ca_file = "client-ca.pem",
	        .client_auth = TR_TLS_CLIENT_AUTH_REQUIRED,
	};

	assert(tr_tls_client_config_validate(&client, 1) == TR_OK);
	assert(tr_tls_client_config_validate(&client, 0) == TR_ERR_UNSUPPORTED);
	client.client_private_key_file = NULL;
	assert(tr_tls_client_config_validate(&client, 1) == TR_ERR_INVALID);
	client.client_certificate_file = NULL;
	client.client_private_key_file = "client-key.pem";
	assert(tr_tls_client_config_validate(&client, 1) == TR_ERR_INVALID);

	assert(tr_tls_server_config_validate(&server, 1) == TR_OK);
	assert(tr_tls_server_config_validate(&server, 0) == TR_ERR_UNSUPPORTED);
	server.client_auth = TR_TLS_CLIENT_AUTH_OPTIONAL;
	assert(tr_tls_server_config_validate(&server, 1) == TR_OK);
	server.client_ca_file = NULL;
	assert(tr_tls_server_config_validate(&server, 1) == TR_ERR_INVALID);
	server.client_auth = TR_TLS_CLIENT_AUTH_NONE;
	server.client_ca_file = "client-ca.pem";
	assert(tr_tls_server_config_validate(&server, 1) == TR_ERR_INVALID);
	server.client_ca_file = NULL;
	assert(tr_tls_server_config_validate(&server, 1) == TR_OK);
}

static void test_peer_identity_snapshot_validates_and_copies_uri(void)
{
	static const uint8_t valid_uri[] = "spiffe://c-trpc.example/client";
	uint8_t too_long_uri[513];
	struct tr_peer_identity_snapshot *identity = NULL;
	struct tr_peer_identity_view view;
	const uint8_t embedded_nul[] = {'a', 0U, 'b'};
	const uint8_t control_byte[] = {'a', 0x1fU, 'b'};
	const uint8_t non_ia5_byte[] = {'a', 0x80U, 'b'};

	assert(tr_peer_identity_snapshot_create(
	               valid_uri, (uint32_t)sizeof(valid_uri) - 1U,
	               &identity) == TR_OK);
	assert(identity != NULL);
	assert(tr_peer_identity_snapshot_view(identity, &view) == TR_OK);
	assert(view.authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE);
	assert(view.uri_san_len == sizeof(valid_uri) - 1U);
	assert(memcmp(view.uri_san, valid_uri, sizeof(valid_uri) - 1U) == 0);
	assert(tr_peer_identity_snapshot_retain(identity) == TR_OK);
	tr_peer_identity_snapshot_release(identity);
	tr_peer_identity_snapshot_release(identity);
	identity = NULL;

	assert(tr_peer_identity_snapshot_create(valid_uri, 0U, &identity) ==
	       TR_ERR_INVALID);
	assert(identity == NULL);
	memset(too_long_uri, 'x', sizeof(too_long_uri));
	assert(tr_peer_identity_snapshot_create(
	               too_long_uri, (uint32_t)sizeof(too_long_uri),
	               &identity) == TR_ERR_BAD_LENGTH);
	assert(identity == NULL);
	assert(tr_peer_identity_snapshot_create(embedded_nul,
	                                        (uint32_t)sizeof(embedded_nul),
	                                        &identity) == TR_ERR_INVALID);
	assert(identity == NULL);
	assert(tr_peer_identity_snapshot_create(control_byte,
	                                        (uint32_t)sizeof(control_byte),
	                                        &identity) == TR_ERR_INVALID);
	assert(identity == NULL);
	assert(tr_peer_identity_snapshot_create(non_ia5_byte,
	                                        (uint32_t)sizeof(non_ia5_byte),
	                                        &identity) == TR_ERR_INVALID);
	assert(identity == NULL);
}

static void test_peer_identity_snapshot_is_charged_to_budget(void)
{
	static const uint8_t uri[] = "spiffe://c-trpc.example/client";
	struct tr_peer_identity_snapshot *identity = NULL;
	struct tr_memory_budget budget;
	struct tr_memory_budget_stats stats;
	tr_memory_budget_init(&budget, 1U);
	assert(tr_peer_identity_snapshot_create(uri, (uint32_t)sizeof(uri) - 1U,
	                                        &identity) == TR_OK);
	assert(tr_peer_identity_snapshot_charge(identity, &budget) == TR_AGAIN);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
	tr_memory_budget_init(&budget, 4096U);
	assert(tr_peer_identity_snapshot_charge(identity, &budget) == TR_OK);
	assert(tr_peer_identity_snapshot_charge(identity, &budget) ==
	       TR_ERR_STATE);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes >= sizeof(uri) - 1U);
	tr_peer_identity_snapshot_release(identity);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
}

static void test_openssl_client_profile_loads_explicit_trust(void)
{
	struct tr_tls_client_config config = {
	        .mode = TR_TLS_REQUIRED,
	        .ca_file = test_identity.ca_file,
	        .expected_peer_name = "server.example.test",
	};
	struct tr_tls_openssl_profile *profile = NULL;

	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_OK);
	assert(profile != NULL);
	tr_tls_openssl_profile_release(profile);

	profile = NULL;
	config.ca_file = NULL;
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_OK);
	tr_tls_openssl_profile_release(profile);

	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	config.ca_file = test_identity.invalid_ca_file;
	assert(tr_tls_openssl_client_profile_create(&config, &profile) < 0);
	assert(profile == NULL);

	config.ca_file = test_identity.ca_file;
	config.ca_directory = "";
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_ERR_INVALID);
	assert(profile == NULL);

	config.ca_file = test_identity.ca_file;
	config.expected_peer_name = "bad_name.example.test";
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_ERR_INVALID);
	assert(profile == NULL);

	config.expected_peer_name = "server.example.test";
	config.sni_name = "127.0.0.1";
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_ERR_INVALID);
	assert(profile == NULL);

	config.sni_name = NULL;
	config.ca_directory = NULL;
	config.client_certificate_file = "client.pem";
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_ERR_INVALID);
	assert(profile == NULL);

	config.client_certificate_file = test_identity.client_certificate_file;
	config.client_private_key_file =
	        test_identity.mismatched_private_key_file;
	assert(tr_tls_openssl_client_profile_create(&config, &profile) < 0);
	assert(profile == NULL);
	config.client_private_key_file = test_identity.client_private_key_file;
	assert(tr_tls_openssl_client_profile_create(&config, &profile) ==
	       TR_OK);
	assert(profile != NULL);
	tr_tls_openssl_profile_release(profile);
}

static void test_openssl_server_profile_checks_certificate_key_pair(void)
{
	struct tr_tls_server_config config = {
	        .mode = TR_TLS_REQUIRED,
	        .certificate_file = test_identity.server_certificate_file,
	        .private_key_file = test_identity.server_private_key_file,
	};
	struct tr_tls_openssl_profile *profile = NULL;

	assert(tr_tls_openssl_server_profile_create(&config, &profile) ==
	       TR_OK);
	assert(profile != NULL);
	tr_tls_openssl_profile_release(profile);

	profile = (struct tr_tls_openssl_profile *)(uintptr_t)1U;
	config.private_key_file = test_identity.mismatched_private_key_file;
	assert(tr_tls_openssl_server_profile_create(&config, &profile) < 0);
	assert(profile == NULL);

	config.private_key_file = test_identity.server_private_key_file;
	config.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	assert(tr_tls_openssl_server_profile_create(&config, &profile) ==
	       TR_ERR_INVALID);
	assert(profile == NULL);

	config.client_ca_file = test_identity.invalid_ca_file;
	assert(tr_tls_openssl_server_profile_create(&config, &profile) < 0);
	assert(profile == NULL);

	config.client_ca_file = test_identity.ca_file;
	assert(tr_tls_openssl_server_profile_create(&config, &profile) ==
	       TR_OK);
	assert(profile != NULL);
	tr_tls_openssl_profile_release(profile);
}

int main(void)
{
	atexit(cleanup_test_identity);
	assert(tls_test_identity_init(&test_identity, "tls-b-profile") == 0);
	test_tls_configs_default_disabled();
	test_tls_disabled_rejects_ignored_fields();
	test_tls_required_requires_identity_material();
	test_tls_invalid_enums_and_empty_values();
	test_tls_required_v2_factories_and_transport_boundary();
	test_tls_mtls_config_validation();
	test_peer_identity_snapshot_validates_and_copies_uri();
	test_peer_identity_snapshot_is_charged_to_budget();
	test_openssl_client_profile_loads_explicit_trust();
	test_openssl_server_profile_checks_certificate_key_pair();
	return 0;
}
