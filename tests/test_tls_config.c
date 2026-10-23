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
#include "../src/io/tls_openssl.h"
#include "tls_test_identity.h"

static struct tls_test_identity test_identity;

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

	tr_client_config_init(&client);
	tr_server_config_init(&server);
	assert(client.tls.mode == TR_TLS_DISABLED);
	assert(client.tls.ca_file == NULL);
	assert(client.tls.expected_peer_name == NULL);
	assert(server.tls.mode == TR_TLS_DISABLED);
	assert(server.tls.certificate_file == NULL);
	assert(server.tls.client_auth == TR_TLS_CLIENT_AUTH_NONE);
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

static void test_tls_required_public_factories_fail_before_activation(void)
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
	client.tls.expected_peer_name = "server.example.test";
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(client_out == NULL);
	assert(tr_client_create(&client, &descriptor, 1U, &client_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(client_out == NULL);

	tr_server_config_init(&server);
	server.tls.mode = TR_TLS_REQUIRED;
	server.tls.certificate_file = "server.pem";
	server.tls.private_key_file = "server-key.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(server_out == NULL);
	assert(tr_server_create(&server, &server_method, 1U, &server_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(server_out == NULL);
}

static void test_tls_b_rejects_not_yet_supported_client_auth(void)
{
	struct tr_client_config client;
	struct tr_server_config server;
	struct tr_client *client_out = NULL;
	struct tr_server *server_out = NULL;

	tr_client_config_init(&client);
	client.tls.mode = TR_TLS_REQUIRED;
	client.tls.expected_peer_name = "server.example.test";
	client.tls.client_certificate_file = "client.pem";
	client.tls.client_private_key_file = "client-key.pem";
	assert(tr_client_create_transport(&client, &client_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(client_out == NULL);

	tr_server_config_init(&server);
	server.tls.mode = TR_TLS_REQUIRED;
	server.tls.certificate_file = "server.pem";
	server.tls.private_key_file = "server-key.pem";
	server.tls.client_auth = TR_TLS_CLIENT_AUTH_OPTIONAL;
	server.tls.client_ca_file = "client-ca.pem";
	assert(tr_server_create_transport(&server, &server_out) ==
	       TR_ERR_UNSUPPORTED);
	assert(server_out == NULL);
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
	       TR_ERR_UNSUPPORTED);
	assert(profile == NULL);
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
	       TR_ERR_UNSUPPORTED);
	assert(profile == NULL);
}

int main(void)
{
	atexit(cleanup_test_identity);
	assert(tls_test_identity_init(&test_identity, "tls-b-profile") == 0);
	test_tls_configs_default_disabled();
	test_tls_disabled_rejects_ignored_fields();
	test_tls_required_requires_identity_material();
	test_tls_invalid_enums_and_empty_values();
	test_tls_required_public_factories_fail_before_activation();
	test_tls_b_rejects_not_yet_supported_client_auth();
	test_openssl_client_profile_loads_explicit_trust();
	test_openssl_server_profile_checks_certificate_key_pair();
	return 0;
}
