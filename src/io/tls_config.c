#include "tls_config.h"

#include <stddef.h>

#include "../../include/tr/status.h"

static int tr_tls_optional_string_valid(const char *value)
{
	return !value || value[0] != '\0';
}

static int tr_tls_client_has_extra(const struct tr_tls_client_config *config)
{
	return config->ca_file || config->ca_directory ||
	       config->expected_peer_name || config->sni_name ||
	       config->client_certificate_file ||
	       config->client_private_key_file;
}

int tr_tls_client_config_validate(const struct tr_tls_client_config *config,
                                  int backend_enabled)
{
	if (!config ||
	    (config->mode != TR_TLS_DISABLED &&
	     config->mode != TR_TLS_REQUIRED) ||
	    !tr_tls_optional_string_valid(config->ca_file) ||
	    !tr_tls_optional_string_valid(config->ca_directory) ||
	    !tr_tls_optional_string_valid(config->expected_peer_name) ||
	    !tr_tls_optional_string_valid(config->sni_name) ||
	    !tr_tls_optional_string_valid(config->client_certificate_file) ||
	    !tr_tls_optional_string_valid(config->client_private_key_file))
		return TR_ERR_INVALID;

	if (config->mode == TR_TLS_DISABLED)
		return tr_tls_client_has_extra(config) ? TR_ERR_INVALID : TR_OK;
	if (!config->expected_peer_name)
		return TR_ERR_INVALID;
	if (!!config->client_certificate_file !=
	    !!config->client_private_key_file)
		return TR_ERR_INVALID;
	return backend_enabled ? TR_OK : TR_ERR_UNSUPPORTED;
}

static int tr_tls_server_has_extra(const struct tr_tls_server_config *config)
{
	return config->certificate_file || config->private_key_file ||
	       config->client_ca_file ||
	       config->client_auth != TR_TLS_CLIENT_AUTH_NONE;
}

static int tr_tls_server_config_validate_internal(
        const struct tr_tls_server_config *config, int backend_enabled,
        int require_file_identity)
{
	if (!config ||
	    (config->mode != TR_TLS_DISABLED &&
	     config->mode != TR_TLS_REQUIRED) ||
	    (config->client_auth != TR_TLS_CLIENT_AUTH_NONE &&
	     config->client_auth != TR_TLS_CLIENT_AUTH_OPTIONAL &&
	     config->client_auth != TR_TLS_CLIENT_AUTH_REQUIRED) ||
	    !tr_tls_optional_string_valid(config->certificate_file) ||
	    !tr_tls_optional_string_valid(config->private_key_file) ||
	    !tr_tls_optional_string_valid(config->client_ca_file))
		return TR_ERR_INVALID;

	if (config->mode == TR_TLS_DISABLED)
		return tr_tls_server_has_extra(config) ? TR_ERR_INVALID : TR_OK;
	if (require_file_identity &&
	    (!config->certificate_file || !config->private_key_file))
		return TR_ERR_INVALID;
	if (!require_file_identity &&
	    (!!config->certificate_file != !!config->private_key_file))
		return TR_ERR_INVALID;
	if ((config->client_auth == TR_TLS_CLIENT_AUTH_NONE &&
	     config->client_ca_file) ||
	    (config->client_auth != TR_TLS_CLIENT_AUTH_NONE &&
	     !config->client_ca_file))
		return TR_ERR_INVALID;
	return backend_enabled ? TR_OK : TR_ERR_UNSUPPORTED;
}

int tr_tls_server_config_validate(const struct tr_tls_server_config *config,
                                  int backend_enabled)
{
	return tr_tls_server_config_validate_internal(config, backend_enabled,
	                                              1);
}

int tr_tls_server_config_validate_for_pem(
        const struct tr_tls_server_config *config, int backend_enabled)
{
	return tr_tls_server_config_validate_internal(config, backend_enabled,
	                                              0);
}

int tr_tls_credentials_provider_validate(
        const struct tr_tls_credentials_provider *provider,
        enum tr_tls_mode mode)
{
	int has_fetch;
	int has_cancel;

	if (!provider || (mode != TR_TLS_DISABLED && mode != TR_TLS_REQUIRED))
		return TR_ERR_INVALID;
	has_fetch = provider->fetch != NULL;
	has_cancel = provider->cancel != NULL;
	if (has_fetch != has_cancel || (has_fetch && mode != TR_TLS_REQUIRED))
		return TR_ERR_INVALID;
	return TR_OK;
}
