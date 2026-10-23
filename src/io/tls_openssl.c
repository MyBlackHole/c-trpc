#define _POSIX_C_SOURCE 200809L
#include "tls_openssl.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/tr/status.h"
#include "byte_endpoint.h"
#include "tls_config.h"

enum tr_tls_openssl_role {
	TR_TLS_OPENSSL_CLIENT = 1,
	TR_TLS_OPENSSL_SERVER = 2
};

struct tr_tls_openssl_profile {
	SSL_CTX *context;
	enum tr_tls_openssl_role role;
	char *sni_name;
};

struct tr_tls_openssl_session {
	SSL *ssl;
	enum tr_tls_openssl_role role;
	int early_data_read_finished;
	const void *pending_write_buffer;
	size_t pending_write_length;
	int write_pending;
};

static int tr_tls_dns_name_valid(const char *name)
{
	size_t length;
	size_t label_start = 0U;
	size_t index;

	if (!name || name[0] == '\0')
		return 0;
	length = strlen(name);
	if (length > 254U)
		return 0;
	if (name[length - 1U] == '.')
		--length;
	if (length == 0U)
		return 0;
	for (index = 0U; index <= length; ++index) {
		unsigned char character = index == length
		                                  ? (unsigned char)'.'
		                                  : (unsigned char)name[index];
		if (character == '.') {
			size_t label_length = index - label_start;
			if (label_length == 0U || label_length > 63U ||
			    name[label_start] == '-' || name[index - 1U] == '-')
				return 0;
			label_start = index + 1U;
			continue;
		}
		if (!((character >= 'a' && character <= 'z') ||
		      (character >= 'A' && character <= 'Z') ||
		      (character >= '0' && character <= '9') ||
		      character == '-'))
			return 0;
	}
	return 1;
}

static int tr_tls_name_is_ip(const char *name)
{
	struct in_addr ipv4;
	struct in6_addr ipv6;
	return inet_pton(AF_INET, name, &ipv4) == 1 ||
	       inet_pton(AF_INET6, name, &ipv6) == 1;
}

static char *tr_tls_string_copy(const char *value)
{
	size_t length;
	char *copy;
	if (!value)
		return NULL;
	length = strlen(value);
	if (length == SIZE_MAX)
		return NULL;
	copy = (char *)malloc(length + 1U);
	if (!copy)
		return NULL;
	memcpy(copy, value, length + 1U);
	return copy;
}

static int tr_tls_openssl_context_create(SSL_CTX **out)
{
	SSL_CTX *context;
	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	context = SSL_CTX_new(TLS_method());
	if (!context)
		return TR_ERR_NOMEM;
	if (SSL_CTX_set_min_proto_version(context, TLS1_3_VERSION) != 1 ||
	    SSL_CTX_set_max_proto_version(context, TLS1_3_VERSION) != 1 ||
	    SSL_CTX_set_max_early_data(context, 0U) != 1 ||
	    SSL_CTX_set_recv_max_early_data(context, 0U) != 1) {
		SSL_CTX_free(context);
		return TR_ERR_INVALID;
	}
	*out = context;
	return TR_OK;
}

static int tr_tls_openssl_profile_alloc(enum tr_tls_openssl_role role,
                                        struct tr_tls_openssl_profile **out)
{
	struct tr_tls_openssl_profile *profile;
	SSL_CTX *context = NULL;
	int status;
	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	profile = (struct tr_tls_openssl_profile *)calloc(1, sizeof(*profile));
	if (!profile)
		return TR_ERR_NOMEM;
	status = tr_tls_openssl_context_create(&context);
	if (status != TR_OK) {
		free(profile);
		return status;
	}
	profile->context = context;
	profile->role = role;
	*out = profile;
	return TR_OK;
}

int tr_tls_openssl_client_profile_create(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out)
{
	struct tr_tls_openssl_profile *profile = NULL;
	X509_VERIFY_PARAM *verify_parameters;
	int status;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	status = tr_tls_client_config_validate(config, 1);
	if (status != TR_OK)
		return status;
	if (config->mode != TR_TLS_REQUIRED ||
	    (config->sni_name && (!tr_tls_dns_name_valid(config->sni_name) ||
	                          tr_tls_name_is_ip(config->sni_name))))
		return TR_ERR_INVALID;
	if (!tr_tls_name_is_ip(config->expected_peer_name) &&
	    !tr_tls_dns_name_valid(config->expected_peer_name))
		return TR_ERR_INVALID;

	status = tr_tls_openssl_profile_alloc(TR_TLS_OPENSSL_CLIENT, &profile);
	if (status != TR_OK)
		return status;
	if (config->ca_file || config->ca_directory) {
		if (SSL_CTX_load_verify_locations(profile->context,
		                                  config->ca_file,
		                                  config->ca_directory) != 1) {
			status = TR_ERR_INVALID;
			goto fail;
		}
	} else if (SSL_CTX_set_default_verify_paths(profile->context) != 1) {
		status = TR_ERR_INVALID;
		goto fail;
	}
	SSL_CTX_set_verify(profile->context, SSL_VERIFY_PEER, NULL);
	verify_parameters = SSL_CTX_get0_param(profile->context);
	if (!verify_parameters ||
	    X509_VERIFY_PARAM_set_purpose(verify_parameters,
	                                  X509_PURPOSE_SSL_SERVER) != 1) {
		status = TR_ERR_INVALID;
		goto fail;
	}
	X509_VERIFY_PARAM_set_hostflags(
	        verify_parameters,
	        X509_CHECK_FLAG_NEVER_CHECK_SUBJECT |
	                X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
	if ((tr_tls_name_is_ip(config->expected_peer_name)
	             ? X509_VERIFY_PARAM_set1_ip_asc(verify_parameters,
	                                             config->expected_peer_name)
	             : X509_VERIFY_PARAM_set1_host(verify_parameters,
	                                           config->expected_peer_name,
	                                           0U)) != 1) {
		status = TR_ERR_INVALID;
		goto fail;
	}
	if (config->sni_name) {
		profile->sni_name = tr_tls_string_copy(config->sni_name);
		if (!profile->sni_name) {
			status = TR_ERR_NOMEM;
			goto fail;
		}
	}
	*out = profile;
	return TR_OK;

fail:
	tr_tls_openssl_profile_release(profile);
	return status;
}

int tr_tls_openssl_server_profile_create(
        const struct tr_tls_server_config *config,
        struct tr_tls_openssl_profile **out)
{
	struct tr_tls_openssl_profile *profile = NULL;
	int status;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	status = tr_tls_server_config_validate(config, 1);
	if (status != TR_OK)
		return status;
	if (config->mode != TR_TLS_REQUIRED)
		return TR_ERR_INVALID;
	status = tr_tls_openssl_profile_alloc(TR_TLS_OPENSSL_SERVER, &profile);
	if (status != TR_OK)
		return status;
	if (SSL_CTX_use_certificate_chain_file(profile->context,
	                                       config->certificate_file) != 1 ||
	    SSL_CTX_use_PrivateKey_file(profile->context,
	                                config->private_key_file,
	                                SSL_FILETYPE_PEM) != 1 ||
	    SSL_CTX_check_private_key(profile->context) != 1) {
		status = TR_ERR_INVALID;
		goto fail;
	}
	*out = profile;
	return TR_OK;

fail:
	tr_tls_openssl_profile_release(profile);
	return status;
}

void tr_tls_openssl_profile_release(struct tr_tls_openssl_profile *profile)
{
	if (!profile)
		return;
	SSL_CTX_free(profile->context);
	free(profile->sni_name);
	free(profile);
}

static int tr_tls_openssl_error_status(struct tr_tls_openssl_session *session,
                                       int ssl_result, uint32_t *wait_events)
{
	int ssl_error = SSL_get_error(session->ssl, ssl_result);
	switch (ssl_error) {
	case SSL_ERROR_WANT_READ:
		*wait_events = TR_BYTE_ENDPOINT_EVENT_READ;
		return TR_AGAIN;
	case SSL_ERROR_WANT_WRITE:
		*wait_events = TR_BYTE_ENDPOINT_EVENT_WRITE;
		return TR_AGAIN;
	case SSL_ERROR_ZERO_RETURN:
		return TR_ERR_CLOSED;
	default:
		return TR_ERR_SYS;
	}
}

int tr_tls_openssl_session_create(struct tr_tls_openssl_profile *profile,
                                  int fd, struct tr_tls_openssl_session **out)
{
	struct tr_tls_openssl_session *session;
	BIO *socket_bio;
	int flags;
	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!profile || !profile->context || fd < 0 ||
	    (profile->role != TR_TLS_OPENSSL_CLIENT &&
	     profile->role != TR_TLS_OPENSSL_SERVER))
		return TR_ERR_INVALID;
	flags = fcntl(fd, F_GETFL);
	if (flags < 0)
		return TR_ERR_SYS;
	if ((flags & O_NONBLOCK) == 0)
		return TR_ERR_INVALID;
	session = (struct tr_tls_openssl_session *)calloc(1, sizeof(*session));
	if (!session)
		return TR_ERR_NOMEM;
	session->ssl = SSL_new(profile->context);
	if (!session->ssl) {
		free(session);
		return TR_ERR_NOMEM;
	}
	socket_bio = BIO_new_socket(fd, BIO_NOCLOSE);
	if (!socket_bio) {
		SSL_free(session->ssl);
		free(session);
		return TR_ERR_NOMEM;
	}
	SSL_set_bio(session->ssl, socket_bio, socket_bio);
	session->role = profile->role;
	if (session->role == TR_TLS_OPENSSL_CLIENT) {
		if (profile->sni_name &&
		    SSL_set_tlsext_host_name(session->ssl, profile->sni_name) !=
		            1) {
			SSL_free(session->ssl);
			free(session);
			return TR_ERR_INVALID;
		}
		SSL_set_connect_state(session->ssl);
	} else {
		SSL_set_accept_state(session->ssl);
	}
	*out = session;
	return TR_OK;
}

static int tr_tls_openssl_handshake(void *arg, uint32_t *wait_events)
{
	struct tr_tls_openssl_session *session =
	        (struct tr_tls_openssl_session *)arg;
	int result;
	if (!session || !wait_events)
		return TR_ERR_INVALID;
	*wait_events = 0U;
	if (session->role == TR_TLS_OPENSSL_SERVER &&
	    !session->early_data_read_finished) {
		unsigned char discarded_early_data;
		size_t read_bytes = 0U;
		ERR_clear_error();
		result = SSL_read_early_data(
		        session->ssl, &discarded_early_data,
		        sizeof(discarded_early_data), &read_bytes);
		if (result == SSL_READ_EARLY_DATA_ERROR)
			return tr_tls_openssl_error_status(session, result,
			                                   wait_events);
		if (result == SSL_READ_EARLY_DATA_SUCCESS) {
			/* max_early_data is zero: never pass early bytes to the
			 * app. */
			return TR_ERR_SYS;
		}
		if (result != SSL_READ_EARLY_DATA_FINISH || read_bytes != 0U)
			return TR_ERR_STATE;
		session->early_data_read_finished = 1;
		/* Schedule SSL_do_handshake() as a separate nonblocking
		 * progress. */
		*wait_events = TR_BYTE_ENDPOINT_EVENT_WRITE;
		return TR_AGAIN;
	}
	ERR_clear_error();
	result = SSL_do_handshake(session->ssl);
	if (result == 1)
		return TR_OK;
	return tr_tls_openssl_error_status(session, result, wait_events);
}

static int tr_tls_openssl_read(void *arg, void *buffer, size_t capacity,
                               size_t *read_bytes, uint32_t *wait_events)
{
	struct tr_tls_openssl_session *session =
	        (struct tr_tls_openssl_session *)arg;
	int result;
	if (!session || !buffer || !read_bytes || !wait_events)
		return TR_ERR_INVALID;
	*read_bytes = 0U;
	*wait_events = 0U;
	ERR_clear_error();
	result = SSL_read_ex(session->ssl, buffer, capacity, read_bytes);
	if (result == 1) {
		if (*read_bytes == 0U || *read_bytes > capacity)
			return TR_ERR_STATE;
		return TR_OK;
	}
	*read_bytes = 0U;
	return tr_tls_openssl_error_status(session, result, wait_events);
}

static int tr_tls_openssl_write(void *arg, const void *buffer, size_t length,
                                size_t *written_bytes, uint32_t *wait_events)
{
	struct tr_tls_openssl_session *session =
	        (struct tr_tls_openssl_session *)arg;
	int result;
	if (!session || !buffer || length == 0U || !written_bytes ||
	    !wait_events)
		return TR_ERR_INVALID;
	*written_bytes = 0U;
	*wait_events = 0U;
	if (session->write_pending &&
	    (session->pending_write_buffer != buffer ||
	     session->pending_write_length != length))
		return TR_ERR_STATE;
	ERR_clear_error();
	result = SSL_write_ex(session->ssl, buffer, length, written_bytes);
	if (result == 1) {
		if (*written_bytes == 0U || *written_bytes > length) {
			*written_bytes = 0U;
			return TR_ERR_STATE;
		}
		session->write_pending = 0;
		session->pending_write_buffer = NULL;
		session->pending_write_length = 0U;
		return TR_OK;
	}
	*written_bytes = 0U;
	result = tr_tls_openssl_error_status(session, result, wait_events);
	if (result == TR_AGAIN) {
		session->write_pending = 1;
		session->pending_write_buffer = buffer;
		session->pending_write_length = length;
		return result;
	}
	session->write_pending = 0;
	session->pending_write_buffer = NULL;
	session->pending_write_length = 0U;
	return result;
}

static int tr_tls_openssl_shutdown(void *arg, uint32_t *wait_events)
{
	struct tr_tls_openssl_session *session =
	        (struct tr_tls_openssl_session *)arg;
	int result;
	if (!session || !wait_events)
		return TR_ERR_INVALID;
	*wait_events = 0U;
	ERR_clear_error();
	result = SSL_shutdown(session->ssl);
	if (result == 1)
		return TR_OK;
	if (result == 0) {
		*wait_events = TR_BYTE_ENDPOINT_EVENT_READ;
		return TR_AGAIN;
	}
	return tr_tls_openssl_error_status(session, result, wait_events);
}

static void tr_tls_openssl_session_release(void *arg)
{
	struct tr_tls_openssl_session *session =
	        (struct tr_tls_openssl_session *)arg;
	if (!session)
		return;
	SSL_free(session->ssl);
	session->ssl = NULL;
	free(session);
}

static const struct tr_byte_endpoint_driver tr_tls_openssl_byte_driver = {
        .handshake = tr_tls_openssl_handshake,
        .read = tr_tls_openssl_read,
        .write = tr_tls_openssl_write,
        .shutdown = tr_tls_openssl_shutdown,
        .release = tr_tls_openssl_session_release,
};

const struct tr_byte_endpoint_driver *tr_tls_openssl_driver(void)
{
	return &tr_tls_openssl_byte_driver;
}

SSL *tr_tls_openssl_session_ssl_for_test(struct tr_tls_openssl_session *session)
{
	return session ? session->ssl : NULL;
}
