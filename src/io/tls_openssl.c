#define _POSIX_C_SOURCE 200809L
#include "tls_openssl.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/bio.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../../include/tr/status.h"
#include "byte_endpoint.h"
#include "peer_identity_internal.h"
#include "tls_config.h"
#include "../memory_budget.h"
#include "../refcount.h"

enum tr_tls_openssl_role {
	TR_TLS_OPENSSL_CLIENT = 1,
	TR_TLS_OPENSSL_SERVER = 2
};

struct tr_tls_openssl_profile {
	struct tr_refcount references;
	SSL_CTX *context;
	enum tr_tls_openssl_role role;
	uint64_t generation;
	int peer_identity_enabled;
	int peer_identity_required;
	char *sni_name;
};

struct tr_tls_openssl_session {
	SSL *ssl;
	enum tr_tls_openssl_role role;
	int early_data_read_finished;
	const void *pending_write_buffer;
	size_t pending_write_length;
	int write_pending;
	struct tr_memory_budget *memory_budget;
	uint64_t memory_bytes;
};

struct tr_tls_openssl_socket_bio_data {
	int fd;
	int close_fd;
};

static BIO_METHOD *tr_tls_openssl_socket_bio_method;
static CRYPTO_ONCE tr_tls_openssl_socket_bio_once = CRYPTO_ONCE_STATIC_INIT;

static void
tr_tls_openssl_session_destroy(struct tr_tls_openssl_session *session)
{
	struct tr_memory_budget *memory_budget;
	uint64_t memory_bytes;
	if (!session)
		return;
	memory_budget = session->memory_budget;
	memory_bytes = session->memory_bytes;
	if (session->ssl)
		SSL_free(session->ssl);
	session->ssl = NULL;
	free(session);
	if (memory_budget && memory_bytes != 0U)
		(void)tr_memory_budget_release(memory_budget, memory_bytes);
}

static int tr_tls_openssl_socket_bio_create(BIO *bio)
{
	struct tr_tls_openssl_socket_bio_data *data;

	if (!bio)
		return 0;
	data = (struct tr_tls_openssl_socket_bio_data *)calloc(1,
	                                                       sizeof(*data));
	if (!data)
		return 0;
	data->fd = -1;
	BIO_set_data(bio, data);
	BIO_set_init(bio, 0);
	return 1;
}

static int tr_tls_openssl_socket_bio_destroy(BIO *bio)
{
	struct tr_tls_openssl_socket_bio_data *data;

	if (!bio)
		return 0;
	data = (struct tr_tls_openssl_socket_bio_data *)BIO_get_data(bio);
	if (data) {
		if (BIO_get_init(bio) && data->close_fd && data->fd >= 0)
			(void)close(data->fd);
		free(data);
	}
	BIO_set_data(bio, NULL);
	BIO_set_init(bio, 0);
	return 1;
}

static ssize_t tr_tls_openssl_socket_bio_read(BIO *bio, char *buffer,
                                              size_t capacity)
{
	struct tr_tls_openssl_socket_bio_data *data =
	        (struct tr_tls_openssl_socket_bio_data *)BIO_get_data(bio);
	ssize_t result;

	BIO_clear_retry_flags(bio);
	if (!data || data->fd < 0) {
		errno = EBADF;
		return -1;
	}
	do {
		result = recv(data->fd, buffer, capacity, MSG_DONTWAIT);
	} while (result < 0 && errno == EINTR);
	if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		BIO_set_retry_read(bio);
	return result;
}

static ssize_t tr_tls_openssl_socket_bio_write(BIO *bio, const char *buffer,
                                               size_t length)
{
	struct tr_tls_openssl_socket_bio_data *data =
	        (struct tr_tls_openssl_socket_bio_data *)BIO_get_data(bio);
	ssize_t result;

	BIO_clear_retry_flags(bio);
	if (!data || data->fd < 0) {
		errno = EBADF;
		return -1;
	}
	do {
		result = send(data->fd, buffer, length,
		              MSG_DONTWAIT | MSG_NOSIGNAL);
	} while (result < 0 && errno == EINTR);
	if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
		BIO_set_retry_write(bio);
	return result;
}

static int tr_tls_openssl_socket_bio_read_ex(BIO *bio, char *buffer,
                                             size_t capacity,
                                             size_t *read_bytes)
{
	ssize_t result;

	if (!buffer || !read_bytes)
		return 0;
	*read_bytes = 0U;
	result = tr_tls_openssl_socket_bio_read(bio, buffer, capacity);
	if (result <= 0)
		return 0;
	*read_bytes = (size_t)result;
	return 1;
}

static int tr_tls_openssl_socket_bio_write_ex(BIO *bio, const char *buffer,
                                              size_t length,
                                              size_t *written_bytes)
{
	ssize_t result;

	if (!buffer || !written_bytes)
		return 0;
	*written_bytes = 0U;
	result = tr_tls_openssl_socket_bio_write(bio, buffer, length);
	if (result <= 0)
		return 0;
	*written_bytes = (size_t)result;
	return 1;
}

static int tr_tls_openssl_socket_bio_read_legacy(BIO *bio, char *buffer,
                                                 int capacity)
{
	ssize_t result;
	if (!buffer || capacity <= 0)
		return 0;
	result = tr_tls_openssl_socket_bio_read(bio, buffer, (size_t)capacity);
	return result > INT_MAX ? INT_MAX : (int)result;
}

static int tr_tls_openssl_socket_bio_write_legacy(BIO *bio, const char *buffer,
                                                  int length)
{
	ssize_t result;
	if (!buffer || length <= 0)
		return 0;
	result = tr_tls_openssl_socket_bio_write(bio, buffer, (size_t)length);
	return result > INT_MAX ? INT_MAX : (int)result;
}

static long tr_tls_openssl_socket_bio_ctrl(BIO *bio, int command, long value,
                                           void *pointer)
{
	struct tr_tls_openssl_socket_bio_data *data =
	        (struct tr_tls_openssl_socket_bio_data *)BIO_get_data(bio);

	switch (command) {
	case BIO_C_GET_FD:
		if (!data)
			return -1;
		if (pointer)
			*(int *)pointer = data->fd;
		return data->fd;
	case BIO_CTRL_GET_CLOSE:
		return data ? data->close_fd : 0;
	case BIO_CTRL_SET_CLOSE:
		if (!data)
			return 0;
		data->close_fd = value != BIO_NOCLOSE;
		return 1;
	case BIO_CTRL_FLUSH:
		return 1;
	case BIO_CTRL_PENDING:
	case BIO_CTRL_WPENDING:
	case BIO_CTRL_EOF:
		return 0;
	default:
		return 0;
	}
}

static void tr_tls_openssl_socket_bio_method_init(void)
{
	int type = BIO_get_new_index();
	BIO_METHOD *method;

	if (type < 0)
		return;
	method = BIO_meth_new(type | BIO_TYPE_SOURCE_SINK | BIO_TYPE_DESCRIPTOR,
	                      "c-trpc nonblocking socket");
	if (!method)
		return;
	if (BIO_meth_set_create(method, tr_tls_openssl_socket_bio_create) !=
	            1 ||
	    BIO_meth_set_destroy(method, tr_tls_openssl_socket_bio_destroy) !=
	            1 ||
	    BIO_meth_set_read(method, tr_tls_openssl_socket_bio_read_legacy) !=
	            1 ||
	    BIO_meth_set_write(method,
	                       tr_tls_openssl_socket_bio_write_legacy) != 1 ||
	    BIO_meth_set_read_ex(method, tr_tls_openssl_socket_bio_read_ex) !=
	            1 ||
	    BIO_meth_set_write_ex(method, tr_tls_openssl_socket_bio_write_ex) !=
	            1 ||
	    BIO_meth_set_ctrl(method, tr_tls_openssl_socket_bio_ctrl) != 1) {
		BIO_meth_free(method);
		return;
	}
	tr_tls_openssl_socket_bio_method = method;
}

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
	/* OpenSSL 默认允许 100 KiB；固定策略，避免依赖后端隐式默认值。 */
	(void)SSL_CTX_set_max_cert_list(
	        context, TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES);
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
	profile->generation = 1U;
	status = tr_refcount_init(&profile->references, 1U);
	if (status != TR_OK) {
		SSL_CTX_free(context);
		free(profile);
		return status;
	}
	*out = profile;
	return TR_OK;
}

static int tr_tls_openssl_pem_space(uint8_t byte)
{
	return byte == ' ' || byte == '\t' || byte == '\r' || byte == '\n' ||
	       byte == '\f' || byte == '\v';
}

static size_t tr_tls_openssl_pem_skip_space(const uint8_t *data, size_t length,
                                            size_t offset)
{
	while (offset < length && tr_tls_openssl_pem_space(data[offset]))
		++offset;
	return offset;
}

static size_t tr_tls_openssl_pem_find(const uint8_t *data, size_t length,
                                      size_t offset, const char *marker)
{
	size_t marker_length = strlen(marker);
	size_t index;

	if (marker_length > length || offset > length - marker_length)
		return SIZE_MAX;
	for (index = offset; index <= length - marker_length; ++index) {
		if (memcmp(data + index, marker, marker_length) == 0)
			return index;
	}
	return SIZE_MAX;
}

static int
tr_tls_openssl_bundle_validate(const struct tr_tls_credential_bundle *bundle,
                               uint32_t max_bundle_bytes)
{
	size_t total = 0U;
	const struct tr_tls_bytes *fields[3];
	size_t index;

	if (!bundle || max_bundle_bytes == 0U ||
	    !bundle->certificate_chain_pem.data ||
	    bundle->certificate_chain_pem.len == 0U ||
	    !bundle->private_key_pem.data ||
	    bundle->private_key_pem.len == 0U ||
	    (bundle->private_key_password.len != 0U &&
	     !bundle->private_key_password.data))
		return TR_ERR_INVALID;
	fields[0] = &bundle->certificate_chain_pem;
	fields[1] = &bundle->private_key_pem;
	fields[2] = &bundle->private_key_password;
	for (index = 0U; index < 3U; ++index) {
		if (fields[index]->len > (size_t)INT_MAX ||
		    fields[index]->len > (size_t)max_bundle_bytes - total)
			return TR_ERR_BAD_LENGTH;
		total += fields[index]->len;
	}
	return total == 0U ? TR_ERR_INVALID : TR_OK;
}

static int
tr_tls_openssl_pem_certificate_chain_install(SSL_CTX *context,
                                             const struct tr_tls_bytes *chain)
{
	static const char begin_marker[] = "-----BEGIN CERTIFICATE-----";
	static const char end_marker[] = "-----END CERTIFICATE-----";
	size_t offset = 0U;
	unsigned int certificate_count = 0U;

	while ((offset = tr_tls_openssl_pem_skip_space(chain->data, chain->len,
	                                               offset)) < chain->len) {
		BIO *bio = NULL;
		X509 *certificate = NULL;
		size_t end;
		size_t block_length;
		int status = TR_ERR_INVALID;

		if (chain->len - offset < sizeof(begin_marker) - 1U ||
		    memcmp(chain->data + offset, begin_marker,
		           sizeof(begin_marker) - 1U) != 0)
			return TR_ERR_INVALID;
		end = tr_tls_openssl_pem_find(
		        chain->data, chain->len,
		        offset + sizeof(begin_marker) - 1U, end_marker);
		if (end == SIZE_MAX)
			return TR_ERR_INVALID;
		block_length = end + sizeof(end_marker) - 1U - offset;
		if (block_length > (size_t)INT_MAX)
			return TR_ERR_BAD_LENGTH;
		bio = BIO_new_mem_buf(chain->data + offset, (int)block_length);
		if (!bio)
			return TR_ERR_NOMEM;
		certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL);
		if (!certificate || BIO_ctrl_pending(bio) != 0L)
			goto certificate_done;
		if (certificate_count == 0U) {
			if (SSL_CTX_use_certificate(context, certificate) != 1)
				goto certificate_done;
		} else if (SSL_CTX_add1_chain_cert(context, certificate) != 1) {
			goto certificate_done;
		}
		++certificate_count;
		status = TR_OK;

	certificate_done:
		X509_free(certificate);
		BIO_free(bio);
		if (status != TR_OK)
			return status;
		offset = end + sizeof(end_marker) - 1U;
	}
	return certificate_count == 0U ? TR_ERR_INVALID : TR_OK;
}

static int
tr_tls_openssl_pem_private_key_bounds(const struct tr_tls_bytes *private_key,
                                      size_t *out_offset, size_t *out_length)
{
	static const char begin_prefix[] = "-----BEGIN ";
	size_t offset;
	size_t label_start;
	size_t label_end;
	size_t marker_start;
	size_t marker_end;
	size_t block_end;
	char end_marker[128];
	int written;

	if (!private_key || !out_offset || !out_length)
		return TR_ERR_INVALID;
	offset = tr_tls_openssl_pem_skip_space(private_key->data,
	                                       private_key->len, 0U);
	if (private_key->len - offset < sizeof(begin_prefix) - 1U ||
	    memcmp(private_key->data + offset, begin_prefix,
	           sizeof(begin_prefix) - 1U) != 0)
		return TR_ERR_INVALID;
	label_start = offset + sizeof(begin_prefix) - 1U;
	label_end = tr_tls_openssl_pem_find(private_key->data, private_key->len,
	                                    label_start, "-----");
	if (label_end == SIZE_MAX || label_end == label_start ||
	    label_end - label_start > 64U)
		return TR_ERR_INVALID;
	for (size_t index = label_start; index < label_end; ++index) {
		uint8_t character = private_key->data[index];
		if (!((character >= 'A' && character <= 'Z') ||
		      (character >= '0' && character <= '9') ||
		      character == ' '))
			return TR_ERR_INVALID;
	}
	written = snprintf(end_marker, sizeof(end_marker), "-----END %.*s-----",
	                   (int)(label_end - label_start),
	                   (const char *)private_key->data + label_start);
	if (written <= 0 || (size_t)written >= sizeof(end_marker))
		return TR_ERR_INVALID;
	marker_start = label_end + 5U;
	marker_end = tr_tls_openssl_pem_find(
	        private_key->data, private_key->len, marker_start, end_marker);
	if (marker_end == SIZE_MAX)
		return TR_ERR_INVALID;
	block_end = marker_end + (size_t)written;
	if (tr_tls_openssl_pem_skip_space(private_key->data, private_key->len,
	                                  block_end) != private_key->len)
		return TR_ERR_INVALID;
	*out_offset = offset;
	*out_length = block_end - offset;
	return TR_OK;
}

struct tr_tls_openssl_password_view {
	const uint8_t *data;
	size_t length;
};

static int tr_tls_openssl_password_copy(char *buffer, int capacity, int writing,
                                        void *arg)
{
	const struct tr_tls_openssl_password_view *password = arg;
	(void)writing;
	if (!password || capacity < 0 || password->length > (size_t)capacity ||
	    password->length > (size_t)INT_MAX)
		return 0;
	if (password->length != 0U)
		memcpy(buffer, password->data, password->length);
	return (int)password->length;
}

static int tr_tls_openssl_pem_private_key_read(
        const struct tr_tls_credential_bundle *bundle, EVP_PKEY **out_key)
{
	struct tr_tls_openssl_password_view password;
	BIO *bio = NULL;
	EVP_PKEY *key = NULL;
	uint8_t *private_key_copy = NULL;
	uint8_t *password_copy = NULL;
	size_t key_offset;
	size_t key_length;
	int status;

	if (!out_key)
		return TR_ERR_INVALID;
	*out_key = NULL;
	status = tr_tls_openssl_pem_private_key_bounds(
	        &bundle->private_key_pem, &key_offset, &key_length);
	if (status != TR_OK)
		return status;
	private_key_copy = malloc(key_length);
	if (!private_key_copy)
		return TR_ERR_NOMEM;
	memcpy(private_key_copy, bundle->private_key_pem.data + key_offset,
	       key_length);
	if (bundle->private_key_password.len != 0U) {
		password_copy = malloc(bundle->private_key_password.len);
		if (!password_copy) {
			status = TR_ERR_NOMEM;
			goto done;
		}
		memcpy(password_copy, bundle->private_key_password.data,
		       bundle->private_key_password.len);
	}
	password.data = password_copy;
	password.length = bundle->private_key_password.len;
	bio = BIO_new_mem_buf(private_key_copy, (int)key_length);
	if (!bio) {
		status = TR_ERR_NOMEM;
		goto done;
	}
	key = PEM_read_bio_PrivateKey(bio, NULL, tr_tls_openssl_password_copy,
	                              &password);
	if (!key || BIO_ctrl_pending(bio) != 0L) {
		status = TR_ERR_INVALID;
		goto done;
	}
	*out_key = key;
	key = NULL;
	status = TR_OK;

done:
	EVP_PKEY_free(key);
	BIO_free(bio);
	if (private_key_copy) {
		OPENSSL_cleanse(private_key_copy, key_length);
		free(private_key_copy);
	}
	if (password_copy) {
		OPENSSL_cleanse(password_copy,
		                bundle->private_key_password.len);
		free(password_copy);
	}
	return status;
}

static int tr_tls_openssl_pem_identity_install(
        SSL_CTX *context, const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes)
{
	EVP_PKEY *private_key = NULL;
	int status = tr_tls_openssl_bundle_validate(bundle, max_bundle_bytes);
	if (status != TR_OK)
		return status;
	status = tr_tls_openssl_pem_certificate_chain_install(
	        context, &bundle->certificate_chain_pem);
	if (status != TR_OK)
		return status;
	status = tr_tls_openssl_pem_private_key_read(bundle, &private_key);
	if (status != TR_OK)
		return status;
	if (SSL_CTX_use_PrivateKey(context, private_key) != 1 ||
	    SSL_CTX_check_private_key(context) != 1)
		status = TR_ERR_INVALID;
	EVP_PKEY_free(private_key);
	return status;
}

static int tr_tls_openssl_client_profile_create_internal(
        const struct tr_tls_client_config *config, int peer_identity_enabled,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out)
{
	struct tr_tls_openssl_profile *profile = NULL;
	struct tr_tls_client_config policy_config;
	X509_VERIFY_PARAM *verify_parameters;
	int status;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config)
		return TR_ERR_INVALID;
	policy_config = *config;
	if (bundle) {
		if (!!config->client_certificate_file !=
		    !!config->client_private_key_file)
			return TR_ERR_INVALID;
		policy_config.client_certificate_file = NULL;
		policy_config.client_private_key_file = NULL;
	}
	status = tr_tls_client_config_validate(&policy_config, 1);
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
	if (bundle) {
		status = tr_tls_openssl_pem_identity_install(
		        profile->context, bundle, max_bundle_bytes);
		if (status != TR_OK)
			goto fail;
	} else if (config->client_certificate_file &&
	           (SSL_CTX_use_certificate_chain_file(
	                    profile->context,
	                    config->client_certificate_file) != 1 ||
	            SSL_CTX_use_PrivateKey_file(profile->context,
	                                        config->client_private_key_file,
	                                        SSL_FILETYPE_PEM) != 1 ||
	            SSL_CTX_check_private_key(profile->context) != 1)) {
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
	profile->peer_identity_enabled = peer_identity_enabled != 0;
	profile->peer_identity_required = 0;
	*out = profile;
	return TR_OK;

fail:
	tr_tls_openssl_profile_release(profile);
	return status;
}

int tr_tls_openssl_client_profile_create(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out)
{
	return tr_tls_openssl_client_profile_create_internal(config, 0, NULL,
	                                                     0U, out);
}

int tr_tls_openssl_client_profile_create_with_peer_identity(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out)
{
	return tr_tls_openssl_client_profile_create_internal(config, 1, NULL,
	                                                     0U, out);
}

int tr_tls_openssl_client_profile_create_from_pem(
        const struct tr_tls_client_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out)
{
	return tr_tls_openssl_client_profile_create_internal(
	        config, 0, bundle, max_bundle_bytes, out);
}

int tr_tls_openssl_client_profile_create_from_pem_with_peer_identity(
        const struct tr_tls_client_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out)
{
	return tr_tls_openssl_client_profile_create_internal(
	        config, 1, bundle, max_bundle_bytes, out);
}

static int tr_tls_openssl_server_profile_create_internal(
        const struct tr_tls_server_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out)
{
	struct tr_tls_openssl_profile *profile = NULL;
	X509_VERIFY_PARAM *verify_parameters;
	int verify_mode;
	int status;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	status = bundle ? tr_tls_server_config_validate_for_pem(config, 1)
	                : tr_tls_server_config_validate(config, 1);
	if (status != TR_OK)
		return status;
	if (config->mode != TR_TLS_REQUIRED)
		return TR_ERR_INVALID;
	status = tr_tls_openssl_profile_alloc(TR_TLS_OPENSSL_SERVER, &profile);
	if (status != TR_OK)
		return status;
	if (bundle) {
		status = tr_tls_openssl_pem_identity_install(
		        profile->context, bundle, max_bundle_bytes);
		if (status != TR_OK)
			goto fail;
	} else if (SSL_CTX_use_certificate_chain_file(
	                   profile->context, config->certificate_file) != 1 ||
	           SSL_CTX_use_PrivateKey_file(profile->context,
	                                       config->private_key_file,
	                                       SSL_FILETYPE_PEM) != 1 ||
	           SSL_CTX_check_private_key(profile->context) != 1) {
		status = TR_ERR_INVALID;
		goto fail;
	}
	if (config->client_auth != TR_TLS_CLIENT_AUTH_NONE) {
		if (SSL_CTX_load_verify_locations(profile->context,
		                                  config->client_ca_file,
		                                  NULL) != 1) {
			status = TR_ERR_INVALID;
			goto fail;
		}
		verify_parameters = SSL_CTX_get0_param(profile->context);
		if (!verify_parameters ||
		    X509_VERIFY_PARAM_set_purpose(
		            verify_parameters, X509_PURPOSE_SSL_CLIENT) != 1) {
			status = TR_ERR_INVALID;
			goto fail;
		}
		verify_mode = SSL_VERIFY_PEER;
		if (config->client_auth == TR_TLS_CLIENT_AUTH_REQUIRED)
			verify_mode |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
		SSL_CTX_set_verify(profile->context, verify_mode, NULL);
		profile->peer_identity_enabled = 1;
		profile->peer_identity_required =
		        config->client_auth == TR_TLS_CLIENT_AUTH_REQUIRED;
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
	return tr_tls_openssl_server_profile_create_internal(config, NULL, 0U,
	                                                     out);
}

int tr_tls_openssl_server_profile_create_from_pem(
        const struct tr_tls_server_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out)
{
	return tr_tls_openssl_server_profile_create_internal(
	        config, bundle, max_bundle_bytes, out);
}

int tr_tls_openssl_profile_peer_identity_enabled(
        const struct tr_tls_openssl_profile *profile)
{
	return profile ? profile->peer_identity_enabled : 0;
}

int tr_tls_openssl_profile_peer_identity_required(
        const struct tr_tls_openssl_profile *profile)
{
	return profile ? profile->peer_identity_required : 0;
}

int tr_tls_openssl_profile_retain(struct tr_tls_openssl_profile *profile)
{
	if (!profile)
		return TR_ERR_INVALID;
	return tr_refcount_get(&profile->references);
}

uint64_t
tr_tls_openssl_profile_generation(const struct tr_tls_openssl_profile *profile)
{
	return profile ? profile->generation : 0U;
}

int tr_tls_openssl_profile_next_generation(
        const struct tr_tls_openssl_profile *profile, uint64_t *out_generation)
{
	uint64_t generation;

	if (!profile || !out_generation)
		return TR_ERR_INVALID;
	generation = profile->generation;
	if (generation == 0U || generation == UINT64_MAX)
		return TR_ERR_STATE;
	*out_generation = generation + 1U;
	return TR_OK;
}

int tr_tls_openssl_profile_set_generation(
        struct tr_tls_openssl_profile *profile, uint64_t generation)
{
	if (!profile || generation == 0U)
		return TR_ERR_INVALID;
	if (tr_refcount_read(&profile->references) != 1U)
		return TR_ERR_STATE;
	profile->generation = generation;
	return TR_OK;
}

void tr_tls_openssl_profile_release(struct tr_tls_openssl_profile *profile)
{
	int ret;

	if (!profile)
		return;
	ret = tr_refcount_put(&profile->references);
	if (ret != 1)
		return;
	SSL_CTX_free(profile->context);
	free(profile->sni_name);
	free(profile);
}

int tr_tls_openssl_session_peer_identity(struct tr_tls_openssl_session *session,
                                         int require_uri,
                                         struct tr_peer_identity_snapshot **out)
{
	X509 *certificate = NULL;
	GENERAL_NAMES *names = NULL;
	ASN1_IA5STRING *uri = NULL;
	int uri_count = 0;
	int status = TR_ERR_INVALID;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!session || (require_uri != 0 && require_uri != 1))
		return TR_ERR_INVALID;
	if (SSL_is_init_finished(session->ssl) != 1)
		return TR_ERR_STATE;
	certificate = SSL_get1_peer_certificate(session->ssl);
	if (!certificate)
		return require_uri ? TR_ERR_INVALID : TR_OK;
	if (SSL_get_verify_result(session->ssl) != X509_V_OK)
		goto done;
	names = X509_get_ext_d2i(certificate, NID_subject_alt_name, NULL, NULL);
	if (!names)
		goto done;
	for (int index = 0; index < sk_GENERAL_NAME_num(names); ++index) {
		GENERAL_NAME *name = sk_GENERAL_NAME_value(names, index);
		if (!name || name->type != GEN_URI)
			continue;
		++uri_count;
		uri = name->d.uniformResourceIdentifier;
	}
	if (uri_count != 1 || !uri)
		goto done;
	{
		int length = ASN1_STRING_length(uri);
		const unsigned char *data = ASN1_STRING_get0_data(uri);
		if (length <= 0) {
			status = TR_ERR_INVALID;
			goto done;
		}
		if (length > 512) {
			status = TR_ERR_BAD_LENGTH;
			goto done;
		}
		status = tr_peer_identity_snapshot_create(
		        data, (uint32_t)length, out);
	}

done:
	GENERAL_NAMES_free(names);
	X509_free(certificate);
	return status;
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
                                  int fd,
                                  struct tr_memory_budget *memory_budget,
                                  struct tr_tls_openssl_session **out)
{
	struct tr_tls_openssl_session *session = NULL;
	BIO *socket_bio = NULL;
	struct tr_tls_openssl_socket_bio_data *socket_data;
	uint64_t memory_bytes =
	        (uint64_t)sizeof(*session) +
	        (uint64_t)sizeof(struct tr_tls_openssl_socket_bio_data);
	int flags;
	int ret = TR_ERR_NOMEM;
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
	if (memory_budget) {
		/* BIO 用户数据在 BIO_new 内部分配；与 session 一起预留，
		 * 但 OpenSSL 自身的 SSL/BIO 对象分配不在本账目内。
		 */
		ret = tr_memory_budget_reserve(memory_budget, memory_bytes);
		if (ret != TR_OK)
			return ret;
	}
	session = (struct tr_tls_openssl_session *)calloc(1, sizeof(*session));
	if (!session)
		goto fail;
	session->memory_budget = memory_budget;
	session->memory_bytes = memory_budget ? memory_bytes : 0U;
	session->ssl = SSL_new(profile->context);
	if (!session->ssl)
		goto fail;
	if (CRYPTO_THREAD_run_once(&tr_tls_openssl_socket_bio_once,
	                           tr_tls_openssl_socket_bio_method_init) !=
	            1 ||
	    !tr_tls_openssl_socket_bio_method)
		goto fail;
	socket_bio = BIO_new(tr_tls_openssl_socket_bio_method);
	if (!socket_bio)
		goto fail;
	socket_data = (struct tr_tls_openssl_socket_bio_data *)BIO_get_data(
	        socket_bio);
	if (!socket_data) {
		BIO_free(socket_bio);
		socket_bio = NULL;
		goto fail;
	}
	socket_data->fd = fd;
	socket_data->close_fd = 0;
	BIO_set_init(socket_bio, 1);
	SSL_set_bio(session->ssl, socket_bio, socket_bio);
	socket_bio = NULL;
	session->role = profile->role;
	if (session->role == TR_TLS_OPENSSL_CLIENT) {
		if (profile->sni_name &&
		    SSL_set_tlsext_host_name(session->ssl, profile->sni_name) !=
		            1) {
			ret = TR_ERR_INVALID;
			goto fail;
		}
		SSL_set_connect_state(session->ssl);
	} else {
		SSL_set_accept_state(session->ssl);
	}
	*out = session;
	return TR_OK;

fail:
	if (socket_bio)
		BIO_free(socket_bio);
	if (session)
		tr_tls_openssl_session_destroy(session);
	else if (memory_budget)
		(void)tr_memory_budget_release(memory_budget, memory_bytes);
	return ret;
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
	tr_tls_openssl_session_destroy(session);
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
