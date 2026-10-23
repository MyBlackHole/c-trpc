#define _POSIX_C_SOURCE 200809L
#include "tls_test_identity.h"

#include <errno.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static EVP_PKEY *tls_test_generate_key(void)
{
	EVP_PKEY_CTX *context = NULL;
	EVP_PKEY *key = NULL;

	context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
	if (!context || EVP_PKEY_keygen_init(context) <= 0 ||
	    EVP_PKEY_CTX_set_rsa_keygen_bits(context, 2048) <= 0 ||
	    EVP_PKEY_keygen(context, &key) <= 0) {
		EVP_PKEY_free(key);
		key = NULL;
	}
	EVP_PKEY_CTX_free(context);
	return key;
}

static int tls_test_add_extension(X509 *certificate, X509 *issuer, int nid,
                                  const char *value)
{
	X509V3_CTX context;
	X509_EXTENSION *extension;
	int result;

	X509V3_set_ctx(&context, issuer, certificate, NULL, NULL, 0);
	extension = X509V3_EXT_conf_nid(NULL, &context, nid, value);
	if (!extension)
		return 0;
	result = X509_add_ext(certificate, extension, -1) == 1;
	X509_EXTENSION_free(extension);
	return result;
}

static X509 *
tls_test_generate_certificate(EVP_PKEY *key, X509 *issuer, EVP_PKEY *issuer_key,
                              const char *common_name, const char *san,
                              const char *extended_key_usage,
                              long not_before_offset, long not_after_offset,
                              long serial, int ca)
{
	X509 *certificate = X509_new();
	X509_NAME *subject;

	if (!certificate || X509_set_version(certificate, 2) != 1 ||
	    ASN1_INTEGER_set(X509_get_serialNumber(certificate), serial) != 1 ||
	    !X509_gmtime_adj(X509_getm_notBefore(certificate),
	                     not_before_offset) ||
	    !X509_gmtime_adj(X509_getm_notAfter(certificate),
	                     not_after_offset) ||
	    X509_set_pubkey(certificate, key) != 1) {
		X509_free(certificate);
		return NULL;
	}
	subject = X509_get_subject_name(certificate);
	if (!subject ||
	    X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
	                               (const unsigned char *)common_name, -1,
	                               -1, 0) != 1 ||
	    X509_set_issuer_name(certificate,
	                         issuer ? X509_get_subject_name(issuer)
	                                : subject) != 1) {
		X509_free(certificate);
		return NULL;
	}
	if (ca) {
		if (!tls_test_add_extension(certificate, certificate,
		                            NID_basic_constraints,
		                            "critical,CA:TRUE") ||
		    !tls_test_add_extension(certificate, certificate,
		                            NID_key_usage,
		                            "critical,keyCertSign,cRLSign")) {
			X509_free(certificate);
			return NULL;
		}
	} else {
		if (!tls_test_add_extension(certificate, issuer,
		                            NID_basic_constraints,
		                            "critical,CA:FALSE") ||
		    !tls_test_add_extension(
		            certificate, issuer, NID_key_usage,
		            "critical,digitalSignature,keyEncipherment") ||
		    (extended_key_usage &&
		     !tls_test_add_extension(certificate, issuer,
		                             NID_ext_key_usage,
		                             extended_key_usage)) ||
		    (san &&
		     !tls_test_add_extension(certificate, issuer,
		                             NID_subject_alt_name, san))) {
			X509_free(certificate);
			return NULL;
		}
	}
	if (X509_sign(certificate, issuer_key ? issuer_key : key,
	              EVP_sha256()) <= 0) {
		X509_free(certificate);
		return NULL;
	}
	return certificate;
}

static int tls_test_write_certificate(const char *path, X509 *certificate)
{
	BIO *bio = BIO_new_file(path, "wb");
	int result;
	if (!bio)
		return 0;
	result = PEM_write_bio_X509(bio, certificate) == 1;
	BIO_free(bio);
	return result;
}

static int tls_test_write_key(const char *path, EVP_PKEY *key)
{
	BIO *bio = BIO_new_file(path, "wb");
	int result;
	if (!bio)
		return 0;
	result = PEM_write_bio_PrivateKey(bio, key, NULL, NULL, 0, NULL,
	                                  NULL) == 1;
	BIO_free(bio);
	if (result && chmod(path, S_IRUSR | S_IWUSR) != 0)
		result = 0;
	return result;
}

static int tls_test_path(char *output, size_t capacity, const char *directory,
                         const char *name)
{
	int written = snprintf(output, capacity, "%s/%s", directory, name);
	return written > 0 && (size_t)written < capacity;
}

static int tls_test_worktree_temp_dir(char *output, size_t capacity)
{
	char executable[4096];
	char *search;
	char *build_marker = NULL;
	ssize_t length;
	int written;

	length =
	        readlink("/proc/self/exe", executable, sizeof(executable) - 1U);
	if (length <= 0 || (size_t)length >= sizeof(executable) - 1U)
		return 0;
	executable[length] = '\0';
	search = executable;
	while ((search = strstr(search, "/build/linux/")) != NULL) {
		build_marker = search;
		++search;
	}
	if (!build_marker)
		return 0;
	*build_marker = '\0';
	written = snprintf(output, capacity, "%s/build/test-tmp", executable);
	return written > 0 && (size_t)written < capacity;
}

static int tls_test_directory_ensure(const char *directory)
{
	struct stat info;
	if (mkdir(directory, 0700) == 0)
		return 1;
	return errno == EEXIST && stat(directory, &info) == 0 &&
	       S_ISDIR(info.st_mode);
}

int tls_test_identity_init(struct tls_test_identity *identity,
                           const char *directory)
{
	EVP_PKEY *ca_key = NULL;
	EVP_PKEY *alternate_ca_key = NULL;
	EVP_PKEY *server_key = NULL;
	EVP_PKEY *wrong_key = NULL;
	X509 *ca_certificate = NULL;
	X509 *alternate_ca_certificate = NULL;
	X509 *server_certificate = NULL;
	X509 *wrong_dns_certificate = NULL;
	X509 *wrong_ip_certificate = NULL;
	X509 *common_name_only_certificate = NULL;
	X509 *expired_certificate = NULL;
	X509 *client_only_certificate = NULL;
	char temporary_directory[TR_TLS_TEST_PATH_SIZE];
	int result = -1;
	size_t index;

	if (!identity || !directory || directory[0] == '\0')
		return -1;
	memset(identity, 0, sizeof(*identity));
	if (strchr(directory, '/') || strstr(directory, "..") ||
	    !tls_test_worktree_temp_dir(temporary_directory,
	                                sizeof(temporary_directory)) ||
	    !tls_test_directory_ensure(temporary_directory))
		return -1;
	for (index = 0U; directory[index] != '\0'; ++index) {
		unsigned char character = (unsigned char)directory[index];
		if (!((character >= 'a' && character <= 'z') ||
		      (character >= 'A' && character <= 'Z') ||
		      (character >= '0' && character <= '9') ||
		      character == '-' || character == '_'))
			return -1;
	}
	{
		int written = snprintf(identity->directory,
		                       sizeof(identity->directory), "%s/%s",
		                       temporary_directory, directory);
		if (written < 0 ||
		    (size_t)written >= sizeof(identity->directory))
			return -1;
	}
	if (!tls_test_directory_ensure(identity->directory) ||
	    !tls_test_path(identity->ca_file, sizeof(identity->ca_file),
	                   identity->directory, "ca.pem") ||
	    !tls_test_path(identity->server_certificate_file,
	                   sizeof(identity->server_certificate_file),
	                   identity->directory, "server.pem") ||
	    !tls_test_path(identity->server_private_key_file,
	                   sizeof(identity->server_private_key_file),
	                   identity->directory, "server-key.pem") ||
	    !tls_test_path(identity->alternate_ca_file,
	                   sizeof(identity->alternate_ca_file),
	                   identity->directory, "alternate-ca.pem") ||
	    !tls_test_path(identity->wrong_dns_certificate_file,
	                   sizeof(identity->wrong_dns_certificate_file),
	                   identity->directory, "wrong-dns.pem") ||
	    !tls_test_path(identity->wrong_ip_certificate_file,
	                   sizeof(identity->wrong_ip_certificate_file),
	                   identity->directory, "wrong-ip.pem") ||
	    !tls_test_path(identity->common_name_only_certificate_file,
	                   sizeof(identity->common_name_only_certificate_file),
	                   identity->directory, "common-name-only.pem") ||
	    !tls_test_path(identity->expired_certificate_file,
	                   sizeof(identity->expired_certificate_file),
	                   identity->directory, "expired.pem") ||
	    !tls_test_path(identity->client_only_certificate_file,
	                   sizeof(identity->client_only_certificate_file),
	                   identity->directory, "client-only.pem") ||
	    !tls_test_path(identity->session_file,
	                   sizeof(identity->session_file), identity->directory,
	                   "resumption-session.der") ||
	    !tls_test_path(identity->mismatched_private_key_file,
	                   sizeof(identity->mismatched_private_key_file),
	                   identity->directory, "wrong-key.pem") ||
	    !tls_test_path(identity->invalid_ca_file,
	                   sizeof(identity->invalid_ca_file),
	                   identity->directory, "invalid-ca.pem"))
		goto cleanup;

	ca_key = tls_test_generate_key();
	alternate_ca_key = tls_test_generate_key();
	server_key = tls_test_generate_key();
	wrong_key = tls_test_generate_key();
	if (!ca_key || !alternate_ca_key || !server_key || !wrong_key)
		goto cleanup;
	ca_certificate = tls_test_generate_certificate(
	        ca_key, NULL, NULL, "c-trpc TLS test CA", NULL, NULL, -60,
	        86400, 1, 1);
	alternate_ca_certificate = tls_test_generate_certificate(
	        alternate_ca_key, NULL, NULL, "c-trpc alternate TLS test CA",
	        NULL, NULL, -60, 86400, 8, 1);
	server_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test",
	        "DNS:server.example.test,IP:127.0.0.1", "serverAuth", -60, 3600,
	        2, 0);
	wrong_dns_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test",
	        "DNS:wrong.example.test,IP:127.0.0.1", "serverAuth", -60, 3600,
	        3, 0);
	wrong_ip_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test",
	        "DNS:server.example.test,IP:127.0.0.2", "serverAuth", -60, 3600,
	        4, 0);
	common_name_only_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test", NULL,
	        "serverAuth", -60, 3600, 5, 0);
	expired_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test",
	        "DNS:server.example.test,IP:127.0.0.1", "serverAuth", -7200,
	        -3600, 6, 0);
	client_only_certificate = tls_test_generate_certificate(
	        server_key, ca_certificate, ca_key, "server.example.test",
	        "DNS:server.example.test,IP:127.0.0.1", "clientAuth", -60, 3600,
	        7, 0);
	if (!ca_certificate || !alternate_ca_certificate ||
	    !server_certificate || !wrong_dns_certificate ||
	    !wrong_ip_certificate || !common_name_only_certificate ||
	    !expired_certificate || !client_only_certificate ||
	    !tls_test_write_certificate(identity->ca_file, ca_certificate) ||
	    !tls_test_write_certificate(identity->alternate_ca_file,
	                                alternate_ca_certificate) ||
	    !tls_test_write_certificate(identity->server_certificate_file,
	                                server_certificate) ||
	    !tls_test_write_certificate(identity->wrong_dns_certificate_file,
	                                wrong_dns_certificate) ||
	    !tls_test_write_certificate(identity->wrong_ip_certificate_file,
	                                wrong_ip_certificate) ||
	    !tls_test_write_certificate(
	            identity->common_name_only_certificate_file,
	            common_name_only_certificate) ||
	    !tls_test_write_certificate(identity->expired_certificate_file,
	                                expired_certificate) ||
	    !tls_test_write_certificate(identity->client_only_certificate_file,
	                                client_only_certificate) ||
	    !tls_test_write_key(identity->server_private_key_file,
	                        server_key) ||
	    !tls_test_write_key(identity->mismatched_private_key_file,
	                        wrong_key))
		goto cleanup;
	{
		FILE *file = fopen(identity->invalid_ca_file, "wb");
		int written;
		if (!file)
			goto cleanup;
		written = fputs("not a certificate\n", file) >= 0;
		if (fclose(file) != 0)
			written = 0;
		if (!written)
			goto cleanup;
	}
	result = 0;

cleanup:
	EVP_PKEY_free(ca_key);
	EVP_PKEY_free(alternate_ca_key);
	EVP_PKEY_free(server_key);
	EVP_PKEY_free(wrong_key);
	X509_free(ca_certificate);
	X509_free(alternate_ca_certificate);
	X509_free(server_certificate);
	X509_free(wrong_dns_certificate);
	X509_free(wrong_ip_certificate);
	X509_free(common_name_only_certificate);
	X509_free(expired_certificate);
	X509_free(client_only_certificate);
	if (result != 0)
		tls_test_identity_cleanup(identity);
	return result;
}

void tls_test_identity_cleanup(struct tls_test_identity *identity)
{
	if (!identity || identity->directory[0] == '\0')
		return;
	if (identity->ca_file[0])
		(void)unlink(identity->ca_file);
	if (identity->server_certificate_file[0])
		(void)unlink(identity->server_certificate_file);
	if (identity->server_private_key_file[0])
		(void)unlink(identity->server_private_key_file);
	if (identity->alternate_ca_file[0])
		(void)unlink(identity->alternate_ca_file);
	if (identity->wrong_dns_certificate_file[0])
		(void)unlink(identity->wrong_dns_certificate_file);
	if (identity->wrong_ip_certificate_file[0])
		(void)unlink(identity->wrong_ip_certificate_file);
	if (identity->common_name_only_certificate_file[0])
		(void)unlink(identity->common_name_only_certificate_file);
	if (identity->expired_certificate_file[0])
		(void)unlink(identity->expired_certificate_file);
	if (identity->client_only_certificate_file[0])
		(void)unlink(identity->client_only_certificate_file);
	if (identity->session_file[0])
		(void)unlink(identity->session_file);
	if (identity->mismatched_private_key_file[0])
		(void)unlink(identity->mismatched_private_key_file);
	if (identity->invalid_ca_file[0])
		(void)unlink(identity->invalid_ca_file);
	(void)rmdir(identity->directory);
	memset(identity, 0, sizeof(*identity));
}
