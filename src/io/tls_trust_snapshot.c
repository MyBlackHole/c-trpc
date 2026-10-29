#define _POSIX_C_SOURCE 200809L
#include "tls_trust_snapshot.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../../include/tr/status.h"

enum trust_object_kind { TRUST_BUNDLE, TRUST_CERTIFICATE, TRUST_CRL };

static int trust_crypto_failure(void)
{
	unsigned long error;
	int status = TR_ERR_INVALID;
	/* 构造只通过 TR 状态报告；本次错误队列中的分配失败不能伪装成配置错误。
	 */
	while ((error = ERR_get_error()) != 0UL)
		if (ERR_GET_REASON(error) == ERR_R_MALLOC_FAILURE)
			status = TR_ERR_NOMEM;
	return status;
}

static int trust_no_password(char *buffer, int size, int writing, void *arg)
{
	(void)buffer;
	(void)size;
	(void)writing;
	(void)arg;
	return 0;
}

static int trust_file(SSL_CTX *context, int directory_fd, const char *path,
                      enum trust_object_kind kind, unsigned long expected_hash,
                      int missing_allowed, int *found)
{
	struct stat attributes = {0};
	STACK_OF(X509_INFO) *objects = NULL;
	BIO *input = NULL;
	int fd = -1, loaded = 0, status = TR_ERR_INVALID;
	*found = 0;
	do {
		fd = openat(directory_fd, path,
		            O_RDONLY | O_CLOEXEC | O_NONBLOCK);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0) {
		if (missing_allowed && (errno == ENOENT || errno == ENOTDIR))
			return TR_OK;
		return errno == ENOMEM ? TR_ERR_NOMEM : TR_ERR_INVALID;
	}
	if (fstat(fd, &attributes) != 0) {
		status = errno == ENOMEM ? TR_ERR_NOMEM : TR_ERR_INVALID;
		goto done;
	}
	if (!S_ISREG(attributes.st_mode))
		goto done;
	input = BIO_new_fd(fd, BIO_NOCLOSE);
	if (!input) {
		status = TR_ERR_NOMEM;
		goto done;
	}
	/* fd BIO 不缓存逐行读取；用有界缓冲避免 PEM 每字节一次系统调用。 */
	BIO *buffer = BIO_new(BIO_f_buffer());
	if (!buffer) {
		status = TR_ERR_NOMEM;
		goto done;
	}
	(void)BIO_push(buffer, input);
	input = buffer;
	ERR_clear_error();
	objects = PEM_X509_INFO_read_bio(input, NULL, trust_no_password, NULL);
	if (!objects) {
		status = trust_crypto_failure();
		goto done;
	}
	int count = sk_X509_INFO_num(objects);
	if (count <= 0 || (kind != TRUST_BUNDLE && count != 1))
		goto done;
	for (int i = 0; i < count; ++i) {
		X509_INFO *object = sk_X509_INFO_value(objects, i);
		if (!object)
			goto done;
		if (kind != TRUST_BUNDLE) {
			X509_NAME *name;
			int hash_ok = 0;
			if (object->x_pkey ||
			    (kind == TRUST_CERTIFICATE &&
			     (!object->x509 || object->crl)) ||
			    (kind == TRUST_CRL &&
			     (!object->crl || object->x509)))
				goto done;
			name = kind == TRUST_CERTIFICATE
			               ? X509_get_subject_name(object->x509)
			               : X509_CRL_get_issuer(object->crl);
			ERR_clear_error();
			unsigned long hash =
			        X509_NAME_hash_ex(name, NULL, NULL, &hash_ok);
			if (!hash_ok) {
				status = trust_crypto_failure();
				goto done;
			}
			/* 不因急切加载而信任原哈希查找不会选中的对象。 */
			if (hash != expected_hash) {
				/* 兼容目录中同时存在的旧 hash
				 * 别名，但不因此扩大信任。 */
				*found = 1;
				status = TR_OK;
				goto done;
			}
		}
		if (object->x509) {
			if (X509_STORE_add_cert(SSL_CTX_get_cert_store(context),
			                        object->x509) != 1) {
				status = trust_crypto_failure();
				goto done;
			}
			loaded = 1;
		}
		if (object->crl) {
			if (X509_STORE_add_crl(SSL_CTX_get_cert_store(context),
			                       object->crl) != 1) {
				status = trust_crypto_failure();
				goto done;
			}
			loaded = 1;
		}
	}
	if (loaded) {
		*found = 1;
		status = TR_OK;
	}
done:
	sk_X509_INFO_pop_free(objects, X509_INFO_free);
	BIO_free_all(input);
	if (fd >= 0)
		(void)close(fd);
	return status;
}

static int trust_group_head(const char *name, unsigned long *hash,
                            enum trust_object_kind *kind)
{
	size_t length = strlen(name);
	if (length != 10U && length != 11U)
		return 0;
	if (name[8] != '.' ||
	    (length == 10U ? name[9] != '0'
	                   : name[9] != 'r' || name[10] != '0'))
		return 0;
	*hash = 0UL;
	for (unsigned i = 0; i < 8U; ++i) {
		unsigned digit;
		if (name[i] >= '0' && name[i] <= '9')
			digit = (unsigned)(name[i] - '0');
		else if (name[i] >= 'a' && name[i] <= 'f')
			digit = (unsigned)(name[i] - 'a') + 10U;
		else
			return 0;
		*hash = (*hash << 4U) | digit;
	}
	*kind = length == 10U ? TRUST_CERTIFICATE : TRUST_CRL;
	return 1;
}

static int trust_directory(SSL_CTX *context, const char *path, int optional)
{
	DIR *directory;
	do {
		directory = opendir(path);
	} while (!directory && errno == EINTR);
	int status = TR_OK;
	if (!directory) {
		if (optional && (errno == ENOENT || errno == ENOTDIR))
			return TR_OK;
		return errno == ENOMEM ? TR_ERR_NOMEM : TR_ERR_INVALID;
	}
	for (;;) {
		unsigned long hash = 0UL;
		enum trust_object_kind kind = TRUST_BUNDLE;
		errno = 0;
		struct dirent *entry = readdir(directory);
		if (!entry) {
			if (errno == EINTR)
				continue;
			if (errno)
				status = errno == ENOMEM ? TR_ERR_NOMEM
				                         : TR_ERR_INVALID;
			break;
		}
		if (!trust_group_head(entry->d_name, &hash, &kind))
			continue;
		/* 仅从 .0/.r0
		 * 开始，遇到第一个缺口停止，不能信任越过缺口的条目。 */
		for (unsigned suffix = 0;; ++suffix) {
			char name[32];
			int found = 0;
			int length =
			        snprintf(name, sizeof(name), "%08lx.%s%u", hash,
			                 kind == TRUST_CRL ? "r" : "", suffix);
			if (length <= 0 || (size_t)length >= sizeof(name)) {
				status = TR_ERR_INVALID;
				goto done;
			}
			status = trust_file(context, dirfd(directory), name,
			                    kind, hash, 1, &found);
			if (status != TR_OK)
				goto done;
			if (!found)
				break;
			if (suffix == UINT_MAX) {
				status = TR_ERR_INVALID;
				goto done;
			}
		}
	}
done:
	if (closedir(directory) != 0 && status == TR_OK)
		status = TR_ERR_INVALID;
	return status;
}

static int trust_directories(SSL_CTX *context, const char *paths, int optional)
{
	size_t length = strlen(paths);
	if (length == SIZE_MAX)
		return TR_ERR_INVALID;
	char *copy = malloc(length + 1U);
	if (!copy)
		return TR_ERR_NOMEM;
	memcpy(copy, paths, length + 1U);
	char *cursor = copy;
	int status = TR_OK;
	for (;;) {
		char *separator = strchr(cursor, ':');
		if (separator)
			*separator = '\0';
		if (*cursor) {
			status = trust_directory(context, cursor, optional);
			if (status != TR_OK)
				break;
		}
		if (!separator)
			break;
		cursor = separator + 1;
	}
	free(copy);
	return status;
}

int tr_tls_openssl_trust_snapshot(SSL_CTX *context,
                                  const struct tr_tls_client_config *config)
{
	const char *file, *directories;
	int optional = 0, found = 0, status;
	if (!context || !config || !SSL_CTX_get_cert_store(context))
		return TR_ERR_INVALID;
	file = config->ca_file;
	directories = config->ca_directory;
	if (!file && !directories) {
		/* 与 OpenSSL 的安全环境读取保持一致；特权进程不采用环境覆盖。
		 */
		optional = 1;
		if (!OPENSSL_issetugid()) {
			file = getenv(X509_get_default_cert_file_env());
			directories = getenv(X509_get_default_cert_dir_env());
		}
		if (!file)
			file = X509_get_default_cert_file();
		if (!directories)
			directories = X509_get_default_cert_dir();
	}
	ERR_clear_error();
	if (file) {
		status = trust_file(context, AT_FDCWD, file, TRUST_BUNDLE, 0UL,
		                    optional, &found);
		if (status != TR_OK)
			return status;
	}
	return directories ? trust_directories(context, directories, optional)
	                   : TR_OK;
}
