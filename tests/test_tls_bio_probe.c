#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

struct probe_identity {
	EVP_PKEY *ca_key;
	EVP_PKEY *server_key;
	X509 *ca;
	X509 *server;
};

struct probe_memory_io {
	unsigned char pending[16384];
	size_t pending_pos;
	size_t pending_len;
	unsigned long encrypted_sent;
	unsigned long encrypted_received;
	unsigned long send_eagain;
	unsigned long recv_eagain;
	size_t peak_bio_pending;
	size_t peak_external_pending;
};

static X509 *probe_cert(EVP_PKEY *key, X509 *issuer, EVP_PKEY *issuer_key,
                        const char *common_name, int ca)
{
	X509 *cert = X509_new();
	X509_NAME *name;
	X509V3_CTX extension_ctx;
	X509_EXTENSION *extension;
	assert(cert);
	assert(X509_set_version(cert, 2) == 1);
	assert(ASN1_INTEGER_set(X509_get_serialNumber(cert), ca ? 1 : 2) == 1);
	assert(X509_gmtime_adj(X509_getm_notBefore(cert), -60) != NULL);
	assert(X509_gmtime_adj(X509_getm_notAfter(cert), 3600) != NULL);
	assert(X509_set_pubkey(cert, key) == 1);
	name = X509_get_subject_name(cert);
	assert(X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
	                                  (const unsigned char *)common_name,
	                                  -1, -1, 0) == 1);
	assert(X509_set_issuer_name(cert, issuer ? X509_get_subject_name(issuer)
	                                         : name) == 1);
	X509V3_set_ctx(&extension_ctx, issuer ? issuer : cert, cert, NULL, NULL,
	               0);
	extension = X509V3_EXT_conf_nid(
	        NULL, &extension_ctx, NID_basic_constraints,
	        ca ? "critical,CA:TRUE" : "critical,CA:FALSE");
	assert(extension);
	assert(X509_add_ext(cert, extension, -1) == 1);
	X509_EXTENSION_free(extension);
	extension = X509V3_EXT_conf_nid(NULL, &extension_ctx, NID_key_usage,
	                                ca ? "critical,keyCertSign"
	                                   : "critical,digitalSignature");
	assert(extension);
	assert(X509_add_ext(cert, extension, -1) == 1);
	X509_EXTENSION_free(extension);
	if (!ca) {
		extension = X509V3_EXT_conf_nid(
		        NULL, &extension_ctx, NID_ext_key_usage, "serverAuth");
		assert(extension);
		assert(X509_add_ext(cert, extension, -1) == 1);
		X509_EXTENSION_free(extension);
		extension = X509V3_EXT_conf_nid(NULL, &extension_ctx,
		                                NID_subject_alt_name,
		                                "DNS:localhost");
		assert(extension);
		assert(X509_add_ext(cert, extension, -1) == 1);
		X509_EXTENSION_free(extension);
	}
	assert(X509_sign(cert, issuer_key ? issuer_key : key, NULL) > 0);
	return cert;
}

static struct probe_identity probe_identity_create(void)
{
	struct probe_identity identity = {0};
	identity.ca_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
	identity.server_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
	assert(identity.ca_key && identity.server_key);
	identity.ca =
	        probe_cert(identity.ca_key, NULL, NULL, "TLS-A test CA", 1);
	identity.server = probe_cert(identity.server_key, identity.ca,
	                             identity.ca_key, "localhost", 0);
	return identity;
}

static void probe_identity_destroy(struct probe_identity *identity)
{
	X509_free(identity->server);
	X509_free(identity->ca);
	EVP_PKEY_free(identity->server_key);
	EVP_PKEY_free(identity->ca_key);
}

static void probe_memory_pump(SSL *ssl, int fd, struct probe_memory_io *io)
{
	/* 每次只放行少量密文，制造真实 socket 分片与发送背压。 */
	unsigned char received[4096];
	ssize_t count;
	size_t bio_pending = BIO_ctrl_pending(SSL_get_wbio(ssl));
	if (bio_pending > io->peak_bio_pending)
		io->peak_bio_pending = bio_pending;
	if (io->pending_len - io->pending_pos > io->peak_external_pending)
		io->peak_external_pending = io->pending_len - io->pending_pos;
	while (io->pending_pos < io->pending_len) {
		size_t remaining = io->pending_len - io->pending_pos;
		if (remaining > 257)
			remaining = 257;
		count = send(fd, io->pending + io->pending_pos, remaining,
		             MSG_NOSIGNAL);
		if (count < 0) {
			assert(errno == EAGAIN || errno == EWOULDBLOCK);
			++io->send_eagain;
			break;
		}
		assert(count > 0);
		io->pending_pos += (size_t)count;
		io->encrypted_sent += (unsigned long)count;
	}
	if (io->pending_pos == io->pending_len) {
		io->pending_pos = 0;
		io->pending_len = 0;
		count = BIO_read(SSL_get_wbio(ssl), io->pending,
		                 sizeof(io->pending));
		if (count > 0)
			io->pending_len = (size_t)count;
	}
	while ((count = recv(fd, received, 193, MSG_DONTWAIT)) > 0) {
		assert(BIO_write(SSL_get_rbio(ssl), received, (int)count) ==
		       count);
		io->encrypted_received += (unsigned long)count;
	}
	if (count < 0) {
		assert(errno == EAGAIN || errno == EWOULDBLOCK);
		++io->recv_eagain;
	} else
		assert(count < 0);
}

static void probe_bulk_transfer(SSL *client, SSL *server, int fd[2],
                                int memory_bio,
                                struct probe_memory_io *client_io,
                                struct probe_memory_io *server_io)
{
	unsigned char payload[16384];
	unsigned char received[4096];
	size_t sent_total = 0;
	size_t received_total = 0;
	size_t chunk_position = 0;
	size_t chunk_length = 0;
	unsigned int write_want = 0;
	int iteration;
	for (size_t index = 0; index < sizeof(payload); ++index)
		payload[index] = (unsigned char)(index * 131U + 17U);
	for (iteration = 0; iteration < 20000 && received_total < 131072;
	     ++iteration) {
		if (sent_total < 131072) {
			size_t written = 0;
			if (chunk_position == chunk_length) {
				chunk_position = 0U;
				chunk_length = 131072U - sent_total;
				if (chunk_length > sizeof(payload))
					chunk_length = sizeof(payload);
			}
			int result = SSL_write_ex(
			        client, payload + chunk_position,
			        chunk_length - chunk_position, &written);
			if (result == 1) {
				assert(written > 0 &&
				       written <=
				               chunk_length - chunk_position);
				chunk_position += written;
				sent_total += written;
			} else {
				int error = SSL_get_error(client, result);
				assert(error == SSL_ERROR_WANT_READ ||
				       error == SSL_ERROR_WANT_WRITE);
				++write_want;
			}
		}
		if (memory_bio) {
			probe_memory_pump(client, fd[0], client_io);
			probe_memory_pump(server, fd[1], server_io);
		}
		if (received_total < 131072) {
			size_t count = 0;
			int result = SSL_read_ex(server, received,
			                         sizeof(received), &count);
			if (result == 1) {
				assert(count > 0 && count <= sizeof(received));
				for (size_t index = 0; index < count; ++index) {
					size_t absolute =
					        received_total + index;
					unsigned char expected =
					        (unsigned char)(absolute *
					                                131U +
					                        17U);
					assert(received[index] == expected);
				}
				received_total += count;
			} else {
				int error = SSL_get_error(server, result);
				assert(error == SSL_ERROR_WANT_READ ||
				       error == SSL_ERROR_WANT_WRITE);
			}
		}
		if (memory_bio) {
			probe_memory_pump(client, fd[0], client_io);
			probe_memory_pump(server, fd[1], server_io);
		}
	}
	assert(sent_total == 131072);
	assert(received_total == 131072);
	if (!memory_bio)
		assert(write_want > 0);
	else
		assert(client_io->send_eagain + server_io->send_eagain > 0U);
}

static void probe_clean_close(SSL *client, SSL *server, int fd[2],
                              int memory_bio, struct probe_memory_io *client_io,
                              struct probe_memory_io *server_io)
{
	int client_done = 0;
	int server_done = 0;
	for (int iteration = 0;
	     iteration < 1000 && (!client_done || !server_done); ++iteration) {
		if (!client_done) {
			int result = SSL_shutdown(client);
			if (result == 1)
				client_done = 1;
			else if (result < 0) {
				int error = SSL_get_error(client, result);
				assert(error == SSL_ERROR_WANT_READ ||
				       error == SSL_ERROR_WANT_WRITE);
			}
		}
		if (!server_done) {
			int result = SSL_shutdown(server);
			if (result == 1)
				server_done = 1;
			else if (result < 0) {
				int error = SSL_get_error(server, result);
				assert(error == SSL_ERROR_WANT_READ ||
				       error == SSL_ERROR_WANT_WRITE);
			}
		}
		if (memory_bio) {
			probe_memory_pump(client, fd[0], client_io);
			probe_memory_pump(server, fd[1], server_io);
		}
	}
	assert(client_done && server_done);
}

static void probe_truncated_close(SSL *client, SSL *server, int fd[2],
                                  int memory_bio)
{
	unsigned char byte = 0;
	size_t count = 0;
	int result;
	SSL_free(client);
	/* 写半关闭形成明确的传输 EOF；直接 close 可能因未读票据触发 RST。 */
	assert(shutdown(fd[0], SHUT_WR) == 0);
	if (memory_bio)
		BIO_set_mem_eof_return(SSL_get_rbio(server), 0);
	result = SSL_read_ex(server, &byte, sizeof(byte), &count);
	assert(result == 0);
	assert(SSL_get_error(server, result) == SSL_ERROR_SSL);
}

static void probe_attach_memory(SSL *ssl)
{
	BIO *read_bio = BIO_new(BIO_s_mem());
	BIO *write_bio = BIO_new(BIO_s_mem());
	assert(read_bio && write_bio);
	BIO_set_mem_eof_return(read_bio, -1);
	BIO_set_mem_eof_return(write_bio, -1);
	SSL_set_bio(ssl, read_bio, write_bio);
}

static void probe_handshake(const struct probe_identity *identity,
                            X509 *trusted_ca, const char *expected_host,
                            int memory_bio, int should_succeed, int truncated)
{
	SSL_CTX *client_ctx = SSL_CTX_new(TLS_client_method());
	SSL_CTX *server_ctx = SSL_CTX_new(TLS_server_method());
	SSL *client;
	SSL *server;
	int fd[2];
	int client_done = 0;
	int server_done = 0;
	int client_failed = 0;
	struct probe_memory_io client_io = {0};
	struct probe_memory_io server_io = {0};
	int iteration;
	assert(client_ctx && server_ctx);
	assert(SSL_CTX_set_min_proto_version(client_ctx, TLS1_3_VERSION) == 1);
	assert(SSL_CTX_set_max_proto_version(client_ctx, TLS1_3_VERSION) == 1);
	assert(SSL_CTX_set_max_early_data(client_ctx, 0U) == 1);
	assert(SSL_CTX_set_min_proto_version(server_ctx, TLS1_3_VERSION) == 1);
	assert(SSL_CTX_set_max_proto_version(server_ctx, TLS1_3_VERSION) == 1);
	assert(SSL_CTX_set_max_early_data(server_ctx, 0U) == 1);
	SSL_CTX_set_verify(client_ctx, SSL_VERIFY_PEER, NULL);
	assert(X509_STORE_add_cert(SSL_CTX_get_cert_store(client_ctx),
	                           trusted_ca) == 1);
	assert(SSL_CTX_use_certificate(server_ctx, identity->server) == 1);
	assert(SSL_CTX_use_PrivateKey(server_ctx, identity->server_key) == 1);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, fd) == 0);
	{
		int capacity = 1024;
		assert(setsockopt(fd[0], SOL_SOCKET, SO_SNDBUF, &capacity,
		                  sizeof(capacity)) == 0);
		assert(setsockopt(fd[1], SOL_SOCKET, SO_SNDBUF, &capacity,
		                  sizeof(capacity)) == 0);
	}
	client = SSL_new(client_ctx);
	server = SSL_new(server_ctx);
	assert(client && server);
	assert(SSL_set1_host(client, expected_host) == 1);
	assert(SSL_set_tlsext_host_name(client, "localhost") == 1);
	if (memory_bio) {
		probe_attach_memory(client);
		probe_attach_memory(server);
	} else {
		assert(SSL_set_fd(client, fd[0]) == 1);
		assert(SSL_set_fd(server, fd[1]) == 1);
	}
	SSL_set_connect_state(client);
	SSL_set_accept_state(server);
	for (iteration = 0; iteration < 1000 && !client_failed &&
	                    (!client_done || !server_done);
	     ++iteration) {
		if (!client_done) {
			int result = SSL_do_handshake(client);
			if (result == 1)
				client_done = 1;
			else {
				int error = SSL_get_error(client, result);
				client_failed = error != SSL_ERROR_WANT_READ &&
				                error != SSL_ERROR_WANT_WRITE;
			}
		}
		if (!server_done) {
			int result = SSL_do_handshake(server);
			if (result == 1)
				server_done = 1;
			else {
				int error = SSL_get_error(server, result);
				assert(error == SSL_ERROR_WANT_READ ||
				       error == SSL_ERROR_WANT_WRITE ||
				       client_failed);
			}
		}
		if (memory_bio) {
			probe_memory_pump(client, fd[0], &client_io);
			probe_memory_pump(server, fd[1], &server_io);
			probe_memory_pump(client, fd[0], &client_io);
			probe_memory_pump(server, fd[1], &server_io);
		}
	}
	if (should_succeed) {
		assert(client_done && server_done && !client_failed);
		assert(SSL_version(client) == TLS1_3_VERSION);
		assert(SSL_get_verify_result(client) == X509_V_OK);
		if (memory_bio) {
			assert(client_io.encrypted_sent > 0);
			assert(server_io.encrypted_received > 0);
		}
		probe_bulk_transfer(client, server, fd, memory_bio, &client_io,
		                    &server_io);
		{
			int reverse_fd[2] = {fd[1], fd[0]};
			probe_bulk_transfer(server, client, reverse_fd,
			                    memory_bio, &server_io, &client_io);
		}
		if (memory_bio && !truncated)
			printf("memory BIO 峰值: client BIO=%zu 外部密文=%zu "
			       "send EAGAIN=%lu; server BIO=%zu 外部密文=%zu "
			       "send EAGAIN=%lu\n",
			       client_io.peak_bio_pending,
			       client_io.peak_external_pending,
			       client_io.send_eagain,
			       server_io.peak_bio_pending,
			       server_io.peak_external_pending,
			       server_io.send_eagain);
		if (truncated) {
			probe_truncated_close(client, server, fd, memory_bio);
			client = NULL;
		} else {
			probe_clean_close(client, server, fd, memory_bio,
			                  &client_io, &server_io);
		}
	} else {
		assert(client_failed && !client_done);
	}
	SSL_free(server);
	SSL_free(client);
	close(fd[1]);
	close(fd[0]);
	SSL_CTX_free(server_ctx);
	SSL_CTX_free(client_ctx);
}

int main(void)
{
	struct probe_identity identity = probe_identity_create();
	struct probe_identity stranger = probe_identity_create();
	probe_handshake(&identity, identity.ca, "localhost", 0, 1, 0);
	probe_handshake(&identity, identity.ca, "localhost", 0, 1, 1);
	probe_handshake(&identity, identity.ca, "wrong.example", 0, 0, 0);
	probe_handshake(&identity, stranger.ca, "localhost", 0, 0, 0);
	probe_handshake(&identity, identity.ca, "localhost", 1, 1, 0);
	probe_handshake(&identity, identity.ca, "localhost", 1, 1, 1);
	probe_handshake(&identity, identity.ca, "wrong.example", 1, 0, 0);
	probe_handshake(&identity, stranger.ca, "localhost", 1, 0, 0);
	probe_identity_destroy(&stranger);
	probe_identity_destroy(&identity);
	puts("TLS-A 两种 BIO 的认证、背压、正常关闭与截断检测通过");
	return 0;
}
