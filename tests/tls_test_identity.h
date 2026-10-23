#ifndef TR_TLS_TEST_IDENTITY_H
#define TR_TLS_TEST_IDENTITY_H

#define TR_TLS_TEST_PATH_SIZE 512U

struct tls_test_identity {
	char directory[TR_TLS_TEST_PATH_SIZE];
	char ca_file[TR_TLS_TEST_PATH_SIZE];
	char alternate_ca_file[TR_TLS_TEST_PATH_SIZE];
	char server_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char wrong_dns_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char wrong_ip_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char common_name_only_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char expired_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char client_only_certificate_file[TR_TLS_TEST_PATH_SIZE];
	char server_private_key_file[TR_TLS_TEST_PATH_SIZE];
	char session_file[TR_TLS_TEST_PATH_SIZE];
	char mismatched_private_key_file[TR_TLS_TEST_PATH_SIZE];
	char invalid_ca_file[TR_TLS_TEST_PATH_SIZE];
};

int tls_test_identity_init(struct tls_test_identity *identity,
                           const char *directory);
void tls_test_identity_cleanup(struct tls_test_identity *identity);

#endif
