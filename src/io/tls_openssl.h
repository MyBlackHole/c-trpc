#ifndef TR_TLS_OPENSSL_H
#define TR_TLS_OPENSSL_H

#include "../../include/tr/tls.h"
#include <openssl/ssl.h>

#define TR_TLS_OPENSSL_MAX_PEER_CERT_LIST_BYTES (100L * 1024L)

#ifdef __cplusplus
extern "C" {
#endif

struct tr_tls_openssl_profile;
struct tr_tls_openssl_session;
struct tr_byte_endpoint_driver;
struct tr_peer_identity_snapshot;
struct tr_memory_budget;

int tr_tls_openssl_client_profile_create(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out);
int tr_tls_openssl_client_profile_create_with_peer_identity(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out);
int tr_tls_openssl_client_profile_create_from_pem(
        const struct tr_tls_client_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out);
int tr_tls_openssl_client_profile_create_from_pem_with_peer_identity(
        const struct tr_tls_client_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out);
int tr_tls_openssl_server_profile_create(
        const struct tr_tls_server_config *config,
        struct tr_tls_openssl_profile **out);
int tr_tls_openssl_server_profile_create_from_pem(
        const struct tr_tls_server_config *config,
        const struct tr_tls_credential_bundle *bundle,
        uint32_t max_bundle_bytes, struct tr_tls_openssl_profile **out);
int tr_tls_openssl_profile_peer_identity_enabled(
        const struct tr_tls_openssl_profile *profile);
int tr_tls_openssl_profile_peer_identity_required(
        const struct tr_tls_openssl_profile *profile);
int tr_tls_openssl_profile_retain(struct tr_tls_openssl_profile *profile);
uint64_t
tr_tls_openssl_profile_generation(const struct tr_tls_openssl_profile *profile);
int tr_tls_openssl_profile_next_generation(
        const struct tr_tls_openssl_profile *profile, uint64_t *out_generation);
int tr_tls_openssl_profile_set_generation(
        struct tr_tls_openssl_profile *profile, uint64_t generation);
void tr_tls_openssl_profile_release(struct tr_tls_openssl_profile *profile);
/*
 * memory_budget 是借用指针，必须长于 session。记账只覆盖本库创建的
 * session 与 socket BIO 用户数据；OpenSSL 内部对象不计入该预算。
 * session 持有 profile 强引用，直到 SSL/BIO 全部释放后才归还；调用方
 * 可以在创建成功后释放自己的 profile 引用。
 */
int tr_tls_openssl_session_create(struct tr_tls_openssl_profile *profile,
                                  int fd,
                                  struct tr_memory_budget *memory_budget,
                                  struct tr_tls_openssl_session **out);
/* 仅在 TLS 握手成功后调用。无证书且 require_uri 为 0 时返回 TR_OK
 * 并把 out 置空；证书存在时必须含一个有效 URI SAN。 */
int tr_tls_openssl_session_peer_identity(
        struct tr_tls_openssl_session *session, int require_uri,
        struct tr_peer_identity_snapshot **out);
const struct tr_byte_endpoint_driver *tr_tls_openssl_driver(void);
/* 仅供仓库内 0-RTT 恢复测试检查 SSL 状态，不属于已安装 SDK 接口。 */
SSL *tr_tls_openssl_session_ssl_for_test(
        struct tr_tls_openssl_session *session);

#ifdef __cplusplus
}
#endif

#endif
