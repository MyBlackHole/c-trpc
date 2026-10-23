#ifndef TR_TLS_OPENSSL_H
#define TR_TLS_OPENSSL_H

#include "../../include/tr/tls.h"
#include <openssl/ssl.h>

#ifdef __cplusplus
extern "C" {
#endif

struct tr_tls_openssl_profile;
struct tr_tls_openssl_session;
struct tr_byte_endpoint_driver;

int tr_tls_openssl_client_profile_create(
        const struct tr_tls_client_config *config,
        struct tr_tls_openssl_profile **out);
int tr_tls_openssl_server_profile_create(
        const struct tr_tls_server_config *config,
        struct tr_tls_openssl_profile **out);
void tr_tls_openssl_profile_release(struct tr_tls_openssl_profile *profile);
int tr_tls_openssl_session_create(struct tr_tls_openssl_profile *profile,
                                  int fd, struct tr_tls_openssl_session **out);
const struct tr_byte_endpoint_driver *tr_tls_openssl_driver(void);
/* 仅供仓库内 0-RTT 恢复测试检查 SSL 状态，不属于已安装 SDK 接口。 */
SSL *tr_tls_openssl_session_ssl_for_test(
        struct tr_tls_openssl_session *session);

#ifdef __cplusplus
}
#endif

#endif
