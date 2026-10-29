#ifndef TR_TLS_TRUST_SNAPSHOT_H
#define TR_TLS_TRUST_SNAPSHOT_H

#include <openssl/ssl.h>
#include <tr/tls.h>

/* 只在 profile 构造期调用；返回前完成信任源 I/O，不注册延迟路径查找器。 */
int tr_tls_openssl_trust_snapshot(SSL_CTX *context,
                                  const struct tr_tls_client_config *config);

#endif
