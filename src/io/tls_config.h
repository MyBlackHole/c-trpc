#ifndef TR_TLS_CONFIG_H
#define TR_TLS_CONFIG_H

#include "../../include/tr/tls.h"

#ifdef __cplusplus
extern "C" {
#endif

/* backend_enabled 为 0 时，仅允许明文；TLS_REQUIRED 在校验结构后
 * 返回 TR_ERR_UNSUPPORTED，防止调用方误以为 TLS 已启用。
 */
int tr_tls_client_config_validate(const struct tr_tls_client_config *config,
                                  int backend_enabled);
int tr_tls_server_config_validate(const struct tr_tls_server_config *config,
                                  int backend_enabled);

#ifdef __cplusplus
}
#endif

#endif
