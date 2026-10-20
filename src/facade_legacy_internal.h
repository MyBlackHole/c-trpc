#ifndef TR_FACADE_LEGACY_INTERNAL_H
#define TR_FACADE_LEGACY_INTERNAL_H

#include "tr/server.h"

/* 仅供尚未迁移的服务端旧 RPC 回归与历史基准使用，不安装到 SDK。
 * 不提供宏别名、协议协商或静默回退。
 */
int tr_server_legacy_create(const struct tr_server_config *config,
                            struct tr_server **out);

/* V1 有意限制只能在服务端启动之前注册方法。 */
int tr_server_legacy_register_method(struct tr_server *server,
                                     const struct tr_rpc_method_desc *method,
                                     tr_rpc_unary_handler handler,
                                     void *handler_arg);

int tr_server_legacy_register_stream_method(
        struct tr_server *server, const struct tr_rpc_method_desc *method,
        const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

#endif
