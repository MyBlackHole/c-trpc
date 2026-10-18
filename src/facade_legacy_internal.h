#ifndef TR_FACADE_LEGACY_INTERNAL_H
#define TR_FACADE_LEGACY_INTERNAL_H

#include "tr/client.h"
#include "tr/server.h"

/* 仅供尚未迁移的内部回归与历史基准使用，不安装到 SDK。
 * 这些入口明确构造旧 RPC 内核，不能用于普通公开门面验收。
 * 保留旧回归直到覆盖迁移完成；不提供宏别名、协议协商或静默回退。
 */
int tr_client_legacy_create(const struct tr_client_config *config,
                            struct tr_client **out);
int tr_server_legacy_create(const struct tr_server_config *config,
                            struct tr_server **out);

int tr_client_legacy_register_method(struct tr_client *client,
                                     const struct tr_rpc_method_desc *method);

int tr_client_legacy_unary_call_ex(struct tr_client *client,
                                   uint32_t service_id, uint32_t method_id,
                                   const struct tr_rpc_bytes *request,
                                   const struct tr_rpc_call_options *options,
                                   tr_rpc_unary_result_cb result_cb,
                                   void *result_arg,
                                   struct tr_rpc_call_handle *out);

int tr_client_legacy_unary_call(struct tr_client *client, uint32_t service_id,
                                uint32_t method_id,
                                const struct tr_rpc_bytes *request,
                                tr_rpc_unary_result_cb result_cb,
                                void *result_arg,
                                struct tr_rpc_call_handle *out);

int tr_client_legacy_call_start_ex(
        struct tr_client *client, uint32_t service_id, uint32_t method_id,
        const struct tr_rpc_call_options *options,
        const struct tr_rpc_call_callbacks *callbacks,
        struct tr_rpc_call_handle *out);

int tr_client_legacy_call_start(struct tr_client *client, uint32_t service_id,
                                uint32_t method_id,
                                const struct tr_rpc_call_callbacks *callbacks,
                                struct tr_rpc_call_handle *out);

/* V1 有意限制只能在服务端启动之前注册方法。 */
int tr_server_legacy_register_method(struct tr_server *server,
                                     const struct tr_rpc_method_desc *method,
                                     tr_rpc_unary_handler handler,
                                     void *handler_arg);

int tr_server_legacy_register_stream_method(
        struct tr_server *server, const struct tr_rpc_method_desc *method,
        const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

#endif
