#ifndef TR_PIPELINE_INGRESS_INTERNAL_H
#define TR_PIPELINE_INGRESS_INTERNAL_H

#include "../../group/pipeline_registry_internal.h"
#include "../../execution/reactor.h"

struct tr_pipeline_ingress_config {
	struct tr_pipeline_registry *registry;
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
};

/* 仅限所有者的 Pipeline DATA 套接字已接受 fd 交接。
 *
 * TR_OK 时反应器拥有 fd，且已安装固定 48 字节的 TRR1 前导门禁。
 * 只有精确注册表预留附接才会将连接转入正常 TRP1 帧分派。
 *
 * 出错时 fd 所有权仍归调用者。
 */
/* 共享 TRR1 门禁解析出精确 DATA 路由后，附接已接管的连接。
 * 供分片 Pipeline 监听器使用，使 CONTROL 与 DATA
 * 可以共用一个已接受套接字的路由门禁。
 */
int tr_pipeline_ingress_attach_data_route_on_owner(
	const struct tr_pipeline_ingress_config *config,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection);

int tr_pipeline_ingress_adopt_data_fd_on_owner(
	const struct tr_pipeline_ingress_config *config, int fd,
	struct tr_conn_handle *out);

#endif
