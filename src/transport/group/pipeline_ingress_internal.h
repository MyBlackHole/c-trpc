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

/*
 * 仅所有者可用的 Pipeline DATA 套接字已接收 fd 移交接口。
 *
 * 返回 TR_OK 时，Reactor 接管 fd，并安装固定 48 字节 TRR1 前导信息门控。
 * 只有注册表中的精确预留附着成功后，连接才进入普通 TRP1 帧分发。
 *
 * 发生错误时，fd 所有权仍归调用方。
 */
/*
 * 共享 TRR1 门控解析出精确 DATA 路由后，
 * 将一个已经接管的连接附着到 Pipeline。
 * 分片 Pipeline 监听器使用该接口，使 CONTROL 与 DATA
 * 可以共享同一个已接收套接字路由门控。
 */
int tr_pipeline_ingress_attach_data_route_on_owner(
	const struct tr_pipeline_ingress_config *config,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection);

int tr_pipeline_ingress_adopt_data_fd_on_owner(
	const struct tr_pipeline_ingress_config *config, int fd,
	struct tr_conn_handle *out);

#endif
