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
 * 仅限所有者使用的 Pipeline DATA socket 已接受 fd 移交接口。
 *
 * 返回 TR_OK 后，fd 由 Reactor 拥有，并安装固定 48 字节的 TRR1 前导数据门控。
 * 只有精确匹配注册表预留并完成 attach 后，
 * 连接才会进入正常 TRP1 帧分发。
 *
 * 出错时 fd 所有权仍归调用方。
 */
/*
 * 共享 TRR1 门控解析出精确 DATA 路由后，
 * 把一个已经由 Reactor 接管的连接附加到 Pipeline。shard Pipeline listener 使 CONTROL 和 DATA
 * 可以共享同一个已接受 socket 的路由门控。
 */
int tr_pipeline_ingress_attach_data_route_on_owner(
	const struct tr_pipeline_ingress_config *config,
	const struct tr_pipeline_route_preface *preface,
	struct tr_conn_handle connection);

int tr_pipeline_ingress_adopt_data_fd_on_owner(
	const struct tr_pipeline_ingress_config *config, int fd,
	struct tr_conn_handle *out);

#endif
