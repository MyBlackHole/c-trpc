#ifndef TR_CHANNEL_INTERNAL_H
#define TR_CHANNEL_INTERNAL_H

#include "tr/channel.h"

struct tr_maintenance_scheduler;

/* Channel 创建后所属 Reactor 不再变化，仅供内部 owner routing 使用。 */
struct tr_reactor *tr_channel_reactor(struct tr_channel *channel);

/*
 * Server facade 使用 deferred create，先完成 RPC Endpoint/Method 安装，
 * 再启动 HELLO handshake，避免 peer 在服务层 ready 前发送 RPC 数据。
 */
int tr_channel_create_deferred(
	const struct tr_channel_config *config,
	struct tr_conn_handle control_connection,
	struct tr_conn_handle bulk_connection,
	tr_stream_data_cb data_cb,
	tr_stream_event_cb stream_event_cb,
	tr_channel_event_cb channel_event_cb,
	void *callback_arg,
	struct tr_channel **out);

int tr_channel_start(struct tr_channel *channel);

int tr_channel_set_maintenance_scheduler(
	struct tr_channel *channel,
	struct tr_maintenance_scheduler *maintenance);

#endif
