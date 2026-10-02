#ifndef TR_CHANNEL_INTERNAL_H
#define TR_CHANNEL_INTERNAL_H

#include "tr/channel.h"

/* Channel 创建后所属 Reactor 不再变化，仅供内部 owner routing 使用。 */
struct tr_reactor *tr_channel_reactor(struct tr_channel *channel);

/* Stream wire-id hash shared by the bounded Channel index and its tests. */
static inline uint32_t tr_channel_stream_id_hash(uint32_t stream_id)
{
	uint32_t value = stream_id;

	value ^= value >> 16;
	value *= UINT32_C(0x7feb352d);
	value ^= value >> 15;
	value *= UINT32_C(0x846ca68b);
	value ^= value >> 16;
	return value;
}

/* 创建后固定的 Stream slot 数，仅供内部预分配索引使用。 */
uint32_t tr_channel_max_streams(struct tr_channel *channel);

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

/*
 * Facade-only socket policy injection. Must be set before automatic reconnect
 * starts; low-level Channel reconnect otherwise preserves the kernel default.
 */
int tr_channel_set_reconnect_tcp_nodelay(struct tr_channel *channel,
					    int enabled);

/*
 * Internal lifecycle observer is independent from the upper-layer Channel
 * handler used by RPC. It receives Channel events after the normal handler and
 * must only perform short non-blocking notification work.
 */
int tr_channel_set_lifecycle_observer(struct tr_channel *channel,
				      tr_channel_event_cb event_cb,
				      void *callback_arg);

/*
 * Server peer teardown is split in two:
 * - detach_for_finalize(): owner-serialized, removes every Reactor/timer callback
 *   source and makes the Channel unreachable from protocol dispatch;
 * - finalize_detached(): owner-free memory/resource release, safe on a cleanup
 *   context after detach returned.
 */
int tr_channel_detach_for_finalize(struct tr_channel *channel);
void tr_channel_finalize_detached(struct tr_channel *channel);

int tr_channel_start(struct tr_channel *channel);

#endif
