#ifndef TR_CHANNEL_INTERNAL_H
#define TR_CHANNEL_INTERNAL_H

#include "channel.h"

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
 * Facade-only socket policy injection。通过 Channel 所属 Reactor owner 串行化，
 * 必须在 automatic reconnect 启动前设置；低层 Channel reconnect 否则保留
 * kernel default。
 */
int tr_channel_set_reconnect_tcp_nodelay(struct tr_channel *channel,
					    int enabled);

/*
 * Internal lifecycle observer 独立于 RPC 使用的 upper-layer Channel handler。
 * publication 与读取都在 Channel 所属 Reactor owner 上完成，不属于
 * channel->lock 保护域。observer 在 normal handler 之后收到 Channel event，
 * 且只能执行短小、非阻塞的通知工作。
 */
int tr_channel_set_lifecycle_observer(struct tr_channel *channel,
				      tr_channel_event_cb event_cb,
				      void *callback_arg);

/*
 * Server peer teardown 分成两个阶段：
 * - detach_for_finalize()：由 owner 串行执行，移除全部 Reactor/timer callback
 *   source，使 Channel 不再能被协议 dispatch 触达；
 * - finalize_detached()：只做不依赖 owner 的内存/资源释放，必须在 detach
 *   成功后才能调用。
 *
 * detach_for_finalize() 是可重试生命周期屏障。handler/timer detach 失败时
 * Channel 继续由调用方拥有，并保留所有尚未确认解除的 publication；只有返回
 * TR_OK 才会发布 teardown_detached，并允许进入 finalization。
 */
int tr_channel_detach_for_finalize(struct tr_channel *channel);
int tr_channel_finalize_detached(struct tr_channel *channel);

int tr_channel_start(struct tr_channel *channel);

#endif
