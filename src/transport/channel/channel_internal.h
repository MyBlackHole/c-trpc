#ifndef TR_CHANNEL_INTERNAL_H
#define TR_CHANNEL_INTERNAL_H

#include "channel.h"
#include "../../execution/deadline_internal.h"

/* Channel 创建后所属反应器不再变化，仅供内部所有者路由使用。 */
struct tr_reactor *tr_channel_reactor(struct tr_channel *channel);

/* 仅供所有者处理上层生命周期失败：无需命令准入，同步退役当前通道的
 * 精确连接。回调内联执行；调用时不得持有 Channel 或上层协议锁。
 */
int tr_channel_abort_lane_on_owner(struct tr_channel *channel,
                                  enum tr_lane lane, int status);

/* 返回精确流代次的逻辑通道，供 RPC 入站方法校验。 */
int tr_stream_get_lane_internal(struct tr_stream_handle stream,
                               enum tr_lane *out);

/* 有界 Channel 索引与其测试共用的流线协议标识哈希。 */
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

/* 创建后固定的流槽位数，仅供内部预分配索引使用。 */
uint32_t tr_channel_max_streams(struct tr_channel *channel);

/* 服务端门面使用延后创建，先完成 RPC Endpoint/Method 安装，
 * 再启动 HELLO 握手，避免对端在服务层就绪前发送 RPC 数据。
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

/* 仅供门面的套接字策略注入。通过 Channel 所属反应器所有者串行化，
 * 必须在自动重连启动前设置；否则底层 Channel 重连保留
 * 内核默认值。
 */
int tr_channel_set_reconnect_tcp_nodelay(struct tr_channel *channel,
					    int enabled);

/* 内部生命周期观察者独立于 RPC 使用的上层 Channel 处理函数。
 * 发布与读取都在 Channel 所属反应器所有者上完成，不属于
 * channel->lock 保护域。观察者在普通处理函数之后收到 Channel 事件，
 * 且只能执行短小、非阻塞的通知工作。
 */
int tr_channel_set_lifecycle_observer(struct tr_channel *channel,
				      tr_channel_event_cb event_cb,
				      void *callback_arg);

/* 服务端对端拆除分成两个阶段：
 * - detach_for_finalize()：由所有者串行执行，移除全部反应器或定时器回调
 *   源，使 Channel 不再能被协议分派触达；
 * - finalize_detached()：只做不依赖所有者的内存与资源释放，必须在解绑
 *   成功后才能调用。
 *
 * detach_for_finalize() 是可重试生命周期屏障。处理函数或定时器解绑失败时
 * Channel 继续由调用方拥有，并保留所有尚未确认解除的发布；只有返回
 * TR_OK 才会发布 teardown_detached，并允许进入收尾。
 */
int tr_channel_detach_for_finalize(struct tr_channel *channel);
/* detach_for_finalize() 成功后，所有可失败生命周期屏障已经完成。
 * finalize_detached() 只做不依赖所有者的本地资源释放，语义上不可失败。
 */
void tr_channel_finalize_detached(struct tr_channel *channel);

/* 供客户端连接使用；TCP 与 HELLO 共享上游创建的截止时间。 */
int tr_channel_wait_ready_deadline(
	struct tr_channel *channel, enum tr_lane lane,
	const struct tr_deadline *deadline);

/* 在已启动排空屏障的 Channel 上等待。deadline 为同一操作共享的
 * 有限 CLOCK_MONOTONIC 绝对截止时间；NULL 表示只检查当前状态、不阻塞。
 * 到期时仍返回 TR_AGAIN，与公开 wait_drained 的返回码契约一致。
 */
int tr_channel_wait_drained_deadline(
	struct tr_channel *channel, const struct tr_deadline *deadline);

int tr_channel_start(struct tr_channel *channel);

#endif
