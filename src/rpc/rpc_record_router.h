#ifndef TR_RPC_RECORD_ROUTER_H
#define TR_RPC_RECORD_ROUTER_H

#include "rpc_record_stream.h"

struct tr_rpc_record_router;

/* 库内弱能力：字段不可自行修改，不能延长路由器/方法参数的寿命。
 * generation 不回绕；外部须在所有异步使用结束后再退役路由/销毁路由器。
 */
struct tr_rpc_route_handle {
	struct tr_rpc_record_router *router;
	uint64_t generation;
	uint32_t slot;
};

typedef void (*tr_rpc_route_received_fn)(struct tr_rpc_route_handle route,
                                         const struct tr_rpc_record_view *view,
                                         void *method_arg);

struct tr_rpc_record_method {
	struct tr_rpc_method_desc descriptor;
	/* 这是 owner 上的准入/调度回调，不是可阻塞的业务工作函数。
	 * view 只借用到返回；投递异步任务前必须持有或复制业务体和元数据。
	 */
	tr_rpc_route_received_fn received;
	void *arg;
};

typedef void (*tr_rpc_route_terminal_fn)(struct tr_rpc_route_handle route,
                                         struct tr_stream_handle stream,
                                         enum tr_rpc_status status,
                                         enum tr_rpc_record_delivery delivery,
                                         void *arg);

/* 创建时复制全部方法描述并冻结，原数组可以随即释放或修改。arg 是借用，
 * 必须存活到销毁完成。相同 service/method 拒绝，允许不同 service 使用
 * 相同 method。当前只支持 NONE，由已有绑定验证各方法 codec/基数/上限。
 *
 * 不替换 Channel handler：集成方在 OPENED 时 admit，再按同一精确流
 * 转发 receive/event。原 RX 缓冲与信用仍由集成方释放。本模块不拥有
 * Channel/Stream，不自动 RESET 或 close，terminal 是本地结果而非已发
 * 给对端的状态。未知方法/无效 OPEN 可由集成方选择流级拒绝，不必断连接。
 */
int tr_rpc_record_router_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_record_method *methods,
        size_t method_count, uint32_t capacity,
        tr_rpc_route_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_router **out);

/* 只有 TR_OK 发布能力并占用槽位；满载返回 AGAIN，无回调且不接管流。
 * 仅接纳尚在 Channel 中有效的流，禁止为同一传输槽位重复建立路由。
 * 入站固定头最多保留48字节，未取得完整 OPEN 之前不建立方法绑定。
 */
int tr_rpc_record_router_admit_on_owner(struct tr_rpc_record_router *router,
                                        struct tr_stream_handle stream,
                                        struct tr_rpc_route_handle *out);
int tr_rpc_record_router_receive_on_owner(struct tr_rpc_record_router *router,
                                          struct tr_stream_handle stream,
                                          const uint8_t *data, size_t size);
int tr_rpc_record_router_event_on_owner(struct tr_rpc_record_router *router,
                                        struct tr_stream_handle stream,
                                        enum tr_stream_event event);

/* 收到完整且有效 OPEN 才触发对应方法回调。终局后收到字节返回 STATE，
 * 不重新查表或重复回调。期限/鉴权仍属后续调度层，不在此隐式放宽约束。
 * send 沿用绑定的成功才接管 body、成功才提交状态契约。
 * 回调内允许 send/abort；禁止递归 receive/event、admit、retire 或 destroy。
 */
int tr_rpc_record_router_send_on_owner(
        struct tr_rpc_route_handle route,
        const struct tr_rpc_record_header *header, const uint8_t *metadata,
        struct tr_buffer **body);
int tr_rpc_record_router_abort_on_owner(struct tr_rpc_route_handle route,
                                        enum tr_rpc_status status);

/* terminal 至多一次，但槽位继续保留直到显式退役，避免晚到字节新建调用。
 * 退役前外层必须移除该流的后续路由，且所有异步使用者已结束。不能仅因
 * 收到 terminal 就立即释放仍被工作线程使用的参数。active 调用拒绝退役。
 */
int tr_rpc_record_router_retire_on_owner(struct tr_rpc_route_handle route);

/* 销毁前停止外部路由与所有 API 调用。正常关闭以 CANCELLED 收敛全部未
 * 终局路由，所有回调返回后释放。回调参数须存活到整个操作返回；不允许
 * 回调销毁同一个路由器。TX 前缀的预算责任可能长于路由器本身。
 */
int tr_rpc_record_router_destroy_on_owner(struct tr_rpc_record_router **router);
/* 父线程已 join 后逐绑定复用现有恢复租约；全程禁止父循环重启/销毁及
 * 外部并发使用。清理域只通知 OWNER_STOPPED/CANCELLED，不运行方法回调。
 * 部分失败保留路由器供重试，已经通知的终局不会重复。
 */
int tr_rpc_record_router_reap_stopped(struct tr_rpc_record_router **router);

#endif
