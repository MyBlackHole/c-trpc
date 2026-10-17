#ifndef TR_CLIENT_RECORD_INTERNAL_H
#define TR_CLIENT_RECORD_INTERNAL_H

#include "tr/client.h"
#include "facade_tuning_internal.h"
#include "rpc/rpc_record_client.h"

/* 显式 V2 入口，返回既有 Client；不安装为 SDK，不协商或回退到 V1。
 * calls 是此 Client 的有界调用容量和进度配置，没有另一个工作线程池。
 * memory_budget_bytes 是单个 Runtime 分片的非零硬预算，覆盖已记账的
 * Reactor、重组池、冻结方法表、调用槽和记录存储；不是进程 RSS 上限。
 * 业务发送体仍由调用者提供有明确释放责任的 tr_buffer。
 */
struct tr_client_record_config {
	struct tr_rpc_record_client_config calls;
	uint64_t memory_budget_bytes;
};

/* 创建时复制并冻结方法表；之后使用 tr_client_connect/wait_ready 和
 * begin_drain/wait_drained/destroy。连接装配在同一 owner 轮次接管 fd、
 * 发布 deferred Channel 及记录宿主，再启动 HELLO。普通连接失败完整
 * 回滚后可重试；回滚自身失败保留 Client，此后只允许 destroy 重试。
 *
 * limits.max_message_bytes 与初始窗口均须至少容纳固定头和最大控制
 * 元数据，并容纳每个方法的最大正文加固定头。对端限制另行校验；本入口
 * 要求更新阈值 T <= W-min(W,max_message_bytes)+1，防止已消费但尚未
 * 通告的信用卡住下一条完整记录；W 是本地初始窗口。
 * 没有解决任意 L>W 的发送活性。仅支持 NONE，拒绝自动重连、interceptor、
 * 连接组配置和旧方法注册/调用/稳定 RPC 统计，不把 V1 字段解释成 V2 能力。
 * tuning 的 executor_* 与 rpc_message_pool_count 在本入口不使用。
 *
 * destroy 先停止并 join 父 Reactor，再交付残留调用的 OWNER_STOPPED
 * 终局、解绑宿主和 Channel，最后释放预算依赖；错误保留对象以便重试。
 * 生命周期由外部控制线程串行执行，owner、旧 RPC 或通用任务工作回调
 * 中拒绝阻塞等待；停止域的终局回调中也不允许重入生命周期操作。
 */
int tr_client_create_record(const struct tr_client_config *config,
                            const struct tr_facade_tuning *tuning,
                            const struct tr_client_record_config *record,
                            const struct tr_rpc_method_desc *methods,
                            size_t method_count, struct tr_client **out);

/* 下列操作由所属 owner 串行执行；普通调用线程同步提交短操作，同一
 * owner 直接执行，其他 Reactor owner 返回 STATE。提交可能等待命令
 * 容量，不是实时或信号安全接口。生命周期必须外部串行化，弱句柄不能
 * 延长 Client 寿命。首次 destroy 尝试开始后只允许销毁重试；即使失败
 * 返回且 Client 尚存，调用句柄也不再可用，不得再发送、取消或查询。
 *
 * start 只有 TR_OK 写 out 并接管一次终局责任；OPEN 暂时未被发送接受
 * 仍是已受理调用。回调可能先于外部 start 返回，arg 必须预先准备，out
 * 存储不能依赖 arg 的寿命。received 借用 view 到返回；允许 send、半关闭
 * 和本地 cancel，但不能在回调内递归 start。终局延后当前 received 返回。
 *
 * options 元数据复制到有界 OPEN 存储；timeout_ms 从调用受理开始使用
 * 本地单调期限，重试 OPEN 传递实际剩余值。配套 V2 Server 从完整 OPEN
 * 接收时建立自身期限并合作取消任务；本地超时不证明服务端已停止执行，
 * 也不证明任意对端实现此契约。断线终止旧调用，不透明重放。连接器的
 * 停止域回收尚未接入，所以 enable_reconnect 非零明确返回 UNSUPPORTED。
 */
int tr_client_record_call_start(
        struct tr_client *client, uint32_t service_id, uint32_t method_id,
        const struct tr_rpc_call_options *options,
        const struct tr_rpc_record_client_callbacks *callbacks,
        struct tr_rpc_record_client_handle *out);

/* 只有 TR_OK 消费并清空 *body；AGAIN 及所有错误保留正文和协议状态。
 * 正文不隐式排队，收到 writable 后仍需由应用原样重试。空 MESSAGE
 * 仍计一条。成功终局与本地取消遵循 first-final-wins；终局后停止交付
 * received，安全退役后旧句柄返回 STALE，不能命中复用后的调用槽。
 */
int tr_client_record_send(struct tr_rpc_record_client_handle handle,
                          struct tr_buffer **body);
int tr_client_record_half_close(struct tr_rpc_record_client_handle handle);

/* 只选择本地 CANCELLED；现有 V2 没有取消/重置帧，不能撤销服务端已受理
 * 的工作。请求尚未 HALF_CLOSE 时重试传输半关闭；已经 HALF_CLOSE 的调用
 * 则先交付终局、停止用户回调，保留固定槽排空晚到响应并归还信用，直到
 * 对端关闭后再退役。不合作的远端可占用该槽至断连或本地 destroy，不保证
 * 限时回收；不会因此关闭整个连接或把旧响应交付给槽位复用后的调用。
 */
int tr_client_record_cancel(struct tr_rpc_record_client_handle handle);

#endif
