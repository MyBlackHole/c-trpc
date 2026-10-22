#ifndef TR_RUNTIME_INTERNAL_H
#define TR_RUNTIME_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#include "../execution/reactor.h"
#include "../execution/deadline_internal.h"
#include "../memory_budget.h"
#include "message_service.h"

struct tr_runtime;
struct tr_runtime_shard;
struct tr_channel;

struct tr_runtime_peer {
	int used;
	/* 握手租约和就绪标记只由所属反应器修改。 */
	int ready;
	struct tr_deadline handshake_deadline;
	void *finalize_ctx;
	struct tr_conn_handle connection;
	struct tr_channel *channel;
};

struct tr_runtime_peer_stats {
	uint32_t capacity;
	uint32_t current;
	uint32_t peak;
	uint32_t reaping_current;
	uint64_t ready_total;
	uint64_t reaped_total;
	uint64_t capacity_rejections;
};

struct tr_runtime_shard_config {
	struct tr_reactor_config reactor;
	uint32_t peer_capacity;
	/* 0 使用通用 tr_runtime_peer 的大小；非零支持调用方私有扩展。
	 * 单槽位必须从 struct tr_runtime_peer 开始并满足其对齐。
	 * Runtime 只分配和回收原始存储，不解释额外字段。
	 */
	size_t peer_stride;

	/* 内部第七阶段分片内存预算。在所有主要分片本地使用方参与
	 * 此能力之前，0 表示仅记账、不设上限。
	 */
	uint64_t memory_budget_bytes;

	/* capacity==0 明确关闭消息服务，保持无消息变换时不创建额外线程。
	 * 启用时要求非零分片硬预算；由 Runtime 创建、停止并回收。 */
	struct tr_message_service_config messages;
};

struct tr_runtime_config {
	uint32_t shard_count;
	const struct tr_runtime_shard_config *shards;
};

/* 正常失败时 *out 保持 NULL。若构造回滚本身失败，函数返回
 * 生命周期错误，同时 *out 保留部分构造的运行时所有权；调用方必须
 * 继续 tr_runtime_destroy() 重试，不能丢弃该对象。
 */
int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out);
/* start/stop/destroy 的跨分片控制轮次由 Runtime 原子门禁串行化，
 * 并发控制者立即返回 TR_ERR_STATE，不接管另一控制者的线程或部分回滚。
 * 停止失败后可再次 stop/destroy；启动失败若自身回滚也失败，
 * 后续 start 应拒绝，直到 stop 完成部分轮次收敛。
 *
 * 门禁只保护这些生命周期控制操作。调用方必须在最终 destroy 释放存储
 * 前停止其他 Runtime/Shard 弱指针访问，并保证消息操作不跨越销毁；
 * 内部门禁并不延长悬挂指针的有效期。
 */
int tr_runtime_start(struct tr_runtime *runtime);
int tr_runtime_stop(struct tr_runtime *runtime);
int tr_runtime_destroy(struct tr_runtime *runtime);

/* 库内分片消息操作，不暴露可自行销毁的服务指针。普通调用线程同步
 * 切到所属 owner；所属 owner 内直接执行。其他 Reactor owner、通用任务
 * 和 RPC 工作线程返回 STATE，避免跨执行域同步等待。命令队列满时可以
 * 等待容量；这些不是实时/信号安全接口。未启用返回 UNSUPPORTED，
 * 尚未启动、停止中或父资源已释放返回 CLOSED。
 *
 * submit 借用 input，只有 TR_OK 消费输入、写 handle 并承担一次完成；
 * 失败保留 input/handle，不调用 done。普通线程收到 TR_OK 前，原 owner
 * 可能已交付 done；arg 须提前准备，handle 输出存储不能依赖 arg 存活。
 * 与 start/stop/destroy 的生命周期操作须由外部串行化，不能把弱句柄当引用。
 * stop 取消残留消息；旧 owner 已 join 后可能由 stop 调用线程交付明确的
 * OWNER_STOPPED 终局。destroy 在输出/输入仍占分片预算时返回 AGAIN，保留
 * Runtime 预算外壳直到应用释放引用后重试。启用不等于 RPC 网络压缩。 */
int tr_runtime_shard_message_submit(
        struct tr_runtime_shard *shard, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);

/* 只提交取消意图，不释放输入/输出/arg，也不从调用线程交付完成。
 * 终局已提交返回 STATE，过期或其他分片/实例能力返回 STALE。取消成功
 * 后仍须等待原完成责任收敛；stop 保留既有 OWNER_STOPPED 清理域契约。
 */
int tr_runtime_shard_message_cancel(struct tr_runtime_shard *shard,
                                    struct tr_task_handle handle);

/* 仅 TR_OK 发布完整快照，任何失败均保留 *out。统计不是引用、准入
 * 预约或销毁屏障；服务重启后从新实例统计，不累计旧实例数据。
 */
int tr_runtime_shard_message_get_stats(struct tr_runtime_shard *shard,
                                       struct tr_message_service_stats *out);

uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime);
struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index);
uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard);
struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard);
int tr_runtime_shard_call(struct tr_runtime_shard *shard, int (*fn)(void *arg),
                          void *arg);
/* 内部分片内存预算能力。使用方必须在分配预算内字节前预留，
 * 并在拆除时释放精确的预留额度。
 */
struct tr_memory_budget *
tr_runtime_shard_memory_budget(struct tr_runtime_shard *shard);
void tr_runtime_shard_memory_stats(
	const struct tr_runtime_shard *shard,
	struct tr_memory_budget_stats *out);

/* 监听器生命周期由分片拥有。监听器事件源注册到该分片的反应器；
 * 关闭时先注销所有者事件源，再关闭 fd。
 */
int tr_runtime_shard_listen_ipv4(struct tr_runtime_shard *shard,
				 const char *address, uint16_t port,
				 int backlog, uint16_t *out_bound_port);
int tr_runtime_shard_listen_ipv4_ex(struct tr_runtime_shard *shard,
				    const char *address, uint16_t port,
				    int backlog, int reuse_port,
				    uint16_t *out_bound_port);
int tr_runtime_shard_listener_fd(const struct tr_runtime_shard *shard);
uint16_t tr_runtime_shard_bound_port(const struct tr_runtime_shard *shard);
typedef void (*tr_runtime_listener_cb)(int fd, uint32_t events, void *arg);

int tr_runtime_shard_enable_listener_events(struct tr_runtime_shard *shard,
					    tr_runtime_listener_cb callback,
					    void *arg);
int tr_runtime_shard_disable_listener_events(struct tr_runtime_shard *shard);
int tr_runtime_shard_close_listener(struct tr_runtime_shard *shard);

/* 对端存储由分片拥有。接受、发布、生命周期解绑与存活快照均在反应器
 * 所有者上运行。运行时拥有存储与计数器；只有已解绑收尾器的退役计数器
 * 可以在所有者之外更新。
 */
uint32_t tr_runtime_shard_peer_capacity(const struct tr_runtime_shard *shard);
struct tr_runtime_peer *
tr_runtime_shard_peer_at(struct tr_runtime_shard *shard, uint32_t slot);
void tr_runtime_shard_peer_note_added(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_ready(struct tr_runtime_shard *shard);
int tr_runtime_shard_peer_reaping_at_capacity(
	const struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_removed_for_reap(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_reaped(struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_note_capacity_rejection(
	struct tr_runtime_shard *shard);
void tr_runtime_shard_peer_stats(const struct tr_runtime_shard *shard,
				 struct tr_runtime_peer_stats *out);

/* 分片本地的延后对端生命周期事件源。通道回调从反应器所有者上通知它；
 * epoll 在后续反应器处理轮次分派它，确保共享连接的 DOWN 通知
 * 在开始解绑前完成。
 */
typedef void (*tr_runtime_peer_event_cb)(int fd, uint32_t events, void *arg);

int tr_runtime_shard_peer_event_fd(const struct tr_runtime_shard *shard);
int tr_runtime_shard_enable_peer_events(struct tr_runtime_shard *shard,
					tr_runtime_peer_event_cb callback,
					void *arg);
int tr_runtime_shard_disable_peer_events(struct tr_runtime_shard *shard);
void tr_runtime_shard_signal_peer_event(struct tr_runtime_shard *shard);
void tr_runtime_shard_drain_peer_event(struct tr_runtime_shard *shard);

#endif
