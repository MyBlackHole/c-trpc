#ifndef TR_RUNTIME_INTERNAL_H
#define TR_RUNTIME_INTERNAL_H

#include <stdint.h>

#include "../execution/reactor.h"
#include "../execution/deadline_internal.h"
#include "../memory_budget.h"
#include "message_service.h"

struct tr_runtime;
struct tr_runtime_shard;
struct tr_rpc_executor_group;
struct tr_channel;
struct tr_rpc_endpoint;

struct tr_runtime_peer {
	int used;
	/* 握手租约和就绪标记只由所属反应器修改。 */
	int ready;
	struct tr_deadline handshake_deadline;
	void *finalize_ctx;
	struct tr_conn_handle connection;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
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

/* 运行时为每个分片拥有一个独立资源域。每个分片显式配置，
 * 因此启用 N 个分片绝不会隐式地将服务端总预算放大 N 倍。
 */
struct tr_runtime_rpc_executor_config {
	uint32_t endpoint_capacity;
	uint32_t max_calls_per_endpoint;
	uint32_t thread_count;
};

struct tr_runtime_shard_config {
	struct tr_reactor_config reactor;
	uint32_t peer_capacity;

	/* 内部第七阶段分片内存预算。在所有主要分片本地使用方参与
	 * 此能力之前，0 表示仅记账、不设上限。
	 */
	uint64_t memory_budget_bytes;

	struct tr_runtime_rpc_executor_config rpc_executor;
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
/* start() 要么启动全部分片，要么返回错误。若回滚本身遇到生命周期屏障
 * 失败，对应分片仍标记为已启动，且拒绝新的启动，
 * 直到 stop() 收敛该部分轮次。
 */
int tr_runtime_start(struct tr_runtime *runtime);
/* stop() 也会收敛启动回滚失败留下的部分分片，
 * 即使整个运行时从未发布为已启动。
 */
int tr_runtime_stop(struct tr_runtime *runtime);
int tr_runtime_destroy(struct tr_runtime *runtime);

/* 库内真实分片入口，借用 input，只有 TR_OK 消费输入并承担一次完成。
 * 运行中由所属 owner 提交，未启用返回 UNSUPPORTED，停止中返回 CLOSED。
 * 与 start/stop/destroy 的生命周期操作须由外部串行化，不能把弱句柄当引用。
 * stop 取消残留消息；旧 owner 已 join 后可能由 stop 调用线程交付明确的
 * OWNER_STOPPED 终局。destroy 在输出/输入仍占分片预算时返回 AGAIN，保留
 * Runtime 预算外壳直到应用释放引用后重试。部分销毁后返回 CLOSED。
 * 普通线程或本分片 owner 可调用；另一 Reactor 的 owner 返回 STATE，
 * 不执行跨分片同步等待。启用不等于 RPC 网络压缩。 */
int tr_runtime_shard_message_submit(
        struct tr_runtime_shard *shard, struct tr_message *input,
        const struct tr_message_transform_config *config,
        tr_message_service_done_fn done, void *arg,
        struct tr_task_handle *handle);

/* 与 submit 相同的生命周期及执行上下文限制。运行中按实例/槽位代次
 * 校验，错误分片/旧实例返回 STALE；终局已提交返回 STATE，已退役返回
 * STALE。未配置消息服务返回 UNSUPPORTED，父已停止/部分销毁返回 CLOSED。
 * TR_OK 仅接受取消意图，仍须等待原 done 交付，不能提前释放 arg/载荷。
 * 不取得任务或分片强引用，不与 Runtime 生命周期操作并发。 */
int tr_runtime_shard_message_cancel(struct tr_runtime_shard *shard,
                                    struct tr_task_handle handle);

uint32_t tr_runtime_shard_count(const struct tr_runtime *runtime);
struct tr_runtime_shard *tr_runtime_shard_at(struct tr_runtime *runtime,
					     uint32_t index);
uint32_t tr_runtime_shard_id(const struct tr_runtime_shard *shard);
struct tr_reactor *
tr_runtime_shard_reactor(const struct tr_runtime_shard *shard);
int tr_runtime_shard_call(struct tr_runtime_shard *shard,
			  int (*fn)(void *arg), void *arg);
struct tr_rpc_executor_group *
tr_runtime_shard_rpc_executor(const struct tr_runtime_shard *shard);

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
