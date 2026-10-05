#ifndef TR_RUNTIME_INTERNAL_H
#define TR_RUNTIME_INTERNAL_H

#include <stdint.h>

#include "../execution/reactor.h"
#include "../memory_budget.h"

struct tr_runtime;
struct tr_runtime_shard;
struct tr_rpc_executor_group;
struct tr_channel;
struct tr_rpc_endpoint;

struct tr_runtime_peer {
	int used;
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

/*
 * Runtime 为每个分片拥有一个独立资源域。
 * 配置按分片显式提供，因此启用 N 个分片不会隐式放大 Server 全局预算。
 */
struct tr_runtime_rpc_executor_config {
	uint32_t endpoint_capacity;
	uint32_t max_calls_per_endpoint;
	uint32_t thread_count;
};

struct tr_runtime_shard_config {
	struct tr_reactor_config reactor;
	uint32_t peer_capacity;

	/*
	 * 阶段 7 的内部分片内存预算。
	 * 在主要分片本地资源使用方全部接入之前，0 表示只记账、不限制。
	 */
	uint64_t memory_budget_bytes;

	struct tr_runtime_rpc_executor_config rpc_executor;
};

struct tr_runtime_config {
	uint32_t shard_count;
	const struct tr_runtime_shard_config *shards;
};

int tr_runtime_create(const struct tr_runtime_config *config,
		      struct tr_runtime **out);
int tr_runtime_start(struct tr_runtime *runtime);
int tr_runtime_stop(struct tr_runtime *runtime);
void tr_runtime_destroy(struct tr_runtime *runtime);

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

/*
 * 内部分片内存预算能力。
 * 使用方必须先预留预算再分配受预算约束的字节，并在销毁时精确归还预留值。
 */
struct tr_memory_budget *
tr_runtime_shard_memory_budget(struct tr_runtime_shard *shard);
void tr_runtime_shard_memory_stats(
	const struct tr_runtime_shard *shard,
	struct tr_memory_budget_stats *out);

/*
 * 监听器生命周期由分片拥有。
 * 监听器事件源注册到该分片的 Reactor；
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
void tr_runtime_shard_close_listener(struct tr_runtime_shard *shard);

/*
 * 对端存储由分片拥有。
 * 接收、发布、生命周期解除关联和存活快照都在 Reactor 所有者上执行。
 * Runtime 拥有存储和计数器；只有已解除关联最终清理器的退役计数
 * 允许在非所有者上下文更新。
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

/*
 * 分片本地的延迟对端生命周期事件源。
 * Channel 回调从 Reactor 所有者发出通知；
 * epoll 在后续 Reactor 轮次分发该事件，
 * 确保共享连接的 DOWN 通知全部完成后才开始解除关联。
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
