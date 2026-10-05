#ifndef TR_RPC_INTERNAL_H
#define TR_RPC_INTERNAL_H

#include <stdint.h>

#include "../execution/buffer.h"
#include "../transport/channel/channel.h"
#include "../observability.h"
#include "tr/rpc.h"

struct tr_rpc_endpoint;

/*
 * 当前线程属于 c-trpc RPC Executor 工作线程时返回真。
 * 同步销毁不得 join 或等待正在执行销毁操作的工作线程自身。
 */
int tr_rpc_in_worker_context(void);

enum tr_rpc_role { TR_RPC_CLIENT = 1, TR_RPC_SERVER = 2 };

struct tr_rpc_endpoint_config {
	enum tr_rpc_role role;
	uint32_t max_methods;
	uint32_t max_calls;

	/* 用于 RPC 信封和控制路径复制。 */
	struct tr_buffer_pool *message_pool;

	uint32_t executor_queue_capacity;
	uint32_t executor_threads;
	uint32_t executor_continuation_reserve;
	uint32_t observability_flags;
	struct tr_rpc_interceptor interceptor;
};

struct tr_rpc_endpoint_stats {
	uint32_t max_methods;
	uint32_t registered_methods;
	uint32_t max_calls;
	uint32_t opening_calls;
	uint32_t active_calls;
	uint32_t terminal_calls;

	uint32_t executor_threads;
	uint32_t executor_queued_tasks;
	uint32_t executor_running_tasks;
	uint32_t executor_queue_capacity;
	uint32_t executor_continuation_reserve;

	struct tr_queue_observation executor_queue;
	uint32_t executor_ready_calls;
	uint32_t executor_ready_calls_peak;
	uint32_t observability_flags;

	uint64_t executor_enqueued_tasks;
	uint64_t executor_taken_tasks;
	uint64_t executor_admission_limit_hits;
	uint64_t executor_hard_full_events;
	struct tr_latency_histogram executor_queue_wait_ns;
	struct tr_latency_histogram executor_handler_ns;

	uint64_t calls_started;
	uint64_t calls_completed;
	uint64_t calls_cancelled;
	uint64_t calls_deadline_exceeded;

	struct tr_rpc_semantic_stats semantic;
};

/* Endpoint 生命周期内稳定的 Method 哈希，与冲突测试共用。 */
static inline uint64_t tr_rpc_method_hash(uint32_t service_id,
					  uint32_t method_id)
{
	uint64_t value = ((uint64_t)service_id << 32) | (uint64_t)method_id;

	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	value ^= value >> 31;
	return value;
}

/* 底层 Endpoint 引擎，仅供门面和测试内部使用。 */
int tr_rpc_endpoint_create(struct tr_channel *channel,
			   const struct tr_rpc_endpoint_config *config,
			   struct tr_rpc_endpoint **out);
void tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint);

/*
 * Method publication 是同步 Reactor-owner control-plane barrier。
 *
 * method/handlers descriptor 在调用期间复制进 owner request；TR_OK 返回后 Method
 * entry/index 已完整发布。handler_arg 的生命周期仍由调用方管理，至少必须覆盖
 * Endpoint/已注册 Method 的使用期。
 */
int tr_rpc_register_method(struct tr_rpc_endpoint *endpoint,
			   const struct tr_rpc_method_desc *method,
			   tr_rpc_unary_handler unary_handler,
			   void *handler_arg);
int tr_rpc_register_stream_method(
	struct tr_rpc_endpoint *endpoint, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg);

int tr_rpc_unary_call_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 const struct tr_rpc_call_options *options,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out);
int tr_rpc_unary_call(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id, const struct tr_rpc_bytes *request,
		      tr_rpc_unary_result_cb result_cb, void *result_arg,
		      struct tr_rpc_call_handle *out);
int tr_rpc_call_start_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_options *options,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out);
int tr_rpc_call_start(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id,
		      const struct tr_rpc_call_callbacks *callbacks,
		      struct tr_rpc_call_handle *out);

/* 内部零拷贝/最少拷贝 Buffer 快路径。 */
int tr_rpc_call_send_buffer(struct tr_rpc_call_handle call,
			    struct tr_buffer *payload);

int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint);
int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out);
int tr_rpc_endpoint_get_semantic_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_semantic_stats *out);

/* 测试/诊断桥接接口，不在公开 tr_rpc_message 中暴露 Stream。 */
int tr_rpc_message_stream_internal(const struct tr_rpc_message *message,
				   struct tr_stream_handle *out);

struct tr_rpc_executor_group;
int tr_rpc_executor_group_create(uint32_t endpoint_capacity,
				 uint32_t max_calls_per_endpoint,
				 uint32_t thread_count,
				 struct tr_rpc_executor_group **out);
void tr_rpc_executor_group_destroy(struct tr_rpc_executor_group *group);

int tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel, const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group, struct tr_rpc_endpoint **out);

/*
 * Server 回收路径：同步静止并排空 Endpoint，获取最终计数器快照后再释放。
 * stats 可以为 NULL。
 */
void tr_rpc_endpoint_destroy_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats);

/*
 * Server Peer 两阶段清理。
 */
int tr_rpc_endpoint_detach_for_finalize(struct tr_rpc_endpoint *endpoint);
void tr_rpc_endpoint_finalize_detached_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats);

typedef void (*tr_rpc_endpoint_detached_finalizer)(
	const struct tr_rpc_endpoint_stats *stats, void *arg);

int tr_rpc_endpoint_arm_detached_finalizer(
	struct tr_rpc_endpoint *endpoint,
	tr_rpc_endpoint_detached_finalizer finalizer, void *arg);
void tr_rpc_endpoint_release_detached_owner(struct tr_rpc_endpoint *endpoint);

/* 有界 Call 截止时间堆的内部确定性诊断接口。 */
int tr_rpc_deadline_heap_snapshot(struct tr_rpc_endpoint *endpoint,
				  uint32_t *count,
				  struct tr_rpc_call_handle *root,
				  uint64_t *root_deadline_ns);

#endif
