#ifndef TR_RPC_INTERNAL_H
#define TR_RPC_INTERNAL_H

#include <stdint.h>

#include "../execution/buffer.h"
#include "../transport/channel/channel.h"
#include "../observability.h"
#include "tr/rpc.h"

struct tr_rpc_endpoint;

enum tr_rpc_role { TR_RPC_CLIENT = 1, TR_RPC_SERVER = 2 };

struct tr_rpc_endpoint_config {
	enum tr_rpc_role role;
	uint32_t max_methods;
	uint32_t max_calls;

	/* 用于 RPC 封装或控制路径复制。 */
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

/* 与碰撞测试共用的、在 Endpoint 生命周期内稳定的 Method 哈希。 */
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

/* 底层 Endpoint 引擎，仅供门面与测试使用。
 *
 * 正常规则只有 TR_OK 才转移所有权。两种可重试的生命周期例外：
 * - 已发布截止时间定时器，但后续 Channel handler 安装失败且定时器注销
 *   也失败：错误返回时 *out 保留部分构造的 Endpoint，不能提前 free；
 * - 独立工作线程启动失败，后续停止或线程回收屏障也失败：同样保留 *out。
 * 调用方必须先持有 Channel/Runtime，再对部分 Endpoint 反复调用
 * tr_rpc_endpoint_destroy()，直到其返回 TR_OK 才可释放依赖资源。
 */
int tr_rpc_endpoint_create(struct tr_channel *channel,
			   const struct tr_rpc_endpoint_config *config,
			   struct tr_rpc_endpoint **out);
int tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint);

/* Method 发布是同步反应器所有者控制平面屏障。
 *
 * method/handlers 描述符在调用期间复制进所有者请求；TR_OK 返回后 Method
 * 条目与索引已完整发布。handler_arg 的生命周期仍由调用方管理，至少必须覆盖
 * Endpoint 或已注册 Method 的使用期。
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

/* 内部零拷贝或最少拷贝缓冲区快速路径。 */
int tr_rpc_call_send_buffer(struct tr_rpc_call_handle call,
			    struct tr_buffer *payload);

int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint);
int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out);
int tr_rpc_endpoint_get_semantic_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_semantic_stats *out);

/* 测试与诊断桥接，不在公开 tr_rpc_message 中暴露流。 */
int tr_rpc_message_stream_internal(const struct tr_rpc_message *message,
				   struct tr_stream_handle *out);

struct tr_rpc_executor_group;

/* 创建只建立组软状态，不启动工作线程；因此失败路径只包含不可失败
 * 的本地资源释放。工作线程轮次由 Runtime 启动或停止显式控制。
 */
int tr_rpc_executor_group_create(uint32_t endpoint_capacity,
				 uint32_t max_calls_per_endpoint,
				 uint32_t thread_count,
				 struct tr_rpc_executor_group **out);

/* start/stop/destroy 同一组时独占生命周期控制门禁。并发控制者收到
 * TR_ERR_STATE 且不得修改线程数组、准入门禁或回收游标，调用方可随后重试。
 * 这道门禁不持有组锁等待 pthread_create 或 pthread_join。
 *
 * destroy_checked() 释放组的存储，因此外部组所有者必须在销毁前禁止
 * 新的裸指针使用者；内部忙状态不能代替对象内存的外部存活期管理。
 */
int tr_rpc_executor_group_can_start(struct tr_rpc_executor_group *group);
int tr_rpc_executor_group_start(struct tr_rpc_executor_group *group);
int tr_rpc_executor_group_stop(struct tr_rpc_executor_group *group);

/* 终止销毁先执行可重试的停止与线程回收屏障；pthread_join 失败时组
 * 存储与精确的未回收线程句柄保持不变，调用方继续拥有组。
 */
int tr_rpc_executor_group_destroy_checked(
	struct tr_rpc_executor_group *group);

/* group!=NULL 时 Endpoint 借用分片执行器组，不启动独立工作线程。
 * 两种构造模式都可能因异步定时器回滚失败而返回错误且 *out 非 NULL。
 * group==NULL 时 Endpoint 发布完成并先转移到 *out，随后启动独立
 * 工作线程；线程启动回滚时的回收失败也保留 *out 所有权。
 */
int tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel, const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group, struct tr_rpc_endpoint **out);

/* 服务端回收路径：同步使 Endpoint 静止并排空，对其最终计数器取快照，
 * 然后释放它。stats 可以为 NULL。
 */
int tr_rpc_endpoint_destroy_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats);

/* 服务端对端两阶段拆除。 */
int tr_rpc_endpoint_detach_for_finalize(struct tr_rpc_endpoint *endpoint);
void tr_rpc_endpoint_finalize_detached_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats);

typedef void (*tr_rpc_endpoint_detached_finalizer)(
	const struct tr_rpc_endpoint_stats *stats, void *arg);

int tr_rpc_endpoint_arm_detached_finalizer(
	struct tr_rpc_endpoint *endpoint,
	tr_rpc_endpoint_detached_finalizer finalizer, void *arg);
void tr_rpc_endpoint_release_detached_owner(struct tr_rpc_endpoint *endpoint);

/* 用于有界 Call 截止时间堆的内部确定性诊断。 */
int tr_rpc_deadline_heap_snapshot(struct tr_rpc_endpoint *endpoint,
				  uint32_t *count,
				  struct tr_rpc_call_handle *root,
				  uint64_t *root_deadline_ns);

#endif
