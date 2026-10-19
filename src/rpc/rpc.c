#define _GNU_SOURCE
#include "../execution/worker_context.h"
#include "tr/rpc.h"

#include "rpc_wire.h"
#include "tr/status.h"
#include "../endian.h"
#include "../guard.h"
#include "../refcount.h"
#include "rpc_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../execution/reactor_internal.h"
#include "../observability_internal.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

_Static_assert(sizeof(uintptr_t) <= sizeof(uint64_t),
	       "RPC Call capability requires pointers no wider than 64 bits");

static inline struct tr_rpc_endpoint *
tr_rpc_call_handle_endpoint(struct tr_rpc_call_handle handle)
{
	return (struct tr_rpc_endpoint *)(uintptr_t)handle._private[0];
}

static inline uint32_t
tr_rpc_call_handle_slot(struct tr_rpc_call_handle handle)
{
	return (uint32_t)handle._private[1];
}

static inline uint32_t
tr_rpc_call_handle_generation(struct tr_rpc_call_handle handle)
{
	return (uint32_t)(handle._private[1] >> 32);
}

enum tr_rpc_call_state {
	TR_RPC_CALL_FREE = 0,
	TR_RPC_CALL_OPENING,
	TR_RPC_CALL_ACTIVE,
	TR_RPC_CALL_TERMINAL
};

#define TR_RPC_CALL_FREE_NONE UINT32_MAX
#define TR_RPC_STREAM_CALL_NONE UINT32_MAX
#define TR_RPC_DEADLINE_HEAP_NONE UINT32_MAX

enum tr_rpc_handler_kind {
	TR_RPC_HANDLER_NONE = 0,
	TR_RPC_HANDLER_UNARY,
	TR_RPC_HANDLER_STREAM
};

enum tr_rpc_task_type {
	TR_RPC_TASK_SERVER_UNARY = 1,
	TR_RPC_TASK_SERVER_STREAM_MESSAGE,
	TR_RPC_TASK_SERVER_HALF_CLOSE,
	TR_RPC_TASK_SERVER_WRITABLE,
	TR_RPC_TASK_SERVER_CLOSE,
	TR_RPC_TASK_CLIENT_UNARY_RESULT,
	TR_RPC_TASK_CLIENT_MESSAGE,
	TR_RPC_TASK_CLIENT_EVENT
};

struct tr_rpc_method_index_entry {
	uint32_t service_id;
	uint32_t method_id;
	uint32_t slot;
};

struct tr_rpc_method_entry {
	int used;
	enum tr_rpc_handler_kind handler_kind;
	struct tr_rpc_method_desc desc;

	tr_rpc_unary_handler unary_handler;
	struct tr_rpc_stream_handlers stream_handlers;
	void *handler_arg;
};

struct tr_rpc_task {
	uint16_t type;
	uint16_t first_message;
	int status;
	enum tr_rpc_call_event event;
	struct tr_rpc_call_handle call;
	struct tr_buffer *payload;

	/* 工作线程执行快照。可变 Call/Endpoint 状态仍由反应器拥有；
	 * 工作线程只消耗这些复制的能力凭证。
	 */
	struct tr_stream_handle stream;
	union {
		struct {
			tr_rpc_unary_handler handler;
			void *handler_arg;
			struct tr_rpc_method_desc method;
		} server_unary;
		struct {
			struct tr_rpc_stream_handlers handlers;
			void *handler_arg;
		} server_stream;
		struct {
			tr_rpc_unary_result_cb result_cb;
			void *result_arg;
		} client_unary;
		struct {
			struct tr_rpc_call_callbacks callbacks;
		} client_stream;
	} u;
};

struct tr_rpc_call_slot {
	uint32_t generation;
	uint32_t free_next;
	enum tr_rpc_call_state state;

	struct tr_stream_handle stream;
	struct tr_rpc_method_entry *method;

	uint32_t tx_count;
	uint32_t rx_count;
	uint32_t task_refs;
	uint32_t interceptor_mask;
	int interceptor_active;

	int is_unary;
	int local_closed;
	int remote_closed;
	int final_status_seen;
	int final_status_sent;
	int final_status;
	int admission_rejected;
	int executor_overloaded;
	int pending_control_is_final_status;
	int pending_remote_half_close;

	int cancelled;
	int cancel_status;
	int terminal_notified;
	int semantic_started;
	int semantic_finished;
	uint64_t deadline_ns;
	uint32_t deadline_heap_pos;

	uint8_t local_initial_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t local_initial_metadata_len;
	uint8_t peer_initial_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t peer_initial_metadata_len;
	uint8_t local_trailing_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t local_trailing_metadata_len;
	uint8_t peer_trailing_metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t peer_trailing_metadata_len;

	struct tr_buffer *pending_tx;
	struct tr_buffer *pending_control;
	int need_local_close;
	/* 失败的服务端一元调用完成结果现在承担错误或关闭义务。 */
	int unary_failure_pending;
	int response_received;
	int result_delivered;

	tr_rpc_unary_result_cb result_cb;
	void *result_arg;

	struct tr_rpc_call_callbacks callbacks;

	/* 每个 Call 最多有一个服务端执行器任务在执行器队列外等待。
	 * 这使过载背压受 max_calls 限制，无需分配无界旁路队列。
	 */
	struct tr_rpc_task pending_executor_task;
	int pending_executor_task_valid;

	/* 每个 Call 一个终止回调义务。与普通待处理接收工作不同，
	 * 它在取消后仍存活，并保留响应接收所有权，直到能够进入有界执行器，
	 * 或 Endpoint 被显式拆除。无需额外无界分配或生产者线程。
	 */
	struct tr_rpc_task pending_terminal_task;
	int pending_terminal_task_valid;
};

/* 一元调用工作线程只产生业务结果；线协议元数据、Call 状态和发送动作
 * 必须回到反应器所有者线程后再处理。
 */
struct tr_rpc_unary_completion {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;
	int status;
	uint32_t response_len;
	uint8_t response[];
};

struct tr_rpc_task_completion {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;
};

#define TR_RPC_EXEC_NONE UINT32_MAX

struct tr_rpc_executor_node {
	struct tr_rpc_task task;
	uint64_t enqueued_ns;
	uint32_t next;
};

struct tr_rpc_executor_callq {
	uint32_t generation;
	uint32_t head;
	uint32_t tail;
	uint32_t queued_count;
	int ready;
	int running;
	int cancelled;
};

struct tr_rpc_executor_group;
struct tr_rpc_endpoint;

struct tr_rpc_executor {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t *threads;
	uint32_t thread_count;

	struct tr_rpc_executor_node *nodes;
	struct tr_rpc_executor_callq *callq;
	uint32_t *ready_calls;

	uint32_t capacity;
	uint32_t continuation_reserve;
	uint32_t free_head;
	uint32_t queued_count;
	uint32_t queued_peak;
	uint32_t running_count;

	uint64_t enqueued_tasks;
	uint64_t taken_tasks;
	uint64_t admission_limit_hits;
	uint64_t hard_full_events;
	struct tr_latency_histogram queue_wait_ns;
	struct tr_latency_histogram handler_ns;

	uint32_t ready_capacity;
	uint32_t ready_head;
	uint32_t ready_tail;
	uint32_t ready_count;
	uint32_t ready_peak;

	struct tr_rpc_executor_group *group;
	/* 由执行器组锁保护；每个 Endpoint 一个侵入式调度节点。 */
	struct tr_rpc_endpoint *group_ready_next;
	int group_enqueued;
	int stopping;
	/* 互斥锁与条件变量已初始化；构造失败清理据此判断是否可执行关闭。 */
	int sync_ready;
	uint32_t started_threads;
	/* 已成功回收的线程前缀；失败重试从该位置继续，禁止重复回收。 */
	uint32_t join_cursor;
};

struct tr_rpc_executor_group {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t *threads;

	/* 侵入式就绪队列：每个 Endpoint 最多通过 executor.group_enqueued
	 * 发布一个调度令牌。因此队列容量遵循 Endpoint 生命周期上限，
	 * 而不是另一个可能填满的固定环形队列。
	 */
	struct tr_rpc_endpoint *ready_head;
	struct tr_rpc_endpoint *ready_tail;
	uint32_t ready_count;

	uint32_t thread_count;
	uint32_t started_threads;
	/* 已成功回收的线程前缀；销毁失败时保留以便精确重试。 */
	uint32_t join_cursor;
	/* 已接受但尚未完成的任务数；工作线程停机前必须排空。 */
	uint64_t outstanding_tasks;
	/* 必须有活跃工作线程轮次才允许接收新任务。stop 后保持
	 * 关闭，直到下一次 start；与 stopping 同由 group->lock 保护。
	 */
	int accepting;
	int stopping;
};

TR_DEFINE_PTR_OWNERSHIP(tr_rpc_group_mem, struct tr_rpc_executor_group, free)
TR_DEFINE_PTR_OWNERSHIP(tr_rpc_thread_array, pthread_t, free)

struct tr_rpc_endpoint {
	/* Call、Method 与协议状态。 */
	pthread_mutex_t lock;
	/* 仅用于强引用等待与已解绑收尾器生命周期。 */
	pthread_mutex_t ref_lock;
	pthread_cond_t ref_cond;
	int deadline_stopping;
	struct tr_reactor_timer_handle deadline_timer;
	int deadline_timer_registered;
	uint32_t *deadline_heap;
	uint32_t deadline_heap_count;
	/* 共享所有者定时器，用于截止时间和最终 STATUS 半关闭重试。 */
	uint64_t final_close_retry_deadline_ns;

	struct tr_channel *channel;
	struct tr_rpc_endpoint_config config;

	struct tr_rpc_method_entry *methods;
	struct tr_rpc_method_index_entry *method_index;
	size_t method_index_capacity;
	uint32_t method_count;
	struct tr_rpc_call_slot *calls;
	uint32_t free_call_head;
	uint32_t *call_by_stream_slot;
	uint32_t stream_slot_capacity;

	struct tr_rpc_executor executor;
	struct tr_refcount refs;
	uint32_t ref_waiters;
	int teardown_detached;
	tr_rpc_endpoint_detached_finalizer detached_finalizer;
	void *detached_finalizer_arg;

	/* 所有者可见的合并状态，用于重试有界待处理任务。 */
	int pending_executor_work;
	int pending_retry_scheduled;
	uint32_t pending_retry_cursor;

	uint64_t stat_calls_started;
	uint64_t stat_calls_completed;
	uint64_t stat_calls_cancelled;
	uint64_t stat_calls_deadline_exceeded;

	struct tr_rpc_semantic_stats semantic;
};

#define TR_RPC_METADATA_RESERVED_TIMEOUT ":timeout-ms"
#define TR_RPC_METADATA_RESERVED_TIMEOUT_LEN 11U
#define TR_RPC_METADATA_TLV_HEADER_SIZE 3U
#define TR_RPC_FINAL_CLOSE_RETRY_NS UINT64_C(25000000)
#define TR_RPC_DEADLINE_METADATA_BYTES     \
	(TR_RPC_METADATA_TLV_HEADER_SIZE + \
	 TR_RPC_METADATA_RESERVED_TIMEOUT_LEN + 8U)

static int tr_rpc_cancel_internal(struct tr_rpc_call_handle handle, int status);
static int tr_rpc_deadline_set_locked(struct tr_rpc_endpoint *endpoint,
				      struct tr_rpc_call_slot *call,
				      uint64_t deadline_ns);
static int tr_rpc_endpoint_get(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_endpoint_put(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_endpoint_release(struct tr_rpc_endpoint *endpoint);
static void tr_rpc_executor_complete_task(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle);
static void tr_rpc_executor_mark_cancelled(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle);
static int tr_rpc_notify_terminal_locked(
	struct tr_rpc_endpoint *endpoint, uint32_t slot,
	struct tr_rpc_call_slot *call, int status);

static uint64_t tr_rpc_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static uint64_t tr_rpc_timeout_deadline_ns(uint32_t timeout_ms)
{
	uint64_t now;
	uint64_t delta;

	if (timeout_ms == 0)
		return 0;
	now = tr_rpc_now_ns();
	/* 0 是时钟读取失败的哨兵，不能当作正常的纪元起点。 */
	if (now == 0U)
		return 0;
	delta = (uint64_t)timeout_ms * UINT64_C(1000000);
	if (UINT64_MAX - now < delta)
		return UINT64_MAX;
	return now + delta;
}

/* 可变 RPC 协议状态由 Channel 所属反应器所有者应用。
 * endpoint->lock 只保护 Call、Method 与协议转换；强引用等待与
 * 已解绑收尾器生命周期已拆到 endpoint->ref_lock。
 *
 * endpoint->lock 仍是过渡锁，直到剩余应用控制平面 API 也迁移到
 * 所有者命令。
 */
static int tr_rpc_owner_call(struct tr_rpc_endpoint *endpoint,
			     int (*fn)(void *arg), void *arg)
{
	struct tr_reactor *reactor;

	if (!endpoint || !fn)
		return TR_ERR_INVALID;

	reactor = tr_channel_reactor(endpoint->channel);
	if (!reactor)
		return TR_ERR_STATE;
	return tr_reactor_call(reactor, fn, arg);
}

static int tr_rpc_metadata_key_valid(const char *key, size_t *len_out)
{
	size_t i;
	size_t len;

	if (!key)
		return 0;
	len = strlen(key);
	if (len == 0 || len > TR_RPC_METADATA_MAX_KEY_LEN || key[0] == ':')
		return 0;

	for (i = 0; i < len; ++i) {
		unsigned char c = (unsigned char)key[i];
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
		      c == '_' || c == '.' || c == '-'))
			return 0;
	}

	if (len_out)
		*len_out = len;
	return 1;
}

static int tr_rpc_metadata_add_raw(uint8_t *dst, uint16_t *len_io,
				   const char *key, size_t key_len,
				   const void *value, uint16_t value_len,
				   int allow_reserved)
{
	uint32_t need;
	uint32_t off;
	uint32_t pos;

	if (!dst || !len_io || !key || key_len == 0 || key_len > UINT8_MAX ||
	    (value_len != 0 && !value))
		return TR_ERR_INVALID;
	if (!allow_reserved && key[0] == ':')
		return TR_ERR_INVALID;

	/* 拒绝重复键，保证元数据查找语义确定且唯一。 */
	off = 0;
	while (off < *len_io) {
		uint8_t old_key_len;
		uint16_t old_value_len;
		uint32_t entry_len;

		if (*len_io - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_STATE;
		old_key_len = dst[off];
		old_value_len = tr_get_le16(dst + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)old_key_len + (uint32_t)old_value_len;
		if (entry_len > (uint32_t)(*len_io - off))
			return TR_ERR_STATE;
		if (old_key_len == key_len &&
		    memcmp(dst + off + TR_RPC_METADATA_TLV_HEADER_SIZE, key,
			   key_len) == 0)
			return TR_ERR_STATE;
		off += entry_len;
	}

	need = TR_RPC_METADATA_TLV_HEADER_SIZE + (uint32_t)key_len + value_len;
	if ((uint32_t)*len_io + need > TR_RPC_METADATA_MAX_BYTES)
		return TR_ERR_BAD_LENGTH;

	pos = *len_io;
	dst[pos] = (uint8_t)key_len;
	tr_put_le16(dst + pos + 1U, value_len);
	memcpy(dst + pos + TR_RPC_METADATA_TLV_HEADER_SIZE, key, key_len);
	if (value_len)
		memcpy(dst + pos + TR_RPC_METADATA_TLV_HEADER_SIZE + key_len,
		       value, value_len);
	*len_io = (uint16_t)(pos + need);
	return TR_OK;
}

static int tr_rpc_metadata_find_raw(const uint8_t *data, uint16_t len,
				    const char *key, size_t key_len,
				    const uint8_t **value, uint16_t *value_len)
{
	uint32_t off = 0;

	if (!data || !key || !value || !value_len)
		return TR_ERR_INVALID;

	while (off < len) {
		uint8_t item_key_len;
		uint16_t item_value_len;
		uint32_t entry_len;
		const uint8_t *item_key;

		if (len - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_BAD_LENGTH;
		item_key_len = data[off];
		item_value_len = tr_get_le16(data + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)item_key_len + item_value_len;
		if (entry_len > (uint32_t)(len - off) || item_key_len == 0)
			return TR_ERR_BAD_LENGTH;
		item_key = data + off + TR_RPC_METADATA_TLV_HEADER_SIZE;
		if (item_key_len == key_len &&
		    memcmp(item_key, key, key_len) == 0) {
			*value = item_key + item_key_len;
			*value_len = item_value_len;
			return TR_OK;
		}
		off += entry_len;
	}
	return TR_ERR_STALE;
}

static int tr_rpc_metadata_validate_raw(const uint8_t *data, uint16_t len)
{
	uint32_t off = 0;

	if (len != 0 && !data)
		return TR_ERR_INVALID;
	while (off < len) {
		uint8_t key_len;
		uint16_t value_len;
		uint32_t entry_len;

		if (len - off < TR_RPC_METADATA_TLV_HEADER_SIZE)
			return TR_ERR_BAD_LENGTH;
		key_len = data[off];
		value_len = tr_get_le16(data + off + 1U);
		entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
			    (uint32_t)key_len + value_len;
		if (key_len == 0 || entry_len > (uint32_t)(len - off))
			return TR_ERR_BAD_LENGTH;
		off += entry_len;
	}
	return TR_OK;
}

static int
tr_rpc_apply_options_locked(struct tr_rpc_endpoint *endpoint,
			    struct tr_rpc_call_slot *call,
			    const struct tr_rpc_call_options *options,
			    uint64_t deadline_ns)
{
	uint16_t i;
	int ret;

	if (!options)
		return TR_OK;

	for (i = 0; i < options->metadata_count; ++i) {
		const struct tr_rpc_metadata *item = &options->metadata[i];
		size_t key_len;

		if (!tr_rpc_metadata_key_valid(item->key, &key_len))
			return TR_ERR_INVALID;
		ret = tr_rpc_metadata_add_raw(call->local_initial_metadata,
					      &call->local_initial_metadata_len,
					      item->key, key_len, item->value,
					      item->value_len, 0);
		if (ret != TR_OK)
			return ret;
	}

	if (options->timeout_ms) {
		if ((uint32_t)call->local_initial_metadata_len +
			    TR_RPC_DEADLINE_METADATA_BYTES >
		    TR_RPC_METADATA_MAX_BYTES)
			return TR_ERR_BAD_LENGTH;
		if (deadline_ns == 0U)
			return TR_ERR_SYS;
		ret = tr_rpc_deadline_set_locked(endpoint, call, deadline_ns);
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

static int tr_rpc_build_outbound_metadata_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call,
	uint16_t wire_type, uint8_t out[TR_RPC_METADATA_MAX_BYTES],
	uint16_t *out_len)
{
	uint16_t len = 0;

	if (!endpoint || !call || !out || !out_len)
		return TR_ERR_INVALID;

	/* 元数据作用范围由 RPC 封装决定：
	 *   首个 REQUEST/RESPONSE -> 初始元数据
	 *   最终 STATUS           -> 尾部元数据
	 */
	if (wire_type == TR_RPC_WIRE_STATUS) {
		len = call->local_trailing_metadata_len;
		if (len)
			memcpy(out, call->local_trailing_metadata, len);
	} else if (call->tx_count == 0 && wire_type != TR_RPC_WIRE_CANCEL) {
		len = call->local_initial_metadata_len;
		if (len)
			memcpy(out, call->local_initial_metadata, len);
	}

	if (endpoint->config.role == TR_RPC_CLIENT &&
	    wire_type == TR_RPC_WIRE_REQUEST && call->tx_count == 0 &&
	    call->deadline_ns != 0) {
		uint64_t now = tr_rpc_now_ns();
		uint64_t remaining_ns;
		uint64_t remaining_ms;
		uint8_t value[8];
		int ret;

		if (now == 0)
			return TR_ERR_SYS;
		remaining_ns =
			call->deadline_ns > now ? call->deadline_ns - now : 0;
		remaining_ms =
			remaining_ns / UINT64_C(1000000) +
			(remaining_ns % UINT64_C(1000000) != 0U);
		if (remaining_ms == 0)
			remaining_ms = 1;
		tr_put_le64(value, remaining_ms);
		ret = tr_rpc_metadata_add_raw(
			out, &len, TR_RPC_METADATA_RESERVED_TIMEOUT,
			TR_RPC_METADATA_RESERVED_TIMEOUT_LEN, value,
			sizeof(value), 1);
		if (ret != TR_OK)
			return ret;
	}

	*out_len = len;
	return TR_OK;
}

static int tr_rpc_import_peer_metadata_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call,
	uint16_t wire_type, const uint8_t *metadata, uint16_t metadata_len)
{
	uint8_t *dst;
	uint16_t *dst_len;
	uint32_t off = 0;
	int ret;

	if (!endpoint || !call)
		return TR_ERR_INVALID;
	ret = tr_rpc_metadata_validate_raw(metadata, metadata_len);
	if (ret != TR_OK)
		return ret;

	if (wire_type == TR_RPC_WIRE_STATUS) {
		dst = call->peer_trailing_metadata;
		dst_len = &call->peer_trailing_metadata_len;
	} else {
		dst = call->peer_initial_metadata;
		dst_len = &call->peer_initial_metadata_len;
	}
	*dst_len = 0;

	while (off < metadata_len) {
		uint8_t key_len = metadata[off];
		uint16_t value_len = tr_get_le16(metadata + off + 1U);
		const uint8_t *key =
			metadata + off + TR_RPC_METADATA_TLV_HEADER_SIZE;
		const uint8_t *value = key + key_len;
		uint32_t entry_len = TR_RPC_METADATA_TLV_HEADER_SIZE +
				     (uint32_t)key_len + value_len;

		if (key_len == TR_RPC_METADATA_RESERVED_TIMEOUT_LEN &&
		    memcmp(key, TR_RPC_METADATA_RESERVED_TIMEOUT,
			   TR_RPC_METADATA_RESERVED_TIMEOUT_LEN) == 0) {
			if (endpoint->config.role != TR_RPC_SERVER ||
			    wire_type != TR_RPC_WIRE_REQUEST || value_len != 8U)
				return TR_ERR_BAD_LENGTH;
			{
				uint64_t timeout_ms = tr_get_le64(value);
				uint64_t now = tr_rpc_now_ns();
				uint64_t deadline_ns;
				uint64_t delta;

				if (timeout_ms == 0)
					return TR_ERR_BAD_LENGTH;
				if (now == 0U)
					return TR_ERR_SYS;
				if (timeout_ms > UINT64_MAX / UINT64_C(1000000))
					deadline_ns = UINT64_MAX;
				else {
					delta = timeout_ms * UINT64_C(1000000);
					deadline_ns =
						UINT64_MAX - now < delta ?
							UINT64_MAX :
							now + delta;
				}
				ret = tr_rpc_deadline_set_locked(
					endpoint, call, deadline_ns);
				if (ret != TR_OK)
					return ret;
			}
		} else {
			uint16_t new_len = *dst_len;
			int add_ret;
			char key_copy[TR_RPC_METADATA_MAX_KEY_LEN + 1U];

			if (key_len > TR_RPC_METADATA_MAX_KEY_LEN)
				return TR_ERR_BAD_LENGTH;
			memcpy(key_copy, key, key_len);
			key_copy[key_len] = '\0';
			if (!tr_rpc_metadata_key_valid(key_copy, NULL))
				return TR_ERR_BAD_LENGTH;
			add_ret = tr_rpc_metadata_add_raw(
				dst, &new_len, key_copy, key_len,
				value, value_len, 0);
			if (add_ret != TR_OK)
				return add_ret;
			*dst_len = new_len;
		}
		off += entry_len;
	}
	return TR_OK;
}

static int tr_rpc_stream_equal(struct tr_stream_handle a,
			       struct tr_stream_handle b)
{
	return a.channel == b.channel && a.slot == b.slot &&
	       a.generation == b.generation;
}

static struct tr_rpc_call_handle
tr_rpc_make_call_handle(struct tr_rpc_endpoint *endpoint, uint32_t slot,
			const struct tr_rpc_call_slot *call)
{
	struct tr_rpc_call_handle handle = { { 0U, 0U } };

	handle._private[0] = (uint64_t)(uintptr_t)endpoint;
	handle._private[1] = ((uint64_t)call->generation << 32) | (uint64_t)slot;
	return handle;
}

static int tr_rpc_deadline_less_locked(const struct tr_rpc_endpoint *endpoint,
				       uint32_t left_slot,
				       uint32_t right_slot)
{
	const struct tr_rpc_call_slot *left = &endpoint->calls[left_slot];
	const struct tr_rpc_call_slot *right = &endpoint->calls[right_slot];

	if (left->deadline_ns != right->deadline_ns)
		return left->deadline_ns < right->deadline_ns;
	return left_slot < right_slot;
}

static void tr_rpc_deadline_heap_swap_locked(struct tr_rpc_endpoint *endpoint,
					      uint32_t left,
					      uint32_t right)
{
	uint32_t left_slot = endpoint->deadline_heap[left];
	uint32_t right_slot = endpoint->deadline_heap[right];

	endpoint->deadline_heap[left] = right_slot;
	endpoint->deadline_heap[right] = left_slot;
	endpoint->calls[right_slot].deadline_heap_pos = left;
	endpoint->calls[left_slot].deadline_heap_pos = right;
}

static void tr_rpc_deadline_sift_up_locked(struct tr_rpc_endpoint *endpoint,
					   uint32_t pos)
{
	while (pos != 0U) {
		uint32_t parent = (pos - 1U) / 2U;

		if (!tr_rpc_deadline_less_locked(
			    endpoint, endpoint->deadline_heap[pos],
			    endpoint->deadline_heap[parent]))
			break;
		tr_rpc_deadline_heap_swap_locked(endpoint, pos, parent);
		pos = parent;
	}
}

static void tr_rpc_deadline_sift_down_locked(struct tr_rpc_endpoint *endpoint,
					     uint32_t pos)
{
	for (;;) {
		uint32_t left = pos * 2U + 1U;
		uint32_t right;
		uint32_t best;

		if (left >= endpoint->deadline_heap_count)
			break;
		right = left + 1U;
		best = left;
		if (right < endpoint->deadline_heap_count &&
		    tr_rpc_deadline_less_locked(
			    endpoint, endpoint->deadline_heap[right],
			    endpoint->deadline_heap[left]))
			best = right;
		if (!tr_rpc_deadline_less_locked(
			    endpoint, endpoint->deadline_heap[best],
			    endpoint->deadline_heap[pos]))
			break;
		tr_rpc_deadline_heap_swap_locked(endpoint, pos, best);
		pos = best;
	}
}

static int tr_rpc_deadline_update_locked(struct tr_rpc_endpoint *endpoint,
					 struct tr_rpc_call_slot *call,
					 uint64_t deadline_ns)
{
	uint32_t slot;
	uint32_t pos;
	uint64_t old_deadline;

	if (!endpoint || !call)
		return TR_ERR_INVALID;
	slot = (uint32_t)(call - endpoint->calls);
	if (slot >= endpoint->config.max_calls)
		return TR_ERR_INVALID;

	pos = call->deadline_heap_pos;
	old_deadline = call->deadline_ns;
	if (deadline_ns == 0U) {
		if (pos != TR_RPC_DEADLINE_HEAP_NONE) {
			uint32_t last_pos;
			uint32_t moved_slot;

			if (pos >= endpoint->deadline_heap_count)
				return TR_ERR_STATE;
			last_pos = --endpoint->deadline_heap_count;
			if (pos != last_pos) {
				moved_slot = endpoint->deadline_heap[last_pos];
				endpoint->deadline_heap[pos] = moved_slot;
				endpoint->calls[moved_slot].deadline_heap_pos = pos;
			}
			call->deadline_heap_pos = TR_RPC_DEADLINE_HEAP_NONE;
			call->deadline_ns = 0U;
			if (pos != last_pos) {
				if (pos != 0U &&
				    tr_rpc_deadline_less_locked(
					    endpoint,
					    endpoint->deadline_heap[pos],
					    endpoint->deadline_heap[
						    (pos - 1U) / 2U]))
					tr_rpc_deadline_sift_up_locked(endpoint,
								      pos);
				else
					tr_rpc_deadline_sift_down_locked(endpoint,
									pos);
			}
			return TR_OK;
		}
		call->deadline_ns = 0U;
		return TR_OK;
	}

	if (pos == TR_RPC_DEADLINE_HEAP_NONE) {
		if (endpoint->deadline_heap_count >= endpoint->config.max_calls)
			return TR_ERR_STATE;
		pos = endpoint->deadline_heap_count++;
		endpoint->deadline_heap[pos] = slot;
		call->deadline_heap_pos = pos;
		call->deadline_ns = deadline_ns;
		tr_rpc_deadline_sift_up_locked(endpoint, pos);
		return TR_OK;
	}
	if (pos >= endpoint->deadline_heap_count ||
	    endpoint->deadline_heap[pos] != slot)
		return TR_ERR_STATE;

	call->deadline_ns = deadline_ns;
	if (deadline_ns < old_deadline)
		tr_rpc_deadline_sift_up_locked(endpoint, pos);
	else if (deadline_ns > old_deadline)
		tr_rpc_deadline_sift_down_locked(endpoint, pos);
	return TR_OK;
}

static int tr_rpc_deadline_rearm_locked(struct tr_rpc_endpoint *endpoint)
{
	uint64_t earliest = 0U;
	uint64_t close_retry = endpoint->final_close_retry_deadline_ns;

	if (endpoint->deadline_stopping ||
	    !endpoint->deadline_timer_registered)
		return TR_ERR_CLOSED;
	if (endpoint->deadline_heap_count != 0U)
		earliest = endpoint->calls[endpoint->deadline_heap[0]].deadline_ns;
	if (close_retry != 0U && (earliest == 0U || close_retry < earliest))
		earliest = close_retry;
	return tr_reactor_timer_arm(endpoint->deadline_timer, earliest);
}

/* 终局控制帧以及取消后的 STREAM_CLOSE 在控制发送背压下
 * 仍是必须履行的协议义务。由现有所有者定时器驱动，
 * 而不是由无界工作线程或客户端刷新驱动。
 */
static int tr_rpc_schedule_final_close_retry_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call)
{
	uint64_t now;
	uint64_t candidate;
	enum tr_lane lane = (enum tr_lane)call->method->desc.lane;
	int ret;
	int abort_ret;

	if (endpoint->deadline_stopping ||
	    !endpoint->deadline_timer_registered) {
		ret = TR_ERR_CLOSED;
		goto fail;
	}
	now = tr_rpc_now_ns();
	if (now == 0U) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	candidate = UINT64_MAX - now < TR_RPC_FINAL_CLOSE_RETRY_NS ?
			UINT64_MAX : now + TR_RPC_FINAL_CLOSE_RETRY_NS;
	if (endpoint->final_close_retry_deadline_ns == 0U ||
	    candidate < endpoint->final_close_retry_deadline_ns)
		endpoint->final_close_retry_deadline_ns = candidate;
	ret = tr_rpc_deadline_rearm_locked(endpoint);
	if (ret == TR_OK)
		return TR_OK;

fail:
	/* STATUS、取消或一元调用失败已经锁存，调用者无法回滚，也不能依赖后续可写通知
	 * 退役流。释放协议锁后同步中止精确通道，通过已有连接错误回调
	 * 收敛义务。这些回调可能退役 Call；返回错误后不得继续原协议转换。
	 */
	pthread_mutex_unlock(&endpoint->lock);
	abort_ret = tr_channel_abort_lane_on_owner(
		endpoint->channel, lane, ret < 0 ? ret : TR_ERR_SYS);
	pthread_mutex_lock(&endpoint->lock);
	return abort_ret == TR_OK || abort_ret == TR_ERR_STALE ? ret : abort_ret;
}

static int tr_rpc_deadline_set_locked(struct tr_rpc_endpoint *endpoint,
				      struct tr_rpc_call_slot *call,
				      uint64_t deadline_ns)
{
	uint64_t old_deadline_ns;
	int ret;

	if (!endpoint || !call)
		return TR_ERR_INVALID;
	old_deadline_ns = call->deadline_ns;
	ret = tr_rpc_deadline_update_locked(endpoint, call, deadline_ns);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_deadline_rearm_locked(endpoint);
	if (ret != TR_OK && deadline_ns != 0U) {
		int rollback_ret;

		/* 新 Call 只在定时器设置也成功后才可发布。否则该 Call
		 * 虽已入堆，却可能永远没有超时回调。旧堆截止时间
		 * 恢复后，先前成功设置的定时器仍负责其原有 Call。
		 */
		rollback_ret = tr_rpc_deadline_update_locked(
			endpoint, call, old_deadline_ns);
		return rollback_ret == TR_OK ? ret : rollback_ret;
	}
	/* 终态删除不能反向复活已完成的 Call。即使重新设置定时失败，
	 * 也继续从堆移除并报告错误；原定时器仍能触发下一轮，
	 * 定时器回调自己使用返回值执行后继重排。
	 */
	return ret;
}

int tr_rpc_deadline_heap_snapshot(struct tr_rpc_endpoint *endpoint,
				  uint32_t *count,
				  struct tr_rpc_call_handle *root,
				  uint64_t *root_deadline_ns)
{
	if (!endpoint || !count)
		return TR_ERR_INVALID;

	if (root)
		memset(root, 0, sizeof(*root));
	if (root_deadline_ns)
		*root_deadline_ns = 0U;

	pthread_mutex_lock(&endpoint->lock);
	*count = endpoint->deadline_heap_count;
	if (endpoint->deadline_heap_count != 0U) {
		uint32_t slot = endpoint->deadline_heap[0];
		struct tr_rpc_call_slot *call;

		if (slot >= endpoint->config.max_calls) {
			pthread_mutex_unlock(&endpoint->lock);
			return TR_ERR_STATE;
		}
		call = &endpoint->calls[slot];
		if (call->deadline_heap_pos != 0U || call->deadline_ns == 0U) {
			pthread_mutex_unlock(&endpoint->lock);
			return TR_ERR_STATE;
		}
		if (root)
			*root = tr_rpc_make_call_handle(endpoint, slot, call);
		if (root_deadline_ns)
			*root_deadline_ns = call->deadline_ns;
	}
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

static size_t tr_rpc_method_index_capacity_for(uint32_t max_methods)
{
	size_t capacity = 1U;
	size_t target;

	if (max_methods == 0U)
		return 0U;
#if SIZE_MAX <= UINT32_MAX
	if (max_methods > (uint32_t)(SIZE_MAX / 2U))
		return 0U;
#endif
	target = (size_t)max_methods * 2U;
	while (capacity < target) {
		if (capacity > SIZE_MAX / 2U)
			return 0U;
		capacity <<= 1U;
	}
	return capacity;
}

static size_t
tr_rpc_method_index_home(const struct tr_rpc_endpoint *endpoint,
			 uint32_t service_id, uint32_t method_id)
{
	return (size_t)tr_rpc_method_hash(service_id, method_id) &
	       (endpoint->method_index_capacity - 1U);
}

static int tr_rpc_method_index_insert_locked(struct tr_rpc_endpoint *endpoint,
					      uint32_t service_id,
					      uint32_t method_id,
					      uint32_t slot)
{
	size_t mask;
	size_t pos;
	size_t probes;

	if (!endpoint || !endpoint->method_index || service_id == 0U ||
	    method_id == 0U || slot >= endpoint->config.max_methods ||
	    endpoint->method_index_capacity == 0U)
		return TR_ERR_INVALID;

	mask = endpoint->method_index_capacity - 1U;
	pos = tr_rpc_method_index_home(endpoint, service_id, method_id);
	for (probes = 0; probes < endpoint->method_index_capacity; ++probes) {
		struct tr_rpc_method_index_entry *entry =
			&endpoint->method_index[pos];

		if (entry->service_id == 0U) {
			entry->service_id = service_id;
			entry->method_id = method_id;
			entry->slot = slot;
			return TR_OK;
		}
		if (entry->service_id == service_id &&
		    entry->method_id == method_id)
			return TR_ERR_STATE;
		pos = (pos + 1U) & mask;
	}
	return TR_AGAIN;
}

static struct tr_rpc_method_entry *
tr_rpc_find_method_locked(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			  uint32_t method_id)
{
	size_t mask;
	size_t pos;
	size_t probes;

	if (!endpoint || !endpoint->method_index || service_id == 0U ||
	    method_id == 0U || endpoint->method_index_capacity == 0U)
		return NULL;

	mask = endpoint->method_index_capacity - 1U;
	pos = tr_rpc_method_index_home(endpoint, service_id, method_id);
	for (probes = 0; probes < endpoint->method_index_capacity; ++probes) {
		struct tr_rpc_method_index_entry *index =
			&endpoint->method_index[pos];
		struct tr_rpc_method_entry *entry;

		if (index->service_id == 0U)
			return NULL;
		if (index->service_id != service_id ||
		    index->method_id != method_id) {
			pos = (pos + 1U) & mask;
			continue;
		}
		if (index->slot >= endpoint->method_count ||
		    index->slot >= endpoint->config.max_methods)
			return NULL;

		entry = &endpoint->methods[index->slot];
		if (!entry->used || entry->desc.service_id != service_id ||
		    entry->desc.method_id != method_id)
			return NULL;
		return entry;
	}
	return NULL;
}

static struct tr_rpc_call_slot *
tr_rpc_find_call_by_stream_locked(struct tr_rpc_endpoint *endpoint,
				  struct tr_stream_handle stream,
				  uint32_t *slot_out)
{
	struct tr_rpc_call_slot *call;
	uint32_t slot;

	if (!endpoint || stream.channel != endpoint->channel ||
	    stream.slot >= endpoint->stream_slot_capacity)
		return NULL;

	slot = endpoint->call_by_stream_slot[stream.slot];
	if (slot == TR_RPC_STREAM_CALL_NONE || slot >= endpoint->config.max_calls)
		return NULL;

	call = &endpoint->calls[slot];
	if (call->state == TR_RPC_CALL_FREE ||
	    !tr_rpc_stream_equal(call->stream, stream))
		return NULL;

	if (slot_out)
		*slot_out = slot;
	return call;
}

static struct tr_rpc_call_slot *
tr_rpc_lookup_call_handle_locked(struct tr_rpc_call_handle handle)
{
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;

	if (!endpoint || tr_rpc_call_handle_slot(handle) >= endpoint->config.max_calls)
		return NULL;

	call = &endpoint->calls[tr_rpc_call_handle_slot(handle)];
	if (call->state == TR_RPC_CALL_FREE ||
	    call->generation != tr_rpc_call_handle_generation(handle))
		return NULL;
	return call;
}

static int tr_rpc_allocate_call_locked(struct tr_rpc_endpoint *endpoint,
				       struct tr_rpc_call_slot **out,
				       uint32_t *slot_out)
{
	struct tr_rpc_call_slot *call;
	uint32_t generation;
	uint32_t slot = endpoint->free_call_head;

	if (slot == TR_RPC_CALL_FREE_NONE)
		return TR_AGAIN;

	call = &endpoint->calls[slot];
	endpoint->free_call_head = call->free_next;
	generation = call->generation + 1U;
	if (generation == 0)
		generation = 1U;

	memset(call, 0, sizeof(*call));
	call->generation = generation;
	call->free_next = TR_RPC_CALL_FREE_NONE;
	call->deadline_heap_pos = TR_RPC_DEADLINE_HEAP_NONE;
	endpoint->stat_calls_started++;
	if (out)
		*out = call;
	if (slot_out)
		*slot_out = slot;
	return TR_OK;
}

static int tr_rpc_bind_call_stream_locked(struct tr_rpc_endpoint *endpoint,
					  uint32_t call_slot,
					  struct tr_stream_handle stream)
{
	if (!endpoint || call_slot >= endpoint->config.max_calls ||
	    stream.channel != endpoint->channel ||
	    stream.slot >= endpoint->stream_slot_capacity)
		return TR_ERR_INVALID;
	if (endpoint->call_by_stream_slot[stream.slot] !=
	    TR_RPC_STREAM_CALL_NONE)
		return TR_ERR_STATE;

	endpoint->call_by_stream_slot[stream.slot] = call_slot;
	return TR_OK;
}

static void tr_rpc_unbind_call_stream_locked(struct tr_rpc_endpoint *endpoint,
					      uint32_t call_slot,
					      struct tr_rpc_call_slot *call)
{
	if (!endpoint || !call || call_slot >= endpoint->config.max_calls)
		return;
	if (call->stream.channel != endpoint->channel ||
	    call->stream.slot >= endpoint->stream_slot_capacity)
		return;
	if (endpoint->call_by_stream_slot[call->stream.slot] == call_slot)
		endpoint->call_by_stream_slot[call->stream.slot] =
			TR_RPC_STREAM_CALL_NONE;
}

static void tr_rpc_mark_local_closed_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call)
{
	uint32_t slot;

	if (!endpoint || !call)
		return;

	call->local_closed = 1;
	if (!call->remote_closed)
		return;

	slot = (uint32_t)(call - endpoint->calls);
	tr_rpc_unbind_call_stream_locked(endpoint, slot, call);
}

static void
tr_rpc_drop_pending_executor_task_locked(struct tr_rpc_call_slot *call)
{
	if (!call || !call->pending_executor_task_valid)
		return;

	if (call->pending_executor_task.payload)
		tr_buffer_release(call->pending_executor_task.payload);
	memset(&call->pending_executor_task, 0,
	       sizeof(call->pending_executor_task));
	call->pending_executor_task_valid = 0;
}

static void tr_rpc_drop_pending_terminal_task_locked(
	struct tr_rpc_call_slot *call)
{
	if (!call || !call->pending_terminal_task_valid)
		return;

	if (call->pending_terminal_task.payload)
		tr_buffer_release(call->pending_terminal_task.payload);
	memset(&call->pending_terminal_task, 0,
	       sizeof(call->pending_terminal_task));
	call->pending_terminal_task_valid = 0;
}

static void tr_rpc_free_call_locked(struct tr_rpc_endpoint *endpoint,
				    struct tr_rpc_call_slot *call)
{
	uint32_t generation;
	uint32_t slot;

	if (!endpoint || !call || call->state == TR_RPC_CALL_FREE)
		return;

	slot = (uint32_t)(call - endpoint->calls);
	generation = call->generation;
	(void)tr_rpc_deadline_update_locked(endpoint, call, 0U);
	tr_rpc_unbind_call_stream_locked(endpoint, slot, call);
	if (call->pending_tx)
		tr_buffer_release(call->pending_tx);
	if (call->pending_control)
		tr_buffer_release(call->pending_control);
	tr_rpc_drop_pending_executor_task_locked(call);
	tr_rpc_drop_pending_terminal_task_locked(call);
	memset(call, 0, sizeof(*call));
	call->generation = generation;
	call->free_next = endpoint->free_call_head;
	call->deadline_heap_pos = TR_RPC_DEADLINE_HEAP_NONE;
	endpoint->free_call_head = slot;
}

static void tr_rpc_maybe_free_call_locked(struct tr_rpc_endpoint *endpoint,
					  struct tr_rpc_call_slot *call)
{
	if (call && call->state == TR_RPC_CALL_TERMINAL &&
	    call->task_refs == 0 && !call->pending_control &&
	    !call->need_local_close && !call->pending_executor_task_valid &&
	    !call->pending_terminal_task_valid)
		tr_rpc_free_call_locked(endpoint, call);
}

static enum tr_rpc_cardinality
tr_rpc_outbound_cardinality(const struct tr_rpc_endpoint *endpoint,
			    const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->request_cardinality :
		       method->response_cardinality;
}

static uint32_t tr_rpc_outbound_codec(const struct tr_rpc_endpoint *endpoint,
				      const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->request_codec_id :
		       method->response_codec_id;
}

static uint32_t tr_rpc_outbound_limit(const struct tr_rpc_endpoint *endpoint,
				      const struct tr_rpc_method_desc *method)
{
	return endpoint->config.role == TR_RPC_CLIENT ?
		       method->max_request_bytes :
		       method->max_response_bytes;
}

static uint16_t
tr_rpc_outbound_wire_type(const struct tr_rpc_endpoint *endpoint)
{
	return endpoint->config.role == TR_RPC_CLIENT ? TR_RPC_WIRE_REQUEST :
							TR_RPC_WIRE_RESPONSE;
}

static int tr_rpc_validate_cardinality(enum tr_rpc_cardinality cardinality,
				       uint32_t count)
{
	if (cardinality == TR_RPC_NONE)
		return TR_ERR_STATE;
	if (cardinality == TR_RPC_ONE && count != 0)
		return TR_ERR_STATE;
	if (cardinality != TR_RPC_ONE && cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	return TR_OK;
}

static int tr_rpc_status_valid(int status)
{
	return status >= TR_RPC_STATUS_OK &&
	       status <= TR_RPC_STATUS_UNAUTHENTICATED;
}

static void tr_rpc_semantic_start_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call)
{
	if (!endpoint || !call || call->semantic_started)
		return;

	call->semantic_started = 1;
	endpoint->semantic.calls_started++;
}

static void tr_rpc_semantic_finish_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call,
	int status)
{
	if (!endpoint || !call || !call->semantic_started ||
	    call->semantic_finished)
		return;

	if (!tr_rpc_status_valid(status))
		status = TR_RPC_STATUS_INTERNAL;

	call->semantic_finished = 1;
	endpoint->semantic.calls_finished++;
	endpoint->semantic.final_status[(uint32_t)status]++;
}

#define TR_RPC_INTERCEPTOR_CLIENT_PRE_BIT  (1U << 0)
#define TR_RPC_INTERCEPTOR_SERVER_PRE_BIT  (1U << 1)
#define TR_RPC_INTERCEPTOR_SERVER_POST_BIT (1U << 2)
#define TR_RPC_INTERCEPTOR_CLIENT_POST_BIT (1U << 3)

static uint32_t
tr_rpc_interceptor_phase_bit(enum tr_rpc_interceptor_phase phase)
{
	switch (phase) {
	case TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL:
		return TR_RPC_INTERCEPTOR_CLIENT_PRE_BIT;
	case TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER:
		return TR_RPC_INTERCEPTOR_SERVER_PRE_BIT;
	case TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER:
		return TR_RPC_INTERCEPTOR_SERVER_POST_BIT;
	case TR_RPC_INTERCEPTOR_CLIENT_POST_CALL:
		return TR_RPC_INTERCEPTOR_CLIENT_POST_BIT;
	default:
		return 0U;
	}
}

/* endpoint->lock 必须已持有，且调用者必须位于反应器所有者。
 *
 * 钩子执行前临时释放协议锁，使钩子可以安全调用
 * tr_rpc_call_get_context()/metadata API。所有者线程在钩子返回前不会处理
 * 其他反应器事件，因此正常协议状态仍由同一所有者串行处理。
 *
 * V1 只允许 SERVER_PRE_HANDLER 的返回值拒绝 Call；其他阶段的返回值忽略。
 */
static int tr_rpc_run_interceptor_locked(
	struct tr_rpc_endpoint *endpoint, uint32_t slot,
	struct tr_rpc_call_slot *call, enum tr_rpc_interceptor_phase phase,
	int status, int *hook_status)
{
	tr_rpc_interceptor_fn fn;
	void *fn_arg;
	struct tr_rpc_call_handle handle;
	uint32_t bit;
	uint32_t generation;
	int result = TR_RPC_STATUS_OK;

	if (hook_status)
		*hook_status = TR_RPC_STATUS_OK;
	if (!endpoint || !call || slot >= endpoint->config.max_calls)
		return TR_ERR_INVALID;

	fn = endpoint->config.interceptor.fn;
	if (!fn)
		return TR_OK;

	bit = tr_rpc_interceptor_phase_bit(phase);
	if (bit == 0U)
		return TR_ERR_INVALID;
	if ((call->interceptor_mask & bit) != 0U)
		return TR_OK;

	generation = call->generation;
	handle = tr_rpc_make_call_handle(endpoint, slot, call);
	fn_arg = endpoint->config.interceptor.arg;
	call->interceptor_mask |= bit;
	call->interceptor_active = 1;

	pthread_mutex_unlock(&endpoint->lock);
	result = fn(handle, phase, status, fn_arg);
	pthread_mutex_lock(&endpoint->lock);

	call = &endpoint->calls[slot];
	if (call->state == TR_RPC_CALL_FREE ||
	    call->generation != generation)
		return TR_ERR_STALE;
	call->interceptor_active = 0;

	if (phase == TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER) {
		if (!tr_rpc_status_valid(result))
			result = TR_RPC_STATUS_INTERNAL;
		if (hook_status)
			*hook_status = result;
	}
	return TR_OK;
}

/* 流式调用的最终 STATUS 是应用终止屏障。
 *
 * 对端必须标识与先前 RESPONSE 帧相同的 Method 与编解码器。
 * 成功的 ONE 响应要求 STATUS(OK) 之前恰好到达一个 RESPONSE。
 * 错误 STATUS 可以在产生预期响应之前终止。
 */
static int tr_rpc_validate_stream_status_locked(
	const struct tr_rpc_call_slot *call,
	const struct tr_rpc_wire_header *wire)
{
	const struct tr_rpc_method_desc *method;

	if (!call || !call->method || !wire || call->is_unary ||
	    call->final_status_seen || wire->payload_len != 0)
		return TR_ERR_STATE;

	method = &call->method->desc;
	if (wire->service_id != method->service_id ||
	    wire->method_id != method->method_id ||
	    wire->codec_id != method->response_codec_id ||
	    !tr_rpc_status_valid(wire->status))
		return TR_ERR_STATE;

	if (wire->status == TR_RPC_STATUS_OK &&
	    method->response_cardinality == TR_RPC_ONE &&
	    call->rx_count != 1U)
		return TR_ERR_STATE;

	return TR_OK;
}

static int tr_rpc_encode_message(struct tr_rpc_endpoint *endpoint,
				 struct tr_rpc_call_slot *call, uint16_t type,
				 const struct tr_rpc_method_desc *method,
				 uint32_t codec_id, int status,
				 const struct tr_rpc_bytes *message,
				 struct tr_buffer **out)
{
	struct tr_rpc_wire_header header;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t metadata_len = 0;
	uint32_t encoded = 0;
	uint32_t metadata_wire_len = 0;
	uint32_t payload_off;
	uint32_t total;
	int ret;

	if (!endpoint || !call || !method || !message || !out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (codec_id != TR_RPC_CODEC_RAW)
		return TR_ERR_BAD_TYPE;
	ret = tr_rpc_build_outbound_metadata_locked(endpoint, call, type,
						    metadata, &metadata_len);
	if (ret != TR_OK)
		return ret;
	if (metadata_len)
		metadata_wire_len =
			TR_RPC_WIRE_METADATA_PREFIX_SIZE + metadata_len;
	if (message->len >
	    UINT32_MAX - TR_RPC_WIRE_HEADER_SIZE - metadata_wire_len)
		return TR_ERR_BAD_LENGTH;
	total = TR_RPC_WIRE_HEADER_SIZE + metadata_wire_len + message->len;

	ret = tr_buffer_acquire(endpoint->config.message_pool, total, &buffer);
	if (ret != TR_OK)
		return ret;

	memset(&header, 0, sizeof(header));
	header.version = TR_RPC_WIRE_VERSION;
	header.type = type;
	header.service_id = method->service_id;
	header.method_id = method->method_id;
	header.codec_id = codec_id;
	header.flags = metadata_len ? TR_RPC_WIRE_F_METADATA : 0;
	header.status = status;
	header.payload_len = message->len;

	ret = tr_rpc_wire_encode(buffer->data, &header);
	if (ret != TR_OK)
		return ret;

	payload_off = TR_RPC_WIRE_HEADER_SIZE;
	if (metadata_len) {
		tr_put_le16(buffer->data + payload_off, metadata_len);
		memcpy(buffer->data + payload_off +
			       TR_RPC_WIRE_METADATA_PREFIX_SIZE,
		       metadata, metadata_len);
		payload_off += metadata_wire_len;
	}

	ret = tr_rpc_raw_encode(message, buffer->data + payload_off,
				buffer->capacity - payload_off, &encoded);
	if (ret != TR_OK || encoded != message->len) {
		if (ret == TR_OK)
			ret = TR_ERR_STATE;
		return ret;
	}

	buffer->len = total;
	*out = tr_buffer_take(&buffer);
	return TR_OK;
}

static int tr_rpc_encode_header_buffer(struct tr_rpc_endpoint *endpoint,
				       struct tr_rpc_call_slot *call,
				       uint16_t type,
				       const struct tr_rpc_method_desc *method,
				       uint32_t codec_id, int status,
				       uint32_t payload_len,
				       struct tr_buffer **out)
{
	struct tr_rpc_wire_header header;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	uint16_t metadata_len = 0;
	uint32_t metadata_wire_len = 0;
	uint32_t total;
	int ret;

	if (!endpoint || !call || !method || !out)
		return TR_ERR_INVALID;
	*out = NULL;

	ret = tr_rpc_build_outbound_metadata_locked(endpoint, call, type,
						    metadata, &metadata_len);
	if (ret != TR_OK)
		return ret;
	if (metadata_len)
		metadata_wire_len =
			TR_RPC_WIRE_METADATA_PREFIX_SIZE + metadata_len;
	total = TR_RPC_WIRE_HEADER_SIZE + metadata_wire_len;

	ret = tr_buffer_acquire(endpoint->config.message_pool, total, &buffer);
	if (ret != TR_OK)
		return ret;

	memset(&header, 0, sizeof(header));
	header.version = TR_RPC_WIRE_VERSION;
	header.type = type;
	header.service_id = method->service_id;
	header.method_id = method->method_id;
	header.codec_id = codec_id;
	header.flags = metadata_len ? TR_RPC_WIRE_F_METADATA : 0;
	header.status = status;
	header.payload_len = payload_len;

	ret = tr_rpc_wire_encode(buffer->data, &header);
	if (ret != TR_OK)
		return ret;

	if (metadata_len) {
		tr_put_le16(buffer->data + TR_RPC_WIRE_HEADER_SIZE,
			    metadata_len);
		memcpy(buffer->data + TR_RPC_WIRE_HEADER_SIZE +
			       TR_RPC_WIRE_METADATA_PREFIX_SIZE,
		       metadata, metadata_len);
	}

	buffer->len = total;
	*out = tr_buffer_take(&buffer);
	return TR_OK;
}

static int tr_rpc_encode_control_locked(struct tr_rpc_endpoint *endpoint,
					struct tr_rpc_call_slot *call,
					uint16_t type, int status,
					struct tr_buffer **out)
{
	struct tr_rpc_method_desc method;

	if (!endpoint || !call || !call->method || !out)
		return TR_ERR_INVALID;
	method = call->method->desc;
	return tr_rpc_encode_header_buffer(endpoint, call, type, &method, 0,
					   status, 0, out);
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_try_unary_send_locked(struct tr_rpc_endpoint *endpoint,
					struct tr_rpc_call_slot *call)
{
	int ret = TR_OK;

	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL)
		return TR_ERR_CLOSED;
	if (call->pending_tx) {
		ret = tr_stream_send(call->stream, call->pending_tx);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&call->pending_tx);
			call->tx_count++;
			call->need_local_close = 1;
			call->state = TR_RPC_CALL_ACTIVE;
			if (endpoint->config.role == TR_RPC_SERVER)
				(void)tr_rpc_deadline_set_locked(endpoint, call,
							      0U);
		} else {
			return ret;
		}
	}

	if (call->need_local_close) {
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			call->need_local_close = 0;
			tr_rpc_mark_local_closed_locked(endpoint, call);
			if (endpoint->config.role == TR_RPC_SERVER &&
			    call->remote_closed) {
				call->state = TR_RPC_CALL_TERMINAL;
				(void)tr_rpc_deadline_set_locked(endpoint, call,
							      0U);
				tr_rpc_maybe_free_call_locked(endpoint, call);
			}
			return TR_OK;
		}
		if (ret == TR_AGAIN && endpoint->config.role == TR_RPC_SERVER)
			return tr_rpc_schedule_final_close_retry_locked(endpoint, call);
	}
	return ret;
}

/* 服务端一元调用过载是协议级拒绝，不是连接失败。
 * 执行器入队返回 TR_AGAIN 时请求处理函数尚未运行，
 * 因此可安全地立即返回 RESOURCE_EXHAUSTED。
 *
 * 反应器所有者线程必须已经持有 endpoint->lock。
 */
static int tr_rpc_reject_unary_locked(struct tr_rpc_endpoint *endpoint,
				      struct tr_rpc_call_slot *call,
				      int status)
{
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty = { NULL, 0 };
	int ret;

	if (!endpoint || !call || !call->method || !call->is_unary ||
	    endpoint->config.role != TR_RPC_SERVER)
		return TR_ERR_INVALID;

	ret = tr_rpc_encode_message(
		endpoint, call, TR_RPC_WIRE_RESPONSE, &call->method->desc,
		call->method->desc.response_codec_id, status, &empty, &encoded);
	if (ret != TR_OK)
		return ret;

	call->pending_tx = tr_buffer_take(&encoded);
	(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
	tr_rpc_semantic_finish_locked(endpoint, call, status);

	ret = tr_rpc_try_unary_send_locked(endpoint, call);
	/* TR_AGAIN 表示为现有 WRITABLE 或刷新重试路径保留 pending_tx。
	 * 过载决策本身已经被接受。
	 */
	return ret == TR_AGAIN ? TR_OK : ret;
}

/* 已经执行的服务端一元调用绝不能在响应分配或编码失败后消失。
 * 优先使用空 INTERNAL RESPONSE；若连它也无法分配，
 * 则半关闭流，使客户端观察到 UNAVAILABLE，
 * 而不是等待永远不会到来的结果。
 *
 * 此回退自身不做堆分配，始终由反应器所有者在持有 endpoint->lock 时应用。
 * 重试受现有每 Call 状态与共享 RPC 所有者定时器限制。
 */
static int tr_rpc_fail_server_unary_locked(struct tr_rpc_endpoint *endpoint,
					    struct tr_rpc_call_slot *call)
{
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty = { NULL, 0 };
	int ret;

	if (!endpoint || !call || !call->method || !call->is_unary ||
	    endpoint->config.role != TR_RPC_SERVER)
		return TR_ERR_INVALID;
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL ||
	    call->unary_failure_pending || call->pending_tx || call->tx_count != 0U)
		return TR_OK;

	call->unary_failure_pending = 1;
	call->final_status = TR_RPC_STATUS_INTERNAL;
	tr_rpc_semantic_finish_locked(endpoint, call, TR_RPC_STATUS_INTERNAL);
	(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);

	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_RESPONSE,
				    &call->method->desc,
				    call->method->desc.response_codec_id,
				    TR_RPC_STATUS_INTERNAL, &empty, &encoded);
	if (ret == TR_OK)
		call->pending_tx = tr_buffer_take(&encoded);
	else
		call->need_local_close = 1;

	ret = tr_rpc_try_unary_send_locked(endpoint, call);
	/* 重试调度失败可能已经同步退役通道或 Call。 */
	if (call->state == TR_RPC_CALL_FREE || call->state == TR_RPC_CALL_TERMINAL)
		return ret;
	if (ret == TR_OK)
		return TR_OK;
	if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE) {
		call->need_local_close = 0;
		call->state = TR_RPC_CALL_TERMINAL;
		tr_rpc_maybe_free_call_locked(endpoint, call);
		return TR_OK;
	}
	if (ret == TR_AGAIN)
		return tr_rpc_schedule_final_close_retry_locked(endpoint, call);

	/* 如果错误封装因非背压原因无法获准，则不要保留永远无法推进的响应：
	 * 降级为不带头部的半关闭。它自身的 TR_AGAIN 也使用定时器。
	 */
	if (call->pending_tx)
		tr_buffer_release(tr_buffer_take(&call->pending_tx));
	call->need_local_close = 1;
	ret = tr_rpc_try_unary_send_locked(endpoint, call);
	/* 重试调度失败可能已经同步退役通道或 Call。 */
	if (call->state == TR_RPC_CALL_FREE || call->state == TR_RPC_CALL_TERMINAL)
		return ret;
	if (ret == TR_OK)
		return TR_OK;
	if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE) {
		call->need_local_close = 0;
		call->state = TR_RPC_CALL_TERMINAL;
		tr_rpc_maybe_free_call_locked(endpoint, call);
		return TR_OK;
	}
	return tr_rpc_schedule_final_close_retry_locked(endpoint, call);
}

/* 无法进入有界服务端执行器的首个流式消息仍属于准入失败：
 * 尚未运行任何应用回调。返回最终 RESOURCE_EXHAUSTED 以保留该区别，
 * 而不是把执行器压力变成传输或连接失败。
 *
 * 反应器所有者必须已经持有 endpoint->lock。
 */
static int
tr_rpc_reject_stream_admission_locked(struct tr_rpc_endpoint *endpoint,
				      struct tr_rpc_call_slot *call,
				      int status)
{
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty = { NULL, 0 };
	struct tr_rpc_method_desc method;
	int ret;

	if (!endpoint || !call || !call->method || call->is_unary ||
	    endpoint->config.role != TR_RPC_SERVER || call->rx_count != 1U ||
	    call->task_refs != 0U || call->tx_count != 0U ||
	    call->admission_rejected || call->final_status_sent)
		return TR_ERR_STATE;

	method = call->method->desc;
	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_STATUS, &method,
				    method.response_codec_id, status, &empty,
				    &encoded);
	if (ret != TR_OK)
		return ret;

	call->admission_rejected = 1;
	call->final_status = status;
	(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
	endpoint->stat_calls_completed++;
	tr_rpc_semantic_finish_locked(endpoint, call, status);

	ret = tr_stream_send(call->stream, encoded);
	if (ret == TR_OK) {
		(void)tr_buffer_take(&encoded);
		call->final_status_sent = 1;
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			tr_rpc_mark_local_closed_locked(endpoint, call);
			call->need_local_close = 0;
			ret = TR_OK;
		} else if (ret == TR_AGAIN) {
			call->need_local_close = 1;
			ret = tr_rpc_schedule_final_close_retry_locked(endpoint, call);
		}
	} else if (ret == TR_AGAIN) {
		call->pending_control = tr_buffer_take(&encoded);
		call->pending_control_is_final_status = 1;
		ret = TR_OK;
	}
	return ret;
}

/* 一旦应用回调可能已运行，过载就不是准入拒绝。
 * 停止接纳此 Call 的后续回调，返回最终 RESOURCE_EXHAUSTED 状态，
 * 并保持物理连接存活。丢弃至多一个待处理接收任务，不归还流额度：
 * 流正在终止，因此其流控状态将退役。
 */
static int
tr_rpc_fail_stream_midstream_overload_locked(
	struct tr_rpc_endpoint *endpoint, uint32_t slot,
	struct tr_rpc_call_slot *call, int status)
{
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty = { NULL, 0 };
	struct tr_rpc_call_handle handle;
	struct tr_rpc_method_desc method;
	int ret;

	if (!endpoint || !call || !call->method || call->is_unary ||
	    endpoint->config.role != TR_RPC_SERVER ||
	    call->admission_rejected || call->executor_overloaded ||
	    call->final_status_sent || call->pending_control)
		return TR_ERR_STATE;

	call->executor_overloaded = 1;
	call->pending_remote_half_close = 0;
	tr_rpc_drop_pending_executor_task_locked(call);
	handle = tr_rpc_make_call_handle(endpoint, slot, call);
	tr_rpc_executor_mark_cancelled(endpoint, handle);

	method = call->method->desc;
	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_STATUS, &method,
				    method.response_codec_id, status, &empty,
				    &encoded);
	if (ret != TR_OK)
		return ret;

	call->final_status = status;
	(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);

	/* 有界延续任务准入一旦耗尽，应用 Call 即进入终态。
	 * 现在就把 on_close 排在已获准任务之后，而不是依赖哪一侧 TCP
	 * 半关闭最后到达。 保持协议 Call 槽位存活，直到流本身终止，
	 * 使并发到达的对端数据仍被识别为过载的后续影响。
	 */
	(void)tr_rpc_notify_terminal_locked(endpoint, slot, call, status);

	ret = tr_stream_send(call->stream, encoded);
	if (ret == TR_OK) {
		(void)tr_buffer_take(&encoded);
		call->final_status_sent = 1;
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			tr_rpc_mark_local_closed_locked(endpoint, call);
			call->need_local_close = 0;
			ret = TR_OK;
		} else if (ret == TR_AGAIN) {
			call->need_local_close = 1;
			ret = tr_rpc_schedule_final_close_retry_locked(endpoint, call);
		}
	} else if (ret == TR_AGAIN) {
		call->pending_control = tr_buffer_take(&encoded);
		call->pending_control_is_final_status = 1;
		ret = TR_OK;
	}
	return ret;
}

static int tr_rpc_executor_ready_push_locked(struct tr_rpc_executor *executor,
					     uint32_t slot)
{
	if (executor->ready_count == executor->ready_capacity)
		return TR_ERR_STATE;

	executor->ready_calls[executor->ready_tail] = slot;
	executor->ready_tail =
		(executor->ready_tail + 1U) % executor->ready_capacity;
	executor->ready_count++;
	tr_observe_high_water_u32(&executor->ready_peak, executor->ready_count);
	return TR_OK;
}

static void
tr_rpc_executor_ready_undo_push_locked(struct tr_rpc_executor *executor,
				       uint32_t slot)
{
	uint32_t tail;

	if (executor->ready_count == 0 || executor->ready_capacity == 0)
		return;

	tail = (executor->ready_tail + executor->ready_capacity - 1U) %
	       executor->ready_capacity;
	if (executor->ready_calls[tail] != slot)
		return;

	executor->ready_tail = tail;
	executor->ready_count--;
}

static int tr_rpc_executor_group_task_accept(
	struct tr_rpc_executor_group *group)
{
	int ret = TR_OK;

	if (!group)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&group->lock);
	if (!group->accepting || group->stopping) {
		ret = TR_ERR_CLOSED;
	} else if (group->outstanding_tasks == UINT64_MAX) {
		ret = TR_ERR_STATE;
	} else {
		group->outstanding_tasks++;
	}
	pthread_mutex_unlock(&group->lock);
	return ret;
}

static void tr_rpc_executor_group_task_done(
	struct tr_rpc_executor_group *group)
{
	if (!group)
		return;

	pthread_mutex_lock(&group->lock);
#ifndef NDEBUG
	assert(group->outstanding_tasks != 0U);
#endif
	if (group->outstanding_tasks != 0U)
		group->outstanding_tasks--;
	pthread_cond_broadcast(&group->cond);
	pthread_mutex_unlock(&group->lock);
}

static int tr_rpc_executor_group_enqueue(struct tr_rpc_executor_group *group,
					 struct tr_rpc_endpoint *endpoint)
{
	int ret = TR_OK;

	if (!group || !endpoint)
		return TR_ERR_INVALID;

		/* 调用者持有 endpoint->executor.lock，而 group_ready_next
		 * 仅由 group->lock 保护。group_enqueued 防止重复发布。
		 */
#ifndef NDEBUG
	assert(!endpoint->executor.group_enqueued);
#endif
	pthread_mutex_lock(&group->lock);
	/* 停机只拒绝新任务；已有强引用任务仍可发布内部调度令牌。 */
	if (group->stopping && group->outstanding_tasks == 0U) {
		ret = TR_ERR_CLOSED;
	} else {
		endpoint->executor.group_ready_next = NULL;
		if (group->ready_tail)
			group->ready_tail->executor.group_ready_next = endpoint;
		else
			group->ready_head = endpoint;
		group->ready_tail = endpoint;
		group->ready_count++;
		pthread_cond_signal(&group->cond);
	}
	pthread_mutex_unlock(&group->lock);
	return ret;
}

static int
tr_rpc_executor_task_is_admission(const struct tr_rpc_endpoint *endpoint,
				  const struct tr_rpc_task *task)
{
	if (!endpoint || !task || endpoint->config.role != TR_RPC_SERVER)
		return 0;

	if (task->type == TR_RPC_TASK_SERVER_UNARY)
		return 1;
	return task->type == TR_RPC_TASK_SERVER_STREAM_MESSAGE &&
	       task->first_message != 0;
}

static int tr_rpc_executor_push(struct tr_rpc_endpoint *endpoint,
				const struct tr_rpc_task *task)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	struct tr_rpc_executor_callq *callq;
	struct tr_rpc_executor_node *node;
	uint32_t node_index;
	int admission;
	int group_task_accepted = 0;
	int ret = TR_OK;

	if (!task || tr_rpc_call_handle_slot(task->call) >= endpoint->config.max_calls)
		return TR_ERR_INVALID;
	admission = tr_rpc_executor_task_is_admission(endpoint, task);

	pthread_mutex_lock(&executor->lock);
	if (executor->stopping) {
		ret = TR_ERR_CLOSED;
		goto out;
	}
	if (admission && executor->continuation_reserve != 0 &&
	    executor->queued_count >=
		    executor->capacity - executor->continuation_reserve) {
		/* 将最后的预留节点留给已跨过准入的 Call 所属工作。
		 * 现有首任务拒绝路径会把此 TR_AGAIN 转成 RESOURCE_EXHAUSTED。
		 */
		executor->admission_limit_hits++;
		ret = TR_AGAIN;
		goto out;
	}
	if (executor->free_head == TR_RPC_EXEC_NONE) {
		executor->hard_full_events++;
		ret = TR_AGAIN;
		goto out;
	}

	callq = &executor->callq[tr_rpc_call_handle_slot(task->call)];
	if (callq->generation != tr_rpc_call_handle_generation(task->call)) {
		if (callq->running || callq->ready ||
		    callq->head != TR_RPC_EXEC_NONE) {
			ret = TR_ERR_STATE;
			goto out;
		}
		callq->generation = tr_rpc_call_handle_generation(task->call);
		callq->head = TR_RPC_EXEC_NONE;
		callq->tail = TR_RPC_EXEC_NONE;
		callq->queued_count = 0;
		callq->cancelled = 0;
	}
	if (executor->group) {
		ret = tr_rpc_executor_group_task_accept(executor->group);
		if (ret != TR_OK)
			goto out;
		group_task_accepted = 1;
	}

	node_index = executor->free_head;
	node = &executor->nodes[node_index];
	executor->free_head = node->next;
	node->task = *task;
	node->enqueued_ns =
		(endpoint->config.observability_flags & TR_OBSERVABILITY_TIMING) ?
			tr_rpc_now_ns() : 0;
	node->next = TR_RPC_EXEC_NONE;

	if (callq->tail != TR_RPC_EXEC_NONE)
		executor->nodes[callq->tail].next = node_index;
	else
		callq->head = node_index;
	callq->tail = node_index;
	callq->queued_count++;
	executor->queued_count++;

	if (!callq->running && !callq->ready) {
		ret = tr_rpc_executor_ready_push_locked(executor,
							tr_rpc_call_handle_slot(task->call));
		if (ret != TR_OK) {
			uint32_t cur = callq->head;
			uint32_t prev = TR_RPC_EXEC_NONE;

			while (cur != TR_RPC_EXEC_NONE && cur != node_index) {
				prev = cur;
				cur = executor->nodes[cur].next;
			}
			if (cur == node_index) {
				if (prev != TR_RPC_EXEC_NONE)
					executor->nodes[prev].next =
						TR_RPC_EXEC_NONE;
				else
					callq->head = TR_RPC_EXEC_NONE;
				callq->tail = prev;
			}
			node->next = executor->free_head;
			executor->free_head = node_index;
			if (callq->queued_count != 0)
				callq->queued_count--;
			executor->queued_count--;
			if (group_task_accepted)
				tr_rpc_executor_group_task_done(executor->group);
			goto out;
		}
		callq->ready = 1;
		if (executor->group) {
			if (!executor->group_enqueued) {
				ret = tr_rpc_executor_group_enqueue(
					executor->group, endpoint);
				if (ret != TR_OK) {
					callq->ready = 0;
					tr_rpc_executor_ready_undo_push_locked(
						executor, tr_rpc_call_handle_slot(task->call));
					{
						uint32_t cur = callq->head;
						uint32_t prev = TR_RPC_EXEC_NONE;

						while (cur != TR_RPC_EXEC_NONE &&
						       cur != node_index) {
							prev = cur;
							cur = executor->nodes[cur].next;
						}
						if (cur == node_index) {
							if (prev != TR_RPC_EXEC_NONE)
								executor->nodes[prev].next =
									TR_RPC_EXEC_NONE;
							else
								callq->head =
									TR_RPC_EXEC_NONE;
							callq->tail = prev;
						}
					}
					node->next = executor->free_head;
					executor->free_head = node_index;
					if (callq->queued_count != 0)
						callq->queued_count--;
					if (executor->queued_count != 0)
						executor->queued_count--;
					if (group_task_accepted)
						tr_rpc_executor_group_task_done(
							executor->group);
					goto out;
				}
				executor->group_enqueued = 1;
			}
		} else {
			pthread_cond_signal(&executor->cond);
		}
	}

	if (ret == TR_OK) {
		executor->enqueued_tasks++;
		tr_observe_high_water_u32(&executor->queued_peak,
					  executor->queued_count);
	}

out:
	pthread_mutex_unlock(&executor->lock);
	return ret;
}

/* endpoint->lock 必须已经持有。
 *
 * 任务在入队前复制工作线程所需的全部回调、处理函数与流能力凭证。
 * 入队之后工作线程不再回读可变 Call/Endpoint 协议状态。
 */
static int tr_rpc_prepare_task_snapshot_locked(
	struct tr_rpc_call_slot *call, struct tr_rpc_task *task)
{
	if (!call || !task)
		return TR_ERR_INVALID;

	task->stream = call->stream;

	switch (task->type) {
	case TR_RPC_TASK_SERVER_UNARY:
		if (!call->method ||
		    call->method->handler_kind != TR_RPC_HANDLER_UNARY)
			return TR_ERR_STATE;
		task->u.server_unary.handler = call->method->unary_handler;
		task->u.server_unary.handler_arg = call->method->handler_arg;
		task->u.server_unary.method = call->method->desc;
		break;

	case TR_RPC_TASK_SERVER_STREAM_MESSAGE:
	case TR_RPC_TASK_SERVER_HALF_CLOSE:
	case TR_RPC_TASK_SERVER_WRITABLE:
	case TR_RPC_TASK_SERVER_CLOSE:
		if (!call->method ||
		    call->method->handler_kind != TR_RPC_HANDLER_STREAM)
			return TR_ERR_STATE;
		task->u.server_stream.handlers = call->method->stream_handlers;
		task->u.server_stream.handler_arg = call->method->handler_arg;
		break;

	case TR_RPC_TASK_CLIENT_UNARY_RESULT:
		task->u.client_unary.result_cb = call->result_cb;
		task->u.client_unary.result_arg = call->result_arg;
		break;

	case TR_RPC_TASK_CLIENT_MESSAGE:
	case TR_RPC_TASK_CLIENT_EVENT:
		task->u.client_stream.callbacks = call->callbacks;
		break;

	default:
		return TR_ERR_INVALID;
	}

	return TR_OK;
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_queue_task_locked(struct tr_rpc_endpoint *endpoint,
				    struct tr_rpc_call_slot *call,
				    const struct tr_rpc_task *task)
{
	struct tr_rpc_task snapshot;
	int ret;

	if (!task)
		return TR_ERR_INVALID;
	snapshot = *task;
	ret = tr_rpc_prepare_task_snapshot_locked(call, &snapshot);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_endpoint_get(endpoint);
	if (ret != TR_OK)
		return ret;

	call->task_refs++;
	ret = tr_rpc_executor_push(endpoint, &snapshot);
	if (ret != TR_OK) {
		call->task_refs--;
		/* 当前仍持有 endpoint->lock，且所有者引用仍然存活，
		 * 因此这里不会成为最后一次 put；仍必须走统一生命周期包装函数，
		 * 让可能等待 refs==1 的析构函数得到 ref_cond 唤醒。
		 */
		tr_rpc_endpoint_put(endpoint);
	}
	return ret;
}

/* 终止回调不能依赖 Call 终止时的执行器容量。发生背压时，
 * 在 Call 自身保留一个已快照的义务；执行器的节点取出通知会重试它。
 * Call 槽位由 pending_terminal_task_valid 保持生命周期保护，
 * 直到准入将所有权转移给执行器，或 Endpoint 被显式拆除。
 * 调用者在反应器所有者上持有 endpoint->lock。
 */
static int tr_rpc_queue_terminal_task_locked(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_slot *call,
	const struct tr_rpc_task *task)
{
	struct tr_rpc_task snapshot;
	int ret;

	if (!endpoint || !call || !task)
		return TR_ERR_INVALID;
	if (call->pending_terminal_task_valid)
		return TR_ERR_STATE;

	ret = tr_rpc_queue_task_locked(endpoint, call, task);
	if (ret != TR_AGAIN)
		return ret;

	snapshot = *task;
	ret = tr_rpc_prepare_task_snapshot_locked(call, &snapshot);
	if (ret != TR_OK)
		return ret;

	call->pending_terminal_task = snapshot;
	call->pending_terminal_task_valid = 1;
	endpoint->pending_executor_work = 1;
	return TR_OK;
}

/* 在有界执行器外为每个 Call 精确保留一个任务。
 * 任务快照拥有接收负载，直到获准或 Call 终止。
 * 必须已经持有 endpoint->lock。
 */
static int
tr_rpc_store_pending_executor_task_locked(struct tr_rpc_endpoint *endpoint,
					  struct tr_rpc_call_slot *call,
					  const struct tr_rpc_task *task)
{
	struct tr_rpc_task snapshot;
	int ret;

	if (!endpoint || !call || !task)
		return TR_ERR_INVALID;
	if (call->pending_executor_task_valid)
		return TR_AGAIN;

	snapshot = *task;
	ret = tr_rpc_prepare_task_snapshot_locked(call, &snapshot);
	if (ret != TR_OK)
		return ret;

	call->pending_executor_task = snapshot;
	call->pending_executor_task_valid = 1;
	endpoint->pending_executor_work = 1;
	return TR_OK;
}

static void tr_rpc_task_done(struct tr_rpc_endpoint *endpoint,
			     struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor_group *group = endpoint->executor.group;

	pthread_mutex_lock(&endpoint->lock);
	if (tr_rpc_call_handle_slot(handle) < endpoint->config.max_calls) {
		struct tr_rpc_call_slot *call =
			&endpoint->calls[tr_rpc_call_handle_slot(handle)];

		if (call->state != TR_RPC_CALL_FREE &&
		    call->generation == tr_rpc_call_handle_generation(handle)) {
			if (call->task_refs != 0)
				call->task_refs--;
			tr_rpc_maybe_free_call_locked(endpoint, call);
		}
	}
	pthread_mutex_unlock(&endpoint->lock);

	/* 协议状态已释放 endpoint->lock 后再归还强引用。
	 * 所有等待仅剩所有者引用的等待者，其唤醒统一由 tr_rpc_endpoint_put()
	 * 处理， 避免某种强引用来源漏掉 ref_cond 信号通知。
	 */
	tr_rpc_endpoint_put(endpoint);
	/* 归还端点强引用后，再确认执行组中的任务已完成。 */
	tr_rpc_executor_group_task_done(group);
}

struct tr_rpc_stream_payload_release {
	struct tr_stream_handle stream;
	struct tr_buffer *payload;
	int executed;
};

static int tr_rpc_stream_release_payload_on_owner(void *arg)
{
	struct tr_rpc_stream_payload_release *request =
		(struct tr_rpc_stream_payload_release *)arg;

	request->executed = 1;
	return tr_stream_release_payload(request->stream, request->payload);
}

/* 接收额度属于流协议状态，因此工作线程把负载所有权归还反应器所有者，
 * 而不是在 channel->lock 下修改流。如果反应器已不能接受命令，
 * 它就无法消耗负载；在本地释放缓冲区，使关闭不会泄漏内存。
 */
static int tr_rpc_release_stream_payload(struct tr_stream_handle stream,
					 struct tr_buffer *payload)
{
	struct tr_rpc_stream_payload_release request;
	struct tr_reactor *reactor;
	int ret;

	if (!payload)
		return TR_ERR_INVALID;
	if (!stream.channel) {
		tr_buffer_release(payload);
		return TR_ERR_STALE;
	}

	reactor = tr_channel_reactor(stream.channel);
	if (!reactor) {
		tr_buffer_release(payload);
		return TR_ERR_STATE;
	}

	request.stream = stream;
	request.payload = payload;
	request.executed = 0;
	ret = tr_reactor_call(reactor, tr_rpc_stream_release_payload_on_owner,
			      &request);
	if (!request.executed)
		tr_buffer_release(payload);
	return ret;
}

static void tr_rpc_release_task_payload(struct tr_rpc_task *task)
{
	if (!task || !task->payload)
		return;

	(void)tr_rpc_release_stream_payload(task->stream,
					    tr_buffer_take(&task->payload));
}

static int tr_rpc_message_bind_internal(
	struct tr_rpc_message *message, struct tr_rpc_endpoint *endpoint,
	struct tr_buffer *storage, struct tr_stream_handle stream)
{
	int ret;

	if (!message || !endpoint || !storage || !stream.channel)
		return TR_ERR_INVALID;

	/* 保留的公开消息可能比工作任务甚至对端连接活得更久。
	 * 发布描述符前先保护 Endpoint 生命周期，使其借用的 Channel
	 * 保持存活直到 tr_rpc_message_release()。
	 */
	ret = tr_rpc_endpoint_get(endpoint);
	if (ret != TR_OK)
		return ret;

	message->_private[0] = (uintptr_t)storage;
	message->_private[1] = (uintptr_t)endpoint;
	message->_private[2] = (uintptr_t)stream.slot;
	message->_private[3] = (uintptr_t)stream.generation;
	return TR_OK;
}

static void tr_rpc_message_unpin_internal(struct tr_rpc_message *message)
{
	struct tr_rpc_endpoint *endpoint;

	if (!message)
		return;
	endpoint =
		(struct tr_rpc_endpoint *)(uintptr_t)message->_private[1];
	message->_private[1] = (uintptr_t)0;
	if (endpoint)
		tr_rpc_endpoint_put(endpoint);
}

static int tr_rpc_message_unpack_internal(
	const struct tr_rpc_message *message, struct tr_buffer **storage_out,
	struct tr_rpc_endpoint **endpoint_out,
	struct tr_stream_handle *stream_out)
{
	struct tr_rpc_endpoint *endpoint;

	if (!message || message->_private[0] == (uintptr_t)0 ||
	    message->_private[1] == (uintptr_t)0)
		return TR_ERR_INVALID;

	endpoint =
		(struct tr_rpc_endpoint *)(uintptr_t)message->_private[1];
	if (storage_out)
		*storage_out =
			(struct tr_buffer *)(uintptr_t)message->_private[0];
	if (endpoint_out)
		*endpoint_out = endpoint;
	if (stream_out) {
		memset(stream_out, 0, sizeof(*stream_out));
		stream_out->channel = endpoint->channel;
		stream_out->slot = (uint32_t)message->_private[2];
		stream_out->generation = (uint32_t)message->_private[3];
	}
	return TR_OK;
}

static int tr_rpc_decode_task_message(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_task *task,
	struct tr_rpc_message *message, struct tr_rpc_wire_header *wire,
	int pin_lifetime)
{
	const uint8_t *metadata = NULL;
	const uint8_t *body = NULL;
	uint16_t metadata_len = 0;
	int ret;

	if (!task->payload || !message || !wire)
		return TR_ERR_INVALID;

	ret = tr_rpc_wire_decode_ex(task->payload->data, task->payload->len,
				    wire, &metadata, &metadata_len, &body);
	if (ret != TR_OK)
		return ret;
	(void)metadata;
	(void)metadata_len;

	if (wire->codec_id != TR_RPC_CODEC_RAW)
		return TR_ERR_BAD_TYPE;

	memset(message, 0, sizeof(*message));
	message->bytes.data = body;
	message->bytes.len = wire->payload_len;
	if (!pin_lifetime)
		return TR_OK;

	ret = tr_rpc_message_bind_internal(
		message, endpoint, task->payload, task->stream);
	if (ret != TR_OK) {
		memset(message, 0, sizeof(*message));
		return ret;
	}
	return TR_OK;
}

static void tr_rpc_finish_task_on_owner(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle call)
{
	tr_rpc_executor_complete_task(endpoint, call);
	tr_rpc_task_done(endpoint, call);
}

static void tr_rpc_apply_task_completion(void *arg)
{
	struct tr_rpc_task_completion *completion =
		(struct tr_rpc_task_completion *)arg;
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;

	if (!completion)
		return;

	endpoint = completion->endpoint;
	call = completion->call;
	free(completion);
	tr_rpc_finish_task_on_owner(endpoint, call);
}

static int tr_rpc_apply_task_completion_sync(void *arg)
{
	struct tr_rpc_task_completion *completion =
		(struct tr_rpc_task_completion *)arg;

	if (!completion || !completion->endpoint)
		return TR_ERR_INVALID;
	tr_rpc_finish_task_on_owner(completion->endpoint, completion->call);
	return TR_OK;
}

static int tr_rpc_defer_task_completion(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle call)
{
	struct tr_rpc_task_completion *completion;
	struct tr_reactor *reactor;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;

	completion =
		(struct tr_rpc_task_completion *)malloc(sizeof(*completion));
	reactor = tr_channel_reactor(endpoint->channel);
	if (!completion) {
		/* 内存不足不能迫使工作线程回退到修改
		 * Call。同步所有者调用使用栈上
		 * 请求，不需要额外分配，并保持同一生命周期规则。
		 */
		struct tr_rpc_task_completion sync_completion;

		sync_completion.endpoint = endpoint;
		sync_completion.call = call;
		return tr_reactor_call(reactor,
				       tr_rpc_apply_task_completion_sync,
				       &sync_completion);
	}
	completion->endpoint = endpoint;
	completion->call = call;

	ret = tr_reactor_complete(reactor, tr_rpc_apply_task_completion,
			      completion);
	if (ret != TR_OK)
		free(completion);
	return ret;
}

/* 工作线程取出排队任务时，执行器节点容量就可用，而非等该任务稍后完成。
 * 因此工作线程在 take() 后合并一次所有者完成通知；
 * 所有者重试各 Call 的待处理任务，不反转 endpoint->lock -> executor->lock
 * 锁序。
 */
static void tr_rpc_retry_pending_executor_on_owner(void *arg)
{
	struct tr_rpc_endpoint *endpoint = arg;
	uint32_t max_calls;
	uint32_t scan_start;
	uint32_t n;

	if (!endpoint)
		return;

	pthread_mutex_lock(&endpoint->lock);
	endpoint->pending_retry_scheduled = 0;
	endpoint->pending_executor_work = 0;
	max_calls = endpoint->config.max_calls;
	scan_start = endpoint->pending_retry_cursor;

	/* 固定本轮扫描的起始位置。成功准入只更新下一轮的游标；
	 * 若本循环使用可变游标，每次重试成功后都会跳过紧接着的槽位。
	 */
	for (n = 0; n < max_calls; ++n) {
		uint32_t slot = (scan_start + n) % max_calls;
		struct tr_rpc_call_slot *call = &endpoint->calls[slot];
		int ret = TR_OK;

		/* 每个 Call 的接收或半关闭工作优先于终止回调义务。
		 * 这保留回调顺序。
		 */
		if (call->pending_executor_task_valid) {
			struct tr_rpc_task task = call->pending_executor_task;

			if (call->state == TR_RPC_CALL_FREE ||
			    tr_rpc_call_handle_generation(task.call) != call->generation ||
			    call->cancelled || call->admission_rejected ||
			    call->executor_overloaded) {
				tr_rpc_drop_pending_executor_task_locked(call);
			} else {
				ret = tr_rpc_queue_task_locked(endpoint, call, &task);
				if (ret == TR_OK) {
					int queue_half_close =
						call->pending_remote_half_close &&
						task.type ==
							TR_RPC_TASK_SERVER_STREAM_MESSAGE;

					memset(&call->pending_executor_task, 0,
					       sizeof(call->pending_executor_task));
					call->pending_executor_task_valid = 0;
					endpoint->pending_retry_cursor =
						(slot + 1U) % max_calls;

					if (queue_half_close) {
						struct tr_rpc_task half_close;

						memset(&half_close, 0,
						       sizeof(half_close));
						half_close.type =
							TR_RPC_TASK_SERVER_HALF_CLOSE;
						half_close.call =
							tr_rpc_make_call_handle(
								endpoint, slot,
								call);
						call->pending_remote_half_close = 0;
						ret = tr_rpc_queue_task_locked(
							endpoint, call,
							&half_close);
						if (ret == TR_AGAIN)
							ret =
								tr_rpc_store_pending_executor_task_locked(
									endpoint,
									call,
									&half_close);
					}
				} else if (ret != TR_AGAIN) {
					tr_rpc_drop_pending_executor_task_locked(call);
				}
			}

			if (ret == TR_AGAIN) {
				endpoint->pending_executor_work = 1;
				endpoint->pending_retry_cursor = slot;
				break;
			}
		}

		if (!call->pending_executor_task_valid &&
		    call->pending_terminal_task_valid) {
			struct tr_rpc_task task = call->pending_terminal_task;

			if (call->state == TR_RPC_CALL_FREE ||
			    tr_rpc_call_handle_generation(task.call) !=
				    call->generation) {
				tr_rpc_drop_pending_terminal_task_locked(call);
				continue;
			}

			ret = tr_rpc_queue_task_locked(endpoint, call, &task);
			if (ret == TR_OK) {
				/* 执行器现在拥有任务及其响应负载。 */
				memset(&call->pending_terminal_task, 0,
				       sizeof(call->pending_terminal_task));
				call->pending_terminal_task_valid = 0;
				endpoint->pending_retry_cursor =
					(slot + 1U) % max_calls;
			} else if (ret == TR_AGAIN) {
				endpoint->pending_executor_work = 1;
				endpoint->pending_retry_cursor = slot;
				break;
			} else if (ret == TR_ERR_CLOSED) {
				/* 执行器准入已显式停止。本轮次无法运行任何回调；
				 * 退役未被接受的义务，使 Endpoint
				 * 关闭不会保留失管负载。
				 */
				tr_rpc_drop_pending_terminal_task_locked(call);
				tr_rpc_maybe_free_call_locked(endpoint, call);
			} else {
				/* 意外的不变量失败：保留所有权。 */
				endpoint->pending_executor_work = 1;
				endpoint->pending_retry_cursor = slot;
				break;
			}
		}
	}

	pthread_mutex_unlock(&endpoint->lock);
	tr_rpc_endpoint_put(endpoint);
}

static void
tr_rpc_schedule_pending_executor_retry(struct tr_rpc_endpoint *endpoint)
{
	struct tr_reactor *reactor;
	int submit = 0;
	int ret;

	if (!endpoint)
		return;

	pthread_mutex_lock(&endpoint->lock);
	if (endpoint->pending_executor_work &&
	    !endpoint->pending_retry_scheduled &&
	    tr_rpc_endpoint_get(endpoint) == TR_OK) {
		endpoint->pending_retry_scheduled = 1;
		submit = 1;
	}
	pthread_mutex_unlock(&endpoint->lock);
	if (!submit)
		return;

	reactor = tr_channel_reactor(endpoint->channel);
	ret = tr_reactor_complete(reactor,
				  tr_rpc_retry_pending_executor_on_owner,
				  endpoint);
	if (ret != TR_OK) {
		pthread_mutex_lock(&endpoint->lock);
		endpoint->pending_retry_scheduled = 0;
		pthread_mutex_unlock(&endpoint->lock);
		tr_rpc_endpoint_put(endpoint);
	}
}

struct tr_rpc_unary_failure_request {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_handle call;
};

/* 工作线程保持其 task_ref 与 Endpoint 强引用，直到正常完成交接结束。
 * 因此此仅用栈的所有者请求不会与 Call 槽位复用竞争，
 * 内存不足时也无需第二次分配。
 */
static int tr_rpc_fail_server_unary_on_owner(void *arg)
{
	struct tr_rpc_unary_failure_request *request = arg;
	struct tr_rpc_call_slot *call;
	int ret = TR_OK;

	if (!request || !request->endpoint)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&request->endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->call);
	if (call && call->method && call->is_unary && !call->cancelled &&
	    call->state != TR_RPC_CALL_TERMINAL)
		ret = tr_rpc_fail_server_unary_locked(request->endpoint, call);
	pthread_mutex_unlock(&request->endpoint->lock);
	return ret;
}

static void tr_rpc_apply_unary_completion(void *arg)
{
	struct tr_rpc_unary_completion *completion =
		(struct tr_rpc_unary_completion *)arg;
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_call_slot *call;
	struct tr_buffer *response_buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes response;
	int ret = TR_OK;

	if (!completion)
		return;

	endpoint = completion->endpoint;
	response.data = completion->response_len ? completion->response : NULL;
	response.len = completion->response_len;

	/* 这里由反应器所有者线程执行。endpoint->lock 暂时保留用于兼容
	 * 仍可从应用线程进入的旧 API；后续所有权
	 * 收敛后再缩减这把锁，而不是在本阶段直接替换成原子操作。
	 */
	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(completion->call);
	if (call && call->method && call->is_unary && !call->cancelled &&
	    !call->pending_tx) {
		uint32_t slot = tr_rpc_call_handle_slot(completion->call);

		ret = tr_rpc_run_interceptor_locked(
			endpoint, slot, call, TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER,
			completion->status, NULL);
		if (ret == TR_OK) {
			call = &endpoint->calls[slot];
			ret = tr_rpc_encode_message(
				endpoint, call, TR_RPC_WIRE_RESPONSE,
				&call->method->desc,
				call->method->desc.response_codec_id,
				completion->status, &response, &response_buffer);
			if (ret == TR_OK) {
				call->pending_tx = tr_buffer_take(&response_buffer);
				tr_rpc_semantic_finish_locked(
					endpoint, call, completion->status);
				(void)tr_rpc_try_unary_send_locked(endpoint, call);
			}
		}
		if (ret != TR_OK) {
			/* 编码或拦截器失败是已有责任方的终止边界。 */
			call = &endpoint->calls[slot];
			(void)tr_rpc_fail_server_unary_locked(endpoint, call);
		}
	}
	pthread_mutex_unlock(&endpoint->lock);

	/* 完成项接管了原任务的 Endpoint 强引用和 task_refs；
	 * 必须在所有者线程最后归还，避免 Call 在完成项应用前复用。
	 */
	{
		struct tr_rpc_call_handle call_handle = completion->call;

		free(completion);
		/* 每个调用的执行器串行化必须覆盖完成项应用；
		 * 只有所有者已应用本次结果后，才允许同一 Call
		 * 的下一项任务运行。
		 */
		tr_rpc_finish_task_on_owner(endpoint, call_handle);
	}
}

static int tr_rpc_prepare_unary_completion(
	struct tr_rpc_endpoint *endpoint, const struct tr_rpc_task *task,
	int status, const struct tr_rpc_bytes *response,
	struct tr_rpc_unary_completion **out)
{
	struct tr_rpc_unary_completion *completion;
	size_t size;

	if (!endpoint || !task || !response || !out ||
	    (response->len != 0 && !response->data))
		return TR_ERR_INVALID;
	*out = NULL;
#if SIZE_MAX <= UINT32_MAX
	if (response->len > (uint32_t)(SIZE_MAX - sizeof(*completion)))
		return TR_ERR_BAD_LENGTH;
#endif

	size = sizeof(*completion) + (size_t)response->len;
	completion = (struct tr_rpc_unary_completion *)malloc(size);
	if (!completion)
		return TR_ERR_NOMEM;

	completion->endpoint = endpoint;
	completion->call = task->call;
	completion->status = status;
	completion->response_len = response->len;
	if (response->len)
		memcpy(completion->response, response->data, response->len);

	*out = completion;
	return TR_OK;
}

static int tr_rpc_submit_unary_completion(
	struct tr_rpc_endpoint *endpoint,
	struct tr_rpc_unary_completion *completion)
{
	struct tr_reactor *reactor;

	if (!endpoint || !completion)
		return TR_ERR_INVALID;

	reactor = tr_channel_reactor(endpoint->channel);
	return tr_reactor_complete(reactor, tr_rpc_apply_unary_completion,
			       completion);
}

static int tr_rpc_executor_run_server_unary(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_task *task)
{
	struct tr_rpc_message message = { 0 };
	struct tr_rpc_wire_header wire;
	struct tr_rpc_unary_response response;
	struct tr_rpc_bytes completion_response;
	struct tr_rpc_unary_completion *completion = NULL;
	tr_rpc_unary_handler handler = task->u.server_unary.handler;
	int ret = TR_ERR_BAD_TYPE;
	int deferred = 0;

	if (!handler || tr_rpc_decode_task_message(
			    endpoint, task, &message, &wire, 0) != TR_OK)
		goto out;

	memset(&response, 0, sizeof(response));
	response.status = TR_RPC_STATUS_INTERNAL;

	ret = handler(task->call, &message.bytes, &response,
		      task->u.server_unary.handler_arg);
	if (ret != TR_OK || !tr_rpc_status_valid(response.status))
		response.status = TR_RPC_STATUS_INTERNAL;

	if (response.message.len >
		    task->u.server_unary.method.max_response_bytes ||
	    (response.message.len != 0 && !response.message.data)) {
		response.status = TR_RPC_STATUS_INTERNAL;
		response.message.data = NULL;
		response.message.len = 0;
	}

	completion_response = response.message;
	ret = tr_rpc_prepare_unary_completion(
		endpoint, task, response.status, &completion_response,
		&completion);

out:
	/* 先结束工作线程对请求负载的所有访问，再提交会释放任务
	 * 强引用的完成项。否则反应器可能先执行完成项，
	 * 拆除随后越过仍在归还接收缓冲区的工作线程。
	 */
	tr_rpc_release_task_payload(task);

	if (completion) {
		ret = tr_rpc_submit_unary_completion(endpoint, completion);
		if (ret == TR_OK) {
			deferred = 1;
			completion = NULL;
		} else {
			free(completion);
			completion = NULL;
		}
	}
	if (!deferred) {
		struct tr_rpc_unary_failure_request request;

		request.endpoint = endpoint;
		request.call = task->call;
		/* 不依赖堆，也不在工作线程侧修改 Call。失败的反应器所有者轮次
		 * 会拒绝此请求；随后由其连接关闭路径负责剩余 Call 生命周期。
		 */
		(void)tr_rpc_owner_call(endpoint,
					tr_rpc_fail_server_unary_on_owner, &request);
	}
	return deferred;
}

static void tr_rpc_executor_mark_cancelled(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor;

	if (!endpoint || tr_rpc_call_handle_slot(handle) >= endpoint->config.max_calls)
		return;

	executor = &endpoint->executor;
	pthread_mutex_lock(&executor->lock);
	if (executor->callq[tr_rpc_call_handle_slot(handle)].generation == tr_rpc_call_handle_generation(handle))
		executor->callq[tr_rpc_call_handle_slot(handle)].cancelled = 1;
	pthread_mutex_unlock(&executor->lock);
}

static int tr_rpc_executor_task_cancelled(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor;
	int cancelled = 0;

	if (!endpoint || tr_rpc_call_handle_slot(handle) >= endpoint->config.max_calls)
		return 1;

	executor = &endpoint->executor;
	pthread_mutex_lock(&executor->lock);
	if (executor->callq[tr_rpc_call_handle_slot(handle)].generation == tr_rpc_call_handle_generation(handle))
		cancelled = executor->callq[tr_rpc_call_handle_slot(handle)].cancelled;
	pthread_mutex_unlock(&executor->lock);
	return cancelled;
}

static int tr_rpc_executor_run_task(struct tr_rpc_endpoint *endpoint,
				    struct tr_rpc_task *task)
{
	if (task->type == TR_RPC_TASK_SERVER_UNARY)
		return tr_rpc_executor_run_server_unary(endpoint, task);

	/* 取消是执行器调度元数据，不是工作线程读取可变 RPC Call 状态。
	 * 关闭、结果与事件通知仍必须运行。
	 */
	if (task->type != TR_RPC_TASK_SERVER_CLOSE &&
	    task->type != TR_RPC_TASK_CLIENT_UNARY_RESULT &&
	    task->type != TR_RPC_TASK_CLIENT_EVENT &&
	    tr_rpc_executor_task_cancelled(endpoint, task->call)) {
		tr_rpc_release_task_payload(task);
		return 0;
	}

	switch (task->type) {
	case TR_RPC_TASK_SERVER_STREAM_MESSAGE: {
		struct tr_rpc_message message = { 0 };
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_stream_handlers *handlers =
			&task->u.server_stream.handlers;
		void *handler_arg = task->u.server_stream.handler_arg;
		enum tr_rpc_message_disposition disposition =
			TR_RPC_MESSAGE_RELEASE;
		int ret = TR_OK;

		if (tr_rpc_decode_task_message(
			    endpoint, task, &message, &wire, 1) != TR_OK) {
			(void)tr_rpc_call_finish(task->call,
						 TR_RPC_STATUS_INTERNAL);
			break;
		}

		if (task->first_message && handlers->on_open)
			ret = handlers->on_open(task->call, handler_arg);
		if (ret == TR_OK && handlers->on_message)
			disposition = handlers->on_message(task->call, &message,
							    handler_arg);
		if (ret != TR_OK)
			(void)tr_rpc_call_finish(task->call,
						 TR_RPC_STATUS_INTERNAL);

		if (disposition == TR_RPC_MESSAGE_TAKE_OWNERSHIP)
			(void)tr_buffer_take(&task->payload);
		else
			tr_rpc_message_unpin_internal(&message);
		break;
	}

	case TR_RPC_TASK_SERVER_HALF_CLOSE:
		if (task->u.server_stream.handlers.on_half_close)
			task->u.server_stream.handlers.on_half_close(
				task->call, task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_SERVER_WRITABLE:
		if (task->u.server_stream.handlers.on_writable)
			task->u.server_stream.handlers.on_writable(
				task->call, task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_SERVER_CLOSE:
		if (task->u.server_stream.handlers.on_close)
			task->u.server_stream.handlers.on_close(
				task->call, task->status,
				task->u.server_stream.handler_arg);
		break;

	case TR_RPC_TASK_CLIENT_UNARY_RESULT: {
		struct tr_rpc_message message = { 0 };
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_bytes *bytes = NULL;
		int status = task->status;

		if (task->payload) {
			if (tr_rpc_decode_task_message(
				    endpoint, task, &message, &wire, 0) == TR_OK) {
				bytes = &message.bytes;
				status = wire.status;
			} else {
				status = TR_RPC_STATUS_INTERNAL;
			}
		}
		if (task->u.client_unary.result_cb)
			task->u.client_unary.result_cb(
				task->call, status, bytes,
				task->u.client_unary.result_arg);
		break;
	}

	case TR_RPC_TASK_CLIENT_MESSAGE: {
		struct tr_rpc_message message = { 0 };
		struct tr_rpc_wire_header wire;
		const struct tr_rpc_call_callbacks *callbacks =
			&task->u.client_stream.callbacks;
		enum tr_rpc_message_disposition disposition =
			TR_RPC_MESSAGE_RELEASE;

		if (tr_rpc_decode_task_message(
			    endpoint, task, &message, &wire, 1) == TR_OK) {
			if (callbacks->on_message)
				disposition = callbacks->on_message(
					task->call, &message, callbacks->arg);
		}
		if (disposition == TR_RPC_MESSAGE_TAKE_OWNERSHIP)
			(void)tr_buffer_take(&task->payload);
		else
			tr_rpc_message_unpin_internal(&message);
		break;
	}

	case TR_RPC_TASK_CLIENT_EVENT:
		if (task->u.client_stream.callbacks.on_event)
			task->u.client_stream.callbacks.on_event(
				task->call, task->event, task->status,
				task->u.client_stream.callbacks.arg);
		break;

	default:
		break;
	}

	tr_rpc_release_task_payload(task);
	return 0;
}

static int tr_rpc_executor_run_observed_task(struct tr_rpc_endpoint *endpoint,
					      struct tr_rpc_task *task)
{
	uint64_t start_ns = 0;
	uint64_t end_ns;
	int ret;

	if (endpoint->config.observability_flags & TR_OBSERVABILITY_TIMING)
		start_ns = tr_rpc_now_ns();

	ret = tr_rpc_executor_run_task(endpoint, task);

	if (start_ns != 0) {
		end_ns = tr_rpc_now_ns();
		if (end_ns >= start_ns) {
			struct tr_rpc_executor *executor = &endpoint->executor;

			pthread_mutex_lock(&executor->lock);
			tr_observe_latency_ns(&executor->handler_ns,
					      end_ns - start_ns);
			pthread_mutex_unlock(&executor->lock);
		}
	}
	return ret;
}

static int tr_rpc_executor_take(struct tr_rpc_endpoint *endpoint,
				struct tr_rpc_task *task, int wait)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	struct tr_rpc_executor_callq *callq;
	struct tr_rpc_executor_node *node;
	uint32_t slot;
	uint32_t node_index;

	pthread_mutex_lock(&executor->lock);
	if (executor->group && !wait)
		executor->group_enqueued = 0;

	while (wait && executor->ready_count == 0 && !executor->stopping)
		pthread_cond_wait(&executor->cond, &executor->lock);

	if (executor->ready_count == 0) {
		int ret = executor->stopping ? 0 : -1;
		pthread_mutex_unlock(&executor->lock);
		return ret;
	}

	slot = executor->ready_calls[executor->ready_head];
	executor->ready_head =
		(executor->ready_head + 1U) % executor->ready_capacity;
	executor->ready_count--;

	callq = &executor->callq[slot];
	callq->ready = 0;
	node_index = callq->head;
	if (node_index == TR_RPC_EXEC_NONE || callq->running) {
		pthread_mutex_unlock(&executor->lock);
		return -1;
	}

	node = &executor->nodes[node_index];
	*task = node->task;
	if (node->enqueued_ns != 0) {
		uint64_t now_ns = tr_rpc_now_ns();

		if (now_ns >= node->enqueued_ns)
			tr_observe_latency_ns(&executor->queue_wait_ns,
					      now_ns - node->enqueued_ns);
	}
	node->enqueued_ns = 0;
	executor->taken_tasks++;
	callq->head = node->next;
	if (callq->head == TR_RPC_EXEC_NONE)
		callq->tail = TR_RPC_EXEC_NONE;

	node->next = executor->free_head;
	executor->free_head = node_index;
	if (callq->queued_count != 0)
		callq->queued_count--;
	if (executor->queued_count != 0)
		executor->queued_count--;
	callq->running = 1;
	executor->running_count++;

	/* 执行器组工作线程已消费该 Endpoint 的唤醒令牌。
	 * 如果该 Endpoint 上还有其他就绪 Call，则在释放执行器锁之前
	 * 再发布一个替代令牌，让同分片的其他工作线程可以并行处理。
	 */
	if (executor->group && executor->ready_count != 0 &&
	    !executor->group_enqueued) {
		if (tr_rpc_executor_group_enqueue(executor->group, endpoint) ==
		    TR_OK)
			executor->group_enqueued = 1;
	}

	pthread_mutex_unlock(&executor->lock);
	tr_rpc_schedule_pending_executor_retry(endpoint);
	return 1;
}

static void tr_rpc_executor_complete_task(struct tr_rpc_endpoint *endpoint,
					  struct tr_rpc_call_handle handle)
{
	struct tr_rpc_executor *executor = &endpoint->executor;

	pthread_mutex_lock(&executor->lock);
	if (tr_rpc_call_handle_slot(handle) < endpoint->config.max_calls) {
		struct tr_rpc_executor_callq *callq =
			&executor->callq[tr_rpc_call_handle_slot(handle)];

		if (callq->generation == tr_rpc_call_handle_generation(handle) && callq->running) {
			callq->running = 0;
			if (executor->running_count != 0)
				executor->running_count--;

			if (callq->head != TR_RPC_EXEC_NONE && !callq->ready) {
				if (tr_rpc_executor_ready_push_locked(
					    executor, tr_rpc_call_handle_slot(handle)) == TR_OK) {
					callq->ready = 1;
					if (executor->group) {
						if (!executor->group_enqueued &&
						    tr_rpc_executor_group_enqueue(
							    executor->group,
							    endpoint) == TR_OK)
							executor->group_enqueued = 1;
					} else {
						pthread_cond_signal(&executor->cond);
					}
				}
			}
		}
	}
	pthread_mutex_unlock(&executor->lock);
}

static void *tr_rpc_executor_main(void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;

	struct tr_worker_context worker = tr_execution_enter_worker();
	for (;;) {
		struct tr_rpc_task task;
		int ret = tr_rpc_executor_take(endpoint, &task, 1);

		if (ret == 0)
			break;
		if (ret < 0)
			continue;

		{
			int task_done_deferred =
				tr_rpc_executor_run_observed_task(endpoint, &task);
			if (!task_done_deferred &&
			    tr_rpc_defer_task_completion(endpoint, task.call) !=
				    TR_OK) {
				/* 完成队列满时 tr_reactor_complete()
				 * 会等待容量，
				 * 不再把瞬时满载当成失败。只有反应器已停止或交接
				 * 本身失败时，工作线程才在本地完成回滚或收尾，
				 * 不能泄漏任务强引用。
				 */
				tr_rpc_executor_complete_task(endpoint, task.call);
				tr_rpc_task_done(endpoint, task.call);
			}
		}
	}

	tr_execution_leave_worker(worker);
	return NULL;
}

static struct tr_rpc_endpoint *
tr_rpc_executor_group_take(struct tr_rpc_executor_group *group)
{
	struct tr_rpc_endpoint *endpoint;

	pthread_mutex_lock(&group->lock);
	while (!group->ready_head &&
	       (!group->stopping || group->outstanding_tasks != 0U))
		pthread_cond_wait(&group->cond, &group->lock);
	if (!group->ready_head && group->stopping &&
	    group->outstanding_tasks == 0U) {
		pthread_mutex_unlock(&group->lock);
		return NULL;
	}

	endpoint = group->ready_head;
	group->ready_head = endpoint->executor.group_ready_next;
	if (!group->ready_head)
		group->ready_tail = NULL;
	endpoint->executor.group_ready_next = NULL;
	if (group->ready_count != 0U)
		group->ready_count--;
	pthread_mutex_unlock(&group->lock);
	return endpoint;
}

static void *tr_rpc_executor_group_main(void *arg)
{
	struct tr_rpc_executor_group *group =
		(struct tr_rpc_executor_group *)arg;

	struct tr_worker_context worker = tr_execution_enter_worker();
	for (;;) {
		struct tr_rpc_endpoint *endpoint =
			tr_rpc_executor_group_take(group);
		struct tr_rpc_task task;
		int ret;

		if (!endpoint)
			break;

		ret = tr_rpc_executor_take(endpoint, &task, 0);
		if (ret <= 0)
			continue;

		{
			int task_done_deferred =
				tr_rpc_executor_run_observed_task(endpoint, &task);
			if (!task_done_deferred &&
			    tr_rpc_defer_task_completion(endpoint, task.call) !=
				    TR_OK) {
				/* 反应器已停止、队列满或内存不足时必须在工作线程
				 * 侧完成回滚或收尾，不能泄漏任务强引用。
				 * 正常运行路径统一由反应器所有者完成。
				 */
				tr_rpc_executor_complete_task(endpoint, task.call);
				tr_rpc_task_done(endpoint, task.call);
			}
		}
	}

	tr_execution_leave_worker(worker);
	return NULL;
}

static void tr_rpc_executor_cleanup(struct tr_rpc_executor *executor)
{
	if (!executor)
		return;

	free(executor->ready_calls);
	executor->ready_calls = NULL;
	free(executor->callq);
	executor->callq = NULL;
	free(executor->nodes);
	executor->nodes = NULL;
	free(executor->threads);
	executor->threads = NULL;
}


static int tr_rpc_executor_init(struct tr_rpc_endpoint *endpoint,
				uint32_t capacity,
				uint32_t continuation_reserve,
				struct tr_rpc_executor_group *group)
{
	struct tr_rpc_executor *executor = &endpoint->executor;
	uint32_t thread_count;
	uint32_t i;

	memset(executor, 0, sizeof(*executor));
	if (capacity == 0)
		capacity = endpoint->config.max_calls * 4U + 16U;
	if (capacity < 16U)
		capacity = 16U;
	if (endpoint->config.role == TR_RPC_SERVER &&
	    continuation_reserve != 0 &&
	    continuation_reserve >= capacity)
		return TR_ERR_INVALID;

	thread_count = endpoint->config.executor_threads;
	if (thread_count == 0)
		thread_count = endpoint->config.max_calls < 4U ?
				       endpoint->config.max_calls :
				       4U;
	if (thread_count == 0)
		thread_count = 1U;
	if (thread_count > endpoint->config.max_calls)
		thread_count = endpoint->config.max_calls;

	if (pthread_mutex_init(&executor->lock, NULL) != 0)
		return TR_ERR_INVALID;
	if (pthread_cond_init(&executor->cond, NULL) != 0) {
		pthread_mutex_destroy(&executor->lock);
		return TR_ERR_INVALID;
	}
	executor->sync_ready = 1;

	executor->group = group;
	if (!group)
		executor->threads =
			(pthread_t *)calloc(thread_count, sizeof(*executor->threads));
	executor->nodes = (struct tr_rpc_executor_node *)calloc(
		capacity, sizeof(*executor->nodes));
	executor->callq = (struct tr_rpc_executor_callq *)calloc(
		endpoint->config.max_calls, sizeof(*executor->callq));
	executor->ready_calls = (uint32_t *)calloc(
		endpoint->config.max_calls, sizeof(*executor->ready_calls));
	if ((!group && !executor->threads) || !executor->nodes ||
	    !executor->callq || !executor->ready_calls)
		return TR_ERR_NOMEM;

	executor->capacity = capacity;
	executor->continuation_reserve =
		endpoint->config.role == TR_RPC_SERVER ?
			continuation_reserve : 0U;
	executor->thread_count = group ? group->thread_count : thread_count;
	executor->ready_capacity = endpoint->config.max_calls;
	executor->free_head = capacity ? 0U : TR_RPC_EXEC_NONE;

	for (i = 0; i < capacity; ++i)
		executor->nodes[i].next =
			(i + 1U < capacity) ? i + 1U : TR_RPC_EXEC_NONE;
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		executor->callq[i].head = TR_RPC_EXEC_NONE;
		executor->callq[i].tail = TR_RPC_EXEC_NONE;
	}

	return TR_OK;
}

static int tr_rpc_executor_start(struct tr_rpc_endpoint *endpoint)
{
	struct tr_rpc_executor *executor;
	uint32_t i;

	if (!endpoint)
		return TR_ERR_INVALID;
	executor = &endpoint->executor;
	if (executor->group)
		return TR_OK;
	if (!executor->sync_ready || !executor->threads)
		return TR_ERR_STATE;

	pthread_mutex_lock(&executor->lock);
	if (executor->stopping) {
		pthread_mutex_unlock(&executor->lock);
		return TR_ERR_STATE;
	}
	pthread_mutex_unlock(&executor->lock);

	for (i = executor->started_threads; i < executor->thread_count; ++i) {
		if (pthread_create(&executor->threads[i], NULL,
				   tr_rpc_executor_main, endpoint) != 0)
			return TR_ERR_SYS;
		executor->started_threads++;
	}
	return TR_OK;
}

static int tr_rpc_executor_shutdown_checked(
	struct tr_rpc_endpoint *endpoint)
{
	struct tr_rpc_executor *executor = &endpoint->executor;

	pthread_mutex_lock(&executor->lock);
	if (!executor->stopping) {
		executor->stopping = 1;
		pthread_cond_broadcast(&executor->cond);
	}
	pthread_mutex_unlock(&executor->lock);

	/* 独立 Endpoint 自己拥有工作线程，因此所有者最后一次 put 之前
	 * 必须确认全部工作线程已回收。回收失败不清 started_threads，也不释放
	 * 执行器存储；join_cursor 只在成功回收后前移，下一次销毁从
	 * 精确的未收敛线程继续。
	 *
	 * 服务端分片本地工作线程由执行器组拥有，这里只关闭该
	 * Endpoint 的任务准入，不回收组工作线程。
	 */
	if (executor->group)
		return TR_OK;

	while (executor->join_cursor < executor->started_threads) {
		if (pthread_join(
			    executor->threads[executor->join_cursor], NULL) != 0)
			return TR_ERR_SYS;
		executor->join_cursor++;
	}
	return TR_OK;
}

static void tr_rpc_executor_release(struct tr_rpc_endpoint *endpoint)
{
	struct tr_rpc_executor *executor = &endpoint->executor;

	tr_rpc_executor_cleanup(executor);
	if (executor->sync_ready) {
		pthread_cond_destroy(&executor->cond);
		pthread_mutex_destroy(&executor->lock);
		executor->sync_ready = 0;
	}
}

int tr_rpc_executor_group_create(uint32_t endpoint_capacity,
				 uint32_t max_calls_per_endpoint,
				 uint32_t thread_count,
				 struct tr_rpc_executor_group **out)
{
	struct tr_rpc_executor_group *group_mem
		TR_AUTO(tr_rpc_group_mem_cleanup) = NULL;
	pthread_t *threads TR_AUTO(tr_rpc_thread_array_cleanup) = NULL;
	uint64_t total_calls;

	if (!out || endpoint_capacity == 0 || max_calls_per_endpoint == 0 ||
	    endpoint_capacity == UINT32_MAX)
		return TR_ERR_INVALID;
	*out = NULL;

	total_calls = (uint64_t)endpoint_capacity * max_calls_per_endpoint;
	if (total_calls == 0)
		return TR_ERR_INVALID;

	if (thread_count == 0)
		thread_count = total_calls < 4U ? (uint32_t)total_calls : 4U;
	if ((uint64_t)thread_count > total_calls)
		thread_count = (uint32_t)total_calls;
	if (thread_count == 0)
		thread_count = 1U;

	group_mem =
		(struct tr_rpc_executor_group *)calloc(1, sizeof(*group_mem));
	if (!group_mem)
		return TR_ERR_NOMEM;

	threads = (pthread_t *)calloc(thread_count, sizeof(*threads));
	if (!threads)
		return TR_ERR_NOMEM;

	if (pthread_mutex_init(&group_mem->lock, NULL) != 0)
		return TR_ERR_INVALID;
	if (pthread_cond_init(&group_mem->cond, NULL) != 0) {
		pthread_mutex_destroy(&group_mem->lock);
		return TR_ERR_INVALID;
	}

	group_mem->threads = tr_rpc_thread_array_take(&threads);
	group_mem->thread_count = thread_count;

	/* 创建只建立组软状态，不启动任何工作线程。条件变量初始化成功后
	 * 已经没有后续可失败步骤，因此这里可以直接完成所有权转移，不需要
	 * 一个会触碰线程回收屏障的 TR_AUTO 所有者清理。
	 */
	*out = tr_rpc_group_mem_take(&group_mem);
	return TR_OK;
}

int tr_rpc_executor_group_stop(struct tr_rpc_executor_group *group)
{
	if (!group)
		return TR_OK;
	if (tr_execution_in_worker_context())
		return TR_ERR_STATE;

	pthread_mutex_lock(&group->lock);
	if (group->started_threads == 0U) {
		group->accepting = 0;
		if (group->ready_head || group->ready_tail ||
		    group->ready_count != 0U ||
		    group->outstanding_tasks != 0U) {
			pthread_mutex_unlock(&group->lock);
			return TR_ERR_STATE;
		}
		group->stopping = 0;
		group->join_cursor = 0U;
		pthread_mutex_unlock(&group->lock);
		return TR_OK;
	}
	group->accepting = 0;
	group->stopping = 1;
	pthread_cond_broadcast(&group->cond);
	pthread_mutex_unlock(&group->lock);

	while (group->join_cursor < group->started_threads) {
		if (pthread_join(group->threads[group->join_cursor], NULL) != 0)
			return TR_ERR_SYS;
		group->join_cursor++;
	}

	pthread_mutex_lock(&group->lock);
	if (group->ready_head || group->ready_tail ||
	    group->ready_count != 0U || group->outstanding_tasks != 0U) {
		pthread_mutex_unlock(&group->lock);
		return TR_ERR_STATE;
	}

#ifndef NDEBUG
	assert(group->ready_head == NULL);
	assert(group->ready_tail == NULL);
	assert(group->ready_count == 0U);
	assert(group->outstanding_tasks == 0U);
#endif
	pthread_mutex_unlock(&group->lock);

	/* 完整回收线程后结束本工作线程轮次。组存储与 pthread_t 数组保留，
	 * 因而 Runtime 可以在后续 start() 开启一个全新的工作线程轮次。
	 */
	group->started_threads = 0U;
	group->join_cursor = 0U;
	pthread_mutex_lock(&group->lock);
	group->stopping = 0;
	pthread_mutex_unlock(&group->lock);
	return TR_OK;
}

int tr_rpc_executor_group_can_start(struct tr_rpc_executor_group *group)
{
	int startable;

	if (!group)
		return TR_OK;
	if (tr_execution_in_worker_context())
		return TR_ERR_STATE;

	pthread_mutex_lock(&group->lock);
	startable = !group->stopping &&
		    group->started_threads == 0U &&
		    group->join_cursor == 0U;
	pthread_mutex_unlock(&group->lock);
	return startable ? TR_OK : TR_ERR_STATE;
}

int tr_rpc_executor_group_start(struct tr_rpc_executor_group *group)
{
	uint32_t i;
	int cause;
	int stop_ret;

	if (!group || !group->threads || group->thread_count == 0U)
		return TR_ERR_INVALID;
	if (tr_execution_in_worker_context())
		return TR_ERR_STATE;

	pthread_mutex_lock(&group->lock);
	if (group->stopping || group->started_threads != 0U ||
	    group->join_cursor != 0U) {
		pthread_mutex_unlock(&group->lock);
		return TR_ERR_STATE;
	}
	pthread_mutex_unlock(&group->lock);

	for (i = 0; i < group->thread_count; ++i) {
		if (pthread_create(&group->threads[i], NULL,
				   tr_rpc_executor_group_main, group) != 0) {
			cause = TR_ERR_SYS;
			/* Runtime 已经拥有组，因此回滚屏障可以返回失败且
			 * 保留所有权。若线程回收失败，生命周期错误优先。
			 */
			stop_ret = tr_rpc_executor_group_stop(group);
			return stop_ret != TR_OK ? stop_ret : cause;
		}
		group->started_threads++;
	}
	/* 不允许预启动或已停止组在没有消费者时接收任务引用。
	 * 线程已全部建立后再发布接受门禁，避免部分启动失败遗留任务。
	 */
	pthread_mutex_lock(&group->lock);
	group->accepting = 1;
	pthread_mutex_unlock(&group->lock);
	return TR_OK;
}

int tr_rpc_executor_group_destroy_checked(
	struct tr_rpc_executor_group *group)
{
	int ret;

	if (!group)
		return TR_OK;

	ret = tr_rpc_executor_group_stop(group);
	if (ret != TR_OK)
		return ret;

	free(group->threads);
	pthread_cond_destroy(&group->cond);
	pthread_mutex_destroy(&group->lock);
	free(group);
	return TR_OK;
}

static int tr_rpc_queue_client_event_locked(struct tr_rpc_endpoint *endpoint,
					    uint32_t slot,
					    struct tr_rpc_call_slot *call,
					    enum tr_rpc_call_event event,
					    int status)
{
	struct tr_rpc_task task;

	memset(&task, 0, sizeof(task));
	task.type = TR_RPC_TASK_CLIENT_EVENT;
	task.call = tr_rpc_make_call_handle(endpoint, slot, call);
	task.event = event;
	task.status = status;
	return tr_rpc_queue_task_locked(endpoint, call, &task);
}

/* 终止事件必须在执行器暂时满载时仍被保留。 */
static int tr_rpc_queue_client_terminal_event_locked(
	struct tr_rpc_endpoint *endpoint, uint32_t slot,
	struct tr_rpc_call_slot *call, enum tr_rpc_call_event event, int status)
{
	struct tr_rpc_task task;

	memset(&task, 0, sizeof(task));
	task.type = TR_RPC_TASK_CLIENT_EVENT;
	task.call = tr_rpc_make_call_handle(endpoint, slot, call);
	task.event = event;
	task.status = status;
	return tr_rpc_queue_terminal_task_locked(endpoint, call, &task);
}

static int tr_rpc_queue_unary_failure_locked(struct tr_rpc_endpoint *endpoint,
					     uint32_t slot,
					     struct tr_rpc_call_slot *call,
					     int status)
{
	struct tr_rpc_task task;
	int ret;

	if (call->result_delivered)
		return TR_OK;

	memset(&task, 0, sizeof(task));
	task.type = TR_RPC_TASK_CLIENT_UNARY_RESULT;
	task.call = tr_rpc_make_call_handle(endpoint, slot, call);
	task.status = status;
	ret = tr_rpc_queue_terminal_task_locked(endpoint, call, &task);
	if (ret == TR_OK)
		call->result_delivered = 1;
	return ret;
}

static int tr_rpc_notify_terminal_locked(struct tr_rpc_endpoint *endpoint,
					 uint32_t slot,
					 struct tr_rpc_call_slot *call,
					 int status)
{
	struct tr_rpc_task task;
	int ret = TR_OK;

	if (call->terminal_notified)
		return TR_OK;

	if (endpoint->config.role == TR_RPC_CLIENT) {
		ret = tr_rpc_run_interceptor_locked(
			endpoint, slot, call, TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
			status, NULL);
		if (ret != TR_OK)
			return ret;
		call = &endpoint->calls[slot];

		if (call->is_unary)
			ret = tr_rpc_queue_unary_failure_locked(endpoint, slot,
								call, status);
		else
			ret = tr_rpc_queue_client_terminal_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_FINISHED, status);
	} else if (!call->is_unary && call->method &&
		   call->method->handler_kind == TR_RPC_HANDLER_STREAM) {
		memset(&task, 0, sizeof(task));
		task.type = TR_RPC_TASK_SERVER_CLOSE;
		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		task.status = status;
		ret = tr_rpc_queue_terminal_task_locked(endpoint, call, &task);
	}

	if (ret == TR_OK) {
		call->terminal_notified = 1;
		endpoint->stat_calls_completed++;
		tr_rpc_semantic_finish_locked(endpoint, call, status);
	}
	return ret;
}

/* 调用方必须已经持有 endpoint->lock。 */
static int tr_rpc_try_cancel_send_locked(struct tr_rpc_endpoint *endpoint,
					 struct tr_rpc_call_slot *call)
{
	int ret = TR_OK;

	if (call->pending_control) {
		ret = tr_stream_send(call->stream, call->pending_control);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&call->pending_control);
			if (call->pending_control_is_final_status) {
				call->final_status_sent = 1;
				call->pending_control_is_final_status = 0;
			}
			call->need_local_close = 1;
		} else if (ret != TR_AGAIN) {
			tr_buffer_release(tr_buffer_take(&call->pending_control));
			call->pending_control_is_final_status = 0;
			call->need_local_close = 1;
		} else {
			return tr_rpc_schedule_final_close_retry_locked(
			        endpoint, call);
		}
	}

	if (call->need_local_close && !call->local_closed) {
		ret = tr_stream_close(call->stream);
		if (ret == TR_OK || ret == TR_ERR_CLOSED) {
			call->need_local_close = 0;
			tr_rpc_mark_local_closed_locked(endpoint, call);
			if (call->admission_rejected && call->remote_closed)
				call->state = TR_RPC_CALL_TERMINAL;
			ret = TR_OK;
		} else if (ret == TR_AGAIN) {
			ret = tr_rpc_schedule_final_close_retry_locked(endpoint, call);
		}
	}
	return ret;
}

struct tr_rpc_cancel_request {
	struct tr_rpc_call_handle handle;
	int status;
};

static int tr_rpc_cancel_on_owner(void *arg)
{
	struct tr_rpc_cancel_request *request =
		(struct tr_rpc_cancel_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	int status = request->status;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	struct tr_buffer *control TR_AUTO(tr_buffer_cleanup) = NULL;
	int peer_visible;
	int ret = TR_OK;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_STALE;
	}
	if (call->interceptor_active) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_STATE;
	}
	if (call->cancelled) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_OK;
	}
	if (call->state == TR_RPC_CALL_TERMINAL ||
	    call->executor_overloaded) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_CLOSED;
	}

	call->cancelled = 1;
	call->cancel_status = status;
	tr_rpc_drop_pending_executor_task_locked(call);
	tr_rpc_executor_mark_cancelled(endpoint, handle);
	if (status == TR_RPC_STATUS_DEADLINE_EXCEEDED)
		endpoint->stat_calls_deadline_exceeded++;
	else
		endpoint->stat_calls_cancelled++;
	(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
	call->final_status_seen = 1;
	call->final_status = status;
	call->state = TR_RPC_CALL_TERMINAL;
	tr_rpc_semantic_finish_locked(endpoint, call, status);
	(void)tr_rpc_notify_terminal_locked(endpoint, tr_rpc_call_handle_slot(handle), call,
					    status);

	/* 客户端对端在第一条 REQUEST 真正发送前并不知道该流式 Call。
	 * 服务端 Call 按定义一定来自已经收到的 REQUEST，因此天然对对端可见。
	 */
	peer_visible = endpoint->config.role == TR_RPC_SERVER ||
		       call->tx_count != 0;
	if (peer_visible && !call->local_closed && call->method) {
		ret = tr_rpc_encode_control_locked(
			endpoint, call, TR_RPC_WIRE_CANCEL, status, &control);
		if (ret == TR_OK)
			call->pending_control = tr_buffer_take(&control);
	}

	call->need_local_close = 1;
	(void)tr_rpc_try_cancel_send_locked(endpoint, call);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (call)
		tr_rpc_maybe_free_call_locked(endpoint, call);
	pthread_mutex_unlock(&endpoint->lock);

	return TR_OK;
}

static int tr_rpc_cancel_internal(struct tr_rpc_call_handle handle, int status)
{
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_cancel_request request;

	if (!endpoint || (status != TR_RPC_STATUS_CANCELLED &&
			  status != TR_RPC_STATUS_DEADLINE_EXCEEDED))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status = status;
	return tr_rpc_owner_call(endpoint, tr_rpc_cancel_on_owner, &request);
}
/* STATUS 是应用终止屏障。有界控制池返回 TR_AGAIN 时，传输层仍承担待处理
 * 半关闭。绝不重新提交 STATUS；只在 Call 存活时为同一代次重试 STREAM_CLOSE。
 */
static int tr_rpc_try_final_close_locked(struct tr_rpc_endpoint *endpoint,
					 uint32_t slot,
					 struct tr_rpc_call_slot *call)
{
	int ret = tr_stream_close(call->stream);

	if (ret != TR_OK && ret != TR_ERR_CLOSED && ret != TR_ERR_STALE)
		return ret;
	call->need_local_close = 0;
	tr_rpc_mark_local_closed_locked(endpoint, call);
	if (ret == TR_ERR_STALE || call->remote_closed) {
		call->state = TR_RPC_CALL_TERMINAL;
		(void)tr_rpc_notify_terminal_locked(
			endpoint, slot, call,
			ret == TR_ERR_STALE ? TR_RPC_STATUS_UNAVAILABLE :
						call->final_status);
		tr_rpc_maybe_free_call_locked(endpoint, call);
	}
	return TR_OK;
}

static uint64_t tr_rpc_deadline_timer_main(void *arg, uint64_t now_ns)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	struct tr_rpc_call_handle expired_handle = { 0 };
	struct tr_rpc_cancel_request request;
	uint64_t next_deadline_ns = 0U;
	uint32_t i;
	int expired = 0;
	int retry = 0;

	if (now_ns == 0U)
		now_ns = tr_rpc_now_ns();

	pthread_mutex_lock(&endpoint->lock);
	if (endpoint->deadline_stopping) {
		pthread_mutex_unlock(&endpoint->lock);
		return 0;
	}

	if (endpoint->final_close_retry_deadline_ns != 0U &&
	    endpoint->final_close_retry_deadline_ns <= now_ns) {
		endpoint->final_close_retry_deadline_ns = 0U;
		for (i = 0; i < endpoint->config.max_calls; ++i) {
			struct tr_rpc_call_slot *call = &endpoint->calls[i];
			int ret;

			if (call->state == TR_RPC_CALL_FREE)
				continue;
			if ((call->cancelled || call->pending_control) &&
			    (call->pending_control || call->need_local_close)) {
				struct tr_rpc_call_handle handle =
				        tr_rpc_make_call_handle(endpoint, i,
				                                call);

				ret = tr_rpc_try_cancel_send_locked(endpoint,
				                                    call);
				call = tr_rpc_lookup_call_handle_locked(handle);
				if (!call)
					continue;
				if (call->executor_overloaded &&
				    call->local_closed && call->remote_closed &&
				    call->state != TR_RPC_CALL_TERMINAL) {
					call->state = TR_RPC_CALL_TERMINAL;
					(void)tr_rpc_notify_terminal_locked(
					        endpoint, i, call,
					        call->final_status);
				}
				tr_rpc_maybe_free_call_locked(endpoint, call);
			} else if (call->is_unary &&
			           (call->unary_failure_pending ||
			            call->need_local_close) &&
			           (call->pending_tx ||
			            call->need_local_close)) {
				struct tr_rpc_call_handle handle =
					tr_rpc_make_call_handle(endpoint, i, call);

				ret = tr_rpc_try_unary_send_locked(endpoint, call);
				/* 同步中止的回调可能退役并复用槽位；重新校验代次。
				 */
				call = tr_rpc_lookup_call_handle_locked(handle);
				if (!call)
					continue;
				if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE) {
					call->need_local_close = 0;
					call->state = TR_RPC_CALL_TERMINAL;
					tr_rpc_maybe_free_call_locked(endpoint, call);
					ret = TR_OK;
				}
			} else if (call->final_status_sent &&
				   call->need_local_close) {
				ret = tr_rpc_try_final_close_locked(endpoint, i,
								   call);
			} else {
				continue;
			}
			if (ret != TR_OK)
				retry = 1;
		}
		if (retry)
			endpoint->final_close_retry_deadline_ns =
				UINT64_MAX - now_ns < TR_RPC_FINAL_CLOSE_RETRY_NS ?
				UINT64_MAX :
				now_ns + TR_RPC_FINAL_CLOSE_RETRY_NS;
	}

	/* 每次调用消耗一个已到期截止时间；保留现有截止时间堆排序
	 * 与定时器的回调版本契约。
	 */
	if (endpoint->deadline_heap_count != 0U) {
		uint32_t root = endpoint->deadline_heap[0];
		struct tr_rpc_call_slot *call;

		if (root < endpoint->config.max_calls) {
			call = &endpoint->calls[root];
			if (call->deadline_heap_pos == 0U &&
			    call->deadline_ns != 0U &&
			    call->deadline_ns <= now_ns) {
				expired_handle =
					tr_rpc_make_call_handle(endpoint, root, call);
				(void)tr_rpc_deadline_update_locked(
					endpoint, call, 0U);
				expired = 1;
			}
		}
	}
	/* 反应器的定时器队列已经从堆摘下当前到期条目。
	 * 回调直接返回下一绝对期限，由 timer_queue_run_due()
	 * 自动重排。不能只调用 tr_reactor_timer_arm() 并丢弃其错误：
	 * 一次注入的定时设置失败会使剩余 Call 永远无人唤醒。
	 *
	 * 后续取消若自行成功设置或注销定时器，定时器队列的
	 * 版本屏障会忽略此返回值；若它的重设失败，当前
	 * 回调的返回值仍可安全恢复后继截止时间。
	 */
	if (endpoint->deadline_heap_count != 0U)
		next_deadline_ns =
			endpoint->calls[endpoint->deadline_heap[0]].deadline_ns;
	if (endpoint->final_close_retry_deadline_ns != 0U &&
	    (next_deadline_ns == 0U ||
	     endpoint->final_close_retry_deadline_ns < next_deadline_ns))
		next_deadline_ns = endpoint->final_close_retry_deadline_ns;
	pthread_mutex_unlock(&endpoint->lock);

	if (expired) {
		request.handle = expired_handle;
		request.status = TR_RPC_STATUS_DEADLINE_EXCEEDED;
		(void)tr_rpc_cancel_on_owner(&request);
	}
	return next_deadline_ns;
}

static int tr_rpc_deadline_init(struct tr_rpc_endpoint *endpoint)
{
	struct tr_reactor *reactor;
	int ret;

	reactor = tr_channel_reactor(endpoint->channel);
	if (!reactor)
		return TR_ERR_STATE;

	endpoint->deadline_stopping = 0;
	ret = tr_reactor_timer_register(reactor, tr_rpc_deadline_timer_main,
					 endpoint, &endpoint->deadline_timer);
	if (ret != TR_OK)
		return ret;
	endpoint->deadline_timer_registered = 1;
	return TR_OK;
}

static int tr_rpc_deadline_destroy(struct tr_rpc_endpoint *endpoint)
{
	struct tr_reactor_timer_handle timer;
	int ret;

	if (!endpoint->deadline_timer_registered)
		return TR_OK;

	pthread_mutex_lock(&endpoint->lock);
	endpoint->deadline_stopping = 1;
	timer = endpoint->deadline_timer;
	pthread_mutex_unlock(&endpoint->lock);

	/* 注销是同步回调生命周期屏障。在该屏障真正成功前，
	 * 保持本地注册发布完整；正在停止的反应器可能返回
	 * CLOSED，必须保留可重试性。
	 */
	ret = tr_reactor_timer_unregister(timer);
	if (ret != TR_OK && ret != TR_ERR_STALE)
		return ret;

	pthread_mutex_lock(&endpoint->lock);
	endpoint->deadline_timer_registered = 0;
	memset(&endpoint->deadline_timer, 0, sizeof(endpoint->deadline_timer));
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

/* 接收处理可能跨越期限，而本轮定时器预算尚未再次执行。 */
static int tr_rpc_response_deadline_locked(const struct tr_rpc_call_slot *call)
{
	uint64_t now;

	if (call->deadline_ns == 0U)
		return TR_OK;
	now = tr_rpc_now_ns();
	if (now == 0U)
		return TR_ERR_SYS;
	return now >= call->deadline_ns ? TR_ERR_TIMEOUT : TR_OK;
}

static void tr_rpc_reject_response_deadline_on_owner(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_call_handle handle,
	enum tr_lane lane, int error)
{
	if (error == TR_ERR_TIMEOUT)
		(void)tr_rpc_cancel_internal(handle, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	else
		/* 时钟不可靠时，复用已有 UNAVAILABLE 连接错误路径。 */
		(void)tr_channel_abort_lane_on_owner(endpoint->channel, lane, error);
}

static enum tr_stream_data_disposition
tr_rpc_on_data(struct tr_stream_handle stream, uint64_t message_id,
	       struct tr_buffer *payload, void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	struct tr_rpc_wire_header wire;
	const uint8_t *metadata = NULL;
	const uint8_t *body = NULL;
	uint16_t metadata_len = 0;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_task task;
	uint32_t slot = 0;
	int first_message = 0;
	int ret;

	(void)message_id;
	if (!payload)
		return TR_STREAM_DATA_RELEASE;

	ret = tr_rpc_wire_decode_ex(payload->data, payload->len, &wire,
				    &metadata, &metadata_len, &body);
	if (ret != TR_OK) {
		(void)tr_stream_close(stream);
		return TR_STREAM_DATA_RELEASE;
	}
	(void)body;

	memset(&task, 0, sizeof(task));
	task.payload = payload;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_find_call_by_stream_locked(endpoint, stream, &slot);

	if (call &&
	    (call->admission_rejected || call->executor_overloaded) &&
	    (wire.type == TR_RPC_WIRE_REQUEST ||
	     (call->executor_overloaded && wire.type == TR_RPC_WIRE_CANCEL))) {
		/* 拒绝路径负责最终 STATUS 与本地半关闭的排序。
		 * 对端可能在观察到 STATUS 前并发发送额外请求数据；
		 * 丢弃它，不要在待处理拒绝帧之前插入提前关闭。
		 */
		pthread_mutex_unlock(&endpoint->lock);
		return TR_STREAM_DATA_RELEASE;
	}

	if (wire.type == TR_RPC_WIRE_REQUEST) {
		enum tr_rpc_cardinality cardinality;
		int hook_status = TR_RPC_STATUS_OK;

		if (endpoint->config.role != TR_RPC_SERVER ||
		    wire.status != 0) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (!call) {
			enum tr_lane actual_lane;

			method = tr_rpc_find_method_locked(
				endpoint, wire.service_id, wire.method_id);
			if (!method ||
			    method->handler_kind == TR_RPC_HANDLER_NONE ||
			    tr_stream_get_lane_internal(stream, &actual_lane) != TR_OK ||
			    (uint32_t)actual_lane != method->desc.lane ||
			    tr_rpc_allocate_call_locked(endpoint, &call,
							&slot) != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}

			call->state = TR_RPC_CALL_ACTIVE;
			call->stream = stream;
			call->method = method;
			call->is_unary = method->handler_kind ==
					 TR_RPC_HANDLER_UNARY;
			ret = tr_rpc_bind_call_stream_locked(endpoint, slot, stream);
			if (ret != TR_OK) {
				tr_rpc_free_call_locked(endpoint, call);
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			first_message = 1;
		} else {
			method = call->method;
		}

		if (!method || call->cancelled ||
		    wire.service_id != method->desc.service_id ||
		    wire.method_id != method->desc.method_id ||
		    wire.codec_id != method->desc.request_codec_id ||
		    wire.codec_id != TR_RPC_CODEC_RAW ||
		    wire.payload_len > method->desc.max_request_bytes ||
		    (!first_message && metadata_len != 0)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (first_message) {
			ret = tr_rpc_import_peer_metadata_locked(endpoint, call,
								 wire.type,
								 metadata,
								 metadata_len);
			if (ret != TR_OK) {
				tr_rpc_free_call_locked(endpoint, call);
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		if (!first_message && !call->is_unary &&
		    call->pending_executor_task_valid) {
			ret = tr_rpc_fail_stream_midstream_overload_locked(
				endpoint, slot, call,
				TR_RPC_STATUS_RESOURCE_EXHAUSTED);
			pthread_mutex_unlock(&endpoint->lock);
			if (ret != TR_OK)
				(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		cardinality = method->desc.request_cardinality;
		if (tr_rpc_validate_cardinality(cardinality, call->rx_count) !=
		    TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		call->rx_count++;

		if (first_message) {
			tr_rpc_semantic_start_locked(endpoint, call);
			ret = tr_rpc_run_interceptor_locked(
				endpoint, slot, call,
				TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER,
				TR_RPC_STATUS_OK, &hook_status);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			call = &endpoint->calls[slot];
			if (hook_status != TR_RPC_STATUS_OK) {
				if (call->is_unary)
					ret = tr_rpc_reject_unary_locked(
						endpoint, call, hook_status);
				else
					ret = tr_rpc_reject_stream_admission_locked(
						endpoint, call, hook_status);
				pthread_mutex_unlock(&endpoint->lock);
				if (ret != TR_OK)
					(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		task.first_message = (uint16_t)first_message;
		task.type = call->is_unary ? TR_RPC_TASK_SERVER_UNARY :
					     TR_RPC_TASK_SERVER_STREAM_MESSAGE;
		ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		if (ret == TR_AGAIN && first_message) {
			/* 首个执行器任务获准前，尚未运行任何服务端处理函数。
			 * 对一元与流式 Call
			 * 都显式报告有界准入压力，不使连接失效。
			 */
			if (call->is_unary)
				ret = tr_rpc_reject_unary_locked(
					endpoint, call,
					TR_RPC_STATUS_RESOURCE_EXHAUSTED);
			else
				ret = tr_rpc_reject_stream_admission_locked(
					endpoint, call,
					TR_RPC_STATUS_RESOURCE_EXHAUSTED);
			pthread_mutex_unlock(&endpoint->lock);
			if (ret != TR_OK)
				(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		if (ret == TR_AGAIN && !first_message && !call->is_unary) {
			ret = tr_rpc_store_pending_executor_task_locked(
				endpoint, call, &task);
			pthread_mutex_unlock(&endpoint->lock);
			if (ret == TR_OK)
				return TR_STREAM_DATA_TAKE_OWNERSHIP;
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK) {
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}

	if (wire.type == TR_RPC_WIRE_RESPONSE) {
		enum tr_rpc_cardinality cardinality;
		int first_response;

		if (endpoint->config.role != TR_RPC_CLIENT || !call ||
		    !call->method || call->cancelled ||
		    call->final_status_seen) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		method = call->method;
		first_response = call->rx_count == 0;
		if (wire.service_id != method->desc.service_id ||
		    wire.method_id != method->desc.method_id ||
		    wire.codec_id != method->desc.response_codec_id ||
		    wire.codec_id != TR_RPC_CODEC_RAW ||
		    wire.payload_len > method->desc.max_response_bytes ||
		    (!first_response && metadata_len != 0)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (first_response) {
			ret = tr_rpc_import_peer_metadata_locked(endpoint, call,
								 wire.type,
								 metadata,
								 metadata_len);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
		}

		cardinality = method->desc.response_cardinality;
		if (tr_rpc_validate_cardinality(cardinality, call->rx_count) !=
		    TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		ret = tr_rpc_response_deadline_locked(call);
		if (ret != TR_OK) {
			struct tr_rpc_call_handle handle =
				tr_rpc_make_call_handle(endpoint, slot, call);
			enum tr_lane lane = (enum tr_lane)call->method->desc.lane;

			pthread_mutex_unlock(&endpoint->lock);
			tr_rpc_reject_response_deadline_on_owner(endpoint, handle, lane, ret);
			return TR_STREAM_DATA_RELEASE;
		}
		call->rx_count++;

		task.call = tr_rpc_make_call_handle(endpoint, slot, call);
		if (call->is_unary) {
			if (!tr_rpc_status_valid(wire.status) ||
			    call->response_received || call->result_delivered) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			call->response_received = 1;
			ret = tr_rpc_run_interceptor_locked(
				endpoint, slot, call,
				TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
				wire.status, NULL);
			if (ret != TR_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			call = &endpoint->calls[slot];
			(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
			task.type = TR_RPC_TASK_CLIENT_UNARY_RESULT;
		} else {
			if (wire.status != TR_RPC_STATUS_OK) {
				pthread_mutex_unlock(&endpoint->lock);
				(void)tr_stream_close(stream);
				return TR_STREAM_DATA_RELEASE;
			}
			task.type = TR_RPC_TASK_CLIENT_MESSAGE;
		}

		ret = call->is_unary ?
			tr_rpc_queue_terminal_task_locked(endpoint, call, &task) :
			tr_rpc_queue_task_locked(endpoint, call, &task);
		if (ret == TR_OK && call->is_unary) {
			call->result_delivered = 1;
			tr_rpc_semantic_finish_locked(endpoint, call, wire.status);
		}
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK) {
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}

	if (wire.type == TR_RPC_WIRE_STATUS) {
		if (endpoint->config.role != TR_RPC_CLIENT ||
		    tr_rpc_validate_stream_status_locked(call, &wire) != TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		/* STATUS 元数据始终是尾部元数据，无论其前是否有 RESPONSE 消息。
		 * 这有意与首个响应的初始元数据分开。
		 */
		ret = tr_rpc_import_peer_metadata_locked(
			endpoint, call, wire.type, metadata, metadata_len);
		if (ret != TR_OK) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		ret = tr_rpc_response_deadline_locked(call);
		if (ret != TR_OK) {
			struct tr_rpc_call_handle handle =
				tr_rpc_make_call_handle(endpoint, slot, call);
			enum tr_lane lane = (enum tr_lane)call->method->desc.lane;

			pthread_mutex_unlock(&endpoint->lock);
			tr_rpc_reject_response_deadline_on_owner(endpoint, handle, lane, ret);
			return TR_STREAM_DATA_RELEASE;
		}
		call->final_status_seen = 1;
		call->final_status = wire.status;
		(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
		/* 正常 STATUS、取消、截止时间、连接失败都必须经过
		 * 同一个终止辅助函数：它同时线性化 FINISHED 的仅一次发生
		 * 与 calls_completed 可观测指标。
		 */
		ret = tr_rpc_notify_terminal_locked(
			endpoint, slot, call, wire.status);
		pthread_mutex_unlock(&endpoint->lock);
		if (ret != TR_OK)
			(void)tr_stream_close(stream);
		return TR_STREAM_DATA_RELEASE;
	}

	if (wire.type == TR_RPC_WIRE_CANCEL) {
		int cancel_status = wire.status;
		struct tr_rpc_call_handle handle;

		if (!call || !call->method || metadata_len != 0 ||
		    wire.payload_len != 0 ||
		    wire.service_id != call->method->desc.service_id ||
		    wire.method_id != call->method->desc.method_id ||
		    (cancel_status != TR_RPC_STATUS_CANCELLED &&
		     cancel_status != TR_RPC_STATUS_DEADLINE_EXCEEDED)) {
			pthread_mutex_unlock(&endpoint->lock);
			(void)tr_stream_close(stream);
			return TR_STREAM_DATA_RELEASE;
		}

		if (!call->cancelled) {
			call->cancelled = 1;
			call->cancel_status = cancel_status;
			tr_rpc_drop_pending_executor_task_locked(call);
			tr_rpc_executor_mark_cancelled(
				endpoint,
				tr_rpc_make_call_handle(endpoint, slot, call));
			(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
			call->final_status_seen = 1;
			call->final_status = cancel_status;
			call->state = TR_RPC_CALL_TERMINAL;
			tr_rpc_semantic_finish_locked(
				endpoint, call, cancel_status);
			(void)tr_rpc_notify_terminal_locked(
				endpoint, slot, call, cancel_status);
		}
		handle = tr_rpc_make_call_handle(endpoint, slot, call);
		call->need_local_close = 1;
		(void)tr_rpc_try_cancel_send_locked(endpoint, call);
		call = tr_rpc_lookup_call_handle_locked(handle);
		if (call)
			tr_rpc_maybe_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return TR_STREAM_DATA_RELEASE;
	}

	pthread_mutex_unlock(&endpoint->lock);
	(void)tr_stream_close(stream);
	return TR_STREAM_DATA_RELEASE;
}

static void tr_rpc_on_stream_event(struct tr_stream_handle stream,
				   enum tr_stream_event event, int status,
				   void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_task task;
	uint32_t slot = 0;
	int ret = TR_OK;

	memset(&task, 0, sizeof(task));
	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_find_call_by_stream_locked(endpoint, stream, &slot);

	if (!call) {
		int close_unbound =
			event == TR_STREAM_EVENT_REMOTE_CLOSED;

		pthread_mutex_unlock(&endpoint->lock);
		/* V1 在首个 REQUEST 帧中标识 RPC Method。如果对端在发布该帧前
		 * 半关闭（例如零消息的 MANY 请求，或首个请求前取消），
		 * 就没有 RPC Call 来拥有该流。关闭我们这一半，避免 Channel
		 * 槽位泄漏。
		 */
		if (close_unbound)
			(void)tr_stream_close(stream);
		return;
	}

	if (event == TR_STREAM_EVENT_OPENED) {
		if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
			struct tr_rpc_call_handle handle =
			        tr_rpc_make_call_handle(endpoint, slot, call);

			(void)tr_rpc_try_cancel_send_locked(endpoint, call);
			call = tr_rpc_lookup_call_handle_locked(handle);
			if (call)
				tr_rpc_maybe_free_call_locked(endpoint, call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		call->state = TR_RPC_CALL_ACTIVE;
		if (call->is_unary && endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call, TR_RPC_CALL_EVENT_OPENED,
				TR_RPC_STATUS_OK);
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_WRITABLE) {
		/* 最终 STATUS 是应用终止屏障。关闭事件被消耗前，传输层
		 * 仍可能报告可写，但 STATUS 之后不再允许新的应用延续任务。
		 */
		if (call->final_status_sent && call->need_local_close) {
			ret = tr_rpc_try_final_close_locked(endpoint, slot, call);
			if (ret == TR_AGAIN)
				ret = tr_rpc_schedule_final_close_retry_locked(endpoint, call);
			pthread_mutex_unlock(&endpoint->lock);
			(void)ret;
			return;
		}
		if ((call->final_status_seen || call->final_status_sent) &&
		    !call->cancelled && !call->pending_control) {
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		if (call->cancelled || call->pending_control) {
			struct tr_rpc_call_handle handle =
			        tr_rpc_make_call_handle(endpoint, slot, call);

			ret = tr_rpc_try_cancel_send_locked(endpoint, call);
			call = tr_rpc_lookup_call_handle_locked(handle);
			if (!call) {
				pthread_mutex_unlock(&endpoint->lock);
				return;
			}
			if (call->executor_overloaded && call->local_closed &&
			    call->remote_closed &&
			    call->state != TR_RPC_CALL_TERMINAL) {
				call->state = TR_RPC_CALL_TERMINAL;
				(void)tr_rpc_notify_terminal_locked(
					endpoint, slot, call, call->final_status);
			}
			tr_rpc_maybe_free_call_locked(endpoint, call);
		} else if (call->is_unary)
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (endpoint->config.role == TR_RPC_CLIENT)
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_WRITABLE, TR_RPC_STATUS_OK);
		else if (call->method &&
			 call->method->handler_kind == TR_RPC_HANDLER_STREAM) {
			task.type = TR_RPC_TASK_SERVER_WRITABLE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			ret = tr_rpc_queue_task_locked(endpoint, call, &task);
		}
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		call->remote_closed = 1;
		if (call->admission_rejected) {
			if (call->local_closed && !call->pending_control &&
			    !call->need_local_close)
				call->state = TR_RPC_CALL_TERMINAL;
			tr_rpc_maybe_free_call_locked(endpoint, call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		if (call->cancelled) {
			tr_rpc_maybe_free_call_locked(endpoint, call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		if (call->executor_overloaded) {
			if (call->local_closed && !call->pending_control &&
			    !call->need_local_close) {
				call->state = TR_RPC_CALL_TERMINAL;
				(void)tr_rpc_notify_terminal_locked(
					endpoint, slot, call, call->final_status);
				tr_rpc_maybe_free_call_locked(endpoint, call);
			}
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}
		if (!call->is_unary && endpoint->config.role == TR_RPC_SERVER) {
			if (call->pending_executor_task_valid) {
				call->pending_remote_half_close = 1;
				pthread_mutex_unlock(&endpoint->lock);
				return;
			}
			task.type = TR_RPC_TASK_SERVER_HALF_CLOSE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			ret = tr_rpc_queue_task_locked(endpoint, call, &task);
			if (ret == TR_AGAIN)
				ret = tr_rpc_store_pending_executor_task_locked(
					endpoint, call, &task);
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_CLIENT &&
			   !call->final_status_seen) {
			/* FINISHED
			 * 是应用终止屏障。正常服务端在关闭响应半侧前发送
			 * STATUS， 因此后续传输半关闭不得在 FINISHED
			 * 之后产生事件。 REMOTE_CLOSED 仅在观察到最终 STATUS
			 * 前对端已半关闭时仍有用。
			 */
			ret = tr_rpc_queue_client_event_locked(
				endpoint, slot, call,
				TR_RPC_CALL_EVENT_REMOTE_CLOSED,
				TR_RPC_STATUS_OK);
		}
		pthread_mutex_unlock(&endpoint->lock);
		(void)ret;
		return;
	}

	if (event == TR_STREAM_EVENT_ERROR || event == TR_STREAM_EVENT_CLOSED) {
		int terminal_status = call->executor_overloaded ?
					      call->final_status :
					      (status == TR_OK ?
						       TR_RPC_STATUS_OK :
						       TR_RPC_STATUS_UNAVAILABLE);

		/* 没有完整 RPC 结果的正常传输 EOF 仍是 UNAVAILABLE。 */
		if (endpoint->config.role == TR_RPC_CLIENT &&
		    ((call->is_unary && !call->result_delivered) ||
		     (!call->is_unary && !call->final_status_seen)))
			terminal_status = TR_RPC_STATUS_UNAVAILABLE;

		/* 流生命周期在此结束，即使 Call 生命周期还因执行器回调或
		 * task_refs 排空而继续。立即释放流槽位索引，使 Channel
		 * 可以为新代次复用该槽位。
		 */
		tr_rpc_unbind_call_stream_locked(endpoint, slot, call);

		(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
		tr_rpc_drop_pending_executor_task_locked(call);
		if (call->cancelled) {
			if (call->pending_control) {
				tr_buffer_release(call->pending_control);
				call->pending_control = NULL;
			}
			call->need_local_close = 0;
			tr_rpc_mark_local_closed_locked(endpoint, call);
			call->state = TR_RPC_CALL_TERMINAL;
			tr_rpc_maybe_free_call_locked(endpoint, call);
			pthread_mutex_unlock(&endpoint->lock);
			return;
		}

		if (call->is_unary && endpoint->config.role == TR_RPC_CLIENT &&
		    !call->result_delivered) {
			ret = tr_rpc_run_interceptor_locked(
				endpoint, slot, call,
				TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
				TR_RPC_STATUS_UNAVAILABLE, NULL);
			call = &endpoint->calls[slot];
			if (ret == TR_OK &&
			    tr_rpc_queue_unary_failure_locked(
				    endpoint, slot, call,
				    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
				call->terminal_notified = 1;
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_CLIENT &&
			   !call->final_status_seen && !call->terminal_notified) {
			ret = tr_rpc_run_interceptor_locked(
				endpoint, slot, call,
				TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
				TR_RPC_STATUS_UNAVAILABLE, NULL);
			call = &endpoint->calls[slot];
			if (ret == TR_OK &&
			    tr_rpc_queue_client_terminal_event_locked(
				    endpoint, slot, call,
				    TR_RPC_CALL_EVENT_ERROR,
				    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
				call->terminal_notified = 1;
		} else if (!call->is_unary &&
			   endpoint->config.role == TR_RPC_SERVER &&
			   call->method &&
			   call->method->handler_kind ==
				   TR_RPC_HANDLER_STREAM &&
			   !call->terminal_notified &&
			   !call->admission_rejected &&
			   !call->executor_overloaded) {
			task.type = TR_RPC_TASK_SERVER_CLOSE;
			task.call =
				tr_rpc_make_call_handle(endpoint, slot, call);
			task.status = terminal_status;
			ret = tr_rpc_queue_terminal_task_locked(endpoint, call, &task);
			if (ret == TR_OK)
				call->terminal_notified = 1;
		}

		tr_rpc_semantic_finish_locked(endpoint, call, terminal_status);
		call->state = TR_RPC_CALL_TERMINAL;
		tr_rpc_maybe_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return;
	}

	pthread_mutex_unlock(&endpoint->lock);
}

static void tr_rpc_on_channel_event(struct tr_channel *channel,
				    enum tr_channel_event event, int status,
				    void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	enum tr_lane failed_lane;
	uint32_t i;
	int ret;

	(void)channel;
	(void)status;
	if (event != TR_CHANNEL_EVENT_CONTROL_DOWN &&
	    event != TR_CHANNEL_EVENT_BULK_DOWN)
		return;
	failed_lane = event == TR_CHANNEL_EVENT_BULK_DOWN ? TR_LANE_BULK :
							    TR_LANE_CONTROL;

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		struct tr_rpc_task task;

		if (call->state == TR_RPC_CALL_FREE || !call->method ||
		    call->method->desc.lane != (uint32_t)failed_lane)
			continue;

		/* 失败的 Channel 通道已不再有存活流事件源。
		 * 终止 Call 回调排空前先解绑槽位索引。
		 */
		tr_rpc_unbind_call_stream_locked(endpoint, i, call);
		(void)tr_rpc_deadline_update_locked(endpoint, call, 0U);
		tr_rpc_drop_pending_executor_task_locked(call);
		call->state = TR_RPC_CALL_TERMINAL;
		call->need_local_close = 0;
		tr_rpc_mark_local_closed_locked(endpoint, call);
		if (call->pending_control) {
			tr_buffer_release(call->pending_control);
			call->pending_control = NULL;
		}

		if (endpoint->config.role == TR_RPC_CLIENT) {
			ret = tr_rpc_run_interceptor_locked(
				endpoint, i, call,
				TR_RPC_INTERCEPTOR_CLIENT_POST_CALL,
				TR_RPC_STATUS_UNAVAILABLE, NULL);
			call = &endpoint->calls[i];
			if (ret == TR_OK && call->is_unary) {
				if (tr_rpc_queue_unary_failure_locked(
					    endpoint, i, call,
					    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
					call->terminal_notified = 1;
			} else if (ret == TR_OK && !call->terminal_notified) {
				if (tr_rpc_queue_client_terminal_event_locked(
					    endpoint, i, call,
					    TR_RPC_CALL_EVENT_ERROR,
					    TR_RPC_STATUS_UNAVAILABLE) == TR_OK)
					call->terminal_notified = 1;
			}
		} else if (!call->is_unary &&
			   call->method->handler_kind ==
				   TR_RPC_HANDLER_STREAM &&
			   !call->terminal_notified &&
			   !call->admission_rejected) {
			memset(&task, 0, sizeof(task));
			task.type = TR_RPC_TASK_SERVER_CLOSE;
			task.call = tr_rpc_make_call_handle(endpoint, i, call);
			task.status = TR_RPC_STATUS_UNAVAILABLE;
			if (tr_rpc_queue_terminal_task_locked(endpoint, call,
							&task) == TR_OK)
				call->terminal_notified = 1;
		}

		tr_rpc_semantic_finish_locked(
			endpoint, call, TR_RPC_STATUS_UNAVAILABLE);
		tr_rpc_maybe_free_call_locked(endpoint, call);
	}
	tr_rpc_deadline_rearm_locked(endpoint);
	pthread_mutex_unlock(&endpoint->lock);
}

struct tr_rpc_endpoint_build {
	struct tr_rpc_endpoint *endpoint;
	int lock_ready;
	int ref_lock_ready;
	int ref_cond_ready;
	int executor_ready;
};

struct tr_rpc_endpoint_publish_request {
	struct tr_rpc_endpoint *endpoint;
	int recovery_required;
};

static int tr_rpc_publish_on_owner(void *arg)
{
	struct tr_rpc_endpoint_publish_request *request = arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	int rollback_ret;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;

	/* 截止时间定时器与 Channel callback_arg 都属于异步发布。
	 * 二者必须在同一个反应器所有者处理轮次内形成一个事务，
	 * 避免停止或事件插入发布与回滚之间。
	 */
	ret = tr_rpc_deadline_init(endpoint);
	if (ret != TR_OK)
		return ret;

	ret = tr_channel_set_handler(
		endpoint->channel, tr_rpc_on_data,
		tr_rpc_on_stream_event, tr_rpc_on_channel_event, endpoint);
	if (ret == TR_OK)
		return TR_OK;

	rollback_ret = tr_rpc_deadline_destroy(endpoint);
	/* 定时器注销失败时仍可引用 Endpoint。构造所有者必须保留
	 * 本对象，直到显式 destroy 重试成功，不能交给 TR_AUTO 释放。
	 */
	if (rollback_ret != TR_OK)
		request->recovery_required = 1;
	return rollback_ret != TR_OK ? rollback_ret : ret;
}

static void tr_rpc_endpoint_build_cleanup(struct tr_rpc_endpoint_build *build)
{
	struct tr_rpc_endpoint *endpoint;

	if (!build || !build->endpoint)
		return;
	endpoint = build->endpoint;

	/* 构造清理只处理从未发布给异步事件源的本地资源。
	 * 定时器与处理函数发布必须由 publish_on_owner() 在返回前自行收敛；
	 * 工作线程只有在构造所有权已转移给 *out 之后才允许启动。
	 */
	if (build->executor_ready)
		tr_rpc_executor_release(endpoint);

	free(endpoint->call_by_stream_slot);
	free(endpoint->method_index);
	free(endpoint->deadline_heap);
	free(endpoint->calls);
	free(endpoint->methods);
	if (build->ref_cond_ready)
		pthread_cond_destroy(&endpoint->ref_cond);
	if (build->ref_lock_ready)
		pthread_mutex_destroy(&endpoint->ref_lock);
	if (build->lock_ready)
		pthread_mutex_destroy(&endpoint->lock);
	free(endpoint);
	build->endpoint = NULL;
}

int tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel, const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group, struct tr_rpc_endpoint **out)
{
	struct tr_rpc_endpoint_build build
		TR_AUTO(tr_rpc_endpoint_build_cleanup) = { 0 };
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_endpoint_publish_request req;
	int ret;

	if (!channel || !config || !out || !config->message_pool ||
	    (config->role != TR_RPC_CLIENT && config->role != TR_RPC_SERVER) ||
	    config->max_methods == 0 || config->max_calls == 0 ||
	    (config->observability_flags & ~TR_OBSERVABILITY_VALID_FLAGS))
		return TR_ERR_INVALID;

	*out = NULL;
	endpoint = (struct tr_rpc_endpoint *)calloc(1, sizeof(*endpoint));
	if (!endpoint)
		return TR_ERR_NOMEM;
	build.endpoint = endpoint;

	if (pthread_mutex_init(&endpoint->lock, NULL) != 0)
		return TR_ERR_INVALID;
	build.lock_ready = 1;
	if (pthread_mutex_init(&endpoint->ref_lock, NULL) != 0)
		return TR_ERR_INVALID;
	build.ref_lock_ready = 1;
	if (pthread_cond_init(&endpoint->ref_cond, NULL) != 0)
		return TR_ERR_INVALID;
	build.ref_cond_ready = 1;
	if (tr_refcount_init(&endpoint->refs, 1U) != TR_OK)
		return TR_ERR_STATE;

	endpoint->methods = (struct tr_rpc_method_entry *)calloc(
		config->max_methods, sizeof(*endpoint->methods));
	endpoint->method_index_capacity =
		tr_rpc_method_index_capacity_for(config->max_methods);
	if (endpoint->method_index_capacity == 0U)
		return TR_ERR_INVALID;
	endpoint->method_index = (struct tr_rpc_method_index_entry *)calloc(
		endpoint->method_index_capacity, sizeof(*endpoint->method_index));
	endpoint->calls = (struct tr_rpc_call_slot *)calloc(
		config->max_calls, sizeof(*endpoint->calls));
	endpoint->deadline_heap = (uint32_t *)calloc(
		config->max_calls, sizeof(*endpoint->deadline_heap));
	endpoint->stream_slot_capacity = tr_channel_max_streams(channel);
	endpoint->call_by_stream_slot = (uint32_t *)calloc(
		endpoint->stream_slot_capacity,
		sizeof(*endpoint->call_by_stream_slot));
	if (!endpoint->methods || !endpoint->method_index || !endpoint->calls ||
	    !endpoint->deadline_heap || !endpoint->call_by_stream_slot ||
	    endpoint->stream_slot_capacity == 0U)
		return TR_ERR_NOMEM;
	{
		uint32_t i;

		endpoint->free_call_head = 0U;
		for (i = 0; i < config->max_calls; ++i) {
			endpoint->calls[i].free_next =
				(i + 1U < config->max_calls) ?
					i + 1U : TR_RPC_CALL_FREE_NONE;
			endpoint->calls[i].deadline_heap_pos =
				TR_RPC_DEADLINE_HEAP_NONE;
		}
		for (i = 0; i < endpoint->stream_slot_capacity; ++i)
			endpoint->call_by_stream_slot[i] =
				TR_RPC_STREAM_CALL_NONE;
	}

	endpoint->channel = channel;
	endpoint->config = *config;

	/* executor_init() 只创建软状态，不启动工作线程，因此即使中途失败，
	 * 构造清理仍然只做不可失败的本地资源释放。
	 */
	build.executor_ready = 1;
	ret = tr_rpc_executor_init(
		endpoint, config->executor_queue_capacity,
		config->executor_continuation_reserve, group);
	if (ret != TR_OK)
		return ret;

	req.endpoint = endpoint;
	req.recovery_required = 0;
	ret = tr_rpc_owner_call(endpoint, tr_rpc_publish_on_owner, &req);
	if (ret != TR_OK) {
		/* 仅异步定时器仍可能持有 Endpoint 时转移部分构造对象。
		 * 失败本身不能改变普通未发布资源的词法清理契约。
		 */
		if (req.recovery_required) {
			*out = endpoint;
			build.endpoint = NULL;
		}
		return ret;
	}

	/* 至此 Endpoint 存储、定时器与处理函数已构造完成，所有权先交给调用方。
	 * 独立工作线程启动发生在所有权转移之后，因此部分启动
	 * 无论后续线程回收是否失败，都有一个可见 Endpoint 可以显式销毁或重试。
	 */
	*out = endpoint;
	build.endpoint = NULL;

	ret = tr_rpc_executor_start(endpoint);
	if (ret != TR_OK) {
		int rollback_ret = tr_rpc_endpoint_destroy(endpoint);

		if (rollback_ret == TR_OK) {
			*out = NULL;
			return ret;
		}
		/* 工作线程启动回滚自身失败时，Endpoint 保持在 *out 中；
		 * 生命周期错误优先，调用方必须继续销毁该部分构造的 Endpoint。
		 */
		return rollback_ret;
	}
	return TR_OK;
}

int tr_rpc_endpoint_create(struct tr_channel *channel,
			   const struct tr_rpc_endpoint_config *config,
			   struct tr_rpc_endpoint **out)
{
	return tr_rpc_endpoint_create_with_executor_group(
		channel, config, NULL, out);
}

static int tr_rpc_endpoint_get(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	return tr_refcount_get(&endpoint->refs);
}

static void tr_rpc_endpoint_wait_owner_only(struct tr_rpc_endpoint *endpoint)
{
	pthread_mutex_lock(&endpoint->ref_lock);
	while (tr_refcount_read(&endpoint->refs) != 1U) {
		endpoint->ref_waiters++;
		(void)pthread_cond_wait(&endpoint->ref_cond, &endpoint->ref_lock);
		endpoint->ref_waiters--;
	}
	pthread_mutex_unlock(&endpoint->ref_lock);
}

static void tr_rpc_endpoint_release(struct tr_rpc_endpoint *endpoint)
{
	tr_rpc_endpoint_detached_finalizer finalizer;
	void *finalizer_arg;
	struct tr_rpc_endpoint_stats final_stats;
	int have_finalizer;
	uint32_t i;

	if (!endpoint)
		return;

	pthread_mutex_lock(&endpoint->ref_lock);
	finalizer = endpoint->detached_finalizer;
	finalizer_arg = endpoint->detached_finalizer_arg;
	have_finalizer = finalizer != NULL;
	pthread_mutex_unlock(&endpoint->ref_lock);
	memset(&final_stats, 0, sizeof(final_stats));
	if (have_finalizer)
		(void)tr_rpc_endpoint_get_stats(endpoint, &final_stats);

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i)
		tr_rpc_free_call_locked(endpoint, &endpoint->calls[i]);
	pthread_mutex_unlock(&endpoint->lock);

	tr_rpc_executor_release(endpoint);
	free(endpoint->call_by_stream_slot);
	free(endpoint->method_index);
	free(endpoint->deadline_heap);
	free(endpoint->calls);
	free(endpoint->methods);
#ifndef NDEBUG
	pthread_mutex_lock(&endpoint->ref_lock);
	assert(endpoint->ref_waiters == 0U);
	pthread_mutex_unlock(&endpoint->ref_lock);
#endif
	pthread_cond_destroy(&endpoint->ref_cond);
	pthread_mutex_destroy(&endpoint->ref_lock);
	pthread_mutex_destroy(&endpoint->lock);
	free(endpoint);

	if (have_finalizer)
		finalizer(&final_stats, finalizer_arg);
}

static void tr_rpc_endpoint_put(struct tr_rpc_endpoint *endpoint)
{
	int last;

	if (!endpoint)
		return;

	/* 所有强引用释放都通过同一个生命周期锁线性化仅剩所有者引用等待的唤醒。
	 * 这样任务、待重试完成项等任意引用来源把 refs 从 2 降到 1 时，
	 * 都会通知同步析构函数。
	 */
	pthread_mutex_lock(&endpoint->ref_lock);
	last = tr_refcount_put(&endpoint->refs);
	/* 不把信号通知条件绑定到一次额外的引用计数读取。只要同步析构函数
	 * 正在等待，任意非最后一次释放都会让等待者重新检查 refs==1；
	 * 这样未来新增强引用来源也不会出现“2->1 唤醒遗漏”。
	 */
	if (last == 0 && endpoint->ref_waiters != 0U)
		pthread_cond_broadcast(&endpoint->ref_cond);
	pthread_mutex_unlock(&endpoint->ref_lock);

	if (last == 1)
		tr_rpc_endpoint_release(endpoint);
}

static int tr_rpc_endpoint_detach_protocol_sources(
	struct tr_rpc_endpoint *endpoint)
{
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;

	/* Channel 处理函数发布是首要回调生命周期屏障。
	 * callback_arg 仍可引用 Endpoint
	 * 存储时，不要推进定时器、执行器或引用拆除。
	 */
	ret = tr_channel_set_handler(
		endpoint->channel, NULL, NULL, NULL, NULL);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_deadline_destroy(endpoint);
	if (ret != TR_OK)
		return ret;
	return TR_OK;
}

static int tr_rpc_endpoint_detach_on_owner(void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;
	if (!endpoint->executor.group)
		return TR_ERR_STATE;

	pthread_mutex_lock(&endpoint->ref_lock);
	if (endpoint->teardown_detached) {
		pthread_mutex_unlock(&endpoint->ref_lock);
		return TR_OK;
	}
	pthread_mutex_unlock(&endpoint->ref_lock);

	/* 只有全部所有者可见的回调与定时器源消失后才发布已解绑状态。
	 * 否则屏障失败会使重试错误地观察到 Endpoint 已解绑，
	 * 而回调源仍存活。
	 */
	ret = tr_rpc_endpoint_detach_protocol_sources(endpoint);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_executor_shutdown_checked(endpoint);
	if (ret != TR_OK)
		return ret;

	pthread_mutex_lock(&endpoint->ref_lock);
	endpoint->teardown_detached = 1;
	pthread_mutex_unlock(&endpoint->ref_lock);
	return TR_OK;
}

int tr_rpc_endpoint_detach_for_finalize(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	if (!endpoint->executor.group)
		return TR_ERR_STATE;
	return tr_rpc_owner_call(endpoint, tr_rpc_endpoint_detach_on_owner,
				 endpoint);
}

int tr_rpc_endpoint_arm_detached_finalizer(
	struct tr_rpc_endpoint *endpoint,
	tr_rpc_endpoint_detached_finalizer finalizer, void *arg)
{
	if (!endpoint || !finalizer)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&endpoint->ref_lock);
	if (!endpoint->teardown_detached || endpoint->detached_finalizer) {
		pthread_mutex_unlock(&endpoint->ref_lock);
		return TR_ERR_STATE;
	}
	endpoint->detached_finalizer = finalizer;
	endpoint->detached_finalizer_arg = arg;
	pthread_mutex_unlock(&endpoint->ref_lock);
	return TR_OK;
}

void tr_rpc_endpoint_release_detached_owner(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return;

#ifndef NDEBUG
	pthread_mutex_lock(&endpoint->ref_lock);
	assert(endpoint->teardown_detached);
	assert(endpoint->detached_finalizer != NULL);
	pthread_mutex_unlock(&endpoint->ref_lock);
#endif

	tr_rpc_endpoint_put(endpoint);
}

void tr_rpc_endpoint_finalize_detached_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats)
{
	if (!endpoint)
		return;

#ifndef NDEBUG
	pthread_mutex_lock(&endpoint->ref_lock);
	assert(endpoint->teardown_detached);
	pthread_mutex_unlock(&endpoint->ref_lock);
#endif

	tr_rpc_endpoint_wait_owner_only(endpoint);

	/* 现有工作线程与任务引用现在均已消失。
	 * 释放所有者初始强引用前先捕获最终计数器。
	 */
	if (stats)
		(void)tr_rpc_endpoint_get_stats(endpoint, stats);
	tr_rpc_endpoint_put(endpoint);
}

int tr_rpc_endpoint_destroy_with_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_endpoint_stats *stats)
{
	int ret;

	if (!endpoint)
		return TR_OK;

	if (tr_reactor_in_owner_context() || tr_execution_in_worker_context())
		return TR_ERR_STATE;

	/* 每个异步事件源必须在执行器或引用拆除前同步解绑。
	 * 失败保留 Endpoint 所有权以供重试。
	 */
	ret = tr_rpc_endpoint_detach_protocol_sources(endpoint);
	if (ret != TR_OK)
		return ret;

	ret = tr_rpc_executor_shutdown_checked(endpoint);
	if (ret != TR_OK)
		return ret;
	tr_rpc_endpoint_wait_owner_only(endpoint);

	if (stats)
		(void)tr_rpc_endpoint_get_stats(endpoint, stats);

	tr_rpc_endpoint_put(endpoint);
	return TR_OK;
}

int tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint)
{
	return tr_rpc_endpoint_destroy_with_stats(endpoint, NULL);
}


static int tr_rpc_validate_method(const struct tr_rpc_method_desc *method)
{
	if (!method || method->service_id == 0 || method->method_id == 0 ||
	    (method->lane != TR_RPC_LANE_CONTROL &&
	     method->lane != TR_RPC_LANE_BULK) ||
	    method->request_codec_id != TR_RPC_CODEC_RAW ||
	    method->response_codec_id != TR_RPC_CODEC_RAW ||
	    method->max_request_bytes == 0 || method->max_response_bytes == 0)
		return TR_ERR_INVALID;

	if (method->request_cardinality != TR_RPC_ONE &&
	    method->request_cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	if (method->response_cardinality != TR_RPC_ONE &&
	    method->response_cardinality != TR_RPC_MANY)
		return TR_ERR_INVALID;
	return TR_OK;
}

struct tr_rpc_register_method_request {
	struct tr_rpc_endpoint *endpoint;
	struct tr_rpc_method_desc method;
	enum tr_rpc_handler_kind kind;
	tr_rpc_unary_handler unary_handler;
	struct tr_rpc_stream_handlers stream_handlers;
	int has_stream_handlers;
	void *handler_arg;
};

static int tr_rpc_register_method_on_owner(void *arg)
{
	struct tr_rpc_register_method_request *request =
		(struct tr_rpc_register_method_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	struct tr_rpc_method_entry *entry;
	uint32_t slot;
	int ret;

	/* Method 发布与入站 REQUEST、客户端 Call 启动共享同一个
	 * 反应器所有者排序。endpoint->lock 仍保留给统计或回退等
	 * 非所有者快照，不再用它把应用线程变成 Method 写者。
	 */
	pthread_mutex_lock(&endpoint->lock);
	if (tr_rpc_find_method_locked(
		    endpoint, request->method.service_id,
		    request->method.method_id)) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_STATE;
	}

	if (endpoint->method_count == endpoint->config.max_methods) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_AGAIN;
	}

	slot = endpoint->method_count;
	entry = &endpoint->methods[slot];
	entry->used = 1;
	entry->handler_kind = request->kind;
	entry->desc = request->method;
	entry->unary_handler = request->unary_handler;
	if (request->has_stream_handlers)
		entry->stream_handlers = request->stream_handlers;
	entry->handler_arg = request->handler_arg;

	ret = tr_rpc_method_index_insert_locked(
		endpoint, request->method.service_id,
		request->method.method_id, slot);
	if (ret != TR_OK) {
		memset(entry, 0, sizeof(*entry));
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	endpoint->method_count++;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

static int tr_rpc_register_method_internal(
	struct tr_rpc_endpoint *endpoint,
	const struct tr_rpc_method_desc *method, enum tr_rpc_handler_kind kind,
	tr_rpc_unary_handler unary_handler,
	const struct tr_rpc_stream_handlers *stream_handlers, void *handler_arg)
{
	struct tr_rpc_register_method_request request;
	int ret;

	if (!endpoint)
		return TR_ERR_INVALID;
	ret = tr_rpc_validate_method(method);
	if (ret != TR_OK)
		return ret;

	if (kind == TR_RPC_HANDLER_UNARY &&
	    (method->request_cardinality != TR_RPC_ONE ||
	     method->response_cardinality != TR_RPC_ONE || !unary_handler))
		return TR_ERR_INVALID;
	if (kind == TR_RPC_HANDLER_STREAM && !stream_handlers)
		return TR_ERR_INVALID;

	memset(&request, 0, sizeof(request));
	request.endpoint = endpoint;
	request.method = *method;
	request.kind = kind;
	request.unary_handler = unary_handler;
	request.handler_arg = handler_arg;
	if (stream_handlers) {
		request.stream_handlers = *stream_handlers;
		request.has_stream_handlers = 1;
	}

	return tr_rpc_owner_call(
		endpoint, tr_rpc_register_method_on_owner, &request);
}

int tr_rpc_register_method(struct tr_rpc_endpoint *endpoint,
			   const struct tr_rpc_method_desc *method,
			   tr_rpc_unary_handler unary_handler,
			   void *handler_arg)
{
	enum tr_rpc_handler_kind kind = TR_RPC_HANDLER_NONE;

	if (!endpoint)
		return TR_ERR_INVALID;

	if (endpoint->config.role == TR_RPC_SERVER) {
		if (!unary_handler)
			return TR_ERR_INVALID;
		kind = TR_RPC_HANDLER_UNARY;
	} else if (unary_handler) {
		return TR_ERR_INVALID;
	}

	return tr_rpc_register_method_internal(
		endpoint, method, kind, unary_handler, NULL, handler_arg);
}

int tr_rpc_register_stream_method(struct tr_rpc_endpoint *endpoint,
				  const struct tr_rpc_method_desc *method,
				  const struct tr_rpc_stream_handlers *handlers,
				  void *handler_arg)
{
	if (!endpoint || endpoint->config.role != TR_RPC_SERVER)
		return TR_ERR_INVALID;

	return tr_rpc_register_method_internal(endpoint, method,
					       TR_RPC_HANDLER_STREAM, NULL,
					       handlers, handler_arg);
}

struct tr_rpc_unary_call_owner_request {
	struct tr_rpc_endpoint *endpoint;
	uint32_t service_id;
	uint32_t method_id;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_options options;
	int has_options;
	uint64_t deadline_ns;
	tr_rpc_unary_result_cb result_cb;
	void *result_arg;
	struct tr_rpc_call_handle *out;
};

static int tr_rpc_unary_call_on_owner(void *arg)
{
	struct tr_rpc_unary_call_owner_request *request =
		(struct tr_rpc_unary_call_owner_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_call_handle handle;
	struct tr_buffer *request_buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	const struct tr_rpc_call_options *options =
		request->has_options ? &request->options : NULL;
	uint32_t slot;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	method = tr_rpc_find_method_locked(endpoint, request->service_id,
					   request->method_id);
	if (!method || method->desc.request_cardinality != TR_RPC_ONE ||
	    method->desc.response_cardinality != TR_RPC_ONE ||
	    request->request.len > method->desc.max_request_bytes) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_INVALID;
	}

	ret = tr_rpc_allocate_call_locked(endpoint, &call, &slot);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	call->state = TR_RPC_CALL_OPENING;
	call->method = method;
	call->is_unary = 1;
	call->result_cb = request->result_cb;
	call->result_arg = request->result_arg;

	ret = tr_rpc_apply_options_locked(endpoint, call, options,
					  request->deadline_ns);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	handle = tr_rpc_make_call_handle(endpoint, slot, call);
	ret = tr_rpc_run_interceptor_locked(
		endpoint, slot, call, TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL,
		TR_RPC_STATUS_OK, NULL);
	if (ret != TR_OK) {
		call = &endpoint->calls[slot];
		if (call->state != TR_RPC_CALL_FREE &&
		    call->generation == tr_rpc_call_handle_generation(handle))
			tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_REQUEST,
				    &method->desc,
				    method->desc.request_codec_id,
				    TR_RPC_STATUS_OK, &request->request,
				    &request_buffer);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}
	call->pending_tx = tr_buffer_take(&request_buffer);

	ret = tr_stream_open(endpoint->channel, (enum tr_lane)method->desc.lane,
			     &call->stream);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}
	ret = tr_rpc_bind_call_stream_locked(endpoint, slot, call->stream);
	if (ret != TR_OK) {
		enum tr_lane lane = (enum tr_lane)method->desc.lane;

		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		/* 未确认的 OPEN 无法单向关闭；绑定失败后已没有 Call 承接重试。
		 */
		(void)tr_channel_abort_lane_on_owner(
		        endpoint->channel, lane, ret < 0 ? ret : TR_ERR_STATE);
		return ret;
	}

	tr_rpc_semantic_start_locked(endpoint, call);
	*request->out = handle;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

int tr_rpc_unary_call_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id, const struct tr_rpc_bytes *request,
			 const struct tr_rpc_call_options *options,
			 tr_rpc_unary_result_cb result_cb, void *result_arg,
			 struct tr_rpc_call_handle *out)
{
	struct tr_rpc_unary_call_owner_request owner_request;

	if (!endpoint || !request || !out ||
	    endpoint->config.role != TR_RPC_CLIENT ||
	    (request->len != 0 && !request->data) ||
	    (options && options->metadata_count != 0 && !options->metadata))
		return TR_ERR_INVALID;

	memset(&owner_request, 0, sizeof(owner_request));
	owner_request.endpoint = endpoint;
	owner_request.service_id = service_id;
	owner_request.method_id = method_id;
	owner_request.request = *request;
	owner_request.result_cb = result_cb;
	owner_request.result_arg = result_arg;
	owner_request.out = out;
	if (options) {
		owner_request.options = *options;
		owner_request.has_options = 1;
		if (options->timeout_ms) {
			owner_request.deadline_ns =
				tr_rpc_timeout_deadline_ns(options->timeout_ms);
			if (owner_request.deadline_ns == 0U)
				return TR_ERR_SYS;
		}
	}

	return tr_rpc_owner_call(endpoint, tr_rpc_unary_call_on_owner,
				 &owner_request);
}

int tr_rpc_unary_call(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id, const struct tr_rpc_bytes *request,
		      tr_rpc_unary_result_cb result_cb, void *result_arg,
		      struct tr_rpc_call_handle *out)
{
	return tr_rpc_unary_call_ex(endpoint, service_id, method_id, request,
				    NULL, result_cb, result_arg, out);
}

struct tr_rpc_call_start_owner_request {
	struct tr_rpc_endpoint *endpoint;
	uint32_t service_id;
	uint32_t method_id;
	struct tr_rpc_call_options options;
	int has_options;
	uint64_t deadline_ns;
	struct tr_rpc_call_callbacks callbacks;
	int has_callbacks;
	struct tr_rpc_call_handle *out;
};

static int tr_rpc_call_start_on_owner(void *arg)
{
	struct tr_rpc_call_start_owner_request *request =
		(struct tr_rpc_call_start_owner_request *)arg;
	struct tr_rpc_endpoint *endpoint = request->endpoint;
	struct tr_rpc_method_entry *method;
	struct tr_rpc_call_slot *call;
	struct tr_rpc_call_handle handle;
	const struct tr_rpc_call_options *options =
		request->has_options ? &request->options : NULL;
	uint32_t slot;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	method = tr_rpc_find_method_locked(endpoint, request->service_id,
					   request->method_id);
	if (!method) {
		pthread_mutex_unlock(&endpoint->lock);
		return TR_ERR_INVALID;
	}

	ret = tr_rpc_allocate_call_locked(endpoint, &call, &slot);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	call->state = TR_RPC_CALL_OPENING;
	call->method = method;
	call->is_unary = 0;
	if (request->has_callbacks)
		call->callbacks = request->callbacks;

	ret = tr_rpc_apply_options_locked(endpoint, call, options,
					  request->deadline_ns);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	handle = tr_rpc_make_call_handle(endpoint, slot, call);
	ret = tr_rpc_run_interceptor_locked(
		endpoint, slot, call, TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL,
		TR_RPC_STATUS_OK, NULL);
	if (ret != TR_OK) {
		call = &endpoint->calls[slot];
		if (call->state != TR_RPC_CALL_FREE &&
		    call->generation == tr_rpc_call_handle_generation(handle))
			tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}

	ret = tr_stream_open(endpoint->channel, (enum tr_lane)method->desc.lane,
			     &call->stream);
	if (ret != TR_OK) {
		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		return ret;
	}
	ret = tr_rpc_bind_call_stream_locked(endpoint, slot, call->stream);
	if (ret != TR_OK) {
		enum tr_lane lane = (enum tr_lane)method->desc.lane;

		tr_rpc_free_call_locked(endpoint, call);
		pthread_mutex_unlock(&endpoint->lock);
		/* 未确认的 OPEN 无法单向关闭；绑定失败后已没有 Call 承接重试。
		 */
		(void)tr_channel_abort_lane_on_owner(
		        endpoint->channel, lane, ret < 0 ? ret : TR_ERR_STATE);
		return ret;
	}

	tr_rpc_semantic_start_locked(endpoint, call);
	*request->out = handle;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}

int tr_rpc_call_start_ex(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
			 uint32_t method_id,
			 const struct tr_rpc_call_options *options,
			 const struct tr_rpc_call_callbacks *callbacks,
			 struct tr_rpc_call_handle *out)
{
	struct tr_rpc_call_start_owner_request owner_request;

	if (!endpoint || !out || endpoint->config.role != TR_RPC_CLIENT ||
	    (options && options->metadata_count != 0 && !options->metadata))
		return TR_ERR_INVALID;

	memset(&owner_request, 0, sizeof(owner_request));
	owner_request.endpoint = endpoint;
	owner_request.service_id = service_id;
	owner_request.method_id = method_id;
	owner_request.out = out;
	if (options) {
		owner_request.options = *options;
		owner_request.has_options = 1;
		if (options->timeout_ms) {
			owner_request.deadline_ns =
				tr_rpc_timeout_deadline_ns(options->timeout_ms);
			if (owner_request.deadline_ns == 0U)
				return TR_ERR_SYS;
		}
	}
	if (callbacks) {
		owner_request.callbacks = *callbacks;
		owner_request.has_callbacks = 1;
	}

	return tr_rpc_owner_call(endpoint, tr_rpc_call_start_on_owner,
				 &owner_request);
}

int tr_rpc_call_start(struct tr_rpc_endpoint *endpoint, uint32_t service_id,
		      uint32_t method_id,
		      const struct tr_rpc_call_callbacks *callbacks,
		      struct tr_rpc_call_handle *out)
{
	return tr_rpc_call_start_ex(endpoint, service_id, method_id, NULL,
				    callbacks, out);
}

static int tr_rpc_prepare_send_locked(struct tr_rpc_call_handle handle,
				      struct tr_rpc_call_slot **call_out,
				      struct tr_rpc_method_desc *method_out,
				      uint16_t *wire_type_out,
				      uint32_t *codec_out, uint32_t *limit_out)
{
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	enum tr_rpc_cardinality cardinality;

	if (!endpoint)
		return TR_ERR_INVALID;

	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary)
		return TR_ERR_STALE;
	if (call->interceptor_active)
		return TR_ERR_STATE;
	if (call->cancelled || call->executor_overloaded ||
	    call->state == TR_RPC_CALL_TERMINAL ||
	    call->final_status_seen || call->final_status_sent ||
	    call->local_closed)
		return TR_ERR_CLOSED;
	if (call->state != TR_RPC_CALL_ACTIVE)
		return TR_AGAIN;

	cardinality =
		tr_rpc_outbound_cardinality(endpoint, &call->method->desc);
	if (tr_rpc_validate_cardinality(cardinality, call->tx_count) != TR_OK)
		return TR_ERR_STATE;

	*call_out = call;
	*method_out = call->method->desc;
	*wire_type_out = tr_rpc_outbound_wire_type(endpoint);
	*codec_out = tr_rpc_outbound_codec(endpoint, &call->method->desc);
	*limit_out = tr_rpc_outbound_limit(endpoint, &call->method->desc);
	return TR_OK;
}

struct tr_rpc_send_request {
	struct tr_rpc_call_handle handle;
	const struct tr_rpc_bytes *message;
};

static int tr_rpc_call_send_on_owner(void *arg)
{
	struct tr_rpc_send_request *request = (struct tr_rpc_send_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	const struct tr_rpc_bytes *message = request->message;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *encoded TR_AUTO(tr_buffer_cleanup) = NULL;
	uint16_t wire_type;
	uint32_t codec_id;
	uint32_t limit;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	ret = tr_rpc_prepare_send_locked(handle, &call, &method, &wire_type,
					 &codec_id, &limit);
	if (ret == TR_OK && message->len > limit)
		ret = TR_ERR_BAD_LENGTH;
	if (ret == TR_OK)
		ret = tr_rpc_encode_message(endpoint, call, wire_type, &method,
					    codec_id, TR_RPC_STATUS_OK, message,
					    &encoded);
	if (ret == TR_OK) {
		ret = tr_stream_send(call->stream, encoded);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&encoded);
			call->tx_count++;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_send(struct tr_rpc_call_handle handle,
		     const struct tr_rpc_bytes *message)
{
	struct tr_rpc_send_request request;

	if (!tr_rpc_call_handle_endpoint(handle) || !message ||
	    (message->len != 0 && !message->data))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.message = message;
	return tr_rpc_owner_call(tr_rpc_call_handle_endpoint(handle), tr_rpc_call_send_on_owner,
				 &request);
}

struct tr_rpc_send_buffer_request {
	struct tr_rpc_call_handle handle;
	struct tr_buffer *payload;
};

static int tr_rpc_call_send_buffer_on_owner(void *arg)
{
	struct tr_rpc_send_buffer_request *request =
		(struct tr_rpc_send_buffer_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	struct tr_buffer *payload = request->payload;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *header TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_buffer *parts[2];
	uint16_t wire_type;
	uint32_t codec_id;
	uint32_t limit;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	ret = tr_rpc_prepare_send_locked(handle, &call, &method, &wire_type,
					 &codec_id, &limit);
	if (ret == TR_OK && payload->len > limit)
		ret = TR_ERR_BAD_LENGTH;
	if (ret == TR_OK && codec_id != TR_RPC_CODEC_RAW)
		ret = TR_ERR_BAD_TYPE;
	if (ret == TR_OK)
		ret = tr_rpc_encode_header_buffer(
			endpoint, call, wire_type, &method, codec_id,
			TR_RPC_STATUS_OK, payload->len, &header);
	if (ret == TR_OK) {
		parts[0] = header;
		parts[1] = payload;
		ret = tr_stream_sendv(call->stream, parts, 2);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&header);
			call->tx_count++;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_send_buffer(struct tr_rpc_call_handle handle,
			    struct tr_buffer *payload)
{
	struct tr_rpc_send_buffer_request request;

	if (!tr_rpc_call_handle_endpoint(handle) || !payload || payload->len == 0)
		return TR_ERR_INVALID;

	request.handle = handle;
	request.payload = payload;
	return tr_rpc_owner_call(tr_rpc_call_handle_endpoint(handle),
				 tr_rpc_call_send_buffer_on_owner, &request);
}

struct tr_rpc_close_send_request {
	struct tr_rpc_call_handle handle;
};

static int tr_rpc_call_close_send_on_owner(void *arg)
{
	struct tr_rpc_close_send_request *request =
		(struct tr_rpc_close_send_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	enum tr_rpc_cardinality cardinality;
	struct tr_stream_handle completed_stream = { 0 };
	int completed = 0;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary || call->local_closed) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->interceptor_active) {
		ret = TR_ERR_STATE;
		goto out;
	}
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}

	if (endpoint->config.role != TR_RPC_CLIENT) {
		ret = TR_ERR_INVALID;
		goto out;
	}

	cardinality =
		tr_rpc_outbound_cardinality(endpoint, &call->method->desc);
	if (cardinality == TR_RPC_ONE && call->tx_count != 1U) {
		ret = TR_ERR_STATE;
		goto out;
	}
	if (cardinality == TR_RPC_MANY && call->tx_count == 0U) {
		/* V1 没有独立的 Method 打开封装：首个 REQUEST 携带
		 * service_id/method_id。 因此目前尚不能表示零消息请求流，
		 * 不得静默地半关闭匿名 Channel 流。
		 */
		ret = TR_ERR_STATE;
		goto out;
	}
	if (cardinality != TR_RPC_ONE && cardinality != TR_RPC_MANY) {
		ret = TR_ERR_STATE;
		goto out;
	}

	ret = tr_stream_close(call->stream);
	if (ret == TR_OK || ret == TR_ERR_CLOSED) {
		if (call->remote_closed) {
			/* Channel 已退役最后半关闭的流，但不会再次上报 CLOSED。
			 * 保留索引直到下方共同终态路径执行，避免留下无事件源的
			 * Call。
			 */
			call->local_closed = 1;
			completed_stream = call->stream;
			completed = 1;
		} else {
			tr_rpc_mark_local_closed_locked(endpoint, call);
		}
		ret = TR_OK;
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	if (completed)
		tr_rpc_on_stream_event(completed_stream, TR_STREAM_EVENT_CLOSED,
				       TR_OK, endpoint);
	return ret;
}

int tr_rpc_call_close_send(struct tr_rpc_call_handle handle)
{
	struct tr_rpc_close_send_request request;

	if (!tr_rpc_call_handle_endpoint(handle))
		return TR_ERR_INVALID;
	request.handle = handle;
	return tr_rpc_owner_call(tr_rpc_call_handle_endpoint(handle),
				 tr_rpc_call_close_send_on_owner, &request);
}

struct tr_rpc_finish_request {
	struct tr_rpc_call_handle handle;
	int status;
};

static int tr_rpc_call_finish_on_owner(void *arg)
{
	struct tr_rpc_finish_request *request =
		(struct tr_rpc_finish_request *)arg;
	struct tr_rpc_call_handle handle = request->handle;
	int status = request->status;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(handle);
	struct tr_rpc_call_slot *call;
	struct tr_rpc_method_desc method;
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	struct tr_rpc_bytes empty;
	enum tr_rpc_cardinality cardinality;
	int ret;

	empty.data = NULL;
	empty.len = 0;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(handle);
	if (!call || !call->method || call->is_unary ||
	    call->final_status_sent) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->interceptor_active) {
		ret = TR_ERR_STATE;
		goto out;
	}
	if (call->cancelled || call->executor_overloaded ||
	    call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}

	cardinality = call->method->desc.response_cardinality;
	if (status == TR_RPC_STATUS_OK && cardinality == TR_RPC_ONE &&
	    call->tx_count != 1U) {
		ret = TR_ERR_STATE;
		goto out;
	}
	/* V1 初始响应元数据只由首个 RESPONSE 携带。如果 MANY 响应方法
	 * 在没有响应时结束，STATUS 只能携带尾部元数据；
	 * 静默地把初始元数据重新归类会使两个作用范围产生歧义。
	 */
	if (call->tx_count == 0U &&
	    call->local_initial_metadata_len != 0U) {
		ret = TR_ERR_STATE;
		goto out;
	}

	ret = tr_rpc_run_interceptor_locked(
		endpoint, tr_rpc_call_handle_slot(handle), call,
		TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER, status, NULL);
	if (ret != TR_OK)
		goto out;
	call = &endpoint->calls[tr_rpc_call_handle_slot(handle)];

	method = call->method->desc;
	ret = tr_rpc_encode_message(endpoint, call, TR_RPC_WIRE_STATUS, &method,
				    method.response_codec_id, status, &empty,
				    &buffer);
	if (ret == TR_OK) {
		ret = tr_stream_send(call->stream, buffer);
		if (ret == TR_OK) {
			(void)tr_buffer_take(&buffer);
			call->final_status_sent = 1;
			call->final_status = status;
			tr_rpc_semantic_finish_locked(endpoint, call, status);
			(void)tr_rpc_deadline_set_locked(endpoint, call, 0U);
			ret = tr_stream_close(call->stream);
			if (ret == TR_OK || ret == TR_ERR_CLOSED) {
				tr_rpc_mark_local_closed_locked(endpoint, call);
				call->need_local_close = 0;
				if (call->remote_closed) {
					call->state = TR_RPC_CALL_TERMINAL;
					(void)tr_rpc_notify_terminal_locked(
						endpoint, tr_rpc_call_handle_slot(handle), call,
						status);
					tr_rpc_maybe_free_call_locked(endpoint, call);
				}
				ret = TR_OK;
			} else if (ret == TR_AGAIN) {
				/* STATUS 已提交：调度剩余半关闭。 */
				call->need_local_close = 1;
				ret = tr_rpc_schedule_final_close_retry_locked(endpoint, call);
			}
		}
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_finish(struct tr_rpc_call_handle handle, int status)
{
	struct tr_rpc_finish_request request;

	if (!tr_rpc_call_handle_endpoint(handle) ||
	    tr_rpc_call_handle_endpoint(handle)->config.role != TR_RPC_SERVER ||
	    !tr_rpc_status_valid(status))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status = status;
	return tr_rpc_owner_call(tr_rpc_call_handle_endpoint(handle), tr_rpc_call_finish_on_owner,
				 &request);
}

int tr_rpc_call_cancel(struct tr_rpc_call_handle handle)
{
	return tr_rpc_cancel_internal(handle, TR_RPC_STATUS_CANCELLED);
}

struct tr_rpc_cancel_query {
	struct tr_rpc_call_handle handle;
	int *status_out;
};

static int tr_rpc_call_is_cancelled_on_owner(void *arg)
{
	struct tr_rpc_cancel_query *request =
		(struct tr_rpc_cancel_query *)arg;
	struct tr_rpc_endpoint *endpoint = tr_rpc_call_handle_endpoint(request->handle);
	struct tr_rpc_call_slot *call;
	int cancelled;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}

	cancelled = call->cancelled;
	if (request->status_out)
		*request->status_out = cancelled ? call->cancel_status :
						   TR_RPC_STATUS_OK;
	ret = cancelled ? 1 : 0;

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_is_cancelled(struct tr_rpc_call_handle handle, int *status_out)
{
	struct tr_rpc_cancel_query request;

	if (!tr_rpc_call_handle_endpoint(handle))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.status_out = status_out;
	return tr_rpc_owner_call(tr_rpc_call_handle_endpoint(handle),
				 tr_rpc_call_is_cancelled_on_owner, &request);
}

struct tr_rpc_set_metadata_request {
	struct tr_rpc_call_handle handle;
	const char *key;
	const void *value;
	uint16_t value_len;
	size_t key_len;
	int trailing;
};

static int tr_rpc_call_set_metadata_on_owner(void *arg)
{
	struct tr_rpc_set_metadata_request *request =
		(struct tr_rpc_set_metadata_request *)arg;
	struct tr_rpc_endpoint *endpoint =
		tr_rpc_call_handle_endpoint(request->handle);
	struct tr_rpc_call_slot *call;
	uint8_t *dst;
	uint16_t *dst_len;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}
	if (call->cancelled || call->state == TR_RPC_CALL_TERMINAL) {
		ret = TR_ERR_CLOSED;
		goto out;
	}

	if (request->trailing) {
		if (endpoint->config.role != TR_RPC_SERVER || call->is_unary ||
		    call->final_status_sent || call->pending_control_is_final_status) {
			ret = TR_ERR_STATE;
			goto out;
		}
		dst = call->local_trailing_metadata;
		dst_len = &call->local_trailing_metadata_len;
	} else {
		if (call->tx_count != 0 || call->pending_tx != NULL ||
		    call->pending_control != NULL) {
			ret = TR_ERR_STATE;
			goto out;
		}
		if (call->deadline_ns != 0 &&
		    (uint32_t)call->local_initial_metadata_len +
			    TR_RPC_METADATA_TLV_HEADER_SIZE +
			    request->key_len + request->value_len +
			    TR_RPC_DEADLINE_METADATA_BYTES >
			TR_RPC_METADATA_MAX_BYTES) {
			ret = TR_ERR_BAD_LENGTH;
			goto out;
		}
		dst = call->local_initial_metadata;
		dst_len = &call->local_initial_metadata_len;
	}

	ret = tr_rpc_metadata_add_raw(
		dst, dst_len, request->key, request->key_len,
		request->value, request->value_len, 0);

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

static int tr_rpc_call_set_metadata_scope(
	struct tr_rpc_call_handle handle, const char *key,
	const void *value, uint16_t value_len, int trailing)
{
	struct tr_rpc_set_metadata_request request;
	size_t key_len;

	if (!tr_rpc_call_handle_endpoint(handle) ||
	    !tr_rpc_metadata_key_valid(key, &key_len) ||
	    (value_len != 0 && !value))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.key = key;
	request.value = value;
	request.value_len = value_len;
	request.key_len = key_len;
	request.trailing = trailing;
	return tr_rpc_owner_call(
		tr_rpc_call_handle_endpoint(handle),
		tr_rpc_call_set_metadata_on_owner, &request);
}

int tr_rpc_call_set_metadata(struct tr_rpc_call_handle handle, const char *key,
			     const void *value, uint16_t value_len)
{
	return tr_rpc_call_set_metadata_scope(
		handle, key, value, value_len, 0);
}

int tr_rpc_call_set_trailing_metadata(
	struct tr_rpc_call_handle handle, const char *key,
	const void *value, uint16_t value_len)
{
	return tr_rpc_call_set_metadata_scope(
		handle, key, value, value_len, 1);
}

struct tr_rpc_get_metadata_request {
	struct tr_rpc_call_handle handle;
	const char *key;
	void *value;
	uint16_t *value_len;
	size_t key_len;
	int trailing;
};

static int tr_rpc_call_get_peer_metadata_on_owner(void *arg)
{
	struct tr_rpc_get_metadata_request *request =
		(struct tr_rpc_get_metadata_request *)arg;
	struct tr_rpc_endpoint *endpoint =
		tr_rpc_call_handle_endpoint(request->handle);
	struct tr_rpc_call_slot *call;
	const uint8_t *src;
	uint16_t src_len;
	const uint8_t *found = NULL;
	uint16_t found_len = 0;
	int ret;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call) {
		ret = TR_ERR_STALE;
		goto out;
	}

	if (request->trailing) {
		if (endpoint->config.role != TR_RPC_CLIENT || call->is_unary) {
			ret = TR_ERR_STATE;
			goto out;
		}
		if (!call->final_status_seen) {
			ret = TR_AGAIN;
			goto out;
		}
		src = call->peer_trailing_metadata;
		src_len = call->peer_trailing_metadata_len;
	} else {
		src = call->peer_initial_metadata;
		src_len = call->peer_initial_metadata_len;
	}

	ret = tr_rpc_metadata_find_raw(
		src, src_len, request->key, request->key_len,
		&found, &found_len);
	if (ret == TR_OK) {
		if (!request->value) {
			*request->value_len = found_len;
		} else if (*request->value_len < found_len) {
			*request->value_len = found_len;
			ret = TR_ERR_BAD_LENGTH;
		} else {
			if (found_len)
				memcpy(request->value, found, found_len);
			*request->value_len = found_len;
		}
	}

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

static int tr_rpc_call_get_peer_metadata_scope(
	struct tr_rpc_call_handle handle, const char *key, void *value,
	uint16_t *value_len, int trailing)
{
	struct tr_rpc_get_metadata_request request;
	size_t key_len;

	if (!tr_rpc_call_handle_endpoint(handle) || !value_len ||
	    !tr_rpc_metadata_key_valid(key, &key_len))
		return TR_ERR_INVALID;

	request.handle = handle;
	request.key = key;
	request.value = value;
	request.value_len = value_len;
	request.key_len = key_len;
	request.trailing = trailing;
	return tr_rpc_owner_call(
		tr_rpc_call_handle_endpoint(handle),
		tr_rpc_call_get_peer_metadata_on_owner, &request);
}

int tr_rpc_call_get_peer_metadata(struct tr_rpc_call_handle handle,
				  const char *key, void *value,
				  uint16_t *value_len)
{
	return tr_rpc_call_get_peer_metadata_scope(
		handle, key, value, value_len, 0);
}

int tr_rpc_call_get_peer_trailing_metadata(
	struct tr_rpc_call_handle handle, const char *key, void *value,
	uint16_t *value_len)
{
	return tr_rpc_call_get_peer_metadata_scope(
		handle, key, value, value_len, 1);
}

struct tr_rpc_context_request {
	struct tr_rpc_call_handle handle;
	struct tr_rpc_context *out;
};

static int tr_rpc_call_get_context_on_owner(void *arg)
{
	struct tr_rpc_context_request *request =
		(struct tr_rpc_context_request *)arg;
	struct tr_rpc_endpoint *endpoint =
		tr_rpc_call_handle_endpoint(request->handle);
	struct tr_rpc_call_slot *call;
	struct tr_rpc_context context;
	uint64_t now;
	uint64_t remaining_ns;
	int ret = TR_OK;

	pthread_mutex_lock(&endpoint->lock);
	call = tr_rpc_lookup_call_handle_locked(request->handle);
	if (!call || !call->method) {
		ret = TR_ERR_STALE;
		goto out;
	}

	memset(&context, 0, sizeof(context));
	context.service_id = call->method->desc.service_id;
	context.method_id = call->method->desc.method_id;
	context.request_cardinality = call->method->desc.request_cardinality;
	context.response_cardinality = call->method->desc.response_cardinality;
	context.cancelled = call->cancelled;
	context.cancel_status =
		call->cancelled ? call->cancel_status : TR_RPC_STATUS_OK;

	if (call->deadline_ns != 0U) {
		context.has_deadline = 1;
		now = tr_rpc_now_ns();
		if (now == 0U) {
			ret = TR_ERR_SYS;
			goto out;
		}
		remaining_ns =
			call->deadline_ns > now ? call->deadline_ns - now : 0U;
		context.deadline_remaining_ms =
			remaining_ns / UINT64_C(1000000) +
			(remaining_ns % UINT64_C(1000000) != 0U);
	}
	*request->out = context;

out:
	pthread_mutex_unlock(&endpoint->lock);
	return ret;
}

int tr_rpc_call_get_context(struct tr_rpc_call_handle handle,
			    struct tr_rpc_context *out)
{
	struct tr_rpc_context_request request;

	if (!tr_rpc_call_handle_endpoint(handle) || !out)
		return TR_ERR_INVALID;

	request.handle = handle;
	request.out = out;
	return tr_rpc_owner_call(
		tr_rpc_call_handle_endpoint(handle),
		tr_rpc_call_get_context_on_owner, &request);
}

int tr_rpc_message_stream_internal(const struct tr_rpc_message *message,
				   struct tr_stream_handle *out)
{
	if (!out)
		return TR_ERR_INVALID;
	return tr_rpc_message_unpack_internal(message, NULL, NULL, out);
}

int tr_rpc_message_release(struct tr_rpc_message *message)
{
	struct tr_buffer *storage = NULL;
	struct tr_rpc_endpoint *endpoint = NULL;
	struct tr_stream_handle stream;
	int ret;

	memset(&stream, 0, sizeof(stream));
	ret = tr_rpc_message_unpack_internal(
		message, &storage, &endpoint, &stream);
	if (ret != TR_OK)
		return ret;

	/* 精确消耗一次公开能力凭证。归还流接收额度期间，
	 * Endpoint 仍受生命周期保护，保证 stream.channel
	 * 不会因本次释放中的对象销毁而变得陈旧。
	 */
	memset(message, 0, sizeof(*message));
	ret = tr_rpc_release_stream_payload(stream, storage);
	tr_rpc_endpoint_put(endpoint);
	return ret;
}

static int tr_rpc_endpoint_flush_on_owner(void *arg)
{
	struct tr_rpc_endpoint *endpoint = (struct tr_rpc_endpoint *)arg;
	uint32_t i;
	int result;

	result = tr_channel_flush(endpoint->channel);

	pthread_mutex_lock(&endpoint->lock);
	for (i = 0; i < endpoint->config.max_calls; ++i) {
		struct tr_rpc_call_slot *call = &endpoint->calls[i];
		int ret = TR_OK;

		if (call->state == TR_RPC_CALL_FREE)
			continue;

		if (call->cancelled ||
		    (!call->is_unary && call->pending_control)) {
			struct tr_rpc_call_handle handle =
			        tr_rpc_make_call_handle(endpoint, i, call);

			ret = tr_rpc_try_cancel_send_locked(endpoint, call);
			call = tr_rpc_lookup_call_handle_locked(handle);
			if (call) {
				if (call->executor_overloaded &&
				    call->local_closed && call->remote_closed &&
				    call->state != TR_RPC_CALL_TERMINAL) {
					call->state = TR_RPC_CALL_TERMINAL;
					(void)tr_rpc_notify_terminal_locked(
					        endpoint, i, call,
					        call->final_status);
				}
				tr_rpc_maybe_free_call_locked(endpoint, call);
			}
		} else if (call->is_unary &&
		           (call->pending_tx || call->need_local_close))
			ret = tr_rpc_try_unary_send_locked(endpoint, call);
		else if (!call->is_unary && call->need_local_close) {
			ret = tr_stream_close(call->stream);
			if (ret == TR_OK || ret == TR_ERR_CLOSED) {
				call->need_local_close = 0;
				tr_rpc_mark_local_closed_locked(endpoint, call);
				if (call->admission_rejected &&
				    call->remote_closed)
					call->state = TR_RPC_CALL_TERMINAL;
				else if (call->executor_overloaded &&
					 call->remote_closed) {
					call->state = TR_RPC_CALL_TERMINAL;
					(void)tr_rpc_notify_terminal_locked(
						endpoint, i, call,
						call->final_status);
				}
				tr_rpc_maybe_free_call_locked(endpoint, call);
				ret = TR_OK;
			}
		}

		if (ret != TR_OK && ret != TR_AGAIN && result == TR_OK)
			result = ret;
		else if (ret == TR_AGAIN && result == TR_OK)
			result = TR_AGAIN;
	}
	pthread_mutex_unlock(&endpoint->lock);
	return result;
}

int tr_rpc_endpoint_flush(struct tr_rpc_endpoint *endpoint)
{
	if (!endpoint)
		return TR_ERR_INVALID;
	return tr_rpc_owner_call(endpoint, tr_rpc_endpoint_flush_on_owner,
				 endpoint);
}

int tr_rpc_endpoint_get_stats(struct tr_rpc_endpoint *endpoint,
			      struct tr_rpc_endpoint_stats *out)
{
	uint32_t i;

	if (!endpoint || !out)
		return TR_ERR_INVALID;

	memset(out, 0, sizeof(*out));

	pthread_mutex_lock(&endpoint->lock);
	out->max_methods = endpoint->config.max_methods;
	out->max_calls = endpoint->config.max_calls;
	out->calls_started = endpoint->stat_calls_started;
	out->calls_completed = endpoint->stat_calls_completed;
	out->calls_cancelled = endpoint->stat_calls_cancelled;
	out->calls_deadline_exceeded = endpoint->stat_calls_deadline_exceeded;
	out->semantic = endpoint->semantic;
	out->semantic.calls_inflight =
		out->semantic.calls_started >= out->semantic.calls_finished ?
			out->semantic.calls_started - out->semantic.calls_finished :
			0U;

	out->registered_methods = endpoint->method_count;

	for (i = 0; i < endpoint->config.max_calls; ++i) {
		switch (endpoint->calls[i].state) {
		case TR_RPC_CALL_OPENING:
			out->opening_calls++;
			break;
		case TR_RPC_CALL_ACTIVE:
			out->active_calls++;
			break;
		case TR_RPC_CALL_TERMINAL:
			out->terminal_calls++;
			break;
		default:
			break;
		}
	}
	pthread_mutex_unlock(&endpoint->lock);

	pthread_mutex_lock(&endpoint->executor.lock);
	out->executor_threads = endpoint->executor.thread_count;
	out->executor_queued_tasks = endpoint->executor.queued_count;
	out->executor_running_tasks = endpoint->executor.running_count;
	out->executor_queue_capacity = endpoint->executor.capacity;
	out->executor_continuation_reserve =
		endpoint->executor.continuation_reserve;
	out->executor_queue.capacity = endpoint->executor.capacity;
	out->executor_queue.current = endpoint->executor.queued_count;
	out->executor_queue.peak = endpoint->executor.queued_peak;
	out->executor_queue.full_events = endpoint->executor.hard_full_events;
	out->executor_ready_calls = endpoint->executor.ready_count;
	out->executor_ready_calls_peak = endpoint->executor.ready_peak;
	out->observability_flags = endpoint->config.observability_flags;
	out->executor_enqueued_tasks = endpoint->executor.enqueued_tasks;
	out->executor_taken_tasks = endpoint->executor.taken_tasks;
	out->executor_admission_limit_hits =
		endpoint->executor.admission_limit_hits;
	out->executor_hard_full_events = endpoint->executor.hard_full_events;
	out->executor_queue_wait_ns = endpoint->executor.queue_wait_ns;
	out->executor_handler_ns = endpoint->executor.handler_ns;
	pthread_mutex_unlock(&endpoint->executor.lock);

	return TR_OK;
}

int tr_rpc_endpoint_get_semantic_stats(
	struct tr_rpc_endpoint *endpoint, struct tr_rpc_semantic_stats *out)
{
	if (!endpoint || !out)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&endpoint->lock);
	*out = endpoint->semantic;
	out->calls_inflight =
		out->calls_started >= out->calls_finished ?
			out->calls_started - out->calls_finished : 0U;
	pthread_mutex_unlock(&endpoint->lock);
	return TR_OK;
}
