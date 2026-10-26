#define _POSIX_C_SOURCE 200809L
#include "rpc_record_dispatch.h"
#include "rpc_record_producer.h"
#include "rpc_v2_internal.h"
#include "../execution/reactor_internal.h"
#include "../io/peer_identity_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>

struct tr_reply_body {
	struct tr_buffer buffer;
	struct tr_memory_budget *budget;
	struct tr_reply_body *next;
	size_t bytes;
	enum tr_rpc_compression compression;
	uint8_t data[];
};
struct tr_reply_metadata {
	uint8_t *data;
	uint32_t size;
	bool set;
};
struct tr_rpc_record_reply {
	struct tr_memory_budget *budget;
	struct tr_rpc_record_producer_domain *producer_domain;
	/* 只由当前 worker 创建/拥有，done 后移交调用；owner 在此之前
	 * 不读取本字段。失败清理仍须关闭并归还这一份库内引用。 */
	struct tr_rpc_v2_producer *producer;
	struct tr_reply_body *head, *tail;
	struct tr_reply_metadata initial, trailing;
	size_t used, limit;
	uint32_t count, max_count, max_message;
	uint32_t response_compression_algorithms;
	enum tr_rpc_status status;
	bool finished, failed, headers_sent, asynchronous;
};
struct tr_dispatch_slot;
struct tr_rpc_v2_cancel {
	const struct tr_task_cancel *task;
	struct tr_dispatch_slot *slot;
};
struct tr_rpc_v2_reply {
	struct tr_rpc_record_reply *record;
};

/* 公共能力仅在当前工作回调的栈上有效。先比较地址再解引用，使
 * 跨线程或回调外误用不会读取已经失效的包装存储。终局回调也计入
 * 深度，尤其不能在停止清理线程中递归调用门面 destroy。 */
static _Thread_local const struct tr_rpc_v2_cancel *tr_v2_active_cancel;
static _Thread_local struct tr_rpc_v2_reply *tr_v2_active_reply;
static _Thread_local unsigned tr_v2_callback_depth;
static _Atomic uint64_t tr_dispatch_instances;

#define TR_RPC_V2_CONTEXT_RESERVED UINT64_MAX

struct tr_rpc_v2_context_state {
	struct tr_rpc_record_dispatch *dispatch;
	_Atomic uint64_t references;
	_Atomic uint64_t generation;
	_Atomic int status;
	uint64_t deadline;
};

struct tr_dispatch_context_link {
	_Atomic bool locked;
	struct tr_rpc_v2_context_state *state;
	uint64_t route_generation;
	int parent_status;
};

/* API 失败锁存整批；业务显式选择的非 OK 则仍可携带有效元数据。
 * 保留第一个错误，后续返回 OK 或另一个业务状态都不能解除失败。 */
static int tr_reply_fail(struct tr_rpc_record_reply *reply, int error,
                         enum tr_rpc_status status)
{
	reply->failed = true;
	if (reply->status == TR_RPC_STATUS_OK)
		reply->status = status;
	return error;
}

int tr_rpc_record_dispatch_in_callback(void)
{
	return tr_v2_callback_depth != 0;
}

int tr_rpc_v2_cancel_requested(const struct tr_rpc_v2_cancel *cancel)
{
	if (!cancel)
		return TR_ERR_INVALID;
	if (cancel != tr_v2_active_cancel)
		return TR_ERR_STATE;
	return tr_task_cancel_requested(cancel->task);
}

int tr_rpc_v2_reply_append(struct tr_rpc_v2_reply *reply,
                           const struct tr_rpc_bytes *message)
{
	return tr_rpc_v2_reply_append_with_compression(reply, message,
	                                               TR_RPC_COMPRESSION_NONE);
}

int tr_rpc_v2_reply_append_with_compression(struct tr_rpc_v2_reply *reply,
                                            const struct tr_rpc_bytes *message,
                                            enum tr_rpc_compression compression)
{
	if (!reply)
		return TR_ERR_INVALID;
	if (reply != tr_v2_active_reply)
		return TR_ERR_STATE;
	if (reply->record->asynchronous || reply->record->finished ||
	    reply->record->status != TR_RPC_STATUS_OK)
		return tr_reply_fail(reply->record, TR_ERR_STATE,
		                     TR_RPC_STATUS_FAILED_PRECONDITION);
	if (!message)
		return tr_reply_fail(reply->record, TR_ERR_INVALID,
		                     TR_RPC_STATUS_INVALID_ARGUMENT);
	return tr_rpc_record_reply_append_with_compression(
	        reply->record, message->data, message->len, compression);
}

int tr_rpc_v2_reply_set_metadata(struct tr_rpc_v2_reply *reply,
                                 enum tr_rpc_v2_metadata_phase phase,
                                 const struct tr_rpc_metadata *entries,
                                 size_t count)
{
	if (!reply)
		return TR_ERR_INVALID;
	if (reply != tr_v2_active_reply)
		return TR_ERR_STATE;
	return tr_rpc_record_reply_set_metadata(reply->record, phase, entries,
	                                        count);
}

int tr_rpc_v2_reply_finish(struct tr_rpc_v2_reply *reply,
                           enum tr_rpc_status status)
{
	if (!reply)
		return TR_ERR_INVALID;
	if (reply != tr_v2_active_reply)
		return TR_ERR_STATE;
	return tr_rpc_record_reply_finish(reply->record, status);
}

int tr_rpc_v2_reply_acquire_producer(struct tr_rpc_v2_reply *reply,
                                     struct tr_rpc_v2_producer **out)
{
	struct tr_rpc_record_reply *record;
	struct tr_rpc_v2_producer *producer = NULL;
	int ret;
	if (!reply || !out)
		return TR_ERR_INVALID;
	if (reply != tr_v2_active_reply)
		return TR_ERR_STATE;
	record = reply->record;
	/* 导出是独占输出权的一次交接，失败不锁存当前 work。不能把已经
	 * 生成的同步批次与来自其他线程的新命令混排。 */
	if (record->asynchronous || record->finished || record->failed ||
	    record->status != TR_RPC_STATUS_OK || record->count ||
	    record->initial.set || record->trailing.set)
		return TR_ERR_STATE;
	if (!record->producer_domain)
		return TR_ERR_UNSUPPORTED;
	ret = tr_rpc_record_producer_create(
	        record->producer_domain, record->max_message,
	        record->headers_sent, record->response_compression_algorithms,
	        &producer);
	if (ret != TR_OK)
		return ret;
	record->producer = producer;
	record->asynchronous = true;
	*out = producer;
	return TR_OK;
}

static int tr_dispatch_instance(uint64_t *out)
{
	uint64_t value = atomic_load_explicit(&tr_dispatch_instances,
	                                      memory_order_relaxed);
	for (;;) {
		if (value == UINT64_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &tr_dispatch_instances, &value, value + 1,
		            memory_order_relaxed, memory_order_relaxed)) {
			*out = value + 1;
			return TR_OK;
		}
	}
}
struct tr_dispatch_slot;
enum tr_dispatch_job_stage {
	TR_DISPATCH_JOB_APPLICATION = 0,
	TR_DISPATCH_JOB_AUTHORIZATION = 1
};
struct tr_dispatch_job {
	struct tr_dispatch_job *next;
	struct tr_dispatch_slot *slot;
	enum tr_dispatch_job_stage stage;
	struct tr_rpc_dispatch_id id;
	struct tr_rpc_record_view view;
	tr_rpc_record_work_fn work;
	tr_rpc_v2_work_fn v2_work;
	tr_rpc_v2_authorize_fn v2_authorize;
	struct tr_rpc_v2_call_id v2_id;
	struct tr_rpc_v2_authorization_result authorization_result;
	void *arg;
	struct tr_rpc_record_reply reply;
	uint64_t deadline;
	size_t bytes;
	uint8_t data[];
};
struct tr_dispatch_method {
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_rpc_dispatch_method method;
};
struct tr_dispatch_slot {
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_rpc_route_handle route;
	struct tr_stream_handle stream;
	struct tr_dispatch_job *head, *tail, *submitted, *auth_job, *ready;
	struct tr_rpc_v2_producer *producer;
	struct tr_peer_identity_snapshot *peer_identity;
	size_t peer_identity_budget_bytes;
	struct tr_task_handle task;
	/* 只在完整合法 OPEN 的准入和期限检查通过后设置。即使后续
	 * job 分配失败，也已承担该方法的一次公共终局通知责任。 */
	const struct tr_rpc_dispatch_method *v2_method;
	uint64_t deadline;
	uint64_t open_due;
	uint32_t response_codec;
	size_t pending;
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	bool active, terminal, notified, headers_sent, final_accepted;
	bool admission_failed;
	bool semantic_started;
	bool peer_identity_bound;
	bool v2_open_observed;
	bool authorized;
	/* 单向切换后不因生产者关闭而恢复同步回复。公平轮转让回复背压
	 * 与请求 work 的 FIFO 各自推进，不占用工作线程等待邮箱。 */
	bool asynchronous, producer_turn;
	/* owner 已观察到非法输出并锁定错误 FINAL；不能让新输入启动
	 * work，或让已在运行的 work 改写该失败及其合法尾部。 */
	bool producer_failed;
};
struct tr_rpc_record_dispatch {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_task_executor *executor;
	struct tr_rpc_record_router *router;
	struct tr_memory_budget *budget;
	struct tr_rpc_record_producer_domain *producer_domain;
	struct tr_dispatch_slot *slots;
	struct tr_rpc_v2_context_state *contexts;
	struct tr_dispatch_context_link *context_links;
	struct tr_dispatch_method *methods;
	struct tr_rpc_record_dispatch_config config;
	struct tr_rpc_record_semantic_counters local_semantic;
	struct tr_rpc_record_semantic_counters *semantic;
	tr_rpc_dispatch_terminal_fn terminal;
	void *arg;
	size_t bytes;
	uint64_t instance;
	uint32_t cursor;
	_Atomic uint64_t external_context_refs;
	unsigned depth;
	bool stopping, cleanup, admitted;
};

static int tr_dispatch_owner(struct tr_rpc_record_dispatch *d)
{
	if (!d)
		return TR_ERR_INVALID;
	if (!tr_reactor_in_owner_context_for(d->reactor) || d->cleanup)
		return TR_ERR_STATE;
	return TR_OK;
}
static int tr_dispatch_now(uint64_t *out)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
	    now.tv_nsec < 0 || now.tv_nsec >= 1000000000L ||
	    (uint64_t)now.tv_sec >
	            (UINT64_MAX - (uint64_t)now.tv_nsec) / UINT64_C(1000000000))
		return TR_ERR_SYS;
	*out = (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
	return TR_OK;
}

static void tr_dispatch_context_lock(struct tr_dispatch_context_link *link)
{
	while (atomic_exchange_explicit(&link->locked, true,
	                                memory_order_acquire)) {
	}
}

static void tr_dispatch_context_unlock(struct tr_dispatch_context_link *link)
{
	atomic_store_explicit(&link->locked, false, memory_order_release);
}

static int
tr_dispatch_context_external_retain(struct tr_rpc_record_dispatch *dispatch)
{
	uint64_t references = atomic_load_explicit(
	        &dispatch->external_context_refs, memory_order_relaxed);
	for (;;) {
		if (references == UINT64_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &dispatch->external_context_refs, &references,
		            references + 1, memory_order_acq_rel,
		            memory_order_relaxed))
			return TR_OK;
	}
}

static int
tr_dispatch_context_reference_retain(struct tr_rpc_v2_context_state *state)
{
	uint64_t references =
	        atomic_load_explicit(&state->references, memory_order_acquire);
	for (;;) {
		if (!references)
			return TR_ERR_STALE;
		if (references == TR_RPC_V2_CONTEXT_RESERVED)
			return TR_ERR_STALE;
		if (references == UINT64_MAX - 1U)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &state->references, &references, references + 1,
		            memory_order_acq_rel, memory_order_acquire))
			return TR_OK;
	}
}

static void
tr_dispatch_context_reference_drop(struct tr_rpc_v2_context_state *state)
{
	(void)atomic_fetch_sub_explicit(&state->references, 1,
	                                memory_order_acq_rel);
}

static int tr_dispatch_context_reject_status(int status)
{
	return status == TR_RPC_STATUS_DEADLINE_EXCEEDED ? TR_ERR_TIMEOUT
	                                                 : TR_ERR_CANCELLED;
}

static void tr_dispatch_context_publish_terminal(struct tr_dispatch_slot *slot,
                                                 enum tr_rpc_status status)
{
	struct tr_rpc_record_dispatch *dispatch = slot->dispatch;
	struct tr_dispatch_context_link *link =
	        &dispatch->context_links[slot->route.slot];
	int published = status == TR_RPC_STATUS_DEADLINE_EXCEEDED
	                        ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                        : TR_RPC_STATUS_CANCELLED;
	tr_dispatch_context_lock(link);
	if (link->route_generation == slot->route.generation &&
	    link->parent_status == TR_RPC_V2_CONTEXT_ACTIVE) {
		link->parent_status = published;
		if (link->state)
			atomic_store_explicit(&link->state->status, published,
			                      memory_order_release);
	}
	tr_dispatch_context_unlock(link);
}

static void tr_dispatch_context_detach(struct tr_dispatch_slot *slot)
{
	struct tr_rpc_record_dispatch *dispatch = slot->dispatch;
	struct tr_dispatch_context_link *link;
	struct tr_rpc_v2_context_state *state = NULL;
	if (!dispatch->context_links ||
	    slot->route.slot >= dispatch->config.capacity)
		return;
	link = &dispatch->context_links[slot->route.slot];
	tr_dispatch_context_lock(link);
	if (link->route_generation == slot->route.generation) {
		state = link->state;
		link->state = NULL;
		if (link->parent_status == TR_RPC_V2_CONTEXT_ACTIVE) {
			link->parent_status = TR_RPC_STATUS_CANCELLED;
			if (state)
				atomic_store_explicit(&state->status,
				                      TR_RPC_STATUS_CANCELLED,
				                      memory_order_release);
		}
	}
	tr_dispatch_context_unlock(link);
	if (state)
		tr_dispatch_context_reference_drop(state); /* link 的基准引用 */
}

static int tr_dispatch_context_deadline_status(uint64_t deadline)
{
	uint64_t now;
	int ret;
	if (!deadline)
		return TR_OK;
	ret = tr_dispatch_now(&now);
	if (ret != TR_OK)
		return ret;
	return now >= deadline ? TR_ERR_TIMEOUT : TR_OK;
}

int tr_rpc_v2_cancel_context_hold(const struct tr_rpc_v2_cancel *cancel,
                                  struct tr_rpc_v2_call_context *out)
{
	struct tr_dispatch_slot *slot;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_dispatch_context_link *link;
	struct tr_rpc_v2_context_state *state = NULL;
	uint64_t generation;
	bool new_state = false;
	int ret;
	if (!cancel || !out)
		return TR_ERR_INVALID;
	if (out->_private[0] || out->_private[1])
		return TR_ERR_INVALID;
	if (cancel != tr_v2_active_cancel || !cancel->task || !cancel->slot)
		return TR_ERR_STATE;
	slot = cancel->slot;
	dispatch = slot->dispatch;
	link = &dispatch->context_links[slot->route.slot];
	if (tr_task_cancel_requested(cancel->task))
		return TR_ERR_CANCELLED;
	ret = tr_dispatch_context_deadline_status(slot->deadline);
	if (ret != TR_OK)
		return ret;
	tr_dispatch_context_lock(link);
	if (link->route_generation != slot->route.generation) {
		ret = TR_ERR_STALE;
		goto done;
	}
	if (link->parent_status != TR_RPC_V2_CONTEXT_ACTIVE) {
		ret = tr_dispatch_context_reject_status(link->parent_status);
		goto done;
	}
	if (tr_task_cancel_requested(cancel->task)) {
		ret = TR_ERR_CANCELLED;
		goto done;
	}
	ret = tr_dispatch_context_deadline_status(slot->deadline);
	if (ret != TR_OK)
		goto done;
	state = link->state;
	if (!state) {
		for (uint32_t i = 0; i < dispatch->config.capacity; ++i) {
			struct tr_rpc_v2_context_state *candidate =
			        &dispatch->contexts[i];
			uint64_t expected = 0;
			if (!atomic_compare_exchange_strong_explicit(
			            &candidate->references, &expected,
			            TR_RPC_V2_CONTEXT_RESERVED,
			            memory_order_acq_rel, memory_order_acquire))
				continue;
			generation = atomic_load_explicit(
			        &candidate->generation, memory_order_relaxed);
			if (generation == UINT64_MAX) {
				atomic_store_explicit(&candidate->references, 0,
				                      memory_order_release);
				ret = TR_ERR_STATE;
				goto done;
			}
			++generation;
			atomic_store_explicit(&candidate->generation,
			                      generation, memory_order_relaxed);
			candidate->deadline = slot->deadline;
			atomic_store_explicit(&candidate->status,
			                      TR_RPC_V2_CONTEXT_ACTIVE,
			                      memory_order_release);
			atomic_store_explicit(&candidate->references, 1,
			                      memory_order_release);
			state = candidate;
			link->state = state;
			new_state = true;
			break;
		}
		if (!state) {
			ret = TR_AGAIN;
			goto done;
		}
	}
	ret = tr_dispatch_context_reference_retain(state);
	if (ret != TR_OK) {
		if (new_state) {
			link->state = NULL;
			tr_dispatch_context_reference_drop(state);
		}
		goto done;
	}
	ret = tr_dispatch_context_external_retain(dispatch);
	if (ret != TR_OK) {
		tr_dispatch_context_reference_drop(state);
		if (new_state) {
			link->state = NULL;
			tr_dispatch_context_reference_drop(state);
		}
		goto done;
	}
	generation =
	        atomic_load_explicit(&state->generation, memory_order_acquire);
	out->_private[0] = (uint64_t)(uintptr_t)state;
	out->_private[1] = generation;
	ret = TR_OK;
done:
	tr_dispatch_context_unlock(link);
	return ret;
}

static void
tr_dispatch_context_drop_external(struct tr_rpc_v2_context_state *state)
{
	struct tr_rpc_record_dispatch *dispatch = state->dispatch;
	tr_dispatch_context_reference_drop(state);
	/* 此计数减至零后不再访问 Dispatch；销毁侧以 acquire 读取。 */
	(void)atomic_fetch_sub_explicit(&dispatch->external_context_refs, 1,
	                                memory_order_release);
}

int tr_rpc_v2_call_context_release(struct tr_rpc_v2_call_context *context)
{
	struct tr_rpc_v2_context_state *state;
	uint64_t generation;
	int ret;
	if (!context)
		return TR_ERR_INVALID;
	if (!context->_private[0] || !context->_private[1])
		return TR_ERR_STALE;
	state = (struct tr_rpc_v2_context_state *)(uintptr_t)
	                context->_private[0];
	ret = tr_dispatch_context_reference_retain(state);
	if (ret != TR_OK)
		return ret == TR_ERR_STATE ? ret : TR_ERR_STALE;
	generation =
	        atomic_load_explicit(&state->generation, memory_order_acquire);
	if (generation != context->_private[1]) {
		tr_dispatch_context_reference_drop(state);
		return TR_ERR_STALE;
	}
	*context = (struct tr_rpc_v2_call_context){0};
	tr_dispatch_context_reference_drop(state);
	tr_dispatch_context_drop_external(state);
	return TR_OK;
}

int tr_rpc_v2_context_acquire_internal(
        const struct tr_rpc_v2_call_context *context,
        struct tr_rpc_v2_context_state **out, uint64_t *deadline)
{
	struct tr_rpc_v2_context_state *state;
	uint64_t generation, now;
	int status, ret;
	if (!context || !out || !deadline)
		return TR_ERR_INVALID;
	if (!context->_private[0] || !context->_private[1])
		return TR_ERR_STALE;
	state = (struct tr_rpc_v2_context_state *)(uintptr_t)
	                context->_private[0];
	ret = tr_dispatch_context_reference_retain(state);
	if (ret != TR_OK)
		return ret;
	generation =
	        atomic_load_explicit(&state->generation, memory_order_acquire);
	if (generation != context->_private[1]) {
		tr_dispatch_context_reference_drop(state);
		return TR_ERR_STALE;
	}
	status = atomic_load_explicit(&state->status, memory_order_acquire);
	if (status != TR_RPC_V2_CONTEXT_ACTIVE) {
		tr_dispatch_context_reference_drop(state);
		return tr_dispatch_context_reject_status(status);
	}
	if (state->deadline) {
		ret = tr_dispatch_now(&now);
		if (ret != TR_OK || now >= state->deadline) {
			tr_dispatch_context_reference_drop(state);
			return ret == TR_OK ? TR_ERR_TIMEOUT : ret;
		}
	}
	ret = tr_dispatch_context_external_retain(state->dispatch);
	if (ret != TR_OK) {
		tr_dispatch_context_reference_drop(state);
		return ret;
	}
	status = atomic_load_explicit(&state->status, memory_order_acquire);
	if (status != TR_RPC_V2_CONTEXT_ACTIVE) {
		tr_dispatch_context_drop_external(state);
		return tr_dispatch_context_reject_status(status);
	}
	*out = state;
	*deadline = state->deadline;
	return TR_OK;
}

void tr_rpc_v2_context_release_internal(struct tr_rpc_v2_context_state *context)
{
	if (context)
		tr_dispatch_context_drop_external(context);
}

int tr_rpc_v2_context_status_internal(struct tr_rpc_v2_context_state *context,
                                      int *status)
{
	if (!context || !status)
		return TR_ERR_INVALID;
	*status = atomic_load_explicit(&context->status, memory_order_acquire);
	return TR_OK;
}

static int tr_dispatch_deadline_check(uint64_t deadline)
{
	uint64_t now;
	int ret;
	if (!deadline)
		return TR_OK;
	ret = tr_dispatch_now(&now);
	if (ret != TR_OK)
		return ret;
	return now >= deadline ? TR_ERR_TIMEOUT : TR_OK;
}
static struct tr_rpc_dispatch_handle
tr_dispatch_handle(struct tr_dispatch_slot *s)
{
	return (struct tr_rpc_dispatch_handle){
	        s->dispatch, {s->route.generation, s->route.slot}};
}
static struct tr_rpc_v2_call_id tr_dispatch_v2_id(struct tr_dispatch_slot *s)
{
	return (struct tr_rpc_v2_call_id){
	        {s->dispatch->instance, s->route.generation}};
}
static void tr_dispatch_peer_identity_release(struct tr_dispatch_slot *s)
{
	if (!s)
		return;
	tr_peer_identity_snapshot_release(s->peer_identity);
	s->peer_identity = NULL;
	if (s->peer_identity_budget_bytes) {
		(void)tr_memory_budget_release(s->dispatch->budget,
		                               s->peer_identity_budget_bytes);
		s->peer_identity_budget_bytes = 0U;
	}
	s->peer_identity_bound = false;
}
static int tr_dispatch_lookup(struct tr_rpc_dispatch_handle h,
                              struct tr_dispatch_slot **out)
{
	int ret = tr_dispatch_owner(h.dispatch);
	if (ret != TR_OK)
		return ret;
	if (h.id.slot >= h.dispatch->config.capacity || !h.id.generation)
		return TR_ERR_STALE;
	struct tr_dispatch_slot *s = &h.dispatch->slots[h.id.slot];
	if (!s->active || s->route.generation != h.id.generation)
		return TR_ERR_STALE;
	*out = s;
	return TR_OK;
}
static void tr_reply_body_release(struct tr_buffer *buffer)
{
	struct tr_reply_body *b = (struct tr_reply_body *)buffer;
	struct tr_memory_budget *budget = b->budget;
	size_t bytes = b->bytes;
	free(b);
	(void)tr_memory_budget_release(budget, bytes);
}
static void tr_reply_clear_bodies(struct tr_rpc_record_reply *reply)
{
	while (reply->head) {
		struct tr_reply_body *b = reply->head;
		reply->head = b->next;
		tr_buffer_release(&b->buffer);
	}
	reply->tail = NULL;
}
static void tr_reply_clear_metadata(struct tr_rpc_record_reply *reply,
                                    struct tr_reply_metadata *metadata)
{
	if (metadata->data) {
		free(metadata->data);
		(void)tr_memory_budget_release(reply->budget, metadata->size);
	}
	*metadata = (struct tr_reply_metadata){0};
}
static void tr_reply_clear(struct tr_rpc_record_reply *reply)
{
	tr_reply_clear_bodies(reply);
	tr_reply_clear_metadata(reply, &reply->initial);
	tr_reply_clear_metadata(reply, &reply->trailing);
}
int tr_rpc_record_reply_append(struct tr_rpc_record_reply *r, const void *data,
                               size_t size)
{
	return tr_rpc_record_reply_append_with_compression(
	        r, data, size, TR_RPC_COMPRESSION_NONE);
}

int tr_rpc_record_reply_append_with_compression(
        struct tr_rpc_record_reply *r, const void *data, size_t size,
        enum tr_rpc_compression compression)
{
	struct tr_reply_body *b;
	size_t bytes;
	int ret;
	if (!r)
		return TR_ERR_INVALID;
	if (r->asynchronous || r->finished || r->status != TR_RPC_STATUS_OK)
		return tr_reply_fail(r, TR_ERR_STATE,
		                     TR_RPC_STATUS_FAILED_PRECONDITION);
	if (compression != TR_RPC_COMPRESSION_NONE &&
	    compression != TR_RPC_COMPRESSION_ZSTD)
		return tr_reply_fail(r, TR_ERR_INVALID,
		                     TR_RPC_STATUS_INVALID_ARGUMENT);
	if (compression == TR_RPC_COMPRESSION_ZSTD &&
	    !(r->response_compression_algorithms &
	      TR_RPC_COMPRESSION_MASK_ZSTD))
		return tr_reply_fail(r, TR_ERR_UNSUPPORTED,
		                     TR_RPC_STATUS_INVALID_ARGUMENT);
	if ((!data && size) || size > r->max_message ||
	    r->count == r->max_count || size > r->limit - r->used ||
	    size > SIZE_MAX - sizeof(*b))
		return tr_reply_fail(r, TR_ERR_BAD_LENGTH,
		                     TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	bytes = sizeof(*b) + size;
	ret = tr_memory_budget_reserve(r->budget, bytes);
	if (ret != TR_OK)
		return tr_reply_fail(r, TR_ERR_NOMEM,
		                     TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	b = malloc(bytes);
	if (!b) {
		(void)tr_memory_budget_release(r->budget, bytes);
		return tr_reply_fail(r, TR_ERR_NOMEM,
		                     TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	}
	*b = (struct tr_reply_body){
	        .buffer = {.data = b->data,
	                   .capacity = (uint32_t)size,
	                   .len = (uint32_t)size,
	                   .release_cb = tr_reply_body_release},
	        .budget = r->budget,
	        .bytes = bytes,
	        .compression = compression};
	if (size)
		memcpy(b->data, data, size);
	if (r->tail)
		r->tail->next = b;
	else
		r->head = b;
	r->tail = b;
	r->used += size;
	++r->count;
	return TR_OK;
}
int tr_rpc_record_reply_set_metadata(struct tr_rpc_record_reply *reply,
                                     enum tr_rpc_v2_metadata_phase phase,
                                     const struct tr_rpc_metadata *entries,
                                     size_t count)
{
	uint8_t encoded[TR_RPC_METADATA_MAX_BYTES];
	struct tr_reply_metadata *metadata;
	uint8_t *copy = NULL;
	uint32_t size;
	int ret;
	if (!reply)
		return TR_ERR_INVALID;
	if (reply->asynchronous || reply->finished ||
	    reply->status != TR_RPC_STATUS_OK)
		return tr_reply_fail(reply, TR_ERR_STATE,
		                     TR_RPC_STATUS_FAILED_PRECONDITION);
	if (phase != TR_RPC_V2_METADATA_INITIAL &&
	    phase != TR_RPC_V2_METADATA_TRAILING)
		return tr_reply_fail(reply, TR_ERR_INVALID,
		                     TR_RPC_STATUS_INVALID_ARGUMENT);
	metadata = phase == TR_RPC_V2_METADATA_INITIAL ? &reply->initial
	                                               : &reply->trailing;
	if (metadata->set ||
	    (phase == TR_RPC_V2_METADATA_INITIAL && reply->headers_sent))
		return tr_reply_fail(reply, TR_ERR_STATE,
		                     TR_RPC_STATUS_FAILED_PRECONDITION);
	ret = tr_rpc_v2_metadata_encode(entries, count, encoded, &size);
	if (ret != TR_OK)
		return tr_reply_fail(reply, ret,
		                     TR_RPC_STATUS_INVALID_ARGUMENT);
	/* 编码先在有界栈上完成，验证失败不产生部分分配。每阶段至多一个
	 * 精确大小副本；空列表仍关闭该阶段，但无需分配零字节存储。 */
	if (size) {
		ret = tr_memory_budget_reserve(reply->budget, size);
		if (ret != TR_OK)
			return tr_reply_fail(reply, TR_ERR_NOMEM,
			                     TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		copy = malloc(size);
		if (!copy) {
			(void)tr_memory_budget_release(reply->budget, size);
			return tr_reply_fail(reply, TR_ERR_NOMEM,
			                     TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		}
		memcpy(copy, encoded, size);
	}
	*metadata = (struct tr_reply_metadata){copy, size, true};
	return TR_OK;
}
int tr_rpc_record_reply_finish(struct tr_rpc_record_reply *r,
                               enum tr_rpc_status status)
{
	if (r && r->asynchronous)
		return tr_reply_fail(r, TR_ERR_STATE,
		                     TR_RPC_STATUS_FAILED_PRECONDITION);
	if (!r || status < TR_RPC_STATUS_OK ||
	    status >= (int)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	if (r->finished)
		return TR_ERR_STATE;
	r->finished = true;
	if (r->status == TR_RPC_STATUS_OK)
		r->status = status;
	return TR_OK;
}
static void tr_dispatch_free_job(struct tr_dispatch_job *job)
{
	struct tr_dispatch_slot *s = job->slot;
	struct tr_memory_budget *budget = job->reply.budget;
	size_t bytes = job->bytes;
	tr_reply_clear(&job->reply);
	free(job);
	--s->pending;
	(void)tr_memory_budget_release(budget, bytes);
}
static void tr_dispatch_drop_queued(struct tr_dispatch_slot *s)
{
	while (s->head) {
		struct tr_dispatch_job *job = s->head;
		s->head = job->next;
		tr_dispatch_free_job(job);
	}
	s->tail = NULL;
}

static bool tr_dispatch_close_producer(struct tr_dispatch_slot *s)
{
	if (!s->producer)
		return true;
	/* 发送借用只持续到当前 step 的 complete。保留失败时的库内引用，
	 * 不能在下层发送尚未返回时释放其邮箱。 */
	if (tr_rpc_record_producer_close(s->producer) != TR_OK)
		return false;
	tr_rpc_record_producer_put(s->producer);
	s->producer = NULL;
	return true;
}
static enum tr_rpc_v2_delivery
tr_dispatch_v2_delivery(const struct tr_dispatch_slot *s)
{
	return s->delivery == TR_RPC_RECORD_OWNER_STOPPED
	               ? TR_RPC_V2_DELIVERY_SHUTDOWN
	               : TR_RPC_V2_DELIVERY_LIVE;
}
/* 路由已经终局不等于工作线程已经停止。只释放未提交/已交付的对象；
 * submitted 直到原执行器的 done 返回这里后才可以释放和通知用户。 */
static void tr_dispatch_settle(struct tr_dispatch_slot *s)
{
	struct tr_rpc_record_dispatch *d = s->dispatch;
	if (!s->terminal)
		return;
	/* 原owner已选定本地取消但业务尚未退出时，通知可能一直延后到
	 * 清理线程。交付域按实际执行位置决定，不能沿用旧owner标记。 */
	if (d->cleanup && !s->notified) {
		s->delivery = TR_RPC_RECORD_OWNER_STOPPED;
		if (!s->final_accepted) {
			s->status = TR_RPC_STATUS_CANCELLED;
		}
	}
	if (!tr_dispatch_close_producer(s))
		return;
	tr_dispatch_drop_queued(s);
	if (s->ready) {
		tr_dispatch_free_job(s->ready);
		s->ready = NULL;
	}
	if (s->submitted) {
		(void)tr_task_executor_cancel(d->executor, s->task);
		return;
	}
	if (!s->notified) {
		s->notified = true;
		/* 网络终局可早于原工作线程退出，只有此收口兑现业务结果。
		 * 未经完整 OPEN 准入的拒绝只清理路由，不伪造一次 RPC。 */
		if (s->semantic_started)
			tr_rpc_record_semantic_finish(d->semantic, s->status);
		++d->depth;
		if (s->v2_open_observed && s->v2_method &&
		    s->v2_method->v2_observer) {
			struct tr_rpc_v2_observation observation = {
			        .call = tr_dispatch_v2_id(s),
			        .event = TR_RPC_V2_OBSERVE_TERMINAL,
			        .request = NULL,
			        .status = s->status,
			        .delivery = tr_dispatch_v2_delivery(s)};
			++tr_v2_callback_depth;
			s->v2_method->v2_observer(&observation,
			                          s->v2_method->arg);
			--tr_v2_callback_depth;
		}
		if (s->v2_method && s->v2_method->v2_terminal) {
			++tr_v2_callback_depth;
			s->v2_method->v2_terminal(
			        tr_dispatch_v2_id(s), s->status,
			        tr_dispatch_v2_delivery(s), s->v2_method->arg);
			--tr_v2_callback_depth;
		}
		d->terminal(tr_dispatch_handle(s), s->stream, s->status,
		            s->delivery, s->final_accepted, d->arg);
		--d->depth;
	}
}
static void tr_dispatch_route_terminal(struct tr_rpc_route_handle route,
                                       struct tr_stream_handle stream,
                                       enum tr_rpc_status status,
                                       enum tr_rpc_record_delivery delivery,
                                       void *arg)
{
	struct tr_rpc_record_dispatch *d = arg;
	struct tr_dispatch_slot *s = &d->slots[route.slot];
	(void)stream;
	s->terminal = true;
	s->status = status;
	s->delivery = delivery;
	tr_dispatch_context_publish_terminal(s, status);
	/* 不在下层发送栈内释放 ready，也不在下层路由深度内通知/退役。 */
	if (s->submitted)
		(void)tr_task_executor_cancel(d->executor, s->task);
}
static void tr_dispatch_local_abort(struct tr_dispatch_slot *s,
                                    enum tr_rpc_status status)
{
	if (!s->terminal)
		(void)tr_rpc_record_router_abort_on_owner(s->route, status);
}
/* 期限仅由 owner 提交到原有终局状态；时钟失败不能冒充超时。每个
 * 任务准入/完成/发送边界重新取时，不能把一轮 pump 的旧时间用于其他调用。 */
static bool tr_dispatch_live(struct tr_dispatch_slot *s)
{
	int ret;
	if (s->terminal)
		return false;
	ret = tr_dispatch_deadline_check(s->open_due ? s->open_due
	                                             : s->deadline);
	if (ret == TR_OK)
		return true;
	if (s->open_due)
		s->admission_failed = true;
	tr_dispatch_local_abort(s, ret == TR_ERR_TIMEOUT
	                                   ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                   : TR_RPC_STATUS_INTERNAL);
	return false;
}
static enum tr_rpc_status
tr_dispatch_v2_request_init(const struct tr_rpc_record_view *view,
                            struct tr_peer_identity_snapshot *peer_identity,
                            struct tr_rpc_v2_request *request)
{
	*request = (struct tr_rpc_v2_request){0};
	switch (view->header.kind) {
	case TR_RPC_RECORD_OPEN:
		request->kind = TR_RPC_V2_REQUEST_OPEN;
		break;
	case TR_RPC_RECORD_MESSAGE:
		request->kind = TR_RPC_V2_REQUEST_MESSAGE;
		break;
	case TR_RPC_RECORD_HALF_CLOSE:
		request->kind = TR_RPC_V2_REQUEST_HALF_CLOSE;
		break;
	default:
		return TR_RPC_STATUS_INTERNAL;
	}
	request->body =
	        (struct tr_rpc_bytes){view->body, view->header.decoded_size};
	tr_rpc_v2_metadata_view_init(&request->metadata, view->metadata,
	                             view->header.metadata_size);
	if (peer_identity) {
		if (tr_peer_identity_snapshot_view(
		            peer_identity, &request->peer_identity) != TR_OK)
			return TR_RPC_STATUS_INTERNAL;
	} else {
		request->peer_identity = (struct tr_peer_identity_view){
		        .authentication = TR_PEER_IDENTITY_NONE,
		        .uri_san = NULL,
		        .uri_san_len = 0U,
		};
	}
	return TR_RPC_STATUS_OK;
}

static enum tr_rpc_status
tr_dispatch_v2_work(struct tr_dispatch_job *job,
                    const struct tr_task_cancel *cancel)
{
	struct tr_rpc_v2_request request;
	struct tr_rpc_v2_cancel public_cancel = {cancel, job->slot};
	struct tr_rpc_v2_reply public_reply = {&job->reply};
	const struct tr_rpc_v2_cancel *prior_cancel = tr_v2_active_cancel;
	struct tr_rpc_v2_reply *prior_reply = tr_v2_active_reply;
	enum tr_rpc_status result;
	result = tr_dispatch_v2_request_init(
	        &job->view, job->slot->peer_identity, &request);
	if (result != TR_RPC_STATUS_OK)
		return result;
	tr_v2_active_cancel = &public_cancel;
	tr_v2_active_reply = &public_reply;
	++tr_v2_callback_depth;
	result = job->v2_work(job->v2_id, &request, &public_cancel,
	                      &public_reply, job->arg);
	--tr_v2_callback_depth;
	tr_v2_active_cancel = prior_cancel;
	tr_v2_active_reply = prior_reply;
	return result;
}

static int tr_dispatch_v2_authorize(struct tr_dispatch_job *job,
                                    const struct tr_task_cancel *cancel)
{
	struct tr_rpc_v2_request request;
	struct tr_rpc_v2_cancel public_cancel = {cancel, job->slot};
	const struct tr_rpc_v2_cancel *prior_cancel = tr_v2_active_cancel;
	struct tr_rpc_v2_reply *prior_reply = tr_v2_active_reply;
	enum tr_rpc_status status;

	if (!job->v2_authorize)
		return TR_ERR_STATE;
	status = tr_dispatch_v2_request_init(
	        &job->view, job->slot->peer_identity, &request);
	if (status != TR_RPC_STATUS_OK)
		return TR_ERR_STATE;
	tr_v2_active_cancel = &public_cancel;
	tr_v2_active_reply = NULL;
	++tr_v2_callback_depth;
	job->authorization_result = job->v2_authorize(job->v2_id, &request,
	                                              &public_cancel, job->arg);
	--tr_v2_callback_depth;
	tr_v2_active_cancel = prior_cancel;
	tr_v2_active_reply = prior_reply;
	return TR_OK;
}
static int tr_dispatch_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct tr_dispatch_job *job = arg;
	struct tr_rpc_record_reply *reply = &job->reply;
	enum tr_rpc_status status = TR_RPC_STATUS_CANCELLED;
	if (job->stage == TR_DISPATCH_JOB_AUTHORIZATION) {
		if (tr_task_cancel_requested(cancel))
			return TR_OK;
		int ret = tr_dispatch_deadline_check(job->deadline);
		if (ret != TR_OK)
			return ret;
		return tr_dispatch_v2_authorize(job, cancel);
	}
	if (!tr_task_cancel_requested(cancel)) {
		/* 任务可能在共享执行器排队到期限之后才开始。这里只读取
		 * 提交前固定的期限并返回私有结果，绝不触碰 owner 槽位/路由。
		 * 业务执行中的取消仍通过原执行器令牌合作观察。 */
		int ret = tr_dispatch_deadline_check(job->deadline);
		if (ret != TR_OK)
			return ret;
		status = job->v2_work ? tr_dispatch_v2_work(job, cancel)
		                      : job->work(job->id, &job->view, cancel,
		                                  reply, job->arg);
	}
	if (status < TR_RPC_STATUS_OK || status >= (int)TR_RPC_STATUS_COUNT)
		status = TR_RPC_STATUS_INTERNAL;
	if (status != TR_RPC_STATUS_OK && !reply->failed)
		reply->status = status;
	if (job->view.header.kind == TR_RPC_RECORD_HALF_CLOSE &&
	    !reply->asynchronous)
		reply->finished = true;
	/* 尾部仅属于当前终局批次，不能静默丢弃或跨事件长期借用。显式
	 * 业务错误同样形成终局，允许它携带此前设置的有效尾部元数据。 */
	if (reply->trailing.set && !reply->finished &&
	    reply->status == TR_RPC_STATUS_OK)
		(void)tr_reply_fail(reply, TR_ERR_STATE,
		                    TR_RPC_STATUS_FAILED_PRECONDITION);
	if (reply->status != TR_RPC_STATUS_OK) {
		reply->finished = true;
		if (reply->failed)
			tr_reply_clear(reply);
		else
			tr_reply_clear_bodies(reply);
	}
	return TR_OK;
}
static bool tr_dispatch_authorization_result_valid(
        struct tr_rpc_v2_authorization_result result)
{
	if (result.decision == TR_RPC_V2_AUTHORIZATION_ALLOW)
		return result.rejection_status == TR_RPC_STATUS_OK;
	if (result.decision == TR_RPC_V2_AUTHORIZATION_REJECT)
		return result.rejection_status > TR_RPC_STATUS_OK &&
		       result.rejection_status < (int)TR_RPC_STATUS_COUNT;
	return false;
}
static void tr_dispatch_authorization_done(struct tr_dispatch_job *job,
                                           struct tr_task_result result)
{
	struct tr_dispatch_slot *s = job->slot;
	struct tr_rpc_record_dispatch *d = s->dispatch;
	struct tr_rpc_v2_authorization_result authorization =
	        job->authorization_result;
	struct tr_rpc_v2_call_id live_id = tr_dispatch_v2_id(s);
	bool current = s->auth_job == job &&
	               job->view.header.kind == TR_RPC_RECORD_OPEN &&
	               job->id.slot == s->route.slot &&
	               job->id.generation == s->route.generation &&
	               memcmp(&job->v2_id, &live_id, sizeof(live_id)) == 0;
	s->submitted = NULL;
	if (s->auth_job == job)
		s->auth_job = NULL;
	if (!current) {
		tr_dispatch_free_job(job);
		if (!d->cleanup && !s->terminal && !d->stopping)
			tr_dispatch_local_abort(s, TR_RPC_STATUS_INTERNAL);
		tr_dispatch_settle(s);
		return;
	}
	if (d->cleanup || s->terminal || d->stopping) {
		tr_dispatch_free_job(job);
		tr_dispatch_settle(s);
		return;
	}
	if (!tr_dispatch_live(s)) {
		tr_dispatch_free_job(job);
		tr_dispatch_settle(s);
		return;
	}
	if (result.cancelled || result.work_status != TR_OK) {
		enum tr_rpc_status status =
		        result.cancelled ? TR_RPC_STATUS_CANCELLED
		        : result.work_status == TR_ERR_TIMEOUT
		                ? TR_RPC_STATUS_DEADLINE_EXCEEDED
		        : result.work_status == TR_ERR_STATE
		                ? TR_RPC_STATUS_INTERNAL
		                : TR_RPC_STATUS_UNAVAILABLE;
		tr_dispatch_free_job(job);
		tr_dispatch_local_abort(s, status);
		tr_dispatch_settle(s);
		return;
	}
	if (!tr_dispatch_authorization_result_valid(authorization)) {
		job->reply.finished = true;
		job->reply.status = TR_RPC_STATUS_INTERNAL;
		s->ready = job;
		tr_dispatch_drop_queued(s);
	} else if (authorization.decision == TR_RPC_V2_AUTHORIZATION_REJECT) {
		job->reply.finished = true;
		job->reply.status = authorization.rejection_status;
		s->ready = job;
		tr_dispatch_drop_queued(s);
	} else {
		/* 原 OPEN job 先于认证期间到达的记录重新排队；pending 容量和
		 * 输入副本沿用原对象，不创建第二份工作或额外队列。 */
		s->authorized = true;
		job->stage = TR_DISPATCH_JOB_APPLICATION;
		job->next = s->head;
		s->head = job;
		if (!s->tail)
			s->tail = job;
	}
	tr_dispatch_settle(s);
}
static void tr_dispatch_done(void *arg, struct tr_task_handle handle,
                             struct tr_task_result result)
{
	struct tr_dispatch_job *job = arg;
	struct tr_dispatch_slot *s = job->slot;
	struct tr_rpc_record_dispatch *d = s->dispatch;
	bool acquired = job->reply.producer != NULL;
	(void)handle;
	if (job->stage == TR_DISPATCH_JOB_AUTHORIZATION) {
		tr_dispatch_authorization_done(job, result);
		return;
	}
	/* executor完成与本调用的submitted存储一一对应，路由在此之前不能退役。
	 * 清理域须由reap先授权；外层不能先换owner交付，再追认授权。 */
	s->submitted = NULL;
	if (job->reply.producer) {
		/* work 返回后才读取其可变结果。此前库内引用始终归 job；
		 * 即使应用已归还自身引用，胶囊也能在此安全关闭或激活。 */
		s->producer = job->reply.producer;
		job->reply.producer = NULL;
		s->asynchronous = true;
	}
	if (d->cleanup || s->terminal || d->stopping) {
		(void)tr_dispatch_close_producer(s);
		tr_dispatch_free_job(job);
	} else if (s->producer_failed) {
		/* 错误 FINAL 已由发送边界先选定。迟到的业务结果只清理，
		 * 仍在本次完成边界检查期限，终局通知仍须等待这个 done。 */
		tr_dispatch_free_job(job);
		(void)tr_dispatch_live(s);
	} else if (result.cancelled || result.work_status != TR_OK) {
		enum tr_rpc_status status = TR_RPC_STATUS_INTERNAL;
		if (result.cancelled)
			status = TR_RPC_STATUS_CANCELLED;
		else if (result.work_status == TR_ERR_TIMEOUT)
			status = TR_RPC_STATUS_DEADLINE_EXCEEDED;
		tr_dispatch_free_job(job);
		tr_dispatch_local_abort(s, status);
	} else if (!tr_dispatch_live(s)) {
		/* 工作完成不等于回复已经被接受；迟到成功只释放其拥有型结果。 */
		tr_dispatch_free_job(job);
	} else if (s->asynchronous && job->reply.status == TR_RPC_STATUS_OK) {
		if (acquired && s->producer)
			tr_rpc_record_producer_activate(s->producer);
		/* 异步输出不占用已完成的输入事件，也不让邮箱背压阻塞
		 * 后续请求。仍只在 done 后提交同调用的下一次 work。 */
		tr_dispatch_free_job(job);
	} else {
		if (s->asynchronous)
			(void)tr_dispatch_close_producer(s);
		s->ready = job;
		if (job->reply.finished)
			tr_dispatch_drop_queued(s);
	}
	tr_dispatch_settle(s);
}
static void tr_dispatch_received(struct tr_rpc_route_handle route,
                                 const struct tr_rpc_record_view *view,
                                 void *arg)
{
	struct tr_dispatch_method *m = arg;
	struct tr_rpc_record_dispatch *d = m->dispatch;
	struct tr_dispatch_slot *s = &d->slots[route.slot];
	struct tr_dispatch_job *job;
	enum tr_rpc_status hook_rejection = TR_RPC_STATUS_OK;
	size_t bytes, payload, body_size;
	if (s->terminal || s->producer_failed || d->stopping ||
	    (s->ready && s->ready->reply.finished))
		return;
	/* 解码和元数据验证可能跨过外层接收入口的取时边界。完整有效 OPEN
	 * 再次检查固定准入期限，然后才清除它并建立远端相对 CALL 期限。
	 * 两种期限不能互相续期；工作任务只继承完整准入后的 CALL 期限。 */
	if (view->header.kind == TR_RPC_RECORD_OPEN) {
		if (!tr_dispatch_live(s))
			return;
		s->open_due = 0;
		if (view->header.remaining_timeout_ns) {
			uint64_t now;
			if (tr_dispatch_now(&now) != TR_OK) {
				tr_dispatch_local_abort(s,
				                        TR_RPC_STATUS_INTERNAL);
				return;
			}
			uint64_t delta = view->header.remaining_timeout_ns;
			s->deadline = delta > UINT64_MAX - now ? UINT64_MAX
			                                       : now + delta;
		}
	}
	if (!tr_dispatch_live(s))
		return;
	if (view->header.kind == TR_RPC_RECORD_OPEN) {
		if (!s->semantic_started) {
			s->semantic_started = true;
			tr_rpc_record_semantic_start(d->semantic);
		}
		if (m->method.v2_work) {
			s->v2_method = &m->method;
			if (!s->peer_identity_bound) {
				struct tr_peer_identity_snapshot *identity =
				        NULL;
				int identity_status =
				        tr_channel_peer_identity_snapshot_on_owner(
				                d->channel, &identity);
				if (identity_status != TR_OK) {
					tr_dispatch_local_abort(
					        s,
					        identity_status == TR_ERR_NOMEM
					                ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
					                : TR_RPC_STATUS_INTERNAL);
					return;
				}
				if (identity) {
					struct tr_peer_identity_view
					        identity_view;
					identity_status =
					        tr_peer_identity_snapshot_view(
					                identity,
					                &identity_view);
					if (identity_status != TR_OK ||
					    tr_memory_budget_reserve(
					            d->budget,
					            identity_view
					                    .uri_san_len) !=
					            TR_OK) {
						tr_dispatch_local_abort(
						        s,
						        TR_RPC_STATUS_RESOURCE_EXHAUSTED);
						return;
					}
					identity_status =
					        tr_peer_identity_snapshot_retain(
					                identity);
					if (identity_status != TR_OK) {
						(void)tr_memory_budget_release(
						        d->budget,
						        identity_view
						                .uri_san_len);
						tr_dispatch_local_abort(
						        s,
						        TR_RPC_STATUS_RESOURCE_EXHAUSTED);
						return;
					}
					s->peer_identity_budget_bytes =
					        identity_view.uri_san_len;
				}
				s->peer_identity = identity;
				s->peer_identity_bound = true;
			}
			struct tr_rpc_v2_request request;
			if (tr_dispatch_v2_request_init(view, s->peer_identity,
			                                &request) !=
			    TR_RPC_STATUS_OK) {
				tr_dispatch_local_abort(s,
				                        TR_RPC_STATUS_INTERNAL);
				return;
			}
			if (s->v2_method->v2_observer) {
				struct tr_rpc_v2_observation observation = {
				        .call = tr_dispatch_v2_id(s),
				        .event = TR_RPC_V2_OBSERVE_OPEN,
				        .request = &request,
				        .status = TR_RPC_STATUS_OK,
				        .delivery = TR_RPC_V2_DELIVERY_LIVE};
				s->v2_open_observed = true;
				++tr_v2_callback_depth;
				s->v2_method->v2_observer(&observation,
				                          s->v2_method->arg);
				--tr_v2_callback_depth;
			}
			if (s->v2_method->v2_admit) {
				struct tr_rpc_v2_admission_result admission;
				bool valid;
				++tr_v2_callback_depth;
				admission = s->v2_method->v2_admit(
				        tr_dispatch_v2_id(s), &request,
				        s->v2_method->arg);
				--tr_v2_callback_depth;
				valid = admission.decision ==
				                        TR_RPC_V2_ADMISSION_ACCEPT
				                ? admission.rejection_status ==
				                          TR_RPC_STATUS_OK
				        : admission.decision ==
				                        TR_RPC_V2_ADMISSION_REJECT
				                ? admission.rejection_status >
				                                  TR_RPC_STATUS_OK &&
				                          admission.rejection_status <
				                                  (int)TR_RPC_STATUS_COUNT
				                : false;
				if (!valid) {
					hook_rejection = TR_RPC_STATUS_INTERNAL;
				} else if (admission.decision ==
				           TR_RPC_V2_ADMISSION_REJECT)
					hook_rejection =
					        admission.rejection_status;
			}
		}
	}
	if (s->pending >= d->config.max_pending_per_call) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	/* Stream 层交付的是解码后的正文；压缩记录的 encoded_size 只描述
	 * 线上帧。V2 业务任务必须按 decoded_size 复制完整正文。 */
	body_size = view->header.decoded_size;
	payload = view->header.metadata_size;
	if (body_size > SIZE_MAX - payload ||
	    payload + body_size > SIZE_MAX - sizeof(*job)) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	payload += body_size;
	bytes = sizeof(*job) + payload;
	if (tr_memory_budget_reserve(d->budget, bytes) != TR_OK) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	job = calloc(1, bytes);
	if (!job) {
		(void)tr_memory_budget_release(d->budget, bytes);
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	job->slot = s;
	job->id = tr_dispatch_handle(s).id;
	job->view.header = view->header;
	job->work = m->method.work;
	job->v2_work = m->method.v2_work;
	job->v2_authorize = m->method.v2_authorize;
	job->v2_id = tr_dispatch_v2_id(s);
	job->arg = m->method.arg;
	job->bytes = bytes;
	job->reply = (struct tr_rpc_record_reply){
	        .budget = d->budget,
	        .producer_domain = d->producer_domain,
	        .limit = d->config.max_reply_bytes,
	        .max_count = d->config.max_replies_per_event,
	        .max_message = m->method.descriptor.max_response_bytes,
	        .response_compression_algorithms =
	                m->method.descriptor.response_compression_algorithms};
	job->deadline = s->deadline;
	if (view->header.metadata_size) {
		memcpy(job->data, view->metadata, view->header.metadata_size);
		job->view.metadata = job->data;
	}
	if (body_size) {
		uint8_t *body = job->data + view->header.metadata_size;
		memcpy(body, view->body, body_size);
		job->view.body = body;
	}
	s->response_codec = m->method.descriptor.response_codec_id;
	if (hook_rejection != TR_RPC_STATUS_OK) {
		/* 钩子拒绝是 RPC 级结果，沿用 OPEN job 的有界回复存储交付
		 * FINAL；它不应被降级为仅服务端可见的本地 route abort。 */
		job->reply.finished = true;
		job->reply.status = hook_rejection;
		s->ready = job;
		++s->pending;
		return;
	}
	if (s->tail)
		s->tail->next = job;
	else
		s->head = job;
	s->tail = job;
	++s->pending;
}
static bool tr_dispatch_submit(struct tr_dispatch_slot *s)
{
	struct tr_rpc_record_dispatch *d = s->dispatch;
	struct tr_dispatch_job *job;
	struct tr_task_spec spec;
	bool authorize;
	int ret;
	if (s->terminal || s->producer_failed || s->submitted || !s->head ||
	    d->stopping)
		return false;
	job = s->head;
	authorize = job->view.header.kind == TR_RPC_RECORD_OPEN &&
	            job->v2_authorize && !s->authorized;
	job->stage = authorize ? TR_DISPATCH_JOB_AUTHORIZATION
	                       : TR_DISPATCH_JOB_APPLICATION;
	/* 排队期间前批可能已发 INITIAL 或导出生产者，只在实际提交时
	 * 固定 worker 的快照。worker 不读取 owner 的可变槽位。 */
	job->reply.headers_sent = s->headers_sent;
	job->reply.asynchronous = s->asynchronous;
	spec = (struct tr_task_spec){tr_dispatch_work, tr_dispatch_done, job};
	ret = tr_task_executor_submit(d->executor, &spec, &s->task);
	if (ret == TR_OK) {
		s->head = job->next;
		if (!s->head)
			s->tail = NULL;
		job->next = NULL;
		if (authorize)
			s->auth_job = job;
		s->submitted = job;
		return true;
	}
	if (ret != TR_AGAIN) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_UNAVAILABLE);
		tr_dispatch_settle(s);
		return true;
	}
	return false;
}

/* 返回 true 表示发送/终局状态已经前进。take 只借用一个邮箱命令；补
 * 空 INITIAL 或传输 AGAIN 都不会提前归还消息容量。发送栈外 complete
 * 后才可能关闭胶囊，避免路由终局重入释放仍在使用的正文或元数据。 */
static bool tr_dispatch_producer_step(struct tr_dispatch_slot *s)
{
	struct tr_rpc_v2_producer *producer = s->producer;
	struct tr_rpc_record_producer_command command;
	struct tr_rpc_record_header header = {0};
	struct tr_buffer *body;
	const uint8_t *metadata;
	int ret = tr_rpc_record_producer_take(producer, &command);
	if (ret == TR_AGAIN)
		return false;
	if (ret != TR_OK) {
		tr_dispatch_local_abort(s, ret == TR_ERR_CLOSED
		                                   ? TR_RPC_STATUS_CANCELLED
		                                   : TR_RPC_STATUS_INTERNAL);
		tr_dispatch_settle(s);
		return true;
	}
	header.kind = command.kind;
	header.status = command.status;
	header.metadata_size = command.metadata_size;
	metadata = command.metadata;
	body = command.body;
	if (command.kind == TR_RPC_RECORD_MESSAGE) {
		if (!s->headers_sent) {
			header.kind = TR_RPC_RECORD_INITIAL_METADATA;
			body = NULL;
		} else {
			header.codec_id = s->response_codec;
			header.compression = (uint32_t)command.compression;
			header.decoded_size = body ? body->len : 0;
			header.encoded_size =
			        command.compression == TR_RPC_COMPRESSION_NONE
			                ? header.decoded_size
			                : 0U;
		}
	}
	ret = tr_rpc_record_router_send_on_owner(s->route, &header, metadata,
	                                         &body);
	tr_rpc_record_producer_complete(
	        producer, ret == TR_OK && header.kind == command.kind);
	if (ret == TR_OK) {
		if (header.kind == TR_RPC_RECORD_INITIAL_METADATA)
			s->headers_sent = true;
		else if (header.kind == TR_RPC_RECORD_FINAL_STATUS)
			s->final_accepted = true;
	} else if (ret != TR_AGAIN) {
		/* 与同步回复相同，基数失败转为合法错误 FINAL；原 FINAL
		 * 已验证的尾部保留，失败的 MESSAGE/INITIAL 则不再发送。 */
		if (ret == TR_ERR_STATE && !s->terminal &&
		    !(command.kind == TR_RPC_RECORD_FINAL_STATUS &&
		      command.status == TR_RPC_STATUS_INTERNAL)) {
			int fail_ret = tr_rpc_record_producer_fail(
			        producer, TR_RPC_STATUS_INTERNAL);
			if (fail_ret == TR_OK)
				s->producer_failed = true;
			else
				tr_dispatch_local_abort(
				        s, fail_ret == TR_ERR_CLOSED
				                   ? TR_RPC_STATUS_CANCELLED
				                   : TR_RPC_STATUS_INTERNAL);
			tr_dispatch_drop_queued(s);
		} else {
			tr_dispatch_local_abort(
			        s, ret == TR_ERR_BAD_LENGTH
			                   ? TR_RPC_STATUS_INVALID_ARGUMENT
			           : ret == TR_ERR_NOMEM
			                   ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
			                   : TR_RPC_STATUS_UNAVAILABLE);
		}
	}
	tr_dispatch_settle(s);
	return ret != TR_AGAIN || s->terminal;
}

/* 同步批次仍阻止下一 work 越过其回复；导出后，请求任务和生产者输出
 * 公平交替。各方向内部保持 FIFO，任一侧的背压不阻止另一侧前进。 */
static void tr_dispatch_step(struct tr_dispatch_slot *s)
{
	struct tr_dispatch_job *job = s->ready;
	int ret;
	if (!tr_dispatch_live(s)) {
		tr_dispatch_settle(s);
		return;
	}
	if (s->producer && tr_rpc_record_producer_abandoned(s->producer)) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_CANCELLED);
		tr_dispatch_settle(s);
		return;
	}
	if (!job) {
		bool tried_producer = false;
		if (s->producer &&
		    (s->producer_turn || s->submitted || !s->head)) {
			tried_producer = true;
			if (tr_dispatch_producer_step(s)) {
				s->producer_turn = false;
				return;
			}
		}
		if (tr_dispatch_submit(s)) {
			s->producer_turn = true;
			return;
		}
		if (s->producer && !tried_producer)
			(void)tr_dispatch_producer_step(s);
		s->producer_turn = false;
		return;
	}
	struct tr_rpc_record_reply *reply = &job->reply;
	struct tr_rpc_record_header h = {0};
	struct tr_reply_body *b = reply->head;
	struct tr_reply_body *next = b ? b->next : NULL;
	struct tr_buffer *body = b ? &b->buffer : NULL;
	const uint8_t *metadata = NULL;
	if (!s->headers_sent && (b || reply->initial.set)) {
		h.kind = TR_RPC_RECORD_INITIAL_METADATA;
		h.metadata_size = reply->initial.size;
		metadata = reply->initial.data;
		body = NULL;
	} else if (b) {
		h.kind = TR_RPC_RECORD_MESSAGE;
		h.codec_id = s->response_codec;
		h.compression = (uint32_t)b->compression;
		h.decoded_size = body->len;
		h.encoded_size = b->compression == TR_RPC_COMPRESSION_NONE
		                         ? body->len
		                         : 0U;
	} else if (reply->finished) {
		h.kind = TR_RPC_RECORD_FINAL_STATUS;
		h.status = reply->status;
		h.metadata_size = reply->trailing.size;
		metadata = reply->trailing.data;
	} else {
		s->ready = NULL;
		tr_dispatch_free_job(job);
		return;
	}
	ret = tr_rpc_record_router_send_on_owner(s->route, &h, metadata, &body);
	if (ret == TR_OK) {
		if (h.kind == TR_RPC_RECORD_INITIAL_METADATA) {
			s->headers_sent = true;
			tr_reply_clear_metadata(reply, &reply->initial);
		} else if (h.kind == TR_RPC_RECORD_MESSAGE) {
			/* b可以已经由传输释放；next在接受前取得，之后不再读b。
			 */
			reply->head = next;
			if (!next)
				reply->tail = NULL;
		} else {
			s->final_accepted = true;
			tr_reply_clear_metadata(reply, &reply->trailing);
		}
	} else if (ret != TR_AGAIN) {
		/* 非背压失败不能伪装成成功；停止重试坏回复。若是业务基数
		 * 错误且绑定仍活跃，下一轮用合法错误FINAL解释失败。 */
		if (ret == TR_ERR_STATE && !s->terminal &&
		    !(reply->finished &&
		      reply->status == TR_RPC_STATUS_INTERNAL)) {
			tr_reply_clear_bodies(reply);
			reply->finished = true;
			reply->status = TR_RPC_STATUS_INTERNAL;
			tr_dispatch_drop_queued(s);
		} else {
			tr_dispatch_local_abort(
			        s, ret == TR_ERR_BAD_LENGTH
			                   ? TR_RPC_STATUS_INVALID_ARGUMENT
			           : ret == TR_ERR_NOMEM
			                   ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
			                   : TR_RPC_STATUS_UNAVAILABLE);
		}
	}
	tr_dispatch_settle(s);
}

int tr_rpc_record_dispatch_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_dispatch_config *config,
        tr_rpc_dispatch_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_dispatch **out)
{
	struct tr_rpc_record_dispatch *d;
	struct tr_rpc_record_method *routes;
	size_t bytes, method_bytes, temporary;
	uint64_t instance;
	int ret;
	if (!channel || !executor || !methods || !method_count || !config ||
	    !config->capacity || !config->max_pending_per_call ||
	    !config->max_replies_per_event || !terminal || !budget ||
	    !budget->limit_bytes || !out)
		return TR_ERR_INVALID;
	if (*out ||
	    !tr_reactor_in_owner_context_for(tr_channel_reactor(channel)) ||
	    !tr_task_executor_is_current_owner(executor))
		return TR_ERR_STATE;
	if (config->capacity > tr_channel_max_streams(channel))
		return TR_ERR_INVALID;
	if (sizeof(struct tr_dispatch_slot) >
	    (SIZE_MAX - sizeof(*d)) / config->capacity)
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*d) + config->capacity * sizeof(struct tr_dispatch_slot);
	if (config->capacity >
	    (SIZE_MAX - bytes) / sizeof(struct tr_rpc_v2_context_state))
		return TR_ERR_BAD_LENGTH;
	bytes += config->capacity * sizeof(struct tr_rpc_v2_context_state);
	if (config->capacity >
	    (SIZE_MAX - bytes) / sizeof(struct tr_dispatch_context_link))
		return TR_ERR_BAD_LENGTH;
	bytes += config->capacity * sizeof(struct tr_dispatch_context_link);
	if (method_count >
	    (SIZE_MAX - bytes) / sizeof(struct tr_dispatch_method))
		return TR_ERR_BAD_LENGTH;
	method_bytes = method_count * sizeof(struct tr_dispatch_method);
	bytes += method_bytes;
	if (method_count > (SIZE_MAX - bytes) / sizeof(*routes))
		return TR_ERR_BAD_LENGTH;
	temporary = method_count * sizeof(*routes);
	if (temporary > SIZE_MAX - bytes)
		return TR_ERR_BAD_LENGTH;
	for (size_t i = 0; i < method_count; ++i)
		if ((methods[i].work == NULL) == (methods[i].v2_work == NULL) ||
		    ((methods[i].v2_terminal || methods[i].v2_observer ||
		      methods[i].v2_admit || methods[i].v2_authorize) &&
		     !methods[i].v2_work))
			return TR_ERR_INVALID;
	/* 实例编号与路由代次共同形成公共身份。即使创建失败也不复用
	 * 已领取编号，避免对象地址复用使不同对端的调用身份发生碰撞。 */
	ret = tr_dispatch_instance(&instance);
	if (ret != TR_OK)
		return ret;
	ret = tr_memory_budget_reserve(budget, bytes + temporary);
	if (ret != TR_OK)
		return ret;
	d = calloc(1, sizeof(*d));
	routes = malloc(temporary);
	if (!d || !routes) {
		free(d);
		free(routes);
		(void)tr_memory_budget_release(budget, bytes + temporary);
		return TR_ERR_NOMEM;
	}
	d->slots = calloc(config->capacity, sizeof(*d->slots));
	d->contexts = calloc(config->capacity, sizeof(*d->contexts));
	d->context_links = calloc(config->capacity, sizeof(*d->context_links));
	d->methods = malloc(method_bytes);
	if (!d->slots || !d->contexts || !d->context_links || !d->methods) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	d->reactor = tr_channel_reactor(channel);
	d->channel = channel;
	d->executor = executor;
	d->budget = budget;
	d->bytes = bytes;
	d->instance = instance;
	d->config = *config;
	d->semantic = &d->local_semantic;
	d->terminal = terminal;
	d->arg = arg;
	atomic_init(&d->external_context_refs, 0);
	for (size_t i = 0; i < method_count; ++i) {
		d->methods[i] = (struct tr_dispatch_method){d, methods[i]};
		routes[i] = (struct tr_rpc_record_method){methods[i].descriptor,
		                                          tr_dispatch_received,
		                                          &d->methods[i]};
	}
	for (uint32_t i = 0; i < config->capacity; ++i) {
		d->slots[i].dispatch = d;
		d->contexts[i].dispatch = d;
		atomic_init(&d->contexts[i].references, 0);
		atomic_init(&d->contexts[i].generation, 0);
		atomic_init(&d->contexts[i].status, TR_RPC_V2_CONTEXT_ACTIVE);
		atomic_init(&d->context_links[i].locked, false);
		d->context_links[i].parent_status = TR_RPC_V2_CONTEXT_ACTIVE;
	}
	ret = tr_rpc_record_router_create_on_owner(
	        channel, routes, method_count, config->capacity,
	        tr_dispatch_route_terminal, d, budget, &d->router);
	if (ret != TR_OK)
		goto fail;
	free(routes);
	(void)tr_memory_budget_release(budget, temporary);
	*out = d;
	return TR_OK;
fail:
	free(routes);
	free(d->methods);
	free(d->context_links);
	free(d->contexts);
	free(d->slots);
	free(d);
	(void)tr_memory_budget_release(budget, bytes + temporary);
	return ret;
}
int tr_rpc_record_dispatch_bind_producers_on_owner(
        struct tr_rpc_record_dispatch *d,
        struct tr_rpc_record_producer_domain *domain)
{
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (!domain)
		return TR_ERR_INVALID;
	if (d->depth || d->stopping || d->producer_domain || d->admitted)
		return TR_ERR_STATE;
	d->producer_domain = domain;
	return TR_OK;
}
int tr_rpc_record_dispatch_bind_semantic_stats_on_owner(
        struct tr_rpc_record_dispatch *d,
        struct tr_rpc_record_semantic_counters *counters)
{
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (!counters)
		return TR_ERR_INVALID;
	if (d->depth || d->stopping || d->semantic != &d->local_semantic ||
	    d->local_semantic.value.calls_started || d->local_semantic.overflow)
		return TR_ERR_STATE;
	for (uint32_t i = 0; i < d->config.capacity; ++i)
		if (d->slots[i].active)
			return TR_ERR_STATE;
	d->semantic = counters;
	return TR_OK;
}
int tr_rpc_record_dispatch_get_semantic_stats_on_owner(
        struct tr_rpc_record_dispatch *d, struct tr_rpc_semantic_stats *out)
{
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	return tr_rpc_record_semantic_stats_read(d->semantic, out);
}
int tr_rpc_record_dispatch_admit_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle stream,
                                          struct tr_rpc_dispatch_handle *out)
{
	return tr_rpc_record_dispatch_admit_until_on_owner(d, stream, 0, out);
}
int tr_rpc_record_dispatch_admit_until_on_owner(
        struct tr_rpc_record_dispatch *d, struct tr_stream_handle stream,
        uint64_t open_due_ns, struct tr_rpc_dispatch_handle *out)
{
	struct tr_rpc_route_handle route;
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (!out)
		return TR_ERR_INVALID;
	if (d->depth || d->stopping)
		return TR_ERR_STATE;
	ret = tr_rpc_record_router_admit_on_owner(d->router, stream, &route);
	if (ret != TR_OK)
		return ret;
	d->admitted = true;
	struct tr_dispatch_slot *s = &d->slots[route.slot];
	struct tr_dispatch_context_link *link = &d->context_links[route.slot];
	tr_dispatch_context_lock(link);
	link->state = NULL;
	link->route_generation = route.generation;
	link->parent_status = TR_RPC_V2_CONTEXT_ACTIVE;
	tr_dispatch_context_unlock(link);
	tr_dispatch_peer_identity_release(s);
	*s = (struct tr_dispatch_slot){.dispatch = d,
	                               .route = route,
	                               .stream = stream,
	                               .open_due = open_due_ns,
	                               .active = true};
	*out = tr_dispatch_handle(s);
	return TR_OK;
}
static struct tr_dispatch_slot *
tr_dispatch_find(struct tr_rpc_record_dispatch *d,
                 struct tr_stream_handle stream)
{
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		struct tr_dispatch_slot *s = &d->slots[i];
		if (s->active && s->stream.channel == stream.channel &&
		    s->stream.slot == stream.slot &&
		    s->stream.generation == stream.generation)
			return s;
	}
	return NULL;
}
int tr_rpc_record_dispatch_receive_on_owner(struct tr_rpc_record_dispatch *d,
                                            struct tr_stream_handle stream,
                                            const uint8_t *data, size_t size)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (d->depth || d->stopping)
		return TR_ERR_STATE;
	s = tr_dispatch_find(d, stream);
	if (!s)
		return TR_ERR_STALE;
	++d->depth;
	/* 已建立的期限先于分段缓存准入检查；到期或取时失败已经选定
	 * 本地终局，返回 STATE 而非可重放已吸收网络字节的 AGAIN。 */
	ret = tr_dispatch_live(s) ? tr_rpc_record_router_receive_on_owner(
	                                    d->router, stream, data, size)
	                          : TR_ERR_STATE;
	tr_dispatch_settle(s);
	--d->depth;
	return ret;
}
int tr_rpc_record_dispatch_event_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle stream,
                                          enum tr_stream_event event,
                                          int status)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (d->depth)
		return TR_ERR_STATE;
	s = tr_dispatch_find(d, stream);
	if (!s)
		return TR_ERR_STALE;
	++d->depth;
	ret = tr_rpc_record_router_event_on_owner(d->router, stream, event,
	                                          status);
	tr_dispatch_settle(s);
	--d->depth;
	return ret;
}
int tr_rpc_record_dispatch_pump_on_owner(struct tr_rpc_record_dispatch *d,
                                         size_t limit)
{
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (!limit)
		return TR_ERR_INVALID;
	if (d->depth)
		return TR_ERR_STATE;
	++d->depth;
	if (limit > d->config.capacity)
		limit = d->config.capacity;
	while (limit--) {
		struct tr_dispatch_slot *s = &d->slots[d->cursor];
		if (++d->cursor == d->config.capacity)
			d->cursor = 0;
		if (s->active)
			tr_dispatch_step(s);
	}
	--d->depth;
	return TR_OK;
}
int tr_rpc_record_dispatch_abort_on_owner(struct tr_rpc_dispatch_handle handle,
                                          enum tr_rpc_status status)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_lookup(handle, &s);
	if (ret != TR_OK)
		return ret;
	if (handle.dispatch->depth)
		return TR_ERR_STATE;
	++handle.dispatch->depth;
	ret = tr_rpc_record_router_abort_on_owner(s->route, status);
	tr_dispatch_settle(s);
	--handle.dispatch->depth;
	return ret;
}
int tr_rpc_record_dispatch_get_stats_on_owner(
        struct tr_rpc_record_dispatch *d,
        struct tr_rpc_record_dispatch_stats *out)
{
	struct tr_rpc_record_dispatch_stats stats = {0};
	int ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (!out)
		return TR_ERR_INVALID;
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		struct tr_dispatch_slot *s = &d->slots[i];
		/* 退役只撤销 active；旧终局字段留到下次准入重置，不能算作
		 * 仍占用资源的调用。以下字段描述当前活跃调用，均按此过滤。 */
		if (!s->active)
			continue;
		++stats.active_calls;
		stats.held_events += s->pending;
		stats.submitted_calls += s->submitted != NULL;
		stats.reply_calls += s->ready != NULL;
		stats.terminal_calls += s->terminal;
	}
	*out = stats;
	return TR_OK;
}
int tr_rpc_record_dispatch_result_on_owner(struct tr_rpc_dispatch_handle handle,
                                           struct tr_rpc_dispatch_result *out)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_lookup(handle, &s);
	if (ret != TR_OK)
		return ret;
	if (!out)
		return TR_ERR_INVALID;
	out->status = s->status;
	out->terminal = s->terminal;
	out->final_accepted = s->final_accepted;
	out->admission_failed = s->admission_failed;
	return TR_OK;
}
int tr_rpc_record_dispatch_retire_on_owner(struct tr_rpc_dispatch_handle handle)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_lookup(handle, &s);
	if (ret != TR_OK)
		return ret;
	if (handle.dispatch->depth || !s->notified || s->pending || s->producer)
		return TR_ERR_STATE;
	ret = tr_rpc_record_router_retire_on_owner(s->route);
	if (ret == TR_OK) {
		tr_dispatch_context_detach(s);
		tr_dispatch_peer_identity_release(s);
		s->active = false;
	}
	return ret;
}
static int tr_dispatch_free(struct tr_rpc_record_dispatch **p)
{
	struct tr_rpc_record_dispatch *d = *p;
	struct tr_memory_budget *budget = d->budget;
	size_t bytes = d->bytes;
	if (atomic_load_explicit(&d->external_context_refs,
	                         memory_order_acquire))
		return TR_AGAIN;
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		if (d->slots[i].route.generation)
			tr_dispatch_context_detach(&d->slots[i]);
		tr_dispatch_peer_identity_release(&d->slots[i]);
	}
	free(d->context_links);
	free(d->contexts);
	free(d->slots);
	free(d->methods);
	free(d);
	*p = NULL;
	return tr_memory_budget_release(budget, bytes);
}
int tr_rpc_record_dispatch_destroy_on_owner(struct tr_rpc_record_dispatch **p)
{
	struct tr_rpc_record_dispatch *d;
	bool busy = false;
	int ret;
	if (!p)
		return TR_ERR_INVALID;
	d = *p;
	ret = tr_dispatch_owner(d);
	if (ret != TR_OK)
		return ret;
	if (d->depth)
		return TR_ERR_STATE;
	d->stopping = true;
	++d->depth;
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		struct tr_dispatch_slot *s = &d->slots[i];
		if (!s->active)
			continue;
		tr_dispatch_local_abort(s, TR_RPC_STATUS_CANCELLED);
		tr_dispatch_settle(s);
		busy |= s->pending != 0 || s->producer != NULL;
	}
	busy |= atomic_load_explicit(&d->external_context_refs,
	                             memory_order_acquire) != 0;
	if (busy) {
		--d->depth;
		return TR_AGAIN;
	}
	ret = tr_rpc_record_router_destroy_on_owner(&d->router);
	--d->depth;
	return ret == TR_OK ? tr_dispatch_free(p) : ret;
}
int tr_rpc_record_dispatch_reap_stopped(struct tr_rpc_record_dispatch **p)
{
	struct tr_reactor_recovery_guard guard = {0};
	struct tr_rpc_record_dispatch *d;
	bool busy = false;
	int ret;
	if (!p || !*p)
		return TR_ERR_INVALID;
	d = *p;
	ret = tr_reactor_recovery_acquire(d->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (d->depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	d->cleanup = d->stopping = true;
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		struct tr_dispatch_slot *s = &d->slots[i];
		if (s->active)
			tr_dispatch_context_publish_terminal(
			        s, TR_RPC_STATUS_CANCELLED);
		/* 已激活胶囊立即关闭，不等原任务退出。尚在 work 内创建的
		 * STAGED 对象仍只由 job 持有，交接前不能跨线程读取。 */
		if (!tr_dispatch_close_producer(s))
			busy = true;
		tr_dispatch_drop_queued(s);
		if (s->ready) {
			tr_dispatch_free_job(s->ready);
			s->ready = NULL;
		}
		if (s->submitted) {
			(void)tr_task_executor_cancel(d->executor, s->task);
			busy = true;
		}
	}
	ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK || busy)
		return ret != TR_OK ? ret : TR_AGAIN;
	if (atomic_load_explicit(&d->external_context_refs,
	                         memory_order_acquire))
		return TR_AGAIN;
	++d->depth;
	ret = tr_rpc_record_router_reap_stopped(&d->router);
	for (uint32_t i = 0; i < d->config.capacity; ++i) {
		struct tr_dispatch_slot *s = &d->slots[i];
		if (s->active)
			tr_dispatch_settle(s);
		busy |= s->pending != 0 || s->producer != NULL;
	}
	--d->depth;
	if (ret != TR_OK || busy)
		return ret != TR_OK ? ret : TR_AGAIN;
	if (atomic_load_explicit(&d->external_context_refs,
	                         memory_order_acquire))
		return TR_AGAIN;
	return tr_dispatch_free(p);
}
