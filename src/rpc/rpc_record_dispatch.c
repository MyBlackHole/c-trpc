#include "rpc_record_dispatch.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdlib.h>
#include <string.h>

struct tr_reply_body {
	struct tr_buffer buffer;
	struct tr_memory_budget *budget;
	struct tr_reply_body *next;
	size_t bytes;
	uint8_t data[];
};
struct tr_rpc_record_reply {
	struct tr_memory_budget *budget;
	struct tr_reply_body *head, *tail;
	size_t used, limit;
	uint32_t count, max_count, max_message;
	enum tr_rpc_status status;
	bool finished;
};
struct tr_dispatch_slot;
struct tr_dispatch_job {
	struct tr_dispatch_job *next;
	struct tr_dispatch_slot *slot;
	struct tr_rpc_dispatch_id id;
	struct tr_rpc_record_view view;
	tr_rpc_record_work_fn work;
	void *arg;
	struct tr_rpc_record_reply reply;
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
	struct tr_dispatch_job *head, *tail, *submitted, *ready;
	struct tr_task_handle task;
	uint32_t response_codec;
	size_t pending;
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	bool active, terminal, notified, headers_sent, final_accepted;
};
struct tr_rpc_record_dispatch {
	struct tr_reactor *reactor;
	struct tr_task_executor *executor;
	struct tr_rpc_record_router *router;
	struct tr_memory_budget *budget;
	struct tr_dispatch_slot *slots;
	struct tr_dispatch_method *methods;
	struct tr_rpc_record_dispatch_config config;
	tr_rpc_dispatch_terminal_fn terminal;
	void *arg;
	size_t bytes;
	uint32_t cursor;
	unsigned depth;
	bool stopping, cleanup;
};

static int tr_dispatch_owner(struct tr_rpc_record_dispatch *d)
{
	if (!d)
		return TR_ERR_INVALID;
	if (!tr_reactor_in_owner_context_for(d->reactor) || d->cleanup)
		return TR_ERR_STATE;
	return TR_OK;
}
static struct tr_rpc_dispatch_handle
tr_dispatch_handle(struct tr_dispatch_slot *s)
{
	return (struct tr_rpc_dispatch_handle){
	        s->dispatch, {s->route.generation, s->route.slot}};
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
static void tr_reply_clear(struct tr_rpc_record_reply *reply)
{
	while (reply->head) {
		struct tr_reply_body *b = reply->head;
		reply->head = b->next;
		tr_buffer_release(&b->buffer);
	}
	reply->tail = NULL;
}
int tr_rpc_record_reply_append(struct tr_rpc_record_reply *r, const void *data,
                               size_t size)
{
	struct tr_reply_body *b;
	size_t bytes;
	int ret;
	if (!r)
		return TR_ERR_INVALID;
	if (r->finished || r->status != TR_RPC_STATUS_OK)
		return TR_ERR_STATE;
	if ((!data && size) || size > r->max_message ||
	    r->count == r->max_count || size > r->limit - r->used ||
	    size > SIZE_MAX - sizeof(*b)) {
		r->status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		return TR_ERR_BAD_LENGTH;
	}
	bytes = sizeof(*b) + size;
	ret = tr_memory_budget_reserve(r->budget, bytes);
	if (ret != TR_OK) {
		r->status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		return TR_ERR_NOMEM;
	}
	b = malloc(bytes);
	if (!b) {
		(void)tr_memory_budget_release(r->budget, bytes);
		r->status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		return TR_ERR_NOMEM;
	}
	*b = (struct tr_reply_body){
	        .buffer = {.data = b->data,
	                   .capacity = (uint32_t)size,
	                   .len = (uint32_t)size,
	                   .release_cb = tr_reply_body_release},
	        .budget = r->budget,
	        .bytes = bytes};
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
int tr_rpc_record_reply_finish(struct tr_rpc_record_reply *r,
                               enum tr_rpc_status status)
{
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
		s->status = TR_RPC_STATUS_CANCELLED;
		s->delivery = TR_RPC_RECORD_OWNER_STOPPED;
		s->final_accepted = false;
	}
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
		++d->depth;
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
static int tr_dispatch_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct tr_dispatch_job *job = arg;
	struct tr_rpc_record_reply *reply = &job->reply;
	enum tr_rpc_status status = TR_RPC_STATUS_CANCELLED;
	if (!tr_task_cancel_requested(cancel))
		status =
		        job->work(job->id, &job->view, cancel, reply, job->arg);
	if (status < TR_RPC_STATUS_OK || status >= (int)TR_RPC_STATUS_COUNT)
		status = TR_RPC_STATUS_INTERNAL;
	if (status != TR_RPC_STATUS_OK)
		reply->status = status;
	if (reply->status != TR_RPC_STATUS_OK) {
		reply->finished = true;
		tr_reply_clear(reply);
	}
	if (job->view.header.kind == TR_RPC_RECORD_HALF_CLOSE)
		reply->finished = true;
	return TR_OK;
}
static void tr_dispatch_done(void *arg, struct tr_task_handle handle,
                             struct tr_task_result result)
{
	struct tr_dispatch_job *job = arg;
	struct tr_dispatch_slot *s = job->slot;
	struct tr_rpc_record_dispatch *d = s->dispatch;
	(void)handle;
	/* executor完成与本调用的submitted存储一一对应，路由在此之前不能退役。
	 * 清理域须由reap先授权；外层不能先换owner交付，再追认授权。 */
	s->submitted = NULL;
	if (d->cleanup || s->terminal || d->stopping) {
		tr_dispatch_free_job(job);
	} else if (result.cancelled || result.work_status != TR_OK) {
		tr_dispatch_free_job(job);
		tr_dispatch_local_abort(s, result.cancelled
		                                   ? TR_RPC_STATUS_CANCELLED
		                                   : TR_RPC_STATUS_INTERNAL);
	} else {
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
	size_t bytes, payload;
	if (s->terminal || d->stopping ||
	    (s->ready && s->ready->reply.finished))
		return;
	if (s->pending >= d->config.max_pending_per_call) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	payload = view->header.metadata_size;
	if (view->header.encoded_size > SIZE_MAX - payload ||
	    payload + view->header.encoded_size > SIZE_MAX - sizeof(*job)) {
		tr_dispatch_local_abort(s, TR_RPC_STATUS_RESOURCE_EXHAUSTED);
		return;
	}
	payload += view->header.encoded_size;
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
	job->arg = m->method.arg;
	job->bytes = bytes;
	job->reply = (struct tr_rpc_record_reply){
	        .budget = d->budget,
	        .limit = d->config.max_reply_bytes,
	        .max_count = d->config.max_replies_per_event,
	        .max_message = m->method.descriptor.max_response_bytes};
	if (view->header.metadata_size) {
		memcpy(job->data, view->metadata, view->header.metadata_size);
		job->view.metadata = job->data;
	}
	if (view->header.encoded_size) {
		uint8_t *body = job->data + view->header.metadata_size;
		memcpy(body, view->body, view->header.encoded_size);
		job->view.body = body;
	}
	s->response_codec = m->method.descriptor.response_codec_id;
	if (s->tail)
		s->tail->next = job;
	else
		s->head = job;
	s->tail = job;
	++s->pending;
}
/* 一步只投递任务或发一条记录。结果背压期间不启动下一业务事件，因而
 * 每调用的输入与输出次序都可证明；不同调用仍可独立前进。 */
static void tr_dispatch_step(struct tr_dispatch_slot *s)
{
	struct tr_rpc_record_dispatch *d = s->dispatch;
	struct tr_dispatch_job *job = s->ready;
	int ret;
	if (s->terminal) {
		tr_dispatch_settle(s);
		return;
	}
	if (!job) {
		struct tr_task_spec spec;
		if (s->submitted || !s->head || d->stopping)
			return;
		job = s->head;
		spec = (struct tr_task_spec){tr_dispatch_work, tr_dispatch_done,
		                             job};
		ret = tr_task_executor_submit(d->executor, &spec, &s->task);
		if (ret == TR_OK) {
			s->head = job->next;
			if (!s->head)
				s->tail = NULL;
			job->next = NULL;
			s->submitted = job;
		} else if (ret != TR_AGAIN) {
			tr_dispatch_local_abort(s, TR_RPC_STATUS_UNAVAILABLE);
			tr_dispatch_settle(s);
		}
		return;
	}
	struct tr_rpc_record_reply *reply = &job->reply;
	struct tr_rpc_record_header h = {0};
	struct tr_reply_body *b = reply->head;
	struct tr_reply_body *next = b ? b->next : NULL;
	struct tr_buffer *body = b ? &b->buffer : NULL;
	if (b && !s->headers_sent) {
		h.kind = TR_RPC_RECORD_INITIAL_METADATA;
		body = NULL;
	} else if (b) {
		h.kind = TR_RPC_RECORD_MESSAGE;
		h.codec_id = s->response_codec;
		h.encoded_size = h.decoded_size = body->len;
	} else if (reply->finished) {
		h.kind = TR_RPC_RECORD_FINAL_STATUS;
		h.status = reply->status;
	} else {
		s->ready = NULL;
		tr_dispatch_free_job(job);
		return;
	}
	ret = tr_rpc_record_router_send_on_owner(s->route, &h, NULL, &body);
	if (ret == TR_OK) {
		if (h.kind == TR_RPC_RECORD_INITIAL_METADATA)
			s->headers_sent = true;
		else if (h.kind == TR_RPC_RECORD_MESSAGE) {
			/* b可以已经由传输释放；next在接受前取得，之后不再读b。
			 */
			reply->head = next;
			if (!next)
				reply->tail = NULL;
		} else {
			s->final_accepted = true;
		}
	} else if (ret != TR_AGAIN) {
		/* 非背压失败不能伪装成成功；停止重试坏回复。若是业务基数
		 * 错误且绑定仍活跃，下一轮用合法错误FINAL解释失败。 */
		if (ret == TR_ERR_STATE && !s->terminal &&
		    !(reply->finished &&
		      reply->status == TR_RPC_STATUS_INTERNAL)) {
			tr_reply_clear(reply);
			reply->finished = true;
			reply->status = TR_RPC_STATUS_INTERNAL;
			tr_dispatch_drop_queued(s);
		} else {
			tr_dispatch_local_abort(
			        s, ret == TR_ERR_NOMEM
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
	if (method_count >
	    (SIZE_MAX - bytes) / sizeof(struct tr_dispatch_method))
		return TR_ERR_BAD_LENGTH;
	method_bytes = method_count * sizeof(struct tr_dispatch_method);
	bytes += method_bytes;
	if (method_count > (SIZE_MAX - bytes) / sizeof(*routes))
		return TR_ERR_BAD_LENGTH;
	temporary = method_count * sizeof(*routes);
	for (size_t i = 0; i < method_count; ++i)
		if (!methods[i].work)
			return TR_ERR_INVALID;
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
	d->methods = malloc(method_bytes);
	if (!d->slots || !d->methods) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	d->reactor = tr_channel_reactor(channel);
	d->executor = executor;
	d->budget = budget;
	d->bytes = bytes;
	d->config = *config;
	d->terminal = terminal;
	d->arg = arg;
	for (size_t i = 0; i < method_count; ++i) {
		d->methods[i] = (struct tr_dispatch_method){d, methods[i]};
		routes[i] = (struct tr_rpc_record_method){methods[i].descriptor,
		                                          tr_dispatch_received,
		                                          &d->methods[i]};
	}
	for (uint32_t i = 0; i < config->capacity; ++i)
		d->slots[i].dispatch = d;
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
	free(d->slots);
	free(d);
	(void)tr_memory_budget_release(budget, bytes + temporary);
	return ret;
}
int tr_rpc_record_dispatch_admit_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle stream,
                                          struct tr_rpc_dispatch_handle *out)
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
	struct tr_dispatch_slot *s = &d->slots[route.slot];
	*s = (struct tr_dispatch_slot){.dispatch = d,
	                               .route = route,
	                               .stream = stream,
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
	ret = tr_rpc_record_router_receive_on_owner(d->router, stream, data,
	                                            size);
	tr_dispatch_settle(s);
	--d->depth;
	return ret;
}
int tr_rpc_record_dispatch_event_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle stream,
                                          enum tr_stream_event event)
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
	ret = tr_rpc_record_router_event_on_owner(d->router, stream, event);
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
		stats.active_calls += s->active;
		stats.held_events += s->pending;
		stats.submitted_calls += s->submitted != NULL;
		stats.reply_calls += s->ready != NULL;
		stats.terminal_calls += s->terminal;
	}
	*out = stats;
	return TR_OK;
}
int tr_rpc_record_dispatch_retire_on_owner(struct tr_rpc_dispatch_handle handle)
{
	struct tr_dispatch_slot *s;
	int ret = tr_dispatch_lookup(handle, &s);
	if (ret != TR_OK)
		return ret;
	if (handle.dispatch->depth || !s->notified || s->pending)
		return TR_ERR_STATE;
	ret = tr_rpc_record_router_retire_on_owner(s->route);
	if (ret == TR_OK)
		s->active = false;
	return ret;
}
static int tr_dispatch_free(struct tr_rpc_record_dispatch **p)
{
	struct tr_rpc_record_dispatch *d = *p;
	struct tr_memory_budget *budget = d->budget;
	size_t bytes = d->bytes;
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
		busy |= s->pending != 0;
	}
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
	++d->depth;
	ret = tr_rpc_record_router_reap_stopped(&d->router);
	for (uint32_t i = 0; i < d->config.capacity; ++i)
		if (d->slots[i].active)
			tr_dispatch_settle(&d->slots[i]);
	--d->depth;
	return ret == TR_OK ? tr_dispatch_free(p) : ret;
}
