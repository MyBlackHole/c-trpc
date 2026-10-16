#define _POSIX_C_SOURCE 200809L
#include "rpc_record_endpoint.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../memory_budget.h"
#include "../refcount.h"
#include "tr/status.h"

#include <stdlib.h>
#include <time.h>

struct tr_endpoint_finals;
struct tr_endpoint_final {
	struct tr_buffer buffer;
	struct tr_endpoint_finals *pool;
	_Atomic bool busy;
	uint8_t data[TR_RPC_RECORD_HEADER_SIZE];
};
struct tr_endpoint_finals {
	struct tr_refcount refs;
	struct tr_memory_budget *budget;
	size_t bytes;
	struct tr_endpoint_final items[];
};
struct tr_endpoint_slot {
	struct tr_stream_handle stream;
	struct tr_rpc_dispatch_handle call;
	struct tr_rpc_record_endpoint_result result;
	uint64_t final_deadline;
	bool used, routed, terminal, dead, notified, skip_final;
};
struct tr_rpc_record_endpoint {
	struct tr_channel *channel;
	struct tr_reactor *reactor;
	struct tr_task_executor *executor;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_memory_budget *budget;
	struct tr_endpoint_finals *finals;
	struct tr_reactor_timer_handle timer;
	struct tr_rpc_record_endpoint_config config;
	tr_rpc_endpoint_terminal_fn terminal;
	void *arg;
	size_t bytes;
	uint32_t capacity, cursor;
	unsigned depth;
	bool start_attempted, attached, timer_attached, stopping, draining,
	        cleanup;
	struct tr_endpoint_slot slots[];
};

static int tr_endpoint_owner(struct tr_rpc_record_endpoint *ep)
{
	if (!ep)
		return TR_ERR_INVALID;
	return !ep->cleanup && tr_reactor_in_owner_context_for(ep->reactor)
	               ? TR_OK
	               : TR_ERR_STATE;
}
static uint64_t tr_endpoint_after(uint64_t now, uint64_t interval)
{
	return now > UINT64_MAX - interval ? UINT64_MAX : now + interval;
}
static int tr_endpoint_now(uint64_t *out)
{
	struct timespec t;
	if (clock_gettime(CLOCK_MONOTONIC, &t) != 0 || t.tv_sec < 0 ||
	    (uint64_t)t.tv_sec > (UINT64_MAX - 999999999) / 1000000000)
		return TR_ERR_SYS;
	*out = (uint64_t)t.tv_sec * 1000000000 + (uint64_t)t.tv_nsec;
	return TR_OK;
}
static void tr_endpoint_finals_put(struct tr_endpoint_finals *p)
{
	if (tr_refcount_put(&p->refs) == 1) {
		struct tr_memory_budget *budget = p->budget;
		size_t bytes = p->bytes;
		free(p);
		(void)tr_memory_budget_release(budget, bytes);
	}
}
static void tr_endpoint_final_release(struct tr_buffer *buffer)
{
	struct tr_endpoint_final *f = (struct tr_endpoint_final *)buffer;
	struct tr_endpoint_finals *p = f->pool;
	/* 数据已不再借用才发布可复用；随后不再访问f，p另由此TX引用保护。 */
	atomic_store_explicit(&f->busy, false, memory_order_release);
	tr_endpoint_finals_put(p);
}
static struct tr_endpoint_slot *
tr_endpoint_find(struct tr_rpc_record_endpoint *ep, struct tr_stream_handle s)
{
	if (s.channel != ep->channel || s.slot >= ep->capacity)
		return NULL;
	struct tr_endpoint_slot *slot = &ep->slots[s.slot];
	return slot->used && slot->stream.generation == s.generation ? slot
	                                                             : NULL;
}
static void tr_endpoint_selected(struct tr_rpc_dispatch_handle h,
                                 struct tr_stream_handle stream,
                                 enum tr_rpc_status status,
                                 enum tr_rpc_record_delivery delivery,
                                 bool accepted, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *s = tr_endpoint_find(ep, stream);
	(void)h;
	if (!s || s->terminal)
		return;
	s->terminal = true;
	s->result.status = status;
	s->result.delivery = delivery;
	s->result.final_accepted = accepted;
}
static void tr_endpoint_local(struct tr_endpoint_slot *s,
                              enum tr_rpc_status status)
{
	s->terminal = true;
	s->result.status = status;
	s->result.delivery = TR_RPC_RECORD_ON_OWNER;
}
static int tr_endpoint_send_final(struct tr_rpc_record_endpoint *ep,
                                  struct tr_endpoint_slot *s)
{
	struct tr_endpoint_final *f = &ep->finals->items[s->stream.slot];
	const struct tr_rpc_record_limits limits = {0, 0, 0,
	                                            TR_RPC_RECORD_ACCEPT_NONE};
	struct tr_rpc_record_header h = {.kind = TR_RPC_RECORD_FINAL_STATUS,
	                                 .status = s->result.status};
	int ret;
	if (atomic_load_explicit(&f->busy, memory_order_acquire))
		return TR_AGAIN;
	/* 仅非OK本地终局可以绕过已关闭的RPC状态机发拒绝；绝不制造成功。 */
	if (h.status == TR_RPC_STATUS_OK)
		return TR_ERR_STATE;
	ret = tr_rpc_record_encode_header(f->data, sizeof(f->data), &h,
	                                  &limits);
	if (ret != TR_OK)
		return ret;
	ret = tr_refcount_get(&ep->finals->refs);
	if (ret != TR_OK)
		return ret;
	atomic_store_explicit(&f->busy, true, memory_order_release);
	ret = tr_stream_send(s->stream, &f->buffer);
	if (ret != TR_OK)
		tr_endpoint_final_release(&f->buffer);
	return ret;
}
static int tr_endpoint_finish(struct tr_rpc_record_endpoint *ep,
                              struct tr_endpoint_slot *s, uint64_t now)
{
	int ret;
	if (!s->terminal || s->notified)
		return TR_OK;
	if (!s->final_deadline)
		s->final_deadline =
		        tr_endpoint_after(now, ep->config.final_timeout_ns);
	if (!s->dead && !s->result.final_accepted && !s->skip_final) {
		ret = tr_endpoint_send_final(ep, s);
		if (ret == TR_OK) {
			s->result.final_accepted = true;
		} else if (ret == TR_AGAIN && now < s->final_deadline) {
			return TR_AGAIN;
		} else {
			s->skip_final = true;
			s->result.transport_error = ret;
			if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE)
				s->dead = true;
		}
	}
	if (!s->dead && !s->result.close_accepted) {
		ret = tr_stream_close(s->stream);
		if (ret == TR_AGAIN)
			return ret;
		if (ret == TR_OK || ret == TR_ERR_CLOSED)
			s->result.close_accepted = true;
		else if (ret == TR_ERR_STALE) {
			s->dead = true;
			s->result.transport_error = ret;
		} else {
			s->result.transport_error = ret;
			return ret;
		}
	}
	if (s->routed) {
		ret = tr_rpc_record_dispatch_retire_on_owner(s->call);
		if (ret != TR_OK)
			return ret;
		s->routed = false;
	}
	s->notified = true;
	ep->terminal(s->stream, s->result, ep->arg);
	/* 保留精确流墓碑，晚到DATA只释放，不重新建立调用。槽位跟随
	 * Channel代次复用；TX头有独立busy及引用，不随墓碑重置。 */
	return TR_OK;
}
static int tr_endpoint_progress(struct tr_rpc_record_endpoint *ep, uint64_t now)
{
	size_t count;
	int ret;
	if (ep->depth || ep->cleanup)
		return TR_ERR_STATE;
	++ep->depth;
	if (ep->stopping) {
		for (uint32_t i = 0; i < ep->capacity; ++i) {
			struct tr_endpoint_slot *s = &ep->slots[i];
			if (s->routed && !s->terminal)
				(void)tr_rpc_record_dispatch_abort_on_owner(
				        s->call, TR_RPC_STATUS_CANCELLED);
		}
	}
	ret = tr_task_executor_dispatch(ep->executor, ep->config.work_limit,
	                                &count);
	if (ret == TR_OK)
		ret = tr_rpc_record_dispatch_pump_on_owner(
		        ep->dispatch, ep->config.work_limit);
	if (ret == TR_OK) {
		size_t visits = ep->config.work_limit < ep->capacity
		                        ? ep->config.work_limit
		                        : ep->capacity;
		while (visits--) {
			struct tr_endpoint_slot *s = &ep->slots[ep->cursor];
			if (++ep->cursor == ep->capacity)
				ep->cursor = 0;
			if (s->used) {
				int finish_ret = tr_endpoint_finish(ep, s, now);
				if (finish_ret != TR_OK &&
				    finish_ret != TR_AGAIN && ret == TR_OK)
					ret = finish_ret;
			}
		}
	}
	--ep->depth;
	return ret;
}
static uint64_t tr_endpoint_tick(void *arg, uint64_t now)
{
	struct tr_rpc_record_endpoint *ep = arg;
	(void)tr_endpoint_progress(ep, now);
	return tr_endpoint_after(now, ep->config.progress_interval_ns);
}
static enum tr_stream_data_disposition
tr_endpoint_data(struct tr_stream_handle stream, uint64_t id,
                 struct tr_buffer *data, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *s = tr_endpoint_find(ep, stream);
	(void)id;
	++ep->depth;
	if (s && s->routed && !s->terminal && !ep->stopping)
		(void)tr_rpc_record_dispatch_receive_on_owner(
		        ep->dispatch, stream, data->data, data->len);
	--ep->depth;
	return TR_STREAM_DATA_RELEASE;
}
static void tr_endpoint_event(struct tr_stream_handle stream,
                              enum tr_stream_event event, int status, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *s;
	int ret;
	(void)status;
	if (stream.channel != ep->channel || stream.slot >= ep->capacity)
		return;
	++ep->depth;
	s = &ep->slots[stream.slot];
	if (event == TR_STREAM_EVENT_OPENED) {
		/* 同代次重复OPENED不接管第二次；未静默路由也不可被槽位覆盖。 */
		if (s->used &&
		    (s->stream.generation == stream.generation || s->routed))
			goto out;
		*s = (struct tr_endpoint_slot){.stream = stream, .used = true};
		if (ep->stopping) {
			tr_endpoint_local(s, TR_RPC_STATUS_UNAVAILABLE);
			goto out;
		}
		ret = tr_rpc_record_dispatch_admit_on_owner(ep->dispatch,
		                                            stream, &s->call);
		if (ret == TR_OK)
			s->routed = true;
		else
			tr_endpoint_local(
			        s, ret == TR_AGAIN
			                   ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
			                   : TR_RPC_STATUS_UNAVAILABLE);
	} else {
		s = tr_endpoint_find(ep, stream);
		if (!s)
			goto out;
		if (event == TR_STREAM_EVENT_ERROR ||
		    event == TR_STREAM_EVENT_CLOSED)
			s->dead = true;
		if (s->routed && !s->terminal)
			(void)tr_rpc_record_dispatch_event_on_owner(
			        ep->dispatch, stream, event);
	}
out:
	--ep->depth;
	/* WRITABLE也推进，不要求测试/使用者手工转发事件或pump。
	 * 重入时只依靠已注册定时器，禁止嵌套派发相同调用。 */
	if (event == TR_STREAM_EVENT_WRITABLE && !ep->depth) {
		uint64_t now;
		if (tr_endpoint_now(&now) == TR_OK)
			(void)tr_endpoint_progress(ep, now);
	}
}

/* 端点属于当前Channel连接轮次。物理连接失效后关闭新流准入，防止
 * Channel复用槽位覆盖尚在等待工作线程退出的旧路由；重连由上层新建端点。
 */
static void tr_endpoint_channel_event(struct tr_channel *channel,
                                      enum tr_channel_event event, int status,
                                      void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	(void)status;
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN) {
		ep->stopping = true;
		if (tr_channel_begin_drain(channel) == TR_OK)
			ep->draining = true;
	}
}

int tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_endpoint_config *config,
        tr_rpc_endpoint_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_endpoint **out)
{
	struct tr_rpc_record_endpoint *ep;
	struct tr_endpoint_finals *p;
	size_t bytes, pool_bytes;
	uint32_t capacity;
	int ret;
	if (!channel || !executor || !config || !terminal || !out || !budget ||
	    !budget->limit_bytes || !config->work_limit ||
	    !config->final_timeout_ns ||
	    config->progress_interval_ns < 1000000 ||
	    config->progress_interval_ns > 1000000000)
		return TR_ERR_INVALID;
	if (*out ||
	    !tr_reactor_in_owner_context_for(tr_channel_reactor(channel)) ||
	    !tr_task_executor_is_current_owner(executor) ||
	    tr_channel_active_streams(channel))
		return TR_ERR_STATE;
	capacity = tr_channel_max_streams(channel);
	if (!capacity || capacity >= UINT32_MAX - 1 ||
	    sizeof(ep->slots[0]) > (SIZE_MAX - sizeof(*ep)) / capacity ||
	    sizeof(p->items[0]) > (SIZE_MAX - sizeof(*p)) / capacity)
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*ep) + (size_t)capacity * sizeof(ep->slots[0]);
	pool_bytes = sizeof(*p) + (size_t)capacity * sizeof(p->items[0]);
	if (bytes > SIZE_MAX - pool_bytes)
		return TR_ERR_BAD_LENGTH;
	ret = tr_memory_budget_reserve(budget, bytes + pool_bytes);
	if (ret != TR_OK)
		return ret;
	ep = calloc(1, bytes);
	p = calloc(1, pool_bytes);
	if (!ep || !p) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}
	ret = tr_rpc_record_dispatch_create_on_owner(
	        channel, executor, methods, method_count, &config->dispatch,
	        tr_endpoint_selected, ep, budget, &ep->dispatch);
	if (ret != TR_OK)
		goto fail;
	ep->channel = channel;
	ep->reactor = tr_channel_reactor(channel);
	ep->executor = executor;
	ep->budget = budget;
	ep->config = *config;
	ep->terminal = terminal;
	ep->arg = arg;
	ep->capacity = capacity;
	ep->bytes = bytes;
	ep->finals = p;
	p->budget = budget;
	p->bytes = pool_bytes;
	(void)tr_refcount_init(&p->refs, 1);
	for (uint32_t i = 0; i < capacity; ++i) {
		struct tr_endpoint_final *f = &p->items[i];
		f->pool = p;
		atomic_init(&f->busy, false);
		f->buffer = (struct tr_buffer){
		        .data = f->data,
		        .len = sizeof(f->data),
		        .capacity = sizeof(f->data),
		        .release_cb = tr_endpoint_final_release};
	}
	*out = ep;
	return TR_OK;
fail:
	free(p);
	free(ep);
	(void)tr_memory_budget_release(budget, bytes + pool_bytes);
	return ret;
}
int tr_rpc_record_endpoint_start_on_owner(struct tr_rpc_record_endpoint *ep)
{
	uint64_t now;
	int ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->depth || ep->start_attempted || ep->stopping ||
	    tr_channel_active_streams(ep->channel))
		return TR_ERR_STATE;
	ep->start_attempted = true;
	ret = tr_endpoint_now(&now);
	if (ret == TR_OK)
		ret = tr_reactor_timer_register(ep->reactor, tr_endpoint_tick,
		                                ep, &ep->timer);
	if (ret != TR_OK)
		return ret;
	ep->timer_attached = true;
	ret = tr_reactor_timer_arm(
	        ep->timer,
	        tr_endpoint_after(now, ep->config.progress_interval_ns));
	if (ret == TR_OK)
		ret = tr_channel_set_handler(ep->channel, tr_endpoint_data,
		                             tr_endpoint_event,
		                             tr_endpoint_channel_event, ep);
	if (ret == TR_OK)
		ep->attached = true;
	return ret;
}
int tr_rpc_record_endpoint_abort_on_owner(struct tr_rpc_record_endpoint *ep,
                                          struct tr_stream_handle stream,
                                          enum tr_rpc_status status)
{
	struct tr_endpoint_slot *s;
	int ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->depth)
		return TR_ERR_STATE;
	s = tr_endpoint_find(ep, stream);
	if (!s || !s->routed)
		return TR_ERR_STALE;
	if (s->terminal)
		return TR_ERR_STATE;
	++ep->depth;
	ret = tr_rpc_record_dispatch_abort_on_owner(s->call, status);
	--ep->depth;
	return ret;
}
static int tr_endpoint_detach(struct tr_rpc_record_endpoint *ep)
{
	int ret;
	if (ep->attached) {
		ret = tr_channel_set_handler(ep->channel, NULL, NULL, NULL,
		                             NULL);
		if (ret != TR_OK)
			return ret;
		ep->attached = false;
	}
	if (ep->timer_attached) {
		ret = tr_reactor_timer_unregister(ep->timer);
		if (ret != TR_OK)
			return ret;
		ep->timer_attached = false;
	}
	return TR_OK;
}
static int tr_endpoint_free(struct tr_rpc_record_endpoint **p)
{
	struct tr_rpc_record_endpoint *ep = *p;
	struct tr_memory_budget *budget = ep->budget;
	size_t bytes = ep->bytes;
	tr_endpoint_finals_put(ep->finals);
	free(ep);
	*p = NULL;
	return tr_memory_budget_release(budget, bytes);
}
int tr_rpc_record_endpoint_close_on_owner(struct tr_rpc_record_endpoint **p)
{
	struct tr_rpc_record_endpoint *ep;
	uint64_t now;
	bool busy = false;
	int ret;
	if (!p)
		return TR_ERR_INVALID;
	ep = *p;
	ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->depth)
		return TR_ERR_STATE;
	ep->stopping = true;
	if (ep->attached && !ep->draining) {
		ret = tr_channel_begin_drain(ep->channel);
		if (ret != TR_OK)
			return ret;
		ep->draining = true;
	}
	++ep->depth;
	for (uint32_t i = 0; i < ep->capacity; ++i) {
		struct tr_endpoint_slot *s = &ep->slots[i];
		if (s->used && s->routed && !s->terminal)
			(void)tr_rpc_record_dispatch_abort_on_owner(
			        s->call, TR_RPC_STATUS_CANCELLED);
	}
	--ep->depth;
	ret = tr_endpoint_now(&now);
	if (ret != TR_OK)
		return ret;
	ret = tr_endpoint_progress(ep, now);
	if (ret != TR_OK)
		return ret;
	for (uint32_t i = 0; i < ep->capacity; ++i)
		busy |= ep->slots[i].used && !ep->slots[i].notified;
	if (busy)
		return TR_AGAIN;
	ret = tr_endpoint_detach(ep);
	if (ret != TR_OK)
		return ret;
	ret = tr_rpc_record_dispatch_destroy_on_owner(&ep->dispatch);
	return ret == TR_OK ? tr_endpoint_free(p) : ret;
}
int tr_rpc_record_endpoint_reap_stopped(struct tr_rpc_record_endpoint **p)
{
	struct tr_rpc_record_endpoint *ep;
	struct tr_reactor_recovery_guard guard = {0};
	int ret, release_ret;
	if (!p || !*p)
		return TR_ERR_INVALID;
	ep = *p;
	ret = tr_reactor_recovery_acquire(ep->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (ep->depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	ep->cleanup = ep->stopping = true;
	ret = tr_endpoint_detach(ep);
	release_ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK || release_ret != TR_OK)
		return ret != TR_OK ? ret : release_ret;
	/* 下层自行取得租约；外部全程排除父循环重启/销毁，不嵌套不同guard。 */
	ret = tr_rpc_record_dispatch_reap_stopped(&ep->dispatch);
	if (ret != TR_OK)
		return ret;
	++ep->depth;
	for (uint32_t i = 0; i < ep->capacity; ++i) {
		struct tr_endpoint_slot *s = &ep->slots[i];
		if (s->used && !s->notified) {
			s->notified = true;
			s->result = (struct tr_rpc_record_endpoint_result){
			        .status = TR_RPC_STATUS_CANCELLED,
			        .delivery = TR_RPC_RECORD_OWNER_STOPPED};
			ep->terminal(s->stream, s->result, ep->arg);
		}
	}
	--ep->depth;
	return tr_endpoint_free(p);
}
