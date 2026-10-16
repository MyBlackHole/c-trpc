#define _POSIX_C_SOURCE 200809L
#include "rpc_record_endpoint.h"
#include "../execution/reactor_internal.h"
#include "../transport/channel/channel_internal.h"
#include "../memory_budget.h"
#include "tr/status.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <time.h>

struct tr_endpoint_frames;
struct tr_endpoint_frame {
	struct tr_buffer buffer;
	struct tr_endpoint_frames *arena;
	_Atomic bool busy;
	uint8_t bytes[TR_RPC_RECORD_HEADER_SIZE];
};
struct tr_endpoint_frames {
	_Atomic size_t refs;
	struct tr_memory_budget *budget;
	size_t bytes;
	struct tr_endpoint_frame frames[];
};
struct tr_endpoint_slot {
	struct tr_stream_handle stream;
	struct tr_rpc_dispatch_handle call;
	struct tr_rpc_endpoint_result result;
	uint64_t final_due;
	enum tr_stream_event pending_event;
	bool active, terminal, reported, peer_closed, dead, close_accepted;
};
struct tr_rpc_record_endpoint {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_task_executor *executor;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_memory_budget *budget;
	struct tr_endpoint_frames *frames;
	struct tr_reactor_timer_handle timer;
	struct tr_rpc_record_endpoint_config config;
	tr_rpc_endpoint_terminal_fn terminal;
	void *arg;
	size_t bytes, capacity, cursor;
	unsigned depth;
	bool started, stopping, timer_live, handler_live, cleanup;
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
static int tr_endpoint_now(uint64_t *out)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0 || ts.tv_sec < 0 ||
	    (uint64_t)ts.tv_sec > (UINT64_MAX - 999999999) / 1000000000)
		return TR_ERR_SYS;
	*out = (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
	return TR_OK;
}
static uint64_t tr_endpoint_after(uint64_t now, uint64_t delta)
{
	return delta > UINT64_MAX - now ? UINT64_MAX : now + delta;
}
static void tr_endpoint_frames_put(struct tr_endpoint_frames *arena)
{
	if (atomic_fetch_sub_explicit(&arena->refs, 1, memory_order_acq_rel) ==
	    1) {
		struct tr_memory_budget *budget = arena->budget;
		size_t bytes = arena->bytes;
		free(arena);
		(void)tr_memory_budget_release(budget, bytes);
	}
}
static void tr_endpoint_frame_release(struct tr_buffer *buffer)
{
	struct tr_endpoint_frame *frame = (struct tr_endpoint_frame *)buffer;
	struct tr_endpoint_frames *arena = frame->arena;
	/* 发布可复用状态后不再访问 frame；另一 owner turn 可以立即重新租用。
	 * arena 的旧 TX 引用仍在，最后一次 put 才可能释放整个存储。 */
	atomic_store_explicit(&frame->busy, false, memory_order_release);
	tr_endpoint_frames_put(arena);
}
static struct tr_endpoint_slot *
tr_endpoint_find(struct tr_rpc_record_endpoint *ep,
                 struct tr_stream_handle stream)
{
	for (size_t i = 0; i < ep->capacity; ++i) {
		struct tr_endpoint_slot *slot = &ep->slots[i];
		if (slot->active && slot->stream.channel == stream.channel &&
		    slot->stream.slot == stream.slot &&
		    slot->stream.generation == stream.generation)
			return slot;
	}
	return NULL;
}
static void tr_endpoint_terminal(struct tr_rpc_record_endpoint *ep,
                                 struct tr_endpoint_slot *slot,
                                 struct tr_rpc_endpoint_result result)
{
	uint64_t now;
	if (slot->terminal)
		return;
	slot->terminal = true;
	slot->result = result;
	/* 时钟失效时不假装仍有等待期限，直接转为只重试关闭。 */
	slot->final_due =
	        tr_endpoint_now(&now) == TR_OK
	                ? tr_endpoint_after(now, ep->config.final_wait_ns)
	                : 0;
}
static void tr_endpoint_dispatch_terminal(struct tr_rpc_dispatch_handle call,
                                          struct tr_stream_handle stream,
                                          enum tr_rpc_status status,
                                          enum tr_rpc_record_delivery delivery,
                                          bool accepted, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *slot = tr_endpoint_find(ep, stream);
	if (!slot || slot->call.dispatch != call.dispatch ||
	    slot->call.id.generation != call.id.generation ||
	    slot->call.id.slot != call.id.slot)
		return;
	tr_endpoint_terminal(
	        ep, slot,
	        (struct tr_rpc_endpoint_result){status, delivery, accepted});
}

static int tr_endpoint_send_error(struct tr_rpc_record_endpoint *ep,
                                  struct tr_endpoint_slot *slot)
{
	struct tr_endpoint_frame *frame =
	        &ep->frames->frames[(size_t)(slot - ep->slots)];
	const struct tr_rpc_record_limits limits = {0, 0, 0,
	                                            TR_RPC_RECORD_ACCEPT_NONE};
	struct tr_rpc_record_header header = {
	        .kind = TR_RPC_RECORD_FINAL_STATUS,
	        .status = (uint32_t)slot->result.status};
	struct tr_buffer *buffer = &frame->buffer;
	int ret;
	if (atomic_load_explicit(&frame->busy, memory_order_acquire))
		return TR_AGAIN;
	ret = tr_rpc_record_encode_header(frame->bytes, sizeof(frame->bytes),
	                                  &header, &limits);
	if (ret != TR_OK)
		return ret;
	frame->buffer =
	        (struct tr_buffer){.data = frame->bytes,
	                           .capacity = sizeof(frame->bytes),
	                           .len = sizeof(frame->bytes),
	                           .release_cb = tr_endpoint_frame_release};
	atomic_fetch_add_explicit(&ep->frames->refs, 1, memory_order_relaxed);
	atomic_store_explicit(&frame->busy, true, memory_order_release);
	ret = tr_stream_sendv(slot->stream, &buffer, 1);
	if (ret != TR_OK)
		tr_endpoint_frame_release(buffer);
	/* 成功后不读取可能已释放/复用的帧，只提交宿主的发送阶段。 */
	if (ret == TR_OK)
		slot->result.final_accepted = true;
	return ret;
}
static void tr_endpoint_report(struct tr_rpc_record_endpoint *ep,
                               struct tr_endpoint_slot *slot)
{
	if (slot->reported)
		return;
	slot->reported = true;
	ep->terminal(slot->stream, slot->result, ep->arg);
}
static int tr_endpoint_progress(struct tr_rpc_record_endpoint *ep,
                                struct tr_endpoint_slot *slot, uint64_t now)
{
	int ret;
	if (!slot->active || !slot->terminal)
		return TR_OK;
	if (!slot->dead && !slot->close_accepted) {
		if (!slot->result.final_accepted &&
		    slot->result.status != TR_RPC_STATUS_OK &&
		    now < slot->final_due) {
			ret = tr_endpoint_send_error(ep, slot);
			if (ret == TR_AGAIN)
				return TR_AGAIN;
			if (ret != TR_OK && ret != TR_ERR_CLOSED &&
			    ret != TR_ERR_STALE)
				slot->final_due = 0;
		}
		/* Reactor 的 STREAM_CLOSE 是 FIFO 屏障，不得越过已接受 DATA。
		 * 这里仍须保留关闭准入阶段，失败不冒充已经关闭。 */
		ret = tr_stream_close(slot->stream);
		if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE) {
			slot->dead = true;
		} else if (ret == TR_OK) {
			slot->close_accepted = true;
		} else {
			return ret;
		}
	}
	tr_endpoint_report(ep, slot);
	if ((slot->dead || (slot->close_accepted && slot->peer_closed)) &&
	    !ep->stopping) {
		if (slot->call.dispatch) {
			ret = tr_rpc_record_dispatch_retire_on_owner(
			        slot->call);
			if (ret != TR_OK)
				return ret;
		}
		*slot = (struct tr_endpoint_slot){0};
	}
	return TR_OK;
}
int tr_rpc_record_endpoint_pump_on_owner(struct tr_rpc_record_endpoint *ep)
{
	uint64_t now;
	size_t delivered;
	int ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->depth)
		return TR_ERR_STATE;
	++ep->depth;
	/* 发送/关闭可能内联触发传输事件。嵌套栈只登记，下一次推进才进入
	 * 派发器，不能静默丢掉断连，也不能递归破坏其当前调用。 */
	for (size_t i = 0; i < ep->config.progress_limit && i < ep->capacity;
	     ++i) {
		struct tr_endpoint_slot *slot =
		        &ep->slots[(ep->cursor + i) % ep->capacity];
		if (slot->active && slot->pending_event) {
			enum tr_stream_event event = slot->pending_event;
			slot->pending_event = 0;
			if (ep->dispatch && slot->call.dispatch &&
			    !slot->terminal)
				(void)tr_rpc_record_dispatch_event_on_owner(
				        ep->dispatch, slot->stream, event);
		}
	}
	ret = TR_OK;
	if (ep->dispatch) {
		ret = tr_task_executor_dispatch(
		        ep->executor, ep->config.completion_limit, &delivered);
		if (ret == TR_OK)
			ret = tr_rpc_record_dispatch_pump_on_owner(
			        ep->dispatch, ep->config.progress_limit);
	}
	if (tr_endpoint_now(&now) != TR_OK)
		now = UINT64_MAX;
	for (size_t n = 0; n < ep->config.progress_limit && n < ep->capacity;
	     ++n) {
		struct tr_endpoint_slot *slot = &ep->slots[ep->cursor];
		ep->cursor = (ep->cursor + 1) % ep->capacity;
		/* 每条流的发送错误不升级成关闭整个
		 * Channel。未接受关闭保留责任。 */
		int progress_ret = tr_endpoint_progress(ep, slot, now);
		if (ret == TR_OK && progress_ret != TR_OK &&
		    progress_ret != TR_AGAIN)
			ret = progress_ret;
	}
	--ep->depth;
	return ret;
}
static uint64_t tr_endpoint_tick(void *arg, uint64_t now)
{
	struct tr_rpc_record_endpoint *ep = arg;
	(void)tr_rpc_record_endpoint_pump_on_owner(ep);
	return tr_endpoint_after(now, ep->config.progress_period_ns);
}
static enum tr_stream_data_disposition
tr_endpoint_data(struct tr_stream_handle stream, uint64_t message_id,
                 struct tr_buffer *payload, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *slot = tr_endpoint_find(ep, stream);
	(void)message_id;
	/* DATA 与控制事件一样可能在下层发送栈内联重入。接收字节
	 * 只借用到本回调结束，不能在重入时直接丢弃并报告成功。
	 * 当前不建立第二条 RX 队列：将该流标记为协议失败，在下一次
	 * owner pump 中终止它。不得误影响同 Channel 的其他流。 */
	if (ep->depth && slot && !slot->terminal) {
		slot->pending_event = TR_STREAM_EVENT_ERROR;
		return TR_STREAM_DATA_RELEASE;
	}
	if (!ep->depth && slot && slot->call.dispatch && !slot->terminal) {
		++ep->depth;
		(void)tr_rpc_record_dispatch_receive_on_owner(
		        ep->dispatch, stream, payload->data, payload->len);
		--ep->depth;
		(void)tr_rpc_record_endpoint_pump_on_owner(ep);
	}
	return TR_STREAM_DATA_RELEASE;
}
static void tr_endpoint_event(struct tr_stream_handle stream,
                              enum tr_stream_event event, int status, void *arg)
{
	struct tr_rpc_record_endpoint *ep = arg;
	struct tr_endpoint_slot *slot = tr_endpoint_find(ep, stream);
	int ret;
	(void)status;
	if (ep->depth) {
		if (slot) {
			if (event == TR_STREAM_EVENT_REMOTE_CLOSED)
				slot->peer_closed = true;
			if (event == TR_STREAM_EVENT_CLOSED ||
			    event == TR_STREAM_EVENT_ERROR)
				slot->dead = true;
			if (event >= TR_STREAM_EVENT_REMOTE_CLOSED &&
			    event <= TR_STREAM_EVENT_ERROR &&
			    event > slot->pending_event)
				slot->pending_event = event;
		}
		return;
	}
	++ep->depth;
	if (event == TR_STREAM_EVENT_OPENED && !slot) {
		for (size_t i = 0; i < ep->capacity; ++i) {
			struct tr_endpoint_slot *candidate = &ep->slots[i];
			/* 事件突发可能先复用传输slot、后轮到宿主的轮转扫描。
			 * 及时退役已静默的旧代次，不能让死流积压吞掉新流的记录容量。
			 */
			if (candidate->active && candidate->dead &&
			    candidate->terminal) {
				if (!ep->stopping) {
					(void)tr_endpoint_progress(
					        ep, candidate, 0);
				} else if (!candidate->call.dispatch ||
				           !ep->dispatch) {
					tr_endpoint_report(ep, candidate);
					*candidate =
					        (struct tr_endpoint_slot){0};
				}
			}
			if (!candidate->active) {
				slot = candidate;
				break;
			}
		}
		if (slot) {
			*slot = (struct tr_endpoint_slot){.stream = stream,
			                                  .active = true};
			ret = ep->stopping
			              ? TR_ERR_CLOSED
			              : tr_rpc_record_dispatch_admit_on_owner(
			                        ep->dispatch, stream,
			                        &slot->call);
			if (ret != TR_OK)
				tr_endpoint_terminal(
				        ep, slot,
				        (struct tr_rpc_endpoint_result){
				                ep->stopping
				                        ? TR_RPC_STATUS_UNAVAILABLE
				                        : TR_RPC_STATUS_RESOURCE_EXHAUSTED,
				                TR_RPC_RECORD_ON_OWNER, false});
		}
	} else if (slot) {
		if (event == TR_STREAM_EVENT_REMOTE_CLOSED)
			slot->peer_closed = true;
		if (event == TR_STREAM_EVENT_CLOSED ||
		    event == TR_STREAM_EVENT_ERROR)
			slot->dead = true;
		if (slot->call.dispatch && !slot->terminal)
			(void)tr_rpc_record_dispatch_event_on_owner(
			        ep->dispatch, stream, event);
	}
	--ep->depth;
	(void)tr_rpc_record_endpoint_pump_on_owner(ep);
}

static int tr_endpoint_free(struct tr_rpc_record_endpoint **pointer)
{
	struct tr_rpc_record_endpoint *ep = *pointer;
	struct tr_memory_budget *budget = ep->budget;
	size_t bytes = ep->bytes;
	tr_endpoint_frames_put(ep->frames);
	free(ep);
	*pointer = NULL;
	return tr_memory_budget_release(budget, bytes);
}
int tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t method_count,
        const struct tr_rpc_record_endpoint_config *config,
        tr_rpc_endpoint_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_endpoint **out)
{
	struct tr_rpc_record_endpoint *ep;
	struct tr_endpoint_frames *arena;
	struct tr_reactor *reactor;
	size_t slots, bytes, frame_bytes;
	int ret;
	if (!channel || !executor || !methods || !method_count || !config ||
	    !terminal || !budget || !budget->limit_bytes || !out ||
	    !config->completion_limit || !config->progress_limit ||
	    config->progress_period_ns < 1000000 ||
	    config->progress_period_ns > 1000000000 || !config->final_wait_ns)
		return TR_ERR_INVALID;
	reactor = tr_channel_reactor(channel);
	if (*out || !tr_reactor_in_owner_context_for(reactor))
		return TR_ERR_STATE;
	slots = tr_channel_max_streams(channel);
	if (!config->dispatch.capacity || config->dispatch.capacity > slots ||
	    slots > SIZE_MAX - config->dispatch.capacity)
		return TR_ERR_INVALID;
	/* 旧调用最多占 dispatch.capacity 个槽，新传输最多占 Channel 容量。
	 * 不能按传输slot直接覆盖尚被旧工作线程持有的不同代次调用。 */
	slots += config->dispatch.capacity;
	if (slots > (SIZE_MAX - sizeof(*ep)) / sizeof(ep->slots[0]) ||
	    slots > (SIZE_MAX - sizeof(*arena)) / sizeof(arena->frames[0]))
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*ep) + slots * sizeof(ep->slots[0]);
	frame_bytes = sizeof(*arena) + slots * sizeof(arena->frames[0]);
	if (frame_bytes > SIZE_MAX - bytes)
		return TR_ERR_BAD_LENGTH;
	ret = tr_memory_budget_reserve(budget, bytes + frame_bytes);
	if (ret != TR_OK)
		return ret;
	ep = calloc(1, bytes);
	arena = calloc(1, frame_bytes);
	if (!ep || !arena) {
		free(ep);
		free(arena);
		(void)tr_memory_budget_release(budget, bytes + frame_bytes);
		return TR_ERR_NOMEM;
	}
	atomic_init(&arena->refs, 1);
	arena->budget = budget;
	arena->bytes = frame_bytes;
	for (size_t i = 0; i < slots; ++i) {
		arena->frames[i].arena = arena;
		atomic_init(&arena->frames[i].busy, false);
	}
	ep->reactor = reactor;
	ep->channel = channel;
	ep->executor = executor;
	ep->budget = budget;
	ep->frames = arena;
	ep->bytes = bytes;
	ep->capacity = slots;
	ep->config = *config;
	ep->terminal = terminal;
	ep->arg = arg;
	ret = tr_rpc_record_dispatch_create_on_owner(
	        channel, executor, methods, method_count, &config->dispatch,
	        tr_endpoint_dispatch_terminal, ep, budget, &ep->dispatch);
	if (ret != TR_OK) {
		tr_endpoint_frames_put(arena);
		free(ep);
		(void)tr_memory_budget_release(budget, bytes);
		return ret;
	}
	*out = ep;
	return TR_OK;
}
int tr_rpc_record_endpoint_start_on_owner(struct tr_rpc_record_endpoint *ep)
{
	uint64_t now;
	int ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->started || ep->stopping || ep->depth)
		return TR_ERR_STATE;
	ep->started = true;
	ret = tr_endpoint_now(&now);
	if (ret == TR_OK)
		ret = tr_reactor_timer_register(ep->reactor, tr_endpoint_tick,
		                                ep, &ep->timer);
	if (ret != TR_OK) {
		ep->stopping = true;
		return ret;
	}
	ep->timer_live = true;
	ret = tr_reactor_timer_arm(
	        ep->timer,
	        tr_endpoint_after(now, ep->config.progress_period_ns));
	if (ret == TR_OK)
		ret = tr_channel_set_handler(ep->channel, tr_endpoint_data,
		                             tr_endpoint_event, NULL, ep);
	if (ret == TR_OK)
		ep->handler_live = true;
	else
		ep->stopping = true;
	return ret;
}
static int tr_endpoint_detach(struct tr_rpc_record_endpoint *ep)
{
	int ret;
	if (ep->handler_live) {
		ret = tr_channel_set_handler(ep->channel, NULL, NULL, NULL,
		                             NULL);
		if (ret != TR_OK)
			return ret;
		ep->handler_live = false;
	}
	if (ep->timer_live) {
		ret = tr_reactor_timer_unregister(ep->timer);
		if (ret != TR_OK)
			return ret;
		ep->timer_live = false;
	}
	return TR_OK;
}
int tr_rpc_record_endpoint_close_on_owner(
        struct tr_rpc_record_endpoint **pointer)
{
	struct tr_rpc_record_endpoint *ep;
	int ret;
	if (!pointer)
		return TR_ERR_INVALID;
	ep = *pointer;
	ret = tr_endpoint_owner(ep);
	if (ret != TR_OK)
		return ret;
	if (ep->depth)
		return TR_ERR_STATE;
	ep->stopping = true;
	if (ep->dispatch) {
		++ep->depth;
		for (size_t i = 0; i < ep->capacity; ++i) {
			struct tr_endpoint_slot *slot = &ep->slots[i];
			if (slot->active && !slot->terminal &&
			    slot->call.dispatch)
				(void)tr_rpc_record_dispatch_abort_on_owner(
				        slot->call, TR_RPC_STATUS_CANCELLED);
		}
		--ep->depth;
	}
	/* 派发器可能已销毁但处理器解绑失败；期间新到达的拒绝流仍须收敛，
	 * 不能因为没有业务派发器就丢弃已登记的终止责任。 */
	ret = tr_rpc_record_endpoint_pump_on_owner(ep);
	if (ret != TR_OK)
		return ret;
	for (size_t i = 0; i < ep->capacity; ++i) {
		struct tr_endpoint_slot *slot = &ep->slots[i];
		if (slot->active &&
		    (!slot->terminal || (!slot->dead && !slot->close_accepted)))
			return TR_AGAIN;
	}
	if (ep->dispatch) {
		++ep->depth;
		ret = tr_rpc_record_dispatch_destroy_on_owner(&ep->dispatch);
		--ep->depth;
		if (ret != TR_OK)
			return ret;
	}
	ret = tr_endpoint_detach(ep);
	return ret == TR_OK ? tr_endpoint_free(pointer) : ret;
}
int tr_rpc_record_endpoint_reap_stopped(struct tr_rpc_record_endpoint **pointer)
{
	struct tr_reactor_recovery_guard guard = {0};
	struct tr_rpc_record_endpoint *ep;
	int ret;
	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	ep = *pointer;
	ret = tr_reactor_recovery_acquire(ep->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (ep->depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	ep->cleanup = ep->stopping = true;
	ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK)
		return ret;
	if (ep->dispatch) {
		++ep->depth;
		ret = tr_rpc_record_dispatch_reap_stopped(&ep->dispatch);
		--ep->depth;
		if (ret != TR_OK)
			return ret;
	}
	ret = tr_reactor_recovery_acquire(ep->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	++ep->depth;
	for (size_t i = 0; i < ep->capacity; ++i) {
		struct tr_endpoint_slot *slot = &ep->slots[i];
		if (!slot->active || slot->reported)
			continue;
		slot->result = (struct tr_rpc_endpoint_result){
		        TR_RPC_STATUS_CANCELLED, TR_RPC_RECORD_OWNER_STOPPED,
		        false};
		tr_endpoint_report(ep, slot);
	}
	--ep->depth;
	ret = tr_endpoint_detach(ep);
	int release_ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK || release_ret != TR_OK)
		return ret != TR_OK ? ret : release_ret;
	return tr_endpoint_free(pointer);
}
