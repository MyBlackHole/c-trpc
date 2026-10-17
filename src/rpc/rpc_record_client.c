#define _POSIX_C_SOURCE 200809L
#include "rpc_record_client.h"
#include "../endian.h"
#include "../execution/reactor_internal.h"
#include "../memory_budget.h"
#include "../transport/channel/channel_internal.h"
#include "tr/status.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct tr_record_client_slot {
	struct tr_rpc_record_client *client;
	struct tr_stream_handle stream;
	struct tr_rpc_record_stream *binding;
	const struct tr_rpc_method_desc *method;
	struct tr_rpc_record_client_callbacks callbacks;
	uint64_t generation, deadline, peer_window;
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
	enum tr_stream_event pending_event;
	uint32_t metadata_size;
	unsigned receiving, sending;
	bool active, routed, accepted, open_pending, writable,
	        peer_window_known;
	bool terminal, reported, peer_final, peer_closed, dead, close_accepted;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
};

struct tr_rpc_record_client {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	const struct tr_rpc_method_desc *methods;
	size_t method_count;
	struct tr_memory_budget *budget;
	struct tr_rpc_record_client_config config;
	struct tr_reactor_timer_handle timer;
	uint32_t *routes;
	uint32_t route_capacity;
	size_t bytes, cursor;
	uint64_t generation;
	unsigned depth, callback_depth;
	bool started, stopping, cleanup, handler_live, timer_live;
	struct tr_record_client_slot slots[];
};

static _Thread_local unsigned tr_record_client_callback_depth;

static int tr_record_client_owner(struct tr_rpc_record_client *client)
{
	if (!client)
		return TR_ERR_INVALID;
	return !client->cleanup &&
	                       tr_reactor_in_owner_context_for(client->reactor)
	               ? TR_OK
	               : TR_ERR_STATE;
}

struct tr_reactor *
tr_rpc_record_client_reactor(struct tr_rpc_record_client *client)
{
	return client ? client->reactor : NULL;
}

int tr_rpc_record_client_in_callback(void)
{
	return tr_record_client_callback_depth != 0;
}

static int tr_record_client_now(uint64_t *out)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0 || ts.tv_sec < 0 ||
	    (uint64_t)ts.tv_sec > (UINT64_MAX - 999999999) / 1000000000)
		return TR_ERR_SYS;
	*out = (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
	return TR_OK;
}

static uint64_t tr_record_client_after(uint64_t now, uint64_t delta)
{
	return delta > UINT64_MAX - now ? UINT64_MAX : now + delta;
}

static struct tr_rpc_record_client_handle
tr_record_client_handle(struct tr_record_client_slot *slot)
{
	return (struct tr_rpc_record_client_handle){
	        .client = slot->client,
	        .generation = slot->generation,
	        .slot = (uint32_t)(slot - slot->client->slots)};
}

static struct tr_record_client_slot *
tr_record_client_find(struct tr_rpc_record_client *client,
                      struct tr_stream_handle stream)
{
	struct tr_record_client_slot *slot;
	uint32_t route;
	if (stream.channel != client->channel ||
	    stream.slot >= client->route_capacity)
		return NULL;
	route = client->routes[stream.slot];
	if (!route || route > client->config.capacity)
		return NULL;
	slot = &client->slots[route - 1];
	return slot->active && slot->routed &&
	                       slot->stream.channel == stream.channel &&
	                       slot->stream.slot == stream.slot &&
	                       slot->stream.generation == stream.generation
	               ? slot
	               : NULL;
}

static void tr_record_client_unroute(struct tr_record_client_slot *slot)
{
	struct tr_rpc_record_client *client = slot->client;
	uint32_t route = (uint32_t)(slot - client->slots) + 1;
	/* 传输槽可能先重用、旧调用后退役；只能移除仍指向本调用的索引。 */
	if (slot->routed && slot->stream.slot < client->route_capacity &&
	    client->routes[slot->stream.slot] == route)
		client->routes[slot->stream.slot] = 0;
	slot->routed = false;
}

static int tr_record_client_lookup(struct tr_rpc_record_client_handle handle,
                                   struct tr_record_client_slot **out)
{
	struct tr_rpc_record_client *client = handle.client;
	struct tr_record_client_slot *slot;
	int ret = tr_record_client_owner(client);
	if (ret != TR_OK)
		return ret;
	if (!handle.generation || handle.slot >= client->config.capacity)
		return TR_ERR_STALE;
	slot = &client->slots[handle.slot];
	if (!slot->active || !slot->accepted ||
	    slot->generation != handle.generation)
		return TR_ERR_STALE;
	if (client->stopping || slot->terminal)
		return TR_ERR_STATE;
	*out = slot;
	return TR_OK;
}

static void tr_record_client_callback_enter(struct tr_rpc_record_client *client)
{
	++client->callback_depth;
	++tr_record_client_callback_depth;
}

static void tr_record_client_callback_leave(struct tr_rpc_record_client *client)
{
	--tr_record_client_callback_depth;
	--client->callback_depth;
}

static void tr_record_client_report(struct tr_record_client_slot *slot)
{
	struct tr_rpc_record_client *client = slot->client;
	if (!slot->accepted || !slot->terminal || slot->reported ||
	    client->callback_depth)
		return;
	slot->reported = true;
	tr_record_client_callback_enter(client);
	slot->callbacks.terminal(tr_record_client_handle(slot), slot->status,
	                         slot->delivery, slot->callbacks.arg);
	tr_record_client_callback_leave(client);
}

static void tr_record_client_terminal(struct tr_rpc_record_stream *binding,
                                      enum tr_rpc_status status,
                                      enum tr_rpc_record_delivery delivery,
                                      void *arg)
{
	struct tr_record_client_slot *slot = arg;
	if (slot->binding != binding || slot->terminal)
		return;
	slot->terminal = true;
	slot->status = status;
	slot->delivery = delivery;
	slot->writable = false;
}

/* 只保存每槽一个传输事实，没有额外事件队列或借用 DATA 存储。
 * 对该调用做任何后续语义操作前先应用它；内联发送期间不能改绑定，
 * 返回安全边界后才终止，禁止已知错误被同批后续 FINAL 越过。 */
static void tr_record_client_defer(struct tr_record_client_slot *slot,
                                   enum tr_stream_event event)
{
	if (event == TR_STREAM_EVENT_REMOTE_CLOSED)
		slot->peer_closed = true;
	if (event == TR_STREAM_EVENT_CLOSED || event == TR_STREAM_EVENT_ERROR)
		slot->dead = true;
	if (event >= TR_STREAM_EVENT_REMOTE_CLOSED &&
	    event <= TR_STREAM_EVENT_ERROR && event > slot->pending_event)
		slot->pending_event = event;
}

static void tr_record_client_apply_pending(struct tr_record_client_slot *slot)
{
	enum tr_stream_event event = slot->pending_event;
	int ret;
	if (!event || slot->sending)
		return;
	if (!slot->binding || slot->terminal) {
		slot->pending_event = 0;
		return;
	}
	/* received 内允许本地 abort；普通 event 则必须等 receive 返回。
	 * 这里已有关闭/错误事实，不排队重放剩余借用字节来争取成功终局。 */
	ret = slot->receiving
	              ? tr_rpc_record_stream_abort_on_owner(
	                        slot->binding, TR_RPC_STATUS_UNAVAILABLE)
	              : tr_rpc_record_stream_event_on_owner(
	                        slot->binding, slot->stream, event);
	if (ret == TR_OK || ret == TR_ERR_STATE)
		slot->pending_event = 0;
}

static void tr_record_client_expire(struct tr_record_client_slot *slot,
                                    uint64_t now)
{
	if (slot->binding && !slot->terminal && slot->deadline &&
	    now >= slot->deadline && !slot->sending)
		(void)tr_rpc_record_stream_abort_on_owner(
		        slot->binding, TR_RPC_STATUS_DEADLINE_EXCEEDED);
}

static int tr_record_client_time(struct tr_record_client_slot *slot,
                                 uint64_t *now)
{
	int ret = tr_record_client_now(now);
	/* 钟源故障不能证明期限已过；仍由唯一绑定选择终局，保留先到结果。 */
	if (ret != TR_OK && slot->accepted && slot->binding && !slot->terminal)
		(void)tr_rpc_record_stream_abort_on_owner(
		        slot->binding, TR_RPC_STATUS_INTERNAL);
	return ret;
}

static void tr_record_client_received(struct tr_rpc_record_stream *binding,
                                      const struct tr_rpc_record_view *view,
                                      void *arg)
{
	struct tr_record_client_slot *slot = arg;
	struct tr_rpc_record_client *client = slot->client;
	if (slot->binding != binding || !slot->accepted)
		return;
	/* 只记录唯一绑定已经接纳的 FINAL 事实，包括合法的 OPEN 前拒绝。
	 * 本地先终局后不再解析晚到记录，不能靠状态码猜测终局来源。 */
	if (view->header.kind == TR_RPC_RECORD_FINAL_STATUS)
		slot->peer_final = true;
	if (slot->reported || client->stopping)
		return;
	tr_record_client_callback_enter(client);
	slot->callbacks.received(tr_record_client_handle(slot), view,
	                         slot->callbacks.arg);
	tr_record_client_callback_leave(client);
	tr_record_client_apply_pending(slot);
}

static int
tr_record_client_send_record(struct tr_record_client_slot *slot,
                             const struct tr_rpc_record_header *header,
                             const uint8_t *metadata, struct tr_buffer **body)
{
	int ret;
	++slot->sending;
	ret = tr_rpc_record_stream_send_on_owner(slot->binding, header,
	                                         metadata, body);
	--slot->sending;
	tr_record_client_apply_pending(slot);
	return ret;
}

static int tr_record_client_retire(struct tr_record_client_slot *slot)
{
	int ret;
	if (slot->accepted && (!slot->terminal || !slot->reported))
		return TR_AGAIN;
	if (slot->accepted && slot->binding && !slot->peer_final &&
	    !slot->peer_closed && !slot->dead && !slot->close_accepted) {
		struct tr_rpc_record_state state;
		size_t cached_bytes;
		ret = tr_rpc_record_stream_snapshot_on_owner(
		        slot->binding, &state, &cached_bytes);
		if (ret != TR_OK)
			return ret;
		/* 请求 HALF_CLOSE 已被接受时，干净的传输半关闭不能取消
		 * 服务端响应。保留固定槽/路由，丢弃晚 DATA 并继续归信用，
		 * 否则响应可永久卡在窗口上。只有精确传输关闭才能结束该
		 * 排空；不能旁路解析已终局绑定之后的字节来寻找 FINAL。 */
		if (state.request_closed)
			return TR_AGAIN;
	}
	if (!slot->dead && !slot->close_accepted) {
		/* 已排队信用保持在 CLOSE 前；未接受和迟到 DATA 产生的信用
		 * 不再发送，不能在对端已经退役该流后补发 WINDOW_UPDATE。 */
		ret = tr_stream_stop_credit_on_owner(slot->stream);
		if (ret == TR_OK)
			ret = tr_stream_close(slot->stream);
		if (ret == TR_OK)
			slot->close_accepted = true;
		else if (ret == TR_ERR_CLOSED || ret == TR_ERR_STALE)
			slot->dead = true;
		else
			return ret;
	}
	/* 本地 CLOSE 是已接受 DATA 的 FIFO 屏障。没有工作线程借用绑定，
	 * 不必等远端确认；Channel 继续保留旧流并释放晚到载荷。 */
	tr_record_client_unroute(slot);
	if (slot->binding) {
		ret = tr_rpc_record_stream_destroy_on_owner(&slot->binding);
		if (ret != TR_OK)
			return ret;
	}
	*slot = (struct tr_record_client_slot){0};
	return TR_OK;
}

static int tr_record_client_progress(struct tr_record_client_slot *slot)
{
	struct tr_rpc_record_client *client = slot->client;
	uint64_t now;
	int ret;
	if (!slot->active)
		return TR_OK;
	tr_record_client_apply_pending(slot);
	/* 前一槽的应用回调可能消耗时间，OPEN 不复用整个 pump 的旧时刻。 */
	ret = TR_OK;
	if (slot->accepted && !slot->terminal) {
		ret = tr_record_client_time(slot, &now);
		if (ret == TR_OK)
			tr_record_client_expire(slot, now);
	}
	if (!slot->accepted || slot->terminal) {
		tr_record_client_report(slot);
		int retire_ret = tr_record_client_retire(slot);
		return retire_ret == TR_OK ? ret : retire_ret;
	}
	if (ret != TR_OK)
		return ret;
	if (slot->open_pending) {
		if (!slot->peer_window_known)
			return TR_AGAIN;
		struct tr_rpc_record_header header = {
		        .kind = TR_RPC_RECORD_OPEN,
		        .service_id = slot->method->service_id,
		        .method_id = slot->method->method_id,
		        .metadata_size = slot->metadata_size,
		        .remaining_timeout_ns =
		                slot->deadline ? slot->deadline - now : 0};
		ret = TR_RPC_RECORD_HEADER_SIZE +
		                              (uint64_t)slot->metadata_size >
		                      slot->peer_window
		              ? TR_ERR_BAD_LENGTH
		              : tr_record_client_send_record(
		                        slot, &header, slot->metadata, NULL);
		if (ret == TR_OK) {
			slot->open_pending = false;
			slot->writable = true;
		} else if (ret == TR_AGAIN) {
			return TR_AGAIN;
		} else {
			(void)tr_rpc_record_stream_abort_on_owner(
			        slot->binding,
			        ret == TR_ERR_BAD_LENGTH
			                ? TR_RPC_STATUS_INVALID_ARGUMENT
			        : ret == TR_ERR_NOMEM
			                ? TR_RPC_STATUS_RESOURCE_EXHAUSTED
			                : TR_RPC_STATUS_UNAVAILABLE);
		}
	}
	if (!slot->terminal && slot->writable && slot->callbacks.writable &&
	    !client->callback_depth) {
		slot->writable = false;
		tr_record_client_callback_enter(client);
		slot->callbacks.writable(tr_record_client_handle(slot),
		                         slot->callbacks.arg);
		tr_record_client_callback_leave(client);
		tr_record_client_apply_pending(slot);
	}
	tr_record_client_report(slot);
	return slot->terminal ? tr_record_client_retire(slot) : TR_OK;
}

int tr_rpc_record_client_pump_on_owner(struct tr_rpc_record_client *client)
{
	int ret = tr_record_client_owner(client);
	if (ret != TR_OK)
		return ret;
	if (client->depth || client->callback_depth)
		return TR_ERR_STATE;
	++client->depth;
	for (size_t n = 0;
	     n < client->config.progress_limit && n < client->config.capacity;
	     ++n) {
		struct tr_record_client_slot *slot =
		        &client->slots[client->cursor];
		client->cursor = (client->cursor + 1) % client->config.capacity;
		int progress_ret = tr_record_client_progress(slot);
		if (ret == TR_OK && progress_ret != TR_OK &&
		    progress_ret != TR_AGAIN)
			ret = progress_ret;
	}
	--client->depth;
	return ret;
}

static uint64_t tr_record_client_tick(void *arg, uint64_t now)
{
	struct tr_rpc_record_client *client = arg;
	(void)tr_rpc_record_client_pump_on_owner(client);
	return tr_record_client_after(now, client->config.progress_period_ns);
}

static enum tr_stream_data_disposition
tr_record_client_data(struct tr_stream_handle stream, uint64_t message_id,
                      struct tr_buffer *payload, void *arg)
{
	struct tr_rpc_record_client *client = arg;
	struct tr_record_client_slot *slot =
	        tr_record_client_find(client, stream);
	uint64_t now;
	(void)message_id;
	/* 本地终局后的排空不再访问 callbacks/arg，也不解析第二遍 RPC；
	 * RELEASE 让 Channel 消费载荷并继续补充尚未停止的接收信用。 */
	if (!slot || !slot->binding || !slot->accepted || slot->terminal)
		return TR_STREAM_DATA_RELEASE;
	if (client->depth) {
		tr_record_client_defer(slot, TR_STREAM_EVENT_ERROR);
		return TR_STREAM_DATA_RELEASE;
	}
	++client->depth;
	tr_record_client_apply_pending(slot);
	if (tr_record_client_time(slot, &now) == TR_OK)
		tr_record_client_expire(slot, now);
	if (!slot->terminal) {
		++slot->receiving;
		(void)tr_rpc_record_stream_receive_on_owner(
		        slot->binding, stream, payload->data, payload->len);
		--slot->receiving;
		tr_record_client_apply_pending(slot);
	}
	tr_record_client_report(slot);
	--client->depth;
	/* Channel 在本回调返回后才消费载荷并提交 WINDOW_UPDATE。
	 * CLOSE 必须等下一 owner 轮次，否则它会越过本条接收信用，
	 * 让对端退役流后再收到未知流的 WINDOW_UPDATE。终局可先报告，
	 * 绑定与关闭责任交给既有进度定时器或后续安全 owner 操作。 */
	return TR_STREAM_DATA_RELEASE;
}

static void tr_record_client_event(struct tr_stream_handle stream,
                                   enum tr_stream_event event, int status,
                                   void *arg)
{
	struct tr_rpc_record_client *client = arg;
	struct tr_record_client_slot *slot =
	        tr_record_client_find(client, stream);
	(void)status;
	if (!slot)
		return;
	if (event == TR_STREAM_EVENT_OPENED && !slot->peer_window_known) {
		struct tr_stream_flow_state flow;
		if (tr_stream_get_flow_state(stream, &flow) == TR_OK) {
			slot->peer_window = flow.tx_send_limit;
			slot->peer_window_known = true;
		} else {
			tr_record_client_defer(slot, TR_STREAM_EVENT_ERROR);
		}
	}
	tr_record_client_defer(slot, event);
	if (event == TR_STREAM_EVENT_WRITABLE)
		slot->writable = true;
	if (client->depth)
		return;
	++client->depth;
	/* ACK 后立刻尝试 OPEN，先于同一读批次内的业务响应；共享 TX
	 * 暂无容量时交给定时器，早拒绝仍经原绑定的 FINAL 规则处理。 */
	(void)tr_record_client_progress(slot);
	--client->depth;
}

static int tr_record_client_metadata(const struct tr_rpc_call_options *options,
                                     uint8_t *out, uint32_t *size)
{
	size_t used = 0;
	if (!options || !options->metadata_count) {
		*size = 0;
		return TR_OK;
	}
	if (!options->metadata ||
	    options->metadata_count > TR_RPC_METADATA_MAX_BYTES / 4)
		return TR_ERR_INVALID;
	for (uint16_t i = 0; i < options->metadata_count; ++i) {
		const struct tr_rpc_metadata *entry = &options->metadata[i];
		size_t key_size, entry_size;
		if (!entry->key || (entry->value_len && !entry->value))
			return TR_ERR_INVALID;
		key_size = strnlen(entry->key, TR_RPC_METADATA_MAX_KEY_LEN + 1);
		entry_size = 3 + key_size + entry->value_len;
		if (!key_size || key_size > TR_RPC_METADATA_MAX_KEY_LEN ||
		    entry_size > TR_RPC_METADATA_MAX_BYTES - used)
			return TR_ERR_BAD_LENGTH;
		for (size_t j = 0; j < key_size; ++j) {
			unsigned char ch = (unsigned char)entry->key[j];
			if (!((ch >= 'a' && ch <= 'z') ||
			      (ch >= '0' && ch <= '9') || ch == '_' ||
			      ch == '.' || ch == '-'))
				return TR_ERR_BAD_FLAGS;
		}
		for (size_t prior = 0; prior < used;) {
			size_t n = out[prior];
			if (n == key_size &&
			    !memcmp(out + prior + 3, entry->key, n))
				return TR_ERR_BAD_FLAGS;
			prior += 3 + n + tr_get_le16(out + prior + 1);
		}
		out[used] = (uint8_t)key_size;
		tr_put_le16(out + used + 1, entry->value_len);
		memcpy(out + used + 3, entry->key, key_size);
		if (entry->value_len)
			memcpy(out + used + 3 + key_size, entry->value,
			       entry->value_len);
		used += entry_size;
	}
	*size = (uint32_t)used;
	return TR_OK;
}

int tr_rpc_record_client_call_start_on_owner(
        struct tr_rpc_record_client *client, uint32_t service_id,
        uint32_t method_id, const struct tr_rpc_call_options *options,
        const struct tr_rpc_record_client_callbacks *callbacks,
        struct tr_rpc_record_client_handle *out)
{
	struct tr_record_client_slot *slot = NULL;
	const struct tr_rpc_method_desc *method = NULL;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	uint32_t metadata_size;
	uint64_t now;
	int ret = tr_record_client_owner(client);
	if (ret != TR_OK)
		return ret;
	if (!callbacks || !callbacks->received || !callbacks->terminal || !out)
		return TR_ERR_INVALID;
	if (!client->started || client->stopping || client->depth ||
	    client->callback_depth || client->generation == UINT64_MAX)
		return TR_ERR_STATE;
	for (size_t i = 0; i < client->method_count; ++i) {
		if (client->methods[i].service_id == service_id &&
		    client->methods[i].method_id == method_id) {
			method = &client->methods[i];
			break;
		}
	}
	if (!method)
		return TR_ERR_UNSUPPORTED;
	ret = tr_record_client_metadata(options, metadata, &metadata_size);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_client_now(&now);
	if (ret != TR_OK)
		return ret;
	for (uint32_t i = 0; i < client->config.capacity; ++i) {
		if (!client->slots[i].active) {
			slot = &client->slots[i];
			break;
		}
	}
	if (!slot)
		return TR_AGAIN;
	*slot = (struct tr_record_client_slot){
	        .client = client,
	        .method = method,
	        .generation = ++client->generation,
	        .deadline =
	                options && options->timeout_ms
	                        ? tr_record_client_after(
	                                  now, (uint64_t)options->timeout_ms *
	                                               1000000)
	                        : 0,
	        .metadata_size = metadata_size,
	        .active = true,
	        .open_pending = true};
	if (metadata_size)
		memcpy(slot->metadata, metadata, metadata_size);
	++client->depth;
	ret = tr_stream_open(client->channel, (enum tr_lane)method->lane,
	                     &slot->stream);
	if (ret != TR_OK) {
		*slot = (struct tr_record_client_slot){0};
		--client->depth;
		return ret;
	}
	slot->routed = true;
	client->routes[slot->stream.slot] =
	        (uint32_t)(slot - client->slots) + 1;
	struct tr_rpc_record_stream_config config = {
	        .send_direction = TR_RPC_RECORD_REQUEST_DIRECTION,
	        .method = *method};
	struct tr_rpc_record_stream_callbacks stream_callbacks = {
	        .received = tr_record_client_received,
	        .terminal = tr_record_client_terminal,
	        .arg = slot};
	ret = tr_rpc_record_stream_create_on_owner(
	        slot->stream, &config, &stream_callbacks, client->budget,
	        &slot->binding);
	if (ret == TR_OK) {
		slot->callbacks = *callbacks;
		slot->accepted = true;
		*out = tr_record_client_handle(slot);
	} else {
		/* 传输已接管 OPEN，但应用尚未获得调用；不能漏掉关闭责任，
		 * 也不能在返回失败后借用其 callbacks/arg。 */
		(void)tr_record_client_retire(slot);
	}
	--client->depth;
	if (ret == TR_OK)
		(void)tr_rpc_record_client_pump_on_owner(client);
	return ret;
}

static int tr_record_client_request(struct tr_rpc_record_client_handle handle,
                                    enum tr_rpc_record_kind kind,
                                    struct tr_buffer **body)
{
	struct tr_record_client_slot *slot;
	uint64_t now;
	int ret = tr_record_client_lookup(handle, &slot);
	if (ret != TR_OK)
		return ret;
	if (slot->sending)
		return TR_ERR_STATE;
	struct tr_rpc_record_client *client = slot->client;
	++client->depth;
	tr_record_client_apply_pending(slot);
	ret = tr_record_client_time(slot, &now);
	if (ret == TR_OK)
		tr_record_client_expire(slot, now);
	if (ret == TR_OK) {
		struct tr_rpc_record_state state;
		size_t cached_bytes;
		ret = tr_rpc_record_stream_snapshot_on_owner(
		        slot->binding, &state, &cached_bytes);
		/* FINAL 的 received 回调尚未退出时，绑定已选终局而宿主的
		 * terminal 桥尚未运行；不能用 open_pending 把它误报为 AGAIN。
		 */
		if (ret == TR_OK && state.terminal)
			ret = TR_ERR_STATE;
	}
	if (ret == TR_OK && slot->open_pending)
		ret = TR_AGAIN;
	if (ret == TR_OK) {
		struct tr_buffer *buffer = body ? *body : NULL;
		struct tr_rpc_record_header header = {.kind = kind};
		if (kind == TR_RPC_RECORD_MESSAGE) {
			header.codec_id = slot->method->request_codec_id;
			header.encoded_size = buffer ? buffer->len : 0;
			header.decoded_size = header.encoded_size;
		}
		ret = TR_RPC_RECORD_HEADER_SIZE +
		                              (uint64_t)header.encoded_size >
		                      slot->peer_window
		              ? TR_ERR_BAD_LENGTH
		              : tr_record_client_send_record(slot, &header,
		                                             NULL, body);
	}
	if (ret == TR_AGAIN)
		slot->writable = true;
	tr_record_client_report(slot);
	--client->depth;
	if (!client->depth)
		(void)tr_rpc_record_client_pump_on_owner(client);
	return ret;
}

int tr_rpc_record_client_send_on_owner(
        struct tr_rpc_record_client_handle handle, struct tr_buffer **body)
{
	return tr_record_client_request(handle, TR_RPC_RECORD_MESSAGE, body);
}

int tr_rpc_record_client_half_close_on_owner(
        struct tr_rpc_record_client_handle handle)
{
	return tr_record_client_request(handle, TR_RPC_RECORD_HALF_CLOSE, NULL);
}

int tr_rpc_record_client_cancel_on_owner(
        struct tr_rpc_record_client_handle handle)
{
	struct tr_record_client_slot *slot;
	int ret = tr_record_client_lookup(handle, &slot);
	if (ret != TR_OK)
		return ret;
	if (slot->sending)
		return TR_ERR_STATE;
	struct tr_rpc_record_client *client = slot->client;
	++client->depth;
	tr_record_client_apply_pending(slot);
	ret = slot->terminal ? TR_ERR_STATE
	                     : tr_rpc_record_stream_abort_on_owner(
	                               slot->binding, TR_RPC_STATUS_CANCELLED);
	tr_record_client_report(slot);
	--client->depth;
	if (!client->depth)
		(void)tr_rpc_record_client_pump_on_owner(client);
	return ret;
}

int tr_rpc_record_client_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_method_desc *methods,
        size_t method_count, const struct tr_rpc_record_client_config *config,
        struct tr_memory_budget *budget, struct tr_rpc_record_client **out)
{
	struct tr_rpc_record_client *client;
	struct tr_reactor *reactor;
	size_t bytes, capacity, route_capacity;
	int ret;
	if (!channel || !methods || !method_count || !config || !budget ||
	    !budget->limit_bytes || !out || !config->capacity ||
	    !config->progress_limit || config->progress_period_ns < 1000000 ||
	    config->progress_period_ns > 1000000000)
		return TR_ERR_INVALID;
	reactor = tr_channel_reactor(channel);
	if (*out || !tr_reactor_in_owner_context_for(reactor))
		return TR_ERR_STATE;
	route_capacity = tr_channel_max_streams(channel);
	if (config->capacity > route_capacity)
		return TR_ERR_INVALID;
	capacity = config->capacity;
	if (capacity > (SIZE_MAX - sizeof(*client)) / sizeof(client->slots[0]))
		return TR_ERR_BAD_LENGTH;
	bytes = sizeof(*client) + capacity * sizeof(client->slots[0]);
	if (route_capacity > (SIZE_MAX - bytes) / sizeof(client->routes[0]))
		return TR_ERR_BAD_LENGTH;
	bytes += route_capacity * sizeof(client->routes[0]);
	/* 聚合槽位和路由后再次验证总长，分配必须容纳完整控制块。 */
	if (bytes < sizeof(*client))
		return TR_ERR_BAD_LENGTH;
	ret = tr_memory_budget_reserve(budget, bytes);
	if (ret != TR_OK)
		return ret;
	client = calloc(1, bytes);
	if (!client) {
		(void)tr_memory_budget_release(budget, bytes);
		return TR_ERR_NOMEM;
	}
	client->reactor = reactor;
	client->channel = channel;
	client->methods = methods;
	client->method_count = method_count;
	client->config = *config;
	client->route_capacity = (uint32_t)route_capacity;
	client->routes = (uint32_t *)(client->slots + capacity);
	client->budget = budget;
	client->bytes = bytes;
	*out = client;
	return TR_OK;
}

int tr_rpc_record_client_start_on_owner(struct tr_rpc_record_client *client)
{
	uint64_t now;
	int ret = tr_record_client_owner(client);
	if (ret != TR_OK)
		return ret;
	if (client->started || client->stopping || client->depth ||
	    client->callback_depth)
		return TR_ERR_STATE;
	client->started = true;
	ret = tr_record_client_now(&now);
	if (ret == TR_OK)
		ret = tr_reactor_timer_register(client->reactor,
		                                tr_record_client_tick, client,
		                                &client->timer);
	if (ret != TR_OK) {
		client->stopping = true;
		return ret;
	}
	client->timer_live = true;
	ret = tr_reactor_timer_arm(
	        client->timer,
	        tr_record_client_after(now, client->config.progress_period_ns));
	if (ret == TR_OK)
		ret = tr_channel_set_handler(
		        client->channel, tr_record_client_data,
		        tr_record_client_event, NULL, client);
	if (ret == TR_OK)
		client->handler_live = true;
	else
		client->stopping = true;
	return ret;
}

static int tr_record_client_detach(struct tr_rpc_record_client *client)
{
	int ret;
	if (client->handler_live) {
		ret = tr_channel_set_handler(client->channel, NULL, NULL, NULL,
		                             NULL);
		if (ret != TR_OK)
			return ret;
		client->handler_live = false;
	}
	if (client->timer_live) {
		ret = tr_reactor_timer_unregister(client->timer);
		if (ret != TR_OK)
			return ret;
		client->timer_live = false;
	}
	return TR_OK;
}

static int tr_record_client_free(struct tr_rpc_record_client **pointer)
{
	struct tr_rpc_record_client *client = *pointer;
	struct tr_memory_budget *budget = client->budget;
	size_t bytes = client->bytes;
	free(client);
	*pointer = NULL;
	return tr_memory_budget_release(budget, bytes);
}

int tr_rpc_record_client_close_on_owner(struct tr_rpc_record_client **pointer)
{
	struct tr_rpc_record_client *client;
	int ret;
	if (!pointer)
		return TR_ERR_INVALID;
	client = *pointer;
	ret = tr_record_client_owner(client);
	if (ret != TR_OK)
		return ret;
	if (client->depth || client->callback_depth)
		return TR_ERR_STATE;
	client->stopping = true;
	++client->depth;
	for (uint32_t i = 0; i < client->config.capacity; ++i) {
		struct tr_record_client_slot *slot = &client->slots[i];
		if (slot->active && slot->binding && !slot->terminal) {
			tr_record_client_apply_pending(slot);
			if (!slot->terminal)
				(void)tr_rpc_record_stream_abort_on_owner(
				        slot->binding, TR_RPC_STATUS_CANCELLED);
		}
	}
	--client->depth;
	ret = tr_rpc_record_client_pump_on_owner(client);
	if (ret != TR_OK)
		return ret;
	for (uint32_t i = 0; i < client->config.capacity; ++i) {
		if (client->slots[i].active)
			return TR_AGAIN;
	}
	ret = tr_record_client_detach(client);
	return ret == TR_OK ? tr_record_client_free(pointer) : ret;
}

int tr_rpc_record_client_reap_stopped(struct tr_rpc_record_client **pointer)
{
	struct tr_reactor_recovery_guard guard = {0};
	struct tr_rpc_record_client *client;
	int ret;
	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	client = *pointer;
	ret = tr_reactor_recovery_acquire(client->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	if (client->depth || client->callback_depth) {
		(void)tr_reactor_recovery_release(&guard);
		return TR_ERR_STATE;
	}
	client->cleanup = client->stopping = true;
	ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK)
		return ret;
	++client->depth;
	for (uint32_t i = 0; i < client->config.capacity; ++i) {
		struct tr_record_client_slot *slot = &client->slots[i];
		if (!slot->active)
			continue;
		tr_record_client_unroute(slot);
		if (slot->binding) {
			ret = tr_rpc_record_stream_reap_stopped(&slot->binding);
			if (ret != TR_OK) {
				--client->depth;
				return ret;
			}
		}
		/* 未发布的 binding 可安静回收；已受理但 OPEN 仍 pending 的
		 * 调用另有一次完成责任，不能把 wire opened 当成接纳标志。 */
		if (slot->accepted && !slot->terminal) {
			slot->terminal = true;
			slot->status = TR_RPC_STATUS_CANCELLED;
		}
		/* 另一个应用回调可先选定本槽终局，报告却延迟到 owner 退出
		 * 之后。保留该结果，但交付域必须反映现在的清理线程。 */
		if (!slot->reported)
			slot->delivery = TR_RPC_RECORD_OWNER_STOPPED;
		tr_record_client_report(slot);
		*slot = (struct tr_record_client_slot){0};
	}
	--client->depth;
	ret = tr_reactor_recovery_acquire(client->reactor, &guard);
	if (ret != TR_OK)
		return ret;
	ret = tr_record_client_detach(client);
	int release_ret = tr_reactor_recovery_release(&guard);
	if (ret != TR_OK || release_ret != TR_OK)
		return ret != TR_OK ? ret : release_ret;
	return tr_record_client_free(pointer);
}
