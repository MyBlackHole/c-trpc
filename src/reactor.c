#define _GNU_SOURCE
#include "tr/reactor.h"

#include "tr/command_queue.h"
#include "tr/crc32c.h"
#include "tr/parser.h"
#include "tr/socket.h"
#include "tr/status.h"
#include "tr/wire.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define TR_REACTOR_EVENT_BATCH 64U
#define TR_COMMAND_BATCH 64U
#define TR_TX_READY_BATCH 64U
#define TR_WAKE_TOKEN UINT64_MAX

struct tr_tx_pool;

struct tr_tx_item {
	struct tr_tx_item *next;
	struct tr_tx_pool *owner_pool;
	uint8_t header[TR_WIRE_HEADER_SIZE];
	struct tr_buffer *payloads[TR_REACTOR_MAX_TX_SLICES];
	uint32_t payload_count;
	uint16_t type;
	uint32_t base_flags;
	uint32_t stream_id;
	uint64_t message_id;
	uint64_t message_len;
	uint64_t message_pos;
	uint32_t frame_payload_len;
	uint32_t max_frame_payload_len;
	uint64_t wire_pos;
	uint64_t wire_len;
};

struct tr_tx_pool {
	pthread_mutex_t lock;
	struct tr_tx_item *items;
	struct tr_tx_item *free_list;
	uint32_t capacity;
	uint32_t free_count;
};

TR_DEFINE_PTR_OWNERSHIP(tr_tx_item_array, struct tr_tx_item, free)

struct tr_connection {
	int fd;
	uint32_t slot;
	uint32_t generation;
	enum tr_connection_state state;

	/*
	 * 以下 handler 只允许 Reactor owner thread 修改和读取。
	 * 外部线程必须通过 TR_CMD_SET_HANDLER 提交变更。
	 */
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;

	struct tr_parser parser;

	struct tr_tx_item *tx_head;
	struct tr_tx_item *tx_tail;

	uint32_t epoll_events;
	int tx_wait_writable;
	int tx_scheduled;
	int rx_paused;

	struct tr_connection *tx_ready_next;

	uint64_t rx_bytes;
	uint64_t tx_bytes;
	uint64_t rx_frames;
	uint64_t tx_frames;
	uint64_t recv_eagain;
	uint64_t send_eagain;
	uint64_t rx_pauses_count;
	uint64_t last_rx_activity_ns;
	uint64_t last_tx_activity_ns;
	uint32_t tx_queued_items;
};

struct tr_slot {
	/*
	 * capability metadata 允许跨线程读取/预留，但必须通过 C11 atomic
	 * 一次性更新 generation + state，避免复用 slot 时出现撕裂状态。
	 */
	_Atomic uint64_t meta;
};

struct tr_reactor_sync {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int done;
	int status;
};

struct tr_reactor_handler_request {
	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
	struct tr_reactor_sync sync;
};

struct tr_reactor {
	struct tr_reactor_config config;

	int epoll_fd;
	int wake_fd;
	pthread_t thread;

	pthread_mutex_t ctl_lock;
	int started;
	int accepting;
	int stopping;

	struct tr_command_queue commands;
	struct tr_tx_pool tx_pool;
	struct tr_tx_pool control_tx_pool;
	struct tr_buffer_pool rx_pool;

	struct tr_slot *slots;
	struct tr_connection *connections;

	struct tr_connection *tx_ready_head;
	struct tr_connection *tx_ready_tail;

	tr_reactor_frame_cb frame_cb;
	tr_reactor_event_cb event_cb;
	void *callback_arg;
};

static uint32_t tr_nonzero(uint32_t value, uint32_t fallback)
{
	return value ? value : fallback;
}

static uint64_t tr_reactor_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)ts.tv_nsec;
}

static uint64_t tr_conn_token(uint32_t slot, uint32_t generation)
{
	return ((uint64_t)generation << 32) | (uint64_t)slot;
}

static void tr_conn_token_decode(uint64_t token, uint32_t *slot,
				 uint32_t *generation)
{
	*slot = (uint32_t)token;
	*generation = (uint32_t)(token >> 32);
}

static int tr_tx_pool_init(struct tr_tx_pool *pool, uint32_t capacity)
{
	struct tr_tx_item *items TR_AUTO(tr_tx_item_array_cleanup) = NULL;
	uint32_t i;

	if (!pool || capacity == 0)
		return TR_ERR_INVALID;

	memset(pool, 0, sizeof(*pool));
	items = (struct tr_tx_item *)calloc(capacity, sizeof(*items));
	if (!items)
		return TR_ERR_NOMEM;

	if (pthread_mutex_init(&pool->lock, NULL) != 0)
		return TR_ERR_INVALID;

	pool->items = tr_tx_item_array_take(&items);
	pool->capacity = capacity;
	pool->free_count = capacity;

	for (i = 0; i < capacity; ++i) {
		pool->items[i].next = pool->free_list;
		pool->free_list = &pool->items[i];
	}

	return TR_OK;
}

static void tr_tx_pool_destroy(struct tr_tx_pool *pool)
{
	if (!pool)
		return;

	free(pool->items);
	pool->items = NULL;
	pool->free_list = NULL;
	pool->capacity = 0;
	pool->free_count = 0;
	pthread_mutex_destroy(&pool->lock);
}

static struct tr_tx_item *tr_tx_pool_acquire(struct tr_tx_pool *pool)
{
	struct tr_tx_item *item;

	pthread_mutex_lock(&pool->lock);
	item = pool->free_list;
	if (item) {
		pool->free_list = item->next;
		pool->free_count--;
	}
	pthread_mutex_unlock(&pool->lock);

	if (item) {
		memset(item, 0, sizeof(*item));
		item->owner_pool = pool;
	}

	return item;
}

static void tr_tx_pool_release(struct tr_tx_pool *pool, struct tr_tx_item *item)
{
	if (!pool || !item)
		return;

	memset(item->payloads, 0, sizeof(item->payloads));
	item->payload_count = 0;
	item->wire_pos = 0;
	item->wire_len = 0;

	pthread_mutex_lock(&pool->lock);
	item->next = pool->free_list;
	pool->free_list = item;
	pool->free_count++;
	pthread_mutex_unlock(&pool->lock);
}

static int tr_reactor_signal(struct tr_reactor *reactor)
{
	uint64_t value = 1;
	ssize_t n;

	do {
		n = write(reactor->wake_fd, &value, sizeof(value));
	} while (n < 0 && errno == EINTR);

	if (n == (ssize_t)sizeof(value))
		return TR_OK;

	if (n < 0 && errno == EAGAIN)
		return TR_OK;

	return TR_ERR_SYS;
}


static int tr_reactor_sync_init(struct tr_reactor_sync *sync)
{
	if (!sync)
		return TR_ERR_INVALID;

	memset(sync, 0, sizeof(*sync));
	if (pthread_mutex_init(&sync->lock, NULL) != 0)
		return TR_ERR_SYS;
	if (pthread_cond_init(&sync->cond, NULL) != 0) {
		pthread_mutex_destroy(&sync->lock);
		return TR_ERR_SYS;
	}
	sync->status = TR_OK;
	return TR_OK;
}

static void tr_reactor_sync_destroy(struct tr_reactor_sync *sync)
{
	if (!sync)
		return;
	pthread_cond_destroy(&sync->cond);
	pthread_mutex_destroy(&sync->lock);
}

static void tr_reactor_sync_complete(struct tr_reactor_sync *sync, int status)
{
	if (!sync)
		return;

	pthread_mutex_lock(&sync->lock);
	sync->status = status;
	sync->done = 1;
	pthread_cond_signal(&sync->cond);
	pthread_mutex_unlock(&sync->lock);
}

static int tr_reactor_sync_wait(struct tr_reactor_sync *sync)
{
	int status;

	if (!sync)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&sync->lock);
	while (!sync->done)
		pthread_cond_wait(&sync->cond, &sync->lock);
	status = sync->status;
	pthread_mutex_unlock(&sync->lock);
	return status;
}

static int tr_reactor_push_locked(struct tr_reactor *reactor,
				  const struct tr_command *command)
{
	int need_wake = 0;
	int ret;

	ret = tr_command_queue_push(&reactor->commands, command, &need_wake);
	if (ret != TR_OK)
		return ret;

	/*
     * command 一旦进入有界队列，ownership 就已经转移给 Reactor。
     * 后续 eventfd wake 失败不能再把这次 push 报告成失败，否则 producer
     * 可能释放仍被 queue 引用的资源，造成 UAF。
     * tr_reactor_signal() 已把 EAGAIN 视为成功；其他 wake 失败表示 runtime
     * 已异常，但已经入队的 command 仍然归 Reactor 所有。
     */
	if (need_wake)
		(void)tr_reactor_signal(reactor);

	return TR_OK;
}

static uint64_t tr_slot_meta_make(uint32_t generation,
				       enum tr_connection_state state)
{
	return ((uint64_t)generation << 32) | (uint32_t)state;
}

static uint32_t tr_slot_meta_generation(uint64_t meta)
{
	return (uint32_t)(meta >> 32);
}

static enum tr_connection_state tr_slot_meta_state(uint64_t meta)
{
	return (enum tr_connection_state)(uint32_t)meta;
}

static int tr_slot_live(struct tr_reactor *reactor, uint32_t slot,
			uint32_t generation)
{
	uint64_t meta;
	enum tr_connection_state state;

	if (slot >= reactor->config.max_connections)
		return 0;

	meta = atomic_load_explicit(&reactor->slots[slot].meta,
				    memory_order_acquire);
	if (tr_slot_meta_generation(meta) != generation)
		return 0;

	state = tr_slot_meta_state(meta);
	return state == TR_CONN_RESERVED || state == TR_CONN_ACTIVE;
}

static int tr_slot_set_state(struct tr_reactor *reactor, uint32_t slot,
			     uint32_t generation,
			     enum tr_connection_state state)
{
	uint64_t old;
	uint64_t desired;

	if (slot >= reactor->config.max_connections)
		return TR_ERR_STALE;

	old = atomic_load_explicit(&reactor->slots[slot].meta,
				   memory_order_acquire);
	for (;;) {
		if (tr_slot_meta_generation(old) != generation)
			return TR_ERR_STALE;

		desired = tr_slot_meta_make(generation, state);
		if (atomic_compare_exchange_weak_explicit(
			    &reactor->slots[slot].meta, &old, desired,
			    memory_order_acq_rel, memory_order_acquire))
			return TR_OK;
	}
}

static int tr_slot_reserve(struct tr_reactor *reactor, uint32_t *slot_out,
			   uint32_t *generation_out)
{
	uint32_t slot;

	for (slot = 0; slot < reactor->config.max_connections; ++slot) {
		uint64_t old = atomic_load_explicit(&reactor->slots[slot].meta,
						   memory_order_acquire);
		uint32_t generation;
		uint64_t desired;

		if (tr_slot_meta_state(old) != TR_CONN_FREE)
			continue;

		generation = tr_slot_meta_generation(old) + 1U;
		if (generation == 0)
			generation = 1U;
		desired = tr_slot_meta_make(generation, TR_CONN_RESERVED);

		if (!atomic_compare_exchange_strong_explicit(
			    &reactor->slots[slot].meta, &old, desired,
			    memory_order_acq_rel, memory_order_acquire))
			continue;

		*slot_out = slot;
		*generation_out = generation;
		return TR_OK;
	}

	return TR_AGAIN;
}

static uint32_t tr_connection_interest(const struct tr_connection *connection)
{
	uint32_t events = EPOLLRDHUP;

	if (!__atomic_load_n(&connection->rx_paused, __ATOMIC_RELAXED))
		events |= EPOLLIN;
	if (__atomic_load_n(&connection->tx_wait_writable, __ATOMIC_RELAXED))
		events |= EPOLLOUT;

	return events;
}

static int tr_connection_update_interest(struct tr_reactor *reactor,
					 struct tr_connection *connection)
{
	struct epoll_event event;
	uint32_t events;

	events = tr_connection_interest(connection);
	if (events == connection->epoll_events)
		return TR_OK;

	memset(&event, 0, sizeof(event));
	event.events = events;
	event.data.u64 =
		tr_conn_token(connection->slot, connection->generation);

	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_MOD, connection->fd,
		      &event) < 0)
		return TR_ERR_SYS;

	connection->epoll_events = events;
	return TR_OK;
}

static void tr_schedule_tx(struct tr_reactor *reactor,
			   struct tr_connection *connection)
{
	if (!connection || connection->state != TR_CONN_ACTIVE ||
	    __atomic_load_n(&connection->tx_wait_writable, __ATOMIC_RELAXED) ||
	    connection->tx_scheduled || !connection->tx_head)
		return;

	connection->tx_scheduled = 1;
	connection->tx_ready_next = NULL;

	if (reactor->tx_ready_tail)
		reactor->tx_ready_tail->tx_ready_next = connection;
	else
		reactor->tx_ready_head = connection;

	reactor->tx_ready_tail = connection;
}

static void tr_unschedule_tx(struct tr_reactor *reactor,
			     struct tr_connection *connection)
{
	struct tr_connection *prev = NULL;
	struct tr_connection *cur = reactor->tx_ready_head;

	if (!connection->tx_scheduled)
		return;

	while (cur) {
		if (cur == connection) {
			if (prev)
				prev->tx_ready_next = cur->tx_ready_next;
			else
				reactor->tx_ready_head = cur->tx_ready_next;

			if (reactor->tx_ready_tail == cur)
				reactor->tx_ready_tail = prev;

			cur->tx_ready_next = NULL;
			cur->tx_scheduled = 0;
			return;
		}

		prev = cur;
		cur = cur->tx_ready_next;
	}

	connection->tx_scheduled = 0;
	connection->tx_ready_next = NULL;
}

static void tr_release_tx_queue(struct tr_reactor *reactor,
				struct tr_connection *connection)
{
	struct tr_tx_item *item = connection->tx_head;

	(void)reactor;

	while (item) {
		struct tr_tx_item *next = item->next;
		{
			uint32_t i;
			for (i = 0; i < item->payload_count; ++i)
				if (item->payloads[i])
					tr_buffer_release(item->payloads[i]);
		}
		tr_tx_pool_release(item->owner_pool, item);
		item = next;
	}

	connection->tx_head = NULL;
	connection->tx_tail = NULL;
	__atomic_store_n(&connection->tx_queued_items, 0U, __ATOMIC_RELAXED);
}

static void tr_connection_notify(struct tr_reactor *reactor,
				 const struct tr_connection *connection,
				 enum tr_connection_event event, int status)
{
	struct tr_conn_handle handle;

	if (!connection->event_cb)
		return;

	handle.reactor = reactor;
	handle.slot = connection->slot;
	handle.generation = connection->generation;

	connection->event_cb(handle, event, status, connection->callback_arg);
}

static void tr_connection_close_internal(struct tr_reactor *reactor,
					 struct tr_connection *connection,
					 enum tr_connection_event event,
					 int status)
{
	if (!connection || connection->state != TR_CONN_ACTIVE)
		return;

	connection->state = (event == TR_CONN_EVENT_ERROR) ? TR_CONN_ERROR :
							     TR_CONN_CLOSED;

	(void)epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, connection->fd, NULL);

	close(connection->fd);
	connection->fd = -1;

	tr_unschedule_tx(reactor, connection);
	tr_parser_reset(&connection->parser);
	tr_release_tx_queue(reactor, connection);

	__atomic_store_n(&connection->tx_wait_writable, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&connection->rx_paused, 0, __ATOMIC_RELAXED);

	tr_slot_set_state(reactor, connection->slot, connection->generation,
			  TR_CONN_FREE);

	tr_connection_notify(reactor, connection, event, status);
}

static int tr_connection_adopt(struct tr_reactor *reactor, uint32_t slot,
			       uint32_t generation, int fd)
{
	struct tr_connection *connection;
	struct epoll_event event;
	struct tr_wire_limits limits;
	int owned_fd TR_AUTO(tr_fd_cleanup) = fd;
	int ret;

	if (slot >= reactor->config.max_connections)
		return TR_ERR_STALE;

	connection = &reactor->connections[slot];
	memset(connection, 0, sizeof(*connection));
	connection->fd = -1;
	connection->slot = slot;
	connection->generation = generation;
	connection->state = TR_CONN_ACTIVE;
	connection->frame_cb = reactor->frame_cb;
	connection->event_cb = reactor->event_cb;
	connection->callback_arg = reactor->callback_arg;
	__atomic_store_n(&connection->last_rx_activity_ns, tr_reactor_now_ns(),
			 __ATOMIC_RELAXED);
	__atomic_store_n(&connection->last_tx_activity_ns, tr_reactor_now_ns(),
			 __ATOMIC_RELAXED);

	limits.max_payload_len = reactor->config.max_payload_len;
	ret = tr_parser_init(&connection->parser, &reactor->rx_pool, &limits);
	if (ret != TR_OK) {
		tr_slot_set_state(reactor, slot, generation, TR_CONN_FREE);
		return ret;
	}

	memset(&event, 0, sizeof(event));
	connection->epoll_events = tr_connection_interest(connection);
	event.events = connection->epoll_events;
	event.data.u64 = tr_conn_token(slot, generation);

	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, owned_fd, &event) < 0) {
		tr_parser_reset(&connection->parser);
		connection->state = TR_CONN_ERROR;
		tr_slot_set_state(reactor, slot, generation, TR_CONN_FREE);
		return TR_ERR_SYS;
	}

	connection->fd = tr_fd_take(&owned_fd);
	tr_slot_set_state(reactor, slot, generation, TR_CONN_ACTIVE);
	return TR_OK;
}

static uint32_t tr_tx_crc_range(const struct tr_tx_item *item, uint64_t start,
				uint32_t len)
{
	uint32_t state = tr_crc32c_begin();
	uint64_t pos = start;
	uint32_t remaining = len;
	uint32_t i;

	for (i = 0; i < item->payload_count && remaining != 0; ++i) {
		const struct tr_buffer *payload = item->payloads[i];
		uint32_t take;

		if (!payload)
			continue;
		if (pos >= payload->len) {
			pos -= payload->len;
			continue;
		}

		take = payload->len - (uint32_t)pos;
		if (take > remaining)
			take = remaining;
		state = tr_crc32c_update(state, payload->data + (size_t)pos,
					 take);
		remaining -= take;
		pos = 0;
	}

	return remaining == 0 ? tr_crc32c_finish(state) : 0U;
}

static int tr_tx_prepare_frame(struct tr_reactor *reactor,
			       struct tr_tx_item *item)
{
	struct tr_frame_header header;
	uint64_t remaining;
	uint32_t frame_len;
	uint32_t flags;

	if (!reactor || !item || item->message_pos > item->message_len)
		return TR_ERR_STATE;

	remaining = item->message_len - item->message_pos;
	if (remaining > item->max_frame_payload_len)
		frame_len = item->max_frame_payload_len;
	else
		frame_len = (uint32_t)remaining;

	/* 长度为 0 的非 DATA control message 仍然构成一个完整 frame。 */
	if (item->type != TR_FRAME_DATA && item->message_pos != 0)
		return TR_ERR_STATE;

	flags = item->base_flags & ~(TR_FRAME_F_FIRST | TR_FRAME_F_LAST);
	if (item->type == TR_FRAME_DATA) {
		if (item->message_pos == 0)
			flags |= TR_FRAME_F_FIRST;
		if (item->message_pos + frame_len == item->message_len)
			flags |= TR_FRAME_F_LAST;
	} else {
		flags = item->base_flags;
	}

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = item->type;
	header.flags = flags;
	header.stream_id = item->stream_id;
	header.message_id = item->message_id;
	header.payload_len = frame_len;
	header.payload_crc32c =
		tr_tx_crc_range(item, item->message_pos, frame_len);

	if (tr_wire_header_encode(item->header, &header) != TR_OK)
		return TR_ERR_INVALID;

	item->frame_payload_len = frame_len;
	item->wire_pos = 0;
	item->wire_len = TR_WIRE_HEADER_SIZE + (uint64_t)frame_len;
	return TR_OK;
}

static int tr_tx_build_iov(struct tr_tx_item *item,
			   struct iovec iov[1U + TR_REACTOR_MAX_TX_SLICES])
{
	uint64_t pos = item->wire_pos;
	uint64_t payload_pos;
	uint32_t remaining;
	int count = 0;
	uint32_t i;

	if (pos < TR_WIRE_HEADER_SIZE) {
		iov[count].iov_base = item->header + (size_t)pos;
		iov[count].iov_len = TR_WIRE_HEADER_SIZE - (size_t)pos;
		count++;
		pos = 0;
	} else {
		pos -= TR_WIRE_HEADER_SIZE;
	}

	if (pos > item->frame_payload_len)
		return -1;

	payload_pos = item->message_pos + pos;
	remaining = item->frame_payload_len - (uint32_t)pos;

	for (i = 0; i < item->payload_count && remaining != 0; ++i) {
		struct tr_buffer *payload = item->payloads[i];
		uint32_t take;

		if (!payload)
			continue;
		if (payload_pos >= payload->len) {
			payload_pos -= payload->len;
			continue;
		}

		take = payload->len - (uint32_t)payload_pos;
		if (take > remaining)
			take = remaining;

		iov[count].iov_base = payload->data + (size_t)payload_pos;
		iov[count].iov_len = take;
		count++;
		remaining -= take;
		payload_pos = 0;
	}

	if (remaining != 0)
		return -1;
	return count;
}

static void tr_connection_complete_tx(struct tr_reactor *reactor,
				      struct tr_connection *connection)
{
	struct tr_tx_item *item = connection->tx_head;

	(void)reactor;

	if (!item)
		return;

	connection->tx_head = item->next;
	if (!connection->tx_head)
		connection->tx_tail = NULL;
	(void)__atomic_fetch_sub(&connection->tx_queued_items, 1U,
				 __ATOMIC_RELAXED);

	{
		uint32_t i;
		for (i = 0; i < item->payload_count; ++i)
			if (item->payloads[i])
				tr_buffer_release(item->payloads[i]);
	}
	tr_tx_pool_release(item->owner_pool, item);
}

static void tr_connection_flush_tx(struct tr_reactor *reactor,
				   struct tr_connection *connection)
{
	size_t budget = reactor->config.tx_budget_bytes;

	connection->tx_scheduled = 0;
	connection->tx_ready_next = NULL;

	while (budget != 0 && connection->state == TR_CONN_ACTIVE &&
	       connection->tx_head) {
		struct tr_tx_item *item = connection->tx_head;
		struct iovec iov[1U + TR_REACTOR_MAX_TX_SLICES];
		struct msghdr message;
		ssize_t n;
		int iov_count;

		iov_count = tr_tx_build_iov(item, iov);
		if (iov_count <= 0) {
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR,
						     TR_ERR_STATE);
			return;
		}

		memset(&message, 0, sizeof(message));
		message.msg_iov = iov;
		message.msg_iovlen = (size_t)iov_count;

		n = sendmsg(connection->fd, &message, MSG_NOSIGNAL);
		if (n > 0) {
			(void)__atomic_fetch_add(&connection->tx_bytes,
						 (uint64_t)n, __ATOMIC_RELAXED);
			__atomic_store_n(&connection->last_tx_activity_ns,
					 tr_reactor_now_ns(), __ATOMIC_RELAXED);
			item->wire_pos += (uint64_t)n;
			if ((size_t)n >= budget)
				budget = 0;
			else
				budget -= (size_t)n;

			if (item->wire_pos == item->wire_len) {
				(void)__atomic_fetch_add(&connection->tx_frames,
							 UINT64_C(1),
							 __ATOMIC_RELAXED);
				if (item->type == TR_FRAME_DATA) {
					item->message_pos +=
						item->frame_payload_len;
					if (item->message_pos <
					    item->message_len) {
						int ret = tr_tx_prepare_frame(
							reactor, item);
						if (ret != TR_OK) {
							tr_connection_close_internal(
								reactor,
								connection,
								TR_CONN_EVENT_ERROR,
								ret);
							return;
						}
					} else {
						tr_connection_complete_tx(
							reactor, connection);
					}
				} else {
					tr_connection_complete_tx(reactor,
								  connection);
				}
			}
			continue;
		}

		if (n < 0 && errno == EINTR)
			continue;

		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			(void)__atomic_fetch_add(&connection->send_eagain,
						 UINT64_C(1), __ATOMIC_RELAXED);
			__atomic_store_n(&connection->tx_wait_writable, 1,
					 __ATOMIC_RELAXED);
			if (tr_connection_update_interest(reactor,
							  connection) != TR_OK)
				tr_connection_close_internal(
					reactor, connection,
					TR_CONN_EVENT_ERROR, TR_ERR_SYS);
			return;
		}

		tr_connection_close_internal(
			reactor, connection, TR_CONN_EVENT_ERROR,
			n == 0 ? TR_ERR_STATE : TR_ERR_SYS);
		return;
	}

	if (connection->state == TR_CONN_ACTIVE && connection->tx_head)
		tr_schedule_tx(reactor, connection);
}

static void tr_run_tx_ready(struct tr_reactor *reactor)
{
	uint32_t count = 0;

	while (reactor->tx_ready_head && count < TR_TX_READY_BATCH) {
		struct tr_connection *connection = reactor->tx_ready_head;

		reactor->tx_ready_head = connection->tx_ready_next;
		if (!reactor->tx_ready_head)
			reactor->tx_ready_tail = NULL;

		connection->tx_ready_next = NULL;
		tr_connection_flush_tx(reactor, connection);
		count++;
	}
}

static void tr_dispatch_frame(struct tr_reactor *reactor,
			      struct tr_connection *connection,
			      struct tr_frame *frame)
{
	enum tr_frame_disposition disposition = TR_FRAME_RELEASE;
	struct tr_conn_handle handle;

	handle.reactor = reactor;
	handle.slot = connection->slot;
	handle.generation = connection->generation;

	(void)__atomic_fetch_add(&connection->rx_frames, UINT64_C(1),
				 __ATOMIC_RELAXED);

	if (connection->frame_cb)
		disposition = connection->frame_cb(handle, frame,
						   connection->callback_arg);

	if (disposition != TR_FRAME_TAKE_OWNERSHIP)
		tr_frame_release(frame);
}

static void tr_connection_on_readable(struct tr_reactor *reactor,
				      struct tr_connection *connection)
{
	size_t budget = reactor->config.rx_budget_bytes;

	while (budget != 0 && connection->state == TR_CONN_ACTIVE) {
		struct tr_frame frame;
		size_t writable;
		void *dst;
		ssize_t n;
		int ret;

		ret = tr_parser_prepare(&connection->parser);
		if (ret == TR_AGAIN) {
			__atomic_store_n(&connection->rx_paused, 1,
					 __ATOMIC_RELAXED);
			(void)__atomic_fetch_add(&connection->rx_pauses_count,
						 UINT64_C(1), __ATOMIC_RELAXED);
			if (tr_connection_update_interest(reactor,
							  connection) != TR_OK)
				tr_connection_close_internal(
					reactor, connection,
					TR_CONN_EVENT_ERROR, TR_ERR_SYS);
			return;
		}
		if (ret != TR_OK) {
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR, ret);
			return;
		}

		writable = tr_parser_write_len(&connection->parser);
		dst = tr_parser_write_ptr(&connection->parser);
		if (!dst || writable == 0) {
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR,
						     TR_ERR_STATE);
			return;
		}

		if (writable > budget)
			writable = budget;

		n = recv(connection->fd, dst, writable, 0);
		if (n > 0) {
			(void)__atomic_fetch_add(&connection->rx_bytes,
						 (uint64_t)n, __ATOMIC_RELAXED);
			__atomic_store_n(&connection->last_rx_activity_ns,
					 tr_reactor_now_ns(), __ATOMIC_RELAXED);
			tr_frame_init(&frame);
			budget -= (size_t)n;

			ret = tr_parser_produce(&connection->parser, (size_t)n,
						&frame);
			if (ret == TR_FRAME_READY) {
				tr_dispatch_frame(reactor, connection, &frame);
				continue;
			}
			if (ret == TR_AGAIN) {
				__atomic_store_n(&connection->rx_paused, 1,
						 __ATOMIC_RELAXED);
				(void)__atomic_fetch_add(
					&connection->rx_pauses_count,
					UINT64_C(1), __ATOMIC_RELAXED);
				if (tr_connection_update_interest(
					    reactor, connection) != TR_OK)
					tr_connection_close_internal(
						reactor, connection,
						TR_CONN_EVENT_ERROR,
						TR_ERR_SYS);
				return;
			}
			if (ret != TR_OK) {
				tr_connection_close_internal(
					reactor, connection,
					TR_CONN_EVENT_ERROR, ret);
				return;
			}
			continue;
		}

		if (n == 0) {
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_CLOSED,
						     TR_OK);
			return;
		}

		if (errno == EINTR)
			continue;

		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			(void)__atomic_fetch_add(&connection->recv_eagain,
						 UINT64_C(1), __ATOMIC_RELAXED);
			return;
		}

		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
		return;
	}
}

static void tr_connection_on_writable(struct tr_reactor *reactor,
				      struct tr_connection *connection)
{
	if (connection->state != TR_CONN_ACTIVE ||
	    !__atomic_load_n(&connection->tx_wait_writable, __ATOMIC_RELAXED))
		return;

	__atomic_store_n(&connection->tx_wait_writable, 0, __ATOMIC_RELAXED);
	if (tr_connection_update_interest(reactor, connection) != TR_OK) {
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
		return;
	}

	tr_schedule_tx(reactor, connection);
}

static void tr_reactor_drain_wake(struct tr_reactor *reactor)
{
	uint64_t value;

	for (;;) {
		ssize_t n = read(reactor->wake_fd, &value, sizeof(value));

		if (n == (ssize_t)sizeof(value))
			continue;
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			return;
		return;
	}
}

static struct tr_connection *tr_lookup_connection(struct tr_reactor *reactor,
						  uint32_t slot,
						  uint32_t generation)
{
	struct tr_connection *connection;

	if (slot >= reactor->config.max_connections)
		return NULL;

	connection = &reactor->connections[slot];
	if (connection->state != TR_CONN_ACTIVE ||
	    connection->generation != generation)
		return NULL;

	return connection;
}

static void tr_process_send(struct tr_reactor *reactor,
			    const struct tr_command *command)
{
	struct tr_connection *connection;
	struct tr_tx_item *item = command->u.send.item;

	connection = tr_lookup_connection(reactor, command->slot,
					  command->generation);
	if (!connection) {
		{
			uint32_t i;
			for (i = 0; i < item->payload_count; ++i)
				if (item->payloads[i])
					tr_buffer_release(item->payloads[i]);
		}
		tr_tx_pool_release(item->owner_pool, item);
		return;
	}

	item->next = NULL;
	if (connection->tx_tail)
		connection->tx_tail->next = item;
	else
		connection->tx_head = item;
	connection->tx_tail = item;
	(void)__atomic_fetch_add(&connection->tx_queued_items, 1U,
				 __ATOMIC_RELAXED);

	tr_schedule_tx(reactor, connection);
}

static void tr_process_resume_rx(struct tr_reactor *reactor,
				 const struct tr_command *command)
{
	struct tr_connection *connection;
	int ret;

	connection = tr_lookup_connection(reactor, command->slot,
					  command->generation);
	if (!connection ||
	    !__atomic_load_n(&connection->rx_paused, __ATOMIC_RELAXED))
		return;

	ret = tr_parser_prepare(&connection->parser);
	if (ret == TR_AGAIN)
		return;
	if (ret != TR_OK) {
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR, ret);
		return;
	}

	__atomic_store_n(&connection->rx_paused, 0, __ATOMIC_RELAXED);
	if (tr_connection_update_interest(reactor, connection) != TR_OK)
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
}

static void tr_process_close(struct tr_reactor *reactor,
			     const struct tr_command *command)
{
	struct tr_connection *connection;

	connection = tr_lookup_connection(reactor, command->slot,
					  command->generation);
	if (connection)
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_CLOSED, TR_OK);
}

static void tr_process_abort(struct tr_reactor *reactor,
			     const struct tr_command *command)
{
	struct tr_connection *connection;

	connection = tr_lookup_connection(reactor, command->slot,
					  command->generation);
	if (connection)
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR,
					     command->u.abort.status);
}

static void tr_process_set_handler(struct tr_reactor *reactor,
					   const struct tr_command *command)
{
	struct tr_reactor_handler_request *request = command->u.handler.request;
	struct tr_connection *connection;
	int status = TR_ERR_STALE;

	if (!request)
		return;

	connection = tr_lookup_connection(reactor, command->slot,
					  command->generation);
	if (connection) {
		connection->frame_cb = request->frame_cb;
		connection->event_cb = request->event_cb;
		connection->callback_arg = request->callback_arg;
		status = TR_OK;
	}

	tr_reactor_sync_complete(&request->sync, status);
}

static void tr_process_quiesce(const struct tr_command *command)
{
	tr_reactor_sync_complete(command->u.quiesce.sync, TR_OK);
}

static void tr_process_commands(struct tr_reactor *reactor)
{
	struct tr_command commands[TR_COMMAND_BATCH];

	for (;;) {
		size_t count;
		size_t i;

		count = tr_command_queue_pop_batch(&reactor->commands, commands,
						   TR_COMMAND_BATCH);
		if (count == 0)
			return;

		for (i = 0; i < count; ++i) {
			const struct tr_command *command = &commands[i];

			switch (command->type) {
			case TR_CMD_ADOPT_FD:
				(void)tr_connection_adopt(reactor,
							  command->slot,
							  command->generation,
							  command->u.adopt.fd);
				break;
			case TR_CMD_SEND:
				tr_process_send(reactor, command);
				break;
			case TR_CMD_RESUME_RX:
				tr_process_resume_rx(reactor, command);
				break;
			case TR_CMD_CLOSE:
				tr_process_close(reactor, command);
				break;
			case TR_CMD_ABORT:
				tr_process_abort(reactor, command);
				break;
			case TR_CMD_SET_HANDLER:
				tr_process_set_handler(reactor, command);
				break;
			case TR_CMD_QUIESCE:
				tr_process_quiesce(command);
				break;
			case TR_CMD_STOP:
				reactor->stopping = 1;
				break;
			default:
				break;
			}
		}

		if (reactor->stopping)
			return;
	}
}

static void tr_handle_connection_event(struct tr_reactor *reactor,
				       uint64_t token, uint32_t events)
{
	struct tr_connection *connection;
	uint32_t slot;
	uint32_t generation;

	tr_conn_token_decode(token, &slot, &generation);
	connection = tr_lookup_connection(reactor, slot, generation);
	if (!connection)
		return;

	if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP))
		tr_connection_on_readable(reactor, connection);

	if (connection->state != TR_CONN_ACTIVE)
		return;

	if (events & EPOLLOUT)
		tr_connection_on_writable(reactor, connection);

	if (connection->state != TR_CONN_ACTIVE)
		return;

	if (events & EPOLLERR) {
		tr_connection_close_internal(reactor, connection,
					     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
	}
}

static void tr_cleanup_connections(struct tr_reactor *reactor)
{
	uint32_t i;

	for (i = 0; i < reactor->config.max_connections; ++i) {
		if (reactor->connections[i].state == TR_CONN_ACTIVE)
			tr_connection_close_internal(reactor,
						     &reactor->connections[i],
						     TR_CONN_EVENT_CLOSED,
						     TR_OK);
	}
}

static void *tr_reactor_thread_main(void *arg)
{
	struct tr_reactor *reactor = (struct tr_reactor *)arg;
	struct epoll_event events[TR_REACTOR_EVENT_BATCH];

	while (!reactor->stopping) {
		int timeout = reactor->tx_ready_head ? 0 : -1;
		int count;
		int i;

		tr_process_commands(reactor);
		tr_run_tx_ready(reactor);
		if (reactor->stopping)
			break;

		timeout = reactor->tx_ready_head ? 0 : -1;
		count = epoll_wait(reactor->epoll_fd, events,
				   (int)TR_REACTOR_EVENT_BATCH, timeout);
		if (count < 0) {
			if (errno == EINTR)
				continue;
			break;
		}

		for (i = 0; i < count; ++i) {
			if (events[i].data.u64 == TR_WAKE_TOKEN) {
				tr_reactor_drain_wake(reactor);
				tr_process_commands(reactor);
			} else {
				tr_handle_connection_event(reactor,
							   events[i].data.u64,
							   events[i].events);
			}

			if (reactor->stopping)
				break;
		}

		tr_run_tx_ready(reactor);
	}

	tr_cleanup_connections(reactor);
	return NULL;
}

static void tr_default_config(struct tr_reactor_config *config)
{
	config->max_connections = tr_nonzero(config->max_connections, 128U);
	config->command_capacity = tr_nonzero(config->command_capacity, 1024U);
	config->tx_item_capacity = tr_nonzero(config->tx_item_capacity, 1024U);
	config->control_tx_item_capacity =
		tr_nonzero(config->control_tx_item_capacity, 64U);
	config->rx_buffer_count = tr_nonzero(config->rx_buffer_count, 128U);
	config->rx_buffer_size =
		tr_nonzero(config->rx_buffer_size, TR_WIRE_DEFAULT_MAX_PAYLOAD);
	config->max_payload_len = tr_nonzero(config->max_payload_len,
					     TR_WIRE_DEFAULT_MAX_PAYLOAD);
	config->rx_budget_bytes =
		tr_nonzero(config->rx_budget_bytes, 4U * 1024U * 1024U);
	config->tx_budget_bytes =
		tr_nonzero(config->tx_budget_bytes, 4U * 1024U * 1024U);
}

struct tr_reactor_build {
	struct tr_reactor *reactor;
	int ctl_lock_ready;
	int commands_ready;
	int tx_pool_ready;
	int control_tx_pool_ready;
	int rx_pool_ready;
};

static void tr_reactor_build_cleanup(struct tr_reactor_build *build)
{
	struct tr_reactor *reactor;

	if (!build || !build->reactor)
		return;
	reactor = build->reactor;

	if (reactor->wake_fd >= 0)
		close(reactor->wake_fd);
	if (reactor->epoll_fd >= 0)
		close(reactor->epoll_fd);

	if (build->rx_pool_ready)
		tr_buffer_pool_destroy(&reactor->rx_pool);
	if (build->control_tx_pool_ready)
		tr_tx_pool_destroy(&reactor->control_tx_pool);
	if (build->tx_pool_ready)
		tr_tx_pool_destroy(&reactor->tx_pool);
	if (build->commands_ready)
		tr_command_queue_destroy(&reactor->commands);

	free(reactor->connections);
	free(reactor->slots);

	if (build->ctl_lock_ready)
		pthread_mutex_destroy(&reactor->ctl_lock);
	free(reactor);
	build->reactor = NULL;
}

int tr_reactor_create(const struct tr_reactor_config *config,
		      tr_reactor_frame_cb frame_cb,
		      tr_reactor_event_cb event_cb, void *callback_arg,
		      struct tr_reactor **out)
{
	struct tr_reactor_build build TR_AUTO(tr_reactor_build_cleanup) = { 0 };
	struct tr_reactor *reactor;
	struct epoll_event wake_event;
	uint32_t i;
	int ret;

	if (!out)
		return TR_ERR_INVALID;

	*out = NULL;
	reactor = (struct tr_reactor *)calloc(1, sizeof(*reactor));
	if (!reactor)
		return TR_ERR_NOMEM;
	build.reactor = reactor;
	reactor->epoll_fd = -1;
	reactor->wake_fd = -1;

	if (config)
		reactor->config = *config;
	tr_default_config(&reactor->config);

	if (reactor->config.max_payload_len > reactor->config.rx_buffer_size)
		return TR_ERR_BAD_LENGTH;

	reactor->frame_cb = frame_cb;
	reactor->event_cb = event_cb;
	reactor->callback_arg = callback_arg;

	if (pthread_mutex_init(&reactor->ctl_lock, NULL) != 0)
		return TR_ERR_INVALID;
	build.ctl_lock_ready = 1;

	reactor->slots = (struct tr_slot *)calloc(
		reactor->config.max_connections, sizeof(*reactor->slots));
	reactor->connections = (struct tr_connection *)calloc(
		reactor->config.max_connections, sizeof(*reactor->connections));
	if (!reactor->slots || !reactor->connections)
		return TR_ERR_NOMEM;

	for (i = 0; i < reactor->config.max_connections; ++i) {
		atomic_init(&reactor->slots[i].meta,
			    tr_slot_meta_make(0U, TR_CONN_FREE));
		reactor->connections[i].fd = -1;
		reactor->connections[i].state = TR_CONN_FREE;
	}

	ret = tr_command_queue_init(&reactor->commands,
				    reactor->config.command_capacity);
	if (ret != TR_OK)
		return ret;
	build.commands_ready = 1;

	ret = tr_tx_pool_init(&reactor->tx_pool,
			      reactor->config.tx_item_capacity);
	if (ret != TR_OK)
		return ret;
	build.tx_pool_ready = 1;

	ret = tr_tx_pool_init(&reactor->control_tx_pool,
			      reactor->config.control_tx_item_capacity);
	if (ret != TR_OK)
		return ret;
	build.control_tx_pool_ready = 1;

	ret = tr_buffer_pool_init(&reactor->rx_pool,
				  reactor->config.rx_buffer_count,
				  reactor->config.rx_buffer_size);
	if (ret != TR_OK)
		return ret;
	build.rx_pool_ready = 1;

	reactor->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (reactor->epoll_fd < 0)
		return TR_ERR_SYS;

	reactor->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (reactor->wake_fd < 0)
		return TR_ERR_SYS;

	memset(&wake_event, 0, sizeof(wake_event));
	wake_event.events = EPOLLIN;
	wake_event.data.u64 = TR_WAKE_TOKEN;
	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, reactor->wake_fd,
		      &wake_event) < 0)
		return TR_ERR_SYS;

	reactor->accepting = 1;
	*out = reactor;
	build.reactor = NULL;
	return TR_OK;
}

int tr_reactor_start(struct tr_reactor *reactor)
{
	int error;

	if (!reactor)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (reactor->started) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_STATE;
	}

	error = pthread_create(&reactor->thread, NULL, tr_reactor_thread_main,
			       reactor);
	if (error != 0) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_SYS;
	}

	reactor->started = 1;
	pthread_mutex_unlock(&reactor->ctl_lock);
	return TR_OK;
}

int tr_reactor_adopt_fd(struct tr_reactor *reactor, int fd,
			struct tr_conn_handle *out)
{
	struct tr_command command;
	uint32_t slot;
	uint32_t generation;
	int ret;

	if (!reactor || fd < 0 || !out)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started || !reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_CLOSED;
	}

	/*
	 * ctl_lock 只串行化 producer 控制面；slot capability 本身使用 atomic
	 * generation+state 发布，Reactor event loop 不需要参与这把锁。
	 */
	ret = tr_slot_reserve(reactor, &slot, &generation);
	if (ret != TR_OK) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_ADOPT_FD;
	command.slot = slot;
	command.generation = generation;
	command.u.adopt.fd = fd;

	ret = tr_reactor_push_locked(reactor, &command);
	if (ret != TR_OK) {
		(void)tr_slot_set_state(reactor, slot, generation, TR_CONN_FREE);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}

	out->reactor = reactor;
	out->slot = slot;
	out->generation = generation;

	pthread_mutex_unlock(&reactor->ctl_lock);
	return TR_OK;
}

int tr_reactor_sendv_limited(struct tr_conn_handle connection, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id,
			     struct tr_buffer *const *payloads,
			     uint32_t payload_count,
			     uint32_t max_frame_payload_len)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_tx_item *item;
	struct tr_command command;
	uint64_t payload_len64 = 0;
	uint32_t i;
	int ret;

	if (!reactor || !tr_frame_type_valid(type) ||
	    (flags & ~TR_FRAME_F_KNOWN_MASK) ||
	    payload_count > TR_REACTOR_MAX_TX_SLICES ||
	    (payload_count != 0 && !payloads) || max_frame_payload_len == 0 ||
	    max_frame_payload_len > reactor->config.max_payload_len)
		return TR_ERR_INVALID;

	for (i = 0; i < payload_count; ++i) {
		uint32_t j;
		struct tr_buffer *payload = payloads[i];

		if (!payload || (payload->len != 0 && !payload->data))
			return TR_ERR_INVALID;
		for (j = 0; j < i; ++j)
			if (payloads[j] == payload)
				return TR_ERR_INVALID;
		if (UINT32_MAX - payload_len64 < payload->len)
			return TR_ERR_BAD_LENGTH;
		payload_len64 += payload->len;
	}
	if (type != TR_FRAME_DATA && payload_len64 > max_frame_payload_len)
		return TR_ERR_BAD_LENGTH;

	item = tr_tx_pool_acquire(type == TR_FRAME_DATA ?
					  &reactor->tx_pool :
					  &reactor->control_tx_pool);
	if (!item)
		return TR_AGAIN;

	for (i = 0; i < payload_count; ++i)
		item->payloads[i] = payloads[i];
	item->payload_count = payload_count;
	item->type = type;
	item->base_flags = flags;
	item->stream_id = stream_id;
	item->message_id = message_id;
	item->message_len = payload_len64;
	item->message_pos = 0;
	item->max_frame_payload_len = max_frame_payload_len;

	ret = tr_tx_prepare_frame(reactor, item);
	if (ret != TR_OK) {
		memset(item->payloads, 0, sizeof(item->payloads));
		item->payload_count = 0;
		tr_tx_pool_release(item->owner_pool, item);
		return ret;
	}

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_SEND;
	command.slot = connection.slot;
	command.generation = connection.generation;
	command.u.send.item = item;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started || !reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		memset(item->payloads, 0, sizeof(item->payloads));
		item->payload_count = 0;
		tr_tx_pool_release(item->owner_pool, item);
		return TR_ERR_CLOSED;
	}

	if (!tr_slot_live(reactor, connection.slot, connection.generation)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		memset(item->payloads, 0, sizeof(item->payloads));
		item->payload_count = 0;
		tr_tx_pool_release(item->owner_pool, item);
		return TR_ERR_STALE;
	}

	ret = tr_reactor_push_locked(reactor, &command);
	pthread_mutex_unlock(&reactor->ctl_lock);

	if (ret != TR_OK) {
		memset(item->payloads, 0, sizeof(item->payloads));
		item->payload_count = 0;
		tr_tx_pool_release(item->owner_pool, item);
	}

	return ret;
}

int tr_reactor_sendv(struct tr_conn_handle connection, uint16_t type,
		     uint32_t flags, uint32_t stream_id, uint64_t message_id,
		     struct tr_buffer *const *payloads, uint32_t payload_count)
{
	struct tr_reactor *reactor = connection.reactor;

	if (!reactor)
		return TR_ERR_INVALID;
	return tr_reactor_sendv_limited(connection, type, flags, stream_id,
					message_id, payloads, payload_count,
					reactor->config.max_payload_len);
}

int tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
		    uint32_t flags, uint32_t stream_id, uint64_t message_id,
		    struct tr_buffer *payload)
{
	struct tr_buffer *payloads[1];

	if (!payload)
		return tr_reactor_sendv(connection, type, flags, stream_id,
					message_id, NULL, 0);

	payloads[0] = payload;
	return tr_reactor_sendv(connection, type, flags, stream_id, message_id,
				payloads, 1);
}

static int tr_reactor_simple_command(struct tr_conn_handle connection,
				     uint16_t type)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_command command;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;

	memset(&command, 0, sizeof(command));
	command.type = type;
	command.slot = connection.slot;
	command.generation = connection.generation;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started || !reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_CLOSED;
	}

	if (!tr_slot_live(reactor, connection.slot, connection.generation)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_STALE;
	}

	ret = tr_reactor_push_locked(reactor, &command);
	pthread_mutex_unlock(&reactor->ctl_lock);
	return ret;
}

int tr_reactor_set_handler(struct tr_conn_handle connection,
			   tr_reactor_frame_cb frame_cb,
			   tr_reactor_event_cb event_cb, void *callback_arg)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_reactor_handler_request request;
	struct tr_command command;
	int ret;

	if (!reactor || connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;

	/*
	 * 已经位于 Reactor owner thread 时直接修改 connection 状态，
	 * 避免同步 command 对自己形成自等待。
	 */
	if (reactor->started && pthread_equal(pthread_self(), reactor->thread)) {
		struct tr_connection *conn =
			tr_lookup_connection(reactor, connection.slot,
					     connection.generation);
		if (!conn)
			return TR_ERR_STALE;
		conn->frame_cb = frame_cb;
		conn->event_cb = event_cb;
		conn->callback_arg = callback_arg;
		return TR_OK;
	}

	memset(&request, 0, sizeof(request));
	ret = tr_reactor_sync_init(&request.sync);
	if (ret != TR_OK)
		return ret;
	request.frame_cb = frame_cb;
	request.event_cb = event_cb;
	request.callback_arg = callback_arg;

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_SET_HANDLER;
	command.slot = connection.slot;
	command.generation = connection.generation;
	command.u.handler.request = &request;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started || !reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		tr_reactor_sync_destroy(&request.sync);
		return TR_ERR_CLOSED;
	}
	if (!tr_slot_live(reactor, connection.slot, connection.generation)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		tr_reactor_sync_destroy(&request.sync);
		return TR_ERR_STALE;
	}

	do {
		ret = tr_reactor_push_locked(reactor, &command);
		if (ret == TR_AGAIN) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			sched_yield();
			pthread_mutex_lock(&reactor->ctl_lock);
			if (!reactor->started || !reactor->accepting) {
				ret = TR_ERR_CLOSED;
				break;
			}
		}
	} while (ret == TR_AGAIN);
	pthread_mutex_unlock(&reactor->ctl_lock);

	if (ret == TR_OK)
		ret = tr_reactor_sync_wait(&request.sync);
	tr_reactor_sync_destroy(&request.sync);
	return ret;
}

int tr_reactor_quiesce(struct tr_reactor *reactor)
{
	struct tr_reactor_sync sync;
	struct tr_command command;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;

	memset(&sync, 0, sizeof(sync));
	if (pthread_mutex_init(&sync.lock, NULL) != 0)
		return TR_ERR_SYS;
	if (pthread_cond_init(&sync.cond, NULL) != 0) {
		pthread_mutex_destroy(&sync.lock);
		return TR_ERR_SYS;
	}

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_QUIESCE;
	command.u.quiesce.sync = &sync;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		pthread_cond_destroy(&sync.cond);
		pthread_mutex_destroy(&sync.lock);
		return TR_OK;
	}
	if (pthread_equal(pthread_self(), reactor->thread)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		pthread_cond_destroy(&sync.cond);
		pthread_mutex_destroy(&sync.lock);
		return TR_ERR_STATE;
	}
	if (!reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		pthread_cond_destroy(&sync.cond);
		pthread_mutex_destroy(&sync.lock);
		return TR_ERR_CLOSED;
	}

	do {
		ret = tr_reactor_push_locked(reactor, &command);
		if (ret == TR_AGAIN) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			sched_yield();
			pthread_mutex_lock(&reactor->ctl_lock);
			if (!reactor->started || !reactor->accepting) {
				ret = TR_ERR_CLOSED;
				break;
			}
		}
	} while (ret == TR_AGAIN);
	pthread_mutex_unlock(&reactor->ctl_lock);

	if (ret == TR_OK) {
		pthread_mutex_lock(&sync.lock);
		while (!sync.done)
			pthread_cond_wait(&sync.cond, &sync.lock);
		pthread_mutex_unlock(&sync.lock);
	}

	pthread_cond_destroy(&sync.cond);
	pthread_mutex_destroy(&sync.lock);
	return ret;
}

int tr_reactor_get_limits(struct tr_reactor *reactor,
			  struct tr_reactor_limits *out)
{
	if (!reactor || !out)
		return TR_ERR_INVALID;

	out->max_payload_len = reactor->config.max_payload_len;
	return TR_OK;
}

int tr_reactor_get_connection_state(struct tr_conn_handle connection,
				    enum tr_connection_state *out)
{
	struct tr_reactor *reactor = connection.reactor;

	if (!reactor || !out ||
	    connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&reactor->slot_lock);
	if (reactor->slots[connection.slot].generation !=
	    connection.generation) {
		pthread_mutex_unlock(&reactor->slot_lock);
		return TR_ERR_STALE;
	}
	*out = reactor->slots[connection.slot].state;
	pthread_mutex_unlock(&reactor->slot_lock);
	return TR_OK;
}

int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_connection *conn;
	enum tr_connection_state state;

	if (!reactor || !out ||
	    connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&reactor->slot_lock);
	if (reactor->slots[connection.slot].generation !=
	    connection.generation) {
		pthread_mutex_unlock(&reactor->slot_lock);
		return TR_ERR_STALE;
	}
	state = reactor->slots[connection.slot].state;
	pthread_mutex_unlock(&reactor->slot_lock);

	conn = &reactor->connections[connection.slot];
	memset(out, 0, sizeof(*out));
	out->state = state;
	out->rx_bytes = __atomic_load_n(&conn->rx_bytes, __ATOMIC_RELAXED);
	out->tx_bytes = __atomic_load_n(&conn->tx_bytes, __ATOMIC_RELAXED);
	out->rx_frames = __atomic_load_n(&conn->rx_frames, __ATOMIC_RELAXED);
	out->tx_frames = __atomic_load_n(&conn->tx_frames, __ATOMIC_RELAXED);
	out->recv_eagain =
		__atomic_load_n(&conn->recv_eagain, __ATOMIC_RELAXED);
	out->send_eagain =
		__atomic_load_n(&conn->send_eagain, __ATOMIC_RELAXED);
	out->rx_pauses =
		__atomic_load_n(&conn->rx_pauses_count, __ATOMIC_RELAXED);
	out->last_rx_activity_ns =
		__atomic_load_n(&conn->last_rx_activity_ns, __ATOMIC_RELAXED);
	out->last_tx_activity_ns =
		__atomic_load_n(&conn->last_tx_activity_ns, __ATOMIC_RELAXED);
	out->tx_queued_items =
		__atomic_load_n(&conn->tx_queued_items, __ATOMIC_RELAXED);
	out->rx_paused = __atomic_load_n(&conn->rx_paused, __ATOMIC_RELAXED);
	out->tx_wait_writable =
		__atomic_load_n(&conn->tx_wait_writable, __ATOMIC_RELAXED);
	return TR_OK;
}

int tr_reactor_resume_rx(struct tr_conn_handle connection)
{
	return tr_reactor_simple_command(connection, TR_CMD_RESUME_RX);
}

int tr_reactor_close(struct tr_conn_handle connection)
{
	return tr_reactor_simple_command(connection, TR_CMD_CLOSE);
}

int tr_reactor_abort(struct tr_conn_handle connection, int status)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_command command;
	int ret;

	if (!reactor || status >= 0)
		return TR_ERR_INVALID;

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_ABORT;
	command.slot = connection.slot;
	command.generation = connection.generation;
	command.u.abort.status = status;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started || !reactor->accepting) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_CLOSED;
	}
	if (!tr_slot_live(reactor, connection.slot, connection.generation)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_STALE;
	}
	ret = tr_reactor_push_locked(reactor, &command);
	pthread_mutex_unlock(&reactor->ctl_lock);
	return ret;
}

int tr_reactor_stop(struct tr_reactor *reactor)
{
	struct tr_command command;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_OK;
	}

	reactor->accepting = 0;
	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_STOP;

	do {
		ret = tr_reactor_push_locked(reactor, &command);
		if (ret == TR_AGAIN) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			sched_yield();
			pthread_mutex_lock(&reactor->ctl_lock);
		}
	} while (ret == TR_AGAIN);

	pthread_mutex_unlock(&reactor->ctl_lock);

	if (ret != TR_OK)
		return ret;

	pthread_join(reactor->thread, NULL);

	pthread_mutex_lock(&reactor->ctl_lock);
	reactor->started = 0;
	pthread_mutex_unlock(&reactor->ctl_lock);
	return TR_OK;
}

void tr_reactor_destroy(struct tr_reactor *reactor)
{
	if (!reactor)
		return;

	if (reactor->started)
		(void)tr_reactor_stop(reactor);

	if (reactor->wake_fd >= 0)
		close(reactor->wake_fd);
	if (reactor->epoll_fd >= 0)
		close(reactor->epoll_fd);

	tr_buffer_pool_destroy(&reactor->rx_pool);
	tr_tx_pool_destroy(&reactor->control_tx_pool);
	tr_tx_pool_destroy(&reactor->tx_pool);
	tr_command_queue_destroy(&reactor->commands);

	free(reactor->connections);
	free(reactor->slots);

	pthread_mutex_destroy(&reactor->ctl_lock);
	free(reactor);
}
