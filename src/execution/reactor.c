#define _GNU_SOURCE
#include "reactor.h"

#include "reactor_internal.h"
#include "completion_queue.h"
#include "timer_queue.h"
#include "command_queue.h"
#include "../crc32c.h"
#include "../transport/protocol/parser.h"
#include "../io/socket.h"
#include "tr/status.h"
#include "../transport/protocol/wire.h"
#include "../memory_budget.h"
#include "../observability_internal.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
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
/* 每个事件循环轮次只处理一个命令批次，不持续排空到队列为空。 */
#define TR_COMMAND_BATCH 64U
#define TR_COMPLETION_BATCH 64U
#define TR_TIMER_BATCH 64U
#define TR_TX_READY_BATCH 64U
#define TR_RX_READY_BATCH 64U
#define TR_IO_QUANTUM (64U * 1024U)
#define TR_REACTOR_MAX_PREFACE_BYTES 256U
/* 限制最老 DATA 帧被优先级越过的次数，该限制跨事件循环轮次保持。 */
#define TR_CONTROL_BURST 8U
#define TR_WAKE_TOKEN UINT64_MAX
#define TR_LISTENER_TOKEN (UINT64_MAX - UINT64_C(1))
#define TR_PEER_EVENT_TOKEN (UINT64_MAX - UINT64_C(2))
#define TR_REACTOR_AUX_EVENT_CAPACITY 16U

/*
 * 每个 Reactor owner thread 只登记自己当前执行的 Reactor。
 * 外部线程不会写该 TLS，因此 owner 校验不需要 mutex/atomic。
 */
static _Thread_local struct tr_reactor *tr_current_reactor_owner;

static int tr_reactor_is_owner_thread(const struct tr_reactor *reactor)
{
	return reactor && tr_current_reactor_owner == reactor;
}

int tr_reactor_in_owner_context(void)
{
	return tr_current_reactor_owner != NULL;
}

#ifndef NDEBUG
#define TR_ASSERT_REACTOR_OWNER(reactor) \
	assert(tr_reactor_is_owner_thread((reactor)))
#else
#define TR_ASSERT_REACTOR_OWNER(reactor) ((void)(reactor))
#endif

/* 每个外层循环只创建一个实例，供所有分发入口共享。 */
struct tr_reactor_turn {
	struct tr_reactor_work left;
	uint64_t timer_lateness_ns;
	uint64_t start_ns;
	uint64_t poll_start_ns;
	uint64_t poll_end_ns;
	int poll_timeout;
	int polled;
};

struct tr_tx_pool;

struct tr_tx_item {
	struct tr_tx_item *next;
	struct tr_tx_item *prev;
	/* 全部非 DATA 项的 FIFO，包括顺序屏障。 */
	struct tr_tx_item *control_next;
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
	uint32_t peak_in_use;
	uint64_t exhausted_events;
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

	uint32_t preface_remaining;
	tr_reactor_preface_feed_cb preface_feed;
	tr_reactor_preface_release_cb preface_release;
	void *preface_arg;

	struct tr_parser parser;

	struct tr_tx_item *tx_head;
	struct tr_tx_item *tx_tail;
	struct tr_tx_item *tx_control_head;
	struct tr_tx_item *tx_control_tail;
	struct tr_tx_item *tx_active;
	uint32_t tx_control_streak;

	uint32_t epoll_events;
	int tx_wait_writable;
	int tx_scheduled;
	int rx_paused;
	int rx_scheduled;
	int rx_error_pending;

	struct tr_connection *tx_ready_next;
	struct tr_connection *rx_ready_next;

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

struct tr_aux_event_source {
	int fd;
	uint16_t generation;
	int used;
	tr_reactor_aux_event_cb callback;
	void *arg;
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
	uint64_t memory_bytes;

	int epoll_fd;
	int wake_fd;
	int listener_fd;
	tr_reactor_listener_cb listener_cb;
	void *listener_arg;
	int peer_event_fd;
	tr_reactor_peer_event_cb peer_event_cb;
	void *peer_event_arg;
	struct tr_aux_event_source aux_events[TR_REACTOR_AUX_EVENT_CAPACITY];
	pthread_t thread;

	pthread_mutex_t ctl_lock;
	int started;
	_Atomic int accepting;
	int stopping;
	/* 仅所有者访问：已弹出的命令批次中仍可能存在当前工作的 FIFO 前驱。 */
	int command_dispatching;

	struct tr_command_queue commands;
	struct tr_completion_queue completions;
	struct tr_timer_queue timers;
	struct tr_tx_pool tx_pool;
	struct tr_tx_pool control_tx_pool;
	struct tr_buffer_pool rx_pool;

	struct tr_slot *slots;
	struct tr_connection *connections;

	struct tr_connection *tx_ready_head;
	struct tr_connection *tx_ready_tail;
	struct tr_connection *rx_ready_head;
	struct tr_connection *rx_ready_tail;

	/* 仅所有者写入；外部快照通过所有者调用串行化读取。 */
	struct tr_reactor_stats stats;

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

/*
 * connection generation 永远跳过 UINT32_MAX，因此高 32 位全 1 可作为
 * Reactor 内部 auxiliary event token 的独立 namespace。
 *
 * low 32 位：高 16 位是 aux generation，低 16 位是 slot。
 */
static uint64_t tr_aux_event_token(uint32_t slot, uint16_t generation)
{
	return (UINT64_C(0xffffffff) << 32) |
	       ((uint64_t)generation << 16) | (uint64_t)slot;
}

static int tr_aux_event_token_decode(uint64_t token, uint32_t *slot,
				     uint16_t *generation)
{
	uint32_t low;

	if ((uint32_t)(token >> 32) != UINT32_MAX)
		return 0;
	low = (uint32_t)token;
	*slot = low & UINT32_C(0xffff);
	*generation = (uint16_t)(low >> 16);
	return *slot < TR_REACTOR_AUX_EVENT_CAPACITY &&
	       *generation != 0U && *generation != UINT16_MAX;
}

static uint16_t tr_aux_event_next_generation(uint16_t generation)
{
	generation++;
	if (generation == 0U || generation == UINT16_MAX)
		generation = 1U;
	return generation;
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
	pool->peak_in_use = 0;
	pool->exhausted_events = 0;
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
		tr_observe_high_water_u32(&pool->peak_in_use,
					  pool->capacity - pool->free_count);
	} else {
		pool->exhausted_events++;
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

static int tr_reactor_push_command_wait(
	struct tr_reactor *reactor, const struct tr_command *command,
	uint64_t expected_generation)
{
	int need_wake = 0;
	int ret;

	ret = tr_command_queue_push_wait(
		&reactor->commands, command, expected_generation, &need_wake);
	if (ret != TR_OK)
		return ret;
	if (need_wake)
		(void)tr_reactor_signal(reactor);
	return TR_OK;
}

static int tr_reactor_push_command_wait_force(
	struct tr_reactor *reactor, const struct tr_command *command)
{
	int need_wake = 0;
	int ret;

	ret = tr_command_queue_push_wait_force(
		&reactor->commands, command, &need_wake);
	if (ret != TR_OK)
		return ret;
	if (need_wake)
		(void)tr_reactor_signal(reactor);
	return TR_OK;
}

static int tr_reactor_push_completion(
	struct tr_reactor *reactor, const struct tr_completion *completion)
{
	int need_wake = 0;
	int ret;

	ret = tr_completion_queue_push_wait(
		&reactor->completions, completion, &need_wake);
	if (ret != TR_OK)
		return ret;

	/*
	 * 与 command 相同：queue 已接受后 ownership 已转移，eventfd 的异常
	 * 不能把提交结果翻转成失败。
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
		if (generation == 0U || generation == UINT32_MAX)
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

	TR_ASSERT_REACTOR_OWNER(reactor);
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

static void tr_schedule_rx(struct tr_reactor *reactor,
			   struct tr_connection *connection)
{
	if (!connection || connection->state != TR_CONN_ACTIVE ||
	    __atomic_load_n(&connection->rx_paused, __ATOMIC_RELAXED) ||
	    connection->rx_scheduled)
		return;

	connection->rx_scheduled = 1;
	connection->rx_ready_next = NULL;

	if (reactor->rx_ready_tail)
		reactor->rx_ready_tail->rx_ready_next = connection;
	else
		reactor->rx_ready_head = connection;

	reactor->rx_ready_tail = connection;
}

static void tr_unschedule_rx(struct tr_reactor *reactor,
			     struct tr_connection *connection)
{
	struct tr_connection *prev = NULL;
	struct tr_connection *cur = reactor->rx_ready_head;

	if (!connection->rx_scheduled)
		return;

	while (cur) {
		if (cur == connection) {
			if (prev)
				prev->rx_ready_next = cur->rx_ready_next;
			else
				reactor->rx_ready_head = cur->rx_ready_next;

			if (reactor->rx_ready_tail == cur)
				reactor->rx_ready_tail = prev;

			cur->rx_ready_next = NULL;
			cur->rx_scheduled = 0;
			return;
		}

		prev = cur;
		cur = cur->rx_ready_next;
	}

	connection->rx_scheduled = 0;
	connection->rx_ready_next = NULL;
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
	connection->tx_control_head = NULL;
	connection->tx_control_tail = NULL;
	connection->tx_active = NULL;
	connection->tx_control_streak = 0;
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
	TR_ASSERT_REACTOR_OWNER(reactor);

	if (!connection || connection->state != TR_CONN_ACTIVE)
		return;

	connection->state = (event == TR_CONN_EVENT_ERROR) ? TR_CONN_ERROR :
							     TR_CONN_CLOSED;

	(void)epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, connection->fd, NULL);

	close(connection->fd);
	connection->fd = -1;

	tr_unschedule_tx(reactor, connection);
	tr_unschedule_rx(reactor, connection);
	connection->rx_error_pending = 0;
	if (connection->preface_release) {
		tr_reactor_preface_release_cb release =
			connection->preface_release;
		void *release_arg = connection->preface_arg;

		connection->preface_remaining = 0U;
		connection->preface_feed = NULL;
		connection->preface_release = NULL;
		connection->preface_arg = NULL;
		release(release_arg);
	}
	tr_parser_reset(&connection->parser);
	tr_release_tx_queue(reactor, connection);

	__atomic_store_n(&connection->tx_wait_writable, 0, __ATOMIC_RELAXED);
	__atomic_store_n(&connection->rx_paused, 0, __ATOMIC_RELAXED);

	tr_slot_set_state(reactor, connection->slot, connection->generation,
			  TR_CONN_FREE);

	tr_connection_notify(reactor, connection, event, status);
}

int tr_reactor_close_on_owner(struct tr_conn_handle connection)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_connection *conn;

	if (!reactor || connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;
	TR_ASSERT_REACTOR_OWNER(reactor);

	conn = &reactor->connections[connection.slot];
	if (conn->state != TR_CONN_ACTIVE ||
	    conn->generation != connection.generation)
		return TR_ERR_STALE;

	tr_connection_close_internal(
		reactor, conn, TR_CONN_EVENT_CLOSED, TR_OK);
	return TR_OK;
}

int tr_reactor_abort_on_owner(struct tr_conn_handle connection, int status)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_connection *conn;

	if (!reactor || status >= 0 ||
	    connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;
	TR_ASSERT_REACTOR_OWNER(reactor);

	conn = &reactor->connections[connection.slot];
	if (conn->state != TR_CONN_ACTIVE ||
	    conn->generation != connection.generation)
		return TR_ERR_STALE;

	tr_connection_close_internal(
		reactor, conn, TR_CONN_EVENT_ERROR, status);
	return TR_OK;
}

static int tr_connection_adopt(
	struct tr_reactor *reactor, uint32_t slot, uint32_t generation, int fd,
	const struct tr_reactor_preface_handler *preface)
{
	struct tr_connection *connection;

	TR_ASSERT_REACTOR_OWNER(reactor);
	struct epoll_event event;
	struct tr_wire_limits limits;
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
	if (preface) {
		if (preface->byte_count == 0U ||
		    preface->byte_count > TR_REACTOR_MAX_PREFACE_BYTES ||
		    !preface->feed) {
			tr_slot_set_state(reactor, slot, generation,
					  TR_CONN_FREE);
			return TR_ERR_INVALID;
		}
		connection->preface_remaining = preface->byte_count;
		connection->preface_feed = preface->feed;
		connection->preface_release = preface->release;
		connection->preface_arg = preface->arg;
	}
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

	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0) {
		tr_parser_reset(&connection->parser);
		connection->state = TR_CONN_ERROR;
		tr_slot_set_state(reactor, slot, generation, TR_CONN_FREE);
		return TR_ERR_SYS;
	}

	connection->fd = fd;
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

/* 只有这些仅头部帧可以越过 DATA，但绝不能越过其他控制项。 */
static int tr_tx_can_bypass_data(const struct tr_tx_item *item)
{
	if (item->message_len != 0 || item->base_flags != 0)
		return 0;
	if (item->type == TR_FRAME_WINDOW_UPDATE)
		return item->stream_id != 0;
	return (item->type == TR_FRAME_PING || item->type == TR_FRAME_PONG) &&
	       item->stream_id == 0;
}

static struct tr_tx_item *tr_connection_next_tx(struct tr_connection *connection)
{
	struct tr_tx_item *item = connection->tx_active;
	struct tr_tx_item *control = connection->tx_control_head;

	/* 已部分提交的帧必须跨预算让出和 EAGAIN 保持活动状态。 */
	if (item && item->wire_pos != 0)
		return item;

	item = connection->tx_head;
	if (item && item->type == TR_FRAME_DATA && control &&
	    connection->tx_control_streak < TR_CONTROL_BURST &&
	    tr_tx_can_bypass_data(control))
		item = control;
	connection->tx_active = item;
	return item;
}

static void tr_connection_complete_tx(struct tr_connection *connection,
				      struct tr_tx_item *item)
{
	/* 被选中的控制项可能位于 FIFO 中部，因此必须以 O(1) 删除。 */
	if (item->prev)
		item->prev->next = item->next;
	else
		connection->tx_head = item->next;
	if (item->next)
		item->next->prev = item->prev;
	else
		connection->tx_tail = item->prev;

	if (item->type != TR_FRAME_DATA) {
		assert(connection->tx_control_head == item);
		connection->tx_control_head = item->control_next;
		if (!connection->tx_control_head)
			connection->tx_control_tail = NULL;
	}
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
				   struct tr_connection *connection,
				   struct tr_reactor_turn *turn)
{
	size_t budget = turn->left.tx_bytes < TR_IO_QUANTUM ?
		(size_t)turn->left.tx_bytes : TR_IO_QUANTUM;

	connection->tx_scheduled = 0;
	connection->tx_ready_next = NULL;

	while (budget != 0 && connection->state == TR_CONN_ACTIVE &&
	       connection->tx_head) {
		struct tr_tx_item *item = tr_connection_next_tx(connection);
		struct iovec iov[1U + TR_REACTOR_MAX_TX_SLICES];
		struct msghdr message;
		ssize_t n;
		int iov_count;

		/* 只有真正选中后才准备续帧，不能提前于 CONTROL 准备。 */
		if (item->wire_len == 0) {
			int ret = tr_tx_prepare_frame(reactor, item);

			if (ret != TR_OK) {
				tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR, ret);
				return;
			}
		}
		iov_count = tr_tx_build_iov(item, iov);
		if (iov_count <= 0) {
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR,
						     TR_ERR_STATE);
			return;
		}

		/* 直接限制系统调用本身，包括帧头和分散/聚集分片。 */
		{
			size_t remaining = budget;
			int i;

			for (i = 0; i < iov_count; ++i) {
				if (iov[i].iov_len >= remaining) {
					iov[i].iov_len = remaining;
					iov_count = i + 1;
					break;
				}
				remaining -= iov[i].iov_len;
			}
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
			budget -= (size_t)n;
			turn->left.tx_bytes -= (uint64_t)n;

			if (item->wire_pos == item->wire_len) {
				(void)__atomic_fetch_add(&connection->tx_frames,
							 UINT64_C(1),
							 __ATOMIC_RELAXED);
				connection->tx_active = NULL;
				if (item->type == TR_FRAME_DATA) {
					connection->tx_control_streak = 0;
					item->message_pos += item->frame_payload_len;
					if (item->message_pos < item->message_len) {
						item->wire_pos = 0;
						item->wire_len = 0;
					} else {
						tr_connection_complete_tx(connection, item);
					}
				} else {
					if (tr_tx_can_bypass_data(item) &&
					    connection->tx_control_streak < TR_CONTROL_BURST)
						connection->tx_control_streak++;
					tr_connection_complete_tx(connection, item);
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

static void tr_run_tx_ready(struct tr_reactor *reactor,
			    struct tr_reactor_turn *turn)
{
	while (reactor->tx_ready_head && turn->left.tx_dispatches != 0 &&
	       turn->left.tx_bytes != 0) {
		struct tr_connection *connection = reactor->tx_ready_head;

		reactor->tx_ready_head = connection->tx_ready_next;
		if (!reactor->tx_ready_head)
			reactor->tx_ready_tail = NULL;

		connection->tx_ready_next = NULL;
		turn->left.tx_dispatches--;
		tr_connection_flush_tx(reactor, connection, turn);
	}
}

static void tr_dispatch_frame(struct tr_reactor *reactor,
			      struct tr_connection *connection,
			      struct tr_frame *frame)
{
	enum tr_frame_disposition disposition = TR_FRAME_RELEASE;

	TR_ASSERT_REACTOR_OWNER(reactor);
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

static int tr_connection_read_preface(
	struct tr_reactor *reactor, struct tr_connection *connection,
	struct tr_reactor_turn *turn, size_t *budget)
{
	uint8_t raw[TR_REACTOR_MAX_PREFACE_BYTES];
	struct tr_conn_handle handle;
	size_t want;
	ssize_t n;
	int done = 0;
	int ret;

	if (!connection->preface_remaining || !connection->preface_feed)
		return TR_ERR_STATE;

	want = connection->preface_remaining;
	if (want > *budget)
		want = *budget;
	if (want > sizeof(raw))
		want = sizeof(raw);

	n = recv(connection->fd, raw, want, 0);
	if (n > 0) {
		(void)__atomic_fetch_add(&connection->rx_bytes,
					 (uint64_t)n, __ATOMIC_RELAXED);
		__atomic_store_n(&connection->last_rx_activity_ns,
				 tr_reactor_now_ns(), __ATOMIC_RELAXED);
		*budget -= (size_t)n;
		turn->left.rx_bytes -= (uint64_t)n;
		connection->preface_remaining -= (uint32_t)n;

		handle.reactor = reactor;
		handle.slot = connection->slot;
		handle.generation = connection->generation;
		ret = connection->preface_feed(
			handle, raw, (size_t)n, &done, connection->preface_arg);
		if (ret != TR_OK)
			return ret;

		if (connection->preface_remaining == 0U) {
			tr_reactor_preface_release_cb release;
			void *release_arg;

			if (!done)
				return TR_ERR_STATE;

			release = connection->preface_release;
			release_arg = connection->preface_arg;
			connection->preface_feed = NULL;
			connection->preface_release = NULL;
			connection->preface_arg = NULL;
			if (release)
				release(release_arg);
		} else if (done) {
			return TR_ERR_STATE;
		}
		return TR_OK;
	}

	if (n == 0)
		return TR_ERR_CLOSED;
	if (errno == EINTR)
		return TR_AGAIN;
	if (errno == EAGAIN || errno == EWOULDBLOCK) {
		(void)__atomic_fetch_add(&connection->recv_eagain,
					 UINT64_C(1), __ATOMIC_RELAXED);
		return TR_AGAIN;
	}
	return TR_ERR_SYS;
}

static void tr_connection_on_readable(struct tr_reactor *reactor,
				      struct tr_connection *connection,
				      struct tr_reactor_turn *turn)
{
	size_t budget = turn->left.rx_bytes < TR_IO_QUANTUM ?
		(size_t)turn->left.rx_bytes : TR_IO_QUANTUM;

	while (budget != 0 && connection->state == TR_CONN_ACTIVE) {
		struct tr_frame frame;

		if (connection->preface_remaining != 0U) {
			int preface_ret = tr_connection_read_preface(
				reactor, connection, turn, &budget);

			if (preface_ret == TR_AGAIN)
				return;
			if (preface_ret == TR_ERR_CLOSED) {
				tr_connection_close_internal(
					reactor, connection,
					TR_CONN_EVENT_CLOSED, TR_OK);
				return;
			}
			if (preface_ret != TR_OK) {
				tr_connection_close_internal(
					reactor, connection,
					TR_CONN_EVENT_ERROR, preface_ret);
				return;
			}
			continue;
		}
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
			turn->left.rx_bytes -= (uint64_t)n;

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

	/* 量子或整轮预算耗尽仍表示存在可运行工作，不同于 EAGAIN 或 RX 暂停。 */
	tr_schedule_rx(reactor, connection);
}

static void tr_run_rx_ready(struct tr_reactor *reactor,
			    struct tr_reactor_turn *turn)
{
	while (reactor->rx_ready_head && turn->left.rx_dispatches != 0 &&
	       turn->left.rx_bytes != 0) {
		struct tr_connection *connection = reactor->rx_ready_head;
		int error_pending = connection->rx_error_pending;

		reactor->rx_ready_head = connection->rx_ready_next;
		if (!reactor->rx_ready_head)
			reactor->rx_ready_tail = NULL;
		connection->rx_scheduled = 0;
		connection->rx_ready_next = NULL;
		connection->rx_error_pending = 0;
		turn->left.rx_dispatches--;
		tr_connection_on_readable(reactor, connection, turn);
		if (error_pending)
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
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

		/* 非信号量模式 eventfd 一次读取会消费累计计数。 */
		if (n == (ssize_t)sizeof(value))
			return;
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

static void tr_enqueue_tx_item_owner(struct tr_reactor *reactor,
				     struct tr_connection *connection,
				     struct tr_tx_item *item)
{
	TR_ASSERT_REACTOR_OWNER(reactor);
	assert(connection && connection->state == TR_CONN_ACTIVE);
	assert(item);

	item->next = NULL;
	item->prev = connection->tx_tail;
	item->control_next = NULL;
	if (connection->tx_tail)
		connection->tx_tail->next = item;
	else
		connection->tx_head = item;
	connection->tx_tail = item;
	if (item->type != TR_FRAME_DATA) {
		if (connection->tx_control_tail)
			connection->tx_control_tail->control_next = item;
		else
			connection->tx_control_head = item;
		connection->tx_control_tail = item;
	}
	(void)__atomic_fetch_add(&connection->tx_queued_items, 1U,
				 __ATOMIC_RELAXED);

	tr_schedule_tx(reactor, connection);
}

static int tr_send_item_owner(struct tr_reactor *reactor,
			      struct tr_conn_handle handle,
			      struct tr_tx_item *item)
{
	struct tr_connection *connection;

	TR_ASSERT_REACTOR_OWNER(reactor);
	connection = tr_lookup_connection(reactor, handle.slot,
					  handle.generation);
	if (!connection)
		return TR_ERR_STALE;

	tr_enqueue_tx_item_owner(reactor, connection, item);
	return TR_OK;
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

	tr_enqueue_tx_item_owner(reactor, connection, item);
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

static int tr_process_completions(struct tr_reactor *reactor,
				  uint64_t *remaining)
{
	struct tr_completion completions[TR_COMPLETION_BATCH];
	size_t count;
	size_t i;
	int has_more = 0;

	TR_ASSERT_REACTOR_OWNER(reactor);

	count = tr_completion_queue_pop_batch(
		&reactor->completions, completions, (size_t)*remaining,
		&has_more);
	*remaining -= count;
	for (i = 0; i < count; ++i)
		if (completions[i].fn)
			completions[i].fn(completions[i].arg);

	return has_more;
}

static void tr_process_call(const struct tr_command *command)
{
	int status = TR_ERR_INVALID;

	if (command->u.call.fn)
		status = command->u.call.fn(command->u.call.arg);
	tr_reactor_sync_complete(command->u.call.sync, status);
}

static int tr_process_commands(struct tr_reactor *reactor,
			       struct tr_reactor_turn *turn)
{
	struct tr_command commands[TR_COMMAND_BATCH];
	size_t count;
	size_t i;

	TR_ASSERT_REACTOR_OWNER(reactor);

	count = tr_command_queue_pop_batch(&reactor->commands, commands,
					   (size_t)turn->left.commands);
	turn->left.commands -= count;
	/*
	 * 已复制到本地批次的命令不再体现在环形队列计数中，
	 * 但批次中的后续命令仍然是处理较早命令期间新生成工作的 FIFO 前驱。
	 */
	assert(!reactor->command_dispatching);
	reactor->command_dispatching = count != 0;
	for (i = 0; i < count; ++i) {
		const struct tr_command *command = &commands[i];

		switch (command->type) {
		case TR_CMD_ADOPT_FD:
			if (tr_connection_adopt(reactor,
						command->slot,
						command->generation,
						command->u.adopt.fd, NULL) != TR_OK)
				close(command->u.adopt.fd);
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
		case TR_CMD_CALL:
			tr_process_call(command);
			break;
		case TR_CMD_STOP:
			reactor->stopping = 1;
			break;
		default:
			break;
		}
	}
	reactor->command_dispatching = 0;

	/*
	 * 取满一个批次后，可能仍有工作，而对应唤醒已经被消费。
	 * 因此即使本批恰好清空环形队列，也保守地再执行一次非阻塞轮询。
	 * 如果是短批次，则队列已经在 queue->lock 下被确认清空；
	 * 后续生产者会重新设置 wake_pending 并通知 eventfd。
	 * 不需要无同步读取队列计数，也不需要新增队列 API。
	 */
	return turn->left.commands == 0;
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
		tr_schedule_rx(reactor, connection);

	if (connection->state != TR_CONN_ACTIVE)
		return;

	if (events & EPOLLOUT)
		tr_connection_on_writable(reactor, connection);

	if (connection->state != TR_CONN_ACTIVE)
		return;

	if (events & EPOLLERR) {
		/* RX 被延后处理时，仍保持先处理可读事件、再处理错误事件的顺序。 */
		if (connection->rx_scheduled)
			connection->rx_error_pending = 1;
		else
			tr_connection_close_internal(reactor, connection,
						     TR_CONN_EVENT_ERROR, TR_ERR_SYS);
	}
}

static int tr_process_timers(struct tr_reactor *reactor,
			     struct tr_reactor_turn *turn)
{
	uint64_t now_ns;
	uint64_t deadline_ns;
	size_t count;
	int has_more_due = 0;

	TR_ASSERT_REACTOR_OWNER(reactor);
	now_ns = tr_reactor_now_ns();
	if (now_ns == 0)
		return 0;

	deadline_ns = tr_timer_queue_next_deadline(&reactor->timers);
	if (turn->left.timer_callbacks != 0 && deadline_ns != 0 &&
	    deadline_ns <= now_ns &&
	    now_ns - deadline_ns > turn->timer_lateness_ns)
		turn->timer_lateness_ns = now_ns - deadline_ns;
	count = tr_timer_queue_run_due(&reactor->timers, now_ns,
				       (size_t)turn->left.timer_callbacks,
				       &has_more_due);
	turn->left.timer_callbacks -= count;
	return has_more_due;
}

static int tr_reactor_timer_timeout_ms(struct tr_reactor *reactor)
{
	uint64_t deadline_ns;
	uint64_t now_ns;
	uint64_t delta_ns;
	uint64_t timeout_ms;

	deadline_ns = tr_timer_queue_next_deadline(&reactor->timers);
	if (deadline_ns == 0)
		return -1;

	now_ns = tr_reactor_now_ns();
	if (now_ns == 0 || deadline_ns <= now_ns)
		return 0;

	delta_ns = deadline_ns - now_ns;
	timeout_ms = (delta_ns + UINT64_C(999999)) / UINT64_C(1000000);
	if (timeout_ms > (uint64_t)INT_MAX)
		return INT_MAX;
	return (int)timeout_ms;
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

static void tr_record_turn(struct tr_reactor *reactor,
			   const struct tr_reactor_turn *turn)
{
	struct tr_reactor_stats *stats = &reactor->stats;

	stats->turns++;
#define TR_RECORD_WORK(field) do { \
	uint64_t used = stats->limits.field - turn->left.field; \
	stats->total.field += used; \
	if (used > stats->max_per_turn.field) \
		stats->max_per_turn.field = used; \
	if (turn->left.field == 0) \
		stats->budget_hits.field++; \
} while (0)
	TR_RECORD_WORK(commands);
	TR_RECORD_WORK(completions);
	TR_RECORD_WORK(timer_callbacks);
	TR_RECORD_WORK(rx_bytes);
	TR_RECORD_WORK(tx_bytes);
	TR_RECORD_WORK(rx_dispatches);
	TR_RECORD_WORK(tx_dispatches);
#undef TR_RECORD_WORK
	if (turn->timer_lateness_ns > stats->timer_lateness_ns_max)
		stats->timer_lateness_ns_max = turn->timer_lateness_ns;
	if (turn->polled) {
		if (turn->poll_timeout == 0)
			stats->epoll_polls++;
		else
			stats->epoll_waits++;
	}

	if ((reactor->config.observability_flags & TR_OBSERVABILITY_TIMING) &&
	    turn->start_ns != 0) {
		uint64_t end_ns = tr_reactor_now_ns();
		uint64_t busy_ns = 0;
		uint64_t poll_ns = 0;

		if (end_ns >= turn->start_ns) {
			if (turn->polled && turn->poll_start_ns >= turn->start_ns &&
			    turn->poll_end_ns >= turn->poll_start_ns &&
			    end_ns >= turn->poll_end_ns) {
				poll_ns = turn->poll_end_ns - turn->poll_start_ns;
				busy_ns = (turn->poll_start_ns - turn->start_ns) +
					  (end_ns - turn->poll_end_ns);
			} else {
				busy_ns = end_ns - turn->start_ns;
			}

			stats->busy_ns += busy_ns;
			stats->poll_ns += poll_ns;
			tr_observe_latency_ns(&stats->turn_busy_ns, busy_ns);
		}
	}
}

static void tr_drain_completions(struct tr_reactor *reactor)
{
	int more;

	/* STOP 前已经关闭 accepting，因此剩余已接受项目必须全部应用。 */
	do {
		uint64_t remaining = TR_COMPLETION_BATCH;

		more = tr_process_completions(reactor, &remaining);
		reactor->stats.shutdown_completions += TR_COMPLETION_BATCH - remaining;
	} while (more);
}

static int tr_reactor_dispatch_aux_event(
	struct tr_reactor *reactor, uint64_t token, uint32_t events)
{
	struct tr_aux_event_source *source;
	uint32_t slot;
	uint16_t generation;

	if (!tr_aux_event_token_decode(token, &slot, &generation))
		return 0;

	source = &reactor->aux_events[slot];
	if (!source->used || source->generation != generation ||
	    !source->callback)
		return 1;

	source->callback(source->fd, events, source->arg);
	return 1;
}

static void *tr_reactor_thread_main(void *arg)
{
	struct tr_reactor *reactor = (struct tr_reactor *)arg;
	struct epoll_event events[TR_REACTOR_EVENT_BATCH];

	assert(tr_current_reactor_owner == NULL);
	tr_current_reactor_owner = reactor;

	while (!reactor->stopping) {
		struct tr_reactor_turn turn = { .left = reactor->stats.limits };
		int commands_pending;

		if (reactor->config.observability_flags & TR_OBSERVABILITY_TIMING)
			turn.start_ns = tr_reactor_now_ns();
		int completions_pending;
		int timers_pending;
		int count;
		int i;

		commands_pending = tr_process_commands(reactor, &turn);
		if (reactor->stopping) {
			tr_record_turn(reactor, &turn);
			tr_drain_completions(reactor);
			break;
		}

		completions_pending = tr_process_completions(reactor,
							     &turn.left.completions);
		timers_pending = tr_process_timers(reactor, &turn);
		tr_run_rx_ready(reactor, &turn);
		tr_run_tx_ready(reactor, &turn);

		if (reactor->rx_ready_head || reactor->tx_ready_head ||
		    commands_pending || completions_pending || timers_pending)
			turn.poll_timeout = 0;
		else
			turn.poll_timeout = tr_reactor_timer_timeout_ms(reactor);

		turn.polled = 1;
		if (reactor->config.observability_flags & TR_OBSERVABILITY_TIMING)
			turn.poll_start_ns = tr_reactor_now_ns();
		count = epoll_wait(reactor->epoll_fd, events,
				   (int)TR_REACTOR_EVENT_BATCH, turn.poll_timeout);
		if (reactor->config.observability_flags & TR_OBSERVABILITY_TIMING)
			turn.poll_end_ns = tr_reactor_now_ns();
		if (count < 0) {
			int interrupted = errno == EINTR;

			tr_record_turn(reactor, &turn);
			if (interrupted)
				continue;
			break;
		}

		for (i = 0; i < count; ++i) {
			if (events[i].data.u64 == TR_WAKE_TOKEN) {
				tr_reactor_drain_wake(reactor);
				(void)tr_process_completions(reactor,
							     &turn.left.completions);
			} else if (events[i].data.u64 == TR_LISTENER_TOKEN) {
				if (reactor->listener_cb && reactor->listener_fd >= 0)
					reactor->listener_cb(
						reactor->listener_fd,
						events[i].events,
						reactor->listener_arg);
			} else if (events[i].data.u64 == TR_PEER_EVENT_TOKEN) {
				if (reactor->peer_event_cb &&
				    reactor->peer_event_fd >= 0)
					reactor->peer_event_cb(
						reactor->peer_event_fd,
						events[i].events,
						reactor->peer_event_arg);
			} else if (tr_reactor_dispatch_aux_event(
					   reactor, events[i].data.u64,
					   events[i].events)) {
				/* auxiliary source 已在 helper 内完成 dispatch/stale drop */
			} else {
				tr_handle_connection_event(reactor,
							   events[i].data.u64,
							   events[i].events);
			}
		}

		tr_run_rx_ready(reactor, &turn);
		/* I/O 处理可能跨过截止时间，但不会补充定时器或 TX 预算。 */
		(void)tr_process_timers(reactor, &turn);
		tr_run_tx_ready(reactor, &turn);
		tr_record_turn(reactor, &turn);
	}

	tr_cleanup_connections(reactor);
	tr_current_reactor_owner = NULL;
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

static int tr_reactor_memory_add(
	uint64_t *total, uint64_t count, uint64_t item_bytes)
{
	uint64_t bytes;

	if (!total)
		return TR_ERR_INVALID;
	if (count != 0U && item_bytes > UINT64_MAX / count)
		return TR_ERR_BAD_LENGTH;
	bytes = count * item_bytes;
	if (*total > UINT64_MAX - bytes)
		return TR_ERR_BAD_LENGTH;
	*total += bytes;
	return TR_OK;
}

static int tr_reactor_fixed_memory_bytes(
	const struct tr_reactor_config *config, uint64_t *out)
{
	uint64_t timer_capacity;
	uint64_t total = 0U;
	int ret;

	if (!config || !out)
		return TR_ERR_INVALID;

	timer_capacity =
		(uint64_t)config->max_connections * 4U + 16U;
	if (timer_capacity > UINT32_MAX)
		return TR_ERR_BAD_LENGTH;

#define TR_REACTOR_MEMORY_ADD(count, type_or_bytes)                  \
	do {                                                          \
		ret = tr_reactor_memory_add(                           \
			&total, (uint64_t)(count),                    \
			(uint64_t)(type_or_bytes));                    \
		if (ret != TR_OK)                                    \
			return ret;                                   \
	} while (0)

	TR_REACTOR_MEMORY_ADD(1U, sizeof(struct tr_reactor));
	TR_REACTOR_MEMORY_ADD(config->max_connections, sizeof(struct tr_slot));
	TR_REACTOR_MEMORY_ADD(
		config->max_connections, sizeof(struct tr_connection));
	TR_REACTOR_MEMORY_ADD(
		config->command_capacity, sizeof(struct tr_command));
	TR_REACTOR_MEMORY_ADD(
		config->command_capacity, sizeof(struct tr_completion));
	TR_REACTOR_MEMORY_ADD(
		timer_capacity, sizeof(struct tr_timer_entry));
	TR_REACTOR_MEMORY_ADD(timer_capacity, sizeof(uint32_t));
	TR_REACTOR_MEMORY_ADD(
		config->tx_item_capacity, sizeof(struct tr_tx_item));
	TR_REACTOR_MEMORY_ADD(
		config->control_tx_item_capacity, sizeof(struct tr_tx_item));
	TR_REACTOR_MEMORY_ADD(
		config->rx_buffer_count, sizeof(struct tr_buffer));
	TR_REACTOR_MEMORY_ADD(
		config->rx_buffer_count, config->rx_buffer_size);

#undef TR_REACTOR_MEMORY_ADD

	*out = total;
	return TR_OK;
}


struct tr_reactor_build {
	struct tr_reactor *reactor;
	int ctl_lock_ready;
	int commands_ready;
	int completions_ready;
	int timers_ready;
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
	if (build->timers_ready)
		tr_timer_queue_destroy(&reactor->timers);
	if (build->completions_ready)
		tr_completion_queue_destroy(&reactor->completions);
	if (build->commands_ready)
		tr_command_queue_destroy(&reactor->commands);

	free(reactor->connections);
	free(reactor->slots);

	if (build->ctl_lock_ready)
		pthread_mutex_destroy(&reactor->ctl_lock);
	if (reactor->config.memory_budget && reactor->memory_bytes != 0U)
		(void)tr_memory_budget_release(
			reactor->config.memory_budget, reactor->memory_bytes);
	free(reactor);
	build->reactor = NULL;
}

int tr_reactor_create(const struct tr_reactor_config *config,
		      tr_reactor_frame_cb frame_cb,
		      tr_reactor_event_cb event_cb, void *callback_arg,
		      struct tr_reactor **out)
{
	struct tr_reactor_build build TR_AUTO(tr_reactor_build_cleanup) = { 0 };
	struct tr_reactor_config effective;
	struct tr_reactor *reactor;
	struct epoll_event wake_event;
	uint64_t memory_bytes = 0U;
	uint32_t i;
	int ret;

	if (!out)
		return TR_ERR_INVALID;

	*out = NULL;
	memset(&effective, 0, sizeof(effective));
	if (config)
		effective = *config;
	tr_default_config(&effective);
	if (effective.observability_flags & ~TR_OBSERVABILITY_VALID_FLAGS)
		return TR_ERR_INVALID;
	if (effective.max_payload_len > effective.rx_buffer_size)
		return TR_ERR_BAD_LENGTH;

	ret = tr_reactor_fixed_memory_bytes(&effective, &memory_bytes);
	if (ret != TR_OK)
		return ret;
	if (effective.memory_budget) {
		ret = tr_memory_budget_reserve(
			effective.memory_budget, memory_bytes);
		if (ret != TR_OK)
			return ret;
	}

	reactor = (struct tr_reactor *)calloc(1, sizeof(*reactor));
	if (!reactor) {
		if (effective.memory_budget)
			(void)tr_memory_budget_release(
				effective.memory_budget, memory_bytes);
		return TR_ERR_NOMEM;
	}
	build.reactor = reactor;
	reactor->config = effective;
	reactor->memory_bytes = memory_bytes;
	reactor->epoll_fd = -1;
	reactor->wake_fd = -1;
	reactor->listener_fd = -1;
	reactor->peer_event_fd = -1;
	for (i = 0; i < TR_REACTOR_AUX_EVENT_CAPACITY; ++i)
		reactor->aux_events[i].fd = -1;

	reactor->stats.observability_flags = reactor->config.observability_flags;
	reactor->stats.limits = (struct tr_reactor_work) {
		.commands = TR_COMMAND_BATCH,
		.completions = TR_COMPLETION_BATCH,
		.timer_callbacks = TR_TIMER_BATCH,
		.rx_bytes = reactor->config.rx_budget_bytes,
		.tx_bytes = reactor->config.tx_budget_bytes,
		.rx_dispatches = TR_RX_READY_BATCH,
		.tx_dispatches = TR_TX_READY_BATCH
	};

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

	/*
	 * V1 的完成队列容量继承 command_capacity。
	 * 两个队列在物理上相互独立；未来可以通过公开调优参数拆分容量，
	 * 而不改变完成事件的所有权语义。
	 */
	ret = tr_completion_queue_init(&reactor->completions,
				       reactor->config.command_capacity);
	if (ret != TR_OK)
		return ret;
	build.completions_ready = 1;

	{
		uint64_t timer_capacity =
			(uint64_t)reactor->config.max_connections * 4U + 16U;

		if (timer_capacity > UINT32_MAX)
			return TR_ERR_BAD_LENGTH;
		ret = tr_timer_queue_init(&reactor->timers,
					  (uint32_t)timer_capacity);
		if (ret != TR_OK)
			return ret;
		build.timers_ready = 1;
	}

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

	atomic_store_explicit(&reactor->accepting, 1, memory_order_release);
	*out = reactor;
	build.reactor = NULL;
	return TR_OK;
}

struct tr_reactor_listener_request {
	struct tr_reactor *reactor;
	int fd;
	tr_reactor_listener_cb callback;
	void *arg;
};

static int tr_reactor_listener_register_now(
	struct tr_reactor *reactor, int fd, tr_reactor_listener_cb callback,
	void *arg)
{
	struct epoll_event event;

	if (reactor->listener_fd >= 0)
		return TR_ERR_STATE;

	memset(&event, 0, sizeof(event));
	event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
	event.data.u64 = TR_LISTENER_TOKEN;
	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
		return TR_ERR_SYS;

	reactor->listener_fd = fd;
	reactor->listener_cb = callback;
	reactor->listener_arg = arg;
	return TR_OK;
}

static int tr_reactor_listener_unregister_now(struct tr_reactor *reactor,
					       int fd)
{
	if (reactor->listener_fd < 0)
		return TR_OK;
	if (reactor->listener_fd != fd)
		return TR_ERR_STALE;

	(void)epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	reactor->listener_fd = -1;
	reactor->listener_cb = NULL;
	reactor->listener_arg = NULL;
	return TR_OK;
}

static int tr_reactor_listener_register_on_owner(void *arg)
{
	struct tr_reactor_listener_request *request =
		(struct tr_reactor_listener_request *)arg;

	TR_ASSERT_REACTOR_OWNER(request->reactor);
	return tr_reactor_listener_register_now(
		request->reactor, request->fd, request->callback, request->arg);
}

static int tr_reactor_listener_unregister_on_owner(void *arg)
{
	struct tr_reactor_listener_request *request =
		(struct tr_reactor_listener_request *)arg;

	TR_ASSERT_REACTOR_OWNER(request->reactor);
	return tr_reactor_listener_unregister_now(request->reactor, request->fd);
}

int tr_reactor_listener_register(struct tr_reactor *reactor, int fd,
				 tr_reactor_listener_cb callback, void *arg)
{
	struct tr_reactor_listener_request request;
	int started;
	int ret;

	if (!reactor || fd < 0 || !callback)
		return TR_ERR_INVALID;

	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_listener_register_now(reactor, fd, callback, arg);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_listener_register_now(reactor, fd, callback, arg);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.callback = callback;
	request.arg = arg;
	return tr_reactor_call(reactor, tr_reactor_listener_register_on_owner,
			       &request);
}

int tr_reactor_listener_unregister(struct tr_reactor *reactor, int fd)
{
	struct tr_reactor_listener_request request;
	int started;
	int ret;

	if (!reactor || fd < 0)
		return TR_ERR_INVALID;

	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_listener_unregister_now(reactor, fd);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_listener_unregister_now(reactor, fd);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.callback = NULL;
	request.arg = NULL;
	return tr_reactor_call(reactor, tr_reactor_listener_unregister_on_owner,
			       &request);
}

struct tr_reactor_peer_event_request {
	struct tr_reactor *reactor;
	int fd;
	tr_reactor_peer_event_cb callback;
	void *arg;
};

static int tr_reactor_peer_event_register_now(
	struct tr_reactor *reactor, int fd, tr_reactor_peer_event_cb callback,
	void *arg)
{
	struct epoll_event event;

	if (reactor->peer_event_fd >= 0)
		return TR_ERR_STATE;

	memset(&event, 0, sizeof(event));
	event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
	event.data.u64 = TR_PEER_EVENT_TOKEN;
	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
		return TR_ERR_SYS;

	reactor->peer_event_fd = fd;
	reactor->peer_event_cb = callback;
	reactor->peer_event_arg = arg;
	return TR_OK;
}

static int tr_reactor_peer_event_unregister_now(
	struct tr_reactor *reactor, int fd)
{
	if (reactor->peer_event_fd < 0)
		return TR_OK;
	if (reactor->peer_event_fd != fd)
		return TR_ERR_STALE;

	(void)epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	reactor->peer_event_fd = -1;
	reactor->peer_event_cb = NULL;
	reactor->peer_event_arg = NULL;
	return TR_OK;
}

static int tr_reactor_peer_event_register_on_owner(void *arg)
{
	struct tr_reactor_peer_event_request *request =
		(struct tr_reactor_peer_event_request *)arg;

	return tr_reactor_peer_event_register_now(
		request->reactor, request->fd, request->callback, request->arg);
}

static int tr_reactor_peer_event_unregister_on_owner(void *arg)
{
	struct tr_reactor_peer_event_request *request =
		(struct tr_reactor_peer_event_request *)arg;

	return tr_reactor_peer_event_unregister_now(request->reactor,
						    request->fd);
}

int tr_reactor_peer_event_register(struct tr_reactor *reactor, int fd,
				   tr_reactor_peer_event_cb callback, void *arg)
{
	struct tr_reactor_peer_event_request request;
	int started;
	int ret;

	if (!reactor || fd < 0 || !callback)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_peer_event_register_now(reactor, fd, callback,
							  arg);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_peer_event_register_now(reactor, fd, callback,
							 arg);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.callback = callback;
	request.arg = arg;
	return tr_reactor_call(reactor, tr_reactor_peer_event_register_on_owner,
			       &request);
}

int tr_reactor_peer_event_unregister(struct tr_reactor *reactor, int fd)
{
	struct tr_reactor_peer_event_request request;
	int started;
	int ret;

	if (!reactor || fd < 0)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_peer_event_unregister_now(reactor, fd);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_peer_event_unregister_now(reactor, fd);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.callback = NULL;
	request.arg = NULL;
	return tr_reactor_call(reactor,
			       tr_reactor_peer_event_unregister_on_owner,
			       &request);
}

struct tr_reactor_aux_event_request {
	struct tr_reactor *reactor;
	int fd;
	uint32_t events;
	tr_reactor_aux_event_cb callback;
	void *arg;
};

static int tr_reactor_aux_event_register_now(
	struct tr_reactor *reactor, int fd, uint32_t events,
	tr_reactor_aux_event_cb callback, void *arg)
{
	struct epoll_event event;
	struct tr_aux_event_source *source = NULL;
	uint32_t i;

	if (events == 0U || (events & ~(uint32_t)(EPOLLIN | EPOLLOUT)) != 0U)
		return TR_ERR_INVALID;

	for (i = 0; i < TR_REACTOR_AUX_EVENT_CAPACITY; ++i) {
		if (reactor->aux_events[i].used &&
		    reactor->aux_events[i].fd == fd)
			return TR_ERR_STATE;
		if (!source && !reactor->aux_events[i].used)
			source = &reactor->aux_events[i];
	}
	if (!source)
		return TR_AGAIN;

	source->generation =
		tr_aux_event_next_generation(source->generation);
	memset(&event, 0, sizeof(event));
	event.events = events | EPOLLERR | EPOLLHUP;
	event.data.u64 = tr_aux_event_token(
		(uint32_t)(source - reactor->aux_events), source->generation);
	if (epoll_ctl(reactor->epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
		return TR_ERR_SYS;

	source->fd = fd;
	source->callback = callback;
	source->arg = arg;
	source->used = 1;
	return TR_OK;
}

static int tr_reactor_aux_event_unregister_now(
	struct tr_reactor *reactor, int fd)
{
	uint32_t i;
	int any_used = 0;

	for (i = 0; i < TR_REACTOR_AUX_EVENT_CAPACITY; ++i) {
		struct tr_aux_event_source *source = &reactor->aux_events[i];

		if (!source->used)
			continue;
		any_used = 1;
		if (source->fd != fd)
			continue;

		(void)epoll_ctl(reactor->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
		source->fd = -1;
		source->callback = NULL;
		source->arg = NULL;
		source->used = 0;
		return TR_OK;
	}

	return any_used ? TR_ERR_STALE : TR_OK;
}

static int tr_reactor_aux_event_register_on_owner(void *arg)
{
	struct tr_reactor_aux_event_request *request =
		(struct tr_reactor_aux_event_request *)arg;

	TR_ASSERT_REACTOR_OWNER(request->reactor);
	return tr_reactor_aux_event_register_now(
		request->reactor, request->fd, request->events,
		request->callback, request->arg);
}

static int tr_reactor_aux_event_unregister_on_owner(void *arg)
{
	struct tr_reactor_aux_event_request *request =
		(struct tr_reactor_aux_event_request *)arg;

	TR_ASSERT_REACTOR_OWNER(request->reactor);
	return tr_reactor_aux_event_unregister_now(
		request->reactor, request->fd);
}

int tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
				  uint32_t events,
				  tr_reactor_aux_event_cb callback, void *arg)
{
	struct tr_reactor_aux_event_request request;
	int started;
	int ret;

	if (!reactor || fd < 0 || !callback)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_aux_event_register_now(
			reactor, fd, events, callback, arg);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_aux_event_register_now(
			reactor, fd, events, callback, arg);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.events = events;
	request.callback = callback;
	request.arg = arg;
	return tr_reactor_call(
		reactor, tr_reactor_aux_event_register_on_owner, &request);
}

int tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd)
{
	struct tr_reactor_aux_event_request request;
	int started;
	int ret;

	if (!reactor || fd < 0)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_aux_event_unregister_now(reactor, fd);

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_reactor_aux_event_unregister_now(reactor, fd);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.fd = fd;
	request.events = 0U;
	request.callback = NULL;
	request.arg = NULL;
	return tr_reactor_call(
		reactor, tr_reactor_aux_event_unregister_on_owner, &request);
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

	/*
	 * 只有消费线程已经存在后才开放完成事件准入。
	 * 从这一刻开始，生产者只与完成队列同步，不再经过 ctl_lock。
	 */
	(void)tr_completion_queue_open(&reactor->completions);
	(void)tr_command_queue_wait_open(&reactor->commands);
	reactor->started = 1;
	pthread_mutex_unlock(&reactor->ctl_lock);
	return TR_OK;
}

int tr_reactor_adopt_fd_prefaced_on_owner(
	struct tr_reactor *reactor, int fd,
	const struct tr_reactor_preface_handler *preface,
	struct tr_conn_handle *out)
{
	uint32_t slot;
	uint32_t generation;
	int ret;

	if (!reactor || fd < 0 || !preface || !out)
		return TR_ERR_INVALID;
	if (!tr_reactor_is_owner_thread(reactor))
		return TR_ERR_STATE;
	if (!atomic_load_explicit(&reactor->accepting, memory_order_acquire))
		return TR_ERR_CLOSED;

	ret = tr_slot_reserve(reactor, &slot, &generation);
	if (ret != TR_OK)
		return ret;

	ret = tr_connection_adopt(reactor, slot, generation, fd, preface);
	if (ret != TR_OK) {
		(void)tr_slot_set_state(reactor, slot, generation, TR_CONN_FREE);
		return ret;
	}

	out->reactor = reactor;
	out->slot = slot;
	out->generation = generation;
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

	if (tr_reactor_is_owner_thread(reactor)) {
		if (!atomic_load_explicit(&reactor->accepting,
					 memory_order_acquire))
			return TR_ERR_CLOSED;

		ret = tr_slot_reserve(reactor, &slot, &generation);
		if (ret != TR_OK)
			return ret;

		ret = tr_connection_adopt(reactor, slot, generation, fd, NULL);
		if (ret != TR_OK) {
			(void)tr_slot_set_state(reactor, slot, generation,
						TR_CONN_FREE);
			return ret;
		}

		out->reactor = reactor;
		out->slot = slot;
		out->generation = generation;
		return TR_OK;
	}

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

	/*
	 * Channel/RPC 所有者回调只有在不会越过既有命令工作时，才能直接附着 TX。
	 * 带同步的空队列检查用于与并发生产者建立线性化顺序。
	 * command_dispatching 还覆盖已经弹入当前本地批次的命令：
	 * 这些命令虽然不再计入环形队列数量，但仍必须保持为 FIFO 前驱。
	 *
	 * 只要任意一种前驱存在，就回退到普通有界命令路径。
	 * accepting 继续作为关闭流程的线性化门禁。
	 */
	if (tr_reactor_is_owner_thread(reactor) &&
	    !reactor->command_dispatching &&
	    tr_command_queue_is_empty(&reactor->commands)) {
		if (!atomic_load_explicit(&reactor->accepting,
					 memory_order_acquire)) {
			ret = TR_ERR_CLOSED;
		} else {
			ret = tr_send_item_owner(reactor, connection, item);
		}
		if (ret != TR_OK) {
			memset(item->payloads, 0, sizeof(item->payloads));
			item->payload_count = 0;
			tr_tx_pool_release(item->owner_pool, item);
		}
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
	if (tr_reactor_is_owner_thread(reactor)) {
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

	{
		uint64_t wait_generation;

		pthread_mutex_lock(&reactor->ctl_lock);
		if (!reactor->started || !reactor->accepting) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			tr_reactor_sync_destroy(&request.sync);
			return TR_ERR_CLOSED;
		}
		if (!tr_slot_live(
			    reactor, connection.slot, connection.generation)) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			tr_reactor_sync_destroy(&request.sync);
			return TR_ERR_STALE;
		}
		wait_generation =
			tr_command_queue_wait_generation(&reactor->commands);
		pthread_mutex_unlock(&reactor->ctl_lock);

		if (wait_generation == 0U)
			ret = TR_ERR_CLOSED;
		else
			ret = tr_reactor_push_command_wait(
				reactor, &command, wait_generation);
	}

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

	{
		uint64_t wait_generation;

		pthread_mutex_lock(&reactor->ctl_lock);
		if (!reactor->started) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			pthread_cond_destroy(&sync.cond);
			pthread_mutex_destroy(&sync.lock);
			return TR_OK;
		}
		if (tr_reactor_is_owner_thread(reactor)) {
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
		wait_generation =
			tr_command_queue_wait_generation(&reactor->commands);
		pthread_mutex_unlock(&reactor->ctl_lock);

		if (wait_generation == 0U)
			ret = TR_ERR_CLOSED;
		else
			ret = tr_reactor_push_command_wait(
				reactor, &command, wait_generation);
	}

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
	uint64_t meta;

	if (!reactor || !out ||
	    connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;

	meta = atomic_load_explicit(&reactor->slots[connection.slot].meta,
				    memory_order_acquire);
	if (tr_slot_meta_generation(meta) != connection.generation)
		return TR_ERR_STALE;

	*out = tr_slot_meta_state(meta);
	return TR_OK;
}

int tr_reactor_get_connection_stats(struct tr_conn_handle connection,
				    struct tr_connection_stats *out)
{
	struct tr_reactor *reactor = connection.reactor;
	struct tr_connection *conn;
	uint64_t before;
	uint64_t after;

	if (!reactor || !out ||
	    connection.slot >= reactor->config.max_connections)
		return TR_ERR_INVALID;

	conn = &reactor->connections[connection.slot];

	for (;;) {
		before = atomic_load_explicit(&reactor->slots[connection.slot].meta,
					      memory_order_acquire);
		if (tr_slot_meta_generation(before) != connection.generation)
			return TR_ERR_STALE;

		memset(out, 0, sizeof(*out));
		out->state = tr_slot_meta_state(before);
		out->rx_bytes = __atomic_load_n(&conn->rx_bytes, __ATOMIC_RELAXED);
		out->tx_bytes = __atomic_load_n(&conn->tx_bytes, __ATOMIC_RELAXED);
		out->rx_frames =
			__atomic_load_n(&conn->rx_frames, __ATOMIC_RELAXED);
		out->tx_frames =
			__atomic_load_n(&conn->tx_frames, __ATOMIC_RELAXED);
		out->recv_eagain =
			__atomic_load_n(&conn->recv_eagain, __ATOMIC_RELAXED);
		out->send_eagain =
			__atomic_load_n(&conn->send_eagain, __ATOMIC_RELAXED);
		out->rx_pauses =
			__atomic_load_n(&conn->rx_pauses_count,
					__ATOMIC_RELAXED);
		out->last_rx_activity_ns =
			__atomic_load_n(&conn->last_rx_activity_ns,
					__ATOMIC_RELAXED);
		out->last_tx_activity_ns =
			__atomic_load_n(&conn->last_tx_activity_ns,
					__ATOMIC_RELAXED);
		out->tx_queued_items =
			__atomic_load_n(&conn->tx_queued_items,
					__ATOMIC_RELAXED);
		out->rx_paused =
			__atomic_load_n(&conn->rx_paused, __ATOMIC_RELAXED);
		out->tx_wait_writable =
			__atomic_load_n(&conn->tx_wait_writable,
					__ATOMIC_RELAXED);

		/*
		 * slot 在读取统计期间发生 close/reuse 时重新读取。
		 * 如果 generation 已变化，则旧 handle 已 stale；如果只是同一
		 * generation 的 state 转换，则重试得到同一个生命周期阶段的快照。
		 */
		after = atomic_load_explicit(&reactor->slots[connection.slot].meta,
					     memory_order_acquire);
		if (before == after)
			return TR_OK;
		if (tr_slot_meta_generation(after) != connection.generation)
			return TR_ERR_STALE;
	}
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

int tr_reactor_complete(struct tr_reactor *reactor, void (*fn)(void *arg),
			void *arg)
{
	struct tr_completion completion;
	int ret;

	if (!reactor || !fn)
		return TR_ERR_INVALID;

	/*
	 * owner 内提交 completion 时直接应用；跨线程才进入独立 completion
	 * queue，避免 completion 再占用控制 command 容量。
	 */
	if (tr_reactor_is_owner_thread(reactor)) {
		fn(arg);
		return TR_OK;
	}

	completion.fn = fn;
	completion.arg = arg;

	/*
	 * completion admission 与 capacity wait 都由 completions.lock/not_full
	 * 串行化，不借用 Reactor ctl_lock。queue 满时 worker 睡眠；Reactor pop
	 * 释放容量后唤醒，stop 则 close admission 并唤醒全部 waiter。
	 *
	 * 无论背压多重，worker 都不会回退直接修改 Reactor-owned protocol state。
	 */
	ret = tr_reactor_push_completion(reactor, &completion);
	return ret;
}

int tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *arg),
		    void *arg)
{
	struct tr_reactor_sync sync;
	struct tr_command command;
	int ret;

	if (!reactor || !fn)
		return TR_ERR_INVALID;

	/*
	 * Reactor callback/command 内部再次调用 owner API 时必须直接执行，
	 * 否则同步等待自己会死锁。
	 */
	if (tr_reactor_is_owner_thread(reactor))
		return fn(arg);

	ret = tr_reactor_sync_init(&sync);
	if (ret != TR_OK)
		return ret;

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_CALL;
	command.u.call.fn = fn;
	command.u.call.arg = arg;
	command.u.call.sync = &sync;

	{
		uint64_t wait_generation;

		pthread_mutex_lock(&reactor->ctl_lock);
		if (!reactor->started || !reactor->accepting) {
			pthread_mutex_unlock(&reactor->ctl_lock);
			tr_reactor_sync_destroy(&sync);
			return TR_ERR_CLOSED;
		}
		wait_generation =
			tr_command_queue_wait_generation(&reactor->commands);
		pthread_mutex_unlock(&reactor->ctl_lock);

		/*
		 * 同步 owner call 保留现有 RPC API 的同步语义：有界 command ring
		 * 满时在 queue-local condition 上等待容量，不把容量压力误报成业务失败，
		 * 也不使用 sched_yield 忙等。
		 */
		if (wait_generation == 0U)
			ret = TR_ERR_CLOSED;
		else
			ret = tr_reactor_push_command_wait(
				reactor, &command, wait_generation);
	}

	if (ret == TR_OK)
		ret = tr_reactor_sync_wait(&sync);
	tr_reactor_sync_destroy(&sync);
	return ret;
}

int tr_reactor_call_or_stopped(
	struct tr_reactor *reactor, int (*fn)(void *arg), void *arg)
{
	int ret;

	if (!reactor || !fn)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return fn(arg);

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started) {
		/*
		 * 没有 owner thread，也没有可能复制 callback_arg 的 event dispatch。
		 * ctl_lock 同时阻止另一个线程在 direct mutation 中启动 Reactor。
		 */
		ret = fn(arg);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	if (!atomic_load_explicit(&reactor->accepting, memory_order_acquire)) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_ERR_CLOSED;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	/*
	 * stop 可能在释放 ctl_lock 后抢先关闭 admission；这种并发情况下
	 * tr_reactor_call() 返回 CLOSED，caller 不能回退 direct mutation。
	 */
	return tr_reactor_call(reactor, fn, arg);
}

struct tr_reactor_stats_request {
	struct tr_reactor *reactor;
	struct tr_reactor_stats *out;
};

static void tr_reactor_snapshot_tx_pool(struct tr_tx_pool *pool,
					 struct tr_pool_observation *out)
{
	pthread_mutex_lock(&pool->lock);
	out->capacity = pool->capacity;
	out->current = pool->capacity - pool->free_count;
	out->peak = pool->peak_in_use;
	out->exhausted_events = pool->exhausted_events;
	pthread_mutex_unlock(&pool->lock);
}

static void tr_reactor_snapshot_resources(struct tr_reactor *reactor,
					   struct tr_reactor_stats *out)
{
	pthread_mutex_lock(&reactor->commands.lock);
	out->command_queue.capacity = reactor->commands.capacity;
	out->command_queue.current = reactor->commands.count;
	out->command_queue.peak = reactor->commands.peak_count;
	out->command_queue.full_events = reactor->commands.full_events;
	out->command_send.enqueued = reactor->commands.pushed_send;
	out->command_send.full_events = reactor->commands.full_send;
	out->command_resume_rx.enqueued = reactor->commands.pushed_resume_rx;
	out->command_resume_rx.full_events = reactor->commands.full_resume_rx;
	out->command_call.enqueued = reactor->commands.pushed_call;
	out->command_call.full_events = reactor->commands.full_call;
	out->command_other.enqueued = reactor->commands.pushed_other;
	out->command_other.full_events = reactor->commands.full_other;
	pthread_mutex_unlock(&reactor->commands.lock);

	pthread_mutex_lock(&reactor->completions.lock);
	out->completion_queue.capacity = reactor->completions.capacity;
	out->completion_queue.current = reactor->completions.count;
	out->completion_queue.peak = reactor->completions.peak_count;
	out->completion_queue.full_events = reactor->completions.full_events;
	pthread_mutex_unlock(&reactor->completions.lock);

	(void)tr_buffer_pool_get_stats(&reactor->rx_pool,
				       &out->rx_buffer_pool);
	tr_reactor_snapshot_tx_pool(&reactor->tx_pool, &out->tx_item_pool);
	tr_reactor_snapshot_tx_pool(&reactor->control_tx_pool,
				    &out->control_tx_item_pool);
}

static int tr_reactor_stats_on_owner(void *arg)
{
	struct tr_reactor_stats_request *request = arg;

	*request->out = request->reactor->stats;
	tr_reactor_snapshot_resources(request->reactor, request->out);
	return TR_OK;
}

int tr_reactor_get_stats(struct tr_reactor *reactor, struct tr_reactor_stats *out)
{
	struct tr_reactor_stats_request request = { reactor, out };

	if (!reactor || !out)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor))
		return tr_reactor_stats_on_owner(&request);

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started) {
		*out = reactor->stats;
		tr_reactor_snapshot_resources(reactor, out);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_OK;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);
	return tr_reactor_call(reactor, tr_reactor_stats_on_owner, &request);
}

struct tr_reactor_timer_register_request {
	struct tr_reactor *reactor;
	tr_reactor_timer_cb callback;
	void *arg;
	struct tr_reactor_timer_handle *out;
};

static int tr_reactor_timer_register_on_owner(void *arg)
{
	struct tr_reactor_timer_register_request *request =
		(struct tr_reactor_timer_register_request *)arg;
	struct tr_timer_token token;
	int ret;

	ret = tr_timer_queue_register(&request->reactor->timers,
				      request->callback, request->arg, &token);
	if (ret == TR_OK) {
		request->out->reactor = request->reactor;
		request->out->slot = token.slot;
		request->out->generation = token.generation;
	}
	return ret;
}

int tr_reactor_timer_register(struct tr_reactor *reactor,
			      tr_reactor_timer_cb callback, void *arg,
			      struct tr_reactor_timer_handle *out)
{
	struct tr_reactor_timer_register_request request;
	struct tr_timer_token token;
	int started;
	int ret;

	if (!reactor || !callback || !out)
		return TR_ERR_INVALID;
	memset(out, 0, sizeof(*out));

	if (tr_reactor_is_owner_thread(reactor)) {
		request.reactor = reactor;
		request.callback = callback;
		request.arg = arg;
		request.out = out;
		return tr_reactor_timer_register_on_owner(&request);
	}

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		ret = tr_timer_queue_register(&reactor->timers, callback, arg,
					      &token);
		if (ret == TR_OK) {
			out->reactor = reactor;
			out->slot = token.slot;
			out->generation = token.generation;
		}
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.reactor = reactor;
	request.callback = callback;
	request.arg = arg;
	request.out = out;
	return tr_reactor_call(reactor, tr_reactor_timer_register_on_owner,
			       &request);
}

struct tr_reactor_timer_arm_request {
	struct tr_reactor_timer_handle handle;
	uint64_t deadline_ns;
};

static int tr_reactor_timer_arm_on_owner(void *arg)
{
	struct tr_reactor_timer_arm_request *request =
		(struct tr_reactor_timer_arm_request *)arg;
	struct tr_timer_token token;

	token.slot = request->handle.slot;
	token.generation = request->handle.generation;
	return tr_timer_queue_arm(&request->handle.reactor->timers, token,
				  request->deadline_ns);
}

int tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			 uint64_t deadline_ns)
{
	struct tr_reactor_timer_arm_request request;
	struct tr_timer_token token;
	struct tr_reactor *reactor = handle.reactor;
	int started;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor)) {
		request.handle = handle;
		request.deadline_ns = deadline_ns;
		return tr_reactor_timer_arm_on_owner(&request);
	}

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		token.slot = handle.slot;
		token.generation = handle.generation;
		ret = tr_timer_queue_arm(&reactor->timers, token, deadline_ns);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.handle = handle;
	request.deadline_ns = deadline_ns;
	return tr_reactor_call(reactor, tr_reactor_timer_arm_on_owner,
			       &request);
}

struct tr_reactor_timer_unregister_request {
	struct tr_reactor_timer_handle handle;
};

static int tr_reactor_timer_unregister_on_owner(void *arg)
{
	struct tr_reactor_timer_unregister_request *request =
		(struct tr_reactor_timer_unregister_request *)arg;
	struct tr_timer_token token;

	token.slot = request->handle.slot;
	token.generation = request->handle.generation;
	return tr_timer_queue_unregister(&request->handle.reactor->timers,
					 token);
}

int tr_reactor_timer_unregister(struct tr_reactor_timer_handle handle)
{
	struct tr_reactor_timer_unregister_request request;
	struct tr_timer_token token;
	struct tr_reactor *reactor = handle.reactor;
	int started;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;
	if (tr_reactor_is_owner_thread(reactor)) {
		request.handle = handle;
		return tr_reactor_timer_unregister_on_owner(&request);
	}

	pthread_mutex_lock(&reactor->ctl_lock);
	started = reactor->started;
	if (!started) {
		token.slot = handle.slot;
		token.generation = handle.generation;
		ret = tr_timer_queue_unregister(&reactor->timers, token);
		pthread_mutex_unlock(&reactor->ctl_lock);
		return ret;
	}
	pthread_mutex_unlock(&reactor->ctl_lock);

	request.handle = handle;
	return tr_reactor_call(reactor, tr_reactor_timer_unregister_on_owner,
			       &request);
}

int tr_reactor_stop(struct tr_reactor *reactor)
{
	struct tr_command command;
	int ret;

	if (!reactor)
		return TR_ERR_INVALID;

	/*
	 * pthread_join(self) 属于生命周期错误，不是可以恢复的停止路径。
	 * 必须在关闭准入之前拒绝，使所有者回调不能部分停止自己的 Reactor，
	 * 然后继续访问已经释放或关闭的状态。
	 */
	if (tr_reactor_is_owner_thread(reactor))
		return TR_ERR_STATE;

	pthread_mutex_lock(&reactor->ctl_lock);
	if (!reactor->started) {
		pthread_mutex_unlock(&reactor->ctl_lock);
		return TR_OK;
	}

	atomic_store_explicit(&reactor->accepting, 0, memory_order_release);
	/*
	 * 先关闭所有 producer admission：
	 * - completion waiter 由 completion queue close 唤醒；
	 * - synchronous command waiter 由 command wait_close 唤醒；
	 * 然后 STOP 自己以 lifecycle-only force wait 等待 ring capacity。
	 *
	 * Reactor pop command 不取得 ctl_lock，因此这里持 ctl_lock 睡眠不会阻止
	 * owner 释放 command slot，同时也阻止第二批普通 producer 越过 stop 边界。
	 */
	tr_completion_queue_close(&reactor->completions);
	tr_command_queue_wait_close(&reactor->commands);
	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_STOP;
	ret = tr_reactor_push_command_wait_force(reactor, &command);

	pthread_mutex_unlock(&reactor->ctl_lock);

	if (ret != TR_OK)
		return ret;

	ret = pthread_join(reactor->thread, NULL);
	if (ret != 0)
		return TR_ERR_SYS;

	pthread_mutex_lock(&reactor->ctl_lock);
	reactor->started = 0;
	pthread_mutex_unlock(&reactor->ctl_lock);
	return TR_OK;
}

void tr_reactor_destroy(struct tr_reactor *reactor)
{
	int ret;

	if (!reactor)
		return;

	if (reactor->started) {
		ret = tr_reactor_stop(reactor);
#ifndef NDEBUG
		assert(ret == TR_OK);
#endif
		if (ret != TR_OK)
			return;
	}

	if (reactor->wake_fd >= 0)
		close(reactor->wake_fd);
	if (reactor->epoll_fd >= 0)
		close(reactor->epoll_fd);

	tr_buffer_pool_destroy(&reactor->rx_pool);
	tr_tx_pool_destroy(&reactor->control_tx_pool);
	tr_tx_pool_destroy(&reactor->tx_pool);
	tr_timer_queue_destroy(&reactor->timers);
	tr_completion_queue_destroy(&reactor->completions);
	tr_command_queue_destroy(&reactor->commands);

	free(reactor->connections);
	free(reactor->slots);

	pthread_mutex_destroy(&reactor->ctl_lock);
	if (reactor->config.memory_budget && reactor->memory_bytes != 0U)
		(void)tr_memory_budget_release(
			reactor->config.memory_budget, reactor->memory_bytes);
	free(reactor);
}
