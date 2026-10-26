#define _GNU_SOURCE
#include "../src/transport/channel/channel.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/transport/protocol/wire.h"
#include "../src/execution/reactor_internal.h"
#include "../src/crc32c.h"
#include "../src/endian.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define WINDOW 256U
#define FRAME 64U
#define CAPACITY 4U
#define CONTROL 4U
#define COMMANDS 8U
#define TRACKED_STREAMS 256U

struct fixture {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_conn_handle connection[2];
	int peer[2];
	int split;
	struct tr_buffer_pool pool;
	struct tr_buffer_pool assembly;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened, resets, closed, errors, received, down, writable,
	        remote_closed;
	unsigned reopened_count;
	unsigned refused;
	int down_status;
	int error_status;
	struct tr_stream_handle stream[TRACKED_STREAMS];
	int reset_status[TRACKED_STREAMS];
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
	struct tr_buffer *held_extra;
	struct tr_stream_handle held_extra_stream;
	int hold_data;
	int block_reset_callback;
	int reset_callback_blocked;
	int release_reset_callback;
	unsigned release_threads_ready;
	int release_threads_go;
	int queue_reset_predecessors;
	int reopen_on_closed;
	int circular_stream_tracking;
	struct tr_stream_handle reopened_stream;
};

static _Thread_local int fail_reset_arm;
static atomic_uint arm_failures;
static atomic_int partial_fd = -1;
static atomic_int partial_phase;
static atomic_int partial_type = TR_FRAME_STREAM_RESET;
static atomic_int block_partial;
static atomic_uint partial_eagain;
static atomic_int block_all_tx;
static atomic_int repeat_goaway_phase;
static atomic_int force_local_stream_id_exhaustion;
static struct tr_reactor_timer_handle repeat_goaway_timer;

int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t deadline_ns);
int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t deadline_ns)
{
	if (fail_reset_arm) {
		fail_reset_arm = 0;
		atomic_fetch_add(&arm_failures, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_arm(timer, deadline_ns);
}

ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);
int __real_tr_channel_local_stream_id_take(uint32_t *next, uint32_t *out);
int __wrap_tr_channel_local_stream_id_take(uint32_t *next, uint32_t *out)
{
	if (atomic_exchange_explicit(&force_local_stream_id_exhaustion, 0,
	                             memory_order_acq_rel)) {
		assert(next);
		*next = 0U;
	}
	return __real_tr_channel_local_stream_id_take(next, out);
}

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	if (fd == atomic_load(&partial_fd)) {
		if (atomic_load(&block_all_tx)) {
			errno = EAGAIN;
			return -1;
		}
		if (atomic_load(&repeat_goaway_phase) == 1 &&
		    message->msg_iovlen == 1 &&
		    message->msg_iov[0].iov_len == TR_WIRE_HEADER_SIZE &&
		    tr_get_le16((const uint8_t *)message->msg_iov[0].iov_base +
		                TR_WIRE_OFF_TYPE) == TR_FRAME_GOAWAY) {
			ssize_t sent = __real_sendmsg(fd, message, flags);
			assert(sent == TR_WIRE_HEADER_SIZE);
			/* 真正写完旧 GOAWAY 后登记下一轮 owner 回调；返回后
			 * Reactor 才完成内嵌项，下一轮仍有大量 RESET 前驱命令。
			 */
			atomic_store(&repeat_goaway_phase, 2);
			assert(tr_reactor_timer_arm(repeat_goaway_timer, 1) ==
			       TR_OK);
			return sent;
		}
		int phase = atomic_load(&partial_phase);
		if (phase == 1 && message->msg_iovlen &&
		    message->msg_iov[0].iov_len >= TR_WIRE_HEADER_SIZE) {
			const uint8_t *bytes = message->msg_iov[0].iov_base;
			if (tr_get_le16(bytes + TR_WIRE_OFF_TYPE) ==
			    atomic_load(&partial_type)) {
				struct msghdr cut = *message;
				struct iovec first = message->msg_iov[0];
				first.iov_len = 7;
				cut.msg_iov = &first;
				cut.msg_iovlen = 1;
				ssize_t sent = __real_sendmsg(fd, &cut, flags);
				if (sent > 0) {
					assert(sent == 7);
					atomic_store(&partial_phase, 2);
				}
				return sent;
			}
		} else if (phase == 2) {
			atomic_fetch_add(&partial_eagain, 1U);
			if (!atomic_load(&block_partial))
				atomic_store(&partial_phase, 3);
			errno = EAGAIN;
			return -1;
		}
	}
	return __real_sendmsg(fd, message, flags);
}

static void pause_ms(void)
{
	const struct timespec pause = {0, 1000000L};
	(void)nanosleep(&pause, NULL);
}

static void wait_count(struct fixture *f, unsigned *counter, unsigned target)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (*counter < target)
		assert(pthread_cond_timedwait(&f->cond, &f->lock, &deadline) ==
		       0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

struct payload_release_request {
	struct fixture *fixture;
	struct tr_stream_handle stream;
	struct tr_buffer *payload;
	int result;
};

static void *release_payload_after_gate(void *arg)
{
	struct payload_release_request *request = arg;
	struct fixture *f = request->fixture;

	assert(pthread_mutex_lock(&f->lock) == 0);
	++f->release_threads_ready;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	while (!f->release_threads_go)
		assert(pthread_cond_wait(&f->cond, &f->lock) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	request->result =
	        tr_stream_release_payload(request->stream, request->payload);
	return NULL;
}

static void assert_fixed_history_budget(
	struct tr_memory_budget *budget, uint64_t fixed_bytes)
{
	struct tr_memory_budget_stats stats;

	tr_memory_budget_get_stats(budget, &stats);
	assert(stats.current_bytes == fixed_bytes);
	assert(stats.peak_bytes == fixed_bytes);
	assert(stats.rejection_events == 0U);
}

static unsigned count(struct fixture *f, const unsigned *counter)
{
	unsigned value;
	assert(pthread_mutex_lock(&f->lock) == 0);
	value = *counter;
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return value;
}

static enum tr_stream_data_disposition on_data(struct tr_stream_handle stream,
                                               uint64_t message_id,
                                               struct tr_buffer *payload,
                                               void *arg)
{
	struct fixture *f = arg;
	(void)message_id;
	assert(pthread_mutex_lock(&f->lock) == 0);
	++f->received;
	if (f->hold_data) {
		if (!f->held) {
			f->held = payload;
			f->held_stream = stream;
		} else {
			assert(!f->held_extra);
			f->held_extra = payload;
			f->held_extra_stream = stream;
		}
	}
	int hold = f->hold_data;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return hold ? TR_STREAM_DATA_TAKE_OWNERSHIP : TR_STREAM_DATA_RELEASE;
}

static void on_stream(struct tr_stream_handle stream,
                      enum tr_stream_event event, int status, void *arg)
{
	struct fixture *f = arg;
	int block_reset_callback = 0;
	int queue_predecessors = 0;
	int reopen = 0;
	assert(pthread_mutex_lock(&f->lock) == 0);
	if (event == TR_STREAM_EVENT_OPENED) {
		if (f->circular_stream_tracking) {
			f->stream[f->opened % TRACKED_STREAMS] = stream;
			++f->opened;
		} else {
			assert(f->opened < TRACKED_STREAMS);
			f->stream[f->opened++] = stream;
		}
	} else if (event == TR_STREAM_EVENT_RESET) {
		if (f->circular_stream_tracking) {
			f->reset_status[f->resets % TRACKED_STREAMS] = status;
			++f->resets;
		} else {
			assert(f->resets < TRACKED_STREAMS);
			f->reset_status[f->resets++] = status;
		}
		queue_predecessors = f->queue_reset_predecessors;
		f->queue_reset_predecessors = 0;
		block_reset_callback = f->block_reset_callback;
		f->block_reset_callback = 0;
		if (block_reset_callback)
			f->reset_callback_blocked = 1;
	} else if (event == TR_STREAM_EVENT_CLOSED) {
		++f->closed;
		reopen = f->reopen_on_closed;
		f->reopen_on_closed = 0;
	} else if (event == TR_STREAM_EVENT_ERROR) {
		++f->errors;
		f->error_status = status;
	} else if (event == TR_STREAM_EVENT_REFUSED) {
		assert(status == TR_ERR_NOMEM);
		++f->refused;
	} else if (event == TR_STREAM_EVENT_WRITABLE) {
		++f->writable;
	} else if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		++f->remote_closed;
	}
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	if (block_reset_callback) {
		assert(pthread_mutex_lock(&f->lock) == 0);
		while (!f->release_reset_callback)
			assert(pthread_cond_wait(&f->cond, &f->lock) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
	}
	if (queue_predecessors) {
		/* RESET 还未交给 Reactor；在真实 owner 回调中固定四条前驱。 */
		for (unsigned i = 0; i < 4; ++i)
			assert(tr_reactor_resume_rx(f->connection[0]) == TR_OK);
	}
	if (reopen) {
		struct tr_stream_handle next;
		/* CLOSED 已归还唯一槽，重入 OPEN 必须排在响应 RESET 之后。 */
		assert(tr_stream_open(f->channel, TR_LANE_CONTROL, &next) ==
		       TR_OK);
		assert(pthread_mutex_lock(&f->lock) == 0);
		f->reopened_stream = next;
		++f->reopened_count;
		assert(pthread_cond_broadcast(&f->cond) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
	}
}

static void on_channel(struct tr_channel *channel, enum tr_channel_event event,
                       int status, void *arg)
{
	struct fixture *f = arg;
	(void)channel;
	if (event != TR_CHANNEL_EVENT_CONTROL_DOWN &&
	    event != TR_CHANNEL_EVENT_BULK_DOWN)
		return;
	assert(pthread_mutex_lock(&f->lock) == 0);
	++f->down;
	f->down_status = status;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void transfer(int fd, void *bytes, size_t size, int writing)
{
	uint8_t *cursor = bytes;
	while (size) {
		ssize_t count = writing ? send(fd, cursor, size, MSG_NOSIGNAL)
		                        : recv(fd, cursor, size, 0);
		if (count < 0 && errno == EINTR)
			continue;
		if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			struct pollfd wait = {fd, writing ? POLLOUT : POLLIN,
			                      0};
			assert(poll(&wait, 1, 5000) > 0);
			continue;
		}
		assert(count > 0);
		cursor += (size_t)count;
		size -= (size_t)count;
	}
}

static void send_frame(struct fixture *f, unsigned lane, uint16_t type,
                       uint32_t flags, uint32_t stream_id, uint64_t message_id,
                       const void *body, uint32_t size)
{
	uint8_t raw[TR_WIRE_HEADER_SIZE];
	struct tr_frame_header header = {.version = TR_WIRE_ENV_VERSION,
	                                 .type = type,
	                                 .flags = flags,
	                                 .stream_id = stream_id,
	                                 .message_id = message_id,
	                                 .payload_len = size,
	                                 .payload_crc32c =
	                                         tr_crc32c(body, size)};
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
	transfer(f->peer[lane], raw, sizeof(raw), 1);
	if (size)
		transfer(f->peer[lane], (void *)body, size, 1);
}

static struct tr_frame_header receive_frame(struct fixture *f, unsigned lane)
{
	uint8_t raw[TR_WIRE_HEADER_SIZE], body[FRAME];
	struct tr_frame_header header;
	const struct tr_wire_limits limits = {FRAME};
	transfer(f->peer[lane], raw, sizeof(raw), 0);
	assert(tr_wire_header_decode(raw, &header) == TR_OK);
	assert(tr_wire_header_validate(raw, &header, &limits) == TR_OK);
	assert(header.payload_len <= sizeof(body));
	if (header.payload_len)
		transfer(f->peer[lane], body, header.payload_len, 0);
	assert(tr_crc32c(body, header.payload_len) == header.payload_crc32c);
	return header;
}

static void barrier(struct fixture *f, unsigned lane)
{
	send_frame(f, lane, TR_FRAME_PING, 0, 0, 999, NULL, 0);
	for (;;) {
		struct tr_frame_header h = receive_frame(f, lane);
		if (h.type == TR_FRAME_PONG && h.message_id == 999)
			break;
	}
}

static void barrier_without_stream_window_update(struct fixture *f,
                                                 unsigned lane,
                                                 uint32_t stream_id, int status)
{
	unsigned resets = 0U;
	struct tr_frame_header h;

	send_frame(f, lane, TR_FRAME_PING, 0, 0, 1000U, NULL, 0);
	do {
		h = receive_frame(f, lane);
		if (h.type == TR_FRAME_WINDOW_UPDATE &&
		    h.stream_id == stream_id)
			assert(!"RESET stream received late WINDOW_UPDATE");
		if (h.type == TR_FRAME_STREAM_RESET &&
		    h.stream_id == stream_id) {
			assert(h.message_id == (uint64_t)-status);
			++resets;
		}
	} while (h.type != TR_FRAME_PONG || h.message_id != 1000U);
	assert(resets == 1U);
}

static void hello_with_max_message(struct fixture *f, unsigned lane,
                                   uint64_t features, int ack,
                                   uint32_t max_message_bytes)
{
	uint8_t bytes[32] = {0};
	tr_put_le16(bytes, TR_CHANNEL_PROTOCOL_VERSION);
	if (!ack)
		tr_put_le16(bytes + 2, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le32(bytes + 4, f->split ? (1U << lane) : 3U);
	tr_put_le32(bytes + 8, FRAME);
	tr_put_le32(bytes + 12, max_message_bytes);
	tr_put_le64(bytes + 16, features);
	send_frame(f, lane, ack ? TR_FRAME_HELLO_ACK : TR_FRAME_HELLO, 0, 0, 0,
	           bytes, sizeof(bytes));
}

static void hello(struct fixture *f, unsigned lane, uint64_t features, int ack)
{
	hello_with_max_message(f, lane, features, ack, WINDOW);
}

static void init_fixture_with_commands(struct fixture *f, uint32_t capacity,
                                       int split, uint64_t features,
                                       uint64_t required, int handshake,
                                       uint32_t commands,
                                       uint32_t max_message_bytes,
                                       struct tr_memory_budget *memory_budget)
{
	const struct tr_reactor_config rc = {.max_connections = 2,
	                                     .command_capacity = commands,
	                                     .tx_item_capacity = 8,
	                                     .control_tx_item_capacity =
	                                             CONTROL,
	                                     .rx_buffer_count = 16,
	                                     .rx_buffer_size = FRAME,
	                                     .max_payload_len = FRAME};
	struct tr_channel_config cc = {
	        .role = TR_CHANNEL_SERVER,
	        .mode = split ? TR_CHANNEL_SPLIT_CONNECTIONS
	                      : TR_CHANNEL_SHARED_CONNECTION,
	        .max_streams = capacity,
	        .initial_window_bytes = WINDOW,
	        .window_update_threshold_bytes = 128,
	        .max_message_bytes = max_message_bytes,
	        .feature_bits = features,
	        .required_feature_bits = required,
	        .memory_budget = memory_budget};
	memset(f, 0, sizeof(*f));
	f->peer[0] = f->peer[1] = -1;
	f->split = split;
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	assert(tr_buffer_pool_init(&f->pool, 16, WINDOW) == TR_OK);
	assert(tr_buffer_pool_init(&f->assembly, 2, WINDOW) == TR_OK);
	cc.reassembly_pool = &f->assembly;
	assert(tr_reactor_create(&rc, NULL, NULL, NULL, &f->reactor) == TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	for (unsigned lane = 0; lane < (split ? 2U : 1U); ++lane) {
		int sockets[2];
		assert(socketpair(AF_UNIX,
		                  SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
		                  sockets) == 0);
		assert(tr_reactor_adopt_fd(f->reactor, sockets[0],
		                           &f->connection[lane]) == TR_OK);
		f->peer[lane] = sockets[1];
		/* 被测 sendmsg 只触达已交给 Reactor 的精确 fd。 */
		if (!lane)
			atomic_store(&partial_fd, sockets[0]);
	}
	if (!split)
		f->connection[1] = f->connection[0];
	assert(tr_channel_create(&cc, f->connection[0], f->connection[1],
	                         on_data, on_stream, on_channel, f,
	                         &f->channel) == TR_OK);
	for (unsigned lane = 0; lane < (split ? 2U : 1U); ++lane) {
		assert(receive_frame(f, lane).type == TR_FRAME_HELLO);
		if (handshake) {
			hello_with_max_message(f, lane, features, 0,
			                       max_message_bytes);
			assert(receive_frame(f, lane).type ==
			       TR_FRAME_HELLO_ACK);
		}
	}
	if (handshake) {
		assert(tr_channel_wait_ready(f->channel, TR_LANE_CONTROL,
		                             5000) == TR_OK);
		if (split)
			assert(tr_channel_wait_ready(f->channel, TR_LANE_BULK,
			                             5000) == TR_OK);
	}
}

static void init_fixture(struct fixture *f, uint32_t capacity, int split,
                         uint64_t features, uint64_t required, int handshake)
{
	init_fixture_with_commands(f, capacity, split, features, required,
	                           handshake, COMMANDS, WINDOW, NULL);
}

static void destroy_fixture(struct fixture *f)
{
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(!f->held);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(tr_channel_destroy(f->channel) == TR_OK);
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	atomic_store(&partial_fd, -1);
	atomic_store(&partial_phase, 0);
	atomic_store(&partial_type, TR_FRAME_STREAM_RESET);
	atomic_store(&partial_eagain, 0);
	atomic_store(&block_partial, 0);
	atomic_store(&block_all_tx, 0);
	atomic_store(&repeat_goaway_phase, 0);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	for (unsigned lane = 0; lane < (f->split ? 2U : 1U); ++lane)
		assert(close(f->peer[lane]) == 0);
	assert(tr_buffer_pool_free_count(&f->pool) == 16);
	assert(tr_buffer_pool_free_count(&f->assembly) == 2);
	tr_buffer_pool_destroy(&f->pool);
	tr_buffer_pool_destroy(&f->assembly);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static struct tr_stream_handle open_remote(struct fixture *f, unsigned lane,
                                           uint32_t id, unsigned count)
{
	send_frame(f, lane, TR_FRAME_STREAM_OPEN,
	           lane ? TR_FRAME_F_LANE_BULK : 0, id, WINDOW, NULL, 0);
	struct tr_frame_header h = receive_frame(f, lane);
	assert(h.type == TR_FRAME_WINDOW_UPDATE && h.stream_id == id);
	assert(h.message_id == WINDOW);
	wait_count(f, &f->opened, count);
	return f->stream[(count - 1U) % TRACKED_STREAMS];
}

struct reset_request {
	struct tr_stream_handle stream;
	int status;
	int fail_arm;
};

static int reset_owner(void *arg)
{
	struct reset_request *r = arg;
	fail_reset_arm = r->fail_arm;
	int ret = tr_stream_reset_on_owner(r->stream, r->status);
	fail_reset_arm = 0;
	return ret;
}

static int reactor_noop(void *arg)
{
	(void)arg;
	return TR_OK;
}

static int reset(struct fixture *f, struct tr_stream_handle stream, int status)
{
	struct reset_request r = {stream, status, 0};
	return tr_reactor_call(f->reactor, reset_owner, &r);
}

static void peer_reset(struct fixture *f, unsigned lane, uint32_t id,
                       int status)
{
	send_frame(f, lane, TR_FRAME_STREAM_RESET, 0, id, (uint64_t)-status,
	           NULL, 0);
}

static void finish_reset(struct fixture *f, unsigned lane, uint32_t id,
                         int local, int remote, unsigned count)
{
	struct tr_frame_header h;
	do {
		h = receive_frame(f, lane);
	} while (h.type != TR_FRAME_STREAM_RESET);
	assert(h.stream_id == id && h.message_id == (uint64_t)-local);
	peer_reset(f, lane, id, remote);
	wait_count(f, &f->closed, count);
}

static void test_late_and_reuse(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};
	struct tr_stream_flow_state flow;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle old = open_remote(&f, 0, 1, 1);
	/* 完整第一片已经占用真实重组池，再取消，不能仅覆盖零缓存路径。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 1, 1, bytes, 8);
	for (unsigned i = 0;; ++i) {
		assert(i < 5000);
		assert(tr_stream_get_flow_state(old, &flow) == TR_OK);
		if (flow.rx_received_bytes == 8)
			break;
		pause_ms();
	}
	assert(tr_buffer_pool_free_count(&f.assembly) == 1);
	assert(reset(&f, old, TR_ERR_CLOSED) == TR_OK);
	assert(tr_buffer_pool_free_count(&f.assembly) == 2);
	assert(reset(&f, old, TR_ERR_TIMEOUT) == TR_OK);
	assert(count(&f, &f.resets) == 1 && f.reset_status[0] == TR_ERR_CLOSED);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 1, bytes, 8);
	send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, 1, WINDOW + 8, NULL, 0);
	barrier(&f, 0);
	assert(tr_stream_get_flow_state(old, &flow) == TR_OK);
	assert(flow.rx_received_bytes == 16 && flow.rx_consumed_bytes == 16);
	assert(flow.rx_advertised_limit == WINDOW);
	assert(count(&f, &f.received) == 0 && count(&f, &f.writable) == 0);
	/* barrier 已读取本端 RESET；对端以另一原因主动 RESET 也合法。 */
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	wait_count(&f, &f.closed, 1);
	assert(tr_channel_active_streams(f.channel) == 0);
	struct tr_stream_handle fresh = open_remote(&f, 0, 3, 2);
	assert(fresh.slot == old.slot && fresh.generation != old.generation);
	assert(reset(&f, old, TR_ERR_CLOSED) == TR_ERR_STALE);
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	barrier(&f, 0);
	assert(tr_channel_active_streams(f.channel) == 1 &&
	       count(&f, &f.resets) == 1);
	assert(count(&f, &f.down) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(reset(&f, fresh, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2);
	destroy_fixture(&f);
}

static void test_tombstone_connection_generation(void)
{
	struct fixture f;
	struct tr_stream_handle old_stream;
	struct tr_conn_handle old_connection, new_connection;
	enum tr_connection_state connection_state;
	enum tr_channel_lane_state lane_state = TR_CHANNEL_LANE_UP;
	unsigned down_before;
	int slot_free = 0;
	int sockets[2];
	init_fixture(&f, CAPACITY, 0, TR_CHANNEL_FEATURE_STREAM_RESET,
	             TR_CHANNEL_FEATURE_STREAM_RESET, 1);
	old_stream = open_remote(&f, 0, 1, 1);
	old_connection = f.connection[0];
	assert(reset(&f, old_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);

	/* 先让旧连接完全退役；墓碑仍留在逻辑 Channel 的有界历史里。 */
	assert(close(f.peer[0]) == 0);
	f.peer[0] = f.peer[1] = -1;
	for (unsigned i = 0; i < 5000U; ++i) {
		assert(tr_channel_get_lane_state(f.channel, TR_LANE_CONTROL,
		                                &lane_state) == TR_OK);
		if (lane_state == TR_CHANNEL_LANE_DOWN)
			break;
		pause_ms();
	}
	assert(lane_state == TR_CHANNEL_LANE_DOWN);
	wait_count(&f, &f.down, 1U);
	for (unsigned i = 0; i < 5000U; ++i) {
		int ret = tr_reactor_get_connection_state(old_connection,
		                                        &connection_state);
		if (ret == TR_ERR_STALE) {
			slot_free = 1;
			break;
		}
		assert(ret == TR_OK);
		if (connection_state == TR_CONN_FREE) {
			slot_free = 1;
			break;
		}
		pause_ms();
	}
	assert(slot_free);

	/* 同一 Reactor 槽位重用，但新连接必须使用不同代次。 */
	assert(socketpair(AF_UNIX,
	                  SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
	                  sockets) == 0);
	assert(tr_reactor_adopt_fd(f.reactor, sockets[0], &new_connection) ==
	       TR_OK);
	assert(new_connection.slot == old_connection.slot);
	assert(new_connection.generation != old_connection.generation);
	f.connection[0] = f.connection[1] = new_connection;
	f.peer[0] = f.peer[1] = sockets[1];
	atomic_store(&partial_fd, sockets[0]);
	assert(tr_channel_replace_connection(f.channel, TR_LANE_CONTROL,
	                                    new_connection) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_HELLO);
	hello(&f, 0, TR_CHANNEL_FEATURE_STREAM_RESET, 0);
	assert(receive_frame(&f, 0).type == TR_FRAME_HELLO_ACK);
	assert(tr_channel_wait_ready(f.channel, TR_LANE_CONTROL, 5000) ==
	       TR_OK);

	/* 旧连接的同 ID/原因墓碑不得吞掉新代次上的未知 RESET。 */
	down_before = count(&f, &f.down);
	peer_reset(&f, 0, 1, TR_ERR_CLOSED);
	wait_count(&f, &f.down, down_before + 1U);
	assert(f.down_status == TR_ERR_STATE);
	assert(tr_channel_active_streams(f.channel) == 0U);
	destroy_fixture(&f);
}

static void test_tombstone_split_connection_identity(void)
{
	struct fixture f;
	enum tr_channel_lane_state control_state = TR_CHANNEL_LANE_UP;
	enum tr_channel_lane_state bulk_state = TR_CHANNEL_LANE_UP;
	uint8_t bytes[8] = {1};
	init_fixture(&f, CAPACITY, 1, TR_CHANNEL_FEATURE_STREAM_RESET,
	             TR_CHANNEL_FEATURE_STREAM_RESET, 1);
	struct tr_stream_handle old = open_remote(&f, 0, 1, 1);
	assert(reset(&f, old, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);

	/* CONTROL 的墓碑不得跨物理连接吸收 BULK 上相同 ID/原因的 RESET。 */
	peer_reset(&f, 1, 1, TR_ERR_CLOSED);
	wait_count(&f, &f.down, 1);
	assert(f.down_status == TR_ERR_STATE);
	for (unsigned i = 0; i < 5000U; ++i) {
		assert(tr_channel_get_lane_state(f.channel, TR_LANE_BULK,
		                                &bulk_state) == TR_OK);
		if (bulk_state == TR_CHANNEL_LANE_DOWN)
			break;
		pause_ms();
	}
	assert(bulk_state == TR_CHANNEL_LANE_DOWN);
	assert(tr_channel_get_lane_state(f.channel, TR_LANE_CONTROL,
	                                &control_state) == TR_OK);
	assert(control_state == TR_CHANNEL_LANE_UP);
	assert(count(&f, &f.resets) == 1U && count(&f, &f.closed) == 1U);
	assert(tr_channel_active_streams(f.channel) == 0U);

	/* CONTROL 物理连接仍可继续准入、交付并正常终止另一条流。 */
	struct tr_stream_handle live = open_remote(&f, 0, 3, 2);
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3, 1, bytes,
	           sizeof(bytes));
	wait_count(&f, &f.received, 1U);
	assert(count(&f, &f.down) == 1U);
	assert(reset(&f, live, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2);
	assert(tr_channel_active_streams(f.channel) == 0U);
	destroy_fixture(&f);
}

static void test_tombstone_same_id_across_split_connections(void)
{
	struct fixture f;
	struct tr_stream_handle control, bulk;
	init_fixture(&f, CAPACITY, 1, TR_CHANNEL_FEATURE_STREAM_RESET,
	             TR_CHANNEL_FEATURE_STREAM_RESET, 1);

	/* Split 连接各自推进 OPEN 前沿，同一个 ID 可在另一连接上重用。 */
	control = open_remote(&f, 0, 1U, 1U);
	assert(reset(&f, control, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1U, TR_ERR_CLOSED, TR_ERR_CLOSED, 1U);
	bulk = open_remote(&f, 1, 1U, 2U);
	assert(reset(&f, bulk, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 1, 1U, TR_ERR_CLOSED, TR_ERR_CLOSED, 2U);

	/* 两个同 ID 墓碑都只吸收其所属物理连接上的同原因重复 RESET。 */
	peer_reset(&f, 0, 1U, TR_ERR_CLOSED);
	barrier(&f, 0);
	peer_reset(&f, 1, 1U, TR_ERR_CLOSED);
	barrier(&f, 1);
	assert(count(&f, &f.down) == 0U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	destroy_fixture(&f);
}

static void test_tombstone_long_fifo_turnover(void)
{
	struct fixture f;
	const unsigned stream_count = CAPACITY * 32U;
	uint32_t newest_id = 0U;
	uint32_t oldest_resident_id =
	        2U * (stream_count - CAPACITY) + 1U;
	init_fixture(&f, CAPACITY, 0, TR_CHANNEL_FEATURE_STREAM_RESET,
	             TR_CHANNEL_FEATURE_STREAM_RESET, 1);

	/* 在同一真实连接上周转 32 次完整墓碑环，不重建 Channel/Reactor。 */
	for (unsigned i = 0U; i < stream_count; ++i) {
		uint32_t stream_id = 2U * i + 1U;
		struct tr_stream_handle stream =
		        open_remote(&f, 0, stream_id, i + 1U);
		assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
		finish_reset(&f, 0, stream_id, TR_ERR_CLOSED, TR_ERR_CLOSED,
		             i + 1U);
		newest_id = stream_id;
	}

	/* FIFO 当前最老和最新墓碑均命中；环外最早墓碑已淘汰。 */
	peer_reset(&f, 0, oldest_resident_id, TR_ERR_CLOSED);
	barrier(&f, 0);
	peer_reset(&f, 0, newest_id, TR_ERR_CLOSED);
	barrier(&f, 0);
	assert(count(&f, &f.down) == 0U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	peer_reset(&f, 0, 1U, TR_ERR_CLOSED);
	wait_count(&f, &f.down, 1U);
	assert(f.down_status == TR_ERR_STATE);
	assert(count(&f, &f.closed) == stream_count);
	destroy_fixture(&f);
}

static uint64_t timespec_ns(const struct timespec *value)
{
	return (uint64_t)value->tv_sec * UINT64_C(1000000000) +
	       (uint64_t)value->tv_nsec;
}

static int compare_u64_ns(const void *left, const void *right)
{
	uint64_t a = *(const uint64_t *)left;
	uint64_t b = *(const uint64_t *)right;
	return (a > b) - (a < b);
}

static uint64_t percentile_u64_ns(const uint64_t *sorted, unsigned count,
                                  unsigned percentile)
{
	size_t rank;
	assert(sorted && count > 0U && percentile > 0U && percentile <= 100U);
	rank = ((size_t)count * percentile + 99U) / 100U;
	return sorted[rank - 1U];
}

static void test_tombstone_real_channel_capacity(void)
{
	struct fixture f;
	const unsigned channel_capacity =
#if defined(__SANITIZE_THREAD__)
	        5000U;
#else
	        40000U;
#endif
	uint32_t newest_id = 0U;
	uint64_t *reset_rtt_ns =
	        calloc(channel_capacity, sizeof(*reset_rtt_ns));
	struct timespec loop_wall_start, loop_wall_end;
	struct timespec loop_cpu_start, loop_cpu_end;
	uint64_t wall_ns, cpu_ns;
	assert(reset_rtt_ns);
	init_fixture(&f, channel_capacity, 0, TR_CHANNEL_FEATURE_STREAM_RESET,
	             TR_CHANNEL_FEATURE_STREAM_RESET, 1);
	f.circular_stream_tracking = 1;

	/* TSan 降低容量以守住目标运行时限；Release/ASan 按生产最大配置运行。 */
	assert(clock_gettime(CLOCK_MONOTONIC, &loop_wall_start) == 0);
	assert(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &loop_cpu_start) == 0);
	for (unsigned i = 0U; i < channel_capacity; ++i) {
		uint32_t stream_id = 2U * i + 1U;
		struct tr_stream_handle stream =
		        open_remote(&f, 0, stream_id, i + 1U);
		struct timespec reset_start, reset_end;
		assert(clock_gettime(CLOCK_MONOTONIC, &reset_start) == 0);
		assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
		finish_reset(&f, 0, stream_id, TR_ERR_CLOSED, TR_ERR_CLOSED,
		             i + 1U);
		assert(clock_gettime(CLOCK_MONOTONIC, &reset_end) == 0);
		reset_rtt_ns[i] =
		        timespec_ns(&reset_end) - timespec_ns(&reset_start);
		newest_id = stream_id;
	}
	assert(clock_gettime(CLOCK_MONOTONIC, &loop_wall_end) == 0);
	assert(clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &loop_cpu_end) == 0);
	wall_ns = timespec_ns(&loop_wall_end) - timespec_ns(&loop_wall_start);
	cpu_ns = timespec_ns(&loop_cpu_end) - timespec_ns(&loop_cpu_start);
	assert(wall_ns > 0U);

	/* 最老和最新墓碑都仍驻留；再加入一条后最老墓碑必须淘汰。 */
	peer_reset(&f, 0, 1U, TR_ERR_CLOSED);
	barrier(&f, 0);
	peer_reset(&f, 0, newest_id, TR_ERR_CLOSED);
	barrier(&f, 0);
	struct tr_stream_handle rollover =
	        open_remote(&f, 0, newest_id + 2U, channel_capacity + 1U);
	assert(reset(&f, rollover, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, newest_id + 2U, TR_ERR_CLOSED, TR_ERR_CLOSED,
	             channel_capacity + 1U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	peer_reset(&f, 0, newest_id + 2U, TR_ERR_CLOSED);
	barrier(&f, 0);
	peer_reset(&f, 0, 1U, TR_ERR_CLOSED);
	wait_count(&f, &f.down, 1U);
	assert(f.down_status == TR_ERR_STATE);
	assert(count(&f, &f.closed) == channel_capacity + 1U);
	printf("stream_reset: real Channel tombstone capacity %u passed\n",
	       channel_capacity);
	qsort(reset_rtt_ns, channel_capacity, sizeof(*reset_rtt_ns),
	      compare_u64_ns);
	printf("stream_reset: RESET round-trip us p50=%.2f p95=%.2f p99=%.2f; "
	       "OPEN/RESET loop wall_ms=%.2f process_cpu_ms=%.2f "
	       "open_reset/s=%.0f\n",
	       (double)percentile_u64_ns(reset_rtt_ns, channel_capacity, 50U) /
	               1000.0,
	       (double)percentile_u64_ns(reset_rtt_ns, channel_capacity, 95U) /
	               1000.0,
	       (double)percentile_u64_ns(reset_rtt_ns, channel_capacity, 99U) /
	               1000.0,
	       (double)wall_ns / 1000000.0, (double)cpu_ns / 1000000.0,
	       (double)channel_capacity * 1000000000.0 / (double)wall_ns);
	free(reset_rtt_ns);
	destroy_fixture(&f);
}

static void test_local_stream_id_exhaustion(void)
{
	uint32_t next = UINT32_MAX - 2U;
	uint32_t stream_id = 0U;

	/* Client 的最后两个奇数标识都可分配，之后必须饱和而不是回绕。 */
	assert(tr_channel_local_stream_id_take(&next, &stream_id) == TR_OK);
	assert(stream_id == UINT32_MAX - 2U && next == UINT32_MAX);
	assert(tr_channel_local_stream_id_take(&next, &stream_id) == TR_OK);
	assert(stream_id == UINT32_MAX && next == 0U);
	stream_id = 17U;
	assert(tr_channel_local_stream_id_take(&next, &stream_id) ==
	       TR_ERR_CLOSED);
	assert(stream_id == 17U && next == 0U);

	/* Server 的最高偶数标识同样只分配一次，并保留耗尽状态。 */
	next = UINT32_MAX - 1U;
	assert(tr_channel_local_stream_id_take(&next, &stream_id) == TR_OK);
	assert(stream_id == UINT32_MAX - 1U && next == 0U);
	stream_id = 19U;
	assert(tr_channel_local_stream_id_take(&next, &stream_id) ==
	       TR_ERR_CLOSED);
	assert(stream_id == 19U && next == 0U);
}

static void test_local_stream_id_exhaustion_drains(void)
{
	struct fixture f;
	struct tr_stream_handle stream = {
	        .channel = NULL, .slot = 77U, .generation = 91U};
	enum tr_channel_state state;
	struct tr_frame_header h;

	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	atomic_store_explicit(&force_local_stream_id_exhaustion, 1,
	                      memory_order_release);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &stream) ==
	       TR_ERR_CLOSED);
	assert(atomic_load_explicit(&force_local_stream_id_exhaustion,
	                            memory_order_acquire) == 0);
	assert(stream.channel == NULL && stream.slot == 77U &&
	       stream.generation == 91U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_GOAWAY && h.stream_id == 0U &&
	       h.message_id == 0U);
	assert(tr_channel_get_state(f.channel, &state) == TR_OK);
	assert(state == TR_CHANNEL_DRAINING);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	assert(tr_channel_wait_drained(f.channel, 5000) == TR_OK);
	assert(tr_channel_get_state(f.channel, &state) == TR_OK);
	assert(state == TR_CHANNEL_DRAINED);
	destroy_fixture(&f);
}

static struct tr_buffer *payload(struct fixture *f, uint32_t size)
{
	struct tr_buffer *buffer;
	assert(tr_buffer_acquire(&f->pool, size, &buffer) == TR_OK);
	memset(buffer->data, 0xa5, size);
	buffer->len = size;
	return buffer;
}

static int pressure_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_buffer *body = payload(f, WINDOW);
	assert(tr_stream_send(f->stream[0], body) == TR_OK);
	body = payload(f, 1);
	assert(tr_stream_send(f->stream[0], body) == TR_AGAIN);
	tr_buffer_release(body);
	for (unsigned i = 0; i < CONTROL; ++i)
		assert(tr_reactor_send(f->connection[0], TR_FRAME_PING, 0, 0,
		                       100 + i, NULL) == TR_OK);
	assert(tr_reactor_send(f->connection[0], TR_FRAME_PING, 0, 0, 200,
	                       NULL) == TR_AGAIN);
	unsigned count = 0;
	while (tr_reactor_resume_rx(f->connection[0]) == TR_OK)
		assert(++count <= COMMANDS);
	assert(count);
	/* 普通控制池、命令环和目标流 DATA 信用均耗尽，两个终止预留
	 * 仍独立接受。第二条流必须由既有维护定时器公平取得 RESET 槽。 */
	assert(tr_stream_reset_on_owner(f->stream[0], TR_ERR_CLOSED) == TR_OK);
	assert(tr_reactor_send_goaway_on_owner(f->connection[0], 0) == TR_OK);
	assert(tr_stream_reset_on_owner(f->stream[1], TR_ERR_TIMEOUT) == TR_OK);
	return TR_OK;
}

static void test_reservation_and_fifo(void)
{
	struct fixture f;
	unsigned bytes = 0, resets = 0, pings = 0, goaway = 0;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	(void)open_remote(&f, 0, 1, 1);
	(void)open_remote(&f, 0, 3, 2);
	assert(tr_reactor_call(f.reactor, pressure_owner, &f) == TR_OK);
	while (resets != 2 || !goaway) {
		struct tr_frame_header h = receive_frame(&f, 0);
		if (h.type == TR_FRAME_DATA)
			bytes += h.payload_len;
		else if (h.type == TR_FRAME_PING)
			++pings;
		else if (h.type == TR_FRAME_GOAWAY) {
			assert(resets >= 1);
			++goaway;
		} else if (h.type == TR_FRAME_STREAM_RESET) {
			assert(bytes == WINDOW && pings == CONTROL);
			assert(h.stream_id == (resets ? 3U : 1U));
			++resets;
		} else
			assert(0);
	}
	peer_reset(&f, 0, 1, TR_ERR_CLOSED);
	peer_reset(&f, 0, 3, TR_ERR_TIMEOUT);
	wait_count(&f, &f.closed, 2);
	assert(count(&f, &f.resets) == 2 && count(&f, &f.down) == 0);
	assert(tr_channel_active_streams(f.channel) == 0);
	destroy_fixture(&f);
}

static int close_owner(void *arg)
{
	return tr_stream_close(*(struct tr_stream_handle *)arg);
}

static void test_opening_and_refusal(void)
{
	struct fixture f;
	struct tr_stream_handle stream;
	struct tr_stream_flow_state flow;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &stream) == TR_OK);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_OPEN);
	uint32_t id = h.stream_id;
	/* 尚未取得对端槽位责任时只能保留本地终止请求，不能先发 RESET。 */
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_AGAIN);
	assert(count(&f, &f.resets) == 0 && count(&f, &f.opened) == 0);
	assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
	assert(flow.tx_send_limit == 0 &&
	       tr_channel_active_streams(f.channel) == 1);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	wait_count(&f, &f.errors, 1);
	assert(count(&f, &f.errors) == 1 && f.error_status == TR_ERR_CLOSED);
	assert(count(&f, &f.remote_closed) == 0 && count(&f, &f.closed) == 0);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) ==
	       TR_ERR_STALE);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_GOAWAY && h.message_id == 0);
	assert(tr_channel_active_streams(f.channel) == 0);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_ERR_STALE);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &stream) ==
	       TR_ERR_CLOSED);
	destroy_fixture(&f);

	/* GOAWAY 后不重开原连接；独立会话证明准入后仍能选择 RESET。 */
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &stream) == TR_OK);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_OPEN && h.stream_id == id);
	send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, h.stream_id, WINDOW, NULL,
	           0);
	wait_count(&f, &f.opened, 1);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, h.stream_id, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_opening_local_close(void)
{
	struct fixture f;
	struct tr_stream_handle stream;
	init_fixture(&f, 1, 0, 1, 1, 1);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &stream) == TR_OK);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_OPEN);
	uint32_t id = h.stream_id;
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_AGAIN);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_AGAIN);
	/* PONG 是真实 RX/FIFO 屏障，前面不能出现未获准流的 CLOSE。 */
	send_frame(&f, 0, TR_FRAME_PING, 0, 0, 999, NULL, 0);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_PONG && h.message_id == 999);
	send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, id, WINDOW, NULL, 0);
	wait_count(&f, &f.opened, 1);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_OK);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_CLOSE && h.stream_id == id);
	/* 准入后已经接受的本地普通 CLOSE 仍不能改成主动 RESET。 */
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_ERR_CLOSED);
	assert(count(&f, &f.resets) == 0);
	send_frame(&f, 0, TR_FRAME_STREAM_CLOSE, 0, id, 0, NULL, 0);
	wait_count(&f, &f.closed, 1);
	/* 普通 CLOSE 的回调返回后才退役槽；PONG 证明该 RX 回调已退出。 */
	barrier(&f, 0);
	assert(tr_channel_active_streams(f.channel) == 0);
	destroy_fixture(&f);
}

static void test_receive_isolation(int resource)
{
	struct fixture f;
	uint8_t bytes[8] = {1};
	struct tr_stream_flow_state flow;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	(void)open_remote(&f, 0, 1, 1);
	(void)open_remote(&f, 0, 3, 2);
	struct tr_stream_handle broken =
	        resource ? open_remote(&f, 0, 5, 3) : f.stream[0];
	uint32_t id = resource ? 5U : 1U;
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 1, 1, bytes, 8);
	if (resource)
		send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 3, 1, bytes,
		           8);
	barrier(&f, 0);
	assert(tr_buffer_pool_free_count(&f.assembly) == (resource ? 0U : 1U));
	/* 真实共享池耗尽，或仍在重组时重复 FIRST；两者只终止明确目标流。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, id, 1, bytes, 8);
	wait_count(&f, &f.resets, 1);
	struct tr_frame_header h = receive_frame(&f, 0);
	int reason = resource ? TR_ERR_NOMEM : TR_ERR_STATE;
	assert(h.type == TR_FRAME_STREAM_RESET && h.stream_id == id);
	assert(h.message_id == (uint64_t)-reason &&
	       f.reset_status[0] == reason);
	assert(tr_buffer_pool_free_count(&f.assembly) == (resource ? 0U : 2U));
	/* 资源失败仍保持序号；已失步的逻辑消息仅受精确流/线帧/冻结信用约束。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, id,
	           resource ? 1U : 777U, bytes, 8);
	if (resource) {
		send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 1, bytes,
		           8);
		send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 3, 1, bytes,
		           8);
	} else {
		send_frame(&f, 0, TR_FRAME_DATA,
		           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3, 1, bytes, 8);
	}
	wait_count(&f, &f.received, resource ? 2U : 1U);
	barrier(&f, 0);
	assert(tr_buffer_pool_free_count(&f.assembly) == 2);
	assert(tr_stream_get_flow_state(broken, &flow) == TR_OK);
	assert(flow.rx_received_bytes == (resource ? 16U : 24U));
	assert(flow.rx_consumed_bytes == flow.rx_received_bytes);
	assert(flow.rx_advertised_limit == WINDOW);
	assert(count(&f, &f.down) == 0 && count(&f, &f.resets) == 1);
	peer_reset(&f, 0, id, reason);
	wait_count(&f, &f.closed, 1);
	/* 同一个物理连接上的另一调用在失败流退役后仍能继续。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           2, bytes, 8);
	wait_count(&f, &f.received, resource ? 3U : 2U);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_data_type_flag_isolation(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};

	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	(void)open_remote(&f, 0, 1, 1);
	(void)open_remote(&f, 0, 3, 2);
	/* LANE_BULK 是有效线帧标志，但 DATA 不允许携带；流身份已知。 */
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST | TR_FRAME_F_LANE_BULK, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.resets, 1);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_RESET && h.stream_id == 1);
	assert(h.message_id == (uint64_t)-TR_ERR_STATE);
	assert(f.reset_status[0] == TR_ERR_STATE);
	assert(count(&f, &f.received) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(count(&f, &f.down) == 0);
	peer_reset(&f, 0, 1, TR_ERR_STATE);
	wait_count(&f, &f.closed, 1);
	barrier(&f, 0);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_data_type_flag_without_reset(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};

	init_fixture(&f, CAPACITY, 0, 0, 0, 1);
	(void)open_remote(&f, 0, 1, 1);
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST | TR_FRAME_F_LANE_BULK, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.down, 1);
	assert(count(&f, &f.resets) == 0);
	assert(f.down_status == TR_ERR_STATE);
	destroy_fixture(&f);
}

static void test_closed_receive_resource_failure(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};
	struct tr_buffer *occupied[2];
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	(void)open_remote(&f, 0, 3, 2);
	for (unsigned i = 0; i < 2; ++i)
		assert(tr_buffer_acquire(&f.assembly, WINDOW, &occupied[i]) ==
		       TR_OK);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_CLOSE);
	/* 已发普通 CLOSE 后不能再主动 RESET；此极端下明确牺牲连接，
	 * 不伪造一个仅通知本地而没有双向静默屏障的收尾路径。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 1, 1, bytes, 8);
	wait_count(&f, &f.down, 2);
	assert(count(&f, &f.resets) == 0 && f.down_status == TR_ERR_NOMEM);
	assert(tr_channel_active_streams(f.channel) == 0);
	for (unsigned i = 0; i < 2; ++i)
		tr_buffer_release(occupied[i]);
	destroy_fixture(&f);
}

static void test_frozen_discard_credit(int unordered)
{
	struct fixture f;
	uint8_t bytes[FRAME] = {1};
	struct tr_stream_flow_state flow;
	uint32_t consumed = 0;
	uint64_t message_id = 1;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	if (unordered) {
		/* 没有 FIRST 的完整尾片先触发流级语义终止。 */
		send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 1, bytes,
		           8);
		consumed = 8;
	} else {
		assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
	}
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_RESET);
	while (consumed < WINDOW) {
		uint32_t size = WINDOW - consumed;
		if (size > FRAME)
			size = FRAME;
		send_frame(&f, 0, TR_FRAME_DATA,
		           unordered ? 0 : TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
		           1, message_id++, bytes, size);
		consumed += size;
	}
	barrier(&f, 0);
	assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == WINDOW &&
	       flow.rx_consumed_bytes == WINDOW);
	assert(flow.rx_advertised_limit == WINDOW);
	assert(count(&f, &f.received) == 0 && count(&f, &f.down) == 0);
	/* 即使已失步也不能补信用或宽松吞无限字节；再多一个字节即连接失败。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           message_id, bytes, 1);
	wait_count(&f, &f.down, 2);
	assert(f.down_status == TR_ERR_BAD_LENGTH);
	assert(count(&f, &f.resets) == 1);
	destroy_fixture(&f);
}

static void test_reset_rejects_compressed_data_lane_flag(void)
{
	struct fixture f;
	uint8_t bytes[1] = {0};
	init_fixture(&f, CAPACITY, 1, 1, 1, 1);
	struct tr_stream_handle reset_stream = open_remote(&f, 0, 1, 1);
	struct tr_stream_handle live_stream = open_remote(&f, 1, 3, 2);

	assert(reset(&f, reset_stream, TR_ERR_CLOSED) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_RESET);
	/* COMPRESSED DATA 在 RESET 状态仍必须遵守 DATA 类型标志合同。
	 * 控制物理连接上的帧不能伪称来自 BULK lane。 */
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST | TR_FRAME_F_COMPRESSED |
	                   TR_FRAME_F_LANE_BULK,
	           1, 1, bytes, sizeof(bytes));
	wait_count(&f, &f.down, 1);
	assert(f.down_status == TR_ERR_BAD_FLAGS);
	assert(tr_channel_active_streams(f.channel) == 1U);

	/* 该错误只关闭对应 CONTROL 物理连接，BULK 上的流仍可交付。 */
	send_frame(&f, 1, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(count(&f, &f.down) == 1U);
	assert(reset(&f, live_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 1, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
	destroy_fixture(&f);
}

static int control_pressure_owner(void *arg)
{
	struct fixture *f = arg;
	for (unsigned i = 0; i < CONTROL; ++i)
		assert(tr_reactor_send(f->connection[0], TR_FRAME_PING, 0, 0,
		                       100 + i, NULL) == TR_OK);
	return TR_OK;
}

static int control_pressure_without_retry_owner(void *arg)
{
	int ret = control_pressure_owner(arg);
	fail_reset_arm = 1;
	return ret;
}

static void test_open_admission_boundaries(int initial_credit)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 1, 1, 1);
	if (initial_credit) {
		atomic_store(&block_all_tx, 1);
		assert(tr_reactor_call(
		               f.reactor,
		               initial_credit == 2
		                       ? control_pressure_without_retry_owner
		                       : control_pressure_owner,
		               &f) == TR_OK);
	} else {
		(void)open_remote(&f, 0, 1, 1);
	}
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 3, WINDOW, NULL, 0);
	if (initial_credit == 2) {
		/* 无法取得维护定时器责任时不能留下永久暂停或未确认流。 */
		wait_count(&f, &f.down, 2);
		assert(f.down_status == TR_ERR_SYS);
		assert(tr_channel_active_streams(f.channel) == 0);
		assert(count(&f, &f.opened) == 0 && count(&f, &f.resets) == 0);
		destroy_fixture(&f);
		return;
	}
	if (initial_credit == 1) {
		/* 控制池满时已有一个有界流槽承担重试；恢复后仍可正常 RESET，
		 * 不能提前交付 OPENED，也不能把暂时背压升级为整连接失败。 */
		struct tr_connection_stats stats;
		for (unsigned i = 0;; ++i) {
			assert(i < 5000);
			assert(tr_reactor_get_connection_stats(
			               f.connection[0], &stats) == TR_OK);
			if (stats.rx_paused)
				break;
			pause_ms();
		}
		assert(count(&f, &f.down) == 0 && count(&f, &f.opened) == 0);
		atomic_store(&block_all_tx, 0);
		for (unsigned i = 0; i < CONTROL; ++i) {
			struct tr_frame_header h = receive_frame(&f, 0);
			assert(h.type == TR_FRAME_PING &&
			       h.message_id == 100 + i);
		}
		struct tr_frame_header h = receive_frame(&f, 0);
		assert(h.type == TR_FRAME_WINDOW_UPDATE && h.stream_id == 3 &&
		       h.message_id == WINDOW);
		wait_count(&f, &f.opened, 1);
		assert(reset(&f, f.stream[0], TR_ERR_CLOSED) == TR_OK);
		finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
		assert(count(&f, &f.down) == 0);
		destroy_fixture(&f);
		return;
	}
	/* 容量不足必须拒绝尚未受理的新流，保留已有流与后续准入能力。 */
	fputs("capacity refusal: expect REFUSED, keep stream 1 alive\n",
	      stderr);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 3 &&
	       h.message_id == (uint64_t)-TR_ERR_NOMEM);
	for (uint32_t id = 5; id < 21; id += 2) {
		send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, id, WINDOW, NULL, 0);
		h = receive_frame(&f, 0);
		assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == id &&
		       h.message_id == (uint64_t)-TR_ERR_NOMEM);
		assert(tr_channel_active_streams(f.channel) == 1);
	}
	uint8_t bytes[8] = {1};
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(count(&f, &f.down) == 0 && count(&f, &f.opened) == 1);
	assert(reset(&f, f.stream[0], TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
	(void)open_remote(&f, 0, 21, 2);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void wait_rx_hold(struct fixture *f)
{
	for (unsigned i = 0;; ++i) {
		struct tr_connection_stats stats;
		assert(i < 5000);
		assert(tr_reactor_get_connection_stats(f->connection[0],
		                                       &stats) == TR_OK);
		if (stats.rx_paused)
			return;
		pause_ms();
	}
}

static void test_refusal_pressure(unsigned ending)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 1, 1, 1);
	(void)open_remote(&f, 0, 1, 1);
	atomic_store(&block_all_tx, 1);
	assert(tr_reactor_call(f.reactor,
	                       ending == 4
	                               ? control_pressure_without_retry_owner
	                               : control_pressure_owner,
	                       &f) == TR_OK);
	/* 首次拒绝使用真实预留，第二次遇到忙预留，不能分配第二区域。 */
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 3, WINDOW, NULL, 0);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 5, WINDOW, NULL, 0);
	if (ending == 4) {
		wait_count(&f, &f.down, 2);
		assert(f.down_status == TR_ERR_SYS);
		assert(tr_channel_active_streams(f.channel) == 0);
		destroy_fixture(&f);
		return;
	}
	uint8_t bytes[8] = {1};
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	wait_rx_hold(&f);
	assert(tr_channel_active_streams(f.channel) == 1);
	assert(count(&f, &f.down) == 0 && count(&f, &f.received) == 0);
	if (ending == 2) {
		assert(tr_channel_detach_for_finalize(f.channel) == TR_OK);
		struct tr_connection_stats stats;
		assert(tr_reactor_get_connection_stats(f.connection[0],
		                                       &stats) == TR_OK);
		assert(!stats.rx_paused);
		destroy_fixture(&f);
		return;
	}
	if (ending == 3) {
		assert(tr_reactor_stop(f.reactor) == TR_OK);
		destroy_fixture(&f);
		return;
	}
	if (ending == 1)
		assert(tr_channel_begin_drain(f.channel) == TR_OK);
	atomic_store(&block_all_tx, 0);
	for (unsigned i = 0; i < CONTROL; ++i) {
		struct tr_frame_header h = receive_frame(&f, 0);
		assert(h.type == TR_FRAME_PING && h.message_id == 100 + i);
	}
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 3);
	h = receive_frame(&f, 0);
	if (ending == 1) {
		assert(h.type == TR_FRAME_GOAWAY && h.message_id == 1);
		send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	} else {
		assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 5);
	}
	wait_count(&f, &f.received, 1);
	assert(reset(&f, f.stream[0], TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
	if (ending == 1)
		assert(tr_channel_wait_drained(f.channel, 5000) == TR_OK);
	else
		(void)open_remote(&f, 0, 7, 2);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_refusal_reset_fairness(void)
{
	struct fixture f;
	init_fixture(&f, 2, 0, 1, 1, 1);
	struct tr_stream_handle first = open_remote(&f, 0, 1, 1);
	struct tr_stream_handle second = open_remote(&f, 0, 3, 2);
	atomic_store(&block_all_tx, 1);
	assert(reset(&f, first, TR_ERR_CLOSED) == TR_OK);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 5, WINDOW, NULL, 0);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 7, WINDOW, NULL, 0);
	wait_rx_hold(&f);
	assert(reset(&f, second, TR_ERR_CLOSED) == TR_OK);
	atomic_store(&partial_type, TR_FRAME_STREAM_REFUSED);
	atomic_store(&partial_phase, 1);
	atomic_store(&block_partial, 1);
	atomic_store(&block_all_tx, 0);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_RESET && h.stream_id == 1);
	for (unsigned i = 0; !atomic_load(&partial_eagain); ++i) {
		assert(i < 5000);
		pause_ms();
	}
	wait_rx_hold(&f);
	atomic_store(&block_partial, 0);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 5);
	/* 连续的新 OPEN 不能不断抢走预留，使已接受的 RESET 永久等待。 */
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_RESET && h.stream_id == 3);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 7);
	peer_reset(&f, 0, 1, TR_ERR_CLOSED);
	peer_reset(&f, 0, 3, TR_ERR_CLOSED);
	wait_count(&f, &f.closed, 2);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_receive_refusal(unsigned invalid)
{
	struct fixture f;
	struct tr_stream_handle stream, next;
	struct tr_stream_flow_state flow;
	init_fixture(&f, 1, invalid == 4, 1, 1, 1);
	enum tr_lane lane = invalid == 4 ? TR_LANE_BULK : TR_LANE_CONTROL;
	assert(tr_stream_open(f.channel, lane, &stream) == TR_OK);
	struct tr_frame_header h = receive_frame(&f, (unsigned)lane);
	assert(h.type == TR_FRAME_STREAM_OPEN);
	uint32_t id = h.stream_id;
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_AGAIN);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_AGAIN);
	if (invalid == 2) {
		send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, id, WINDOW, NULL,
		           0);
		wait_count(&f, &f.opened, 1);
	}
	send_frame(&f, 0, TR_FRAME_STREAM_REFUSED, 0,
	           invalid == 1 ? id + 2 : id, (uint64_t)-TR_ERR_NOMEM, NULL,
	           0);
	if (invalid == 1 || invalid == 2 || invalid == 4) {
		wait_count(&f, &f.down, 1);
		assert(count(&f, &f.refused) == 0);
		destroy_fixture(&f);
		return;
	}
	wait_count(&f, &f.refused, 1);
	assert(tr_stream_get_flow_state(stream, &flow) == TR_ERR_STALE);
	assert(tr_channel_active_streams(f.channel) == 0);
	/* 不回 RESET/CLOSE；后续帧必须是新 OPEN。旧代次不能命中新槽。 */
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &next) == TR_OK);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_OPEN && h.stream_id > id);
	assert(next.slot == stream.slot &&
	       next.generation != stream.generation);
	if (invalid == 3) {
		send_frame(&f, 0, TR_FRAME_STREAM_REFUSED, 0, id,
		           (uint64_t)-TR_ERR_NOMEM, NULL, 0);
		wait_count(&f, &f.down, 2);
		assert(count(&f, &f.refused) == 1);
	} else {
		send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, h.stream_id,
		           WINDOW, NULL, 0);
		wait_count(&f, &f.opened, 1);
		assert(count(&f, &f.down) == 0 && count(&f, &f.errors) == 0);
	}
	destroy_fixture(&f);
}

static int refusal_full_queues_owner(void *arg)
{
	struct fixture *f = arg;
	assert(control_pressure_owner(f) == TR_OK);
	/* owner 内填满真实命令环，返回前不可能被调度器消费。 */
	for (unsigned i = 0; i < COMMANDS - CONTROL; ++i)
		assert(tr_reactor_resume_rx(f->connection[0]) == TR_OK);
	assert(tr_reactor_resume_rx(f->connection[0]) == TR_AGAIN);
	assert(tr_reactor_send_stream_refused_on_owner(f->connection[0], 3) ==
	       TR_OK);
	assert(tr_reactor_send_stream_refused_on_owner(f->connection[0], 3) ==
	       TR_OK);
	assert(tr_reactor_send_stream_reset_on_owner(f->connection[0], 3,
	                                             TR_ERR_NOMEM) == TR_AGAIN);
	assert(tr_reactor_send_stream_refused_on_owner(f->connection[0], 5) ==
	       TR_AGAIN);
	return TR_OK;
}

static void test_refusal_full_queues(void)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 0, 0, 1);
	atomic_store(&block_all_tx, 1);
	assert(tr_reactor_call(f.reactor, refusal_full_queues_owner, &f) ==
	       TR_OK);
	atomic_store(&block_all_tx, 0);
	for (unsigned i = 0; i < CONTROL; ++i) {
		struct tr_frame_header h = receive_frame(&f, 0);
		assert(h.type == TR_FRAME_PING && h.message_id == 100 + i);
	}
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 3);
	barrier(&f, 0);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_refusal_without_reset_feature(void)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 0, 0, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 3, WINDOW, NULL, 0);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 3);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_CLOSE);
	send_frame(&f, 0, TR_FRAME_STREAM_CLOSE, 0, 1, 0, NULL, 0);
	barrier(&f, 0);
	(void)open_remote(&f, 0, 5, 2);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_refusal_split_progress(void)
{
	struct fixture f;
	init_fixture(&f, 2, 1, 1, 1, 1);
	(void)open_remote(&f, 0, 3, 1);
	(void)open_remote(&f, 1, 1, 2);
	atomic_store(&block_all_tx, 1);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 5, WINDOW, NULL, 0);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 7, WINDOW, NULL, 0);
	wait_rx_hold(&f);
	/* 控制连接的拒绝暂停不能阻止另一物理连接拒绝和交付已有数据。 */
	send_frame(&f, 1, TR_FRAME_STREAM_OPEN, TR_FRAME_F_LANE_BULK, 9, WINDOW,
	           NULL, 0);
	struct tr_frame_header h = receive_frame(&f, 1);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 9);
	uint8_t bytes[8] = {1};
	send_frame(&f, 1, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(tr_channel_active_streams(f.channel) == 2);
	atomic_store(&block_all_tx, 0);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 5);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 7);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_refusal_partial(void)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 1, 1, 1);
	(void)open_remote(&f, 0, 1, 1);
	atomic_store(&partial_type, TR_FRAME_STREAM_REFUSED);
	atomic_store(&partial_phase, 1);
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 3, WINDOW, NULL, 0);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_REFUSED && h.stream_id == 3);
	assert(atomic_load(&partial_phase) == 3 &&
	       atomic_load(&partial_eagain) == 1);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static uint64_t repeat_goaway(void *arg, uint64_t now_ns)
{
	struct fixture *f = arg;
	(void)now_ns;
	assert(atomic_load(&repeat_goaway_phase) == 2);
	assert(tr_reactor_send_goaway_on_owner(f->connection[0], 0) == TR_OK);
	atomic_store(&repeat_goaway_phase, 3);
	return 0;
}

static int repeat_goaway_owner(void *arg)
{
	struct fixture *f = arg;
	assert(tr_reactor_resume_rx(f->connection[0]) == TR_OK);
	assert(tr_reactor_send_goaway_on_owner(f->connection[0], 0) == TR_OK);
	for (unsigned i = 1; i < 256; ++i)
		assert(tr_reactor_resume_rx(f->connection[0]) == TR_OK);
	assert(tr_reactor_send_stream_reset_on_owner(f->connection[0], 1,
	                                             TR_ERR_CLOSED) == TR_OK);
	return TR_OK;
}

static void test_repeat_goaway_fifo(void)
{
	struct fixture f;
	init_fixture_with_commands(&f, CAPACITY, 0, 1, 1, 1, 256, WINDOW, NULL);
	(void)open_remote(&f, 0, 1, 1);
	assert(tr_reactor_timer_register(f.reactor, repeat_goaway, &f,
	                                 &repeat_goaway_timer) == TR_OK);
	atomic_store(&repeat_goaway_phase, 1);
	assert(tr_reactor_call(f.reactor, repeat_goaway_owner, &f) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_GOAWAY);
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_RESET);
	assert(receive_frame(&f, 0).type == TR_FRAME_GOAWAY);
	assert(atomic_load(&repeat_goaway_phase) == 3);
	assert(tr_reactor_timer_unregister(repeat_goaway_timer) == TR_OK);
	destroy_fixture(&f);
}

static void test_closed_reentrant_open_fifo(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};
	init_fixture(&f, 1, 0, 1, 1, 1);
	struct tr_stream_handle old = open_remote(&f, 0, 1, 1);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.queue_reset_predecessors = 1;
	f.reopen_on_closed = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	peer_reset(&f, 0, 1, TR_ERR_CLOSED);
	wait_count(&f, &f.reopened_count, 1);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_RESET && h.stream_id == 1);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_OPEN && h.stream_id == 2);
	assert(f.reopened_stream.slot == old.slot);
	assert(f.reopened_stream.generation != old.generation);
	assert(reset(&f, old, TR_ERR_CLOSED) == TR_ERR_STALE);
	send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, 2, WINDOW, NULL, 0);
	wait_count(&f, &f.opened, 2);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 2,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(reset(&f, f.reopened_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 2, TR_ERR_CLOSED, TR_ERR_CLOSED, 2);
	assert(count(&f, &f.down) == 0);
	destroy_fixture(&f);
}

static void test_close_and_held_payload(void)
{
	struct fixture f;
	uint8_t bytes[8] = {0};
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(tr_reactor_call(f.reactor, close_owner, &stream) == TR_OK);
	assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_CLOSE);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_ERR_CLOSED);
	/* 普通本地半关闭之后仍必须回应对端取消。 */
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_RESET && h.message_id == 16);
	wait_count(&f, &f.closed, 1);
	assert(count(&f, &f.resets) == 1 &&
	       f.reset_status[0] == TR_ERR_TIMEOUT);
	assert(tr_stream_release_payload(f.held_stream, f.held) ==
	       TR_ERR_STALE);
	f.held = NULL;
	destroy_fixture(&f);
}

static void test_reset_releases_held_payload_without_credit(void)
{
	struct fixture f;
	uint8_t bytes[8] = {1};
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle reset_stream = open_remote(&f, 0, 1, 1);
	struct tr_stream_handle live_stream = open_remote(&f, 0, 3, 2);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 1, bytes,
	           sizeof(bytes));
	wait_count(&f, &f.received, 1U);
	assert(f.held && f.held_stream.generation == reset_stream.generation);

	assert(reset(&f, reset_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_TIMEOUT, 1U);
	assert(tr_channel_active_streams(f.channel) == 1U);
	/* RESET 退役后迟到的应用释放只还内存，不得为旧流补发窗口信用。 */
	assert(tr_stream_release_payload(f.held_stream, f.held) == TR_ERR_STALE);
	f.held = NULL;
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 0;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_PING, 0, 0, 1001, NULL, 0);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_PONG && h.message_id == 1001);
	assert(count(&f, &f.down) == 0U);

	/* 迟到释放不能影响同一 Channel 上仍活动的流。 */
	send_frame(&f, 0, TR_FRAME_DATA,
	           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3, 1, bytes,
	           sizeof(bytes));
	wait_count(&f, &f.received, 2U);
	assert(reset(&f, live_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(count(&f, &f.resets) == 2U && count(&f, &f.closed) == 2U);
	assert(count(&f, &f.down) == 0U);
	destroy_fixture(&f);
}

static void test_reset_begun_held_payload_release_without_credit(void)
{
	struct fixture f;
	uint8_t bytes[128] = {1};
	struct tr_stream_flow_state flow;
	struct tr_buffer *held;
	struct tr_stream_handle reset_stream;
	struct tr_stream_handle live_stream;

	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	reset_stream = open_remote(&f, 0, 1, 1);
	live_stream = open_remote(&f, 0, 3, 2);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 1, 1, bytes, 64U);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 1, bytes + 64U,
	           64U);
	wait_count(&f, &f.received, 1U);
	assert(f.held && f.held_stream.generation == reset_stream.generation);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.block_reset_callback = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);

	/* RESET 已把流置为 RESETTING，但 owner 回调尚未退役槽位。 */
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	wait_count(&f, &f.resets, 1U);
	assert(f.reset_status[0] == TR_ERR_TIMEOUT);
	assert(pthread_mutex_lock(&f.lock) == 0);
	assert(f.reset_callback_blocked);
	held = f.held;
	assert(held);
	f.held = NULL;
	f.hold_data = 0;
	assert(pthread_mutex_unlock(&f.lock) == 0);

	/* 应用在线程外释放已交付正文；RESETTING 状态不得补发窗口信用。 */
	assert(tr_stream_release_payload(reset_stream, held) == TR_OK);
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == sizeof(bytes));
	assert(flow.rx_consumed_bytes == sizeof(bytes));
	assert(flow.rx_advertised_limit == WINDOW);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.release_reset_callback = 1;
	assert(pthread_cond_broadcast(&f.cond) == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	wait_count(&f, &f.closed, 1U);
	assert(tr_channel_active_streams(f.channel) == 1U);
	/* 所有先前的 owner 命令完成后，再检查 wire 上没有迟到信用帧。 */
	assert(tr_reactor_call(f.reactor, reactor_noop, NULL) == TR_OK);
	barrier_without_stream_window_update(&f, 0, 1U, TR_ERR_TIMEOUT);

	/* 仍存活的同伴流可以交付消息并正常完成双向 RESET。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, 8U);
	wait_count(&f, &f.received, 2U);
	assert(count(&f, &f.down) == 0U);
	assert(reset(&f, live_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(count(&f, &f.down) == 0U);
	destroy_fixture(&f);
}

static void
test_reset_begun_concurrent_held_payload_releases_without_credit(void)
{
	struct fixture f;
	uint8_t bytes[64] = {1};
	struct tr_stream_flow_state flow;
	struct tr_channel_stats stats_before, stats_after;
	struct tr_stream_handle reset_stream;
	struct tr_stream_handle live_stream;
	struct tr_buffer *held[2];
	struct payload_release_request release[2];
	pthread_t release_threads[2];

	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	reset_stream = open_remote(&f, 0, 1, 1);
	live_stream = open_remote(&f, 0, 3, 2);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           2, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 2U);
	assert(pthread_mutex_lock(&f.lock) == 0);
	assert(f.held && f.held_extra);
	assert(f.held_stream.generation == reset_stream.generation);
	assert(f.held_extra_stream.generation == reset_stream.generation);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == 2U * sizeof(bytes));
	assert(flow.rx_consumed_bytes == 0U);
	assert(flow.rx_advertised_limit == WINDOW);
	assert(tr_channel_get_stats(f.channel, &stats_before) == TR_OK);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.block_reset_callback = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	wait_count(&f, &f.resets, 1U);
	assert(f.reset_status[0] == TR_ERR_TIMEOUT);
	assert(pthread_mutex_lock(&f.lock) == 0);
	assert(f.reset_callback_blocked);
	held[0] = f.held;
	held[1] = f.held_extra;
	assert(held[0] && held[1]);
	f.held = NULL;
	f.held_extra = NULL;
	f.hold_data = 0;
	assert(pthread_mutex_unlock(&f.lock) == 0);

	/* 两个应用线程同时释放同一条 RESETTING 流上的独立正文。 */
	for (unsigned i = 0; i < 2; ++i) {
		release[i].fixture = &f;
		release[i].stream = reset_stream;
		release[i].payload = held[i];
		release[i].result = TR_ERR_STATE;
		assert(pthread_create(&release_threads[i], NULL,
		                      release_payload_after_gate,
		                      &release[i]) == 0);
	}
	wait_count(&f, &f.release_threads_ready, 2U);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.release_threads_go = 1;
	assert(pthread_cond_broadcast(&f.cond) == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	for (unsigned i = 0; i < 2; ++i) {
		assert(pthread_join(release_threads[i], NULL) == 0);
		assert(release[i].result == TR_OK);
	}
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == 2U * sizeof(bytes));
	assert(flow.rx_consumed_bytes == 2U * sizeof(bytes));
	assert(flow.rx_advertised_limit == WINDOW);
	assert(tr_channel_get_stats(f.channel, &stats_after) == TR_OK);
	assert(stats_after.window_updates_tx == stats_before.window_updates_tx);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.release_reset_callback = 1;
	assert(pthread_cond_broadcast(&f.cond) == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	wait_count(&f, &f.closed, 1U);
	assert(tr_channel_active_streams(f.channel) == 1U);
	assert(tr_reactor_call(f.reactor, reactor_noop, NULL) == TR_OK);
	barrier_without_stream_window_update(&f, 0, 1U, TR_ERR_TIMEOUT);
	assert(tr_channel_get_stats(f.channel, &stats_after) == TR_OK);
	assert(stats_after.window_updates_tx == stats_before.window_updates_tx);

	/* 目标流的并发释放不会妨碍同一 Channel 上的健康同伴。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 3U);
	assert(count(&f, &f.down) == 0U);
	assert(reset(&f, live_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(count(&f, &f.down) == 0U);
	destroy_fixture(&f);
}

static void test_reset_release_preserves_cross_message_credit_debt(void)
{
	struct fixture f;
	uint8_t bytes[128] = {1};
	struct tr_stream_flow_state flow;
	struct tr_buffer *held;
	struct tr_stream_handle reset_stream;
	struct tr_stream_handle live_stream;
	struct tr_frame_header h;

	/* 消息上限 128、窗口 256：两个 64 字节释放先累计至更新阈值。 */
	init_fixture_with_commands(&f, CAPACITY, 0, 1, 1, 1, COMMANDS,
	                           2U * FRAME, NULL);
	reset_stream = open_remote(&f, 0, 1, 1);
	live_stream = open_remote(&f, 0, 3, 2);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, 64U);
	wait_count(&f, &f.received, 1U);
	/* 第一条消息留下 64 字节未通告债务，尚不能发送 WINDOW_UPDATE。 */
	send_frame(&f, 0, TR_FRAME_PING, 0, 0, 998U, NULL, 0);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_PONG && h.message_id == 998U);

	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           2, bytes, 64U);
	wait_count(&f, &f.received, 2U);
	h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_WINDOW_UPDATE && h.stream_id == 1U);
	assert(h.message_id == WINDOW + 128U);
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == 128U &&
	       flow.rx_consumed_bytes == 128U &&
	       flow.rx_advertised_limit == WINDOW + 128U);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.hold_data = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST, 1, 3, bytes, FRAME);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 3, bytes + FRAME,
	           FRAME);
	wait_count(&f, &f.received, 3U);
	assert(f.held && f.held_stream.generation == reset_stream.generation);
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == WINDOW &&
	       flow.rx_consumed_bytes == 128U &&
	       flow.rx_advertised_limit == WINDOW + 128U);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.block_reset_callback = 1;
	assert(pthread_mutex_unlock(&f.lock) == 0);
	peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
	wait_count(&f, &f.resets, 1U);
	assert(f.reset_status[0] == TR_ERR_TIMEOUT);
	assert(pthread_mutex_lock(&f.lock) == 0);
	assert(f.reset_callback_blocked);
	held = f.held;
	assert(held);
	f.held = NULL;
	f.hold_data = 0;
	assert(pthread_mutex_unlock(&f.lock) == 0);

	/* 前两条消息留下的信用状态不能被 RESET 窗口中的释放覆盖。 */
	assert(tr_stream_release_payload(reset_stream, held) == TR_OK);
	assert(tr_stream_get_flow_state(reset_stream, &flow) == TR_OK);
	assert(flow.rx_received_bytes == WINDOW &&
	       flow.rx_consumed_bytes == WINDOW &&
	       flow.rx_advertised_limit == WINDOW + 128U);

	assert(pthread_mutex_lock(&f.lock) == 0);
	f.release_reset_callback = 1;
	assert(pthread_cond_broadcast(&f.cond) == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	wait_count(&f, &f.closed, 1U);
	assert(tr_channel_active_streams(f.channel) == 1U);
	assert(tr_reactor_call(f.reactor, reactor_noop, NULL) == TR_OK);
	barrier_without_stream_window_update(&f, 0, 1U, TR_ERR_TIMEOUT);

	/* 目标流在两种消费路径上都没有泄漏到同一 Channel 的健康流。 */
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3,
	           1, bytes, 8U);
	wait_count(&f, &f.received, 4U);
	assert(count(&f, &f.down) == 0U);
	assert(reset(&f, live_stream, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 0, 3, TR_ERR_CLOSED, TR_ERR_CLOSED, 2U);
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(count(&f, &f.down) == 0U);
	destroy_fixture(&f);
}

static void test_timer_failure_and_partial(int stopping)
{
	struct fixture f;
	init_fixture(&f, CAPACITY, 0, 1, 1, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	struct reset_request request = {stream, TR_ERR_CLOSED, 1};
	unsigned before = atomic_load(&arm_failures);
	assert(tr_reactor_call(f.reactor, reset_owner, &request) == TR_ERR_SYS);
	assert(atomic_load(&arm_failures) == before + 1);
	assert(count(&f, &f.resets) == 0 &&
	       tr_channel_active_streams(f.channel) == 1);
	assert(tr_stream_reset_on_owner(stream, TR_ERR_CLOSED) == TR_ERR_STATE);
	atomic_store(&partial_eagain, 0);
	atomic_store(&block_partial, stopping);
	atomic_store(&partial_phase, 1);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
	if (stopping) {
		for (unsigned i = 0; !atomic_load(&partial_eagain); ++i) {
			assert(i < 5000);
			pause_ms();
		}
		assert(atomic_load(&partial_phase) == 2);
		/* 停止必须释放仍在发送的内嵌项，而非当成普通池节点归还。 */
		assert(tr_reactor_stop(f.reactor) == TR_OK);
	} else {
		finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
		assert(atomic_load(&partial_phase) == 3);
		assert(atomic_load(&partial_eagain) == 1);
	}
	destroy_fixture(&f);
}

enum invalid_kind {
	UNKNOWN_RESET,
	BAD_DUPLICATE,
	LATE_DATA,
	LATE_CREDIT,
	BAD_PARTIAL,
	EXCESS_CREDIT,
	EVICTED_RESET
};

static void test_invalid(enum invalid_kind kind)
{
	struct fixture f;
	struct tr_memory_budget budget;
	uint64_t fixed_bytes = 0U;
	uint8_t bytes[8] = {0};
	if (kind == EVICTED_RESET) {
		struct tr_memory_budget_stats stats;

		tr_memory_budget_init(&budget, 0U);
		init_fixture_with_commands(&f, 2U, 0, 1U, 1U, 1, COMMANDS,
		                           WINDOW, &budget);
		tr_memory_budget_get_stats(&budget, &stats);
		fixed_bytes = stats.current_bytes;
		assert(fixed_bytes > 0U);
		budget.limit_bytes = fixed_bytes;
		assert_fixed_history_budget(&budget, fixed_bytes);
	} else {
		init_fixture(&f, 2, 0, 1, 1, 1);
	}
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	if (kind == BAD_PARTIAL || kind == EXCESS_CREDIT) {
		assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
		assert(receive_frame(&f, 0).type == TR_FRAME_STREAM_RESET);
		if (kind == BAD_PARTIAL)
			send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_LAST, 1, 1,
			           bytes, sizeof(bytes));
		else
			send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, 1,
			           WINDOW - 1U, NULL, 0);
	} else {
		assert(reset(&f, stream, TR_ERR_CLOSED) == TR_OK);
		finish_reset(&f, 0, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
		if (kind == EVICTED_RESET)
			assert_fixed_history_budget(&budget, fixed_bytes);
		if (kind == EVICTED_RESET) {
			/* FIFO 中仍保留的墓碑必须吸收重复 RESET。 */
			peer_reset(&f, 0, 1, TR_ERR_CLOSED);
			barrier(&f, 0);
			assert_fixed_history_budget(&budget, fixed_bytes);
			for (unsigned i = 1; i <= 2; ++i) {
				stream =
				        open_remote(&f, 0, 2U * i + 1U, i + 1U);
				assert(reset(&f, stream, TR_ERR_CLOSED) ==
				       TR_OK);
				finish_reset(&f, 0, 2U * i + 1U, TR_ERR_CLOSED,
				             TR_ERR_CLOSED, i + 1U);
				assert_fixed_history_budget(&budget, fixed_bytes);
			}
			peer_reset(&f, 0, 1, TR_ERR_CLOSED);
		} else if (kind == UNKNOWN_RESET)
			peer_reset(&f, 0, 777, TR_ERR_CLOSED);
		else if (kind == BAD_DUPLICATE)
			peer_reset(&f, 0, 1, TR_ERR_TIMEOUT);
		else if (kind == LATE_DATA)
			send_frame(&f, 0, TR_FRAME_DATA,
			           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 1,
			           bytes, sizeof(bytes));
		else
			send_frame(&f, 0, TR_FRAME_WINDOW_UPDATE, 0, 1,
			           WINDOW + 1, NULL, 0);
	}
	wait_count(&f, &f.down, 1);
	assert(tr_channel_active_streams(f.channel) == 0);
	destroy_fixture(&f);
	if (kind == EVICTED_RESET) {
		struct tr_memory_budget_stats stats;

		tr_memory_budget_get_stats(&budget, &stats);
		assert(stats.current_bytes == 0U);
		assert(stats.peak_bytes == fixed_bytes);
		assert(stats.rejection_events == 0U);
	}
}

static void test_split_frontier(void)
{
	struct fixture f;
	uint8_t bytes[8] = {0};
	init_fixture(&f, CAPACITY, 1, 1, 1, 1);
	(void)open_remote(&f, 0, 3, 1);
	struct tr_stream_handle bulk = open_remote(&f, 1, 1, 2);
	/* 同 ID 在错误物理连接上不能命中 BULK；另一物理连接继续工作。 */
	peer_reset(&f, 0, 1, TR_ERR_CLOSED);
	wait_count(&f, &f.down, 1);
	assert(tr_channel_active_streams(f.channel) == 1);
	send_frame(&f, 1, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, bytes, sizeof(bytes));
	wait_count(&f, &f.received, 1);
	assert(reset(&f, bulk, TR_ERR_CLOSED) == TR_OK);
	finish_reset(&f, 1, 1, TR_ERR_CLOSED, TR_ERR_CLOSED, 1);
	destroy_fixture(&f);
}

static void test_feature_gate(void)
{
	for (int ack = 0; ack <= 1; ++ack) {
		struct fixture f;
		init_fixture(&f, CAPACITY, 0, 1, 1, 0);
		hello(&f, 0, 0, ack);
		wait_count(&f, &f.down, 1);
		assert(tr_channel_wait_ready(f.channel, TR_LANE_CONTROL, 1) !=
		       TR_OK);
		assert(count(&f, &f.opened) == 0);
		destroy_fixture(&f);
	}
	struct fixture f;
	init_fixture(&f, CAPACITY, 0, 0, 0, 1);
	struct tr_stream_handle stream = open_remote(&f, 0, 1, 1);
	assert(reset(&f, stream, TR_ERR_CLOSED) == TR_ERR_UNSUPPORTED);
	assert(count(&f, &f.resets) == 0);
	destroy_fixture(&f);
}

static void test_wire_shape(uint16_t type)
{
	struct tr_frame_header h = {.version = TR_WIRE_ENV_VERSION,
	                            .type = type,
	                            .stream_id = 1,
	                            .message_id = (uint64_t)-TR_ERR_NOMEM};
	uint8_t bytes[TR_WIRE_HEADER_SIZE];
	const struct tr_wire_limits limits = {FRAME};
	for (unsigned bad = 0; bad <= 6; ++bad) {
		struct tr_frame_header copy = h;
		if (bad == 1)
			copy.flags = TR_FRAME_F_FIRST;
		if (bad == 2)
			copy.payload_len = 1;
		if (bad == 3)
			copy.stream_id = 0;
		if (bad == 4)
			copy.message_id = UINT64_MAX;
		if (bad == 5)
			copy.message_id = 0;
		if (bad == 6)
			copy.message_id = (uint64_t)-TR_ERR_SYS;
		assert(tr_wire_header_encode(bytes, &copy) == TR_OK);
		assert(tr_wire_header_decode(bytes, &copy) == TR_OK);
		int ret = tr_wire_header_validate(bytes, &copy, &limits);
		assert(bad && !(bad == 6 && type == TR_FRAME_STREAM_RESET)
		               ? ret < 0
		               : ret == TR_OK);
	}
}

int main(void)
{
	test_reset_rejects_compressed_data_lane_flag();
	test_wire_shape(TR_FRAME_STREAM_RESET);
	test_wire_shape(TR_FRAME_STREAM_REFUSED);
	test_feature_gate();
	test_local_stream_id_exhaustion();
	test_local_stream_id_exhaustion_drains();
	test_late_and_reuse();
	test_tombstone_connection_generation();
	test_tombstone_split_connection_identity();
	test_tombstone_same_id_across_split_connections();
	test_tombstone_long_fifo_turnover();
	test_reset_begun_concurrent_held_payload_releases_without_credit();
	test_reset_release_preserves_cross_message_credit_debt();
	test_reset_begun_held_payload_release_without_credit();
	test_tombstone_real_channel_capacity();
	test_reservation_and_fifo();
	test_repeat_goaway_fifo();
	test_closed_reentrant_open_fifo();
	test_opening_and_refusal();
	test_opening_local_close();
	test_open_admission_boundaries(0);
	test_open_admission_boundaries(1);
	test_open_admission_boundaries(2);
	for (unsigned i = 0; i < 5; ++i)
		test_refusal_pressure(i);
	test_refusal_reset_fairness();
	for (unsigned i = 0; i < 5; ++i)
		test_receive_refusal(i);
	test_refusal_partial();
	test_refusal_full_queues();
	test_refusal_without_reset_feature();
	test_refusal_split_progress();
	test_receive_isolation(0);
	test_receive_isolation(1);
	test_data_type_flag_isolation();
	test_data_type_flag_without_reset();
	test_closed_receive_resource_failure();
	test_frozen_discard_credit(0);
	test_frozen_discard_credit(1);
	test_close_and_held_payload();
	test_reset_releases_held_payload_without_credit();
	test_timer_failure_and_partial(0);
	test_timer_failure_and_partial(1);
	for (enum invalid_kind k = UNKNOWN_RESET; k <= EVICTED_RESET; ++k)
		test_invalid(k);
	test_split_frontier();
	puts("stream_reset: wire, long FIFO turnover, feature, pressure, retry, retirement, "
	     "ID exhaustion passed");
	return 0;
}
