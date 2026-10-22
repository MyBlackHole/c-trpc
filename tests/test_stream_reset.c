#define _GNU_SOURCE
#include "../src/transport/channel/channel.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/transport/protocol/wire.h"
#include "../src/execution/reactor_internal.h"
#include "../src/crc32c.h"
#include "../src/endian.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define WINDOW 256U
#define FRAME 64U
#define CAPACITY 4U
#define CONTROL 4U
#define COMMANDS 8U

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
	int down_status;
	int error_status;
	struct tr_stream_handle stream[32];
	int reset_status[32];
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
	int hold_data;
	int queue_reset_predecessors;
	int reopen_on_closed;
	struct tr_stream_handle reopened_stream;
};

static _Thread_local int fail_reset_arm;
static atomic_uint arm_failures;
static atomic_int partial_fd = -1;
static atomic_int partial_phase;
static atomic_int block_partial;
static atomic_uint partial_eagain;
static atomic_int block_all_tx;
static atomic_int repeat_goaway_phase;
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
			    TR_FRAME_STREAM_RESET) {
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
		assert(!f->held);
		f->held = payload;
		f->held_stream = stream;
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
	int queue_predecessors = 0;
	int reopen = 0;
	assert(pthread_mutex_lock(&f->lock) == 0);
	if (event == TR_STREAM_EVENT_OPENED) {
		assert(f->opened < 32);
		f->stream[f->opened++] = stream;
	} else if (event == TR_STREAM_EVENT_RESET) {
		assert(f->resets < 32);
		f->reset_status[f->resets++] = status;
		queue_predecessors = f->queue_reset_predecessors;
		f->queue_reset_predecessors = 0;
	} else if (event == TR_STREAM_EVENT_CLOSED) {
		++f->closed;
		reopen = f->reopen_on_closed;
		f->reopen_on_closed = 0;
	} else if (event == TR_STREAM_EVENT_ERROR) {
		++f->errors;
		f->error_status = status;
	} else if (event == TR_STREAM_EVENT_WRITABLE) {
		++f->writable;
	} else if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		++f->remote_closed;
	}
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
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

static void hello(struct fixture *f, unsigned lane, uint64_t features, int ack)
{
	uint8_t bytes[32] = {0};
	tr_put_le16(bytes, TR_CHANNEL_PROTOCOL_VERSION);
	if (!ack)
		tr_put_le16(bytes + 2, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le32(bytes + 4, f->split ? (1U << lane) : 3U);
	tr_put_le32(bytes + 8, FRAME);
	tr_put_le32(bytes + 12, WINDOW);
	tr_put_le64(bytes + 16, features);
	send_frame(f, lane, ack ? TR_FRAME_HELLO_ACK : TR_FRAME_HELLO, 0, 0, 0,
	           bytes, sizeof(bytes));
}

static void init_fixture_with_commands(struct fixture *f, uint32_t capacity,
                                       int split, uint64_t features,
                                       uint64_t required, int handshake,
                                       uint32_t commands)
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
	        .max_message_bytes = WINDOW,
	        .feature_bits = features,
	        .required_feature_bits = required};
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
			hello(f, lane, features, 0);
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
	                           handshake, COMMANDS);
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
	return f->stream[count - 1U];
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

static int control_pressure_owner(void *arg)
{
	struct fixture *f = arg;
	for (unsigned i = 0; i < CONTROL; ++i)
		assert(tr_reactor_send(f->connection[0], TR_FRAME_PING, 0, 0,
		                       100 + i, NULL) == TR_OK);
	return TR_OK;
}

static void test_open_admission_failure(int initial_credit)
{
	struct fixture f;
	init_fixture(&f, 1, 0, 1, 1, 1);
	if (initial_credit) {
		atomic_store(&block_all_tx, 1);
		assert(tr_reactor_call(f.reactor, control_pressure_owner, &f) ==
		       TR_OK);
	} else {
		(void)open_remote(&f, 0, 1, 1);
	}
	/* 槽位耗尽和初始信用无法准入都必须终止精确连接，不能静默留下
	 * 永远等不到 WINDOW_UPDATE 的发起方，也不能容忍未知 RESET。 */
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 3, WINDOW, NULL, 0);
	wait_count(&f, &f.down, 2);
	assert(f.down_status == (initial_credit ? TR_ERR_STATE : TR_ERR_NOMEM));
	assert(tr_channel_active_streams(f.channel) == 0);
	assert(count(&f, &f.resets) == 0);
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
	init_fixture_with_commands(&f, CAPACITY, 0, 1, 1, 1, 256);
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
	uint8_t bytes[8] = {0};
	init_fixture(&f, 2, 0, 1, 1, 1);
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
		if (kind == EVICTED_RESET) {
			for (unsigned i = 1; i <= 2; ++i) {
				stream =
				        open_remote(&f, 0, 2U * i + 1U, i + 1U);
				assert(reset(&f, stream, TR_ERR_CLOSED) ==
				       TR_OK);
				finish_reset(&f, 0, 2U * i + 1U, TR_ERR_CLOSED,
				             TR_ERR_CLOSED, i + 1U);
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

static void test_wire_shape(void)
{
	struct tr_frame_header h = {.version = TR_WIRE_ENV_VERSION,
	                            .type = TR_FRAME_STREAM_RESET,
	                            .stream_id = 1,
	                            .message_id = 13};
	uint8_t bytes[TR_WIRE_HEADER_SIZE];
	const struct tr_wire_limits limits = {FRAME};
	for (unsigned bad = 0; bad <= 5; ++bad) {
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
		assert(tr_wire_header_encode(bytes, &copy) == TR_OK);
		assert(tr_wire_header_decode(bytes, &copy) == TR_OK);
		int ret = tr_wire_header_validate(bytes, &copy, &limits);
		assert(bad ? ret < 0 : ret == TR_OK);
	}
}

int main(void)
{
	test_wire_shape();
	test_feature_gate();
	test_late_and_reuse();
	test_reservation_and_fifo();
	test_repeat_goaway_fifo();
	test_closed_reentrant_open_fifo();
	test_opening_and_refusal();
	test_opening_local_close();
	test_open_admission_failure(0);
	test_open_admission_failure(1);
	test_receive_isolation(0);
	test_receive_isolation(1);
	test_closed_receive_resource_failure();
	test_frozen_discard_credit(0);
	test_frozen_discard_credit(1);
	test_close_and_held_payload();
	test_timer_failure_and_partial(0);
	test_timer_failure_and_partial(1);
	for (enum invalid_kind k = UNKNOWN_RESET; k <= EVICTED_RESET; ++k)
		test_invalid(k);
	test_split_frontier();
	puts("stream_reset: wire, feature, FIFO, pressure, retry, retirement "
	     "passed");
	return 0;
}
