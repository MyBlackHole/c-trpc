#define _GNU_SOURCE
#include "../src/transport/channel/channel.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/transport/protocol/wire.h"
#include "../src/crc32c.h"
#include "../src/execution/reactor_internal.h"
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

#define FRAME 64U
#define WINDOW 256U
#define CAPACITY 16U

struct observed_event {
	struct tr_stream_handle stream;
	enum tr_stream_event event;
	int status;
};

struct fixture {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_conn_handle connection[2];
	int peer[2];
	int adopted_fd[2];
	int split;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened, closed, remote_closed, errors, received;
	unsigned down[2], goaway[2];
	int down_status[2];
	struct tr_stream_handle streams[32];
	struct observed_event events[128];
	unsigned event_count;
};

static atomic_int close_fd = -1;
static atomic_int close_blocked;
static atomic_uint close_attempts;
static struct tr_conn_handle pressure_connection;
static atomic_int pressure_fd = -1;
static atomic_int pressure_blocked;
static atomic_int pressure_fill;
static atomic_uint pressure_items;
static atomic_uint pressure_failures;

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload);

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload)
{
	int ret;
	if (atomic_load(&pressure_blocked) && type == TR_FRAME_WINDOW_UPDATE &&
	    stream_id == 5U &&
	    connection.reactor == pressure_connection.reactor &&
	    connection.slot == pressure_connection.slot &&
	    connection.generation == pressure_connection.generation) {
		if (atomic_exchange(&pressure_fill, 0)) {
			unsigned filled = 0;
			/* 在真实初始信用发送前填满真实控制池；暂停的只是此 fd
			 * 的 系统调用，池、命令顺序、解析器与恢复均走生产实现。
			 */
			while ((ret = __real_tr_reactor_send(
			                connection, TR_FRAME_PING, 0, 0, 123,
			                NULL)) == TR_OK) {
				assert(++filled <= 16U);
			}
			assert(ret == TR_AGAIN && filled != 0);
			atomic_store(&pressure_items, filled);
		}
		ret = __real_tr_reactor_send(connection, type, flags, stream_id,
		                             message_id, payload);
		if (ret == TR_AGAIN)
			atomic_fetch_add(&pressure_failures, 1U);
		return ret;
	}
	return __real_tr_reactor_send(connection, type, flags, stream_id,
	                              message_id, payload);
}

ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);
ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	if (fd == atomic_load(&pressure_fd) && atomic_load(&pressure_blocked)) {
		errno = EAGAIN;
		return -1;
	}
	if (fd == atomic_load(&close_fd) && atomic_load(&close_blocked) &&
	    message->msg_iovlen &&
	    message->msg_iov[0].iov_len >= TR_WIRE_HEADER_SIZE) {
		const uint8_t *header = message->msg_iov[0].iov_base;
		if (tr_get_le16(header + TR_WIRE_OFF_TYPE) ==
		            TR_FRAME_STREAM_CLOSE &&
		    tr_get_le32(header + TR_WIRE_OFF_STREAM_ID) == 1U) {
			/* owner 不等待测试线程。只有最终 CLOSE 的真实发送尝试
			 * 被持续返回 EAGAIN，HELLO/GOAWAY 与其他连接不受影响。
			 */
			atomic_fetch_add(&close_attempts, 1U);
			errno = EAGAIN;
			return -1;
		}
	}
	return __real_sendmsg(fd, message, flags);
}

static void wait_close_attempt(void)
{
	struct timespec end, now;
	const struct timespec pause = {0, 1000000L};
	assert(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += 5;
	while (!atomic_load(&close_attempts)) {
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		assert(now.tv_sec < end.tv_sec ||
		       (now.tv_sec == end.tv_sec && now.tv_nsec < end.tv_nsec));
		(void)nanosleep(&pause, NULL);
	}
}

static void wait_count(struct fixture *f, const unsigned *counter,
                       unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (*counter < expected)
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
	(void)stream;
	assert(message_id == 1 && payload->len == 8);
	assert(memcmp(payload->data, "survives", 8) == 0);
	assert(pthread_mutex_lock(&f->lock) == 0);
	++f->received;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return TR_STREAM_DATA_RELEASE;
}

static void on_stream(struct tr_stream_handle stream,
                      enum tr_stream_event event, int status, void *arg)
{
	struct fixture *f = arg;
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(f->event_count < 128U);
	f->events[f->event_count++] =
	        (struct observed_event){stream, event, status};
	if (event == TR_STREAM_EVENT_OPENED) {
		assert(f->opened < 32U && status == TR_OK);
		f->streams[f->opened++] = stream;
	} else if (event == TR_STREAM_EVENT_CLOSED)
		++f->closed;
	else if (event == TR_STREAM_EVENT_REMOTE_CLOSED)
		++f->remote_closed;
	else if (event == TR_STREAM_EVENT_ERROR)
		++f->errors;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void on_channel(struct tr_channel *channel, enum tr_channel_event event,
                       int status, void *arg)
{
	struct fixture *f = arg;
	(void)channel;
	assert(pthread_mutex_lock(&f->lock) == 0);
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN) {
		unsigned lane = event == TR_CHANNEL_EVENT_BULK_DOWN;
		++f->down[lane];
		f->down_status[lane] = status;
	} else if (event == TR_CHANNEL_EVENT_CONTROL_GOAWAY ||
	           event == TR_CHANNEL_EVENT_BULK_GOAWAY) {
		assert(status == TR_OK);
		++f->goaway[event == TR_CHANNEL_EVENT_BULK_GOAWAY];
	}
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void transfer(int fd, void *bytes, size_t size, int writing)
{
	uint8_t *cursor = bytes;
	while (size) {
		ssize_t n = writing ? send(fd, cursor, size, MSG_NOSIGNAL)
		                    : recv(fd, cursor, size, 0);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			struct pollfd p = {fd, writing ? POLLOUT : POLLIN, 0};
			int ready;
			do {
				ready = poll(&p, 1, 5000);
			} while (ready < 0 && errno == EINTR);
			assert(ready > 0);
			continue;
		}
		assert(n > 0);
		cursor += (size_t)n;
		size -= (size_t)n;
	}
}

static void send_frame(struct fixture *f, unsigned lane, uint16_t type,
                       uint32_t flags, uint32_t stream_id, uint64_t message_id,
                       const void *body, uint32_t size)
{
	uint8_t raw[TR_WIRE_HEADER_SIZE];
	struct tr_frame_header h = {.version = TR_WIRE_ENV_VERSION,
	                            .type = type,
	                            .flags = flags,
	                            .stream_id = stream_id,
	                            .message_id = message_id,
	                            .payload_len = size,
	                            .payload_crc32c = tr_crc32c(body, size)};
	assert(tr_wire_header_encode(raw, &h) == TR_OK);
	transfer(f->peer[lane], raw, sizeof(raw), 1);
	if (size)
		transfer(f->peer[lane], (void *)body, size, 1);
}

static struct tr_frame_header receive_frame(struct fixture *f, unsigned lane)
{
	uint8_t raw[TR_WIRE_HEADER_SIZE], body[FRAME];
	struct tr_frame_header h;
	const struct tr_wire_limits limits = {FRAME};
	transfer(f->peer[lane], raw, sizeof(raw), 0);
	assert(tr_wire_header_decode(raw, &h) == TR_OK);
	assert(tr_wire_header_validate(raw, &h, &limits) == TR_OK);
	assert(h.payload_len <= sizeof(body));
	if (h.payload_len)
		transfer(f->peer[lane], body, h.payload_len, 0);
	assert(tr_crc32c(body, h.payload_len) == h.payload_crc32c);
	return h;
}

static void hello(struct fixture *f, unsigned lane, uint16_t version, int ack)
{
	uint8_t body[32] = {0};
	tr_put_le16(body, version);
	if (!ack)
		tr_put_le16(body + 2, version);
	tr_put_le32(body + 4, f->split ? (1U << lane) : 3U);
	tr_put_le32(body + 8, FRAME);
	tr_put_le32(body + 12, FRAME);
	send_frame(f, lane, ack ? TR_FRAME_HELLO_ACK : TR_FRAME_HELLO, 0, 0, 0,
	           body, sizeof(body));
}

/* 只用于尚有流或还未收到对端 GOAWAY 的阶段；双 GOAWAY 无流后协议
 * 停止生成 PONG。精确检查下一帧，不能吞掉不应出现的 CLOSE 或 GOAWAY。 */
static void barrier(struct fixture *f, unsigned lane)
{
	send_frame(f, lane, TR_FRAME_PING, 0, 0, 999, NULL, 0);
	struct tr_frame_header h = receive_frame(f, lane);
	assert(h.type == TR_FRAME_PONG && h.message_id == 999);
}

static void init_fixture(struct fixture *f, int split, int handshake)
{
	const struct tr_reactor_config rc = {.max_connections = 2,
	                                     .command_capacity = 64,
	                                     .tx_item_capacity = 16,
	                                     .control_tx_item_capacity = 16,
	                                     .rx_buffer_count = 16,
	                                     .rx_buffer_size = FRAME,
	                                     .max_payload_len = FRAME};
	const struct tr_channel_config cc = {
	        .role = TR_CHANNEL_SERVER,
	        .mode = split ? TR_CHANNEL_SPLIT_CONNECTIONS
	                      : TR_CHANNEL_SHARED_CONNECTION,
	        .max_streams = CAPACITY,
	        .initial_window_bytes = WINDOW,
	        .window_update_threshold_bytes = 128,
	        .max_message_bytes = FRAME};
	memset(f, 0, sizeof(*f));
	f->split = split;
	f->peer[0] = f->peer[1] = -1;
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	assert(tr_reactor_create(&rc, NULL, NULL, NULL, &f->reactor) == TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	for (unsigned lane = 0; lane < (split ? 2U : 1U); ++lane) {
		int sockets[2];
		assert(socketpair(AF_UNIX,
		                  SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
		                  sockets) == 0);
		assert(tr_reactor_adopt_fd(f->reactor, sockets[0],
		                           &f->connection[lane]) == TR_OK);
		f->adopted_fd[lane] = sockets[0];
		f->peer[lane] = sockets[1];
	}
	if (!split)
		f->connection[1] = f->connection[0];
	assert(tr_channel_create(&cc, f->connection[0], f->connection[1],
	                         on_data, on_stream, on_channel, f,
	                         &f->channel) == TR_OK);
	for (unsigned lane = 0; lane < (split ? 2U : 1U); ++lane) {
		assert(receive_frame(f, lane).type == TR_FRAME_HELLO);
		if (handshake) {
			hello(f, lane, TR_CHANNEL_PROTOCOL_VERSION, 0);
			assert(receive_frame(f, lane).type ==
			       TR_FRAME_HELLO_ACK);
			hello(f, lane, TR_CHANNEL_PROTOCOL_VERSION, 1);
			barrier(f, lane);
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

static void destroy_fixture(struct fixture *f)
{
	assert(tr_channel_destroy(f->channel) == TR_OK);
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	for (unsigned lane = 0; lane < (f->split ? 2U : 1U); ++lane)
		assert(close(f->peer[lane]) == 0);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static void expect_goaway(struct fixture *f, unsigned lane, uint32_t cutoff)
{
	struct tr_frame_header h = receive_frame(f, lane);
	assert(h.type == TR_FRAME_GOAWAY && h.message_id == cutoff);
	assert(h.flags == 0 && h.stream_id == 0 && h.payload_len == 0);
}

static struct tr_stream_handle open_remote(struct fixture *f, enum tr_lane lane,
                                           uint32_t id)
{
	unsigned n = count(f, &f->opened);
	unsigned physical = f->split ? (unsigned)lane : 0U;
	send_frame(f, physical, TR_FRAME_STREAM_OPEN,
	           lane == TR_LANE_BULK ? TR_FRAME_F_LANE_BULK : 0, id, WINDOW,
	           NULL, 0);
	struct tr_frame_header h = receive_frame(f, physical);
	assert(h.type == TR_FRAME_WINDOW_UPDATE && h.stream_id == id);
	assert(h.message_id == WINDOW);
	wait_count(f, &f->opened, n + 1U);
	return f->streams[n];
}

static struct tr_stream_handle open_local(struct fixture *f, enum tr_lane lane,
                                          uint32_t id, int acknowledge)
{
	struct tr_stream_handle stream;
	unsigned physical = f->split ? (unsigned)lane : 0U;
	unsigned n = count(f, &f->opened);
	assert(tr_stream_open(f->channel, lane, &stream) == TR_OK);
	struct tr_frame_header h = receive_frame(f, physical);
	assert(h.type == TR_FRAME_STREAM_OPEN && h.stream_id == id);
	assert(h.flags == (lane == TR_LANE_BULK ? TR_FRAME_F_LANE_BULK : 0));
	if (acknowledge) {
		send_frame(f, physical, TR_FRAME_WINDOW_UPDATE, 0, id, WINDOW,
		           NULL, 0);
		wait_count(f, &f->opened, n + 1U);
	}
	return stream;
}

static void close_stream(struct fixture *f, unsigned physical, uint32_t id,
                         struct tr_stream_handle stream)
{
	unsigned n = count(f, &f->remote_closed);
	send_frame(f, physical, TR_FRAME_STREAM_CLOSE, 0, id, 0, NULL, 0);
	wait_count(f, &f->remote_closed, n + 1U);
	assert(tr_stream_close(stream) == TR_OK);
	struct tr_frame_header h = receive_frame(f, physical);
	assert(h.type == TR_FRAME_STREAM_CLOSE && h.stream_id == id);
	assert(tr_stream_close(stream) == TR_ERR_STALE);
}

static void assert_state(struct fixture *f, enum tr_channel_state expected)
{
	enum tr_channel_state state;
	struct tr_channel_stats stats;
	assert(tr_channel_get_state(f->channel, &state) == TR_OK);
	assert(state == expected);
	assert(tr_channel_get_stats(f->channel, &stats) == TR_OK);
	assert(stats.state == expected);
}

static void assert_refused(struct fixture *f, struct tr_stream_handle stream)
{
	unsigned found = 0;
	assert(pthread_mutex_lock(&f->lock) == 0);
	for (unsigned i = 0; i < f->event_count; ++i) {
		const struct observed_event *e = &f->events[i];
		if (e->stream.slot != stream.slot ||
		    e->stream.generation != stream.generation)
			continue;
		assert(e->event == TR_STREAM_EVENT_ERROR);
		assert(e->status == TR_ERR_CLOSED);
		++found;
	}
	assert(found == 1U);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(tr_stream_close(stream) == TR_ERR_STALE);
}

static void expect_down(struct fixture *f, unsigned lane, int status)
{
	wait_count(f, &f->down[lane], 1);
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(f->down_status[lane] == status);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void test_shared_crossing_open(void)
{
	struct fixture f;
	struct tr_stream_handle rejected;
	init_fixture(&f, 0, 1);
	struct tr_stream_handle a = open_remote(&f, TR_LANE_CONTROL, 1);
	struct tr_stream_handle b = open_remote(&f, TR_LANE_BULK, 3);
	struct tr_stream_handle c = open_local(&f, TR_LANE_CONTROL, 2, 1);
	struct tr_stream_handle pending = open_local(&f, TR_LANE_BULK, 4, 0);
	assert(tr_stream_close(pending) == TR_AGAIN);
	barrier(&f, 0);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 3);
	/* 5 在本端冻结后、对端发出对称 GOAWAY 前到达：只验证并丢弃。
	 * 第二次 begin 不能把已见前沿 5 偷换成已准入边界 3。 */
	send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 5, WINDOW, NULL, 0);
	barrier(&f, 0);
	assert(tr_channel_active_streams(f.channel) == 4U);
	assert(count(&f, &f.opened) == 3U);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	barrier(&f, 0);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 2, NULL, 0);
	wait_count(&f, &f.errors, 1);
	wait_count(&f, &f.goaway[0], 1);
	wait_count(&f, &f.goaway[1], 1);
	assert_refused(&f, pending);
	assert(tr_channel_active_streams(f.channel) == 3U);
	assert(tr_stream_open(f.channel, TR_LANE_CONTROL, &rejected) ==
	       TR_ERR_CLOSED);
	assert(tr_channel_wait_drained(f.channel, 0) == TR_AGAIN);
	assert_state(&f, TR_CHANNEL_DRAINING);
	/* 重复的同边界 GOAWAY 是幂等通知，不产生第二个拒绝或逐流 CLOSE。 */
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 2, NULL, 0);
	barrier(&f, 0);
	assert(count(&f, &f.errors) == 1U && count(&f, &f.closed) == 0U);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, "survives", 8);
	wait_count(&f, &f.received, 1);
	close_stream(&f, 0, 1, a);
	close_stream(&f, 0, 3, b);
	close_stream(&f, 0, 2, c);
	assert(tr_channel_wait_drained(f.channel, 2000) == TR_OK);
	assert_state(&f, TR_CHANNEL_DRAINED);
	assert(count(&f, &f.down[0]) == 0U);
	destroy_fixture(&f);
}

static void test_peer_must_confirm(void)
{
	struct fixture f;
	init_fixture(&f, 0, 1);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 0);
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(tr_channel_wait_drained(f.channel, 0) == TR_AGAIN);
	assert(tr_channel_wait_drained(f.channel, 10) == TR_AGAIN);
	assert_state(&f, TR_CHANNEL_DRAINING);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	assert(tr_channel_wait_drained(f.channel, 2000) == TR_OK);
	assert_state(&f, TR_CHANNEL_DRAINED);
	assert(count(&f, &f.down[0]) == 0U);
	destroy_fixture(&f);
}

static void test_invalid_cutoffs(void)
{
	/* 即使 2 是本连接已发布的 OPEN，尚无初始 WU 时也不能声称已准入。
	 * 其他变体分别检查方向、未来值、64 位越界与 GOAWAY 固定头字段。 */
	for (unsigned variant = 0; variant < 7U; ++variant) {
		struct fixture f;
		uint64_t cutoff[] = {2, 1, 4, UINT64_C(1) << 32, 0, 0, 0};
		uint8_t body = 1;
		init_fixture(&f, 0, 1);
		(void)open_local(&f, TR_LANE_CONTROL, 2, 0);
		send_frame(&f, 0, TR_FRAME_GOAWAY,
		           variant == 4U ? TR_FRAME_F_LANE_BULK : 0,
		           variant == 5U ? 2U : 0U, cutoff[variant],
		           variant == 6U ? &body : NULL,
		           variant == 6U ? 1U : 0U);
		expect_down(&f, 0, TR_ERR_STATE);
		destroy_fixture(&f);
	}
}

static void test_last_close_tx_completion(void)
{
	struct fixture f;
	init_fixture(&f, 0, 1);
	struct tr_stream_handle stream = open_remote(&f, TR_LANE_CONTROL, 1);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 1);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	barrier(&f, 0);
	atomic_store(&close_attempts, 0);
	atomic_store(&close_fd, f.adopted_fd[0]);
	atomic_store(&close_blocked, 1);
	send_frame(&f, 0, TR_FRAME_STREAM_CLOSE, 0, 1, 0, NULL, 0);
	wait_count(&f, &f.remote_closed, 1);
	assert(tr_stream_close(stream) == TR_OK);
	wait_close_attempt();
	/* 已观察到精确 CLOSE 的发送失败；gate 持续保持，保证这个否定
	 * 断言不依赖一次统计采样或发送线程是否碰巧尚未获得调度。 */
	assert(tr_channel_active_streams(f.channel) == 0U);
	assert(tr_channel_wait_drained(f.channel, 0) == TR_AGAIN);
	assert(tr_channel_wait_drained(f.channel, 10) == TR_AGAIN);
	assert_state(&f, TR_CHANNEL_DRAINING);
	atomic_store(&close_blocked, 0);
	struct tr_frame_header h = receive_frame(&f, 0);
	assert(h.type == TR_FRAME_STREAM_CLOSE && h.stream_id == 1U);
	assert(tr_channel_wait_drained(f.channel, 2000) == TR_OK);
	assert_state(&f, TR_CHANNEL_DRAINED);
	atomic_store(&close_fd, -1);
	assert(count(&f, &f.down[0]) == 0U);
	destroy_fixture(&f);
}

static void test_historical_and_missing_ack(void)
{
	struct fixture f;
	init_fixture(&f, 0, 1);
	struct tr_stream_handle stream = open_local(&f, TR_LANE_CONTROL, 2, 1);
	close_stream(&f, 0, 2, stream);
	/* 槽位已归还，历史确认仍然约束 GOAWAY；不能将 cutoff 降回 0。 */
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	expect_down(&f, 0, TR_ERR_STATE);
	destroy_fixture(&f);

	init_fixture(&f, 0, 1);
	(void)open_local(&f, TR_LANE_CONTROL, 2, 0);
	(void)open_local(&f, TR_LANE_CONTROL, 4, 1);
	/* 初始 WU 不能跳过同一 FIFO 中更早的 OPEN；最高 ID 相等仍不足。 */
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 4, NULL, 0);
	expect_down(&f, 0, TR_ERR_STATE);
	destroy_fixture(&f);

	init_fixture(&f, 0, 1);
	(void)open_local(&f, TR_LANE_CONTROL, 2, 1);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 2, NULL, 0);
	expect_goaway(&f, 0, 0);
	barrier(&f, 0);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	expect_down(&f, 0, TR_ERR_STATE);
	destroy_fixture(&f);
}

static void test_late_frames_are_strict(void)
{
	for (unsigned variant = 0; variant < 3U; ++variant) {
		struct fixture f;
		init_fixture(&f, 0, 1);
		assert(tr_channel_begin_drain(f.channel) == TR_OK);
		expect_goaway(&f, 0, 0);
		if (variant < 2U) {
			send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 1, WINDOW,
			           NULL, 0);
			barrier(&f, 0);
			assert(tr_channel_active_streams(f.channel) == 0U);
			assert(count(&f, &f.opened) == 0U);
			/* 已丢弃的 OPEN 不授权未知 CLOSE 或重复 OPEN。 */
			send_frame(&f, 0,
			           variant == 0U ? TR_FRAME_STREAM_CLOSE
			                         : TR_FRAME_STREAM_OPEN,
			           0, 1, variant == 0U ? 0U : WINDOW, NULL, 0);
		} else {
			send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
			assert(tr_channel_wait_drained(f.channel, 2000) ==
			       TR_OK);
			/* 对端 GOAWAY 已关闭 crossing OPEN 窗口。 */
			send_frame(&f, 0, TR_FRAME_STREAM_OPEN, 0, 1, WINDOW,
			           NULL, 0);
		}
		expect_down(&f, 0, TR_ERR_STATE);
		destroy_fixture(&f);
	}
}

static void test_split_boundaries(void)
{
	struct fixture f;
	struct tr_stream_handle refused;
	init_fixture(&f, 1, 1);
	/* 两条物理连接没有跨连接 FIFO：BULK 的较大 ID 可以先抵达。 */
	struct tr_stream_handle a = open_remote(&f, TR_LANE_BULK, 3);
	struct tr_stream_handle b = open_remote(&f, TR_LANE_CONTROL, 1);
	struct tr_stream_handle c = open_local(&f, TR_LANE_CONTROL, 2, 1);
	struct tr_stream_handle d = open_local(&f, TR_LANE_BULK, 4, 1);
	send_frame(&f, 1, TR_FRAME_GOAWAY, 0, 0, 4, NULL, 0);
	expect_goaway(&f, 1, 3);
	wait_count(&f, &f.goaway[1], 1);
	assert(count(&f, &f.goaway[0]) == 0U);
	assert_state(&f, TR_CHANNEL_RUNNING);
	assert(tr_stream_open(f.channel, TR_LANE_BULK, &refused) ==
	       TR_ERR_CLOSED);
	struct tr_stream_handle e = open_local(&f, TR_LANE_CONTROL, 6, 1);
	struct tr_stream_handle g = open_remote(&f, TR_LANE_CONTROL, 5);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 5);
	barrier(&f, 1);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 6, NULL, 0);
	wait_count(&f, &f.goaway[0], 1);
	close_stream(&f, 0, 1, b);
	close_stream(&f, 0, 2, c);
	close_stream(&f, 0, 6, e);
	close_stream(&f, 0, 5, g);
	assert(tr_channel_wait_drained(f.channel, 0) == TR_AGAIN);
	close_stream(&f, 1, 3, a);
	close_stream(&f, 1, 4, d);
	assert(tr_channel_wait_drained(f.channel, 2000) == TR_OK);
	assert_state(&f, TR_CHANNEL_DRAINED);
	assert(count(&f, &f.down[0]) == 0U && count(&f, &f.down[1]) == 0U);
	destroy_fixture(&f);
}

static void test_split_wrong_connection_cutoff(void)
{
	struct fixture f;
	init_fixture(&f, 1, 1);
	(void)open_local(&f, TR_LANE_CONTROL, 2, 1);
	(void)open_local(&f, TR_LANE_BULK, 4, 1);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 4, NULL, 0);
	expect_down(&f, 0, TR_ERR_STATE);
	assert(count(&f, &f.down[1]) == 0U);
	/* 错误只终止收到该 GOAWAY 的精确物理连接。 */
	(void)open_local(&f, TR_LANE_BULK, 6, 1);
	destroy_fixture(&f);
}

static void test_old_hello_is_rejected(void)
{
	struct fixture f;
	init_fixture(&f, 0, 0);
	hello(&f, 0, 1, 0);
	expect_down(&f, 0, TR_ERR_BAD_VERSION);
	destroy_fixture(&f);

	init_fixture(&f, 0, 1);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 0);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	assert(tr_channel_wait_drained(f.channel, 2000) == TR_OK);
	/* 排空后重发 HELLO 不能生成一个越过 TX 完成边界的新 ACK。 */
	hello(&f, 0, TR_CHANNEL_PROTOCOL_VERSION, 0);
	expect_down(&f, 0, TR_ERR_STATE);
	destroy_fixture(&f);
}

static int owner_barrier(void *arg)
{
	(void)arg;
	return TR_OK;
}

static void wait_pressure(struct fixture *f, unsigned failures)
{
	struct timespec end, now;
	const struct timespec pause = {0, 1000000L};
	assert(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += 5;
	while (atomic_load(&pressure_failures) < failures) {
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		assert(now.tv_sec < end.tv_sec ||
		       (now.tv_sec == end.tv_sec && now.tv_nsec < end.tv_nsec));
		(void)nanosleep(&pause, NULL);
	}
	assert(tr_reactor_call(f->reactor, owner_barrier, NULL) == TR_OK);
}

static void start_pressure(struct fixture *f)
{
	pressure_connection = f->connection[0];
	atomic_store(&pressure_fd, f->adopted_fd[0]);
	atomic_store(&pressure_items, 0);
	atomic_store(&pressure_failures, 0);
	atomic_store(&pressure_fill, 1);
	atomic_store(&pressure_blocked, 1);
	send_frame(f, 0, TR_FRAME_STREAM_OPEN, 0, 5, WINDOW, NULL, 0);
	/* 后一个 OPEN 已在 TCP 中，但不能越过待确认的 5。 */
	send_frame(f, 0, TR_FRAME_STREAM_OPEN, 0, 7, WINDOW, NULL, 0);
	wait_pressure(f, 1);
	assert(count(f, &f->down[0]) == 0);
	struct tr_reactor_stats stats;
	assert(tr_reactor_get_stats(f->reactor, &stats) == TR_OK);
	assert(stats.control_tx_item_pool.exhausted_events >= 2);
}

static void release_pressure(struct fixture *f)
{
	atomic_store(&pressure_blocked, 0);
	for (unsigned i = 0; i < atomic_load(&pressure_items); ++i) {
		struct tr_frame_header h = receive_frame(f, 0);
		assert(h.type == TR_FRAME_PING && h.message_id == 123);
	}
}

static void test_open_pressure_recovers(int split)
{
	struct fixture f;
	init_fixture(&f, split, 1);
	(void)open_remote(&f, TR_LANE_CONTROL, 1);
	unsigned opened = 1;
	if (split) {
		(void)open_remote(&f, TR_LANE_BULK, 3);
		++opened;
	}
	start_pressure(&f);
	assert(tr_channel_active_streams(f.channel) == opened + 1);
	assert(count(&f, &f.opened) == opened);
	/* 旧缓冲池 RESUME_RX 和禁用保活都不能撤销控制发送背压。 */
	assert(tr_reactor_resume_rx(f.connection[0]) == TR_OK);
	assert(tr_channel_disable_keepalive(f.channel) == TR_OK);
	wait_pressure(&f, 2);
	assert(tr_channel_active_streams(f.channel) == opened + 1);
	if (split) {
		send_frame(&f, 1, TR_FRAME_DATA,
		           TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3, 1, "survives",
		           8);
		wait_count(&f, &f.received, 1);
		assert(count(&f, &f.down[1]) == 0);
	}
	release_pressure(&f);
	for (uint32_t id = 5; id <= 7; id += 2) {
		struct tr_frame_header h = receive_frame(&f, 0);
		assert(h.type == TR_FRAME_WINDOW_UPDATE && h.stream_id == id &&
		       h.message_id == WINDOW);
	}
	wait_count(&f, &f.opened, opened + 2);
	send_frame(&f, 0, TR_FRAME_DATA, TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1,
	           1, "survives", 8);
	wait_count(&f, &f.received, split ? 2U : 1U);
	assert(count(&f, &f.errors) == 0 && count(&f, &f.down[0]) == 0);
	close_stream(&f, 0, 7, f.streams[opened + 1]);
	struct tr_stream_handle replacement =
	        open_remote(&f, TR_LANE_CONTROL, 9);
	assert(replacement.slot == f.streams[opened + 1].slot &&
	       replacement.generation != f.streams[opened + 1].generation);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	expect_goaway(&f, 0, 9);
	if (split)
		expect_goaway(&f, 1, 3);
	destroy_fixture(&f);
	atomic_store(&pressure_fd, -1);
}

static void test_drain_rejects_pending_open(int split)
{
	struct fixture f;
	init_fixture(&f, split, 1);
	struct tr_stream_handle first = open_remote(&f, TR_LANE_CONTROL, 1);
	start_pressure(&f);
	assert(tr_channel_begin_drain(f.channel) == TR_OK);
	assert(tr_channel_active_streams(f.channel) == 1);
	release_pressure(&f);
	/* 未发初始信用的 5 与尚未解析的 7 均由 cutoff=1 拒绝。
	 * 不得在 GOAWAY 后泄漏迟到 WINDOW_UPDATE 或上层 OPENED。 */
	expect_goaway(&f, 0, 1);
	if (split)
		expect_goaway(&f, 1, 0);
	barrier(&f, 0);
	assert(count(&f, &f.opened) == 1 && count(&f, &f.errors) == 0);
	send_frame(&f, 0, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	if (split)
		send_frame(&f, 1, TR_FRAME_GOAWAY, 0, 0, 0, NULL, 0);
	close_stream(&f, 0, 1, first);
	assert(tr_channel_wait_drained(f.channel, 5000) == TR_OK);
	assert(count(&f, &f.opened) == 1 && count(&f, &f.down[0]) == 0);
	destroy_fixture(&f);
	atomic_store(&pressure_fd, -1);
}

static void test_pending_open_teardown_releases_rx(int detach)
{
	struct fixture f;
	struct tr_connection_stats stats;
	init_fixture(&f, 0, 1);
	(void)open_remote(&f, TR_LANE_CONTROL, 1);
	start_pressure(&f);
	assert(tr_reactor_get_connection_stats(f.connection[0], &stats) ==
	       TR_OK);
	assert(stats.rx_paused);
	if (detach) {
		assert(tr_channel_detach_for_finalize(f.channel) == TR_OK);
		tr_channel_finalize_detached(f.channel);
	} else {
		assert(tr_channel_destroy(f.channel) == TR_OK);
	}
	f.channel = NULL;
	/* Channel 借用的连接仍存活；解绑后不可留下失去恢复者的接收暂停。 */
	assert(tr_reactor_get_connection_stats(f.connection[0], &stats) ==
	       TR_OK);
	assert(!stats.rx_paused && stats.state == TR_CONN_ACTIVE);
	release_pressure(&f);
	destroy_fixture(&f);
	atomic_store(&pressure_fd, -1);
}

int main(void)
{
	test_open_pressure_recovers(0);
	test_open_pressure_recovers(1);
	test_drain_rejects_pending_open(0);
	test_drain_rejects_pending_open(1);
	test_pending_open_teardown_releases_rx(0);
	test_pending_open_teardown_releases_rx(1);
	test_shared_crossing_open();
	test_peer_must_confirm();
	test_last_close_tx_completion();
	test_invalid_cutoffs();
	test_historical_and_missing_ack();
	test_late_frames_are_strict();
	test_split_boundaries();
	test_split_wrong_connection_cutoff();
	test_old_hello_is_rejected();
	puts("channel drain tests passed (15 groups)");
	return 0;
}
