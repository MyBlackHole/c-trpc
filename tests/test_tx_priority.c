#define _GNU_SOURCE
#include "tr/reactor.h"
#include "tr/crc32c.h"
#include "tr/status.h"
#include "../src/reactor_internal.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define FRAME_BYTES 64U
#define CONTROL_CAPACITY 32U
#define OBSERVED_CAPACITY 16384U
#define BURST 8U

struct fixture {
	struct tr_reactor *reactor;
	struct tr_conn_handle handle;
	struct tr_buffer_pool pool;
	struct tr_reactor_stats stats;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int peer;
	int entered;
	int released;
	int tx_entered;
	int tx_released;
	size_t pause_after;
	int inject_eagain;
	unsigned eagain_count;
	unsigned refill_left;
	uint64_t next_ping;
	unsigned char observed[OBSERVED_CAPACITY];
	size_t observed_len;
	size_t parsed_len;
};

/* Set before Reactor start; reset only after join. Observations are owner-only. */
static struct fixture *active;
ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);

static void observe_sent(struct fixture *f, const struct msghdr *message, size_t n)
{
	size_t i;

	assert(n <= sizeof(f->observed) - f->observed_len);
	for (i = 0; i < message->msg_iovlen && n; ++i) {
		size_t take = message->msg_iov[i].iov_len;

		if (take > n)
			take = n;
		memcpy(f->observed + f->observed_len, message->msg_iov[i].iov_base, take);
		f->observed_len += take;
		n -= take;
	}
	assert(n == 0);
	while (f->observed_len - f->parsed_len >= TR_WIRE_HEADER_SIZE) {
		struct tr_frame_header header;
		size_t frame_len;

		assert(tr_wire_header_decode(f->observed + f->parsed_len, &header) == TR_OK);
		frame_len = TR_WIRE_HEADER_SIZE + header.payload_len;
		if (f->observed_len - f->parsed_len < frame_len)
			break;
		f->parsed_len += frame_len;
		if (header.type == TR_FRAME_PING && f->refill_left) {
			f->refill_left--;
			/* The real producer API enqueues work; it does not dispatch inline. */
			assert(tr_reactor_send(f->handle, TR_FRAME_PING, 0, 0,
					       f->next_ping++, NULL) == TR_OK);
		}
	}
}

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	struct fixture *f = active;
	ssize_t n;

	/* Fault injection only: keep the real socket and EPOLLOUT resume path. */
	if (f && f->inject_eagain && f->observed_len == 1U && !f->eagain_count) {
		f->eagain_count++;
		errno = EAGAIN;
		return -1;
	}
	n = __real_sendmsg(fd, message, flags);
	if (!f || n <= 0)
		return n;
	observe_sent(f, message, (size_t)n);
	if (f->pause_after && f->observed_len == f->pause_after && !f->tx_entered) {
		assert(pthread_mutex_lock(&f->lock) == 0);
		f->tx_entered = 1;
		assert(pthread_cond_broadcast(&f->cond) == 0);
		while (!f->tx_released)
			assert(pthread_cond_wait(&f->cond, &f->lock) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
	}
	return n;
}

static void wait_flag(struct fixture *f, const int *flag)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10; /* Hang guard, not a performance threshold. */
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (!*flag)
		assert(pthread_cond_timedwait(&f->cond, &f->lock, &deadline) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static int gate(void *arg)
{
	struct fixture *f = arg;

	assert(pthread_mutex_lock(&f->lock) == 0);
	f->entered = 1;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	while (!f->released)
		assert(pthread_cond_wait(&f->cond, &f->lock) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return TR_OK;
}

static void *gate_thread(void *arg)
{
	struct fixture *f = arg;

	assert(tr_reactor_call(f->reactor, gate, f) == TR_OK);
	return NULL;
}

static pthread_t hold_owner(struct fixture *f)
{
	pthread_t thread;

	assert(pthread_mutex_lock(&f->lock) == 0);
	f->entered = f->released = 0;
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(pthread_create(&thread, NULL, gate_thread, f) == 0);
	wait_flag(f, &f->entered);
	return thread;
}

static void release_owner(struct fixture *f, pthread_t thread)
{
	assert(pthread_mutex_lock(&f->lock) == 0);
	f->released = 1;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(pthread_join(thread, NULL) == 0);
}

static void release_tx(struct fixture *f)
{
	assert(pthread_mutex_lock(&f->lock) == 0);
	f->tx_released = 1;
	assert(pthread_cond_broadcast(&f->cond) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void adopt_pair(struct fixture *f)
{
	int sockets[2];

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == 0);
	assert(tr_reactor_adopt_fd(f->reactor, sockets[0], &f->handle) == TR_OK);
	f->peer = sockets[1];
	assert(tr_reactor_quiesce(f->reactor) == TR_OK);
}

static void init_fixture(struct fixture *f, uint32_t budget, size_t pause_after,
			 int inject_eagain, unsigned refill)
{
	struct tr_reactor_config config = {
		.max_connections = 1U, .command_capacity = 256U,
		.tx_item_capacity = 8U, .control_tx_item_capacity = CONTROL_CAPACITY,
		.rx_buffer_count = 4U, .rx_buffer_size = FRAME_BYTES,
		.max_payload_len = FRAME_BYTES,
		.rx_budget_bytes = 4096U, .tx_budget_bytes = budget
	};

	memset(f, 0, sizeof(*f));
	f->pause_after = pause_after;
	f->inject_eagain = inject_eagain;
	f->refill_left = refill;
	f->next_ping = BURST + 1U;
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	assert(tr_buffer_pool_init(&f->pool, 48U, 4U * FRAME_BYTES) == TR_OK);
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &f->reactor) == TR_OK);
	active = f;
	assert(tr_reactor_start(f->reactor) == TR_OK);
	adopt_pair(f);
}

static void destroy_fixture(struct fixture *f)
{
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_reactor_get_stats(f->reactor, &f->stats) == TR_OK);
	assert(f->stats.total.tx_bytes == f->observed_len);
	assert(f->stats.max_per_turn.tx_bytes <= f->stats.limits.tx_bytes);
	assert(f->stats.max_per_turn.tx_dispatches <= f->stats.limits.tx_dispatches);
	active = NULL;
	tr_reactor_destroy(f->reactor);
	assert(close(f->peer) == 0);
	assert(tr_buffer_pool_free_count(&f->pool) == 48U);
	tr_buffer_pool_destroy(&f->pool);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static void send_item(struct fixture *f, uint16_t type, uint32_t flags,
		      uint32_t stream, uint64_t id, uint32_t len)
{
	struct tr_buffer *buffer = NULL;

	if (len) {
		assert(tr_buffer_acquire(&f->pool, len, &buffer) == TR_OK);
		memset(buffer->data, 0x5a, len);
		buffer->len = len;
	}
	assert(tr_reactor_send(f->handle, type, flags, stream, id, buffer) == TR_OK);
}

static void send_data(struct fixture *f, uint64_t id, unsigned frames)
{
	send_item(f, TR_FRAME_DATA, 0U, 1U, id, frames * FRAME_BYTES);
}

static void read_exact(int fd, void *buffer, size_t len)
{
	unsigned char *p = buffer;

	while (len) {
		struct pollfd pollfd = {.fd = fd, .events = POLLIN};
		ssize_t n;
		int ret;

		do {
			ret = poll(&pollfd, 1U, 10000);
		} while (ret < 0 && errno == EINTR);
		assert(ret == 1);
		n = read(fd, p, len);
		if (n < 0 && (errno == EINTR || errno == EAGAIN))
			continue;
		assert(n > 0);
		p += (size_t)n;
		len -= (size_t)n;
	}
}

static void expect_item(struct fixture *f, uint16_t type, uint32_t flags,
			uint32_t stream, uint64_t id, uint32_t len)
{
	struct tr_frame_header header;
	struct tr_wire_limits limits = {.max_payload_len = FRAME_BYTES};
	unsigned char raw[TR_WIRE_HEADER_SIZE];
	unsigned char payload[FRAME_BYTES];
	uint32_t i;

	read_exact(f->peer, raw, sizeof(raw));
	assert(tr_wire_header_decode(raw, &header) == TR_OK);
	assert(tr_wire_header_validate(raw, &header, &limits) == TR_OK);
	if (header.type != type || header.message_id != id)
		fprintf(stderr, "wire expected type=%u id=%llu, got type=%u id=%llu\n",
			(unsigned)type, (unsigned long long)id, (unsigned)header.type,
			(unsigned long long)header.message_id);
	assert(header.type == type && header.message_id == id);
	assert(header.flags == flags && header.stream_id == stream);
	assert(header.payload_len == len);
	read_exact(f->peer, payload, len);
	assert(header.payload_crc32c == tr_crc32c(payload, len));
	for (i = 0; i < len; ++i)
		assert(payload[i] == 0x5a);
}

static void expect_data(struct fixture *f, uint64_t id, unsigned i, unsigned count)
{
	uint32_t flags = (i == 0 ? TR_FRAME_F_FIRST : 0U) |
		(i + 1U == count ? TR_FRAME_F_LAST : 0U);

	expect_item(f, TR_FRAME_DATA, flags, 1U, id, FRAME_BYTES);
}

static void test_priority(uint32_t budget)
{
	struct fixture f;
	pthread_t gate_id;
	unsigned i;

	init_fixture(&f, budget, 0, 0, 0);
	gate_id = hold_owner(&f);
	send_data(&f, 1U, 4U);
	send_item(&f, TR_FRAME_PING, 0, 0, 11U, 0);
	send_item(&f, TR_FRAME_PONG, 0, 0, 12U, 0);
	send_item(&f, TR_FRAME_WINDOW_UPDATE, 0, 1U, 13U, 0);
	send_data(&f, 2U, 1U);
	release_owner(&f, gate_id);
	expect_item(&f, TR_FRAME_PING, 0, 0, 11U, 0);
	expect_item(&f, TR_FRAME_PONG, 0, 0, 12U, 0);
	expect_item(&f, TR_FRAME_WINDOW_UPDATE, 0, 1U, 13U, 0);
	for (i = 0; i < 4U; ++i)
		expect_data(&f, 1U, i, 4U);
	expect_data(&f, 2U, 0, 1U);
	destroy_fixture(&f);
	printf("priority/DATA-order/budget=%u: ok\n", budget);
}

static void test_barriers(void)
{
	static const struct {
		uint16_t type;
		uint32_t flags;
		uint32_t stream;
		uint32_t len;
	} barriers[] = {
		{TR_FRAME_HELLO, 0, 0, 1U}, {TR_FRAME_HELLO_ACK, 0, 0, 1U},
		{TR_FRAME_STREAM_OPEN, 0, 1U, 0}, {TR_FRAME_STREAM_CLOSE, 0, 1U, 0},
		{TR_FRAME_GOAWAY, 0, 0, 0}, {TR_FRAME_ERROR, 0, 0, 0},
		{TR_FRAME_PING, 0, 0, 1U}, {TR_FRAME_PONG, 0, 1U, 0},
		{TR_FRAME_WINDOW_UPDATE, 0, 0, 0},
		{TR_FRAME_PING, TR_FRAME_F_LANE_BULK, 0, 0}
	};
	size_t b;

	for (b = 0; b < sizeof(barriers) / sizeof(barriers[0]); ++b) {
		struct fixture f;
		pthread_t gate_id;

		init_fixture(&f, 17U, 0, 0, 0);
		gate_id = hold_owner(&f);
		send_data(&f, 1U, 2U);
		send_item(&f, barriers[b].type, barriers[b].flags, barriers[b].stream,
			  2U, barriers[b].len);
		send_item(&f, TR_FRAME_PING, 0, 0, 3U, 0);
		send_item(&f, TR_FRAME_WINDOW_UPDATE, 0, 1U, 4U, 0);
		send_data(&f, 5U, 1U);
		release_owner(&f, gate_id);
		expect_data(&f, 1U, 0, 2U);
		expect_data(&f, 1U, 1U, 2U);
		expect_item(&f, barriers[b].type, barriers[b].flags, barriers[b].stream,
			    2U, barriers[b].len);
		expect_item(&f, TR_FRAME_PING, 0, 0, 3U, 0);
		expect_item(&f, TR_FRAME_WINDOW_UPDATE, 0, 1U, 4U, 0);
		expect_data(&f, 5U, 0, 1U);
		destroy_fixture(&f);
	}
	puts("handshake/open/close/GOAWAY/noncanonical-control barriers: ok");
}

static void test_mid_frame(size_t pause_after, int inject_eagain)
{
	struct fixture f;
	pthread_t gate_id;
	uint32_t budget = (uint32_t)pause_after;

	init_fixture(&f, budget, pause_after, inject_eagain, 0);
	gate_id = hold_owner(&f);
	send_data(&f, 1U, 2U);
	release_owner(&f, gate_id);
	wait_flag(&f, &f.tx_entered);
	send_item(&f, TR_FRAME_PING, 0, 0, 2U, 0);
	send_item(&f, TR_FRAME_PONG, 0, 0, 3U, 0);
	release_tx(&f);
	expect_data(&f, 1U, 0, 2U);
	expect_item(&f, TR_FRAME_PING, 0, 0, 2U, 0);
	expect_item(&f, TR_FRAME_PONG, 0, 0, 3U, 0);
	expect_data(&f, 1U, 1U, 2U);
	destroy_fixture(&f);
	assert(f.eagain_count == (unsigned)inject_eagain);
	printf("active-frame/pause=%zu/EAGAIN=%d: ok\n", pause_after, inject_eagain);
}

static void test_refill_fairness(void)
{
	struct fixture f;
	pthread_t gate_id;
	unsigned frame;
	unsigned i;

	init_fixture(&f, 1U, 0, 0, 3U * BURST);
	gate_id = hold_owner(&f);
	send_data(&f, 100U, 4U);
	for (i = 1U; i <= BURST; ++i)
		send_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	release_owner(&f, gate_id);
	for (frame = 0; frame < 4U; ++frame) {
		for (i = 1U; i <= BURST; ++i)
			expect_item(&f, TR_FRAME_PING, 0, 0, frame * BURST + i, 0);
		expect_data(&f, 100U, frame, 4U);
	}
	destroy_fixture(&f);
	assert(f.refill_left == 0U);
	puts("continuous-CONTROL/8-overtakes/cross-turn-DATA-progress: ok");
}

static void test_older_control_fifo(void)
{
	struct fixture f;
	pthread_t gate_id;
	unsigned i;

	init_fixture(&f, 1U, 0, 0, 0);
	gate_id = hold_owner(&f);
	for (i = 1U; i <= 12U; ++i)
		send_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	send_data(&f, 100U, 2U);
	send_item(&f, TR_FRAME_PING, 0, 0, 13U, 0);
	release_owner(&f, gate_id);
	/* Older controls are not reordered behind DATA; the limit is on overtakes. */
	for (i = 1U; i <= 12U; ++i)
		expect_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	expect_data(&f, 100U, 0, 2U);
	expect_item(&f, TR_FRAME_PING, 0, 0, 13U, 0);
	expect_data(&f, 100U, 1U, 2U);
	destroy_fixture(&f);
	puts("older-CONTROL-FIFO/streak-saturation: ok");
}

static void test_close_reuse(void)
{
	struct fixture f;
	struct tr_conn_handle old;
	struct tr_buffer *unaccepted;
	pthread_t gate_id;
	unsigned i;

	init_fixture(&f, 1U, 1U, 0, 0);
	old = f.handle;
	gate_id = hold_owner(&f);
	send_data(&f, 1U, 4U);
	release_owner(&f, gate_id);
	wait_flag(&f, &f.tx_entered);
	for (i = 0; i < CONTROL_CAPACITY; ++i)
		send_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	send_data(&f, 2U, 2U);
	assert(tr_buffer_acquire(&f.pool, 1U, &unaccepted) == TR_OK);
	unaccepted->len = 0;
	assert(tr_reactor_send(f.handle, TR_FRAME_PING, 0, 0, 100U, unaccepted) == TR_AGAIN);
	tr_buffer_release(unaccepted); /* Failed send did not take ownership. */
	assert(tr_reactor_close(old) == TR_OK);
	release_tx(&f);
	assert(tr_reactor_quiesce(f.reactor) == TR_OK);
	assert(tr_buffer_pool_free_count(&f.pool) == 48U);
	assert(close(f.peer) == 0);
	adopt_pair(&f);
	assert(f.handle.slot == old.slot && f.handle.generation != old.generation);
	assert(tr_reactor_send(old, TR_FRAME_PING, 0, 0, 1U, NULL) == TR_ERR_STALE);
	/* Stop the observer parsing the deliberately truncated old byte stream. */
	gate_id = hold_owner(&f);
	f.parsed_len = f.observed_len;
	for (i = 0; i < CONTROL_CAPACITY; ++i)
		send_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	send_data(&f, 3U, 1U);
	release_owner(&f, gate_id);
	for (i = 0; i < CONTROL_CAPACITY; ++i)
		expect_item(&f, TR_FRAME_PING, 0, 0, i, 0);
	expect_data(&f, 3U, 0, 1U);
	destroy_fixture(&f);
	puts("partial-frame/queued-controls/close/reuse/full-pool: ok");
}

int main(void)
{
	assert(setvbuf(stdout, NULL, _IONBF, 0) == 0);
	alarm(60U);
	test_priority(1U);
	test_priority(17U);
	test_priority(4096U);
	test_barriers();
	test_mid_frame(1U, 0);
	test_mid_frame(17U, 0);
	test_mid_frame(TR_WIRE_HEADER_SIZE + FRAME_BYTES, 0);
	test_mid_frame(1U, 1);
	test_refill_fairness();
	test_older_control_fifo();
	test_close_reuse();
	alarm(0U);
	return 0;
}
