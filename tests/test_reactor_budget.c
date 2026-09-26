#define _GNU_SOURCE
#include "tr/reactor.h"
#include "tr/command_queue.h"
#include "tr/crc32c.h"
#include "tr/status.h"
#include "../src/completion_queue.h"
#include "../src/reactor_internal.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define CALLBACKS 200U
#define PAYLOAD_SIZE 64U
#define WIRE_SIZE (2U * (TR_WIRE_HEADER_SIZE + PAYLOAD_SIZE))

struct test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_reactor *reactor;
	struct tr_reactor_timer_handle timer;
	struct tr_reactor_stats stats;
	uint32_t byte_budget;
	unsigned timer_goal;
	unsigned completion_goal;
	unsigned frame_goal;
	unsigned timers;
	unsigned completions;
	unsigned frames;
	/* Below are owner-only observations, delimited by command dequeue. */
	unsigned turn_timers;
	unsigned turn_completions;
	size_t turn_rx;
	size_t turn_tx;
	unsigned checked_turns;
	int monitor;
	int send_eagain;
	int watch_idle; /* Immutable while the Reactor is running. */
	int gate_first_recv;
	int first_recv_seen;
	/* Gate and progress predicates are protected by lock. */
	int entered;
	int release;
	int done;
	int idle;
	int rx_entered;
	int rx_release;
};

/* Installed before pthread_create and cleared after stop/join. */
static struct test_ctx *active;

size_t __real_tr_command_queue_pop_batch(struct tr_command_queue *queue,
					struct tr_command *out, size_t max);
ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);
ssize_t __real_recv(int fd, void *buffer, size_t len, int flags);
int __real_epoll_wait(int fd, struct epoll_event *events, int max, int timeout);

static void check_turn(struct test_ctx *ctx)
{
	assert(ctx->turn_completions <= 64U);
	assert(ctx->turn_timers <= 64U);
	assert(ctx->turn_rx <= ctx->byte_budget);
	assert(ctx->turn_tx <= ctx->byte_budget);
	ctx->checked_turns++;
	ctx->turn_completions = 0;
	ctx->turn_timers = 0;
	ctx->turn_rx = 0;
	ctx->turn_tx = 0;
}

size_t __wrap_tr_command_queue_pop_batch(struct tr_command_queue *queue,
					struct tr_command *out, size_t max)
{
	if (active && active->monitor)
		check_turn(active);
	assert(max <= 64U);
	return __real_tr_command_queue_pop_batch(queue, out, max);
}

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	size_t offered = 0;
	size_t i;
	ssize_t n;

	for (i = 0; i < message->msg_iovlen; ++i)
		offered += message->msg_iov[i].iov_len;
	if (active && active->monitor)
		assert(offered <= active->byte_budget - active->turn_tx);
	n = __real_sendmsg(fd, message, flags);
	if (active && active->monitor) {
		if (n > 0)
			active->turn_tx += (size_t)n;
		else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			active->send_eagain = 1;
	}
	return n;
}

ssize_t __wrap_recv(int fd, void *buffer, size_t len, int flags)
{
	ssize_t n;

	if (active && active->monitor)
		assert(len <= active->byte_budget - active->turn_rx);
	n = __real_recv(fd, buffer, len, flags);
	if (active && active->monitor && n > 0)
		active->turn_rx += (size_t)n;
	if (active && n > 0 && active->gate_first_recv && !active->first_recv_seen) {
		active->first_recv_seen = 1;
		assert(pthread_mutex_lock(&active->lock) == 0);
		active->rx_entered = 1;
		assert(pthread_cond_broadcast(&active->cond) == 0);
		while (!active->rx_release)
			assert(pthread_cond_wait(&active->cond, &active->lock) == 0);
		assert(pthread_mutex_unlock(&active->lock) == 0);
	}
	return n;
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int max, int timeout)
{
	if (active && active->watch_idle && active->send_eagain && timeout != 0) {
		assert(pthread_mutex_lock(&active->lock) == 0);
		active->idle = 1;
		assert(pthread_cond_broadcast(&active->cond) == 0);
		assert(pthread_mutex_unlock(&active->lock) == 0);
	}
	return __real_epoll_wait(fd, events, max, timeout);
}

static void wait_for(struct test_ctx *ctx, const int *predicate)
{
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	while (!*predicate)
		assert(pthread_cond_wait(&ctx->cond, &ctx->lock) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
}

static void maybe_done(struct test_ctx *ctx)
{
	if (ctx->timers != ctx->timer_goal ||
	    ctx->completions != ctx->completion_goal || ctx->frames != ctx->frame_goal)
		return;
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	ctx->done = 1;
	assert(pthread_cond_broadcast(&ctx->cond) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
}

static void completion(void *arg)
{
	struct test_ctx *ctx = arg;

	ctx->completions++;
	ctx->turn_completions++;
	maybe_done(ctx);
}

static uint64_t timer(void *arg, uint64_t now_ns)
{
	struct test_ctx *ctx = arg;

	(void)now_ns;
	ctx->timers++;
	ctx->turn_timers++;
	maybe_done(ctx);
	return ctx->timers < ctx->timer_goal ? 1U : 0U;
}

static enum tr_frame_disposition frame_cb(struct tr_conn_handle handle,
					  struct tr_frame *frame, void *arg)
{
	struct test_ctx *ctx = arg;
	uint32_t i;

	(void)handle;
	assert(frame->header.type == TR_FRAME_DATA);
	assert(frame->header.message_id == 77U);
	assert(frame->payload && frame->payload->len == PAYLOAD_SIZE);
	for (i = 0; i < frame->payload->len; ++i)
		assert(frame->payload->data[i] == 0x5a);
	ctx->frames++;
	maybe_done(ctx);
	return TR_FRAME_RELEASE;
}

/* Test-only blocking gate: production owner callbacks must never wait. */
static int gate(void *arg)
{
	struct test_ctx *ctx = arg;

	assert(pthread_mutex_lock(&ctx->lock) == 0);
	ctx->entered = 1;
	assert(pthread_cond_broadcast(&ctx->cond) == 0);
	while (!ctx->release)
		assert(pthread_cond_wait(&ctx->cond, &ctx->lock) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
	ctx->monitor = 1;
	if (ctx->timer_goal)
		assert(tr_reactor_timer_arm(ctx->timer, 1U) == TR_OK);
	return TR_OK;
}

static void *gate_thread(void *arg)
{
	struct test_ctx *ctx = arg;

	assert(tr_reactor_call(ctx->reactor, gate, ctx) == TR_OK);
	return NULL;
}

static void release_gate(struct test_ctx *ctx, pthread_t thread)
{
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	ctx->release = 1;
	assert(pthread_cond_broadcast(&ctx->cond) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
	assert(pthread_join(thread, NULL) == 0);
}

static void check_stats(const struct tr_reactor_stats *stats, uint32_t budget)
{
	assert(stats->limits.commands == 64U);
	assert(stats->limits.completions == 64U);
	assert(stats->limits.timer_callbacks == 64U);
	assert(stats->limits.rx_dispatches == 64U);
	assert(stats->limits.tx_dispatches == 64U);
	assert(stats->limits.rx_bytes == budget && stats->limits.tx_bytes == budget);
#define CHECK_WORK(field) do { \
	assert(stats->max_per_turn.field <= stats->limits.field); \
	assert(stats->total.field >= stats->max_per_turn.field); \
	assert(stats->budget_hits.field <= stats->turns); \
} while (0)
	CHECK_WORK(commands);
	CHECK_WORK(completions);
	CHECK_WORK(timer_callbacks);
	CHECK_WORK(rx_bytes);
	CHECK_WORK(tx_bytes);
	CHECK_WORK(rx_dispatches);
	CHECK_WORK(tx_dispatches);
#undef CHECK_WORK
	assert(stats->epoll_polls + stats->epoll_waits <= stats->turns);
}

static int finish_on_owner(void *arg)
{
	struct test_ctx *ctx = arg;
	struct tr_reactor_stats again;

	/* Two direct owner snapshots cannot observe a partly recorded turn. */
	assert(tr_reactor_get_stats(ctx->reactor, &ctx->stats) == TR_OK);
	assert(tr_reactor_get_stats(ctx->reactor, &again) == TR_OK);
	assert(memcmp(&again, &ctx->stats, sizeof(again)) == 0);
	check_stats(&again, ctx->byte_budget);
	check_turn(ctx);
	ctx->monitor = 0;
	assert(ctx->checked_turns != 0);
	return TR_OK;
}

static void create_ctx(struct test_ctx *ctx, uint32_t budget, uint32_t connections)
{
	struct tr_reactor_config config = {
		.max_connections = connections, .command_capacity = 512U,
		.rx_buffer_count = 8U, .rx_buffer_size = PAYLOAD_SIZE,
		.max_payload_len = PAYLOAD_SIZE,
		.rx_budget_bytes = budget, .tx_budget_bytes = budget
	};

	struct tr_reactor_stats unchanged;

	memset(ctx, 0, sizeof(*ctx));
	ctx->byte_budget = budget;
	assert(pthread_mutex_init(&ctx->lock, NULL) == 0);
	assert(pthread_cond_init(&ctx->cond, NULL) == 0);
	assert(tr_reactor_create(&config, frame_cb, NULL, ctx, &ctx->reactor) == TR_OK);
	assert(tr_reactor_timer_register(ctx->reactor, timer, ctx, &ctx->timer) == TR_OK);
	assert(tr_reactor_get_stats(ctx->reactor, &ctx->stats) == TR_OK);
	check_stats(&ctx->stats, budget);
	assert(ctx->stats.turns == 0 && ctx->stats.total.commands == 0);
	unchanged = ctx->stats;
	assert(tr_reactor_get_stats(NULL, &ctx->stats) == TR_ERR_INVALID);
	assert(memcmp(&unchanged, &ctx->stats, sizeof(unchanged)) == 0);
	assert(tr_reactor_get_stats(ctx->reactor, NULL) == TR_ERR_INVALID);
	active = ctx;
}

static void destroy_ctx(struct test_ctx *ctx)
{
	struct tr_reactor_stats running;

	assert(tr_reactor_get_stats(ctx->reactor, &running) == TR_OK);
	check_stats(&running, ctx->byte_budget);
	assert(tr_reactor_call(ctx->reactor, finish_on_owner, ctx) == TR_OK);
	assert(ctx->stats.turns >= running.turns);
	assert(tr_reactor_stop(ctx->reactor) == TR_OK);
	assert(tr_reactor_get_stats(ctx->reactor, &ctx->stats) == TR_OK);
	check_stats(&ctx->stats, ctx->byte_budget);
	assert(ctx->stats.turns > running.turns);
	active = NULL;
	tr_reactor_destroy(ctx->reactor);
	assert(pthread_cond_destroy(&ctx->cond) == 0);
	assert(pthread_mutex_destroy(&ctx->lock) == 0);
}

static void read_exact(int fd, void *buffer, size_t len)
{
	unsigned char *p = buffer;

	while (len) {
		struct pollfd pollfd = { .fd = fd, .events = POLLIN };
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

static void encode_data(unsigned char wire[WIRE_SIZE])
{
	unsigned i;

	for (i = 0; i < 2U; ++i) {
		unsigned char *raw = wire + i * (TR_WIRE_HEADER_SIZE + PAYLOAD_SIZE);
		struct tr_frame_header header = {
			.version = TR_WIRE_ENV_VERSION, .type = TR_FRAME_DATA,
			.flags = i ? TR_FRAME_F_LAST : TR_FRAME_F_FIRST,
			.stream_id = 1U, .message_id = 77U, .payload_len = PAYLOAD_SIZE
		};

		memset(raw + TR_WIRE_HEADER_SIZE, 0x5a, PAYLOAD_SIZE);
		header.payload_crc32c = tr_crc32c(raw + TR_WIRE_HEADER_SIZE, PAYLOAD_SIZE);
		assert(tr_wire_header_encode(raw, &header) == TR_OK);
	}
}

static void test_mixed_budget(uint32_t budget)
{
	struct test_ctx ctx;
	struct tr_buffer_pool pool;
	struct tr_conn_handle handles[2];
	unsigned char wire[WIRE_SIZE];
	unsigned char received[WIRE_SIZE];
	pthread_t thread;
	int sockets[2][2];
	unsigned i;

	create_ctx(&ctx, budget, 4U);
	ctx.timer_goal = CALLBACKS;
	ctx.completion_goal = CALLBACKS;
	ctx.frame_goal = 4U;
	assert(tr_buffer_pool_init(&pool, 8U, PAYLOAD_SIZE / 2U) == TR_OK);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	for (i = 0; i < 2U; ++i) {
		assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets[i]) == 0);
		assert(tr_reactor_adopt_fd(ctx.reactor, sockets[i][0], &handles[i]) == TR_OK);
	}
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);
	assert(pthread_create(&thread, NULL, gate_thread, &ctx) == 0);
	wait_for(&ctx, &ctx.entered);
	for (i = 0; i < CALLBACKS; ++i)
		assert(tr_reactor_complete(ctx.reactor, completion, &ctx) == TR_OK);
	encode_data(wire);
	for (i = 0; i < 2U; ++i) {
		struct tr_buffer *slices[4];
		unsigned j;

		for (j = 0; j < 4U; ++j) {
			assert(tr_buffer_acquire(&pool, PAYLOAD_SIZE / 2U, &slices[j]) == TR_OK);
			slices[j]->len = PAYLOAD_SIZE / 2U;
			memset(slices[j]->data, 0x5a, slices[j]->len);
		}
		assert(tr_reactor_sendv(handles[i], TR_FRAME_DATA, 0U, 1U, 77U,
					 slices, 4U) == TR_OK);
		assert(write(sockets[i][1], wire, sizeof(wire)) == (ssize_t)sizeof(wire));
	}
	release_gate(&ctx, thread);
	for (i = 0; i < 2U; ++i) {
		read_exact(sockets[i][1], received, sizeof(received));
		assert(memcmp(wire, received, sizeof(wire)) == 0);
	}
	wait_for(&ctx, &ctx.done);
	destroy_ctx(&ctx);
	assert(ctx.stats.total.completions == CALLBACKS);
	assert(ctx.stats.total.timer_callbacks == CALLBACKS);
	assert(ctx.stats.total.rx_bytes == 2U * WIRE_SIZE);
	assert(ctx.stats.total.tx_bytes == 2U * WIRE_SIZE);
	assert(ctx.stats.max_per_turn.completions == 64U);
	assert(ctx.stats.max_per_turn.timer_callbacks == 64U);
	assert(ctx.stats.budget_hits.completions == CALLBACKS / 64U);
	assert(ctx.stats.budget_hits.timer_callbacks == CALLBACKS / 64U);
	assert(ctx.stats.timer_lateness_ns_max != 0);
	assert(ctx.stats.shutdown_completions == 0);
	if (budget < WIRE_SIZE) {
		assert(ctx.stats.max_per_turn.rx_bytes == budget);
		assert(ctx.stats.max_per_turn.tx_bytes == budget);
		assert(ctx.stats.budget_hits.rx_bytes != 0);
		assert(ctx.stats.budget_hits.tx_bytes != 0);
	}
	for (i = 0; i < 2U; ++i)
		assert(close(sockets[i][1]) == 0);
	assert(tr_buffer_pool_free_count(&pool) == 8U);
	tr_buffer_pool_destroy(&pool);
	printf("mixed/completion/timer/RX/TX budget=%u: ok\n", budget);
}

static void test_eagain_wait(void)
{
	struct test_ctx ctx;
	struct tr_conn_handle handle;
	struct tr_frame_header header = {
		.version = TR_WIRE_ENV_VERSION, .type = TR_FRAME_PING
	};
	unsigned char expected[TR_WIRE_HEADER_SIZE];
	unsigned char padding[4096] = {0};
	unsigned char *received;
	size_t filled = 0;
	pthread_t thread;
	int sockets[2];
	int size = 4096;

	create_ctx(&ctx, 17U, 4U);
	ctx.watch_idle = 1;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) == 0);
	assert(setsockopt(sockets[0], SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0);
	for (;;) {
		ssize_t n = write(sockets[0], padding, sizeof(padding));

		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			break;
		assert(n > 0 && filled < 1024U * 1024U);
		filled += (size_t)n;
	}
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(ctx.reactor, sockets[0], &handle) == TR_OK);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);
	assert(pthread_create(&thread, NULL, gate_thread, &ctx) == 0);
	wait_for(&ctx, &ctx.entered);
	assert(tr_reactor_send(handle, TR_FRAME_PING, 0U, 0U, 0U, NULL) == TR_OK);
	release_gate(&ctx, thread);
	wait_for(&ctx, &ctx.idle); /* Real EAGAIN must permit a blocking epoll wait. */
	received = malloc(filled + TR_WIRE_HEADER_SIZE);
	assert(received);
	read_exact(sockets[1], received, filled + TR_WIRE_HEADER_SIZE);
	assert(tr_wire_header_encode(expected, &header) == TR_OK);
	assert(memcmp(received + filled, expected, sizeof(expected)) == 0);
	free(received);
	destroy_ctx(&ctx);
	assert(ctx.stats.total.tx_bytes == TR_WIRE_HEADER_SIZE);
	assert(ctx.stats.total.rx_bytes == 0);
	assert(ctx.stats.epoll_waits != 0);
	assert(close(sockets[1]) == 0);
	puts("TX/EAGAIN/wait/resume: ok");
}

static void test_rx_ready_close_reuse(void)
{
	struct test_ctx ctx;
	struct tr_conn_handle old_handle;
	struct tr_conn_handle new_handle;
	struct tr_reactor_stats stats;
	unsigned char wire[WIRE_SIZE];
	pthread_t thread;
	int old_sockets[2];
	int new_sockets[2];

	create_ctx(&ctx, 1U, 1U);
	ctx.gate_first_recv = 1;
	ctx.frame_goal = 2U;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, old_sockets) == 0);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(ctx.reactor, old_sockets[0], &old_handle) == TR_OK);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);
	assert(pthread_create(&thread, NULL, gate_thread, &ctx) == 0);
	wait_for(&ctx, &ctx.entered);
	encode_data(wire);
	assert(write(old_sockets[1], wire, sizeof(wire)) == (ssize_t)sizeof(wire));
	release_gate(&ctx, thread);
	wait_for(&ctx, &ctx.rx_entered);
	/* The first byte uses the turn quota; close must remove its RX continuation. */
	assert(tr_reactor_close(old_handle) == TR_OK);
	assert(pthread_mutex_lock(&ctx.lock) == 0);
	ctx.rx_release = 1;
	assert(pthread_cond_broadcast(&ctx.cond) == 0);
	assert(pthread_mutex_unlock(&ctx.lock) == 0);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);
	assert(tr_reactor_get_stats(ctx.reactor, &stats) == TR_OK);
	assert(stats.total.rx_bytes == 1U);
	assert(close(old_sockets[1]) == 0);

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, new_sockets) == 0);
	assert(tr_reactor_adopt_fd(ctx.reactor, new_sockets[0], &new_handle) == TR_OK);
	assert(new_handle.slot == old_handle.slot);
	assert(new_handle.generation != old_handle.generation);
	assert(tr_reactor_close(old_handle) == TR_ERR_STALE);
	assert(write(new_sockets[1], wire, sizeof(wire)) == (ssize_t)sizeof(wire));
	wait_for(&ctx, &ctx.done);
	destroy_ctx(&ctx);
	assert(ctx.stats.total.rx_bytes == 1U + WIRE_SIZE);
	assert(close(new_sockets[1]) == 0);
	puts("RX/queued-close/same-slot-reuse: ok");
}

static void noop(void *arg)
{
	(void)arg;
}

static void test_completion_zero_budget(void)
{
	struct tr_completion_queue queue;
	struct tr_completion item = { .fn = noop };
	struct tr_completion out = {0};
	int more = -1;
	int wake = -1;

	assert(tr_completion_queue_pop_batch(NULL, &out, 0U, &more) == 0U);
	assert(more == 0);
	assert(tr_completion_queue_init(&queue, 1U) == TR_OK);
	assert(tr_completion_queue_pop_batch(&queue, &out, 0U, &more) == 0U);
	assert(more == 0);
	assert(tr_completion_queue_push(&queue, &item, &wake) == TR_OK && wake == 1);
	assert(tr_completion_queue_pop_batch(&queue, &out, 0U, &more) == 0U);
	assert(more == 1 && out.fn == NULL && queue.wake_pending == 1);
	assert(tr_completion_queue_pop_batch(&queue, &out, 1U, &more) == 1U);
	assert(more == 0 && out.fn == noop && queue.wake_pending == 0);
	tr_completion_queue_destroy(&queue);
	puts("completion/zero-budget/pending: ok");
}

int main(void)
{
	alarm(60U); /* Hang protection, not a latency or throughput threshold. */
	test_mixed_budget(1U);
	test_mixed_budget(17U);
	test_mixed_budget(4096U);
	test_eagain_wait();
	test_rx_ready_close_reuse();
	test_completion_zero_budget();
	alarm(0U);
	return 0;
}
