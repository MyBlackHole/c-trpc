#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/transport/channel/channel.h"
#include "../src/transport/protocol/wire.h"
#include "tr/status.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define WINDOW_BYTES 256U
#define MAX_HELD 8U

static _Atomic uint32_t fail_window_updates;
static _Atomic uint32_t window_attempts;

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload);

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload)
{
	if (type == TR_FRAME_WINDOW_UPDATE) {
		uint32_t remaining = atomic_load_explicit(
			&fail_window_updates, memory_order_relaxed);

		(void)atomic_fetch_add_explicit(&window_attempts, 1U,
					       memory_order_relaxed);
		while (remaining != 0U) {
			if (atomic_compare_exchange_weak_explicit(
				    &fail_window_updates, &remaining,
				    remaining - 1U, memory_order_relaxed,
				    memory_order_relaxed))
				return TR_AGAIN;
		}
	}
	return __real_tr_reactor_send(connection, type, flags, stream_id,
				      message_id, payload);
}

struct callback_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened;
	unsigned received;
	struct tr_stream_handle last_stream;
	struct tr_stream_handle incoming[MAX_HELD];
	struct tr_buffer *held[MAX_HELD];
};

static void sleep_ms(long ms)
{
	const struct timespec pause = { ms / 1000, (ms % 1000) * 1000000L };

	(void)nanosleep(&pause, NULL);
}

static void create_pair(int *a, int *b)
{
	int fds[2];
	int i;

	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
	for (i = 0; i < 2; ++i) {
		int flags = fcntl(fds[i], F_GETFL, 0);
		int fdflags = fcntl(fds[i], F_GETFD, 0);

		assert(flags >= 0 && fdflags >= 0);
		assert(fcntl(fds[i], F_SETFL, flags | O_NONBLOCK) == 0);
		assert(fcntl(fds[i], F_SETFD, fdflags | FD_CLOEXEC) == 0);
	}
	*a = fds[0];
	*b = fds[1];
}

static void init_context(struct callback_context *ctx)
{
	memset(ctx, 0, sizeof(*ctx));
	assert(pthread_mutex_init(&ctx->lock, NULL) == 0);
	assert(pthread_cond_init(&ctx->cond, NULL) == 0);
}

static void destroy_context(struct callback_context *ctx)
{
	unsigned i;

	for (i = 0; i < MAX_HELD; ++i)
		assert(ctx->held[i] == NULL);
	assert(pthread_cond_destroy(&ctx->cond) == 0);
	assert(pthread_mutex_destroy(&ctx->lock) == 0);
}

static enum tr_stream_data_disposition
on_data(struct tr_stream_handle stream, uint64_t message_id,
	struct tr_buffer *payload, void *arg)
{
	struct callback_context *ctx = arg;
	(void)message_id;

	pthread_mutex_lock(&ctx->lock);
	assert(ctx->received < MAX_HELD);
	ctx->incoming[ctx->received] = stream;
	ctx->held[ctx->received] = payload;
	ctx->received++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_STREAM_DATA_TAKE_OWNERSHIP;
}

static void on_stream_event(struct tr_stream_handle stream,
			    enum tr_stream_event event, int status, void *arg)
{
	struct callback_context *ctx = arg;
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	if (event == TR_STREAM_EVENT_OPENED) {
		ctx->opened++;
		ctx->last_stream = stream;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_count(struct callback_context *ctx, unsigned *value,
		       unsigned wanted)
{
	struct timespec limit;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &limit) == 0);
	limit.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < wanted && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &limit);
	assert(*value >= wanted);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_flow(struct tr_stream_handle stream, uint64_t limit)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_stream_flow_state flow;

		assert(tr_stream_get_flow_state(stream, &flow) == TR_OK);
		if (flow.tx_send_limit >= limit)
			return;
		sleep_ms(1);
	}
	assert(!"WINDOW_UPDATE did not recover without manual flush");
}

static void wait_attempts(uint32_t minimum)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (atomic_load_explicit(&window_attempts,
					 memory_order_relaxed) >= minimum)
			return;
		sleep_ms(1);
	}
	assert(!"expected injected WINDOW_UPDATE retry was not observed");
}

static void wait_closed(struct tr_channel *client, struct tr_channel *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_channel_active_streams(client) == 0U &&
		    tr_channel_active_streams(server) == 0U)
			return;
		sleep_ms(1);
	}
	assert(!"Stream close did not converge");
}

static struct tr_stream_handle
open_stream(struct tr_channel *client, struct callback_context *client_ctx,
	    struct callback_context *server_ctx, unsigned n)
{
	struct tr_stream_handle stream;

	assert(tr_stream_open(client, TR_LANE_CONTROL, &stream) == TR_OK);
	wait_count(client_ctx, &client_ctx->opened, n);
	wait_count(server_ctx, &server_ctx->opened, n);
	return stream;
}

static void send_bytes(struct tr_stream_handle stream,
		       struct tr_buffer_pool *pool, uint32_t len)
{
	struct tr_buffer *buffer;

	assert(tr_buffer_acquire(pool, len, &buffer) == TR_OK);
	memset(buffer->data, 0xa5, len);
	buffer->len = len;
	assert(tr_stream_send(stream, buffer) == TR_OK);
}

static int release_message(struct callback_context *ctx, unsigned index)
{
	struct tr_buffer *payload;
	struct tr_stream_handle stream;

	pthread_mutex_lock(&ctx->lock);
	assert(index < ctx->received);
	payload = ctx->held[index];
	stream = ctx->incoming[index];
	assert(payload != NULL);
	ctx->held[index] = NULL;
	pthread_mutex_unlock(&ctx->lock);
	return tr_stream_release_payload(stream, payload);
}

static void close_stream(struct tr_stream_handle client_stream,
			 struct callback_context *server_ctx,
			 struct tr_channel *client, struct tr_channel *server)
{
	struct tr_stream_handle server_stream;

	pthread_mutex_lock(&server_ctx->lock);
	server_stream = server_ctx->last_stream;
	pthread_mutex_unlock(&server_ctx->lock);

	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_stream_close(server_stream) == TR_OK);
	wait_closed(client, server);
}

int main(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_keepalive_config keepalive;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client = NULL;
	struct tr_channel *server = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle first;
	struct tr_stream_handle second;
	struct tr_stream_handle third;
	struct tr_stream_handle fourth;
	struct tr_stream_flow_state flow;
	struct tr_channel_stats stats;
	struct tr_buffer_pool pool;
	struct callback_context client_ctx;
	struct callback_context server_ctx;
	uint32_t before;
	int fd_client;
	int fd_server;

	init_context(&client_ctx);
	init_context(&server_ctx);
	create_pair(&fd_client, &fd_server);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 32U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, fd_client, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, fd_server, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 1U;
	channel_config.initial_window_bytes = WINDOW_BYTES;
	channel_config.window_update_threshold_bytes = 128U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, on_stream_event, NULL, &client_ctx,
				 &client) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 on_data, on_stream_event, NULL, &server_ctx,
				 &server) == TR_OK);
	assert(tr_channel_wait_ready(client, TR_LANE_CONTROL, 5000U) == TR_OK);
	assert(tr_channel_wait_ready(server, TR_LANE_CONTROL, 5000U) == TR_OK);
	assert(tr_buffer_pool_init(&pool, 8U, 512U) == TR_OK);

	/*
	 * Two failed submissions must be coalesced to the latest absolute
	 * credit limit. The timer stays active even though keepalive is disabled.
	 */
	first = open_stream(client, &client_ctx, &server_ctx, 1U);
	send_bytes(first, &pool, 128U);
	send_bytes(first, &pool, 128U);
	wait_count(&server_ctx, &server_ctx.received, 2U);
	atomic_store_explicit(&fail_window_updates, 2U, memory_order_relaxed);
	assert(release_message(&server_ctx, 0U) == TR_AGAIN);
	assert(release_message(&server_ctx, 1U) == TR_AGAIN);
	assert(tr_stream_get_flow_state(first, &flow) == TR_OK);
	assert(flow.tx_send_limit == WINDOW_BYTES);
	wait_attempts(3U);
	wait_flow(first, 2U * WINDOW_BYTES);
	assert(tr_channel_get_stats(server, &stats) == TR_OK);
	/* Absolute credit never overflows the peer window, regardless of how
	 * many retries were needed. The wire limit is checked above.
	 */
	assert(stats.window_updates_tx >= 1U);
	send_bytes(first, &pool, 256U);
	wait_count(&server_ctx, &server_ctx.received, 3U);
	assert(release_message(&server_ctx, 2U) == TR_OK);
	close_stream(first, &server_ctx, client, server);

	/* Disabling keepalive cannot disarm the only pending credit retry. */
	second = open_stream(client, &client_ctx, &server_ctx, 2U);
	keepalive.interval_ms = 10000U;
	keepalive.timeout_ms = 10000U;
	assert(tr_channel_enable_keepalive(server, &keepalive) == TR_OK);
	send_bytes(second, &pool, WINDOW_BYTES);
	wait_count(&server_ctx, &server_ctx.received, 4U);
	atomic_store_explicit(&fail_window_updates, 2U, memory_order_relaxed);
	assert(release_message(&server_ctx, 3U) == TR_AGAIN);
	assert(tr_channel_disable_keepalive(server) == TR_OK);
	wait_flow(second, 2U * WINDOW_BYTES);
	close_stream(second, &server_ctx, client, server);

	/*
	 * Retire a Stream with an outstanding retry, then reuse its only slot.
	 * The old absolute credit must never apply to the new stream_id.
	 */
	third = open_stream(client, &client_ctx, &server_ctx, 3U);
	send_bytes(third, &pool, WINDOW_BYTES);
	wait_count(&server_ctx, &server_ctx.received, 5U);
	atomic_store_explicit(&fail_window_updates, 100U, memory_order_relaxed);
	assert(release_message(&server_ctx, 4U) == TR_AGAIN);
	close_stream(third, &server_ctx, client, server);
	before = atomic_load_explicit(&window_attempts, memory_order_relaxed);
	fourth = open_stream(client, &client_ctx, &server_ctx, 4U);
	assert(fourth.slot == third.slot);
	assert(fourth.generation != third.generation);
	sleep_ms(100);
	assert(atomic_load_explicit(&window_attempts,
				    memory_order_relaxed) == before);
	assert(tr_stream_get_flow_state(fourth, &flow) == TR_OK);
	assert(flow.tx_send_limit == WINDOW_BYTES);
	close_stream(fourth, &server_ctx, client, server);

	/* A stopped Reactor must retain no callback into freed Channel storage. */
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client) == TR_OK);
	assert(tr_channel_destroy(server) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	destroy_context(&server_ctx);
	destroy_context(&client_ctx);
	return 0;
}
