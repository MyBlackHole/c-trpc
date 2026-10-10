#define _POSIX_C_SOURCE 200809L
#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel.h"
#include "../src/transport/protocol/wire.h"
#include "tr/status.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define COMMAND_CAPACITY 8U
#define PAYLOAD_COUNT 8U
#define BAD_STREAM_ID 777U

enum malformed_frame {
	BAD_WINDOW_UPDATE,
	BAD_DATA,
	BAD_HELLO,
	BAD_HELLO_ACK
};

struct test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_conn_handle bad_server_conn;
	struct tr_channel *bad_server;
	struct tr_channel *good_client;
	struct tr_channel *good_server;
	unsigned bad_server_down;
	unsigned bad_server_opened;
	unsigned bad_server_stream_errors;
	unsigned good_client_opened;
	unsigned good_server_opened;
	unsigned good_down;
	int bad_server_status;
	int bad_server_stream_status;
	struct tr_stream_handle good_server_stream;
	_Atomic int inject;
	_Atomic unsigned abort_calls;
	_Atomic int abort_status;
	_Atomic int close_would_block;
};

static struct test_ctx *active;

int __real_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
				     int status);

/*
 * The malformed frame callback is running on the sole Reactor owner.
 * Fill the *real* command ring with harmless RESUME_RX entries right before
 * the requested abort; no consumer can pop them until this callback returns.
 * An ordinary tr_reactor_close would now fail with TR_AGAIN, whereas the
 * owner-only abort must retire the exact connection immediately.
 */
int __wrap_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
				     int status)
{
	struct test_ctx *ctx = active;

	if (ctx && atomic_load_explicit(&ctx->inject, memory_order_acquire) &&
	    connection.reactor == ctx->bad_server_conn.reactor &&
	    connection.slot == ctx->bad_server_conn.slot &&
	    connection.generation == ctx->bad_server_conn.generation) {
		int saturated = 0;
		unsigned i;

		assert(status < 0);
		for (i = 0; i <= COMMAND_CAPACITY + 2U; ++i) {
			int ret = tr_reactor_resume_rx(connection);

			if (ret == TR_AGAIN) {
				saturated = 1;
				break;
			}
			assert(ret == TR_OK);
		}
		assert(saturated);
		assert(tr_reactor_close(connection) == TR_AGAIN);
		atomic_store_explicit(&ctx->close_would_block, 1,
				      memory_order_release);
		atomic_store_explicit(&ctx->abort_status, status,
				      memory_order_release);
		atomic_fetch_add_explicit(&ctx->abort_calls, 1U,
					  memory_order_release);
	}
	return __real_tr_reactor_abort_on_owner(connection, status);
}

static void pause_1ms(void)
{
	struct timespec pause = { 0, 1000000L };

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

static void wait_count(struct test_ctx *ctx, unsigned *value,
		       unsigned target, const char *name)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline);
	if (*value < target)
		fprintf(stderr, "%s: expected=%u actual=%u error=%d\n",
			name, target, *value, ret);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_streams_empty(struct tr_channel *client,
			       struct tr_channel *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		if (tr_channel_active_streams(client) == 0U &&
		    tr_channel_active_streams(server) == 0U)
			return;
		pause_1ms();
	}
	assert(!"peer Stream slots were not retired");
}

static void on_channel(struct tr_channel *channel, enum tr_channel_event event,
		       int status, void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN) {
		if (channel == ctx->bad_server) {
			ctx->bad_server_down++;
			ctx->bad_server_status = status;
		} else if (channel == ctx->good_client ||
			   channel == ctx->good_server) {
			ctx->good_down++;
		}
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void on_stream(struct tr_stream_handle stream,
		      enum tr_stream_event event, int status, void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	if (stream.channel == ctx->bad_server) {
		if (event == TR_STREAM_EVENT_OPENED)
			ctx->bad_server_opened++;
		else if (event == TR_STREAM_EVENT_ERROR) {
			ctx->bad_server_stream_errors++;
			ctx->bad_server_stream_status = status;
		}
	} else if (stream.channel == ctx->good_client &&
		   event == TR_STREAM_EVENT_OPENED) {
		ctx->good_client_opened++;
	} else if (stream.channel == ctx->good_server &&
		   event == TR_STREAM_EVENT_OPENED) {
		ctx->good_server_opened++;
		ctx->good_server_stream = stream;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int abort_stale_on_owner(void *arg)
{
	struct tr_conn_handle *old = arg;

	return tr_reactor_abort_on_owner(*old, TR_ERR_STATE) == TR_ERR_STALE ?
			TR_OK : TR_ERR_STATE;
}

static void send_bad_frame(enum malformed_frame kind,
			   struct tr_conn_handle sender,
			   struct tr_buffer_pool *pool)
{
	struct tr_buffer *payload = NULL;
	uint16_t type;
	uint32_t flags = 0U;
	uint32_t stream_id = BAD_STREAM_ID;
	uint64_t message_id = 1U;

	switch (kind) {
	case BAD_WINDOW_UPDATE:
		type = TR_FRAME_WINDOW_UPDATE;
		break;
	case BAD_DATA:
		type = TR_FRAME_DATA;
		flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
		break;
	case BAD_HELLO:
		type = TR_FRAME_HELLO;
		stream_id = 0U;
		message_id = 0U;
		break;
	case BAD_HELLO_ACK:
		type = TR_FRAME_HELLO_ACK;
		stream_id = 0U;
		message_id = 0U;
		break;
	default:
		assert(!"unknown malformed frame");
		return;
	}

	if (type == TR_FRAME_DATA || type == TR_FRAME_HELLO ||
	    type == TR_FRAME_HELLO_ACK) {
		uint32_t size = type == TR_FRAME_DATA ? 1U : 32U;

		assert(tr_buffer_acquire(pool, size, &payload) == TR_OK);
		memset(payload->data, 0, size);
		payload->len = size;
	}
	/* On success the Reactor owns payload, including the malformed frame. */
	assert(tr_reactor_send(sender, type, flags, stream_id, message_id,
			       payload) == TR_OK);
}

static void test_protocol_error(enum malformed_frame kind)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *bad_client = NULL;
	struct tr_channel *bad_server = NULL;
	struct tr_channel *good_client = NULL;
	struct tr_channel *good_server = NULL;
	struct tr_conn_handle bad_client_conn;
	struct tr_conn_handle bad_server_conn;
	struct tr_conn_handle good_client_conn;
	struct tr_conn_handle good_server_conn;
	struct tr_conn_handle replacement_conn;
	struct tr_conn_handle replacements[2];
	struct tr_stream_handle bad_stream;
	struct tr_stream_handle good_stream;
	struct tr_stream_handle server_stream;
	struct tr_buffer_pool pool;
	struct test_ctx ctx;
	enum tr_channel_lane_state state;
	int bcfd, bsfd, gcfd, gsfd;
	int replacement_fd, replacement_peer[2] = { -1, -1 };
	int found_replacement = 0;
	unsigned replacement_count = 0;
	int expected_status;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	active = &ctx;
	create_pair(&bcfd, &bsfd);
	create_pair(&gcfd, &gsfd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8U;
	reactor_config.command_capacity = COMMAND_CAPACITY;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 32U;
	reactor_config.rx_buffer_count = 64U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL,
				 &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, bcfd, &bad_client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, bsfd, &bad_server_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, gcfd, &good_client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, gsfd, &good_server_conn) == TR_OK);
	ctx.bad_server_conn = bad_server_conn;

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 2U;
	channel_config.initial_window_bytes = 512U;
	channel_config.window_update_threshold_bytes = 128U;
	channel_config.role = TR_CHANNEL_CLIENT;
	assert(tr_channel_create(&channel_config, bad_client_conn,
				 bad_client_conn, NULL, on_stream,
				 on_channel, &ctx, &bad_client) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, bad_server_conn,
				 bad_server_conn, NULL, on_stream,
				 on_channel, &ctx, &bad_server) == TR_OK);
	ctx.bad_server = bad_server;

	channel_config.role = TR_CHANNEL_CLIENT;
	assert(tr_channel_create(&channel_config, good_client_conn,
				 good_client_conn, NULL, on_stream,
				 on_channel, &ctx, &good_client) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, good_server_conn,
				 good_server_conn, NULL, on_stream,
				 on_channel, &ctx, &good_server) == TR_OK);
	ctx.good_client = good_client;
	ctx.good_server = good_server;

	assert(tr_channel_wait_ready(bad_client, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);
	assert(tr_channel_wait_ready(bad_server, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);
	assert(tr_channel_wait_ready(good_client, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);
	assert(tr_channel_wait_ready(good_server, TR_LANE_CONTROL, 5000U) ==
	       TR_OK);
	assert(tr_buffer_pool_init(&pool, PAYLOAD_COUNT, 64U) == TR_OK);

	/* A live Stream proves Channel error cleanup runs synchronously. */
	assert(tr_stream_open(bad_client, TR_LANE_CONTROL, &bad_stream) ==
	       TR_OK);
	wait_count(&ctx, &ctx.bad_server_opened, 1U, "bad Stream opened");
	(void)bad_stream;

	atomic_store_explicit(&ctx.inject, 1, memory_order_release);
	send_bad_frame(kind, bad_client_conn, &pool);
	wait_count(&ctx, &ctx.bad_server_down, 1U, "bad Channel down");
	wait_count(&ctx, &ctx.bad_server_stream_errors, 1U,
		   "bad Stream error");

	assert(atomic_load_explicit(&ctx.abort_calls, memory_order_acquire) ==
	       1U);
	assert(atomic_load_explicit(&ctx.close_would_block,
				    memory_order_acquire));
	expected_status = atomic_load_explicit(&ctx.abort_status,
					       memory_order_acquire);
	assert(expected_status < 0);
	assert(ctx.bad_server_status == expected_status);
	assert(ctx.bad_server_stream_status == expected_status);
	assert(tr_channel_get_lane_state(bad_server, TR_LANE_CONTROL, &state) ==
	       TR_OK);
	assert(state == TR_CHANNEL_LANE_DOWN);
	assert(tr_channel_active_streams(bad_server) == 0U);

	/* The other connection on this same Reactor remains fully functional. */
	assert(tr_channel_get_lane_state(good_server, TR_LANE_CONTROL, &state) ==
	       TR_OK);
	assert(state == TR_CHANNEL_LANE_UP);
	assert(tr_stream_open(good_client, TR_LANE_CONTROL, &good_stream) ==
	       TR_OK);
	wait_count(&ctx, &ctx.good_client_opened, 1U, "healthy client Stream");
	wait_count(&ctx, &ctx.good_server_opened, 1U, "healthy server Stream");
	pthread_mutex_lock(&ctx.lock);
	server_stream = ctx.good_server_stream;
	assert(ctx.good_down == 0U);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_stream_close(good_stream) == TR_OK);
	assert(tr_stream_close(server_stream) == TR_OK);
	wait_streams_empty(good_client, good_server);

	/*
	 * Reuse the victim connection slot. An old-generation abort must not
	 * touch the replacement, even while its Channel still holds the stale
	 * previous handle pending teardown.
	 */
	atomic_store_explicit(&ctx.inject, 0, memory_order_release);
	/*
	 * The peer half may also have freed a lower-numbered slot. Keep
	 * replacement owners alive until the exact victim slot is reused;
	 * do not assume a fixed order for the two EOF callbacks.
	 */
	while (replacement_count < 2U && !found_replacement) {
		create_pair(&replacement_fd,
			    &replacement_peer[replacement_count]);
		assert(tr_reactor_adopt_fd(reactor, replacement_fd,
					   &replacements[replacement_count]) ==
		       TR_OK);
		replacement_conn = replacements[replacement_count++];
		found_replacement =
			replacement_conn.slot == bad_server_conn.slot;
	}
	assert(found_replacement);
	assert(replacement_conn.generation != bad_server_conn.generation);
	assert(tr_reactor_call(reactor, abort_stale_on_owner,
			      &bad_server_conn) == TR_OK);
	assert(tr_reactor_send(replacement_conn, TR_FRAME_PING, 0U, 0U, 7U,
			       NULL) == TR_OK);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(bad_client) == TR_OK);
	assert(tr_channel_destroy(bad_server) == TR_OK);
	assert(tr_channel_destroy(good_client) == TR_OK);
	assert(tr_channel_destroy(good_server) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(tr_buffer_pool_free_count(&pool) == PAYLOAD_COUNT);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	while (replacement_count != 0U)
		assert(close(replacement_peer[--replacement_count]) == 0);
	active = NULL;
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
}

int main(void)
{
	test_protocol_error(BAD_WINDOW_UPDATE);
	test_protocol_error(BAD_DATA);
	test_protocol_error(BAD_HELLO);
	test_protocol_error(BAD_HELLO_ACK);
	return 0;
}
