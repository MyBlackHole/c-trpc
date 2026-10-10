#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/transport/channel/channel.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/*
 * A non-writable pipe read end makes a deterministic CONNECTING socket:
 * the Connector registers EPOLLOUT but cannot complete before cancellation.
 * Only the reconnect attempt is redirected; initial Channel connections
 * use ordinary socketpairs and the real Reactor.
 */
static atomic_int fake_connect_enabled;
static atomic_int fake_connector_fd;
static atomic_int fake_connector_peer;
static atomic_uint aux_register_count;
static atomic_uint aux_unregister_count;
static atomic_int fail_aux_unregister_once;

int __real_tr_tcp_connect_ipv4(const char *address, uint16_t port,
			       int *out_fd);
int __real_tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
					 uint32_t events,
					 tr_reactor_aux_event_cb callback,
					 void *arg);
int __real_tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd);

int __wrap_tr_tcp_connect_ipv4(const char *address, uint16_t port,
			       int *out_fd)
{
	int fds[2];

	if (!atomic_load_explicit(&fake_connect_enabled, memory_order_relaxed))
		return __real_tr_tcp_connect_ipv4(address, port, out_fd);
	(void)address;
	(void)port;
	assert(pipe2(fds, O_NONBLOCK | O_CLOEXEC) == 0);
	*out_fd = fds[0];
	/* Keep the writer open: otherwise EPOLLHUP would complete the fake
	 * connection before the cancellation barrier can be exercised.
	 */
	atomic_store_explicit(&fake_connector_peer, fds[1],
				      memory_order_release);
	atomic_store_explicit(&fake_connector_fd, fds[0],
			      memory_order_release);
	return TR_IN_PROGRESS;
}

int __wrap_tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
					 uint32_t events,
					 tr_reactor_aux_event_cb callback,
					 void *arg)
{
	int ret = __real_tr_reactor_aux_event_register(
		reactor, fd, events, callback, arg);

	if (ret == TR_OK &&
	    fd == atomic_load_explicit(&fake_connector_fd,
				       memory_order_acquire))
		atomic_fetch_add_explicit(&aux_register_count, 1U,
					  memory_order_release);
	return ret;
}

int __wrap_tr_reactor_aux_event_unregister(struct tr_reactor *reactor, int fd)
{
	int expected = 1;

	if (fd == atomic_load_explicit(&fake_connector_fd,
				       memory_order_acquire)) {
		atomic_fetch_add_explicit(&aux_unregister_count, 1U,
					  memory_order_release);
		if (atomic_compare_exchange_strong_explicit(
			    &fail_aux_unregister_once, &expected, 0,
			    memory_order_acq_rel, memory_order_relaxed))
			return TR_ERR_SYS;
	}
	return __real_tr_reactor_aux_event_unregister(reactor, fd);
}

struct test_context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned down;
	unsigned unexpected;
};

static void on_channel(struct tr_channel *channel,
		       enum tr_channel_event event, int status, void *arg)
{
	struct test_context *ctx = arg;

	(void)channel;
	(void)status;
	pthread_mutex_lock(&ctx->lock);
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN)
		ctx->down++;
	else if (event == TR_CHANNEL_EVENT_CONTROL_RECONNECT_FAILED)
		ctx->unexpected++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_down(struct test_context *ctx)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx->lock);
	while (!ctx->down && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline);
	assert(ctx->down == 1U);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_for_connector_watch(void)
{
	struct timespec pause = { 0, 1000000L };
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		if (atomic_load_explicit(&aux_register_count,
					 memory_order_acquire) == 1U)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"Connector did not enter its in-flight watched state");
}

static void make_pair(int *client_fd, int *server_fd)
{
	int fds[2];

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			  0, fds) == 0);
	*client_fd = fds[0];
	*server_fd = fds[1];
}

int main(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client = NULL;
	struct tr_channel *server = NULL;
	struct tr_conn_handle client_conn, server_conn;
	struct test_context ctx;
	int client_fd, server_fd;
	int watched_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	atomic_store(&fake_connector_fd, -1);
	atomic_store(&fake_connector_peer, -1);
	make_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 16U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 16U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, on_channel, &ctx, &client) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL, &server) == TR_OK);
	assert(tr_channel_wait_ready(client, TR_LANE_CONTROL, 5000U) == TR_OK);
	assert(tr_channel_wait_ready(server, TR_LANE_CONTROL, 5000U) == TR_OK);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = 9U;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 40U;
	reconnect_config.connect_timeout_ms = 5000U;
	atomic_store(&fake_connect_enabled, 1);
	assert(tr_channel_enable_client_reconnect(client, &reconnect_config) ==
	       TR_OK);

	assert(tr_reactor_close(client_conn) == TR_OK);
	wait_down(&ctx);
	wait_for_connector_watch();
	watched_fd = atomic_load_explicit(&fake_connector_fd,
					 memory_order_acquire);
	assert(watched_fd >= 0);
	assert(fcntl(watched_fd, F_GETFD) >= 0);

	/*
	 * The first Channel destroy crosses the waiter and keepalive barriers,
	 * then fails to detach the Connector's epoll source. The Channel and
	 * callback_arg must remain live so a second destroy can retry safely.
	 */
	atomic_store(&fail_aux_unregister_once, 1);
	assert(tr_channel_destroy(client) == TR_ERR_SYS);
	assert(atomic_load(&fail_aux_unregister_once) == 0);
	assert(atomic_load(&aux_unregister_count) == 1U);
	assert(fcntl(watched_fd, F_GETFD) >= 0);

	/* Retry removes the source, disarms the timeout, closes its fd, and
	 * finally releases Channel storage. No late callback may follow.
	 */
	assert(tr_channel_destroy(client) == TR_OK);
	client = NULL;
	assert(atomic_load(&aux_unregister_count) == 2U);
	errno = 0;
	assert(fcntl(watched_fd, F_GETFD) == -1 && errno == EBADF);
	assert(tr_reactor_quiesce(reactor) == TR_OK);
	assert(ctx.unexpected == 0U);
	assert(close(atomic_load(&fake_connector_peer)) == 0);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(server) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
	return 0;
}
