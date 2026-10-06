#define _GNU_SOURCE
#include "../src/transport/channel/channel_internal.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

struct channel_detach_fixture {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_conn_handle connection;
	int peer_fd;
};

static atomic_int fail_handler_once;
static atomic_int fail_timer_once;
static atomic_uint handler_attempts;
static atomic_uint timer_attempts;

int __real_tr_reactor_set_handler(
	struct tr_conn_handle connection,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg);
int __real_tr_reactor_timer_unregister(
	struct tr_reactor_timer_handle handle);

int __wrap_tr_reactor_set_handler(
	struct tr_conn_handle connection,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg)
{
	atomic_fetch_add(&handler_attempts, 1U);
	if (atomic_exchange(&fail_handler_once, 0))
		return TR_ERR_SYS;
	return __real_tr_reactor_set_handler(
		connection, frame_cb, event_cb, callback_arg);
}

int __wrap_tr_reactor_timer_unregister(
	struct tr_reactor_timer_handle handle)
{
	atomic_fetch_add(&timer_attempts, 1U);
	if (atomic_exchange(&fail_timer_once, 0))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_unregister(handle);
}

static void reset_faults(void)
{
	atomic_store(&fail_handler_once, 0);
	atomic_store(&fail_timer_once, 0);
	atomic_store(&handler_attempts, 0U);
	atomic_store(&timer_attempts, 0U);
}

static void fixture_init(struct channel_detach_fixture *fixture)
{
	struct tr_channel_config config;
	int sockets[2];

	memset(fixture, 0, sizeof(*fixture));
	fixture->peer_fd = -1;

	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
		       0, sockets) == 0);
	assert(tr_reactor_create(
		       NULL, NULL, NULL, NULL, &fixture->reactor) == TR_OK);
	assert(tr_reactor_start(fixture->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(
		       fixture->reactor, sockets[0],
		       &fixture->connection) == TR_OK);
	fixture->peer_fd = sockets[1];

	memset(&config, 0, sizeof(config));
	config.role = TR_CHANNEL_SERVER;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;

	assert(tr_channel_create_deferred(
		       &config, fixture->connection, fixture->connection,
		       NULL, NULL, NULL, NULL, &fixture->channel) == TR_OK);
	assert(fixture->channel != NULL);

	/* 忽略构造阶段调用；故障注入只从 publication 完成后开始。 */
	reset_faults();
}

static void fixture_finish(struct channel_detach_fixture *fixture)
{
	tr_channel_finalize_detached(fixture->channel);
	fixture->channel = NULL;

	assert(tr_reactor_stop(fixture->reactor) == TR_OK);
	assert(tr_reactor_destroy(fixture->reactor) == TR_OK);
	fixture->reactor = NULL;

	assert(close(fixture->peer_fd) == 0);
	fixture->peer_fd = -1;
}

static void test_handler_detach_failure_is_retryable(void)
{
	struct channel_detach_fixture fixture;

	fixture_init(&fixture);

	atomic_store(&fail_handler_once, 1);
	assert(tr_channel_detach_for_finalize(fixture.channel) == TR_ERR_SYS);
	assert(atomic_load(&fail_handler_once) == 0);

	/*
	 * handler barrier 在 timer detach 前失败。Channel 仍由调用方拥有，
	 * timer publication 不能被提前消费或清零。
	 */
	assert(atomic_load(&handler_attempts) == 1U);
	assert(atomic_load(&timer_attempts) == 0U);

	assert(tr_channel_detach_for_finalize(fixture.channel) == TR_OK);
	assert(atomic_load(&handler_attempts) == 2U);
	assert(atomic_load(&timer_attempts) == 1U);

	fixture_finish(&fixture);
}

static void test_timer_detach_failure_keeps_publication(void)
{
	struct channel_detach_fixture fixture;

	fixture_init(&fixture);

	atomic_store(&fail_timer_once, 1);
	assert(tr_channel_detach_for_finalize(fixture.channel) == TR_ERR_SYS);
	assert(atomic_load(&fail_timer_once) == 0);
	assert(atomic_load(&handler_attempts) == 1U);
	assert(atomic_load(&timer_attempts) == 1U);

	/*
	 * 第一次 unregister 失败后必须保留 keepalive_timer_registered 和 exact
	 * timer handle，因此重试时必须再次真正进入 timer_unregister。
	 */
	assert(tr_channel_detach_for_finalize(fixture.channel) == TR_OK);
	assert(atomic_load(&handler_attempts) == 2U);
	assert(atomic_load(&timer_attempts) == 2U);

	fixture_finish(&fixture);
}

static void test_stale_connection_is_already_detached(void)
{
	struct channel_detach_fixture fixture;

	fixture_init(&fixture);

	assert(tr_reactor_close(fixture.connection) == TR_OK);
	assert(tr_reactor_quiesce(fixture.reactor) == TR_OK);

	/*
	 * exact connection generation 已经退休。set_handler() 可以返回
	 * TR_ERR_STALE，这能够证明未来不会再有该 connection callback 引用
	 * Channel；但 timer detach 仍然必须完成。
	 */
	reset_faults();
	assert(tr_channel_detach_for_finalize(fixture.channel) == TR_OK);
	assert(atomic_load(&handler_attempts) == 1U);
	assert(atomic_load(&timer_attempts) == 1U);

	fixture_finish(&fixture);
}

int main(void)
{
	test_handler_detach_failure_is_retryable();
	test_timer_detach_failure_keeps_publication();
	test_stale_connection_is_already_detached();
	return 0;
}
