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

struct channel_create_fixture {
	struct tr_reactor *reactor;
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
	int control_peer;
	int bulk_peer;
};

static atomic_uint handler_attempts;
static atomic_uint timer_register_attempts;
static atomic_uint timer_unregister_attempts;
static atomic_uint send_attempts;
static atomic_uint fail_handler_at;
static atomic_uint fail_timer_register_at;
static atomic_uint fail_send_at;

int __real_tr_reactor_set_handler(
	struct tr_conn_handle connection,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg);
int __real_tr_reactor_timer_register(
	struct tr_reactor *reactor,
	tr_reactor_timer_cb callback, void *arg,
	struct tr_reactor_timer_handle *out);
int __real_tr_reactor_timer_unregister(
	struct tr_reactor_timer_handle handle);
int __real_tr_reactor_send(
	struct tr_conn_handle connection, uint16_t type,
	uint32_t flags, uint32_t stream_id, uint64_t message_id,
	struct tr_buffer *payload);

int __wrap_tr_reactor_set_handler(
	struct tr_conn_handle connection,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg)
{
	unsigned attempt = atomic_fetch_add(&handler_attempts, 1U) + 1U;

	if (attempt == atomic_load(&fail_handler_at))
		return TR_ERR_SYS;
	return __real_tr_reactor_set_handler(
		connection, frame_cb, event_cb, callback_arg);
}

int __wrap_tr_reactor_timer_register(
	struct tr_reactor *reactor,
	tr_reactor_timer_cb callback, void *arg,
	struct tr_reactor_timer_handle *out)
{
	unsigned attempt =
		atomic_fetch_add(&timer_register_attempts, 1U) + 1U;

	if (attempt == atomic_load(&fail_timer_register_at))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_register(
		reactor, callback, arg, out);
}

int __wrap_tr_reactor_timer_unregister(
	struct tr_reactor_timer_handle handle)
{
	atomic_fetch_add(&timer_unregister_attempts, 1U);
	return __real_tr_reactor_timer_unregister(handle);
}

int __wrap_tr_reactor_send(
	struct tr_conn_handle connection, uint16_t type,
	uint32_t flags, uint32_t stream_id, uint64_t message_id,
	struct tr_buffer *payload)
{
	unsigned attempt = atomic_fetch_add(&send_attempts, 1U) + 1U;

	if (attempt == atomic_load(&fail_send_at))
		return TR_ERR_SYS;
	return __real_tr_reactor_send(
		connection, type, flags, stream_id, message_id, payload);
}

static void reset_faults(void)
{
	atomic_store(&handler_attempts, 0U);
	atomic_store(&timer_register_attempts, 0U);
	atomic_store(&timer_unregister_attempts, 0U);
	atomic_store(&send_attempts, 0U);
	atomic_store(&fail_handler_at, 0U);
	atomic_store(&fail_timer_register_at, 0U);
	atomic_store(&fail_send_at, 0U);
}

static void fixture_init(struct channel_create_fixture *fixture, int split)
{
	int control_pair[2];
	int bulk_pair[2] = { -1, -1 };

	memset(fixture, 0, sizeof(*fixture));
	fixture->control_peer = -1;
	fixture->bulk_peer = -1;

	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
		       0, control_pair) == 0);
	if (split)
		assert(socketpair(
			       AF_UNIX,
			       SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			       0, bulk_pair) == 0);

	assert(tr_reactor_create(
		       NULL, NULL, NULL, NULL, &fixture->reactor) == TR_OK);
	assert(tr_reactor_start(fixture->reactor) == TR_OK);

	assert(tr_reactor_adopt_fd(
		       fixture->reactor, control_pair[0],
		       &fixture->control) == TR_OK);
	fixture->control_peer = control_pair[1];

	if (split) {
		assert(tr_reactor_adopt_fd(
			       fixture->reactor, bulk_pair[0],
			       &fixture->bulk) == TR_OK);
		fixture->bulk_peer = bulk_pair[1];
	} else {
		fixture->bulk = fixture->control;
	}

	/* adopt command 必须先进入 owner state，随后构造预检才能观察 ACTIVE。 */
	assert(tr_reactor_quiesce(fixture->reactor) == TR_OK);
	reset_faults();
}

static void assert_connection_state(
	struct tr_conn_handle connection, enum tr_connection_state expected)
{
	enum tr_connection_state state = TR_CONN_ERROR;

	assert(tr_reactor_get_connection_state(connection, &state) == TR_OK);
	assert(state == expected);
}

static void fixture_finish(
	struct channel_create_fixture *fixture, int split,
	int control_live, int bulk_live)
{
	if (control_live)
		assert(tr_reactor_close(fixture->control) == TR_OK);
	if (split && bulk_live)
		assert(tr_reactor_close(fixture->bulk) == TR_OK);
	if (control_live || (split && bulk_live))
		assert(tr_reactor_quiesce(fixture->reactor) == TR_OK);

	assert(tr_reactor_stop(fixture->reactor) == TR_OK);
	assert(tr_reactor_destroy(fixture->reactor) == TR_OK);
	fixture->reactor = NULL;

	assert(close(fixture->control_peer) == 0);
	fixture->control_peer = -1;
	if (split) {
		assert(close(fixture->bulk_peer) == 0);
		fixture->bulk_peer = -1;
	}
}

static struct tr_channel_config channel_config(int split)
{
	struct tr_channel_config config;

	memset(&config, 0, sizeof(config));
	config.role = TR_CHANNEL_SERVER;
	config.mode = split ? TR_CHANNEL_SPLIT_CONNECTIONS :
				    TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;
	return config;
}

static void test_first_handler_failure_has_no_lifecycle_publication(void)
{
	struct channel_create_fixture fixture;
	struct tr_channel_config config = channel_config(0);
	struct tr_channel *channel = NULL;

	fixture_init(&fixture, 0);
	atomic_store(&fail_handler_at, 1U);

	assert(tr_channel_create_deferred(
		       &config, fixture.control, fixture.bulk,
		       NULL, NULL, NULL, NULL, &channel) == TR_ERR_SYS);
	assert(channel == NULL);
	assert(atomic_load(&handler_attempts) == 1U);
	assert(atomic_load(&timer_register_attempts) == 0U);
	assert(atomic_load(&timer_unregister_attempts) == 0U);
	assert_connection_state(fixture.control, TR_CONN_ACTIVE);

	fixture_finish(&fixture, 0, 1, 0);
}

static void test_second_handler_failure_rolls_back_in_owner_turn(void)
{
	struct channel_create_fixture fixture;
	struct tr_channel_config config = channel_config(1);
	struct tr_channel *channel = NULL;

	fixture_init(&fixture, 1);
	atomic_store(&fail_handler_at, 2U);

	assert(tr_channel_create_deferred(
		       &config, fixture.control, fixture.bulk,
		       NULL, NULL, NULL, NULL, &channel) == TR_ERR_SYS);
	assert(channel == NULL);

	/*
	 * 第一次 control publication 成功，第二次 bulk publication 被注入失败；
	 * 第三次 set_handler 是同一 owner turn 内撤销 control callback source。
	 */
	assert(atomic_load(&handler_attempts) == 3U);
	assert(atomic_load(&timer_register_attempts) == 0U);
	assert(atomic_load(&timer_unregister_attempts) == 0U);
	assert_connection_state(fixture.control, TR_CONN_FREE);
	assert_connection_state(fixture.bulk, TR_CONN_FREE);

	fixture_finish(&fixture, 1, 0, 0);
}

static void test_handshake_failure_rolls_back_before_timer_publish(void)
{
	struct channel_create_fixture fixture;
	struct tr_channel_config config = channel_config(0);
	struct tr_channel *channel = NULL;

	fixture_init(&fixture, 0);
	atomic_store(&fail_send_at, 1U);

	assert(tr_channel_create(
		       &config, fixture.control, fixture.bulk,
		       NULL, NULL, NULL, NULL, &channel) == TR_ERR_SYS);
	assert(channel == NULL);
	assert(atomic_load(&send_attempts) == 1U);
	assert(atomic_load(&handler_attempts) == 2U);
	assert(atomic_load(&timer_register_attempts) == 0U);
	assert(atomic_load(&timer_unregister_attempts) == 0U);
	assert_connection_state(fixture.control, TR_CONN_FREE);

	fixture_finish(&fixture, 0, 0, 0);
}

static void test_timer_publish_failure_needs_no_timer_rollback(void)
{
	struct channel_create_fixture fixture;
	struct tr_channel_config config = channel_config(0);
	struct tr_channel *channel = NULL;

	fixture_init(&fixture, 0);
	atomic_store(&fail_timer_register_at, 1U);

	assert(tr_channel_create_deferred(
		       &config, fixture.control, fixture.bulk,
		       NULL, NULL, NULL, NULL, &channel) == TR_ERR_SYS);
	assert(channel == NULL);

	/*
	 * timer 是 transaction 的最后一个 publication。register 自身失败时
	 * handle 从未发布，因此 error path 不允许调用 timer_unregister。
	 */
	assert(atomic_load(&handler_attempts) == 2U);
	assert(atomic_load(&timer_register_attempts) == 1U);
	assert(atomic_load(&timer_unregister_attempts) == 0U);
	assert_connection_state(fixture.control, TR_CONN_FREE);

	fixture_finish(&fixture, 0, 0, 0);
}

static void test_success_commits_timer_once(void)
{
	struct channel_create_fixture fixture;
	struct tr_channel_config config = channel_config(0);
	struct tr_channel *channel = NULL;

	fixture_init(&fixture, 0);

	assert(tr_channel_create_deferred(
		       &config, fixture.control, fixture.bulk,
		       NULL, NULL, NULL, NULL, &channel) == TR_OK);
	assert(channel != NULL);
	assert(atomic_load(&handler_attempts) == 1U);
	assert(atomic_load(&timer_register_attempts) == 1U);
	assert(atomic_load(&timer_unregister_attempts) == 0U);

	assert(tr_channel_destroy(channel) == TR_OK);
	channel = NULL;
	assert(atomic_load(&timer_unregister_attempts) == 1U);
	assert_connection_state(fixture.control, TR_CONN_ACTIVE);

	fixture_finish(&fixture, 0, 1, 0);
}

int main(void)
{
	test_first_handler_failure_has_no_lifecycle_publication();
	test_second_handler_failure_rolls_back_in_owner_turn();
	test_handshake_failure_rolls_back_before_timer_publish();
	test_timer_publish_failure_needs_no_timer_rollback();
	test_success_commits_timer_once();
	return 0;
}
