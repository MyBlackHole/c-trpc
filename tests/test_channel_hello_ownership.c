#define _GNU_SOURCE
#include "../src/transport/channel/channel_internal.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 仅本测试链接时截获发送与释放。协议缓冲区以 tr_buffer 为首成员，
 * 因此被发送的 payload 指针就是最终交给 free() 的分配地址。
 */
#define TRACKED_PROTOCOL_BUFFERS 8U

static _Atomic(uintptr_t) tracked[TRACKED_PROTOCOL_BUFFERS];
static atomic_uint tracked_count;
static atomic_uint released_count;
static atomic_uint hello_count;
static atomic_uint ack_count;
static atomic_uint failed_ack_count;
static atomic_int fail_hello_status;
static atomic_int fail_first_ack;

/* 只在新用例中暂停首次 HELLO；不暂停 Reactor，也不修改生产锁。 */
static atomic_int pause_first_hello;
static pthread_mutex_t start_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t start_cond;
static int start_entered;
static int start_released;

static void start_wait_locked(const int *condition)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 5;
	while (!*condition)
		assert(pthread_cond_timedwait(&start_cond, &start_lock,
		                              &deadline) == 0);
}

static void pause_hello_admission(void)
{
	assert(pthread_mutex_lock(&start_lock) == 0);
	start_entered = 1;
	assert(pthread_cond_broadcast(&start_cond) == 0);
	start_wait_locked(&start_released);
	assert(pthread_mutex_unlock(&start_lock) == 0);
}

static pthread_mutex_t ack_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ack_cond = PTHREAD_COND_INITIALIZER;

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload);
void __real_free(void *ptr);

static int tracked_release(unsigned slot, uintptr_t address)
{
	return atomic_compare_exchange_strong(&tracked[slot], &address, 0U);
}

void __wrap_free(void *ptr)
{
	uintptr_t addr = (uintptr_t)ptr;
	unsigned i;

	if (addr != 0U) {
		for (i = 0U; i < TRACKED_PROTOCOL_BUFFERS; ++i) {
			if (tracked_release(i, addr)) {
				atomic_fetch_add(&released_count, 1U);
				break;
			}
		}
	}
	__real_free(ptr);
}

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload)
{
	int injected = TR_OK;

	if (type == TR_FRAME_HELLO || type == TR_FRAME_HELLO_ACK) {
		unsigned slot = atomic_fetch_add(&tracked_count, 1U);
		uintptr_t prior;

		assert(slot < TRACKED_PROTOCOL_BUFFERS);
		assert(payload != NULL);
		assert(payload->pool == NULL && payload->release_cb != NULL);
		prior = atomic_exchange(&tracked[slot], (uintptr_t)payload);
		assert(prior == 0U);
		if (type == TR_FRAME_HELLO) {
			atomic_fetch_add(&hello_count, 1U);
			injected = atomic_exchange(&fail_hello_status, TR_OK);
			if (atomic_exchange(&pause_first_hello, 0) != 0)
				pause_hello_admission();
		} else {
			atomic_fetch_add(&ack_count, 1U);
			if (atomic_exchange(&fail_first_ack, 0) != 0) {
				injected = TR_ERR_SYS;
				atomic_fetch_add(&failed_ack_count, 1U);
				assert(pthread_mutex_lock(&ack_lock) == 0);
				assert(pthread_cond_broadcast(&ack_cond) == 0);
				assert(pthread_mutex_unlock(&ack_lock) == 0);
			}
		}
	}

	if (injected != TR_OK)
		return injected;
	return __real_tr_reactor_send(connection, type, flags, stream_id,
	                              message_id, payload);
}

static void reset_tracking(void)
{
	unsigned i;

	for (i = 0U; i < TRACKED_PROTOCOL_BUFFERS; ++i)
		assert(atomic_load(&tracked[i]) == 0U);
	atomic_store(&tracked_count, 0U);
	atomic_store(&released_count, 0U);
	atomic_store(&hello_count, 0U);
	atomic_store(&ack_count, 0U);
	atomic_store(&failed_ack_count, 0U);
	atomic_store(&fail_hello_status, TR_OK);
	atomic_store(&fail_first_ack, 0);
}

static void check_released(unsigned hellos, int expect_acks)
{
	unsigned count = atomic_load(&tracked_count);
	unsigned i;

	assert(atomic_load(&hello_count) == hellos);
	if (expect_acks >= 0)
		assert(atomic_load(&ack_count) == (unsigned)expect_acks);
	assert(count == atomic_load(&hello_count) + atomic_load(&ack_count));
	assert(atomic_load(&released_count) == count);
	for (i = 0U; i < TRACKED_PROTOCOL_BUFFERS; ++i)
		assert(atomic_load(&tracked[i]) == 0U);
}

static struct tr_channel_config channel_config(enum tr_channel_role role)
{
	struct tr_channel_config config = {0};

	config.role = role;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	config.window_update_threshold_bytes = 1024U;
	return config;
}

static void test_hello_failed_admission(int status)
{
	struct tr_reactor *reactor = NULL;
	struct tr_channel *channel = NULL;
	struct tr_conn_handle connection;
	struct tr_channel_config config = channel_config(TR_CHANNEL_CLIENT);
	int sockets[2];
	int flags = SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC;
	int ret;

	reset_tracking();
	assert(socketpair(AF_UNIX, flags, 0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sockets[0], &connection) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);
	atomic_store(&fail_hello_status, status);

	ret = tr_channel_create(&config, connection, connection, NULL, NULL,
	                        NULL, NULL, &channel);
	assert(ret == status);
	assert(channel == NULL);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(sockets[1]) == 0);
	check_released(1U, 0);
}

struct pair {
	struct tr_reactor *reactor;
	struct tr_channel *client;
	struct tr_channel *server;
};

static void create_pair(struct pair *pair, int inject_ack_failure)
{
	struct tr_channel_config config;
	struct tr_conn_handle client_connection, server_connection;
	int sockets[2];
	int flags = SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC;
	int ret;

	memset(pair, 0, sizeof(*pair));
	assert(socketpair(AF_UNIX, flags, 0, sockets) == 0);
	ret = tr_reactor_create(NULL, NULL, NULL, NULL, &pair->reactor);
	assert(ret == TR_OK);
	assert(tr_reactor_start(pair->reactor) == TR_OK);
	ret = tr_reactor_adopt_fd(pair->reactor, sockets[0],
	                          &client_connection);
	assert(ret == TR_OK);
	ret = tr_reactor_adopt_fd(pair->reactor, sockets[1],
	                          &server_connection);
	assert(ret == TR_OK);

	/* 服务端先发布回调但延后握手，排除 HELLO 到达时尚未安装处理函数。 */
	config = channel_config(TR_CHANNEL_SERVER);
	ret = tr_channel_create_deferred(&config, server_connection,
	                                 server_connection, NULL, NULL, NULL,
	                                 NULL, &pair->server);
	assert(ret == TR_OK);
	atomic_store(&fail_first_ack, inject_ack_failure);
	config = channel_config(TR_CHANNEL_CLIENT);
	ret = tr_channel_create(&config, client_connection, client_connection,
	                        NULL, NULL, NULL, NULL, &pair->client);
	assert(ret == TR_OK);
	assert(tr_channel_start(pair->server) == TR_OK);
}

static void destroy_pair(struct pair *pair)
{
	assert(tr_channel_destroy(pair->client) == TR_OK);
	assert(tr_channel_destroy(pair->server) == TR_OK);
	assert(tr_reactor_stop(pair->reactor) == TR_OK);
	assert(tr_reactor_destroy(pair->reactor) == TR_OK);
}

static void wait_injected_ack(void)
{
	struct timespec deadline;
	int ret;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&ack_lock) == 0);
	while (atomic_load(&failed_ack_count) == 0U) {
		ret = pthread_cond_timedwait(&ack_cond, &ack_lock, &deadline);
		assert(ret == 0);
	}
	assert(pthread_mutex_unlock(&ack_lock) == 0);
}

static void test_successful_handshake(void)
{
	struct pair pair;
	int ret;

	reset_tracking();
	create_pair(&pair, 0);

	ret = tr_channel_wait_ready(pair.client, TR_LANE_CONTROL, 5000U);
	assert(ret == TR_OK);
	ret = tr_channel_wait_ready(pair.server, TR_LANE_CONTROL, 5000U);
	assert(ret == TR_OK);
	destroy_pair(&pair);
	assert(atomic_load(&failed_ack_count) == 0U);
	check_released(2U, 2);
}

static void test_hello_ack_failed_admission(void)
{
	struct pair pair;

	reset_tracking();
	create_pair(&pair, 1);
	wait_injected_ack();
	/* 在尝试最终释放对象前，跨过错误处理的 Reactor 所有者屏障。 */
	assert(tr_reactor_quiesce(pair.reactor) == TR_OK);
	destroy_pair(&pair);
	assert(atomic_load(&failed_ack_count) == 1U);
	assert(atomic_load(&ack_count) >= 1U);
	/* 错误可在服务端发出自己的 HELLO 之前发生。 */
	assert(atomic_load(&hello_count) >= 1U);
	assert(atomic_load(&hello_count) <= 2U);
	check_released(atomic_load(&hello_count), -1);
}

struct start_probe {
	struct tr_channel *channel;
	int result;
};

static void *channel_start_main(void *arg)
{
	struct start_probe *probe = arg;

	probe->result = tr_channel_start(probe->channel);
	return NULL;
}

/* 成功、暂时准入失败和永久失败共用同一受控并发窗口。
 * start() 的成功只代表 HELLO 已接管，不代表已收到对端 ACK。
 */
static void test_concurrent_start_result(int final_status)
{
	struct tr_reactor *reactor = NULL;
	struct tr_channel *channel = NULL;
	struct tr_conn_handle connection;
	struct tr_channel_config config = channel_config(TR_CHANNEL_CLIENT);
	struct start_probe probe = {0};
	struct tr_reactor_stats stats;
	pthread_condattr_t attr;
	pthread_t starter;
	int sockets[2];

	reset_tracking();
	assert(pthread_condattr_init(&attr) == 0);
	assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&start_cond, &attr) == 0);
	assert(pthread_condattr_destroy(&attr) == 0);
	start_entered = 0;
	start_released = 0;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, sockets) == 0);
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sockets[0], &connection) == TR_OK);
	assert(tr_channel_create_deferred(&config, connection, connection, NULL,
	                                  NULL, NULL, NULL, &channel) == TR_OK);
	assert(tr_reactor_quiesce(reactor) == TR_OK);
	probe.channel = channel;
	probe.result = TR_ERR_STATE;
	atomic_store(&fail_hello_status, final_status);
	atomic_store(&pause_first_hello, 1);
	assert(pthread_create(&starter, NULL, channel_start_main, &probe) == 0);

	assert(pthread_mutex_lock(&start_lock) == 0);
	start_wait_locked(&start_entered);
	assert(pthread_mutex_unlock(&start_lock) == 0);
	/* 另一个调用已发布启动状态，但尚未提交 HELLO。 */
	assert(tr_channel_start(channel) == TR_AGAIN);
	assert(atomic_load(&hello_count) == 1U);
	assert(pthread_mutex_lock(&start_lock) == 0);
	start_released = 1;
	assert(pthread_cond_broadcast(&start_cond) == 0);
	assert(pthread_mutex_unlock(&start_lock) == 0);
	assert(pthread_join(starter, NULL) == 0);
	assert(probe.result == final_status);
	assert(tr_channel_start(channel) == final_status);
	assert(atomic_load(&hello_count) == 1U);

	/* 等全部发送/所有者退出再销毁；每个缓冲只释放一次。 */
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_get_stats(reactor, &stats) == TR_OK);
	assert(stats.control_tx_item_pool.current == 0U);
	assert(tr_channel_destroy(channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(sockets[1]) == 0);
	check_released(1U, 0);
	assert(pthread_cond_destroy(&start_cond) == 0);
}

int main(void)
{
	test_concurrent_start_result(TR_OK);
	test_concurrent_start_result(TR_AGAIN);
	test_concurrent_start_result(TR_ERR_SYS);
	test_hello_failed_admission(TR_ERR_SYS);
	test_hello_failed_admission(TR_AGAIN);
	test_successful_handshake();
	test_hello_ack_failed_admission();
	puts("Channel HELLO/ACK 缓冲区所有权与故障回收：通过");
	return 0;
}
