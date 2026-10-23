#define _GNU_SOURCE
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

struct fixture {
	struct tr_reactor *reactor;
	struct tr_conn_handle connections[70];
	int fd[70], peer[70];
	unsigned connection_count;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned ready, producer_entered, release_producer, sleeping;
	struct tr_conn_handle observed[160];
	atomic_int block_tx;
	atomic_int observe_sleep;
	unsigned rearm;
	int send_on_ready;
	int action;
};

static struct fixture *active;
static _Thread_local int hold_prepare;

static void wait_locked(struct fixture *f, const unsigned *value, unsigned n)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	while (*value < n)
		assert(pthread_cond_timedwait(&f->cond, &f->lock, &deadline) ==
		       0);
}

static void wait_count(struct fixture *f, const unsigned *value, unsigned n)
{
	pthread_mutex_lock(&f->lock);
	wait_locked(f, value, n);
	pthread_mutex_unlock(&f->lock);
}

ssize_t __real_sendmsg(int fd, const struct msghdr *msg, int flags);
ssize_t __wrap_sendmsg(int fd, const struct msghdr *msg, int flags)
{
	if (active && fd == active->fd[0] && atomic_load(&active->block_tx)) {
		errno = EAGAIN;
		return -1;
	}
	return __real_sendmsg(fd, msg, flags);
}

int __real_tr_wire_header_encode(uint8_t out[TR_WIRE_HEADER_SIZE],
                                 const struct tr_frame_header *header);
int __wrap_tr_wire_header_encode(uint8_t out[TR_WIRE_HEADER_SIZE],
                                 const struct tr_frame_header *header)
{
	if (hold_prepare) {
		struct fixture *f = active;
		/* 生产者已取得唯一控制项，尚未尝试命令准入，也未持有 ctl_lock。
		 * 随后使用陈旧连接失败，唯一唤醒来源就是跨线程归还池项。 */
		pthread_mutex_lock(&f->lock);
		f->producer_entered = 1;
		pthread_cond_broadcast(&f->cond);
		wait_locked(f, &f->release_producer, 1);
		pthread_mutex_unlock(&f->lock);
	}
	return __real_tr_wire_header_encode(out, header);
}

int __real_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout);
int __wrap_epoll_wait(int fd, struct epoll_event *events, int count,
                      int timeout)
{
	struct fixture *f = active;
	if (f && timeout != 0 && atomic_load(&f->observe_sleep)) {
		pthread_mutex_lock(&f->lock);
		f->sleeping = 1;
		pthread_cond_broadcast(&f->cond);
		pthread_mutex_unlock(&f->lock);
	}
	return __real_epoll_wait(fd, events, count, timeout);
}

static void on_event(struct tr_conn_handle connection,
                     enum tr_connection_event event, int status, void *arg)
{
	struct fixture *f = arg;
	assert(tr_reactor_in_owner_context_for(f->reactor));
	assert(status == TR_OK);
	if (event == TR_CONN_EVENT_CLOSED)
		return;
	assert(event == TR_CONN_EVENT_CONTROL_TX_READY);
	if (f->send_on_ready)
		assert(tr_reactor_send(connection, TR_FRAME_PING, 0, 0, 456,
		                       NULL) == TR_OK);
	if (f->rearm) {
		--f->rearm;
		assert(tr_reactor_watch_control_tx_on_owner(connection, 1) ==
		       TR_OK);
	}
	pthread_mutex_lock(&f->lock);
	assert(f->ready < 160);
	f->observed[f->ready++] = connection;
	pthread_cond_broadcast(&f->cond);
	pthread_mutex_unlock(&f->lock);
}

static void adopt(struct fixture *f, unsigned i)
{
	int sockets[2];
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, sockets) == 0);
	f->fd[i] = sockets[0];
	f->peer[i] = sockets[1];
	assert(tr_reactor_adopt_fd(f->reactor, sockets[0],
	                           &f->connections[i]) == TR_OK);
}

static void init_count(struct fixture *f, unsigned connections)
{
	const struct tr_reactor_config config = {
	        .max_connections = connections,
	        .command_capacity = connections > 4 ? connections : 4,
	        .control_tx_item_capacity = 1,
	        .tx_item_capacity = 1,
	        .rx_buffer_count = 3,
	        .rx_buffer_size = 64,
	        .max_payload_len = 64};
	memset(f, 0, sizeof(*f));
	f->connection_count = connections;
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	atomic_init(&f->block_tx, 1);
	atomic_init(&f->observe_sleep, 0);
	active = f;
	assert(tr_reactor_create(&config, NULL, on_event, f, &f->reactor) ==
	       TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	for (unsigned i = 0; i < connections; ++i)
		adopt(f, i);
	assert(tr_reactor_watch_control_tx_on_owner(f->connections[0], 1) ==
	       TR_ERR_STATE);
}

static void init(struct fixture *f)
{
	init_count(f, 3);
}

static void destroy(struct fixture *f)
{
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	active = NULL;
	for (unsigned i = 0; i < f->connection_count; ++i)
		close(f->peer[i]);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static int barrier(void *arg)
{
	(void)arg;
	return TR_OK;
}

static int fill_and_watch(void *arg)
{
	struct fixture *f = arg;
	assert(tr_reactor_send(f->connections[0], TR_FRAME_PING, 0, 0, 123,
	                       NULL) == TR_OK);
	assert(tr_reactor_send(f->connections[0], TR_FRAME_PING, 0, 0, 124,
	                       NULL) == TR_AGAIN);
	for (unsigned i = 1; i < 3; ++i) {
		assert(tr_reactor_watch_control_tx_on_owner(f->connections[i],
		                                            1) == TR_OK);
		assert(tr_reactor_watch_control_tx_on_owner(f->connections[i],
		                                            1) == TR_OK);
	}
	assert(f->ready == 0); /* 登记不得内联回调。 */
	if (f->action == 1)
		assert(tr_reactor_watch_control_tx_on_owner(f->connections[1],
		                                            0) == TR_OK);
	else if (f->action == 2)
		assert(tr_reactor_set_handler(f->connections[1], NULL, on_event,
		                              f) == TR_OK);
	else if (f->action == 3)
		assert(tr_reactor_close_on_owner(f->connections[1]) == TR_OK);
	return TR_OK;
}

static void test_shared_pool(int action)
{
	struct fixture f;
	init(&f);
	f.action = action;
	f.send_on_ready = 1;
	assert(tr_reactor_call(f.reactor, fill_and_watch, &f) == TR_OK);
	assert(tr_reactor_call(f.reactor, barrier, NULL) == TR_OK);
	pthread_mutex_lock(&f.lock);
	assert(f.ready == 0);
	pthread_mutex_unlock(&f.lock);
	if (action == 3) {
		struct tr_conn_handle old = f.connections[1];
		close(f.peer[1]);
		adopt(&f, 1);
		assert(old.slot == f.connections[1].slot);
		assert(old.generation != f.connections[1].generation);
	}
	atomic_store(&f.block_tx, 0);
	unsigned expected = action ? 1U : 2U;
	wait_count(&f, &f.ready, expected);
	assert(tr_reactor_call(f.reactor, barrier, NULL) == TR_OK);
	pthread_mutex_lock(&f.lock);
	assert(f.ready == expected);
	assert(f.observed[0].slot == f.connections[action ? 2 : 1].slot);
	if (!action)
		assert(f.observed[1].slot == f.connections[2].slot);
	pthread_mutex_unlock(&f.lock);
	destroy(&f);
}

static int watch_one(void *arg)
{
	struct fixture *f = arg;
	assert(tr_reactor_watch_control_tx_on_owner(f->connections[1], 1) ==
	       TR_OK);
	atomic_store(&f->observe_sleep, 1);
	return TR_OK;
}

static void *send_stale(void *arg)
{
	struct fixture *f = arg;
	struct tr_conn_handle stale = f->connections[0];
	++stale.generation;
	hold_prepare = 1;
	assert(tr_reactor_send(stale, TR_FRAME_PING, 0, 0, 789, NULL) ==
	       TR_ERR_STALE);
	return NULL;
}

static void test_producer_release_wakes_owner(void)
{
	struct fixture f;
	pthread_t producer;
	init(&f);
	f.send_on_ready = 1;
	assert(pthread_create(&producer, NULL, send_stale, &f) == 0);
	wait_count(&f, &f.producer_entered, 1);
	assert(tr_reactor_call(f.reactor, watch_one, &f) == TR_OK);
	wait_count(&f, &f.sleeping, 1);
	pthread_mutex_lock(&f.lock);
	assert(f.ready == 0);
	f.release_producer = 1;
	pthread_cond_broadcast(&f.cond);
	pthread_mutex_unlock(&f.lock);
	assert(pthread_join(producer, NULL) == 0);
	/* 此处无 timer、TX、其他命令或网络流量可唤醒沉睡的所有者。 */
	wait_count(&f, &f.ready, 1);
	destroy(&f);
}

static void test_rearm_without_io(void)
{
	struct fixture f;
	struct tr_reactor_stats stats;
	init(&f);
	f.rearm = 129;
	assert(tr_reactor_call(f.reactor, watch_one, &f) == TR_OK);
	wait_count(&f, &f.ready, 130);
	assert(tr_reactor_get_stats(f.reactor, &stats) == TR_OK);
	assert(stats.total.tx_dispatches >= 130);
	assert(stats.max_per_turn.tx_dispatches <= stats.limits.tx_dispatches);
	destroy(&f);
}

static int watch_many(void *arg)
{
	struct fixture *f = arg;
	for (unsigned i = 0; i < f->connection_count; ++i)
		assert(tr_reactor_watch_control_tx_on_owner(f->connections[i],
		                                            1) == TR_OK);
	assert(f->ready == 0);
	return TR_OK;
}

static void test_notification_budget(void)
{
	struct fixture f;
	struct tr_reactor_stats stats;
	init_count(&f, 70);
	assert(tr_reactor_call(f.reactor, watch_many, &f) == TR_OK);
	wait_count(&f, &f.ready, 70);
	assert(tr_reactor_get_stats(f.reactor, &stats) == TR_OK);
	assert(stats.budget_hits.tx_dispatches != 0);
	assert(stats.max_per_turn.tx_dispatches == stats.limits.tx_dispatches);
	pthread_mutex_lock(&f.lock);
	for (unsigned i = 0; i < 70; ++i)
		assert(f.observed[i].slot == f.connections[i].slot);
	pthread_mutex_unlock(&f.lock);
	destroy(&f);
}

static int wait_for_commands(void *arg)
{
	struct fixture *f = arg;
	pthread_mutex_lock(&f->lock);
	f->producer_entered = 1;
	pthread_cond_broadcast(&f->cond);
	wait_locked(f, &f->release_producer, 1);
	pthread_mutex_unlock(&f->lock);
	/* 控制池有空位，但命令环已满；登记不需要额外命令容量。 */
	assert(tr_reactor_send(f->connections[1], TR_FRAME_PING, 0, 0, 789,
	                       NULL) == TR_AGAIN);
	return watch_one(f);
}

static void *call_wait(void *arg)
{
	struct fixture *f = arg;
	assert(tr_reactor_call(f->reactor, wait_for_commands, f) == TR_OK);
	return NULL;
}

static void test_command_capacity(void)
{
	struct fixture f;
	pthread_t caller;
	init(&f);
	f.send_on_ready = 1;
	assert(pthread_create(&caller, NULL, call_wait, &f) == 0);
	wait_count(&f, &f.producer_entered, 1);
	for (unsigned i = 0; i < 4; ++i)
		assert(tr_reactor_resume_rx(f.connections[0]) == TR_OK);
	assert(tr_reactor_resume_rx(f.connections[0]) == TR_AGAIN);
	pthread_mutex_lock(&f.lock);
	f.release_producer = 1;
	pthread_cond_broadcast(&f.cond);
	pthread_mutex_unlock(&f.lock);
	assert(pthread_join(caller, NULL) == 0);
	wait_count(&f, &f.ready, 1);
	destroy(&f);
}

int main(void)
{
	for (int action = 0; action < 4; ++action)
		test_shared_pool(action);
	test_producer_release_wakes_owner();
	test_rearm_without_io();
	test_notification_budget();
	test_command_capacity();
	puts("reactor control ready tests passed (8 groups)");
	return 0;
}
