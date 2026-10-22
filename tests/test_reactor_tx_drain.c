#define _GNU_SOURCE
#include "../src/crc32c.h"
#include "../src/execution/reactor_internal.h"
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

#define DATA_BYTES 64U

struct fixture {
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	struct tr_buffer_pool pool;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int send_fd;
	int peer_fd;
	unsigned entered;
	unsigned released;
	unsigned blocked;
	unsigned drained;
	unsigned closed;
	int with_reserved;
	atomic_int hold_tx;
	atomic_int rearm_once;
	atomic_size_t written;
	atomic_size_t required_bytes;
};

/* 初始化早于 owner 启动，重置晚于 owner 回收；只截取本测试连接。 */
static struct fixture *active;
ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	struct fixture *f = active;
	struct msghdr limited;
	struct iovec first;
	ssize_t result;
	size_t i;

	if (!f || fd != f->send_fd)
		return __real_sendmsg(fd, message, flags);
	if (atomic_load(&f->hold_tx)) {
		pthread_mutex_lock(&f->lock);
		f->blocked = 1U;
		pthread_cond_broadcast(&f->cond);
		pthread_mutex_unlock(&f->lock);
		errno = EAGAIN;
		return -1;
	}
	/* 真实发送最多 13 字节，覆盖头部和载荷的部分写入。 */
	for (i = 0; i < message->msg_iovlen; ++i)
		if (message->msg_iov[i].iov_len != 0U)
			break;
	assert(i < message->msg_iovlen);
	first = message->msg_iov[i];
	if (first.iov_len > 13U)
		first.iov_len = 13U;
	limited = *message;
	limited.msg_iov = &first;
	limited.msg_iovlen = 1U;
	result = __real_sendmsg(fd, &limited, flags);
	if (result > 0)
		atomic_fetch_add(&f->written, (size_t)result);
	return result;
}

static void wait_count(struct fixture *f, unsigned *value, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&f->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&f->cond, &f->lock, &deadline);
	assert(*value >= target);
	pthread_mutex_unlock(&f->lock);
}

static void on_event(struct tr_conn_handle connection,
                     enum tr_connection_event event, int status, void *arg)
{
	struct fixture *f = arg;
	int rearm = 0;

	assert(status == TR_OK);
	assert(tr_reactor_in_owner_context());
	assert(tr_buffer_pool_free_count(&f->pool) == 1U);
	pthread_mutex_lock(&f->lock);
	if (event == TR_CONN_EVENT_TX_DRAINED) {
		assert(atomic_load(&f->written) >=
		       atomic_load(&f->required_bytes));
		f->drained++;
		rearm = atomic_exchange(&f->rearm_once, 0);
	} else {
		assert(event == TR_CONN_EVENT_CLOSED);
		f->closed++;
	}
	pthread_cond_broadcast(&f->cond);
	pthread_mutex_unlock(&f->lock);
	/* 空连接重新登记必须自行唤起检查，不能等不存在的后续流量。 */
	if (rearm)
		assert(tr_reactor_notify_tx_drained_on_owner(connection) ==
		       TR_OK);
}

static int gate_owner(void *arg)
{
	struct fixture *f = arg;
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&f->lock);
	f->entered = 1U;
	pthread_cond_broadcast(&f->cond);
	while (!f->released && ret == 0)
		ret = pthread_cond_timedwait(&f->cond, &f->lock, &deadline);
	assert(f->released);
	pthread_mutex_unlock(&f->lock);
	/* 生产者的 DATA 已被接受，但仍在当前 CALL 之后的命令队列里。 */
	assert(tr_reactor_notify_tx_drained_on_owner(f->connection) == TR_OK);
	assert(tr_reactor_notify_tx_drained_on_owner(f->connection) == TR_OK);
	if (f->with_reserved) {
		assert(tr_reactor_send_goaway_on_owner(f->connection, 37U) ==
		       TR_OK);
		assert(tr_reactor_send_goaway_on_owner(f->connection, 37U) ==
		       TR_OK);
		assert(tr_reactor_send_goaway_on_owner(f->connection, 39U) ==
		       TR_ERR_STATE);
		assert(tr_reactor_send_stream_reset_on_owner(
		               f->connection, 3U, TR_ERR_CLOSED) == TR_OK);
	}
	return TR_OK;
}

static void *run_gate(void *arg)
{
	struct fixture *f = arg;

	assert(tr_reactor_call(f->reactor, gate_owner, f) == TR_OK);
	return NULL;
}

static int observe_on_owner(void *arg)
{
	struct tr_conn_handle *connection = arg;

	return tr_reactor_notify_tx_drained_on_owner(*connection);
}

static void init_fixture(struct fixture *f, int with_reserved)
{
	struct tr_reactor_config config = {0};
	int pair[2];

	memset(f, 0, sizeof(*f));
	f->with_reserved = with_reserved;
	atomic_init(&f->hold_tx, 1);
	atomic_init(&f->rearm_once, 0);
	atomic_init(&f->written, 0U);
	atomic_init(&f->required_bytes,
	            DATA_BYTES +
	                    TR_WIRE_HEADER_SIZE * (with_reserved ? 3U : 1U));
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	assert(tr_buffer_pool_init(&f->pool, 1U, DATA_BYTES) == TR_OK);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, pair) == 0);
	f->send_fd = pair[0];
	f->peer_fd = pair[1];
	config.max_connections = 1U;
	config.command_capacity = 8U;
	config.tx_item_capacity = 1U;
	config.control_tx_item_capacity = 1U;
	config.rx_buffer_count = 2U;
	config.rx_buffer_size = 256U;
	config.max_payload_len = 128U;
	active = f;
	assert(tr_reactor_create(&config, NULL, on_event, f, &f->reactor) ==
	       TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(f->reactor, f->send_fd, &f->connection) ==
	       TR_OK);
	assert(tr_reactor_notify_tx_drained_on_owner(f->connection) ==
	       TR_ERR_STATE);
}

static void start_blocked_tx(struct fixture *f)
{
	struct tr_buffer *payload = NULL;
	pthread_t producer;

	assert(pthread_create(&producer, NULL, run_gate, f) == 0);
	wait_count(f, &f->entered, 1U);
	assert(tr_buffer_acquire(&f->pool, DATA_BYTES, &payload) == TR_OK);
	memset(payload->data, 0x5a, DATA_BYTES);
	payload->len = DATA_BYTES;
	assert(tr_reactor_send(f->connection, TR_FRAME_DATA,
	                       TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1U, 9U,
	                       payload) == TR_OK);
	payload = NULL;
	pthread_mutex_lock(&f->lock);
	f->released = 1U;
	pthread_cond_broadcast(&f->cond);
	pthread_mutex_unlock(&f->lock);
	assert(pthread_join(producer, NULL) == 0);
	wait_count(f, &f->blocked, 1U);
	pthread_mutex_lock(&f->lock);
	assert(f->drained == 0U && f->closed == 0U);
	pthread_mutex_unlock(&f->lock);
	assert(tr_buffer_pool_free_count(&f->pool) == 0U);
}

static void receive_frames(struct fixture *f)
{
	uint8_t bytes[DATA_BYTES + 3U * TR_WIRE_HEADER_SIZE];
	struct tr_wire_limits limits = {128U};
	size_t required = atomic_load(&f->required_bytes);
	size_t received = 0U;
	size_t offset = 0U;
	unsigned index = 0U;

	while (received < required) {
		struct pollfd descriptor = {f->peer_fd, POLLIN, 0};
		ssize_t count;

		assert(poll(&descriptor, 1U, 5000) == 1);
		count = recv(f->peer_fd, bytes + received, required - received,
		             0);
		if (count < 0 && (errno == EAGAIN || errno == EINTR))
			continue;
		assert(count > 0);
		received += (size_t)count;
	}
	while (offset < received) {
		struct tr_frame_header header;

		assert(tr_wire_header_decode(bytes + offset, &header) == TR_OK);
		assert(tr_wire_header_validate(bytes + offset, &header,
		                               &limits) == TR_OK);
		if (index == 0U) {
			assert(header.type == TR_FRAME_DATA &&
			       header.stream_id == 1U);
			assert(header.message_id == 9U &&
			       header.payload_len == DATA_BYTES);
			assert(header.payload_crc32c ==
			       tr_crc32c(bytes + offset + TR_WIRE_HEADER_SIZE,
			                 DATA_BYTES));
			for (size_t i = 0; i < DATA_BYTES; ++i)
				assert(bytes[offset + TR_WIRE_HEADER_SIZE +
				             i] == 0x5a);
		} else if (index == 1U) {
			assert(header.type == TR_FRAME_GOAWAY &&
			       header.message_id == 37U);
			assert(header.flags == 0U && header.stream_id == 0U &&
			       header.payload_len == 0U);
		} else {
			assert(index == 2U &&
			       header.type == TR_FRAME_STREAM_RESET);
			assert(header.stream_id == 3U &&
			       header.payload_len == 0U);
			assert(header.message_id == (uint64_t)(-TR_ERR_CLOSED));
		}
		offset += TR_WIRE_HEADER_SIZE + header.payload_len;
		index++;
	}
	assert(index == (f->with_reserved ? 3U : 1U));
	assert(offset == required);
}

static void destroy_fixture(struct fixture *f)
{
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	active = NULL;
	assert(close(f->peer_fd) == 0);
	assert(tr_buffer_pool_destroy(&f->pool) == TR_OK);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static void test_complete(int with_reserved)
{
	struct fixture f;

	init_fixture(&f, with_reserved);
	atomic_store(&f.rearm_once, 1);
	start_blocked_tx(&f);
	atomic_store(&f.hold_tx, 0);
	receive_frames(&f);
	wait_count(&f, &f.drained, 2U);
	assert(tr_reactor_close(f.connection) == TR_OK);
	wait_count(&f, &f.closed, 1U);
	assert(f.drained == 2U);
	destroy_fixture(&f);
}

static void test_close_and_reuse(void)
{
	struct fixture f;
	struct tr_conn_handle stale;
	int pair[2];

	init_fixture(&f, 1);
	start_blocked_tx(&f);
	stale = f.connection;
	assert(tr_reactor_close(f.connection) == TR_OK);
	wait_count(&f, &f.closed, 1U);
	assert(f.drained == 0U);
	assert(tr_reactor_call(f.reactor, observe_on_owner, &stale) ==
	       TR_ERR_STALE);
	assert(close(f.peer_fd) == 0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
	                  0, pair) == 0);
	f.peer_fd = pair[1];
	assert(tr_reactor_adopt_fd(f.reactor, pair[0], &f.connection) == TR_OK);
	assert(f.connection.slot == stale.slot &&
	       f.connection.generation != stale.generation);
	atomic_store(&f.required_bytes, 0U);
	assert(tr_reactor_call(f.reactor, observe_on_owner, &f.connection) ==
	       TR_OK);
	wait_count(&f, &f.drained, 1U);
	assert(tr_reactor_call(f.reactor, observe_on_owner, &stale) ==
	       TR_ERR_STALE);
	assert(tr_reactor_close(f.connection) == TR_OK);
	wait_count(&f, &f.closed, 2U);
	assert(f.drained == 1U);
	destroy_fixture(&f);
}

int main(void)
{
	test_complete(0);
	test_complete(1);
	test_close_and_reuse();
	puts("反应器发送排空：命令前沿、独立预留、短写、重入通知与代次回收通"
	     "过");
	return 0;
}
