#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/command_queue.h"
#include "../src/execution/completion_queue.h"
#include "../src/execution/buffer.h"
#include "../src/transport/protocol/wire.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define COMMAND_CAPACITY 6U
#define BUFFER_COUNT 2U

enum sync_kind {
	SYNC_CALL,
	SYNC_QUIESCE,
	SYNC_SET_HANDLER
};

struct fatal_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	struct tr_command_queue *observed_command_queue;
	struct tr_completion_queue *observed_completion_queue;
	int trap_poll;
	int gate_entered;
	int release_gate;
	int sync_entered;
	int sync_admitted;
	int completion_entered;
	int completion_admitted;
	int connection_errors;
	int connection_status;
	int unexpected_calls;
	int completed;
	int stop_ret;
};

struct sync_request {
	struct fatal_ctx *ctx;
	enum sync_kind kind;
	int ret;
};

static struct fatal_ctx *active;

int __real_epoll_wait(int fd, struct epoll_event *events, int maxevents,
		      int timeout);
int __real_tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t generation, int *need_wake);
int __real_tr_completion_queue_push_wait(
	struct tr_completion_queue *queue, const struct tr_completion *completion,
	uint64_t generation, int *need_wake);

static void notify(struct fatal_ctx *ctx)
{
	pthread_cond_broadcast(&ctx->cond);
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int maxevents,
		      int timeout)
{
	struct fatal_ctx *ctx = active;
	int fail = 0;

	if (ctx) {
		pthread_mutex_lock(&ctx->lock);
		if (ctx->trap_poll && !ctx->gate_entered) {
			ctx->gate_entered = 1;
			notify(ctx);
			while (!ctx->release_gate)
				pthread_cond_wait(&ctx->cond, &ctx->lock);
			fail = 1;
		}
		pthread_mutex_unlock(&ctx->lock);
	}
	if (fail) {
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, maxevents, timeout);
}

int __wrap_tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t generation, int *need_wake)
{
	struct fatal_ctx *ctx = active;
	int ret;

	if (ctx) {
		pthread_mutex_lock(&ctx->lock);
		ctx->observed_command_queue = queue;
		ctx->sync_entered++;
		notify(ctx);
		pthread_mutex_unlock(&ctx->lock);
	}

	ret = __real_tr_command_queue_push_wait(
		queue, command, generation, need_wake);
	if (ctx && ret == TR_OK) {
		pthread_mutex_lock(&ctx->lock);
		ctx->sync_admitted++;
		notify(ctx);
		pthread_mutex_unlock(&ctx->lock);
	}
	return ret;
}

int __wrap_tr_completion_queue_push_wait(
	struct tr_completion_queue *queue, const struct tr_completion *completion,
	uint64_t generation, int *need_wake)
{
	struct fatal_ctx *ctx = active;
	int ret;

	if (ctx) {
		pthread_mutex_lock(&ctx->lock);
		ctx->observed_completion_queue = queue;
		ctx->completion_entered++;
		notify(ctx);
		pthread_mutex_unlock(&ctx->lock);
	}
	ret = __real_tr_completion_queue_push_wait(
		queue, completion, generation, need_wake);
	if (ctx && ret == TR_OK) {
		pthread_mutex_lock(&ctx->lock);
		ctx->completion_admitted++;
		notify(ctx);
		pthread_mutex_unlock(&ctx->lock);
	}
	return ret;
}

static void wait_value(struct fatal_ctx *ctx, const int *value, int target,
		       const char *name)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	if (*value < target)
		fprintf(stderr, "%s: got %d expected %d, err %d\n",
			name, *value, target, ret);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void pause_1ms(void)
{
	const struct timespec pause = { 0, 1000000L };
	(void)nanosleep(&pause, NULL);
}

static void wait_queue_waiters(struct fatal_ctx *ctx, int completion)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		unsigned waiters = 0;

		pthread_mutex_lock(&ctx->lock);
		if (completion) {
			struct tr_completion_queue *q =
				ctx->observed_completion_queue;

			if (q) {
				pthread_mutex_lock(&q->lock);
				waiters = q->waiters;
				pthread_mutex_unlock(&q->lock);
			}
		} else {
			struct tr_command_queue *q =
				ctx->observed_command_queue;

			if (q) {
				pthread_mutex_lock(&q->lock);
				waiters = q->waiters;
				pthread_mutex_unlock(&q->lock);
			}
		}
		pthread_mutex_unlock(&ctx->lock);
		if (waiters != 0U)
			return;
		pause_1ms();
	}
	assert(!"expected a genuine full-queue producer waiter");
}

static int never_execute(void *arg)
{
	struct fatal_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->unexpected_calls++;
	notify(ctx);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static int owner_alive(void *arg)
{
	(void)arg;
	return TR_OK;
}

static void on_completion(void *arg)
{
	struct fatal_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->completed++;
	notify(ctx);
	pthread_mutex_unlock(&ctx->lock);
}

static void on_connection(struct tr_conn_handle connection,
			  enum tr_connection_event event, int status, void *arg)
{
	struct fatal_ctx *ctx = arg;

	(void)connection;
	assert(event == TR_CONN_EVENT_ERROR);
	pthread_mutex_lock(&ctx->lock);
	ctx->connection_status = status;
	ctx->connection_errors++;
	notify(ctx);
	pthread_mutex_unlock(&ctx->lock);
}

static void *sync_thread(void *arg)
{
	struct sync_request *request = arg;
	struct fatal_ctx *ctx = request->ctx;

	switch (request->kind) {
	case SYNC_CALL:
		request->ret = tr_reactor_call(ctx->reactor, never_execute, ctx);
		break;
	case SYNC_QUIESCE:
		request->ret = tr_reactor_quiesce(ctx->reactor);
		break;
	case SYNC_SET_HANDLER:
		request->ret = tr_reactor_set_handler(
			ctx->connection, NULL, on_connection, ctx);
		break;
	}
	return NULL;
}

struct completion_request {
	struct fatal_ctx *ctx;
	int ret;
};

static void *completion_thread(void *arg)
{
	struct completion_request *request = arg;

	request->ret = tr_reactor_complete(
		request->ctx->reactor, on_completion, request->ctx);
	return NULL;
}

static void *stop_thread(void *arg)
{
	struct fatal_ctx *ctx = arg;

	ctx->stop_ret = tr_reactor_stop(ctx->reactor);
	return NULL;
}

static void socket_pair(int *a, int *b)
{
	int sockets[2];
	int i;

	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
	for (i = 0; i < 2; ++i) {
		int flags = fcntl(sockets[i], F_GETFL, 0);
		int fdflags = fcntl(sockets[i], F_GETFD, 0);

		assert(flags >= 0 && fdflags >= 0);
		assert(fcntl(sockets[i], F_SETFL, flags | O_NONBLOCK) == 0);
		assert(fcntl(sockets[i], F_SETFD, fdflags | FD_CLOEXEC) == 0);
	}
	*a = sockets[0];
	*b = sockets[1];
}

static void test_fatal_epoch(int concurrent_stop)
{
	struct tr_reactor_config config;
	struct fatal_ctx ctx;
	struct sync_request req[4];
	struct completion_request completion_request;
	struct tr_buffer_pool pool;
	struct tr_buffer *buffer;
	struct tr_conn_handle queued_adopt;
	pthread_t request_threads[4];
	pthread_t blocked_completion;
	pthread_t stopper;
	int conn_fd;
	int peer_fd;
	int queued_fd;
	int queued_peer;
	int stop_spawned = 0;
	int i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	active = &ctx;

	memset(&config, 0, sizeof(config));
	config.max_connections = 4U;
	config.command_capacity = COMMAND_CAPACITY;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 8U;
	config.rx_buffer_count = 8U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	assert(tr_reactor_create(&config, NULL, on_connection, &ctx,
				 &ctx.reactor) == TR_OK);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	socket_pair(&conn_fd, &peer_fd);
	assert(tr_reactor_adopt_fd(ctx.reactor, conn_fd,
				   &ctx.connection) == TR_OK);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);

	/* Force next owner poll to fail only after we have filled both queues. */
	pthread_mutex_lock(&ctx.lock);
	ctx.trap_poll = 1;
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_reactor_complete(ctx.reactor, on_completion, &ctx) == TR_OK);
	wait_value(&ctx, &ctx.gate_entered, 1, "owner poll gate");
	wait_value(&ctx, &ctx.completed, 1, "trigger completion");

	assert(tr_buffer_pool_init(&pool, BUFFER_COUNT, 512U) == TR_OK);
	assert(tr_buffer_acquire(&pool, 4U, &buffer) == TR_OK);
	memcpy(buffer->data, "data", 4U);
	buffer->len = 4U;
	assert(tr_reactor_send(ctx.connection, TR_FRAME_DATA,
			       TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
			       1U, 1U, buffer) == TR_OK);

	/* Accepted ADOPT owns the new fd before any epoll processing occurs. */
	socket_pair(&queued_fd, &queued_peer);
	assert(tr_reactor_adopt_fd(ctx.reactor, queued_fd, &queued_adopt) ==
	       TR_OK);

	for (i = 0; i < 3; ++i) {
		req[i].ctx = &ctx;
		req[i].kind = (enum sync_kind)i;
		req[i].ret = 0;
		assert(pthread_create(&request_threads[i], NULL,
				      sync_thread, &req[i]) == 0);
		wait_value(&ctx, &ctx.sync_admitted, i + 2,
			   "accepted synchronous owner commands");
	}

	/* SEND + ADOPT + CALL + QUIESCE + SET_HANDLER + RESUME = full ring. */
	assert(tr_reactor_resume_rx(ctx.connection) == TR_OK);
	req[3].ctx = &ctx;
	req[3].kind = SYNC_CALL;
	req[3].ret = 0;
	assert(pthread_create(&request_threads[3], NULL,
			      sync_thread, &req[3]) == 0);
	wait_value(&ctx, &ctx.sync_entered, 5, "blocked sync producer");
	wait_queue_waiters(&ctx, 0);

	/* The separate bounded completion queue must wake its blocked worker. */
	for (i = 0; i < (int)COMMAND_CAPACITY; ++i)
		assert(tr_reactor_complete(ctx.reactor, on_completion, &ctx) ==
		       TR_OK);
	completion_request.ctx = &ctx;
	completion_request.ret = 0;
	assert(pthread_create(&blocked_completion, NULL, completion_thread,
			      &completion_request) == 0);
	wait_queue_waiters(&ctx, 1);

	if (concurrent_stop) {
		assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
		stop_spawned = 1;
		/* The normal stop closes admission before trying to join owner. */
		for (i = 0; i < 10000; ++i) {
			if (tr_reactor_complete(ctx.reactor, on_completion, &ctx) ==
			    TR_ERR_CLOSED)
				break;
			/* Queue full itself can block; do not retry this operation. */
			assert(!"stop admission must close before probe");
		}
	}

	/* Owner enters EIO exit, rejects pending work, and drains completions. */
	pthread_mutex_lock(&ctx.lock);
	ctx.release_gate = 1;
	notify(&ctx);
	pthread_mutex_unlock(&ctx.lock);

	for (i = 0; i < 4; ++i)
		assert(pthread_join(request_threads[i], NULL) == 0);
	assert(pthread_join(blocked_completion, NULL) == 0);
	if (stop_spawned) {
		assert(pthread_join(stopper, NULL) == 0);
		assert(ctx.stop_ret == TR_OK);
	} else {
		assert(tr_reactor_stop(ctx.reactor) == TR_OK);
	}

	for (i = 0; i < 3; ++i)
		assert(req[i].ret == TR_ERR_SYS);
	assert(req[3].ret == TR_ERR_CLOSED);
	assert(completion_request.ret == TR_ERR_CLOSED);
	assert(ctx.unexpected_calls == 0);
	assert(ctx.completed == 1 + COMMAND_CAPACITY);
	assert(ctx.connection_errors == 1);
	assert(ctx.connection_status == TR_ERR_SYS);
	assert(tr_buffer_pool_free_count(&pool) == BUFFER_COUNT);
	errno = 0;
	assert(fcntl(queued_fd, F_GETFD) == -1 && errno == EBADF);
	assert(close(peer_fd) == 0);
	assert(close(queued_peer) == 0);

	/* A joined failed epoch cannot replay any old command on restart. */
	pthread_mutex_lock(&ctx.lock);
	ctx.trap_poll = 0;
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(tr_reactor_call(ctx.reactor, owner_alive, NULL) == TR_OK);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);
	assert(tr_reactor_stop(ctx.reactor) == TR_OK);
	assert(ctx.unexpected_calls == 0);
	assert(ctx.completed == 1 + COMMAND_CAPACITY);

	assert(tr_reactor_destroy(ctx.reactor) == TR_OK);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
	active = NULL;
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
}

int main(void)
{
	test_fatal_epoch(0);
	test_fatal_epoch(1);
	return 0;
}
