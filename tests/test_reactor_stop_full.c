#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/command_queue.h"
#include "../src/transport/protocol/wire.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

enum callback_operation {
	CALL_CLOSE,
	CALL_ABORT,
	CALL_SEND,
	CALL_RESUME
};

struct stop_test {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	enum callback_operation operation;
	int callback_entered;
	int callback_released;
	int callback_result;
	int call_result;
	int stop_result;
	int fail_join;
	int measuring;
	unsigned stop_submissions;
	unsigned stop_pops;
	unsigned predecessor_pops;
	unsigned join_attempts;
	uint64_t predecessor_sequence;
};

/* 在 owner 创建之前发布，owner 已 join 后才撤销。其可变字段受 lock 保护。 */
static struct stop_test *active;
static _Thread_local int joining_owner;

int __real_tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake);
size_t __real_tr_command_queue_pop_batch(
	struct tr_command_queue *queue, struct tr_command *out, size_t count);
int __real_pthread_join(pthread_t thread, void **result);

int __wrap_tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake)
{
	struct stop_test *ctx = active;
	int measuring;

	assert(ctx != NULL);
	pthread_mutex_lock(&ctx->lock);
	measuring = ctx->measuring;
	pthread_mutex_unlock(&ctx->lock);
	if (measuring) {
		assert(command->type == TR_CMD_STOP);
		pthread_mutex_lock(&queue->lock);
		assert(queue->capacity == 1U);
		assert(queue->count == queue->capacity);
		pthread_mutex_unlock(&queue->lock);

		/*
		 * 此刻 stop 已持 ctl_lock、关闭准入，且普通队列确实已满。
		 * 先允许 owner 回调尝试 close/abort/send，再进入真实 STOP 路径。
		 * 旧实现会持 ctl_lock 等待空位，构成确定性的相互等待。
		 */
		pthread_mutex_lock(&ctx->lock);
		ctx->stop_submissions++;
		ctx->callback_released = 1;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}
	return __real_tr_command_queue_push_wait_force(queue, command, need_wake);
}

size_t __wrap_tr_command_queue_pop_batch(
	struct tr_command_queue *queue, struct tr_command *out, size_t count)
{
	struct stop_test *ctx = active;
	size_t popped = __real_tr_command_queue_pop_batch(queue, out, count);
	size_t i;

	assert(ctx != NULL);
	pthread_mutex_lock(&ctx->lock);
	if (ctx->measuring && ctx->callback_released) {
		for (i = 0; i < popped; ++i) {
			if (out[i].type == TR_CMD_RESUME_RX) {
				assert(ctx->stop_pops == 0U);
				ctx->predecessor_pops++;
				ctx->predecessor_sequence = out[i].sequence;
			} else if (out[i].type == TR_CMD_STOP) {
				assert(ctx->predecessor_pops == 1U);
				assert(out[i].sequence != ctx->predecessor_sequence);
				assert(tr_command_sequence_after_eq(
					out[i].sequence, ctx->predecessor_sequence));
				ctx->stop_pops++;
			}
		}
	}
	pthread_mutex_unlock(&ctx->lock);
	return popped;
}

int __wrap_pthread_join(pthread_t thread, void **result)
{
	struct stop_test *ctx = active;
	int fail = 0;

	if (joining_owner) {
		pthread_mutex_lock(&ctx->lock);
		ctx->join_attempts++;
		fail = ctx->fail_join && ctx->join_attempts == 1U;
		pthread_mutex_unlock(&ctx->lock);
	}
	if (fail)
		return EINVAL;
	return __real_pthread_join(thread, result);
}

static int owner_callback(void *arg)
{
	struct stop_test *ctx = arg;
	int ret;

	pthread_mutex_lock(&ctx->lock);
	ctx->callback_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->callback_released)
		assert(pthread_cond_wait(&ctx->cond, &ctx->lock) == 0);
	pthread_mutex_unlock(&ctx->lock);

	switch (ctx->operation) {
	case CALL_CLOSE:
		ret = tr_reactor_close(ctx->connection);
		break;
	case CALL_ABORT:
		ret = tr_reactor_abort(ctx->connection, TR_ERR_SYS);
		break;
	case CALL_SEND:
		ret = tr_reactor_send(ctx->connection, TR_FRAME_PING,
				      0U, 0U, 1U, NULL);
		break;
	case CALL_RESUME:
		ret = tr_reactor_resume_rx(ctx->connection);
		break;
	default:
		assert(0);
		return TR_ERR_INVALID;
	}
	ctx->callback_result = ret;
	return TR_OK;
}

static void *call_thread(void *arg)
{
	struct stop_test *ctx = arg;

	ctx->call_result = tr_reactor_call(ctx->reactor, owner_callback, ctx);
	return NULL;
}

static void *stop_thread(void *arg)
{
	struct stop_test *ctx = arg;

	joining_owner = 1;
	ctx->stop_result = tr_reactor_stop(ctx->reactor);
	joining_owner = 0;
	return NULL;
}

static int restart_callback(void *arg)
{
	int *seen = arg;

	(*seen)++;
	return TR_OK;
}

static void watchdog(int signal_number)
{
	static const char message[] =
		"R8 regression: timeout while stop and owner callback wait\n";

	(void)signal_number;
	(void)write(STDERR_FILENO, message, sizeof(message) - 1U);
	_exit(124);
}

static void run_case(enum callback_operation operation, int fail_join)
{
	struct stop_test ctx;
	struct tr_reactor_config config;
	pthread_t caller;
	pthread_t stopper;
	int sockets[2];
	int seen = 0;

	memset(&ctx, 0, sizeof(ctx));
	memset(&config, 0, sizeof(config));
	config.command_capacity = 1U;
	config.max_connections = 1U;
	config.rx_buffer_count = 2U;
	config.rx_buffer_size = 128U;
	config.max_payload_len = 128U;
	config.tx_item_capacity = 2U;
	config.control_tx_item_capacity = 2U;
	ctx.operation = operation;
	ctx.fail_join = fail_join;
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	active = &ctx;
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			 0, sockets) == 0);
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &ctx.reactor) == TR_OK);
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(ctx.reactor, sockets[0], &ctx.connection) == TR_OK);
	assert(tr_reactor_quiesce(ctx.reactor) == TR_OK);

	assert(pthread_create(&caller, NULL, call_thread, &ctx) == 0);
	pthread_mutex_lock(&ctx.lock);
	while (!ctx.callback_entered)
		assert(pthread_cond_wait(&ctx.cond, &ctx.lock) == 0);
	ctx.measuring = 1;
	pthread_mutex_unlock(&ctx.lock);

	/* owner 停留在回调内，这条已经接受的命令将占满唯一的普通槽位。 */
	assert(tr_reactor_resume_rx(ctx.connection) == TR_OK);
	assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
	assert(pthread_join(stopper, NULL) == 0);
	assert(pthread_join(caller, NULL) == 0);
	assert(ctx.callback_result == TR_ERR_CLOSED);
	assert(ctx.call_result == TR_OK);
	assert(ctx.stop_result == (fail_join ? TR_ERR_SYS : TR_OK));

	/* 保留既有 join 重试保证：第二次 stop 不得再次提交 STOP。 */
	joining_owner = 1;
	assert(tr_reactor_stop(ctx.reactor) == TR_OK);
	joining_owner = 0;
	assert(ctx.stop_submissions == 1U);
	assert(ctx.stop_pops == 1U);
	assert(ctx.predecessor_pops == 1U);
	assert(ctx.join_attempts == (fail_join ? 2U : 1U));

	ctx.measuring = 0; /* owner 已 join，不存在并发访问。 */
	assert(tr_reactor_start(ctx.reactor) == TR_OK);
	assert(tr_reactor_call(ctx.reactor, restart_callback, &seen) == TR_OK);
	assert(seen == 1);
	assert(tr_reactor_stop(ctx.reactor) == TR_OK);
	assert(tr_reactor_destroy(ctx.reactor) == TR_OK);
	active = NULL;
	assert(close(sockets[1]) == 0);
	assert(pthread_cond_destroy(&ctx.cond) == 0);
	assert(pthread_mutex_destroy(&ctx.lock) == 0);
}

int main(void)
{
	int operation;
	int fail_join;

	for (operation = CALL_CLOSE; operation <= CALL_RESUME; ++operation) {
		for (fail_join = 0; fail_join <= 1; ++fail_join) {
			pid_t child;
			int status;

			printf("R8: operation=%d join_fault=%d\n", operation, fail_join);
			fflush(stdout);
			/* 每项从单线程父进程启动，超时只终止该进程，不取消持锁线程。 */
			child = fork();
			assert(child >= 0);
			if (child == 0) {
				assert(signal(SIGALRM, watchdog) != SIG_ERR);
				alarm(8U);
				run_case((enum callback_operation)operation, fail_join);
				alarm(0U);
				return 0;
			}
			assert(waitpid(child, &status, 0) == child);
			if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
				return 1;
		}
	}
	puts("R8 real Reactor full-queue stop/retry/restart: ok");
	return 0;
}
