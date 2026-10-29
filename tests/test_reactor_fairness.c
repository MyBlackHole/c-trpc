#define _GNU_SOURCE
#include "../src/execution/command_queue.h"
#include "../src/execution/async_operation_internal.h"
#include "../src/execution/reactor.h"
#include "tr/status.h"
#include "../src/transport/protocol/wire.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/completion_queue.h"
#include "../src/memory_budget.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define CAPACITY 256U
#define COMMAND_BUDGET 64U
#define COMPLETIONS 200U

/* 链接时观察者仅属于此可执行文件，不属于库或 SDK。 */
size_t __real_tr_command_queue_pop_batch(struct tr_command_queue *queue,
	struct tr_command *out, size_t max_commands);
int __real_tr_command_queue_push(struct tr_command_queue *queue,
	const struct tr_command *command, int *need_wake);
int __real_tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t expected_generation, int *need_wake);
int __real_tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake);
int __real_tr_completion_queue_push_wait(struct tr_completion_queue *queue,
	const struct tr_completion *completion, uint64_t expected_generation,
	int *need_wake);
int __real_epoll_wait(int fd, struct epoll_event *events, int maxevents,
	int timeout);

enum scenario {
	BACKLOG,
	REFILL,
	FULL_WAITERS,
	STOP_DRAIN,
	FULL_STOP,
	COMPLETION_FULL_WAIT,
	COMPLETION_FULL_STOP,
	OPERATION_COMPLETION_FULL_WAIT,
	OPERATION_COMPLETION_FULL_STOP
};

struct test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	struct tr_reactor_timer_handle timer;
	struct tr_memory_budget operation_budget;
	struct tr_async_operation_pool *operation_pool;
	struct tr_async_operation operation;
	struct tr_async_operation_ref operation_producer_ref;
	struct tr_async_operation_ref operation_completion_ref;
	enum scenario scenario;
	int peer_fd;
	int gate_entered;
	int gate_release;
	int measuring;
	int idle_seen;
	int call_full;
	int quiesce_full;
	int stop_full;
	int stop_accepted;
	int stop_popped;
	int completion_full;
	int timer_seen;
	int frame_seen;
	unsigned resumes;
	unsigned issued;
	unsigned commands_since_poll;
	unsigned polls;
	unsigned completions;
	unsigned completions_after_stop;
	unsigned operation_callbacks;
	unsigned operation_payload_destroys;
	int operation_callback_done;
	int operation_submit_done;
	int operation_submit_ret;
};

/* 启动反应器前发布，仅在线程回收后清除。 */
static struct test_ctx *active;

static void wait_flag(struct test_ctx *ctx, const int *flag)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10; /* 防挂起保护，不是延迟验收阈值。 */
	pthread_mutex_lock(&ctx->lock);
	while (!*flag)
		assert(pthread_cond_timedwait(&ctx->cond, &ctx->lock,
					     &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

int __wrap_tr_command_queue_push(struct tr_command_queue *queue,
	const struct tr_command *command, int *need_wake)
{
	return __real_tr_command_queue_push(queue, command, need_wake);
}

static int command_queue_full(struct tr_command_queue *queue)
{
	int full;

	pthread_mutex_lock(&queue->lock);
	full = queue->count == queue->capacity;
	pthread_mutex_unlock(&queue->lock);
	return full;
}

int __wrap_tr_command_queue_push_wait(
	struct tr_command_queue *queue, const struct tr_command *command,
	uint64_t expected_generation, int *need_wake)
{
	struct test_ctx *ctx = active;
	int full = command_queue_full(queue);

	if (ctx && ctx->gate_entered && !ctx->gate_release && full) {
		pthread_mutex_lock(&ctx->lock);
		if (command->type == TR_CMD_CALL)
			ctx->call_full = 1;
		if (command->type == TR_CMD_QUIESCE)
			ctx->quiesce_full = 1;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}
	return __real_tr_command_queue_push_wait(
		queue, command, expected_generation, need_wake);
}

int __wrap_tr_command_queue_push_wait_force(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake)
{
	struct test_ctx *ctx = active;
	int full = command_queue_full(queue);
	int ret;

	if (ctx && command->type == TR_CMD_STOP && full) {
		pthread_mutex_lock(&ctx->lock);
		ctx->stop_full = 1;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}

	ret = __real_tr_command_queue_push_wait_force(
		queue, command, need_wake);
	if (ctx && command->type == TR_CMD_STOP && ret == TR_OK) {
		pthread_mutex_lock(&ctx->lock);
		ctx->stop_accepted = 1;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}
	return ret;
}

int __wrap_tr_completion_queue_push_wait(
	struct tr_completion_queue *queue,
	const struct tr_completion *completion, uint64_t expected_generation,
	int *need_wake)
{
	struct test_ctx *ctx = active;
	int full = 0;
	int ret;

	/* 仅供测试的观测：所有者被门禁阻塞期间 count 不会下降，因此在调用
	 * 真实阻塞 API 前观察满队列，等价于证明生产者即将进入容量等待。
	 */
	if (ctx && (ctx->scenario == COMPLETION_FULL_WAIT ||
	            ctx->scenario == COMPLETION_FULL_STOP ||
	            ctx->scenario == OPERATION_COMPLETION_FULL_WAIT ||
	            ctx->scenario == OPERATION_COMPLETION_FULL_STOP)) {
		pthread_mutex_lock(&queue->lock);
		full = queue->accepting && queue->count == queue->capacity;
		pthread_mutex_unlock(&queue->lock);
		if (full) {
			pthread_mutex_lock(&ctx->lock);
			ctx->completion_full = 1;
			pthread_cond_broadcast(&ctx->cond);
			pthread_mutex_unlock(&ctx->lock);
		}
	}

	ret = __real_tr_completion_queue_push_wait(
		queue, completion, expected_generation, need_wake);
	return ret;
}

size_t __wrap_tr_command_queue_pop_batch(struct tr_command_queue *queue,
	struct tr_command *out, size_t max_commands)
{
	struct test_ctx *ctx = active;
	size_t count = __real_tr_command_queue_pop_batch(queue, out, max_commands);
	size_t refill = 0;
	size_t i;

	if (!ctx)
		return count;
	pthread_mutex_lock(&ctx->lock);
	if (ctx->measuring) {
		ctx->commands_since_poll += (unsigned)count;
		for (i = 0; i < count; ++i) {
			if (out[i].type == TR_CMD_RESUME_RX)
				ctx->resumes++;
			if (out[i].type == TR_CMD_STOP)
				ctx->stop_popped = 1;
		}
		/* 分派前通过真实公开生产者路径重新填充。 */
		if (ctx->scenario == REFILL && ctx->issued < 3U * CAPACITY) {
			refill = count;
			if (refill > 3U * CAPACITY - ctx->issued)
				refill = 3U * CAPACITY - ctx->issued;
			ctx->issued += (unsigned)refill;
		}
	}
	pthread_mutex_unlock(&ctx->lock);
	for (i = 0; i < refill; ++i)
		assert(tr_reactor_resume_rx(ctx->connection) == TR_OK);
	return count;
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int maxevents,
	int timeout)
{
	struct test_ctx *ctx = active;

	if (ctx) {
		pthread_mutex_lock(&ctx->lock);
		if (ctx->measuring) {
			/* 也捕获唤醒事件路径中的第二个命令批次。 */
			assert(ctx->commands_since_poll <= COMMAND_BUDGET);
			ctx->commands_since_poll = 0;
			ctx->polls++;
			if (ctx->scenario == BACKLOG || ctx->scenario == REFILL) {
				if (ctx->resumes < ctx->issued) {
					/* 首次轮询可能消耗初始生产者唤醒。 */
					if (ctx->resumes != 0U)
						assert(timeout == 0);
				} else if (timeout != 0) {
					ctx->idle_seen = 1;
					pthread_cond_broadcast(&ctx->cond);
				}
			}
		}
		pthread_mutex_unlock(&ctx->lock);
	}
	return __real_epoll_wait(fd, events, maxevents, timeout);
}

static uint64_t timer_callback(void *arg, uint64_t now_ns)
{
	struct test_ctx *ctx = arg;

	(void)now_ns;
	pthread_mutex_lock(&ctx->lock);
	assert(ctx->resumes < ctx->issued);
	ctx->timer_seen = 1;
	pthread_mutex_unlock(&ctx->lock);
	return 0;
}

static void completion_callback(void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	if (ctx->scenario == BACKLOG || ctx->scenario == REFILL)
		assert(ctx->resumes < ctx->issued);
	ctx->completions++;
	if (ctx->stop_popped)
		ctx->completions_after_stop++;
	pthread_mutex_unlock(&ctx->lock);
}

static void operation_payload_destroy(void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->operation_payload_destroys++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void operation_completion_callback(void *arg)
{
	struct test_ctx *ctx = arg;
	struct tr_async_operation_spec spec = {0};
	struct tr_async_operation_completion completion = {0};
	int cancelled = 0;
	int cancel_reason = TR_OK;

	assert(tr_async_operation_consume(ctx->operation_pool,
	                                  &ctx->operation_completion_ref, &spec,
	                                  &completion, &cancelled,
	                                  &cancel_reason) == TR_OK);
	assert(spec.kind == TR_ASYNC_OPERATION_KIND_RPC_AUTHORIZATION);
	assert(completion.status == TR_OK);
	assert(completion.value == UINT64_C(0x1020304050607080));
	assert(completion.payload == ctx);
	assert(completion.destroy == operation_payload_destroy);
	assert(!cancelled && cancel_reason == TR_OK);
	completion.destroy(completion.payload);
	assert(tr_async_operation_retire(ctx->operation_pool,
	                                 &ctx->operation_completion_ref) ==
	       TR_OK);

	/* 先完成全部观察计数，再发布 done，避免等待线程看到半成品。 */
	completion_callback(ctx);

	pthread_mutex_lock(&ctx->lock);
	ctx->operation_callbacks++;
	ctx->operation_callback_done = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int prepare_operation_on_owner(void *arg)
{
	struct test_ctx *ctx = arg;
	struct tr_async_operation_spec spec = {
	        .kind = TR_ASYNC_OPERATION_KIND_RPC_AUTHORIZATION,
	        .domain_generation = 1U,
	        .object_generation = 2U,
	};
	int ready = -1;
	int ret;

	ret = tr_async_operation_reserve(ctx->operation_pool, &spec,
	                                 &ctx->operation,
	                                 &ctx->operation_producer_ref);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_accept(ctx->operation_pool, ctx->operation,
	                                &ctx->operation_completion_ref, &ready);
	if (ret != TR_OK)
		return ret;
	assert(ready == 0);
	return TR_OK;
}

static int bind_operation_pool_on_owner(void *arg)
{
	struct test_ctx *ctx = arg;

	return tr_async_operation_pool_bind_owner(ctx->operation_pool);
}

static void operation_pool_create(struct test_ctx *ctx)
{
	tr_memory_budget_init(&ctx->operation_budget, 65536U);
	assert(tr_async_operation_pool_create(1U, &ctx->operation_budget,
	                                      &ctx->operation_pool) == TR_OK);
	assert(tr_reactor_call(ctx->reactor, bind_operation_pool_on_owner,
	                       ctx) == TR_OK);
	assert(tr_reactor_call(ctx->reactor, prepare_operation_on_owner, ctx) ==
	       TR_OK);
}

static void *operation_completion_submit_thread(void *arg)
{
	struct test_ctx *ctx = arg;
	struct tr_async_operation_completion completion = {
	        .status = TR_OK,
	        .value = UINT64_C(0x1020304050607080),
	        .payload = ctx,
	        .destroy = operation_payload_destroy,
	};
	int ready = 0;
	int ret;

	assert(tr_async_operation_publish(
	               ctx->operation_pool, &ctx->operation_producer_ref,
	               completion, &ctx->operation_completion_ref,
	               &ready) == TR_OK);
	assert(ready == 1);
	ret = tr_reactor_complete(ctx->reactor, operation_completion_callback,
	                          ctx);
	pthread_mutex_lock(&ctx->lock);
	ctx->operation_submit_ret = ret;
	ctx->operation_submit_done = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	assert(ret == (ctx->scenario == OPERATION_COMPLETION_FULL_STOP
	                       ? TR_ERR_CLOSED
	                       : TR_OK));
	return NULL;
}

static int operation_pool_close_destroy_on_owner(void *arg)
{
	struct test_ctx *ctx = arg;
	int ret = tr_async_operation_pool_close(ctx->operation_pool);
	if (ret != TR_OK)
		return ret;
	return tr_async_operation_pool_destroy(&ctx->operation_pool);
}

static void operation_pool_reap_after_stop(struct test_ctx *ctx)
{
	struct tr_memory_budget_stats stats;
	uint32_t reaped = 0U;

	assert(tr_async_operation_reap_stopped(ctx->operation_pool, 1,
	                                       &reaped) == TR_OK);
	assert(reaped == 1U);
	memset(&ctx->operation_completion_ref, 0,
	       sizeof(ctx->operation_completion_ref));
	assert(tr_async_operation_pool_destroy(&ctx->operation_pool) == TR_OK);
	tr_memory_budget_get_stats(&ctx->operation_budget, &stats);
	assert(stats.current_bytes == 0U);
}

static enum tr_frame_disposition frame_callback(struct tr_conn_handle connection,
	struct tr_frame *frame, void *arg)
{
	struct test_ctx *ctx = arg;

	(void)connection;
	assert(frame->header.type == TR_FRAME_PING);
	pthread_mutex_lock(&ctx->lock);
	assert(ctx->resumes < ctx->issued);
	ctx->frame_seen = 1;
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

/* 唯一有意阻塞的所有者回调是此测试屏障。 */
static int gate_callback(void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->gate_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->gate_release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	ctx->measuring = 1;
	pthread_mutex_unlock(&ctx->lock);
	if (ctx->scenario == BACKLOG || ctx->scenario == REFILL)
		assert(tr_reactor_timer_arm(ctx->timer, 1U) == TR_OK);
	return TR_OK;
}

static void *gate_thread(void *arg)
{
	struct test_ctx *ctx = arg;

	assert(tr_reactor_call(ctx->reactor, gate_callback, ctx) == TR_OK);
	return NULL;
}

static int marker_callback(void *arg)
{
	struct test_ctx *ctx = arg;

	pthread_mutex_lock(&ctx->lock);
	assert(ctx->resumes == CAPACITY);
	pthread_mutex_unlock(&ctx->lock);
	return TR_ERR_UNSUPPORTED; /* 保留精确的所有者结果。 */
}

static void *call_thread(void *arg)
{
	struct test_ctx *ctx = arg;
	int ret = tr_reactor_call(ctx->reactor, marker_callback, ctx);

	assert(ret == (ctx->scenario == FULL_STOP ? TR_ERR_CLOSED :
		       TR_ERR_UNSUPPORTED));
	return NULL;
}

static void *quiesce_thread(void *arg)
{
	struct test_ctx *ctx = arg;
	int ret = tr_reactor_quiesce(ctx->reactor);

	assert(ret == (ctx->scenario == FULL_STOP ? TR_ERR_CLOSED : TR_OK));
	if (ret == TR_OK) {
		pthread_mutex_lock(&ctx->lock);
		assert(ctx->resumes == CAPACITY);
		pthread_mutex_unlock(&ctx->lock);
	}
	return NULL;
}

static void *stop_thread(void *arg)
{
	struct test_ctx *ctx = arg;

	assert(tr_reactor_stop(ctx->reactor) == TR_OK);
	return NULL;
}

static void *completion_submit_thread(void *arg)
{
	struct test_ctx *ctx = arg;
	int ret = tr_reactor_complete(
		ctx->reactor, completion_callback, ctx);

	assert(ret == (ctx->scenario == COMPLETION_FULL_STOP ?
		       TR_ERR_CLOSED : TR_OK));
	return NULL;
}

static void setup(struct test_ctx *ctx, enum scenario scenario)
{
	struct tr_reactor_config config = {0};
	int fds[2];

	memset(ctx, 0, sizeof(*ctx));
	ctx->scenario = scenario;
	assert(pthread_mutex_init(&ctx->lock, NULL) == 0);
	assert(pthread_cond_init(&ctx->cond, NULL) == 0);
	active = ctx;
	config.max_connections = 2U;
	config.command_capacity = CAPACITY;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 8U;
	config.rx_buffer_count = 4U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	assert(tr_reactor_create(&config, frame_callback, NULL, ctx,
				 &ctx->reactor) == TR_OK);
	assert(tr_reactor_timer_register(ctx->reactor, timer_callback, ctx,
					 &ctx->timer) == TR_OK);
	assert(tr_reactor_start(ctx->reactor) == TR_OK);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			  0, fds) == 0);
	ctx->peer_fd = fds[1];
	assert(tr_reactor_adopt_fd(ctx->reactor, fds[0], &ctx->connection) == TR_OK);
	assert(tr_reactor_quiesce(ctx->reactor) == TR_OK);
}

static void release_gate(struct test_ctx *ctx)
{
	pthread_mutex_lock(&ctx->lock);
	ctx->gate_release = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void teardown(struct test_ctx *ctx)
{
	assert(tr_reactor_stop(ctx->reactor) == TR_OK);
	assert(tr_reactor_timer_unregister(ctx->timer) == TR_OK);
	tr_reactor_destroy(ctx->reactor);
	close(ctx->peer_fd);
	active = NULL;
	pthread_cond_destroy(&ctx->cond);
	pthread_mutex_destroy(&ctx->lock);
}

static void enqueue_resumes(struct test_ctx *ctx, unsigned count)
{
	unsigned i;

	/* 所有者停在门禁；填满环形队列期间不可能有命令消失。 */
	for (i = 0; i < count; ++i)
		assert(tr_reactor_resume_rx(ctx->connection) == TR_OK);
	ctx->issued = count;
}

static void test_fairness(enum scenario scenario)
{
	struct test_ctx ctx;
	struct tr_frame_header header = {0};
	uint8_t wire[TR_WIRE_HEADER_SIZE];
	uint8_t response[TR_WIRE_HEADER_SIZE];
	pthread_t gate;

	setup(&ctx, scenario);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);
	assert(tr_reactor_send(ctx.connection, TR_FRAME_PING, 0U, 0U, 7U,
			       NULL) == TR_OK);
	enqueue_resumes(&ctx, CAPACITY - 1U);
	assert(tr_reactor_resume_rx(ctx.connection) == TR_AGAIN);
	assert(tr_reactor_complete(ctx.reactor, completion_callback, &ctx) == TR_OK);
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_PING;
	header.message_id = 9U;
	assert(tr_wire_header_encode(wire, &header) == TR_OK);
	assert(send(ctx.peer_fd, wire, sizeof(wire), MSG_NOSIGNAL) ==
	       (ssize_t)sizeof(wire));
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	/* 没有新生产者或定时器唤醒剩余积压来使测试通过。 */
	wait_flag(&ctx, &ctx.idle_seen);
	assert(recv(ctx.peer_fd, response, sizeof(response), MSG_WAITALL) ==
	       (ssize_t)sizeof(response));
	teardown(&ctx);
	assert(ctx.timer_seen && ctx.frame_seen && ctx.completions == 1U);
	assert(ctx.resumes == ctx.issued && ctx.polls > 1U);
	puts(scenario == BACKLOG ? "backlog/no-new-wake/idle: ok" :
	     "continuous-refill/fairness: ok");
}

static void test_full_queue_waiters(int stopping)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t caller;
	pthread_t quiescer;
	pthread_t stopper;

	setup(&ctx, stopping ? FULL_STOP : FULL_WAITERS);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);
	enqueue_resumes(&ctx, CAPACITY);
	assert(pthread_create(&caller, NULL, call_thread, &ctx) == 0);
	assert(pthread_create(&quiescer, NULL, quiesce_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.call_full);
	wait_flag(&ctx, &ctx.quiesce_full);
	if (stopping) {
		assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
		wait_flag(&ctx, &ctx.stop_full);
		/* 这些操作必须拒绝，无需等待被阻塞所有者恢复。 */
		assert(pthread_join(caller, NULL) == 0);
		assert(pthread_join(quiescer, NULL) == 0);
	}
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	if (stopping) {
		assert(pthread_join(stopper, NULL) == 0);
	} else {
		assert(pthread_join(caller, NULL) == 0);
		assert(pthread_join(quiescer, NULL) == 0);
	}
	teardown(&ctx);
	assert(ctx.resumes == CAPACITY);
	puts(stopping ? "full-queue/stop/rejected-waiters: ok" :
	     "full-queue/synchronous-call/quiesce: ok");
}

static void test_stop_drain(void)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t stopper;
	unsigned i;

	setup(&ctx, STOP_DRAIN);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);
	enqueue_resumes(&ctx, 32U);
	for (i = 0; i < COMPLETIONS; ++i)
		assert(tr_reactor_complete(ctx.reactor, completion_callback, &ctx) == TR_OK);
	assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.stop_accepted);
	assert(tr_reactor_complete(ctx.reactor, completion_callback, &ctx) == TR_ERR_CLOSED);
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(stopper, NULL) == 0);
	teardown(&ctx);
	assert(ctx.resumes == 32U);
	assert(ctx.completions == COMPLETIONS && ctx.completions_after_stop > 0U);
	puts("stop/fifo/completion-drain: ok");
}

static void test_completion_full_wait_resume(void)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t producer;
	unsigned i;

	setup(&ctx, COMPLETION_FULL_WAIT);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);

	for (i = 0; i < CAPACITY; ++i)
		assert(tr_reactor_complete(
			       ctx.reactor, completion_callback, &ctx) == TR_OK);

	assert(pthread_create(
		       &producer, NULL, completion_submit_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.completion_full);

	/* 所有者恢复后，弹出批次释放容量，被阻塞生产者必须成功交接。 */
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(producer, NULL) == 0);
	teardown(&ctx);

	assert(ctx.completions == CAPACITY + 1U);
	puts("completion-full/wait/pop-wake: ok");
}

static void test_completion_full_stop(void)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t producer;
	pthread_t stopper;
	unsigned i;

	setup(&ctx, COMPLETION_FULL_STOP);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);

	for (i = 0; i < CAPACITY; ++i)
		assert(tr_reactor_complete(ctx.reactor, completion_callback, &ctx) ==
		       TR_OK);

	assert(pthread_create(&producer, NULL, completion_submit_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.completion_full);

	assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.stop_accepted);

	/* 即使所有者仍被阻塞，生产者也必须观察到队列本地准入关闭。
	 * 这证明停止不依赖完成项生产者交接 ctl_lock。
	 */
	assert(pthread_join(producer, NULL) == 0);

	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(stopper, NULL) == 0);
	teardown(&ctx);

	assert(ctx.completions == CAPACITY);
	assert(ctx.completions_after_stop > 0U);
	assert(ctx.completions_after_stop < CAPACITY);
	puts("completion-full/stop/admission-close: ok");
}

static void test_operation_completion_full_wait_resume(void)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t producer;
	unsigned i;

	setup(&ctx, OPERATION_COMPLETION_FULL_WAIT);
	operation_pool_create(&ctx);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);

	for (i = 0; i < CAPACITY; ++i)
		assert(tr_reactor_complete(ctx.reactor, completion_callback,
		                           &ctx) == TR_OK);

	assert(pthread_create(&producer, NULL,
	                      operation_completion_submit_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.completion_full);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.operation_payload_destroys == 0U);
	assert(ctx.operation_callbacks == 0U);
	pthread_mutex_unlock(&ctx.lock);

	/* 完成项受阻期间操作仍保持已发布状态；队列恢复后必须只交付和退役一次。
	 */
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	wait_flag(&ctx, &ctx.operation_callback_done);
	assert(pthread_join(producer, NULL) == 0);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.operation_submit_ret == TR_OK);
	assert(ctx.operation_callbacks == 1U);
	assert(ctx.operation_payload_destroys == 1U);
	assert(ctx.completions == CAPACITY + 1U);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_call(ctx.reactor,
	                       operation_pool_close_destroy_on_owner,
	                       &ctx) == TR_OK);
	assert(ctx.operation_pool == NULL);
	teardown(&ctx);
	puts("accepted-operation/completion-full/wait-retire-once: ok");
}

static void test_operation_completion_full_stop_reaps_once(void)
{
	struct test_ctx ctx;
	pthread_t gate;
	pthread_t producer;
	pthread_t stopper;
	unsigned i;

	setup(&ctx, OPERATION_COMPLETION_FULL_STOP);
	operation_pool_create(&ctx);
	assert(pthread_create(&gate, NULL, gate_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.gate_entered);

	for (i = 0; i < CAPACITY; ++i)
		assert(tr_reactor_complete(ctx.reactor, completion_callback,
		                           &ctx) == TR_OK);

	assert(pthread_create(&producer, NULL,
	                      operation_completion_submit_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.completion_full);
	assert(pthread_create(&stopper, NULL, stop_thread, &ctx) == 0);
	wait_flag(&ctx, &ctx.stop_accepted);
	assert(pthread_join(producer, NULL) == 0);
	wait_flag(&ctx, &ctx.operation_submit_done);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.operation_submit_ret == TR_ERR_CLOSED);
	assert(ctx.operation_callbacks == 0U);
	assert(ctx.operation_payload_destroys == 0U);
	pthread_mutex_unlock(&ctx.lock);

	/* 队列关闭会拒绝交接但不消费结果；producer 与 owner 都停止后，
	 * 外部清理者才可接管并丢弃该结果。
	 */
	release_gate(&ctx);
	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(stopper, NULL) == 0);
	operation_pool_reap_after_stop(&ctx);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.operation_callbacks == 0U);
	assert(ctx.operation_payload_destroys == 1U);
	assert(ctx.completions == CAPACITY);
	pthread_mutex_unlock(&ctx.lock);
	teardown(&ctx);
	puts("accepted-operation/completion-full/stop-reap-once: ok");
}

int main(void)
{
	alarm(60); /* 也覆盖线程回收；失败绝不会让 CI 永久阻塞。 */
	test_fairness(BACKLOG);
	test_fairness(REFILL);
	test_full_queue_waiters(0);
	test_full_queue_waiters(1);
	test_stop_drain();
	test_completion_full_wait_resume();
	test_completion_full_stop();
	test_operation_completion_full_wait_resume();
	test_operation_completion_full_stop_reaps_once();
	alarm(0);
	return 0;
}
