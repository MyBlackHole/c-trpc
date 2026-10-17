#define _POSIX_C_SOURCE 200809L
/* V2 门面分层排空：网络槽、任务、完成交付与用户回调分别设置真实门闩。
 * 只在公开模块边界观察或暂停；全部套接字、记录流和时钟均为真实实现。 */
#include "../src/client_record_internal.h"
#include "../src/server_record_internal.h"
#include "../src/endian.h"
#include "../src/execution/deadline_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/memory_budget.h"
#include "../src/transport/channel/channel_internal.h"
#include "tr/status.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)

enum { CAP = 2, BODY = 32, WIRE = 4096, WAIT_MS = 15000 };

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool released;
	_Atomic unsigned entered;
};
struct fixture {
	struct tr_client *client;
	struct tr_server *server;
	_Atomic(struct tr_channel *) client_channel, server_channel;
	_Atomic(struct tr_rpc_record_endpoint *) endpoint;
	_Atomic(struct tr_task_executor *) executor;
	struct tr_memory_budget *client_budget, *server_budget;
	uint64_t client_idle_bytes, server_idle_bytes;
	tr_rpc_endpoint_terminal_fn original_terminal;
	void *original_terminal_arg;
	struct gate work_gate, server_terminal_gate, client_terminal_gate;
	struct gate owner_gate;
	_Atomic bool block_work, pause_done, block_server_terminal;
	_Atomic bool block_client_terminal, block_open, drain_in_writable;
	_Atomic unsigned client_creates, server_creates, received[6],
	        executed[6];
	_Atomic unsigned open_attempts, open_accepted, bodies, bodies_released;
	_Atomic unsigned work_entered, work_cancelled, work_exited;
	_Atomic unsigned server_terminals, server_terminal_returns;
	_Atomic unsigned writable_drains;
};
struct call {
	struct fixture *fixture;
	struct tr_rpc_record_client_handle handle;
	_Atomic unsigned writable, messages, finals, terminals,
	        terminal_returns;
	enum tr_rpc_status status;
};
struct drain_wait {
	struct fixture *fixture;
	bool server;
	int result;
	struct tr_deadline *deadline;
	struct timespec absolute;
	_Atomic unsigned inits, rpc_waits, channel_waits, channel_cond_waits;
	_Atomic unsigned done;
	bool in_channel;
};

/* 创建线程前发布，在所有 owner/worker join 后清除。 */
static struct fixture *active;
static _Thread_local struct drain_wait *current_wait;

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(!clock_gettime(CLOCK_MONOTONIC, &ts));
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void pause_once(void)
{
	const struct timespec delay = {0, 1000000};
	CHECK(!nanosleep(&delay, NULL) || errno == EINTR);
}
static void wait_count(_Atomic unsigned *count, unsigned expected)
{
	uint64_t end = now_ms() + WAIT_MS;
	while (atomic_load(count) < expected) {
		CHECK(now_ms() < end);
		pause_once();
	}
}
static void gate_init(struct gate *gate)
{
	pthread_condattr_t attr;
	CHECK(!pthread_mutex_init(&gate->lock, NULL));
	CHECK(!pthread_condattr_init(&attr));
	CHECK(!pthread_condattr_setclock(&attr, CLOCK_MONOTONIC));
	CHECK(!pthread_cond_init(&gate->cond, &attr));
	CHECK(!pthread_condattr_destroy(&attr));
}
static void gate_wait(struct gate *gate)
{
	struct timespec deadline;
	CHECK(!clock_gettime(CLOCK_MONOTONIC, &deadline));
	deadline.tv_sec += WAIT_MS / 1000;
	CHECK(!pthread_mutex_lock(&gate->lock));
	atomic_fetch_add(&gate->entered, 1);
	while (!gate->released)
		CHECK(!pthread_cond_timedwait(&gate->cond, &gate->lock,
		                              &deadline));
	CHECK(!pthread_mutex_unlock(&gate->lock));
}
static void gate_release(struct gate *gate)
{
	CHECK(!pthread_mutex_lock(&gate->lock));
	gate->released = true;
	CHECK(!pthread_cond_broadcast(&gate->cond));
	CHECK(!pthread_mutex_unlock(&gate->lock));
}
static void gate_destroy(struct gate *gate)
{
	CHECK(!pthread_cond_destroy(&gate->cond));
	CHECK(!pthread_mutex_destroy(&gate->lock));
}
static struct tr_reactor *owner(struct fixture *f, bool server)
{
	struct tr_channel *channel = server ? atomic_load(&f->server_channel)
	                                    : atomic_load(&f->client_channel);
	CHECK(channel);
	return tr_channel_reactor(channel);
}
static int barrier(void *arg)
{
	(void)arg;
	CHECK(tr_reactor_in_owner_context());
	return TR_OK;
}

int __real_tr_deadline_init_ms(struct tr_deadline *, uint32_t);
int __wrap_tr_deadline_init_ms(struct tr_deadline *deadline, uint32_t timeout)
{
	int ret = __real_tr_deadline_init_ms(deadline, timeout);
	if (current_wait) {
		CHECK(timeout == WAIT_MS && ret == TR_OK);
		CHECK(atomic_fetch_add(&current_wait->inits, 1) == 0);
		current_wait->deadline = deadline;
		current_wait->absolute = deadline->absolute;
	}
	return ret;
}
int __real_tr_channel_wait_drained_deadline(struct tr_channel *,
                                            const struct tr_deadline *);
int __wrap_tr_channel_wait_drained_deadline(struct tr_channel *channel,
                                            const struct tr_deadline *deadline)
{
	struct drain_wait *wait = current_wait;
	if (wait) {
		CHECK(deadline == wait->deadline && deadline &&
		      !deadline->infinite);
		CHECK(deadline->absolute.tv_sec == wait->absolute.tv_sec &&
		      deadline->absolute.tv_nsec == wait->absolute.tv_nsec);
		CHECK(atomic_fetch_add(&wait->channel_waits, 1) == 0);
		wait->in_channel = true;
	}
	int ret = __real_tr_channel_wait_drained_deadline(channel, deadline);
	if (wait)
		wait->in_channel = false;
	return ret;
}
int __real_pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *,
                                  const struct timespec *);
int __wrap_pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *lock,
                                  const struct timespec *absolute)
{
	/* 只观察正在执行门面 drain 的外部线程；owner、worker 以及测试
	 * 门闩的时钟与条件变量原样透传。两个层次必须借用同一绝对值。 */
	struct drain_wait *wait = current_wait;
	if (wait) {
		CHECK(wait->deadline && absolute == &wait->deadline->absolute);
		CHECK(absolute->tv_sec == wait->absolute.tv_sec &&
		      absolute->tv_nsec == wait->absolute.tv_nsec);
		atomic_fetch_add(wait->in_channel ? &wait->channel_cond_waits
		                                  : &wait->rpc_waits,
		                 1);
	}
	return __real_pthread_cond_timedwait(cond, lock, absolute);
}

static void server_terminal(struct tr_stream_handle stream,
                            struct tr_rpc_endpoint_result result, void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_reactor_in_owner_context_for(owner(f, true)));
	CHECK(result.delivery == TR_RPC_RECORD_ON_OWNER);
	atomic_fetch_add(&f->server_terminals, 1);
	if (atomic_load(&f->block_server_terminal))
		gate_wait(&f->server_terminal_gate);
	f->original_terminal(stream, result, f->original_terminal_arg);
	atomic_fetch_add(&f->server_terminal_returns, 1);
}
int __real_tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *, struct tr_task_executor *,
        const struct tr_rpc_dispatch_method *, size_t,
        const struct tr_rpc_record_endpoint_config *,
        tr_rpc_endpoint_terminal_fn, void *, struct tr_memory_budget *,
        struct tr_rpc_record_endpoint **);
int __wrap_tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t count,
        const struct tr_rpc_record_endpoint_config *config,
        tr_rpc_endpoint_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_endpoint **out)
{
	struct fixture *f = active;
	CHECK(f);
	f->original_terminal = terminal;
	f->original_terminal_arg = arg;
	atomic_store(&f->server_channel, channel);
	int ret = __real_tr_rpc_record_endpoint_create_on_owner(
	        channel, executor, methods, count, config, server_terminal, f,
	        budget, out);
	if (ret == TR_OK) {
		f->server_budget = budget;
		atomic_store(&f->endpoint, *out);
		atomic_store(&f->executor, executor);
		atomic_fetch_add(&f->server_creates, 1);
	}
	return ret;
}
int __real_tr_rpc_record_client_create_on_owner(
        struct tr_channel *, const struct tr_rpc_method_desc *, size_t,
        const struct tr_rpc_record_client_config *, struct tr_memory_budget *,
        struct tr_rpc_record_client **);
int __wrap_tr_rpc_record_client_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_method_desc *methods,
        size_t count, const struct tr_rpc_record_client_config *config,
        struct tr_memory_budget *budget, struct tr_rpc_record_client **out)
{
	int ret = __real_tr_rpc_record_client_create_on_owner(
	        channel, methods, count, config, budget, out);
	if (ret == TR_OK) {
		CHECK(active);
		active->client_budget = budget;
		atomic_store(&active->client_channel, channel);
		atomic_fetch_add(&active->client_creates, 1);
	}
	return ret;
}
int __real_tr_task_executor_dispatch(struct tr_task_executor *, size_t,
                                     size_t *);
int __wrap_tr_task_executor_dispatch(struct tr_task_executor *executor,
                                     size_t limit, size_t *delivered)
{
	if (active && executor == atomic_load(&active->executor) &&
	    atomic_load(&active->pause_done)) {
		CHECK(tr_reactor_in_owner_context());
		*delivered = 0;
		return TR_OK;
	}
	return __real_tr_task_executor_dispatch(executor, limit, delivered);
}
static unsigned record_kind(const uint8_t *data, size_t size)
{
	if (size < TR_RPC_RECORD_HEADER_SIZE || memcmp(data, "TRPC", 4))
		return 0;
	unsigned kind = tr_get_le16(data + 6);
	CHECK(kind >= TR_RPC_RECORD_OPEN && kind <= TR_RPC_RECORD_FINAL_STATUS);
	return kind;
}
int __real_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *, struct tr_stream_handle,
        const uint8_t *, size_t);
int __wrap_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *dispatch, struct tr_stream_handle stream,
        const uint8_t *data, size_t size)
{
	unsigned kind = record_kind(data, size);
	CHECK(active && kind &&
	      stream.channel == atomic_load(&active->server_channel));
	int ret = __real_tr_rpc_record_dispatch_receive_on_owner(
	        dispatch, stream, data, size);
	if (ret == TR_OK)
		atomic_fetch_add(&active->received[kind], 1);
	return ret;
}
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	struct fixture *f = active;
	CHECK(f);
	bool open = stream.channel == atomic_load(&f->client_channel) &&
	            count &&
	            record_kind(buffers[0]->data, buffers[0]->len) ==
	                    TR_RPC_RECORD_OPEN;
	if (open) {
		atomic_fetch_add(&f->open_attempts, 1);
		if (atomic_load(&f->block_open))
			return TR_AGAIN;
	}
	int ret = __real_tr_stream_sendv(stream, buffers, count);
	if (open && ret == TR_OK)
		atomic_fetch_add(&f->open_accepted, 1);
	return ret;
}

static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct fixture *f = arg;
	unsigned kind = view->header.kind;
	(void)id;
	CHECK(tr_task_in_worker_context() && !tr_reactor_in_owner_context());
	if (kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(view->header.decoded_size == BODY);
		atomic_fetch_add(&f->work_entered, 1);
		if (atomic_load(&f->block_work)) {
			uint64_t end = now_ms() + WAIT_MS;
			while (!tr_task_cancel_requested(cancel)) {
				CHECK(now_ms() < end);
				pause_once();
			}
			atomic_fetch_add(&f->work_cancelled, 1);
			gate_wait(&f->work_gate);
		}
		/* 取消后仍验证借用并生成迟到成功；等待完成前不能释放这些存储。
		 */
		for (unsigned i = 0; i < BODY; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7 + 3));
		CHECK(tr_rpc_record_reply_append(reply, view->body, BODY) ==
		      TR_OK);
		atomic_fetch_add(&f->work_exited, 1);
	}
	atomic_fetch_add(&f->executed[kind], 1);
	return TR_RPC_STATUS_OK;
}
static void fixture_init(struct fixture *f)
{
	CHECK(!active);
	*f = (struct fixture){0};
	active = f;
	gate_init(&f->work_gate);
	gate_init(&f->server_terminal_gate);
	gate_init(&f->client_terminal_gate);
	gate_init(&f->owner_gate);
	struct tr_server_config sc;
	struct tr_client_config cc;
	struct tr_facade_tuning tuning;
	tr_server_config_init(&sc);
	sc.shard_count = sc.max_peers = 1;
	sc.keepalive_interval_ms = 0;
	sc.limits.max_calls = sc.limits.max_streams = CAP;
	sc.limits.max_methods = 1;
	sc.limits.max_frame_payload_bytes = 256;
	sc.limits.max_message_bytes = WIRE;
	sc.limits.initial_window_bytes = WIRE;
	sc.limits.window_update_threshold_bytes = 1;
	tr_client_config_init(&cc);
	cc.limits = sc.limits;
	cc.keepalive_interval_ms = 0;
	cc.connect_timeout_ms = 2000;
	tr_facade_tuning_init(&tuning);
	tuning.command_capacity = 64;
	tuning.tx_item_capacity = 32;
	tuning.control_tx_item_capacity = 16;
	tuning.rx_buffer_count = 16;
	tuning.reassembly_pool_count = 4;
	const struct tr_server_record_config sr = {
	        .endpoint = {.dispatch = {CAP, 4, 1, BODY},
	                     .completion_limit = 2,
	                     .progress_limit = 2,
	                     .progress_period_ns = 1000000,
	                     .final_wait_ns = 1000000000},
	        .tasks = {1, 4},
	        .memory_budget_bytes = 8 * 1024 * 1024};
	const struct tr_client_record_config cr = {.calls = {CAP, 2, 1000000},
	                                           .memory_budget_bytes =
	                                                   8 * 1024 * 1024};
	const struct tr_rpc_dispatch_method method = {
	        .descriptor = {17, 1, TR_RPC_MANY, TR_RPC_MANY,
	                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
	                       TR_LANE_CONTROL, BODY, BODY},
	        .work = business,
	        .arg = f};
	CHECK(tr_server_create_record(&sc, &tuning, &sr, &method, 1,
	                              &f->server) == TR_OK);
	CHECK(tr_client_create_record(&cc, &tuning, &cr, &method.descriptor, 1,
	                              &f->client) == TR_OK);
	uint16_t port;
	CHECK(tr_server_listen(f->server, "127.0.0.1", 0, &port) == TR_OK);
	CHECK(tr_server_start(f->server) == TR_OK);
	CHECK(tr_client_connect(f->client, "127.0.0.1", port) == TR_OK);
	CHECK(tr_client_wait_ready(f->client, WAIT_MS) == TR_OK);
	wait_count(&f->client_creates, 1);
	wait_count(&f->server_creates, 1);
	CHECK(tr_reactor_call(owner(f, false), barrier, NULL) == TR_OK);
	CHECK(tr_reactor_call(owner(f, true), barrier, NULL) == TR_OK);
	f->client_idle_bytes = atomic_load(&f->client_budget->current_bytes);
	f->server_idle_bytes = atomic_load(&f->server_budget->current_bytes);
}
static void fixture_destroy(struct fixture *f)
{
	CHECK(tr_client_destroy(f->client) == TR_OK);
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	CHECK(atomic_load(&f->bodies) == atomic_load(&f->bodies_released));
	gate_destroy(&f->owner_gate);
	gate_destroy(&f->client_terminal_gate);
	gate_destroy(&f->server_terminal_gate);
	gate_destroy(&f->work_gate);
	active = NULL;
}
static void client_received(struct tr_rpc_record_client_handle handle,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct call *c = arg;
	(void)handle;
	CHECK(tr_reactor_in_owner_context() && !atomic_load(&c->terminals));
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(view->header.decoded_size == BODY);
		for (unsigned i = 0; i < BODY; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7 + 3));
		atomic_fetch_add(&c->messages, 1);
	} else if (view->header.kind == TR_RPC_RECORD_FINAL_STATUS) {
		CHECK(atomic_fetch_add(&c->finals, 1) == 0);
	} else {
		CHECK(view->header.kind == TR_RPC_RECORD_INITIAL_METADATA);
	}
}
static void client_writable(struct tr_rpc_record_client_handle handle,
                            void *arg)
{
	struct call *c = arg;
	(void)handle;
	if (atomic_exchange(&c->fixture->drain_in_writable, false)) {
		CHECK(tr_client_begin_drain(c->fixture->client) == TR_OK);
		atomic_fetch_add(&c->fixture->writable_drains, 1);
	}
	atomic_fetch_add(&c->writable, 1);
}
static void client_terminal(struct tr_rpc_record_client_handle handle,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct call *c = arg;
	(void)handle;
	CHECK(tr_reactor_in_owner_context() &&
	      delivery == TR_RPC_RECORD_ON_OWNER);
	CHECK(tr_client_wait_drained(c->fixture->client, 1) == TR_ERR_STATE);
	CHECK(tr_server_drain(c->fixture->server, 1) == TR_ERR_STATE);
	c->status = status;
	CHECK(atomic_fetch_add(&c->terminals, 1) == 0);
	if (atomic_load(&c->fixture->block_client_terminal))
		gate_wait(&c->fixture->client_terminal_gate);
	atomic_fetch_add(&c->terminal_returns, 1);
}
static void call_start(struct call *c, struct fixture *f)
{
	*c = (struct call){.fixture = f};
	const struct tr_rpc_record_client_callbacks callbacks = {
	        client_received, client_writable, client_terminal, c};
	CHECK(tr_client_record_call_start(f->client, 17, 1, NULL, &callbacks,
	                                  &c->handle) == TR_OK);
}
struct body {
	struct tr_buffer buffer;
	struct fixture *fixture;
	uint8_t data[BODY];
};
static void body_release(struct tr_buffer *buffer)
{
	struct body *body = (struct body *)buffer;
	atomic_fetch_add(&body->fixture->bodies_released, 1);
	free(body);
}
static void call_send_and_half_close(struct call *c)
{
	struct body *body = malloc(sizeof(*body));
	CHECK(body);
	body->fixture = c->fixture;
	body->buffer = (struct tr_buffer){.data = body->data,
	                                  .capacity = BODY,
	                                  .len = BODY,
	                                  .release_cb = body_release};
	for (unsigned i = 0; i < BODY; ++i)
		body->data[i] = (uint8_t)(i * 7 + 3);
	atomic_fetch_add(&c->fixture->bodies, 1);
	struct tr_buffer *buffer = &body->buffer;
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_send(c->handle, &buffer)) == TR_AGAIN) {
		CHECK(buffer == &body->buffer && now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK && !buffer);
	while ((ret = tr_client_record_half_close(c->handle)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	/* 本地发送接受不证明服务端收齐；必须排除迟到 HALF_CLOSE 推动测试。 */
	wait_count(&c->fixture->received[TR_RPC_RECORD_MESSAGE], 1);
	wait_count(&c->fixture->received[TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(tr_reactor_call(owner(c->fixture, true), barrier, NULL) == TR_OK);
}
static void *drain_thread(void *arg)
{
	struct drain_wait *wait = arg;
	current_wait = wait;
	wait->result = wait->server
	                       ? tr_server_drain(wait->fixture->server, WAIT_MS)
	                       : tr_client_wait_drained(wait->fixture->client,
	                                                WAIT_MS);
	current_wait = NULL;
	atomic_store(&wait->done, 1);
	return NULL;
}
static void drain_finish(struct drain_wait *wait, pthread_t thread)
{
	wait_count(&wait->done, 1);
	CHECK(!pthread_join(thread, NULL));
	CHECK(wait->result == TR_OK);
	CHECK(atomic_load(&wait->inits) == 1);
	CHECK(atomic_load(&wait->rpc_waits) > 0);
	CHECK(atomic_load(&wait->channel_waits) == 1);
}
static void check_waiting(struct drain_wait *wait)
{
	CHECK(!atomic_load(&wait->done));
	CHECK(!atomic_load(&wait->channel_waits));
}
static void wait_streams_zero(struct fixture *f)
{
	uint64_t end = now_ms() + WAIT_MS;
	while (tr_channel_active_streams(atomic_load(&f->client_channel)) ||
	       tr_channel_active_streams(atomic_load(&f->server_channel))) {
		CHECK(now_ms() < end);
		pause_once();
	}
}
static void wait_server_network_drained(struct fixture *f)
{
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_channel_wait_drained(atomic_load(&f->server_channel),
	                                      0)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
}
static void wait_completed(struct fixture *f)
{
	uint64_t end = now_ms() + WAIT_MS;
	struct tr_task_executor_stats stats;
	for (;;) {
		CHECK(tr_task_executor_get_stats(atomic_load(&f->executor),
		                                 &stats) == TR_OK);
		if (stats.completed == 1 && !stats.running && !stats.queued) {
			CHECK(stats.outstanding == 1 && !stats.delivering);
			return;
		}
		CHECK(now_ms() < end);
		pause_once();
	}
}
static void check_reclaimed(struct fixture *f, struct call *c)
{
	CHECK(tr_client_record_cancel(c->handle) == TR_ERR_STALE);
	CHECK(tr_client_record_half_close(c->handle) == TR_ERR_STALE);
	struct tr_task_executor_stats stats;
	CHECK(tr_task_executor_get_stats(atomic_load(&f->executor), &stats) ==
	      TR_OK);
	CHECK(!stats.outstanding && !stats.queued && !stats.running &&
	      !stats.completed && !stats.delivering);
	uint64_t end = now_ms() + WAIT_MS;
	while (atomic_load(&f->client_budget->current_bytes) !=
	               f->client_idle_bytes ||
	       atomic_load(&f->server_budget->current_bytes) !=
	               f->server_idle_bytes) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(atomic_load(&f->client_creates) == 1 &&
	      atomic_load(&f->server_creates) == 1);
}

static void test_server_task_completion_and_callback(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f);
	atomic_store(&f.block_work, true);
	call_start(&c, &f);
	wait_count(&c.writable, 1);
	call_send_and_half_close(&c);
	wait_count(&f.work_entered, 1);
	CHECK(tr_client_record_cancel(c.handle) == TR_OK);
	wait_count(&f.work_cancelled, 1);
	wait_count(&f.work_gate.entered, 1);
	wait_streams_zero(&f);
	CHECK(tr_server_drain(f.server, 0) == TR_AGAIN);
	/* 真正的 Channel 网络/TX 排空已经成功，不能替原任务承诺静默。 */
	wait_server_network_drained(&f);
	CHECK(tr_server_drain(f.server, 1) == TR_AGAIN);
	CHECK(!atomic_load(&f.server_terminals));
	atomic_store(&f.pause_done, true);
	atomic_store(&f.block_server_terminal, true);
	struct drain_wait wait = {.fixture = &f, .server = true};
	pthread_t thread;
	CHECK(!pthread_create(&thread, NULL, drain_thread, &wait));
	wait_count(&wait.rpc_waits, 1);
	check_waiting(&wait);
	gate_release(&f.work_gate);
	wait_completed(&f);
	CHECK(atomic_load(&f.work_exited) == 1);
	CHECK(!atomic_load(&f.server_terminals));
	check_waiting(&wait);
	atomic_store(&f.pause_done, false);
	wait_count(&f.server_terminal_gate.entered, 1);
	CHECK(!atomic_load(&f.server_terminal_returns));
	check_waiting(&wait);
	gate_release(&f.server_terminal_gate);
	drain_finish(&wait, thread);
	CHECK(atomic_load(&f.server_terminal_returns) == 1);
	CHECK(atomic_load(&c.terminals) == 1 &&
	      atomic_load(&c.terminal_returns) == 1 &&
	      c.status == TR_RPC_STATUS_CANCELLED);
	CHECK(!atomic_load(&c.messages) && !atomic_load(&c.finals));
	check_reclaimed(&f, &c);
	fixture_destroy(&f);
}

static int freeze_before_pending_open(void *arg)
{
	struct fixture *f = arg;
	gate_wait(&f->owner_gate);
	/* owner 暂停期间，Client 已接纳本地 STREAM_OPEN；在读取它前
	 * 冻结最后受理边界为 0，真实 GOAWAY 在客户端直接退役 OPENING。 */
	return tr_channel_begin_drain(atomic_load(&f->server_channel));
}
static void *freeze_owner_thread(void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_reactor_call(owner(f, true), freeze_before_pending_open, f) ==
	      TR_OK);
	return NULL;
}
static void test_client_opening_and_terminal(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f);
	atomic_store(&f.block_client_terminal, true);
	pthread_t owner_thread;
	CHECK(!pthread_create(&owner_thread, NULL, freeze_owner_thread, &f));
	wait_count(&f.owner_gate.entered, 1);
	call_start(&c, &f);
	CHECK(!atomic_load(&c.writable) && !atomic_load(&f.open_accepted));
	CHECK(tr_channel_active_streams(atomic_load(&f.client_channel)) == 1);
	CHECK(tr_client_begin_drain(f.client) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, 0) == TR_AGAIN);
	CHECK(tr_client_wait_drained(f.client, 1) == TR_AGAIN);
	struct drain_wait wait = {.fixture = &f};
	pthread_t wait_thread;
	CHECK(!pthread_create(&wait_thread, NULL, drain_thread, &wait));
	wait_count(&wait.rpc_waits, 1);
	gate_release(&f.owner_gate);
	CHECK(!pthread_join(owner_thread, NULL));
	wait_count(&f.client_terminal_gate.entered, 1);
	CHECK(tr_channel_active_streams(atomic_load(&f.client_channel)) == 0);
	CHECK(!atomic_load(&f.open_accepted) &&
	      !atomic_load(&f.received[TR_RPC_RECORD_OPEN]) &&
	      !atomic_load(&f.executed[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&c.terminal_returns));
	check_waiting(&wait);
	gate_release(&f.client_terminal_gate);
	drain_finish(&wait, wait_thread);
	CHECK(c.status == TR_RPC_STATUS_UNAVAILABLE &&
	      atomic_load(&c.terminals) == 1 &&
	      atomic_load(&c.terminal_returns) == 1);
	CHECK(tr_server_drain(f.server, WAIT_MS) == TR_OK);
	check_reclaimed(&f, &c);
	fixture_destroy(&f);
}

static void test_pending_record_open_keeps_progress(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f);
	atomic_store(&f.block_open, true);
	atomic_store(&f.drain_in_writable, true);
	call_start(&c, &f);
	wait_count(&f.open_attempts, 1);
	CHECK(!atomic_load(&f.open_accepted) && !atomic_load(&c.writable));
	CHECK(tr_client_begin_drain(f.client) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, 0) == TR_AGAIN);
	struct drain_wait wait = {.fixture = &f};
	pthread_t thread;
	CHECK(!pthread_create(&thread, NULL, drain_thread, &wait));
	wait_count(&wait.rpc_waits, 1);
	check_waiting(&wait);
	atomic_store(&f.block_open, false);
	wait_count(&c.writable, 1);
	CHECK(atomic_load(&f.writable_drains) == 1);
	call_send_and_half_close(&c);
	drain_finish(&wait, thread);
	CHECK(c.status == TR_RPC_STATUS_OK && atomic_load(&c.terminals) == 1 &&
	      atomic_load(&c.messages) == 1 && atomic_load(&c.finals) == 1);
	CHECK(atomic_load(&f.open_accepted) == 1 &&
	      atomic_load(&f.executed[TR_RPC_RECORD_OPEN]) == 1 &&
	      atomic_load(&f.executed[TR_RPC_RECORD_MESSAGE]) == 1 &&
	      atomic_load(&f.executed[TR_RPC_RECORD_HALF_CLOSE]) == 1);
	CHECK(tr_server_drain(f.server, WAIT_MS) == TR_OK);
	check_reclaimed(&f, &c);
	fixture_destroy(&f);
}

int main(void)
{
	test_server_task_completion_and_callback();
	puts("record drain task/completion/callback: ok");
	test_client_opening_and_terminal();
	puts("record drain opening/terminal: ok");
	test_pending_record_open_keeps_progress();
	puts("record drain pending OPEN/progress: ok");
	return 0;
}
