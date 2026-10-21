#define _POSIX_C_SOURCE 200809L
#include "../src/execution/worker_context.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/server_record_internal.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/facade_tuning_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)

#define CALLS 4U
#define BODY 4097U
#define WIRE_LIMIT 8192U
#define WAIT_MS 15000U
#define OBSERVED_ENDPOINTS 32U

/* 只在真实模块的边界观察资源身份或注入一次失败。正常分支始终调用
 * 原实现；服务端监听、接纳、调度和回收全部由 tr_server 自己驱动。 */
static _Atomic bool capture_start;
static _Atomic unsigned executor_creates, executor_destroys, worker_total;
static _Atomic unsigned task_capacity_total, legacy_creates, worker_attempts;
static _Atomic unsigned fail_worker_at, endpoint_creates, endpoint_fail_at;
static _Atomic unsigned endpoint_start_fail_at, endpoint_starts;
static _Atomic unsigned reactor_count, fatal_polls, detach_failures;
static _Atomic unsigned cancel_requests;
static _Atomic unsigned observation_count, stopped_reaps, fail_stopped_reap_at;
static _Atomic unsigned cleanup_dispatches;
static _Atomic unsigned executor_join_failures, workers_joined;
static _Atomic bool fail_poll, fail_handler_detach, fail_executor_join;
static _Atomic(struct tr_reactor *) reactors[4];
static _Atomic(struct tr_channel *) observed_channel;
static _Thread_local bool starting_tasks;
static _Thread_local bool joining_tasks;
static _Thread_local bool failing_endpoint_start;
static struct {
	_Atomic(struct tr_rpc_record_endpoint *) endpoint;
	_Atomic(struct tr_task_executor *) executor;
	_Atomic bool cleanup_entered;
} observations[OBSERVED_ENDPOINTS];

static unsigned observe_endpoint(struct tr_rpc_record_endpoint *endpoint)
{
	for (unsigned i = 0; i < atomic_load(&observation_count); ++i)
		if (atomic_load(&observations[i].endpoint) == endpoint)
			return i;
	CHECK(false);
	return 0;
}

int __real_tr_reactor_start(struct tr_reactor *);
int __wrap_tr_reactor_start(struct tr_reactor *r)
{
	int ret = __real_tr_reactor_start(r);
	if (ret == TR_OK && atomic_load(&capture_start)) {
		unsigned i = atomic_fetch_add(&reactor_count, 1);
		CHECK(i < 4);
		atomic_store(&reactors[i], r);
	}
	return ret;
}

int __real_tr_task_executor_create(const struct tr_task_executor_config *,
                                   struct tr_memory_budget *,
                                   struct tr_task_executor **);
int __wrap_tr_task_executor_create(const struct tr_task_executor_config *c,
                                   struct tr_memory_budget *b,
                                   struct tr_task_executor **out)
{
	int ret = __real_tr_task_executor_create(c, b, out);
	if (ret == TR_OK) {
		atomic_fetch_add(&executor_creates, 1);
		atomic_fetch_add(&worker_total, (unsigned)c->workers);
		atomic_fetch_add(&task_capacity_total, (unsigned)c->capacity);
	}
	return ret;
}

int __real_tr_task_executor_start(struct tr_task_executor *);
int __wrap_tr_task_executor_start(struct tr_task_executor *e)
{
	starting_tasks = true;
	int ret = __real_tr_task_executor_start(e);
	starting_tasks = false;
	return ret;
}

int __real_pthread_create(pthread_t *, const pthread_attr_t *,
                          void *(*)(void *), void *);
int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg)
{
	if (starting_tasks && atomic_fetch_add(&worker_attempts, 1) + 1 ==
	                              atomic_load(&fail_worker_at))
		return EAGAIN;
	return __real_pthread_create(thread, attr, fn, arg);
}

int __real_tr_task_executor_join(struct tr_task_executor *);
int __wrap_tr_task_executor_join(struct tr_task_executor *e)
{
	bool prior = joining_tasks;
	joining_tasks = true;
	int ret = __real_tr_task_executor_join(e);
	joining_tasks = prior;
	return ret;
}

int __real_pthread_join(pthread_t, void **);
int __wrap_pthread_join(pthread_t thread, void **result)
{
	/* 先观察 workers_alive 再调用 join 存在检查与使用之间的竞态：
	 * worker 可在两次加锁间退出，真实 join 成功而故障从未注入。
	 * 只在真实执行器已经准入 pthread_join 的边界失败；父 Reactor
	 * 和其他线程的 join 原样透传，不依赖工作线程退出的调度时机。 */
	if (joining_tasks && atomic_exchange(&fail_executor_join, false)) {
		atomic_fetch_add(&executor_join_failures, 1);
		return EINVAL;
	}
	int ret = __real_pthread_join(thread, result);
	if (joining_tasks && ret == 0)
		atomic_fetch_add(&workers_joined, 1);
	return ret;
}

int __real_tr_task_executor_destroy(struct tr_task_executor **);
int __wrap_tr_task_executor_destroy(struct tr_task_executor **e)
{
	int ret = __real_tr_task_executor_destroy(e);
	if (ret == TR_OK)
		atomic_fetch_add(&executor_destroys, 1);
	return ret;
}

int __real_tr_task_executor_cancel(struct tr_task_executor *,
                                   struct tr_task_handle);
int __wrap_tr_task_executor_cancel(struct tr_task_executor *e,
                                   struct tr_task_handle h)
{
	int ret = __real_tr_task_executor_cancel(e, h);
	if (ret == TR_OK)
		atomic_fetch_add(&cancel_requests, 1);
	return ret;
}

int __real_tr_task_executor_dispatch(struct tr_task_executor *, size_t,
                                     size_t *);
int __wrap_tr_task_executor_dispatch(struct tr_task_executor *e, size_t limit,
                                     size_t *delivered)
{
	if (!tr_reactor_in_owner_context()) {
		/* 共享完成队列可交付任一对端的结果。清理线程交付前，所有
		 * 仍存活的对端都必须已通过其公开 reap 边界进入清理域。 */
		for (unsigned i = 0; i < atomic_load(&observation_count); ++i)
			if (atomic_load(&observations[i].endpoint) &&
			    atomic_load(&observations[i].executor) == e)
				CHECK(atomic_load(
				        &observations[i].cleanup_entered));
		atomic_fetch_add(&cleanup_dispatches, 1);
	}
	return __real_tr_task_executor_dispatch(e, limit, delivered);
}

int __real_tr_rpc_executor_group_create(uint32_t, uint32_t, uint32_t,
                                        struct tr_rpc_executor_group **);
int __wrap_tr_rpc_executor_group_create(uint32_t n, uint32_t calls,
                                        uint32_t threads,
                                        struct tr_rpc_executor_group **out)
{
	atomic_fetch_add(&legacy_creates, 1);
	return __real_tr_rpc_executor_group_create(n, calls, threads, out);
}

int __real_tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *, struct tr_task_executor *,
        const struct tr_rpc_dispatch_method *, size_t,
        const struct tr_rpc_record_endpoint_config *,
        tr_rpc_endpoint_terminal_fn, void *, struct tr_memory_budget *,
        struct tr_rpc_record_endpoint **);
int __wrap_tr_rpc_record_endpoint_create_on_owner(
        struct tr_channel *c, struct tr_task_executor *e,
        const struct tr_rpc_dispatch_method *m, size_t n,
        const struct tr_rpc_record_endpoint_config *cfg,
        tr_rpc_endpoint_terminal_fn fn, void *arg, struct tr_memory_budget *b,
        struct tr_rpc_record_endpoint **out)
{
	if (atomic_fetch_add(&endpoint_creates, 1) + 1 ==
	    atomic_load(&endpoint_fail_at))
		return TR_ERR_NOMEM;
	int ret = __real_tr_rpc_record_endpoint_create_on_owner(
	        c, e, m, n, cfg, fn, arg, b, out);
	if (ret == TR_OK) {
		atomic_store(&observed_channel, c);
		unsigned i = atomic_fetch_add(&observation_count, 1);
		CHECK(i < OBSERVED_ENDPOINTS);
		atomic_store(&observations[i].executor, e);
		atomic_store(&observations[i].cleanup_entered, false);
		atomic_store(&observations[i].endpoint, *out);
	}
	return ret;
}

int __real_tr_rpc_record_endpoint_close_on_owner(
        struct tr_rpc_record_endpoint **);
int __wrap_tr_rpc_record_endpoint_close_on_owner(
        struct tr_rpc_record_endpoint **ep)
{
	unsigned i = observe_endpoint(*ep);
	int ret = __real_tr_rpc_record_endpoint_close_on_owner(ep);
	if (ret == TR_OK) {
		CHECK(!*ep);
		atomic_store(&observations[i].endpoint, NULL);
	}
	return ret;
}

int __real_tr_rpc_record_endpoint_reap_stopped(
        struct tr_rpc_record_endpoint **);
int __wrap_tr_rpc_record_endpoint_reap_stopped(
        struct tr_rpc_record_endpoint **ep)
{
	unsigned i = observe_endpoint(*ep);
	if (atomic_fetch_add(&stopped_reaps, 1) + 1 ==
	    atomic_load(&fail_stopped_reap_at))
		return TR_ERR_SYS;
	int ret = __real_tr_rpc_record_endpoint_reap_stopped(ep);
	if (ret == TR_OK || ret == TR_AGAIN)
		atomic_store(&observations[i].cleanup_entered, true);
	if (ret == TR_OK) {
		CHECK(!*ep);
		atomic_store(&observations[i].endpoint, NULL);
	}
	return ret;
}

int __real_tr_rpc_record_endpoint_start_on_owner(
        struct tr_rpc_record_endpoint *);
int __wrap_tr_rpc_record_endpoint_start_on_owner(
        struct tr_rpc_record_endpoint *e)
{
	failing_endpoint_start = atomic_fetch_add(&endpoint_starts, 1) + 1 ==
	                         atomic_load(&endpoint_start_fail_at);
	int ret = __real_tr_rpc_record_endpoint_start_on_owner(e);
	failing_endpoint_start = false;
	return ret;
}

int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle, uint64_t);
int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t deadline)
{
	/* 在真实 endpoint start 已注册定时器之后失败，回滚必须负责
	 * 撤销已经发布的回调资源，不能只处理尚未开始的构造失败。 */
	if (failing_endpoint_start)
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_arm(timer, deadline);
}

int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
int __wrap_tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb changed, void *arg)
{
	if (c == atomic_load(&observed_channel) && !data && !event &&
	    atomic_exchange(&fail_handler_detach, false)) {
		atomic_fetch_add(&detach_failures, 1);
		return TR_ERR_SYS;
	}
	return __real_tr_channel_set_handler(c, data, event, changed, arg);
}

int __real_epoll_wait(int, struct epoll_event *, int, int);
int __wrap_epoll_wait(int fd, struct epoll_event *events, int n, int timeout)
{
	struct tr_reactor *r = atomic_load(&reactors[0]);
	if (r && tr_reactor_in_owner_context_for(r) &&
	    atomic_exchange(&fail_poll, false)) {
		atomic_fetch_add(&fatal_polls, 1);
		errno = EIO;
		return -1;
	}
	return __real_epoll_wait(fd, events, n, timeout);
}

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void pause_once(void)
{
	const struct timespec ts = {0, 1000000};
	(void)nanosleep(&ts, NULL);
}

static void wait_count(_Atomic unsigned *value, unsigned target)
{
	uint64_t end = now_ms() + WAIT_MS;
	while (atomic_load(value) < target) {
		CHECK(now_ms() < end);
		pause_once();
	}
}

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned entered;
	bool released;
};

static void gate_init(struct gate *g)
{
	pthread_condattr_t attr;
	*g = (struct gate){0};
	CHECK(pthread_mutex_init(&g->lock, NULL) == 0);
	CHECK(pthread_condattr_init(&attr) == 0);
	CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	CHECK(pthread_cond_init(&g->cond, &attr) == 0);
	CHECK(pthread_condattr_destroy(&attr) == 0);
}

static void gate_wait(struct gate *g, unsigned entered, bool worker)
{
	struct timespec end;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += WAIT_MS / 1000;
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	if (worker) {
		++g->entered;
		CHECK(pthread_cond_broadcast(&g->cond) == 0);
	}
	while (worker ? !g->released : g->entered < entered)
		CHECK(pthread_cond_timedwait(&g->cond, &g->lock, &end) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

static void gate_release(struct gate *g)
{
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	g->released = true;
	CHECK(pthread_cond_broadcast(&g->cond) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

struct fixture {
	struct tr_server *server;
	struct tr_server_config config;
	struct tr_facade_tuning tuning;
	struct tr_server_record_config record;
	struct tr_rpc_dispatch_method methods[2];
	struct gate gate;
	_Atomic bool block;
	_Atomic unsigned calls, messages, self_checks, cancelled;
	uint16_t port;
};

static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct fixture *f = arg;
	(void)id;
	CHECK(tr_execution_in_worker_context() &&
	      !tr_reactor_in_owner_context());
	if (view->header.kind == TR_RPC_RECORD_OPEN)
		atomic_fetch_add(&f->calls, 1);
	if (view->header.kind != TR_RPC_RECORD_MESSAGE)
		return TR_RPC_STATUS_OK;
	CHECK(tr_server_destroy(f->server) == TR_ERR_STATE);
	CHECK(tr_server_drain(f->server, 1) == TR_ERR_STATE);
	CHECK(tr_server_start(f->server) == TR_ERR_STATE);
	atomic_fetch_add(&f->self_checks, 1);
	if (atomic_load(&f->block))
		gate_wait(&f->gate, 0, true);
	if (tr_task_cancel_requested(cancel))
		atomic_fetch_add(&f->cancelled, 1);
	/* 即使取消获胜也生成迟到成功，检验组合根不会再发出这些结果。 */
	CHECK(tr_rpc_record_reply_append(reply, view->body,
	                                 view->header.decoded_size) == TR_OK);
	atomic_fetch_add(&f->messages, 1);
	return TR_RPC_STATUS_OK;
}

static void reset_observation(void)
{
	atomic_store(&capture_start, false);
	atomic_store(&executor_creates, 0);
	atomic_store(&executor_destroys, 0);
	atomic_store(&worker_total, 0);
	atomic_store(&task_capacity_total, 0);
	atomic_store(&legacy_creates, 0);
	atomic_store(&worker_attempts, 0);
	atomic_store(&fail_worker_at, 0);
	atomic_store(&endpoint_creates, 0);
	atomic_store(&endpoint_starts, 0);
	atomic_store(&endpoint_fail_at, 0);
	atomic_store(&endpoint_start_fail_at, 0);
	atomic_store(&reactor_count, 0);
	atomic_store(&fatal_polls, 0);
	atomic_store(&detach_failures, 0);
	atomic_store(&cancel_requests, 0);
	atomic_store(&observation_count, 0);
	atomic_store(&stopped_reaps, 0);
	atomic_store(&fail_stopped_reap_at, 0);
	atomic_store(&cleanup_dispatches, 0);
	atomic_store(&executor_join_failures, 0);
	atomic_store(&workers_joined, 0);
	atomic_store(&fail_poll, false);
	atomic_store(&fail_handler_detach, false);
	atomic_store(&fail_executor_join, false);
	atomic_store(&observed_channel, NULL);
	for (unsigned i = 0; i < 4; ++i)
		atomic_store(&reactors[i], NULL);
	for (unsigned i = 0; i < OBSERVED_ENDPOINTS; ++i) {
		atomic_store(&observations[i].endpoint, NULL);
		atomic_store(&observations[i].executor, NULL);
		atomic_store(&observations[i].cleanup_entered, false);
	}
}

static void fixture_init(struct fixture *f, unsigned shards, unsigned peers)
{
	*f = (struct fixture){0};
	reset_observation();
	gate_init(&f->gate);
	atomic_init(&f->block, false);
	atomic_init(&f->calls, 0);
	atomic_init(&f->messages, 0);
	atomic_init(&f->self_checks, 0);
	atomic_init(&f->cancelled, 0);
	tr_server_config_init(&f->config);
	f->config.shard_count = shards;
	f->config.max_peers = peers;
	f->config.limits.max_streams = CALLS;
	f->config.limits.max_calls = CALLS;
	f->config.limits.max_methods = 2;
	f->config.limits.max_frame_payload_bytes = 256;
	f->config.limits.max_message_bytes = WIRE_LIMIT;
	f->config.limits.initial_window_bytes = WIRE_LIMIT * 4;
	f->config.limits.window_update_threshold_bytes = 1;
	f->config.keepalive_interval_ms = 0;
	f->config.handshake_timeout_ms = 2000;
	tr_facade_tuning_init(&f->tuning);
	f->tuning.command_capacity = 128;
	f->tuning.tx_item_capacity = 128;
	f->tuning.control_tx_item_capacity = 64;
	f->tuning.rx_buffer_count = 64;
	f->tuning.reassembly_pool_count = 16;
	f->record = (struct tr_server_record_config){
	        .endpoint = {.dispatch = {CALLS, 8, 2, WIRE_LIMIT},
	                     .completion_limit = 2,
	                     .progress_limit = 2,
	                     .progress_period_ns = 1000000,
	                     .final_wait_ns = 1000000000},
	        .tasks = {shards == 1 ? 2 : 3, 16},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	for (unsigned i = 0; i < 2; ++i) {
		f->methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {17, i + 1, i ? TR_RPC_MANY : TR_RPC_ONE,
		                       i ? TR_RPC_MANY : TR_RPC_ONE,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_CONTROL, BODY, BODY},
		        .work = business,
		        .arg = f};
	}
}

static void fixture_create(struct fixture *f)
{
	/* 构造后修改原数组，真实新连接仍须使用已冻结的描述和工作回调。 */
	struct tr_rpc_dispatch_method copied[2];
	memcpy(copied, f->methods, sizeof(copied));
	CHECK(tr_server_create_record(&f->config, &f->tuning, &f->record,
	                              copied, 2, &f->server) == TR_OK);
	memset(copied, 0, sizeof(copied));
	CHECK(atomic_load(&legacy_creates) == 0);
	CHECK(tr_server_listen(f->server, "127.0.0.1", 0, &f->port) == TR_OK);
}

static int fixture_start(struct fixture *f)
{
	atomic_store(&capture_start, true);
	int ret = tr_server_start(f->server);
	atomic_store(&capture_start, false);
	return ret;
}

static int destroy_until_stable(struct fixture *f)
{
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	if (ret == TR_OK)
		f->server = NULL;
	return ret;
}

static void fixture_destroy(struct fixture *f)
{
	if (f->server)
		CHECK(destroy_until_stable(f) == TR_OK);
	CHECK(atomic_load(&executor_creates) ==
	      atomic_load(&executor_destroys));
	CHECK(atomic_load(&legacy_creates) == 0);
	for (unsigned i = 0; i < 4; ++i)
		atomic_store(&reactors[i], NULL);
	atomic_store(&observed_channel, NULL);
	CHECK(pthread_cond_destroy(&f->gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&f->gate.lock) == 0);
}

struct peer {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_conn_handle connection;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_rpc_record_stream *calls[CALLS];
	struct tr_stream_handle streams[CALLS];
	struct tr_rpc_method_desc methods[CALLS];
	_Atomic unsigned opened, terminals[CALLS], messages[CALLS];
	uint32_t statuses[CALLS];
	size_t bytes[CALLS];
};

static unsigned peer_slot(struct peer *p, struct tr_rpc_record_stream *call)
{
	for (unsigned i = 0; i < CALLS; ++i)
		if (p->calls[i] == call)
			return i;
	CHECK(0);
	return 0;
}

static void received(struct tr_rpc_record_stream *call,
                     const struct tr_rpc_record_view *view, void *arg)
{
	struct peer *p = arg;
	unsigned i = peer_slot(p, call);
	if (view->header.kind != TR_RPC_RECORD_MESSAGE)
		return;
	for (size_t j = 0; j < view->header.decoded_size; ++j)
		CHECK(view->body[j] == (uint8_t)(j * 5 + 7));
	p->bytes[i] += view->header.decoded_size;
	atomic_fetch_add(&p->messages[i], 1);
}

static void terminal(struct tr_rpc_record_stream *call,
                     enum tr_rpc_status status,
                     enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = peer_slot(p, call);
	(void)delivery;
	p->statuses[i] = (uint32_t)status;
	CHECK(atomic_fetch_add(&p->terminals[i], 1) == 0);
}

static enum tr_stream_data_disposition on_data(struct tr_stream_handle stream,
                                               uint64_t id, struct tr_buffer *b,
                                               void *arg)
{
	struct peer *p = arg;
	(void)id;
	CHECK(stream.slot < CALLS && p->calls[stream.slot]);
	CHECK(tr_rpc_record_stream_receive_on_owner(
	              p->calls[stream.slot], stream, b->data, b->len) == TR_OK);
	return TR_STREAM_DATA_RELEASE;
}

static void on_event(struct tr_stream_handle stream, enum tr_stream_event event,
                     int status, void *arg)
{
	struct peer *p = arg;
	CHECK(stream.slot < CALLS);
	if (event == TR_STREAM_EVENT_OPENED) {
		CHECK(p->calls[stream.slot]);
		struct tr_rpc_record_header h = {
		        .kind = TR_RPC_RECORD_OPEN,
		        .service_id = p->methods[stream.slot].service_id,
		        .method_id = p->methods[stream.slot].method_id};
		/* ACK 回调立即建立本地 OPEN 语义，覆盖同轮早到方法拒绝。 */
		CHECK(tr_rpc_record_stream_send_on_owner(
		              p->calls[stream.slot], &h, NULL, NULL) == TR_OK);
		atomic_fetch_add(&p->opened, 1);
	} else if (p->calls[stream.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[stream.slot],
		                                          stream, event,
		                                          status) == TR_OK);
	}
}

struct peer_adopt_request {
	struct peer *peer;
	const struct tr_channel_config *config;
	int fd;
};

static int peer_adopt_on_owner(void *arg)
{
	struct peer_adopt_request *r = arg;
	struct peer *p = r->peer;
	int ret;

	/* 接纳失败时服务端可以立即关闭连接。接管与 Channel 发布必须在
	 * 同一所有者轮次完成，使关闭事件在处理函数安装后才被分派。 */
	ret = tr_reactor_adopt_fd(p->reactor, r->fd, &p->connection);
	if (ret != TR_OK)
		return ret;
	r->fd = -1;
	return tr_channel_create(r->config, p->connection, p->connection,
	                         on_data, on_event, NULL, p, &p->channel);
}

static void peer_connect(struct peer *p, uint16_t port, bool ready)
{
	struct tr_reactor_config rc = {.max_connections = 2,
	                               .command_capacity = 32,
	                               .tx_item_capacity = 32,
	                               .control_tx_item_capacity = 32,
	                               .rx_buffer_count = 16,
	                               .rx_buffer_size = 256,
	                               .max_payload_len = 256};
	struct tr_channel_config cc = {.role = TR_CHANNEL_CLIENT,
	                               .mode = TR_CHANNEL_SHARED_CONNECTION,
	                               .feature_bits =
	                                       TR_CHANNEL_FEATURE_STREAM_RESET,
	                               .max_streams = CALLS,
	                               .initial_window_bytes = WIRE_LIMIT * 4,
	                               .window_update_threshold_bytes = 1,
	                               .max_message_bytes = WIRE_LIMIT};
	int fd = -1;
	*p = (struct peer){0};
	atomic_init(&p->opened, 0);
	for (unsigned i = 0; i < CALLS; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->messages[i], 0);
	}
	tr_memory_budget_init(&p->budget, 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CALLS, WIRE_LIMIT) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	int ret = tr_tcp_connect_ipv4("127.0.0.1", port, &fd);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	if (ret == TR_IN_PROGRESS) {
		struct pollfd pollfd = {fd, POLLOUT, 0};
		CHECK(poll(&pollfd, 1, WAIT_MS) == 1);
		CHECK(tr_tcp_finish_connect(fd) == TR_OK);
	}
	struct peer_adopt_request request = {p, &cc, fd};
	CHECK(tr_reactor_call(p->reactor, peer_adopt_on_owner, &request) ==
	      TR_OK);
	CHECK(request.fd == -1);
	if (ready)
		CHECK(tr_channel_wait_ready(p->channel, TR_LANE_CONTROL,
		                            WAIT_MS) == TR_OK);
}

static void peer_destroy(struct peer *p)
{
	CHECK(tr_reactor_stop(p->reactor) == TR_OK);
	for (unsigned i = 0; i < CALLS; ++i)
		if (p->calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(&p->calls[i]) ==
			      TR_OK);
	CHECK(tr_channel_destroy(p->channel) == TR_OK);
	CHECK(tr_reactor_destroy(p->reactor) == TR_OK);
	CHECK(tr_buffer_pool_destroy(&p->pool) == TR_OK);
	CHECK(atomic_load(&p->budget.current_bytes) == 0);
}

struct open_request {
	struct peer *peer;
	struct tr_rpc_method_desc method;
	struct tr_stream_handle stream;
};

static int open_on_owner(void *arg)
{
	struct open_request *r = arg;
	struct peer *p = r->peer;
	struct tr_rpc_record_stream_config config = {
	        TR_RPC_RECORD_REQUEST_DIRECTION, r->method};
	struct tr_rpc_record_stream_callbacks callbacks = {received, terminal,
	                                                   p};
	int ret = tr_stream_open(p->channel, TR_LANE_CONTROL, &r->stream);
	if (ret != TR_OK)
		return ret;
	unsigned i = r->stream.slot;
	CHECK(i < CALLS);
	if (p->calls[i])
		CHECK(tr_rpc_record_stream_destroy_on_owner(&p->calls[i]) ==
		      TR_OK);
	atomic_store(&p->terminals[i], 0);
	atomic_store(&p->messages[i], 0);
	p->bytes[i] = 0;
	p->methods[i] = r->method;
	p->streams[i] = r->stream;
	return tr_rpc_record_stream_create_on_owner(
	        r->stream, &config, &callbacks, &p->budget, &p->calls[i]);
}

static unsigned peer_open(struct peer *p, struct tr_rpc_method_desc method)
{
	struct open_request request = {.peer = p, .method = method};
	unsigned before = atomic_load(&p->opened);
	CHECK(tr_reactor_call(p->reactor, open_on_owner, &request) == TR_OK);
	wait_count(&p->opened, before + 1);
	return request.stream.slot;
}

struct body {
	struct tr_buffer buffer;
	uint8_t bytes[];
};

static void release_body(struct tr_buffer *b)
{
	free(b);
}

struct send_request {
	struct peer *peer;
	unsigned slot;
	struct tr_rpc_record_header header;
	struct tr_buffer *body;
};

static int send_on_owner(void *arg)
{
	struct send_request *r = arg;
	return tr_rpc_record_stream_send_on_owner(r->peer->calls[r->slot],
	                                          &r->header, NULL, &r->body);
}

static void peer_send(struct peer *p, unsigned slot,
                      enum tr_rpc_record_kind kind, unsigned size)
{
	struct send_request r = {
	        .peer = p, .slot = slot, .header = {.kind = kind}};
	if (kind == TR_RPC_RECORD_MESSAGE) {
		struct body *body = malloc(sizeof(*body) + size);
		CHECK(body);
		body->buffer = (struct tr_buffer){.data = body->bytes,
		                                  .len = size,
		                                  .capacity = size,
		                                  .release_cb = release_body};
		for (unsigned i = 0; i < size; ++i)
			body->bytes[i] = (uint8_t)(i * 5 + 7);
		r.body = &body->buffer;
		r.header.codec_id = TR_RPC_CODEC_RAW;
		r.header.encoded_size = r.header.decoded_size = size;
	}
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_reactor_call(p->reactor, send_on_owner, &r)) ==
	       TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK && !r.body);
}

static int close_on_owner(void *arg)
{
	struct tr_stream_handle *stream = arg;
	return tr_stream_close(*stream);
}

static void peer_finish(struct peer *p, unsigned slot,
                        enum tr_rpc_status status, unsigned count, size_t size)
{
	wait_count(&p->terminals[slot], 1);
	CHECK(p->statuses[slot] == (uint32_t)status);
	CHECK(atomic_load(&p->messages[slot]) == count);
	CHECK(p->bytes[slot] == size);
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_reactor_call(p->reactor, close_on_owner,
	                              &p->streams[slot])) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK || ret == TR_ERR_CLOSED || ret == TR_ERR_STALE);
}

static void echo(struct fixture *f, struct peer *p, unsigned size)
{
	unsigned i = peer_open(p, f->methods[0].descriptor);
	peer_send(p, i, TR_RPC_RECORD_MESSAGE, size);
	peer_send(p, i, TR_RPC_RECORD_HALF_CLOSE, 0);
	peer_finish(p, i, TR_RPC_STATUS_OK, 1, size);
}

static void wait_peers(struct fixture *f, unsigned current, unsigned reaped)
{
	uint64_t end = now_ms() + WAIT_MS;
	for (;;) {
		struct tr_server_stats stats;
		CHECK(tr_server_get_stats(f->server, &stats) == TR_OK);
		CHECK(stats.peers_current <= f->config.max_peers);
		if (stats.peers_current == current &&
		    stats.peers_reaped_total >= reaped)
			return;
		CHECK(now_ms() < end);
		pause_once();
	}
}

static void test_shapes_and_reuse(void)
{
	struct fixture f;
	fixture_init(&f, 1, 1);
	fixture_create(&f);
	struct tr_rpc_semantic_stats stats, sentinel;
	memset(&stats, 0xa5, sizeof(stats));
	memset(&sentinel, 0, sizeof(sentinel));
	CHECK(tr_server_get_rpc_semantic_stats(f.server, &stats) == TR_OK);
	CHECK(!memcmp(&stats, &sentinel, sizeof(stats)));
	CHECK(fixture_start(&f) == TR_OK);
	for (unsigned round = 0; round < 6; ++round) {
		struct peer p;
		peer_connect(&p, f.port, true);
		if (round == 0) {
			struct tr_rpc_method_desc unknown =
			        f.methods[0].descriptor;
			unknown.method_id = 99;
			unsigned i = peer_open(&p, unknown);
			peer_finish(&p, i, TR_RPC_STATUS_UNIMPLEMENTED, 0, 0);
			CHECK(atomic_load(&f.calls) == 0);
			i = peer_open(&p, f.methods[1].descriptor);
			peer_send(&p, i, TR_RPC_RECORD_HALF_CLOSE, 0);
			peer_finish(&p, i, TR_RPC_STATUS_OK, 0, 0);
			echo(&f, &p, 0);
		}
		echo(&f, &p, BODY);
		peer_destroy(&p);
		wait_peers(&f, 0, round + 1);
		CHECK(tr_server_get_rpc_semantic_stats(f.server, &stats) ==
		      TR_OK);
		CHECK(stats.calls_started == round + 3);
		CHECK(stats.calls_finished == round + 3);
		CHECK(stats.calls_inflight == 0);
		for (unsigned status = 0; status < TR_RPC_STATUS_COUNT;
		     ++status)
			CHECK(stats.final_status[status] ==
			      (status == TR_RPC_STATUS_OK ? round + 3 : 0));
	}
	CHECK(atomic_load(&executor_creates) == 1);
	CHECK(atomic_load(&worker_total) == 2);
	CHECK(atomic_load(&f.self_checks) == 7);
	fixture_destroy(&f);
}

static void test_shared_executor(void)
{
	struct fixture f;
	struct peer peers[2];
	unsigned slots[2];
	fixture_init(&f, 1, 2);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	atomic_store(&f.block, true);
	for (unsigned j = 0; j < 2; ++j) {
		peer_connect(&peers[j], f.port, true);
		slots[j] = peer_open(&peers[j], f.methods[0].descriptor);
		peer_send(&peers[j], slots[j], TR_RPC_RECORD_MESSAGE, 32);
		peer_send(&peers[j], slots[j], TR_RPC_RECORD_HALF_CLOSE, 0);
	}
	gate_wait(&f.gate, 2, false);
	CHECK(atomic_load(&executor_creates) == 1);
	CHECK(atomic_load(&worker_total) == 2);
	CHECK(atomic_load(&task_capacity_total) == f.record.tasks.capacity);
	gate_release(&f.gate);
	for (unsigned j = 0; j < 2; ++j)
		peer_finish(&peers[j], slots[j], TR_RPC_STATUS_OK, 1, 32);
	CHECK(tr_server_drain(f.server, WAIT_MS) == TR_OK);
	for (unsigned j = 0; j < 2; ++j)
		peer_destroy(&peers[j]);
	fixture_destroy(&f);
}

static void test_disconnected_work(void)
{
	struct fixture f;
	struct peer p;
	fixture_init(&f, 1, 1);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	peer_connect(&p, f.port, true);
	atomic_store(&f.block, true);
	unsigned i = peer_open(&p, f.methods[0].descriptor);
	peer_send(&p, i, TR_RPC_RECORD_MESSAGE, 16);
	gate_wait(&f.gate, 1, false);
	peer_destroy(&p);
	/* 已断开但业务尚未静默的对端仍占其有界槽位。 */
	wait_count(&cancel_requests, 1);
	wait_peers(&f, 1, 0);
	gate_release(&f.gate);
	wait_peers(&f, 0, 1);
	CHECK(atomic_load(&f.cancelled) == 1);
	atomic_store(&f.block, false);
	peer_connect(&p, f.port, true);
	echo(&f, &p, 40);
	peer_destroy(&p);
	wait_peers(&f, 0, 2);
	fixture_destroy(&f);
}

static void test_stopped_work(bool fatal, bool cleanup_failure,
                              bool join_failure)
{
	struct fixture f;
	struct peer peers[2];
	unsigned slots[2];
	fixture_init(&f, 1, 2);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	atomic_store(&f.block, true);
	for (unsigned j = 0; j < 2; ++j) {
		peer_connect(&peers[j], f.port, true);
		slots[j] = peer_open(&peers[j], f.methods[0].descriptor);
		peer_send(&peers[j], slots[j], TR_RPC_RECORD_MESSAGE, 64);
		peer_send(&peers[j], slots[j], TR_RPC_RECORD_HALF_CLOSE, 0);
	}
	gate_wait(&f.gate, 2, false);
	if (fatal) {
		atomic_store(&fail_poll, true);
		wait_count(&fatal_polls, 1);
	}
	if (cleanup_failure) {
		atomic_store(&fail_stopped_reap_at, 2);
		CHECK(tr_server_destroy(f.server) == TR_ERR_SYS);
		CHECK(atomic_load(&cleanup_dispatches) == 0);
	}
	CHECK(tr_server_destroy(f.server) == TR_AGAIN);
	CHECK(atomic_load(&executor_destroys) == 0);
	gate_release(&f.gate);
	if (join_failure) {
		/* 两条请求已受理、Server 已停止接受；在取消工作返回后
		 * 精确拒绝第一次真实 pthread_join，不能释放执行器/Endpoint。
		 */
		atomic_store(&fail_executor_join, true);
		CHECK(destroy_until_stable(&f) == TR_ERR_SYS);
		CHECK(f.server && atomic_load(&executor_destroys) == 0);
		CHECK(atomic_load(&executor_join_failures) == 1);
		CHECK(!atomic_load(&fail_executor_join));
	}
	CHECK(destroy_until_stable(&f) == TR_OK);
	CHECK(atomic_load(&cleanup_dispatches) > 0);
	for (unsigned j = 0; j < 2; ++j) {
		wait_count(&peers[j].terminals[slots[j]], 1);
		CHECK(peers[j].statuses[slots[j]] != TR_RPC_STATUS_OK);
		CHECK(atomic_load(&peers[j].messages[slots[j]]) == 0);
		peer_destroy(&peers[j]);
	}
	CHECK(atomic_load(&f.cancelled) == 2);
	if (join_failure) {
		CHECK(atomic_load(&executor_join_failures) == 1);
		CHECK(atomic_load(&workers_joined) ==
		      atomic_load(&worker_total));
	}
	fixture_destroy(&f);
}

static void test_multishard_and_start_failure(unsigned fail_at)
{
	struct fixture f;
	fixture_init(&f, 2, 4);
	fixture_create(&f);
	atomic_store(&fail_worker_at, fail_at);
	int ret = fixture_start(&f);
	if (fail_at) {
		CHECK(ret == TR_ERR_SYS || ret == TR_AGAIN);
		CHECK(atomic_load(&worker_attempts) == fail_at);
	} else {
		CHECK(ret == TR_OK);
		CHECK(atomic_load(&executor_creates) == 2);
		CHECK(atomic_load(&worker_total) == 3);
		CHECK(atomic_load(&task_capacity_total) == 16);
		for (unsigned j = 0; j < 4; ++j) {
			struct peer p;
			peer_connect(&p, f.port, true);
			echo(&f, &p, 40);
			peer_destroy(&p);
			wait_peers(&f, 0, j + 1);
		}
	}
	fixture_destroy(&f);
}

static void test_adopt_failure(bool start)
{
	struct fixture f;
	struct peer p;
	fixture_init(&f, 1, 1);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	atomic_store(start ? &endpoint_start_fail_at : &endpoint_fail_at, 1);
	peer_connect(&p, f.port, false);
	wait_count(start ? &endpoint_starts : &endpoint_creates, 1);
	/* 客户端仍存活，服务端必须自行回滚失败的接管并归还对端槽。 */
	wait_peers(&f, 0, 1);
	peer_destroy(&p);
	peer_connect(&p, f.port, true);
	echo(&f, &p, 48);
	peer_destroy(&p);
	wait_peers(&f, 0, 2);
	fixture_destroy(&f);
}

static void test_detach_retry(bool stopping)
{
	struct fixture f;
	struct peer p;
	fixture_init(&f, 1, 1);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	peer_connect(&p, f.port, true);
	echo(&f, &p, 48);
	atomic_store(&fail_handler_detach, true);
	if (stopping) {
		CHECK(destroy_until_stable(&f) == TR_ERR_SYS);
		CHECK(f.server && atomic_load(&detach_failures) == 1);
		CHECK(destroy_until_stable(&f) == TR_OK);
		peer_destroy(&p);
	} else {
		peer_destroy(&p);
		wait_count(&detach_failures, 1);
		/* 仅靠服务端既有事件源及重试时钟继续收敛，不由测试手工pump。 */
		wait_peers(&f, 0, 1);
		peer_connect(&p, f.port, true);
		echo(&f, &p, 48);
		peer_destroy(&p);
		wait_peers(&f, 0, 2);
	}
	fixture_destroy(&f);
}

static void test_join_retry(void)
{
	struct fixture f;
	fixture_init(&f, 1, 1);
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	atomic_store(&fail_executor_join, true);
	CHECK(destroy_until_stable(&f) == TR_ERR_SYS);
	CHECK(f.server && atomic_load(&executor_destroys) == 0);
	CHECK(!atomic_load(&fail_executor_join));
	CHECK(atomic_load(&executor_join_failures) == 1);
	CHECK(atomic_load(&workers_joined) == 0);
	fixture_destroy(&f);
	CHECK(atomic_load(&executor_join_failures) == 1);
	CHECK(atomic_load(&workers_joined) == atomic_load(&worker_total));
}

static void test_configuration(void)
{
	struct fixture f;
	fixture_init(&f, 1, 1);
	struct tr_server_record_config bad = f.record;
	bad.memory_budget_bytes = 0;
	CHECK(tr_server_create_record(&f.config, &f.tuning, &bad, f.methods, 2,
	                              &f.server) == TR_ERR_INVALID);
	CHECK(!f.server);
	struct tr_server_config bad_config = f.config;
	bad_config.limits.max_message_bytes =
	        BODY + TR_RPC_RECORD_HEADER_SIZE - 1;
	CHECK(tr_server_create_record(&bad_config, &f.tuning, &f.record,
	                              f.methods, 2,
	                              &f.server) == TR_ERR_INVALID);
	CHECK(!f.server);
	bad_config = f.config;
	bad_config.limits.initial_window_bytes =
	        BODY + TR_RPC_RECORD_HEADER_SIZE - 1;
	CHECK(tr_server_create_record(&bad_config, &f.tuning, &f.record,
	                              f.methods, 2,
	                              &f.server) == TR_ERR_INVALID);
	CHECK(!f.server);
	bad_config = f.config;
	bad_config.interceptor.arg = &f;
	CHECK(tr_server_create_record(&bad_config, &f.tuning, &f.record,
	                              f.methods, 2,
	                              &f.server) == TR_ERR_UNSUPPORTED);
	CHECK(!f.server);
	bad = f.record;
	bad.tasks.capacity = 1;
	CHECK(tr_server_create_record(&f.config, &f.tuning, &bad, f.methods, 2,
	                              &f.server) == TR_ERR_INVALID);
	CHECK(!f.server);
	struct tr_rpc_dispatch_method duplicate[2] = {f.methods[0],
	                                              f.methods[0]};
	CHECK(tr_server_create_record(&f.config, &f.tuning, &f.record,
	                              duplicate, 2,
	                              &f.server) == TR_ERR_INVALID);
	CHECK(!f.server);
	/* 非零但不足的硬预算必须在构造时明确拒绝，不能降级无界预算。 */
	bad = f.record;
	bad.memory_budget_bytes = 1;
	CHECK(tr_server_create_record(&f.config, &f.tuning, &bad, f.methods, 2,
	                              &f.server) != TR_OK);
	fixture_destroy(&f);
}

static void test_exact_window(void)
{
	struct fixture f;
	struct peer p;
	fixture_init(&f, 1, 1);
	f.config.limits.max_message_bytes = BODY + TR_RPC_RECORD_HEADER_SIZE;
	f.config.limits.initial_window_bytes = BODY + TR_RPC_RECORD_HEADER_SIZE;
	fixture_create(&f);
	CHECK(fixture_start(&f) == TR_OK);
	peer_connect(&p, f.port, true);
	/* 请求整条记录恰好等于服务端接收窗口，OPEN 占用的信用需要
	 * 实际归还；不能只在构造阶段接受边界而永远无法发送。 */
	echo(&f, &p, BODY);
	peer_destroy(&p);
	wait_peers(&f, 0, 1);
	fixture_destroy(&f);
}

int main(void)
{
	test_configuration();
	test_exact_window();
	test_shapes_and_reuse();
	test_shared_executor();
	test_disconnected_work();
	test_stopped_work(false, false, false);
	test_stopped_work(true, false, false);
	test_stopped_work(false, true, false);
	test_stopped_work(false, false, true);
	test_multishard_and_start_failure(0);
	test_multishard_and_start_failure(2);
	test_multishard_and_start_failure(3);
	test_adopt_failure(false);
	test_adopt_failure(true);
	test_detach_retry(false);
	test_detach_retry(true);
	test_join_retry();
	puts("server_record: 17 scenarios passed");
	return 0;
}
