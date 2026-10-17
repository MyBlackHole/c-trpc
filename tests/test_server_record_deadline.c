#define _POSIX_C_SOURCE 200809L
/* 期限证据来自真实两端门面的线协议。Client 始终没有本地 timeout；
 * 测试只替换其 OPEN 前缀中的剩余期限，观察 Server 实际发送的 FINAL/RESET。
 * 精确时刻只作用于 RPC 入口，Reactor、传输、等待和业务回调保持真实时钟。 */
#include "../src/client_record_internal.h"
#include "../src/server_record_internal.h"
#include "../src/endian.h"
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

enum { CAP = 4, METHODS = 4, TASKS = 16, BODY = 4097, WIRE = 8192 };
enum { WAIT_MS = 15000 };
enum clock_domain {
	CLOCK_REAL,
	CLOCK_RECEIVE,
	CLOCK_PUMP,
	CLOCK_DONE,
	CLOCK_WORK
};
enum work_mode { WORK_ECHO, WORK_GATE, WORK_CANCEL_GATE };

struct fixture;
struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned entered;
	bool released;
};
struct method {
	struct fixture *fixture;
	unsigned index;
	struct gate gate;
	_Atomic int mode;
	_Atomic unsigned seen[6], received[6], cancelled, copies;
	_Atomic unsigned messages_sent, finals_sent, message_again, terminals;
	_Atomic uint64_t timeout_received;
	struct tr_rpc_dispatch_id id;
	struct tr_stream_handle stream;
	enum tr_rpc_status terminal_status;
};
struct channel_observation {
	struct tr_channel *channel;
	tr_stream_data_cb data;
	tr_stream_event_cb event;
	tr_channel_event_cb channel_event;
	void *arg;
};
struct task_observation {
	struct fixture *fixture;
	struct tr_task_spec original;
	bool busy;
};
struct fixture {
	struct tr_server *server;
	struct tr_client *client;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server_record_config record_config;
	struct tr_client_record_config client_record;
	struct tr_facade_tuning tuning;
	struct tr_rpc_dispatch_method methods[METHODS];
	struct tr_rpc_method_desc descriptors[METHODS];
	struct method business[METHODS];
	struct channel_observation client_observation, server_observation;
	_Atomic(struct tr_channel *) client_channel, server_channel;
	_Atomic(struct tr_rpc_record_dispatch *) dispatch;
	_Atomic(struct tr_task_executor *) executor;
	struct tr_memory_budget *server_budget;
	tr_rpc_dispatch_terminal_fn original_terminal;
	void *original_terminal_arg;
	struct task_observation tasks[TASKS];
	_Atomic bool fake_clock, pause_pump, pause_done, hold_responses;
	_Atomic bool split_open, partial_message;
	_Atomic uint64_t clock_ns, wire_timeout;
	_Atomic int fail_domain;
	_Atomic unsigned clock_failures, completed_work, dispatch_delivered;
	_Atomic unsigned prefixes, prefixes_released, bodies, bodies_released;
	_Atomic unsigned held_count;
	_Atomic unsigned partial_rejections, split_opens;
	struct tr_stream_handle held_stream;
	struct tr_buffer *held;
	unsigned stream_method[CAP];
	uint64_t origin;
};
/* active 只在所有相关 Reactor/worker 创建前发布、全部销毁后清除。 */
static struct fixture *active;
static _Thread_local enum clock_domain clock_scope;

int __real_clock_gettime(clockid_t, struct timespec *);
int __wrap_clock_gettime(clockid_t id, struct timespec *out)
{
	struct fixture *f = active;
	if (id == CLOCK_MONOTONIC && f && clock_scope != CLOCK_REAL) {
		int expected = clock_scope;
		if (atomic_compare_exchange_strong(&f->fail_domain, &expected,
		                                   CLOCK_REAL)) {
			atomic_fetch_add(&f->clock_failures, 1);
			errno = EIO;
			return -1;
		}
		if (atomic_load(&f->fake_clock)) {
			uint64_t value = atomic_load(&f->clock_ns);
			out->tv_sec = (time_t)(value / 1000000000);
			out->tv_nsec = (long)(value % 1000000000);
			return 0;
		}
	}
	return __real_clock_gettime(id, out);
}
static uint64_t real_now_ns(void)
{
	struct timespec ts;
	CHECK(__real_clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
}
static void pause_once(void)
{
	const struct timespec ts = {0, 1000000};
	CHECK(!nanosleep(&ts, NULL) || errno == EINTR);
}
static void wait_count(_Atomic unsigned *count, unsigned expected)
{
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	while (atomic_load(count) < expected) {
		CHECK(real_now_ns() < end);
		pause_once();
	}
}
static void gate_init(struct gate *g)
{
	pthread_condattr_t attr;
	*g = (struct gate){0};
	CHECK(!pthread_mutex_init(&g->lock, NULL));
	CHECK(!pthread_condattr_init(&attr));
	CHECK(!pthread_condattr_setclock(&attr, CLOCK_MONOTONIC));
	CHECK(!pthread_cond_init(&g->cond, &attr));
	CHECK(!pthread_condattr_destroy(&attr));
}
static void gate_wait(struct gate *g, bool worker)
{
	struct timespec end;
	CHECK(clock_scope == CLOCK_REAL);
	CHECK(!__real_clock_gettime(CLOCK_MONOTONIC, &end));
	end.tv_sec += WAIT_MS / 1000;
	CHECK(!pthread_mutex_lock(&g->lock));
	if (worker) {
		++g->entered;
		CHECK(!pthread_cond_broadcast(&g->cond));
	}
	while (worker ? !g->released : !g->entered)
		CHECK(!pthread_cond_timedwait(&g->cond, &g->lock, &end));
	CHECK(!pthread_mutex_unlock(&g->lock));
}
static void gate_release(struct gate *g)
{
	CHECK(!pthread_mutex_lock(&g->lock));
	g->released = true;
	CHECK(!pthread_cond_broadcast(&g->cond));
	CHECK(!pthread_mutex_unlock(&g->lock));
}
static unsigned kind_of(const struct tr_buffer *b)
{
	if (!b || b->len < TR_RPC_RECORD_HEADER_SIZE ||
	    memcmp(b->data, "TRPC", 4))
		return 0;
	unsigned kind = tr_get_le16(b->data + 6);
	CHECK(kind >= TR_RPC_RECORD_OPEN && kind <= TR_RPC_RECORD_FINAL_STATUS);
	return kind;
}
struct replacement_prefix {
	struct tr_buffer buffer;
	struct fixture *fixture;
	uint8_t bytes[];
};
static void release_prefix(struct tr_buffer *buffer)
{
	struct replacement_prefix *p = (struct replacement_prefix *)buffer;
	atomic_fetch_add(&p->fixture->prefixes_released, 1);
	free(p);
}
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	struct fixture *f = active;
	struct replacement_prefix *replacement = NULL;
	struct tr_buffer *patched[2];
	bool client = f && stream.channel == atomic_load(&f->client_channel);
	bool server = f && stream.channel == atomic_load(&f->server_channel);
	unsigned kind = count ? kind_of(buffers[0]) : 0;
	if (client && kind == TR_RPC_RECORD_OPEN) {
		CHECK(count == 1 && tr_get_le64(buffers[0]->data + 40) == 0);
		size_t size = buffers[0]->len;
		replacement = malloc(sizeof(*replacement) + size);
		CHECK(replacement);
		replacement->fixture = f;
		replacement->buffer =
		        (struct tr_buffer){.data = replacement->bytes,
		                           .len = (uint32_t)size,
		                           .capacity = (uint32_t)size,
		                           .release_cb = release_prefix};
		memcpy(replacement->bytes, buffers[0]->data, size);
		tr_put_le64(replacement->bytes + 40,
		            atomic_load(&f->wire_timeout));
		patched[0] = &replacement->buffer;
		atomic_fetch_add(&f->prefixes, 1);
	}
	/* 真实发送内部可能读取 Reactor 计时；不能把 RPC 的合成时刻泄露进去。
	 * 原前缀仅在 TR_OK 时由包装器接管，失败时仍交给原调用者释放。 */
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_REAL;
	int ret = __real_tr_stream_sendv(
	        stream, replacement ? patched : buffers, count);
	clock_scope = prior;
	if (replacement) {
		if (ret == TR_OK)
			tr_buffer_release(buffers[0]);
		else
			tr_buffer_release(&replacement->buffer);
	}
	if (server && kind && stream.slot < CAP) {
		unsigned index = f->stream_method[stream.slot];
		CHECK(index < METHODS);
		struct method *m = &f->business[index];
		if (kind == TR_RPC_RECORD_MESSAGE) {
			if (ret == TR_OK)
				atomic_fetch_add(&m->messages_sent, 1);
			else if (ret == TR_AGAIN)
				atomic_fetch_add(&m->message_again, 1);
		} else if (kind == TR_RPC_RECORD_FINAL_STATUS && ret == TR_OK) {
			atomic_fetch_add(&m->finals_sent, 1);
		}
	}
	return ret;
}

int __real_tr_channel_create_deferred(const struct tr_channel_config *,
                                      struct tr_conn_handle,
                                      struct tr_conn_handle, tr_stream_data_cb,
                                      tr_stream_event_cb, tr_channel_event_cb,
                                      void *, struct tr_channel **);
int __wrap_tr_channel_create_deferred(const struct tr_channel_config *cfg,
                                      struct tr_conn_handle control,
                                      struct tr_conn_handle bulk,
                                      tr_stream_data_cb data,
                                      tr_stream_event_cb event,
                                      tr_channel_event_cb channel_event,
                                      void *arg, struct tr_channel **out)
{
	CHECK((cfg->feature_bits & TR_CHANNEL_FEATURE_STREAM_RESET) != 0);
	CHECK((cfg->required_feature_bits & TR_CHANNEL_FEATURE_STREAM_RESET) !=
	      0);
	int ret = __real_tr_channel_create_deferred(
	        cfg, control, bulk, data, event, channel_event, arg, out);
	if (ret == TR_OK && active) {
		if (cfg->role == TR_CHANNEL_CLIENT)
			atomic_store(&active->client_channel, *out);
		else
			atomic_store(&active->server_channel, *out);
	}
	return ret;
}
static enum tr_stream_data_disposition
observed_data(struct tr_stream_handle stream, uint64_t id,
              struct tr_buffer *payload, void *arg)
{
	struct channel_observation *o = arg;
	struct fixture *f = active;
	unsigned kind = kind_of(payload), index = METHODS;
	bool server = stream.channel == atomic_load(&f->server_channel);
	if (server && kind) {
		CHECK(stream.slot < CAP);
		if (kind == TR_RPC_RECORD_OPEN) {
			unsigned method = tr_get_le32(payload->data + 12);
			CHECK(method && method <= METHODS);
			f->stream_method[stream.slot] = method - 1;
			f->business[method - 1].stream = stream;
			atomic_store(&f->business[method - 1].timeout_received,
			             tr_get_le64(payload->data + 40));
		}
		index = f->stream_method[stream.slot];
		CHECK(index < METHODS);
	}
	enum tr_stream_data_disposition disposition =
	        o->data(stream, id, payload, o->arg);
	if (index < METHODS)
		atomic_fetch_add(&f->business[index].received[kind], 1);
	if (!server && kind == TR_RPC_RECORD_MESSAGE &&
	    atomic_load(&f->hold_responses)) {
		CHECK(disposition == TR_STREAM_DATA_RELEASE && !f->held);
		f->held = payload;
		f->held_stream = stream;
		atomic_store(&f->held_count, 1);
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return disposition;
}
static void observed_event(struct tr_stream_handle stream,
                           enum tr_stream_event event, int status, void *arg)
{
	struct channel_observation *o = arg;
	if (o->event)
		o->event(stream, event, status, o->arg);
}
static void observed_channel_event(struct tr_channel *channel,
                                   enum tr_channel_event event, int status,
                                   void *arg)
{
	struct channel_observation *o = arg;
	if (o->channel_event)
		o->channel_event(channel, event, status, o->arg);
}
int __real_tr_channel_set_handler(struct tr_channel *, tr_stream_data_cb,
                                  tr_stream_event_cb, tr_channel_event_cb,
                                  void *);
int __wrap_tr_channel_set_handler(struct tr_channel *channel,
                                  tr_stream_data_cb data,
                                  tr_stream_event_cb event,
                                  tr_channel_event_cb channel_event, void *arg)
{
	struct fixture *f = active;
	if (!f || !data)
		return __real_tr_channel_set_handler(channel, data, event,
		                                     channel_event, arg);
	struct channel_observation *o;
	if (channel == atomic_load(&f->client_channel))
		o = &f->client_observation;
	else if (channel == atomic_load(&f->server_channel))
		o = &f->server_observation;
	else
		return __real_tr_channel_set_handler(channel, data, event,
		                                     channel_event, arg);
	CHECK(!o->channel);
	*o = (struct channel_observation){channel, data, event, channel_event,
	                                  arg};
	return __real_tr_channel_set_handler(channel, observed_data,
	                                     observed_event,
	                                     observed_channel_event, o);
}
static void observed_terminal(struct tr_rpc_dispatch_handle handle,
                              struct tr_stream_handle stream,
                              enum tr_rpc_status status,
                              enum tr_rpc_record_delivery delivery,
                              bool final_accepted, void *arg)
{
	struct fixture *f = arg;
	CHECK(stream.slot < CAP);
	unsigned index = f->stream_method[stream.slot];
	CHECK(index < METHODS);
	f->business[index].terminal_status = status;
	atomic_fetch_add(&f->business[index].terminals, 1);
	/* Endpoint 的 final_wait 是真实 Transport 收尾期限，与本测试的 RPC
	 * 业务时刻独立；进入原终局回调之前恢复真实取时。 */
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_REAL;
	f->original_terminal(handle, stream, status, delivery, final_accepted,
	                     f->original_terminal_arg);
	clock_scope = prior;
}
int __real_tr_rpc_record_dispatch_create_on_owner(
        struct tr_channel *, struct tr_task_executor *,
        const struct tr_rpc_dispatch_method *, size_t,
        const struct tr_rpc_record_dispatch_config *,
        tr_rpc_dispatch_terminal_fn, void *, struct tr_memory_budget *,
        struct tr_rpc_record_dispatch **);
int __wrap_tr_rpc_record_dispatch_create_on_owner(
        struct tr_channel *channel, struct tr_task_executor *executor,
        const struct tr_rpc_dispatch_method *methods, size_t count,
        const struct tr_rpc_record_dispatch_config *config,
        tr_rpc_dispatch_terminal_fn terminal, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_dispatch **out)
{
	struct fixture *f = active;
	CHECK(f && channel == atomic_load(&f->server_channel));
	f->original_terminal = terminal;
	f->original_terminal_arg = arg;
	int ret = __real_tr_rpc_record_dispatch_create_on_owner(
	        channel, executor, methods, count, config, observed_terminal, f,
	        budget, out);
	if (ret == TR_OK) {
		f->server_budget = budget;
		atomic_store(&f->executor, executor);
		atomic_store(&f->dispatch, *out);
	}
	return ret;
}
int __real_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *, struct tr_stream_handle,
        const uint8_t *, size_t);
int __wrap_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *d, struct tr_stream_handle stream,
        const uint8_t *bytes, size_t size)
{
	enum clock_domain prior = clock_scope;
	if (active && d == atomic_load(&active->dispatch))
		clock_scope = CLOCK_RECEIVE;
	int ret;
	struct fixture *f = active;
	unsigned kind =
	        size >= TR_RPC_RECORD_HEADER_SIZE ? tr_get_le16(bytes + 6) : 0;
	if (f && kind == TR_RPC_RECORD_OPEN &&
	    atomic_exchange(&f->split_open, false)) {
		/* 线上的 OPEN 确实已到达；这里有意在既有接收契约边界分两次
		 * 交付，单独验证实际记录解析器，不把它称作 TCP 分片时序。 */
		CHECK(atomic_load(&f->pause_pump));
		CHECK(size > TR_RPC_RECORD_HEADER_SIZE &&
		      tr_get_le32(bytes + 32) > 0);
		ret = __real_tr_rpc_record_dispatch_receive_on_owner(
		        d, stream, bytes, size - 1);
		CHECK(ret == TR_OK);
		struct tr_rpc_record_dispatch_stats stats;
		CHECK(tr_rpc_record_dispatch_get_stats_on_owner(d, &stats) ==
		      TR_OK);
		CHECK(!stats.held_events && !stats.terminal_calls);
		atomic_store(&f->clock_ns, f->origin + 1000);
		ret = __real_tr_rpc_record_dispatch_receive_on_owner(
		        d, stream, bytes + size - 1, 1);
		atomic_fetch_add(&f->split_opens, 1);
	} else if (f && kind == TR_RPC_RECORD_MESSAGE &&
	           atomic_exchange(&f->partial_message, false)) {
		/* 到期时只交付不足一个头的七字节前缀；应在路由解析/追加
		 * 缓存前拒绝，已排队 OPEN 可释放，但不能增加持有的预算。 */
		uint64_t before = atomic_load(&f->server_budget->current_bytes);
		ret = __real_tr_rpc_record_dispatch_receive_on_owner(d, stream,
		                                                     bytes, 7);
		CHECK(ret == TR_ERR_STATE);
		CHECK(atomic_load(&f->server_budget->current_bytes) <= before);
		atomic_fetch_add(&f->partial_rejections, 1);
	} else {
		ret = __real_tr_rpc_record_dispatch_receive_on_owner(
		        d, stream, bytes, size);
	}
	clock_scope = prior;
	return ret;
}
int __real_tr_rpc_record_dispatch_pump_on_owner(struct tr_rpc_record_dispatch *,
                                                size_t);
int __wrap_tr_rpc_record_dispatch_pump_on_owner(
        struct tr_rpc_record_dispatch *d, size_t limit)
{
	if (active && d == atomic_load(&active->dispatch) &&
	    atomic_load(&active->pause_pump))
		return TR_OK;
	enum clock_domain prior = clock_scope;
	if (active && d == atomic_load(&active->dispatch))
		clock_scope = CLOCK_PUMP;
	int ret = __real_tr_rpc_record_dispatch_pump_on_owner(d, limit);
	clock_scope = prior;
	return ret;
}
static int observed_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct task_observation *o = arg;
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_WORK;
	int ret = o->original.work(o->original.arg, cancel);
	clock_scope = prior;
	atomic_fetch_add(&o->fixture->completed_work, 1);
	return ret;
}
static void observed_done(void *arg, struct tr_task_handle handle,
                          struct tr_task_result result)
{
	struct task_observation *o = arg;
	struct tr_task_spec original = o->original;
	CHECK(o->busy);
	original.done(original.arg, handle, result);
	o->busy = false;
}
int __real_tr_task_executor_submit(struct tr_task_executor *,
                                   const struct tr_task_spec *,
                                   struct tr_task_handle *);
int __wrap_tr_task_executor_submit(struct tr_task_executor *executor,
                                   const struct tr_task_spec *spec,
                                   struct tr_task_handle *out)
{
	struct fixture *f = active;
	struct task_observation *o = NULL;
	struct tr_task_spec wrapped;
	if (f && executor == atomic_load(&f->executor)) {
		for (unsigned i = 0; i < TASKS; ++i)
			if (!f->tasks[i].busy) {
				o = &f->tasks[i];
				break;
			}
		CHECK(o);
		*o = (struct task_observation){f, *spec, true};
		wrapped =
		        (struct tr_task_spec){observed_work, observed_done, o};
	}
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_REAL;
	int ret = __real_tr_task_executor_submit(executor, o ? &wrapped : spec,
	                                         out);
	clock_scope = prior;
	if (o && ret != TR_OK)
		o->busy = false;
	return ret;
}
int __real_tr_task_executor_dispatch(struct tr_task_executor *, size_t,
                                     size_t *);
int __wrap_tr_task_executor_dispatch(struct tr_task_executor *executor,
                                     size_t limit, size_t *delivered)
{
	struct fixture *f = active;
	bool selected = f && executor == atomic_load(&f->executor);
	if (selected && tr_reactor_in_owner_context() &&
	    atomic_load(&f->pause_done)) {
		*delivered = 0;
		return TR_OK;
	}
	/* 当前真实执行器不取时。合成域仅覆盖它交付的 RPC done；done 转入
	 * Endpoint/Transport 的所有出口均由上述包装恢复真实时钟。 */
	enum clock_domain prior = clock_scope;
	if (selected && tr_reactor_in_owner_context())
		clock_scope = CLOCK_DONE;
	int ret = __real_tr_task_executor_dispatch(executor, limit, delivered);
	clock_scope = prior;
	if (selected && ret == TR_OK)
		atomic_fetch_add(&f->dispatch_delivered, (unsigned)*delivered);
	return ret;
}

static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct method *m = arg;
	unsigned kind = view->header.kind;
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_REAL;
	CHECK(tr_task_in_worker_context() && !tr_reactor_in_owner_context());
	if (kind == TR_RPC_RECORD_OPEN) {
		m->id = id;
		if (atomic_load(&m->fixture->split_opens)) {
			const uint8_t metadata[] = {5,   4,   0,   'p',
			                            'r', 'o', 'o', 'f',
			                            'd', 'a', 't', 'a'};
			CHECK(view->header.metadata_size == sizeof(metadata));
			CHECK(!memcmp(view->metadata, metadata,
			              sizeof(metadata)));
		}
	}
	if (kind == TR_RPC_RECORD_MESSAGE) {
		int mode = atomic_load(&m->mode);
		if (mode == WORK_CANCEL_GATE) {
			uint64_t end =
			        real_now_ns() + (uint64_t)WAIT_MS * 1000000;
			while (!tr_task_cancel_requested(cancel)) {
				CHECK(real_now_ns() < end);
				pause_once();
			}
			atomic_fetch_add(&m->cancelled, 1);
		}
		if (mode != WORK_ECHO)
			gate_wait(&m->gate, true);
		if (mode == WORK_GATE && tr_task_cancel_requested(cancel))
			atomic_fetch_add(&m->cancelled, 1);
		for (unsigned i = 0; i < atomic_load(&m->copies); ++i)
			CHECK(tr_rpc_record_reply_append(
			              reply, view->body,
			              view->header.decoded_size) == TR_OK);
	}
	atomic_fetch_add(&m->seen[kind], 1);
	clock_scope = prior;
	return TR_RPC_STATUS_OK;
}
static void fixture_init(struct fixture *f, bool fake)
{
	CHECK(!active);
	*f = (struct fixture){0};
	active = f;
	f->origin = real_now_ns();
	atomic_store(&f->clock_ns, f->origin);
	atomic_store(&f->fake_clock, fake);
	for (unsigned i = 0; i < CAP; ++i)
		f->stream_method[i] = METHODS;
	tr_server_config_init(&f->server_config);
	f->server_config.shard_count = 1;
	f->server_config.max_peers = 1;
	f->server_config.keepalive_interval_ms = 0;
	f->server_config.limits.max_streams = CAP;
	f->server_config.limits.max_calls = CAP;
	f->server_config.limits.max_methods = METHODS;
	f->server_config.limits.max_frame_payload_bytes = 256;
	f->server_config.limits.max_message_bytes = WIRE;
	f->server_config.limits.initial_window_bytes = WIRE * 4;
	f->server_config.limits.window_update_threshold_bytes = 1;
	tr_client_config_init(&f->client_config);
	f->client_config.limits = f->server_config.limits;
	f->client_config.keepalive_interval_ms = 0;
	f->client_config.connect_timeout_ms = 2000;
	tr_facade_tuning_init(&f->tuning);
	f->tuning.command_capacity = 128;
	f->tuning.tx_item_capacity = 128;
	f->tuning.control_tx_item_capacity = 64;
	f->tuning.rx_buffer_count = 64;
	f->tuning.reassembly_pool_count = 16;
	f->record_config = (struct tr_server_record_config){
	        .endpoint = {.dispatch = {CAP, 8, 3, BODY * 3},
	                     .completion_limit = 2,
	                     .progress_limit = 2,
	                     .progress_period_ns = 1000000,
	                     .final_wait_ns = 1000000000},
	        .tasks = {1, TASKS},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	f->client_record = (struct tr_client_record_config){
	        .calls = {CAP, 2, 1000000},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	for (unsigned i = 0; i < METHODS; ++i) {
		struct method *m = &f->business[i];
		m->fixture = f;
		m->index = i;
		atomic_store(&m->copies, 1);
		gate_init(&m->gate);
		f->methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {17, i + 1, TR_RPC_MANY, TR_RPC_MANY,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_CONTROL, BODY, BODY},
		        .work = business,
		        .arg = m};
		f->descriptors[i] = f->methods[i].descriptor;
	}
}
static void fixture_start(struct fixture *f)
{
	uint16_t port;
	CHECK(tr_server_create_record(&f->server_config, &f->tuning,
	                              &f->record_config, f->methods, METHODS,
	                              &f->server) == TR_OK);
	CHECK(tr_client_create_record(&f->client_config, &f->tuning,
	                              &f->client_record, f->descriptors,
	                              METHODS, &f->client) == TR_OK);
	CHECK(tr_server_listen(f->server, "127.0.0.1", 0, &port) == TR_OK);
	CHECK(tr_server_start(f->server) == TR_OK);
	CHECK(tr_client_connect(f->client, "127.0.0.1", port) == TR_OK);
	CHECK(tr_client_wait_ready(f->client, WAIT_MS) == TR_OK);
}
static struct tr_reactor *server_owner(struct fixture *f)
{
	struct tr_channel *channel = atomic_load(&f->server_channel);
	CHECK(channel);
	return tr_channel_reactor(channel);
}
static int barrier(void *arg)
{
	(void)arg;
	CHECK(tr_reactor_in_owner_context());
	return TR_OK;
}
static void owner_barrier(struct fixture *f)
{
	CHECK(tr_reactor_call(server_owner(f), barrier, NULL) == TR_OK);
}
static int release_held(void *arg)
{
	struct fixture *f = arg;
	if (f->held) {
		int ret = tr_stream_release_payload(f->held_stream, f->held);
		CHECK(ret == TR_OK || ret == TR_AGAIN || ret == TR_ERR_STALE);
		f->held = NULL;
		atomic_store(&f->held_count, 0);
	}
	return TR_OK;
}
static void fixture_destroy(struct fixture *f)
{
	atomic_store(&f->pause_pump, false);
	atomic_store(&f->pause_done, false);
	atomic_store(&f->fail_domain, CLOCK_REAL);
	atomic_store(&f->hold_responses, false);
	for (unsigned i = 0; i < METHODS; ++i)
		gate_release(&f->business[i].gate);
	CHECK(tr_reactor_call(
	              tr_channel_reactor(atomic_load(&f->client_channel)),
	              release_held, f) == TR_OK);
	CHECK(tr_client_destroy(f->client) == TR_OK);
	f->client = NULL;
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	int ret;
	while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
		CHECK(real_now_ns() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	f->server = NULL;
	for (unsigned i = 0; i < TASKS; ++i)
		CHECK(!f->tasks[i].busy);
	for (unsigned i = 0; i < METHODS; ++i) {
		CHECK(!pthread_cond_destroy(&f->business[i].gate.cond));
		CHECK(!pthread_mutex_destroy(&f->business[i].gate.lock));
	}
	CHECK(atomic_load(&f->prefixes) == atomic_load(&f->prefixes_released));
	CHECK(atomic_load(&f->bodies) == atomic_load(&f->bodies_released));
	active = NULL;
}

struct call {
	struct fixture *fixture;
	struct tr_rpc_record_client_handle handle;
	_Atomic unsigned writable, messages, finals, terminals;
	enum tr_rpc_status status;
};
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
	CHECK(tr_reactor_in_owner_context() && !atomic_load(&c->terminals));
	atomic_fetch_add(&c->writable, 1);
}
static void client_terminal(struct tr_rpc_record_client_handle handle,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct call *c = arg;
	(void)handle;
	CHECK(delivery == TR_RPC_RECORD_ON_OWNER);
	CHECK(tr_reactor_in_owner_context());
	c->status = status;
	CHECK(atomic_fetch_add(&c->terminals, 1) == 0);
}
static void call_start(struct call *c, struct fixture *f, unsigned method,
                       uint64_t timeout)
{
	*c = (struct call){.fixture = f};
	atomic_store(&f->wire_timeout, timeout);
	const struct tr_rpc_record_client_callbacks callbacks = {
	        client_received, client_writable, client_terminal, c};
	const struct tr_rpc_metadata metadata = {"proof", "data", 4};
	const struct tr_rpc_call_options metadata_only = {.metadata = &metadata,
	                                                  .metadata_count = 1};
	const struct tr_rpc_call_options *options =
	        atomic_load(&f->split_open) ? &metadata_only : NULL;
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	int ret;
	while ((ret = tr_client_record_call_start(f->client, 17, method,
	                                          options, &callbacks,
	                                          &c->handle)) == TR_AGAIN) {
		CHECK(!atomic_load(&c->terminals) &&
		      !atomic_load(&c->writable));
		CHECK(real_now_ns() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	wait_count(&f->business[method - 1].received[TR_RPC_RECORD_OPEN], 1);
	CHECK(atomic_load(&f->business[method - 1].timeout_received) ==
	      timeout);
}
struct body {
	struct tr_buffer buffer;
	struct fixture *fixture;
	uint8_t bytes[BODY];
};
static void body_release(struct tr_buffer *buffer)
{
	struct body *body = (struct body *)buffer;
	atomic_fetch_add(&body->fixture->bodies_released, 1);
	free(body);
}
static void call_message(struct call *c)
{
	struct body *b = malloc(sizeof(*b));
	CHECK(b);
	b->fixture = c->fixture;
	b->buffer = (struct tr_buffer){.data = b->bytes,
	                               .len = BODY,
	                               .capacity = BODY,
	                               .release_cb = body_release};
	for (unsigned i = 0; i < BODY; ++i)
		b->bytes[i] = (uint8_t)(i * 7 + 3);
	atomic_fetch_add(&c->fixture->bodies, 1);
	struct tr_buffer *buffer = &b->buffer;
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	int ret;
	while ((ret = tr_client_record_send(c->handle, &buffer)) == TR_AGAIN) {
		CHECK(buffer == &b->buffer && real_now_ns() < end);
		pause_once();
	}
	CHECK(ret == TR_OK && !buffer);
}
static void call_half_close(struct call *c)
{
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	int ret;
	while ((ret = tr_client_record_half_close(c->handle)) == TR_AGAIN) {
		CHECK(real_now_ns() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
}
static void call_finish_records(struct call *c, enum tr_rpc_status status,
                                unsigned messages, unsigned finals)
{
	wait_count(&c->terminals, 1);
	if (c->status != status)
		fprintf(stderr, "deadline: status=%d expected=%d\n", c->status,
		        status);
	CHECK(c->status == status && atomic_load(&c->terminals) == 1);
	CHECK(atomic_load(&c->finals) == finals);
	CHECK(atomic_load(&c->messages) == messages);
}
static void call_finish(struct call *c, enum tr_rpc_status status,
                        unsigned messages)
{
	call_finish_records(c, status, messages, 1);
}
static void call_retired(struct call *c)
{
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	int ret;
	while ((ret = tr_client_record_cancel(c->handle)) == TR_ERR_STATE) {
		CHECK(real_now_ns() < end);
		pause_once();
	}
	CHECK(ret == TR_ERR_STALE);
}
static void wait_tasks(struct fixture *f, size_t queued, size_t completed)
{
	struct tr_task_executor_stats stats;
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	for (;;) {
		CHECK(tr_task_executor_get_stats(atomic_load(&f->executor),
		                                 &stats) == TR_OK);
		if (stats.queued >= queued && stats.completed >= completed)
			return;
		CHECK(real_now_ns() < end);
		pause_once();
	}
}
struct snapshot {
	struct fixture *fixture;
	struct tr_rpc_record_dispatch_stats stats;
};
static int snapshot_on_owner(void *arg)
{
	struct snapshot *s = arg;
	return tr_rpc_record_dispatch_get_stats_on_owner(
	        atomic_load(&s->fixture->dispatch), &s->stats);
}
static void wait_no_events(struct fixture *f)
{
	struct snapshot snapshot = {.fixture = f};
	uint64_t end = real_now_ns() + (uint64_t)WAIT_MS * 1000000;
	for (;;) {
		CHECK(tr_reactor_call(server_owner(f), snapshot_on_owner,
		                      &snapshot) == TR_OK);
		if (!snapshot.stats.held_events)
			return;
		CHECK(real_now_ns() < end);
		pause_once();
	}
}

/* 真实时钟与真实 RESET 共同证明服务端期限。网络先停，业务停下之前
 * 不释放已受理责任；业务故意在取消后仍追加三条跨窗口正文。 */
static void test_running_server_deadline(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, false);
	atomic_store(&f.business[0].mode, WORK_CANCEL_GATE);
	atomic_store(&f.business[0].copies, 3);
	fixture_start(&f);
	call_start(&c, &f, 1, 1000000000);
	call_message(&c);
	call_half_close(&c);
	wait_count(&f.business[0].cancelled, 1);
	gate_wait(&f.business[0].gate, false);
	call_finish_records(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0, 0);
	CHECK(!atomic_load(&f.business[0].terminals));
	struct snapshot pending = {.fixture = &f};
	CHECK(tr_reactor_call(server_owner(&f), snapshot_on_owner, &pending) ==
	      TR_OK);
	CHECK(pending.stats.held_events > 0 &&
	      pending.stats.submitted_calls == 1 &&
	      pending.stats.terminal_calls == 1);
	gate_release(&f.business[0].gate);
	wait_count(&f.business[0].terminals, 1);
	call_finish_records(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0, 0);
	call_retired(&c);
	CHECK(atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]) == 1);
	CHECK(atomic_load(&f.business[0].seen[TR_RPC_RECORD_MESSAGE]) == 1);
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_HALF_CLOSE]));
	CHECK(!atomic_load(&f.business[0].messages_sent));
	CHECK(!atomic_load(&f.business[0].finals_sent));
	CHECK(f.business[0].terminal_status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(atomic_load(&f.business[0].terminals) == 1);
	fixture_destroy(&f);
}

/* B 已进入共享执行器队列；暂停所有 owner 扫描和完成交付，只让 worker
 * 在 expiry 开始 B。若只在定时器/owner 中检查期限，就会错误执行业务。 */
static void test_queued_worker_expiry(void)
{
	struct fixture f;
	struct call blocker, expired;
	fixture_init(&f, true);
	atomic_store(&f.business[0].mode, WORK_GATE);
	fixture_start(&f);
	call_start(&blocker, &f, 1, 0);
	call_message(&blocker);
	call_half_close(&blocker);
	gate_wait(&f.business[0].gate, false);
	call_start(&expired, &f, 2, 100);
	call_message(&expired);
	call_half_close(&expired);
	wait_count(&f.business[1].received[TR_RPC_RECORD_MESSAGE], 1);
	wait_count(&f.business[1].received[TR_RPC_RECORD_HALF_CLOSE], 1);
	wait_tasks(&f, 1, 0);
	atomic_store(&f.pause_pump, true);
	atomic_store(&f.pause_done, true);
	owner_barrier(&f);
	atomic_store(&f.clock_ns, f.origin + 100);
	gate_release(&f.business[0].gate);
	wait_tasks(&f, 0, 2);
	CHECK(!atomic_load(&f.business[1].seen[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&f.business[1].seen[TR_RPC_RECORD_MESSAGE]));
	CHECK(!atomic_load(&expired.terminals));
	atomic_store(&f.pause_done, false);
	call_finish(&expired, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0);
	CHECK(!atomic_load(&f.business[1].messages_sent));
	atomic_store(&f.pause_pump, false);
	call_finish(&blocker, TR_RPC_STATUS_OK, 1);
	call_retired(&blocker);
	call_retired(&expired);
	fixture_destroy(&f);
}

static void test_completed_result_expiry(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	atomic_store(&f.business[0].copies, 3);
	fixture_start(&f);
	call_start(&c, &f, 1, 100);
	wait_count(&f.business[0].seen[TR_RPC_RECORD_OPEN], 1);
	wait_no_events(&f);
	atomic_store(&f.pause_done, true);
	owner_barrier(&f);
	call_message(&c);
	call_half_close(&c);
	wait_count(&f.business[0].received[TR_RPC_RECORD_MESSAGE], 1);
	wait_count(&f.business[0].received[TR_RPC_RECORD_HALF_CLOSE], 1);
	wait_tasks(&f, 0, 1);
	CHECK(atomic_load(&f.business[0].seen[TR_RPC_RECORD_MESSAGE]) == 1);
	atomic_store(&f.pause_pump, true);
	owner_barrier(&f);
	atomic_store(&f.clock_ns, f.origin + 100);
	atomic_store(&f.pause_done, false);
	call_finish(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0);
	CHECK(!atomic_load(&f.business[0].messages_sent));
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_HALF_CLOSE]));
	call_retired(&c);
	fixture_destroy(&f);
}

static void test_data_boundary(uint64_t elapsed)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	atomic_store(&f.pause_pump, true);
	fixture_start(&f);
	call_start(&c, &f, 1, 100);
	atomic_store(&f.clock_ns, f.origin + elapsed);
	if (elapsed == 100)
		atomic_store(&f.partial_message, true);
	call_message(&c);
	wait_count(&f.business[0].received[TR_RPC_RECORD_MESSAGE], 1);
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]));
	if (elapsed < 100) {
		CHECK(!atomic_load(&c.terminals));
		struct snapshot snapshot = {.fixture = &f};
		CHECK(tr_reactor_call(server_owner(&f), snapshot_on_owner,
		                      &snapshot) == TR_OK);
		CHECK(snapshot.stats.held_events == 2);
		call_half_close(&c);
		atomic_store(&f.pause_pump, false);
		call_finish(&c, TR_RPC_STATUS_OK, 1);
	} else {
		/* 自动 pump 一次也没有接纳队列中的 OPEN；DATA 接收本身应
		 * 选定期限状态，不能等公平 cursor 扫描才拒绝迟到字节。 */
		call_finish(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0);
		CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_MESSAGE]));
		if (elapsed == 100)
			CHECK(atomic_load(&f.partial_rejections) == 1);
	}
	call_retired(&c);
	fixture_destroy(&f);
}

static void test_complete_open_starts_deadline(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	atomic_store(&f.pause_pump, true);
	atomic_store(&f.split_open, true);
	fixture_start(&f);
	call_start(&c, &f, 1, 100);
	CHECK(atomic_load(&f.split_opens) == 1);
	CHECK(!atomic_load(&c.terminals));
	atomic_store(&f.clock_ns, f.origin + 1099);
	call_message(&c);
	call_half_close(&c);
	atomic_store(&f.pause_pump, false);
	call_finish(&c, TR_RPC_STATUS_OK, 1);
	call_retired(&c);
	fixture_destroy(&f);
}
static int pump_expired_on_owner(void *arg)
{
	struct fixture *f = arg;
	enum clock_domain prior = clock_scope;
	clock_scope = CLOCK_PUMP;
	int ret = __real_tr_rpc_record_dispatch_pump_on_owner(
	        atomic_load(&f->dispatch), CAP);
	clock_scope = prior;
	return ret;
}
static void test_dispatch_queue_expiry(void)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	atomic_store(&f.pause_pump, true);
	fixture_start(&f);
	call_start(&c, &f, 1, 100);
	atomic_store(&f.clock_ns, f.origin + 100);
	CHECK(tr_reactor_call(server_owner(&f), pump_expired_on_owner, &f) ==
	      TR_OK);
	call_finish(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0);
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&f.completed_work));
	call_retired(&c);
	fixture_destroy(&f);
}

static void test_unlimited_and_saturated(uint64_t timeout)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	fixture_start(&f);
	call_start(&c, &f, 1, timeout);
	wait_count(&f.business[0].seen[TR_RPC_RECORD_OPEN], 1);
	atomic_store(&f.clock_ns, f.origin + 1000000000000);
	call_message(&c);
	call_half_close(&c);
	call_finish(&c, TR_RPC_STATUS_OK, 1);
	call_retired(&c);
	fixture_destroy(&f);
}

static void test_clock_failure(enum clock_domain domain)
{
	struct fixture f;
	struct call c;
	fixture_init(&f, true);
	if (domain == CLOCK_PUMP || domain == CLOCK_WORK)
		atomic_store(&f.pause_pump, true);
	fixture_start(&f);
	if (domain == CLOCK_RECEIVE)
		atomic_store(&f.fail_domain, domain);
	call_start(&c, &f, 1, 100);
	if (domain == CLOCK_DONE) {
		wait_count(&f.business[0].seen[TR_RPC_RECORD_OPEN], 1);
		wait_no_events(&f);
		atomic_store(&f.pause_done, true);
		owner_barrier(&f);
		call_message(&c);
		call_half_close(&c);
		wait_count(&f.business[0].received[TR_RPC_RECORD_MESSAGE], 1);
		wait_count(&f.business[0].received[TR_RPC_RECORD_HALF_CLOSE],
		           1);
		wait_tasks(&f, 0, 1);
		atomic_store(&f.pause_pump, true);
		owner_barrier(&f);
		atomic_store(&f.fail_domain, domain);
		atomic_store(&f.pause_done, false);
	} else if (domain == CLOCK_PUMP || domain == CLOCK_WORK) {
		atomic_store(&f.fail_domain, domain);
		atomic_store(&f.pause_pump, false);
	}
	call_finish(&c, TR_RPC_STATUS_INTERNAL, 0);
	CHECK(atomic_load(&f.clock_failures) == 1);
	CHECK(atomic_load(&f.fail_domain) == CLOCK_REAL);
	CHECK(!atomic_load(&f.business[0].messages_sent));
	if (domain != CLOCK_DONE)
		CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]));
	call_retired(&c);
	fixture_destroy(&f);
}

struct stale_check {
	struct tr_rpc_dispatch_handle handle;
};
static int stale_on_owner(void *arg)
{
	struct stale_check *s = arg;
	return tr_rpc_record_dispatch_abort_on_owner(s->handle,
	                                             TR_RPC_STATUS_CANCELLED);
}
static void test_final_and_slot_reuse(void)
{
	struct fixture f;
	struct call first, next;
	fixture_init(&f, true);
	f.record_config.endpoint.dispatch.capacity = 1;
	f.client_record.calls.capacity = 1;
	f.client_config.limits.max_streams = 1;
	f.client_config.limits.max_calls = 1;
	fixture_start(&f);
	call_start(&first, &f, 1, 100);
	atomic_store(&f.clock_ns, f.origin + 99);
	call_message(&first);
	call_half_close(&first);
	call_finish(&first, TR_RPC_STATUS_OK, 1);
	call_retired(&first);
	wait_count(&f.business[0].seen[TR_RPC_RECORD_OPEN], 1);
	struct stale_check old = {{atomic_load(&f.dispatch), f.business[0].id}};
	atomic_store(&f.clock_ns, f.origin + 101);
	call_start(&next, &f, 2, 0);
	call_message(&next);
	call_half_close(&next);
	CHECK(tr_client_record_cancel(first.handle) == TR_ERR_STALE);
	CHECK(tr_client_record_half_close(first.handle) == TR_ERR_STALE);
	CHECK(tr_reactor_call(server_owner(&f), stale_on_owner, &old) ==
	      TR_ERR_STALE);
	call_finish(&next, TR_RPC_STATUS_OK, 1);
	call_retired(&next);
	CHECK(atomic_load(&first.terminals) == 1 &&
	      first.status == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.business[0].terminals) == 1);
	CHECK(atomic_load(&f.business[1].terminals) == 1);
	fixture_destroy(&f);
}

static void test_reply_window_expiry(void)
{
	struct fixture f;
	struct call c, following;
	fixture_init(&f, true);
	f.client_config.limits.initial_window_bytes = WIRE;
	atomic_store(&f.business[0].copies, 3);
	atomic_store(&f.hold_responses, true);
	fixture_start(&f);
	call_start(&c, &f, 1, 100);
	call_message(&c);
	call_half_close(&c);
	wait_count(&f.business[0].message_again, 1);
	wait_count(&f.held_count, 1);
	CHECK(atomic_load(&f.business[0].messages_sent) == 1);
	atomic_store(&f.pause_pump, true);
	owner_barrier(&f);
	atomic_store(&f.clock_ns, f.origin + 100);
	atomic_store(&f.pause_pump, false);
	call_finish(&c, TR_RPC_STATUS_DEADLINE_EXCEEDED, 1);
	call_retired(&c);
	CHECK(atomic_load(&f.business[0].messages_sent) == 1);
	atomic_store(&f.hold_responses, false);
	CHECK(tr_reactor_call(
	              tr_channel_reactor(atomic_load(&f.client_channel)),
	              release_held, &f) == TR_OK);
	call_start(&following, &f, 2, 0);
	call_message(&following);
	call_half_close(&following);
	call_finish(&following, TR_RPC_STATUS_OK, 1);
	call_retired(&following);
	CHECK(atomic_load(&c.terminals) == 1);
	fixture_destroy(&f);
}

int main(void)
{
	(void)setvbuf(stdout, NULL, _IOLBF, 0);
	test_running_server_deadline();
	puts("test_server_record_deadline: running server deadline passed");
	test_queued_worker_expiry();
	test_dispatch_queue_expiry();
	puts("test_server_record_deadline: queued deadlines passed");
	test_completed_result_expiry();
	puts("test_server_record_deadline: delayed completion passed");
	test_data_boundary(99);
	test_data_boundary(100);
	test_data_boundary(101);
	test_complete_open_starts_deadline();
	puts("test_server_record_deadline: exact data boundaries passed");
	test_unlimited_and_saturated(0);
	test_unlimited_and_saturated(UINT64_MAX);
	puts("test_server_record_deadline: unlimited and saturation passed");
	test_clock_failure(CLOCK_RECEIVE);
	test_clock_failure(CLOCK_PUMP);
	test_clock_failure(CLOCK_DONE);
	test_clock_failure(CLOCK_WORK);
	puts("test_server_record_deadline: clock failures passed");
	test_final_and_slot_reuse();
	test_reply_window_expiry();
	puts("test_server_record_deadline: retirement and response pressure "
	     "passed");
	return 0;
}
