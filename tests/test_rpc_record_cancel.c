#define _POSIX_C_SOURCE 200809L
/* 真实两端 V2 门面上的调用级 RESET。没有直接取消服务端任务；令牌变化
 * 必须来自 TCP 上的对端重置。所有门闩与等待使用真实单调时钟。 */
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

enum { CAP = 2, METHODS = 3, BODY = 257, WIRE = 4096, WAIT_MS = 15000 };
enum work_mode { WORK_ECHO, WORK_GATE, WORK_CANCEL_GATE };

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool released;
};
struct method {
	struct gate gate;
	enum work_mode mode;
	struct tr_rpc_dispatch_id id;
	_Atomic unsigned received[6], executed[6], entered, cancelled, exited;
	_Atomic unsigned terminals, messages_sent, finals_sent, final_attempts;
	_Atomic unsigned closes;
	_Atomic uint64_t wire_timeout;
	enum tr_rpc_status status;
	bool final_accepted;
};
struct fixture {
	struct tr_client *client;
	struct tr_server *server;
	struct method methods[METHODS];
	_Atomic(struct tr_channel *) client_channel, server_channel;
	_Atomic(struct tr_rpc_record_dispatch *) dispatch;
	struct tr_memory_budget *client_budget, *server_budget;
	uint64_t client_idle_bytes, server_idle_bytes;
	tr_rpc_dispatch_terminal_fn original_terminal;
	void *original_terminal_arg;
	unsigned stream_method[CAP];
	_Atomic unsigned client_creates, server_creates;
	_Atomic unsigned bodies, bodies_released;
	_Atomic unsigned prefixes, prefixes_released, prefixes_accepted;
	_Atomic unsigned reset_attempts, reset_accepted, reset_rejections;
	_Atomic int reset_status, blocked_final_method;
	_Atomic bool clear_open_deadline, fail_reset_once;
};
/* 在创建任何 Reactor/worker 前发布，在全部销毁后清除。 */
static struct fixture *active;

static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(!clock_gettime(CLOCK_MONOTONIC, &ts));
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void pause_once(void)
{
	const struct timespec ts = {0, 1000000};
	CHECK(!nanosleep(&ts, NULL) || errno == EINTR);
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
	struct timespec end;
	CHECK(!clock_gettime(CLOCK_MONOTONIC, &end));
	end.tv_sec += WAIT_MS / 1000;
	CHECK(!pthread_mutex_lock(&gate->lock));
	while (!gate->released)
		CHECK(!pthread_cond_timedwait(&gate->cond, &gate->lock, &end));
	CHECK(!pthread_mutex_unlock(&gate->lock));
}
static void gate_release(struct gate *gate)
{
	CHECK(!pthread_mutex_lock(&gate->lock));
	gate->released = true;
	CHECK(!pthread_cond_broadcast(&gate->cond));
	CHECK(!pthread_mutex_unlock(&gate->lock));
}
static unsigned record_kind(const uint8_t *data, size_t size)
{
	if (size < TR_RPC_RECORD_HEADER_SIZE || memcmp(data, "TRPC", 4))
		return 0;
	unsigned kind = tr_get_le16(data + 6);
	CHECK(kind >= TR_RPC_RECORD_OPEN && kind <= TR_RPC_RECORD_FINAL_STATUS);
	return kind;
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
static void server_terminal(struct tr_rpc_dispatch_handle handle,
                            struct tr_stream_handle stream,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery,
                            bool final_accepted, void *arg)
{
	struct fixture *f = arg;
	CHECK(stream.slot < CAP);
	unsigned index = f->stream_method[stream.slot];
	CHECK(index < METHODS);
	struct method *m = &f->methods[index];
	CHECK(delivery == TR_RPC_RECORD_ON_OWNER);
	m->status = status;
	m->final_accepted = final_accepted;
	CHECK(atomic_fetch_add(&m->terminals, 1) == 0);
	f->original_terminal(handle, stream, status, delivery, final_accepted,
	                     f->original_terminal_arg);
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
	CHECK(f);
	f->original_terminal = terminal;
	f->original_terminal_arg = arg;
	int ret = __real_tr_rpc_record_dispatch_create_on_owner(
	        channel, executor, methods, count, config, server_terminal, f,
	        budget, out);
	if (ret == TR_OK) {
		f->server_budget = budget;
		atomic_store(&f->dispatch, *out);
		atomic_store(&f->server_channel, channel);
		atomic_fetch_add(&f->server_creates, 1);
	}
	return ret;
}
int __real_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *, struct tr_stream_handle,
        const uint8_t *, size_t);
int __wrap_tr_rpc_record_dispatch_receive_on_owner(
        struct tr_rpc_record_dispatch *dispatch, struct tr_stream_handle stream,
        const uint8_t *data, size_t size)
{
	struct fixture *f = active;
	CHECK(f && stream.channel == atomic_load(&f->server_channel));
	unsigned kind = record_kind(data, size);
	CHECK(kind && stream.slot < CAP);
	if (kind == TR_RPC_RECORD_OPEN) {
		unsigned method = tr_get_le32(data + 12);
		CHECK(method && method <= METHODS);
		f->stream_method[stream.slot] = method - 1;
		atomic_store(&f->methods[method - 1].wire_timeout,
		             tr_get_le64(data + 40));
	}
	unsigned index = f->stream_method[stream.slot];
	CHECK(index < METHODS);
	int ret = __real_tr_rpc_record_dispatch_receive_on_owner(
	        dispatch, stream, data, size);
	if (ret == TR_OK)
		atomic_fetch_add(&f->methods[index].received[kind], 1);
	return ret;
}

struct replacement_prefix {
	struct tr_buffer buffer;
	struct fixture *fixture;
	uint8_t data[];
};
static void prefix_release(struct tr_buffer *buffer)
{
	struct replacement_prefix *prefix = (struct replacement_prefix *)buffer;
	atomic_fetch_add(&prefix->fixture->prefixes_released, 1);
	free(prefix);
}
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	struct fixture *f = active;
	CHECK(f);
	unsigned kind =
	        count ? record_kind(buffers[0]->data, buffers[0]->len) : 0;
	bool server = stream.channel == atomic_load(&f->server_channel);
	struct method *m = NULL;
	if (server && kind) {
		CHECK(stream.slot < CAP &&
		      f->stream_method[stream.slot] < METHODS);
		unsigned index = f->stream_method[stream.slot];
		m = &f->methods[index];
		if (kind == TR_RPC_RECORD_FINAL_STATUS) {
			atomic_fetch_add(&m->final_attempts, 1);
			if (atomic_load(&f->blocked_final_method) == (int)index)
				return TR_AGAIN;
		}
	}
	struct replacement_prefix *replacement = NULL;
	struct tr_buffer *patched[1];
	if (!server && kind == TR_RPC_RECORD_OPEN &&
	    atomic_load(&f->clear_open_deadline)) {
		/* 保留真实 Client timeout，线上改为无限；否则服务端自己的期限
		 * 可能先取消任务，掩盖 Client 忘记发送
		 * RESET。成功才接管原前缀。 */
		CHECK(count == 1 && tr_get_le64(buffers[0]->data + 40) > 0);
		size_t size = buffers[0]->len;
		replacement = malloc(sizeof(*replacement) + size);
		CHECK(replacement);
		replacement->fixture = f;
		replacement->buffer =
		        (struct tr_buffer){.data = replacement->data,
		                           .capacity = (uint32_t)size,
		                           .len = (uint32_t)size,
		                           .release_cb = prefix_release};
		memcpy(replacement->data, buffers[0]->data, size);
		tr_put_le64(replacement->data + 40, 0);
		patched[0] = &replacement->buffer;
		atomic_fetch_add(&f->prefixes, 1);
	}
	int ret = __real_tr_stream_sendv(
	        stream, replacement ? patched : buffers, count);
	if (replacement) {
		if (ret == TR_OK) {
			atomic_store(&f->clear_open_deadline, false);
			atomic_fetch_add(&f->prefixes_accepted, 1);
			tr_buffer_release(buffers[0]);
		} else {
			tr_buffer_release(&replacement->buffer);
		}
	}
	if (m && ret == TR_OK) {
		if (kind == TR_RPC_RECORD_MESSAGE)
			atomic_fetch_add(&m->messages_sent, 1);
		else if (kind == TR_RPC_RECORD_FINAL_STATUS)
			atomic_fetch_add(&m->finals_sent, 1);
	}
	return ret;
}
int __real_tr_stream_close(struct tr_stream_handle);
int __wrap_tr_stream_close(struct tr_stream_handle stream)
{
	struct fixture *f = active;
	int ret = __real_tr_stream_close(stream);
	if (f && stream.channel == atomic_load(&f->server_channel) &&
	    ret == TR_OK) {
		CHECK(stream.slot < CAP &&
		      f->stream_method[stream.slot] < METHODS);
		atomic_fetch_add(
		        &f->methods[f->stream_method[stream.slot]].closes, 1);
	}
	return ret;
}
int __real_tr_stream_reset_on_owner(struct tr_stream_handle, int);
int __wrap_tr_stream_reset_on_owner(struct tr_stream_handle stream, int status)
{
	struct fixture *f = active;
	bool client = f && stream.channel == atomic_load(&f->client_channel);
	if (client) {
		atomic_fetch_add(&f->reset_attempts, 1);
		atomic_store(&f->reset_status, status);
		if (atomic_exchange(&f->fail_reset_once, false)) {
			atomic_fetch_add(&f->reset_rejections, 1);
			return TR_AGAIN;
		}
	}
	int ret = __real_tr_stream_reset_on_owner(stream, status);
	if (client && ret == TR_OK)
		atomic_fetch_add(&f->reset_accepted, 1);
	return ret;
}

static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct method *m = arg;
	unsigned kind = view->header.kind;
	CHECK(tr_task_in_worker_context() && !tr_reactor_in_owner_context());
	if (kind == TR_RPC_RECORD_OPEN)
		m->id = id;
	if (kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(view->header.decoded_size == BODY);
		atomic_fetch_add(&m->entered, 1);
		if (m->mode == WORK_CANCEL_GATE) {
			uint64_t end = now_ms() + WAIT_MS;
			while (!tr_task_cancel_requested(cancel)) {
				CHECK(now_ms() < end);
				pause_once();
			}
			atomic_fetch_add(&m->cancelled, 1);
		}
		if (m->mode != WORK_ECHO)
			gate_wait(&m->gate);
		/* 即使已观察取消也主动产生晚成功，真正的丢弃责任在完成路径。 */
		CHECK(tr_rpc_record_reply_append(reply, view->body, BODY) ==
		      TR_OK);
		atomic_fetch_add(&m->exited, 1);
	}
	atomic_fetch_add(&m->executed[kind], 1);
	return TR_RPC_STATUS_OK;
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
static void fixture_init(struct fixture *f, enum work_mode mode)
{
	CHECK(!active);
	*f = (struct fixture){0};
	active = f;
	atomic_store(&f->blocked_final_method, -1);
	for (unsigned i = 0; i < CAP; ++i)
		f->stream_method[i] = METHODS;
	for (unsigned i = 0; i < METHODS; ++i)
		gate_init(&f->methods[i].gate);
	f->methods[0].mode = mode;
	struct tr_server_config sc;
	struct tr_client_config cc;
	struct tr_facade_tuning tuning;
	tr_server_config_init(&sc);
	sc.shard_count = 1;
	sc.max_peers = 1;
	sc.keepalive_interval_ms = 0;
	sc.limits.max_calls = sc.limits.max_streams = CAP;
	sc.limits.max_methods = METHODS;
	sc.limits.max_frame_payload_bytes = 256;
	sc.limits.max_message_bytes = WIRE;
	sc.limits.initial_window_bytes = WIRE;
	sc.limits.window_update_threshold_bytes = 1;
	tr_client_config_init(&cc);
	cc.limits = sc.limits;
	cc.keepalive_interval_ms = 0;
	cc.connect_timeout_ms = 2000;
	tr_facade_tuning_init(&tuning);
	tuning.command_capacity = 128;
	tuning.tx_item_capacity = 64;
	tuning.control_tx_item_capacity = 32;
	tuning.rx_buffer_count = 32;
	tuning.reassembly_pool_count = 8;
	const struct tr_server_record_config sr = {
	        .endpoint = {.dispatch = {CAP, 8, 3, BODY * 3},
	                     .completion_limit = 2,
	                     .progress_limit = 2,
	                     .progress_period_ns = 1000000,
	                     .final_wait_ns = 1000000000},
	        .tasks = {2, 8},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	const struct tr_client_record_config cr = {.calls = {CAP, 2, 1000000},
	                                           .memory_budget_bytes =
	                                                   16 * 1024 * 1024};
	struct tr_rpc_dispatch_method methods[METHODS];
	struct tr_rpc_method_desc descriptors[METHODS];
	for (unsigned i = 0; i < METHODS; ++i) {
		methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {17, i + 1, TR_RPC_MANY, TR_RPC_MANY,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_CONTROL, BODY, BODY},
		        .work = business,
		        .arg = &f->methods[i]};
		descriptors[i] = methods[i].descriptor;
	}
	CHECK(tr_server_create_record(&sc, &tuning, &sr, methods, METHODS,
	                              &f->server) == TR_OK);
	CHECK(tr_client_create_record(&cc, &tuning, &cr, descriptors, METHODS,
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

struct call {
	struct fixture *fixture;
	unsigned method, tag;
	struct tr_rpc_record_client_handle handle;
	_Atomic unsigned writable, messages, finals, terminals;
	enum tr_rpc_status status;
};
static uint8_t pattern(unsigned tag, unsigned offset)
{
	return (uint8_t)(tag * 19 + offset * 7 + 3);
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
			CHECK(view->body[i] == pattern(c->tag, i));
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
	CHECK(tr_reactor_in_owner_context());
	CHECK(delivery == TR_RPC_RECORD_ON_OWNER);
	c->status = status;
	CHECK(atomic_fetch_add(&c->terminals, 1) == 0);
}
static void call_start(struct call *c, struct fixture *f, unsigned method,
                       unsigned tag, uint32_t timeout_ms)
{
	*c = (struct call){.fixture = f, .method = method, .tag = tag};
	const struct tr_rpc_record_client_callbacks callbacks = {
	        client_received, client_writable, client_terminal, c};
	const struct tr_rpc_call_options options = {.timeout_ms = timeout_ms};
	unsigned opens =
	        atomic_load(&f->methods[method].executed[TR_RPC_RECORD_OPEN]);
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_call_start(f->client, 17, method + 1,
	                                          &options, &callbacks,
	                                          &c->handle)) == TR_AGAIN) {
		CHECK(!atomic_load(&c->terminals) && now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	wait_count(&c->writable, 1);
	wait_count(&f->methods[method].executed[TR_RPC_RECORD_OPEN], opens + 1);
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
static void call_message(struct call *c)
{
	struct body *b = malloc(sizeof(*b));
	CHECK(b);
	b->fixture = c->fixture;
	b->buffer = (struct tr_buffer){.data = b->data,
	                               .capacity = BODY,
	                               .len = BODY,
	                               .release_cb = body_release};
	for (unsigned i = 0; i < BODY; ++i)
		b->data[i] = pattern(c->tag, i);
	atomic_fetch_add(&c->fixture->bodies, 1);
	struct tr_buffer *buffer = &b->buffer;
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_send(c->handle, &buffer)) == TR_AGAIN) {
		CHECK(buffer == &b->buffer && now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK && !buffer);
}
static void call_half_close(struct call *c)
{
	struct method *m = &c->fixture->methods[c->method];
	unsigned received = atomic_load(&m->received[TR_RPC_RECORD_HALF_CLOSE]);
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_half_close(c->handle)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	/* 本地 TX 接受不足以排除迟到 HALF_CLOSE 抢先选择终局。 */
	wait_count(&m->received[TR_RPC_RECORD_HALF_CLOSE], received + 1);
	CHECK(tr_reactor_call(owner(c->fixture, true), barrier, NULL) == TR_OK);
}
static void call_finish(struct call *c, enum tr_rpc_status status,
                        unsigned messages, unsigned finals)
{
	wait_count(&c->terminals, 1);
	CHECK(c->status == status && atomic_load(&c->terminals) == 1);
	CHECK(atomic_load(&c->messages) == messages);
	CHECK(atomic_load(&c->finals) == finals);
}
static void call_retired(struct call *c)
{
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_cancel(c->handle)) == TR_ERR_STATE) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_ERR_STALE);
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
static void wait_server_calls(struct fixture *f, size_t calls)
{
	struct snapshot s = {.fixture = f};
	uint64_t end = now_ms() + WAIT_MS;
	for (;;) {
		CHECK(tr_reactor_call(owner(f, true), snapshot_on_owner, &s) ==
		      TR_OK);
		if (s.stats.active_calls == calls && !s.stats.held_events &&
		    !s.stats.submitted_calls && !s.stats.reply_calls &&
		    !s.stats.terminal_calls)
			return;
		CHECK(now_ms() < end);
		pause_once();
	}
}
struct stale_request {
	struct tr_rpc_dispatch_handle handle;
};
static int stale_on_owner(void *arg)
{
	struct stale_request *request = arg;
	return tr_rpc_record_dispatch_abort_on_owner(request->handle,
	                                             TR_RPC_STATUS_CANCELLED);
}
static void check_stale(struct call *c, struct tr_rpc_dispatch_id server_id)
{
	struct fixture *f = c->fixture;
	CHECK(tr_client_record_cancel(c->handle) == TR_ERR_STALE);
	CHECK(tr_client_record_half_close(c->handle) == TR_ERR_STALE);
	struct tr_buffer *empty = NULL;
	CHECK(tr_client_record_send(c->handle, &empty) == TR_ERR_STALE &&
	      !empty);
	struct stale_request request = {{atomic_load(&f->dispatch), server_id}};
	CHECK(tr_reactor_call(owner(f, true), stale_on_owner, &request) ==
	      TR_ERR_STALE);
}
static void fixture_idle(struct fixture *f)
{
	wait_server_calls(f, 0);
	CHECK(tr_reactor_call(owner(f, false), barrier, NULL) == TR_OK);
	uint64_t end = now_ms() + WAIT_MS;
	while (atomic_load(&f->client_budget->current_bytes) !=
	               f->client_idle_bytes ||
	       atomic_load(&f->server_budget->current_bytes) !=
	               f->server_idle_bytes) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(atomic_load(&f->client_creates) == 1);
	CHECK(atomic_load(&f->server_creates) == 1);
}
static void fixture_destroy(struct fixture *f)
{
	CHECK(tr_client_destroy(f->client) == TR_OK);
	f->client = NULL;
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
	f->server = NULL;
	CHECK(atomic_load(&f->bodies) == atomic_load(&f->bodies_released));
	CHECK(atomic_load(&f->prefixes) == atomic_load(&f->prefixes_released));
	for (unsigned i = 0; i < METHODS; ++i) {
		CHECK(!pthread_cond_destroy(&f->methods[i].gate.cond));
		CHECK(!pthread_mutex_destroy(&f->methods[i].gate.lock));
	}
	active = NULL;
}

static void test_half_closed_remote_cancel(void)
{
	struct fixture f;
	struct call cancelled, parallel, following;
	fixture_init(&f, WORK_CANCEL_GATE);
	call_start(&cancelled, &f, 0, 11, 0);
	call_message(&cancelled);
	call_half_close(&cancelled);
	wait_count(&f.methods[0].entered, 1);
	CHECK(!atomic_load(&f.methods[0].wire_timeout));
	struct tr_rpc_dispatch_id old_id = f.methods[0].id;
	call_start(&parallel, &f, 1, 29, 0);
	call_message(&parallel);
	wait_count(&parallel.messages, 1);
	atomic_store(&f.fail_reset_once, true);
	CHECK(tr_client_record_cancel(cancelled.handle) == TR_OK);
	call_finish(&cancelled, TR_RPC_STATUS_CANCELLED, 0, 0);
	wait_count(&f.methods[0].cancelled, 1);
	wait_count(&f.reset_accepted, 1);
	CHECK(atomic_load(&f.reset_rejections) == 1);
	CHECK(atomic_load(&f.reset_accepted) >= 1);
	CHECK(atomic_load(&f.reset_status) == TR_ERR_CLOSED);
	CHECK(!atomic_load(&f.methods[0].exited));
	CHECK(!atomic_load(&f.methods[0].terminals));
	struct snapshot s = {.fixture = &f};
	CHECK(tr_reactor_call(owner(&f, true), snapshot_on_owner, &s) == TR_OK);
	CHECK(s.stats.active_calls == 2 && s.stats.submitted_calls == 1);
	/* A 仍借用 view/reply，另一 worker 和同一连接上的 B 必须持续工作。 */
	call_message(&parallel);
	wait_count(&parallel.messages, 2);
	gate_release(&f.methods[0].gate);
	wait_count(&f.methods[0].terminals, 1);
	CHECK(f.methods[0].status == TR_RPC_STATUS_CANCELLED);
	CHECK(!f.methods[0].final_accepted);
	CHECK(!atomic_load(&f.methods[0].messages_sent));
	CHECK(!atomic_load(&f.methods[0].executed[TR_RPC_RECORD_HALF_CLOSE]));
	call_retired(&cancelled);
	wait_server_calls(&f, 1);
	/* B 占据另一槽，因此 C 必须复用 A 的唯一空槽，不能借额外容量通过。 */
	call_start(&following, &f, 2, 47, 0);
	CHECK(following.handle.slot == cancelled.handle.slot);
	CHECK(following.handle.generation != cancelled.handle.generation);
	CHECK(f.methods[2].id.slot == old_id.slot);
	CHECK(f.methods[2].id.generation != old_id.generation);
	check_stale(&cancelled, old_id);
	call_message(&following);
	call_half_close(&following);
	call_finish(&following, TR_RPC_STATUS_OK, 1, 1);
	call_retired(&following);
	call_half_close(&parallel);
	call_finish(&parallel, TR_RPC_STATUS_OK, 2, 1);
	call_retired(&parallel);
	fixture_idle(&f);
	fixture_destroy(&f);
	call_finish(&cancelled, TR_RPC_STATUS_CANCELLED, 0, 0);
}

static void test_cancel_before_final_accept(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f, WORK_ECHO);
	atomic_store(&f.blocked_final_method, 0);
	call_start(&call, &f, 0, 61, 0);
	call_message(&call);
	call_half_close(&call);
	wait_count(&call.messages, 1);
	wait_count(&f.methods[0].final_attempts, 1);
	CHECK(!atomic_load(&f.methods[0].finals_sent));
	CHECK(tr_client_record_cancel(call.handle) == TR_OK);
	call_finish(&call, TR_RPC_STATUS_CANCELLED, 1, 0);
	wait_count(&f.methods[0].terminals, 1);
	CHECK(f.methods[0].status == TR_RPC_STATUS_CANCELLED);
	CHECK(!f.methods[0].final_accepted);
	atomic_store(&f.blocked_final_method, -1);
	call_retired(&call);
	fixture_idle(&f);
	CHECK(!atomic_load(&f.methods[0].finals_sent));
	fixture_destroy(&f);
	call_finish(&call, TR_RPC_STATUS_CANCELLED, 1, 0);
}

static int cancel_after_server_close(void *arg)
{
	struct call *c = arg;
	struct method *m = &c->fixture->methods[c->method];
	CHECK(tr_reactor_in_owner_context_for(owner(c->fixture, false)));
	/* 唯一受控 owner 暂停：本请求已在 Server 完整接收，响应总长度小于
	 * 初始窗口。Server 可以独立发送，不依赖被暂停 Client 的信用回补。 */
	gate_release(&m->gate);
	wait_count(&m->finals_sent, 1);
	wait_count(&m->closes, 1);
	CHECK(!atomic_load(&c->messages) && !atomic_load(&c->finals));
	return tr_client_record_cancel(c->handle);
}
static void test_crossed_final_and_cancel(void)
{
	struct fixture f;
	struct call crossed, following;
	fixture_init(&f, WORK_GATE);
	call_start(&crossed, &f, 0, 73, 0);
	call_message(&crossed);
	call_half_close(&crossed);
	wait_count(&f.methods[0].entered, 1);
	CHECK(tr_reactor_call(owner(&f, false), cancel_after_server_close,
	                      &crossed) == TR_OK);
	call_finish(&crossed, TR_RPC_STATUS_CANCELLED, 0, 0);
	wait_count(&f.methods[0].terminals, 1);
	CHECK(f.methods[0].status == TR_RPC_STATUS_OK);
	CHECK(f.methods[0].final_accepted);
	CHECK(atomic_load(&f.methods[0].messages_sent) == 1);
	call_retired(&crossed);
	wait_server_calls(&f, 0);
	/* 同一连接随后真实成功；Client 已接纳 FINAL 时取消不得新增 RESET。 */
	call_start(&following, &f, 1, 89, 0);
	call_message(&following);
	call_half_close(&following);
	call_finish(&following, TR_RPC_STATUS_OK, 1, 1);
	unsigned resets = atomic_load(&f.reset_attempts);
	int ret = tr_client_record_cancel(following.handle);
	CHECK(ret == TR_ERR_STATE || ret == TR_ERR_STALE);
	CHECK(atomic_load(&f.reset_attempts) == resets);
	call_retired(&following);
	fixture_idle(&f);
	fixture_destroy(&f);
	call_finish(&crossed, TR_RPC_STATUS_CANCELLED, 0, 0);
	call_finish(&following, TR_RPC_STATUS_OK, 1, 1);
}

static void test_local_deadline_resets_remote(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f, WORK_CANCEL_GATE);
	atomic_store(&f.clear_open_deadline, true);
	call_start(&call, &f, 0, 101, 1000);
	call_message(&call);
	call_half_close(&call);
	wait_count(&f.methods[0].entered, 1);
	CHECK(!atomic_load(&f.methods[0].wire_timeout));
	CHECK(atomic_load(&f.prefixes_accepted) == 1);
	call_finish(&call, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0, 0);
	wait_count(&f.methods[0].cancelled, 1);
	wait_count(&f.reset_accepted, 1);
	CHECK(atomic_load(&f.reset_status) == TR_ERR_TIMEOUT);
	CHECK(!atomic_load(&f.methods[0].terminals));
	int ret = tr_client_record_cancel(call.handle);
	CHECK(ret == TR_ERR_STATE || ret == TR_ERR_STALE);
	gate_release(&f.methods[0].gate);
	wait_count(&f.methods[0].terminals, 1);
	CHECK(f.methods[0].status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(!f.methods[0].final_accepted);
	CHECK(!atomic_load(&f.methods[0].messages_sent));
	call_retired(&call);
	fixture_idle(&f);
	fixture_destroy(&f);
	call_finish(&call, TR_RPC_STATUS_DEADLINE_EXCEEDED, 0, 0);
}

int main(void)
{
	test_half_closed_remote_cancel();
	puts("rpc_record_cancel: half-close, remote token and slot reuse "
	     "passed");
	test_cancel_before_final_accept();
	puts("rpc_record_cancel: pending FINAL cancellation passed");
	test_crossed_final_and_cancel();
	puts("rpc_record_cancel: accepted FINAL/CLOSE crossing passed");
	test_local_deadline_resets_remote();
	puts("rpc_record_cancel: local deadline reaches remote task passed");
	return 0;
}
