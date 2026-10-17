#define _POSIX_C_SOURCE 200809L
#include "../src/client_record_internal.h"
#include "../src/server_record_internal.h"
#include "../src/endian.h"
#include "../src/execution/reactor_internal.h"
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

#define CHECK(c)                                                               \
	do {                                                                   \
		if (!(c)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #c);                                           \
			abort();                                               \
		}                                                              \
	} while (0)

enum { CALLS = 4, BODY = 4097, WIRE_LIMIT = 8192, WAIT_MS = 15000 };
enum { OBSERVED_CHANNELS = 8, HELD_PAYLOADS = 8, EXPECTED_MESSAGES = 4 };

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

struct channel_observation {
	struct tr_channel *channel;
	struct tr_conn_handle connection;
	tr_stream_data_cb data;
	tr_stream_event_cb stream_event;
	tr_channel_event_cb channel_event;
	void *arg;
	bool shimmed;
};

struct held_payload {
	struct tr_stream_handle stream;
	struct tr_buffer *payload;
};

/* 仅在完整销毁前一组门面后重置。连接与有界保留表用锁发布，回调
 * 函数仅在对应 owner 安装一次，销毁完成前不复用其存储。 */
static pthread_mutex_t observation_lock = PTHREAD_MUTEX_INITIALIZER;
static struct channel_observation observations[OBSERVED_CHANNELS];
static unsigned observation_count;
static struct held_payload held[HELD_PAYLOADS];
static unsigned held_count;
static struct tr_stream_handle last_client_stream;
static _Atomic(struct tr_channel *) client_channel, server_channel;
static _Atomic bool fail_client_create, fail_client_destroy, hold_credit;
static _Atomic unsigned held_published, client_creates, client_destroys;
static _Atomic unsigned create_failures, destroy_failures;
static _Atomic unsigned attempts[6], accepted[6], fake_again, real_again;
static _Atomic unsigned server_messages_accepted, server_half_closes_received;
static _Atomic uint64_t server_message_bytes;
static _Atomic int fail_kind, block_kind;
static _Atomic unsigned clock_failures;
static _Atomic bool clear_open_deadline;
static _Atomic unsigned deadline_prefixes, deadline_prefixes_accepted,
        deadline_prefixes_released;
static _Thread_local bool fail_clock_once;

int __real_clock_gettime(clockid_t, struct timespec *);
int __wrap_clock_gettime(clockid_t clock, struct timespec *time)
{
	if (clock == CLOCK_MONOTONIC && fail_clock_once) {
		fail_clock_once = false;
		atomic_fetch_add(&clock_failures, 1);
		errno = EIO;
		return -1;
	}
	return __real_clock_gettime(clock, time);
}

static unsigned record_kind(const struct tr_buffer *buffer)
{
	if (!buffer || buffer->len < TR_RPC_RECORD_HEADER_SIZE ||
	    memcmp(buffer->data, "TRPC", 4))
		return 0;
	unsigned kind = tr_get_le16(buffer->data + 6);
	return kind <= TR_RPC_RECORD_FINAL_STATUS ? kind : 0;
}

static void reset_observation(void)
{
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	CHECK(!held_count);
	memset(observations, 0, sizeof(observations));
	observation_count = 0;
	last_client_stream = (struct tr_stream_handle){0};
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	atomic_store(&client_channel, NULL);
	atomic_store(&server_channel, NULL);
	atomic_store(&fail_client_create, false);
	atomic_store(&fail_client_destroy, false);
	atomic_store(&hold_credit, false);
	atomic_store(&held_published, 0);
	atomic_store(&client_creates, 0);
	atomic_store(&client_destroys, 0);
	atomic_store(&create_failures, 0);
	atomic_store(&destroy_failures, 0);
	atomic_store(&fake_again, 0);
	atomic_store(&real_again, 0);
	atomic_store(&server_messages_accepted, 0);
	atomic_store(&server_half_closes_received, 0);
	atomic_store(&server_message_bytes, 0);
	atomic_store(&fail_kind, 0);
	atomic_store(&block_kind, 0);
	atomic_store(&clock_failures, 0);
	atomic_store(&clear_open_deadline, false);
	atomic_store(&deadline_prefixes, 0);
	atomic_store(&deadline_prefixes_accepted, 0);
	atomic_store(&deadline_prefixes_released, 0);
	for (unsigned i = 0; i < 6; ++i) {
		atomic_store(&attempts[i], 0);
		atomic_store(&accepted[i], 0);
	}
}

int __real_tr_channel_create_deferred(const struct tr_channel_config *,
                                      struct tr_conn_handle,
                                      struct tr_conn_handle, tr_stream_data_cb,
                                      tr_stream_event_cb, tr_channel_event_cb,
                                      void *, struct tr_channel **);
int __wrap_tr_channel_create_deferred(const struct tr_channel_config *config,
                                      struct tr_conn_handle control,
                                      struct tr_conn_handle bulk,
                                      tr_stream_data_cb data,
                                      tr_stream_event_cb stream_event,
                                      tr_channel_event_cb channel_event,
                                      void *arg, struct tr_channel **out)
{
	if (config->role == TR_CHANNEL_CLIENT &&
	    atomic_exchange(&fail_client_create, false)) {
		atomic_fetch_add(&create_failures, 1);
		return TR_ERR_NOMEM;
	}
	int ret = __real_tr_channel_create_deferred(config, control, bulk, data,
	                                            stream_event, channel_event,
	                                            arg, out);
	if (ret != TR_OK)
		return ret;
	CHECK(tr_reactor_in_owner_context_for(control.reactor));
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	CHECK(observation_count < OBSERVED_CHANNELS);
	observations[observation_count++] = (struct channel_observation){
	        .channel = *out, .connection = control};
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	if (config->role == TR_CHANNEL_CLIENT) {
		atomic_fetch_add(&client_creates, 1);
		atomic_store(&client_channel, *out);
	} else {
		atomic_store(&server_channel, *out);
	}
	return TR_OK;
}

int __real_tr_channel_destroy(struct tr_channel *);
int __wrap_tr_channel_destroy(struct tr_channel *channel)
{
	bool selected = channel == atomic_load(&client_channel);
	if (selected && atomic_exchange(&fail_client_destroy, false)) {
		atomic_fetch_add(&destroy_failures, 1);
		return TR_ERR_SYS;
	}
	int ret = __real_tr_channel_destroy(channel);
	if (ret == TR_OK && selected) {
		atomic_fetch_add(&client_destroys, 1);
		atomic_store(&client_channel, NULL);
	}
	return ret;
}

struct open_deadline_prefix {
	struct tr_buffer buffer;
	uint8_t bytes[];
};

static void release_open_deadline_prefix(struct tr_buffer *buffer)
{
	atomic_fetch_add(&deadline_prefixes_released, 1);
	free(buffer);
}

int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	unsigned kind = stream.channel == atomic_load(&client_channel) && count
	                        ? record_kind(buffers[0])
	                        : 0;
	unsigned server_kind =
	        stream.channel == atomic_load(&server_channel) && count
	                ? record_kind(buffers[0])
	                : 0;
	uint64_t server_bytes = 0;
	if (server_kind == TR_RPC_RECORD_MESSAGE)
		for (uint32_t i = 0; i < count; ++i)
			server_bytes += buffers[i]->len;
	if (kind) {
		atomic_fetch_add(&attempts[kind], 1);
		int once = (int)kind;
		if (atomic_load(&block_kind) == (int)kind ||
		    atomic_compare_exchange_strong(&fail_kind, &once, 0)) {
			atomic_fetch_add(&fake_again, 1);
			return TR_AGAIN;
		}
	}
	struct open_deadline_prefix *replacement = NULL;
	struct tr_buffer *patched[1];
	if (kind == TR_RPC_RECORD_OPEN && atomic_load(&clear_open_deadline)) {
		/* 仅用于本地超时后的晚响应排空场景：模拟不执行 OPEN 期限的
		 * 对端，保留真实 Client 本地计时。不能修改已交给传输的存储，
		 * 也不能在发送失败时释放原前缀。 */
		CHECK(count == 1 && tr_get_le64(buffers[0]->data + 40) > 0);
		size_t size = buffers[0]->len;
		replacement = malloc(sizeof(*replacement) + size);
		CHECK(replacement);
		replacement->buffer = (struct tr_buffer){
		        .data = replacement->bytes,
		        .len = (uint32_t)size,
		        .capacity = (uint32_t)size,
		        .release_cb = release_open_deadline_prefix};
		memcpy(replacement->bytes, buffers[0]->data, size);
		tr_put_le64(replacement->bytes + 40, 0);
		patched[0] = &replacement->buffer;
		atomic_fetch_add(&deadline_prefixes, 1);
	}
	int ret = __real_tr_stream_sendv(
	        stream, replacement ? patched : buffers, count);
	if (replacement) {
		if (ret == TR_OK) {
			atomic_store(&clear_open_deadline, false);
			atomic_fetch_add(&deadline_prefixes_accepted, 1);
			tr_buffer_release(buffers[0]);
		} else {
			tr_buffer_release(&replacement->buffer);
		}
	}
	if (server_kind == TR_RPC_RECORD_MESSAGE && ret == TR_OK) {
		atomic_fetch_add(&server_messages_accepted, 1);
		atomic_fetch_add(&server_message_bytes, server_bytes);
	}
	if (kind && ret == TR_OK) {
		CHECK(pthread_mutex_lock(&observation_lock) == 0);
		last_client_stream = stream;
		CHECK(pthread_mutex_unlock(&observation_lock) == 0);
		atomic_fetch_add(&accepted[kind], 1);
	} else if (kind && ret == TR_AGAIN) {
		atomic_fetch_add(&real_again, 1);
	}
	return ret;
}

static enum tr_stream_data_disposition
observed_data(struct tr_stream_handle stream, uint64_t id,
              struct tr_buffer *payload, void *arg)
{
	struct channel_observation *o = arg;
	unsigned kind = record_kind(payload);
	enum tr_stream_data_disposition disposition =
	        o->data ? o->data(stream, id, payload, o->arg)
	                : TR_STREAM_DATA_RELEASE;
	if (stream.channel == atomic_load(&server_channel) &&
	    kind == TR_RPC_RECORD_HALF_CLOSE)
		atomic_fetch_add(&server_half_closes_received, 1);
	/* 控制记录立即归还信用；只保留已经交付原宿主的 MESSAGE，
	 * 既不阻塞 owner，也不改变服务端业务的接收与完成顺序。 */
	if (disposition == TR_STREAM_DATA_RELEASE &&
	    atomic_load(&hold_credit) &&
	    stream.channel == atomic_load(&server_channel) &&
	    kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(pthread_mutex_lock(&observation_lock) == 0);
		CHECK(held_count < HELD_PAYLOADS);
		held[held_count++] = (struct held_payload){stream, payload};
		atomic_store(&held_published, held_count);
		CHECK(pthread_mutex_unlock(&observation_lock) == 0);
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return disposition;
}

static void observed_stream_event(struct tr_stream_handle stream,
                                  enum tr_stream_event event, int status,
                                  void *arg)
{
	struct channel_observation *o = arg;
	if (o->stream_event)
		o->stream_event(stream, event, status, o->arg);
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
                                  tr_stream_event_cb stream_event,
                                  tr_channel_event_cb channel_event, void *arg)
{
	if (channel != atomic_load(&server_channel) || !data)
		return __real_tr_channel_set_handler(
		        channel, data, stream_event, channel_event, arg);
	CHECK(tr_reactor_in_owner_context_for(tr_channel_reactor(channel)));
	struct channel_observation *o = NULL;
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	for (unsigned i = 0; i < observation_count; ++i)
		if (observations[i].channel == channel)
			o = &observations[i];
	CHECK(o && !o->shimmed);
	o->data = data;
	o->stream_event = stream_event;
	o->channel_event = channel_event;
	o->arg = arg;
	o->shimmed = true;
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	return __real_tr_channel_set_handler(channel, observed_data,
	                                     observed_stream_event,
	                                     observed_channel_event, o);
}

int __real_tr_stream_release_payload(struct tr_stream_handle,
                                     struct tr_buffer *);
int __wrap_tr_stream_release_payload(struct tr_stream_handle stream,
                                     struct tr_buffer *payload)
{
	return __real_tr_stream_release_payload(stream, payload);
}

static int release_credit_on_owner(void *arg)
{
	struct tr_channel *channel = arg;
	struct held_payload pending[HELD_PAYLOADS];
	unsigned count;
	CHECK(tr_reactor_in_owner_context_for(tr_channel_reactor(channel)));
	atomic_store(&hold_credit, false);
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	count = held_count;
	memcpy(pending, held, count * sizeof(*pending));
	held_count = 0;
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	for (unsigned i = 0; i < count; ++i) {
		CHECK(pending[i].stream.channel == channel);
		/* 旧流代次只释放正文，不给复用槽错误增加信用。AGAIN 表示
		 * 归还已完成、WINDOW_UPDATE 由原 Channel 后台继续推进。 */
		int ret = __real_tr_stream_release_payload(pending[i].stream,
		                                           pending[i].payload);
		CHECK(ret == TR_OK || ret == TR_AGAIN || ret == TR_ERR_STALE);
	}
	atomic_store(&held_published, 0);
	return TR_OK;
}

struct fixture {
	struct tr_server *server;
	struct tr_client *client;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server_record_config server_record;
	struct tr_client_record_config client_record;
	struct tr_rpc_dispatch_method server_methods[2];
	struct tr_rpc_method_desc client_methods[3];
	struct gate gate;
	_Atomic bool block_work;
	_Atomic unsigned reply_copies;
	_Atomic unsigned calls, messages, half_closes, bodies_released;
	uint8_t expected_metadata[32];
	unsigned expected_metadata_size;
	uint16_t port;
};

static enum tr_rpc_status business(struct tr_rpc_dispatch_id id,
                                   const struct tr_rpc_record_view *view,
                                   const struct tr_task_cancel *cancel,
                                   struct tr_rpc_record_reply *reply, void *arg)
{
	struct fixture *f = arg;
	(void)id;
	(void)cancel;
	CHECK(tr_task_in_worker_context() && !tr_reactor_in_owner_context());
	if (view->header.kind == TR_RPC_RECORD_OPEN) {
		CHECK(view->header.metadata_size == f->expected_metadata_size);
		if (f->expected_metadata_size)
			CHECK(!memcmp(view->metadata, f->expected_metadata,
			              f->expected_metadata_size));
		atomic_fetch_add(&f->calls, 1);
	} else if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		/* 先快照再进入闸门；测试可为后续调用恢复单回复，不改变
		 * 当前工作线程已经持有的迟到结果批次。 */
		unsigned copies = atomic_load(&f->reply_copies);
		if (atomic_load(&f->block_work))
			gate_wait(&f->gate, 0, true);
		/* 本地取消不承诺撤销远端工作，故主动产生迟到的正常结果。 */
		for (unsigned i = 0; i < copies; ++i)
			CHECK(tr_rpc_record_reply_append(
			              reply, view->body,
			              view->header.decoded_size) == TR_OK);
		atomic_fetch_add(&f->messages, 1);
	} else if (view->header.kind == TR_RPC_RECORD_HALF_CLOSE) {
		atomic_fetch_add(&f->half_closes, 1);
	}
	return TR_RPC_STATUS_OK;
}

static void fixture_init(struct fixture *f)
{
	*f = (struct fixture){0};
	reset_observation();
	gate_init(&f->gate);
	atomic_init(&f->block_work, false);
	atomic_init(&f->reply_copies, 1);
	atomic_init(&f->calls, 0);
	atomic_init(&f->messages, 0);
	atomic_init(&f->half_closes, 0);
	atomic_init(&f->bodies_released, 0);
	tr_server_config_init(&f->server_config);
	f->server_config.shard_count = 1;
	f->server_config.max_peers = 4;
	f->server_config.limits.max_streams = CALLS;
	f->server_config.limits.max_calls = CALLS;
	f->server_config.limits.max_methods = 3;
	f->server_config.limits.max_frame_payload_bytes = 256;
	f->server_config.limits.max_message_bytes = WIRE_LIMIT;
	f->server_config.limits.initial_window_bytes = WIRE_LIMIT * 4;
	f->server_config.limits.window_update_threshold_bytes = 1;
	f->server_config.keepalive_interval_ms = 0;
	f->server_config.handshake_timeout_ms = 2000;
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
	f->server_record = (struct tr_server_record_config){
	        .endpoint = {.dispatch = {CALLS, 8, 2, WIRE_LIMIT},
	                     .completion_limit = 2,
	                     .progress_limit = 2,
	                     .progress_period_ns = 1000000,
	                     .final_wait_ns = 1000000000},
	        .tasks = {2, 16},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	f->client_record = (struct tr_client_record_config){
	        .calls = {CALLS, 2, 1000000},
	        .memory_budget_bytes = 16 * 1024 * 1024};
	for (unsigned i = 0; i < 2; ++i) {
		f->server_methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {17, i + 1, i ? TR_RPC_MANY : TR_RPC_ONE,
		                       i ? TR_RPC_MANY : TR_RPC_ONE,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_CONTROL, BODY, BODY},
		        .work = business,
		        .arg = f};
		f->client_methods[i] = f->server_methods[i].descriptor;
	}
	f->client_methods[2] = f->client_methods[0];
	f->client_methods[2].method_id = 99;
}

static void fixture_create(struct fixture *f)
{
	struct tr_rpc_dispatch_method server_copy[2];
	struct tr_rpc_method_desc client_copy[3];
	memcpy(server_copy, f->server_methods, sizeof(server_copy));
	memcpy(client_copy, f->client_methods, sizeof(client_copy));
	CHECK(tr_server_create_record(&f->server_config, &f->tuning,
	                              &f->server_record, server_copy, 2,
	                              &f->server) == TR_OK);
	CHECK(tr_client_create_record(&f->client_config, &f->tuning,
	                              &f->client_record, client_copy, 3,
	                              &f->client) == TR_OK);
	/* 两端均须冻结输入表，连接之后不再借用构造时的栈存储。 */
	memset(server_copy, 0, sizeof(server_copy));
	memset(client_copy, 0, sizeof(client_copy));
	CHECK(tr_server_listen(f->server, "127.0.0.1", 0, &f->port) == TR_OK);
	CHECK(tr_server_start(f->server) == TR_OK);
}

static void fixture_connect(struct fixture *f)
{
	CHECK(tr_client_connect(f->client, "127.0.0.1", f->port) == TR_OK);
	CHECK(tr_client_wait_ready(f->client, WAIT_MS) == TR_OK);
}

static void fixture_destroy(struct fixture *f)
{
	CHECK(!atomic_load(&held_published));
	gate_release(&f->gate);
	if (f->client) {
		CHECK(tr_client_destroy(f->client) == TR_OK);
		f->client = NULL;
	}
	if (f->server) {
		uint64_t end = now_ms() + WAIT_MS;
		int ret;
		while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
			CHECK(now_ms() < end);
			pause_once();
		}
		CHECK(ret == TR_OK);
		f->server = NULL;
	}
	CHECK(atomic_load(&client_creates) == atomic_load(&client_destroys));
	CHECK(atomic_load(&deadline_prefixes) ==
	      atomic_load(&deadline_prefixes_released));
	CHECK(pthread_cond_destroy(&f->gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&f->gate.lock) == 0);
}

struct owned_body {
	struct tr_buffer buffer;
	_Atomic unsigned *released;
	uint8_t bytes[];
};

static void release_body(struct tr_buffer *buffer)
{
	struct owned_body *body = (struct owned_body *)buffer;
	_Atomic unsigned *released = body->released;
	free(body);
	atomic_fetch_add(released, 1);
}

static uint8_t pattern(unsigned tag, unsigned offset)
{
	return (uint8_t)(tag * 29 + offset * 5 + 7);
}

static struct tr_buffer *make_body(struct fixture *f, unsigned size,
                                   unsigned tag)
{
	struct owned_body *body = malloc(sizeof(*body) + size);
	CHECK(body);
	body->buffer = (struct tr_buffer){.data = body->bytes,
	                                  .capacity = size,
	                                  .len = size,
	                                  .release_cb = release_body};
	body->released = &f->bodies_released;
	for (unsigned i = 0; i < size; ++i)
		body->bytes[i] = pattern(tag, i);
	return &body->buffer;
}

enum callback_action {
	CALLBACK_OBSERVE,
	CALLBACK_SEND_CLOSE,
	CALLBACK_CANCEL,
	CALLBACK_CANCEL_WRITABLE
};

struct expected_message {
	unsigned size, tag;
};

struct call_result {
	struct expected_message expected[EXPECTED_MESSAGES];
	unsigned expected_count;
	_Atomic unsigned messages, initial, final_records, terminals, writable;
	_Atomic unsigned callback_depth, self_checks, arg_freed;
	enum tr_rpc_status status;
	enum tr_rpc_record_delivery delivery;
};

struct call_context {
	struct fixture *fixture;
	struct call_result *result;
	enum callback_action action;
	bool free_on_terminal;
};

struct call {
	struct call_context context;
	struct call_result result;
	/* out 独立于可在终局回调里释放的 arg 存储。 */
	struct tr_rpc_record_client_handle handle;
};

static void received(struct tr_rpc_record_client_handle,
                     const struct tr_rpc_record_view *, void *);
static void writable(struct tr_rpc_record_client_handle, void *);
static void terminal(struct tr_rpc_record_client_handle, enum tr_rpc_status,
                     enum tr_rpc_record_delivery, void *);

static struct tr_rpc_record_client_callbacks callbacks(void *arg)
{
	return (struct tr_rpc_record_client_callbacks){.received = received,
	                                               .writable = writable,
	                                               .terminal = terminal,
	                                               .arg = arg};
}

static void callback_self_checks(struct call_context *context)
{
	struct tr_client *client = context->fixture->client;
	struct tr_rpc_record_client_handle untouched, sentinel;
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	struct tr_rpc_record_client_callbacks cb = callbacks(context);
	CHECK(tr_client_wait_ready(client, 1) == TR_ERR_STATE);
	CHECK(tr_client_wait_drained(client, 1) == TR_ERR_STATE);
	CHECK(tr_client_destroy(client) == TR_ERR_STATE);
	CHECK(tr_server_destroy(context->fixture->server) == TR_ERR_STATE);
	CHECK(tr_server_drain(context->fixture->server, 1) == TR_ERR_STATE);
	CHECK(tr_client_record_call_start(client, 17, 2, NULL, &cb,
	                                  &untouched) == TR_ERR_STATE);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	atomic_fetch_add(&context->result->self_checks, 1);
}

static void received(struct tr_rpc_record_client_handle handle,
                     const struct tr_rpc_record_view *view, void *arg)
{
	struct call_context *context = arg;
	struct call_result *result = context->result;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!atomic_load(&result->terminals));
	CHECK(atomic_fetch_add(&result->callback_depth, 1) == 0);
	if (view->header.kind == TR_RPC_RECORD_INITIAL_METADATA) {
		CHECK(atomic_fetch_add(&result->initial, 1) == 0);
	} else if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(atomic_load(&result->initial) == 1);
		unsigned index = atomic_load(&result->messages);
		CHECK(index < result->expected_count);
		CHECK(view->header.decoded_size ==
		      result->expected[index].size);
		CHECK(view->header.encoded_size ==
		      result->expected[index].size);
		CHECK(view->header.codec_id == TR_RPC_CODEC_RAW);
		for (unsigned i = 0; i < result->expected[index].size; ++i)
			CHECK(view->body[i] ==
			      pattern(result->expected[index].tag, i));
		if (!result->expected[index].size)
			CHECK(!view->body);
		atomic_fetch_add(&result->messages, 1);
		if (!index && context->action != CALLBACK_OBSERVE) {
			callback_self_checks(context);
			if (context->action == CALLBACK_SEND_CLOSE) {
				struct tr_buffer *body =
				        make_body(context->fixture,
				                  result->expected[1].size,
				                  result->expected[1].tag);
				CHECK(tr_client_record_send(handle, &body) ==
				      TR_OK);
				CHECK(!body);
				CHECK(tr_client_record_half_close(handle) ==
				      TR_OK);
			} else {
				CHECK(tr_client_record_cancel(handle) == TR_OK);
				/* 当前 received 尚未退栈，terminal 不得提前调用
				 * 或释放其 arg。 */
				CHECK(!atomic_load(&result->terminals));
			}
		}
	} else {
		CHECK(view->header.kind == TR_RPC_RECORD_FINAL_STATUS);
		CHECK(atomic_fetch_add(&result->final_records, 1) == 0);
	}
	CHECK(atomic_fetch_sub(&result->callback_depth, 1) == 1);
}

static void writable(struct tr_rpc_record_client_handle handle, void *arg)
{
	struct call_context *context = arg;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!atomic_load(&context->result->terminals));
	atomic_fetch_add(&context->result->writable, 1);
	if (context->action == CALLBACK_CANCEL_WRITABLE) {
		CHECK(atomic_fetch_add(&context->result->callback_depth, 1) ==
		      0);
		callback_self_checks(context);
		CHECK(tr_client_record_cancel(handle) == TR_OK);
		CHECK(!atomic_load(&context->result->terminals));
		CHECK(atomic_fetch_sub(&context->result->callback_depth, 1) ==
		      1);
	}
}

static void terminal(struct tr_rpc_record_client_handle handle,
                     enum tr_rpc_status status,
                     enum tr_rpc_record_delivery delivery, void *arg)
{
	struct call_context *context = arg;
	struct call_result *result = context->result;
	(void)handle;
	CHECK(!atomic_load(&result->callback_depth));
	CHECK(!atomic_load(&result->terminals));
	if (delivery == TR_RPC_RECORD_ON_OWNER) {
		CHECK(tr_reactor_in_owner_context());
	} else {
		CHECK(delivery == TR_RPC_RECORD_OWNER_STOPPED);
		CHECK(!tr_reactor_in_owner_context());
		callback_self_checks(context);
	}
	result->status = status;
	result->delivery = delivery;
	if (context->free_on_terminal) {
		free(context);
		atomic_store(&result->arg_freed, 1);
	}
	CHECK(atomic_fetch_add(&result->terminals, 1) == 0);
}

static void call_init(struct call *call, struct fixture *fixture)
{
	*call = (struct call){0};
	call->context = (struct call_context){fixture, &call->result,
	                                      CALLBACK_OBSERVE, false};
	atomic_init(&call->result.messages, 0);
	atomic_init(&call->result.initial, 0);
	atomic_init(&call->result.final_records, 0);
	atomic_init(&call->result.terminals, 0);
	atomic_init(&call->result.writable, 0);
	atomic_init(&call->result.callback_depth, 0);
	atomic_init(&call->result.self_checks, 0);
	atomic_init(&call->result.arg_freed, 0);
}

static void expect_message(struct call *call, unsigned size, unsigned tag)
{
	CHECK(call->result.expected_count < EXPECTED_MESSAGES);
	call->result.expected[call->result.expected_count++] =
	        (struct expected_message){size, tag};
}

static void call_start(struct call *call, unsigned method,
                       const struct tr_rpc_call_options *options)
{
	struct tr_rpc_record_client_callbacks cb = callbacks(&call->context);
	struct tr_rpc_record_client_handle sentinel;
	memset(&call->handle, 0xa5, sizeof(call->handle));
	sentinel = call->handle;
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_call_start(call->context.fixture->client,
	                                          17, method, options, &cb,
	                                          &call->handle)) == TR_AGAIN) {
		CHECK(!memcmp(&call->handle, &sentinel, sizeof(sentinel)));
		CHECK(!atomic_load(&call->result.terminals));
		CHECK(!atomic_load(&call->result.writable));
		CHECK(!atomic_load(&call->result.initial));
		CHECK(!atomic_load(&call->result.messages));
		CHECK(!atomic_load(&call->result.final_records));
		CHECK(now_ms() < end);
		pause_once();
	}
	if (ret != TR_OK)
		fprintf(stderr,
		        "test_client_record: start method=%u status=%d\n",
		        method, ret);
	CHECK(ret == TR_OK);
}

static void call_ready(struct call *call)
{
	wait_count(&call->result.writable, 1);
	CHECK(!atomic_load(&call->result.terminals));
}

static void send_existing_body(struct call *call, struct tr_buffer **body)
{
	struct tr_buffer *original = *body;
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_send(call->handle, body)) == TR_AGAIN) {
		CHECK(*body == original);
		CHECK(now_ms() < end);
		pause_once();
	}
	if (ret != TR_OK)
		fprintf(stderr, "test_client_record: send status=%d\n", ret);
	CHECK(ret == TR_OK && !*body);
}

static void send_body(struct call *call, unsigned size, unsigned tag)
{
	struct tr_buffer *body = make_body(call->context.fixture, size, tag);
	send_existing_body(call, &body);
}

static void half_close(struct call *call)
{
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_half_close(call->handle)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
}

static void call_finish(struct call *call, enum tr_rpc_status status,
                        enum tr_rpc_record_delivery delivery)
{
	wait_count(&call->result.terminals, 1);
	if (call->result.status != status ||
	    call->result.delivery != delivery ||
	    atomic_load(&call->result.messages) != call->result.expected_count)
		fprintf(stderr,
		        "test_client_record: terminal status=%d expected=%d "
		        "delivery=%d expected=%d messages=%u expected=%u\n",
		        call->result.status, status, call->result.delivery,
		        delivery, atomic_load(&call->result.messages),
		        call->result.expected_count);
	CHECK(atomic_load(&call->result.terminals) == 1);
	CHECK(call->result.status == status);
	CHECK(call->result.delivery == delivery);
	CHECK(atomic_load(&call->result.messages) ==
	      call->result.expected_count);
	if (status == TR_RPC_STATUS_OK) {
		if (call->result.expected_count)
			CHECK(atomic_load(&call->result.initial) == 1);
		CHECK(atomic_load(&call->result.final_records) == 1);
	}
}

static void call_retired(struct call *call)
{
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_client_record_cancel(call->handle)) == TR_ERR_STATE) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_ERR_STALE);
}

static void echo(struct call *call, unsigned method, unsigned size,
                 unsigned tag)
{
	expect_message(call, size, tag);
	call_start(call, method, NULL);
	call_ready(call);
	send_body(call, size, tag);
	half_close(call);
	call_finish(call, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(call);
}

static void test_threshold_boundaries(struct fixture *f)
{
	struct tr_client_config client_config = f->client_config;
	struct tr_server_config server_config = f->server_config;
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;
	/* W=32768、传输消息上界8192时，累计归还阈值最多24577；
	 * 再加一会让合法消息消耗剩余窗口后永远等不到 WINDOW_UPDATE。
	 * 此边界由传输上界决定，不能用本地方法的4097字节代替。 */
	client_config.limits.window_update_threshold_bytes = 24578;
	CHECK(tr_client_create_record(&client_config, &f->tuning,
	                              &f->client_record, f->client_methods, 3,
	                              &client) == TR_ERR_INVALID);
	CHECK(!client);
	client_config.limits.window_update_threshold_bytes = 24577;
	CHECK(tr_client_create_record(&client_config, &f->tuning,
	                              &f->client_record, f->client_methods, 3,
	                              &client) == TR_OK);
	CHECK(tr_client_destroy(client) == TR_OK);
	server_config.limits.window_update_threshold_bytes = 24578;
	CHECK(tr_server_create_record(&server_config, &f->tuning,
	                              &f->server_record, f->server_methods, 2,
	                              &server) == TR_ERR_INVALID);
	CHECK(!server);
	server_config.limits.window_update_threshold_bytes = 24577;
	CHECK(tr_server_create_record(&server_config, &f->tuning,
	                              &f->server_record, f->server_methods, 2,
	                              &server) == TR_OK);
	uint64_t end = now_ms() + WAIT_MS;
	int ret;
	while ((ret = tr_server_destroy(server)) == TR_AGAIN) {
		CHECK(now_ms() < end);
		pause_once();
	}
	CHECK(ret == TR_OK);
}

/* 构造拒绝必须保持无回调、无所有权发布；fd 已接管后的装配失败
 * 完整回滚后，同一个 Client 仍能执行真实连接与调用。 */
static void test_configuration_and_rollback(void)
{
	struct fixture f;
	struct tr_client *out = NULL;
	fixture_init(&f);
	test_threshold_boundaries(&f);
	struct tr_client_record_config invalid = f.client_record;
	invalid.calls.capacity = CALLS + 1;
	CHECK(tr_client_create_record(&f.client_config, &f.tuning, &invalid,
	                              f.client_methods, 3,
	                              &out) == TR_ERR_INVALID);
	CHECK(!out);
	invalid = f.client_record;
	invalid.memory_budget_bytes = 0;
	CHECK(tr_client_create_record(&f.client_config, &f.tuning, &invalid,
	                              f.client_methods, 3,
	                              &out) == TR_ERR_INVALID);
	CHECK(!out);
	f.client_config.enable_reconnect = 1;
	CHECK(tr_client_create_record(&f.client_config, &f.tuning,
	                              &f.client_record, f.client_methods, 3,
	                              &out) == TR_ERR_UNSUPPORTED);
	CHECK(!out);
	f.client_config.enable_reconnect = 0;
	fixture_create(&f);
	CHECK(tr_client_register_method(f.client, &f.client_methods[0]) ==
	      TR_ERR_UNSUPPORTED);
	struct tr_rpc_semantic_stats stats, sentinel;
	memset(&stats, 0xa5, sizeof(stats));
	sentinel = stats;
	CHECK(tr_client_get_rpc_semantic_stats(f.client, &stats) ==
	      TR_ERR_UNSUPPORTED);
	CHECK(!memcmp(&stats, &sentinel, sizeof(stats)));
	atomic_store(&fail_client_create, true);
	CHECK(tr_client_connect(f.client, "127.0.0.1", f.port) == TR_ERR_NOMEM);
	CHECK(atomic_load(&create_failures) == 1);
	CHECK(!atomic_load(&client_creates));
	fixture_connect(&f);
	struct call call;
	call_init(&call, &f);
	echo(&call, 1, 31, 1);
	fixture_destroy(&f);
	CHECK(atomic_load(&call.result.terminals) == 1);
}

static void test_metadata_copy(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	const uint8_t expected[] = {5,   3,   0,   't', 'o', 'k',
	                            'e', 'n', 'a', 'b', 'c'};
	memcpy(f.expected_metadata, expected, sizeof(expected));
	f.expected_metadata_size = sizeof(expected);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	expect_message(&call, 32, 9);
	char key[] = "token";
	char value[] = "abc";
	struct tr_rpc_metadata metadata = {key, value, 3};
	struct tr_rpc_call_options options = {.metadata = &metadata,
	                                      .metadata_count = 1};
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	call_start(&call, 1, &options);
	wait_count(&fake_again, 1);
	CHECK(!atomic_load(&f.calls));
	/* OPEN 尚未接受时销毁原元数据内容，后续重试仍须使用有界副本。 */
	memset(key, 'x', sizeof(key));
	memset(value, 'y', sizeof(value));
	memset(&metadata, 0, sizeof(metadata));
	memset(&options, 0, sizeof(options));
	atomic_store(&block_kind, 0);
	call_ready(&call);
	send_body(&call, 32, 9);
	half_close(&call);
	call_finish(&call, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	fixture_destroy(&f);
	CHECK(atomic_load(&f.calls) == 1);
}

static void test_shapes_and_metadata(void)
{
	struct fixture f;
	struct call calls[4];
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	for (unsigned i = 0; i < 4; ++i)
		call_init(&calls[i], &f);
	echo(&calls[0], 1, 0, 2);
	echo(&calls[1], 1, BODY, 3);
	call_start(&calls[2], 2, NULL);
	call_ready(&calls[2]);
	half_close(&calls[2]);
	call_finish(&calls[2], TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(&calls[2]);
	expect_message(&calls[3], 17, 4);
	expect_message(&calls[3], 0, 5);
	expect_message(&calls[3], 512, 6);
	call_start(&calls[3], 2, NULL);
	call_ready(&calls[3]);
	send_body(&calls[3], 17, 4);
	send_body(&calls[3], 0, 5);
	send_body(&calls[3], 512, 6);
	half_close(&calls[3]);
	call_finish(&calls[3], TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	fixture_destroy(&f);
	CHECK(atomic_load(&f.calls) == 4);
	CHECK(atomic_load(&f.messages) == 5);
	CHECK(atomic_load(&f.half_closes) == 4);
	CHECK(atomic_load(&f.bodies_released) == 5);
	for (unsigned i = 0; i < 4; ++i)
		CHECK(atomic_load(&calls[i].result.terminals) == 1);
	test_metadata_copy();
}

static void test_unknown_method(void)
{
	struct fixture f;
	struct call rejected, following;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&rejected, &f);
	call_init(&following, &f);
	call_start(&rejected, 99, NULL);
	call_finish(&rejected, TR_RPC_STATUS_UNIMPLEMENTED,
	            TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&f.calls));
	CHECK(atomic_load(&rejected.result.final_records) == 1);
	call_retired(&rejected);
	echo(&following, 1, 63, 10);
	fixture_destroy(&f);
	CHECK(atomic_load(&rejected.result.terminals) == 1);
	CHECK(atomic_load(&f.calls) == 1);
}

static void test_early_rejection(void)
{
	test_unknown_method();
	struct fixture f;
	struct call occupied, rejected, following;
	fixture_init(&f);
	f.server_record.endpoint.dispatch.capacity = 1;
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&occupied, &f);
	call_init(&rejected, &f);
	call_init(&following, &f);
	expect_message(&occupied, 127, 11);
	call_start(&occupied, 2, NULL);
	call_ready(&occupied);
	send_body(&occupied, 127, 11);
	half_close(&occupied);
	gate_wait(&f.gate, 1, false);
	unsigned opens = atomic_load(&accepted[TR_RPC_RECORD_OPEN]);
	CHECK(opens == 1);
	/* Stream 准入已到达 Server，但第二个调用的 V2 OPEN 始终未
	 * 接受；原样非 OK FINAL 必须穿过 Client 的未 OPEN 状态。 */
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	call_start(&rejected, 2, NULL);
	call_finish(&rejected, TR_RPC_STATUS_RESOURCE_EXHAUSTED,
	            TR_RPC_RECORD_ON_OWNER);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_OPEN]) == opens);
	CHECK(atomic_load(&f.calls) == 1);
	CHECK(atomic_load(&rejected.result.final_records) == 1);
	CHECK(!atomic_load(&rejected.result.initial));
	atomic_store(&block_kind, 0);
	call_retired(&rejected);
	gate_release(&f.gate);
	call_finish(&occupied, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(&occupied);
	echo(&following, 1, 71, 12);
	fixture_destroy(&f);
	CHECK(atomic_load(&f.calls) == 2);
	CHECK(atomic_load(&f.messages) == 2);
	CHECK(atomic_load(&rejected.result.terminals) == 1);
}

static void test_stale_handle(void)
{
	struct fixture f;
	struct call old, current, unaccepted;
	fixture_init(&f);
	f.client_record.calls.capacity = 1;
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&old, &f);
	call_init(&current, &f);
	call_init(&unaccepted, &f);
	echo(&old, 1, 23, 20);
	expect_message(&current, 257, 21);
	call_start(&current, 2, NULL);
	call_ready(&current);
	struct tr_buffer *body = make_body(&f, 7, 22);
	struct tr_buffer *original = body;
	CHECK(tr_client_record_send(old.handle, &body) == TR_ERR_STALE);
	CHECK(body == original);
	CHECK(tr_client_record_half_close(old.handle) == TR_ERR_STALE);
	CHECK(tr_client_record_cancel(old.handle) == TR_ERR_STALE);
	tr_buffer_release(body);
	struct tr_rpc_record_client_handle untouched, sentinel;
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	struct tr_rpc_record_client_callbacks cb =
	        callbacks(&unaccepted.context);
	CHECK(tr_client_record_call_start(f.client, 17, 2, NULL, &cb,
	                                  &untouched) == TR_AGAIN);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(!atomic_load(&unaccepted.result.terminals));
	send_body(&current, 257, 21);
	half_close(&current);
	call_finish(&current, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	fixture_destroy(&f);
	CHECK(atomic_load(&old.result.terminals) == 1);
	CHECK(!atomic_load(&unaccepted.result.terminals));
	CHECK(atomic_load(&f.calls) == 2);
	CHECK(atomic_load(&f.messages) == 2);
	CHECK(atomic_load(&f.bodies_released) == 3);
}

static void test_peer_window(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	f.server_config.limits.initial_window_bytes =
	        1024 + TR_RPC_RECORD_HEADER_SIZE;
	for (unsigned i = 0; i < 2; ++i) {
		f.server_methods[i].descriptor.max_request_bytes = 1024;
		f.server_methods[i].descriptor.max_response_bytes = 1024;
	}
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	expect_message(&call, 512, 32);
	call_start(&call, 1, NULL);
	call_ready(&call);
	struct tr_buffer *body = make_body(&f, BODY, 31);
	struct tr_buffer *original = body;
	unsigned before = atomic_load(&attempts[TR_RPC_RECORD_MESSAGE]);
	CHECK(tr_client_record_send(call.handle, &body) == TR_ERR_BAD_LENGTH);
	CHECK(body == original);
	CHECK(atomic_load(&attempts[TR_RPC_RECORD_MESSAGE]) == before);
	tr_buffer_release(body);
	/* 同一个 ONE 调用仍能发送唯一一条合法正文，失败没有预先推进基数。 */
	send_body(&call, 512, 32);
	half_close(&call);
	call_finish(&call, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	fixture_destroy(&f);
	CHECK(atomic_load(&f.messages) == 1);
	CHECK(atomic_load(&f.bodies_released) == 2);
}

static void test_backpressure(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	f.server_config.limits.initial_window_bytes = WIRE_LIMIT;
	f.client_config.limits.initial_window_bytes = WIRE_LIMIT;
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	expect_message(&call, BODY, 30);
	expect_message(&call, BODY, 31);
	call_start(&call, 2, NULL);
	call_ready(&call);
	atomic_store(&hold_credit, true);
	atomic_store(&fail_kind, TR_RPC_RECORD_MESSAGE);
	struct tr_buffer *body = make_body(&f, BODY, 30);
	struct tr_buffer *original = body;
	CHECK(tr_client_record_send(call.handle, &body) == TR_AGAIN);
	CHECK(body == original && !atomic_load(&f.bodies_released));
	CHECK(atomic_load(&fake_again) == 1);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]));
	CHECK(tr_client_record_send(call.handle, &body) == TR_OK && !body);
	wait_count(&held_published, 1);
	wait_count(&call.result.messages, 1);
	struct tr_stream_handle stream;
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	stream = last_client_stream;
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	struct tr_stream_flow_state before, after;
	CHECK(tr_stream_get_flow_state(stream, &before) == TR_OK);
	body = make_body(&f, BODY, 31);
	original = body;
	unsigned real_before = atomic_load(&real_again);
	CHECK(tr_client_record_send(call.handle, &body) == TR_AGAIN);
	CHECK(body == original);
	CHECK(atomic_load(&real_again) == real_before + 1);
	CHECK(tr_stream_get_flow_state(stream, &after) == TR_OK);
	CHECK(after.tx_sent_bytes == before.tx_sent_bytes);
	unsigned writable_before = atomic_load(&call.result.writable);
	struct tr_channel *server = atomic_load(&server_channel);
	CHECK(tr_reactor_call(tr_channel_reactor(server),
	                      release_credit_on_owner, server) == TR_OK);
	wait_count(&call.result.writable, writable_before + 1);
	CHECK(atomic_load(&f.messages) == 1);
	/* writable 不预约信用；重复提示也必须由应用保留原正文重试。 */
	send_existing_body(&call, &body);
	half_close(&call);
	call_finish(&call, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	fixture_destroy(&f);
	CHECK(atomic_load(&f.messages) == 2);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]) == 2);
	CHECK(atomic_load(&f.bodies_released) == 2);
	test_peer_window();
}

static void test_owner_callbacks(void)
{
	struct fixture f;
	struct call followup, cancelled, freed;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&followup, &f);
	call_init(&cancelled, &f);
	call_init(&freed, &f);
	followup.context.action = CALLBACK_SEND_CLOSE;
	expect_message(&followup, 23, 40);
	expect_message(&followup, 89, 41);
	call_start(&followup, 2, NULL);
	call_ready(&followup);
	send_body(&followup, 23, 40);
	call_finish(&followup, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	CHECK(atomic_load(&followup.result.self_checks) == 1);
	call_retired(&followup);
	cancelled.context.action = CALLBACK_CANCEL;
	expect_message(&cancelled, 47, 42);
	call_start(&cancelled, 2, NULL);
	call_ready(&cancelled);
	send_body(&cancelled, 47, 42);
	call_finish(&cancelled, TR_RPC_STATUS_CANCELLED,
	            TR_RPC_RECORD_ON_OWNER);
	CHECK(atomic_load(&cancelled.result.self_checks) == 1);
	call_retired(&cancelled);
	/* 初次 writable 可在 start 返回前执行；terminal 可释放其 arg，
	 * 但不得在 writable 的当前栈帧内发生。 */
	struct call_context *context = malloc(sizeof(*context));
	CHECK(context);
	*context = freed.context;
	context->action = CALLBACK_CANCEL_WRITABLE;
	context->free_on_terminal = true;
	struct tr_rpc_record_client_callbacks cb = callbacks(context);
	CHECK(tr_client_record_call_start(f.client, 17, 2, NULL, &cb,
	                                  &freed.handle) == TR_OK);
	call_finish(&freed, TR_RPC_STATUS_CANCELLED, TR_RPC_RECORD_ON_OWNER);
	CHECK(atomic_load(&freed.result.arg_freed) == 1);
	CHECK(atomic_load(&freed.result.self_checks) == 1);
	fixture_destroy(&f);
	CHECK(atomic_load(&followup.result.terminals) == 1);
	CHECK(atomic_load(&cancelled.result.terminals) == 1);
	CHECK(atomic_load(&freed.result.terminals) == 1);
}

static int close_connection_on_owner(void *arg)
{
	struct tr_conn_handle *connection = arg;
	CHECK(tr_reactor_in_owner_context_for(connection->reactor));
	return tr_reactor_close_on_owner(*connection);
}

static void test_disconnect(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	call_start(&call, 2, NULL);
	call_ready(&call);
	send_body(&call, 127, 50);
	half_close(&call);
	gate_wait(&f.gate, 1, false);
	struct tr_conn_handle connection = {0};
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	for (unsigned i = 0; i < observation_count; ++i)
		if (observations[i].channel == atomic_load(&client_channel))
			connection = observations[i].connection;
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	CHECK(connection.reactor);
	CHECK(tr_reactor_call(connection.reactor, close_connection_on_owner,
	                      &connection) == TR_OK);
	call_finish(&call, TR_RPC_STATUS_UNAVAILABLE, TR_RPC_RECORD_ON_OWNER);
	gate_release(&f.gate);
	wait_count(&f.messages, 1);
	fixture_destroy(&f);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_OPEN]) == 1);
	CHECK(atomic_load(&client_creates) == 1);
	CHECK(atomic_load(&f.calls) == 1);
	CHECK(!atomic_load(&call.result.messages));
	CHECK(atomic_load(&call.result.terminals) == 1);
}

static void test_pending_deadline(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	struct tr_rpc_call_options options = {.timeout_ms = 50};
	call_start(&call, 2, &options);
	call_finish(&call, TR_RPC_STATUS_DEADLINE_EXCEEDED,
	            TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&f.calls));
	fixture_destroy(&f);
	CHECK(atomic_load(&call.result.terminals) == 1);
}

struct clock_failure_request {
	struct tr_rpc_record_client_handle handle;
	struct tr_buffer *body;
};

static int clock_failure_on_owner(void *arg)
{
	struct clock_failure_request *request = arg;
	CHECK(tr_reactor_in_owner_context());
	/* 只使当前 owner 的下一次取时失败，不改变其他 Reactor、测试
	 * 等待期限或后续时钟值；由真实门面的发送入口消费该故障。 */
	CHECK(!fail_clock_once);
	fail_clock_once = true;
	int ret = tr_client_record_send(request->handle, &request->body);
	CHECK(!fail_clock_once);
	return ret;
}

static void test_clock_failure(void)
{
	struct fixture f;
	struct call failed, following;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&failed, &f);
	call_init(&following, &f);
	call_start(&failed, 2, NULL);
	call_ready(&failed);
	struct clock_failure_request request = {failed.handle,
	                                        make_body(&f, 127, 59)};
	struct tr_buffer *original = request.body;
	struct tr_channel *channel = atomic_load(&client_channel);
	CHECK(tr_reactor_call(tr_channel_reactor(channel),
	                      clock_failure_on_owner, &request) == TR_ERR_SYS);
	CHECK(request.body == original);
	CHECK(atomic_load(&clock_failures) == 1);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]));
	tr_buffer_release(request.body);
	call_finish(&failed, TR_RPC_STATUS_INTERNAL, TR_RPC_RECORD_ON_OWNER);
	call_retired(&failed);
	echo(&following, 1, 63, 60);
	fixture_destroy(&f);
	CHECK(atomic_load(&failed.result.terminals) == 1);
	CHECK(atomic_load(&f.messages) == 1);
	CHECK(atomic_load(&f.bodies_released) == 2);
}

static void test_running_deadline(void)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	struct tr_rpc_call_options options = {.timeout_ms = 1000};
	call_start(&call, 2, &options);
	call_ready(&call);
	send_body(&call, 255, 60);
	half_close(&call);
	gate_wait(&f.gate, 1, false);
	call_finish(&call, TR_RPC_STATUS_DEADLINE_EXCEEDED,
	            TR_RPC_RECORD_ON_OWNER);
	gate_release(&f.gate);
	wait_count(&f.messages, 1);
	fixture_destroy(&f);
	CHECK(!atomic_load(&call.result.messages));
	CHECK(atomic_load(&call.result.terminals) == 1);
}

static void test_late_responses_after_half_close(bool deadline)
{
	struct fixture f;
	struct call late, following[2], unaccepted;
	fixture_init(&f);
	f.client_config.limits.max_calls = 1;
	f.client_config.limits.max_streams = 1;
	f.client_config.limits.initial_window_bytes = WIRE_LIMIT;
	f.client_record.calls.capacity = 1;
	f.server_record.endpoint.dispatch.max_replies_per_event = 3;
	f.server_record.endpoint.dispatch.max_reply_bytes = 3 * BODY;
	atomic_store(&f.reply_copies, 3);
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&late, &f);
	call_init(&unaccepted, &f);
	for (unsigned i = 0; i < 2; ++i)
		call_init(&following[i], &f);
	struct tr_rpc_call_options options = {.timeout_ms = 1000};
	/* 服务端现在会执行 OPEN 期限；本场景仍须独立保护 Client 面对
	 * 不执行该期限的对端时，排空多个窗口晚响应的原有责任。 */
	atomic_store(&clear_open_deadline, deadline);
	call_start(&late, 2, deadline ? &options : NULL);
	call_ready(&late);
	CHECK(!atomic_load(&clear_open_deadline));
	CHECK(atomic_load(&deadline_prefixes_accepted) == (deadline ? 1U : 0U));
	send_body(&late, BODY, 70);
	half_close(&late);
	gate_wait(&f.gate, 1, false);
	/* 发送接受不代表对端已解析；必须等原 Server data 回调处理完
	 * HALF_CLOSE，才选择这个不能撤销远端工作的本地终局。 */
	wait_count(&server_half_closes_received, 1);
	atomic_store(&f.reply_copies, 1);
	if (!deadline)
		CHECK(tr_client_record_cancel(late.handle) == TR_OK);
	enum tr_rpc_status status = deadline ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                     : TR_RPC_STATUS_CANCELLED;
	call_finish(&late, status, TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&server_messages_accepted));
	CHECK(tr_client_record_cancel(late.handle) == TR_ERR_STATE);
	struct tr_rpc_record_client_handle untouched, sentinel;
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	struct tr_rpc_record_client_callbacks cb =
	        callbacks(&unaccepted.context);
	CHECK(tr_client_record_call_start(f.client, 17, 1, NULL, &cb,
	                                  &untouched) == TR_AGAIN);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(!atomic_load(&unaccepted.result.terminals));
	CHECK(!atomic_load(&unaccepted.result.writable));
	gate_release(&f.gate);
	/* 三条4145字节记录总计12435，超过8192字节初始窗口。终局后
	 * 仍须排空并归还信用，才能让远端发送完结果与CLOSE；晚到内容
	 * 不进入应用 received，也不能把本地终局改写为远端成功。 */
	wait_count(&server_messages_accepted, 3);
	call_retired(&late);
	expect_message(&following[0], 63, 71);
	call_start(&following[0], 1, NULL);
	call_ready(&following[0]);
	CHECK(tr_client_record_half_close(late.handle) == TR_ERR_STALE);
	CHECK(tr_client_record_cancel(late.handle) == TR_ERR_STALE);
	send_body(&following[0], 63, 71);
	half_close(&following[0]);
	call_finish(&following[0], TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(&following[0]);
	echo(&following[1], 1, 63, 72);
	fixture_destroy(&f);
	call_finish(&late, status, TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&late.result.final_records));
	CHECK(!atomic_load(&unaccepted.result.terminals));
	CHECK(atomic_load(&server_messages_accepted) == 5);
	CHECK(atomic_load(&server_message_bytes) ==
	      3 * (BODY + TR_RPC_RECORD_HEADER_SIZE) +
	              2 * (63 + TR_RPC_RECORD_HEADER_SIZE));
	CHECK(atomic_load(&server_half_closes_received) == 3);
	CHECK(atomic_load(&f.calls) == 3);
	CHECK(atomic_load(&f.messages) == 3);
	CHECK(atomic_load(&f.half_closes) == 3);
	CHECK(atomic_load(&f.bodies_released) == 3);
	CHECK(atomic_load(&deadline_prefixes_accepted) == (deadline ? 1U : 0U));
	CHECK(atomic_load(&deadline_prefixes) >=
	      atomic_load(&deadline_prefixes_accepted));
}

static void test_drain(void)
{
	struct fixture f;
	struct call active, rejected;
	fixture_init(&f);
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&active, &f);
	call_init(&rejected, &f);
	expect_message(&active, 511, 61);
	call_start(&active, 1, NULL);
	call_ready(&active);
	send_body(&active, 511, 61);
	half_close(&active);
	gate_wait(&f.gate, 1, false);
	CHECK(tr_client_begin_drain(f.client) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, 0) == TR_AGAIN);
	struct tr_rpc_record_client_handle untouched, sentinel;
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	struct tr_rpc_record_client_callbacks cb = callbacks(&rejected.context);
	CHECK(tr_client_record_call_start(f.client, 17, 2, NULL, &cb,
	                                  &untouched) == TR_ERR_CLOSED);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	gate_release(&f.gate);
	call_finish(&active, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	CHECK(tr_client_wait_drained(f.client, WAIT_MS) == TR_OK);
	fixture_destroy(&f);
	CHECK(!atomic_load(&rejected.result.terminals));
	CHECK(atomic_load(&active.result.terminals) == 1);
}

static int disable_client_routes_on_owner(void *arg)
{
	struct tr_channel *channel = arg;
	CHECK(channel == atomic_load(&client_channel));
	CHECK(tr_reactor_in_owner_context_for(tr_channel_reactor(channel)));
	return tr_channel_set_handler(channel, NULL, NULL, NULL, NULL);
}

static void test_destroy_retry(bool stopped_residual)
{
	struct fixture f;
	struct call call;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	call_init(&call, &f);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	call_start(&call, 2, NULL);
	wait_count(&fake_again, 1);
	CHECK(!atomic_load(&call.result.terminals));
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&f.calls));
	if (stopped_residual) {
		/* 显式隔离 Channel 到宿主的退出事件，构造已受理但未被
		 * owner 终结的残留；正常 destroy 不要求关闭这些路由。 */
		struct tr_channel *channel = atomic_load(&client_channel);
		CHECK(channel);
		CHECK(tr_reactor_call(tr_channel_reactor(channel),
		                      disable_client_routes_on_owner,
		                      channel) == TR_OK);
		CHECK(!atomic_load(&call.result.terminals));
	}
	atomic_store(&fail_client_destroy, true);
	CHECK(tr_client_destroy(f.client) == TR_ERR_SYS);
	CHECK(atomic_load(&destroy_failures) == 1);
	if (stopped_residual) {
		call_finish(&call, TR_RPC_STATUS_CANCELLED,
		            TR_RPC_RECORD_OWNER_STOPPED);
		CHECK(atomic_load(&call.result.self_checks) == 1);
	} else {
		/* 正常停止仍在 owner 清理连接；其断线事件先选择
		 * UNAVAILABLE，join 后的残留回收不能覆盖或重复交付。 */
		call_finish(&call, TR_RPC_STATUS_UNAVAILABLE,
		            TR_RPC_RECORD_ON_OWNER);
		CHECK(!atomic_load(&call.result.self_checks));
	}
	CHECK(tr_client_connect(f.client, "127.0.0.1", f.port) == TR_ERR_STATE);
	/* 第一次拆除后宿主可能已释放，不再访问其借用句柄；只重试仍由
	 * 调用方拥有的 Client，残留终局不得重复交付。 */
	CHECK(tr_client_destroy(f.client) == TR_OK);
	f.client = NULL;
	fixture_destroy(&f);
	CHECK(atomic_load(&call.result.terminals) == 1);
	CHECK(!atomic_load(&call.result.writable));
	CHECK(!atomic_load(&call.result.initial));
	CHECK(!atomic_load(&call.result.final_records));
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&f.calls));
}

static void test_deadlines_and_lifecycle(void)
{
	test_clock_failure();
	test_pending_deadline();
	test_running_deadline();
	test_late_responses_after_half_close(false);
	test_late_responses_after_half_close(true);
	test_drain();
	test_destroy_retry(false);
	test_destroy_retry(true);
}

int main(void)
{
	CHECK(setvbuf(stdout, NULL, _IOLBF, 0) == 0);
	test_configuration_and_rollback();
	puts("test_client_record: configuration and rollback passed");
	test_shapes_and_metadata();
	puts("test_client_record: shapes and metadata passed");
	test_early_rejection();
	puts("test_client_record: early rejection passed");
	test_stale_handle();
	puts("test_client_record: stale handles passed");
	test_backpressure();
	puts("test_client_record: backpressure passed");
	test_owner_callbacks();
	puts("test_client_record: owner callbacks passed");
	test_disconnect();
	puts("test_client_record: disconnect passed");
	test_deadlines_and_lifecycle();
	puts("test_client_record: deadlines and lifecycle passed");
	puts("test_client_record: 8 groups passed");
	return 0;
}
