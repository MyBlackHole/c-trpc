#define _POSIX_C_SOURCE 200809L

#include <tr/trpc.h>

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* 同一源码须能脱离仓库作为 C11/C++11 消费端；不引用私有头或测试替身。 */
#define CHECK(expression)                                                      \
	do {                                                                   \
		if (!(expression)) {                                           \
			fprintf(stderr, "sdk-v2: line=%d check=%s\n",          \
			        __LINE__, #expression);                        \
			abort();                                               \
		}                                                              \
	} while (0)

#define METHOD_COUNT 6U
#define ASYNC_METHOD 5U
#define MAX_BODY 4096U
#define MAX_MESSAGES 3U
#define WAIT_MS 15000U

struct fixture;

enum response_metadata { METADATA_NONE, METADATA_VALUES, METADATA_EMPTY };

enum async_mode { ASYNC_COMPLETE, ASYNC_CANCEL, ASYNC_DEADLINE, ASYNC_DESTROY };

struct async_state {
	struct fixture *fixture;
	struct tr_rpc_v2_producer *producer;
	enum async_mode mode;
	unsigned acquired;
	unsigned work_release;
	unsigned cancelled;
	unsigned staged;
	unsigned produced;
	unsigned finish_release;
	unsigned thread_done;
};

struct observed_client {
	struct tr_client *handle;
	uint64_t terminals;
	uint64_t final_status[TR_RPC_STATUS_COUNT];
	int destroying;
};

struct method_state {
	struct fixture *fixture;
	unsigned index;
	unsigned opens;
	unsigned requests;
	unsigned half_closes;
	unsigned terminals;
	unsigned running;
	unsigned sum;
	unsigned entered;
	unsigned cancelled;
	unsigned exited;
	unsigned released;
	int expect_metadata;
	enum response_metadata response_metadata;
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
	struct tr_rpc_v2_call_id id;
	struct async_state *async;
};

struct fixture {
	struct observed_client client;
	struct tr_server *server;
	struct tr_client_config client_config;
	uint16_t port;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int server_destroying;
	struct method_state methods[METHOD_COUNT];
};

struct message_spec {
	uint32_t size;
	unsigned tag;
};

struct call_state {
	struct fixture *fixture;
	struct observed_client *client;
	struct tr_rpc_v2_call handle;
	struct message_spec requests[MAX_MESSAGES];
	struct message_spec responses[MAX_MESSAGES];
	unsigned request_count;
	unsigned response_count;
	unsigned sent;
	unsigned received;
	unsigned writable_returns;
	unsigned again;
	unsigned initial;
	unsigned trailing;
	unsigned terminals;
	unsigned callback_depth;
	unsigned server_terminal_target;
	unsigned method;
	int half_closed;
	int manual;
	int checked_context;
	int check_length;
	enum response_metadata response_metadata;
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
};

static void lock(struct fixture *f)
{
	CHECK(pthread_mutex_lock(&f->lock) == 0);
}

static void unlock(struct fixture *f)
{
	CHECK(pthread_mutex_unlock(&f->lock) == 0);
}

static struct timespec deadline_after(unsigned milliseconds)
{
	struct timespec due;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &due) == 0);
	due.tv_sec += milliseconds / 1000U;
	due.tv_nsec += (long)(milliseconds % 1000U) * 1000000L;
	if (due.tv_nsec >= 1000000000L) {
		++due.tv_sec;
		due.tv_nsec -= 1000000000L;
	}
	return due;
}

static int before(const struct timespec *a, const struct timespec *b)
{
	return a->tv_sec < b->tv_sec ||
	       (a->tv_sec == b->tv_sec && a->tv_nsec < b->tv_nsec);
}

static void wait_count(struct fixture *f, const unsigned *value,
                       unsigned wanted)
{
	struct timespec due = deadline_after(WAIT_MS);
	lock(f);
	while (*value < wanted)
		CHECK(pthread_cond_timedwait(&f->cond, &f->lock, &due) == 0);
	unlock(f);
}

static void publish(struct fixture *f)
{
	CHECK(pthread_cond_broadcast(&f->cond) == 0);
}

static void check_stats_invariants(const struct tr_rpc_semantic_stats *stats)
{
	uint64_t finished = 0;
	CHECK(stats->calls_started >= stats->calls_finished);
	CHECK(stats->calls_inflight ==
	      stats->calls_started - stats->calls_finished);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		finished += stats->final_status[i];
	CHECK(finished == stats->calls_finished);
}

static void check_totals(const struct tr_rpc_semantic_stats *stats,
                         uint64_t started, uint64_t finished,
                         enum tr_rpc_status other, uint64_t other_count)
{
	check_stats_invariants(stats);
	CHECK(stats->calls_started == started);
	CHECK(stats->calls_finished == finished);
	CHECK(finished >= other_count);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i) {
		uint64_t wanted =
		        i == TR_RPC_STATUS_OK ? finished - other_count : 0;
		if (i == (unsigned)other)
			wanted += other_count;
		CHECK(stats->final_status[i] == wanted);
	}
}

static void check_server_totals(struct fixture *f, uint64_t started,
                                uint64_t finished, enum tr_rpc_status other,
                                uint64_t other_count)
{
	struct tr_rpc_semantic_stats stats;
	CHECK(tr_server_get_rpc_semantic_stats(f->server, &stats) == TR_OK);
	check_totals(&stats, started, finished, other, other_count);
}

static void check_client_totals(struct observed_client *client,
                                uint64_t started, uint64_t finished,
                                enum tr_rpc_status other, uint64_t other_count)
{
	struct tr_rpc_semantic_stats stats;
	CHECK(tr_client_get_rpc_semantic_stats(client->handle, &stats) ==
	      TR_OK);
	check_totals(&stats, started, finished, other, other_count);
}

static void check_stats_rejected(struct fixture *f, int foreign_client)
{
	struct tr_rpc_semantic_stats before, stats;
	memset(&before, 0xa5, sizeof(before));
	memcpy(&stats, &before, sizeof(stats));
	lock(f);
	/* 外部销毁 Server 前撤销 Client 回调的跨实例借用。 */
	if (!f->server_destroying) {
		CHECK(tr_server_get_rpc_semantic_stats(f->server, &stats) ==
		      TR_ERR_STATE);
		CHECK(memcmp(&stats, &before, sizeof(stats)) == 0);
	}
	if (foreign_client) {
		/* 销毁前先在相同锁下撤销借用，避免跨实例检查碰到已释放 Client。
		 */
		if (!f->client.destroying && f->client.handle) {
			CHECK(tr_client_get_rpc_semantic_stats(f->client.handle,
			                                       &stats) ==
			      TR_ERR_STATE);
			CHECK(memcmp(&stats, &before, sizeof(stats)) == 0);
		}
	}
	unlock(f);
}

static uint8_t pattern(unsigned tag, unsigned offset)
{
	return (uint8_t)(tag + offset * 17U);
}

static void fill(uint8_t *bytes, unsigned size, unsigned tag)
{
	for (unsigned i = 0; i < size; ++i)
		bytes[i] = pattern(tag, i);
}

static void check_bytes(const struct tr_rpc_bytes *bytes, unsigned size,
                        unsigned tag)
{
	CHECK(bytes && bytes->len == size);
	CHECK(!size || bytes->data);
	for (unsigned i = 0; i < size; ++i)
		CHECK(bytes->data[i] == pattern(tag, i));
}

static void check_missing_metadata(const struct tr_rpc_v2_metadata_view *view)
{
	static const uint8_t sentinel[] = {9};
	struct tr_rpc_bytes value;
	struct tr_rpc_v2_metadata_entry entry;
	value.data = sentinel;
	value.len = sizeof(sentinel);
	entry.key = value;
	entry.value = value;
	CHECK(tr_rpc_v2_metadata_find(view, "missing", &value) == TR_ERR_STALE);
	CHECK(value.data == sentinel && value.len == sizeof(sentinel));
	CHECK(tr_rpc_v2_metadata_at(view, tr_rpc_v2_metadata_count(view),
	                            &entry) == TR_ERR_STALE);
	CHECK(entry.key.data == sentinel && entry.key.len == sizeof(sentinel));
	CHECK(entry.value.data == sentinel &&
	      entry.value.len == sizeof(sentinel));
}

static void check_request_metadata(const struct tr_rpc_v2_metadata_view *view,
                                   int expected)
{
	static const uint8_t token[] = {0, 'x', 255, 0};
	struct tr_rpc_bytes value;
	unsigned found = 0;
	CHECK(tr_rpc_v2_metadata_count(view) == (expected ? 2U : 0U));
	check_missing_metadata(view);
	if (!expected)
		return;
	CHECK(tr_rpc_v2_metadata_find(view, "token", &value) == TR_OK);
	CHECK(value.len == sizeof(token));
	CHECK(memcmp(value.data, token, sizeof(token)) == 0);
	CHECK(tr_rpc_v2_metadata_find(view, "seq", &value) == TR_OK);
	CHECK(value.len == 1U && value.data[0] == 3U);
	for (size_t i = 0; i < tr_rpc_v2_metadata_count(view); ++i) {
		struct tr_rpc_v2_metadata_entry entry;
		CHECK(tr_rpc_v2_metadata_at(view, i, &entry) == TR_OK);
		if (entry.key.len == 5U &&
		    memcmp(entry.key.data, "token", 5U) == 0) {
			CHECK(!(found & 1U));
			CHECK(entry.value.len == sizeof(token));
			CHECK(memcmp(entry.value.data, token, sizeof(token)) ==
			      0);
			found |= 1U;
		} else {
			CHECK(entry.key.len == 3U &&
			      memcmp(entry.key.data, "seq", 3U) == 0);
			CHECK(!(found & 2U));
			CHECK(entry.value.len == 1U &&
			      entry.value.data[0] == 3U);
			found |= 2U;
		}
	}
	CHECK(found == 3U);
}

static void set_response_metadata(struct tr_rpc_v2_reply *reply,
                                  enum tr_rpc_v2_metadata_phase phase,
                                  enum response_metadata mode)
{
	char key[] = "bin";
	char empty_key[] = "empty";
	uint8_t value[] = {0, 255, (uint8_t)phase, 0};
	struct tr_rpc_metadata entries[2];
	if (mode == METADATA_NONE)
		return;
	if (mode == METADATA_EMPTY) {
		CHECK(tr_rpc_v2_reply_set_metadata(reply, phase, NULL, 0) ==
		      TR_OK);
		return;
	}
	entries[0].key = key;
	entries[0].value = value;
	entries[0].value_len = sizeof(value);
	entries[1].key = empty_key;
	entries[1].value = NULL;
	entries[1].value_len = 0;
	CHECK(tr_rpc_v2_reply_set_metadata(reply, phase, entries, 2) == TR_OK);
	/* 两阶段都复制数组、键与二进制值；同名键可分别属于两个阶段。 */
	memset(key, 'q', sizeof(key));
	memset(empty_key, 'q', sizeof(empty_key));
	memset(value, 0x55, sizeof(value));
	memset(entries, 0, sizeof(entries));
}

static void check_response_metadata(const struct tr_rpc_v2_metadata_view *view,
                                    enum tr_rpc_v2_metadata_phase phase,
                                    enum response_metadata mode)
{
	struct tr_rpc_bytes value;
	const uint8_t expected[] = {0, 255, (uint8_t)phase, 0};
	CHECK(tr_rpc_v2_metadata_count(view) ==
	      (mode == METADATA_VALUES ? 2U : 0U));
	check_missing_metadata(view);
	if (mode != METADATA_VALUES)
		return;
	CHECK(tr_rpc_v2_metadata_find(view, "bin", &value) == TR_OK);
	CHECK(value.len == sizeof(expected));
	CHECK(memcmp(value.data, expected, sizeof(expected)) == 0);
	CHECK(tr_rpc_v2_metadata_find(view, "empty", &value) == TR_OK);
	CHECK(value.len == 0U);
	for (size_t i = 0; i < 2U; ++i) {
		struct tr_rpc_v2_metadata_entry entry;
		CHECK(tr_rpc_v2_metadata_at(view, i, &entry) == TR_OK);
		if (i == 0U) {
			CHECK(entry.key.len == 3U &&
			      memcmp(entry.key.data, "bin", 3U) == 0);
			CHECK(entry.value.len == sizeof(expected));
			CHECK(memcmp(entry.value.data, expected,
			             sizeof(expected)) == 0);
		} else {
			CHECK(entry.key.len == 5U &&
			      memcmp(entry.key.data, "empty", 5U) == 0);
			CHECK(entry.value.len == 0U);
		}
	}
}

static void append_response(struct tr_rpc_v2_reply *reply, unsigned size,
                            unsigned tag)
{
	uint8_t storage[MAX_BODY];
	struct tr_rpc_bytes body;
	CHECK(size <= sizeof(storage));
	fill(storage, size, tag);
	body.data = size ? storage : NULL;
	body.len = size;
	CHECK(tr_rpc_v2_reply_append(reply, &body) == TR_OK);
	/* append 承诺复制；返回后立即复用原业务存储。 */
	memset(storage, 0xe7, sizeof(storage));
}

static void cooperate(struct method_state *m,
                      const struct tr_rpc_v2_cancel *cancel)
{
	struct fixture *f = m->fixture;
	struct timespec limit = deadline_after(WAIT_MS);
	lock(f);
	++m->entered;
	publish(f);
	while (!m->released) {
		struct timespec due = deadline_after(10);
		int requested = tr_rpc_v2_cancel_requested(cancel);
		CHECK(requested == 0 || requested == 1);
		if (requested && !m->cancelled) {
			++m->cancelled;
			publish(f);
		}
		CHECK(before(&due, &limit));
		int ret = pthread_cond_timedwait(&f->cond, &f->lock, &due);
		CHECK(ret == 0 || ret == ETIMEDOUT);
	}
	CHECK(m->cancelled == 1U);
	++m->exited;
	publish(f);
	unlock(f);
}

static void acquire_async(struct async_state *a,
                          const struct tr_rpc_v2_cancel *cancel,
                          struct tr_rpc_v2_reply *reply)
{
	struct fixture *f = a->fixture;
	struct tr_rpc_v2_producer *producer = NULL;
	struct timespec limit = deadline_after(WAIT_MS);
	CHECK(tr_rpc_v2_reply_acquire_producer(reply, &producer) == TR_OK);
	CHECK(producer);
	CHECK(tr_rpc_v2_producer_wait_writable(producer, 0) == TR_ERR_STATE);
	/* 发布前各取得一份应用引用：生产线程与控制线程独立归还。 */
	CHECK(tr_rpc_v2_producer_retain(producer) == TR_OK);
	lock(f);
	CHECK(!a->acquired);
	a->producer = producer;
	++a->acquired;
	publish(f);
	while (!a->work_release) {
		struct timespec due = deadline_after(10);
		int requested = tr_rpc_v2_cancel_requested(cancel);
		CHECK(requested == 0 || requested == 1);
		if (requested && !a->cancelled) {
			++a->cancelled;
			publish(f);
		}
		CHECK(before(&due, &limit));
		int ret = pthread_cond_timedwait(&f->cond, &f->lock, &due);
		CHECK(ret == 0 || ret == ETIMEDOUT);
	}
	unlock(f);
}

static enum tr_rpc_status server_work(struct tr_rpc_v2_call_id call,
                                      const struct tr_rpc_v2_request *request,
                                      const struct tr_rpc_v2_cancel *cancel,
                                      struct tr_rpc_v2_reply *reply, void *arg)
{
	struct method_state *m = (struct method_state *)arg;
	struct fixture *f = m->fixture;
	unsigned sum = 0;
	unsigned requests;
	enum response_metadata metadata;
	struct async_state *async;
	CHECK(tr_server_drain(f->server, 0) == TR_ERR_STATE);
	CHECK(tr_server_destroy(f->server) == TR_ERR_STATE);
	check_stats_rejected(f, 1);
	lock(f);
	CHECK(m->running == 0U);
	++m->running;
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		m->id = call;
		++m->opens;
		m->requests = 0;
		m->sum = 0;
		check_request_metadata(&request->metadata, m->expect_metadata);
	} else {
		CHECK(memcmp(&m->id, &call, sizeof(call)) == 0);
		CHECK(tr_rpc_v2_metadata_count(&request->metadata) == 0U);
		if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
			unsigned tag =
			        request->body.len ? request->body.data[0] : 0U;
			check_bytes(&request->body, request->body.len, tag);
			++m->requests;
			m->sum += tag;
		} else {
			CHECK(request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE);
			++m->half_closes;
			sum = m->sum;
		}
	}
	requests = m->requests;
	metadata = m->response_metadata;
	async = m->async;
	publish(f);
	unlock(f);

	if (async) {
		if (request->kind == TR_RPC_V2_REQUEST_OPEN)
			acquire_async(async, cancel, reply);
		/* 取得 producer 后继续消费请求，HALF_CLOSE 不再同步完成。 */
	} else if (request->kind == TR_RPC_V2_REQUEST_OPEN && m->index == 1U) {
		set_response_metadata(reply, TR_RPC_V2_METADATA_INITIAL,
		                      metadata);
	} else if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
		unsigned tag = request->body.len ? request->body.data[0] : 0U;
		if (m->index == 4U) {
			cooperate(m, cancel);
			CHECK(tr_rpc_v2_cancel_requested(cancel) == 1);
			set_response_metadata(reply, TR_RPC_V2_METADATA_INITIAL,
			                      metadata);
			set_response_metadata(
			        reply, TR_RPC_V2_METADATA_TRAILING, metadata);
		} else if (m->index != 2U) {
			unsigned copies = m->index == 1U ? 3U : 1U;
			for (unsigned i = 0; i < copies; ++i)
				append_response(reply, request->body.len,
				                tag + i);
			if (requests == 1U && m->index != 1U)
				set_response_metadata(
				        reply, TR_RPC_V2_METADATA_INITIAL,
				        metadata);
		}
	} else if (request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		if (m->index == 2U)
			append_response(reply, 8U, sum);
		/* INITIAL 的发送次序不依赖 setter 与同批 append 的调用次序。
		 * 空 MANY 也显式交付一个空 INITIAL，随后才交付尾部。 */
		if (m->index == 2U || (m->index == 3U && !requests))
			set_response_metadata(reply, TR_RPC_V2_METADATA_INITIAL,
			                      metadata);
		set_response_metadata(reply, TR_RPC_V2_METADATA_TRAILING,
		                      metadata);
		CHECK(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) == TR_OK);
	}
	lock(f);
	CHECK(m->running == 1U);
	--m->running;
	publish(f);
	unlock(f);
	return m->index == 4U && request->kind == TR_RPC_V2_REQUEST_MESSAGE
	               ? TR_RPC_STATUS_CANCELLED
	               : TR_RPC_STATUS_OK;
}

static void server_terminal(struct tr_rpc_v2_call_id call,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct method_state *m = (struct method_state *)arg;
	struct fixture *f = m->fixture;
	CHECK(delivery == TR_RPC_V2_DELIVERY_LIVE ||
	      delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(tr_server_destroy(f->server) == TR_ERR_STATE);
	check_stats_rejected(f, 1);
	lock(f);
	CHECK(m->running == 0U);
	CHECK(m->terminals < m->opens);
	CHECK(memcmp(&m->id, &call, sizeof(call)) == 0);
	++m->terminals;
	m->status = status;
	m->delivery = delivery;
	publish(f);
	unlock(f);
}

static void describe_methods(struct tr_rpc_method_desc *descriptors)
{
	memset(descriptors, 0, (METHOD_COUNT + 1U) * sizeof(*descriptors));
	for (unsigned i = 0; i < METHOD_COUNT; ++i) {
		descriptors[i].service_id = 23;
		descriptors[i].method_id = i + 1U;
		descriptors[i].request_cardinality =
		        i < 2U ? TR_RPC_ONE : TR_RPC_MANY;
		descriptors[i].response_cardinality =
		        (i == 0U || i == 2U) ? TR_RPC_ONE : TR_RPC_MANY;
		descriptors[i].request_codec_id = TR_RPC_CODEC_RAW;
		descriptors[i].response_codec_id = TR_RPC_CODEC_RAW;
		descriptors[i].lane = TR_RPC_LANE_CONTROL;
		descriptors[i].max_request_bytes = MAX_BODY;
		descriptors[i].max_response_bytes = MAX_BODY;
	}
	descriptors[METHOD_COUNT] = descriptors[0];
	descriptors[METHOD_COUNT].method_id = 99;
}

static void create_client(struct fixture *f, struct observed_client *client)
{
	struct tr_rpc_method_desc descriptors[METHOD_COUNT + 1U];
	memset(client, 0, sizeof(*client));
	describe_methods(descriptors);
	CHECK(tr_client_create(&f->client_config, descriptors,
	                       METHOD_COUNT + 1U, &client->handle) == TR_OK);
	memset(descriptors, 0, sizeof(descriptors));
	check_client_totals(client, 0, 0, TR_RPC_STATUS_OK, 0);
	CHECK(tr_client_connect(client->handle, "127.0.0.1", f->port) == TR_OK);
	CHECK(tr_client_wait_ready(client->handle, WAIT_MS) == TR_OK);
}

static void fixture_create_shards(struct fixture *f, unsigned shards)
{
	struct tr_server_config server;
	struct tr_rpc_v2_method methods[METHOD_COUNT];
	struct tr_rpc_method_desc descriptors[METHOD_COUNT + 1U];
	pthread_condattr_t attr;
	memset(f, 0, sizeof(*f));
	CHECK(pthread_mutex_init(&f->lock, NULL) == 0);
	CHECK(pthread_condattr_init(&attr) == 0);
	CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	CHECK(pthread_cond_init(&f->cond, &attr) == 0);
	CHECK(pthread_condattr_destroy(&attr) == 0);
	tr_client_config_init(&f->client_config);
	tr_server_config_init(&server);
	server.shard_count = shards;
	server.max_peers = 2U * shards;
	server.limits.max_calls = 4;
	server.limits.max_streams = 4;
	server.limits.max_methods = METHOD_COUNT + 1U;
	server.limits.max_message_bytes = MAX_BODY;
	server.limits.max_frame_payload_bytes = 256;
	server.limits.initial_window_bytes = MAX_BODY;
	/* 故意保留较大的公开默认阈值，由 V2 门面推导可推进的内部值。 */
	f->client_config.limits = server.limits;
	f->client_config.connect_timeout_ms = 2000;
	server.handshake_timeout_ms = 2000;
	memset(methods, 0, sizeof(methods));
	describe_methods(descriptors);
	for (unsigned i = 0; i < METHOD_COUNT; ++i) {
		f->methods[i].fixture = f;
		f->methods[i].index = i;
		f->methods[i].response_metadata = METADATA_VALUES;
		methods[i].descriptor = descriptors[i];
		methods[i].work = server_work;
		methods[i].terminal = server_terminal;
		methods[i].arg = &f->methods[i];
	}
	CHECK(tr_server_create(&server, methods, METHOD_COUNT, &f->server) ==
	      TR_OK);
	/* 工厂冻结方法表；后续连接不能继续借用这些构造期数组。 */
	memset(methods, 0, sizeof(methods));
	memset(descriptors, 0, sizeof(descriptors));
	check_server_totals(f, 0, 0, TR_RPC_STATUS_OK, 0);
	CHECK(tr_server_listen(f->server, "127.0.0.1", 0, &f->port) == TR_OK);
	CHECK(f->port != 0);
	CHECK(tr_server_start(f->server) == TR_OK);
	create_client(f, &f->client);
}

static void fixture_create(struct fixture *f)
{
	fixture_create_shards(f, 1);
}

static void destroy_server(struct fixture *f)
{
	struct timespec limit = deadline_after(WAIT_MS);
	int ret;
	lock(f);
	f->server_destroying = 1;
	unlock(f);
	while ((ret = tr_server_destroy(f->server)) == TR_AGAIN) {
		struct timespec now = deadline_after(0);
		struct timespec pause;
		CHECK(before(&now, &limit));
		pause.tv_sec = 0;
		pause.tv_nsec = 1000000L;
		CHECK(nanosleep(&pause, NULL) == 0);
	}
	CHECK(ret == TR_OK);
	f->server = NULL;
}

static void destroy_client(struct fixture *f, struct observed_client *client)
{
	lock(f);
	client->destroying = 1;
	unlock(f);
	CHECK(tr_client_destroy(client->handle) == TR_OK);
	lock(f);
	client->handle = NULL;
	unlock(f);
}

static void fixture_destroy(struct fixture *f)
{
	if (f->client.handle)
		destroy_client(f, &f->client);
	if (f->server)
		destroy_server(f);
	CHECK(pthread_cond_destroy(&f->cond) == 0);
	CHECK(pthread_mutex_destroy(&f->lock) == 0);
}

static void callback_context_checks(struct call_state *c)
{
	struct fixture *f = c->fixture;
	CHECK(tr_client_wait_ready(c->client->handle, 0) == TR_ERR_STATE);
	CHECK(tr_client_wait_drained(c->client->handle, 0) == TR_ERR_STATE);
	CHECK(tr_client_destroy(c->client->handle) == TR_ERR_STATE);
	lock(f);
	if (!f->server_destroying) {
		CHECK(tr_server_drain(f->server, 0) == TR_ERR_STATE);
		CHECK(tr_server_destroy(f->server) == TR_ERR_STATE);
	}
	unlock(f);
	check_stats_rejected(f, 0);
}

static void client_message(struct tr_rpc_v2_call call,
                           const struct tr_rpc_bytes *body, void *arg)
{
	struct call_state *c = (struct call_state *)arg;
	struct fixture *f = c->fixture;
	(void)call;
	lock(f);
	CHECK(!c->terminals && !c->trailing && c->initial == 1U);
	CHECK(c->callback_depth == 0U);
	++c->callback_depth;
	CHECK(c->received < c->response_count);
	check_bytes(body, c->responses[c->received].size,
	            c->responses[c->received].tag);
	++c->received;
	--c->callback_depth;
	publish(f);
	unlock(f);
}

static void client_metadata(struct tr_rpc_v2_call call,
                            enum tr_rpc_v2_metadata_phase phase,
                            const struct tr_rpc_v2_metadata_view *view,
                            void *arg)
{
	struct call_state *c = (struct call_state *)arg;
	struct fixture *f = c->fixture;
	(void)call;
	lock(f);
	CHECK(!c->terminals && c->callback_depth == 0U);
	++c->callback_depth;
	check_response_metadata(view, phase, c->response_metadata);
	if (phase == TR_RPC_V2_METADATA_INITIAL) {
		CHECK(!c->initial && !c->received && !c->trailing);
		++c->initial;
	} else {
		CHECK(phase == TR_RPC_V2_METADATA_TRAILING && !c->trailing);
		++c->trailing;
	}
	--c->callback_depth;
	publish(f);
	unlock(f);
}

static void client_writable(struct tr_rpc_v2_call call, void *arg)
{
	struct call_state *c = (struct call_state *)arg;
	struct fixture *f = c->fixture;
	lock(f);
	CHECK(!c->terminals && c->callback_depth == 0U);
	++c->callback_depth;
	unlock(f);
	if (!c->checked_context) {
		struct tr_rpc_semantic_stats stats;
		callback_context_checks(c);
		CHECK(tr_client_get_rpc_semantic_stats(c->client->handle,
		                                       &stats) == TR_OK);
		check_stats_invariants(&stats);
		CHECK(stats.calls_started > stats.calls_finished);
		c->checked_context = 1;
	}
	if (!c->manual) {
		/* 回调不等待信用。两条整窗口正文不能在同一 owner 回调中
		 * 全部接受，AGAIN 的原字节由下一次 writable 重新提交。 */
		for (unsigned i = 0; i < 2U && c->sent < c->request_count;
		     ++i) {
			uint8_t storage[MAX_BODY + 1U];
			struct tr_rpc_bytes body;
			struct message_spec spec = c->requests[c->sent];
			fill(storage, spec.size, spec.tag);
			body.data = spec.size ? storage : NULL;
			body.len = spec.size;
			if (c->check_length) {
				struct tr_rpc_bytes oversized;
				oversized.data = storage;
				oversized.len = sizeof(storage);
				CHECK(tr_rpc_v2_call_send(call, &oversized) ==
				      TR_ERR_BAD_LENGTH);
				c->check_length = 0;
			}
			int ret = tr_rpc_v2_call_send(call, &body);
			/* 成功必须已经复制；失败也不得保存这一临时正文。 */
			memset(storage, 0xd3, sizeof(storage));
			CHECK(ret == TR_OK || ret == TR_AGAIN);
			if (ret == TR_AGAIN) {
				++c->again;
				break;
			}
			++c->sent;
		}
		if (c->sent == c->request_count && !c->half_closed) {
			int ret = tr_rpc_v2_call_half_close(call);
			CHECK(ret == TR_OK || ret == TR_AGAIN);
			if (ret == TR_OK)
				c->half_closed = 1;
			else
				++c->again;
		}
	}
	lock(f);
	CHECK(c->callback_depth == 1U);
	--c->callback_depth;
	++c->writable_returns;
	publish(f);
	unlock(f);
}

static void client_terminal(struct tr_rpc_v2_call call,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct call_state *c = (struct call_state *)arg;
	struct fixture *f = c->fixture;
	struct tr_rpc_semantic_stats before, stats;
	int destroying;
	(void)call;
	callback_context_checks(c);
	lock(f);
	destroying = c->client->destroying;
	unlock(f);
	memset(&before, 0xa5, sizeof(before));
	memcpy(&stats, &before, sizeof(stats));
	int ret = tr_client_get_rpc_semantic_stats(c->client->handle, &stats);
	if (destroying || delivery == TR_RPC_V2_DELIVERY_SHUTDOWN) {
		CHECK(ret == TR_ERR_STATE);
		CHECK(memcmp(&stats, &before, sizeof(stats)) == 0);
	} else {
		CHECK(ret == TR_OK);
		check_stats_invariants(&stats);
	}
	lock(f);
	CHECK(!c->terminals && c->callback_depth == 0U);
	CHECK((unsigned)status < TR_RPC_STATUS_COUNT);
	++c->client->terminals;
	++c->client->final_status[status];
	if (ret == TR_OK) {
		/* 终局记账先于通知；调用者无需等网络对象退役再观察完成。 */
		CHECK(stats.calls_finished == c->client->terminals);
		for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
			CHECK(stats.final_status[i] ==
			      c->client->final_status[i]);
	}
	c->status = status;
	c->delivery = delivery;
	++c->terminals;
	publish(f);
	unlock(f);
}

static void call_init(struct call_state *c, struct fixture *f, unsigned method,
                      const struct message_spec *requests, unsigned count)
{
	memset(c, 0, sizeof(*c));
	c->fixture = f;
	c->client = &f->client;
	c->method = method;
	c->request_count = count;
	CHECK(count <= MAX_MESSAGES);
	if (count)
		memcpy(c->requests, requests, count * sizeof(*requests));
	if (method < METHOD_COUNT) {
		lock(f);
		c->server_terminal_target = f->methods[method].terminals + 1U;
		c->response_metadata = f->methods[method].response_metadata;
		unlock(f);
	}
}

static void expect(struct call_state *c, unsigned size, unsigned tag)
{
	CHECK(c->response_count < MAX_MESSAGES);
	c->responses[c->response_count].size = size;
	c->responses[c->response_count].tag = tag;
	++c->response_count;
}

static int call_start(struct call_state *c,
                      const struct tr_rpc_call_options *options)
{
	struct tr_rpc_v2_client_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = client_message;
	callbacks.metadata = client_metadata;
	callbacks.writable = client_writable;
	callbacks.terminal = client_terminal;
	callbacks.arg = c;
	return tr_client_call_start(c->client->handle, 23,
	                            c->method < METHOD_COUNT ? c->method + 1U
	                                                     : 99U,
	                            options, &callbacks, &c->handle);
}

/* 安装后的同一 C11/C++11 消费端验证一元自动发送；正文仍只借用
 * 当前栈，收到响应和 FINAL 的回调沿用同一套观察与统计断言。 */
static int unary_start(struct call_state *c,
                       const struct tr_rpc_call_options *options)
{
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_bytes request;
	uint8_t storage[MAX_BODY + 1U];
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = client_message;
	callbacks.metadata = client_metadata;
	callbacks.writable = client_writable;
	callbacks.terminal = client_terminal;
	callbacks.arg = c;
	CHECK(c->request_count == 1U);
	fill(storage, c->requests[0].size, c->requests[0].tag);
	request.data = c->requests[0].size ? storage : NULL;
	request.len = c->requests[0].size;
	if (c->check_length) {
		struct tr_rpc_v2_call untouched, sentinel;
		struct tr_rpc_bytes oversized;
		memset(&untouched, 0xa5, sizeof(untouched));
		sentinel = untouched;
		oversized.data = storage;
		oversized.len = sizeof(storage);
		CHECK(tr_client_unary_call(c->client->handle, 23,
		                           c->method + 1U, &oversized, options,
		                           &callbacks,
		                           &untouched) == TR_ERR_BAD_LENGTH);
		CHECK(memcmp(&untouched, &sentinel, sizeof(untouched)) == 0);
	}
	int ret = tr_client_unary_call(
	        c->client->handle, 23,
	        c->method < METHOD_COUNT ? c->method + 1U : 99U, &request,
	        options, &callbacks, &c->handle);
	memset(storage, 0xd3, sizeof(storage));
	if (ret == TR_OK) {
		lock(c->fixture);
		/* 这里记录的是整个请求被本地受理，不声称 HALF_CLOSE 已上网。 */
		c->sent = 1;
		c->half_closed = 1;
		CHECK(c->writable_returns == 0U);
		unlock(c->fixture);
	}
	return ret;
}

static void call_done(struct call_state *c, enum tr_rpc_status status)
{
	struct fixture *f = c->fixture;
	wait_count(f, &c->terminals, 1);
	if (c->method < METHOD_COUNT)
		wait_count(f, &f->methods[c->method].terminals,
		           c->server_terminal_target);
	lock(f);
	CHECK(c->terminals == 1U && c->status == status);
	CHECK(c->delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(c->received == c->response_count);
	if (status == TR_RPC_STATUS_OK) {
		CHECK(c->sent == c->request_count && c->half_closed);
		CHECK(c->trailing == 1U);
		CHECK(c->initial ==
		      (c->response_count ||
		                       c->response_metadata != METADATA_NONE
		               ? 1U
		               : 0U));
		CHECK(f->methods[c->method].status == TR_RPC_STATUS_OK);
	}
	unlock(f);
}

static void drain(struct fixture *f)
{
	CHECK(tr_client_begin_drain(f->client.handle) == TR_OK);
	CHECK(tr_client_wait_drained(f->client.handle, WAIT_MS) == TR_OK);
	CHECK(tr_server_drain(f->server, WAIT_MS) == TR_OK);
}

static void test_shapes_and_copy(void)
{
	struct fixture f;
	struct call_state one, server_stream, client_stream, bidi, empty, zero;
	struct call_state unknown, rejected;
	struct message_spec request[MAX_MESSAGES];
	struct tr_rpc_call_options options;
	struct tr_rpc_metadata metadata[2];
	char token_key[] = "token";
	char seq_key[] = "seq";
	uint8_t token[] = {0, 'x', 255, 0};
	uint8_t sequence[] = {3};
	fixture_create(&f);

	request[0].size = MAX_BODY;
	request[0].tag = 11;
	call_init(&one, &f, 0, request, 1);
	one.check_length = 1;
	expect(&one, MAX_BODY, 11);
	memset(&options, 0, sizeof(options));
	metadata[0].key = token_key;
	metadata[0].value = token;
	metadata[0].value_len = sizeof(token);
	metadata[1].key = seq_key;
	metadata[1].value = sequence;
	metadata[1].value_len = sizeof(sequence);
	options.metadata = metadata;
	options.metadata_count = 2;
	lock(&f);
	f.methods[0].expect_metadata = 1;
	unlock(&f);
	CHECK(unary_start(&one, &options) == TR_OK);
	memset(token_key, 'q', sizeof(token_key));
	memset(seq_key, 'q', sizeof(seq_key));
	memset(token, 0x55, sizeof(token));
	memset(sequence, 0x55, sizeof(sequence));
	memset(metadata, 0, sizeof(metadata));
	memset(&options, 0, sizeof(options));
	call_done(&one, TR_RPC_STATUS_OK);

	request[0].size = MAX_BODY;
	request[0].tag = 21;
	call_init(&server_stream, &f, 1, request, 1);
	for (unsigned i = 0; i < 3U; ++i)
		expect(&server_stream, MAX_BODY, 21U + i);
	CHECK(call_start(&server_stream, NULL) == TR_OK);
	call_done(&server_stream, TR_RPC_STATUS_OK);

	request[0].size = 7;
	request[0].tag = 31;
	request[1].size = 0;
	request[1].tag = 32;
	request[2].size = 17;
	request[2].tag = 33;
	call_init(&client_stream, &f, 2, request, 3);
	expect(&client_stream, 8, 64);
	CHECK(call_start(&client_stream, NULL) == TR_OK);
	call_done(&client_stream, TR_RPC_STATUS_OK);

	for (unsigned i = 0; i < 3U; ++i) {
		request[i].size = MAX_BODY;
		request[i].tag = 41U + i;
	}
	call_init(&bidi, &f, 3, request, 3);
	for (unsigned i = 0; i < 3U; ++i)
		expect(&bidi, MAX_BODY, 41U + i);
	CHECK(call_start(&bidi, NULL) == TR_OK);
	call_done(&bidi, TR_RPC_STATUS_OK);
	CHECK(bidi.again > 0U);

	lock(&f);
	f.methods[3].response_metadata = METADATA_EMPTY;
	unlock(&f);
	call_init(&empty, &f, 3, NULL, 0);
	CHECK(call_start(&empty, NULL) == TR_OK);
	call_done(&empty, TR_RPC_STATUS_OK);
	lock(&f);
	CHECK(f.methods[3].requests == 0U);
	f.methods[0].expect_metadata = 0;
	unlock(&f);
	request[0].size = 0;
	request[0].tag = 0;
	call_init(&zero, &f, 0, request, 1);
	expect(&zero, 0, 0);
	CHECK(unary_start(&zero, NULL) == TR_OK);
	call_done(&zero, TR_RPC_STATUS_OK);
	lock(&f);
	CHECK(f.methods[0].requests == 1U);
	unlock(&f);

	call_init(&unknown, &f, METHOD_COUNT, NULL, 0);
	unknown.manual = 1;
	CHECK(call_start(&unknown, NULL) == TR_OK);
	call_done(&unknown, TR_RPC_STATUS_UNIMPLEMENTED);
	check_client_totals(&f.client, 7, 7, TR_RPC_STATUS_UNIMPLEMENTED, 1);
	/* 未知方法不建立服务端业务终局责任；两端准入数有意不同。 */
	check_server_totals(&f, 6, 6, TR_RPC_STATUS_OK, 0);
	drain(&f);
	/* 静默屏障之后才要求旧能力已经退役；不查看私有代次或槽号。 */
	CHECK(tr_rpc_v2_call_cancel(one.handle) == TR_ERR_STALE);
	struct tr_rpc_bytes no_body;
	no_body.data = NULL;
	no_body.len = 0;
	CHECK(tr_rpc_v2_call_send(one.handle, &no_body) == TR_ERR_STALE);
	call_init(&rejected, &f, 0, request, 1);
	CHECK(unary_start(&rejected, NULL) == TR_ERR_CLOSED);
	CHECK(rejected.terminals == 0U);
	check_client_totals(&f.client, 7, 7, TR_RPC_STATUS_UNIMPLEMENTED, 1);
	check_server_totals(&f, 6, 6, TR_RPC_STATUS_OK, 0);
	fixture_destroy(&f);
	CHECK(one.terminals == 1U && bidi.terminals == 1U &&
	      unknown.terminals == 1U);
	puts("sdk-v2: case=shapes-copy-metadata-pressure result=pass");
}

static void test_cancel_or_deadline(int deadline)
{
	struct fixture f;
	struct call_state blocked, healthy;
	struct message_spec request;
	struct tr_rpc_call_options options;
	enum tr_rpc_status expected = deadline ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                       : TR_RPC_STATUS_CANCELLED;
	fixture_create(&f);
	request.size = 23;
	request.tag = 71;
	call_init(&blocked, &f, 4, &request, 1);
	memset(&options, 0, sizeof(options));
	options.timeout_ms = deadline ? 2000U : 0U;
	CHECK(call_start(&blocked, &options) == TR_OK);
	wait_count(&f, &f.methods[4].entered, 1);
	request.tag = 81;
	call_init(&healthy, &f, 0, &request, 1);
	expect(&healthy, request.size, request.tag);
	CHECK(call_start(&healthy, NULL) == TR_OK);
	if (!deadline)
		CHECK(tr_rpc_v2_call_cancel(blocked.handle) == TR_OK);
	wait_count(&f, &blocked.terminals, 1);
	wait_count(&f, &f.methods[4].cancelled, 1);
	lock(&f);
	CHECK(blocked.status == expected && blocked.received == 0U);
	CHECK(blocked.initial == 0U && blocked.trailing == 0U);
	CHECK(f.methods[4].exited == 0U && f.methods[4].terminals == 0U);
	unlock(&f);
	call_done(&healthy, TR_RPC_STATUS_OK);
	check_client_totals(&f.client, 2, 2, expected, 1);
	check_server_totals(&f, 2, 1, TR_RPC_STATUS_OK, 0);
	/* 客户端已终局也不能跨过仍借用 request/reply 的服务端工作。 */
	CHECK(tr_server_drain(f.server, 0) == TR_AGAIN);
	lock(&f);
	f.methods[4].released = 1;
	publish(&f);
	unlock(&f);
	call_done(&blocked, expected);
	lock(&f);
	CHECK(f.methods[4].exited == 1U && f.methods[4].terminals == 1U);
	CHECK(f.methods[4].status == expected);
	CHECK(blocked.initial == 0U && blocked.trailing == 0U);
	unlock(&f);
	check_server_totals(&f, 2, 2, expected, 1);
	drain(&f);
	check_client_totals(&f.client, 2, 2, expected, 1);
	check_server_totals(&f, 2, 2, expected, 1);
	fixture_destroy(&f);
	CHECK(blocked.terminals == 1U && healthy.terminals == 1U);
	puts(deadline ? "sdk-v2: case=deadline-drain result=pass"
	              : "sdk-v2: case=cancel-isolation-drain result=pass");
}

static void test_destroy_live_terminal(void)
{
	struct fixture f;
	struct call_state pending;
	fixture_create(&f);
	call_init(&pending, &f, 0, NULL, 0);
	pending.manual = 1;
	CHECK(call_start(&pending, NULL) == TR_OK);
	wait_count(&f, &pending.writable_returns, 1);
	wait_count(&f, &f.methods[0].opens, 1);
	destroy_client(&f, &f.client);
	CHECK(pending.terminals == 1U);
	/* 已发布且仍活跃的连接先由 owner 的正常停止路径关闭。其
	 * UNAVAILABLE/LIVE 终局已经获胜，随后清理不能改成残留取消。
	 * SHUTDOWN 残留的精确交付域由内部故障专测独立验证。 */
	CHECK(pending.status == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(pending.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(pending.received == 0U);
	fixture_destroy(&f);
	CHECK(pending.terminals == 1U);
	puts("sdk-v2: case=destroy-live-terminal result=pass");
}

static void test_stats_peer_retirement(void)
{
	struct fixture f;
	struct observed_client peer;
	struct call_state first, second, surviving;
	struct message_spec request;
	/* 配置两个分片并从公开入口聚合；连接分配由 SO_REUSEPORT 决定，
	 * 此场景只断言真实跨 peer 及断连累计，不推断实际分片分布。 */
	fixture_create_shards(&f, 2);
	create_client(&f, &peer);
	request.size = 17;
	request.tag = 91;
	call_init(&first, &f, 0, &request, 1);
	expect(&first, request.size, request.tag);
	CHECK(call_start(&first, NULL) == TR_OK);
	call_done(&first, TR_RPC_STATUS_OK);
	check_client_totals(&f.client, 1, 1, TR_RPC_STATUS_OK, 0);

	request.tag = 101;
	call_init(&second, &f, 1, &request, 1);
	second.client = &peer;
	for (unsigned i = 0; i < 3U; ++i)
		expect(&second, request.size, request.tag + i);
	CHECK(call_start(&second, NULL) == TR_OK);
	call_done(&second, TR_RPC_STATUS_OK);
	check_client_totals(&peer, 1, 1, TR_RPC_STATUS_OK, 0);
	check_server_totals(&f, 2, 2, TR_RPC_STATUS_OK, 0);
	CHECK(memcmp(&f.methods[0].id, &f.methods[1].id,
	             sizeof(f.methods[0].id)) != 0);

	destroy_client(&f, &f.client);
	check_server_totals(&f, 2, 2, TR_RPC_STATUS_OK, 0);
	request.tag = 111;
	call_init(&surviving, &f, 0, &request, 1);
	surviving.client = &peer;
	expect(&surviving, request.size, request.tag);
	CHECK(call_start(&surviving, NULL) == TR_OK);
	call_done(&surviving, TR_RPC_STATUS_OK);
	check_client_totals(&peer, 2, 2, TR_RPC_STATUS_OK, 0);
	check_server_totals(&f, 3, 3, TR_RPC_STATUS_OK, 0);
	CHECK(tr_client_begin_drain(peer.handle) == TR_OK);
	CHECK(tr_client_wait_drained(peer.handle, WAIT_MS) == TR_OK);
	CHECK(tr_server_drain(f.server, WAIT_MS) == TR_OK);
	destroy_client(&f, &peer);
	check_server_totals(&f, 3, 3, TR_RPC_STATUS_OK, 0);
	fixture_destroy(&f);
	puts("sdk-v2: case=stats-peer-retirement result=pass");
}

static int producer_metadata(struct tr_rpc_v2_producer *producer,
                             enum tr_rpc_v2_metadata_phase phase)
{
	char key[] = "bin";
	char empty_key[] = "empty";
	uint8_t value[] = {0, 255, (uint8_t)phase, 0};
	struct tr_rpc_metadata entries[2];
	entries[0].key = key;
	entries[0].value = value;
	entries[0].value_len = sizeof(value);
	entries[1].key = empty_key;
	entries[1].value = NULL;
	entries[1].value_len = 0;
	int ret = phase == TR_RPC_V2_METADATA_INITIAL
	                  ? tr_rpc_v2_producer_send_initial_metadata(producer,
	                                                             entries, 2)
	                  : tr_rpc_v2_producer_finish(
	                            producer, TR_RPC_STATUS_OK, entries, 2);
	/* 成功已复制，AGAIN 没有保存；两种返回都立即复用所有输入。 */
	memset(key, 'q', sizeof(key));
	memset(empty_key, 'q', sizeof(empty_key));
	memset(value, 0x55, sizeof(value));
	memset(entries, 0, sizeof(entries));
	return ret;
}

static int producer_wait_until(struct tr_rpc_v2_producer *producer,
                               const struct timespec *limit)
{
	struct timespec now = deadline_after(0);
	CHECK(before(&now, limit));
	int64_t ns = (int64_t)(limit->tv_sec - now.tv_sec) * 1000000000LL +
	             limit->tv_nsec - now.tv_nsec;
	uint32_t ms = (uint32_t)((ns + 999999LL) / 1000000LL);
	CHECK(ms > 0U && ms <= WAIT_MS);
	return tr_rpc_v2_producer_wait_writable(producer, ms);
}

static void producer_send_copy(struct tr_rpc_v2_producer *producer,
                               unsigned size, unsigned tag)
{
	struct timespec limit = deadline_after(WAIT_MS);
	uint8_t storage[MAX_BODY];
	struct tr_rpc_bytes body;
	CHECK(size <= sizeof(storage));
	for (;;) {
		fill(storage, size, tag);
		body.data = size ? storage : NULL;
		body.len = size;
		int ret = tr_rpc_v2_producer_send(producer, &body);
		memset(storage, 0xe7, sizeof(storage));
		if (ret == TR_OK)
			return;
		CHECK(ret == TR_AGAIN);
		CHECK(producer_wait_until(producer, &limit) == TR_OK);
	}
}

static void producer_finish_copy(struct tr_rpc_v2_producer *producer)
{
	struct timespec limit = deadline_after(WAIT_MS);
	for (;;) {
		int ret = producer_metadata(producer,
		                            TR_RPC_V2_METADATA_TRAILING);
		if (ret == TR_OK)
			return;
		CHECK(ret == TR_AGAIN);
		CHECK(producer_wait_until(producer, &limit) == TR_OK);
	}
}

static void check_producer_closed(struct tr_rpc_v2_producer *producer)
{
	struct tr_rpc_bytes empty;
	empty.data = NULL;
	empty.len = 0;
	CHECK(tr_rpc_v2_producer_is_closed(producer) == 1);
	CHECK(tr_rpc_v2_producer_wait_writable(producer, 0) == TR_ERR_CLOSED);
	CHECK(tr_rpc_v2_producer_send(producer, &empty) == TR_ERR_CLOSED);
	CHECK(tr_rpc_v2_producer_send_initial_metadata(producer, NULL, 0) ==
	      TR_ERR_CLOSED);
	CHECK(tr_rpc_v2_producer_finish(producer, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_ERR_CLOSED);
}

static void *produce_async(void *arg)
{
	struct async_state *a = (struct async_state *)arg;
	struct fixture *f = a->fixture;
	uint8_t rejected[MAX_BODY];
	struct tr_rpc_bytes body;
	wait_count(f, &a->acquired, 1);
	struct tr_rpc_v2_producer *producer = a->producer;
	CHECK(tr_rpc_v2_producer_is_closed(producer) == 0);
	CHECK(producer_metadata(producer, TR_RPC_V2_METADATA_INITIAL) == TR_OK);
	fill(rejected, sizeof(rejected), 211);
	body.data = rejected;
	body.len = sizeof(rejected);
	/* 创建 work 仍由应用门闩阻塞，唯一槽不可能被 owner 提前消费。 */
	CHECK(tr_rpc_v2_producer_send(producer, &body) == TR_AGAIN);
	memset(rejected, 0x33, sizeof(rejected));
	CHECK(producer_metadata(producer, TR_RPC_V2_METADATA_TRAILING) ==
	      TR_AGAIN);
	CHECK(tr_rpc_v2_producer_wait_writable(producer, 0) == TR_ERR_TIMEOUT);
	CHECK(tr_rpc_v2_producer_wait_writable(producer, 1) == TR_ERR_TIMEOUT);
	lock(f);
	++a->staged;
	publish(f);
	unlock(f);
	if (a->mode == ASYNC_COMPLETE) {
		wait_count(f, &a->work_release, 1);
		/* 这个成功只会出现在创建 work 返回且 done 交接之后；不用
		 * 回调尾部标志或 sleep 冒充已经跨出借用期的证据。 */
		CHECK(tr_rpc_v2_producer_wait_writable(producer, WAIT_MS) ==
		      TR_OK);
		producer_send_copy(producer, MAX_BODY, 137);
		producer_send_copy(producer, 0, 0);
		lock(f);
		++a->produced;
		publish(f);
		unlock(f);
		wait_count(f, &a->finish_release, 1);
		producer_finish_copy(producer);
	} else {
		/* 取消发生在实际阻塞等待之前或之后，都必须观察同一关闭
		 * 谓词；这里没有依赖一次可丢失的应用通知。 */
		CHECK(tr_rpc_v2_producer_wait_writable(producer, WAIT_MS) ==
		      TR_ERR_CLOSED);
	}
	tr_rpc_v2_producer_release(producer);
	lock(f);
	++a->thread_done;
	publish(f);
	unlock(f);
	return NULL;
}

static void test_async_case(enum async_mode mode)
{
	struct fixture f;
	struct async_state a;
	struct call_state call;
	struct message_spec request;
	struct tr_rpc_call_options options;
	pthread_t thread;
	fixture_create(&f);
	memset(&a, 0, sizeof(a));
	a.fixture = &f;
	a.mode = mode;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &a;
	unlock(&f);
	request.size = 17;
	request.tag = 121;
	call_init(&call, &f, ASYNC_METHOD, &request, 1);
	if (mode == ASYNC_COMPLETE) {
		expect(&call, MAX_BODY, 137);
		expect(&call, 0, 0);
	}
	memset(&options, 0, sizeof(options));
	options.timeout_ms = mode == ASYNC_DEADLINE ? 2000U : 0U;
	CHECK(pthread_create(&thread, NULL, produce_async, &a) == 0);
	CHECK(call_start(&call, &options) == TR_OK);
	wait_count(&f, &a.staged, 1);
	check_client_totals(&f.client, 1, 0, TR_RPC_STATUS_OK, 0);
	check_server_totals(&f, 1, 0, TR_RPC_STATUS_OK, 0);
	lock(&f);
	CHECK(f.methods[ASYNC_METHOD].running == 1U);
	CHECK(!call.initial && !call.received && !call.trailing &&
	      !call.terminals);
	unlock(&f);
	if (mode == ASYNC_COMPLETE) {
		lock(&f);
		a.work_release = 1;
		publish(&f);
		unlock(&f);
		wait_count(&f, &a.produced, 1);
		wait_count(&f, &call.received, 2);
		wait_count(&f, &f.methods[ASYNC_METHOD].half_closes, 1);
		/* HALF_CLOSE 已继续执行，但活跃 producer 尚未选择终局。 */
		check_client_totals(&f.client, 1, 0, TR_RPC_STATUS_OK, 0);
		check_server_totals(&f, 1, 0, TR_RPC_STATUS_OK, 0);
		CHECK(tr_server_drain(f.server, 0) == TR_AGAIN);
		lock(&f);
		a.finish_release = 1;
		publish(&f);
		unlock(&f);
		call_done(&call, TR_RPC_STATUS_OK);
		check_client_totals(&f.client, 1, 1, TR_RPC_STATUS_OK, 0);
		check_server_totals(&f, 1, 1, TR_RPC_STATUS_OK, 0);
		drain(&f);
	} else {
		enum tr_rpc_status expected =
		        mode == ASYNC_DEADLINE ? TR_RPC_STATUS_DEADLINE_EXCEEDED
		                               : TR_RPC_STATUS_CANCELLED;
		if (mode == ASYNC_DESTROY) {
			lock(&f);
			f.server_destroying = 1;
			unlock(&f);
			CHECK(tr_server_destroy(f.server) == TR_AGAIN);
		} else {
			if (mode == ASYNC_CANCEL)
				CHECK(tr_rpc_v2_call_cancel(call.handle) ==
				      TR_OK);
			wait_count(&f, &call.terminals, 1);
			CHECK(call.status == expected);
			check_client_totals(&f.client, 1, 1, expected, 1);
			check_server_totals(&f, 1, 0, TR_RPC_STATUS_OK, 0);
			CHECK(tr_server_drain(f.server, 0) == TR_AGAIN);
		}
		/* STAGED 期间仅借用的原 work 令牌承诺及时观察取消；
		 * Client terminal 不是 producer 已关闭的屏障。 */
		wait_count(&f, &a.cancelled, 1);
		lock(&f);
		CHECK(!f.methods[ASYNC_METHOD].terminals);
		a.work_release = 1;
		publish(&f);
		unlock(&f);
		if (mode == ASYNC_DESTROY) {
			destroy_server(&f);
			wait_count(&f, &call.terminals, 1);
			CHECK(f.methods[ASYNC_METHOD].terminals == 1U);
			CHECK(f.methods[ASYNC_METHOD].status ==
			      TR_RPC_STATUS_CANCELLED);
			CHECK(f.methods[ASYNC_METHOD].delivery ==
			      TR_RPC_V2_DELIVERY_SHUTDOWN);
			CHECK(call.status == TR_RPC_STATUS_UNAVAILABLE);
			CHECK(call.delivery == TR_RPC_V2_DELIVERY_LIVE);
			check_client_totals(&f.client, 1, 1,
			                    TR_RPC_STATUS_UNAVAILABLE, 1);
		} else {
			call_done(&call, expected);
			CHECK(f.methods[ASYNC_METHOD].status == expected);
			check_server_totals(&f, 1, 1, expected, 1);
			drain(&f);
		}
		CHECK(call.initial == 0U && call.trailing == 0U &&
		      call.received == 0U);
	}
	/* terminal/排空不等这个控制线程的引用，更不负责 join 应用线程。 */
	wait_count(&f, &a.thread_done, 1);
	CHECK(pthread_join(thread, NULL) == 0);
	struct tr_rpc_v2_producer *retained = a.producer;
	check_producer_closed(retained);
	CHECK(tr_rpc_v2_producer_retain(retained) == TR_OK);
	tr_rpc_v2_producer_release(retained);
	fixture_destroy(&f);
	/* 最后一份应用引用仍有效，已成功销毁 Server 不能留下运行时借用。 */
	check_producer_closed(retained);
	tr_rpc_v2_producer_release(retained);
}

static void test_async_producer(void)
{
	test_async_case(ASYNC_COMPLETE);
	test_async_case(ASYNC_CANCEL);
	test_async_case(ASYNC_DEADLINE);
	test_async_case(ASYNC_DESTROY);
	puts("sdk-v2: case=async-producer-copy-stop result=pass");
}

static void test_default_factory_requires_methods(void)
{
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;

	CHECK(tr_client_create(NULL, NULL, 0, &client) == TR_ERR_INVALID);
	CHECK(!client);
	CHECK(tr_server_create(NULL, NULL, 0, &server) == TR_ERR_INVALID);
	CHECK(!server);
	CHECK(tr_client_create(NULL, NULL, 0, NULL) == TR_ERR_INVALID);
	CHECK(tr_server_create(NULL, NULL, 0, NULL) == TR_ERR_INVALID);
}

/* #280 的一元远端未知方法及本地非 ONE→ONE 拒绝在安装 SDK 中保留。 */
static void test_unary_rejections(void)
{
	struct fixture f;
	struct call_state unknown, rejected;
	struct message_spec spec;
	struct tr_rpc_v2_call sentinel;
	fixture_create(&f);
	spec.size = MAX_BODY;
	spec.tag = 91U;
	call_init(&unknown, &f, METHOD_COUNT, &spec, 1);
	CHECK(unary_start(&unknown, NULL) == TR_OK);
	call_done(&unknown, TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(unknown.writable_returns == 0U);
	call_init(&rejected, &f, 1, &spec, 1);
	memset(&rejected.handle, 0xa5, sizeof(rejected.handle));
	sentinel = rejected.handle;
	CHECK(unary_start(&rejected, NULL) == TR_ERR_UNSUPPORTED);
	CHECK(memcmp(&rejected.handle, &sentinel, sizeof(sentinel)) == 0);
	CHECK(rejected.terminals == 0U && rejected.writable_returns == 0U);
	check_client_totals(&f.client, 1, 1, TR_RPC_STATUS_UNIMPLEMENTED, 1);
	check_server_totals(&f, 0, 0, TR_RPC_STATUS_OK, 0);
	drain(&f);
	CHECK(tr_rpc_v2_call_cancel(unknown.handle) == TR_ERR_STALE);
	fixture_destroy(&f);
	puts("sdk-v2: case=unary-common-terminal result=pass");
}

int main(void)
{
	test_default_factory_requires_methods();
	test_unary_rejections();
	test_shapes_and_copy();
	test_cancel_or_deadline(0);
	test_cancel_or_deadline(1);
	test_destroy_live_terminal();
	test_stats_peer_retirement();
	test_async_producer();
	return 0;
}
