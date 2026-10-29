#define _POSIX_C_SOURCE 200809L

#include <tr/trpc.h>

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef TR_RPC_V2_TEST_FAULTS
#include <stdbool.h>
#include <stdatomic.h>
#endif

/* C/C++ 公共消费端会用 {0} 清零聚合对象；新 g++ 将其报为缺字段警告，
 * 但该初始化在两种语言中都会把剩余字段明确置零。 */
#if defined(__cplusplus) && (defined(__GNUC__) || defined(__clang__))
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#ifdef TR_RPC_V2_TEST_FAULTS
#ifdef __cplusplus
extern "C" {
#endif
void tr_rpc_v2_test_fail_next_stream_open(void);
unsigned tr_rpc_v2_test_stream_open_failures(void);
#ifdef __cplusplus
}
#endif
#endif

/* 同一源码须能脱离仓库作为 C11/C++11 消费端；不引用私有头或测试替身。 */
#define CHECK(expression)                                                      \
	do {                                                                   \
		if (!(expression)) {                                           \
			fprintf(stderr, "sdk-v2: line=%d check=%s\n",          \
			        __LINE__, #expression);                        \
			abort();                                               \
		}                                                              \
	} while (0)

#ifdef TR_RPC_V2_TEST_FAULTS
struct compression_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int open;
};

static _Atomic(struct compression_gate *) next_compression_gate;

struct tr_message_transform;
typedef bool (*compression_cancel_check_fn)(void *arg);
int __real_tr_message_transform_run_checked(
        struct tr_message_transform *operation,
        compression_cancel_check_fn check, void *arg);

int __wrap_tr_message_transform_run_checked(
        struct tr_message_transform *operation,
        compression_cancel_check_fn check, void *arg)
{
	struct compression_gate *gate = atomic_exchange_explicit(
	        &next_compression_gate, NULL, memory_order_acq_rel);
	if (gate) {
		CHECK(pthread_mutex_lock(&gate->lock) == 0);
		gate->entered = 1;
		CHECK(pthread_cond_broadcast(&gate->cond) == 0);
		while (!gate->open)
			CHECK(pthread_cond_wait(&gate->cond, &gate->lock) == 0);
		CHECK(pthread_mutex_unlock(&gate->lock) == 0);
	}
	return __real_tr_message_transform_run_checked(operation, check, arg);
}
#endif

#define METHOD_COUNT 7U
#define ASYNC_METHOD 5U
#define HOOK_METHOD 6U
#define MAX_BODY 4096U
#define LARGE_BODY (320U * 1024U)
#define MAX_MESSAGES 3U
#define WAIT_MS 15000U

struct fixture;

enum compression_direction_fixture {
	COMPRESSION_DIRECTIONS_DEFAULT,
	COMPRESSION_REQUEST_ONLY,
	COMPRESSION_RESPONSE_ONLY
};

static enum compression_direction_fixture compression_direction_override;

enum response_metadata { METADATA_NONE, METADATA_VALUES, METADATA_EMPTY };

enum async_mode {
	ASYNC_COMPLETE,
	ASYNC_REJECT_COMPRESSION,
	ASYNC_CANCEL,
	ASYNC_DEADLINE,
	ASYNC_DESTROY
};

struct async_state {
	struct fixture *fixture;
	struct tr_rpc_v2_producer *producer;
	enum async_mode mode;
	int producer_thread;
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
	int hold_parent_context;
	int block_on_message;
	int context_hold_expected;
	struct tr_rpc_v2_call_context *context_out;
	enum response_metadata response_metadata;
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
	struct tr_rpc_v2_call_id id;
	struct tr_rpc_v2_call_id hook_id;
	unsigned hook_observed_open;
	unsigned hook_observed_terminal;
	unsigned hook_admitted;
	unsigned hook_authorized;
	unsigned hook_authorize_entered;
	unsigned hook_authorize_returned;
	unsigned hook_authorize_cancel_seen;
	unsigned hook_order_count;
	unsigned hook_order[8];
	int hold_hook_authorize;
	enum tr_rpc_status hook_terminal_status;
	struct async_state *async;
};

struct fixture {
	struct observed_client client;
	struct tr_server *server;
	struct tr_client_config client_config;
	uint16_t port;
	uint32_t body_limit;
	int compress_sync_responses;
	int reject_sync_compressed_response;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int server_destroying;
	struct tr_rpc_v2_call_context parent_context;
	unsigned context_held;
	unsigned context_attempted;
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
	unsigned compress_requests;
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

#ifdef TR_RPC_V2_TEST_FAULTS
static void compression_gate_init(struct compression_gate *gate)
{
	pthread_condattr_t attr;
	memset(gate, 0, sizeof(*gate));
	CHECK(pthread_mutex_init(&gate->lock, NULL) == 0);
	CHECK(pthread_condattr_init(&attr) == 0);
	CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	CHECK(pthread_cond_init(&gate->cond, &attr) == 0);
	CHECK(pthread_condattr_destroy(&attr) == 0);
}

static void compression_gate_wait_entered(struct compression_gate *gate)
{
	struct timespec due = deadline_after(WAIT_MS);
	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	while (!gate->entered)
		CHECK(pthread_cond_timedwait(&gate->cond, &gate->lock, &due) ==
		      0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void compression_gate_release(struct compression_gate *gate)
{
	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	gate->open = 1;
	CHECK(pthread_cond_broadcast(&gate->cond) == 0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void compression_gate_destroy(struct compression_gate *gate)
{
	CHECK(pthread_cond_destroy(&gate->cond) == 0);
	CHECK(pthread_mutex_destroy(&gate->lock) == 0);
}
#endif

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

static void overwrite_source(uint8_t *bytes, unsigned size, uint8_t value)
{
	volatile uint8_t *source = bytes;
	for (unsigned i = 0; i < size; ++i)
		source[i] = value;
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

static void
append_response_with_compression(struct tr_rpc_v2_reply *reply, unsigned size,
                                 unsigned tag,
                                 enum tr_rpc_compression compression)
{
	uint8_t *storage = (uint8_t *)malloc(size ? size : 1U);
	struct tr_rpc_bytes body;
	CHECK(storage);
	fill(storage, size, tag);
	body.data = size ? storage : NULL;
	body.len = size;
	CHECK(tr_rpc_v2_reply_append_with_compression(reply, &body,
	                                              compression) == TR_OK);
	/* append 承诺复制；返回后立即复用原业务存储。 */
	overwrite_source(storage, size, 0xe7);
	free(storage);
}

static void append_response(struct tr_rpc_v2_reply *reply, unsigned size,
                            unsigned tag)
{
	append_response_with_compression(reply, size, tag,
	                                 TR_RPC_COMPRESSION_NONE);
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
	/* 生产线程存在时为它保留独立引用；控制线程始终持有创建引用。 */
	if (a->producer_thread)
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
	int reject_response_compression = 0;
	if (request->kind == TR_RPC_V2_REQUEST_OPEN && m->hold_parent_context) {
		struct tr_rpc_v2_call_context *context_out =
		        m->context_out ? m->context_out : &f->parent_context;
		int context_ret =
		        tr_rpc_v2_cancel_context_hold(cancel, context_out);
		CHECK(context_ret == m->context_hold_expected);
		lock(f);
		++f->context_attempted;
		if (context_ret == TR_OK)
			++f->context_held;
		publish(f);
		unlock(f);
	}
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
	if (m->index == HOOK_METHOD) {
		CHECK(m->hook_order_count <
		      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
		m->hook_order[m->hook_order_count++] = 4U;
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
		if (m->index == 4U || m->block_on_message) {
			cooperate(m, cancel);
			CHECK(tr_rpc_v2_cancel_requested(cancel) == 1);
			set_response_metadata(reply, TR_RPC_V2_METADATA_INITIAL,
			                      metadata);
			set_response_metadata(
			        reply, TR_RPC_V2_METADATA_TRAILING, metadata);
		} else if (m->index != 2U) {
			unsigned copies = m->index == 1U ? 3U : 1U;
			for (unsigned i = 0; i < copies; ++i) {
				if (f->compress_sync_responses && m->index < 2U)
					append_response_with_compression(
					        reply, request->body.len,
					        tag + i,
					        TR_RPC_COMPRESSION_ZSTD);
				else
					append_response(reply,
					                request->body.len,
					                tag + i);
			}
			if (requests == 1U && m->index != 1U)
				set_response_metadata(
				        reply, TR_RPC_V2_METADATA_INITIAL,
				        metadata);
		}
	} else if (request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		if (m->index == 2U && f->reject_sync_compressed_response) {
			uint8_t response[8];
			uint8_t expected[8];
			struct tr_rpc_bytes body;
			fill(response, sizeof(response), sum);
			fill(expected, sizeof(expected), sum);
			body.data = response;
			body.len = sizeof(response);
			CHECK(tr_rpc_v2_reply_append_with_compression(
			              reply, &body, TR_RPC_COMPRESSION_ZSTD) ==
			      TR_ERR_UNSUPPORTED);
			CHECK(memcmp(response, expected, sizeof(response)) ==
			      0);
			reject_response_compression = 1;
		} else if (m->index == 2U) {
			append_response(reply, 8U, sum);
		}
		/* INITIAL 的发送次序不依赖 setter 与同批 append 的调用次序。
		 * 空 MANY 也显式交付一个空 INITIAL，随后才交付尾部。 */
		if (!reject_response_compression) {
			if (m->index == 2U || (m->index == 3U && !requests))
				set_response_metadata(
				        reply, TR_RPC_V2_METADATA_INITIAL,
				        metadata);
			set_response_metadata(
			        reply, TR_RPC_V2_METADATA_TRAILING, metadata);
			CHECK(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) ==
			      TR_OK);
		}
	}
	lock(f);
	CHECK(m->running == 1U);
	--m->running;
	publish(f);
	unlock(f);
	if (reject_response_compression)
		return TR_RPC_STATUS_INVALID_ARGUMENT;
	return (m->index == 4U || m->block_on_message) &&
	                       request->kind == TR_RPC_V2_REQUEST_MESSAGE
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
	CHECK(m->terminals < m->opens ||
	      (m->index == HOOK_METHOD &&
	       m->terminals < m->hook_observed_open));
	CHECK(memcmp(&m->id, &call, sizeof(call)) == 0);
	++m->terminals;
	m->status = status;
	m->delivery = delivery;
	if (m->index == HOOK_METHOD) {
		CHECK(m->hook_order_count <
		      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
		m->hook_order[m->hook_order_count++] = 6U;
	}
	publish(f);
	unlock(f);
}

static void method_hook_observe(const struct tr_rpc_v2_observation *observation,
                                void *arg)
{
	struct method_state *m = (struct method_state *)arg;
	struct fixture *f = m->fixture;
	lock(f);
	if (observation->event == TR_RPC_V2_OBSERVE_OPEN) {
		CHECK(observation->request &&
		      observation->request->kind == TR_RPC_V2_REQUEST_OPEN);
		m->id = observation->call;
		m->hook_id = observation->call;
		++m->hook_observed_open;
		CHECK(m->hook_order_count <
		      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
		m->hook_order[m->hook_order_count++] = 1U;
	} else {
		CHECK(observation->event == TR_RPC_V2_OBSERVE_TERMINAL);
		CHECK(!observation->request &&
		      observation->status >= TR_RPC_STATUS_OK &&
		      observation->status < TR_RPC_STATUS_COUNT &&
		      observation->delivery == TR_RPC_V2_DELIVERY_LIVE);
		CHECK(memcmp(&m->hook_id, &observation->call,
		             sizeof(m->hook_id)) == 0);
		m->hook_terminal_status = observation->status;
		++m->hook_observed_terminal;
		CHECK(m->hook_order_count <
		      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
		m->hook_order[m->hook_order_count++] = 5U;
	}
	CHECK(m->hook_order_count <=
	      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
	publish(f);
	unlock(f);
}

static struct tr_rpc_v2_admission_result
method_hook_admit(struct tr_rpc_v2_call_id call,
                  const struct tr_rpc_v2_request *request, void *arg)
{
	struct method_state *m = (struct method_state *)arg;
	struct fixture *f = m->fixture;
	CHECK(request && request->kind == TR_RPC_V2_REQUEST_OPEN);
	lock(f);
	CHECK(memcmp(&m->hook_id, &call, sizeof(call)) == 0);
	++m->hook_admitted;
	CHECK(m->hook_order_count <
	      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
	m->hook_order[m->hook_order_count++] = 2U;
	CHECK(m->hook_order_count <=
	      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
	publish(f);
	unlock(f);
	struct tr_rpc_v2_admission_result result = {TR_RPC_V2_ADMISSION_ACCEPT,
	                                            TR_RPC_STATUS_OK};
	return result;
}

static struct tr_rpc_v2_authorization_result
method_hook_authorize(struct tr_rpc_v2_call_id call,
                      const struct tr_rpc_v2_request *request,
                      const struct tr_rpc_v2_cancel *cancel, void *arg)
{
	struct method_state *m = (struct method_state *)arg;
	struct fixture *f = m->fixture;
	CHECK(request && request->kind == TR_RPC_V2_REQUEST_OPEN);
	CHECK(tr_rpc_v2_cancel_requested(cancel) == 0);
	lock(f);
	CHECK(memcmp(&m->hook_id, &call, sizeof(call)) == 0);
	++m->hook_authorized;
	CHECK(m->hook_order_count <
	      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
	m->hook_order[m->hook_order_count++] = 3U;
	CHECK(m->hook_order_count <=
	      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
	if (m->hold_hook_authorize) {
		struct timespec limit = deadline_after(WAIT_MS);
		++m->hook_authorize_entered;
		publish(f);
		while (!m->hook_authorize_cancel_seen) {
			struct timespec due = deadline_after(10U);
			int requested = tr_rpc_v2_cancel_requested(cancel);
			CHECK(requested == 0 || requested == 1);
			if (requested) {
				++m->hook_authorize_cancel_seen;
				CHECK(m->hook_order_count <
				      sizeof(m->hook_order) /
				              sizeof(m->hook_order[0]));
				m->hook_order[m->hook_order_count++] = 7U;
				publish(f);
				break;
			}
			CHECK(before(&due, &limit));
			int ret = pthread_cond_timedwait(&f->cond, &f->lock,
			                                 &due);
			CHECK(ret == 0 || ret == ETIMEDOUT);
		}
		CHECK(m->hook_order_count <
		      sizeof(m->hook_order) / sizeof(m->hook_order[0]));
		m->hook_order[m->hook_order_count++] = 8U;
	}
	++m->hook_authorize_returned;
	publish(f);
	unlock(f);
	struct tr_rpc_v2_authorization_result result = {
	        TR_RPC_V2_AUTHORIZATION_ALLOW, TR_RPC_STATUS_OK};
	return result;
}

static void describe_methods(struct tr_rpc_method_desc *descriptors,
                             uint32_t body_limit)
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
		descriptors[i].max_request_bytes = body_limit;
		descriptors[i].max_response_bytes = body_limit;
	}
	descriptors[0].request_compression_algorithms =
	        TR_RPC_COMPRESSION_MASK_ZSTD;
	descriptors[0].response_compression_algorithms =
	        TR_RPC_COMPRESSION_MASK_ZSTD;
	descriptors[1].response_compression_algorithms =
	        TR_RPC_COMPRESSION_MASK_ZSTD;
	descriptors[3].request_compression_algorithms =
	        TR_RPC_COMPRESSION_MASK_ZSTD;
	descriptors[ASYNC_METHOD].response_compression_algorithms =
	        TR_RPC_COMPRESSION_MASK_ZSTD;
	if (compression_direction_override != COMPRESSION_DIRECTIONS_DEFAULT) {
		for (unsigned i = 0; i < METHOD_COUNT; ++i) {
			descriptors[i].request_compression_algorithms =
			        compression_direction_override ==
			                        COMPRESSION_REQUEST_ONLY
			                ? TR_RPC_COMPRESSION_MASK_ZSTD
			                : 0U;
			descriptors[i].response_compression_algorithms =
			        compression_direction_override ==
			                        COMPRESSION_RESPONSE_ONLY
			                ? TR_RPC_COMPRESSION_MASK_ZSTD
			                : 0U;
		}
	}
	descriptors[METHOD_COUNT] = descriptors[0];
	descriptors[METHOD_COUNT].method_id = 99;
}

static void create_client(struct fixture *f, struct observed_client *client)
{
	struct tr_rpc_method_desc descriptors[METHOD_COUNT + 1U];
	memset(client, 0, sizeof(*client));
	describe_methods(descriptors, f->body_limit);
	CHECK(tr_client_create(&f->client_config, descriptors,
	                       METHOD_COUNT + 1U, &client->handle) == TR_OK);
	memset(descriptors, 0, sizeof(descriptors));
	check_client_totals(client, 0, 0, TR_RPC_STATUS_OK, 0);
	CHECK(tr_client_connect(client->handle, "127.0.0.1", f->port) == TR_OK);
	CHECK(tr_client_wait_ready(client->handle, WAIT_MS) == TR_OK);
}

static void fixture_create_shards_with_limits(struct fixture *f,
                                              unsigned shards,
                                              uint32_t body_limit,
                                              uint32_t frame_limit,
                                              int allow_async_compression)
{
	struct tr_server_config server;
	struct tr_rpc_v2_method methods[METHOD_COUNT];
	struct tr_rpc_method_desc descriptors[METHOD_COUNT + 1U];
	pthread_condattr_t attr;
	memset(f, 0, sizeof(*f));
	f->body_limit = body_limit;
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
	server.limits.max_message_bytes = body_limit;
	server.limits.max_frame_payload_bytes = frame_limit;
	server.limits.initial_window_bytes = body_limit;
	/* 故意保留较大的公开默认阈值，由 V2 门面推导可推进的内部值。 */
	f->client_config.limits = server.limits;
	f->client_config.connect_timeout_ms = 2000;
	server.handshake_timeout_ms = 2000;
	memset(methods, 0, sizeof(methods));
	describe_methods(descriptors, body_limit);
	if (!allow_async_compression)
		descriptors[ASYNC_METHOD].response_compression_algorithms = 0U;
	for (unsigned i = 0; i < METHOD_COUNT; ++i) {
		f->methods[i].fixture = f;
		f->methods[i].index = i;
		f->methods[i].response_metadata = METADATA_VALUES;
		methods[i].descriptor = descriptors[i];
		methods[i].work = server_work;
		methods[i].terminal =
		        compression_direction_override ==
		                        COMPRESSION_DIRECTIONS_DEFAULT
		                ? server_terminal
		                : NULL;
		methods[i].arg = &f->methods[i];
		if (i == HOOK_METHOD) {
			methods[i].observe = method_hook_observe;
			methods[i].admit = method_hook_admit;
			methods[i].authorize = method_hook_authorize;
		}
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

static void fixture_create_shards(struct fixture *f, unsigned shards)
{
	fixture_create_shards_with_limits(f, shards, MAX_BODY, 256, 1);
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
	struct timespec limit = deadline_after(WAIT_MS);
	int ret;
	lock(f);
	client->destroying = 1;
	unlock(f);
	while ((ret = tr_client_destroy(client->handle)) == TR_AGAIN) {
		struct timespec now = deadline_after(0);
		struct timespec pause = {0, 1000000L};
		CHECK(before(&now, &limit));
		CHECK(nanosleep(&pause, NULL) == 0);
	}
	CHECK(ret == TR_OK);
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
			int ret =
			        c->compress_requests
			                ? tr_rpc_v2_call_send_with_compression(
			                          call, &body,
			                          TR_RPC_COMPRESSION_ZSTD)
			                : tr_rpc_v2_call_send(call, &body);
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

static int call_start_retry(struct call_state *c,
                            const struct tr_rpc_call_options *options)
{
	struct timespec limit = deadline_after(WAIT_MS);
	for (;;) {
		int ret = call_start(c, options);
		if (ret != TR_AGAIN)
			return ret;
		struct timespec now = deadline_after(0);
		struct timespec pause = {0, 1000000L};
		if (!before(&now, &limit))
			return TR_ERR_TIMEOUT;
		if (nanosleep(&pause, NULL) != 0)
			return TR_ERR_SYS;
	}
}

/* 安装后的同一 C11/C++11 消费端验证一元自动发送；正文仍只借用
 * 当前栈，收到响应和 FINAL 的回调沿用同一套观察与统计断言。 */
static int unary_start(struct call_state *c,
                       const struct tr_rpc_call_options *options)
{
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_bytes request;
	uint32_t capacity = c->fixture->body_limit + 1U;
	uint8_t *storage = (uint8_t *)malloc(capacity);
	CHECK(storage);
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
		oversized.len = capacity;
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
	overwrite_source(storage, capacity, 0xd3);
	free(storage);
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
	if (c->terminals != 1U || c->status != status) {
		struct tr_rpc_semantic_stats server_stats;
		int stats_ret = tr_server_get_rpc_semantic_stats(f->server,
		                                                 &server_stats);
		fprintf(stderr,
		        "sdk-v2: call terminal method=%u expected=%d actual=%d "
		        "received=%u/%u opens=%u server_stats=%d "
		        "started=%" PRIu64 " finished=%" PRIu64
		        " unavailable=%" PRIu64 "\n",
		        c->method, status, c->status, c->received,
		        c->response_count,
		        c->method < METHOD_COUNT ? f->methods[c->method].opens
		                                 : 0U,
		        stats_ret,
		        stats_ret == TR_OK ? server_stats.calls_started : 0U,
		        stats_ret == TR_OK ? server_stats.calls_finished : 0U,
		        stats_ret == TR_OK ? server_stats.final_status
		                                     [TR_RPC_STATUS_UNAVAILABLE]
		                           : 0U);
	}
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

static void test_rpc_v2_method_hooks(void)
{
	struct fixture f;
	struct call_state call;
	struct message_spec request = {17U, 73U};
	fixture_create(&f);
	call_init(&call, &f, HOOK_METHOD, &request, 1U);
	expect(&call, request.size, request.tag);
	CHECK(call_start(&call, NULL) == TR_OK);
	call_done(&call, TR_RPC_STATUS_OK);
	lock(&f);
	struct method_state *method = &f.methods[HOOK_METHOD];
	CHECK(method->hook_observed_open == 1U &&
	      method->hook_observed_terminal == 1U &&
	      method->hook_admitted == 1U && method->hook_authorized == 1U);
	CHECK(method->hook_order_count == 8U);
	CHECK(method->hook_order[0] == 1U && method->hook_order[1] == 2U &&
	      method->hook_order[2] == 3U && method->hook_order[3] == 4U &&
	      method->hook_order[4] == 4U && method->hook_order[5] == 4U &&
	      method->hook_order[6] == 5U && method->hook_order[7] == 6U);
	CHECK(method->hook_authorize_entered == 0U &&
	      method->hook_authorize_returned == 1U &&
	      method->hook_terminal_status == TR_RPC_STATUS_OK);
	unlock(&f);
	drain(&f);
	fixture_destroy(&f);
	puts("sdk-v2: case=server-method-hooks result=pass");
}

static void test_method_hook_late_authorization(int deadline)
{
	struct fixture f;
	struct call_state blocked, healthy;
	struct message_spec request = {17U, 61U};
	struct tr_rpc_call_options options;
	enum tr_rpc_status expected = deadline ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                       : TR_RPC_STATUS_CANCELLED;

	fixture_create(&f);
	struct method_state *method = &f.methods[HOOK_METHOD];
	lock(&f);
	method->hold_hook_authorize = 1;
	unlock(&f);

	call_init(&blocked, &f, HOOK_METHOD, &request, 1U);
	memset(&options, 0, sizeof(options));
	options.timeout_ms = deadline ? 2000U : 0U;
	CHECK(call_start(&blocked, &options) == TR_OK);
	wait_count(&f, &method->hook_authorize_entered, 1U);
	if (!deadline)
		CHECK(tr_rpc_v2_call_cancel(blocked.handle) == TR_OK);

	/* 授权回调在服务端自己的取消令牌置位后才返回 ALLOW，确保这是真正
	 * 迟于服务端取消终局的结果，而不是仅早于网络 RESET 到达。 */
	wait_count(&f, &method->hook_authorize_returned, 1U);
	call_done(&blocked, expected);
	lock(&f);
	CHECK(method->hook_authorize_cancel_seen == 1U);
	CHECK(method->opens == 0U && method->terminals == 1U &&
	      method->status == expected);
	CHECK(method->hook_observed_terminal == 1U &&
	      method->hook_terminal_status == expected);
	CHECK(method->hook_order_count == 7U && method->hook_order[0] == 1U &&
	      method->hook_order[1] == 2U && method->hook_order[2] == 3U &&
	      method->hook_order[3] == 7U && method->hook_order[4] == 8U &&
	      method->hook_order[5] == 5U && method->hook_order[6] == 6U);
	unlock(&f);

	/* 迟到 ALLOW 返回后，健康调用仍经公共 TCP Client/Server 完成，旧调用
	 * 则不能重新进入业务 work。 */
	request.tag = 83U;
	call_init(&healthy, &f, 0U, &request, 1U);
	expect(&healthy, request.size, request.tag);
	CHECK(call_start(&healthy, NULL) == TR_OK);
	call_done(&healthy, TR_RPC_STATUS_OK);
	lock(&f);
	CHECK(method->opens == 0U && method->terminals == 1U);
	unlock(&f);
	check_client_totals(&f.client, 2U, 2U, expected, 1U);
	check_server_totals(&f, 2U, 2U, expected, 1U);
	drain(&f);
	fixture_destroy(&f);
	CHECK(blocked.terminals == 1U && healthy.terminals == 1U);
	puts(deadline ? "sdk-v2: case=method-hook-late-allow-after-deadline "
	                "result=pass"
	              : "sdk-v2: case=method-hook-late-allow-after-cancel "
	                "result=pass");
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
	f.compress_sync_responses = 1;

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
	options.request_compression = TR_RPC_COMPRESSION_ZSTD;
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
	bidi.compress_requests = 1U;
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

static void test_call_compression_rejection_preserves_next_message(void)
{
	struct fixture f;
	struct call_state call;
	struct message_spec request = {17U, 73U};
	uint8_t storage[17];
	struct tr_rpc_bytes body = {storage, sizeof(storage)};

	fixture_create(&f);
	call_init(&call, &f, 2U, &request, 1U);
	call.manual = 1;
	expect(&call, 8U, request.tag);
	CHECK(call_start(&call, NULL) == TR_OK);
	wait_count(&f, &call.writable_returns, 1U);
	wait_count(&f, &f.methods[2].opens, 1U);
	fill(storage, sizeof(storage), request.tag);
	CHECK(tr_rpc_v2_call_send_with_compression(call.handle, &body,
	                                           TR_RPC_COMPRESSION_ZSTD) ==
	      TR_ERR_UNSUPPORTED);
	CHECK(body.len == sizeof(storage));
	CHECK(tr_rpc_v2_call_send_with_compression(
	              call.handle, &body, TR_RPC_COMPRESSION_NONE) == TR_OK);
	CHECK(tr_rpc_v2_call_half_close(call.handle) == TR_OK);
	lock(&f);
	call.sent = 1U;
	call.half_closed = 1;
	unlock(&f);
	call_done(&call, TR_RPC_STATUS_OK);
	drain(&f);
	fixture_destroy(&f);
	puts("sdk-v2: case=compression-selection-ownership result=pass");
}

static void test_reply_compression_policy_rejection(void)
{
	struct fixture f;
	struct call_state call;
	struct message_spec request = {17U, 73U};
	fixture_create(&f);
	f.reject_sync_compressed_response = 1;
	call_init(&call, &f, 2U, &request, 1U);
	call.response_metadata = METADATA_NONE;
	CHECK(call_start(&call, NULL) == TR_OK);
	call_done(&call, TR_RPC_STATUS_INVALID_ARGUMENT);
	CHECK(call.received == 0U);
	CHECK(f.methods[2].status == TR_RPC_STATUS_INVALID_ARGUMENT);
	check_client_totals(&f.client, 1, 1, TR_RPC_STATUS_INVALID_ARGUMENT, 1);
	check_server_totals(&f, 1, 1, TR_RPC_STATUS_INVALID_ARGUMENT, 1);
	drain(&f);
	fixture_destroy(&f);
	puts("sdk-v2: case=reply-compression-policy result=pass");
}

static void test_directional_compression_only(void)
{
	struct fixture f;
	struct call_state call;
	struct message_spec request = {MAX_BODY, 47U};
	struct tr_rpc_call_options options;

	memset(&options, 0, sizeof(options));
	compression_direction_override = COMPRESSION_REQUEST_ONLY;
	fixture_create(&f);
	call_init(&call, &f, 0U, &request, 1U);
	call.server_terminal_target = 0U;
	expect(&call, request.size, request.tag);
	options.request_compression = TR_RPC_COMPRESSION_ZSTD;
	CHECK(unary_start(&call, &options) == TR_OK);
	call_done(&call, TR_RPC_STATUS_OK);
	drain(&f);
	fixture_destroy(&f);

	compression_direction_override = COMPRESSION_RESPONSE_ONLY;
	fixture_create(&f);
	f.compress_sync_responses = 1;
	call_init(&call, &f, 0U, &request, 1U);
	call.server_terminal_target = 0U;
	expect(&call, request.size, request.tag);
	CHECK(unary_start(&call, NULL) == TR_OK);
	call_done(&call, TR_RPC_STATUS_OK);
	drain(&f);
	fixture_destroy(&f);

	compression_direction_override = COMPRESSION_DIRECTIONS_DEFAULT;
	puts("sdk-v2: case=directional-compression-only result=pass");
}

static void test_large_message_ownership(void)
{
	struct fixture f;
	struct call_state call;
	struct message_spec request = {LARGE_BODY, 53};
	fixture_create_shards_with_limits(&f, 1, LARGE_BODY, 64U * 1024U, 1);
	call_init(&call, &f, 0, &request, 1);
	expect(&call, LARGE_BODY, 53);
	CHECK(unary_start(&call, NULL) == TR_OK);
	call_done(&call, TR_RPC_STATUS_OK);
	drain(&f);
	fixture_destroy(&f);
	puts("sdk-v2: case=large-message-ownership result=pass");
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

#ifdef TR_RPC_V2_TEST_FAULTS
static void test_cancel_compressed_call_destroy_barrier(void)
{
	struct compression_gate gate;
	struct fixture f;
	struct call_state call;
	struct message_spec request = {MAX_BODY, 117U};
	uint8_t storage[MAX_BODY];
	struct tr_rpc_bytes body;
	struct tr_rpc_v2_call_ref ref = {0};
	struct tr_rpc_v2_call_result result = {TR_RPC_STATUS_INTERNAL,
	                                       TR_RPC_V2_DELIVERY_SHUTDOWN};

	fixture_create_shards_with_limits(&f, 1, MAX_BODY, 1024U, 1);
	call_init(&call, &f, 0, &request, 1U);
	call.manual = 1;
	compression_gate_init(&gate);
	atomic_store_explicit(&next_compression_gate, &gate,
	                      memory_order_release);
	CHECK(call_start(&call, NULL) == TR_OK);
	wait_count(&f, &call.writable_returns, 1U);
	wait_count(&f, &f.methods[0].opens, 1U);
	fill(storage, sizeof(storage), request.tag);
	body.data = storage;
	body.len = sizeof(storage);
	CHECK(tr_rpc_v2_call_send_with_compression(
	              call.handle, &body, TR_RPC_COMPRESSION_ZSTD) == TR_OK);
	overwrite_source(storage, sizeof(storage), 0xd3);
	lock(&f);
	call.sent = 1U;
	unlock(&f);
	compression_gate_wait_entered(&gate);

	/* 压缩任务已进入 worker 执行入口并挂起，此时公开引用取消调用。 */
	CHECK(tr_rpc_v2_call_hold(call.handle, &ref) == TR_OK);
	CHECK(tr_rpc_v2_call_ref_cancel(ref) == TR_OK);
	CHECK(tr_rpc_v2_call_ref_result(ref, &result) == TR_AGAIN);
	CHECK(result.status == TR_RPC_STATUS_INTERNAL &&
	      result.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(tr_rpc_v2_call_ref_release(&ref) == TR_OK);
	lock(&f);
	CHECK(call.terminals == 0U);
	unlock(&f);

	/* 引用已释放，销毁仍须等待被取消但尚未退役的压缩任务。 */
	CHECK(tr_client_destroy(f.client.handle) == TR_AGAIN);
	CHECK(f.client.handle != NULL);
	compression_gate_release(&gate);
	destroy_client(&f, &f.client);

	/* 本地销毁不是对端回调屏障；等待服务端独立观察断连。 */
	wait_count(&f, &f.methods[0].terminals, 1U);
	lock(&f);
	CHECK(call.sent == 1U && !call.half_closed && call.terminals == 1U);
	CHECK(call.status == TR_RPC_STATUS_CANCELLED &&
	      call.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(f.methods[0].opens == 1U && f.methods[0].requests == 0U &&
	      f.methods[0].terminals == 1U &&
	      f.methods[0].status == TR_RPC_STATUS_UNAVAILABLE);
	unlock(&f);
	check_server_totals(&f, 1U, 1U, TR_RPC_STATUS_UNAVAILABLE, 1U);
	destroy_server(&f);
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
	compression_gate_destroy(&gate);
	puts("sdk-v2: case=cancel-compressed-destroy-barrier result=pass");
}
#endif

static void test_parent_context_lifetime(void)
{
	struct fixture f;
	struct call_state parent, rejected;
	struct tr_rpc_call_options options = {0};
	struct tr_rpc_v2_call sentinel;
	struct timespec limit = deadline_after(WAIT_MS);
	int ret;

	fixture_create(&f);
	f.methods[3].hold_parent_context = 1;
	call_init(&parent, &f, 3U, NULL, 0U);
	CHECK(call_start(&parent, NULL) == TR_OK);
	wait_count(&f, &f.context_held, 1U);
	call_done(&parent, TR_RPC_STATUS_OK);
	options.parent_context = &f.parent_context;
	call_init(&rejected, &f, 0U, NULL, 0U);
	memset(&rejected.handle, 0xa5, sizeof(rejected.handle));
	sentinel = rejected.handle;
	CHECK(call_start(&rejected, &options) == TR_ERR_CANCELLED);
	CHECK(memcmp(&rejected.handle, &sentinel, sizeof(sentinel)) == 0);
	CHECK(rejected.terminals == 0U && rejected.writable_returns == 0U);
	drain(&f);
	destroy_client(&f, &f.client);
	lock(&f);
	f.server_destroying = 1;
	unlock(&f);
	CHECK(tr_server_destroy(f.server) == TR_AGAIN);
	CHECK(tr_rpc_v2_call_context_release(&f.parent_context) == TR_OK);
	CHECK(f.parent_context._private[0] == 0U &&
	      f.parent_context._private[1] == 0U);
	while ((ret = tr_server_destroy(f.server)) == TR_AGAIN) {
		struct timespec now = deadline_after(0);
		struct timespec pause = {0, 1000000L};
		CHECK(before(&now, &limit));
		CHECK(nanosleep(&pause, NULL) == 0);
	}
	CHECK(ret == TR_OK);
	f.server = NULL;
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
	CHECK(parent.terminals == 1U && rejected.terminals == 0U);
	puts("sdk-v2: case=parent-context-lifetime result=pass");
}

static void test_parent_context_pool_exhaustion(void)
{
	struct fixture f;
	struct tr_rpc_v2_call_context contexts[5];
	struct tr_rpc_v2_call_context stale_context;
	struct call_state calls[5];
	struct message_spec requests[4] = {
	        {17U, 31U}, {19U, 41U}, {23U, 53U}, {0U, 0U}};
	struct timespec limit = deadline_after(WAIT_MS);
	int ret;

	fixture_create(&f);
	memset(contexts, 0, sizeof(contexts));
	for (unsigned i = 0; i < 4U; ++i) {
		f.methods[i].hold_parent_context = 1;
		f.methods[i].context_out = &contexts[i];
		call_init(&calls[i], &f, i, i < 3U ? &requests[i] : NULL,
		          i < 3U ? 1U : 0U);
		if (i == 0U)
			expect(&calls[i], requests[i].size, requests[i].tag);
		else if (i == 1U) {
			for (unsigned n = 0; n < 3U; ++n)
				expect(&calls[i], requests[i].size,
				       requests[i].tag + n);
		} else if (i == 2U) {
			expect(&calls[i], 8U, requests[i].tag);
		}
		CHECK(call_start_retry(&calls[i], NULL) == TR_OK);
		wait_count(&f, &f.context_held, i + 1U);
		call_done(&calls[i], TR_RPC_STATUS_OK);
	}
	f.methods[3].context_out = &contexts[4];
	f.methods[3].context_hold_expected = TR_AGAIN;
	call_init(&calls[4], &f, 3U, NULL, 0U);
	CHECK(call_start_retry(&calls[4], NULL) == TR_OK);
	wait_count(&f, &f.context_attempted, 5U);
	CHECK(contexts[4]._private[0] == 0U && contexts[4]._private[1] == 0U);
	call_done(&calls[4], TR_RPC_STATUS_OK);
	/* 只保留释放后失效的 token 字节用于代次校验，不再把它当所有者。 */
	stale_context = contexts[0];
	CHECK(tr_rpc_v2_call_context_release(&contexts[0]) == TR_OK);
	f.methods[3].context_hold_expected = TR_OK;
	call_init(&calls[4], &f, 3U, NULL, 0U);
	CHECK(call_start_retry(&calls[4], NULL) == TR_OK);
	wait_count(&f, &f.context_held, 5U);
	call_done(&calls[4], TR_RPC_STATUS_OK);
	CHECK(stale_context._private[0] == contexts[4]._private[0]);
	CHECK(stale_context._private[1] != contexts[4]._private[1]);
	struct tr_rpc_call_options stale_options = {0};
	struct call_state stale_call;
	struct tr_rpc_v2_call sentinel;
	stale_options.parent_context = &stale_context;
	call_init(&stale_call, &f, 0U, NULL, 0U);
	memset(&stale_call.handle, 0xa5, sizeof(stale_call.handle));
	sentinel = stale_call.handle;
	CHECK(call_start(&stale_call, &stale_options) == TR_ERR_STALE);
	CHECK(memcmp(&stale_call.handle, &sentinel, sizeof(sentinel)) == 0);
	CHECK(stale_call.terminals == 0U && stale_call.writable_returns == 0U);
	drain(&f);
	destroy_client(&f, &f.client);
	lock(&f);
	f.server_destroying = 1;
	unlock(&f);
	CHECK(tr_server_destroy(f.server) == TR_AGAIN);
	for (unsigned i = 1U; i < 4U; ++i)
		CHECK(tr_rpc_v2_call_context_release(&contexts[i]) == TR_OK);
	CHECK(tr_rpc_v2_call_context_release(&contexts[4]) == TR_OK);
	while ((ret = tr_server_destroy(f.server)) == TR_AGAIN) {
		struct timespec now = deadline_after(0);
		struct timespec pause = {0, 1000000L};
		CHECK(before(&now, &limit));
		CHECK(nanosleep(&pause, NULL) == 0);
	}
	CHECK(ret == TR_OK);
	f.server = NULL;
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
	puts("sdk-v2: case=parent-context-pool-exhaustion result=pass");
}

static void test_parent_context_propagation(int deadline,
                                            uint32_t child_timeout_ms)
{
	struct fixture f;
	struct async_state async = {0};
	struct call_state parent, child, sibling;
	struct message_spec request = {23U, 71U};
	struct message_spec sibling_request = {19U, 83U};
	struct tr_rpc_call_options parent_options = {0};
	struct tr_rpc_call_options child_options = {0};
	uint8_t storage[23];
	struct tr_rpc_bytes body = {storage, sizeof(storage)};
	uint8_t sibling_storage[19];
	struct tr_rpc_bytes sibling_body = {sibling_storage,
	                                    sizeof(sibling_storage)};
	enum tr_rpc_status expected = deadline ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                       : TR_RPC_STATUS_CANCELLED;

	fixture_create(&f);
	async.fixture = &f;
	async.mode = deadline ? ASYNC_DEADLINE : ASYNC_CANCEL;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &async;
	f.methods[ASYNC_METHOD].hold_parent_context = 1;
	f.methods[3].block_on_message = 1;
	unlock(&f);
	parent_options.timeout_ms = deadline ? 1000U : 0U;
	call_init(&parent, &f, ASYNC_METHOD, NULL, 0U);
	CHECK(call_start(&parent, &parent_options) == TR_OK);
	wait_count(&f, &f.context_held, 1U);
	wait_count(&f, &async.acquired, 1U);
	child_options.parent_context = &f.parent_context;
	child_options.timeout_ms = child_timeout_ms;
	call_init(&child, &f, 4U, &request, 1U);
	child.manual = 1;
	CHECK(call_start(&child, &child_options) == TR_OK);
	wait_count(&f, &child.writable_returns, 1U);
	fill(storage, sizeof(storage), request.tag);
	CHECK(tr_rpc_v2_call_send(child.handle, &body) == TR_OK);
	CHECK(tr_rpc_v2_call_half_close(child.handle) == TR_OK);
	lock(&f);
	child.sent = 1U;
	child.half_closed = 1;
	publish(&f);
	unlock(&f);
	wait_count(&f, &f.methods[4].entered, 1U);
	call_init(&sibling, &f, 3U, &sibling_request, 1U);
	sibling.manual = 1;
	CHECK(call_start(&sibling, &child_options) == TR_OK);
	wait_count(&f, &sibling.writable_returns, 1U);
	fill(sibling_storage, sizeof(sibling_storage), sibling_request.tag);
	CHECK(tr_rpc_v2_call_send(sibling.handle, &sibling_body) == TR_OK);
	CHECK(tr_rpc_v2_call_half_close(sibling.handle) == TR_OK);
	lock(&f);
	sibling.sent = 1U;
	sibling.half_closed = 1;
	publish(&f);
	unlock(&f);
	wait_count(&f, &f.methods[3].entered, 1U);
	if (!deadline)
		CHECK(tr_rpc_v2_call_context_release(&f.parent_context) ==
		      TR_OK);
	if (!deadline)
		CHECK(tr_rpc_v2_call_cancel(parent.handle) == TR_OK);
	wait_count(&f, &parent.terminals, 1U);
	wait_count(&f, &child.terminals, 1U);
	wait_count(&f, &sibling.terminals, 1U);
	wait_count(&f, &async.cancelled, 1U);
	wait_count(&f, &f.methods[4].cancelled, 1U);
	wait_count(&f, &f.methods[3].cancelled, 1U);
	lock(&f);
	CHECK(parent.status == expected && child.status == expected &&
	      sibling.status == expected);
	CHECK(child.received == 0U && child.initial == 0U &&
	      child.trailing == 0U);
	CHECK(sibling.received == 0U && sibling.initial == 0U &&
	      sibling.trailing == 0U);
	CHECK(f.methods[ASYNC_METHOD].terminals == 0U &&
	      f.methods[4].terminals == 0U && f.methods[3].terminals == 0U);
	async.work_release = 1U;
	f.methods[4].released = 1U;
	f.methods[3].released = 1U;
	publish(&f);
	unlock(&f);
	call_done(&parent, expected);
	call_done(&child, expected);
	call_done(&sibling, expected);
	if (deadline) {
		struct call_state rejected;
		struct tr_rpc_v2_call sentinel;
		struct tr_rpc_call_options rejected_options = {0};
		rejected_options.parent_context = &f.parent_context;
		call_init(&rejected, &f, 0U, NULL, 0U);
		memset(&rejected.handle, 0xa5, sizeof(rejected.handle));
		sentinel = rejected.handle;
		CHECK(call_start(&rejected, &rejected_options) ==
		      TR_ERR_TIMEOUT);
		CHECK(memcmp(&rejected.handle, &sentinel, sizeof(sentinel)) ==
		      0);
		CHECK(rejected.terminals == 0U &&
		      rejected.writable_returns == 0U);
		CHECK(tr_rpc_v2_call_context_release(&f.parent_context) ==
		      TR_OK);
	}
	tr_rpc_v2_producer_release(async.producer);
	check_client_totals(&f.client, 3U, 3U, expected, 3U);
	check_server_totals(&f, 3U, 3U, expected, 3U);
	drain(&f);
	fixture_destroy(&f);
	if (deadline && !child_timeout_ms)
		puts("sdk-v2: case=parent-context-deadline-zero-child-timeout "
		     "result=pass");
	else
		puts(deadline ? "sdk-v2: case=parent-context-deadline "
		                "result=pass"
		              : "sdk-v2: case=parent-context-cancel "
		                "result=pass");
}

static void test_child_deadline_isolation(void)
{
	struct fixture f;
	struct async_state async = {0};
	struct call_state parent, child;
	struct message_spec request = {23U, 91U};
	struct tr_rpc_call_options parent_options = {0};
	struct tr_rpc_call_options child_options = {0};
	uint8_t storage[23];
	struct tr_rpc_bytes body = {storage, sizeof(storage)};
	struct tr_rpc_semantic_stats client_stats, server_stats;

	fixture_create(&f);
	async.fixture = &f;
	async.mode = ASYNC_CANCEL;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &async;
	f.methods[ASYNC_METHOD].hold_parent_context = 1;
	unlock(&f);
	call_init(&parent, &f, ASYNC_METHOD, NULL, 0U);
	CHECK(call_start(&parent, &parent_options) == TR_OK);
	wait_count(&f, &f.context_held, 1U);
	wait_count(&f, &async.acquired, 1U);
	child_options.parent_context = &f.parent_context;
	child_options.timeout_ms = 100U;
	call_init(&child, &f, 4U, &request, 1U);
	child.manual = 1;
	CHECK(call_start(&child, &child_options) == TR_OK);
	wait_count(&f, &child.writable_returns, 1U);
	fill(storage, sizeof(storage), request.tag);
	CHECK(tr_rpc_v2_call_send(child.handle, &body) == TR_OK);
	CHECK(tr_rpc_v2_call_half_close(child.handle) == TR_OK);
	lock(&f);
	child.sent = 1U;
	child.half_closed = 1;
	publish(&f);
	unlock(&f);
	wait_count(&f, &f.methods[4].entered, 1U);
	wait_count(&f, &child.terminals, 1U);
	wait_count(&f, &f.methods[4].cancelled, 1U);
	lock(&f);
	CHECK(child.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(parent.terminals == 0U && async.cancelled == 0U);
	unlock(&f);
	CHECK(tr_rpc_v2_call_cancel(parent.handle) == TR_OK);
	wait_count(&f, &async.cancelled, 1U);
	lock(&f);
	async.work_release = 1U;
	f.methods[4].released = 1U;
	publish(&f);
	unlock(&f);
	call_done(&parent, TR_RPC_STATUS_CANCELLED);
	call_done(&child, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	tr_rpc_v2_producer_release(async.producer);
	CHECK(tr_client_get_rpc_semantic_stats(f.client.handle,
	                                       &client_stats) == TR_OK);
	CHECK(tr_server_get_rpc_semantic_stats(f.server, &server_stats) ==
	      TR_OK);
	check_stats_invariants(&client_stats);
	check_stats_invariants(&server_stats);
	CHECK(client_stats.calls_started == 2U &&
	      client_stats.calls_finished == 2U);
	CHECK(server_stats.calls_started == 2U &&
	      server_stats.calls_finished == 2U);
	CHECK(client_stats.final_status[TR_RPC_STATUS_CANCELLED] == 1U &&
	      client_stats.final_status[TR_RPC_STATUS_DEADLINE_EXCEEDED] == 1U);
	CHECK(server_stats.final_status[TR_RPC_STATUS_CANCELLED] == 1U &&
	      server_stats.final_status[TR_RPC_STATUS_DEADLINE_EXCEEDED] == 1U);
	drain(&f);
	CHECK(tr_rpc_v2_call_context_release(&f.parent_context) == TR_OK);
	fixture_destroy(&f);
	puts("sdk-v2: case=child-deadline-isolation result=pass");
}

/* 已选定的父期限终局不能被停止清理重复发布的取消覆盖。 */
static void *produce_async(void *arg);

static void test_parent_deadline_survives_stopped_reap(void)
{
	struct fixture f;
	struct async_state async = {0};
	struct call_state parent, rejected;
	struct tr_rpc_call_options parent_options = {0};
	struct tr_rpc_call_options child_options = {0};
	struct tr_rpc_v2_call sentinel;
	struct timespec limit = deadline_after(WAIT_MS);
	pthread_t thread;
	struct timespec pause = {0, 1000000L};
	int ret;

	fixture_create(&f);
	async.fixture = &f;
	async.mode = ASYNC_DEADLINE;
	async.producer_thread = 1;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &async;
	f.methods[ASYNC_METHOD].hold_parent_context = 1;
	unlock(&f);
	parent_options.timeout_ms = 100U;
	call_init(&parent, &f, ASYNC_METHOD, NULL, 0U);
	CHECK(pthread_create(&thread, NULL, produce_async, &async) == 0);
	CHECK(call_start(&parent, &parent_options) == TR_OK);
	wait_count(&f, &f.context_held, 1U);
	wait_count(&f, &async.staged, 1U);
	wait_count(&f, &parent.terminals, 1U);
	wait_count(&f, &async.cancelled, 1U);
	CHECK(parent.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);

	lock(&f);
	f.server_destroying = 1;
	unlock(&f);
	/* 这次销毁调用会切到 stopped reaper；应用 work 仍在门闩上。 */
	CHECK(tr_server_destroy(f.server) == TR_AGAIN);
	child_options.parent_context = &f.parent_context;
	call_init(&rejected, &f, 0U, NULL, 0U);
	memset(&rejected.handle, 0xa5, sizeof(rejected.handle));
	sentinel = rejected.handle;
	CHECK(call_start(&rejected, &child_options) == TR_ERR_TIMEOUT);
	CHECK(memcmp(&rejected.handle, &sentinel, sizeof(sentinel)) == 0);
	CHECK(rejected.terminals == 0U && rejected.writable_returns == 0U);

	lock(&f);
	async.work_release = 1U;
	publish(&f);
	unlock(&f);
	while (!tr_rpc_v2_producer_is_closed(async.producer)) {
		struct timespec now = deadline_after(0);
		CHECK(before(&now, &limit));
		ret = tr_server_destroy(f.server);
		CHECK(ret == TR_AGAIN);
		CHECK(nanosleep(&pause, NULL) == 0);
	}
	wait_count(&f, &async.thread_done, 1U);
	CHECK(pthread_join(thread, NULL) == 0);
	call_done(&parent, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	tr_rpc_v2_producer_release(async.producer);
	destroy_client(&f, &f.client);
	CHECK(tr_rpc_v2_call_context_release(&f.parent_context) == TR_OK);
	destroy_server(&f);
	CHECK(f.methods[ASYNC_METHOD].terminals == 1U);
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
	puts("sdk-v2: case=parent-deadline-survives-stopped-reap result=pass");
}

#ifdef TR_RPC_V2_TEST_FAULTS
/* 准入后开流失败必须归还此前借用的父上下文引用。 */
static void test_parent_context_open_failure_releases_reference(void)
{
	struct fixture f;
	struct async_state async = {0};
	struct call_state parent, rejected;
	struct tr_rpc_call_options options = {0};
	struct tr_rpc_v2_call sentinel;
	pthread_t thread;
	unsigned failures = tr_rpc_v2_test_stream_open_failures();

	fixture_create(&f);
	async.fixture = &f;
	async.mode = ASYNC_CANCEL;
	async.producer_thread = 1;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &async;
	f.methods[ASYNC_METHOD].hold_parent_context = 1;
	unlock(&f);
	call_init(&parent, &f, ASYNC_METHOD, NULL, 0U);
	CHECK(pthread_create(&thread, NULL, produce_async, &async) == 0);
	CHECK(call_start(&parent, NULL) == TR_OK);
	wait_count(&f, &f.context_held, 1U);
	wait_count(&f, &async.acquired, 1U);
	wait_count(&f, &async.staged, 1U);

	options.parent_context = &f.parent_context;
	call_init(&rejected, &f, 0U, NULL, 0U);
	memset(&rejected.handle, 0xa5, sizeof(rejected.handle));
	sentinel = rejected.handle;
	tr_rpc_v2_test_fail_next_stream_open();
	CHECK(call_start(&rejected, &options) == TR_ERR_NOMEM);
	CHECK(tr_rpc_v2_test_stream_open_failures() == failures + 1U);
	CHECK(memcmp(&rejected.handle, &sentinel, sizeof(sentinel)) == 0);
	CHECK(rejected.terminals == 0U && rejected.writable_returns == 0U);

	CHECK(tr_rpc_v2_call_cancel(parent.handle) == TR_OK);
	wait_count(&f, &parent.terminals, 1U);
	wait_count(&f, &async.cancelled, 1U);
	lock(&f);
	async.work_release = 1U;
	publish(&f);
	unlock(&f);
	wait_count(&f, &async.thread_done, 1U);
	CHECK(pthread_join(thread, NULL) == 0);
	call_done(&parent, TR_RPC_STATUS_CANCELLED);
	tr_rpc_v2_producer_release(async.producer);
	drain(&f);
	destroy_client(&f, &f.client);
	CHECK(tr_rpc_v2_call_context_release(&f.parent_context) == TR_OK);
	destroy_server(&f);
	CHECK(pthread_cond_destroy(&f.cond) == 0);
	CHECK(pthread_mutex_destroy(&f.lock) == 0);
	puts("sdk-v2: case=parent-context-open-failure-release result=pass");
}
#endif

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
	/* 服务端已接受 RPC OPEN 并进入一次业务调用。GOAWAY 只关闭新流准入，
	 * 不会把此调用改判为未执行；因为它还没有 FINAL，排空仍需等待它。 */
	CHECK(tr_server_drain(f.server, 0) == TR_AGAIN);
	lock(&f);
	CHECK(f.methods[0].opens == 1U && f.methods[0].terminals == 0U);
	unlock(&f);
	CHECK(pending.terminals == 0U);
	destroy_client(&f, &f.client);
	CHECK(pending.terminals == 1U);
	/* 已发布且仍活跃的连接先由 owner 的正常停止路径关闭。其
	 * UNAVAILABLE/LIVE 终局已经获胜，随后清理不能改成残留取消。该状态
	 * 不能证明对端未执行；库不自动重试或重放不确定结果。
	 * SHUTDOWN 残留的精确交付域由内部故障专测独立验证。 */
	CHECK(pending.status == TR_RPC_STATUS_UNAVAILABLE);
	CHECK(pending.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(pending.received == 0U);
	wait_count(&f, &f.methods[0].terminals, 1U);
	CHECK(tr_server_drain(f.server, WAIT_MS) == TR_OK);
	lock(&f);
	CHECK(f.methods[0].opens == 1U);
	CHECK(f.methods[0].terminals == 1U);
	unlock(&f);
	fixture_destroy(&f);
	CHECK(pending.terminals == 1U);
	puts("sdk-v2: case=goaway-unknown-outcome-no-replay result=pass");
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
                               unsigned size, unsigned tag,
                               enum tr_rpc_compression compression)
{
	struct timespec limit = deadline_after(WAIT_MS);
	uint8_t storage[MAX_BODY];
	struct tr_rpc_bytes body;
	CHECK(size <= sizeof(storage));
	for (;;) {
		fill(storage, size, tag);
		body.data = size ? storage : NULL;
		body.len = size;
		int ret = tr_rpc_v2_producer_send_with_compression(
		        producer, &body, compression);
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
	if (a->mode == ASYNC_COMPLETE || a->mode == ASYNC_REJECT_COMPRESSION) {
		wait_count(f, &a->work_release, 1);
		/* 这个成功只会出现在创建 work 返回且 done 交接之后；不用
		 * 回调尾部标志或 sleep 冒充已经跨出借用期的证据。 */
		CHECK(tr_rpc_v2_producer_wait_writable(producer, WAIT_MS) ==
		      TR_OK);
		if (a->mode == ASYNC_REJECT_COMPRESSION) {
			struct tr_rpc_bytes body;
			uint8_t expected[MAX_BODY];
			fill(rejected, sizeof(rejected), 137U);
			memcpy(expected, rejected, sizeof(expected));
			body.data = rejected;
			body.len = sizeof(rejected);
			CHECK(tr_rpc_v2_producer_send_with_compression(
			              producer, &body,
			              TR_RPC_COMPRESSION_ZSTD) ==
			      TR_ERR_UNSUPPORTED);
			CHECK(memcmp(rejected, expected, sizeof(expected)) ==
			      0);
			CHECK(tr_rpc_v2_producer_send(producer, &body) ==
			      TR_OK);
			memset(rejected, 0xe7, sizeof(rejected));
		} else {
			producer_send_copy(producer, MAX_BODY, 137,
			                   TR_RPC_COMPRESSION_ZSTD);
			producer_send_copy(producer, 0, 0,
			                   TR_RPC_COMPRESSION_NONE);
		}
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
	if (mode == ASYNC_REJECT_COMPRESSION)
		fixture_create_shards_with_limits(&f, 1U, MAX_BODY, 256U, 0);
	else
		fixture_create(&f);
	memset(&a, 0, sizeof(a));
	a.fixture = &f;
	a.mode = mode;
	a.producer_thread = 1;
	lock(&f);
	f.methods[ASYNC_METHOD].async = &a;
	unlock(&f);
	request.size = 17;
	request.tag = 121;
	call_init(&call, &f, ASYNC_METHOD, &request, 1);
	if (mode == ASYNC_COMPLETE || mode == ASYNC_REJECT_COMPRESSION) {
		if (mode == ASYNC_COMPLETE) {
			expect(&call, MAX_BODY, 137);
			expect(&call, 0, 0);
		} else {
			expect(&call, MAX_BODY, 137);
		}
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
	if (mode == ASYNC_COMPLETE || mode == ASYNC_REJECT_COMPRESSION) {
		lock(&f);
		a.work_release = 1;
		publish(&f);
		unlock(&f);
		wait_count(&f, &a.produced, 1);
		wait_count(&f, &call.received, call.response_count);
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
	test_async_case(ASYNC_REJECT_COMPRESSION);
	test_async_case(ASYNC_CANCEL);
	test_async_case(ASYNC_DEADLINE);
	test_async_case(ASYNC_DESTROY);
	puts("sdk-v2: case=async-producer-copy-stop result=pass");
}

static void test_default_factory_requires_methods(void)
{
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;
	struct tr_client_config config;
	struct tr_rpc_method_desc method;

	CHECK(tr_client_create(NULL, NULL, 0, &client) == TR_ERR_INVALID);
	CHECK(!client);
	CHECK(tr_server_create(NULL, NULL, 0, &server) == TR_ERR_INVALID);
	CHECK(!server);
	CHECK(tr_client_create(NULL, NULL, 0, NULL) == TR_ERR_INVALID);
	CHECK(tr_server_create(NULL, NULL, 0, NULL) == TR_ERR_INVALID);
	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_RPC_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	method.request_compression_algorithms = UINT32_C(0x80000000);
	tr_client_config_init(&config);
	CHECK(tr_client_create(&config, &method, 1, &client) == TR_ERR_INVALID);
	CHECK(!client);
}

/* 安装消费端验证 RAW Codec 仍由公开头和静态 SDK 独立提供。 */
static void test_raw_codec_sdk(void)
{
	const uint8_t source[] = {0x00, 0x7f, 0xff, 0x42};
	const uint8_t expected[] = {0x00, 0x7f, 0xff, 0x42};
	uint8_t encoded[sizeof(source)] = {0};
	struct tr_rpc_bytes input = {source, sizeof(source)};
	struct tr_rpc_bytes decoded = {NULL, 99};
	struct tr_rpc_bytes empty = {NULL, 0};
	uint32_t written = 99;

	CHECK(tr_rpc_raw_encode(&input, encoded, sizeof(encoded), &written) ==
	      TR_OK);
	CHECK(written == sizeof(expected));
	CHECK(memcmp(encoded, expected, sizeof(expected)) == 0);
	CHECK(tr_rpc_raw_decode(encoded, written, &decoded) == TR_OK);
	CHECK(decoded.data == encoded && decoded.len == sizeof(expected));
	CHECK(memcmp(decoded.data, expected, sizeof(expected)) == 0);

	written = 99;
	CHECK(tr_rpc_raw_encode(&empty, NULL, 0, &written) == TR_OK);
	CHECK(written == 0);
	CHECK(tr_rpc_raw_decode(NULL, 0, &decoded) == TR_OK);
	CHECK(decoded.data == NULL && decoded.len == 0);
	puts("sdk-v2: case=raw-codec result=pass");
}

static void
sdk_tls_credentials_fetch(const struct tr_tls_credential_request *request,
                          struct tr_tls_provider_operation operation, void *arg)
{
	(void)request;
	(void)operation;
	(void)arg;
}

static void
sdk_tls_credentials_cancel(struct tr_tls_provider_operation operation,
                           void *arg)
{
	(void)operation;
	(void)arg;
}

static void sdk_connection_group_authorize_async(
        const struct tr_connection_group_authorization *authorization,
        struct tr_tls_provider_operation operation, void *arg)
{
	(void)authorization;
	(void)operation;
	(void)arg;
}

static void sdk_connection_group_authorize_cancel(
        struct tr_tls_provider_operation operation, void *arg)
{
	(void)operation;
	(void)arg;
}

/* 安装消费端编译并链接 TLS-D2 的公开 provider 类型和全部 facade 入口。 */
static void test_tls_d2_sdk_symbols(void)
{
	int (*client_start)(struct tr_client *, uint32_t,
	                    enum tr_tls_rotation_policy,
	                    struct tr_tls_provider_operation *) =
	        tr_client_tls_credentials_start;
	int (*client_complete)(struct tr_client *,
	                       struct tr_tls_provider_operation, int,
	                       const struct tr_tls_credential_bundle *) =
	        tr_client_tls_credentials_complete;
	int (*client_poll)(struct tr_client *, struct tr_tls_provider_operation,
	                   struct tr_tls_credential_result *) =
	        tr_client_tls_credentials_poll;
	int (*client_cancel)(struct tr_client *,
	                     struct tr_tls_provider_operation) =
	        tr_client_tls_credentials_cancel;
	int (*server_start)(struct tr_server *, uint32_t,
	                    enum tr_tls_rotation_policy,
	                    struct tr_tls_provider_operation *) =
	        tr_server_tls_credentials_start;
	int (*server_complete)(struct tr_server *,
	                       struct tr_tls_provider_operation, int,
	                       const struct tr_tls_credential_bundle *) =
	        tr_server_tls_credentials_complete;
	int (*server_poll)(struct tr_server *, struct tr_tls_provider_operation,
	                   struct tr_tls_credential_result *) =
	        tr_server_tls_credentials_poll;
	int (*server_cancel)(struct tr_server *,
	                     struct tr_tls_provider_operation) =
	        tr_server_tls_credentials_cancel;
	int (*authorize_complete)(
	        struct tr_server *, struct tr_tls_provider_operation, int,
	        enum tr_connection_group_authorization_decision) =
	        tr_server_connection_group_authorize_complete;
	struct tr_tls_credentials_provider credentials = {
	        sdk_tls_credentials_fetch, sdk_tls_credentials_cancel, NULL};
	struct tr_connection_group_server_config groups;
	tr_connection_group_authorize_async_cb authorize =
	        sdk_connection_group_authorize_async;
	tr_connection_group_authorize_cancel_cb authorize_cancel =
	        sdk_connection_group_authorize_cancel;

	tr_connection_group_server_config_init(&groups);
	groups.authorize_async = authorize;
	groups.authorize_cancel = authorize_cancel;
	CHECK(credentials.fetch != NULL && credentials.cancel != NULL);
	CHECK(groups.authorize_async != NULL &&
	      groups.authorize_cancel != NULL);
	CHECK(client_start != NULL && client_complete != NULL &&
	      client_poll != NULL && client_cancel != NULL);
	CHECK(server_start != NULL && server_complete != NULL &&
	      server_poll != NULL && server_cancel != NULL &&
	      authorize_complete != NULL);
	puts("sdk-v2: case=tls-d2-public-symbols result=pass");
}

/* 公共 CallRef 的所有新入口必须能从安装 SDK 的 C11/C++11 消费端链接。 */
static void test_call_ref_sdk_symbols(void)
{
	int (*hold)(struct tr_rpc_v2_call, struct tr_rpc_v2_call_ref *) =
	        tr_rpc_v2_call_hold;
	int (*result)(struct tr_rpc_v2_call_ref,
	              struct tr_rpc_v2_call_result *) =
	        tr_rpc_v2_call_ref_result;
	int (*cancel)(struct tr_rpc_v2_call_ref) = tr_rpc_v2_call_ref_cancel;
	int (*release)(struct tr_rpc_v2_call_ref *) =
	        tr_rpc_v2_call_ref_release;
	struct tr_rpc_v2_call_ref ref = {0};
	struct tr_rpc_v2_call_result snapshot;
	memset(&snapshot, 0, sizeof(snapshot));
	CHECK(hold != NULL && result != NULL && cancel != NULL &&
	      release != NULL);
	CHECK(sizeof(ref) == sizeof(uint64_t) * 4 &&
	      snapshot.status == TR_RPC_STATUS_OK &&
	      snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);
	puts("sdk-v2: case=call-ref-public-symbols result=pass");
}

/* 父子上下文接口也必须从安装 SDK 的 C11/C++11 消费端可见。 */
static void test_parent_context_sdk_symbols(void)
{
	int (*hold)(const struct tr_rpc_v2_cancel *,
	            struct tr_rpc_v2_call_context *) =
	        tr_rpc_v2_cancel_context_hold;
	int (*release)(struct tr_rpc_v2_call_context *) =
	        tr_rpc_v2_call_context_release;
	struct tr_rpc_v2_call_context context = {0};
	struct tr_rpc_call_options options = {0};
	const struct tr_rpc_v2_cancel *invalid_cancel =
	        (const struct tr_rpc_v2_cancel *)(uintptr_t)1;

	CHECK(hold != NULL && release != NULL);
	CHECK(options.parent_context == NULL);
	CHECK(tr_rpc_v2_cancel_context_hold(NULL, &context) == TR_ERR_INVALID);
	CHECK(tr_rpc_v2_cancel_context_hold(invalid_cancel, NULL) ==
	      TR_ERR_INVALID);
	CHECK(tr_rpc_v2_call_context_release(&context) == TR_ERR_STALE);
	puts("sdk-v2: case=parent-context-public-symbols result=pass");
}

/* #280 的一元远端未知方法及本地非 ONE→ONE 拒绝在安装 SDK 中保留。 */
static void test_unary_rejections(void)
{
	struct fixture f;
	struct call_state unknown, rejected, invalid_compression;
	struct message_spec spec;
	struct tr_rpc_v2_call sentinel;
	struct tr_rpc_call_options invalid_options;

	memset(&invalid_options, 0, sizeof(invalid_options));
	invalid_options.request_compression = (enum tr_rpc_compression)99;
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
	call_init(&invalid_compression, &f, 0U, &spec, 1U);
	memset(&invalid_compression.handle, 0xa5,
	       sizeof(invalid_compression.handle));
	sentinel = invalid_compression.handle;
	CHECK(unary_start(&invalid_compression, &invalid_options) ==
	      TR_ERR_INVALID);
	CHECK(memcmp(&invalid_compression.handle, &sentinel,
	             sizeof(sentinel)) == 0);
	check_client_totals(&f.client, 1, 1, TR_RPC_STATUS_UNIMPLEMENTED, 1);
	check_server_totals(&f, 0, 0, TR_RPC_STATUS_OK, 0);
	drain(&f);
	CHECK(tr_rpc_v2_call_cancel(unknown.handle) == TR_ERR_STALE);
	fixture_destroy(&f);
	puts("sdk-v2: case=unary-common-terminal result=pass");
}

int main(void)
{
	test_raw_codec_sdk();
	test_tls_d2_sdk_symbols();
	test_call_ref_sdk_symbols();
	test_parent_context_sdk_symbols();
	test_rpc_v2_method_hooks();
	test_method_hook_late_authorization(0);
	test_method_hook_late_authorization(1);
	test_default_factory_requires_methods();
	test_unary_rejections();
	test_call_compression_rejection_preserves_next_message();
	test_reply_compression_policy_rejection();
	test_directional_compression_only();
	test_shapes_and_copy();
	test_large_message_ownership();
	test_cancel_or_deadline(0);
	test_cancel_or_deadline(1);
#ifdef TR_RPC_V2_TEST_FAULTS
	test_cancel_compressed_call_destroy_barrier();
#endif
	test_parent_context_lifetime();
	test_parent_context_pool_exhaustion();
	test_parent_context_propagation(0, 5000U);
	test_parent_context_propagation(1, 5000U);
	test_parent_context_propagation(1, 0U);
	test_child_deadline_isolation();
	test_parent_deadline_survives_stopped_reap();
#ifdef TR_RPC_V2_TEST_FAULTS
	test_parent_context_open_failure_releases_reference();
#endif
	test_destroy_live_terminal();
	test_stats_peer_retirement();
	test_async_producer();
	return 0;
}
