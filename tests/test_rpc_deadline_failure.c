#define _POSIX_C_SOURCE 200809L
#include "../src/endian.h"
#include "../src/execution/reactor_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/rpc/rpc_wire.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 仅指定调用线程看到时钟故障，所有者仍使用真实时钟。 */
static _Thread_local int fail_clock;
static _Thread_local uint64_t fake_now_ns;
static _Atomic int capture_rpc_timer;
static _Atomic uint32_t rpc_timer_slot = UINT32_MAX;
static _Atomic int fail_arm;
static _Atomic unsigned failed_arms;
static _Atomic int inject_response_clock;
static _Atomic int response_clock_failure;
static _Atomic uint64_t response_now;
static _Atomic unsigned response_clock_reads;
/* 以下注入只作用于 Endpoint 构造发布和精确定时器撤销。 */
static _Atomic(uintptr_t) publish_fail_channel;
static _Atomic int fail_publish_once;
static _Atomic int fail_unregister_once;
static _Atomic unsigned failed_publishes;
static _Atomic unsigned failed_unregisters;
static _Thread_local int next_clock_failure;
static _Thread_local uint64_t next_clock_now;

int __real_tr_rpc_wire_decode_ex(const uint8_t *data, uint32_t len,
	struct tr_rpc_wire_header *header, const uint8_t **metadata,
	uint16_t *metadata_len, const uint8_t **payload);

int __wrap_tr_rpc_wire_decode_ex(const uint8_t *data, uint32_t len,
	struct tr_rpc_wire_header *header, const uint8_t **metadata,
	uint16_t *metadata_len, const uint8_t **payload)
{
	int ret = __real_tr_rpc_wire_decode_ex(data, len, header, metadata,
		metadata_len, payload);

	if (ret == TR_OK && tr_reactor_in_owner_context() &&
	    (header->type == TR_RPC_WIRE_RESPONSE || header->type == TR_RPC_WIRE_STATUS) &&
	    atomic_exchange(&inject_response_clock, 0)) {
		next_clock_failure = atomic_load(&response_clock_failure);
		next_clock_now = atomic_load(&response_now);
	}
	return ret;
}

int __real_clock_gettime(clockid_t clock_id, struct timespec *ts);
int __real_tr_reactor_timer_register(struct tr_reactor *reactor,
	tr_reactor_timer_cb callback, void *arg,
	struct tr_reactor_timer_handle *out);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
	uint64_t deadline_ns);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle timer);
typedef struct tr_channel Chan;
typedef tr_stream_data_cb Dcb;
typedef tr_stream_event_cb Scb;
typedef tr_channel_event_cb Ecb;

int __real_tr_channel_set_handler(Chan *, Dcb, Scb, Ecb, void *);

int __wrap_tr_channel_set_handler(Chan *ch, Dcb d, Scb s, Ecb e, void *arg)
{
	if (d && (uintptr_t)ch == atomic_load(&publish_fail_channel) &&
	    atomic_exchange(&fail_publish_once, 0)) {
		atomic_fetch_add(&failed_publishes, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_channel_set_handler(ch, d, s, e, arg);
}

int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle timer)
{
	if (timer.slot == atomic_load(&rpc_timer_slot) &&
	    atomic_exchange(&fail_unregister_once, 0)) {
		atomic_fetch_add(&failed_unregisters, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_unregister(timer);
}

int __wrap_clock_gettime(clockid_t clock_id, struct timespec *ts)
{
	if (clock_id == CLOCK_MONOTONIC) {
		if (next_clock_failure || next_clock_now != 0U) {
			uint64_t now = next_clock_now;
			int failure = next_clock_failure;

			next_clock_now = 0U;
			next_clock_failure = 0;
			atomic_fetch_add(&response_clock_reads, 1U);
			if (failure) {
				errno = EIO;
				return -1;
			}
			ts->tv_sec = (time_t)(now / UINT64_C(1000000000));
			ts->tv_nsec = (long)(now % UINT64_C(1000000000));
			return 0;
		}
		if (fail_clock) {
			errno = EIO;
			return -1;
		}
		if (fake_now_ns != 0U) {
			ts->tv_sec = (time_t)(fake_now_ns / UINT64_C(1000000000));
			ts->tv_nsec = (long)(fake_now_ns % UINT64_C(1000000000));
			return 0;
		}
	}
	return __real_clock_gettime(clock_id, ts);
}

int __wrap_tr_reactor_timer_register(struct tr_reactor *reactor,
	tr_reactor_timer_cb callback, void *arg,
	struct tr_reactor_timer_handle *out)
{
	int ret = __real_tr_reactor_timer_register(reactor, callback, arg, out);

	if (ret == TR_OK && atomic_load(&capture_rpc_timer))
		atomic_store(&rpc_timer_slot, out->slot);
	return ret;
}

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
	uint64_t deadline_ns)
{
	if (timer.slot == atomic_load(&rpc_timer_slot) && atomic_load(&fail_arm)) {
		atomic_fetch_add(&failed_arms, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_arm(timer, deadline_ns);
}

struct fixture;
struct callback_arg {
	struct fixture *fixture;
	unsigned index;
};

struct fixture {
	struct tr_reactor *reactor;
	struct tr_channel *client;
	struct tr_channel *server;
	struct tr_rpc_endpoint *rpc;
	struct tr_buffer_pool pool;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened;
	unsigned remote_closed;
	unsigned terminal;
	unsigned terminal_once[3];
	int status[3];
	unsigned requests;
	int respond;
	uint64_t wire_timeout_ms;
	struct callback_arg args[3];
	struct tr_rpc_call_handle calls[3];
};

static void on_call_event(struct tr_rpc_call_handle call,
	enum tr_rpc_call_event event, int status, void *arg)
{
	struct callback_arg *cb = arg;
	struct fixture *f = cb->fixture;

	(void)call;
	pthread_mutex_lock(&f->lock);
	if (event == TR_RPC_CALL_EVENT_OPENED) {
		f->opened++;
	} else if (event == TR_RPC_CALL_EVENT_REMOTE_CLOSED) {
		f->remote_closed++;
	} else if (event == TR_RPC_CALL_EVENT_ERROR ||
		   event == TR_RPC_CALL_EVENT_FINISHED) {
		assert(f->terminal_once[cb->index]++ == 0U);
		f->terminal++;
		f->status[cb->index] = status;
	}
	pthread_cond_broadcast(&f->cond);
	pthread_mutex_unlock(&f->lock);
}

static void on_peer_event(struct tr_stream_handle stream,
	enum tr_stream_event event, int status, void *arg)
{
	(void)status;
	(void)arg;
	if (event == TR_STREAM_EVENT_REMOTE_CLOSED)
		assert(tr_stream_close(stream) == TR_OK);
}

static enum tr_stream_data_disposition on_peer_data(
	struct tr_stream_handle stream, uint64_t message_id,
	struct tr_buffer *payload, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_wire_header wire;
	const uint8_t *metadata;
	const uint8_t *body;
	uint16_t metadata_len;
	uint64_t timeout_ms = 0U;

	(void)stream;
	(void)message_id;
	assert(tr_rpc_wire_decode_ex(payload->data, payload->len, &wire,
		&metadata, &metadata_len, &body) == TR_OK);
	(void)body;
	if (wire.type != TR_RPC_WIRE_REQUEST)
		return TR_STREAM_DATA_RELEASE;
	if (metadata_len != 0U) {
		const char *key = ":timeout-ms";
		size_t key_len = strlen(key);

		assert(metadata_len == 3U + key_len + 8U);
		assert(metadata[0] == key_len);
		assert(tr_get_le16(metadata + 1U) == 8U);
		assert(memcmp(metadata + 3U, key, key_len) == 0);
		timeout_ms = tr_get_le64(metadata + 3U + key_len);
	}
	if (f->respond == 3) {
		assert(tr_stream_close(stream) == TR_OK);
	} else if (f->respond) {
		struct tr_buffer *reply;

		wire.type = f->respond == 1 ? TR_RPC_WIRE_RESPONSE : TR_RPC_WIRE_STATUS;
		wire.flags = 0U;
		wire.status = TR_RPC_STATUS_OK;
		wire.payload_len = 0U;
		assert(tr_buffer_acquire(&f->pool, TR_RPC_WIRE_HEADER_SIZE, &reply) == TR_OK);
		assert(tr_rpc_wire_encode(reply->data, &wire) == TR_OK);
		reply->len = TR_RPC_WIRE_HEADER_SIZE;
		assert(tr_stream_send(stream, reply) == TR_OK);
	}
	pthread_mutex_lock(&f->lock);
	f->requests++;
	f->wire_timeout_ms = timeout_ms;
	pthread_cond_broadcast(&f->cond);
	pthread_mutex_unlock(&f->lock);
	return TR_STREAM_DATA_RELEASE;
}

static void fixture_init(struct fixture *f)
{
	struct tr_reactor_config rc = { 0 };
	struct tr_channel_config cc = { 0 };
	struct tr_rpc_endpoint_config ec = { 0 };
	struct tr_rpc_method_desc method = { 0 };
	struct tr_conn_handle a;
	struct tr_conn_handle b;
	int pair[2];
	unsigned i;

	memset(f, 0, sizeof(*f));
	atomic_store(&fail_arm, 0);
	atomic_store(&failed_arms, 0U);
	atomic_store(&rpc_timer_slot, UINT32_MAX);
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->cond, NULL) == 0);
	for (i = 0; i < 3U; ++i) {
		f->args[i].fixture = f;
		f->args[i].index = i;
	}
	assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0);
	for (i = 0; i < 2U; ++i) {
		int flags = fcntl(pair[i], F_GETFL, 0);

		assert(flags >= 0);
		assert(fcntl(pair[i], F_SETFL, flags | O_NONBLOCK) == 0);
	}
	rc.max_connections = 4U;
	rc.command_capacity = 64U;
	rc.tx_item_capacity = 32U;
	rc.control_tx_item_capacity = 32U;
	rc.rx_buffer_count = 32U;
	rc.rx_buffer_size = 4096U;
	rc.max_payload_len = 4096U;
	assert(tr_reactor_create(&rc, NULL, NULL, NULL, &f->reactor) == TR_OK);
	assert(tr_reactor_start(f->reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(f->reactor, pair[0], &a) == TR_OK);
	assert(tr_reactor_adopt_fd(f->reactor, pair[1], &b) == TR_OK);
	cc.role = TR_CHANNEL_CLIENT;
	cc.mode = TR_CHANNEL_SHARED_CONNECTION;
	cc.max_streams = 3U;
	cc.initial_window_bytes = 4096U;
	assert(tr_channel_create(&cc, a, a, NULL, NULL, NULL, NULL,
		&f->client) == TR_OK);
	cc.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&cc, b, b, on_peer_data, on_peer_event,
		NULL, f, &f->server) == TR_OK);
	assert(tr_channel_wait_ready(f->client, TR_LANE_CONTROL, 5000U) == TR_OK);
	assert(tr_channel_wait_ready(f->server, TR_LANE_CONTROL, 5000U) == TR_OK);
	assert(tr_buffer_pool_init(&f->pool, 32U, 4096U) == TR_OK);
	ec.role = TR_RPC_CLIENT;
	ec.max_methods = 2U;
	ec.max_calls = 3U;
	ec.executor_threads = 1U;
	ec.executor_queue_capacity = 16U;
	ec.message_pool = &f->pool;
	atomic_store(&capture_rpc_timer, 1);
	assert(tr_rpc_endpoint_create(f->client, &ec, &f->rpc) == TR_OK);
	atomic_store(&capture_rpc_timer, 0);
	assert(atomic_load(&rpc_timer_slot) != UINT32_MAX);
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(f->rpc, &method, NULL, NULL) == TR_OK);
	method.method_id = 2U;
	method.response_cardinality = TR_RPC_MANY;
	assert(tr_rpc_register_method(f->rpc, &method, NULL, NULL) == TR_OK);
}

static void wait_count(struct fixture *f, unsigned *count, unsigned want)
{
	struct timespec until;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &until) == 0);
	until.tv_sec += 5;
	pthread_mutex_lock(&f->lock);
	while (*count < want && ret == 0)
		ret = pthread_cond_timedwait(&f->cond, &f->lock, &until);
	assert(*count == want);
	pthread_mutex_unlock(&f->lock);
}

static void assert_heap_count(struct fixture *f, uint32_t expected)
{
	uint32_t count;

	assert(tr_rpc_deadline_heap_snapshot(f->rpc, &count, NULL, NULL) == TR_OK);
	assert(count == expected);
}

static void fixture_destroy(struct fixture *f)
{
	unsigned i;

	atomic_store(&fail_arm, 0);
	/* 终态通知可能早于对端半关闭，停止发送前先等待双方排空。 */
	for (i = 0; i < 5000U; ++i) {
		struct tr_rpc_endpoint_stats stats;
		const struct timespec pause = { 0, 1000000L };

		assert(tr_rpc_endpoint_get_stats(f->rpc, &stats) == TR_OK);
		if (tr_channel_active_streams(f->client) == 0U &&
		    tr_channel_active_streams(f->server) == 0U &&
		    stats.opening_calls == 0U && stats.active_calls == 0U &&
		    stats.terminal_calls == 0U)
			break;
		(void)nanosleep(&pause, NULL);
	}
	assert(i < 5000U);
	assert(tr_reactor_stop(f->reactor) == TR_OK);
	assert(tr_rpc_endpoint_destroy(f->rpc) == TR_OK);
	assert(tr_channel_destroy(f->client) == TR_OK);
	assert(tr_channel_destroy(f->server) == TR_OK);
	assert(tr_reactor_destroy(f->reactor) == TR_OK);
	assert(tr_buffer_pool_free_count(&f->pool) == 32U);
	assert(tr_buffer_pool_destroy(&f->pool) == TR_OK);
	assert(pthread_cond_destroy(&f->cond) == 0);
	assert(pthread_mutex_destroy(&f->lock) == 0);
}

static int start(struct fixture *f, unsigned index, uint32_t timeout_ms)
{
	struct tr_rpc_call_options options = { 0 };
	struct tr_rpc_call_callbacks callbacks = { 0 };

	options.timeout_ms = timeout_ms;
	callbacks.on_event = on_call_event;
	callbacks.arg = &f->args[index];
	return tr_rpc_call_start_ex(f->rpc, 1U, 1U, &options, &callbacks,
		&f->calls[index]);
}

static void test_clock_failure_and_zero(void)
{
	struct fixture f;
	struct tr_rpc_call_options options = { 0 };
	struct tr_rpc_bytes request = { NULL, 0U };
	struct tr_rpc_call_handle out = { { 0U, 0U } };
	struct tr_rpc_semantic_stats stats;

	fixture_init(&f);
	options.timeout_ms = 100U;
	fail_clock = 1;
	assert(start(&f, 0U, 100U) == TR_ERR_SYS);
	assert(tr_rpc_unary_call_ex(f.rpc, 1U, 1U, &request, &options,
		NULL, NULL, &out) == TR_ERR_SYS);
	fail_clock = 0;
	assert_heap_count(&f, 0U);
	assert(tr_channel_active_streams(f.client) == 0U);
	assert(tr_rpc_endpoint_get_semantic_stats(f.rpc, &stats) == TR_OK);
	assert(stats.calls_started == 0U);

	/* 无超时调用不应依赖调用线程的单调时钟。 */
	fail_clock = 1;
	assert(start(&f, 0U, 0U) == TR_OK);
	options.timeout_ms = 0U;
	assert(tr_rpc_unary_call_ex(f.rpc, 1U, 1U, &request, &options,
		NULL, NULL, &out) == TR_OK);
	fail_clock = 0;
	assert_heap_count(&f, 0U);
	wait_count(&f, &f.opened, 1U);
	assert(tr_rpc_call_cancel(f.calls[0]) == TR_OK);
	wait_count(&f, &f.requests, 1U);
	wait_count(&f, &f.terminal, 1U);
	fixture_destroy(&f);
}

static void test_arm_admission_failure(void)
{
	struct fixture f;
	struct tr_rpc_call_options options = { 0 };
	struct tr_rpc_bytes request = { NULL, 0U };
	struct tr_rpc_call_handle out;
	struct tr_rpc_semantic_stats stats;
	unsigned i;

	fixture_init(&f);
	options.timeout_ms = 1000U;
	atomic_store(&fail_arm, 1);
	/* 失败次数超过 max_calls，验证两个准入入口均能回收槽位。 */
	for (i = 0; i < 6U; ++i) {
		assert(start(&f, 0U, 1000U) == TR_ERR_SYS);
		assert(tr_rpc_unary_call_ex(f.rpc, 1U, 1U, &request, &options,
			NULL, NULL, &out) == TR_ERR_SYS);
		assert_heap_count(&f, 0U);
	}
	assert(atomic_load(&failed_arms) >= 12U);
	assert(tr_channel_active_streams(f.client) == 0U);
	assert(tr_rpc_endpoint_get_semantic_stats(f.rpc, &stats) == TR_OK);
	assert(stats.calls_started == 0U);
	atomic_store(&fail_arm, 0);
	assert(start(&f, 0U, 20U) == TR_OK);
	wait_count(&f, &f.terminal, 1U);
	assert(f.status[0] == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	assert_heap_count(&f, 0U);
	fixture_destroy(&f);
}

static int reject_earlier_on_owner(void *arg)
{
	struct fixture *f = arg;

	assert(start(f, 0U, 40U) == TR_OK);
	atomic_store(&fail_arm, 1);
	assert(start(f, 1U, 1U) == TR_ERR_SYS);
	assert_heap_count(f, 1U);
	return TR_OK;
}

static void test_accepted_call_survives_rejection(void)
{
	struct fixture f;

	fixture_init(&f);
	assert(tr_reactor_call(f.reactor, reject_earlier_on_owner, &f) == TR_OK);
	wait_count(&f, &f.terminal, 1U);
	assert(f.status[0] == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	assert(atomic_load(&failed_arms) >= 1U);
	assert_heap_count(&f, 0U);
	fixture_destroy(&f);
}

static int arm_many_on_owner(void *arg)
{
	struct fixture *f = arg;

	assert(start(f, 0U, 200U) == TR_OK);
	assert(start(f, 1U, 400U) == TR_OK);
	assert(start(f, 2U, 600U) == TR_OK);
	return TR_OK;
}

static int fail_rearm_on_owner(void *arg)
{
	(void)arg;
	atomic_store(&fail_arm, 1);
	return TR_OK;
}

static int cancel_root_on_owner(void *arg)
{
	struct fixture *f = arg;

	atomic_store(&fail_arm, 1);
	assert(tr_rpc_call_cancel(f->calls[0]) == TR_OK);
	return TR_OK;
}

static void test_cleanup_and_callback_progress(int cancel_root)
{
	struct fixture f;
	unsigned i;

	fixture_init(&f);
	assert(tr_reactor_call(f.reactor, arm_many_on_owner, &f) == TR_OK);
	/* 本场景保护已确认调用的堆删除/回调重排故障。先等三个 OPENED，
	 * 隔离尚未确认 OPEN 取消所需的新关闭重试；后者的调度失败必须
	 * 中止连接，已由 final_close_retry 的早取消故障场景单独覆盖。 */
	wait_count(&f, &f.opened, 3U);
	assert(tr_reactor_call(f.reactor,
	                       cancel_root ? cancel_root_on_owner
	                                   : fail_rearm_on_owner,
	                       &f) == TR_OK);
	wait_count(&f, &f.terminal, 3U);
	for (i = 0; i < 3U; ++i)
		assert(f.status[i] == (cancel_root && i == 0U ?
			TR_RPC_STATUS_CANCELLED : TR_RPC_STATUS_DEADLINE_EXCEEDED));
	/* #195 的终态删除仍尝试重新设置定时；注入失败后必须继续唯一终态推进。
	 */
	assert(atomic_load(&failed_arms) >= 1U);
	assert_heap_count(&f, 0U);
	fixture_destroy(&f);
}

static int saturate_on_owner(void *arg)
{
	struct fixture *f = arg;
	int ret;

	fake_now_ns = UINT64_MAX - 10U;
	ret = start(f, 0U, 1U);
	fake_now_ns = 0U;
	return ret;
}

static int inspect_extremes_on_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_context context;
	struct tr_rpc_bytes request = { (const uint8_t *)"x", 1U };
	const uint64_t deltas[] = { UINT64_MAX - 1U, 1000001U, 1000000U, 1U, 0U };
	unsigned i;

	for (i = 0; i < sizeof(deltas) / sizeof(deltas[0]); ++i) {
		fake_now_ns = UINT64_MAX - deltas[i];
		assert(tr_rpc_call_get_context(f->calls[0], &context) == TR_OK);
		assert(context.has_deadline);
		assert(context.deadline_remaining_ms ==
			deltas[i] / UINT64_C(1000000) +
			(deltas[i] % UINT64_C(1000000) != 0U));
	}
	fake_now_ns = 1U;
	assert(tr_rpc_call_send(f->calls[0], &request) == TR_OK);
	fake_now_ns = 0U;
	return TR_OK;
}

static void test_saturated_rounding(void)
{
	struct fixture f;
	uint64_t deadline;
	uint32_t count;

	fixture_init(&f);
	assert(tr_reactor_call(f.reactor, saturate_on_owner, &f) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(f.rpc, &count, NULL, &deadline) == TR_OK);
	assert(count == 1U && deadline == UINT64_MAX);
	wait_count(&f, &f.opened, 1U);
	assert(tr_reactor_call(f.reactor, inspect_extremes_on_owner, &f) == TR_OK);
	wait_count(&f, &f.requests, 1U);
	assert(f.wire_timeout_ms == UINT64_C(18446744073710));
	assert(tr_rpc_call_cancel(f.calls[0]) == TR_OK);
	wait_count(&f, &f.terminal, 1U);
	fixture_destroy(&f);
}

static void on_unary_result(struct tr_rpc_call_handle call, int status,
	const struct tr_rpc_bytes *response, void *arg)
{
	(void)response;
	on_call_event(call, TR_RPC_CALL_EVENT_FINISHED, status, arg);
}

static int send_and_close_on_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_bytes request = { (const uint8_t *)"x", 1U };

	assert(tr_rpc_call_send(f->calls[0], &request) == TR_OK);
	assert(tr_rpc_call_close_send(f->calls[0]) == TR_OK);
	return TR_OK;
}

static void test_response_deadline(int unary, int offset, int clock_failure)
{
	struct fixture f;
	struct tr_rpc_call_options options = { 0 };
	struct tr_rpc_call_callbacks callbacks = { 0 };
	struct tr_rpc_bytes request = { (const uint8_t *)"x", 1U };
	struct tr_rpc_semantic_stats stats;
	const uint64_t base = UINT64_MAX / 2U;
	const uint64_t deadline = base + UINT64_C(5000000000);
	int expected = clock_failure ? TR_RPC_STATUS_UNAVAILABLE :
		(offset < 0 ? TR_RPC_STATUS_OK : TR_RPC_STATUS_DEADLINE_EXCEEDED);

	fixture_init(&f);
	f.respond = unary ? 1 : 2;
	options.timeout_ms = 5000U;
	atomic_store(&response_clock_reads, 0U);
	atomic_store(&response_clock_failure, clock_failure);
	atomic_store(&response_now, offset < 0 ? deadline - 1U :
		deadline + (uint64_t)offset);
	atomic_store(&inject_response_clock, 1);
	fake_now_ns = base;
	if (unary) {
		assert(tr_rpc_unary_call_ex(f.rpc, 1U, 1U, &request, &options,
			on_unary_result, &f.args[0], &f.calls[0]) == TR_OK);
	} else {
		callbacks.on_event = on_call_event;
		callbacks.arg = &f.args[0];
		assert(tr_rpc_call_start_ex(f.rpc, 1U, 2U, &options, &callbacks,
			&f.calls[0]) == TR_OK);
	}
	fake_now_ns = 0U;
	if (!unary) {
		wait_count(&f, &f.opened, 1U);
		assert(tr_reactor_call(f.reactor, send_and_close_on_owner, &f) == TR_OK);
	}
	wait_count(&f, &f.terminal, 1U);
	if (f.status[0] != expected)
		fprintf(stderr, "response unary=%d offset=%d clock_failure=%d: got=%d expected=%d\n",
			unary, offset, clock_failure, f.status[0], expected);
	assert(f.status[0] == expected);
	assert(atomic_load(&response_clock_reads) == 1U);
	assert(tr_rpc_endpoint_get_semantic_stats(f.rpc, &stats) == TR_OK);
	assert(stats.calls_started == 1U && stats.calls_finished == 1U);
	assert(stats.final_status[expected] == 1U);
	assert_heap_count(&f, 0U);
	fixture_destroy(&f);
}

static void test_half_close_order(int remote_first)
{
	struct fixture f;
	struct tr_rpc_call_callbacks callbacks = { 0 };
	struct tr_rpc_bytes request = { (const uint8_t *)"x", 1U };
	struct tr_rpc_semantic_stats stats;
	unsigned i;

	fixture_init(&f);
	f.respond = 3;
	callbacks.on_event = on_call_event;
	/* 不设置截止时间，证明终态由半关闭本身产生，不由定时器兜底。 */
	for (i = 0; i < 3U; ++i) {
		callbacks.arg = &f.args[i];
		assert(tr_rpc_call_start(f.rpc, 1U, 2U, &callbacks, &f.calls[0]) == TR_OK);
		wait_count(&f, &f.opened, i + 1U);
		if (remote_first) {
			assert(tr_rpc_call_send(f.calls[0], &request) == TR_OK);
			wait_count(&f, &f.remote_closed, i + 1U);
			pthread_mutex_lock(&f.lock);
			assert(f.terminal == i);
			pthread_mutex_unlock(&f.lock);
			assert(tr_rpc_call_close_send(f.calls[0]) == TR_OK);
		} else {
			assert(tr_reactor_call(f.reactor, send_and_close_on_owner, &f) == TR_OK);
		}
		wait_count(&f, &f.terminal, i + 1U);
		assert(f.status[i] == TR_RPC_STATUS_UNAVAILABLE);
		assert(tr_rpc_endpoint_get_semantic_stats(f.rpc, &stats) == TR_OK);
		assert(stats.calls_started == i + 1U && stats.calls_finished == i + 1U);
		assert(stats.final_status[TR_RPC_STATUS_UNAVAILABLE] == i + 1U);
		assert(stats.final_status[TR_RPC_STATUS_OK] == 0U);
	}
	fixture_destroy(&f);
}

static void test_create_rollback_retains_timer_owner(int shared)
{
	struct fixture f;
	struct tr_rpc_endpoint_config ec;
	struct tr_rpc_endpoint *partial = NULL;
	struct tr_rpc_executor_group *group = NULL;
	struct tr_rpc_endpoint_stats stats;
	int ret;

	fixture_init(&f);
	memset(&ec, 0, sizeof(ec));
	/* 消除默认 Endpoint 的 handler/timer 发布，保留同一可用 Channel。 */
	assert(tr_rpc_endpoint_destroy(f.rpc) == TR_OK);
	f.rpc = NULL;
	ec.role = TR_RPC_CLIENT;
	ec.max_methods = 2U;
	ec.max_calls = 3U;
	ec.executor_threads = 1U;
	ec.executor_queue_capacity = 16U;
	ec.message_pool = &f.pool;
	if (shared) {
		ret = tr_rpc_executor_group_create(1U, 3U, 1U, &group);
		assert(ret == TR_OK);
		assert(tr_rpc_executor_group_start(group) == TR_OK);
	}

	atomic_store(&capture_rpc_timer, 1);
	atomic_store(&publish_fail_channel, (uintptr_t)f.client);
	atomic_store(&failed_publishes, 0U);
	atomic_store(&failed_unregisters, 0U);

	/* handler 发布失败，定时器撤销成功：不能交付部分对象。 */
	atomic_store(&fail_publish_once, 1);
	ret = tr_rpc_endpoint_create_with_executor_group(
		f.client, &ec, group, &partial);
	assert(ret == TR_ERR_SYS);
	assert(partial == NULL);
	assert(atomic_load(&failed_publishes) == 1U);

	/* handler 发布失败，timer_unregister 也失败：
	 * 注册表仍可能触达 Endpoint，词法自动释放会成为 UAF。
	 */
	atomic_store(&fail_publish_once, 1);
	atomic_store(&fail_unregister_once, 1);
	ret = tr_rpc_endpoint_create_with_executor_group(
		f.client, &ec, group, &partial);
	atomic_store(&capture_rpc_timer, 0);
	assert(ret == TR_ERR_SYS);
	assert(partial != NULL);
	assert(atomic_load(&failed_publishes) == 2U);
	assert(atomic_load(&failed_unregisters) == 1U);
	assert(tr_rpc_endpoint_get_stats(partial, &stats) == TR_OK);

	/* 第一次显式销毁的 timer 屏障再次失败，调用方仍拥有原指针；
	 * 第二次撤销成功后才允许最终释放，并能在同一 Channel 重建。
	 */
	atomic_store(&fail_unregister_once, 1);
	assert(tr_rpc_endpoint_destroy(partial) == TR_ERR_SYS);
	assert(atomic_load(&failed_unregisters) == 2U);
	assert(tr_rpc_endpoint_get_stats(partial, &stats) == TR_OK);
	assert(tr_rpc_endpoint_destroy(partial) == TR_OK);
	partial = NULL;
	atomic_store(&publish_fail_channel, (uintptr_t)0);
	ret = tr_rpc_endpoint_create_with_executor_group(
		f.client, &ec, group, &f.rpc);
	assert(ret == TR_OK);
	fixture_destroy(&f);
	if (group) {
		assert(tr_rpc_executor_group_stop(group) == TR_OK);
		assert(tr_rpc_executor_group_destroy_checked(group) == TR_OK);
	}
}

int main(int argc, char **argv)
{
	if (argc == 1 || strcmp(argv[1], "construct") == 0) {
		test_create_rollback_retains_timer_owner(0);
		test_create_rollback_retains_timer_owner(1);
	}
	if (argc == 1 || strcmp(argv[1], "clock") == 0)
		test_clock_failure_and_zero();
	if (argc == 1 || strcmp(argv[1], "admission") == 0)
		test_arm_admission_failure();
	if (argc == 1 || strcmp(argv[1], "existing") == 0)
		test_accepted_call_survives_rejection();
	if (argc == 1 || strcmp(argv[1], "cleanup") == 0)
		test_cleanup_and_callback_progress(1);
	if (argc == 1 || strcmp(argv[1], "callback") == 0)
		test_cleanup_and_callback_progress(0);
	if (argc == 1 || strcmp(argv[1], "rounding") == 0)
		test_saturated_rounding();
	if (argc == 1 || strcmp(argv[1], "response") == 0) {
		int unary;

		for (unary = 0; unary <= 1; ++unary) {
			test_response_deadline(unary, -1, 0);
			test_response_deadline(unary, 0, 0);
			test_response_deadline(unary, 1, 0);
			test_response_deadline(unary, 0, 1);
		}
	}
	if (argc == 1 || strcmp(argv[1], "halfclose") == 0) {
		test_half_close_order(1);
		test_half_close_order(0);
	}
	puts("test_rpc_deadline_failure: ok");
	return 0;
}
