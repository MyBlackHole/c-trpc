#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#include "../src/execution/worker_context.h"
#include "../src/client_record_internal.h"
#include "../src/server_record_internal.h"
#include "../src/endian.h"
#include "../src/execution/buffer.h"
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
static _Atomic unsigned budget_release_pool_target, budget_release_pool_seen;
static _Atomic uint64_t budget_release_current_before_failure;
static _Atomic(struct tr_memory_budget *) budget_release_injected_budget;
static _Atomic unsigned attempts[6], accepted[6], fake_again, real_again;
static _Atomic unsigned server_messages_accepted, server_half_closes_received,
        server_resets_received;
static _Atomic uint64_t server_message_bytes;
static _Atomic int fail_kind, block_kind, hard_fail_kind;
static _Atomic bool fail_call_open, fail_call_binding;
static _Atomic bool fail_unary_half_close_clock;
static _Atomic bool fail_public_start_clock;
static _Atomic bool fail_client_start_arm;
static _Atomic unsigned client_start_arm_failures;
static _Atomic unsigned client_stream_opens;
static _Atomic bool observe_destroy_stop;
static _Atomic unsigned destroy_join_busy;
static pthread_t destroy_owner_thread;
static _Atomic unsigned client_control_down_events;
static _Atomic bool capture_public_start_clock;
static _Atomic bool saturate_public_start_clock, capture_open_timeout;
static _Atomic bool capture_open_attempt;
static _Atomic uint64_t public_start_ns;
static _Atomic uint64_t observed_open_timeout;
static _Atomic uint64_t observed_open_clock_ns, observed_open_remaining_ns;
static _Atomic unsigned response_clock_mode, response_clock_reads;
static _Atomic unsigned clock_failures;
static _Atomic bool clear_open_deadline;
static _Atomic unsigned deadline_prefixes, deadline_prefixes_accepted,
        deadline_prefixes_released;
static _Atomic bool inline_client_data_once;
static _Atomic unsigned inline_client_data_calls;
static _Thread_local bool fail_clock_once;
static _Thread_local unsigned fail_clock_after;
static _Thread_local bool capture_start_clock;
static _Thread_local bool fake_start_clock_once;
static _Thread_local bool fail_timer_arm_once;
static _Thread_local uint64_t fake_response_clock_ns;
static _Thread_local uint64_t recent_monotonic_ns;
static _Thread_local bool join_inside_destroy_stop;
static _Atomic(struct tr_buffer_pool *) public_send_pool;
static _Atomic bool hold_public_send, public_send_held;
static struct tr_stream_handle public_held_stream;
static struct tr_buffer *public_held_buffers[2];

struct tr_runtime;
int __real_clock_gettime(clockid_t, struct timespec *);
int __real_tr_runtime_stop(struct tr_runtime *);
int __wrap_tr_runtime_stop(struct tr_runtime *runtime)
{
	bool observe = atomic_load(&observe_destroy_stop);
	if (observe) {
		CHECK(!join_inside_destroy_stop);
		join_inside_destroy_stop = true;
	}
	int ret = __real_tr_runtime_stop(runtime);
	if (observe)
		join_inside_destroy_stop = false;
	return ret;
}
int __real_pthread_join(pthread_t, void **);
int __wrap_pthread_join(pthread_t thread, void **value)
{
	if (join_inside_destroy_stop) {
		CHECK(pthread_equal(thread, destroy_owner_thread));
		CHECK(pthread_tryjoin_np(thread, NULL) == EBUSY);
		atomic_fetch_add(&destroy_join_busy, 1);
	}
	return __real_pthread_join(thread, value);
}
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle, uint64_t);
int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t deadline_ns)
{
	if (fail_timer_arm_once) {
		fail_timer_arm_once = false;
		atomic_fetch_add(&client_start_arm_failures, 1);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_arm(timer, deadline_ns);
}

int __real_tr_rpc_record_client_start_on_owner(struct tr_rpc_record_client *);
int __wrap_tr_rpc_record_client_start_on_owner(
        struct tr_rpc_record_client *client)
{
	bool inject = atomic_exchange(&fail_client_start_arm, false);
	if (inject) {
		CHECK(!fail_timer_arm_once);
		fail_timer_arm_once = true;
	}
	int ret = __real_tr_rpc_record_client_start_on_owner(client);
	if (inject)
		CHECK(!fail_timer_arm_once);
	return ret;
}

int __wrap_clock_gettime(clockid_t clock, struct timespec *time)
{
	if (clock == CLOCK_MONOTONIC && fake_response_clock_ns) {
		time->tv_sec = (time_t)(fake_response_clock_ns / 1000000000);
		time->tv_nsec = (long)(fake_response_clock_ns % 1000000000);
		atomic_fetch_add(&response_clock_reads, 1);
		return 0;
	}
	if (clock == CLOCK_MONOTONIC &&
	    (fail_clock_once ||
	     (fail_clock_after && --fail_clock_after == 0))) {
		fail_clock_once = false;
		atomic_fetch_add(&clock_failures, 1);
		errno = EIO;
		return -1;
	}
	int ret;
	if (clock == CLOCK_MONOTONIC && fake_start_clock_once) {
		fake_start_clock_once = false;
		time->tv_sec = (time_t)((UINT64_MAX - 999999999) / 1000000000);
		time->tv_nsec = 999999999;
		ret = 0;
	} else
		ret = __real_clock_gettime(clock, time);
	if (ret == 0 && clock == CLOCK_MONOTONIC && capture_start_clock &&
	    !atomic_load(&public_start_ns))
		atomic_store(&public_start_ns,
		             (uint64_t)time->tv_sec * 1000000000 +
		                     (uint64_t)time->tv_nsec);
	if (ret == 0 && clock == CLOCK_MONOTONIC)
		recent_monotonic_ns = (uint64_t)time->tv_sec * 1000000000 +
		                      (uint64_t)time->tv_nsec;
	return ret;
}

int __real_tr_rpc_record_client_call_start_public_on_owner(
        struct tr_rpc_record_client *, uint32_t, uint32_t,
        const struct tr_rpc_call_options *,
        const struct tr_rpc_v2_client_callbacks *, struct tr_rpc_v2_call *,
        const struct tr_rpc_bytes *);
int __wrap_tr_rpc_record_client_call_start_public_on_owner(
        struct tr_rpc_record_client *client, uint32_t service_id,
        uint32_t method_id, const struct tr_rpc_call_options *options,
        const struct tr_rpc_v2_client_callbacks *callbacks,
        struct tr_rpc_v2_call *out, const struct tr_rpc_bytes *request)
{
	bool inject = atomic_exchange(&fail_public_start_clock, false);
	bool capture = atomic_exchange(&capture_public_start_clock, false);
	bool saturate = atomic_exchange(&saturate_public_start_clock, false);
	if (inject) {
		CHECK(!fail_clock_once);
		fail_clock_once = true;
	}
	if (capture) {
		CHECK(!capture_start_clock);
		capture_start_clock = true;
	}
	if (saturate)
		fake_start_clock_once = true;
	int ret = __real_tr_rpc_record_client_call_start_public_on_owner(
	        client, service_id, method_id, options, callbacks, out,
	        request);
	if (inject)
		CHECK(!fail_clock_once);
	if (capture) {
		CHECK(atomic_load(&public_start_ns));
		capture_start_clock = false;
	}
	if (saturate)
		CHECK(!fake_start_clock_once);
	return ret;
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
	CHECK(!atomic_load(&public_send_held));
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
	atomic_store(&server_resets_received, 0);
	atomic_store(&server_message_bytes, 0);
	atomic_store(&fail_kind, 0);
	atomic_store(&block_kind, 0);
	atomic_store(&hard_fail_kind, 0);
	atomic_store(&fail_call_open, false);
	atomic_store(&fail_call_binding, false);
	atomic_store(&fail_unary_half_close_clock, false);
	atomic_store(&fail_public_start_clock, false);
	atomic_store(&fail_client_start_arm, false);
	atomic_store(&client_start_arm_failures, 0);
	atomic_store(&client_stream_opens, 0);
	atomic_store(&observe_destroy_stop, false);
	atomic_store(&destroy_join_busy, 0);
	atomic_store(&client_control_down_events, 0);
	atomic_store(&capture_public_start_clock, false);
	atomic_store(&saturate_public_start_clock, false);
	atomic_store(&capture_open_timeout, false);
	atomic_store(&capture_open_attempt, false);
	atomic_store(&public_start_ns, 0);
	atomic_store(&observed_open_timeout, 0);
	atomic_store(&observed_open_clock_ns, 0);
	atomic_store(&observed_open_remaining_ns, 0);
	atomic_store(&response_clock_mode, 0);
	atomic_store(&response_clock_reads, 0);
	atomic_store(&clock_failures, 0);
	atomic_store(&clear_open_deadline, false);
	atomic_store(&deadline_prefixes, 0);
	atomic_store(&deadline_prefixes_accepted, 0);
	atomic_store(&deadline_prefixes_released, 0);
	atomic_store(&inline_client_data_once, false);
	atomic_store(&inline_client_data_calls, 0);
	atomic_store(&public_send_pool, NULL);
	atomic_store(&hold_public_send, false);
	atomic_store(&public_send_held, false);
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

int __real_tr_buffer_pool_destroy(struct tr_buffer_pool *pool);
int __wrap_tr_buffer_pool_destroy(struct tr_buffer_pool *pool)
{
	struct tr_memory_budget *budget = pool ? pool->memory_budget : NULL;
	int ret = __real_tr_buffer_pool_destroy(pool);

	if (ret == TR_OK && budget &&
	    atomic_load(&budget_release_pool_target) != 0U &&
	    atomic_fetch_add(&budget_release_pool_seen, 1U) + 1U ==
	            atomic_load(&budget_release_pool_target)) {
		uint64_t current = atomic_load(&budget->current_bytes);
		uint64_t method_bytes = 3U * sizeof(struct tr_rpc_method_desc);

		CHECK(method_bytes > 1U);
		atomic_store(&budget_release_current_before_failure, current);
		atomic_store(&budget_release_injected_budget, budget);
		/* 在方法表释放前制造预算低于该笔所有权的条件。 */
		atomic_store(&budget->current_bytes, method_bytes - 1U);
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

static void inject_inline_client_data(struct tr_stream_handle stream)
{
	struct channel_observation observed = {0};
	uint8_t bytes[TR_RPC_RECORD_HEADER_SIZE];
	struct tr_rpc_record_header header = {
	        .kind = TR_RPC_RECORD_INITIAL_METADATA};
	struct tr_rpc_record_limits limits = {WIRE_LIMIT, WIRE_LIMIT,
	                                      TR_RPC_METADATA_MAX_BYTES,
	                                      TR_RPC_RECORD_ACCEPT_NONE};
	struct tr_buffer payload = {
	        .data = bytes, .len = sizeof(bytes), .capacity = sizeof(bytes)};
	CHECK(tr_reactor_in_owner_context_for(
	        tr_channel_reactor(stream.channel)));
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	for (unsigned i = 0; i < observation_count; ++i)
		if (observations[i].channel == stream.channel)
			observed = observations[i];
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	CHECK(observed.shimmed && observed.data);
	CHECK(tr_rpc_record_encode_header(bytes, sizeof(bytes), &header,
	                                  &limits) == TR_OK);
	/* 真实发送仍在 Client 的 owner 栈内；借用一次完整控制记录触发
	 * 重入 DATA 防线，不伪造连接关闭事实，也不修改生产槽的布局。 */
	CHECK(observed.data(stream, 1, &payload, observed.arg) ==
	      TR_STREAM_DATA_RELEASE);
	atomic_fetch_add(&inline_client_data_calls, 1);
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
	uint64_t attempted_open_timeout = 0;
	if (kind == TR_RPC_RECORD_OPEN && atomic_load(&capture_open_timeout)) {
		CHECK(count == 1 &&
		      buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE);
		attempted_open_timeout = tr_get_le64(buffers[0]->data + 40);
	}
	if (kind == TR_RPC_RECORD_OPEN &&
	    atomic_exchange(&capture_open_attempt, false)) {
		/* progress 刚用的 owner 时钟与线上剩余期限构成绝对截止
		 * 时间；失败的下一次公开准入不得改变这个和。 */
		CHECK(count == 1 &&
		      buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE);
		atomic_store(&observed_open_clock_ns, recent_monotonic_ns);
		atomic_store(&observed_open_remaining_ns,
		             tr_get_le64(buffers[0]->data + 40));
	}
	uint64_t server_bytes = 0;
	if (server_kind == TR_RPC_RECORD_MESSAGE)
		for (uint32_t i = 0; i < count; ++i)
			server_bytes += buffers[i]->len;
	if (kind == TR_RPC_RECORD_MESSAGE && count == 2 && buffers[1]->pool)
		atomic_store(&public_send_pool, buffers[1]->pool);
	if (kind) {
		atomic_fetch_add(&attempts[kind], 1);
		int failure = (int)kind;
		if (atomic_compare_exchange_strong(&hard_fail_kind, &failure,
		                                   0))
			return TR_ERR_NOMEM;
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
		/* 仅用于本地超时后的 RESET 场景：保留真实 Client 本地计时，
		 * 排除服务端自己执行 OPEN 期限的干扰。不能修改已交给传输的
		 * 存储，也不能在发送失败时释放原前缀。 */
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
	int ret;
	if (kind == TR_RPC_RECORD_MESSAGE &&
	    atomic_exchange(&hold_public_send, false)) {
		/* 下游接管后延后实际发送，应用先覆写原字节；恢复时仍通过
		 * 真实 Channel/TCP 检查复制所有权，不用立即送达掩盖借用错误。
		 */
		CHECK(!replacement && count == 2 && buffers[1]->pool);
		CHECK(!atomic_load(&public_send_held));
		public_held_stream = stream;
		public_held_buffers[0] = buffers[0];
		public_held_buffers[1] = buffers[1];
		atomic_store(&public_send_held, true);
		ret = TR_OK;
	} else {
		ret = __real_tr_stream_sendv(
		        stream, replacement ? patched : buffers, count);
	}
	if (kind == TR_RPC_RECORD_OPEN && ret == TR_OK &&
	    atomic_exchange(&capture_open_timeout, false))
		atomic_store(&observed_open_timeout, attempted_open_timeout);
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
	/* 同一所有者发送边界：下一段 HALF_CLOSE 必须重新验证钟源。
	 * 原时间注入仍只影响本测试线程，不把检查钩子带入生产代码。 */
	if (kind == TR_RPC_RECORD_MESSAGE && ret == TR_OK &&
	    atomic_exchange(&fail_unary_half_close_clock, false))
		fail_clock_after = 1;
	if (kind == TR_RPC_RECORD_MESSAGE && ret == TR_OK &&
	    atomic_exchange(&inline_client_data_once, false))
		inject_inline_client_data(stream);
	return ret;
}

static enum tr_stream_data_disposition
observed_data(struct tr_stream_handle stream, uint64_t id,
              struct tr_buffer *payload, void *arg)
{
	struct channel_observation *o = arg;
	unsigned kind = record_kind(payload);
	unsigned mode = stream.channel == atomic_load(&client_channel) &&
	                                kind == TR_RPC_RECORD_MESSAGE
	                        ? atomic_exchange(&response_clock_mode, 0)
	                        : 0;
	if (mode && mode <= 3) {
		uint64_t deadline = atomic_load(&public_start_ns) + 5000000000;
		CHECK(deadline > 5000000000 && !fake_response_clock_ns);
		fake_response_clock_ns = mode == 1   ? deadline - 1
		                         : mode == 3 ? deadline + 1
		                                     : deadline;
	} else if (mode == 4) {
		CHECK(!fail_clock_once);
		fail_clock_once = true;
	}
	enum tr_stream_data_disposition disposition =
	        o->data ? o->data(stream, id, payload, o->arg)
	                : TR_STREAM_DATA_RELEASE;
	if (mode && mode <= 3)
		fake_response_clock_ns = 0;
	else if (mode == 4)
		CHECK(!fail_clock_once);
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
	if (stream.channel == atomic_load(&server_channel) &&
	    event == TR_STREAM_EVENT_RESET)
		atomic_fetch_add(&server_resets_received, 1);
}

static void observed_channel_event(struct tr_channel *channel,
                                   enum tr_channel_event event, int status,
                                   void *arg)
{
	struct channel_observation *o = arg;
	if (channel == atomic_load(&client_channel) &&
	    event == TR_CHANNEL_EVENT_CONTROL_DOWN)
		atomic_fetch_add(&client_control_down_events, 1);
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
	if ((channel != atomic_load(&server_channel) &&
	     channel != atomic_load(&client_channel)) ||
	    !data)
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

int __real_tr_stream_open(struct tr_channel *, enum tr_lane,
                          struct tr_stream_handle *);
int __wrap_tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
                          struct tr_stream_handle *out)
{
	if (channel == atomic_load(&client_channel) &&
	    atomic_exchange(&fail_call_open, false))
		return TR_ERR_NOMEM;
	int ret = __real_tr_stream_open(channel, lane, out);
	if (ret == TR_OK && channel == atomic_load(&client_channel))
		atomic_fetch_add(&client_stream_opens, 1);
	return ret;
}

int __real_tr_rpc_record_stream_create_on_owner(
        struct tr_stream_handle, const struct tr_rpc_record_stream_config *,
        const struct tr_rpc_record_stream_callbacks *,
        struct tr_memory_budget *, struct tr_rpc_record_stream **);
int __wrap_tr_rpc_record_stream_create_on_owner(
        struct tr_stream_handle stream,
        const struct tr_rpc_record_stream_config *config,
        const struct tr_rpc_record_stream_callbacks *callbacks,
        struct tr_memory_budget *budget, struct tr_rpc_record_stream **out)
{
	if (stream.channel == atomic_load(&client_channel) &&
	    atomic_exchange(&fail_call_binding, false))
		return TR_ERR_NOMEM;
	return __real_tr_rpc_record_stream_create_on_owner(
	        stream, config, callbacks, budget, out);
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
	_Atomic int final_status;
	_Atomic unsigned calls, messages, half_closes, bodies_released,
	        cancelled;
	uint8_t expected_metadata[32];
	unsigned expected_metadata_size;
	uint16_t port;
	bool public_client;
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
		if (tr_task_cancel_requested(cancel))
			atomic_fetch_add(&f->cancelled, 1);
		/* 故意忽略已经观察的取消并生成晚结果，完成路径必须负责丢弃。 */
		for (unsigned i = 0; i < copies; ++i)
			CHECK(tr_rpc_record_reply_append(
			              reply, view->body,
			              view->header.decoded_size) == TR_OK);
		atomic_fetch_add(&f->messages, 1);
	} else if (view->header.kind == TR_RPC_RECORD_HALF_CLOSE) {
		atomic_fetch_add(&f->half_closes, 1);
		return (enum tr_rpc_status)atomic_load(&f->final_status);
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
	atomic_init(&f->final_status, TR_RPC_STATUS_OK);
	atomic_init(&f->calls, 0);
	atomic_init(&f->messages, 0);
	atomic_init(&f->half_closes, 0);
	atomic_init(&f->bodies_released, 0);
	atomic_init(&f->cancelled, 0);
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
		                       TR_LANE_CONTROL, BODY, BODY, 0, 0},
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
	if (f->public_client)
		CHECK(tr_client_create(&f->client_config, client_copy, 3,
		                       &f->client) == TR_OK);
	else
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

static void check_client_semantic(struct tr_client *client, uint64_t started,
                                  uint64_t finished, enum tr_rpc_status status)
{
	struct tr_rpc_semantic_stats actual;
	struct tr_rpc_semantic_stats expected = {.calls_started = started,
	                                         .calls_finished = finished,
	                                         .calls_inflight =
	                                                 started - finished};
	expected.final_status[status] = finished;
	memset(&actual, 0xa5, sizeof(actual));
	CHECK(tr_client_get_rpc_semantic_stats(client, &actual) == TR_OK);
	CHECK(!memcmp(&actual, &expected, sizeof(actual)));
}

static void check_client_semantic_rejected(struct tr_client *client)
{
	struct tr_rpc_semantic_stats actual, sentinel;
	memset(&actual, 0xa5, sizeof(actual));
	sentinel = actual;
	CHECK(tr_client_get_rpc_semantic_stats(client, &actual) ==
	      TR_ERR_STATE);
	CHECK(!memcmp(&actual, &sentinel, sizeof(actual)));
}

/* 统计溢出必须可辨认，但不得变成 RPC 准入失败或各字段独立饱和。
 * 先精确达到边界，再模拟下一次受理/终局；失败读取不能改写旧快照。
 * 合并使用两份仍满足生命周期恒等式的分片快照验证边界和原文保留。 */
static void test_semantic_overflow(void)
{
	struct tr_rpc_record_semantic_counters counters = {
	        .value = {
	                .calls_started = UINT64_MAX - 1,
	                .calls_finished = UINT64_MAX - 2,
	                .final_status = {[TR_RPC_STATUS_OK] = UINT64_MAX - 2}}};
	struct tr_rpc_semantic_stats actual,
	        expected = {
	                .calls_started = UINT64_MAX,
	                .calls_finished = UINT64_MAX,
	                .final_status = {[TR_RPC_STATUS_OK] = UINT64_MAX - 2,
	                                 [TR_RPC_STATUS_CANCELLED] = 1,
	                                 [TR_RPC_STATUS_INTERNAL] = 1}};
	tr_rpc_record_semantic_start(&counters);
	tr_rpc_record_semantic_finish(&counters, TR_RPC_STATUS_CANCELLED);
	tr_rpc_record_semantic_finish(&counters,
	                              (enum tr_rpc_status)TR_RPC_STATUS_COUNT);
	CHECK(tr_rpc_record_semantic_stats_read(&counters, &actual) == TR_OK);
	CHECK(!memcmp(&actual, &expected, sizeof(actual)));
	tr_rpc_record_semantic_start(&counters);
	tr_rpc_record_semantic_finish(&counters, TR_RPC_STATUS_OK);
	CHECK(tr_rpc_record_semantic_stats_read(&counters, &actual) ==
	      TR_ERR_BAD_LENGTH);
	CHECK(!memcmp(&actual, &expected, sizeof(actual)));
	CHECK(counters.overflow);

	struct tr_rpc_semantic_stats total = {
	        .calls_started = UINT64_MAX - 2,
	        .calls_finished = UINT64_MAX - 3,
	        .calls_inflight = 1,
	        .final_status = {[TR_RPC_STATUS_OK] = UINT64_MAX - 3}};
	const struct tr_rpc_semantic_stats shard = {
	        .calls_started = 2,
	        .calls_finished = 1,
	        .calls_inflight = 1,
	        .final_status = {[TR_RPC_STATUS_INTERNAL] = 1}};
	expected = (struct tr_rpc_semantic_stats){
	        .calls_started = UINT64_MAX,
	        .calls_finished = UINT64_MAX - 2,
	        .calls_inflight = 2,
	        .final_status = {[TR_RPC_STATUS_OK] = UINT64_MAX - 3,
	                         [TR_RPC_STATUS_INTERNAL] = 1}};
	CHECK(tr_rpc_record_semantic_stats_merge(&total, &shard) == TR_OK);
	CHECK(!memcmp(&total, &expected, sizeof(total)));
	CHECK(tr_rpc_record_semantic_stats_merge(&total, &shard) ==
	      TR_ERR_BAD_LENGTH);
	CHECK(!memcmp(&total, &expected, sizeof(total)));
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
		check_client_semantic_rejected(context->fixture->client);
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
	check_client_semantic(f.client, 0, 0, TR_RPC_STATUS_OK);
	atomic_store(&fail_client_create, true);
	CHECK(tr_client_connect(f.client, "127.0.0.1", f.port) == TR_ERR_NOMEM);
	CHECK(atomic_load(&create_failures) == 1);
	CHECK(!atomic_load(&client_creates));
	check_client_semantic(f.client, 0, 0, TR_RPC_STATUS_OK);
	fixture_connect(&f);
	struct call call;
	call_init(&call, &f);
	echo(&call, 1, 31, 1);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
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
	check_client_semantic(f.client, 1, 0, TR_RPC_STATUS_OK);
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
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
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
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_UNIMPLEMENTED);
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

static void test_inline_data_failure(void)
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
	wait_count(&f.calls, 1);
	struct tr_channel *client = atomic_load(&client_channel);
	struct tr_channel *server = atomic_load(&server_channel);
	atomic_store(&inline_client_data_once, true);
	send_body(&failed, 53, 43);
	call_finish(&failed, TR_RPC_STATUS_UNAVAILABLE, TR_RPC_RECORD_ON_OWNER);
	CHECK(atomic_load(&inline_client_data_calls) == 1);
	CHECK(!atomic_load(&inline_client_data_once));
	CHECK(!atomic_load(&failed.result.final_records));
	/* 本地合成 ERROR 不能证明 Channel 已死。真实 RESET 必须到达
	 * Server，双方网络槽也必须完成确认，不能依赖最终销毁兜底。 */
	wait_count(&server_resets_received, 1);
	call_retired(&failed);
	uint64_t end = now_ms() + WAIT_MS;
	for (;;) {
		struct tr_channel_stats client_stats, server_stats;
		CHECK(tr_channel_get_stats(client, &client_stats) == TR_OK);
		CHECK(tr_channel_get_stats(server, &server_stats) == TR_OK);
		if (!client_stats.active_streams &&
		    !server_stats.active_streams)
			break;
		CHECK(now_ms() < end);
		pause_once();
	}
	expect_message(&following, 61, 44);
	call_start(&following, 1, NULL);
	call_ready(&following);
	CHECK(following.handle.client == failed.handle.client);
	CHECK(following.handle.slot == failed.handle.slot);
	CHECK(following.handle.generation != failed.handle.generation);
	CHECK(tr_client_record_cancel(failed.handle) == TR_ERR_STALE);
	send_body(&following, 61, 44);
	half_close(&following);
	call_finish(&following, TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(&following);
	CHECK(atomic_load(&client_channel) == client);
	CHECK(atomic_load(&server_channel) == server);
	CHECK(atomic_load(&client_creates) == 1);
	CHECK(!atomic_load(&client_destroys));
	CHECK(atomic_load(&server_resets_received) == 1);
	CHECK(atomic_load(&failed.result.terminals) == 1);
	fixture_destroy(&f);
	CHECK(atomic_load(&failed.result.terminals) == 1);
	CHECK(atomic_load(&following.result.terminals) == 1);
	CHECK(atomic_load(&f.bodies_released) == 2);
}

static int reject_cross_owner_semantic(void *arg)
{
	struct fixture *f = arg;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!tr_reactor_in_owner_context_for(
	        tr_channel_reactor(atomic_load(&client_channel))));
	check_client_semantic_rejected(f->client);
	return TR_OK;
}

static void test_owner_callbacks(void)
{
	struct fixture f;
	struct call followup, cancelled, freed;
	fixture_init(&f);
	fixture_create(&f);
	fixture_connect(&f);
	/* 另一 owner 的同步快照不能等待 Client owner，out 保持原文。 */
	struct tr_channel *other = atomic_load(&server_channel);
	CHECK(other);
	CHECK(tr_reactor_call(tr_channel_reactor(other),
	                      reject_cross_owner_semantic, &f) == TR_OK);
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
	test_inline_data_failure();
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
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_UNAVAILABLE);
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
	struct call late, following[2];
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
	for (unsigned i = 0; i < 2; ++i)
		call_init(&following[i], &f);
	struct tr_rpc_call_options options = {.timeout_ms = 1000};
	/* 服务端线上期限为无限，取消必须由真实 Client RESET 到达；保留
	 * 真实 Client 本地一秒 timeout，而非由服务端自己的时钟取消任务。 */
	atomic_store(&clear_open_deadline, deadline);
	call_start(&late, 2, deadline ? &options : NULL);
	call_ready(&late);
	CHECK(!atomic_load(&clear_open_deadline));
	CHECK(atomic_load(&deadline_prefixes_accepted) == (deadline ? 1U : 0U));
	send_body(&late, BODY, 70);
	half_close(&late);
	gate_wait(&f.gate, 1, false);
	/* 发送接受不代表对端已解析；在原 Server data 回调处理完
	 * HALF_CLOSE 后取消，不能用尚未半关闭的路径代替本回归。 */
	wait_count(&server_half_closes_received, 1);
	atomic_store(&f.reply_copies, 1);
	if (!deadline)
		CHECK(tr_client_record_cancel(late.handle) == TR_OK);
	enum tr_rpc_status status = deadline ? TR_RPC_STATUS_DEADLINE_EXCEEDED
	                                     : TR_RPC_STATUS_CANCELLED;
	call_finish(&late, status, TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&server_messages_accepted));
	wait_count(&server_resets_received, 1);
	CHECK(atomic_load(&server_resets_received) == 1);
	/* RESET 接管网络收尾，Client 不再为不合作业务占住唯一调用槽。
	 * 原业务仍在门闩中借用输入；第二个 worker 可继续服务新调用。 */
	call_retired(&late);
	atomic_store(&f.block_work, false);
	expect_message(&following[0], 63, 71);
	call_start(&following[0], 1, NULL);
	call_ready(&following[0]);
	CHECK(tr_client_record_half_close(late.handle) == TR_ERR_STALE);
	CHECK(tr_client_record_cancel(late.handle) == TR_ERR_STALE);
	send_body(&following[0], 63, 71);
	half_close(&following[0]);
	call_finish(&following[0], TR_RPC_STATUS_OK, TR_RPC_RECORD_ON_OWNER);
	call_retired(&following[0]);
	CHECK(atomic_load(&f.messages) == 1);
	CHECK(!atomic_load(&f.cancelled));
	gate_release(&f.gate);
	wait_count(&f.messages, 2);
	wait_count(&f.cancelled, 1);
	/* 原业务仍生成三条 4145 字节的回复，共 12435 > 8192。它们尚未
	 * 被发送接受，全部由取消完成路径释放，不再要求远端把废弃批次发完。 */
	CHECK(3 * (BODY + TR_RPC_RECORD_HEADER_SIZE) > WIRE_LIMIT);
	echo(&following[1], 1, 63, 72);
	fixture_destroy(&f);
	call_finish(&late, status, TR_RPC_RECORD_ON_OWNER);
	CHECK(!atomic_load(&late.result.final_records));
	CHECK(atomic_load(&server_messages_accepted) == 2);
	CHECK(atomic_load(&server_message_bytes) ==
	      2 * (63 + TR_RPC_RECORD_HEADER_SIZE));
	CHECK(atomic_load(&server_half_closes_received) == 3);
	CHECK(atomic_load(&server_resets_received) == 1);
	CHECK(atomic_load(&f.calls) == 3);
	CHECK(atomic_load(&f.messages) == 3);
	CHECK(atomic_load(&f.half_closes) == 2);
	CHECK(atomic_load(&f.cancelled) == 1);
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
	check_client_semantic_rejected(f.client);
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

struct public_result {
	struct fixture *fixture;
	bool check_semantic;
	bool cancel_on_message;
	bool hold_on_terminal;
	struct expected_message expected[2];
	unsigned expected_count;
	_Atomic unsigned messages, initial, trailing, writable, terminal;
	_Atomic unsigned depth, arg_freed;
	struct gate *terminal_gate;
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
	struct tr_rpc_v2_call_ref held_call;
};

struct public_context {
	struct public_result *result;
	bool free_on_terminal;
};

static void public_message(struct tr_rpc_v2_call call,
                           const struct tr_rpc_bytes *message, void *arg)
{
	struct public_result *result = ((struct public_context *)arg)->result;
	(void)call;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!atomic_load(&result->terminal));
	CHECK(atomic_fetch_add(&result->depth, 1) == 0);
	unsigned index = atomic_load(&result->messages);
	CHECK(index < result->expected_count);
	CHECK(message->len == result->expected[index].size);
	for (unsigned i = 0; i < message->len; ++i)
		CHECK(message->data[i] ==
		      pattern(result->expected[index].tag, i));
	if (result->cancel_on_message) {
		CHECK(tr_rpc_v2_call_cancel(call) == TR_OK);
		CHECK(!atomic_load(&result->terminal));
	}
	CHECK(atomic_fetch_sub(&result->depth, 1) == 1);
	atomic_fetch_add(&result->messages, 1);
}

static void public_metadata(struct tr_rpc_v2_call call,
                            enum tr_rpc_v2_metadata_phase phase,
                            const struct tr_rpc_v2_metadata_view *metadata,
                            void *arg)
{
	struct public_result *result = ((struct public_context *)arg)->result;
	(void)call;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!atomic_load(&result->terminal));
	CHECK(tr_rpc_v2_metadata_count(metadata) == 0);
	if (phase == TR_RPC_V2_METADATA_INITIAL)
		CHECK(atomic_fetch_add(&result->initial, 1) == 0);
	else {
		CHECK(phase == TR_RPC_V2_METADATA_TRAILING);
		CHECK(atomic_fetch_add(&result->trailing, 1) == 0);
	}
}

static void public_writable(struct tr_rpc_v2_call call, void *arg)
{
	struct public_result *result = ((struct public_context *)arg)->result;
	(void)call;
	CHECK(tr_reactor_in_owner_context());
	CHECK(!atomic_load(&result->terminal));
	atomic_fetch_add(&result->writable, 1);
}

static void public_terminal(struct tr_rpc_v2_call call,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct public_context *context = arg;
	struct public_result *result = context->result;
	(void)call;
	CHECK(!atomic_load(&result->depth));
	CHECK(!atomic_load(&result->terminal));
	CHECK(delivery == TR_RPC_V2_DELIVERY_LIVE
	              ? tr_reactor_in_owner_context()
	              : !tr_reactor_in_owner_context());
	CHECK(tr_client_destroy(result->fixture->client) == TR_ERR_STATE);
	CHECK(tr_client_wait_drained(result->fixture->client, 1) ==
	      TR_ERR_STATE);
	if (result->check_semantic)
		check_client_semantic(result->fixture->client, 1, 1, status);
	if (result->hold_on_terminal)
		CHECK(tr_rpc_v2_call_hold(call, &result->held_call) == TR_OK);
	result->status = status;
	result->delivery = delivery;
	if (result->terminal_gate) {
		destroy_owner_thread = pthread_self();
		gate_wait(result->terminal_gate, 0, true);
	}
	if (context->free_on_terminal) {
		free(context);
		atomic_store(&result->arg_freed, 1);
	}
	CHECK(atomic_fetch_add(&result->terminal, 1) == 0);
}

static struct tr_rpc_v2_client_callbacks
public_callbacks(struct public_context *context)
{
	return (struct tr_rpc_v2_client_callbacks){.message = public_message,
	                                           .metadata = public_metadata,
	                                           .writable = public_writable,
	                                           .terminal = public_terminal,
	                                           .arg = context};
}

static void public_fixture_init(struct fixture *f)
{
	fixture_init(f);
	f->public_client = true;
	f->client_config.limits.max_message_bytes = BODY;
	fixture_create(f);
	fixture_connect(f);
}

static void public_fixture_init_capacity(struct fixture *f, uint32_t capacity)
{
	fixture_init(f);
	f->public_client = true;
	f->client_config.limits.max_calls = capacity;
	f->client_config.limits.max_streams = capacity;
	f->client_config.limits.max_message_bytes = BODY;
	fixture_create(f);
	fixture_connect(f);
}

static void test_method_budget_release_retry(void)
{
	struct fixture f;
	uint64_t current;
	struct tr_memory_budget *budget;

	public_fixture_init(&f);
	atomic_store(&budget_release_pool_seen, 0U);
	atomic_store(&budget_release_current_before_failure, 0U);
	atomic_store(&budget_release_injected_budget, NULL);
	atomic_store(&budget_release_pool_target, 2U);
	CHECK(tr_client_destroy(f.client) == TR_ERR_STATE);
	CHECK(atomic_load(&budget_release_pool_seen) == 2U);
	budget = atomic_load(&budget_release_injected_budget);
	CHECK(budget != NULL);
	current = atomic_load(&budget_release_current_before_failure);
	CHECK(current > 3U * sizeof(struct tr_rpc_method_desc));

	/* 修复注入造成的预算计数后，第二次 destroy 应能归还方法表额度。 */
	atomic_store(&budget->current_bytes, current);
	atomic_store(&budget_release_pool_target, 0U);
	CHECK(tr_client_destroy(f.client) == TR_OK);
	atomic_store(&budget_release_injected_budget, NULL);
	f.client = NULL;
	fixture_destroy(&f);
}

static int resume_public_send_on_owner(void *arg)
{
	(void)arg;
	CHECK(atomic_load(&public_send_held));
	CHECK(tr_reactor_in_owner_context_for(
	        tr_channel_reactor(public_held_stream.channel)));
	int ret = __real_tr_stream_sendv(public_held_stream,
	                                 public_held_buffers, 2);
	if (ret == TR_OK) {
		public_held_buffers[0] = public_held_buffers[1] = NULL;
		atomic_store(&public_send_held, false);
	}
	return ret;
}

static int pump_public_call_on_owner(void *arg)
{
	struct tr_rpc_record_client_handle handle =
	        tr_rpc_record_client_private_handle(
	                *(struct tr_rpc_v2_call *)arg);
	return tr_rpc_record_client_pump_on_owner(handle.client);
}

static void test_public_copy_and_pool(void)
{
	struct fixture f;
	struct public_result result = {.fixture = &f,
	                               .expected = {{257, 70}, {263, 71}},
	                               .expected_count = 2};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t source[263];
	public_fixture_init(&f);
	CHECK(tr_client_call_start(f.client, 17, 2, NULL, &cb, &call) == TR_OK);
	wait_count(&result.writable, 1);
	for (unsigned i = 0; i < 257; ++i)
		source[i] = pattern(70, i);
	struct tr_rpc_bytes message = {source, 257};
	atomic_store(&hold_public_send, true);
	CHECK(tr_rpc_v2_call_send(call, &message) == TR_OK);
	CHECK(atomic_load(&public_send_held));
	CHECK(public_held_buffers[1]->data != source);
	memset(source, 0xee, sizeof(source));
	struct tr_reactor *owner =
	        tr_channel_reactor(atomic_load(&client_channel));
	CHECK(tr_reactor_call(owner, resume_public_send_on_owner, NULL) ==
	      TR_OK);
	wait_count(&result.messages, 1);

	/* 占满真实复制池，保持普通 TX/信用充足，区分两种准入压力。 */
	struct tr_buffer_pool *pool = atomic_load(&public_send_pool);
	CHECK(pool && pool->buffer_count);
	uint32_t count = pool->buffer_count;
	struct tr_buffer **owned = calloc(count, sizeof(*owned));
	CHECK(owned);
	for (uint32_t i = 0; i < count; ++i)
		CHECK(tr_buffer_acquire(pool, 1, &owned[i]) == TR_OK);
	CHECK(tr_buffer_pool_free_count(pool) == 0);
	unsigned attempts_before =
	        atomic_load(&attempts[TR_RPC_RECORD_MESSAGE]);
	message.len = BODY + 1;
	CHECK(tr_rpc_v2_call_send(call, &message) == TR_ERR_BAD_LENGTH);
	message.len = 263;
	for (unsigned i = 0; i < message.len; ++i)
		source[i] = pattern(71, i);
	CHECK(tr_rpc_v2_call_send(call, &message) == TR_AGAIN);
	CHECK(atomic_load(&attempts[TR_RPC_RECORD_MESSAGE]) == attempts_before);
	unsigned writable_before = atomic_load(&result.writable);
	CHECK(tr_reactor_call(owner, pump_public_call_on_owner, &call) ==
	      TR_OK);
	CHECK(atomic_load(&result.writable) == writable_before);
	tr_buffer_release(owned[0]);
	owned[0] = NULL;
	wait_count(&result.writable, writable_before + 1);
	CHECK(tr_rpc_v2_call_send(call, &message) == TR_OK);
	memset(source, 0xff, sizeof(source));
	wait_count(&result.messages, 2);
	CHECK(tr_rpc_v2_call_half_close(call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_OK);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(atomic_load(&result.initial) == 1);
	CHECK(atomic_load(&result.trailing) == 1);
	/* 新增复制池仍有外部测试持有者时，销毁必须保留预算依赖并可重试。 */
	CHECK(count > 1);
	for (uint32_t i = 2; i < count; ++i)
		tr_buffer_release(owned[i]);
	CHECK(tr_client_destroy(f.client) == TR_ERR_STATE);
	tr_buffer_release(owned[1]);
	free(owned);
	CHECK(tr_client_destroy(f.client) == TR_OK);
	f.client = NULL;
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

/* 自动发送只推进原调用：三段分别背压，响应正文先到也必须等 FINAL。 */
static void test_public_unary_progress(unsigned size, enum tr_rpc_status status)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f, .expected = {{size, 79}}, .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call, untouched, sentinel;
	uint8_t source[BODY];
	for (unsigned i = 0; i < size; ++i)
		source[i] = pattern(79, i);
	struct tr_rpc_bytes request = {size ? source : NULL, size};
	public_fixture_init(&f);
	char key[] = "token";
	uint8_t value[] = {0, 0xff, 3};
	struct tr_rpc_metadata metadata = {key, value, sizeof(value)};
	struct tr_rpc_call_options options = {.metadata = &metadata,
	                                      .metadata_count = 1};
	const uint8_t encoded[] = {5,   3,   0, 't',  'o', 'k',
	                           'e', 'n', 0, 0xff, 3};
	memcpy(f.expected_metadata, encoded, sizeof(encoded));
	f.expected_metadata_size = sizeof(encoded);
	atomic_store(&f.final_status, status);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &options, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	memset(source, 0xee, sizeof(source));
	memset(key, 'x', sizeof(key));
	memset(value, 0xee, sizeof(value));
	memset(&metadata, 0, sizeof(metadata));
	memset(&options, 0, sizeof(options));
	CHECK(tr_rpc_v2_call_send(call, &request) == TR_ERR_STATE);
	CHECK(tr_rpc_v2_call_half_close(call) == TR_ERR_STATE);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_OPEN]));
	check_client_semantic(f.client, 1, 0, TR_RPC_STATUS_OK);
	atomic_store(&block_kind, TR_RPC_RECORD_MESSAGE);
	wait_count(&attempts[TR_RPC_RECORD_MESSAGE], 1);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_OPEN]) == 1);
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]));
	CHECK(!atomic_load(&result.terminal));
	atomic_store(&block_kind, TR_RPC_RECORD_HALF_CLOSE);
	wait_count(&attempts[TR_RPC_RECORD_HALF_CLOSE], 1);
	wait_count(&result.messages, 1);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]) == 1);
	CHECK(!atomic_load(&result.terminal));
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_HALF_CLOSE]));
	CHECK(tr_client_begin_drain(f.client) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, 0) == TR_AGAIN);
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_CLOSED);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	atomic_store(&block_kind, 0);
	wait_count(&result.terminal, 1);
	CHECK(result.status == status);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(tr_client_wait_drained(f.client, WAIT_MS) == TR_OK);
	CHECK(atomic_load(&accepted[TR_RPC_RECORD_HALF_CLOSE]) == 1);
	CHECK(atomic_load(&f.calls) == 1);
	CHECK(atomic_load(&f.messages) == 1);
	CHECK(atomic_load(&f.half_closes) == 1);
	CHECK(atomic_load(&result.trailing) == 1);
	CHECK(!atomic_load(&result.writable));
	CHECK(tr_rpc_v2_call_cancel(call) == TR_ERR_STALE);
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

static void test_public_unary_callback_cancel(void)
{
	struct fixture f;
	struct public_result result = {.fixture = &f,
	                               .cancel_on_message = true,
	                               .expected = {{1, 81}},
	                               .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t byte = pattern(81, 0);
	struct tr_rpc_bytes request = {&byte, 1};
	public_fixture_init(&f);
	atomic_store(&block_kind, TR_RPC_RECORD_HALF_CLOSE);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_CANCELLED);
	CHECK(atomic_load(&result.messages) == 1);
	CHECK(!atomic_load(&result.trailing));
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_HALF_CLOSE]));
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

static void test_public_unary_admission(void)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call, untouched, sentinel;
	uint8_t source[BODY + 1] = {0};
	source[0] = pattern(80, 0);
	struct tr_rpc_bytes request = {source, 1};
	public_fixture_init(&f);
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	CHECK(tr_client_unary_call(f.client, 17, 1, NULL, NULL, &cb,
	                           &untouched) == TR_ERR_INVALID);
	CHECK(tr_client_unary_call(f.client, 17, 2, &request, NULL, &cb,
	                           &untouched) == TR_ERR_UNSUPPORTED);
	CHECK(tr_client_unary_call(f.client, 17, 100, &request, NULL, &cb,
	                           &untouched) == TR_ERR_UNSUPPORTED);
	request.len = BODY + 1;
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_BAD_LENGTH);
	request.len = 1;
	request.data = NULL;
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_INVALID);
	request.data = source;
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, NULL,
	                           &untouched) == TR_ERR_INVALID);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	check_client_semantic(f.client, 0, 0, TR_RPC_STATUS_OK);
	CHECK(!atomic_load(&attempts[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&result.terminal));

	/* 先走一次真实发送取得复制池，再耗尽它；失败不得打开传输流或
	 * 改写 out，释放容量后同一请求可以重新受理，不能悄悄后台排队。 */
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_OK);
	struct tr_buffer_pool *pool = atomic_load(&public_send_pool);
	CHECK(pool && pool->buffer_count);
	uint64_t end = now_ms() + WAIT_MS;
	while (tr_buffer_pool_free_count(pool) != pool->buffer_count) {
		CHECK(now_ms() < end);
		pause_once();
	}
	struct tr_buffer **owned = calloc(pool->buffer_count, sizeof(*owned));
	CHECK(owned);
	for (uint32_t i = 0; i < pool->buffer_count; ++i)
		CHECK(tr_buffer_acquire(pool, 1, &owned[i]) == TR_OK);
	unsigned opens = atomic_load(&attempts[TR_RPC_RECORD_OPEN]);
	/* 容量耗尽不能掩盖不可重试的参数与方法错误。 */
	request.len = BODY + 1;
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_BAD_LENGTH);
	request.len = 1;
	CHECK(tr_client_unary_call(f.client, 17, 2, &request, NULL, &cb,
	                           &untouched) == TR_ERR_UNSUPPORTED);
	struct tr_rpc_metadata invalid_metadata = {"Bad-Key", source, 1};
	struct tr_rpc_call_options invalid_options = {
	        .metadata = &invalid_metadata, .metadata_count = 1};
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &invalid_options,
	                           &cb, &untouched) == TR_ERR_BAD_FLAGS);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_AGAIN);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(atomic_load(&attempts[TR_RPC_RECORD_OPEN]) == opens);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
	for (uint32_t i = 0; i < pool->buffer_count; ++i)
		tr_buffer_release(owned[i]);
	free(owned);
	/* 换独立的用户参数，不能在终局后复用仍被测试观察的回调对象。 */
	struct public_result retry = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context retry_context = {&retry, false};
	cb = public_callbacks(&retry_context);
	/* 正文复制后 stream / binding 构造失败，仍不发布弱能力或终局。 */
	atomic_store(&fail_call_open, true);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_NOMEM);
	CHECK(!atomic_load(&fail_call_open));
	CHECK(tr_buffer_pool_free_count(pool) == pool->buffer_count);
	atomic_store(&fail_call_binding, true);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &untouched) == TR_ERR_NOMEM);
	CHECK(!atomic_load(&fail_call_binding));
	CHECK(tr_buffer_pool_free_count(pool) == pool->buffer_count);
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(!atomic_load(&retry.terminal));
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&retry.terminal, 1);
	CHECK(retry.status == TR_RPC_STATUS_OK);
	check_client_semantic(f.client, 2, 2, TR_RPC_STATUS_OK);
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(atomic_load(&retry.terminal) == 1);
}

/* 两种公开调用入口在发布能力前读钟失败：不产生幽灵终局、OPEN 或
 * 槽位占用；同一 Client 随后仍能完成真实一元调用。 */
static void test_public_start_clock_failure(void)
{
	struct fixture f;
	struct public_result failed = {.fixture = &f};
	struct public_context failed_context = {&failed, false};
	struct tr_rpc_v2_client_callbacks cb =
	        public_callbacks(&failed_context);
	struct tr_rpc_v2_call untouched, sentinel, healthy;
	struct tr_rpc_call_options options = {.timeout_ms = 5000};
	uint8_t source[] = {0};
	struct tr_rpc_bytes request = {source, sizeof(source)};

	fixture_init(&f);
	f.public_client = true;
	f.client_config.limits.max_message_bytes = BODY;
	f.client_config.limits.max_calls = 1;
	f.client_config.limits.max_streams = 1;
	f.client_record.calls.capacity = 1;
	fixture_create(&f);
	fixture_connect(&f);
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	atomic_store(&fail_public_start_clock, true);
	CHECK(tr_client_call_start(f.client, 17, 2, &options, &cb,
	                           &untouched) == TR_ERR_SYS);
	CHECK(!atomic_load(&fail_public_start_clock));
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	atomic_store(&fail_public_start_clock, true);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &options, &cb,
	                           &untouched) == TR_ERR_SYS);
	CHECK(!atomic_load(&fail_public_start_clock));
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(atomic_load(&clock_failures) == 2);
	CHECK(!atomic_load(&attempts[TR_RPC_RECORD_OPEN]));
	CHECK(!atomic_load(&failed.terminal));
	check_client_semantic(f.client, 0, 0, TR_RPC_STATUS_OK);

	source[0] = pattern(80, 0);
	struct public_result recovered = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context recovered_context = {&recovered, false};
	cb = public_callbacks(&recovered_context);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &healthy) == TR_OK);
	wait_count(&recovered.terminal, 1);
	CHECK(recovered.status == TR_RPC_STATUS_OK);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
	fixture_destroy(&f);
	CHECK(!atomic_load(&failed.terminal));
}

/* 早期限准入失败不能缩短已接纳调用的截止时间。 */
static void test_public_failed_earlier_deadline(void)
{
	struct fixture f;
	struct public_result original = {.fixture = &f};
	struct public_result rejected = {.fixture = &f};
	struct public_context original_context = {&original, false};
	struct public_context rejected_context = {&rejected, false};
	struct tr_rpc_v2_client_callbacks original_cb =
	        public_callbacks(&original_context);
	struct tr_rpc_v2_client_callbacks rejected_cb =
	        public_callbacks(&rejected_context);
	struct tr_rpc_v2_call original_call, untouched, sentinel,
	        recovered_call;
	struct tr_rpc_call_options later = {.timeout_ms = 1000};
	struct tr_rpc_call_options earlier = {.timeout_ms = 25};
	uint8_t request_byte = pattern(80, 0);
	struct tr_rpc_bytes request = {&request_byte, 1};
	uint64_t reuse_end;
	int reuse_ret;

	fixture_init(&f);
	f.public_client = true;
	f.client_config.limits.max_calls = 1;
	f.client_config.limits.max_streams = 1;
	f.client_record.calls.capacity = 1;
	fixture_create(&f);
	fixture_connect(&f);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	atomic_store(&capture_public_start_clock, true);
	CHECK(tr_client_call_start(f.client, 17, 2, &later, &original_cb,
	                           &original_call) == TR_OK);
	CHECK(atomic_load(&public_start_ns));
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	unsigned opens = atomic_load(&client_stream_opens);
	memset(&untouched, 0xa5, sizeof(untouched));
	sentinel = untouched;
	atomic_store(&fail_public_start_clock, true);
	CHECK(tr_client_call_start(f.client, 17, 2, &earlier, &rejected_cb,
	                           &untouched) == TR_ERR_SYS);
	CHECK(!atomic_load(&fail_public_start_clock));
	CHECK(!memcmp(&untouched, &sentinel, sizeof(untouched)));
	CHECK(atomic_load(&clock_failures) == 1);
	CHECK(atomic_load(&client_stream_opens) == opens);
	CHECK(!atomic_load(&rejected.terminal));
	check_client_semantic(f.client, 1, 0, TR_RPC_STATUS_OK);
	atomic_store(&capture_open_attempt, true);
	struct tr_reactor *owner =
	        tr_channel_reactor(atomic_load(&client_channel));
	CHECK(tr_reactor_call(owner, pump_public_call_on_owner,
	                      &original_call) == TR_OK);
	CHECK(!atomic_load(&capture_open_attempt));
	CHECK(atomic_load(&observed_open_clock_ns) != 0);
	CHECK(atomic_load(&observed_open_clock_ns) +
	              atomic_load(&observed_open_remaining_ns) ==
	      atomic_load(&public_start_ns) + 1000000000);
	wait_count(&original.terminal, 1);
	CHECK(original.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(original.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(atomic_load(&original.terminal) == 1);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	atomic_store(&block_kind, 0);
	struct public_result recovered = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context recovered_context = {&recovered, false};
	struct tr_rpc_v2_client_callbacks recovered_cb =
	        public_callbacks(&recovered_context);
	recovered_call = sentinel;
	/* terminal 回调可能早于传输层 OPEN 确认。确认到达前 RESET 可返回
	 * TR_AGAIN，槽位继续承担清理责任；推进 owner 并重试有界准入。 */
	reuse_end = now_ms() + WAIT_MS;
	for (;;) {
		reuse_ret =
		        tr_client_unary_call(f.client, 17, 1, &request, NULL,
		                             &recovered_cb, &recovered_call);
		if (reuse_ret != TR_AGAIN)
			break;
		CHECK(!memcmp(&recovered_call, &sentinel,
		              sizeof(recovered_call)));
		CHECK(atomic_load(&client_stream_opens) == opens);
		CHECK(now_ms() < reuse_end);
		CHECK(tr_reactor_call(owner, pump_public_call_on_owner,
		                      &original_call) == TR_OK);
		pause_once();
	}
	CHECK(reuse_ret == TR_OK);
	wait_count(&recovered.terminal, 1);
	CHECK(recovered.status == TR_RPC_STATUS_OK);
	struct tr_rpc_semantic_stats expected = {
	        .calls_started = 2,
	        .calls_finished = 2,
	        .final_status = {[TR_RPC_STATUS_DEADLINE_EXCEEDED] = 1,
	                         [TR_RPC_STATUS_OK] = 1}};
	struct tr_rpc_semantic_stats actual;
	CHECK(tr_client_get_rpc_semantic_stats(f.client, &actual) == TR_OK);
	CHECK(!memcmp(&actual, &expected, sizeof(actual)));
	fixture_destroy(&f);
	CHECK(atomic_load(&original.terminal) == 1);
	CHECK(!atomic_load(&rejected.terminal));
	CHECK(atomic_load(&recovered.terminal) == 1);
}

/* Client 进度定时器首次 arm 失败后回滚连接；同一实例能重新连接，
 * 并完成新的公开调用，证明计时器/Channel 没有残留占用。 */
static void test_public_start_timer_failure(void)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t source[] = {0};
	struct tr_rpc_bytes request = {source, sizeof(source)};

	fixture_init(&f);
	f.public_client = true;
	f.client_config.limits.max_message_bytes = BODY;
	fixture_create(&f);
	atomic_store(&fail_client_start_arm, true);
	CHECK(tr_client_connect(f.client, "127.0.0.1", f.port) == TR_ERR_SYS);
	CHECK(!atomic_load(&fail_client_start_arm));
	CHECK(atomic_load(&client_start_arm_failures) == 1);
	CHECK(!atomic_load(&attempts[TR_RPC_RECORD_OPEN]));
	fixture_connect(&f);
	source[0] = pattern(80, 0);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_OK);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
	fixture_destroy(&f);
}

/* 仅在 Client 接纳响应 MESSAGE 的那次读钟替换纳秒值；其他 Reactor
 * 和 Server 始终使用真实时钟，避免定时器先行到期掩盖边界结果。 */
static void test_public_response_deadline_boundary(unsigned mode)
{
	struct fixture f;
	struct public_result result = {.fixture = &f,
	                               .expected = {{1, 80}},
	                               .expected_count = mode == 1 ? 1 : 0};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	struct tr_rpc_call_options options = {.timeout_ms = 5000};
	uint8_t source[] = {0};
	struct tr_rpc_bytes request = {source, sizeof(source)};

	public_fixture_init(&f);
	source[0] = pattern(80, 0);
	atomic_store(&capture_public_start_clock, true);
	atomic_store(&response_clock_mode, mode);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &options, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(atomic_load(&public_start_ns));
	CHECK(!atomic_load(&response_clock_mode));
	CHECK(mode == 4 ? !atomic_load(&response_clock_reads)
	                : atomic_load(&response_clock_reads) >= 1);
	CHECK(result.status == (mode == 1   ? TR_RPC_STATUS_OK
	                        : mode == 4 ? TR_RPC_STATUS_INTERNAL
	                                    : TR_RPC_STATUS_DEADLINE_EXCEEDED));
	CHECK(atomic_load(&clock_failures) == (mode == 4 ? 1U : 0U));
	CHECK(atomic_load(&result.messages) == (mode == 1 ? 1U : 0U));
	check_client_semantic(f.client, 1, 1, result.status);
	fixture_destroy(&f);
}

/* 起始时间逼近 uint64 纳秒上界，最大公开超时必须饱和，OPEN 上的
 * 剩余期限仍然为大正值，不能回绕成已到期或无期限。 */
static void test_public_saturated_deadline(void)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f, .expected = {{1, 80}}, .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	struct tr_rpc_call_options options = {.timeout_ms = UINT32_MAX};
	uint8_t source[] = {0};
	struct tr_rpc_bytes request = {source, sizeof(source)};
	struct timespec before, after;

	public_fixture_init(&f);
	source[0] = pattern(80, 0);
	CHECK(__real_clock_gettime(CLOCK_MONOTONIC, &before) == 0);
	atomic_store(&saturate_public_start_clock, true);
	atomic_store(&capture_public_start_clock, true);
	atomic_store(&capture_open_timeout, true);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &options, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(__real_clock_gettime(CLOCK_MONOTONIC, &after) == 0);
	CHECK(result.status == TR_RPC_STATUS_OK);
	CHECK(atomic_load(&public_start_ns) > UINT64_MAX - 1000000000);
	CHECK(!atomic_load(&capture_open_timeout));
	uint64_t sent_at = UINT64_MAX - atomic_load(&observed_open_timeout);
	uint64_t before_ns =
	        (uint64_t)before.tv_sec * 1000000000 + (uint64_t)before.tv_nsec;
	uint64_t after_ns =
	        (uint64_t)after.tv_sec * 1000000000 + (uint64_t)after.tv_nsec;
	CHECK(sent_at >= before_ns && sent_at <= after_ns);
	check_client_semantic(f.client, 1, 1, TR_RPC_STATUS_OK);
	fixture_destroy(&f);
}

/* 保留尚未发送的请求时分别取消、到期、停止回收、断连；成功销毁同时
 * 验证复制池及实例预算没有被本地暂存正文永久占有。 */
static void test_public_unary_terminal(unsigned mode)
{
	struct fixture f;
	struct public_result result = {.fixture = &f};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t bytes[257] = {0};
	struct tr_rpc_bytes request = {bytes, sizeof(bytes)};
	struct tr_rpc_call_options options = {.timeout_ms =
	                                              mode == 1 ? 100 : 0};
	public_fixture_init(&f);
	atomic_store(&block_kind, mode >= 4 ? 0 : TR_RPC_RECORD_OPEN);
	atomic_store(&inline_client_data_once, mode == 5);
	CHECK(tr_client_unary_call(f.client, 17, mode == 4 ? 99 : 1, &request,
	                           &options, &cb, &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	enum tr_rpc_status status = TR_RPC_STATUS_CANCELLED;
	enum tr_rpc_v2_delivery delivery = TR_RPC_V2_DELIVERY_LIVE;
	if (mode == 0) {
		CHECK(tr_rpc_v2_call_cancel(call) == TR_OK);
	} else if (mode == 1) {
		status = TR_RPC_STATUS_DEADLINE_EXCEEDED;
	} else if (mode == 2) {
		struct tr_channel *channel = atomic_load(&client_channel);
		CHECK(tr_reactor_call(tr_channel_reactor(channel),
		                      disable_client_routes_on_owner,
		                      channel) == TR_OK);
		atomic_store(&fail_client_destroy, true);
		CHECK(tr_client_destroy(f.client) == TR_ERR_SYS);
		delivery = TR_RPC_V2_DELIVERY_SHUTDOWN;
	} else if (mode == 4) {
		status = TR_RPC_STATUS_UNIMPLEMENTED;
	} else if (mode == 5) {
		status = TR_RPC_STATUS_UNAVAILABLE;
	} else {
		uint64_t end = now_ms() + WAIT_MS;
		int ret;
		while ((ret = tr_server_destroy(f.server)) == TR_AGAIN) {
			CHECK(now_ms() < end);
			pause_once();
		}
		CHECK(ret == TR_OK);
		f.server = NULL;
		status = TR_RPC_STATUS_UNAVAILABLE;
	}
	wait_count(&result.terminal, 1);
	CHECK(result.status == status);
	CHECK(result.delivery == delivery);
	if (mode < 4) {
		CHECK(!atomic_load(&accepted[TR_RPC_RECORD_OPEN]));
		CHECK(!atomic_load(&accepted[TR_RPC_RECORD_MESSAGE]));
	}
	CHECK(!atomic_load(&result.messages));
	CHECK(!atomic_load(&result.writable));
	if (mode == 5) {
		CHECK(atomic_load(&inline_client_data_calls) == 1);
		CHECK(!atomic_load(&accepted[TR_RPC_RECORD_HALF_CLOSE]));
	}
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

static void test_public_call_ref_terminal_snapshot(void)
{
	struct fixture f;
	struct public_result result = {.fixture = &f,
	                               .hold_on_terminal = true,
	                               .expected = {{1, 93}},
	                               .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t byte = pattern(93, 0);
	struct tr_rpc_bytes request = {&byte, 1};

	public_fixture_init(&f);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_OK);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_LIVE);
	struct tr_rpc_v2_call_result snapshot = {0};
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) == TR_OK);
	CHECK(snapshot.status == result.status);
	CHECK(snapshot.delivery == result.delivery);
	CHECK(tr_rpc_v2_call_ref_release(&result.held_call) == TR_OK);
	struct tr_rpc_v2_call_ref zero_ref = {0};
	CHECK(!memcmp(&result.held_call, &zero_ref, sizeof(zero_ref)));
	CHECK(tr_rpc_v2_call_ref_release(&result.held_call) == TR_ERR_STALE);
	fixture_destroy(&f);
}

static void test_public_call_ref_pending_cancel(void)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f, .expected = {{1, 94}}, .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	struct tr_rpc_v2_call_ref duplicate = {
	        ._private = {UINT64_C(0xa5), UINT64_C(0xb6), UINT64_C(0xc7),
	                     UINT64_C(0xd8)}};
	struct tr_rpc_v2_call_ref untouched = duplicate;
	uint8_t byte = pattern(94, 0);
	struct tr_rpc_bytes request = {&byte, 1};

	public_fixture_init(&f);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	CHECK(tr_rpc_v2_call_hold(call, &result.held_call) == TR_OK);
	CHECK(tr_rpc_v2_call_hold(call, &duplicate) == TR_ERR_STATE);
	CHECK(!memcmp(&duplicate, &untouched, sizeof(duplicate)));
	struct tr_rpc_v2_call_result snapshot = {
	        .status = TR_RPC_STATUS_INTERNAL,
	        .delivery = TR_RPC_V2_DELIVERY_SHUTDOWN};
	struct tr_rpc_v2_call_result before = snapshot;
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) ==
	      TR_AGAIN);
	CHECK(!memcmp(&snapshot, &before, sizeof(snapshot)));
	CHECK(tr_rpc_v2_call_ref_cancel(result.held_call) == TR_OK);
	wait_count(&result.terminal, 1);
	CHECK(result.status == TR_RPC_STATUS_CANCELLED);
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) == TR_OK);
	CHECK(snapshot.status == TR_RPC_STATUS_CANCELLED);
	CHECK(snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(tr_rpc_v2_call_ref_cancel(result.held_call) == TR_ERR_STATE);
	atomic_store(&block_kind, 0);
	CHECK(tr_rpc_v2_call_ref_release(&result.held_call) == TR_OK);
	fixture_destroy(&f);
}

static void test_public_call_ref_stale_token_reuse(void)
{
	struct fixture f;
	struct public_result first = {.fixture = &f,
	                              .hold_on_terminal = true,
	                              .expected = {{1, 95}},
	                              .expected_count = 1};
	struct public_context first_context = {&first, false};
	struct tr_rpc_v2_client_callbacks first_cb =
	        public_callbacks(&first_context);
	struct tr_rpc_v2_call first_call;
	uint8_t first_byte = pattern(95, 0);
	struct tr_rpc_bytes first_request = {&first_byte, 1};
	public_fixture_init_capacity(&f, 1);
	CHECK(tr_client_unary_call(f.client, 17, 1, &first_request, NULL,
	                           &first_cb, &first_call) == TR_OK);
	wait_count(&first.terminal, 1);
	struct tr_rpc_v2_call_ref stale_copy = first.held_call;
	uint64_t first_slot = first.held_call._private[2];
	uint64_t first_token = first.held_call._private[3];
	CHECK(tr_rpc_v2_call_ref_release(&first.held_call) == TR_OK);
	CHECK(tr_reactor_call(tr_channel_reactor(atomic_load(&client_channel)),
	                      pump_public_call_on_owner, &first_call) == TR_OK);

	struct public_result second = {.fixture = &f,
	                               .hold_on_terminal = true,
	                               .expected = {{1, 96}},
	                               .expected_count = 1};
	struct public_context second_context = {&second, false};
	struct tr_rpc_v2_client_callbacks second_cb =
	        public_callbacks(&second_context);
	struct tr_rpc_v2_call second_call;
	uint8_t second_byte = pattern(96, 0);
	struct tr_rpc_bytes second_request = {&second_byte, 1};
	CHECK(tr_client_unary_call(f.client, 17, 1, &second_request, NULL,
	                           &second_cb, &second_call) == TR_OK);
	wait_count(&second.terminal, 1);
	CHECK(first.held_call._private[2] == 0);
	CHECK(first_slot == second.held_call._private[2]);
	CHECK(stale_copy._private[3] == first_token);
	CHECK(stale_copy._private[3] != second.held_call._private[3]);
	CHECK(tr_rpc_v2_call_ref_release(&stale_copy) == TR_ERR_STALE);
	struct tr_rpc_v2_call_result snapshot = {0};
	CHECK(tr_rpc_v2_call_ref_result(second.held_call, &snapshot) == TR_OK);
	CHECK(snapshot.status == TR_RPC_STATUS_OK);
	CHECK(tr_rpc_v2_call_ref_release(&second.held_call) == TR_OK);
	fixture_destroy(&f);
}

static void test_public_call_ref_capacity_and_drain(void)
{
	struct fixture f;
	struct public_result held = {.fixture = &f,
	                             .hold_on_terminal = true,
	                             .expected = {{1, 97}},
	                             .expected_count = 1};
	struct public_context held_context = {&held, false};
	struct tr_rpc_v2_client_callbacks held_cb =
	        public_callbacks(&held_context);
	struct tr_rpc_v2_call held_call, rejected_call, sentinel;
	uint8_t byte = pattern(97, 0);
	struct tr_rpc_bytes request = {&byte, 1};
	public_fixture_init_capacity(&f, 1);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &held_cb,
	                           &held_call) == TR_OK);
	wait_count(&held.terminal, 1);
	memset(&rejected_call, 0xa5, sizeof(rejected_call));
	sentinel = rejected_call;
	struct public_result rejected = {.fixture = &f};
	struct public_context rejected_context = {&rejected, false};
	struct tr_rpc_v2_client_callbacks rejected_cb =
	        public_callbacks(&rejected_context);
	CHECK(tr_client_call_start(f.client, 17, 2, NULL, &rejected_cb,
	                           &rejected_call) == TR_AGAIN);
	CHECK(!memcmp(&rejected_call, &sentinel, sizeof(sentinel)));
	CHECK(!atomic_load(&rejected.terminal));
	CHECK(tr_client_begin_drain(f.client) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, 0) == TR_AGAIN);
	CHECK(tr_rpc_v2_call_ref_release(&held.held_call) == TR_OK);
	CHECK(tr_client_wait_drained(f.client, WAIT_MS) == TR_OK);
	fixture_destroy(&f);
}

static void test_public_call_ref_destroy_retry(void)
{
	struct fixture f;
	struct public_result result = {.fixture = &f};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t byte = pattern(98, 0);
	struct tr_rpc_bytes request = {&byte, 1};
	public_fixture_init_capacity(&f, 1);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	CHECK(tr_rpc_v2_call_hold(call, &result.held_call) == TR_OK);
	struct tr_rpc_v2_call_result snapshot = {0};
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) ==
	      TR_AGAIN);
	struct tr_channel *channel = atomic_load(&client_channel);
	CHECK(tr_reactor_call(tr_channel_reactor(channel),
	                      disable_client_routes_on_owner,
	                      channel) == TR_OK);
	CHECK(tr_client_destroy(f.client) == TR_AGAIN);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(result.status == TR_RPC_STATUS_CANCELLED);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) == TR_OK);
	CHECK(snapshot.status == TR_RPC_STATUS_CANCELLED);
	CHECK(snapshot.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(tr_rpc_v2_call_ref_cancel(result.held_call) == TR_ERR_STATE);
	atomic_store(&block_kind, 0);
	CHECK(tr_rpc_v2_call_ref_release(&result.held_call) == TR_OK);
	CHECK(tr_client_destroy(f.client) == TR_OK);
	f.client = NULL;
	fixture_destroy(&f);
}

static void test_public_call_ref_shutdown_callback_hold(void)
{
	struct fixture f;
	struct public_result result = {.fixture = &f, .hold_on_terminal = true};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t byte = pattern(100, 0);
	struct tr_rpc_bytes request = {&byte, 1};
	public_fixture_init_capacity(&f, 1);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	struct tr_channel *channel = atomic_load(&client_channel);
	CHECK(tr_reactor_call(tr_channel_reactor(channel),
	                      disable_client_routes_on_owner,
	                      channel) == TR_OK);
	CHECK(tr_client_destroy(f.client) == TR_AGAIN);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(result.status == TR_RPC_STATUS_CANCELLED);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	struct tr_rpc_v2_call_result snapshot = {0};
	CHECK(tr_rpc_v2_call_ref_result(result.held_call, &snapshot) == TR_OK);
	CHECK(snapshot.status == TR_RPC_STATUS_CANCELLED);
	CHECK(snapshot.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	CHECK(tr_rpc_v2_call_ref_cancel(result.held_call) == TR_ERR_STATE);
	atomic_store(&block_kind, 0);
	CHECK(tr_rpc_v2_call_ref_release(&result.held_call) == TR_OK);
	CHECK(tr_client_destroy(f.client) == TR_OK);
	f.client = NULL;
	fixture_destroy(&f);
}

struct public_call_ref_reader {
	struct tr_rpc_v2_call_ref ref;
	struct gate *done;
	enum tr_rpc_status status;
	enum tr_rpc_v2_delivery delivery;
};

static void *public_call_ref_read_and_release(void *arg)
{
	struct public_call_ref_reader *reader = arg;
	struct tr_rpc_v2_call_result snapshot = {
	        .status = TR_RPC_STATUS_INTERNAL,
	        .delivery = TR_RPC_V2_DELIVERY_SHUTDOWN};
	for (;;) {
		struct tr_rpc_v2_call_result before = snapshot;
		int ret = tr_rpc_v2_call_ref_result(reader->ref, &snapshot);
		if (ret == TR_AGAIN) {
			CHECK(!memcmp(&snapshot, &before, sizeof(snapshot)));
			continue;
		}
		CHECK(ret == TR_OK);
		break;
	}
	reader->status = snapshot.status;
	reader->delivery = snapshot.delivery;
	CHECK(tr_rpc_v2_call_ref_release(&reader->ref) == TR_OK);
	struct tr_rpc_v2_call_ref zero_ref = {0};
	CHECK(!memcmp(&reader->ref, &zero_ref, sizeof(zero_ref)));
	gate_wait(reader->done, 0, true);
	return NULL;
}

struct public_call_cancel_worker {
	struct tr_rpc_v2_call call;
	int status;
};

static void *public_call_cancel(void *arg)
{
	struct public_call_cancel_worker *worker = arg;
	worker->status = tr_rpc_v2_call_cancel(worker->call);
	return NULL;
}

static void test_public_call_ref_concurrent_snapshot_release(void)
{
	struct fixture f;
	struct gate terminal_gate, reader_done;
	struct public_result result = {.fixture = &f,
	                               .terminal_gate = &terminal_gate,
	                               .expected = {{1, 99}},
	                               .expected_count = 1};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t byte = pattern(99, 0);
	struct tr_rpc_bytes request = {&byte, 1};
	struct tr_rpc_v2_call_result pending = {
	        .status = TR_RPC_STATUS_INTERNAL,
	        .delivery = TR_RPC_V2_DELIVERY_SHUTDOWN};
	struct tr_rpc_v2_call_result pending_before = pending;
	struct public_call_ref_reader reader;
	struct public_call_cancel_worker cancel = {0};
	pthread_t reader_thread, cancel_thread;

	gate_init(&terminal_gate);
	gate_init(&reader_done);
	public_fixture_init_capacity(&f, 1);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, NULL, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[TR_RPC_RECORD_OPEN], 1);
	struct tr_rpc_v2_call_ref held = {0};
	CHECK(tr_rpc_v2_call_hold(call, &held) == TR_OK);
	CHECK(tr_rpc_v2_call_ref_result(held, &pending) == TR_AGAIN);
	CHECK(!memcmp(&pending, &pending_before, sizeof(pending)));
	reader = (struct public_call_ref_reader){.ref = held,
	                                         .done = &reader_done};
	held = (struct tr_rpc_v2_call_ref){0};
	cancel = (struct public_call_cancel_worker){.call = call};
	CHECK(pthread_create(&cancel_thread, NULL, public_call_cancel,
	                     &cancel) == 0);
	gate_wait(&terminal_gate, 1, false);
	atomic_store(&block_kind, 0);
	CHECK(pthread_create(&reader_thread, NULL,
	                     public_call_ref_read_and_release, &reader) == 0);
	gate_wait(&reader_done, 1, false);
	gate_release(&reader_done);
	CHECK(pthread_join(reader_thread, NULL) == 0);
	CHECK(reader.status == TR_RPC_STATUS_CANCELLED);
	CHECK(reader.delivery == TR_RPC_V2_DELIVERY_LIVE);
	gate_release(&terminal_gate);
	CHECK(pthread_join(cancel_thread, NULL) == 0);
	CHECK(cancel.status == TR_OK);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(result.status == TR_RPC_STATUS_CANCELLED);
	fixture_destroy(&f);
	CHECK(pthread_cond_destroy(&terminal_gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&terminal_gate.lock) == 0);
	CHECK(pthread_cond_destroy(&reader_done.cond) == 0);
	CHECK(pthread_mutex_destroy(&reader_done.lock) == 0);
}

struct public_early_request {
	struct fixture *fixture;
	struct public_result *result;
	struct tr_rpc_v2_call call;
	bool unary;
};

static int public_early_start_on_owner(void *arg)
{
	struct public_early_request *request = arg;
	struct public_context *context = malloc(sizeof(*context));
	CHECK(context);
	*context = (struct public_context){request->result, true};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(context);
	/* 同一 owner 事务：受理时的时钟成功，受理后首次 pump 的时钟
	 * 失败。终局在 start 返回前释放 arg，out 则放在独立存储中。 */
	fail_clock_after = 2;
	uint8_t byte = 1;
	struct tr_rpc_bytes body = {&byte, 1};
	int ret =
	        request->unary
	                ? tr_client_unary_call(request->fixture->client, 17, 1,
	                                       &body, NULL, &cb, &request->call)
	                : tr_client_call_start(request->fixture->client, 17, 2,
	                                       NULL, &cb, &request->call);
	CHECK(!fail_clock_after);
	CHECK(ret == TR_OK);
	CHECK(atomic_load(&request->result->terminal) == 1);
	CHECK(atomic_load(&request->result->arg_freed) == 1);
	CHECK(request->result->status == TR_RPC_STATUS_INTERNAL);
	CHECK(request->result->delivery == TR_RPC_V2_DELIVERY_LIVE);
	check_client_semantic(request->fixture->client, 1, 1,
	                      TR_RPC_STATUS_INTERNAL);
	return TR_OK;
}

static void test_public_early_terminal(bool unary)
{
	struct fixture f;
	struct public_result result = {.fixture = &f, .check_semantic = true};
	struct public_early_request request = {&f, &result, {{0}}, unary};
	public_fixture_init(&f);
	struct tr_reactor *owner =
	        tr_channel_reactor(atomic_load(&client_channel));
	CHECK(tr_reactor_call(owner, public_early_start_on_owner, &request) ==
	      TR_OK);
	CHECK(atomic_load(&clock_failures) == 1);
	CHECK(!atomic_load(&result.messages));
	CHECK(!atomic_load(&result.writable));
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

enum unary_end {
	UNARY_CANCEL,
	UNARY_CANCEL_AFTER_RESPONSE,
	UNARY_DESTROY,
	UNARY_DISCONNECT,
	UNARY_SEND_ERROR,
	UNARY_HALF_CLOSE_CLOCK
};

static void test_public_unary_end(enum unary_end ending)
{
	struct fixture f;
	struct public_result result = {
	        .fixture = &f,
	        .expected = {{127, 83}},
	        .expected_count =
	                ending == UNARY_CANCEL_AFTER_RESPONSE ? 1U : 0U};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	uint8_t source[127] = {0};
	struct tr_rpc_bytes body = {source, sizeof(source)};
	struct tr_rpc_call_options options = {0};
	unsigned kind = ending == UNARY_CANCEL_AFTER_RESPONSE
	                        ? TR_RPC_RECORD_HALF_CLOSE
	                        : TR_RPC_RECORD_MESSAGE;
	for (unsigned i = 0; i < sizeof(source); ++i)
		source[i] = pattern(83, i);
	public_fixture_init(&f);
	atomic_store(&block_kind, (int)kind);
	CHECK(tr_client_unary_call(f.client, 17, 1, &body, &options, &cb,
	                           &call) == TR_OK);
	wait_count(&attempts[kind], 1);
	struct tr_buffer_pool *pool = atomic_load(&public_send_pool);
	if (kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(pool);
		CHECK(tr_buffer_pool_free_count(pool) ==
		      pool->buffer_count - 1);
	}
	enum tr_rpc_status expected = TR_RPC_STATUS_CANCELLED;
	enum tr_rpc_v2_delivery delivery = TR_RPC_V2_DELIVERY_LIVE;
	if (ending == UNARY_CANCEL || ending == UNARY_CANCEL_AFTER_RESPONSE) {
		if (ending == UNARY_CANCEL_AFTER_RESPONSE) {
			wait_count(&result.messages, 1);
			CHECK(!atomic_load(&result.terminal));
		}
		CHECK(tr_rpc_v2_call_cancel(call) == TR_OK);
	} else if (ending == UNARY_DESTROY) {
		/* 隔离退出网络通知，固定在 join 后交付残留终局；首次销毁
		 * 再故障一次，重试不能重复 terminal 或遗留请求复制池。 */
		struct tr_channel *channel = atomic_load(&client_channel);
		CHECK(tr_reactor_call(tr_channel_reactor(channel),
		                      disable_client_routes_on_owner,
		                      channel) == TR_OK);
		atomic_store(&fail_client_destroy, true);
		CHECK(tr_client_destroy(f.client) == TR_ERR_SYS);
		delivery = TR_RPC_V2_DELIVERY_SHUTDOWN;
		CHECK(tr_client_destroy(f.client) == TR_OK);
		f.client = NULL;
	} else if (ending == UNARY_DISCONNECT) {
		struct tr_conn_handle connection = {0};
		CHECK(pthread_mutex_lock(&observation_lock) == 0);
		for (unsigned i = 0; i < observation_count; ++i)
			if (observations[i].channel ==
			    atomic_load(&client_channel))
				connection = observations[i].connection;
		CHECK(pthread_mutex_unlock(&observation_lock) == 0);
		CHECK(connection.reactor);
		CHECK(tr_reactor_call(connection.reactor,
		                      close_connection_on_owner,
		                      &connection) == TR_OK);
		expected = TR_RPC_STATUS_UNAVAILABLE;
	} else {
		if (ending == UNARY_SEND_ERROR) {
			atomic_store(&hard_fail_kind, TR_RPC_RECORD_MESSAGE);
			expected = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		} else {
			atomic_store(&fail_unary_half_close_clock, true);
			expected = TR_RPC_STATUS_INTERNAL;
		}
		atomic_store(&block_kind, 0);
	}
	wait_count(&result.terminal, 1);
	if (ending == UNARY_HALF_CLOSE_CLOCK) {
		CHECK(!atomic_load(&fail_unary_half_close_clock));
		CHECK(atomic_load(&clock_failures) == 1);
	}
	CHECK(result.status == expected && result.delivery == delivery);
	CHECK(atomic_load(&result.messages) == result.expected_count);
	CHECK(!atomic_load(&result.writable));
	CHECK(!atomic_load(&accepted[TR_RPC_RECORD_HALF_CLOSE]));
	if (f.client && pool && ending != UNARY_HALF_CLOSE_CLOCK)
		CHECK(tr_buffer_pool_free_count(pool) == pool->buffer_count);
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
}

static void test_public_unary_slots(void)
{
	struct fixture f;
	struct public_result results[CALLS + 1] = {0};
	struct public_context contexts[CALLS + 1];
	struct tr_rpc_v2_call calls[CALLS + 1];
	struct tr_rpc_v2_call sentinel = {{11, 22, 33}};
	struct tr_rpc_bytes empty = {NULL, 0};
	public_fixture_init(&f);
	atomic_store(&block_kind, TR_RPC_RECORD_OPEN);
	for (unsigned i = 0; i <= CALLS; ++i) {
		results[i].fixture = &f;
		contexts[i] = (struct public_context){&results[i], false};
		struct tr_rpc_v2_client_callbacks cb =
		        public_callbacks(&contexts[i]);
		calls[i] = sentinel;
		CHECK(tr_client_unary_call(f.client, 17, 1, &empty, NULL, &cb,
		                           &calls[i]) ==
		      (i < CALLS ? TR_OK : TR_AGAIN));
	}
	CHECK(!memcmp(&calls[CALLS], &sentinel, sizeof(sentinel)));
	check_client_semantic(f.client, CALLS, 0, TR_RPC_STATUS_OK);
	for (unsigned i = 0; i < CALLS; ++i) {
		CHECK(tr_rpc_v2_call_cancel(calls[i]) == TR_OK);
		wait_count(&results[i].terminal, 1);
		CHECK(results[i].status == TR_RPC_STATUS_CANCELLED);
	}
	check_client_semantic(f.client, CALLS, CALLS, TR_RPC_STATUS_CANCELLED);
	fixture_destroy(&f);
	CHECK(!atomic_load(&results[CALLS].terminal));
}

struct public_destroy_probe {
	struct fixture *fixture;
	struct tr_conn_handle connection;
	struct gate completed;
	int close_status;
	int status;
};

static void *public_destroy_thread(void *arg)
{
	struct public_destroy_probe *probe = arg;
	probe->close_status = tr_reactor_close(probe->connection);
	probe->status = tr_client_destroy(probe->fixture->client);
	CHECK(pthread_mutex_lock(&probe->completed.lock) == 0);
	++probe->completed.entered;
	CHECK(pthread_cond_broadcast(&probe->completed.cond) == 0);
	CHECK(pthread_mutex_unlock(&probe->completed.lock) == 0);
	return NULL;
}

/* 真实 V2 terminal 卡在 owner 回调内时，销毁到达 owner pthread_join
 * 仍不能越过回调；排队的断连不能覆盖已选定的超时终局。 */
static void test_public_terminal_destroy_barrier(void)
{
	struct fixture f;
	struct gate terminal_gate;
	struct public_result result = {.fixture = &f,
	                               .terminal_gate = &terminal_gate};
	struct public_context context = {&result, false};
	struct tr_rpc_v2_client_callbacks cb = public_callbacks(&context);
	struct tr_rpc_v2_call call;
	struct tr_rpc_call_options options = {.timeout_ms = 1000};
	uint8_t request_byte = pattern(91, 0);
	struct tr_rpc_bytes request = {&request_byte, 1};
	struct public_destroy_probe probe = {.fixture = &f};
	pthread_t destroy_thread;

	gate_init(&terminal_gate);
	gate_init(&probe.completed);
	fixture_init(&f);
	f.public_client = true;
	f.client_config.limits.max_message_bytes = BODY;
	atomic_store(&f.block_work, true);
	fixture_create(&f);
	fixture_connect(&f);
	CHECK(tr_client_unary_call(f.client, 17, 1, &request, &options, &cb,
	                           &call) == TR_OK);
	gate_wait(&f.gate, 1, false);
	gate_wait(&terminal_gate, 1, false);
	CHECK(result.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(!atomic_load(&result.terminal));
	gate_release(&f.gate);
	wait_count(&f.messages, 1);
	struct tr_buffer_pool *pool = atomic_load(&public_send_pool);
	CHECK(pool && tr_buffer_pool_free_count(pool) == pool->buffer_count);
	struct tr_conn_handle connection = {0};
	CHECK(pthread_mutex_lock(&observation_lock) == 0);
	for (unsigned i = 0; i < observation_count; ++i)
		if (observations[i].channel == atomic_load(&client_channel))
			connection = observations[i].connection;
	CHECK(pthread_mutex_unlock(&observation_lock) == 0);
	CHECK(connection.reactor);
	probe.connection = connection;
	atomic_store(&fail_client_destroy, true);
	atomic_store(&observe_destroy_stop, true);
	CHECK(pthread_create(&destroy_thread, NULL, public_destroy_thread,
	                     &probe) == 0);
	wait_count(&destroy_join_busy, 1);
	CHECK(pthread_mutex_lock(&probe.completed.lock) == 0);
	CHECK(!probe.completed.entered);
	CHECK(pthread_mutex_unlock(&probe.completed.lock) == 0);
	CHECK(!atomic_load(&result.terminal));
	gate_release(&terminal_gate);
	gate_wait(&probe.completed, 1, false);
	CHECK(pthread_join(destroy_thread, NULL) == 0);
	atomic_store(&observe_destroy_stop, false);
	CHECK(probe.close_status == TR_OK);
	CHECK(probe.status == TR_ERR_SYS);
	CHECK(atomic_load(&destroy_failures) == 1);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(result.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(result.delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(tr_buffer_pool_free_count(pool) == pool->buffer_count);
	CHECK(atomic_load(&client_control_down_events) == 1);
	CHECK(tr_client_destroy(f.client) == TR_OK);
	f.client = NULL;
	fixture_destroy(&f);
	CHECK(atomic_load(&result.terminal) == 1);
	CHECK(pthread_cond_destroy(&terminal_gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&terminal_gate.lock) == 0);
	CHECK(pthread_cond_destroy(&probe.completed.cond) == 0);
	CHECK(pthread_mutex_destroy(&probe.completed.lock) == 0);
}

int main(void)
{
	CHECK(setvbuf(stdout, NULL, _IOLBF, 0) == 0);
	test_method_budget_release_retry();
	puts("test_client_record: method budget release retry passed");
	test_semantic_overflow();
	puts("test_client_record: semantic overflow passed");
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
	test_public_copy_and_pool();
	puts("test_client_record: public copy and pool passed");
	test_public_early_terminal(false);
	test_public_early_terminal(true);
	puts("test_client_record: public early terminal passed");
	test_public_unary_progress(0, TR_RPC_STATUS_OK);
	test_public_unary_progress(BODY, TR_RPC_STATUS_OK);
	test_public_unary_progress(257, TR_RPC_STATUS_PERMISSION_DENIED);
	puts("test_client_record: public unary progress passed");
	test_public_unary_callback_cancel();
	test_public_unary_admission();
	test_public_start_clock_failure();
	test_public_failed_earlier_deadline();
	test_public_start_timer_failure();
	for (unsigned mode = 1; mode <= 4; ++mode)
		test_public_response_deadline_boundary(mode);
	test_public_saturated_deadline();
	puts("test_client_record: public unary admission passed");
	for (unsigned mode = 0; mode < 6; ++mode)
		test_public_unary_terminal(mode);
	test_public_call_ref_terminal_snapshot();
	test_public_call_ref_pending_cancel();
	test_public_call_ref_stale_token_reuse();
	test_public_call_ref_capacity_and_drain();
	test_public_call_ref_destroy_retry();
	test_public_call_ref_shutdown_callback_hold();
	test_public_call_ref_concurrent_snapshot_release();
	puts("test_client_record: public unary terminal passed");
	for (enum unary_end ending = UNARY_CANCEL;
	     ending <= UNARY_HALF_CLOSE_CLOCK; ++ending)
		test_public_unary_end(ending);
	test_public_unary_slots();
	test_public_terminal_destroy_barrier();
	puts("test_client_record: unary convergence passed");
	puts("test_client_record: 16 groups passed");
	return 0;
}
