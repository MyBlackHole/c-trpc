#define _POSIX_C_SOURCE 200809L
#include "tr/rpc_v2.h"
#include "tr/status.h"
#include "../src/rpc/rpc_record_stream.h"
#include "../src/transport/protocol/wire.h"
#include "../src/endian.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#define CALLS 16U
#define METHODS 16U
#define WAIT_SECONDS 15

/* 故障只发生在真实 OPEN 已进入绑定接收后创建工作事件的 calloc。
 * 记录绑定自身的构造、其他线程及非 OPEN 接收都调用原实现。 */
static _Atomic bool fail_open_job;
static _Atomic unsigned failed_open_jobs;
static _Thread_local bool receiving_open;
static _Atomic bool hold_refusal;
static _Atomic unsigned refusal_blocked;

ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);
ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	if (atomic_load(&hold_refusal) && message->msg_iovlen &&
	    message->msg_iov[0].iov_len >= TR_WIRE_HEADER_SIZE &&
	    tr_get_le16((const uint8_t *)message->msg_iov[0].iov_base +
	                TR_WIRE_OFF_TYPE) == TR_FRAME_STREAM_REFUSED) {
		/* 只阻止真实拒绝帧写出，保留实际流槽、预留项和收尾路径。 */
		atomic_fetch_add(&refusal_blocked, 1);
		errno = EAGAIN;
		return -1;
	}
	return __real_sendmsg(fd, message, flags);
}

void *__real_calloc(size_t count, size_t size);
void *__wrap_calloc(size_t count, size_t size)
{
	if (receiving_open && atomic_exchange(&fail_open_job, false)) {
		atomic_fetch_add(&failed_open_jobs, 1);
		return NULL;
	}
	return __real_calloc(count, size);
}

int __real_tr_rpc_record_stream_receive_on_owner(struct tr_rpc_record_stream *,
                                                 struct tr_stream_handle,
                                                 const uint8_t *, size_t);
int __wrap_tr_rpc_record_stream_receive_on_owner(
        struct tr_rpc_record_stream *call, struct tr_stream_handle stream,
        const uint8_t *data, size_t size)
{
	const struct tr_rpc_record_limits limits = {
	        0, 0, TR_RPC_METADATA_MAX_BYTES, TR_RPC_RECORD_ACCEPT_NONE};
	struct tr_rpc_record_header header;
	size_t total;
	bool prior = receiving_open;

	if (atomic_load(&fail_open_job) &&
	    tr_rpc_record_decode_header(data, size, &limits, &header, &total) ==
	            TR_OK &&
	    header.kind == TR_RPC_RECORD_OPEN)
		receiving_open = true;
	int ret = __real_tr_rpc_record_stream_receive_on_owner(call, stream,
	                                                       data, size);
	receiving_open = prior;
	return ret;
}

enum business_mode {
	ECHO,
	BAD_BATCH,
	FINISHED_BATCH,
	BLOCKED,
	NO_JOB,
	DOUBLE_INITIAL,
	INITIAL_CLOSED,
	TRAILING_OPEN,
	DUPLICATE_KEY,
	BUSINESS_ERROR,
	WINDOW_REPLY,
	ASYNC_SYNC_WRITE,
	ASYNC_ABANDON,
	ASYNC_WORK_ERROR,
	ASYNC_SEALED,
	ASYNC_AUTH_LATE_ALLOW
};
struct fixture;
struct method_context {
	struct fixture *fixture;
	enum business_mode mode;
};
struct call {
	struct fixture *fixture;
	struct tr_rpc_v2_call handle;
	struct tr_rpc_v2_call_id identity;
	unsigned index, opened, messages, halves, terminals;
	unsigned writable, received, client_terminals;
	unsigned initial, trailing;
	unsigned producer_ready;
	struct tr_rpc_v2_producer *producer;
	enum business_mode mode;
	enum tr_rpc_status status, client_status;
	enum tr_rpc_v2_delivery delivery;
	bool identified, entered, release, exited, cancelled;
	bool hold_in_terminal;
	struct tr_rpc_v2_call_ref held_ref;
	uint8_t response[32];
	size_t response_size;
};
struct fixture {
	struct tr_server *server;
	struct tr_client *clients[2];
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct method_context methods[METHODS];
	struct call calls[CALLS];
	unsigned auth_entered, auth_cancelled, auth_work_calls;
	bool auth_release;
};

static struct timespec wait_deadline(void)
{
	struct timespec result;
	assert(clock_gettime(CLOCK_REALTIME, &result) == 0);
	result.tv_sec += WAIT_SECONDS;
	return result;
}

static void wait_counter(struct fixture *f, const unsigned *counter,
                         unsigned value)
{
	struct timespec end = wait_deadline();
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (*counter < value)
		assert(pthread_cond_timedwait(&f->changed, &f->lock, &end) ==
		       0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void wait_entered(struct call *call)
{
	struct fixture *f = call->fixture;
	struct timespec end = wait_deadline();
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (!call->entered)
		assert(pthread_cond_timedwait(&f->changed, &f->lock, &end) ==
		       0);
	assert(!call->terminals);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void release_work(struct call *call)
{
	struct fixture *f = call->fixture;
	struct timespec end = wait_deadline();
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(call->entered && !call->exited && !call->terminals);
	while (!call->cancelled)
		assert(pthread_cond_timedwait(&f->changed, &f->lock, &end) ==
		       0);
	assert(!call->exited && !call->terminals);
	call->release = true;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

/* 调用者持有 fixture.lock。Client 本地终局不能证明远端已收到取消；
 * 公共令牌没有条件变量通知，在固定总期限内等待并由 worker 实际查询。 */
static void wait_worker_cancel(struct fixture *f,
                               const struct tr_rpc_v2_cancel *cancel)
{
	struct timespec end = wait_deadline();
	for (;;) {
		int requested = tr_rpc_v2_cancel_requested(cancel);
		assert(requested == 0 || requested == 1);
		if (requested)
			return;
		struct timespec wake;
		assert(clock_gettime(CLOCK_REALTIME, &wake) == 0);
		assert(wake.tv_sec < end.tv_sec ||
		       (wake.tv_sec == end.tv_sec &&
		        wake.tv_nsec < end.tv_nsec));
		wake.tv_nsec += 1000000L;
		if (wake.tv_nsec >= 1000000000L) {
			++wake.tv_sec;
			wake.tv_nsec -= 1000000000L;
		}
		if (wake.tv_sec > end.tv_sec ||
		    (wake.tv_sec == end.tv_sec && wake.tv_nsec > end.tv_nsec))
			wake = end;
		int ret = pthread_cond_timedwait(&f->changed, &f->lock, &wake);
		assert(ret == 0 || ret == ETIMEDOUT);
	}
}

static void wait_auth_state(struct fixture *f, unsigned *state,
                            unsigned expected)
{
	struct timespec end = wait_deadline();
	assert(pthread_mutex_lock(&f->lock) == 0);
	while (*state < expected)
		assert(pthread_cond_timedwait(&f->changed, &f->lock, &end) ==
		       0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static bool same_id(struct tr_rpc_v2_call_id a, struct tr_rpc_v2_call_id b)
{
	return memcmp(&a, &b, sizeof(a)) == 0;
}

/* 调用者持有 fixture.lock；身份只按公开允许的完整值比较。 */
static struct call *find_call(struct fixture *f, struct tr_rpc_v2_call_id id)
{
	for (unsigned i = 0; i < CALLS; ++i)
		if (f->calls[i].identified && same_id(f->calls[i].identity, id))
			return &f->calls[i];
	return NULL;
}

static void check_lifecycle_reentry(struct fixture *f)
{
	struct tr_server *extra = NULL;
	struct tr_rpc_semantic_stats before, stats;
	assert(tr_server_start(f->server) == TR_ERR_STATE);
	assert(tr_server_drain(f->server, 0) == TR_ERR_STATE);
	assert(tr_server_destroy(f->server) == TR_ERR_STATE);
	assert(tr_server_create(NULL, NULL, 0, &extra) == TR_ERR_STATE);
	assert(!extra);
	assert(tr_client_destroy(f->clients[0]) == TR_ERR_STATE);
	memset(&before, 0xa5, sizeof(before));
	memcpy(&stats, &before, sizeof(stats));
	assert(tr_server_get_rpc_semantic_stats(f->server, &stats) ==
	       TR_ERR_STATE);
	assert(!memcmp(&stats, &before, sizeof(stats)));
	assert(tr_client_get_rpc_semantic_stats(f->clients[0], &stats) ==
	       TR_ERR_STATE);
	assert(!memcmp(&stats, &before, sizeof(stats)));
}

/* 只在外部控制线程的确定性屏障调用；已观察的业务终局独立于库计数。 */
static void check_server_totals(struct fixture *f, uint64_t started)
{
	struct tr_rpc_semantic_stats expected = {0}, actual;
	expected.calls_started = started;
	assert(pthread_mutex_lock(&f->lock) == 0);
	for (unsigned i = 0; i < CALLS; ++i) {
		const struct call *call = &f->calls[i];
		if (!call->terminals)
			continue;
		assert(call->terminals == 1);
		assert((unsigned)call->status < TR_RPC_STATUS_COUNT);
		++expected.calls_finished;
		++expected.final_status[call->status];
	}
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(expected.calls_started >= expected.calls_finished);
	expected.calls_inflight = started - expected.calls_finished;
	assert(tr_server_get_rpc_semantic_stats(f->server, &actual) == TR_OK);
	assert(actual.calls_started == expected.calls_started);
	assert(actual.calls_finished == expected.calls_finished);
	assert(actual.calls_inflight == expected.calls_inflight);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		assert(actual.final_status[i] == expected.final_status[i]);
}

static void set_metadata(struct tr_rpc_v2_reply *reply,
                         enum tr_rpc_v2_metadata_phase phase)
{
	char key[] = "bin";
	uint8_t value[] = {0, 255, (uint8_t)phase};
	struct tr_rpc_metadata entry = {key, value, sizeof(value)};
	assert(tr_rpc_v2_reply_set_metadata(reply, phase, &entry, 1) == TR_OK);
	memset(key, 'q', sizeof(key));
	memset(value, 0x55, sizeof(value));
	memset(&entry, 0, sizeof(entry));
}

static enum tr_rpc_status async_open(struct call *call,
                                     struct tr_rpc_v2_reply *reply)
{
	struct fixture *f = call->fixture;
	struct tr_rpc_v2_producer *producer = NULL;
	const struct tr_rpc_bytes empty = {0};
	assert(tr_rpc_v2_reply_acquire_producer(reply, NULL) == TR_ERR_INVALID);
	assert(tr_rpc_v2_reply_acquire_producer(reply, &producer) == TR_OK);
	assert(producer);
	assert(tr_rpc_v2_producer_wait_writable(producer, 0) == TR_ERR_STATE);
	if (call->mode == ASYNC_SEALED) {
		struct tr_rpc_v2_producer *unchanged = producer;
		const struct tr_rpc_metadata duplicate[] = {{"same", NULL, 0},
		                                            {"same", NULL, 0}};
		assert(tr_rpc_v2_reply_acquire_producer(reply, &unchanged) ==
		       TR_ERR_STATE);
		assert(unchanged == producer);
		assert(tr_rpc_v2_producer_finish(producer, TR_RPC_STATUS_OK,
		                                 duplicate,
		                                 2) == TR_ERR_BAD_FLAGS);
		/* acquire 和 producer 提交失败均不锁存工作错误，也不提前
		 * 消费结束阶段；创建 work 未返回时能精确区分 sealed/closed。 */
		assert(tr_rpc_v2_producer_finish(producer, TR_RPC_STATUS_OK,
		                                 NULL, 0) == TR_OK);
		assert(tr_rpc_v2_producer_is_closed(producer) == 0);
		assert(tr_rpc_v2_producer_send(producer, &empty) ==
		       TR_ERR_STATE);
		assert(tr_rpc_v2_producer_send_initial_metadata(
		               producer, NULL, 0) == TR_ERR_STATE);
		assert(tr_rpc_v2_producer_finish(producer, TR_RPC_STATUS_OK,
		                                 NULL, 0) == TR_ERR_STATE);
		struct timespec end = wait_deadline();
		assert(pthread_mutex_lock(&f->lock) == 0);
		call->producer = producer;
		++call->producer_ready;
		call->entered = true;
		assert(pthread_cond_broadcast(&f->changed) == 0);
		while (!call->release)
			assert(pthread_cond_timedwait(&f->changed, &f->lock,
			                              &end) == 0);
		call->exited = true;
		assert(pthread_mutex_unlock(&f->lock) == 0);
		return TR_RPC_STATUS_OK;
	}
	if (call->mode == ASYNC_SYNC_WRITE) {
		assert(tr_rpc_v2_reply_append(reply, &empty) == TR_ERR_STATE);
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_INITIAL,
		                                    NULL, 0) == TR_ERR_STATE);
		assert(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) ==
		       TR_ERR_STATE);
	} else if (call->mode == ASYNC_WORK_ERROR) {
		uint8_t source[] = {0, 255, 17, 0};
		struct tr_rpc_bytes body = {source, sizeof(source)};
		assert(tr_rpc_v2_producer_send(producer, &body) == TR_OK);
		memset(source, 0xcc, sizeof(source));
	}
	assert(pthread_mutex_lock(&f->lock) == 0);
	call->producer = producer;
	++call->producer_ready;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return call->mode == ASYNC_WORK_ERROR ? TR_RPC_STATUS_INVALID_ARGUMENT
	                                      : TR_RPC_STATUS_OK;
}

static enum tr_rpc_status work(struct tr_rpc_v2_call_id id,
                               const struct tr_rpc_v2_request *request,
                               const struct tr_rpc_v2_cancel *cancel,
                               struct tr_rpc_v2_reply *reply, void *arg)
{
	struct method_context *method = arg;
	struct fixture *f = method->fixture;
	struct call *call;
	uint8_t copied[32];
	if (method->mode == ASYNC_AUTH_LATE_ALLOW) {
		assert(pthread_mutex_lock(&f->lock) == 0);
		++f->auth_work_calls;
		assert(pthread_cond_broadcast(&f->changed) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
		return TR_RPC_STATUS_OK;
	}

	assert(request->peer_identity.authentication == TR_PEER_IDENTITY_NONE);
	assert(request->peer_identity.uri_san == NULL);
	assert(request->peer_identity.uri_san_len == 0U);
	check_lifecycle_reentry(f);
	assert(tr_rpc_v2_cancel_requested(cancel) >= 0);
	assert(pthread_mutex_lock(&f->lock) == 0);
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		struct tr_rpc_bytes tag;
		assert(!request->body.data && !request->body.len);
		assert(tr_rpc_v2_metadata_count(&request->metadata) == 1);
		assert(tr_rpc_v2_metadata_find(&request->metadata, "tag",
		                               &tag) == TR_OK);
		assert(tag.len == 1 && tag.data[0] < CALLS);
		call = &f->calls[tag.data[0]];
		assert(!call->identified && !find_call(f, id));
		call->identity = id;
		call->identified = true;
		++call->opened;
		assert(pthread_cond_broadcast(&f->changed) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
		if (method->mode == INITIAL_CLOSED) {
			assert(tr_rpc_v2_reply_set_metadata(
			               reply, TR_RPC_V2_METADATA_INITIAL, NULL,
			               0) == TR_OK);
		} else if (method->mode == TRAILING_OPEN) {
			/* OPEN
			 * 普通批不选择终局，已设置的尾部不能悄悄跨事件保存。 */
			set_metadata(reply, TR_RPC_V2_METADATA_TRAILING);
		} else if (method->mode >= ASYNC_SYNC_WRITE) {
			return async_open(call, reply);
		}
		return TR_RPC_STATUS_OK;
	}
	call = find_call(f, id);
	assert(call && call->opened == 1 && !call->terminals);
	assert(tr_rpc_v2_metadata_count(&request->metadata) == 0);
	if (request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		++call->halves;
		assert(pthread_cond_broadcast(&f->changed) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
		return TR_RPC_STATUS_OK;
	}
	assert(request->kind == TR_RPC_V2_REQUEST_MESSAGE);
	++call->messages;
	if (method->mode == BLOCKED) {
		call->entered = true;
		assert(pthread_cond_broadcast(&f->changed) == 0);
		wait_worker_cancel(f, cancel);
		call->cancelled = true;
		assert(!call->release && !call->terminals);
		assert(pthread_cond_broadcast(&f->changed) == 0);
		while (!call->release)
			assert(pthread_cond_wait(&f->changed, &f->lock) == 0);
		assert(tr_rpc_v2_cancel_requested(cancel) == 1);
		assert(!call->terminals);
		call->exited = true;
		assert(pthread_cond_broadcast(&f->changed) == 0);
	}
	assert(pthread_mutex_unlock(&f->lock) == 0);
	if (method->mode == WINDOW_REPLY) {
		uint8_t large[640] = {1};
		const struct tr_rpc_bytes body = {large, sizeof(large)};
		assert(tr_rpc_v2_reply_append(reply, &body) == TR_OK);
		memset(large, 0xcc, sizeof(large));
		return TR_RPC_STATUS_OK;
	}
	assert(request->body.len <= sizeof(copied));
	memcpy(copied, request->body.data, request->body.len);
	struct tr_rpc_bytes response = {copied, request->body.len};
	assert(tr_rpc_v2_reply_append(reply, &response) == TR_OK);
	/* append 接管的是副本；业务仍拥有原存储，可以立即覆盖。 */
	memset(copied, 0xcc, sizeof(copied));
	if (method->mode == BAD_BATCH) {
		set_metadata(reply, TR_RPC_V2_METADATA_INITIAL);
		set_metadata(reply, TR_RPC_V2_METADATA_TRAILING);
		response.len = 17;
		assert(tr_rpc_v2_reply_append(reply, &response) ==
		       TR_ERR_BAD_LENGTH);
	} else if (method->mode == FINISHED_BATCH) {
		set_metadata(reply, TR_RPC_V2_METADATA_INITIAL);
		assert(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) ==
		       TR_OK);
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_TRAILING,
		                                    NULL, 0) == TR_ERR_STATE);
		assert(tr_rpc_v2_reply_append(reply, &response) ==
		       TR_ERR_STATE);
	} else if (method->mode == DOUBLE_INITIAL) {
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_INITIAL,
		                                    NULL, 0) == TR_OK);
		/* 空列表也消费本批阶段；忽略第二次错误不能发布已有正文。 */
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_INITIAL,
		                                    NULL, 0) == TR_ERR_STATE);
	} else if (method->mode == INITIAL_CLOSED) {
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_INITIAL,
		                                    NULL, 0) == TR_ERR_STATE);
	} else if (method->mode == DUPLICATE_KEY) {
		const struct tr_rpc_metadata entries[] = {{"same", NULL, 0},
		                                          {"same", NULL, 0}};
		assert(tr_rpc_v2_reply_set_metadata(
		               reply, TR_RPC_V2_METADATA_INITIAL, entries, 2) ==
		       TR_ERR_BAD_FLAGS);
		assert(tr_rpc_v2_reply_set_metadata(reply,
		                                    TR_RPC_V2_METADATA_TRAILING,
		                                    NULL, 0) == TR_ERR_STATE);
		/* 首个参数错误不能被后续业务错误改写。 */
		return TR_RPC_STATUS_UNAVAILABLE;
	} else if (method->mode == BUSINESS_ERROR) {
		set_metadata(reply, TR_RPC_V2_METADATA_INITIAL);
		set_metadata(reply, TR_RPC_V2_METADATA_TRAILING);
		assert(tr_rpc_v2_reply_finish(
		               reply, TR_RPC_STATUS_INVALID_ARGUMENT) == TR_OK);
	} else if (method->mode == BLOCKED) {
		assert(tr_rpc_v2_cancel_requested(cancel) == 1);
		set_metadata(reply, TR_RPC_V2_METADATA_INITIAL);
		set_metadata(reply, TR_RPC_V2_METADATA_TRAILING);
	}
	return TR_RPC_STATUS_OK;
}

static struct tr_rpc_v2_authorization_result
authorize_late_allow(struct tr_rpc_v2_call_id id,
                     const struct tr_rpc_v2_request *request,
                     const struct tr_rpc_v2_cancel *cancel, void *arg)
{
	struct method_context *method = arg;
	struct fixture *f = method->fixture;
	struct tr_rpc_bytes tag;
	struct call *call;
	assert(method->mode == ASYNC_AUTH_LATE_ALLOW);
	assert(tr_rpc_v2_metadata_find(&request->metadata, "tag", &tag) ==
	       TR_OK);
	assert(tag.len == 1 && tag.data[0] < CALLS);
	assert(pthread_mutex_lock(&f->lock) == 0);
	call = &f->calls[tag.data[0]];
	assert(call->mode == ASYNC_AUTH_LATE_ALLOW && !call->identified);
	call->identity = id;
	call->identified = true;
	++f->auth_entered;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	wait_worker_cancel(f, cancel);
	++f->auth_cancelled;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	while (!f->auth_release)
		assert(pthread_cond_wait(&f->changed, &f->lock) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
	return (struct tr_rpc_v2_authorization_result){
	        TR_RPC_V2_AUTHORIZATION_ALLOW, TR_RPC_STATUS_OK};
}

static void server_terminal(struct tr_rpc_v2_call_id id,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct method_context *method = arg;
	struct fixture *f = method->fixture;
	struct call *call;

	check_lifecycle_reentry(f);
	assert(pthread_mutex_lock(&f->lock) == 0);
	call = find_call(f, id);
	if (method->mode == NO_JOB) {
		assert(!call);
		call = &f->calls[4];
		assert(!call->identified && !call->opened);
		call->identity = id;
		call->identified = true;
	}
	assert(call && !call->terminals);
	assert(!call->entered || call->exited);
	if (call->producer) {
		/* 终局已解除库内运行时依赖；这里只借用应用仍持有的引用。 */
		assert(tr_rpc_v2_producer_is_closed(call->producer) == 1);
		assert(tr_rpc_v2_producer_wait_writable(call->producer, 0) ==
		       TR_ERR_STATE);
	}
	call->status = status;
	call->delivery = delivery;
	++call->terminals;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void client_message(struct tr_rpc_v2_call handle,
                           const struct tr_rpc_bytes *body, void *arg)
{
	struct call *call = arg;
	struct fixture *f = call->fixture;
	(void)handle;
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(!call->client_terminals && body->len <= sizeof(call->response));
	assert(call->initial == 1 && !call->trailing);
	memcpy(call->response, body->data, body->len);
	call->response_size = body->len;
	++call->received;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void client_metadata(struct tr_rpc_v2_call handle,
                            enum tr_rpc_v2_metadata_phase phase,
                            const struct tr_rpc_v2_metadata_view *view,
                            void *arg)
{
	struct call *call = arg;
	struct fixture *f = call->fixture;
	(void)handle;
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(!call->client_terminals);
	if (call->mode == BUSINESS_ERROR) {
		struct tr_rpc_bytes value;
		const uint8_t expected[] = {0, 255, (uint8_t)phase};
		assert(tr_rpc_v2_metadata_count(view) == 1);
		assert(tr_rpc_v2_metadata_find(view, "bin", &value) == TR_OK);
		assert(value.len == sizeof(expected));
		assert(!memcmp(value.data, expected, sizeof(expected)));
	} else {
		assert(tr_rpc_v2_metadata_count(view) == 0);
	}
	if (phase == TR_RPC_V2_METADATA_INITIAL) {
		assert(!call->initial && !call->received && !call->trailing);
		++call->initial;
	} else {
		assert(phase == TR_RPC_V2_METADATA_TRAILING && !call->trailing);
		++call->trailing;
	}
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void client_writable(struct tr_rpc_v2_call handle, void *arg)
{
	struct call *call = arg;
	struct fixture *f = call->fixture;
	(void)handle;
	assert(pthread_mutex_lock(&f->lock) == 0);
	++call->writable;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void client_terminal(struct tr_rpc_v2_call handle,
                            enum tr_rpc_status status,
                            enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct call *call = arg;
	struct fixture *f = call->fixture;
	(void)delivery;
	if (call->hold_in_terminal)
		assert(tr_rpc_v2_call_hold(handle, &call->held_ref) == TR_OK);
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(!call->client_terminals);
	call->client_status = status;
	++call->client_terminals;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void fixture_init_options(struct fixture *f, uint32_t streams,
                                 bool asymmetric, bool late_authorize)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_v2_method methods[METHODS];
	struct tr_rpc_method_desc descriptions[METHODS];
	uint16_t port;

	memset(f, 0, sizeof(*f));
	assert(pthread_mutex_init(&f->lock, NULL) == 0);
	assert(pthread_cond_init(&f->changed, NULL) == 0);
	for (unsigned i = 0; i < CALLS; ++i) {
		f->calls[i].fixture = f;
		f->calls[i].index = i;
	}
	tr_server_config_init(&server_config);
	server_config.max_peers = 2;
	server_config.keepalive_interval_ms = 0;
	server_config.limits.max_methods = METHODS;
	server_config.limits.max_calls = 8;
	server_config.limits.max_streams = streams;
	server_config.limits.max_frame_payload_bytes = 64;
	server_config.limits.max_message_bytes = asymmetric ? 1024 : 64;
	server_config.limits.initial_window_bytes = asymmetric ? 2048 : 256;
	server_config.limits.window_update_threshold_bytes = 1;
	for (unsigned i = 0; i < METHODS; ++i) {
		f->methods[i] =
		        (struct method_context){f, (enum business_mode)i};
		descriptions[i] =
		        (struct tr_rpc_method_desc){7,
		                                    i + 1,
		                                    TR_RPC_MANY,
		                                    TR_RPC_MANY,
		                                    TR_RPC_CODEC_RAW,
		                                    TR_RPC_CODEC_RAW,
		                                    TR_RPC_LANE_CONTROL,
		                                    16,
		                                    16,
		                                    0,
		                                    0};
		methods[i] = (struct tr_rpc_v2_method){
		        .descriptor = descriptions[i],
		        .work = work,
		        .terminal = server_terminal,
		        .arg = &f->methods[i],
		};
		if (late_authorize && i == ASYNC_AUTH_LATE_ALLOW)
			methods[i].authorize = authorize_late_allow;
		if (asymmetric && (i == WINDOW_REPLY || i == ASYNC_ABANDON))
			methods[i].descriptor.max_response_bytes = 768;
	}
	assert(tr_server_create(&server_config, methods, METHODS, &f->server) ==
	       TR_OK);
	/* 整个原数组已不再借用，包括 terminal 和 arg 指针。 */
	memset(methods, 0, sizeof(methods));
	assert(tr_server_listen(f->server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(f->server) == TR_OK);
	tr_client_config_init(&client_config);
	client_config.limits = server_config.limits;
	client_config.limits.max_streams = 8;
	/* 双方配置分别合法，方法描述未协商。客户端内部窗口为 560，
	 * 消息能力为 1072；服务端 640+48 的响应只越过对端窗口。 */
	if (asymmetric)
		client_config.limits.initial_window_bytes = 128;
	client_config.keepalive_interval_ms = 0;
	for (unsigned i = 0; i < 2; ++i) {
		assert(tr_client_create(&client_config, descriptions, METHODS,
		                        &f->clients[i]) == TR_OK);
		assert(tr_client_connect(f->clients[i], "127.0.0.1", port) ==
		       TR_OK);
		assert(tr_client_wait_ready(f->clients[i],
		                            WAIT_SECONDS * 1000) == TR_OK);
	}
}

static void fixture_init_capacity(struct fixture *f, uint32_t streams)
{
	fixture_init_options(f, streams, false, false);
}

static void fixture_init(struct fixture *f)
{
	fixture_init_capacity(f, 8);
}

static void call_start(struct fixture *f, unsigned index, unsigned client,
                       enum business_mode method)
{
	struct call *call = &f->calls[index];
	uint8_t tag = (uint8_t)index;
	const struct tr_rpc_metadata metadata = {"tag", &tag, sizeof(tag)};
	const struct tr_rpc_call_options options = {.metadata = &metadata,
	                                            .metadata_count = 1};
	const struct tr_rpc_v2_client_callbacks callbacks = {
	        .message = client_message,
	        .metadata = client_metadata,
	        .writable = client_writable,
	        .terminal = client_terminal,
	        .arg = call};
	call->mode = method;
	assert(tr_client_call_start(f->clients[client], 7, method + 1,
	                            method == NO_JOB ? NULL : &options,
	                            &callbacks, &call->handle) == TR_OK);
}

static void send_or_half_close(struct call *call,
                               const struct tr_rpc_bytes *body)
{
	struct fixture *f = call->fixture;
	struct timespec end = wait_deadline();
	for (;;) {
		assert(pthread_mutex_lock(&f->lock) == 0);
		unsigned before = call->writable;
		assert(!call->client_terminals);
		assert(pthread_mutex_unlock(&f->lock) == 0);
		int ret = body ? tr_rpc_v2_call_send(call->handle, body)
		               : tr_rpc_v2_call_half_close(call->handle);
		if (ret == TR_OK)
			return;
		assert(ret == TR_AGAIN);
		assert(pthread_mutex_lock(&f->lock) == 0);
		while (call->writable == before && !call->client_terminals)
			assert(pthread_cond_timedwait(&f->changed, &f->lock,
			                              &end) == 0);
		assert(pthread_mutex_unlock(&f->lock) == 0);
	}
}

static void check_result(struct call *call, enum tr_rpc_status status,
                         unsigned received)
{
	struct fixture *f = call->fixture;
	unsigned expected_trailing = call->mode == BLOCKED ? 0U : 1U;
	wait_counter(f, &call->terminals, 1);
	wait_counter(f, &call->client_terminals, 1);
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(call->terminals == 1 && call->client_terminals == 1);
	assert(call->status == status && call->client_status == status);
	assert(call->delivery == TR_RPC_V2_DELIVERY_LIVE);
	assert(call->received == received);
	assert(call->initial ==
	       (unsigned)(received || call->mode == INITIAL_CLOSED ||
	                  call->mode == BUSINESS_ERROR));
	/* NO_JOB 和 ASYNC_ABANDON 没有待清理的工作借用；Endpoint 已收到
	 * 完整终局通知，正常连接以空错误 FINAL 交付拒绝或本地取消。
	 * BLOCKED 的工作仍未退出，网络先由 RESET 结束，不交付尾部。 */
	if (call->trailing != expected_trailing)
		fprintf(stderr,
		        "test_rpc_v2_server: index=%u mode=%u status=%u "
		        "initial=%u "
		        "trailing=%u expected_trailing=%u received=%u\n",
		        call->index, (unsigned)call->mode,
		        (unsigned)call->status, call->initial, call->trailing,
		        expected_trailing, call->received);
	assert(call->trailing == expected_trailing);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void retry_destroy(struct fixture *f, bool server)
{
	struct timespec end = wait_deadline(), pause = {0, 1000000};
	for (;;) {
		int ret = server ? tr_server_destroy(f->server)
		                 : tr_client_destroy(f->clients[0]);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);
		struct timespec now;
		assert(clock_gettime(CLOCK_REALTIME, &now) == 0);
		assert(now.tv_sec < end.tv_sec ||
		       (now.tv_sec == end.tv_sec && now.tv_nsec < end.tv_nsec));
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	if (server)
		f->server = NULL;
	else
		f->clients[0] = NULL;
}

static void test_public_server(void)
{
	struct fixture f;
	static const uint8_t payload[] = {0x11, 0x22, 0x00, 0x44};
	const struct tr_rpc_bytes body = {payload, sizeof(payload)};
	fixture_init(&f);

	/* 两个物理对端各自的首调用都执行 OPEN；身份不能只有槽和代次。 */
	for (unsigned i = 0; i < 2; ++i) {
		call_start(&f, i, i, ECHO);
		send_or_half_close(&f.calls[i], &body);
		send_or_half_close(&f.calls[i], NULL);
		check_result(&f.calls[i], TR_RPC_STATUS_OK, 1);
		assert(f.calls[i].response_size == sizeof(payload));
		assert(!memcmp(f.calls[i].response, payload, sizeof(payload)));
		assert(f.calls[i].opened == 1 && f.calls[i].messages == 1 &&
		       f.calls[i].halves == 1);
	}
	assert(!same_id(f.calls[0].identity, f.calls[1].identity));

	call_start(&f, 2, 0, BAD_BATCH);
	send_or_half_close(&f.calls[2], &body);
	check_result(&f.calls[2], TR_RPC_STATUS_RESOURCE_EXHAUSTED, 0);
	call_start(&f, 3, 0, FINISHED_BATCH);
	send_or_half_close(&f.calls[3], &body);
	check_result(&f.calls[3], TR_RPC_STATUS_FAILED_PRECONDITION, 0);

	/* 合法 OPEN 已匹配，但第一个工作事件的分配失败；公共终局仍有
	 * 一次责任，工作函数不得被调用。 */
	atomic_store(&fail_open_job, true);
	call_start(&f, 4, 0, NO_JOB);
	check_result(&f.calls[4], TR_RPC_STATUS_RESOURCE_EXHAUSTED, 0);
	assert(atomic_load(&failed_open_jobs) == 1);
	assert(!f.calls[4].opened && !f.calls[4].messages);
	check_server_totals(&f, 5);

	static const struct {
		enum business_mode mode;
		enum tr_rpc_status status;
	} metadata_cases[] = {
	        {DOUBLE_INITIAL, TR_RPC_STATUS_FAILED_PRECONDITION},
	        {INITIAL_CLOSED, TR_RPC_STATUS_FAILED_PRECONDITION},
	        {TRAILING_OPEN, TR_RPC_STATUS_FAILED_PRECONDITION},
	        {DUPLICATE_KEY, TR_RPC_STATUS_INVALID_ARGUMENT},
	        {BUSINESS_ERROR, TR_RPC_STATUS_INVALID_ARGUMENT}};
	for (unsigned i = 0;
	     i < sizeof(metadata_cases) / sizeof(metadata_cases[0]); ++i) {
		struct call *call = &f.calls[7U + i];
		call_start(&f, call->index, 0, metadata_cases[i].mode);
		if (metadata_cases[i].mode != TRAILING_OPEN)
			send_or_half_close(call, &body);
		check_result(call, metadata_cases[i].status, 0);
	}
	check_server_totals(&f, 10);

	static const struct {
		enum business_mode mode;
		enum tr_rpc_status status;
	} async_cases[] = {
	        {ASYNC_SYNC_WRITE, TR_RPC_STATUS_FAILED_PRECONDITION},
	        {ASYNC_ABANDON, TR_RPC_STATUS_CANCELLED},
	        {ASYNC_WORK_ERROR, TR_RPC_STATUS_INVALID_ARGUMENT},
	        {ASYNC_SEALED, TR_RPC_STATUS_OK}};
	for (unsigned i = 0; i < sizeof(async_cases) / sizeof(async_cases[0]);
	     ++i) {
		struct call *call = &f.calls[12U + i];
		call_start(&f, call->index, 0, async_cases[i].mode);
		if (call->mode == ASYNC_ABANDON || call->mode == ASYNC_SEALED) {
			wait_counter(&f, &call->producer_ready, 1);
			struct tr_rpc_v2_producer *producer = call->producer;
			if (call->mode == ASYNC_ABANDON) {
				/* 先证明 work/done 已交接，唯一原因才是最后应用
				 * 引用未 finish 的主动归还。 */
				assert(tr_rpc_v2_producer_wait_writable(
				               producer,
				               WAIT_SECONDS * 1000U) == TR_OK);
			} else {
				assert(tr_rpc_v2_producer_is_closed(producer) ==
				       0);
				/* 普通线程确认已封闭发送而尚未 CLOSED；这个
				 * STATE 不来自 work/owner 的禁止等待规则。 */
				assert(tr_rpc_v2_producer_wait_writable(
				               producer, 0) == TR_ERR_STATE);
			}
			check_server_totals(&f, 11U + i);
			assert(pthread_mutex_lock(&f.lock) == 0);
			assert(call->opened == 1 && !call->messages &&
			       !call->halves && !call->terminals);
			call->producer = NULL;
			assert(pthread_mutex_unlock(&f.lock) == 0);
			tr_rpc_v2_producer_release(producer);
			if (call->mode == ASYNC_SEALED) {
				assert(pthread_mutex_lock(&f.lock) == 0);
				call->release = true;
				assert(pthread_cond_broadcast(&f.changed) == 0);
				assert(pthread_mutex_unlock(&f.lock) == 0);
			}
		}

		check_result(call, async_cases[i].status, 0);
		assert(call->opened == 1 && !call->messages && !call->halves);
		if (call->producer) {
			struct tr_rpc_v2_producer *producer = call->producer;
			assert(tr_rpc_v2_producer_is_closed(producer) == 1);
			assert(tr_rpc_v2_producer_wait_writable(producer, 0) ==
			       TR_ERR_CLOSED);
			assert(pthread_mutex_lock(&f.lock) == 0);
			call->producer = NULL;
			assert(pthread_mutex_unlock(&f.lock) == 0);
			tr_rpc_v2_producer_release(producer);
		}
		check_server_totals(&f, 11U + i);
	}
	check_server_totals(&f, 14);

	call_start(&f, 5, 0, BLOCKED);
	send_or_half_close(&f.calls[5], &body);
	wait_entered(&f.calls[5]);
	send_or_half_close(&f.calls[5], NULL);
	assert(tr_rpc_v2_call_cancel(f.calls[5].handle) == TR_OK);
	wait_counter(&f, &f.calls[5].client_terminals, 1);
	/* 取消已经结束 Client，仍借用 request/reply 的 work 继续占据
	 * 一次服务端 inflight；服务端计数在业务终局实际交付前不能完成。 */
	check_server_totals(&f, 15);
	release_work(&f.calls[5]);
	check_result(&f.calls[5], TR_RPC_STATUS_CANCELLED, 0);
	assert(f.calls[5].cancelled && !f.calls[5].halves);
	check_server_totals(&f, 15);

	call_start(&f, 6, 1, BLOCKED);
	send_or_half_close(&f.calls[6], &body);
	wait_entered(&f.calls[6]);
	check_server_totals(&f, 16);
	assert(tr_server_destroy(f.server) == TR_AGAIN);
	struct tr_rpc_semantic_stats before, stats;
	memset(&before, 0xa5, sizeof(before));
	memcpy(&stats, &before, sizeof(stats));
	assert(tr_server_get_rpc_semantic_stats(f.server, &stats) ==
	       TR_ERR_STATE);
	assert(!memcmp(&stats, &before, sizeof(stats)));
	release_work(&f.calls[6]);
	retry_destroy(&f, true);
	assert(pthread_mutex_lock(&f.lock) == 0);
	assert(f.calls[6].terminals == 1 && f.calls[6].cancelled);
	assert(f.calls[6].status == TR_RPC_STATUS_CANCELLED);
	assert(f.calls[6].delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	assert(f.calls[6].received == 0);
	assert(f.calls[6].initial == 0 && f.calls[6].trailing == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	retry_destroy(&f, false);
	f.clients[0] = f.clients[1];
	f.clients[1] = NULL;
	retry_destroy(&f, false);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_capacity_refusal(void)
{
	struct fixture f;
	const uint8_t bytes[] = {1, 2, 3};
	const struct tr_rpc_bytes body = {bytes, sizeof(bytes)};
	fixture_init_capacity(&f, 1);
	call_start(&f, 0, 0, ECHO);
	wait_counter(&f, &f.calls[0].opened, 1);
	for (unsigned i = 1; i <= 4; ++i) {
		if (i == 4)
			atomic_store(&hold_refusal, true);
		call_start(&f, i, 0, ECHO);
		if (i == 4) {
			const struct timespec pause = {0, 1000000};
			for (unsigned spin = 0; !atomic_load(&refusal_blocked);
			     ++spin) {
				assert(spin < 5000);
				(void)nanosleep(&pause, NULL);
			}
			assert(tr_rpc_v2_call_cancel(f.calls[i].handle) ==
			       TR_OK);
		}
		wait_counter(&f, &f.calls[i].client_terminals, 1);
		atomic_store(&hold_refusal, false);
		assert(pthread_mutex_lock(&f.lock) == 0);
		assert(f.calls[i].client_status ==
		       (i == 4 ? TR_RPC_STATUS_CANCELLED
		               : TR_RPC_STATUS_RESOURCE_EXHAUSTED));
		assert(f.calls[i].client_terminals == 1 && !f.calls[i].opened &&
		       !f.calls[i].terminals && !f.calls[i].initial &&
		       !f.calls[i].trailing && !f.calls[0].client_terminals);
		assert(pthread_mutex_unlock(&f.lock) == 0);
	}
	/* 真实普通 Client/Server：拒绝从未进入业务，已有调用仍完成。 */
	send_or_half_close(&f.calls[0], &body);
	send_or_half_close(&f.calls[0], NULL);
	check_result(&f.calls[0], TR_RPC_STATUS_OK, 1);
	check_server_totals(&f, 1);
	assert(tr_client_begin_drain(f.clients[0]) == TR_OK);
	assert(tr_client_wait_drained(f.clients[0], WAIT_SECONDS * 1000) ==
	       TR_OK);
	struct tr_rpc_semantic_stats stats;
	assert(tr_client_get_rpc_semantic_stats(f.clients[0], &stats) == TR_OK);
	assert(stats.calls_started == 5 && stats.calls_finished == 5 &&
	       stats.calls_inflight == 0);
	assert(stats.final_status[TR_RPC_STATUS_RESOURCE_EXHAUSTED] == 3 &&
	       stats.final_status[TR_RPC_STATUS_CANCELLED] == 1 &&
	       stats.final_status[TR_RPC_STATUS_OK] == 1);
	retry_destroy(&f, false);
	f.clients[0] = f.clients[1];
	retry_destroy(&f, false);
	retry_destroy(&f, true);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_call_ref_survives_late_authorize_completion(void)
{
	struct fixture f;
	struct tr_rpc_v2_call_result snapshot = {TR_RPC_STATUS_INTERNAL,
	                                         TR_RPC_V2_DELIVERY_SHUTDOWN};
	const struct tr_rpc_v2_call_result untouched = snapshot;
	fixture_init_options(&f, 8, false, true);
	struct call *call = &f.calls[0];
	call_start(&f, 0, 0, ASYNC_AUTH_LATE_ALLOW);
	wait_auth_state(&f, &f.auth_entered, 1);

	assert(tr_rpc_v2_call_hold(call->handle, &call->held_ref) == TR_OK);
	assert(tr_rpc_v2_call_ref_result(call->held_ref, &snapshot) ==
	       TR_AGAIN);
	assert(!memcmp(&snapshot, &untouched, sizeof(snapshot)));
	assert(tr_rpc_v2_call_ref_cancel(call->held_ref) == TR_OK);
	wait_counter(&f, &call->client_terminals, 1);
	assert(call->client_status == TR_RPC_STATUS_CANCELLED);
	assert(tr_rpc_v2_call_ref_result(call->held_ref, &snapshot) == TR_OK);
	assert(snapshot.status == TR_RPC_STATUS_CANCELLED &&
	       snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);

	/* 只有在服务端 worker 实际看见取消后才允许它迟到返回 ALLOW。 */
	wait_auth_state(&f, &f.auth_cancelled, 1);
	assert(pthread_mutex_lock(&f.lock) == 0);
	f.auth_release = true;
	assert(pthread_cond_broadcast(&f.changed) == 0);
	assert(pthread_mutex_unlock(&f.lock) == 0);
	wait_counter(&f, &call->terminals, 1);
	assert(call->status == TR_RPC_STATUS_CANCELLED &&
	       call->terminals == 1 && f.auth_work_calls == 0);
	check_server_totals(&f, 1);

	/* 服务端操作已完成并退役，客户端结果引用仍独立保持有效。 */
	retry_destroy(&f, true);
	assert(tr_rpc_v2_call_ref_result(call->held_ref, &snapshot) == TR_OK);
	assert(snapshot.status == TR_RPC_STATUS_CANCELLED &&
	       snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);
	assert(tr_client_begin_drain(f.clients[0]) == TR_OK);
	assert(tr_client_wait_drained(f.clients[0], 0) == TR_AGAIN);
	assert(tr_rpc_v2_call_ref_release(&call->held_ref) == TR_OK);
	assert(tr_client_wait_drained(f.clients[0], WAIT_SECONDS * 1000) ==
	       TR_OK);
	struct tr_rpc_semantic_stats stats;
	assert(tr_client_get_rpc_semantic_stats(f.clients[0], &stats) == TR_OK);
	assert(stats.calls_started == 1 && stats.calls_finished == 1 &&
	       stats.calls_inflight == 0 &&
	       stats.final_status[TR_RPC_STATUS_CANCELLED] == 1);
	retry_destroy(&f, false);
	f.clients[0] = f.clients[1];
	f.clients[1] = NULL;
	retry_destroy(&f, false);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_call_ref_blocks_destroy_after_terminal(void)
{
	struct fixture f;
	struct tr_rpc_v2_call_result snapshot = {TR_RPC_STATUS_INTERNAL,
	                                         TR_RPC_V2_DELIVERY_SHUTDOWN};
	static const uint8_t payload[] = {0x5a, 0x00, 0xa5};
	const struct tr_rpc_bytes body = {payload, sizeof(payload)};
	fixture_init(&f);
	struct call *call = &f.calls[0];
	call->hold_in_terminal = true;
	call_start(&f, 0, 0, ECHO);
	send_or_half_close(call, &body);
	send_or_half_close(call, NULL);
	check_result(call, TR_RPC_STATUS_OK, 1);
	assert(tr_rpc_v2_call_ref_result(call->held_ref, &snapshot) == TR_OK);
	assert(snapshot.status == TR_RPC_STATUS_OK &&
	       snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);

	/* destroy stops the client but must retain storage while CallRef is
	 * held. */
	assert(tr_client_destroy(f.clients[0]) == TR_AGAIN);
	assert(tr_rpc_v2_call_ref_result(call->held_ref, &snapshot) == TR_OK);
	assert(snapshot.status == TR_RPC_STATUS_OK &&
	       snapshot.delivery == TR_RPC_V2_DELIVERY_LIVE);
	assert(tr_rpc_v2_call_ref_release(&call->held_ref) == TR_OK);
	assert(tr_client_destroy(f.clients[0]) == TR_OK);
	f.clients[0] = NULL;
	f.clients[0] = f.clients[1];
	f.clients[1] = NULL;
	retry_destroy(&f, false);
	retry_destroy(&f, true);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_asymmetric_response_window(void)
{
	struct fixture f;
	uint8_t bytes[640] = {1};
	const struct tr_rpc_bytes large = {bytes, sizeof(bytes)},
	                          small = {bytes, 4};
	fixture_init_options(&f, 8, true, false);
	call_start(&f, 0, 0, ECHO);
	wait_counter(&f, &f.calls[0].opened, 1);
	for (unsigned i = 1; i <= 2; ++i) {
		struct call *call = &f.calls[i];
		call_start(&f, i, 0, i == 1 ? WINDOW_REPLY : ASYNC_ABANDON);
		if (i == 1) {
			send_or_half_close(call, &small);
		} else {
			wait_counter(&f, &call->producer_ready, 1);
			assert(tr_rpc_v2_producer_wait_writable(
			               call->producer, WAIT_SECONDS * 1000) ==
			       TR_OK);
			assert(tr_rpc_v2_producer_send(call->producer,
			                               &large) == TR_OK);
		}
		/* 未设置 deadline；不靠取消或销毁替不可能写出的回复收尾。 */
		wait_counter(&f, &call->terminals, 1);
		wait_counter(&f, &call->client_terminals, 1);
		assert(pthread_mutex_lock(&f.lock) == 0);
		if (call->status != TR_RPC_STATUS_INVALID_ARGUMENT ||
		    call->client_status != TR_RPC_STATUS_INVALID_ARGUMENT)
			fprintf(stderr,
			        "response window: mode=%u server=%u "
			        "client=%u\n",
			        (unsigned)call->mode, (unsigned)call->status,
			        (unsigned)call->client_status);
		assert(call->status == TR_RPC_STATUS_INVALID_ARGUMENT &&
		       call->client_status == TR_RPC_STATUS_INVALID_ARGUMENT);
		assert(call->terminals == 1 && call->client_terminals == 1 &&
		       call->received == 0 && call->initial == 1 &&
		       !f.calls[0].terminals && !f.calls[0].client_terminals);
		assert(pthread_mutex_unlock(&f.lock) == 0);
		if (call->producer) {
			assert(tr_rpc_v2_producer_is_closed(call->producer) ==
			       1);
			assert(tr_rpc_v2_producer_send(call->producer,
			                               &small) ==
			       TR_ERR_CLOSED);
			tr_rpc_v2_producer_release(call->producer);
			call->producer = NULL;
		}
	}
	send_or_half_close(&f.calls[0], &small);
	send_or_half_close(&f.calls[0], NULL);
	check_result(&f.calls[0], TR_RPC_STATUS_OK, 1);
	check_server_totals(&f, 3);
	assert(tr_client_begin_drain(f.clients[0]) == TR_OK);
	assert(tr_client_wait_drained(f.clients[0], WAIT_SECONDS * 1000) ==
	       TR_OK);
	struct tr_rpc_semantic_stats stats;
	assert(tr_client_get_rpc_semantic_stats(f.clients[0], &stats) == TR_OK);
	assert(stats.calls_started == 3 && stats.calls_finished == 3 &&
	       stats.calls_inflight == 0 &&
	       stats.final_status[TR_RPC_STATUS_INVALID_ARGUMENT] == 2 &&
	       stats.final_status[TR_RPC_STATUS_OK] == 1);
	retry_destroy(&f, false);
	f.clients[0] = f.clients[1];
	retry_destroy(&f, false);
	retry_destroy(&f, true);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_configuration(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_rpc_v2_method method = {
	        .descriptor = {7, 1, TR_RPC_MANY, TR_RPC_MANY, TR_RPC_CODEC_RAW,
	                       TR_RPC_CODEC_RAW, TR_RPC_LANE_CONTROL, 33, 33, 0,
	                       0},
	        .work = work};
	assert(tr_rpc_v2_reply_set_metadata(NULL, TR_RPC_V2_METADATA_INITIAL,
	                                    NULL, 0) == TR_ERR_INVALID);
	tr_server_config_init(&config);
	config.limits.max_message_bytes = 32;
	assert(tr_server_create(&config, &method, 1, &server) ==
	       TR_ERR_INVALID);
	assert(!server);
	config.limits.max_message_bytes = 64;
	config.limits.initial_window_bytes = 32;
	assert(tr_server_create(&config, &method, 1, &server) ==
	       TR_ERR_INVALID);
	assert(!server);
	config.limits.initial_window_bytes = 64;
	config.limits.max_frame_payload_bytes = 64;
	/* 没有业务调用时允许不提供可选终局函数，且可正常回收。 */
	assert(tr_server_create(&config, &method, 1, &server) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
}

int main(void)
{
	test_configuration();
	test_call_ref_survives_late_authorize_completion();
	test_call_ref_blocks_destroy_after_terminal();
	test_asymmetric_response_window();
	test_capacity_refusal();
	test_public_server();
	puts("test_rpc_v2_server: OK");
	return 0;
}
