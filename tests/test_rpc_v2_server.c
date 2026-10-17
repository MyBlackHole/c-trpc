#define _POSIX_C_SOURCE 200809L
#include "tr/rpc_v2.h"
#include "tr/status.h"
#include "../src/rpc/rpc_record_stream.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CALLS 7U
#define METHODS 5U
#define WAIT_SECONDS 15

/* 故障只发生在真实 OPEN 已进入绑定接收后创建工作事件的 calloc。
 * 记录绑定自身的构造、其他线程及非 OPEN 接收都调用原实现。 */
static _Atomic bool fail_open_job;
static _Atomic unsigned failed_open_jobs;
static _Thread_local bool receiving_open;

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

enum business_mode { ECHO, BAD_BATCH, FINISHED_BATCH, BLOCKED, NO_JOB };
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
	enum tr_rpc_status status, client_status;
	enum tr_rpc_v2_delivery delivery;
	bool identified, entered, release, exited, cancelled;
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
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(call->entered && !call->exited && !call->terminals);
	call->release = true;
	assert(pthread_cond_broadcast(&f->changed) == 0);
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
	assert(tr_server_start(f->server) == TR_ERR_STATE);
	assert(tr_server_drain(f->server, 0) == TR_ERR_STATE);
	assert(tr_server_destroy(f->server) == TR_ERR_STATE);
	assert(tr_server_create_v2(NULL, NULL, 0, &extra) == TR_ERR_STATE);
	assert(!extra);
	assert(tr_client_destroy(f->clients[0]) == TR_ERR_STATE);
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
		while (!call->release)
			assert(pthread_cond_wait(&f->changed, &f->lock) == 0);
		assert(tr_rpc_v2_cancel_requested(cancel) == 1);
		call->cancelled = true;
		call->exited = true;
		assert(pthread_cond_broadcast(&f->changed) == 0);
	}
	assert(pthread_mutex_unlock(&f->lock) == 0);
	assert(request->body.len <= sizeof(copied));
	memcpy(copied, request->body.data, request->body.len);
	struct tr_rpc_bytes response = {copied, request->body.len};
	assert(tr_rpc_v2_reply_append(reply, &response) == TR_OK);
	/* append 接管的是副本；业务仍拥有原存储，可以立即覆盖。 */
	memset(copied, 0xcc, sizeof(copied));
	if (method->mode == BAD_BATCH) {
		response.len = 17;
		assert(tr_rpc_v2_reply_append(reply, &response) ==
		       TR_ERR_BAD_LENGTH);
	} else if (method->mode == FINISHED_BATCH) {
		assert(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) ==
		       TR_OK);
		assert(tr_rpc_v2_reply_append(reply, &response) ==
		       TR_ERR_STATE);
	}
	return TR_RPC_STATUS_OK;
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
	memcpy(call->response, body->data, body->len);
	call->response_size = body->len;
	++call->received;
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
	(void)handle;
	(void)delivery;
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(!call->client_terminals);
	call->client_status = status;
	++call->client_terminals;
	assert(pthread_cond_broadcast(&f->changed) == 0);
	assert(pthread_mutex_unlock(&f->lock) == 0);
}

static void fixture_init(struct fixture *f)
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
	server_config.limits.max_streams = 8;
	server_config.limits.max_frame_payload_bytes = 64;
	server_config.limits.max_message_bytes = 64;
	server_config.limits.initial_window_bytes = 256;
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
		                                    16};
		methods[i] = (struct tr_rpc_v2_method){
		        descriptions[i], work, server_terminal, &f->methods[i]};
	}
	assert(tr_server_create_v2(&server_config, methods, METHODS,
	                           &f->server) == TR_OK);
	/* 整个原数组已不再借用，包括 terminal 和 arg 指针。 */
	memset(methods, 0, sizeof(methods));
	assert(tr_server_listen(f->server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(f->server) == TR_OK);
	tr_client_config_init(&client_config);
	client_config.limits = server_config.limits;
	client_config.keepalive_interval_ms = 0;
	for (unsigned i = 0; i < 2; ++i) {
		assert(tr_client_create_v2(&client_config, descriptions,
		                           METHODS, &f->clients[i]) == TR_OK);
		assert(tr_client_connect(f->clients[i], "127.0.0.1", port) ==
		       TR_OK);
		assert(tr_client_wait_ready(f->clients[i],
		                            WAIT_SECONDS * 1000) == TR_OK);
	}
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
	        .writable = client_writable,
	        .terminal = client_terminal,
	        .arg = call};
	assert(tr_client_v2_call_start(f->clients[client], 7, method + 1,
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
	wait_counter(f, &call->terminals, 1);
	wait_counter(f, &call->client_terminals, 1);
	assert(pthread_mutex_lock(&f->lock) == 0);
	assert(call->terminals == 1 && call->client_terminals == 1);
	assert(call->status == status && call->client_status == status);
	assert(call->delivery == TR_RPC_V2_DELIVERY_LIVE);
	assert(call->received == received);
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

	call_start(&f, 5, 0, BLOCKED);
	send_or_half_close(&f.calls[5], &body);
	wait_entered(&f.calls[5]);
	send_or_half_close(&f.calls[5], NULL);
	assert(tr_rpc_v2_call_cancel(f.calls[5].handle) == TR_OK);
	wait_counter(&f, &f.calls[5].client_terminals, 1);
	release_work(&f.calls[5]);
	check_result(&f.calls[5], TR_RPC_STATUS_CANCELLED, 0);
	assert(f.calls[5].cancelled && !f.calls[5].halves);

	call_start(&f, 6, 1, BLOCKED);
	send_or_half_close(&f.calls[6], &body);
	wait_entered(&f.calls[6]);
	assert(tr_server_destroy(f.server) == TR_AGAIN);
	release_work(&f.calls[6]);
	retry_destroy(&f, true);
	assert(f.calls[6].terminals == 1 && f.calls[6].cancelled);
	assert(f.calls[6].status == TR_RPC_STATUS_CANCELLED);
	assert(f.calls[6].delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	assert(f.calls[6].received == 0);
	retry_destroy(&f, false);
	f.clients[0] = f.clients[1];
	f.clients[1] = NULL;
	retry_destroy(&f, false);
	assert(pthread_cond_destroy(&f.changed) == 0);
	assert(pthread_mutex_destroy(&f.lock) == 0);
}

static void test_configuration(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_rpc_v2_method method = {
	        .descriptor = {7, 1, TR_RPC_MANY, TR_RPC_MANY, TR_RPC_CODEC_RAW,
	                       TR_RPC_CODEC_RAW, TR_RPC_LANE_CONTROL, 33, 33},
	        .work = work};
	tr_server_config_init(&config);
	config.limits.max_message_bytes = 32;
	assert(tr_server_create_v2(&config, &method, 1, &server) ==
	       TR_ERR_INVALID);
	assert(!server);
	config.limits.max_message_bytes = 64;
	config.limits.initial_window_bytes = 32;
	assert(tr_server_create_v2(&config, &method, 1, &server) ==
	       TR_ERR_INVALID);
	assert(!server);
	config.limits.initial_window_bytes = 64;
	config.limits.max_frame_payload_bytes = 64;
	/* 没有业务调用时允许不提供可选终局函数，且可正常回收。 */
	assert(tr_server_create_v2(&config, &method, 1, &server) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
}

int main(void)
{
	test_configuration();
	test_public_server();
	puts("test_rpc_v2_server: OK");
	return 0;
}
