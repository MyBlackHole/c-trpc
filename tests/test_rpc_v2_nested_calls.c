#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../include/tr/client.h"
#include "../include/tr/rpc_v2.h"
#include "../include/tr/server.h"
#include "../include/tr/status.h"
#include "tls_test_identity.h"

#define TEST_TIMEOUT_MS 10000U
#define NESTED_CALLS 3U
#define NESTED_BODY_BYTES 256U

#define CHECK(condition)                                                       \
	do {                                                                   \
		if (!(condition)) {                                            \
			fprintf(stderr, "CHECK failed at %s:%d: %s\n",         \
			        __FILE__, __LINE__, #condition);               \
			abort();                                               \
		}                                                              \
	} while (0)

struct nested_test;

struct nested_peer {
	struct nested_test *test;
	unsigned side;
	struct tr_server *server;
	struct tr_client *client;
	uint16_t port;
};

struct nested_client_call {
	struct nested_test *test;
	struct tr_rpc_v2_producer *parent_producer;
	struct tr_rpc_v2_call handle;
	unsigned index;
	unsigned client_side;
	unsigned request_sent;
	unsigned half_closed;
	unsigned messages;
	unsigned terminals;
	enum tr_rpc_status status;
};

struct nested_test {
	const struct tls_test_identity *identity;
	enum tr_rpc_compression compression;
	enum tr_rpc_v2_request_kind nested_start;
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct nested_peer peers[2];
	struct tr_rpc_v2_call_context parent_contexts[2];
	struct tr_rpc_v2_call_id server_calls[3];
	unsigned server_call_present[3];
	unsigned server_open_count[2];
	unsigned server_message_count[3];
	unsigned server_terminal_count[2];
	enum tr_rpc_status server_status[3];
	int child_start_status[2];
	unsigned child_start_done[2];
	unsigned parent_context_releases;
	unsigned producer_releases;
	unsigned client_callback_probe_done;
	unsigned server_observer_probe_done;
	unsigned server_authorize_probe_done;
	unsigned server_terminal_probe_done;
	struct nested_client_call calls[NESTED_CALLS];
	unsigned root_terminal_count;
	enum tr_rpc_status root_status;
};

static struct timespec deadline_after_ms(uint32_t ms)
{
	struct timespec result;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &result) == 0);
	result.tv_sec += (time_t)(ms / 1000U);
	result.tv_nsec += (long)(ms % 1000U) * 1000000L;
	if (result.tv_nsec >= 1000000000L) {
		++result.tv_sec;
		result.tv_nsec -= 1000000000L;
	}
	return result;
}

static void nested_client_message(struct tr_rpc_v2_call call,
                                  const struct tr_rpc_bytes *body, void *arg);
static void nested_client_writable(struct tr_rpc_v2_call call, void *arg);
static void nested_client_terminal(struct tr_rpc_v2_call call,
                                   enum tr_rpc_status status,
                                   enum tr_rpc_v2_delivery delivery, void *arg);

static struct tr_rpc_v2_client_callbacks
nested_callbacks(struct nested_client_call *call)
{
	return (struct tr_rpc_v2_client_callbacks){
	        .message = nested_client_message,
	        .writable = nested_client_writable,
	        .terminal = nested_client_terminal,
	        .arg = call,
	};
}

static int nested_claim_probe(struct nested_test *test, unsigned *done)
{
	int claimed = 0;
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	if (!*done) {
		*done = 1U;
		claimed = 1;
	}
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
	return claimed;
}

static void nested_probe_cross_client_start(struct nested_test *test,
                                            unsigned source_side)
{
	struct tr_rpc_v2_client_callbacks callbacks =
	        nested_callbacks(&test->calls[0]);
	struct tr_rpc_v2_call output;
	struct tr_rpc_v2_call sentinel;
	memset(&output, 0xa5, sizeof(output));
	sentinel = output;
	CHECK(tr_client_call_start(test->peers[1U - source_side].client, 1U, 1U,
	                           NULL, &callbacks, &output) == TR_ERR_STATE);
	CHECK(memcmp(&output, &sentinel, sizeof(output)) == 0);
}

static void
nested_server_observer(const struct tr_rpc_v2_observation *observation,
                       void *arg)
{
	struct nested_peer *peer = arg;
	struct nested_test *test = peer->test;
	CHECK(observation != NULL);
	if (observation->event == TR_RPC_V2_OBSERVE_OPEN &&
	    nested_claim_probe(test, &test->server_observer_probe_done))
		nested_probe_cross_client_start(test, peer->side);
}

static struct tr_rpc_v2_authorization_result
nested_server_authorize(struct tr_rpc_v2_call_id call,
                        const struct tr_rpc_v2_request *request,
                        const struct tr_rpc_v2_cancel *cancel, void *arg)
{
	struct nested_peer *peer = arg;
	struct nested_test *test = peer->test;
	(void)call;
	(void)request;
	(void)cancel;
	if (nested_claim_probe(test, &test->server_authorize_probe_done))
		nested_probe_cross_client_start(test, peer->side);
	return (struct tr_rpc_v2_authorization_result){
	        .decision = TR_RPC_V2_AUTHORIZATION_ALLOW,
	        .rejection_status = TR_RPC_STATUS_OK,
	};
}

static void nested_client_message(struct tr_rpc_v2_call call,
                                  const struct tr_rpc_bytes *body, void *arg)
{
	struct nested_client_call *client_call = arg;
	struct nested_test *test = client_call->test;
	(void)call;
	CHECK(body != NULL && body->len == NESTED_BODY_BYTES &&
	      body->data != NULL);
	for (unsigned i = 0U; i < NESTED_BODY_BYTES; ++i)
		CHECK(body->data[i] == (uint8_t)'R');
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	++client_call->messages;
	CHECK(pthread_cond_broadcast(&test->changed) == 0);
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
}

static void nested_client_writable(struct tr_rpc_v2_call call, void *arg)
{
	struct nested_client_call *client_call = arg;
	struct nested_test *test = client_call->test;
	int probe_callback = 0;

	CHECK(pthread_mutex_lock(&test->lock) == 0);
	if (!test->client_callback_probe_done) {
		test->client_callback_probe_done = 1U;
		probe_callback = 1;
	}
	CHECK(pthread_mutex_unlock(&test->lock) == 0);

	/* Client callback 仍不得同步切到另一 I/O owner。 */
	if (probe_callback)
		nested_probe_cross_client_start(test, client_call->client_side);

	if (client_call->half_closed)
		return;
	if (!client_call->request_sent) {
		uint8_t bytes[NESTED_BODY_BYTES];
		const struct tr_rpc_bytes body = {bytes, sizeof(bytes)};
		int ret;
		memset(bytes, 'Q', sizeof(bytes));
		ret = tr_rpc_v2_call_send_with_compression(call, &body,
		                                           test->compression);
		if (ret == TR_AGAIN)
			return;
		CHECK(ret == TR_OK);
		client_call->request_sent = 1U;
	}
	/* 压缩尚未提交到网络时半关闭会背压；后续 WRITABLE 只重试半关闭。 */
	int ret = tr_rpc_v2_call_half_close(call);
	if (ret == TR_AGAIN)
		return;
	CHECK(ret == TR_OK);
	client_call->half_closed = 1U;
}

static void nested_client_terminal(struct tr_rpc_v2_call call,
                                   enum tr_rpc_status status,
                                   enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct nested_client_call *client_call = arg;
	struct nested_test *test = client_call->test;
	(void)call;
	CHECK(delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(client_call->request_sent == 1U);
	CHECK(client_call->terminals == 0U);
	if (client_call->parent_producer) {
		CHECK(tr_rpc_v2_producer_finish(client_call->parent_producer,
		                                status, NULL, 0U) == TR_OK);
		tr_rpc_v2_producer_release(client_call->parent_producer);
		client_call->parent_producer = NULL;
		CHECK(pthread_mutex_lock(&test->lock) == 0);
		++test->producer_releases;
		CHECK(pthread_mutex_unlock(&test->lock) == 0);
	}
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	client_call->status = status;
	++client_call->terminals;
	if (client_call->index == 0U) {
		++test->root_terminal_count;
		test->root_status = status;
	}
	CHECK(pthread_cond_broadcast(&test->changed) == 0);
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
}

static int nested_stage_for_call(struct nested_peer *peer,
                                 struct tr_rpc_v2_call_id call, int is_open)
{
	struct nested_test *test = peer->test;
	int stage = -1;
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	if (is_open) {
		if (peer->side == 0U) {
			unsigned open_index = test->server_open_count[0]++;
			stage = open_index == 0U ? 0 : 2;
			CHECK(open_index < 2U);
		} else {
			unsigned open_index = test->server_open_count[1]++;
			CHECK(open_index == 0U);
			stage = 1;
		}
		CHECK(!test->server_call_present[stage]);
		test->server_calls[stage] = call;
		test->server_call_present[stage] = 1U;
	} else {
		for (unsigned i = 0U; i < 3U; ++i) {
			int side_matches =
			        (peer->side == 0U && (i == 0U || i == 2U)) ||
			        (peer->side == 1U && i == 1U);
			if (side_matches && test->server_call_present[i] &&
			    memcmp(&test->server_calls[i], &call,
			           sizeof(call)) == 0) {
				stage = (int)i;
				break;
			}
		}
	}
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
	CHECK(stage >= 0);
	return stage;
}

static enum tr_rpc_status
nested_server_work(struct tr_rpc_v2_call_id call,
                   const struct tr_rpc_v2_request *request,
                   const struct tr_rpc_v2_cancel *cancel,
                   struct tr_rpc_v2_reply *reply, void *arg)
{
	struct nested_peer *peer = arg;
	struct nested_test *test = peer->test;
	int stage = nested_stage_for_call(
	        peer, call, request->kind == TR_RPC_V2_REQUEST_OPEN);
	/* 每个 work 事件均保留连接身份，不能因授权或嵌套提交丢失。 */
	if (test->identity) {
		static const char expected[] = "spiffe://c-trpc.example/client";
		CHECK(request->peer_identity.authentication ==
		      TR_PEER_IDENTITY_TLS_CERTIFICATE);
		CHECK(request->peer_identity.uri_san != NULL &&
		      request->peer_identity.uri_san_len ==
		              sizeof(expected) - 1U);
		CHECK(memcmp(request->peer_identity.uri_san, expected,
		             sizeof(expected) - 1U) == 0);
	} else {
		CHECK(request->peer_identity.authentication ==
		      TR_PEER_IDENTITY_NONE);
		CHECK(request->peer_identity.uri_san == NULL &&
		      request->peer_identity.uri_san_len == 0U);
	}
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
		CHECK(request->body.len == NESTED_BODY_BYTES &&
		      request->body.data != NULL);
		for (unsigned i = 0U; i < NESTED_BODY_BYTES; ++i)
			CHECK(request->body.data[i] == (uint8_t)'Q');
		CHECK(pthread_mutex_lock(&test->lock) == 0);
		++test->server_message_count[stage];
		CHECK(pthread_mutex_unlock(&test->lock) == 0);
	}

	if ((unsigned)stage < 2U && request->kind == test->nested_start) {
		struct tr_rpc_v2_client_callbacks callbacks;
		struct tr_rpc_call_options options = {0};
		struct nested_client_call *child =
		        &test->calls[(unsigned)stage + 1U];
		struct tr_rpc_v2_producer *producer = NULL;
		struct tr_rpc_v2_call_context *parent =
		        &test->parent_contexts[(unsigned)stage];
		int parent_held = 0;
		int ret;

		ret = tr_rpc_v2_cancel_context_hold(cancel, parent);
		parent_held = ret == TR_OK;
		if (ret == TR_OK)
			ret = tr_rpc_v2_reply_acquire_producer(reply,
			                                       &producer);
		if (ret == TR_OK) {
			child->test = test;
			child->parent_producer = producer;
			child->index = (unsigned)stage + 1U;
			child->client_side = 1U - peer->side;
			callbacks = nested_callbacks(child);
			options.timeout_ms = TEST_TIMEOUT_MS;
			options.parent_context = parent;
			ret = tr_client_call_start(
			        test->peers[1U - peer->side].client, 1U, 1U,
			        &options, &callbacks, &child->handle);
		}
		if (parent_held) {
			CHECK(tr_rpc_v2_call_context_release(parent) == TR_OK);
			CHECK(pthread_mutex_lock(&test->lock) == 0);
			++test->parent_context_releases;
			CHECK(pthread_mutex_unlock(&test->lock) == 0);
		}
		CHECK(pthread_mutex_lock(&test->lock) == 0);
		test->child_start_status[stage] = ret;
		test->child_start_done[stage] = 1U;
		CHECK(pthread_cond_broadcast(&test->changed) == 0);
		CHECK(pthread_mutex_unlock(&test->lock) == 0);
		if (ret != TR_OK) {
			if (producer) {
				CHECK(tr_rpc_v2_producer_finish(
				              producer, TR_RPC_STATUS_INTERNAL,
				              NULL, 0U) == TR_OK);
				tr_rpc_v2_producer_release(producer);
				child->parent_producer = NULL;
				CHECK(pthread_mutex_lock(&test->lock) == 0);
				++test->producer_releases;
				CHECK(pthread_mutex_unlock(&test->lock) == 0);
			}
			return TR_RPC_STATUS_OK;
		}
		return TR_RPC_STATUS_OK;
	}

	if ((unsigned)stage == 2U &&
	    request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		uint8_t bytes[NESTED_BODY_BYTES];
		const struct tr_rpc_bytes body = {bytes, sizeof(bytes)};
		memset(bytes, 'R', sizeof(bytes));
		CHECK(tr_rpc_v2_reply_append_with_compression(
		              reply, &body, test->compression) == TR_OK);
	}
	return TR_RPC_STATUS_OK;
}

static void nested_server_terminal(struct tr_rpc_v2_call_id call,
                                   enum tr_rpc_status status,
                                   enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct nested_peer *peer = arg;
	struct nested_test *test = peer->test;
	int stage = nested_stage_for_call(peer, call, 0);
	CHECK(delivery == TR_RPC_V2_DELIVERY_LIVE);
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	CHECK(test->server_status[stage] == TR_RPC_STATUS_COUNT);
	test->server_status[stage] = status;
	++test->server_terminal_count[peer->side];
	CHECK(pthread_cond_broadcast(&test->changed) == 0);
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
	if (nested_claim_probe(test, &test->server_terminal_probe_done))
		nested_probe_cross_client_start(test, peer->side);
}

static struct tr_rpc_method_desc
nested_method_descriptor(enum tr_rpc_compression compression)
{
	return (struct tr_rpc_method_desc){
	        .service_id = 1U,
	        .method_id = 1U,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_MANY,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = NESTED_BODY_BYTES,
	        .max_response_bytes = NESTED_BODY_BYTES,
	        .request_compression_algorithms =
	                compression == TR_RPC_COMPRESSION_ZSTD
	                        ? TR_RPC_COMPRESSION_MASK_ZSTD
	                        : 0U,
	        .response_compression_algorithms =
	                compression == TR_RPC_COMPRESSION_ZSTD
	                        ? TR_RPC_COMPRESSION_MASK_ZSTD
	                        : 0U,
	};
}

static void nested_peer_create(struct nested_test *test,
                               struct nested_peer *peer, unsigned side)
{
	struct tr_rpc_method_desc descriptor =
	        nested_method_descriptor(test->compression);
	struct tr_rpc_v2_method method = {
	        .descriptor = descriptor,
	        .work = nested_server_work,
	        .terminal = nested_server_terminal,
	        .arg = peer,
	        .observe = nested_server_observer,
	        .authorize = nested_server_authorize,
	};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_limits limits;

	memset(peer, 0, sizeof(*peer));
	peer->test = test;
	peer->side = side;
	tr_facade_limits_init(&limits);
	limits.max_streams = 8U;
	limits.max_methods = 1U;
	limits.max_calls = 8U;
	limits.max_frame_payload_bytes = 256U;
	limits.max_message_bytes = 512U;
	limits.initial_window_bytes = 512U;
	tr_server_config_init(&server_config);
	server_config.limits = limits;
	server_config.max_peers = 4U;
	server_config.handshake_timeout_ms = 3000U;
	tr_client_config_init(&client_config);
	client_config.limits = limits;
	client_config.connect_timeout_ms = 3000U;
	if (test->identity) {
		const struct tls_test_identity *identity = test->identity;
		server_config.tls.mode = TR_TLS_REQUIRED;
		server_config.tls.certificate_file =
		        identity->server_certificate_file;
		server_config.tls.private_key_file =
		        identity->server_private_key_file;
		server_config.tls.client_ca_file = identity->ca_file;
		server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
		client_config.tls.mode = TR_TLS_REQUIRED;
		client_config.tls.ca_file = identity->ca_file;
		client_config.tls.expected_peer_name = "server.example.test";
		client_config.tls.client_certificate_file =
		        identity->client_certificate_file;
		client_config.tls.client_private_key_file =
		        identity->client_private_key_file;
	}
	CHECK(tr_server_create(&server_config, &method, 1U, &peer->server) ==
	      TR_OK);
	CHECK(tr_server_listen(peer->server, "127.0.0.1", 0U, &peer->port) ==
	      TR_OK);
	CHECK(peer->port != 0U);
	CHECK(tr_server_start(peer->server) == TR_OK);
	CHECK(tr_client_create(&client_config, &descriptor, 1U,
	                       &peer->client) == TR_OK);
	CHECK(tr_client_connect(peer->client, "127.0.0.1", peer->port) ==
	      TR_OK);
	CHECK(tr_client_wait_ready(peer->client, 5000U) == TR_OK);
}

static void wait_for_chain(struct nested_test *test)
{
	struct timespec limit = deadline_after_ms(TEST_TIMEOUT_MS);
	CHECK(pthread_mutex_lock(&test->lock) == 0);
	for (;;) {
		int settled = test->root_terminal_count == 1U &&
		              test->child_start_done[0] != 0U &&
		              (test->child_start_status[0] != TR_OK ||
		               test->child_start_done[1] != 0U);
		if (settled)
			break;
		int ret = pthread_cond_timedwait(&test->changed, &test->lock,
		                                 &limit);
		CHECK(ret == 0 || ret == ETIMEDOUT);
		CHECK(ret != ETIMEDOUT);
	}
	CHECK(pthread_mutex_unlock(&test->lock) == 0);
}

static void retry_destroy_client(struct tr_client **client)
{
	struct timespec limit = deadline_after_ms(TEST_TIMEOUT_MS);
	for (;;) {
		int ret = tr_client_destroy(*client);
		if (ret == TR_OK) {
			*client = NULL;
			return;
		}
		CHECK(ret == TR_AGAIN);
		struct timespec now;
		struct timespec pause = {0, 1000000L};
		CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		CHECK(now.tv_sec < limit.tv_sec ||
		      (now.tv_sec == limit.tv_sec &&
		       now.tv_nsec < limit.tv_nsec));
		CHECK(nanosleep(&pause, NULL) == 0);
	}
}

static void retry_destroy_server(struct tr_server **server)
{
	struct timespec limit = deadline_after_ms(TEST_TIMEOUT_MS);
	for (;;) {
		int ret = tr_server_destroy(*server);
		if (ret == TR_OK) {
			*server = NULL;
			return;
		}
		CHECK(ret == TR_AGAIN);
		struct timespec now;
		struct timespec pause = {0, 1000000L};
		CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		CHECK(now.tv_sec < limit.tv_sec ||
		      (now.tv_sec == limit.tv_sec &&
		       now.tv_nsec < limit.tv_nsec));
		CHECK(nanosleep(&pause, NULL) == 0);
	}
}

static void nested_peer_drain_destroy(struct nested_peer *peer)
{
	CHECK(tr_client_begin_drain(peer->client) == TR_OK);
	CHECK(tr_client_wait_drained(peer->client, TEST_TIMEOUT_MS) == TR_OK);
	CHECK(tr_server_drain(peer->server, TEST_TIMEOUT_MS) == TR_OK);
}

static void run_nested_call_case(const struct tls_test_identity *identity,
                                 enum tr_rpc_compression compression,
                                 enum tr_rpc_v2_request_kind nested_start)
{
	struct nested_test test;
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_call_options options = {0};

	memset(&test, 0, sizeof(test));
	test.identity = identity;
	test.compression = compression;
	test.nested_start = nested_start;
	for (unsigned i = 0U; i < 3U; ++i)
		test.server_status[i] = TR_RPC_STATUS_COUNT;
	for (unsigned i = 0U; i < 2U; ++i)
		test.child_start_status[i] = TR_ERR_SYS;
	CHECK(pthread_mutex_init(&test.lock, NULL) == 0);
	{
		pthread_condattr_t attr;
		CHECK(pthread_condattr_init(&attr) == 0);
		CHECK(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
		CHECK(pthread_cond_init(&test.changed, &attr) == 0);
		CHECK(pthread_condattr_destroy(&attr) == 0);
	}
	nested_peer_create(&test, &test.peers[0], 0U);
	nested_peer_create(&test, &test.peers[1], 1U);

	test.calls[0].test = &test;
	test.calls[0].index = 0U;
	test.calls[0].client_side = 0U;
	callbacks = nested_callbacks(&test.calls[0]);
	options.timeout_ms = TEST_TIMEOUT_MS;
	CHECK(tr_client_call_start(test.peers[0].client, 1U, 1U, &options,
	                           &callbacks, &test.calls[0].handle) == TR_OK);
	wait_for_chain(&test);

	nested_peer_drain_destroy(&test.peers[0]);
	nested_peer_drain_destroy(&test.peers[1]);
	retry_destroy_client(&test.peers[0].client);
	retry_destroy_client(&test.peers[1].client);
	retry_destroy_server(&test.peers[0].server);
	retry_destroy_server(&test.peers[1].server);
	/* drain/destroy 后不再有回调；此时读取完整的生命周期证据。 */
	CHECK(pthread_mutex_lock(&test.lock) == 0);
	int child_start_status[2] = {test.child_start_status[0],
	                             test.child_start_status[1]};
	enum tr_rpc_status root_status = test.root_status;
	unsigned root_terminals = test.calls[0].terminals;
	unsigned client_terminals[3] = {test.calls[0].terminals,
	                                test.calls[1].terminals,
	                                test.calls[2].terminals};
	unsigned client_messages[3] = {test.calls[0].messages,
	                               test.calls[1].messages,
	                               test.calls[2].messages};
	unsigned server_terminals[2] = {test.server_terminal_count[0],
	                                test.server_terminal_count[1]};
	unsigned context_releases = test.parent_context_releases;
	unsigned producer_releases = test.producer_releases;
	unsigned observer_probes = test.server_observer_probe_done;
	unsigned authorize_probes = test.server_authorize_probe_done;
	unsigned terminal_probes = test.server_terminal_probe_done;
	enum tr_rpc_status server_status[3] = {test.server_status[0],
	                                       test.server_status[1],
	                                       test.server_status[2]};
	CHECK(pthread_mutex_unlock(&test.lock) == 0);
	CHECK(pthread_cond_destroy(&test.changed) == 0);
	CHECK(pthread_mutex_destroy(&test.lock) == 0);

	if (child_start_status[0] != TR_OK || child_start_status[1] != TR_OK)
		fprintf(stderr, "nested start statuses: %d, %d\n",
		        child_start_status[0], child_start_status[1]);
	CHECK(child_start_status[0] == TR_OK);
	CHECK(child_start_status[1] == TR_OK);
	CHECK(root_terminals == 1U && root_status == TR_RPC_STATUS_OK);
	CHECK(client_terminals[0] == 1U && client_terminals[1] == 1U &&
	      client_terminals[2] == 1U);
	CHECK(client_messages[0] == 0U && client_messages[1] == 0U &&
	      client_messages[2] == 1U);
	CHECK(server_terminals[0] == 2U && server_terminals[1] == 1U);
	CHECK(test.server_message_count[0] <= 1U &&
	      test.server_message_count[1] <= 1U &&
	      test.server_message_count[2] == 1U);
	/* OPEN 上的异步子调用可先于父请求正文完成；HALF_CLOSE 场景须收齐正文。
	 */
	if (nested_start == TR_RPC_V2_REQUEST_HALF_CLOSE)
		CHECK(test.server_message_count[0] == 1U &&
		      test.server_message_count[1] == 1U);
	CHECK(server_status[0] == TR_RPC_STATUS_OK &&
	      server_status[1] == TR_RPC_STATUS_OK &&
	      server_status[2] == TR_RPC_STATUS_OK);
	CHECK(context_releases == 2U && producer_releases == 2U);
	CHECK(observer_probes == 1U && authorize_probes == 1U &&
	      terminal_probes == 1U && test.client_callback_probe_done == 1U);
	printf("rpc-v2-nested: A->B->A tls=%u compression=%u start=%u "
	       "result=pass\n",
	       (unsigned)(identity != NULL), (unsigned)compression,
	       (unsigned)nested_start);
}

int main(void)
{
	struct tls_test_identity identity;
	CHECK(tls_test_identity_init(&identity, "rpc-v2-nested-mtls") == 0);
	const enum tr_rpc_v2_request_kind starts[] = {
	        TR_RPC_V2_REQUEST_OPEN, TR_RPC_V2_REQUEST_HALF_CLOSE};
	for (unsigned i = 0U; i < sizeof(starts) / sizeof(starts[0]); ++i) {
		run_nested_call_case(NULL, TR_RPC_COMPRESSION_NONE, starts[i]);
		run_nested_call_case(NULL, TR_RPC_COMPRESSION_ZSTD, starts[i]);
		run_nested_call_case(&identity, TR_RPC_COMPRESSION_NONE,
		                     starts[i]);
		run_nested_call_case(&identity, TR_RPC_COMPRESSION_ZSTD,
		                     starts[i]);
	}
	tls_test_identity_cleanup(&identity);
	return 0;
}
