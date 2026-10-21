#define _POSIX_C_SOURCE 200809L

#include "tr/trpc.h"
#include "tr/rpc_v2.h"
#include "facade_diagnostics_internal.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define WAIT_MS 10000U

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned entered;
	unsigned terminals;
	int release;
	enum tr_rpc_status final_status[TR_RPC_STATUS_COUNT];
};

static struct timespec realtime_after_ms(unsigned milliseconds)
{
	struct timespec due;
	assert(clock_gettime(CLOCK_REALTIME, &due) == 0);
	due.tv_sec += milliseconds / 1000U;
	due.tv_nsec += (long)(milliseconds % 1000U) * 1000000L;
	if (due.tv_nsec >= 1000000000L) {
		++due.tv_sec;
		due.tv_nsec -= 1000000000L;
	}
	return due;
}

static void wait_for_count(struct gate *gate, const unsigned *value,
			   unsigned target)
{
	struct timespec due = realtime_after_ms(WAIT_MS);
	int ret = 0;
	assert(pthread_mutex_lock(&gate->lock) == 0);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&gate->cond, &gate->lock, &due);
	assert(ret == 0);
	assert(*value >= target);
	assert(pthread_mutex_unlock(&gate->lock) == 0);
}

static enum tr_rpc_status blocked_work(
	struct tr_rpc_v2_call_id call, const struct tr_rpc_v2_request *request,
	const struct tr_rpc_v2_cancel *cancel, struct tr_rpc_v2_reply *reply,
	void *arg)
{
	struct gate *gate = (struct gate *)arg;

	(void)call;
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		assert(pthread_mutex_lock(&gate->lock) == 0);
		++gate->entered;
		assert(pthread_cond_broadcast(&gate->cond) == 0);
		while (!gate->release)
			assert(pthread_cond_wait(&gate->cond, &gate->lock) == 0);
		assert(pthread_mutex_unlock(&gate->lock) == 0);
		return tr_rpc_v2_cancel_requested(cancel) == 1
		               ? TR_RPC_STATUS_CANCELLED
		               : TR_RPC_STATUS_OK;
	}

	if (request->kind == TR_RPC_V2_REQUEST_HALF_CLOSE) {
		struct tr_rpc_bytes empty = {NULL, 0U};
		assert(tr_rpc_v2_reply_append(reply, &empty) == TR_OK);
		assert(tr_rpc_v2_reply_finish(reply, TR_RPC_STATUS_OK) == TR_OK);
	}
	return TR_RPC_STATUS_OK;
}

static void record_terminal(struct tr_rpc_v2_call_id call,
			    enum tr_rpc_status status,
			    enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct gate *gate = (struct gate *)arg;
	(void)call;
	assert(delivery == TR_RPC_V2_DELIVERY_LIVE ||
	       delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	assert((unsigned)status < TR_RPC_STATUS_COUNT);
	assert(pthread_mutex_lock(&gate->lock) == 0);
	++gate->terminals;
	++gate->final_status[status];
	assert(pthread_cond_broadcast(&gate->cond) == 0);
	assert(pthread_mutex_unlock(&gate->lock) == 0);
}

static void ignore_client_message(struct tr_rpc_v2_call call,
				  const struct tr_rpc_bytes *body, void *arg)
{
	(void)call;
	(void)body;
	(void)arg;
}

static void ignore_client_terminal(struct tr_rpc_v2_call call,
				   enum tr_rpc_status status,
				   enum tr_rpc_v2_delivery delivery, void *arg)
{
	(void)call;
	(void)status;
	(void)delivery;
	(void)arg;
}

static void assert_semantic_cumulative(
	const struct tr_rpc_semantic_stats *now,
	const struct tr_rpc_semantic_stats *prior)
{
	assert(now->calls_started >= prior->calls_started);
	assert(now->calls_finished >= prior->calls_finished);
	assert(now->calls_inflight == now->calls_started - now->calls_finished);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		assert(now->final_status[i] >= prior->final_status[i]);
}

static void assert_semantic_totals(const struct tr_rpc_semantic_stats *stats,
				   uint64_t started, uint64_t finished)
{
	uint64_t final_count = 0U;
	assert(stats->calls_started == started);
	assert(stats->calls_finished == finished);
	assert(stats->calls_inflight == started - finished);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		final_count += stats->final_status[i];
	assert(final_count == finished);
}

static void wait_for_peer_not_ready(struct tr_server *server)
{
	struct timespec pause = {0, 1000000L};
	struct tr_server_stats stats;
	for (unsigned attempt = 0; attempt < WAIT_MS; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current != 0U &&
		    stats.peers_ready_current == 0U)
			return;
		assert(nanosleep(&pause, NULL) == 0);
	}
	assert(tr_server_get_stats(server, &stats) == TR_OK);
	assert(stats.peers_current != 0U);
	assert(stats.peers_ready_current == 0U);
}

static void wait_for_peer_reaped(struct tr_server *server,
				 uint64_t expected_total)
{
	struct timespec pause = {0, 1000000L};
	struct tr_server_stats stats;
	for (unsigned attempt = 0; attempt < WAIT_MS; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 0U &&
		    stats.peers_reaped_total >= expected_total)
			return;
		assert(nanosleep(&pause, NULL) == 0);
	}
	assert(tr_server_get_stats(server, &stats) == TR_OK);
	assert(stats.peers_current == 0U);
	assert(stats.peers_reaped_total >= expected_total);
}

static void destroy_server_retry(struct tr_server *server)
{
	struct timespec pause = {0, 1000000L};
	int ret = TR_AGAIN;
	for (unsigned attempt = 0; attempt < WAIT_MS && ret == TR_AGAIN;
	     ++attempt) {
		ret = tr_server_destroy(server);
		if (ret == TR_AGAIN)
			assert(nanosleep(&pause, NULL) == 0);
	}
	assert(ret == TR_OK);
}

static void test_retirement(uint32_t shard_count)
{
	struct gate gate = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,
			    0U, 0U, 0, {0}};
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_method_desc client_method;
	struct tr_rpc_v2_method server_method;
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_bytes request = {(const uint8_t *)"x", 1U};
	struct tr_server *server = NULL;
	uint16_t port = 0U;

	tr_server_config_init(&server_config);
	server_config.shard_count = shard_count;
	server_config.max_peers = shard_count;
	server_config.keepalive_interval_ms = 0U;
	memset(&server_method, 0, sizeof(server_method));
	server_method.descriptor.service_id = 1U;
	server_method.descriptor.method_id = 1U;
	server_method.descriptor.request_cardinality = TR_RPC_ONE;
	server_method.descriptor.response_cardinality = TR_RPC_ONE;
	server_method.descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	server_method.descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	server_method.descriptor.lane = TR_RPC_LANE_CONTROL;
	server_method.descriptor.max_request_bytes = 16U;
	server_method.descriptor.max_response_bytes = 16U;
	server_method.work = blocked_work;
	server_method.terminal = record_terminal;
	server_method.arg = &gate;
	assert(tr_server_create(&server_config, &server_method, 1U, &server) ==
	       TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	memset(&client_method, 0, sizeof(client_method));
	client_method.service_id = 1U;
	client_method.method_id = 1U;
	client_method.request_cardinality = TR_RPC_ONE;
	client_method.response_cardinality = TR_RPC_ONE;
	client_method.request_codec_id = TR_RPC_CODEC_RAW;
	client_method.response_codec_id = TR_RPC_CODEC_RAW;
	client_method.lane = TR_RPC_LANE_CONTROL;
	client_method.max_request_bytes = 16U;
	client_method.max_response_bytes = 16U;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = ignore_client_message;
	callbacks.terminal = ignore_client_terminal;

	for (uint64_t cycle = 1U; cycle <= 3U; ++cycle) {
		struct tr_client *client = NULL;
		struct tr_rpc_v2_call call;
		struct tr_rpc_semantic_stats stats;
		struct tr_rpc_semantic_stats previous = {0};
		struct tr_server_stats server_stats;

		assert(tr_client_create(&client_config, &client_method, 1U, &client) ==
		       TR_OK);
		assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
		assert(tr_client_wait_ready(client, WAIT_MS) == TR_OK);
		assert(tr_client_unary_call(client, 1U, 1U, &request, NULL,
		                            &callbacks, &call) == TR_OK);
		wait_for_count(&gate, &gate.entered, (unsigned)cycle);

		assert(tr_server_get_rpc_semantic_stats(server, &stats) == TR_OK);
		assert_semantic_totals(&stats, cycle, cycle - 1U);
		previous = stats;
		assert(tr_client_destroy(client) == TR_OK);

		wait_for_peer_not_ready(server);
		assert(tr_server_get_stats(server, &server_stats) == TR_OK);
		assert(server_stats.peers_current == 1U);
		assert(server_stats.peers_ready_current == 0U);
		assert(server_stats.peers_reaping_current == 0U);
		for (unsigned sample = 0; sample < 100U; ++sample) {
			assert(tr_server_get_rpc_semantic_stats(server, &stats) ==
			       TR_OK);
			assert_semantic_cumulative(&stats, &previous);
			assert_semantic_totals(&stats, cycle, cycle - 1U);
			previous = stats;
		}

		assert(pthread_mutex_lock(&gate.lock) == 0);
		gate.release = 1;
		assert(pthread_cond_broadcast(&gate.cond) == 0);
		assert(pthread_mutex_unlock(&gate.lock) == 0);
		wait_for_count(&gate, &gate.terminals, (unsigned)cycle);
		wait_for_peer_reaped(server, cycle);

		assert(tr_server_get_rpc_semantic_stats(server, &stats) == TR_OK);
		assert_semantic_cumulative(&stats, &previous);
		assert_semantic_totals(&stats, cycle, cycle);
		previous = stats;
		assert(tr_server_get_stats(server, &server_stats) == TR_OK);
		assert(server_stats.peers_current == 0U);
		assert(server_stats.peers_reaping_current == 0U);

		for (unsigned sample = 0; sample < 100U; ++sample) {
			assert(tr_server_get_rpc_semantic_stats(server, &stats) ==
			       TR_OK);
			assert_semantic_cumulative(&stats, &previous);
			assert_semantic_totals(&stats, cycle, cycle);
			previous = stats;
		}
		assert(pthread_mutex_lock(&gate.lock) == 0);
		gate.release = 0;
		assert(pthread_mutex_unlock(&gate.lock) == 0);
	}

	destroy_server_retry(server);
	assert(pthread_cond_destroy(&gate.cond) == 0);
	assert(pthread_mutex_destroy(&gate.lock) == 0);
}

int main(void)
{
	test_retirement(1U);
	test_retirement(2U);
	return 0;
}
