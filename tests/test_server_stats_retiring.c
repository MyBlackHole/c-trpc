#define _POSIX_C_SOURCE 200809L
#include "tr/trpc.h"
#include "facade_diagnostics_internal.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
struct gate {
	pthread_mutex_t m;
	pthread_cond_t c;
	int entered, release;
};
static int handler(struct tr_rpc_call_handle h, const struct tr_rpc_bytes *r,
                   struct tr_rpc_unary_response *out, void *arg)
{
	struct gate *g = arg;
	(void)h;
	(void)r;
	pthread_mutex_lock(&g->m);
	g->entered = 1;
	pthread_cond_broadcast(&g->c);
	while (!g->release)
		pthread_cond_wait(&g->c, &g->m);
	pthread_mutex_unlock(&g->m);
	out->status = TR_RPC_STATUS_OK;
	out->message.data = NULL;
	out->message.len = 0;
	return TR_OK;
}
static void result(struct tr_rpc_call_handle h, int s,
                   const struct tr_rpc_bytes *b, void *a)
{
	(void)h;
	(void)s;
	(void)b;
	(void)a;
}
static void assert_cumulative(const struct tr_server_stats *now,
                              const struct tr_server_stats *prior)
{
	assert(now->rpc.calls_started >= prior->rpc.calls_started);
	assert(now->rpc.calls_completed >= prior->rpc.calls_completed);
	assert(now->rpc.calls_cancelled >= prior->rpc.calls_cancelled);
	assert(now->rpc.calls_deadline_exceeded >=
	       prior->rpc.calls_deadline_exceeded);
	assert(now->rpc.executor_enqueued_tasks >=
	       prior->rpc.executor_enqueued_tasks);
	assert(now->rpc.executor_taken_tasks >=
	       prior->rpc.executor_taken_tasks);
	assert(now->rpc.semantic.calls_started >=
	       prior->rpc.semantic.calls_started);
	assert(now->rpc.semantic.calls_finished >=
	       prior->rpc.semantic.calls_finished);
	assert(now->channel.streams_opened >= prior->channel.streams_opened);
	assert(now->channel.streams_closed >= prior->channel.streams_closed);
	assert(now->channel.messages_rx >= prior->channel.messages_rx);
	assert(now->channel.messages_tx >= prior->channel.messages_tx);
	assert(now->channel.bytes_rx >= prior->channel.bytes_rx);
	assert(now->channel.bytes_tx >= prior->channel.bytes_tx);
	for (unsigned int i = 0; i < TR_RPC_STATUS_COUNT; i++)
		assert(now->rpc.semantic.final_status[i] >=
		       prior->rpc.semantic.final_status[i]);
}

static void test_retirement(uint32_t shard_count)
{
	struct gate g = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0,
	                 0};
	struct tr_server_config sc;
	struct tr_client_config cc;
	struct tr_server *s = NULL;
	struct tr_client *c = NULL;
	struct tr_rpc_method_desc m = {0};
	struct tr_rpc_call_handle h;
	struct tr_rpc_bytes req = {(const unsigned char *)"x", 1};
	struct tr_server_stats st;
	struct tr_server_stats previous = {0};
	uint16_t port;
	struct timespec pause = {0, 1000000};
	tr_server_config_init(&sc);
	sc.shard_count = shard_count;
	sc.max_peers = shard_count;
	sc.keepalive_interval_ms = 0;
	assert(tr_server_create(&sc, &s) == TR_OK);
	m.service_id = 1;
	m.method_id = 1;
	m.request_cardinality = TR_RPC_ONE;
	m.response_cardinality = TR_RPC_ONE;
	m.request_codec_id = TR_RPC_CODEC_RAW;
	m.response_codec_id = TR_RPC_CODEC_RAW;
	m.lane = TR_RPC_LANE_CONTROL;
	m.max_request_bytes = 16;
	m.max_response_bytes = 16;
	assert(tr_server_register_method(s, &m, handler, &g) == TR_OK);
	assert(tr_server_listen(s, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(s) == TR_OK);
	for (uint64_t cycle = 1; cycle <= 3; cycle++) {
		g.entered = 0;
		g.release = 0;
		tr_client_config_init(&cc);
		cc.keepalive_interval_ms = 0;
		assert(tr_client_create(&cc, &c) == TR_OK);
		assert(tr_client_connect(c, "127.0.0.1", port) == TR_OK);
		assert(tr_client_register_method(c, &m) == TR_OK);
		assert(tr_client_unary_call(c, 1, 1, &req, result, NULL, &h) ==
		       TR_OK);
		pthread_mutex_lock(&g.m);
		while (!g.entered)
			pthread_cond_wait(&g.c, &g.m);
		pthread_mutex_unlock(&g.m);
		assert(tr_server_get_stats(s, &st) == TR_OK);
		assert_cumulative(&st, &previous);
		previous = st;
		printf("before disconnect: started=%llu reaping=%u\n",
		       (unsigned long long)st.rpc.semantic.calls_started,
		       st.peers_reaping_current);
		assert(st.rpc.semantic.calls_started == cycle);
		assert(tr_client_destroy(c) == TR_OK);
		for (int i = 0; i < 5000; i++) {
			assert(tr_server_get_stats(s, &st) == TR_OK);
			assert_cumulative(&st, &previous);
			previous = st;
			if (st.peers_reaping_current)
				break;
			nanosleep(&pause, NULL);
		}
		printf("while worker retained: started=%llu reaping=%u\n",
		       (unsigned long long)st.rpc.semantic.calls_started,
		       st.peers_reaping_current);
		assert(st.peers_reaping_current == 1);
		assert(st.rpc.semantic.calls_started == cycle);
		struct tr_rpc_semantic_stats prior = st.rpc.semantic;
		for (int i = 0; i < 100; i++) {
			assert(tr_server_get_stats(s, &st) == TR_OK);
			assert_cumulative(&st, &previous);
			previous = st;
			assert(st.rpc.semantic.calls_started == cycle);
			assert(st.rpc.semantic.calls_finished >=
			       prior.calls_finished);
			for (unsigned j = 0; j < TR_RPC_STATUS_COUNT; j++)
				assert(st.rpc.semantic.final_status[j] >=
				       prior.final_status[j]);
			prior = st.rpc.semantic;
		}
		pthread_mutex_lock(&g.m);
		g.release = 1;
		pthread_cond_broadcast(&g.c);
		pthread_mutex_unlock(&g.m);
		for (int i = 0; i < 5000; i++) {
			assert(tr_server_get_stats(s, &st) == TR_OK);
			assert_cumulative(&st, &previous);
			previous = st;
			assert(st.rpc.semantic.calls_started == cycle);
			assert(st.rpc.semantic.calls_finished >=
			       prior.calls_finished);
			for (unsigned j = 0; j < TR_RPC_STATUS_COUNT; j++)
				assert(st.rpc.semantic.final_status[j] >=
				       prior.final_status[j]);
			prior = st.rpc.semantic;
			if (!st.peers_reaping_current)
				break;
			nanosleep(&pause, NULL);
		}
		printf("after finalization: started=%llu reaping=%u\n",
		       (unsigned long long)st.rpc.semantic.calls_started,
		       st.peers_reaping_current);
		assert(st.peers_reaping_current == 0);
		assert(st.rpc.semantic.calls_finished == cycle);
		assert(st.rpc.semantic.calls_inflight == 0);
		uint64_t finished = 0;
		for (unsigned j = 0; j < TR_RPC_STATUS_COUNT; j++)
			finished += st.rpc.semantic.final_status[j];
		assert(finished == cycle);
		for (int i = 0; i < 100; i++) {
			assert(tr_server_get_stats(s, &st) == TR_OK);
			assert_cumulative(&st, &previous);
			previous = st;
			assert(st.rpc.semantic.calls_started == cycle);
			assert(st.rpc.semantic.calls_finished == cycle);
		}
	}
	assert(tr_server_destroy(s) == TR_OK);
	pthread_cond_destroy(&g.c);
	pthread_mutex_destroy(&g.m);
}

int main(void)
{
	test_retirement(1);
	test_retirement(2);
	return 0;
}
