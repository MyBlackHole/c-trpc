#define _POSIX_C_SOURCE 200809L
#include "../src/runtime/runtime_internal.h"
#include "../src/message/storage.h"
#include "../src/execution/reactor_internal.h"
#include "tr/status.h"

#include <zstd.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define MIB ((size_t)1024 * 1024)
#define SHARDS 3U

struct gate {
	pthread_mutex_t lock;
	pthread_cond_t condition;
	bool entered;
	bool open;
};

static void gate_init(struct gate *g)
{
	*g = (struct gate){0};
	CHECK(pthread_mutex_init(&g->lock, NULL) == 0);
	CHECK(pthread_cond_init(&g->condition, NULL) == 0);
}

static void gate_wait(struct gate *g, bool worker)
{
	struct timespec limit;
	CHECK(clock_gettime(CLOCK_REALTIME, &limit) == 0);
	limit.tv_sec += 15;
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	if (worker) {
		g->entered = true;
		CHECK(pthread_cond_broadcast(&g->condition) == 0);
	}
	while (worker ? !g->open : !g->entered)
		CHECK(pthread_cond_timedwait(&g->condition, &g->lock, &limit) ==
		      0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

static void gate_release(struct gate *g)
{
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	g->open = true;
	CHECK(pthread_cond_broadcast(&g->condition) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}

static void gate_destroy(struct gate *g)
{
	CHECK(pthread_cond_destroy(&g->condition) == 0);
	CHECK(pthread_mutex_destroy(&g->lock) == 0);
}

static _Atomic(struct gate *) next_gate;
static _Atomic(struct tr_reactor *) failing_owner;
static atomic_uint thread_failures;

size_t __real_ZSTD_compressStream2(ZSTD_CCtx *, ZSTD_outBuffer *,
                                   ZSTD_inBuffer *, ZSTD_EndDirective);
size_t __real_ZSTD_decompressStream(ZSTD_DCtx *, ZSTD_outBuffer *,
                                    ZSTD_inBuffer *);
int __real_pthread_create(pthread_t *, const pthread_attr_t *,
                          void *(*)(void *), void *);

static void checkpoint(void)
{
	struct gate *g = atomic_exchange(&next_gate, NULL);
	if (g)
		gate_wait(g, true);
}

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *c, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(c, out, in, end);
	checkpoint();
	return ret;
}

size_t __wrap_ZSTD_decompressStream(ZSTD_DCtx *c, ZSTD_outBuffer *out,
                                    ZSTD_inBuffer *in)
{
	size_t ret = __real_ZSTD_decompressStream(c, out, in);
	checkpoint();
	return ret;
}

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                          void *(*fn)(void *), void *arg)
{
	struct tr_reactor *r = atomic_load(&failing_owner);
	if (r && tr_reactor_in_owner_context_for(r)) {
		atomic_store(&failing_owner, NULL);
		atomic_fetch_add(&thread_failures, 1);
		return EAGAIN;
	}
	return __real_pthread_create(thread, attr, fn, arg);
}

struct fixture {
	struct tr_runtime *runtime;
	struct tr_runtime_shard *shards[SHARDS];
};

static uint64_t now_seconds(void)
{
	struct timespec now;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec;
}

static void create_fixture(struct fixture *f)
{
	struct tr_runtime_shard_config shards[SHARDS] = {0};
	struct tr_runtime_config config = {SHARDS, shards};
	*f = (struct fixture){0};
	for (size_t i = 0; i < SHARDS; ++i) {
		shards[i].memory_budget_bytes = 128 * MIB;
		shards[i].reactor.max_connections = 4;
		shards[i].reactor.rx_buffer_count = 4;
		shards[i].reactor.rx_buffer_size = 4096;
		shards[i].reactor.max_payload_len = 4096;
		shards[i].reactor.tx_item_capacity = 8;
		shards[i].reactor.control_tx_item_capacity = 8;
		if (i < 2)
			shards[i].messages = (struct tr_message_service_config){
			        {1, 2}, 1, 10000000};
	}
	CHECK(tr_runtime_create(&config, &f->runtime) == TR_OK);
	for (size_t i = 0; i < SHARDS; ++i)
		f->shards[i] = tr_runtime_shard_at(f->runtime, (uint32_t)i);
}

static uint64_t used(struct tr_runtime_shard *shard)
{
	struct tr_memory_budget_stats stats;
	tr_runtime_shard_memory_stats(shard, &stats);
	CHECK(stats.current_bytes <= stats.limit_bytes);
	return stats.current_bytes;
}

static void stop_fixture(struct fixture *f)
{
	uint64_t limit = now_seconds() + 15;
	int ret;
	while ((ret = tr_runtime_stop(f->runtime)) == TR_AGAIN) {
		CHECK(now_seconds() < limit);
		sched_yield();
	}
	CHECK(ret == TR_OK);
}

struct sample {
	struct tr_runtime_shard *shard;
	struct tr_message_storage storage;
	struct tr_message input;
	struct tr_message retained;
	struct tr_task_handle handle;
	atomic_uint releases;
	atomic_uint completions;
	enum tr_message_transform_outcome expected;
};

static void source_release(void *arg)
{
	struct sample *s = arg;
	CHECK(atomic_fetch_add(&s->releases, 1) == 0);
}

static void init_sample(struct sample *s, struct tr_runtime_shard *shard,
                        const void *data, size_t bytes,
                        enum tr_message_transform_outcome expected)
{
	*s = (struct sample){.shard = shard, .expected = expected};
	atomic_init(&s->releases, 0);
	atomic_init(&s->completions, 0);
	CHECK(tr_message_storage_reserve(&s->storage,
	                                 tr_runtime_shard_memory_budget(shard),
	                                 bytes) == TR_OK);
	CHECK(tr_message_storage_publish(&s->storage, data, bytes,
	                                 source_release, s,
	                                 &s->input) == TR_OK);
}

static void completed(void *arg, struct tr_task_handle handle,
                      struct tr_message_service_result result,
                      struct tr_message *message)
{
	struct sample *s = arg;
	struct tr_message_view view;
	CHECK(tr_reactor_in_owner_context_for(
	        tr_runtime_shard_reactor(s->shard)));
	CHECK(result.delivery == TR_MESSAGE_SERVICE_ON_OWNER);
	CHECK(result.outcome == s->expected);
	CHECK(atomic_load(&s->releases) == 1);
	/* 完成正在交付而未退役时，取消应返回 STATE，不再次生成终局。 */
	CHECK(tr_runtime_shard_message_cancel(s->shard, handle) ==
	      TR_ERR_STATE);
	if (s->expected == TR_MESSAGE_TRANSFORM_OK)
		CHECK(tr_message_clone(message, &s->retained) == TR_OK);
	else
		CHECK(tr_message_view(message, &view) == TR_ERR_STATE);
	CHECK(atomic_fetch_add(&s->completions, 1) == 0);
}

static int submit(struct sample *s, bool decode)
{
	struct tr_message_transform_config config = {
	        .kind = decode ? TR_MESSAGE_ZSTD_DECODE
	                       : TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {2 * MIB, 2 * MIB},
	        .decoded_size = decode ? MIB : 0,
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3};
	return tr_runtime_shard_message_submit(s->shard, &s->input, &config,
	                                       completed, s, &s->handle);
}

static int owner_barrier(void *arg)
{
	(void)arg;
	return TR_OK;
}

static void wait_sample(struct sample *s)
{
	uint64_t limit = now_seconds() + 15;
	while (!atomic_load(&s->completions)) {
		CHECK(now_seconds() < limit);
		sched_yield();
	}
	CHECK(tr_runtime_shard_call(s->shard, owner_barrier, NULL) == TR_OK);
}

struct foreign_request {
	struct sample *unsubmitted;
	struct tr_task_handle active;
};

static int foreign_probe(void *arg)
{
	struct foreign_request *r = arg;
	CHECK(tr_runtime_shard_message_cancel(r->unsubmitted->shard,
	                                      r->active) == TR_ERR_STATE);
	CHECK(submit(r->unsubmitted, false) == TR_ERR_STATE);
	return TR_OK;
}

static void test_cancel_and_isolation(bool decode)
{
	struct fixture f;
	struct sample active, queued, rejected, independent;
	struct tr_message_view view;
	struct tr_task_handle old;
	struct gate gate;
	struct foreign_request probe;
	uint8_t *data = calloc(1, MIB);
	uint8_t *frame = NULL;
	size_t length = MIB;
	uint64_t before;

	CHECK(data);
	if (decode) {
		frame = malloc(ZSTD_compressBound(MIB));
		CHECK(frame);
		length = ZSTD_compress(frame, ZSTD_compressBound(MIB), data,
		                       MIB, 3);
		CHECK(!ZSTD_isError(length));
	}
	create_fixture(&f);
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	CHECK(tr_runtime_shard_message_cancel(
	              NULL, (struct tr_task_handle){0}) == TR_ERR_INVALID);
	CHECK(tr_runtime_shard_message_cancel(f.shards[2],
	                                      (struct tr_task_handle){0}) ==
	      TR_ERR_UNSUPPORTED);
	init_sample(&active, f.shards[0], decode ? frame : data, length,
	            TR_MESSAGE_TRANSFORM_CANCELLED);
	init_sample(&queued, f.shards[0], data, 1,
	            TR_MESSAGE_TRANSFORM_CANCELLED);
	init_sample(&rejected, f.shards[0], data, 1, TR_MESSAGE_TRANSFORM_OK);
	init_sample(&independent, f.shards[1], data, 1,
	            TR_MESSAGE_TRANSFORM_OK);
	gate_init(&gate);
	atomic_store(&next_gate, &gate);
	CHECK(submit(&active, decode) == TR_OK);
	gate_wait(&gate, false);
	CHECK(tr_runtime_shard_message_cancel(f.shards[1], active.handle) ==
	      TR_ERR_STALE);
	probe = (struct foreign_request){&rejected, active.handle};
	before = used(f.shards[0]);
	CHECK(tr_runtime_shard_call(f.shards[1], foreign_probe, &probe) ==
	      TR_OK);
	CHECK(used(f.shards[0]) == before);
	CHECK(rejected.handle.generation == 0 &&
	      tr_message_view(&rejected.input, &view) == TR_OK);
	CHECK(submit(&queued, false) == TR_OK);
	CHECK(submit(&rejected, false) == TR_AGAIN);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], queued.handle) ==
	      TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], queued.handle) ==
	      TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], active.handle) ==
	      TR_OK);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], active.handle) ==
	      TR_OK);
	CHECK(atomic_load(&active.releases) == 0 &&
	      atomic_load(&queued.releases) == 0);
	CHECK(submit(&independent, false) == TR_OK);
	wait_sample(&independent);
	CHECK(tr_message_release(&independent.retained) == TR_OK);
	CHECK(!atomic_load(&active.completions) &&
	      !atomic_load(&queued.completions));
	gate_release(&gate);
	wait_sample(&active);
	wait_sample(&queued);
	old = active.handle;
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], old) ==
	      TR_ERR_STALE);
	CHECK(tr_message_release(&rejected.input) == TR_OK);
	gate_destroy(&gate);
	stop_fixture(&f);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], old) ==
	      TR_ERR_CLOSED);
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	init_sample(&active, f.shards[0], data, MIB, TR_MESSAGE_TRANSFORM_OK);
	gate_init(&gate);
	atomic_store(&next_gate, &gate);
	CHECK(submit(&active, false) == TR_OK);
	gate_wait(&gate, false);
	CHECK(active.handle.instance != old.instance);
	CHECK(tr_runtime_shard_message_cancel(f.shards[0], old) ==
	      TR_ERR_STALE);
	gate_release(&gate);
	wait_sample(&active);
	CHECK(tr_message_release(&active.retained) == TR_OK);
	gate_destroy(&gate);
	stop_fixture(&f);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
	free(frame);
	free(data);
}

static void test_multishard_start_rollback(void)
{
	struct fixture f;
	uint64_t before[SHARDS];
	int ret;
	create_fixture(&f);
	for (size_t i = 0; i < SHARDS; ++i)
		before[i] = used(f.shards[i]);
	atomic_store(&thread_failures, 0);
	atomic_store(&failing_owner, tr_runtime_shard_reactor(f.shards[1]));
	ret = tr_runtime_start(f.runtime);
	CHECK(ret == TR_ERR_SYS || ret == TR_AGAIN);
	CHECK(atomic_load(&thread_failures) == 1);
	stop_fixture(&f);
	for (size_t i = 0; i < SHARDS; ++i)
		CHECK(used(f.shards[i]) == before[i]);
	CHECK(tr_runtime_start(f.runtime) == TR_OK);
	stop_fixture(&f);
	CHECK(tr_runtime_destroy(f.runtime) == TR_OK);
}

int main(void)
{
	test_cancel_and_isolation(false);
	test_cancel_and_isolation(true);
	test_multishard_start_rollback();
	puts("runtime_message_control: ok");
	return 0;
}
