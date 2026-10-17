#define _POSIX_C_SOURCE 200809L
#include "../src/execution/worker_context.h"
#include "../src/rpc/rpc_record_dispatch.h"
#include "../src/rpc/rpc_record_producer.h"
#include "../src/rpc/rpc_v2_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/rpc_codec.h"
#include "tr/status.h"

#include <poll.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define CAP 6U
#define BODY 4096U

void *__real_malloc(size_t);
void *__real_calloc(size_t, size_t);
int __real_tr_stream_sendv(struct tr_stream_handle, struct tr_buffer *const *,
                           uint32_t);
int __real_clock_gettime(clockid_t, struct timespec *);
struct admission_clock {
	uint64_t now, advanced;
	unsigned reads, advance_at, fail_at, invalid;
};
static _Thread_local struct admission_clock *admission_clock;
int __wrap_clock_gettime(clockid_t id, struct timespec *out)
{
	struct admission_clock *clock = admission_clock;
	if (!clock)
		return __real_clock_gettime(id, out);
	CHECK(id == CLOCK_MONOTONIC);
	++clock->reads;
	if (clock->reads == clock->fail_at) {
		errno = EIO;
		return -1;
	}
	if (clock->reads == clock->advance_at)
		clock->now = clock->advanced;
	out->tv_sec = (time_t)(clock->now / UINT64_C(1000000000));
	out->tv_nsec = (long)(clock->now % UINT64_C(1000000000));
	if (clock->invalid == 1)
		out->tv_sec = -1;
	if (clock->invalid == 2)
		out->tv_nsec = -1;
	if (clock->invalid == 3)
		out->tv_nsec = 1000000000L;
	if (clock->invalid == 4)
		out->tv_sec = (time_t)(UINT64_MAX / UINT64_C(1000000000) + 1);
	return 0;
}
static _Thread_local unsigned fail_at, allocations;
struct gate;
static void gate_wait(struct gate *g, bool worker);
static _Thread_local struct gate *allocation_gate;
static _Atomic bool fail_worker_malloc;
static _Atomic(struct tr_channel *) observed_channel;
static _Atomic unsigned blocked_kind, send_attempts[6];
static _Atomic unsigned final_status_attempts[TR_RPC_STATUS_COUNT];
static _Atomic uintptr_t retry_body;
static _Atomic bool observe_real_retry;
void *__wrap_malloc(size_t size)
{
	if ((fail_at && ++allocations == fail_at) ||
	    (tr_execution_in_worker_context() &&
	     atomic_exchange(&fail_worker_malloc, false)))
		return NULL;
	if (allocation_gate) {
		struct gate *gate = allocation_gate;
		allocation_gate = NULL;
		gate_wait(gate, true);
	}
	return __real_malloc(size);
}
void *__wrap_calloc(size_t n, size_t size)
{
	if (fail_at && ++allocations == fail_at)
		return NULL;
	if (allocation_gate) {
		struct gate *gate = allocation_gate;
		allocation_gate = NULL;
		gate_wait(gate, true);
	}
	return __real_calloc(n, size);
}
int __wrap_tr_stream_sendv(struct tr_stream_handle stream,
                           struct tr_buffer *const *buffers, uint32_t count)
{
	uintptr_t body = 0;
	if (stream.channel == atomic_load(&observed_channel) && count &&
	    buffers[0]->len >= TR_RPC_RECORD_HEADER_SIZE &&
	    !memcmp(buffers[0]->data, "TRPC", 4)) {
		unsigned kind = tr_get_le16(buffers[0]->data + 6);
		CHECK(kind >= 1 && kind <= 5);
		atomic_fetch_add(&send_attempts[kind], 1);
		if (kind == TR_RPC_RECORD_FINAL_STATUS) {
			uint32_t status = tr_get_le32(buffers[0]->data + 36);
			CHECK(status < TR_RPC_STATUS_COUNT);
			atomic_fetch_add(&final_status_attempts[status], 1);
		}
		if (kind == TR_RPC_RECORD_MESSAGE && count == 2)
			body = (uintptr_t)buffers[1];
		if (kind == atomic_load(&blocked_kind)) {
			if (kind == TR_RPC_RECORD_MESSAGE && count == 2) {
				uintptr_t prior = atomic_exchange(
				        &retry_body, (uintptr_t)buffers[1]);
				CHECK(prior == 0 ||
				      prior == (uintptr_t)buffers[1]);
			}
			return TR_AGAIN;
		}
	}
	int ret = __real_tr_stream_sendv(stream, buffers, count);
	if (ret == TR_AGAIN && body && atomic_load(&observe_real_retry)) {
		uintptr_t prior = atomic_exchange(&retry_body, body);
		CHECK(prior == 0 || prior == body);
	}
	return ret;
}
struct gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool entered, open;
};
static uint64_t now_ms(void)
{
	struct timespec ts;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
static void wait_count(_Atomic unsigned *value, unsigned target)
{
	uint64_t end = now_ms() + 15000;
	while (atomic_load(value) < target) {
		CHECK(now_ms() < end);
		sched_yield();
	}
}
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
static void gate_wait(struct gate *g, bool worker)
{
	struct timespec end;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += 15;
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	if (worker) {
		g->entered = true;
		CHECK(pthread_cond_broadcast(&g->cond) == 0);
	}
	while (worker ? !g->open : !g->entered)
		CHECK(pthread_cond_timedwait(&g->cond, &g->lock, &end) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}
static void gate_open(struct gate *g)
{
	CHECK(pthread_mutex_lock(&g->lock) == 0);
	g->open = true;
	CHECK(pthread_cond_broadcast(&g->cond) == 0);
	CHECK(pthread_mutex_unlock(&g->lock) == 0);
}
static void gate_destroy(struct gate *g)
{
	CHECK(pthread_cond_destroy(&g->cond) == 0);
	CHECK(pthread_mutex_destroy(&g->lock) == 0);
}
struct fixture;
struct producer_case {
	struct fixture *fixture;
	struct tr_rpc_v2_producer *producer;
	struct tr_rpc_v2_call_id id;
	_Atomic unsigned active, acquired, seen[3], terminals;
	int acquire_status, expected_acquire;
	enum tr_rpc_status status;
	enum tr_rpc_status message_status;
	enum tr_rpc_v2_delivery delivery;
	bool block_open, block_message, sync_after_acquire, zero_response;
};
struct method_context {
	struct fixture *fixture;
	unsigned index;
};
struct business_state {
	_Atomic unsigned active;
	_Atomic unsigned seen[6];
	uint64_t generation;
	uint8_t saved[BODY];
	size_t size;
	bool closed;
};
struct peer {
	struct fixture *fixture;
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	struct tr_rpc_dispatch_handle handles[CAP];
	_Atomic unsigned opened, packets, terminals[CAP], messages[CAP],
	        initial[CAP], trailing[CAP];
	uint8_t metadata[CAP][2][TR_RPC_METADATA_MAX_BYTES];
	uint32_t metadata_size[CAP][2];
	uint32_t status[CAP];
	size_t received_bytes[CAP];
	enum tr_rpc_record_delivery delivery[CAP];
	bool final_accepted[CAP];
	bool server, disabled, hold;
	struct tr_buffer *held;
	struct tr_stream_handle held_stream;
};
enum metadata_mode {
	METADATA_NONE,
	METADATA_BOTH,
	METADATA_FINISH_ERROR,
	METADATA_RETURN_ERROR,
	METADATA_BAD_INITIAL,
	METADATA_BAD_TRAILING,
	METADATA_ALLOC_INITIAL,
	METADATA_ALLOC_TRAILING,
	METADATA_BUDGET_INITIAL,
	METADATA_BUDGET_TRAILING,
	METADATA_BAD_APPEND,
	METADATA_SNAPSHOT,
	METADATA_CANCEL,
};
struct fixture {
	struct peer client, server;
	struct tr_task_executor *executor;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_reactor_timer_handle timer;
	struct tr_rpc_record_dispatch_config config;
	struct tr_rpc_dispatch_method methods[4];
	struct method_context contexts[4];
	struct business_state business[CAP];
	struct gate gate;
	_Atomic bool use_gate;
	_Atomic unsigned reject_method;
	struct tr_rpc_record_dispatch_stats stats;
	bool timer_registered, driving;
	_Atomic bool bad_reply, oversized_reply;
	_Atomic enum metadata_mode metadata_mode;
	uint64_t metadata_reserved;
	_Atomic unsigned drive_turns;
	unsigned batch;
	uint64_t open_due;
	uint64_t call_timeout;
	struct producer_case *producer_case;
	struct tr_rpc_record_producer_domain *producer_domain;
};
static enum tr_rpc_status producer_work(struct tr_rpc_v2_call_id call,
                                        const struct tr_rpc_v2_request *request,
                                        const struct tr_rpc_v2_cancel *cancel,
                                        struct tr_rpc_v2_reply *reply,
                                        void *arg)
{
	struct producer_case *c = arg;
	enum tr_rpc_status result = TR_RPC_STATUS_OK;
	CHECK(tr_execution_in_worker_context());
	CHECK(tr_rpc_record_dispatch_in_callback());
	CHECK(atomic_fetch_add(&c->active, 1) == 0);
	CHECK(request->kind <= TR_RPC_V2_REQUEST_HALF_CLOSE);
	if (request->kind == TR_RPC_V2_REQUEST_OPEN) {
		c->id = call;
		c->acquire_status =
		        tr_rpc_v2_reply_acquire_producer(reply, &c->producer);
		CHECK(c->acquire_status == c->expected_acquire);
		if (c->acquire_status == TR_OK) {
			CHECK(c->producer);
			CHECK(tr_rpc_v2_producer_wait_writable(
			              c->producer, 1) == TR_ERR_STATE);
		} else {
			CHECK(!c->producer);
			result = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		}
		atomic_store(&c->acquired, 1);
		if (c->block_open)
			gate_wait(&c->fixture->gate, true);
		if (c->sync_after_acquire) {
			const struct tr_rpc_bytes empty = {NULL, 0};
			CHECK(tr_rpc_v2_reply_append(reply, &empty) ==
			      TR_ERR_STATE);
		}
	} else {
		CHECK(!memcmp(&call, &c->id, sizeof(call)));
		if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
			for (size_t i = 0; i < request->body.len; ++i)
				CHECK(request->body.data[i] ==
				      (uint8_t)(i * 7 + 3));
			if (c->block_message)
				gate_wait(&c->fixture->gate, true);
			result = c->message_status;
		}
	}
	/* 取消沿用原 work 令牌；故意不合作返回，检验迟到结果只清理。 */
	(void)cancel;
	atomic_fetch_add(&c->seen[request->kind], 1);
	CHECK(atomic_fetch_sub(&c->active, 1) == 1);
	return result;
}
static void producer_terminal(struct tr_rpc_v2_call_id call,
                              enum tr_rpc_status status,
                              enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct producer_case *c = arg;
	CHECK(!memcmp(&call, &c->id, sizeof(call)));
	CHECK(!atomic_load(&c->active));
	c->status = status;
	c->delivery = delivery;
	CHECK(atomic_fetch_add(&c->terminals, 1) == 0);
}
static uint8_t metadata_byte(unsigned phase, size_t index)
{
	return (uint8_t)(index * 13U + phase * 17U + 3U);
}
static int set_metadata(struct tr_rpc_record_reply *reply, unsigned phase)
{
	char key[] = "k";
	uint8_t value[TR_RPC_METADATA_MAX_BYTES - 4U];
	struct tr_rpc_metadata entry = {key, value, sizeof(value)};
	for (size_t i = 0; i < sizeof(value); ++i)
		value[i] = metadata_byte(phase, i);
	int ret = tr_rpc_record_reply_set_metadata(
	        reply, (enum tr_rpc_v2_metadata_phase)phase, &entry, 1);
	/* 数组、键和值全是当前栈存储；setter 返回后立即破坏源，发送重试
	 * 只能依赖拥有型副本，不能借用任何一种输入地址。 */
	memset(value, 0xdd, sizeof(value));
	memset(key, 0xdd, sizeof(key));
	memset(&entry, 0xdd, sizeof(entry));
	return ret;
}
static enum tr_rpc_status metadata_work(struct fixture *f, unsigned kind,
                                        struct tr_rpc_record_reply *reply)
{
	enum metadata_mode mode = atomic_load(&f->metadata_mode);
	if (mode == METADATA_NONE)
		return TR_RPC_STATUS_OK;
	if (mode == METADATA_SNAPSHOT) {
		if (kind == TR_RPC_RECORD_OPEN)
			CHECK(set_metadata(reply, TR_RPC_V2_METADATA_INITIAL) ==
			      TR_OK);
		else if (kind == TR_RPC_RECORD_HALF_CLOSE)
			CHECK(set_metadata(reply, TR_RPC_V2_METADATA_INITIAL) ==
			      TR_ERR_STATE);
		return TR_RPC_STATUS_OK;
	}
	if (kind != (mode == METADATA_CANCEL ? TR_RPC_RECORD_MESSAGE
	                                     : TR_RPC_RECORD_HALF_CLOSE))
		return TR_RPC_STATUS_OK;
	for (unsigned phase = 0; phase < 2; ++phase) {
		bool invalid = (mode == METADATA_BAD_INITIAL && phase == 0) ||
		               (mode == METADATA_BAD_TRAILING && phase == 1);
		bool allocation =
		        (mode == METADATA_ALLOC_INITIAL && phase == 0) ||
		        (mode == METADATA_ALLOC_TRAILING && phase == 1);
		bool budget = (mode == METADATA_BUDGET_INITIAL && phase == 0) ||
		              (mode == METADATA_BUDGET_TRAILING && phase == 1);
		if (invalid) {
			const struct tr_rpc_metadata duplicate[] = {
			        {"dup", NULL, 0}, {"dup", NULL, 0}};
			CHECK(tr_rpc_record_reply_set_metadata(
			              reply,
			              (enum tr_rpc_v2_metadata_phase)phase,
			              duplicate, 2) == TR_ERR_BAD_FLAGS);
			return TR_RPC_STATUS_OK;
		}
		if (allocation || budget) {
			gate_wait(&f->gate, true);
			uint64_t before =
			        atomic_load(&f->server.budget.current_bytes);
			if (allocation)
				atomic_store(&fail_worker_malloc, true);
			CHECK(set_metadata(reply, phase) == TR_ERR_NOMEM);
			CHECK(atomic_load(&f->server.budget.current_bytes) ==
			      before);
			if (budget) {
				CHECK(f->metadata_reserved != 0);
				CHECK(tr_memory_budget_release(
				              &f->server.budget,
				              f->metadata_reserved) == TR_OK);
			}
			return TR_RPC_STATUS_OK;
		}
		CHECK(set_metadata(reply, phase) == TR_OK);
	}
	if (mode == METADATA_BAD_APPEND) {
		CHECK(tr_rpc_record_reply_append(reply, NULL, BODY + 1U) ==
		      TR_ERR_BAD_LENGTH);
		/* 首次 API 错误不能被随后另一个非 OK 业务状态覆盖。 */
		return TR_RPC_STATUS_PERMISSION_DENIED;
	}
	if (mode == METADATA_FINISH_ERROR)
		CHECK(tr_rpc_record_reply_finish(
		              reply, TR_RPC_STATUS_PERMISSION_DENIED) == TR_OK);
	if (mode == METADATA_RETURN_ERROR)
		return TR_RPC_STATUS_PERMISSION_DENIED;
	if (mode == METADATA_CANCEL)
		CHECK(tr_rpc_record_reply_finish(reply, TR_RPC_STATUS_OK) ==
		      TR_OK);
	return TR_RPC_STATUS_OK;
}
static enum tr_rpc_status handler(struct tr_rpc_dispatch_id id,
                                  const struct tr_rpc_record_view *view,
                                  const struct tr_task_cancel *cancel,
                                  struct tr_rpc_record_reply *reply, void *arg)
{
	struct method_context *m = arg;
	struct fixture *f = m->fixture;
	struct business_state *s = &f->business[id.slot];
	unsigned kind = view->header.kind;
	enum tr_rpc_status status = TR_RPC_STATUS_OK;
	CHECK(tr_execution_in_worker_context());
	CHECK(!tr_reactor_in_owner_context());
	CHECK(atomic_fetch_add(&s->active, 1) == 0);
	if (kind == TR_RPC_RECORD_OPEN) {
		s->generation = id.generation;
		s->closed = false;
		s->size = 0;
		CHECK(view->header.method_id ==
		      f->methods[m->index].descriptor.method_id);
		CHECK(view->header.metadata_size == 7);
		const uint8_t expected[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
		CHECK(!memcmp(view->metadata, expected, sizeof(expected)));
	} else {
		CHECK(id.generation == s->generation && !s->closed);
	}
	if (kind == TR_RPC_RECORD_MESSAGE) {
		if (atomic_exchange(&f->use_gate, false))
			gate_wait(&f->gate, true);
		/* 原RX已在owner返回前被写为0xdd并释放。这里只能读拥有型副本。
		 */
		for (size_t i = 0; i < view->header.decoded_size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7 + 3));
		s->size = view->header.decoded_size;
		if (s->size)
			memcpy(s->saved, view->body, s->size);
		if (f->methods[m->index].descriptor.response_cardinality ==
		    TR_RPC_MANY)
			for (unsigned i = 0; i < f->batch; ++i) {
				int ret = tr_rpc_record_reply_append(
				        reply, view->body, s->size);
				if (ret != TR_OK)
					status =
					        TR_RPC_STATUS_RESOURCE_EXHAUSTED;
			}
	}
	if (kind == TR_RPC_RECORD_HALF_CLOSE) {
		s->closed = true;
		if (f->methods[m->index].descriptor.response_cardinality ==
		            TR_RPC_ONE &&
		    !f->bad_reply) {
			int ret = tr_rpc_record_reply_append(reply, s->saved,
			                                     s->size);
			if (ret != TR_OK)
				status = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
		}
		if (f->oversized_reply) {
			CHECK(tr_rpc_record_reply_append(reply, NULL,
			                                 BODY + 1) ==
			      TR_ERR_BAD_LENGTH);
			/* 故意忽略错误并返回OK，核心也不得交付截断成功。 */
		}
	}
	if (atomic_load(&f->reject_method) == m->index + 1 &&
	    kind == TR_RPC_RECORD_OPEN) {
		CHECK(tr_rpc_record_reply_finish(
		              reply, TR_RPC_STATUS_PERMISSION_DENIED) == TR_OK);
		CHECK(tr_rpc_record_reply_finish(reply, TR_RPC_STATUS_OK) ==
		      TR_ERR_STATE);
		CHECK(tr_rpc_record_reply_append(reply, NULL, 0) ==
		      TR_ERR_STATE);
	}
	if (atomic_load(&f->metadata_mode) != METADATA_NONE)
		status = metadata_work(f, kind, reply);
	/* 测试允许不合作业务返回成功，取消后的私有结果仍必须被引擎丢弃。 */
	(void)cancel;
	atomic_fetch_add(&s->seen[kind], 1);
	CHECK(atomic_fetch_sub(&s->active, 1) == 1);
	return status;
}
static void server_terminal(struct tr_rpc_dispatch_handle handle,
                            struct tr_stream_handle stream,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery,
                            bool final_accepted, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_dispatch *same = f->dispatch;
	CHECK(stream.slot < CAP && handle.dispatch == f->dispatch);
	CHECK(handle.id.generation ==
	      f->server.handles[stream.slot].id.generation);
	CHECK(atomic_load(f->producer_case
	                          ? &f->producer_case->active
	                          : &f->business[handle.id.slot].active) == 0);
	CHECK((delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (delivery == TR_RPC_RECORD_ON_OWNER) {
		struct tr_rpc_dispatch_result result;
		CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &result) ==
		      TR_OK);
		CHECK(result.terminal && result.status == status &&
		      result.final_accepted == final_accepted);
		CHECK(tr_rpc_record_dispatch_destroy_on_owner(&same) ==
		      TR_ERR_STATE);
		CHECK(tr_rpc_record_dispatch_retire_on_owner(handle) ==
		      TR_ERR_STATE);
	} else {
		CHECK(status == TR_RPC_STATUS_CANCELLED);
		CHECK(tr_rpc_record_dispatch_reap_stopped(&same) ==
		      TR_ERR_STATE);
	}
	CHECK(same == f->dispatch);
	f->server.status[stream.slot] = (uint32_t)status;
	f->server.delivery[stream.slot] = delivery;
	f->server.final_accepted[stream.slot] = final_accepted;
	CHECK(atomic_fetch_add(&f->server.terminals[stream.slot], 1) == 0);
}
static unsigned client_slot(struct peer *p, struct tr_rpc_record_stream *call)
{
	for (unsigned i = 0; i < CAP; ++i)
		if (p->calls[i] == call)
			return i;
	CHECK(0);
	return 0;
}
static void client_received(struct tr_rpc_record_stream *call,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		CHECK(atomic_load(&p->initial[i]) == 1 &&
		      !atomic_load(&p->trailing[i]));
		for (size_t j = 0; j < view->header.decoded_size; ++j)
			CHECK(view->body[j] == (uint8_t)(j * 7 + 3));
		p->received_bytes[i] += view->header.decoded_size;
		atomic_fetch_add(&p->messages[i], 1);
	} else {
		CHECK(view->header.kind == TR_RPC_RECORD_INITIAL_METADATA ||
		      view->header.kind == TR_RPC_RECORD_FINAL_STATUS);
		unsigned phase =
		        view->header.kind == TR_RPC_RECORD_INITIAL_METADATA
		                ? 0U
		                : 1U;
		CHECK(view->header.metadata_size <= TR_RPC_METADATA_MAX_BYTES);
		p->metadata_size[i][phase] = view->header.metadata_size;
		if (view->header.metadata_size)
			memcpy(p->metadata[i][phase], view->metadata,
			       view->header.metadata_size);
		if (phase == 0) {
			CHECK(!atomic_load(&p->messages[i]) &&
			      !atomic_load(&p->trailing[i]));
			CHECK(atomic_fetch_add(&p->initial[i], 1) == 0);
		} else {
			CHECK(!atomic_load(&p->terminals[i]));
			CHECK(atomic_fetch_add(&p->trailing[i], 1) == 0);
		}
	}
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = client_slot(p, call);
	p->status[i] = status;
	p->delivery[i] = delivery;
	CHECK(atomic_fetch_add(&p->terminals[i], 1) == 0);
}
static enum tr_stream_data_disposition on_data(struct tr_stream_handle stream,
                                               uint64_t id, struct tr_buffer *b,
                                               void *arg)
{
	struct peer *p = arg;
	(void)id;
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(stream.slot < CAP);
	if (p->server) {
		int ret = tr_rpc_record_dispatch_receive_on_owner(
		        p->fixture->dispatch, stream, b->data, b->len);
		CHECK(ret == TR_OK || ret == TR_ERR_STATE ||
		      ret == TR_ERR_NOMEM);
		memset(b->data, 0xdd, b->len);
	} else if (p->calls[stream.slot]) {
		CHECK(tr_rpc_record_stream_receive_on_owner(
		              p->calls[stream.slot], stream, b->data, b->len) ==
		      TR_OK);
	}
	atomic_fetch_add(&p->packets, 1);
	if (p->hold) {
		CHECK(!p->held);
		p->held = b;
		p->held_stream = stream;
		return TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	return TR_STREAM_DATA_RELEASE;
}
static void on_event(struct tr_stream_handle s, enum tr_stream_event e,
                     int status, void *arg)
{
	struct peer *p = arg;
	if (p->disabled)
		return;
	CHECK(s.slot < CAP);
	if (e == TR_STREAM_EVENT_OPENED) {
		p->streams[s.slot] = s;
		if (p->server) {
			int ret =
			        p->fixture->open_due
			                ? tr_rpc_record_dispatch_admit_until_on_owner(
			                          p->fixture->dispatch, s,
			                          p->fixture->open_due,
			                          &p->handles[s.slot])
			                : tr_rpc_record_dispatch_admit_on_owner(
			                          p->fixture->dispatch, s,
			                          &p->handles[s.slot]);
			CHECK(ret == TR_OK);
		}
		atomic_fetch_add(&p->opened, 1);
	} else if (p->server && p->handles[s.slot].dispatch) {
		CHECK(tr_rpc_record_dispatch_event_on_owner(
		              p->fixture->dispatch, s, e, status) == TR_OK);
	} else if (!p->server && p->calls[s.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[s.slot], s,
		                                          e, status) == TR_OK);
	}
}
static void tcp_pair(int *a, int *b)
{
	int listener = -1, ret;
	uint16_t port;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) == TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, a);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	uint64_t until = now_ms() + 15000;
	while (tr_tcp_accept(listener, b) == TR_AGAIN) {
		struct pollfd fd = {listener, POLLIN, 0};
		CHECK(now_ms() < until && poll(&fd, 1, 100) >= 0);
	}
	CHECK(*b >= 0);
	if (ret == TR_IN_PROGRESS) {
		struct pollfd fd = {*a, POLLOUT, 0};
		CHECK(poll(&fd, 1, 1000) == 1);
		CHECK(tr_tcp_finish_connect(*a) == TR_OK);
	}
	CHECK(close(listener) == 0);
}
static void peer_init(struct peer *p, struct fixture *f, int fd, bool server,
                      uint64_t window)
{
	struct tr_reactor_config rc = {.max_connections = 4,
	                               .command_capacity = 64,
	                               .tx_item_capacity = 64,
	                               .control_tx_item_capacity = 64,
	                               .rx_buffer_count = 16,
	                               .rx_buffer_size = 256,
	                               .max_payload_len = 256};
	struct tr_channel_config cc = {
	        .role = server ? TR_CHANNEL_SERVER : TR_CHANNEL_CLIENT,
	        .mode = TR_CHANNEL_SHARED_CONNECTION,
	        .feature_bits = TR_CHANNEL_FEATURE_STREAM_RESET,
	        .max_streams = CAP,
	        .initial_window_bytes = window,
	        .window_update_threshold_bytes = 1,
	        .max_message_bytes = BODY + 1024};
	struct tr_conn_handle conn;
	*p = (struct peer){.fixture = f, .server = server};
	atomic_init(&p->opened, 0);
	atomic_init(&p->packets, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->messages[i], 0);
		atomic_init(&p->initial[i], 0);
		atomic_init(&p->trailing[i], 0);
	}
	tr_memory_budget_init(&p->budget, 16 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, CAP, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	CHECK(tr_channel_create(&cc, conn, conn, on_data, on_event, NULL, p,
	                        &p->channel) == TR_OK);
}
static uint64_t drive(void *arg, uint64_t now)
{
	struct fixture *f = arg;
	size_t delivered;
	if (f->driving) {
		atomic_fetch_add(&f->drive_turns, 1);
		CHECK(tr_task_executor_dispatch(f->executor, 2, &delivered) ==
		      TR_OK);
		if (f->dispatch)
			CHECK(tr_rpc_record_dispatch_pump_on_owner(f->dispatch,
			                                           2) == TR_OK);
	}
	return now + 1000000;
}
static int create_dispatch(void *arg)
{
	struct fixture *f = arg;
	int ret = tr_rpc_record_dispatch_create_on_owner(
	        f->server.channel, f->executor, f->methods, 4, &f->config,
	        server_terminal, f, &f->server.budget, &f->dispatch);
	if (ret == TR_OK && f->producer_domain)
		ret = tr_rpc_record_dispatch_bind_producers_on_owner(
		        f->dispatch, f->producer_domain);
	return ret;
}
static int start_server(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_config cfg = {2, 4};
	CHECK(tr_task_executor_create(&cfg, &f->server.budget, &f->executor) ==
	      TR_OK);
	CHECK(tr_task_executor_start(f->executor) == TR_OK);
	CHECK(create_dispatch(f) == TR_OK);
	CHECK(tr_reactor_timer_register(f->server.reactor, drive, f,
	                                &f->timer) == TR_OK);
	f->timer_registered = true;
	f->driving = true;
	CHECK(tr_reactor_timer_arm(f->timer, (now_ms() + 1) * 1000000) ==
	      TR_OK);
	return TR_OK;
}
static void fixture_init_mode(struct fixture *f, uint64_t window,
                              uint32_t pending, struct producer_case *producer,
                              size_t producer_capacity)
{
	int a = -1, b = -1;
	*f = (struct fixture){.config = {CAP, pending, 4, BODY * 4},
	                      .batch = 2,
	                      .producer_case = producer};
	if (producer)
		producer->fixture = f;
	gate_init(&f->gate);
	atomic_init(&f->use_gate, false);
	atomic_init(&f->reject_method, 0);
	atomic_init(&f->bad_reply, false);
	atomic_init(&f->oversized_reply, false);
	atomic_init(&f->metadata_mode, METADATA_NONE);
	atomic_init(&f->drive_turns, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&f->business[i].active, 0);
		for (unsigned j = 0; j < 6; ++j)
			atomic_init(&f->business[i].seen[j], 0);
	}
	for (unsigned i = 0; i < 4; ++i) {
		f->contexts[i] = (struct method_context){f, i};
		f->methods[i] = (struct tr_rpc_dispatch_method){
		        .descriptor = {7, 9 + i,
		                       i < 2 ? TR_RPC_ONE : TR_RPC_MANY,
		                       (i & 1) ? TR_RPC_MANY : TR_RPC_ONE,
		                       TR_RPC_CODEC_RAW, TR_RPC_CODEC_RAW,
		                       TR_LANE_BULK, BODY, BODY},
		        .work = producer ? NULL : handler,
		        .arg = producer ? (void *)producer : &f->contexts[i],
		        .v2_work = producer ? producer_work : NULL,
		        .v2_terminal = producer ? producer_terminal : NULL};
		if (producer && producer->zero_response)
			f->methods[i].descriptor.max_response_bytes = 0;
	}
	tcp_pair(&a, &b);
	peer_init(&f->client, f, a, false, window);
	peer_init(&f->server, f, b, true, window);
	if (producer)
		CHECK(tr_rpc_record_producer_domain_create(
		              producer_capacity, &f->server.budget,
		              &f->producer_domain) == TR_OK);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_reactor_call(f->server.reactor, start_server, f) == TR_OK);
	atomic_store(&observed_channel, f->server.channel);
	atomic_store(&blocked_kind, 0);
	atomic_store(&retry_body, 0);
	atomic_store(&observe_real_retry, false);
	for (unsigned i = 0; i < 6; ++i)
		atomic_store(&send_attempts[i], 0);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		atomic_store(&final_status_attempts[i], 0);
}
static void fixture_init(struct fixture *f, uint64_t window, uint32_t pending)
{
	fixture_init_mode(f, window, pending, NULL, 0);
}
struct client_create {
	struct fixture *f;
	struct tr_stream_handle stream;
	unsigned method;
};
static int client_bind(void *arg)
{
	struct client_create *r = arg;
	struct peer *p = &r->f->client;
	struct tr_rpc_record_stream_config config = {
	        TR_RPC_RECORD_REQUEST_DIRECTION,
	        r->f->methods[r->method].descriptor};
	struct tr_rpc_record_stream_callbacks callbacks = {client_received,
	                                                   client_terminal, p};
	return tr_rpc_record_stream_create_on_owner(r->stream, &config,
	                                            &callbacks, &p->budget,
	                                            &p->calls[r->stream.slot]);
}
static unsigned open_call(struct fixture *f, unsigned method)
{
	struct tr_stream_handle stream;
	unsigned count = atomic_load(&f->client.opened) + 1;
	CHECK(tr_stream_open(f->client.channel, TR_LANE_BULK, &stream) ==
	      TR_OK);
	wait_count(&f->client.opened, count);
	wait_count(&f->server.opened, count);
	struct client_create r = {f, stream, method};
	CHECK(tr_reactor_call(f->client.reactor, client_bind, &r) == TR_OK);
	return stream.slot;
}
struct bytes {
	struct tr_buffer b;
	uint8_t data[];
};
static void bytes_free(struct tr_buffer *b)
{
	free(b);
}
static struct tr_buffer *body_new(size_t size)
{
	struct bytes *b = malloc(sizeof(*b) + size);
	CHECK(b);
	b->b = (struct tr_buffer){.data = b->data,
	                          .capacity = (uint32_t)size,
	                          .len = (uint32_t)size,
	                          .release_cb = bytes_free};
	for (size_t i = 0; i < size; ++i)
		b->data[i] = (uint8_t)(i * 7 + 3);
	return &b->b;
}
struct send_request {
	struct fixture *f;
	unsigned slot;
	struct tr_rpc_record_header h;
	struct tr_buffer *b;
};
static int send_on_owner(void *arg)
{
	struct send_request *r = arg;
	const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
	return tr_rpc_record_stream_send_on_owner(
	        r->f->client.calls[r->slot], &r->h,
	        r->h.metadata_size ? metadata : NULL, &r->b);
}
static void send_record(struct fixture *f, unsigned slot, unsigned method,
                        enum tr_rpc_record_kind kind, size_t size)
{
	struct send_request r = {.f = f, .slot = slot, .h = {.kind = kind}};
	if (kind == TR_RPC_RECORD_OPEN) {
		r.h.service_id = 7;
		r.h.method_id = 9 + method;
		r.h.metadata_size = 7;
		r.h.remaining_timeout_ns = f->call_timeout;
	} else if (kind == TR_RPC_RECORD_MESSAGE) {
		r.h.codec_id = TR_RPC_CODEC_RAW;
		r.h.encoded_size = r.h.decoded_size = (uint32_t)size;
		r.b = body_new(size);
	}
	int ret;
	uint64_t until = now_ms() + 15000;
	while ((ret = tr_reactor_call(f->client.reactor, send_on_owner, &r)) ==
	       TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK && !r.b);
}
static void request(struct fixture *f, unsigned slot, unsigned method,
                    unsigned messages, size_t size)
{
	send_record(f, slot, method, TR_RPC_RECORD_OPEN, 0);
	for (unsigned i = 0; i < messages; ++i)
		send_record(f, slot, method, TR_RPC_RECORD_MESSAGE, size);
	send_record(f, slot, method, TR_RPC_RECORD_HALF_CLOSE, 0);
}
static int snapshot(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                 &f->stats);
}
static int disable_peer(void *arg)
{
	((struct peer *)arg)->disabled = true;
	return TR_OK;
}
static int disable_driver(void *arg)
{
	struct fixture *f = arg;
	f->driving = false;
	if (f->timer_registered) {
		CHECK(tr_reactor_timer_unregister(f->timer) == TR_OK);
		f->timer_registered = false;
	}
	return TR_OK;
}
static int destroy_dispatch(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_destroy_on_owner(&f->dispatch);
}
static int destroy_executor(void *arg)
{
	struct fixture *f = arg;
	size_t count;
	CHECK(tr_task_executor_stop(f->executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_dispatch(f->executor, 8, &count) == TR_OK);
	int ret = tr_task_executor_join(f->executor);
	return ret == TR_OK ? tr_task_executor_destroy(&f->executor) : ret;
}
static void fixture_destroy(struct fixture *f, bool stopped)
{
	atomic_store(&observed_channel, NULL);
	if (!stopped) {
		CHECK(tr_reactor_call(f->server.reactor, disable_peer,
		                      &f->server) == TR_OK);
		uint64_t until = now_ms() + 15000;
		int ret;
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_dispatch, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		CHECK(tr_reactor_call(f->server.reactor, disable_driver, f) ==
		      TR_OK);
		while ((ret = tr_reactor_call(f->server.reactor,
		                              destroy_executor, f)) ==
		       TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	}
	CHECK(!f->dispatch && !f->executor);
	CHECK(tr_reactor_call(f->client.reactor, disable_peer, &f->client) ==
	      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.calls[i]) == TR_OK);
	struct peer *peers[] = {&f->client, &f->server};
	for (unsigned i = 0; i < 2; ++i) {
		CHECK(tr_channel_destroy(peers[i]->channel) == TR_OK);
		CHECK(tr_reactor_destroy(peers[i]->reactor) == TR_OK);
		CHECK(tr_buffer_pool_destroy(&peers[i]->pool) == TR_OK);
		if (peers[i]->server && f->producer_domain)
			CHECK(tr_rpc_record_producer_domain_close(
			              &f->producer_domain) == TR_OK);
		CHECK(atomic_load(&peers[i]->budget.current_bytes) == 0);
	}
	gate_destroy(&f->gate);
}
static void check_metadata(struct peer *peer, unsigned slot, unsigned phase,
                           bool populated)
{
	CHECK(peer->metadata_size[slot][phase] ==
	      (populated ? TR_RPC_METADATA_MAX_BYTES : 0U));
	if (!populated)
		return;
	const uint8_t *data = peer->metadata[slot][phase];
	CHECK(data[0] == 1 && data[1] == 0xfc && data[2] == 1 &&
	      data[3] == 'k');
	for (size_t i = 4; i < TR_RPC_METADATA_MAX_BYTES; ++i)
		CHECK(data[i] == metadata_byte(phase, i - 4));
}
static void test_metadata_encoding(void)
{
	uint8_t out[TR_RPC_METADATA_MAX_BYTES], value[509] = {0};
	uint32_t size = 99;
	const uint8_t binary[] = {0, 0xff, 1};
	const struct tr_rpc_metadata entries[] = {{"empty", NULL, 0},
	                                          {"b", binary, 3}};
	const uint8_t expected[] = {5, 0, 0, 'e', 'm', 'p',  't', 'y',
	                            1, 3, 0, 'b', 0,   0xff, 1};
	CHECK(tr_rpc_v2_metadata_encode(NULL, 0, out, &size) == TR_OK &&
	      size == 0);
	CHECK(tr_rpc_v2_metadata_encode(entries, 2, out, &size) == TR_OK &&
	      size == sizeof(expected) &&
	      memcmp(out, expected, sizeof(expected)) == 0);
	struct tr_rpc_metadata boundary = {"k", value, 508};
	CHECK(tr_rpc_v2_metadata_encode(&boundary, 1, out, &size) == TR_OK &&
	      size == sizeof(out));
	boundary.value_len = 509;
	size = 99;
	CHECK(tr_rpc_v2_metadata_encode(&boundary, 1, out, &size) ==
	      TR_ERR_BAD_LENGTH);
	CHECK(size == 99);
	const struct tr_rpc_metadata duplicate[] = {{"k", NULL, 0},
	                                            {"k", NULL, 0}};
	CHECK(tr_rpc_v2_metadata_encode(duplicate, 2, out, &size) ==
	      TR_ERR_BAD_FLAGS);
	const struct tr_rpc_metadata invalid[] = {{NULL, NULL, 0},
	                                          {"k", NULL, 1},
	                                          {"", NULL, 0},
	                                          {"Upper", NULL, 0},
	                                          {":reserved", NULL, 0}};
	const int errors[] = {TR_ERR_INVALID, TR_ERR_INVALID, TR_ERR_BAD_LENGTH,
	                      TR_ERR_BAD_FLAGS, TR_ERR_BAD_FLAGS};
	for (unsigned i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
		CHECK(tr_rpc_v2_metadata_encode(&invalid[i], 1, out, &size) ==
		      errors[i]);
		CHECK(size == 99);
	}
	CHECK(tr_rpc_v2_metadata_encode(NULL, 1, out, &size) == TR_ERR_INVALID);
	CHECK(tr_rpc_v2_metadata_encode(entries, 129, out, &size) ==
	      TR_ERR_INVALID);
	CHECK(size == 99);
}
static int reserve_metadata_budget(void *arg)
{
	struct fixture *f = arg;
	CHECK(snapshot(f) == TR_OK && f->stats.submitted_calls == 1);
	uint64_t current = atomic_load(&f->server.budget.current_bytes);
	CHECK(current < f->server.budget.limit_bytes);
	f->metadata_reserved = f->server.budget.limit_bytes - current;
	CHECK(tr_memory_budget_reserve(&f->server.budget,
	                               f->metadata_reserved) == TR_OK);
	return TR_OK;
}
static void test_metadata_error(enum metadata_mode mode)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&f.metadata_mode, mode);
	if (mode == METADATA_BOTH)
		atomic_store(&f.bad_reply, true);
	unsigned slot = open_call(&f, 0);
	request(&f, slot, 0, 1, 40);
	bool budget = mode == METADATA_BUDGET_INITIAL ||
	              mode == METADATA_BUDGET_TRAILING;
	bool allocation = mode == METADATA_ALLOC_INITIAL ||
	                  mode == METADATA_ALLOC_TRAILING;
	if (budget || allocation) {
		gate_wait(&f.gate, false);
		/* owner 屏障确保 RX/入队清理已经完成；只对正在运行的批次
		 * 施加资源故障，不让 FINAL 的前缀分配误中故障。 */
		CHECK(tr_reactor_call(f.server.reactor,
		                      budget ? reserve_metadata_budget
		                             : snapshot,
		                      &f) == TR_OK);
		gate_open(&f.gate);
	}
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	bool populated = mode == METADATA_FINISH_ERROR ||
	                 mode == METADATA_RETURN_ERROR || mode == METADATA_BOTH;
	uint32_t expected = mode == METADATA_BOTH ? TR_RPC_STATUS_INTERNAL
	                    : populated ? TR_RPC_STATUS_PERMISSION_DENIED
	                    : (mode == METADATA_BAD_INITIAL ||
	                       mode == METADATA_BAD_TRAILING)
	                            ? TR_RPC_STATUS_INVALID_ARGUMENT
	                            : TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	CHECK(f.client.status[slot] == expected &&
	      f.server.status[slot] == expected &&
	      f.server.final_accepted[slot]);
	CHECK(!atomic_load(&f.client.messages[slot]) &&
	      atomic_load(&f.client.initial[slot]) == (populated ? 1U : 0U) &&
	      atomic_load(&f.client.trailing[slot]) == 1);
	check_metadata(&f.client, slot, 0, populated);
	check_metadata(&f.client, slot, 1, populated);
	if (budget)
		CHECK(atomic_load(&f.server.budget.rejection_events) != 0);
	fixture_destroy(&f, false);
}
static void test_metadata_queued_phase(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&f.metadata_mode, METADATA_SNAPSHOT);
	unsigned slot = open_call(&f, 0);
	atomic_store(&blocked_kind, TR_RPC_RECORD_INITIAL_METADATA);
	request(&f, slot, 0, 1, 40);
	wait_count(&send_attempts[TR_RPC_RECORD_INITIAL_METADATA], 10);
	uint64_t until = now_ms() + 15000;
	do {
		CHECK(now_ms() < until);
		CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	} while (f.stats.held_events != 3);
	unsigned route = f.server.handles[slot].id.slot;
	CHECK(f.stats.reply_calls == 1 &&
	      atomic_load(&f.business[route].seen[TR_RPC_RECORD_OPEN]) == 1 &&
	      !atomic_load(&f.business[route].seen[TR_RPC_RECORD_MESSAGE]) &&
	      !atomic_load(&f.business[route].seen[TR_RPC_RECORD_HALF_CLOSE]) &&
	      !atomic_load(&f.client.initial[slot]));
	/* HALF_CLOSE 在 INITIAL 被接受之前就已入队，但必须在真正执行时
	 * 看到阶段关闭，不能沿用接收时的旧状态重新设置 INITIAL。 */
	atomic_store(&blocked_kind, 0);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_FAILED_PRECONDITION &&
	      f.server.status[slot] == TR_RPC_STATUS_FAILED_PRECONDITION &&
	      f.server.final_accepted[slot]);
	CHECK(atomic_load(&f.client.initial[slot]) == 1 &&
	      !atomic_load(&f.client.messages[slot]) &&
	      atomic_load(&f.client.trailing[slot]) == 1);
	check_metadata(&f.client, slot, 0, true);
	check_metadata(&f.client, slot, 1, false);
	fixture_destroy(&f, false);
}
static void test_shapes(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	for (unsigned m = 0; m < 4; ++m) {
		unsigned slot = open_call(&f, m);
		unsigned count = m < 2 ? 1 : 2;
		request(&f, slot, m, count, m == 2 ? 0 : 1024);
		wait_count(&f.client.terminals[slot], 1);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
		CHECK(f.server.status[slot] == TR_RPC_STATUS_OK &&
		      f.server.final_accepted[slot]);
		CHECK(atomic_load(&f.client.messages[slot]) ==
		      ((m & 1) ? count * 2 : 1));
		unsigned route = f.server.handles[slot].id.slot;
		CHECK(atomic_load(
		              &f.business[route].seen[TR_RPC_RECORD_OPEN]) ==
		      1);
		CHECK(atomic_load(
		              &f.business[route].seen[TR_RPC_RECORD_MESSAGE]) ==
		      count);
		CHECK(atomic_load(&f.business[route]
		                           .seen[TR_RPC_RECORD_HALF_CLOSE]) ==
		      1);
	}
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 0 && f.stats.terminal_calls == 4);
	fixture_destroy(&f, false);
}
static void test_parallel_and_owned_data(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned a = open_call(&f, 0), b = open_call(&f, 2);
	atomic_store(&f.use_gate, true);
	request(&f, a, 0, 1, BODY);
	gate_wait(&f.gate, false);
	request(&f, b, 2, 0, 0);
	wait_count(&f.client.terminals[b], 1);
	CHECK(!atomic_load(&f.client.terminals[a]));
	CHECK(atomic_load(&f.business[f.server.handles[a].id.slot]
	                           .seen[TR_RPC_RECORD_HALF_CLOSE]) == 0);
	gate_open(&f.gate);
	wait_count(&f.client.terminals[a], 1);
	wait_count(&f.server.terminals[a], 1);
	CHECK(f.client.received_bytes[a] == BODY &&
	      f.client.status[b] == TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static void test_send_retry(unsigned kind)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&f.metadata_mode, METADATA_BOTH);
	unsigned slot = open_call(&f, 0);
	atomic_store(&blocked_kind, kind);
	request(&f, slot, 0, 1, 80);
	wait_count(&send_attempts[kind], 10);
	CHECK(!atomic_load(&f.client.terminals[slot]) &&
	      !atomic_load(&f.server.terminals[slot]));
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.reply_calls == 1 && f.stats.held_events == 1);
	unsigned route = f.server.handles[slot].id.slot;
	CHECK(atomic_load(&f.business[route].seen[TR_RPC_RECORD_HALF_CLOSE]) ==
	      1);
	atomic_store(&blocked_kind, 0);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(atomic_load(&f.client.messages[slot]) == 1 &&
	      f.client.received_bytes[slot] == 80);
	CHECK(atomic_load(&f.client.initial[slot]) == 1);
	CHECK(atomic_load(&f.client.trailing[slot]) == 1);
	check_metadata(&f.client, slot, 0, true);
	check_metadata(&f.client, slot, 1, true);
	CHECK(atomic_load(&f.business[route].seen[TR_RPC_RECORD_HALF_CLOSE]) ==
	      1);
	fixture_destroy(&f, false);
}
static int hold_client(void *arg)
{
	((struct fixture *)arg)->client.hold = true;
	return TR_OK;
}
static int release_client(void *arg)
{
	struct fixture *f = arg;
	f->client.hold = false;
	CHECK(f->client.held);
	int ret = tr_stream_release_payload(f->client.held_stream,
	                                    f->client.held);
	f->client.held = NULL;
	return ret;
}
static void test_real_window(void)
{
	struct fixture f;
	fixture_init(&f, 128, 8);
	unsigned slot = open_call(&f, 0);
	CHECK(tr_reactor_call(f.client.reactor, hold_client, &f) == TR_OK);
	request(&f, slot, 0, 1, 80);
	wait_count(&f.client.initial[slot], 1);
	wait_count(&send_attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(!atomic_load(&f.client.messages[slot]));
	CHECK(!atomic_load(&f.server.terminals[slot]));
	CHECK(tr_reactor_call(f.client.reactor, release_client, &f) == TR_OK);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
	fixture_destroy(&f, false);
}
static int cancel_first(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_dispatch_abort_on_owner(f->server.handles[0],
	                                             TR_RPC_STATUS_CANCELLED);
}
static int reset_first(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_dispatch_handle handle = f->server.handles[0];
	struct tr_rpc_dispatch_result before, after;
	struct tr_rpc_record_dispatch_stats stats_before, stats_after;
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &before) == TR_OK);
	CHECK(!before.terminal && !before.final_accepted);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(
	              f->dispatch, &stats_before) == TR_OK);
	CHECK(tr_rpc_record_dispatch_event_on_owner(
	              f->dispatch, f->server.streams[0], TR_STREAM_EVENT_RESET,
	              TR_AGAIN) == TR_ERR_INVALID);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &after) == TR_OK);
	CHECK(after.status == before.status &&
	      after.terminal == before.terminal &&
	      after.final_accepted == before.final_accepted);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                &stats_after) == TR_OK);
	CHECK(stats_after.held_events == stats_before.held_events &&
	      stats_after.terminal_calls == stats_before.terminal_calls);
	CHECK(tr_rpc_record_dispatch_event_on_owner(
	              f->dispatch, f->server.streams[0], TR_STREAM_EVENT_RESET,
	              TR_ERR_CLOSED) == TR_OK);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &after) == TR_OK);
	CHECK(after.terminal && after.status == TR_RPC_STATUS_CANCELLED &&
	      !after.final_accepted);
	CHECK(!atomic_load(&f->server.terminals[0]) &&
	      atomic_load(&f->business[handle.id.slot].active) == 1);
	CHECK(tr_rpc_record_dispatch_retire_on_owner(handle) == TR_ERR_STATE);
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, NULL) ==
	      TR_ERR_INVALID);
	++handle.id.generation;
	CHECK(tr_rpc_record_dispatch_result_on_owner(handle, &before) ==
	      TR_ERR_STALE);
	return TR_OK;
}
static void test_cancel_and_stop(unsigned mode)
{
	bool stopped = mode != 0;
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	atomic_store(&f.metadata_mode, METADATA_CANCEL);
	unsigned slot = open_call(&f, 3);
	CHECK(slot == 0);
	atomic_store(&f.use_gate, true);
	request(&f, slot, 3, 2, 80);
	gate_wait(&f.gate, false);
	if (mode == 0) {
		struct tr_rpc_dispatch_result result = {0};
		CHECK(tr_rpc_record_dispatch_result_on_owner(
		              f.server.handles[slot], &result) == TR_ERR_STATE);
		CHECK(tr_reactor_call(f.server.reactor, reset_first, &f) ==
		      TR_OK);
	} else if (mode != 2)
		CHECK(tr_reactor_call(f.server.reactor, cancel_first, &f) ==
		      TR_OK);
	CHECK(!atomic_load(&f.server.terminals[slot]));
	if (!stopped) {
		CHECK(tr_reactor_call(f.server.reactor, destroy_dispatch, &f) ==
		      TR_AGAIN);
		CHECK(f.dispatch);
		gate_open(&f.gate);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(f.server.delivery[slot] == TR_RPC_RECORD_ON_OWNER);
		CHECK(f.server.status[slot] == TR_RPC_STATUS_CANCELLED);
		CHECK(!f.server.final_accepted[slot]);
		fixture_destroy(&f, false);
		return;
	}
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_ERR_STATE);
	CHECK(tr_reactor_call(f.server.reactor, disable_peer, &f.server) ==
	      TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, disable_driver, &f) == TR_OK);
	CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_AGAIN);
	CHECK(!atomic_load(&f.server.terminals[slot]));
	CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) == TR_OK);
	CHECK(tr_task_executor_adopt_cleanup(f.executor) == TR_OK);
	gate_open(&f.gate);
	uint64_t until = now_ms() + 15000;
	int ret;
	while ((ret = tr_task_executor_join(f.executor)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK);
	size_t delivered;
	CHECK(tr_task_executor_dispatch(f.executor, 8, &delivered) == TR_OK);
	CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) == TR_OK);
	CHECK(atomic_load(&f.server.terminals[slot]) == 1);
	CHECK(f.server.delivery[slot] == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
	fixture_destroy(&f, true);
}
static void test_business_error(unsigned mode)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	unsigned slot = open_call(&f, 0);
	if (mode == 0)
		atomic_store(&f.reject_method, 1);
	else if (mode == 1)
		f.bad_reply = true;
	else if (mode == 2)
		f.oversized_reply = true;
	else
		atomic_store(&fail_worker_malloc, true);
	if (mode == 0)
		send_record(&f, slot, 0, TR_RPC_RECORD_OPEN, 0);
	else
		request(&f, slot, 0, 1, 40);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	uint32_t expected = mode == 0   ? TR_RPC_STATUS_PERMISSION_DENIED
	                    : mode == 1 ? TR_RPC_STATUS_INTERNAL
	                                : TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	CHECK(f.client.status[slot] == expected &&
	      f.server.status[slot] == expected);
	CHECK(f.server.final_accepted[slot]);
	CHECK(!atomic_load(&f.client.messages[slot]));
	fixture_destroy(&f, false);
}
static int pause_driver(void *arg)
{
	((struct fixture *)arg)->driving = false;
	return TR_OK;
}
struct admission_setup {
	struct fixture *fixture;
	uint64_t due;
};
static int prepare_admission(void *arg)
{
	struct admission_setup *setup = arg;
	setup->fixture->driving = false;
	setup->fixture->open_due = setup->due;
	return TR_OK;
}
struct admission_step {
	struct fixture *fixture;
	unsigned slot;
	const uint8_t *data;
	size_t size;
	struct admission_clock clock;
	struct tr_rpc_dispatch_result result;
	struct tr_rpc_record_dispatch_stats stats;
	uint64_t before, after;
	int status;
	bool pump;
};
static int run_admission_step(void *arg)
{
	struct admission_step *step = arg;
	struct fixture *f = step->fixture;
	CHECK(!admission_clock);
	step->clock.reads = 0;
	step->before = atomic_load(&f->server.budget.current_bytes);
	/* 只包住实际派发入口；Reactor 定时器和真实业务线程仍使用本机时钟。
	 * 本组暂停自动派发，因此固定 due 不会被另一个 owner turn 抢先观察。 */
	admission_clock = &step->clock;
	step->status =
	        step->pump
	                ? tr_rpc_record_dispatch_pump_on_owner(f->dispatch, CAP)
	                : tr_rpc_record_dispatch_receive_on_owner(
	                          f->dispatch, f->server.streams[step->slot],
	                          step->data, step->size);
	admission_clock = NULL;
	step->after = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_rpc_record_dispatch_result_on_owner(
	              f->server.handles[step->slot], &step->result) == TR_OK);
	CHECK(tr_rpc_record_dispatch_get_stats_on_owner(f->dispatch,
	                                                &step->stats) == TR_OK);
	return TR_OK;
}
static void admission_step(struct admission_step *step)
{
	CHECK(tr_reactor_call(step->fixture->server.reactor, run_admission_step,
	                      step) == TR_OK);
}
static int retire_admission(void *arg)
{
	struct admission_step *step = arg;
	struct peer *peer = &step->fixture->server;
	CHECK(tr_rpc_record_dispatch_retire_on_owner(
	              peer->handles[step->slot]) == TR_OK);
	peer->handles[step->slot] = (struct tr_rpc_dispatch_handle){0};
	step->after = atomic_load(&peer->budget.current_bytes);
	return TR_OK;
}
static unsigned admission_begin(struct fixture *f, uint64_t due)
{
	fixture_init(f, 1024 * 1024, 8);
	struct admission_setup setup = {f, due};
	CHECK(tr_reactor_call(f->server.reactor, prepare_admission, &setup) ==
	      TR_OK);
	return open_call(f, 0);
}
static void admission_open(uint8_t out[TR_RPC_RECORD_HEADER_SIZE + 7],
                           uint64_t call_timeout)
{
	const struct tr_rpc_record_header header = {.kind = TR_RPC_RECORD_OPEN,
	                                            .service_id = 7,
	                                            .method_id = 9,
	                                            .metadata_size = 7,
	                                            .remaining_timeout_ns =
	                                                    call_timeout};
	const struct tr_rpc_record_limits limits = {BODY, BODY, 7,
	                                            TR_RPC_RECORD_ACCEPT_NONE};
	const uint8_t metadata[] = {1, 3, 0, 'k', 'a', 'b', 'c'};
	CHECK(tr_rpc_record_encode_header(out, TR_RPC_RECORD_HEADER_SIZE,
	                                  &header, &limits) == TR_OK);
	memcpy(out + TR_RPC_RECORD_HEADER_SIZE, metadata, sizeof(metadata));
}
static void admission_terminal(struct admission_step *step,
                               enum tr_rpc_status expected, bool admission)
{
	CHECK(step->result.terminal && step->result.status == expected &&
	      !step->result.final_accepted &&
	      step->result.admission_failed == admission);
	CHECK(step->stats.held_events == 0 && !step->stats.submitted_calls &&
	      !step->stats.reply_calls);
	CHECK(atomic_load(&step->fixture->server.terminals[step->slot]) == 1);
	for (unsigned slot = 0; slot < CAP; ++slot)
		for (unsigned kind = 1; kind <= 5; ++kind)
			CHECK(!atomic_load(
			        &step->fixture->business[slot].seen[kind]));
}
static void test_open_admission_partial_and_timer(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	/* 未验证固定头即便声称无限大的 CALL 预算，也不能解除本机准入期限。 */
	admission_open(open, UINT64_MAX);
	for (unsigned partial = 0; partial < 2; ++partial) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		struct admission_step step = {.fixture = &f,
		                              .slot = slot,
		                              .clock = {.now = due - 100},
		                              .pump = !partial,
		                              .data = open,
		                              .size = 7};
		admission_step(&step);
		uint64_t baseline = step.before;
		uint64_t partial_bytes = 0;
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == 1 && !step.stats.held_events);
		if (partial) {
			step.data = open + 7;
			step.size = sizeof(open) - 8;
			step.clock.now = due - 1;
			admission_step(&step);
			CHECK(step.status == TR_OK && !step.result.terminal &&
			      step.clock.reads == 1 && step.after > baseline &&
			      !step.stats.held_events);
			partial_bytes = step.after;
			step.data = open + sizeof(open) - 1;
			step.size = 1;
		}
		step.clock.now = due;
		admission_step(&step);
		CHECK(step.status == (partial ? TR_ERR_STATE : TR_OK) &&
		      step.clock.reads == 1);
		CHECK(partial ? partial_bytes - step.after == sizeof(open)
		              : step.after == baseline);
		admission_terminal(&step, TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		/* 已知终局不重读故障时钟，也不在重复输入时重选 INTERNAL。 */
		step.clock.fail_at = 1;
		admission_step(&step);
		CHECK(step.clock.reads == 0);
		admission_terminal(&step, TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		CHECK(tr_reactor_call(f.server.reactor, retire_admission,
		                      &step) == TR_OK);
		CHECK(step.after == baseline);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_validation_boundary(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	admission_open(open, 0);
	for (unsigned fault = 0; fault < 2; ++fault) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		struct admission_step step = {
		        .fixture = &f,
		        .slot = slot,
		        .data = open,
		        .size = sizeof(open),
		        .clock = {.now = due - 1,
		                  .advanced = due,
		                  .advance_at = 2,
		                  .fail_at = fault ? 2 : 0}};
		admission_step(&step);
		/* 输入已被真正解码吸收，所以返回 OK；内部回调仍须拒绝准入。
		 * 仅保留外层 receive 检查会使这个 OPEN 进入业务队列。 */
		CHECK(step.status == TR_OK && step.clock.reads == 2 &&
		      step.after > step.before);
		admission_terminal(&step,
		                   fault ? TR_RPC_STATUS_INTERNAL
		                         : TR_RPC_STATUS_DEADLINE_EXCEEDED,
		                   true);
		CHECK(tr_reactor_call(f.server.reactor, retire_admission,
		                      &step) == TR_OK);
		CHECK(step.after == step.before);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_clock_failure(void)
{
	uint8_t partial[] = {'T', 'R', 'P', 'C', 2, 0, 1};
	for (unsigned fault = 0; fault < 6; ++fault) {
		struct fixture f;
		unsigned slot = admission_begin(&f, UINT64_C(10000000000));
		struct admission_step step = {
		        .fixture = &f,
		        .slot = slot,
		        .data = partial,
		        .size = sizeof(partial),
		        .pump = fault == 5,
		        .clock = {.now = 1,
		                  .fail_at = (fault == 0 || fault == 5) ? 1 : 0,
		                  .invalid = fault < 5 ? fault : 0}};
		admission_step(&step);
		CHECK(step.status == (step.pump ? TR_OK : TR_ERR_STATE) &&
		      step.clock.reads == 1 && step.after == step.before);
		admission_terminal(&step, TR_RPC_STATUS_INTERNAL, true);
		fixture_destroy(&f, false);
	}
}
static void test_open_admission_handoff(void)
{
	const uint64_t due = UINT64_C(10000000000);
	uint8_t open[TR_RPC_RECORD_HEADER_SIZE + 7];
	const uint8_t message[] = {'T', 'R', 'P', 'C', 2, 0, 3};
	for (unsigned finite = 0; finite < 2; ++finite) {
		struct fixture f;
		unsigned slot = admission_begin(&f, due);
		admission_open(open, finite ? 1000 : 0);
		struct admission_step step = {.fixture = &f,
		                              .slot = slot,
		                              .data = open,
		                              .size = sizeof(open) - 1,
		                              .clock = {.now = due - 800}};
		admission_step(&step);
		CHECK(step.status == TR_OK && step.clock.reads == 1 &&
		      !step.stats.held_events);
		step.data = open + sizeof(open) - 1;
		step.size = 1;
		step.clock.now = due - 100;
		admission_step(&step);
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == (finite ? 4U : 2U) &&
		      step.stats.held_events == 1);
		step.data = message;
		step.size = sizeof(message);
		step.clock.now = due + 899;
		step.clock.fail_at = finite ? 0 : 1;
		admission_step(&step);
		CHECK(step.status == TR_OK && !step.result.terminal &&
		      step.clock.reads == (finite ? 1U : 0U));
		if (finite) {
			step.clock.now = due + 900;
			admission_step(&step);
			CHECK(step.status == TR_ERR_STATE &&
			      step.clock.reads == 1);
			admission_terminal(
			        &step, TR_RPC_STATUS_DEADLINE_EXCEEDED, false);
		}
		fixture_destroy(&f, false);
	}
	/* 保留低层旧入口：0 不启用 OPEN 期限，完整无限 CALL 不读取时钟。 */
	struct fixture f;
	unsigned slot = admission_begin(&f, 0);
	admission_open(open, 0);
	struct admission_step step = {.fixture = &f,
	                              .slot = slot,
	                              .data = open,
	                              .size = sizeof(open),
	                              .clock = {.now = due, .fail_at = 1}};
	admission_step(&step);
	CHECK(step.status == TR_OK && !step.result.terminal &&
	      !step.clock.reads && step.stats.held_events == 1);
	fixture_destroy(&f, false);
}
static void test_pending_limit(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 2);
	CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) == TR_OK);
	unsigned slot = open_call(&f, 3);
	request(&f, slot, 3, 2, 16);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.server.status[slot] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	CHECK(!f.server.final_accepted[slot]);
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 0 && f.stats.submitted_calls == 0);
	for (unsigned i = 0; i < 6; ++i)
		CHECK(!atomic_load(&f.business[0].seen[i]));
	fixture_destroy(&f, false);
}
static int constructor_probe(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_dispatch *d = NULL;
	uint64_t before = atomic_load(&f->server.budget.current_bytes);
	unsigned success = 0;
	for (unsigned i = 1; i < 12; ++i) {
		fail_at = i;
		allocations = 0;
		int ret = tr_rpc_record_dispatch_create_on_owner(
		        f->server.channel, f->executor, f->methods, 4,
		        &f->config, server_terminal, f, &f->server.budget, &d);
		fail_at = 0;
		if (ret == TR_OK) {
			CHECK(d);
			CHECK(tr_rpc_record_dispatch_destroy_on_owner(&d) ==
			      TR_OK);
			CHECK(atomic_load(&f->server.budget.current_bytes) ==
			      before);
			success = i;
			break;
		}
		CHECK(ret == TR_ERR_NOMEM && !d);
		CHECK(atomic_load(&f->server.budget.current_bytes) == before);
	}
	CHECK(success == 8);
	return TR_OK;
}
struct wrong_executor_request {
	struct fixture *fixture;
	struct tr_task_executor *foreign;
};
static int wrong_executor_owner(void *arg)
{
	struct wrong_executor_request *request = arg;
	struct fixture *f = request->fixture;
	struct tr_rpc_record_dispatch *d = NULL;
	uint64_t before = atomic_load(&f->server.budget.current_bytes);
	CHECK(tr_task_executor_is_current_owner(f->executor));
	CHECK(!tr_task_executor_is_current_owner(request->foreign));
	CHECK(tr_rpc_record_dispatch_create_on_owner(
	              f->server.channel, request->foreign, f->methods, 4,
	              &f->config, server_terminal, f, &f->server.budget,
	              &d) == TR_ERR_STATE);
	CHECK(!d && atomic_load(&f->server.budget.current_bytes) == before);
	return TR_OK;
}
static void test_constructor(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	CHECK(tr_reactor_call(f.server.reactor, constructor_probe, &f) ==
	      TR_OK);
	struct tr_memory_budget budget;
	struct tr_task_executor *foreign = NULL;
	struct tr_task_executor_config config = {1, 1};
	tr_memory_budget_init(&budget, 65536);
	CHECK(tr_task_executor_create(&config, &budget, &foreign) == TR_OK);
	CHECK(tr_task_executor_is_current_owner(foreign));
	struct wrong_executor_request request = {&f, foreign};
	CHECK(tr_reactor_call(f.server.reactor, wrong_executor_owner,
	                      &request) == TR_OK);
	CHECK(tr_task_executor_destroy(&foreign) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
	fixture_destroy(&f, false);
}

struct shared_tasks {
	struct fixture *fixture;
	_Atomic unsigned done;
};
static int shared_work(void *arg, const struct tr_task_cancel *cancel)
{
	struct shared_tasks *jobs = arg;
	(void)cancel;
	gate_wait(&jobs->fixture->gate, true);
	return TR_OK;
}
static void shared_done(void *arg, struct tr_task_handle handle,
                        struct tr_task_result result)
{
	struct shared_tasks *jobs = arg;
	(void)handle;
	CHECK(!result.cancelled && result.work_status == TR_OK);
	atomic_fetch_add(&jobs->done, 1);
}
static int occupy_executor(void *arg)
{
	struct shared_tasks *jobs = arg;
	struct tr_task_spec spec = {shared_work, shared_done, jobs};
	for (unsigned i = 0; i < 4; ++i) {
		struct tr_task_handle handle;
		CHECK(tr_task_executor_submit(jobs->fixture->executor, &spec,
		                              &handle) == TR_OK);
	}
	return TR_OK;
}
static void test_shared_executor_backpressure(void)
{
	struct fixture f;
	fixture_init(&f, 1024 * 1024, 8);
	struct shared_tasks jobs = {.fixture = &f};
	atomic_init(&jobs.done, 0);
	CHECK(tr_reactor_call(f.server.reactor, occupy_executor, &jobs) ==
	      TR_OK);
	gate_wait(&f.gate, false);
	unsigned slot = open_call(&f, 0);
	request(&f, slot, 0, 1, 64);
	unsigned turns = atomic_load(&f.drive_turns);
	wait_count(&f.drive_turns, turns + 10);
	CHECK(tr_reactor_call(f.server.reactor, snapshot, &f) == TR_OK);
	CHECK(f.stats.held_events == 3 && f.stats.submitted_calls == 0);
	CHECK(!atomic_load(&f.business[0].seen[TR_RPC_RECORD_OPEN]));
	gate_open(&f.gate);
	wait_count(&jobs.done, 4);
	wait_count(&f.client.terminals[slot], 1);
	wait_count(&f.server.terminals[slot], 1);
	CHECK(f.client.status[slot] == TR_RPC_STATUS_OK);
	for (unsigned kind = TR_RPC_RECORD_OPEN;
	     kind <= TR_RPC_RECORD_HALF_CLOSE; ++kind)
		if (kind != TR_RPC_RECORD_INITIAL_METADATA)
			CHECK(atomic_load(&f.business[0].seen[kind]) == 1);
	fixture_destroy(&f, false);
}

static void producer_case_init(struct producer_case *c)
{
	*c = (struct producer_case){0};
	atomic_init(&c->active, 0);
	atomic_init(&c->acquired, 0);
	atomic_init(&c->terminals, 0);
	for (unsigned i = 0; i < 3; ++i)
		atomic_init(&c->seen[i], 0);
}
static unsigned producer_open_method(struct fixture *f, struct producer_case *c,
                                     unsigned method)
{
	unsigned slot = open_call(f, method);
	send_record(f, slot, method, TR_RPC_RECORD_OPEN, 0);
	wait_count(&c->acquired, 1);
	return slot;
}
static unsigned producer_open(struct fixture *f, struct producer_case *c)
{
	return producer_open_method(f, c, 3);
}
static void producer_writable(struct tr_rpc_v2_producer *p)
{
	CHECK(tr_rpc_v2_producer_wait_writable(p, 15000) == TR_OK);
}
static int producer_send_body(struct tr_rpc_v2_producer *p, uint32_t size)
{
	uint8_t data[BODY];
	CHECK(size <= sizeof(data));
	for (uint32_t i = 0; i < size; ++i)
		data[i] = (uint8_t)(i * 7 + 3);
	const struct tr_rpc_bytes body = {data, size};
	int ret = tr_rpc_v2_producer_send(p, &body);
	memset(data, 0xdd, sizeof(data));
	return ret;
}
static int producer_metadata(struct tr_rpc_v2_producer *p, unsigned phase,
                             enum tr_rpc_status status)
{
	char key[] = "k";
	uint8_t value[TR_RPC_METADATA_MAX_BYTES - 4U];
	struct tr_rpc_metadata entry = {key, value, sizeof(value)};
	for (size_t i = 0; i < sizeof(value); ++i)
		value[i] = metadata_byte(phase, i);
	int ret =
	        phase == 0
	                ? tr_rpc_v2_producer_send_initial_metadata(p, &entry, 1)
	                : tr_rpc_v2_producer_finish(p, status, &entry, 1);
	memset(value, 0xdd, sizeof(value));
	memset(key, 0xdd, sizeof(key));
	memset(&entry, 0xdd, sizeof(entry));
	return ret;
}
struct producer_semantic_snapshot {
	struct fixture *fixture;
	struct tr_rpc_semantic_stats value;
};
static int producer_semantic_on_owner(void *arg)
{
	struct producer_semantic_snapshot *s = arg;
	return tr_rpc_record_dispatch_get_semantic_stats_on_owner(
	        s->fixture->dispatch, &s->value);
}
static void producer_semantic(struct fixture *f, bool finished,
                              enum tr_rpc_status status)
{
	struct producer_semantic_snapshot s = {.fixture = f};
	CHECK(tr_reactor_call(f->server.reactor, producer_semantic_on_owner,
	                      &s) == TR_OK);
	CHECK(s.value.calls_started == 1 &&
	      s.value.calls_finished == (uint64_t)finished &&
	      s.value.calls_inflight == (uint64_t)!finished);
	for (unsigned i = 0; i < TR_RPC_STATUS_COUNT; ++i)
		CHECK(s.value.final_status[i] ==
		      (uint64_t)(finished && i == (unsigned)status));
}
static int producer_owner_wait(void *arg)
{
	CHECK(tr_rpc_v2_producer_wait_writable(arg, 0) == TR_ERR_STATE);
	return TR_OK;
}
static int resume_driver(void *arg)
{
	((struct fixture *)arg)->driving = true;
	return TR_OK;
}
static int release_reserved_budget(void *arg)
{
	struct fixture *f = arg;
	int ret = tr_memory_budget_release(&f->server.budget,
	                                   f->metadata_reserved);
	f->metadata_reserved = 0;
	return ret;
}
static int reserve_producer_budget(void *arg)
{
	struct fixture *f = arg;
	/* 与同步 metadata 故障夹具不同，此时创建 work 已完成交接，
	 * owner 驱动已暂停，没有可替调用方消费预算的在途输入任务。 */
	CHECK(!f->driving && snapshot(f) == TR_OK &&
	      f->stats.active_calls == 1 && !f->stats.submitted_calls &&
	      !f->stats.held_events && !f->stats.reply_calls);
	uint64_t current = atomic_load(&f->server.budget.current_bytes);
	CHECK(current < f->server.budget.limit_bytes);
	f->metadata_reserved = f->server.budget.limit_bytes - current;
	CHECK(tr_memory_budget_reserve(&f->server.budget,
	                               f->metadata_reserved) == TR_OK);
	return TR_OK;
}
static void producer_finished(struct fixture *f, struct producer_case *c,
                              unsigned slot, enum tr_rpc_status status)
{
	wait_count(&f->client.terminals[slot], 1);
	wait_count(&f->server.terminals[slot], 1);
	wait_count(&c->terminals, 1);
	CHECK(f->client.status[slot] == (uint32_t)status &&
	      f->server.status[slot] == (uint32_t)status &&
	      c->status == status);
	CHECK(c->delivery == TR_RPC_V2_DELIVERY_LIVE);
}
static void test_producer_staged_and_retry(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	c.block_open = true;
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	gate_wait(&f.gate, false);
	struct tr_rpc_v2_producer *p = c.producer;
	CHECK(tr_rpc_v2_producer_is_closed(p) == 0);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_TIMEOUT);
	CHECK(tr_reactor_call(f.server.reactor, producer_owner_wait, p) ==
	      TR_OK);
	atomic_store(&blocked_kind, TR_RPC_RECORD_INITIAL_METADATA);
	CHECK(producer_metadata(p, 0, TR_RPC_STATUS_OK) == TR_OK);
	CHECK(producer_send_body(p, 80) == TR_AGAIN);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_AGAIN);
	producer_semantic(&f, false, TR_RPC_STATUS_OK);
	CHECK(!atomic_load(&send_attempts[TR_RPC_RECORD_INITIAL_METADATA]));
	gate_open(&f.gate);
	wait_count(&send_attempts[TR_RPC_RECORD_INITIAL_METADATA], 10);
	/* 邮箱在真实 work/done 交接后仍有背压，后续输入照常 FIFO 执行。
	 * HALF_CLOSE 返回不再隐式结束异步响应。 */
	send_record(&f, slot, 3, TR_RPC_RECORD_MESSAGE, 80);
	send_record(&f, slot, 3, TR_RPC_RECORD_HALF_CLOSE, 0);
	wait_count(&c.seen[TR_RPC_V2_REQUEST_MESSAGE], 1);
	wait_count(&c.seen[TR_RPC_V2_REQUEST_HALF_CLOSE], 1);
	CHECK(!atomic_load(&c.terminals));
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_TIMEOUT);
	atomic_store(&blocked_kind, 0);
	producer_writable(p);
	wait_count(&f.client.initial[slot], 1);
	CHECK(producer_metadata(p, 0, TR_RPC_STATUS_OK) == TR_ERR_STATE);
	atomic_store(&blocked_kind, TR_RPC_RECORD_MESSAGE);
	CHECK(producer_send_body(p, 80) == TR_OK);
	wait_count(&send_attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(atomic_load(&retry_body) != 0);
	CHECK(producer_send_body(p, 0) == TR_AGAIN);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_AGAIN);
	atomic_store(&blocked_kind, 0);
	producer_writable(p);
	wait_count(&f.client.messages[slot], 1);
	CHECK(producer_send_body(p, 0) == TR_OK);
	producer_writable(p);
	wait_count(&f.client.messages[slot], 2);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	CHECK(producer_metadata(p, 1, TR_RPC_STATUS_PERMISSION_DENIED) ==
	      TR_OK);
	wait_count(&send_attempts[TR_RPC_RECORD_FINAL_STATUS], 10);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 0);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_STATE);
	CHECK(producer_send_body(p, 80) == TR_ERR_STATE);
	atomic_store(&blocked_kind, 0);
	producer_finished(&f, &c, slot, TR_RPC_STATUS_PERMISSION_DENIED);
	CHECK(f.client.received_bytes[slot] == 80 &&
	      atomic_load(&f.client.messages[slot]) == 2);
	check_metadata(&f.client, slot, 0, true);
	check_metadata(&f.client, slot, 1, true);
	producer_semantic(&f, true, TR_RPC_STATUS_PERMISSION_DENIED);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 1);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_CLOSED);
	CHECK(producer_send_body(p, 0) == TR_ERR_CLOSED);
	CHECK(tr_rpc_v2_producer_retain(p) == TR_OK);
	tr_rpc_v2_producer_release(p);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}
static void test_producer_real_window(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	fixture_init_mode(&f, 128, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	CHECK(tr_reactor_call(f.client.reactor, hold_client, &f) == TR_OK);
	atomic_store(&observe_real_retry, true);
	CHECK(producer_send_body(p, 80) == TR_OK);
	wait_count(&f.client.initial[slot], 1);
	wait_count(&send_attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(atomic_load(&blocked_kind) == 0 && atomic_load(&retry_body) != 0);
	CHECK(!atomic_load(&f.client.messages[slot]));
	CHECK(producer_send_body(p, 0) == TR_AGAIN);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_AGAIN);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_TIMEOUT);
	CHECK(tr_reactor_call(f.client.reactor, release_client, &f) == TR_OK);
	producer_writable(p);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) == TR_OK);
	producer_finished(&f, &c, slot, TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.client.messages[slot]) == 1 &&
	      f.client.received_bytes[slot] == 80);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}
static void test_producer_failure_preserves_phase(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) == TR_OK);
	const struct tr_rpc_metadata bad[] = {{"same", NULL, 0},
	                                      {"same", NULL, 0}};
	CHECK(tr_rpc_v2_producer_send_initial_metadata(p, bad, 2) ==
	      TR_ERR_BAD_FLAGS);
	CHECK(tr_rpc_v2_producer_send(p, NULL) == TR_ERR_INVALID);
	const struct tr_rpc_bytes invalid = {NULL, 1};
	CHECK(tr_rpc_v2_producer_send(p, &invalid) == TR_ERR_INVALID);
	uint8_t byte = 0;
	const struct tr_rpc_bytes too_large = {&byte, BODY + 1};
	CHECK(tr_rpc_v2_producer_send(p, &too_large) == TR_ERR_BAD_LENGTH);
	allocations = 0;
	fail_at = 1;
	CHECK(producer_send_body(p, 80) == TR_ERR_NOMEM);
	fail_at = 0;
	CHECK(allocations != 0);
	CHECK(tr_reactor_call(f.server.reactor, reserve_producer_budget, &f) ==
	      TR_OK);
	CHECK(producer_send_body(p, 80) == TR_ERR_NOMEM);
	CHECK(atomic_load(&f.server.budget.current_bytes) ==
	      f.server.budget.limit_bytes);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, release_reserved_budget, &f) ==
	      TR_OK);
	/* 分配/预算失败均未接受 MESSAGE，因此 INITIAL 仍可设置。 */
	CHECK(producer_metadata(p, 0, TR_RPC_STATUS_OK) == TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, resume_driver, &f) == TR_OK);
	producer_writable(p);
	CHECK(producer_send_body(p, 80) == TR_OK);
	producer_writable(p);
	CHECK(tr_rpc_v2_producer_finish(p,
	                                (enum tr_rpc_status)TR_RPC_STATUS_COUNT,
	                                NULL, 0) == TR_ERR_INVALID);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, bad, 2) ==
	      TR_ERR_BAD_FLAGS);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_OK);
	CHECK(producer_metadata(p, 1, TR_RPC_STATUS_OK) == TR_OK);
	producer_finished(&f, &c, slot, TR_RPC_STATUS_OK);
	CHECK(atomic_load(&f.client.messages[slot]) == 1);
	check_metadata(&f.client, slot, 0, true);
	check_metadata(&f.client, slot, 1, true);
	producer_semantic(&f, true, TR_RPC_STATUS_OK);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}

struct producer_waiter {
	struct tr_rpc_v2_producer *producer;
	_Atomic unsigned entered;
	int result;
};
static void *producer_wait_thread(void *arg)
{
	struct producer_waiter *waiter = arg;
	atomic_store(&waiter->entered, 1);
	waiter->result =
	        tr_rpc_v2_producer_wait_writable(waiter->producer, 15000);
	tr_rpc_v2_producer_release(waiter->producer);
	return NULL;
}
static void test_producer_cancel_or_stop(bool stopped)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	c.block_open = true;
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	CHECK(slot == 0);
	gate_wait(&f.gate, false);
	struct tr_rpc_v2_producer *p = c.producer;
	CHECK(producer_send_body(p, 80) == TR_OK);
	CHECK(tr_rpc_v2_producer_retain(p) == TR_OK);
	struct producer_waiter waiter = {.producer = p};
	atomic_init(&waiter.entered, 0);
	pthread_t thread;
	CHECK(pthread_create(&thread, NULL, producer_wait_thread, &waiter) ==
	      0);
	wait_count(&waiter.entered, 1);
	if (!stopped) {
		CHECK(tr_reactor_call(f.server.reactor, cancel_first, &f) ==
		      TR_OK);
		CHECK(!atomic_load(&c.terminals));
		producer_semantic(&f, false, TR_RPC_STATUS_CANCELLED);
		gate_open(&f.gate);
		wait_count(&c.terminals, 1);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(c.status == TR_RPC_STATUS_CANCELLED &&
		      c.delivery == TR_RPC_V2_DELIVERY_LIVE);
		producer_semantic(&f, true, TR_RPC_STATUS_CANCELLED);
	} else {
		CHECK(tr_reactor_call(f.server.reactor, disable_peer,
		                      &f.server) == TR_OK);
		CHECK(tr_reactor_call(f.server.reactor, disable_driver, &f) ==
		      TR_OK);
		CHECK(tr_reactor_stop(f.server.reactor) == TR_OK);
		CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) ==
		      TR_AGAIN);
		CHECK(!atomic_load(&c.terminals));
		CHECK(tr_task_executor_stop(f.executor, TR_TASK_CANCEL) ==
		      TR_OK);
		CHECK(tr_task_executor_adopt_cleanup(f.executor) == TR_OK);
		gate_open(&f.gate);
		uint64_t until = now_ms() + 15000;
		int ret;
		while ((ret = tr_task_executor_join(f.executor)) == TR_AGAIN) {
			CHECK(now_ms() < until);
			sched_yield();
		}
		CHECK(ret == TR_OK);
		size_t delivered;
		CHECK(tr_task_executor_dispatch(f.executor, 8, &delivered) ==
		      TR_OK);
		CHECK(tr_rpc_record_dispatch_reap_stopped(&f.dispatch) ==
		      TR_OK);
		CHECK(tr_task_executor_destroy(&f.executor) == TR_OK);
		CHECK(atomic_load(&c.terminals) == 1 &&
		      c.status == TR_RPC_STATUS_CANCELLED &&
		      c.delivery == TR_RPC_V2_DELIVERY_SHUTDOWN);
	}
	CHECK(pthread_join(thread, NULL) == 0);
	CHECK(waiter.result == TR_ERR_CLOSED);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 1);
	CHECK(!atomic_load(&send_attempts[TR_RPC_RECORD_MESSAGE]));
	fixture_destroy(&f, stopped);
	CHECK(tr_rpc_v2_producer_wait_writable(p, 0) == TR_ERR_CLOSED);
	CHECK(producer_send_body(p, 80) == TR_ERR_CLOSED);
	tr_rpc_v2_producer_release(p);
}
static int producer_idle_on_owner(void *arg)
{
	struct fixture *f = arg;
	struct tr_task_executor_stats stats;
	size_t delivered;
	CHECK(tr_task_executor_dispatch(f->executor, 8, &delivered) == TR_OK);
	CHECK(tr_rpc_record_dispatch_pump_on_owner(f->dispatch, CAP) == TR_OK);
	CHECK(tr_task_executor_get_stats(f->executor, &stats) == TR_OK);
	return stats.outstanding ? TR_AGAIN : TR_OK;
}
static void producer_idle(struct fixture *f)
{
	uint64_t until = now_ms() + 15000;
	int ret;
	while ((ret = tr_reactor_call(f->server.reactor, producer_idle_on_owner,
	                              f)) == TR_AGAIN) {
		CHECK(now_ms() < until);
		sched_yield();
	}
	CHECK(ret == TR_OK);
}
static void test_producer_work_terminal_race(bool work_first)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	c.block_message = !work_first;
	c.message_status = TR_RPC_STATUS_PERMISSION_DENIED;
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	if (work_first)
		atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	if (!work_first) {
		send_record(&f, slot, 3, TR_RPC_RECORD_MESSAGE, 80);
		gate_wait(&f.gate, false);
	}
	CHECK(producer_metadata(p, 1, TR_RPC_STATUS_OK) == TR_OK);
	if (work_first) {
		wait_count(&send_attempts[TR_RPC_RECORD_FINAL_STATUS], 10);
		send_record(&f, slot, 3, TR_RPC_RECORD_MESSAGE, 80);
		wait_count(&c.seen[TR_RPC_V2_REQUEST_MESSAGE], 1);
		/* 等原生 executor outstanding==0，证明 work/done 的错误已经
		 * 收敛；不能用 work 返回前的 seen 标志抢跑释放 FINAL 背压。 */
		producer_idle(&f);
		CHECK(!atomic_load(&c.terminals));
		atomic_store(&blocked_kind, 0);
	} else {
		wait_count(&f.client.terminals[slot], 1);
		CHECK(!atomic_load(&c.terminals) &&
		      atomic_load(&c.active) == 1);
		producer_semantic(&f, false, TR_RPC_STATUS_OK);
		gate_open(&f.gate);
	}
	enum tr_rpc_status status =
	        work_first ? TR_RPC_STATUS_PERMISSION_DENIED : TR_RPC_STATUS_OK;
	producer_finished(&f, &c, slot, status);
	check_metadata(&f.client, slot, 1, !work_first);
	producer_semantic(&f, true, status);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 1);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}
static void test_producer_release(bool finished)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	if (finished) {
		atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
		CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) ==
		      TR_OK);
		wait_count(&send_attempts[TR_RPC_RECORD_FINAL_STATUS], 10);
	} else {
		CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) ==
		      TR_OK);
		CHECK(producer_send_body(p, 80) == TR_OK);
		CHECK(tr_rpc_v2_producer_retain(p) == TR_OK);
		tr_rpc_v2_producer_release(p);
		CHECK(tr_rpc_v2_producer_is_closed(p) == 0);
	}
	tr_rpc_v2_producer_release(p);
	c.producer = NULL;
	if (finished) {
		producer_semantic(&f, false, TR_RPC_STATUS_OK);
		atomic_store(&blocked_kind, 0);
		producer_finished(&f, &c, slot, TR_RPC_STATUS_OK);
		producer_semantic(&f, true, TR_RPC_STATUS_OK);
	} else {
		CHECK(tr_reactor_call(f.server.reactor, resume_driver, &f) ==
		      TR_OK);
		wait_count(&c.terminals, 1);
		wait_count(&f.server.terminals[slot], 1);
		CHECK(c.status == TR_RPC_STATUS_CANCELLED);
		CHECK(!atomic_load(&send_attempts[TR_RPC_RECORD_MESSAGE]));
		producer_semantic(&f, true, TR_RPC_STATUS_CANCELLED);
	}
	fixture_destroy(&f, false);
}
struct producer_replacement {
	struct fixture *fixture;
	struct producer_case *next;
};
static int producer_replace_dispatch(void *arg)
{
	struct producer_replacement *r = arg;
	struct fixture *f = r->fixture;
	CHECK(tr_rpc_record_dispatch_destroy_on_owner(&f->dispatch) == TR_OK);
	memset(f->server.handles, 0, sizeof(f->server.handles));
	f->producer_case = r->next;
	r->next->fixture = f;
	for (unsigned i = 0; i < 4; ++i) {
		f->methods[i].arg = r->next;
		f->methods[i].descriptor.max_response_bytes =
		        r->next->zero_response ? 0 : BODY;
	}
	return create_dispatch(f);
}
static void test_producer_quota_survives_dispatch(void)
{
	struct fixture f;
	struct producer_case cases[3];
	for (unsigned i = 0; i < 3; ++i)
		producer_case_init(&cases[i]);
	cases[1].expected_acquire = TR_AGAIN;
	cases[2].zero_response = true;
	fixture_init_mode(&f, 1024 * 1024, 8, &cases[0], 1);
	unsigned slot = producer_open(&f, &cases[0]);
	struct tr_rpc_v2_producer *retained = cases[0].producer;
	producer_writable(retained);
	CHECK(tr_rpc_v2_producer_finish(retained, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_OK);
	producer_finished(&f, &cases[0], slot, TR_RPC_STATUS_OK);
	CHECK(tr_rpc_v2_producer_is_closed(retained) == 1);
	for (unsigned i = 1; i < 3; ++i) {
		struct producer_replacement r = {&f, &cases[i]};
		CHECK(tr_reactor_call(f.server.reactor,
		                      producer_replace_dispatch, &r) == TR_OK);
		slot = producer_open(&f, &cases[i]);
		if (i == 1) {
			CHECK(cases[i].acquire_status == TR_AGAIN &&
			      !cases[i].producer);
			producer_finished(&f, &cases[i], slot,
			                  TR_RPC_STATUS_RESOURCE_EXHAUSTED);
			/* 新 dispatch 不重置共享租约；归还旧 closed
			 * 句柄才恢复。 */
			tr_rpc_v2_producer_release(retained);
		} else {
			producer_writable(cases[i].producer);
			CHECK(producer_send_body(cases[i].producer, 1) ==
			      TR_ERR_BAD_LENGTH);
			CHECK(producer_send_body(cases[i].producer, 0) ==
			      TR_OK);
			producer_writable(cases[i].producer);
			CHECK(tr_rpc_v2_producer_finish(cases[i].producer,
			                                TR_RPC_STATUS_OK, NULL,
			                                0) == TR_OK);
			producer_finished(&f, &cases[i], slot,
			                  TR_RPC_STATUS_OK);
			CHECK(atomic_load(&f.client.messages[slot]) == 1 &&
			      f.client.received_bytes[slot] == 0);
			tr_rpc_v2_producer_release(cases[i].producer);
		}
	}
	fixture_destroy(&f, false);
}
struct producer_copy {
	struct tr_rpc_v2_producer *producer;
	struct gate *gate;
	int result;
};
static void *producer_copy_thread(void *arg)
{
	struct producer_copy *copy = arg;
	allocation_gate = copy->gate;
	copy->result = producer_send_body(copy->producer, BODY);
	allocation_gate = NULL;
	tr_rpc_v2_producer_release(copy->producer);
	return NULL;
}
static void test_producer_copy_outlives_domain_close(void)
{
	struct fixture f;
	struct producer_case c;
	struct gate copy_gate;
	producer_case_init(&c);
	gate_init(&copy_gate);
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) == TR_OK);
	uint64_t baseline = atomic_load(&f.server.budget.current_bytes);
	CHECK(tr_rpc_v2_producer_retain(p) == TR_OK);
	struct producer_copy copy = {p, &copy_gate, 0};
	pthread_t thread;
	CHECK(pthread_create(&thread, NULL, producer_copy_thread, &copy) == 0);
	gate_wait(&copy_gate, false);
	CHECK(atomic_load(&f.server.budget.current_bytes) >= baseline + BODY);
	CHECK(producer_send_body(p, 0) == TR_AGAIN);
	CHECK(tr_rpc_v2_producer_finish(p, TR_RPC_STATUS_OK, NULL, 0) ==
	      TR_AGAIN);
	/* malloc 屏障位于已占 building/预算之后、复制之前。销毁不能等
	 * 此应用线程，迟到分配及复制只能访问独立 domain 和应用引用。 */
	fixture_destroy(&f, false);
	CHECK(atomic_load(&c.terminals) == 1 &&
	      f.server.status[slot] == TR_RPC_STATUS_CANCELLED);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 1);
	/* 立即复用原预算存储；错误的迟到 upstream release 会改动新账。 */
	uint64_t reused = f.server.budget.limit_bytes;
	CHECK(tr_memory_budget_reserve(&f.server.budget, reused) == TR_OK);
	gate_open(&copy_gate);
	CHECK(pthread_join(thread, NULL) == 0);
	CHECK(copy.result == TR_ERR_CLOSED);
	tr_rpc_v2_producer_release(p);
	CHECK(atomic_load(&f.server.budget.current_bytes) == reused);
	CHECK(tr_memory_budget_release(&f.server.budget, reused) == TR_OK);
	gate_destroy(&copy_gate);
}
static void test_producer_deadline(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	f.call_timeout = UINT64_C(60000000000);
	unsigned slot = producer_open(&f, &c);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	atomic_store(&blocked_kind, TR_RPC_RECORD_MESSAGE);
	CHECK(producer_send_body(p, 80) == TR_OK);
	wait_count(&send_attempts[TR_RPC_RECORD_MESSAGE], 10);
	CHECK(tr_reactor_call(f.server.reactor, pause_driver, &f) == TR_OK);
	struct admission_step step = {
	        .fixture = &f,
	        .slot = slot,
	        .pump = true,
	        .clock = {.now = (now_ms() + 120000) * UINT64_C(1000000)}};
	admission_step(&step);
	CHECK(step.result.terminal &&
	      step.result.status == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      !step.result.final_accepted);
	wait_count(&c.terminals, 1);
	CHECK(c.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(tr_rpc_v2_producer_is_closed(p) == 1);
	producer_semantic(&f, true, TR_RPC_STATUS_DEADLINE_EXCEEDED);
	atomic_store(&blocked_kind, 0);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}
static void test_producer_rejects_sync_write(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	c.sync_after_acquire = true;
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	unsigned slot = producer_open(&f, &c);
	producer_finished(&f, &c, slot, TR_RPC_STATUS_FAILED_PRECONDITION);
	CHECK(tr_rpc_v2_producer_is_closed(c.producer) == 1);
	CHECK(!atomic_load(&f.client.messages[slot]));
	producer_semantic(&f, true, TR_RPC_STATUS_FAILED_PRECONDITION);
	tr_rpc_v2_producer_release(c.producer);
	fixture_destroy(&f, false);
}
static void test_producer_cardinality_fallback_stops_work(void)
{
	struct fixture f;
	struct producer_case c;
	producer_case_init(&c);
	c.block_message = true;
	c.message_status = TR_RPC_STATUS_PERMISSION_DENIED;
	fixture_init_mode(&f, 1024 * 1024, 8, &c, 1);
	/* 方法 2 是 MANY 请求 / ONE 响应；零 MESSAGE 的 OK FINAL 非法。 */
	unsigned slot = producer_open_method(&f, &c, 2);
	struct tr_rpc_v2_producer *p = c.producer;
	producer_writable(p);
	send_record(&f, slot, 2, TR_RPC_RECORD_MESSAGE, 80);
	gate_wait(&f.gate, false);
	atomic_store(&blocked_kind, TR_RPC_RECORD_FINAL_STATUS);
	CHECK(producer_metadata(p, 1, TR_RPC_STATUS_OK) == TR_OK);
	/* wrapper 观察实际送入 transport 的状态，证明 owner 已经把
	 * 非法 OK 改成 INTERNAL；不以 sleep 或发布 finish 代替该屏障。 */
	wait_count(&final_status_attempts[TR_RPC_STATUS_INTERNAL], 10);
	CHECK(!atomic_load(&final_status_attempts[TR_RPC_STATUS_OK]));
	unsigned packets = atomic_load(&f.server.packets);
	send_record(&f, slot, 2, TR_RPC_RECORD_MESSAGE, 80);
	send_record(&f, slot, 2, TR_RPC_RECORD_HALF_CLOSE, 0);
	wait_count(&f.server.packets, packets + 2);
	CHECK(atomic_load(&c.active) == 1 && !atomic_load(&c.terminals));
	producer_semantic(&f, false, TR_RPC_STATUS_INTERNAL);
	gate_open(&f.gate);
	wait_count(&c.seen[TR_RPC_V2_REQUEST_MESSAGE], 1);
	producer_idle(&f);
	CHECK(atomic_load(&c.seen[TR_RPC_V2_REQUEST_MESSAGE]) == 1 &&
	      !atomic_load(&c.seen[TR_RPC_V2_REQUEST_HALF_CLOSE]));
	CHECK(!atomic_load(
	        &final_status_attempts[TR_RPC_STATUS_PERMISSION_DENIED]));
	CHECK(!atomic_load(&c.terminals));
	atomic_store(&blocked_kind, 0);
	producer_finished(&f, &c, slot, TR_RPC_STATUS_INTERNAL);
	check_metadata(&f.client, slot, 1, true);
	producer_semantic(&f, true, TR_RPC_STATUS_INTERNAL);
	tr_rpc_v2_producer_release(p);
	fixture_destroy(&f, false);
}

int main(void)
{
	test_producer_staged_and_retry();
	test_producer_real_window();
	test_producer_failure_preserves_phase();
	test_producer_cancel_or_stop(false);
	test_producer_cancel_or_stop(true);
	test_producer_work_terminal_race(false);
	test_producer_work_terminal_race(true);
	test_producer_release(false);
	test_producer_release(true);
	test_producer_quota_survives_dispatch();
	test_producer_copy_outlives_domain_close();
	test_producer_deadline();
	test_producer_rejects_sync_write();
	test_producer_cardinality_fallback_stops_work();
	test_metadata_encoding();
	test_metadata_queued_phase();
	for (enum metadata_mode mode = METADATA_BOTH;
	     mode <= METADATA_BAD_APPEND; ++mode)
		test_metadata_error(mode);
	test_open_admission_partial_and_timer();
	test_open_admission_validation_boundary();
	test_open_admission_clock_failure();
	test_open_admission_handoff();
	test_shared_executor_backpressure();
	test_constructor();
	test_shapes();
	test_parallel_and_owned_data();
	test_send_retry(TR_RPC_RECORD_INITIAL_METADATA);
	test_send_retry(TR_RPC_RECORD_MESSAGE);
	test_send_retry(TR_RPC_RECORD_FINAL_STATUS);
	test_real_window();
	for (unsigned i = 0; i < 4; ++i)
		test_business_error(i);
	test_pending_limit();
	test_cancel_and_stop(0);
	test_cancel_and_stop(1);
	test_cancel_and_stop(2);
	puts("rpc_record_dispatch: ok");
	return 0;
}
