#define _POSIX_C_SOURCE 200809L
/* 专测宿主契约：下层为可控替身，不宣称真实 TCP/派发器集成已通过。
 * 链接实际 rpc_record_endpoint.c 与真实 memory_budget 原语。 */
#include "../src/rpc/rpc_record_endpoint.h"
#include "../src/execution/reactor_internal.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/status.h"

#include <pthread.h>
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
#define CAP 4U
struct fixture;
struct tr_reactor {
	struct fixture *f;
	bool joined;
};
struct tr_channel {
	struct tr_reactor *reactor;
};
struct tr_task_executor {
	struct fixture *f;
};
struct fake_call {
	struct tr_stream_handle stream;
	struct tr_rpc_dispatch_handle handle;
	bool active, pending, terminal, signalled, wire_final;
	enum tr_rpc_status status;
};
struct tr_rpc_record_dispatch {
	struct fixture *f;
	tr_rpc_dispatch_terminal_fn terminal;
	void *arg;
	struct fake_call calls[CAP];
	uint64_t generation;
	bool cleanup;
};
struct fixture {
	struct tr_reactor reactor;
	struct tr_channel channel;
	struct tr_task_executor executor;
	struct tr_rpc_record_dispatch *dispatch;
	struct tr_rpc_record_endpoint *ep;
	struct tr_memory_budget budget;
	tr_stream_data_cb data;
	tr_stream_event_cb event;
	void *handler_arg;
	tr_reactor_timer_cb tick;
	void *timer_arg;
	bool guard, hold_work, fail_detach, fail_unreg, fail_register, fail_arm,
	        fail_install;
	bool inline_closed, inline_data;
	struct tr_stream_handle nested_streams[CAP];
	unsigned nested_count;
	struct tr_rpc_endpoint_result results[CAP];
	unsigned send_rejects, close_rejects, credit_stop_rejects, credit_stops,
	        sends, closes, receive_calls, done_calls;
	unsigned reset_rejects, reset_attempts, resets;
	int reset_reason;
	struct tr_stream_handle reset_stream;
	unsigned executor_dispatches, dispatch_pumps, reports, retire_calls;
	unsigned sequence, last_send_sequence, last_close_sequence,
	        last_credit_stop_sequence;
	struct tr_stream_handle last_credit_stop_stream;
	unsigned fail_alloc, allocations;
	uint32_t wire_status;
	struct tr_rpc_endpoint_result last;
	struct tr_buffer *tx[64];
	unsigned ntx;
};
static struct fixture *active;
static struct tr_reactor *owner;
static uint64_t now_ns = 1000000000;
static unsigned cases;
void *__real_calloc(size_t, size_t);
void *__wrap_calloc(size_t n, size_t z)
{
	if (active && active->fail_alloc &&
	    ++active->allocations == active->fail_alloc)
		return NULL;
	return __real_calloc(n, z);
}
int __wrap_clock_gettime(clockid_t id, struct timespec *ts)
{
	CHECK(id == CLOCK_MONOTONIC);
	ts->tv_sec = (time_t)(now_ns / 1000000000);
	ts->tv_nsec = (long)(now_ns % 1000000000);
	return 0;
}
int tr_reactor_in_owner_context_for(const struct tr_reactor *r)
{
	return r && owner == r;
}
struct tr_reactor *tr_channel_reactor(struct tr_channel *c)
{
	return c->reactor;
}
uint32_t tr_channel_max_streams(struct tr_channel *c)
{
	(void)c;
	return CAP;
}
int tr_channel_set_handler(struct tr_channel *c, tr_stream_data_cb data,
                           tr_stream_event_cb event, tr_channel_event_cb ce,
                           void *arg)
{
	struct fixture *f = c->reactor->f;
	(void)ce;
	CHECK(owner == c->reactor || (f->guard && c->reactor->joined));
	if ((!data && f->fail_detach) || (data && f->fail_install))
		return TR_ERR_SYS;
	f->data = data;
	f->event = event;
	f->handler_arg = arg;
	return TR_OK;
}
int tr_reactor_timer_register(struct tr_reactor *r, tr_reactor_timer_cb cb,
                              void *arg, struct tr_reactor_timer_handle *out)
{
	CHECK(owner == r);
	if (r->f->fail_register)
		return TR_ERR_SYS;
	r->f->tick = cb;
	r->f->timer_arg = arg;
	*out = (struct tr_reactor_timer_handle){
	        .reactor = r, .slot = 0, .generation = 1};
	return TR_OK;
}
int tr_reactor_timer_arm(struct tr_reactor_timer_handle h, uint64_t due)
{
	CHECK(due > now_ns);
	return h.reactor->f->fail_arm ? TR_ERR_SYS : TR_OK;
}
int tr_reactor_timer_unregister(struct tr_reactor_timer_handle h)
{
	struct fixture *f = h.reactor->f;
	CHECK(owner == h.reactor || (f->guard && h.reactor->joined));
	if (f->fail_unreg)
		return TR_ERR_SYS;
	f->tick = NULL;
	f->timer_arg = NULL;
	return TR_OK;
}
int tr_reactor_recovery_acquire(struct tr_reactor *r,
                                struct tr_reactor_recovery_guard *g)
{
	if (!r->joined || r->f->guard || owner)
		return TR_ERR_STATE;
	r->f->guard = true;
	g->reactor = r;
	return TR_OK;
}
int tr_reactor_recovery_release(struct tr_reactor_recovery_guard *g)
{
	CHECK(g->reactor && g->reactor->f->guard);
	g->reactor->f->guard = false;
	g->reactor = NULL;
	return TR_OK;
}
int tr_rpc_record_encode_header(uint8_t *out, size_t cap,
                                const struct tr_rpc_record_header *h,
                                const struct tr_rpc_record_limits *limits)
{
	CHECK(h->kind == TR_RPC_RECORD_FINAL_STATUS && h->status > 0 &&
	      h->status < TR_RPC_STATUS_COUNT);
	CHECK(cap == 48 && !h->encoded_size && !h->metadata_size &&
	      limits->accepted_compression == 1);
	memset(out, 0, 48);
	memcpy(out, "TRPC", 4);
	tr_put_le16(out + 4, 2);
	tr_put_le16(out + 6, 5);
	tr_put_le32(out + 36, h->status);
	return TR_OK;
}
int tr_stream_sendv(struct tr_stream_handle s, struct tr_buffer *const *bs,
                    uint32_t n)
{
	struct fixture *f = s.channel->reactor->f;
	CHECK(owner == s.channel->reactor && n == 1 && bs[0]->len == 48);
	++f->sends;
	if (f->send_rejects) {
		--f->send_rejects;
		return TR_AGAIN;
	}
	CHECK(f->ntx < 64);
	f->wire_status = tr_get_le32(bs[0]->data + 36);
	f->last_send_sequence = ++f->sequence;
	f->tx[f->ntx++] = bs[0];
	if (f->inline_closed)
		f->event(s, TR_STREAM_EVENT_CLOSED, TR_ERR_CLOSED,
		         f->handler_arg);
	return TR_OK;
}
int tr_stream_stop_credit_on_owner(struct tr_stream_handle s)
{
	struct fixture *f = s.channel->reactor->f;
	CHECK(owner == s.channel->reactor);
	++f->credit_stops;
	if (f->credit_stop_rejects) {
		--f->credit_stop_rejects;
		return TR_ERR_SYS;
	}
	f->last_credit_stop_sequence = ++f->sequence;
	f->last_credit_stop_stream = s;
	return TR_OK;
}
int tr_stream_close(struct tr_stream_handle s)
{
	struct fixture *f = s.channel->reactor->f;
	CHECK(owner == s.channel->reactor);
	CHECK(f->last_credit_stop_sequence > f->last_close_sequence &&
	      f->last_credit_stop_stream.channel == s.channel &&
	      f->last_credit_stop_stream.slot == s.slot &&
	      f->last_credit_stop_stream.generation == s.generation);
	++f->closes;
	if (f->close_rejects) {
		--f->close_rejects;
		return TR_AGAIN;
	}
	f->last_close_sequence = ++f->sequence;
	return TR_OK;
}
int tr_stream_reset_on_owner(struct tr_stream_handle s, int status)
{
	struct fixture *f = s.channel->reactor->f;
	CHECK(owner == s.channel->reactor);
	CHECK(status == TR_ERR_CLOSED || status == TR_ERR_TIMEOUT ||
	      status == TR_ERR_NOMEM || status == TR_ERR_STATE ||
	      status == TR_ERR_SYS);
	++f->reset_attempts;
	if (f->reset_rejects) {
		--f->reset_rejects;
		return TR_AGAIN;
	}
	++f->resets;
	f->reset_reason = status;
	f->reset_stream = s;
	/* 与 Channel 一样，准入成功可在同一 owner 栈中通知 RESET；
	 * 这里只模拟承担网络退役，不替业务完成或伪造 CLOSED 确认。 */
	f->event(s, TR_STREAM_EVENT_RESET, status, f->handler_arg);
	return TR_OK;
}
static struct fake_call *lookup(struct tr_rpc_dispatch_handle h)
{
	CHECK(h.dispatch && h.id.slot < CAP);
	struct fake_call *c = &h.dispatch->calls[h.id.slot];
	CHECK(c->active && c->handle.id.generation == h.id.generation);
	return c;
}
int tr_rpc_record_dispatch_create_on_owner(
        struct tr_channel *c, struct tr_task_executor *ex,
        const struct tr_rpc_dispatch_method *m, size_t count,
        const struct tr_rpc_record_dispatch_config *cfg,
        tr_rpc_dispatch_terminal_fn term, void *arg,
        struct tr_memory_budget *budget, struct tr_rpc_record_dispatch **out)
{
	(void)m;
	(void)count;
	(void)cfg;
	(void)budget;
	CHECK(owner == c->reactor && ex->f == c->reactor->f);
	struct tr_rpc_record_dispatch *d = calloc(1, sizeof(*d));
	if (!d)
		return TR_ERR_NOMEM;
	d->f = c->reactor->f;
	d->terminal = term;
	d->arg = arg;
	*out = d;
	d->f->dispatch = d;
	return TR_OK;
}
int tr_rpc_record_dispatch_admit_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle s,
                                          struct tr_rpc_dispatch_handle *out)
{
	for (unsigned i = 0; i < CAP; ++i)
		if (!d->calls[i].active) {
			struct fake_call *c = &d->calls[i];
			*c = (struct fake_call){.stream = s, .active = true};
			c->handle = (struct tr_rpc_dispatch_handle){
			        d, {++d->generation, i}};
			*out = c->handle;
			return TR_OK;
		}
	return TR_AGAIN;
}
int tr_rpc_record_dispatch_receive_on_owner(struct tr_rpc_record_dispatch *d,
                                            struct tr_stream_handle s,
                                            const uint8_t *data, size_t size)
{
	CHECK(size);
	++d->f->receive_calls;
	for (unsigned i = 0; i < CAP; ++i)
		if (d->calls[i].active && d->calls[i].stream.slot == s.slot &&
		    d->calls[i].stream.generation == s.generation) {
			struct fake_call *c = &d->calls[i];
			if (c->terminal)
				return TR_ERR_STATE;
			c->pending = true;
			/* 模拟下层在接收回调栈内再次投递同流 DATA。不能静默丢弃
			 * 该字节并继续报告原请求成功。 */
			if (d->f->inline_data) {
				uint8_t nested = 0;
				struct tr_buffer b = {0};
				b.data = &nested;
				b.len = 1;
				b.capacity = 1;
				d->f->inline_data = false;
				CHECK(d->f->data(s, 2, &b, d->f->handler_arg) ==
				      TR_STREAM_DATA_RELEASE);
			}
			unsigned nested_count = d->f->nested_count;
			d->f->nested_count = 0;
			for (unsigned j = 0; j < nested_count; ++j) {
				uint8_t nested = 0;
				struct tr_buffer b = {0};
				b.data = &nested;
				b.len = b.capacity = 1;
				/* 重复登记同一槽位必须合并，不能耗尽事件容量。
				 */
				for (unsigned n = 0; n < 2; ++n)
					CHECK(d->f->data(
					              d->f->nested_streams[j],
					              2, &b,
					              d->f->handler_arg) ==
					      TR_STREAM_DATA_RELEASE);
			}
			c->status = data[0] ? TR_RPC_STATUS_UNIMPLEMENTED
			                    : TR_RPC_STATUS_OK;
			c->wire_final = !data[0];
			return TR_OK;
		}
	return TR_ERR_STALE;
}
int tr_task_executor_dispatch(struct tr_task_executor *e, size_t limit,
                              size_t *n)
{
	CHECK(owner == &e->f->reactor && limit);
	++e->f->executor_dispatches;
	*n = 0;
	return TR_OK;
}
int tr_rpc_record_dispatch_pump_on_owner(struct tr_rpc_record_dispatch *d,
                                         size_t n)
{
	CHECK(n);
	++d->f->dispatch_pumps;
	if (!d->f->hold_work)
		for (unsigned i = 0; i < CAP; ++i) {
			struct fake_call *c = &d->calls[i];
			if (c->active && c->pending && !c->signalled) {
				c->terminal = true;
				c->signalled = true;
				++d->f->done_calls;
				d->terminal(c->handle, c->stream, c->status,
				            TR_RPC_RECORD_ON_OWNER,
				            c->wire_final, d->arg);
			}
		}
	return TR_OK;
}
int tr_rpc_record_dispatch_abort_on_owner(struct tr_rpc_dispatch_handle h,
                                          enum tr_rpc_status st)
{
	struct fake_call *c = lookup(h);
	if (c->terminal)
		return TR_ERR_STATE;
	c->pending = true;
	c->terminal = true;
	c->wire_final = false;
	c->status = st;
	return TR_OK;
}
int tr_rpc_record_dispatch_event_on_owner(struct tr_rpc_record_dispatch *d,
                                          struct tr_stream_handle s,
                                          enum tr_stream_event event,
                                          int status)
{
	enum tr_rpc_status result = TR_RPC_STATUS_UNAVAILABLE;
	if (event == TR_STREAM_EVENT_WRITABLE ||
	    event == TR_STREAM_EVENT_OPENED)
		return TR_OK;
	if (event == TR_STREAM_EVENT_RESET) {
		switch (status) {
		case TR_ERR_CLOSED:
			result = TR_RPC_STATUS_CANCELLED;
			break;
		case TR_ERR_TIMEOUT:
			result = TR_RPC_STATUS_DEADLINE_EXCEEDED;
			break;
		case TR_ERR_NOMEM:
			result = TR_RPC_STATUS_RESOURCE_EXHAUSTED;
			break;
		case TR_ERR_STATE:
			result = TR_RPC_STATUS_INVALID_ARGUMENT;
			break;
		case TR_ERR_SYS:
			result = TR_RPC_STATUS_INTERNAL;
			break;
		default:
			return TR_ERR_INVALID;
		}
	}
	for (unsigned i = 0; i < CAP; ++i)
		if (d->calls[i].active && d->calls[i].stream.slot == s.slot &&
		    d->calls[i].stream.generation == s.generation)
			(void)tr_rpc_record_dispatch_abort_on_owner(
			        d->calls[i].handle, result);
	return TR_OK;
}
int tr_rpc_record_dispatch_result_on_owner(struct tr_rpc_dispatch_handle handle,
                                           struct tr_rpc_dispatch_result *out)
{
	if (!handle.dispatch || !out)
		return TR_ERR_INVALID;
	CHECK(owner == &handle.dispatch->f->reactor);
	if (handle.id.slot >= CAP)
		return TR_ERR_STALE;
	struct fake_call *call = &handle.dispatch->calls[handle.id.slot];
	if (!call->active || call->handle.id.generation != handle.id.generation)
		return TR_ERR_STALE;
	*out = (struct tr_rpc_dispatch_result){
	        .status = call->status,
	        .terminal = call->terminal,
	        .final_accepted = call->terminal && call->wire_final};
	return TR_OK;
}
int tr_rpc_record_dispatch_retire_on_owner(struct tr_rpc_dispatch_handle h)
{
	struct fake_call *c = lookup(h);
	CHECK(c->signalled);
	c->active = false;
	++h.dispatch->f->retire_calls;
	return TR_OK;
}
int tr_rpc_record_dispatch_destroy_on_owner(struct tr_rpc_record_dispatch **p)
{
	for (unsigned i = 0; i < CAP; ++i)
		if ((*p)->calls[i].active && !(*p)->calls[i].signalled)
			return TR_AGAIN;
	(*p)->f->dispatch = NULL;
	free(*p);
	*p = NULL;
	return TR_OK;
}
int tr_rpc_record_dispatch_reap_stopped(struct tr_rpc_record_dispatch **p)
{
	struct tr_rpc_record_dispatch *d = *p;
	CHECK(!owner && d->f->reactor.joined && !d->f->guard);
	if (d->f->hold_work)
		return TR_AGAIN;
	for (unsigned i = 0; i < CAP; ++i)
		if (d->calls[i].active && !d->calls[i].signalled) {
			struct fake_call *c = &d->calls[i];
			c->signalled = true;
			d->terminal(c->handle, c->stream,
			            TR_RPC_STATUS_CANCELLED,
			            TR_RPC_RECORD_OWNER_STOPPED, false, d->arg);
		}
	d->f->dispatch = NULL;
	free(d);
	*p = NULL;
	return TR_OK;
}
static void on_terminal(struct tr_stream_handle stream,
                        struct tr_rpc_endpoint_result r, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_endpoint *p = f->ep;
	CHECK(stream.channel == &f->channel);
	CHECK(tr_rpc_record_endpoint_close_on_owner(&p) == TR_ERR_STATE);
	CHECK(p == f->ep);
	++f->reports;
	f->last = r;
	f->results[stream.slot] = r;
}
static struct tr_rpc_record_endpoint_config configuration(void)
{
	return (struct tr_rpc_record_endpoint_config){
	        .dispatch = {CAP, 4, 4, 4096},
	        .completion_limit = 2,
	        .progress_limit = 2,
	        .progress_period_ns = 1000000,
	        .final_wait_ns = 100000000};
}
static void init(struct fixture *f)
{
	*f = (struct fixture){0};
	f->reactor.f = f;
	f->channel.reactor = &f->reactor;
	f->executor.f = f;
	active = f;
	owner = &f->reactor;
	now_ns = 1000000000;
	tr_memory_budget_init(&f->budget, 1 << 20);
}
static int create(struct fixture *f)
{
	struct tr_rpc_record_endpoint_config cfg = configuration();
	const struct tr_rpc_dispatch_method m = {0};
	return tr_rpc_record_endpoint_create_on_owner(&f->channel, &f->executor,
	                                              &m, 1, &cfg, on_terminal,
	                                              f, &f->budget, &f->ep);
}
static void start(struct fixture *f)
{
	CHECK(create(f) == TR_OK);
	CHECK(tr_rpc_record_endpoint_start_on_owner(f->ep) == TR_OK);
}
static struct tr_stream_handle open_stream(struct fixture *f, unsigned slot,
                                           unsigned gen)
{
	struct tr_stream_handle s = {&f->channel, slot, gen};
	CHECK(f->event);
	f->event(s, TR_STREAM_EVENT_OPENED, TR_OK, f->handler_arg);
	return s;
}
static void request(struct fixture *f, struct tr_stream_handle s, unsigned code)
{
	uint8_t byte = (uint8_t)code;
	struct tr_buffer b = {.data = &byte, .len = 1, .capacity = 1};
	CHECK(f->data(s, 1, &b, f->handler_arg) == TR_STREAM_DATA_RELEASE);
}
static void tick(struct fixture *f, unsigned n)
{
	for (unsigned i = 0; i < n; ++i) {
		CHECK(f->tick);
		now_ns += 1000000;
		CHECK(f->tick(f->timer_arg, now_ns) > now_ns);
	}
}
static void drain(struct fixture *f)
{
	for (unsigned i = 0; i < f->ntx; ++i)
		f->tx[i]->release_cb(f->tx[i]);
	f->ntx = 0;
}
static void close_all(struct fixture *f)
{
	int ret = TR_AGAIN;
	for (unsigned i = 0; i < 100 && ret == TR_AGAIN; ++i) {
		now_ns += 1000000;
		ret = tr_rpc_record_endpoint_close_on_owner(&f->ep);
	}
	CHECK(ret == TR_OK && !f->ep && !f->data && !f->tick);
	drain(f);
	CHECK(atomic_load(&f->budget.current_bytes) == 0);
	owner = NULL;
	active = NULL;
	++cases;
}
static void test_order_and_backpressure(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.send_rejects = 3;
	f.close_rejects = 2;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 1);
	CHECK(!f.closes && !f.sends && !f.done_calls);
	tick(&f, 30);
	CHECK(f.sends == 4 && f.closes == 3 && f.reports == 1 &&
	      f.receive_calls == 1 && f.done_calls == 1);
	CHECK(f.wire_status == TR_RPC_STATUS_UNIMPLEMENTED &&
	      f.last.final_accepted);
	CHECK(f.last_close_sequence > f.last_send_sequence);
	f.event(s, TR_STREAM_EVENT_REMOTE_CLOSED, TR_OK, f.handler_arg);
	tick(&f, 8);
	CHECK(f.retire_calls == 1);
	close_all(&f);
}
static void test_credit_stop_failure_retains_close(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.credit_stop_rejects = 100;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 1);
	tick(&f, 8);
	CHECK(f.sends == 1 && f.credit_stops > 0 && !f.closes && !f.reports &&
	      !f.retire_calls && f.ep && f.dispatch);
	f.credit_stop_rejects = 0;
	tick(&f, 8);
	CHECK(f.sends == 1 && f.closes == 1 && f.reports == 1 &&
	      f.last.final_accepted);
	CHECK(f.last_credit_stop_sequence > f.last_send_sequence &&
	      f.last_close_sequence > f.last_credit_stop_sequence);
	f.event(s, TR_STREAM_EVENT_REMOTE_CLOSED, TR_OK, f.handler_arg);
	tick(&f, 8);
	CHECK(f.retire_calls == 1);
	close_all(&f);
}
static void test_inline_data_is_not_silent_success(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.inline_data = true;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	tick(&f, 12);
	CHECK(f.receive_calls == 1 && f.done_calls == 1 && f.reports == 1);
	CHECK(f.last.status == TR_RPC_STATUS_UNAVAILABLE &&
	      f.last.final_accepted);
	close_all(&f);
}
static void test_deferred_events_exceed_turn_budget(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	/* 4 条流超过每轮 2 个事件预算，且公平 cursor 已经被多次推进。 */
	for (unsigned i = 0; i < CAP; ++i) {
		f.nested_streams[i] = open_stream(&f, i, 1);
		request(&f, f.nested_streams[i], 0);
	}
	unsigned prior_pumps = f.dispatch_pumps;
	f.nested_count = CAP;
	f.hold_work = false;
	request(&f, f.nested_streams[CAP - 1], 0);
	CHECK(!f.reports && f.dispatch_pumps == prior_pumps);
	tick(&f, 20);
	CHECK(f.done_calls == CAP && f.reports == CAP && f.sends == CAP);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(f.results[i].status == TR_RPC_STATUS_UNAVAILABLE &&
		      f.results[i].final_accepted);
	close_all(&f);
}
static void test_deadline(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.send_rejects = 100;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 1);
	tick(&f, 125);
	CHECK(f.sends > 0 && f.sends < 100 && !f.closes && f.resets == 1 &&
	      f.reports == 1 && f.retire_calls == 1 && !f.last.final_accepted);
	CHECK(f.reset_reason == TR_ERR_SYS &&
	      f.last.status == TR_RPC_STATUS_UNIMPLEMENTED);
	close_all(&f);
}
static void *release_tx_elsewhere(void *arg)
{
	struct tr_buffer *buffer = arg;
	buffer->release_cb(buffer);
	return NULL;
}
static void test_tx_outlives_host(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 1);
	tick(&f, 8);
	CHECK(f.ntx == 1);
	int ret = tr_rpc_record_endpoint_close_on_owner(&f.ep);
	CHECK(ret == TR_OK && !f.ep &&
	      atomic_load(&f.budget.current_bytes) > 0);
	CHECK(!memcmp(f.tx[0]->data, "TRPC", 4));
	pthread_t reaper;
	CHECK(pthread_create(&reaper, NULL, release_tx_elsewhere, f.tx[0]) ==
	      0);
	CHECK(pthread_join(reaper, NULL) == 0);
	f.ntx = 0;
	CHECK(!atomic_load(&f.budget.current_bytes));
	owner = NULL;
	active = NULL;
	++cases;
}
static void test_success_and_inline_event(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.inline_closed = true;
	struct tr_stream_handle a = open_stream(&f, 0, 1);
	request(&f, a, 1);
	tick(&f, 12);
	CHECK(f.reports == 1 && f.sends == 1); // CLOSED 不能丢失后永久占槽
	CHECK(f.retire_calls == 1);
	struct tr_stream_handle b = open_stream(&f, 0, 2);
	request(&f, b, 0);
	tick(&f, 12);
	CHECK(f.reports == 2 && f.sends == 1 &&
	      f.last.status == TR_RPC_STATUS_OK && f.last.final_accepted);
	close_all(&f);
}
static void test_close_while_worker_pending(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	CHECK(tr_rpc_record_endpoint_close_on_owner(&f.ep) == TR_AGAIN &&
	      f.ep && f.data && f.tick);
	/* close 只推进有界的公平扫描；让 cursor 覆盖当前调用，worker
	 * 保持未退出，不能靠完成回调触发下面的 RESET 断言。 */
	tick(&f, 8);
	CHECK(f.resets == 1 && f.reset_reason == TR_ERR_CLOSED && !f.reports &&
	      !f.done_calls && !f.retire_calls && !f.closes && !f.sends);
	f.hold_work = false;
	tick(&f, 12);
	CHECK(f.reports == 1 && f.last.status == TR_RPC_STATUS_CANCELLED);
	close_all(&f);
}
static void test_reset_before_worker_quiescent(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	f.reset_rejects = 2;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	struct tr_rpc_dispatch_handle call = f.dispatch->calls[0].handle;
	CHECK(tr_rpc_record_dispatch_abort_on_owner(
	              call, TR_RPC_STATUS_DEADLINE_EXCEEDED) == TR_OK);
	tick(&f, 20);
	CHECK(f.reset_attempts == 3 && f.resets == 1 &&
	      f.reset_reason == TR_ERR_TIMEOUT &&
	      f.reset_stream.generation == s.generation);
	CHECK(f.dispatch->calls[0].active && f.dispatch->calls[0].terminal &&
	      !f.dispatch->calls[0].signalled && !f.done_calls && !f.reports &&
	      !f.retire_calls && !f.sends && !f.closes);
	tick(&f, 8);
	CHECK(f.reset_attempts == 3 && !f.reports && !f.retire_calls);
	/* 不投递 CLOSED/确认，证明网络责任已交给 Channel 后，RPC 只等
	 * 自己的 worker 完成；完成之前仍保留原调用和业务参数。 */
	f.hold_work = false;
	tick(&f, 12);
	CHECK(f.done_calls == 1 && f.reports == 1 && f.retire_calls == 1 &&
	      !f.dispatch->calls[0].active && !f.last.final_accepted &&
	      f.last.status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	CHECK(f.resets == 1 && !f.sends && !f.closes);
	close_all(&f);
}
static void test_peer_reset_while_worker_pending(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	f.event(s, TR_STREAM_EVENT_RESET, TR_ERR_TIMEOUT, f.handler_arg);
	CHECK(f.dispatch->calls[0].terminal &&
	      f.dispatch->calls[0].status == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      !f.dispatch->calls[0].signalled && !f.reports && !f.retire_calls);
	/* 后到断连不得覆盖先前 RESET 原因，也不能代替 worker done。 */
	f.event(s, TR_STREAM_EVENT_ERROR, TR_ERR_SYS, f.handler_arg);
	tick(&f, 8);
	CHECK(!f.reports && !f.retire_calls && !f.resets && !f.sends &&
	      f.dispatch->calls[0].status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	f.hold_work = false;
	tick(&f, 12);
	CHECK(f.done_calls == 1 && f.reports == 1 && f.retire_calls == 1 &&
	      f.last.status == TR_RPC_STATUS_DEADLINE_EXCEEDED &&
	      !f.last.final_accepted && !f.resets && !f.closes && !f.sends);
	close_all(&f);
}
static void test_reset_preserves_accepted_final(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	tick(&f, 8);
	CHECK(f.reports == 1 && f.closes == 1 && !f.resets &&
	      f.last.status == TR_RPC_STATUS_OK && f.last.final_accepted &&
	      !f.retire_calls);
	f.event(s, TR_STREAM_EVENT_RESET, TR_ERR_TIMEOUT, f.handler_arg);
	tick(&f, 8);
	CHECK(f.reports == 1 && f.done_calls == 1 && f.retire_calls == 1 &&
	      f.last.status == TR_RPC_STATUS_OK && f.last.final_accepted &&
	      f.closes == 1 && !f.resets && !f.sends);
	close_all(&f);
}
static void test_failures(void)
{
	for (unsigned alloc = 1; alloc <= 3; ++alloc) {
		struct fixture f;
		init(&f);
		f.fail_alloc = alloc;
		CHECK(create(&f) == TR_ERR_NOMEM && !f.ep);
		CHECK(!atomic_load(&f.budget.current_bytes));
		++cases;
	}
	for (unsigned stage = 0; stage < 3; ++stage) {
		struct fixture f;
		init(&f);
		CHECK(create(&f) == TR_OK);
		f.fail_register = stage == 0;
		f.fail_arm = stage == 1;
		f.fail_install = stage == 2;
		CHECK(tr_rpc_record_endpoint_start_on_owner(f.ep) ==
		              TR_ERR_SYS &&
		      f.ep);
		f.fail_register = f.fail_arm = f.fail_install = false;
		close_all(&f);
	}
	struct fixture f;
	init(&f);
	start(&f);
	f.fail_detach = true;
	CHECK(tr_rpc_record_endpoint_close_on_owner(&f.ep) == TR_ERR_SYS &&
	      f.ep && f.data && f.tick);
	/* 派发器已经销毁，但解绑失败仍可能收到新 OPEN。继续拒绝并报告，
	 * 不得向已释放派发器转发，也不能静默遗失新流的关闭责任。 */
	struct tr_stream_handle late = open_stream(&f, 1, 7);
	(void)late;
	tick(&f, 12);
	CHECK(f.reports == 1 && f.last.status == TR_RPC_STATUS_UNAVAILABLE);
	f.fail_detach = false;
	f.fail_unreg = true;
	CHECK(tr_rpc_record_endpoint_close_on_owner(&f.ep) == TR_ERR_SYS &&
	      f.ep && !f.data && f.tick);
	// 上层派发器已销毁，残留定时器必须安全，不再访问其对象。
	tick(&f, 2);
	f.fail_unreg = false;
	close_all(&f);
}
static void test_stopped_cleanup(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	struct tr_stream_handle s = open_stream(&f, 0, 1);
	request(&f, s, 0);
	owner = NULL;
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_ERR_STATE);
	f.reactor.joined = true;
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_AGAIN && f.ep);
	CHECK(f.reports == 0 && f.sends == 0);
	f.hold_work = false;
	f.fail_unreg = true;
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_ERR_SYS && f.ep);
	CHECK(f.reports == 1 &&
	      f.last.delivery == TR_RPC_RECORD_OWNER_STOPPED && !f.sends);
	f.fail_unreg = false;
	CHECK(tr_rpc_record_endpoint_reap_stopped(&f.ep) == TR_OK && !f.ep);
	CHECK(f.reports == 1 && !f.tick && !f.event &&
	      !atomic_load(&f.budget.current_bytes));
	active = NULL;
	++cases;
}

static void test_frame_slot_reuse(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	struct tr_stream_handle first = open_stream(&f, 0, 1);
	request(&f, first, 1);
	tick(&f, 8);
	CHECK(f.sends == 1 && f.ntx == 1);
	f.event(first, TR_STREAM_EVENT_REMOTE_CLOSED, TR_OK, f.handler_arg);
	tick(&f, 8);
	CHECK(f.retire_calls == 1);
	struct tr_stream_handle second = open_stream(&f, 0, 2);
	request(&f, second, 1);
	tick(&f, 8);
	CHECK(f.sends == 1); /* 旧TX持有同一预分配帧，不能改写成新终局。 */
	drain(&f);
	tick(&f, 8);
	CHECK(f.sends == 2 && f.reports == 2);
	close_all(&f);
}
static void test_generations_under_load(void)
{
	struct fixture f;
	init(&f);
	start(&f);
	f.hold_work = true;
	struct tr_stream_handle streams[CAP];
	for (unsigned i = 0; i < CAP; ++i) {
		streams[i] = open_stream(&f, i, 1);
		request(&f, streams[i], 0);
		f.event(streams[i], TR_STREAM_EVENT_ERROR, TR_ERR_SYS,
		        f.handler_arg);
	}
	for (unsigned i = 0; i < CAP; ++i)
		(void)open_stream(&f, i, 2);
	tick(&f, 16);
	CHECK(f.reports == CAP &&
	      f.last.status == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	f.hold_work = false;
	tick(&f, 16);
	CHECK(f.reports == CAP * 2 && f.receive_calls == CAP);
	close_all(&f);
}

int main(void)
{
	test_order_and_backpressure();
	test_credit_stop_failure_retains_close();
	test_inline_data_is_not_silent_success();
	test_deferred_events_exceed_turn_budget();
	test_deadline();
	test_tx_outlives_host();
	test_success_and_inline_event();
	test_close_while_worker_pending();
	test_reset_before_worker_quiescent();
	test_peer_reset_while_worker_pending();
	test_reset_preserves_accepted_final();
	test_failures();
	test_stopped_cleanup();
	test_frame_slot_reuse();
	test_generations_under_load();
	printf("rpc_record_endpoint_contract: %u cases passed\n", cases);
	return 0;
}
