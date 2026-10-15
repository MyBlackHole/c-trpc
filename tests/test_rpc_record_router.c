#define _POSIX_C_SOURCE 200809L
#include "../src/rpc/rpc_record_router.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/socket.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/status.h"

#include <poll.h>
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
static _Thread_local unsigned fail_at, allocation;
static _Thread_local unsigned recovery_at, recovery_calls;
int __real_tr_reactor_recovery_acquire(struct tr_reactor *,
                                       struct tr_reactor_recovery_guard *);
int __wrap_tr_reactor_recovery_acquire(struct tr_reactor *reactor,
                                       struct tr_reactor_recovery_guard *guard)
{
	if (recovery_at && ++recovery_calls == recovery_at)
		return TR_ERR_SYS;
	return __real_tr_reactor_recovery_acquire(reactor, guard);
}

void *__wrap_malloc(size_t n)
{
	if (fail_at && ++allocation == fail_at)
		return NULL;
	return __real_malloc(n);
}
void *__wrap_calloc(size_t n, size_t size)
{
	if (fail_at && ++allocation == fail_at)
		return NULL;
	return __real_calloc(n, size);
}

struct fixture;
struct method_context {
	struct fixture *fixture;
	struct tr_rpc_method_desc descriptor;
	unsigned index;
};
struct peer {
	struct tr_reactor *reactor;
	struct tr_channel *channel;
	struct tr_buffer_pool pool;
	struct tr_memory_budget budget;
	struct fixture *fixture;
	struct tr_stream_handle streams[CAP];
	struct tr_rpc_record_stream *calls[CAP];
	struct tr_rpc_route_handle routes[CAP];
	_Atomic unsigned opened;
	_Atomic unsigned received[CAP][6];
	_Atomic unsigned terminals[CAP];
	_Atomic int last_error[CAP];
	uint32_t status[CAP];
	size_t body_size[CAP];
	enum tr_rpc_record_delivery delivery[CAP];
	int server;
	int disabled;
};
struct fixture {
	struct peer client, server;
	struct tr_rpc_record_router *router;
	struct method_context contexts[4];
	struct tr_rpc_record_method methods[4];
	uint32_t capacity;
	unsigned auto_reply;
};

static uint64_t now_ms(void)
{
	struct timespec t;
	CHECK(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
	return (uint64_t)t.tv_sec * 1000 + (uint64_t)t.tv_nsec / 1000000;
}
static void wait_count(_Atomic unsigned *p, unsigned n)
{
	uint64_t until = now_ms() + 15000;
	while (atomic_load(p) < n) {
		CHECK(now_ms() < until);
		sched_yield();
	}
}
static struct tr_rpc_record_limits limits(void)
{
	return (struct tr_rpc_record_limits){BODY, BODY,
	                                     TR_RPC_METADATA_MAX_BYTES,
	                                     TR_RPC_RECORD_ACCEPT_NONE};
}

struct bytes_buffer {
	struct tr_buffer buffer;
	uint8_t bytes[];
};
static void release_bytes(struct tr_buffer *b)
{
	free(b);
}
static struct tr_buffer *bytes(const uint8_t *data, size_t len)
{
	struct bytes_buffer *b = malloc(sizeof(*b) + len);
	CHECK(b);
	b->buffer = (struct tr_buffer){.data = b->bytes,
	                               .capacity = (uint32_t)len,
	                               .len = (uint32_t)len,
	                               .release_cb = release_bytes};
	if (len)
		memcpy(b->bytes, data, len);
	return &b->buffer;
}

static void client_received(struct tr_rpc_record_stream *call,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct peer *p = arg;
	unsigned i = 0;
	while (i < CAP && p->calls[i] != call)
		++i;
	CHECK(i < CAP);
	atomic_fetch_add(&p->received[i][view->header.kind], 1);
}
static void client_terminal(struct tr_rpc_record_stream *call,
                            enum tr_rpc_status status,
                            enum tr_rpc_record_delivery delivery, void *arg)
{
	struct peer *p = arg;
	unsigned i = 0;
	while (i < CAP && p->calls[i] != call)
		++i;
	CHECK(i < CAP);
	p->status[i] = (uint32_t)status;
	p->delivery[i] = delivery;
	CHECK(atomic_fetch_add(&p->terminals[i], 1) == 0);
}
static void terminal(struct tr_rpc_route_handle route,
                     struct tr_stream_handle s, enum tr_rpc_status status,
                     enum tr_rpc_record_delivery delivery, void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_router *same = f->router;
	CHECK(s.channel == f->server.channel && s.slot < CAP);
	CHECK(route.router == f->router);
	CHECK(route.generation == f->server.routes[s.slot].generation);
	CHECK((delivery == TR_RPC_RECORD_ON_OWNER) ==
	      !!tr_reactor_in_owner_context_for(f->server.reactor));
	if (delivery == TR_RPC_RECORD_ON_OWNER) {
		CHECK(tr_rpc_record_router_retire_on_owner(route) ==
		      TR_ERR_STATE);
		CHECK(tr_rpc_record_router_destroy_on_owner(&same) ==
		      TR_ERR_STATE);
	} else {
		CHECK(status == TR_RPC_STATUS_CANCELLED);
		CHECK(tr_rpc_record_router_reap_stopped(&same) == TR_ERR_STATE);
	}
	CHECK(same == f->router);
	f->server.status[s.slot] = (uint32_t)status;
	f->server.delivery[s.slot] = delivery;
	CHECK(atomic_fetch_add(&f->server.terminals[s.slot], 1) == 0);
}

static void method_received(struct tr_rpc_route_handle route,
                            const struct tr_rpc_record_view *view, void *arg)
{
	struct method_context *m = arg;
	struct fixture *f = m->fixture;
	struct tr_stream_handle s = {0};
	unsigned index = 0;
	while (index < CAP &&
	       f->server.routes[index].generation != route.generation)
		++index;
	CHECK(index < CAP);
	s = f->server.streams[index];
	CHECK(tr_reactor_in_owner_context_for(f->server.reactor));
	CHECK(tr_rpc_record_router_receive_on_owner(f->router, s, NULL, 0) ==
	      TR_ERR_STATE);
	CHECK(tr_rpc_record_router_retire_on_owner(route) == TR_ERR_STATE);
	if (view->header.kind == TR_RPC_RECORD_OPEN) {
		CHECK(view->header.service_id == m->descriptor.service_id);
		CHECK(view->header.method_id == m->descriptor.method_id);
	}
	if (view->header.kind == TR_RPC_RECORD_MESSAGE) {
		f->server.body_size[index] = view->header.decoded_size;
		for (size_t i = 0; i < view->header.decoded_size; ++i)
			CHECK(view->body[i] == (uint8_t)(i * 7));
	}
	atomic_fetch_add(&f->server.received[index][view->header.kind], 1);
	/* 这里只生成空响应，用来证明注册回调和状态绑定。不是在 owner 中
	 * 执行耗时业务；生产工作线程派发仍是下一接口迁移的责任。
	 */
	if (f->auto_reply && view->header.kind == TR_RPC_RECORD_HALF_CLOSE) {
		struct tr_rpc_record_header h = {
		        .kind = TR_RPC_RECORD_INITIAL_METADATA};
		CHECK(tr_rpc_record_router_send_on_owner(route, &h, NULL,
		                                         NULL) == TR_OK);
		if (m->descriptor.response_cardinality == TR_RPC_ONE) {
			h = (struct tr_rpc_record_header){
			        .kind = TR_RPC_RECORD_MESSAGE,
			        .codec_id = m->descriptor.response_codec_id};
			CHECK(tr_rpc_record_router_send_on_owner(
			              route, &h, NULL, NULL) == TR_OK);
		}
		h = (struct tr_rpc_record_header){
		        .kind = TR_RPC_RECORD_FINAL_STATUS};
		CHECK(tr_rpc_record_router_send_on_owner(route, &h, NULL,
		                                         NULL) == TR_OK);
		/* 下层延后终局直到当前 received 退出。 */
		CHECK(atomic_load(&f->server.terminals[index]) == 0);
	}
}

static enum tr_stream_data_disposition
on_data(struct tr_stream_handle s, uint64_t id, struct tr_buffer *b, void *arg)
{
	struct peer *p = arg;
	int ret = TR_OK;
	(void)id;
	if (p->disabled)
		return TR_STREAM_DATA_RELEASE;
	CHECK(s.slot < CAP);
	if (p->server)
		ret = tr_rpc_record_router_receive_on_owner(p->fixture->router,
		                                            s, b->data, b->len);
	else if (p->calls[s.slot])
		ret = tr_rpc_record_stream_receive_on_owner(p->calls[s.slot], s,
		                                            b->data, b->len);
	atomic_store(&p->last_error[s.slot], ret);
	return TR_STREAM_DATA_RELEASE;
}
static void on_event(struct tr_stream_handle s, enum tr_stream_event event,
                     int status, void *arg)
{
	struct peer *p = arg;
	(void)status;
	if (p->disabled)
		return;
	CHECK(s.slot < CAP);
	if (event == TR_STREAM_EVENT_OPENED) {
		p->streams[s.slot] = s;
		if (p->server) {
			int ret = tr_rpc_record_router_admit_on_owner(
			        p->fixture->router, s, &p->routes[s.slot]);
			atomic_store(&p->last_error[s.slot], ret);
		}
		atomic_fetch_add(&p->opened, 1);
	} else if (p->server && p->routes[s.slot].router) {
		int ret = tr_rpc_record_router_event_on_owner(
		        p->fixture->router, s, event);
		CHECK(ret == TR_OK || ret == TR_ERR_STALE);
	} else if (!p->server && p->calls[s.slot]) {
		CHECK(tr_rpc_record_stream_event_on_owner(p->calls[s.slot], s,
		                                          event) == TR_OK);
	}
}
static void tcp_pair(int *a, int *b)
{
	int listener = -1;
	uint16_t port;
	int ret;
	CHECK(tr_tcp_listen_ipv4("127.0.0.1", 0, 8, &listener, &port) == TR_OK);
	ret = tr_tcp_connect_ipv4("127.0.0.1", port, a);
	CHECK(ret == TR_OK || ret == TR_IN_PROGRESS);
	uint64_t until = now_ms() + 10000;
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
static void init_peer(struct peer *p, struct fixture *f, int fd, int server)
{
	struct tr_reactor_config rc = {.max_connections = 4,
	                               .command_capacity = 64,
	                               .tx_item_capacity = 64,
	                               .control_tx_item_capacity = 64,
	                               .rx_buffer_count = 16,
	                               .rx_buffer_size = 256,
	                               .max_payload_len = 256};
	struct tr_channel_config cc = {.role = server ? TR_CHANNEL_SERVER
	                                              : TR_CHANNEL_CLIENT,
	                               .mode = TR_CHANNEL_SHARED_CONNECTION,
	                               .max_streams = CAP,
	                               .initial_window_bytes = 65536,
	                               .window_update_threshold_bytes = 1,
	                               .max_message_bytes = BODY + 1024};
	struct tr_conn_handle conn;
	*p = (struct peer){.fixture = f, .server = server};
	atomic_init(&p->opened, 0);
	for (unsigned i = 0; i < CAP; ++i) {
		atomic_init(&p->terminals[i], 0);
		atomic_init(&p->last_error[i], 0);
		for (unsigned j = 0; j < 6; ++j)
			atomic_init(&p->received[i][j], 0);
	}
	tr_memory_budget_init(&p->budget, 8 * 1024 * 1024);
	CHECK(tr_buffer_pool_init(&p->pool, 8, BODY + 1024) == TR_OK);
	cc.reassembly_pool = &p->pool;
	CHECK(tr_reactor_create(&rc, NULL, NULL, NULL, &p->reactor) == TR_OK);
	CHECK(tr_reactor_start(p->reactor) == TR_OK);
	CHECK(tr_reactor_adopt_fd(p->reactor, fd, &conn) == TR_OK);
	CHECK(tr_channel_create(&cc, conn, conn, on_data, on_event, NULL, p,
	                        &p->channel) == TR_OK);
}
static int create_router(void *arg)
{
	struct fixture *f = arg;
	return tr_rpc_record_router_create_on_owner(
	        f->server.channel, f->methods, 4, f->capacity, terminal, f,
	        &f->server.budget, &f->router);
}
static void init_fixture(struct fixture *f, uint32_t capacity)
{
	int a = -1, b = -1;
	*f = (struct fixture){.capacity = capacity};
	tcp_pair(&a, &b);
	init_peer(&f->client, f, a, 0);
	init_peer(&f->server, f, b, 1);
	CHECK(tr_channel_wait_ready(f->client.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	CHECK(tr_channel_wait_ready(f->server.channel, TR_LANE_BULK, 10000) ==
	      TR_OK);
	for (unsigned i = 0; i < 4; ++i) {
		/* 刻意乱序；不同服务可复用方法号，基数只从本地描述取得。 */
		struct tr_rpc_method_desc d = {9U - i / 2,
		                               4U + (i % 2),
		                               i & 1 ? TR_RPC_MANY : TR_RPC_ONE,
		                               i & 2 ? TR_RPC_MANY : TR_RPC_ONE,
		                               TR_RPC_CODEC_RAW,
		                               TR_RPC_CODEC_RAW,
		                               TR_LANE_BULK,
		                               BODY,
		                               BODY};
		f->contexts[i] = (struct method_context){f, d, i};
		f->methods[i] = (struct tr_rpc_record_method){
		        d, method_received, &f->contexts[i]};
	}
}
static void open_stream(struct fixture *f, unsigned count)
{
	struct tr_stream_handle h;
	CHECK(tr_stream_open(f->client.channel, TR_LANE_BULK, &h) == TR_OK);
	wait_count(&f->client.opened, count);
	wait_count(&f->server.opened, count);
}
struct operation {
	struct fixture *f;
	unsigned index, method;
	const uint8_t *data;
	size_t size;
};
static int make_call(void *arg)
{
	struct operation *o = arg;
	struct peer *p = &o->f->client;
	struct tr_rpc_record_stream_config c = {
	        TR_RPC_RECORD_REQUEST_DIRECTION,
	        o->f->contexts[o->method].descriptor};
	struct tr_rpc_record_stream_callbacks cb = {client_received,
	                                            client_terminal, p};
	return tr_rpc_record_stream_create_on_owner(
	        p->streams[o->index], &c, &cb, &p->budget, &p->calls[o->index]);
}
static int raw_send(void *arg)
{
	struct operation *o = arg;
	struct tr_buffer *b = bytes(o->data, o->size);
	int ret = tr_stream_send(o->f->client.streams[o->index], b);
	if (ret != TR_OK)
		tr_buffer_release(b);
	return ret;
}
static void raw(struct fixture *f, unsigned index, const uint8_t *data,
                size_t size)
{
	struct operation o = {f, index, 0, data, size};
	CHECK(tr_reactor_call(f->client.reactor, raw_send, &o) == TR_OK);
}
static void open_bytes(struct fixture *f, unsigned index, unsigned method,
                       int bytewise, const uint8_t *metadata,
                       uint32_t metadata_size)
{
	uint8_t wire[TR_RPC_RECORD_HEADER_SIZE + TR_RPC_METADATA_MAX_BYTES];
	struct tr_rpc_record_header h = {
	        .kind = TR_RPC_RECORD_OPEN,
	        .service_id = f->contexts[method].descriptor.service_id,
	        .method_id = f->contexts[method].descriptor.method_id,
	        .metadata_size = metadata_size};
	struct tr_rpc_record_limits l = limits();
	CHECK(tr_rpc_record_encode_header(wire, sizeof(wire), &h, &l) == TR_OK);
	if (metadata_size)
		memcpy(wire + TR_RPC_RECORD_HEADER_SIZE, metadata,
		       metadata_size);
	if (bytewise) {
		for (size_t i = 0;
		     i < TR_RPC_RECORD_HEADER_SIZE + metadata_size; ++i)
			raw(f, index, wire + i, 1);
	} else
		raw(f, index, wire, TR_RPC_RECORD_HEADER_SIZE + metadata_size);
}
static int send_valid_call(void *arg)
{
	struct operation *o = arg;
	struct tr_rpc_method_desc *m = &o->f->contexts[o->method].descriptor;
	struct tr_rpc_record_stream *c = o->f->client.calls[o->index];
	struct tr_rpc_record_header h = {.kind = TR_RPC_RECORD_OPEN,
	                                 .service_id = m->service_id,
	                                 .method_id = m->method_id};
	CHECK(tr_rpc_record_stream_send_on_owner(c, &h, NULL, NULL) == TR_OK);
	if (m->request_cardinality == TR_RPC_ONE) {
		h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_MESSAGE,
		                                  .codec_id = TR_RPC_CODEC_RAW};
		CHECK(tr_rpc_record_stream_send_on_owner(c, &h, NULL, NULL) ==
		      TR_OK);
	}
	h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_HALF_CLOSE};
	return tr_rpc_record_stream_send_on_owner(c, &h, NULL, NULL);
}
static int disable(void *arg)
{
	((struct peer *)arg)->disabled = 1;
	return TR_OK;
}
static int destroy_router(void *arg)
{
	struct fixture *f = arg;
	f->server.disabled = 1;
	return tr_rpc_record_router_destroy_on_owner(&f->router);
}
static void cleanup(struct fixture *f, int stopped)
{
	CHECK(tr_reactor_call(f->client.reactor, disable, &f->client) == TR_OK);
	if (stopped) {
		CHECK(tr_reactor_call(f->server.reactor, disable, &f->server) ==
		      TR_OK);
		CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
		/* 第二个绑定的租约获取失败：第一个绑定已回收仍不能遗失路由器，
		 * 再次执行只处理剩余责任，终局不能重复。 */
		recovery_calls = 0;
		recovery_at = 3;
		CHECK(tr_rpc_record_router_reap_stopped(&f->router) ==
		      TR_ERR_SYS);
		CHECK(f->router != NULL);
		recovery_at = 0;
		CHECK(tr_rpc_record_router_reap_stopped(&f->router) == TR_OK);
	} else
		CHECK(tr_reactor_call(f->server.reactor, destroy_router, f) ==
		      TR_OK);
	CHECK(tr_reactor_stop(f->client.reactor) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		if (f->client.calls[i])
			CHECK(tr_rpc_record_stream_reap_stopped(
			              &f->client.calls[i]) == TR_OK);
	CHECK(tr_channel_destroy(f->client.channel) == TR_OK);
	CHECK(tr_channel_destroy(f->server.channel) == TR_OK);
	CHECK(tr_reactor_stop(f->server.reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->client.reactor) == TR_OK);
	CHECK(tr_reactor_destroy(f->server.reactor) == TR_OK);
	CHECK(tr_buffer_pool_destroy(&f->client.pool) == TR_OK);
	CHECK(tr_buffer_pool_destroy(&f->server.pool) == TR_OK);
	CHECK(atomic_load(&f->client.budget.current_bytes) == 0);
	CHECK(atomic_load(&f->server.budget.current_bytes) == 0);
}

static void test_method_dispatch(void)
{
	struct fixture f;
	init_fixture(&f, CAP);
	CHECK(tr_reactor_call(f.server.reactor, create_router, &f) == TR_OK);
	/* 改写注册输入数组不改变已冻结的描述/参数配对。 */
	memset(f.methods, 0, sizeof(f.methods));
	f.auto_reply = 1;
	for (unsigned i = 0; i < 4; ++i) {
		struct operation o = {&f, i, i, NULL, 0};
		open_stream(&f, i + 1);
		CHECK(tr_reactor_call(f.client.reactor, make_call, &o) ==
		      TR_OK);
		CHECK(tr_reactor_call(f.client.reactor, send_valid_call, &o) ==
		      TR_OK);
		wait_count(&f.client.terminals[i], 1);
		wait_count(&f.server.terminals[i], 1);
		CHECK(f.client.status[i] == TR_RPC_STATUS_OK);
		CHECK(f.server.status[i] == TR_RPC_STATUS_OK);
		CHECK(atomic_load(&f.server.received[i][TR_RPC_RECORD_OPEN]) ==
		      1);
	}
	cleanup(&f, 0);
}

static int probe_registry(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_record_router *router = NULL;
	struct tr_rpc_record_method invalid[4];
	uint64_t before = atomic_load(&f->server.budget.current_bytes);
	memcpy(invalid, f->methods, sizeof(invalid));
	invalid[3] = invalid[0];
	CHECK(tr_rpc_record_router_create_on_owner(
	              f->server.channel, invalid, 4, CAP, terminal, f,
	              &f->server.budget, &router) == TR_ERR_STATE);
	CHECK(!router &&
	      atomic_load(&f->server.budget.current_bytes) == before);
	invalid[3].descriptor.request_cardinality = TR_RPC_NONE;
	CHECK(tr_rpc_record_router_create_on_owner(
	              f->server.channel, invalid, 4, CAP, terminal, f,
	              &f->server.budget, &router) == TR_ERR_INVALID);
	for (unsigned n = 1; n <= 3; ++n) {
		allocation = 0;
		fail_at = n;
		CHECK(tr_rpc_record_router_create_on_owner(
		              f->server.channel, f->methods, 4, CAP, terminal,
		              f, &f->server.budget, &router) == TR_ERR_NOMEM);
		fail_at = 0;
		CHECK(!router &&
		      atomic_load(&f->server.budget.current_bytes) == before);
	}
	struct tr_memory_budget small;
	tr_memory_budget_init(&small, 1);
	CHECK(tr_rpc_record_router_create_on_owner(
	              f->server.channel, f->methods, 4, CAP, terminal, f,
	              &small, &router) == TR_AGAIN);
	CHECK(!router && atomic_load(&small.current_bytes) == 0);
	CHECK(tr_rpc_record_router_create_on_owner(
	              f->server.channel, f->methods, SIZE_MAX, CAP, terminal, f,
	              &f->server.budget, &router) == TR_ERR_BAD_LENGTH);
	return TR_OK;
}
static int retire_first(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_route_handle old = f->server.routes[0];
	CHECK(tr_rpc_record_router_retire_on_owner(old) == TR_ERR_STATE);
	CHECK(tr_rpc_record_router_abort_on_owner(
	              old, TR_RPC_STATUS_CANCELLED) == TR_OK);
	CHECK(tr_rpc_record_router_retire_on_owner(old) == TR_OK);
	CHECK(tr_rpc_record_router_abort_on_owner(
	              old, TR_RPC_STATUS_CANCELLED) == TR_ERR_STALE);
	/* 此测试从原流取消路由，随后使用已经打开的另一条流复用槽位。 */
	f->server.routes[0] = (struct tr_rpc_route_handle){0};
	CHECK(tr_rpc_record_router_admit_on_owner(
	              f->router, f->server.streams[1], &f->server.routes[1]) ==
	      TR_OK);
	CHECK(f->server.routes[1].slot == old.slot &&
	      f->server.routes[1].generation != old.generation);
	CHECK(tr_rpc_record_router_abort_on_owner(
	              old, TR_RPC_STATUS_CANCELLED) == TR_ERR_STALE);
	struct tr_rpc_route_handle untouched = old;
	CHECK(tr_rpc_record_router_admit_on_owner(f->router,
	                                          f->server.streams[1],
	                                          &untouched) == TR_ERR_STATE);
	CHECK(untouched.generation == old.generation);
	return TR_OK;
}
static int foreign(void *arg)
{
	struct fixture *f = arg;
	struct tr_rpc_route_handle h = {0};
	CHECK(tr_rpc_record_router_admit_on_owner(
	              f->router, f->server.streams[0], &h) == TR_ERR_STATE);
	CHECK(tr_rpc_record_router_receive_on_owner(f->router,
	                                            f->server.streams[0], NULL,
	                                            0) == TR_ERR_STATE);
	CHECK(tr_rpc_record_router_abort_on_owner(f->server.routes[0],
	                                          TR_RPC_STATUS_CANCELLED) ==
	      TR_ERR_STATE);
	return TR_OK;
}
static void test_registry_and_capacity(void)
{
	struct fixture f;
	init_fixture(&f, 1);
	CHECK(tr_reactor_call(f.server.reactor, probe_registry, &f) == TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, create_router, &f) == TR_OK);
	open_stream(&f, 1);
	open_stream(&f, 2);
	CHECK(atomic_load(&f.server.last_error[1]) == TR_AGAIN);
	CHECK(!f.server.routes[1].router &&
	      !atomic_load(&f.server.terminals[1]));
	CHECK(tr_reactor_call(f.client.reactor, foreign, &f) == TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, retire_first, &f) == TR_OK);
	cleanup(&f, 0);
}

struct injection {
	struct fixture *f;
	unsigned index;
	uint8_t data[1024];
	size_t size;
	int fail;
	int result;
};
static int inject(void *arg)
{
	struct injection *x = arg;
	allocation = 0;
	fail_at = x->fail ? 1U : 0U;
	x->result = tr_rpc_record_router_receive_on_owner(
	        x->f->router, x->f->server.streams[x->index], x->data, x->size);
	fail_at = 0;
	return TR_OK;
}
static void test_open_failures(void)
{
	struct fixture f;
	struct injection x = {.f = &f};
	struct tr_rpc_record_limits l = limits();
	struct tr_rpc_record_header h = {
	        .kind = TR_RPC_RECORD_OPEN, .service_id = 9, .method_id = 4};
	init_fixture(&f, CAP);
	CHECK(tr_reactor_call(f.server.reactor, create_router, &f) == TR_OK);
	for (unsigned i = 0; i < CAP; ++i)
		open_stream(&f, i + 1);
	/* 未知方法在完整头即拒绝，既不缓存元数据，也不运行注册回调。 */
	h.method_id = 999;
	h.metadata_size = 512;
	CHECK(tr_rpc_record_encode_header(x.data, sizeof(x.data), &h, &l) ==
	      TR_OK);
	x.size = TR_RPC_RECORD_HEADER_SIZE;
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK);
	CHECK(x.result == TR_ERR_UNSUPPORTED &&
	      f.server.status[0] == TR_RPC_STATUS_UNIMPLEMENTED);
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK);
	CHECK(x.result == TR_ERR_STATE &&
	      atomic_load(&f.server.terminals[0]) == 1);
	/* 可识别方法但元数据非法，不允许先向方法发布 OPEN。 */
	x.index = 1;
	h.method_id = 4;
	h.metadata_size = 4;
	CHECK(tr_rpc_record_encode_header(x.data, sizeof(x.data), &h, &l) ==
	      TR_OK);
	memcpy(x.data + TR_RPC_RECORD_HEADER_SIZE, "\1\0\0:", 4);
	x.size = TR_RPC_RECORD_HEADER_SIZE + 4;
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK);
	CHECK(x.result == TR_ERR_BAD_FLAGS &&
	      atomic_load(&f.server.received[1][TR_RPC_RECORD_OPEN]) == 0);
	/* 绑定分配失败是终局，不返回可重放已吸收头的 AGAIN。 */
	x.index = 2;
	x.fail = 1;
	x.size = TR_RPC_RECORD_HEADER_SIZE;
	h.metadata_size = 0;
	CHECK(tr_rpc_record_encode_header(x.data, sizeof(x.data), &h, &l) ==
	      TR_OK);
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK);
	CHECK(x.result == TR_ERR_NOMEM &&
	      f.server.status[2] == TR_RPC_STATUS_RESOURCE_EXHAUSTED);
	/* 已知方法的完整头已转交绑定，但元数据只到一字节：停机仍须终局。 */
	x.index = 3;
	x.fail = 0;
	h.metadata_size = 4;
	CHECK(tr_rpc_record_encode_header(x.data, sizeof(x.data), &h, &l) ==
	      TR_OK);
	x.data[TR_RPC_RECORD_HEADER_SIZE] = 1;
	x.size = TR_RPC_RECORD_HEADER_SIZE + 1;
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK &&
	      x.result == TR_OK);
	CHECK(atomic_load(&f.server.received[3][TR_RPC_RECORD_OPEN]) == 0);
	/* 固定头本身不完整的另一条路由。 */
	x.index = 4;
	x.size = 3;
	CHECK(tr_reactor_call(f.server.reactor, inject, &x) == TR_OK &&
	      x.result == TR_OK);
	/* 第六条正常方法通过真实TCP，证明其他坏调用没有关闭整条连接。 */
	open_bytes(&f, 5, 0, 1, (const uint8_t *)"\1\0\0x", 4);
	wait_count(&f.server.received[5][TR_RPC_RECORD_OPEN], 1);
	CHECK(tr_rpc_record_router_reap_stopped(&f.router) == TR_ERR_STATE);
	cleanup(&f, 1);
	for (unsigned i = 0; i < CAP; ++i)
		CHECK(atomic_load(&f.server.terminals[i]) == 1);
	CHECK(f.server.delivery[3] == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(f.server.delivery[4] == TR_RPC_RECORD_OWNER_STOPPED);
	CHECK(f.server.delivery[5] == TR_RPC_RECORD_OWNER_STOPPED);
}

static void test_pipelined_and_eof(void)
{
	struct fixture f;
	struct tr_rpc_record_limits l = limits();
	uint8_t wire[3 * TR_RPC_RECORD_HEADER_SIZE + 4 + 1024];
	size_t offset = 0;
	struct tr_rpc_record_header h = {.kind = TR_RPC_RECORD_OPEN,
	                                 .service_id = 9,
	                                 .method_id = 4,
	                                 .metadata_size = 4};
	init_fixture(&f, CAP);
	CHECK(tr_reactor_call(f.server.reactor, create_router, &f) == TR_OK);
	open_stream(&f, 1);
	open_stream(&f, 2);
	CHECK(tr_rpc_record_encode_header(wire, sizeof(wire), &h, &l) == TR_OK);
	offset = TR_RPC_RECORD_HEADER_SIZE;
	memcpy(wire + offset, "\1\0\0x", 4);
	offset += 4;
	h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_MESSAGE,
	                                  .codec_id = TR_RPC_CODEC_RAW,
	                                  .encoded_size = 1024,
	                                  .decoded_size = 1024};
	CHECK(tr_rpc_record_encode_header(wire + offset, sizeof(wire) - offset,
	                                  &h, &l) == TR_OK);
	offset += TR_RPC_RECORD_HEADER_SIZE;
	for (size_t i = 0; i < 1024; ++i)
		wire[offset++] = (uint8_t)(i * 7);
	h = (struct tr_rpc_record_header){.kind = TR_RPC_RECORD_HALF_CLOSE};
	CHECK(tr_rpc_record_encode_header(wire + offset, sizeof(wire) - offset,
	                                  &h, &l) == TR_OK);
	offset += TR_RPC_RECORD_HEADER_SIZE;
	raw(&f, 0, wire, offset);
	wait_count(&f.server.received[0][TR_RPC_RECORD_HALF_CLOSE], 1);
	CHECK(f.server.body_size[0] == 1024);
	/* 真实流的半关闭使另一条尚不足固定头的记录成为 DATA_LOSS。 */
	raw(&f, 1, wire, 3);
	CHECK(tr_stream_close(f.client.streams[1]) == TR_OK);
	wait_count(&f.server.terminals[1], 1);
	CHECK(f.server.status[1] == TR_RPC_STATUS_DATA_LOSS);
	CHECK(atomic_load(&f.server.received[1][TR_RPC_RECORD_OPEN]) == 0);
	cleanup(&f, 0);
}

int main(void)
{
	test_method_dispatch();
	test_pipelined_and_eof();
	test_registry_and_capacity();
	test_open_failures();
	puts("rpc_record_router: ok");
	return 0;
}
