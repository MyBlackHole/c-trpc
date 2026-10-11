#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/command_queue.h"
#include "../src/transport/protocol/parser.h"
#include "../src/crc32c.h"
#include "tr/status.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* 此专测只拦截自己的链接目标，不给生产库添加观察或故障注入入口。 */
struct context {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	struct tr_buffer *held;
	struct tr_parser *parser;
	size_t recv_limit;
	size_t network_bytes;
	size_t recv_calls;
	size_t plain_recv_calls;
	size_t fortified_recv_calls;
	unsigned injected_eintr;
	unsigned injected_eagain;
	unsigned injected_short_read;
	size_t produced_bytes;
	size_t turn_network;
	size_t turn_produced;
	size_t turn_scratch;
	size_t expected_network;
	unsigned last_recv_turn;
	unsigned empty_kernel_resume;
	int direct_produce;
	size_t max_scratch;
	unsigned byte_budget;
	unsigned frames;
	unsigned events;
	unsigned first_turn;
	unsigned last_turn;
	unsigned turns;
	unsigned errors_left;
	unsigned hold_first;
	unsigned close_first;
	unsigned prepared_again;
	unsigned idle_polls;
	size_t expected_tail;
	int preface_fd;
	unsigned preface_seen;
	unsigned preface_released;
	int status;
};

static struct context *active;
int __real_epoll_wait(int fd, struct epoll_event *events, int maxevents,
                      int timeout);
ssize_t __real_recv(int fd, void *buf, size_t len, int flags);
ssize_t __real___recv_chk(int fd, void *buf, size_t len, size_t buflen,
                          int flags);
int __real_tr_parser_prepare(struct tr_parser *parser);
int __real_tr_parser_produce(struct tr_parser *parser, size_t produced,
                             struct tr_frame *out);
size_t __real_tr_command_queue_pop_batch(struct tr_command_queue *queue,
                                         struct tr_command *out, size_t max);

size_t __wrap_tr_command_queue_pop_batch(struct tr_command_queue *queue,
                                         struct tr_command *out, size_t max)
{
	if (active) {
		assert(active->turn_network <= active->byte_budget);
		/* 直收最多一个网络额度，缓存复制另有一个同大小额度。 */
		assert(active->turn_produced <= 2U * active->byte_budget);
		assert(active->turn_scratch <= active->byte_budget);
		active->turn_scratch = 0;
		active->turn_network = 0;
		active->turn_produced = 0;
		active->turns++;
	}
	return __real_tr_command_queue_pop_batch(queue, out, max);
}

int __wrap_epoll_wait(int fd, struct epoll_event *events, int maxevents,
                      int timeout)
{
	if (active && timeout != 0 && active->prepared_again) {
		assert(pthread_mutex_lock(&active->lock) == 0);
		active->idle_polls++;
		assert(pthread_cond_broadcast(&active->cond) == 0);
		assert(pthread_mutex_unlock(&active->lock) == 0);
	}
	return __real_epoll_wait(fd, events, maxevents, timeout);
}

static ssize_t receive(int fd, void *buf, size_t len, size_t buflen, int flags,
                       int fortified)
{
	struct context *ctx = active;
	ssize_t n;

	assert(ctx != NULL);
	ctx->recv_calls++;
	if (fortified)
		ctx->fortified_recv_calls++;
	else
		ctx->plain_recv_calls++;
	assert(len <= ctx->byte_budget - ctx->turn_network);
	if (ctx->errors_left) {
		errno = ctx->errors_left-- == 2 ? EINTR : EAGAIN;
		if (errno == EINTR)
			ctx->injected_eintr++;
		else
			ctx->injected_eagain++;
		return -1;
	}
	if (ctx->recv_limit && len > ctx->recv_limit) {
		ctx->injected_short_read++;
		len = ctx->recv_limit;
	}
	n = fortified ? __real___recv_chk(fd, buf, len, buflen, flags)
	              : __real_recv(fd, buf, len, flags);
	if (n > 0) {
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
		ctx->direct_produce =
		        ctx->parser && buf != ctx->parser->scratch;
#endif
		ctx->last_recv_turn = ctx->turns;
		ctx->network_bytes += (size_t)n;
		ctx->turn_network += (size_t)n;
	}
	return n;
}

ssize_t __wrap_recv(int fd, void *buf, size_t len, int flags)
{
	return receive(fd, buf, len, 0, flags, 0);
}

ssize_t __wrap___recv_chk(int fd, void *buf, size_t len, size_t buflen,
                          int flags)
{
	/* 先保留真实 Fortify 拒绝；短读或错误注入不能掩盖非法范围。 */
	if (len > buflen)
		return __real___recv_chk(fd, buf, len, buflen, flags);
	return receive(fd, buf, len, buflen, flags, 1);
}

int __wrap_tr_parser_prepare(struct tr_parser *parser)
{
	int ret = __real_tr_parser_prepare(parser);

	active->parser = parser;
	if (ret == TR_AGAIN)
		active->prepared_again++;
	return ret;
}

int __wrap_tr_parser_produce(struct tr_parser *parser, size_t produced,
                             struct tr_frame *out)
{
	active->parser = parser;
	active->turn_produced += produced;
	active->produced_bytes += produced;
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	if (!active->direct_produce) {
		active->turn_scratch += produced;
		if (active->expected_network &&
		    active->network_bytes == active->expected_network &&
		    active->turns > active->last_recv_turn)
			active->empty_kernel_resume++;
	}
	active->direct_produce = 0;
	assert(active->turn_scratch <= active->byte_budget);
	assert(parser->scratch_begin <= parser->scratch_end);
	assert(parser->scratch_end <= TR_PARSER_READAHEAD_BYTES);
	if (parser->scratch_end - parser->scratch_begin > active->max_scratch)
		active->max_scratch =
		        parser->scratch_end - parser->scratch_begin;
#endif
	int ret = __real_tr_parser_produce(parser, produced, out);

	if (ret == TR_AGAIN)
		active->prepared_again++;
	return ret;
}

static enum tr_frame_disposition frame_cb(struct tr_conn_handle connection,
                                          struct tr_frame *frame, void *arg)
{
	struct context *ctx = arg;
	unsigned i;

	assert(frame->header.message_id == ctx->frames + 1U);
	if (frame->payload) {
		assert(frame->payload->len == frame->header.payload_len);
		for (i = 0; i < frame->payload->len; i++)
			assert(frame->payload->data[i] == (uint8_t)(i * 31U));
	}
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	ctx->frames++;
	if (ctx->frames == 1U)
		ctx->first_turn = ctx->turns;
	ctx->last_turn = ctx->turns;
	if (ctx->hold_first && ctx->frames == 1U)
		ctx->held = frame->payload;
	assert(pthread_cond_broadcast(&ctx->cond) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
	if (ctx->close_first)
		assert(tr_reactor_close_on_owner(connection) == TR_OK);
	return ctx->hold_first && ctx->frames == 1U ? TR_FRAME_TAKE_OWNERSHIP
	                                            : TR_FRAME_RELEASE;
}

static void event_cb(struct tr_conn_handle connection,
                     enum tr_connection_event event, int status, void *arg)
{
	struct context *ctx = arg;

	(void)connection;
	(void)event;
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	ctx->events++;
	ctx->status = status;
	assert(pthread_cond_broadcast(&ctx->cond) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
}

static void wait_counts(struct context *ctx, unsigned frames, unsigned events)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&ctx->lock) == 0);
	while (ctx->frames < frames || ctx->events < events)
		assert(pthread_cond_timedwait(&ctx->cond, &ctx->lock,
		                              &deadline) == 0);
	assert(pthread_mutex_unlock(&ctx->lock) == 0);
}

static size_t append_frame(uint8_t *wire, size_t offset, uint32_t bytes,
                           unsigned id)
{
	struct tr_frame_header header = {
	        .version = TR_WIRE_ENV_VERSION,
	        .type = bytes ? TR_FRAME_DATA : TR_FRAME_PING,
	        .flags = bytes ? TR_FRAME_F_FIRST | TR_FRAME_F_LAST : 0,
	        .stream_id = bytes ? 3U : 0U,
	        .message_id = id,
	        .payload_len = bytes,
	};
	unsigned i;

	for (i = 0; i < bytes; i++)
		wire[offset + TR_WIRE_HEADER_SIZE + i] = (uint8_t)(i * 31U);
	header.payload_crc32c =
	        tr_crc32c(wire + offset + TR_WIRE_HEADER_SIZE, bytes);
	assert(tr_wire_header_encode(wire + offset, &header) == TR_OK);
	return offset + TR_WIRE_HEADER_SIZE + bytes;
}

static void create(struct context *ctx, unsigned budget, unsigned pool_count)
{
	struct tr_reactor_config config = {
	        .max_connections = 1,
	        .command_capacity = 64,
	        .rx_buffer_count = pool_count,
	        .rx_buffer_size = 16384,
	        .max_payload_len = 16384,
	        .rx_budget_bytes = budget,
	};

	ctx->byte_budget = budget;
	assert(pthread_mutex_init(&ctx->lock, NULL) == 0);
	assert(pthread_cond_init(&ctx->cond, NULL) == 0);
	assert(tr_reactor_create(&config, frame_cb, event_cb, ctx,
	                         &ctx->reactor) == TR_OK);
	active = ctx;
	assert(tr_reactor_start(ctx->reactor) == TR_OK);
}

static void adopt(struct context *ctx, int sockets[2])
{
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) ==
	       0);
	assert(tr_reactor_adopt_fd(ctx->reactor, sockets[1],
	                           &ctx->connection) == TR_OK);
}

static int check_network(void *arg)
{
	struct context *ctx = arg;
	struct tr_reactor_stats stats;

	assert(tr_reactor_get_stats(ctx->reactor, &stats) == TR_OK);
	if (stats.total.rx_bytes != ctx->network_bytes)
		fprintf(stderr,
		        "接收观测不一致：recv_calls=%zu recv_bytes=%zu "
		        "produce_bytes=%zu stats_rx=%llu\n",
		        ctx->recv_calls, ctx->network_bytes,
		        ctx->produced_bytes,
		        (unsigned long long)stats.total.rx_bytes);
	assert(ctx->plain_recv_calls + ctx->fortified_recv_calls ==
	       ctx->recv_calls);
	assert(stats.total.rx_bytes == ctx->network_bytes);
	assert(stats.max_per_turn.rx_bytes <= ctx->byte_budget);
	return TR_OK;
}

static void destroy(struct context *ctx, int peer)
{
	assert(tr_reactor_call(ctx->reactor, check_network, ctx) == TR_OK);
	assert(tr_reactor_stop(ctx->reactor) == TR_OK);
	active = NULL;
	assert(tr_reactor_destroy(ctx->reactor) == TR_OK);
	assert(close(peer) == 0);
	assert(pthread_cond_destroy(&ctx->cond) == 0);
	assert(pthread_mutex_destroy(&ctx->lock) == 0);
}

static void test_sequence(unsigned budget, size_t recv_limit, int faults)
{
	const unsigned sizes[] = {0, 1, 511, 512, 513, 4056, 4096, 8192, 17, 0};
	struct context ctx = {.recv_limit = recv_limit,
	                      .errors_left = faults ? 2U : 0U};
	uint8_t wire[32768];
	size_t len = 0;
	unsigned i;
	int sockets[2];

	create(&ctx, budget, 2);
	adopt(&ctx, sockets);
	for (i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++)
		len = append_frame(wire, len, sizes[i], i + 1U);
	assert(write(sockets[0], wire, len) == (ssize_t)len);
	assert(shutdown(sockets[0], SHUT_WR) == 0);
	wait_counts(&ctx, 10, 1);
	assert(ctx.status == TR_OK);
	assert(ctx.frames == 10);
	assert(ctx.errors_left == 0);
	if (faults)
		assert(ctx.injected_eintr == 1 && ctx.injected_eagain == 1);
	if (recv_limit && recv_limit < budget)
		assert(ctx.injected_short_read != 0);
	destroy(&ctx, sockets[0]);
}

static int check_tail(void *arg)
{
	struct context *ctx = arg;

	if (!ctx->parser)
		return TR_AGAIN;
	if (ctx->expected_tail < TR_WIRE_HEADER_SIZE) {
		return ctx->parser->state == TR_PARSER_HEADER &&
		                       ctx->parser->header_have ==
		                               ctx->expected_tail
		               ? TR_OK
		               : TR_AGAIN;
	}
	return ctx->parser->state == TR_PARSER_PAYLOAD &&
	                       ctx->parser->payload_have ==
	                               ctx->expected_tail - TR_WIRE_HEADER_SIZE
	               ? TR_OK
	               : TR_AGAIN;
}

/* 一批完整帧后留下半头或半载荷，下一次写入必须精确延续。 */
static void test_tail(size_t tail)
{
	struct context ctx = {.expected_tail = tail};
	uint8_t wire[512];
	size_t prefix = append_frame(wire, 0, 16, 1);
	size_t len = append_frame(wire, prefix, 64, 2);
	unsigned i;
	int sockets[2];

	create(&ctx, 4096, 2);
	adopt(&ctx, sockets);
	assert(write(sockets[0], wire, prefix + tail) ==
	       (ssize_t)(prefix + tail));
	wait_counts(&ctx, 1, 0);
	for (i = 0; i < 10000; i++) {
		if (tr_reactor_call(ctx.reactor, check_tail, &ctx) == TR_OK)
			break;
	}
	assert(i < 10000);
	assert(write(sockets[0], wire + prefix + tail, len - prefix - tail) ==
	       (ssize_t)(len - prefix - tail));
	wait_counts(&ctx, 2, 0);
	destroy(&ctx, sockets[0]);
}

static int assert_paused(void *arg)
{
	struct context *ctx = arg;
	struct tr_connection_stats stats;

	assert(tr_reactor_get_connection_stats(ctx->connection, &stats) ==
	       TR_OK);
	if (!stats.rx_paused)
		return TR_AGAIN;
	assert(ctx->parser != NULL);
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	assert(ctx->parser->scratch_end > ctx->parser->scratch_begin);
	assert(ctx->network_bytes == 312U);
#endif
	return TR_OK;
}

static int release_held(void *arg)
{
	struct context *ctx = arg;

	tr_buffer_release(ctx->held);
	ctx->held = NULL;
	return TR_OK;
}

static void test_pause_resume(int pending_kernel)
{
	struct context ctx = {.hold_first = 1,
	                      .expected_network = pending_kernel ? 624U : 312U};
	uint8_t wire[1024];
	size_t len = 0;
	unsigned i;
	int sockets[2];

	create(&ctx, pending_kernel ? 312U : 4096U, 1);
	adopt(&ctx, sockets);
	for (i = 0; i < (pending_kernel ? 6U : 3U); i++)
		len = append_frame(wire, len, 64, i + 1U);
	assert(write(sockets[0], wire, len) == (ssize_t)len);
	wait_counts(&ctx, 1, 0);
	for (i = 0; i < 10000; i++) {
		if (tr_reactor_call(ctx.reactor, assert_paused, &ctx) == TR_OK)
			break;
	}
	assert(i < 10000);
	/* 没有新事件时进入阻塞 poll，证明暂停没有自调度忙转。 */
	{
		struct timespec deadline;

		assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
		deadline.tv_sec += 5;
		assert(pthread_mutex_lock(&ctx.lock) == 0);
		while (!ctx.idle_polls)
			assert(pthread_cond_timedwait(&ctx.cond, &ctx.lock,
			                              &deadline) == 0);
		assert(pthread_mutex_unlock(&ctx.lock) == 0);
	}
	/* 仍缺资源时 resume 不能误清暂停，也不能消费缓存。 */
	assert(tr_reactor_resume_rx(ctx.connection) == TR_OK);
	assert(tr_reactor_call(ctx.reactor, assert_paused, &ctx) == TR_OK);
	assert(tr_reactor_call(ctx.reactor, release_held, &ctx) == TR_OK);
	assert(tr_reactor_resume_rx(ctx.connection) == TR_OK);
	/* 不再写入任何套接字字节；缓存额度耗尽后也必须跨轮次续跑。 */
	wait_counts(&ctx, pending_kernel ? 6U : 3U, 0);
	assert(tr_reactor_call(ctx.reactor, check_network, &ctx) == TR_OK);
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	assert(ctx.empty_kernel_resume != 0);
#endif
	destroy(&ctx, sockets[0]);
}

static void test_terminal(int bad_crc, int bad_header, int close_callback)
{
	struct context ctx = {.close_first = (unsigned)close_callback};
	uint8_t wire[512];
	size_t first = append_frame(wire, 0, 64, 1);
	size_t len = append_frame(wire, first, 64, 2);
	int sockets[2];

	if (bad_crc)
		wire[TR_WIRE_HEADER_SIZE] ^= 1;
	if (bad_header == 1)
		wire[TR_WIRE_OFF_FLAGS] ^= 0x10;
	if (bad_header > 1) {
		struct tr_frame_header header;

		assert(tr_wire_header_decode(wire, &header) == TR_OK);
		if (bad_header == 2)
			header.type = UINT16_MAX;
		else
			header.payload_len = 16385;
		assert(tr_wire_header_encode(wire, &header) == TR_OK);
	}
	create(&ctx, 4096, 2);
	adopt(&ctx, sockets);
	assert(write(sockets[0], wire, len) == (ssize_t)len);
	wait_counts(&ctx, close_callback ? 1U : 0U, 1);
	assert(ctx.frames == (close_callback ? 1U : 0U));
	assert(ctx.status == (bad_crc           ? TR_ERR_PAYLOAD_CRC
	                      : bad_header == 1 ? TR_ERR_HEADER_CRC
	                      : bad_header == 2 ? TR_ERR_BAD_TYPE
	                      : bad_header == 3 ? TR_ERR_BAD_LENGTH
	                                        : TR_OK));
	destroy(&ctx, sockets[0]);
}

static void test_empty_train(void)
{
	struct context ctx = {0};
	uint8_t wire[12000];
	size_t len = 0;
	unsigned i;
	int sockets[2];

	create(&ctx, 4096, 1);
	adopt(&ctx, sockets);
	for (i = 0; i < 300; i++)
		len = append_frame(wire, len, 0, i + 1U);
	assert(write(sockets[0], wire, len) == (ssize_t)len);
	wait_counts(&ctx, 300, 0);
	assert(ctx.last_turn > ctx.first_turn);
	destroy(&ctx, sockets[0]);
}

static int preface_feed(struct tr_conn_handle connection, const uint8_t *data,
                        size_t len, int *done, void *arg)
{
	struct context *ctx = arg;

	(void)connection;
	assert(len == 7);
	assert(memcmp(data, "preface", 7) == 0);
	assert(ctx->network_bytes == 7);
	ctx->preface_seen++;
	*done = 1;
	return TR_OK;
}

static void preface_release(void *arg)
{
	struct context *ctx = arg;

	ctx->preface_released++;
}

static int adopt_preface(void *arg)
{
	struct context *ctx = arg;
	struct tr_reactor_preface_handler preface = {
	        .byte_count = 7,
	        .feed = preface_feed,
	        .release = preface_release,
	        .arg = ctx,
	};

	return tr_reactor_adopt_fd_prefaced_on_owner(
	        ctx->reactor, ctx->preface_fd, &preface, &ctx->connection);
}

static void test_preface(void)
{
	struct context ctx = {0};
	uint8_t wire[512] = "preface";
	size_t len = append_frame(wire, 7, 64, 1);
	int sockets[2];

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sockets) ==
	       0);
	ctx.preface_fd = sockets[1];
	create(&ctx, 4096, 1);
	assert(write(sockets[0], wire, len) == (ssize_t)len);
	assert(tr_reactor_call(ctx.reactor, adopt_preface, &ctx) == TR_OK);
	wait_counts(&ctx, 1, 0);
	assert(ctx.preface_seen == 1 && ctx.preface_released == 1);
	destroy(&ctx, sockets[0]);
}

static void test_fortified_entry(void)
{
	unsigned entry;
	unsigned fault;

	/* 两个入口都显式验证注入和 MSG_PEEK，不能因编译器选路而静默漏测。 */
	for (entry = 0; entry < 2; entry++) {
		struct context ctx = {
		        .byte_budget = 64, .recv_limit = 3, .errors_left = 2};
		char bytes[8] = {0};
		int sockets[2];
		unsigned call;

		assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0,
		                  sockets) == 0);
		active = &ctx;
		assert(write(sockets[0], "abcdefgh", sizeof(bytes)) ==
		       sizeof(bytes));
		for (call = 0; call < 4; call++) {
			int flags = MSG_DONTWAIT | (call == 2 ? MSG_PEEK : 0);
			ssize_t n =
			        entry ? __wrap___recv_chk(sockets[1], bytes,
			                                  sizeof(bytes),
			                                  sizeof(bytes), flags)
			              : __wrap_recv(sockets[1], bytes,
			                            sizeof(bytes), flags);

			if (call < 2)
				assert(n == -1 &&
				       errno == (call == 0 ? EINTR : EAGAIN));
			else
				assert(n == 3 && memcmp(bytes, "abc", 3) == 0);
		}
		assert(ctx.errors_left == 0 && ctx.recv_calls == 4 &&
		       ctx.network_bytes == 6);
		assert(ctx.plain_recv_calls == (entry ? 0U : 4U));
		assert(ctx.fortified_recv_calls == (entry ? 4U : 0U));
		assert(ctx.injected_eintr == 1 && ctx.injected_eagain == 1 &&
		       ctx.injected_short_read == 2);
		active = NULL;
		assert(close(sockets[0]) == 0);
		assert(close(sockets[1]) == 0);
	}

	/* 每个反例都在无 Reactor 线程的子进程验证真实 Fortify SIGABRT。 */
	for (fault = 0; fault < 3; fault++) {
		pid_t child = fork();
		int status;

		assert(child >= 0);
		if (child == 0) {
			struct context invalid = {.byte_budget = 64,
			                          .recv_limit = 1,
			                          .errors_left = fault};
			char tiny[1];

			active = &invalid;
			(void)__wrap___recv_chk(-1, tiny, 2, sizeof(tiny),
			                        MSG_DONTWAIT);
			_exit(77); /* 不能被短读、EINTR 或 EAGAIN 提前返回。 */
		}
		assert(waitpid(child, &status, 0) == child);
		assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
	}
}

static void test_reset(void)
{
	struct tr_buffer_pool pool;
	struct tr_parser parser;
	struct tr_frame frame;
	uint8_t wire[TR_WIRE_HEADER_SIZE];

	assert(tr_buffer_pool_init(&pool, 1, 512) == TR_OK);
	assert(tr_parser_init(&parser, &pool, NULL) == TR_OK);
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	assert(parser.scratch_begin == 0 && parser.scratch_end == 0);
	parser.scratch_begin = 4;
	parser.scratch_end = 77;
#endif
	(void)append_frame(wire, 0, 0, 1);
	memcpy(tr_parser_write_ptr(&parser), wire, sizeof(wire));
	/* 直接调用 real，避免本地 parser 单测污染 Reactor 的观察者。 */
	assert(__real_tr_parser_produce(&parser, sizeof(wire), &frame) ==
	       TR_FRAME_READY);
	tr_frame_release(&frame);
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	assert(parser.scratch_begin == 4 && parser.scratch_end == 77);
#endif
	tr_parser_reset(&parser);
#ifdef TR_EXPERIMENTAL_BOUNDED_READAHEAD
	assert(parser.scratch_begin == 0 && parser.scratch_end == 0);
#endif
	assert(parser.state == TR_PARSER_HEADER);
	assert(tr_buffer_pool_destroy(&pool) == TR_OK);
}

int main(void)
{
	test_fortified_entry();
	test_reset();
	test_sequence(65536, 0, 0);
	test_sequence(37, 0, 1);
	test_sequence(4096, 1, 0);
	test_sequence(4096, 20, 0);
	test_sequence(4096, 256, 0);
	test_sequence(4096, 4095, 0);
	test_sequence(4096, 4096, 0);
	for (size_t i = 1; i < TR_WIRE_HEADER_SIZE; i++)
		test_tail(i);
	for (size_t i = 1; i < 64; i++)
		test_tail(TR_WIRE_HEADER_SIZE + i);
	test_empty_train();
	test_preface();
	test_pause_resume(0);
	test_pause_resume(1);
	test_terminal(1, 0, 0);
	test_terminal(0, 1, 0);
	test_terminal(0, 2, 0);
	test_terminal(0, 3, 0);
	test_terminal(0, 0, 1);
	printf("有界预读局部测试通过；parser 大小=%zu\n",
	       sizeof(struct tr_parser));
	return 0;
}
