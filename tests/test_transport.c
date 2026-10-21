#include "../src/execution/worker_context.h"
#include "../src/execution/buffer.h"
#include "../src/crc32c.h"
#include "../src/execution/command_queue.h"
#include "../src/endian.h"
#include "../src/transport/protocol/frame.h"
#include "../src/guard.h"
#include "../src/transport/protocol/parser.h"
#include "../src/execution/reactor.h"
#include "../src/refcount.h"
#include "tr/rpc.h"
#include "../src/transport/channel/channel.h"
#include "tr/client.h"
#include "tr/server.h"
#include "../src/io/socket.h"
#include "tr/status.h"
#include "../src/transport/protocol/wire.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/execution/reactor_internal.h"
#include "server_v2_test_fixture.h"
#include "../src/execution/timer_queue.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_POOL_COUNT 4U
#define TEST_BUF_SIZE (1024U * 1024U)

/* 只让重连所有者回调中恰好一次非零定时器设置失败。
 * 夹具不启用保活，因此这只会是重连定时器。
 */
static atomic_uint fail_reconnect_timer_arm;

int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			       uint64_t deadline_ns);

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle handle,
			       uint64_t deadline_ns)
{
	unsigned expected = 1U;

	if (deadline_ns != 0U &&
	    atomic_compare_exchange_strong_explicit(
		    &fail_reconnect_timer_arm, &expected, 0U,
		    memory_order_relaxed, memory_order_relaxed))
		return TR_ERR_SYS;
	return __real_tr_reactor_timer_arm(handle, deadline_ns);
}

static pthread_mutex_t tcp_nodelay_probe_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned tcp_nodelay_probe_count;

int __real_setsockopt(int fd, int level, int option_name,
		      const void *option_value, socklen_t option_len);

int __wrap_setsockopt(int fd, int level, int option_name,
		      const void *option_value, socklen_t option_len)
{
	int ret = __real_setsockopt(fd, level, option_name, option_value,
				    option_len);

	if (ret == 0 && level == IPPROTO_TCP && option_name == TCP_NODELAY &&
	    option_value && option_len == (socklen_t)sizeof(int)) {
		int value;
		memcpy(&value, option_value, sizeof(value));
		if (value == 1) {
			pthread_mutex_lock(&tcp_nodelay_probe_lock);
			tcp_nodelay_probe_count++;
			pthread_mutex_unlock(&tcp_nodelay_probe_lock);
		}
	}
	return ret;
}

static unsigned tcp_nodelay_probe_read(void)
{
	unsigned count;

	pthread_mutex_lock(&tcp_nodelay_probe_lock);
	count = tcp_nodelay_probe_count;
	pthread_mutex_unlock(&tcp_nodelay_probe_lock);
	return count;
}

static void build_wire_frame(uint8_t **wire, size_t *wire_len, uint16_t type,
			     uint32_t flags, uint32_t stream_id,
			     uint64_t message_id, const uint8_t *payload,
			     uint32_t payload_len)
{
	struct tr_frame_header h;
	uint8_t *buf;

	*wire_len = TR_WIRE_HEADER_SIZE + payload_len;
	buf = (uint8_t *)malloc(*wire_len);
	assert(buf != NULL);

	memset(&h, 0, sizeof(h));
	h.version = TR_WIRE_ENV_VERSION;
	h.type = type;
	h.flags = flags;
	h.stream_id = stream_id;
	h.message_id = message_id;
	h.payload_len = payload_len;
	h.payload_crc32c = tr_crc32c(payload, payload_len);

	assert(tr_wire_header_encode(buf, &h) == TR_OK);
	if (payload_len)
		memcpy(buf + TR_WIRE_HEADER_SIZE, payload, payload_len);

	*wire = buf;
}

static int feed_bytes(struct tr_parser *parser, const uint8_t *wire,
		      size_t wire_len, size_t max_fragment,
		      struct tr_frame *out)
{
	size_t off = 0;

	while (off < wire_len) {
		size_t writable;
		size_t n;
		void *dst;
		int ret;

		ret = tr_parser_prepare(parser);
		if (ret != TR_OK)
			return ret;

		writable = tr_parser_write_len(parser);
		assert(writable > 0);

		n = wire_len - off;
		if (n > writable)
			n = writable;
		if (n > max_fragment)
			n = max_fragment;

		dst = tr_parser_write_ptr(parser);
		assert(dst != NULL);
		memcpy(dst, wire + off, n);

		ret = tr_parser_produce(parser, n, out);
		off += n;

		if (ret == TR_FRAME_READY) {
			assert(off == wire_len);
			return ret;
		}

		if (ret != TR_OK)
			return ret;
	}

	return TR_OK;
}

static void test_endian(void)
{
	uint8_t b[8];

	tr_put_le16(b, 0x1234U);
	assert(b[0] == 0x34 && b[1] == 0x12);
	assert(tr_get_le16(b) == 0x1234U);

	tr_put_le32(b, 0x12345678U);
	assert(b[0] == 0x78 && b[1] == 0x56 && b[2] == 0x34 && b[3] == 0x12);
	assert(tr_get_le32(b) == 0x12345678U);

	tr_put_le64(b, UINT64_C(0x0123456789ABCDEF));
	assert(b[0] == 0xEF && b[7] == 0x01);
	assert(tr_get_le64(b) == UINT64_C(0x0123456789ABCDEF));
}

static void test_wire_roundtrip(void)
{
	struct tr_frame_header in;
	struct tr_frame_header out;
	struct tr_wire_limits limits;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&in, 0, sizeof(in));
	in.version = TR_WIRE_ENV_VERSION;
	in.type = TR_FRAME_DATA;
	in.flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
	in.stream_id = 7;
	in.message_id = UINT64_C(0x1122334455667788);
	in.payload_len = 4096;
	in.payload_crc32c = 0xAABBCCDDU;

	assert(tr_wire_header_encode(raw, &in) == TR_OK);
	assert(raw[TR_WIRE_OFF_STREAM_ID + 0] == 7);
	assert(raw[TR_WIRE_OFF_MESSAGE_ID + 0] == 0x88);
	assert(raw[TR_WIRE_OFF_MESSAGE_ID + 7] == 0x11);

	assert(tr_wire_header_decode(raw, &out) == TR_OK);

	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_wire_header_validate(raw, &out, &limits) == TR_OK);

	assert(out.version == in.version);
	assert(out.type == in.type);
	assert(out.flags == in.flags);
	assert(out.stream_id == in.stream_id);
	assert(out.message_id == in.message_id);
	assert(out.payload_len == in.payload_len);
	assert(out.payload_crc32c == in.payload_crc32c);
	assert(out.reserved == 0);
}

static void test_header_corruption(void)
{
	struct tr_frame_header h;
	struct tr_frame_header decoded;
	struct tr_wire_limits limits;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&h, 0, sizeof(h));
	h.version = TR_WIRE_ENV_VERSION;
	h.type = TR_FRAME_PING;
	h.payload_crc32c = tr_crc32c(NULL, 0);

	assert(tr_wire_header_encode(raw, &h) == TR_OK);
	raw[TR_WIRE_OFF_FLAGS] ^= 0x10;
	assert(tr_wire_header_decode(raw, &decoded) == TR_OK);

	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_wire_header_validate(raw, &decoded, &limits) ==
	       TR_ERR_HEADER_CRC);
}

static void test_parser_one_byte_fragments(void)
{
	struct tr_buffer_pool pool;
	struct tr_parser parser;
	struct tr_wire_limits limits;
	struct tr_frame frame;
	uint8_t payload[4096];
	uint8_t *wire;
	size_t wire_len;
	size_t i;

	for (i = 0; i < sizeof(payload); ++i)
		payload[i] = (uint8_t)(i * 31U);

	assert(tr_buffer_pool_init(&pool, TEST_POOL_COUNT, TEST_BUF_SIZE) ==
	       TR_OK);
	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_parser_init(&parser, &pool, &limits) == TR_OK);
	tr_frame_init(&frame);

	build_wire_frame(&wire, &wire_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 3, 99, payload,
			 sizeof(payload));

	assert(feed_bytes(&parser, wire, wire_len, 1, &frame) ==
	       TR_FRAME_READY);
	assert(frame.header.stream_id == 3);
	assert(frame.header.message_id == 99);
	assert(frame.payload != NULL);
	assert(frame.payload->len == sizeof(payload));
	assert(memcmp(frame.payload->data, payload, sizeof(payload)) == 0);

	tr_frame_release(&frame);
	free(wire);
	tr_parser_reset(&parser);
	tr_buffer_pool_destroy(&pool);
}

static void test_parser_payload_crc_failure(void)
{
	struct tr_buffer_pool pool;
	struct tr_parser parser;
	struct tr_wire_limits limits;
	struct tr_frame frame;
	uint8_t payload[128];
	uint8_t *wire;
	size_t wire_len;

	memset(payload, 0xA5, sizeof(payload));

	assert(tr_buffer_pool_init(&pool, TEST_POOL_COUNT, TEST_BUF_SIZE) ==
	       TR_OK);
	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_parser_init(&parser, &pool, &limits) == TR_OK);
	tr_frame_init(&frame);

	build_wire_frame(&wire, &wire_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 1, payload,
			 sizeof(payload));

	wire[wire_len - 1] ^= 0x01;
	assert(feed_bytes(&parser, wire, wire_len, 17, &frame) ==
	       TR_ERR_PAYLOAD_CRC);
	assert(frame.payload == NULL);
	assert(tr_buffer_pool_free_count(&pool) == TEST_POOL_COUNT);

	free(wire);
	tr_parser_reset(&parser);
	tr_buffer_pool_destroy(&pool);
}

static void test_parser_pool_backpressure(void)
{
	struct tr_buffer_pool pool;
	struct tr_parser parser;
	struct tr_wire_limits limits;
	struct tr_frame first;
	struct tr_frame second;
	uint8_t payload1[64];
	uint8_t payload2[64];
	uint8_t *wire1;
	uint8_t *wire2;
	size_t wire1_len;
	size_t wire2_len;
	size_t off;
	int ret;

	memset(payload1, 0x11, sizeof(payload1));
	memset(payload2, 0x22, sizeof(payload2));

	assert(tr_buffer_pool_init(&pool, 1, TEST_BUF_SIZE) == TR_OK);
	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_parser_init(&parser, &pool, &limits) == TR_OK);
	tr_frame_init(&first);
	tr_frame_init(&second);

	build_wire_frame(&wire1, &wire1_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 1, payload1,
			 sizeof(payload1));
	build_wire_frame(&wire2, &wire2_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 2, payload2,
			 sizeof(payload2));

	assert(feed_bytes(&parser, wire1, wire1_len, wire1_len, &first) ==
	       TR_FRAME_READY);
	assert(tr_buffer_pool_free_count(&pool) == 0);

	/* 只输入第二个头部。解析器随后需要负载缓冲区。 */
	off = 0;
	while (off < TR_WIRE_HEADER_SIZE) {
		size_t n = tr_parser_write_len(&parser);
		void *dst = tr_parser_write_ptr(&parser);
		if (n > TR_WIRE_HEADER_SIZE - off)
			n = TR_WIRE_HEADER_SIZE - off;
		memcpy(dst, wire2 + off, n);
		ret = tr_parser_produce(&parser, n, &second);
		off += n;
	}

	assert(ret == TR_AGAIN);
	assert(parser.state == TR_PARSER_WAIT_PAYLOAD_BUFFER);
	assert(tr_parser_prepare(&parser) == TR_AGAIN);

	tr_frame_release(&first);
	assert(tr_buffer_pool_free_count(&pool) == 1);
	assert(tr_parser_prepare(&parser) == TR_OK);

	assert(feed_bytes(&parser, wire2 + TR_WIRE_HEADER_SIZE,
			  wire2_len - TR_WIRE_HEADER_SIZE, 7,
			  &second) == TR_FRAME_READY);
	assert(second.payload != NULL);
	assert(memcmp(second.payload->data, payload2, sizeof(payload2)) == 0);

	tr_frame_release(&second);
	free(wire1);
	free(wire2);
	tr_parser_reset(&parser);
	tr_buffer_pool_destroy(&pool);
}

static void test_payload_limit(void)
{
	struct tr_frame_header h;
	struct tr_frame_header decoded;
	struct tr_wire_limits limits;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&h, 0, sizeof(h));
	h.version = TR_WIRE_ENV_VERSION;
	h.type = TR_FRAME_DATA;
	h.payload_len = 4097;
	h.payload_crc32c = 0;

	assert(tr_wire_header_encode(raw, &h) == TR_OK);
	assert(tr_wire_header_decode(raw, &decoded) == TR_OK);

	limits.max_payload_len = 4096;
	assert(tr_wire_header_validate(raw, &decoded, &limits) ==
	       TR_ERR_BAD_LENGTH);
}

static void test_zero_payload_control_frame(void)
{
	struct tr_buffer_pool pool;
	struct tr_parser parser;
	struct tr_wire_limits limits;
	struct tr_frame frame;
	uint8_t *wire;
	size_t wire_len;

	assert(tr_buffer_pool_init(&pool, 1, TEST_BUF_SIZE) == TR_OK);
	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_parser_init(&parser, &pool, &limits) == TR_OK);
	tr_frame_init(&frame);

	build_wire_frame(&wire, &wire_len, TR_FRAME_PING, 0, 0, 42, NULL, 0);

	assert(feed_bytes(&parser, wire, wire_len, 3, &frame) ==
	       TR_FRAME_READY);
	assert(frame.header.type == TR_FRAME_PING);
	assert(frame.header.message_id == 42);
	assert(frame.payload == NULL);
	assert(tr_buffer_pool_free_count(&pool) == 1);

	tr_frame_release(&frame);
	free(wire);
	tr_parser_reset(&parser);
	tr_buffer_pool_destroy(&pool);
}

static void test_invalid_flags_and_reserved(void)
{
	struct tr_frame_header h;
	struct tr_frame_header decoded;
	struct tr_wire_limits limits;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&h, 0, sizeof(h));
	h.version = TR_WIRE_ENV_VERSION;
	h.type = TR_FRAME_DATA;
	h.flags = (1U << 31);
	assert(tr_wire_header_encode(raw, &h) == TR_OK);
	assert(tr_wire_header_decode(raw, &decoded) == TR_OK);
	limits.max_payload_len = TEST_BUF_SIZE;
	assert(tr_wire_header_validate(raw, &decoded, &limits) ==
	       TR_ERR_BAD_FLAGS);

	memset(&h, 0, sizeof(h));
	h.version = TR_WIRE_ENV_VERSION;
	h.type = TR_FRAME_DATA;
	h.reserved = 1;
	assert(tr_wire_header_encode(raw, &h) == TR_OK);
	assert(tr_wire_header_decode(raw, &decoded) == TR_OK);
	assert(tr_wire_header_validate(raw, &decoded, &limits) ==
	       TR_ERR_RESERVED);
}

struct reactor_timer_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned fired;
};

static uint64_t reactor_timer_test_cb(void *arg, uint64_t now_ns)
{
	struct reactor_timer_test_ctx *ctx =
		(struct reactor_timer_test_ctx *)arg;

	(void)now_ns;
	pthread_mutex_lock(&ctx->lock);
	ctx->fired++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return 0;
}

static uint64_t test_monotonic_now_ns(void)
{
	struct timespec now;

	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}

static void test_reactor_local_timer_loop(void)
{
	struct tr_reactor_config config;
	struct tr_reactor *reactor = NULL;
	struct tr_reactor_timer_handle timer;
	struct reactor_timer_test_ctx ctx;
	struct timespec wait_deadline;
	int ret = 0;

	memset(&config, 0, sizeof(config));
	config.max_connections = 2U;
	config.command_capacity = 32U;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 4U;
	config.rx_buffer_count = 4U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	config.rx_budget_bytes = 64U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_timer_register(reactor, reactor_timer_test_cb,
					 &ctx, &timer) == TR_OK);
	assert(tr_reactor_timer_arm(
		       timer,
		       test_monotonic_now_ns() + UINT64_C(20000000)) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &wait_deadline) == 0);
	wait_deadline.tv_sec += 2;

	pthread_mutex_lock(&ctx.lock);
	while (ctx.fired == 0U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock,
					     &wait_deadline);
	assert(ret == 0);
	assert(ctx.fired == 1U);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_timer_unregister(timer) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void make_tcp_pair(int *client_fd, int *server_fd)
{
	struct pollfd fds[2];
	uint16_t port;
	int listener = -1;
	int client = -1;
	int server = -1;
	int connect_status;
	int attempts;

	assert(tr_tcp_listen_ipv4("127.0.0.1", 0, 16, &listener, &port) ==
	       TR_OK);
	connect_status = tr_tcp_connect_ipv4("127.0.0.1", port, &client);
	assert(connect_status == TR_OK || connect_status == TR_IN_PROGRESS);

	for (attempts = 0; attempts < 100 && server < 0; ++attempts) {
		int ret = tr_tcp_accept(listener, &server);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);

		memset(fds, 0, sizeof(fds));
		fds[0].fd = listener;
		fds[0].events = POLLIN;
		fds[1].fd = client;
		fds[1].events = POLLOUT;
		assert(poll(fds, 2, 50) >= 0);
	}

	assert(server >= 0);

	if (connect_status == TR_IN_PROGRESS) {
		memset(fds, 0, sizeof(fds));
		fds[0].fd = client;
		fds[0].events = POLLOUT;
		assert(poll(fds, 1, 1000) == 1);
		assert(tr_tcp_finish_connect(client) == TR_OK);
	}

	tr_socket_close(&listener);
	*client_fd = client;
	*server_fd = server;
}

static void make_tcp_pair_keep_listener(int *listener_fd, uint16_t *port_out,
					int *client_fd, int *server_fd)
{
	struct pollfd fds[2];
	int listener = -1;
	int client = -1;
	int server = -1;
	int connect_status;
	int attempts;
	uint16_t port = 0;

	assert(tr_tcp_listen_ipv4("127.0.0.1", 0, 16, &listener, &port) ==
	       TR_OK);
	connect_status = tr_tcp_connect_ipv4("127.0.0.1", port, &client);
	assert(connect_status == TR_OK || connect_status == TR_IN_PROGRESS);

	for (attempts = 0; attempts < 100 && server < 0; ++attempts) {
		int ret = tr_tcp_accept(listener, &server);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);

		memset(fds, 0, sizeof(fds));
		fds[0].fd = listener;
		fds[0].events = POLLIN;
		fds[1].fd = client;
		fds[1].events = POLLOUT;
		assert(poll(fds, 2, 50) >= 0);
	}
	assert(server >= 0);

	if (connect_status == TR_IN_PROGRESS) {
		memset(fds, 0, sizeof(fds));
		fds[0].fd = client;
		fds[0].events = POLLOUT;
		assert(poll(fds, 1, 1000) == 1);
		assert(tr_tcp_finish_connect(client) == TR_OK);
	}

	*listener_fd = listener;
	*port_out = port;
	*client_fd = client;
	*server_fd = server;
}

#define REACTOR_RECORDS 8

struct reactor_record {
	struct tr_conn_handle connection;
	uint16_t type;
	uint32_t stream_id;
	uint64_t message_id;
	uint32_t payload_len;
	uint32_t payload_crc;
};

struct reactor_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct reactor_record records[REACTOR_RECORDS];
	unsigned record_count;
	unsigned event_count;
};

static enum tr_frame_disposition
reactor_test_on_frame(struct tr_conn_handle connection, struct tr_frame *frame,
		      void *arg)
{
	struct reactor_test_ctx *ctx = (struct reactor_test_ctx *)arg;
	struct reactor_record *record;

	pthread_mutex_lock(&ctx->lock);
	assert(ctx->record_count < REACTOR_RECORDS);
	record = &ctx->records[ctx->record_count++];
	record->connection = connection;
	record->type = frame->header.type;
	record->stream_id = frame->header.stream_id;
	record->message_id = frame->header.message_id;
	record->payload_len = frame->payload ? frame->payload->len : 0;
	record->payload_crc =
		tr_crc32c(frame->payload ? frame->payload->data : NULL,
			  record->payload_len);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	return TR_FRAME_RELEASE;
}

static void reactor_test_on_event(struct tr_conn_handle connection,
				  enum tr_connection_event event, int status,
				  void *arg)
{
	struct reactor_test_ctx *ctx = (struct reactor_test_ctx *)arg;
	(void)connection;
	(void)event;
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	ctx->event_count++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_for_records(struct reactor_test_ctx *ctx, unsigned count)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (ctx->record_count < count && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->record_count >= count);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_for_pool_full(struct tr_buffer_pool *pool, uint32_t expected)
{
	unsigned i;

	for (i = 0; i < 500; ++i) {
		if (tr_buffer_pool_free_count(pool) == expected)
			return;
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 1000000L;
			nanosleep(&pause_time, NULL);
		}
	}

	assert(tr_buffer_pool_free_count(pool) == expected);
}

static void test_reactor_tcp_roundtrip(void)
{
	struct tr_reactor_config config;
	struct tr_reactor *reactor = NULL;
	struct reactor_test_ctx ctx;
	struct tr_conn_handle client_handle;
	struct tr_conn_handle server_handle;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *buffer;
	uint32_t expected_crc;
	int client_fd;
	int server_fd;
	int small_sendbuf = 4096;
	uint32_t i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);
	assert(setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &small_sendbuf,
			  sizeof(small_sendbuf)) == 0);

	memset(&config, 0, sizeof(config));
	config.max_connections = 8;
	config.command_capacity = 64;
	config.tx_item_capacity = 32;
	config.rx_buffer_count = 8;
	config.rx_buffer_size = TEST_BUF_SIZE;
	config.max_payload_len = TEST_BUF_SIZE;
	config.rx_budget_bytes = 128U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&config, reactor_test_on_frame,
				 reactor_test_on_event, &ctx,
				 &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_handle) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_handle) ==
	       TR_OK);

	assert(tr_buffer_pool_init(&tx_pool, 2, TEST_BUF_SIZE) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, TEST_BUF_SIZE, &buffer) == TR_OK);
	buffer->len = TEST_BUF_SIZE;
	for (i = 0; i < buffer->len; ++i)
		buffer->data[i] = (uint8_t)(i * 17U + 3U);
	expected_crc = tr_crc32c(buffer->data, buffer->len);

	assert(tr_reactor_send(client_handle, TR_FRAME_DATA,
			       TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 7,
			       UINT64_C(1001), buffer) == TR_OK);

	wait_for_records(&ctx, 1);
	assert(ctx.records[0].connection.slot == server_handle.slot);
	assert(ctx.records[0].connection.generation ==
	       server_handle.generation);
	assert(ctx.records[0].type == TR_FRAME_DATA);
	assert(ctx.records[0].stream_id == 7);
	assert(ctx.records[0].message_id == UINT64_C(1001));
	assert(ctx.records[0].payload_len == TEST_BUF_SIZE);
	assert(ctx.records[0].payload_crc == expected_crc);
	wait_for_pool_full(&tx_pool, 2);

	assert(tr_buffer_acquire(&tx_pool, 64, &buffer) == TR_OK);
	memcpy(buffer->data, "reactor-response", 16);
	buffer->len = 16;
	expected_crc = tr_crc32c(buffer->data, buffer->len);

	assert(tr_reactor_send(server_handle, TR_FRAME_DATA,
			       TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 9,
			       UINT64_C(1002), buffer) == TR_OK);

	wait_for_records(&ctx, 2);
	assert(ctx.records[1].connection.slot == client_handle.slot);
	assert(ctx.records[1].stream_id == 9);
	assert(ctx.records[1].message_id == UINT64_C(1002));
	assert(ctx.records[1].payload_len == 16);
	assert(ctx.records[1].payload_crc == expected_crc);
	wait_for_pool_full(&tx_pool, 2);

	assert(tr_reactor_close(client_handle) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&tx_pool);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct reactor_handler_swap_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned old_calls;
	unsigned new_calls;
	int old_entered;
	int old_release;
	int setter_done;
	int setter_ret;
};

struct reactor_handler_set_arg {
	struct tr_conn_handle connection;
	struct reactor_handler_swap_ctx *ctx;
};

static enum tr_frame_disposition
reactor_handler_old_cb(struct tr_conn_handle connection, struct tr_frame *frame,
		       void *arg)
{
	struct reactor_handler_swap_ctx *ctx =
		(struct reactor_handler_swap_ctx *)arg;
	(void)connection;
	(void)frame;

	pthread_mutex_lock(&ctx->lock);
	ctx->old_calls++;
	ctx->old_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->old_release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

static enum tr_frame_disposition
reactor_handler_new_cb(struct tr_conn_handle connection, struct tr_frame *frame,
		       void *arg)
{
	struct reactor_handler_swap_ctx *ctx =
		(struct reactor_handler_swap_ctx *)arg;
	(void)connection;
	(void)frame;

	pthread_mutex_lock(&ctx->lock);
	ctx->new_calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

static void *reactor_handler_set_thread(void *arg)
{
	struct reactor_handler_set_arg *set_arg =
		(struct reactor_handler_set_arg *)arg;
	struct reactor_handler_swap_ctx *ctx = set_arg->ctx;
	int ret;

	ret = tr_reactor_set_handler(set_arg->connection,
				     reactor_handler_new_cb, NULL, ctx);

	pthread_mutex_lock(&ctx->lock);
	ctx->setter_ret = ret;
	ctx->setter_done = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return NULL;
}

static void test_reactor_handler_update_is_owner_serialized(void)
{
	struct tr_reactor_config config;
	struct tr_reactor *reactor = NULL;
	struct tr_conn_handle client_handle;
	struct tr_conn_handle server_handle;
	struct reactor_handler_swap_ctx ctx;
	struct reactor_handler_set_arg set_arg;
	struct timespec deadline;
	struct timespec pause_time;
	pthread_t setter;
	int client_fd;
	int server_fd;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);

	memset(&config, 0, sizeof(config));
	config.max_connections = 4U;
	config.command_capacity = 32U;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 8U;
	config.rx_buffer_count = 4U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	config.rx_budget_bytes = 64U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_handle) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_handle) == TR_OK);
	assert(tr_reactor_set_handler(server_handle, reactor_handler_old_cb,
				      NULL, &ctx) == TR_OK);

	assert(tr_reactor_send(client_handle, TR_FRAME_PING, 0, 0, 1,
			       NULL) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx.lock);
	while (!ctx.old_entered && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.old_calls == 1U);
	pthread_mutex_unlock(&ctx.lock);

	memset(&set_arg, 0, sizeof(set_arg));
	set_arg.connection = server_handle;
	set_arg.ctx = &ctx;
	assert(pthread_create(&setter, NULL, reactor_handler_set_thread,
			      &set_arg) == 0);

	/* 旧回调尚未返回时，SET_HANDLER 命令只能排队，
	 * 不能从生产者线程直接修改处理函数。
	 */
	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 100000000L;
	nanosleep(&pause_time, NULL);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.setter_done == 0);
	ctx.old_release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	assert(pthread_join(setter, NULL) == 0);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.setter_done == 1);
	assert(ctx.setter_ret == TR_OK);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_send(client_handle, TR_FRAME_PING, 0, 0, 2,
			       NULL) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	ret = 0;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.new_calls < 1U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.old_calls == 1U);
	assert(ctx.new_calls == 1U);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct backpressure_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned frames;
	struct tr_buffer *held_payload;
	uint64_t last_message_id;
	int take_first;
};

static enum tr_frame_disposition
backpressure_on_frame(struct tr_conn_handle connection, struct tr_frame *frame,
		      void *arg)
{
	struct backpressure_test_ctx *ctx = (struct backpressure_test_ctx *)arg;
	enum tr_frame_disposition disposition = TR_FRAME_RELEASE;
	(void)connection;

	pthread_mutex_lock(&ctx->lock);
	ctx->frames++;
	ctx->last_message_id = frame->header.message_id;
	if (ctx->frames == 1) {
		ctx->held_payload = frame->payload;
		disposition = TR_FRAME_TAKE_OWNERSHIP;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	return disposition;
}

static void wait_backpressure_frames(struct backpressure_test_ctx *ctx,
				     unsigned count)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (ctx->frames < count && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->frames >= count);
	pthread_mutex_unlock(&ctx->lock);
}

struct channel_handler_owner_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int owner_entered;
	int owner_release;
	int setter_done;
	int setter_ret;
};

struct channel_handler_set_arg {
	struct tr_channel *channel;
	struct channel_handler_owner_ctx *ctx;
};

static int channel_handler_owner_gate(void *arg)
{
	struct channel_handler_owner_ctx *ctx =
		(struct channel_handler_owner_ctx *)arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->owner_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->owner_release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static void *channel_handler_set_thread(void *arg)
{
	struct channel_handler_set_arg *set_arg =
		(struct channel_handler_set_arg *)arg;
	struct channel_handler_owner_ctx *ctx = set_arg->ctx;
	int ret;

	ret = tr_channel_set_handler(
		set_arg->channel, NULL, NULL, NULL, NULL);
	pthread_mutex_lock(&ctx->lock);
	ctx->setter_ret = ret;
	ctx->setter_done = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return NULL;
}

static void *channel_handler_gate_thread(void *arg)
{
	struct channel_handler_set_arg *set_arg =
		(struct channel_handler_set_arg *)arg;

	assert(tr_reactor_call(
		       tr_channel_reactor(set_arg->channel),
		       channel_handler_owner_gate, set_arg->ctx) == TR_OK);
	return NULL;
}

static void test_channel_handler_publication_is_owner_serialized(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *channel = NULL;
	struct tr_conn_handle connection;
	struct channel_handler_owner_ctx ctx;
	struct channel_handler_set_arg arg;
	struct timespec deadline;
	struct timespec pause_time = { 0, 100000000L };
	pthread_t gate;
	pthread_t setter;
	int fds[2];
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 2U;
	reactor_config.command_capacity = 32U;
	reactor_config.tx_item_capacity = 8U;
	reactor_config.control_tx_item_capacity = 8U;
	reactor_config.rx_buffer_count = 4U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, fds[0], &connection) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create_deferred(
		       &channel_config, connection, connection,
		       NULL, NULL, NULL, NULL, &channel) == TR_OK);

	arg.channel = channel;
	arg.ctx = &ctx;
	assert(pthread_create(
		       &gate, NULL, channel_handler_gate_thread, &arg) == 0);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx.lock);
	while (!ctx.owner_entered && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	pthread_mutex_unlock(&ctx.lock);

	assert(pthread_create(
		       &setter, NULL, channel_handler_set_thread, &arg) == 0);
	(void)nanosleep(&pause_time, NULL);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.setter_done == 0);
	ctx.owner_release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(setter, NULL) == 0);
	assert(ctx.setter_ret == TR_OK);

	assert(tr_reactor_stop(reactor) == TR_OK);
	/* 完全停止后允许仅用于拆除的直接发布。 */
	assert(tr_channel_set_handler(
		       channel, NULL, NULL, NULL, NULL) == TR_OK);
	assert(tr_channel_destroy(channel) == TR_OK);
	tr_reactor_destroy(reactor);
	assert(close(fds[1]) == 0);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_reactor_rx_pool_backpressure(void)
{
	struct tr_reactor_config config;
	struct tr_reactor *reactor = NULL;
	struct backpressure_test_ctx ctx;
	struct tr_conn_handle client_handle;
	struct tr_conn_handle server_handle;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *first;
	struct tr_buffer *second;
	struct timespec pause_time;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);

	memset(&config, 0, sizeof(config));
	config.max_connections = 4;
	config.command_capacity = 32;
	config.tx_item_capacity = 8;
	config.rx_buffer_count = 1;
	config.rx_buffer_size = 4096;
	config.max_payload_len = 4096;
	config.rx_budget_bytes = 64U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&config, backpressure_on_frame, NULL, &ctx,
				 &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_handle) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_handle) ==
	       TR_OK);

	assert(tr_buffer_pool_init(&tx_pool, 2, 4096) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 1024, &first) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 1024, &second) == TR_OK);
	memset(first->data, 0x31, 1024);
	memset(second->data, 0x32, 1024);
	first->len = 1024;
	second->len = 1024;

	assert(tr_reactor_send(client_handle, TR_FRAME_DATA,
			       TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 2001,
			       first) == TR_OK);
	assert(tr_reactor_send(client_handle, TR_FRAME_DATA,
			       TR_FRAME_F_FIRST | TR_FRAME_F_LAST, 1, 2002,
			       second) == TR_OK);

	wait_backpressure_frames(&ctx, 1);

	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 100000000L;
	nanosleep(&pause_time, NULL);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.frames == 1);
	assert(ctx.held_payload != NULL);
	pthread_mutex_unlock(&ctx.lock);

	tr_buffer_release(ctx.held_payload);
	ctx.held_payload = NULL;
	assert(tr_reactor_resume_rx(server_handle) == TR_OK);

	wait_backpressure_frames(&ctx, 2);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.last_message_id == 2002);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	wait_for_pool_full(&tx_pool, 2);
	tr_buffer_pool_destroy(&tx_pool);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct channel_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened;
	unsigned received;
	unsigned remote_closed;
	unsigned closed;
	unsigned stream_errors;
	unsigned channel_events;
	unsigned channel_down;
	unsigned channel_up;
	unsigned channel_goaway;
	unsigned reconnect_failures;
	int reconnect_failure_status;
	enum tr_channel_event reconnect_failure_event;
	uint64_t ordered_event_seq;
	uint64_t opened_event_seq;
	uint64_t goaway_event_seq;
	struct tr_stream_handle last_stream;
	struct tr_buffer *held_payload;
	uint64_t last_message_id;
	int take_first;
};

static enum tr_stream_data_disposition
channel_test_on_data(struct tr_stream_handle stream, uint64_t message_id,
		     struct tr_buffer *payload, void *arg)
{
	struct channel_test_ctx *ctx = (struct channel_test_ctx *)arg;
	enum tr_stream_data_disposition disposition = TR_STREAM_DATA_RELEASE;

	pthread_mutex_lock(&ctx->lock);
	ctx->received++;
	ctx->last_stream = stream;
	ctx->last_message_id = message_id;
	if (ctx->take_first && ctx->received == 1 && payload) {
		ctx->held_payload = payload;
		disposition = TR_STREAM_DATA_TAKE_OWNERSHIP;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return disposition;
}

static void channel_test_on_stream_event(struct tr_stream_handle stream,
					 enum tr_stream_event event, int status,
					 void *arg)
{
	struct channel_test_ctx *ctx = (struct channel_test_ctx *)arg;
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	if (event == TR_STREAM_EVENT_OPENED) {
		ctx->opened++;
		ctx->opened_event_seq = ++ctx->ordered_event_seq;
		ctx->last_stream = stream;
	} else if (event == TR_STREAM_EVENT_REMOTE_CLOSED) {
		ctx->remote_closed++;
	} else if (event == TR_STREAM_EVENT_CLOSED) {
		ctx->closed++;
	} else if (event == TR_STREAM_EVENT_ERROR) {
		ctx->stream_errors++;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void channel_test_on_channel_event(struct tr_channel *channel,
					  enum tr_channel_event event,
					  int status, void *arg)
{
	struct channel_test_ctx *ctx = (struct channel_test_ctx *)arg;
	(void)channel;
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	ctx->channel_events++;
	if (event == TR_CHANNEL_EVENT_CONTROL_DOWN ||
	    event == TR_CHANNEL_EVENT_BULK_DOWN)
		ctx->channel_down++;
	else if (event == TR_CHANNEL_EVENT_CONTROL_UP ||
		 event == TR_CHANNEL_EVENT_BULK_UP)
		ctx->channel_up++;
	else if (event == TR_CHANNEL_EVENT_CONTROL_GOAWAY ||
		 event == TR_CHANNEL_EVENT_BULK_GOAWAY) {
		ctx->channel_goaway++;
		if (ctx->goaway_event_seq == 0U)
			ctx->goaway_event_seq = ++ctx->ordered_event_seq;
	} else if (event == TR_CHANNEL_EVENT_CONTROL_RECONNECT_FAILED ||
		   event == TR_CHANNEL_EVENT_BULK_RECONNECT_FAILED) {
		ctx->reconnect_failures++;
		ctx->reconnect_failure_status = status;
		ctx->reconnect_failure_event = event;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_channel_counter(struct channel_test_ctx *ctx, unsigned *value,
				 unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_channel_lane_down(struct tr_channel *channel,
				   enum tr_lane lane);

static void wait_channel_lane_up(struct tr_channel *channel, enum tr_lane lane)
{
	assert(tr_channel_wait_ready(channel, lane, 5000U) == TR_OK);
}

static void init_channel_test_ctx(struct channel_test_ctx *ctx)
{
	memset(ctx, 0, sizeof(*ctx));
	assert(pthread_mutex_init(&ctx->lock, NULL) == 0);
	assert(pthread_cond_init(&ctx->cond, NULL) == 0);
}

static void destroy_channel_test_ctx(struct channel_test_ctx *ctx)
{
	pthread_cond_destroy(&ctx->cond);
	pthread_mutex_destroy(&ctx->lock);
}

static void wait_channel_active_streams(struct tr_channel *channel,
					uint32_t target)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		if (tr_channel_active_streams(channel) == target)
			return;
		{
			struct timespec pause_time = { 0, 1000000L };
			nanosleep(&pause_time, NULL);
		}
	}
	assert(tr_channel_active_streams(channel) == target);
}

static void wait_connection_rx_frames(struct tr_conn_handle connection,
				      uint64_t target)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		struct tr_connection_stats stats;
		assert(tr_reactor_get_connection_stats(connection, &stats) == TR_OK);
		if (stats.rx_frames >= target)
			return;
		{
			struct timespec pause_time = { 0, 1000000L };
			nanosleep(&pause_time, NULL);
		}
	}
	assert(!"connection did not receive expected frame");
}

struct channel_ready_wait_probe {
	struct tr_channel *channel;
	enum tr_lane lane;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int done;
	int ret;
};

static void *channel_ready_wait_main(void *arg)
{
	struct channel_ready_wait_probe *probe =
		(struct channel_ready_wait_probe *)arg;

	pthread_mutex_lock(&probe->lock);
	probe->entered = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);

	probe->ret = tr_channel_wait_ready(
		probe->channel, probe->lane, 5000U);

	pthread_mutex_lock(&probe->lock);
	probe->done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
	return NULL;
}

static void wait_channel_ready_probe(
	struct channel_ready_wait_probe *probe, const int *field)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe->lock);
	while (!*field && ret == 0)
		ret = pthread_cond_timedwait(
			&probe->cond, &probe->lock, &deadline);
	assert(*field);
	pthread_mutex_unlock(&probe->lock);
}

struct channel_split_create_probe {
	struct tr_channel_config config;
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
	struct tr_channel *channel;
	int create_ret;
};

static int channel_split_create_on_owner(void *arg)
{
	struct channel_split_create_probe *probe =
		(struct channel_split_create_probe *)arg;

	probe->create_ret = tr_channel_create(
		&probe->config, probe->control, probe->bulk,
		NULL, NULL, NULL, NULL, &probe->channel);
	return TR_OK;
}

static void test_channel_split_create_tx_exhaustion_rollback(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_reactor *reactor = NULL;
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
	struct channel_split_create_probe probe;
	struct tr_reactor_stats stats;
	int control_fds[2];
	int bulk_fds[2];

	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0,
		       control_fds) == 0);
	assert(socketpair(
		       AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0,
		       bulk_fds) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 32U;
	reactor_config.tx_item_capacity = 8U;
	reactor_config.control_tx_item_capacity = 1U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(
		       reactor, control_fds[0], &control) == TR_OK);
	assert(tr_reactor_adopt_fd(
		       reactor, bulk_fds[0], &bulk) == TR_OK);

	memset(&probe, 0, sizeof(probe));
	probe.control = control;
	probe.bulk = bulk;
	probe.config.role = TR_CHANNEL_CLIENT;
	probe.config.mode = TR_CHANNEL_SPLIT_CONNECTIONS;
	probe.config.max_streams = 4U;
	probe.config.initial_window_bytes = 4096U;

	/* 回调拥有该反应器轮次。第一个 HELLO 消耗唯一 CONTROL 发送项，
	 * 此回调返回前无法分派；因此第二条分离通道 HELLO 确定性返回 TR_AGAIN。
	 */
	assert(tr_reactor_call(
		       reactor, channel_split_create_on_owner, &probe) == TR_OK);
	assert(probe.create_ret == TR_AGAIN);
	assert(probe.channel == NULL);

	/* 第一个 HELLO 命令已在环形队列中排在当前 CALL 之后。
	 * 回滚立即关闭两个句柄；随后排队 SEND 观察到陈旧代次，
	 * 释放其独立拥有的负载与发送项。
	 */
	assert(tr_reactor_quiesce(reactor) == TR_OK);
	memset(&stats, 0, sizeof(stats));
	assert(tr_reactor_get_stats(reactor, &stats) == TR_OK);
	assert(stats.control_tx_item_pool.current == 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(control_fds[1]) == 0);
	assert(close(bulk_fds[1]) == 0);
}

/* 故意让第一个 HELLO 独占唯一控制发送项。第二个延后创建的
 * start() 失败后，重复调用不得把尚未发出的 HELLO 误报为成功。
 */
struct channel_failed_start_probe {
	struct tr_conn_handle first_connection;
	struct tr_conn_handle second_connection;
	struct tr_channel *first;
	struct tr_channel *second;
	int first_ret;
	int second_ret;
	int retry_ret;
};

static int channel_failed_start_on_owner(void *arg)
{
	struct channel_failed_start_probe *p = arg;
	struct tr_channel_config config = {0};

	config.role = TR_CHANNEL_CLIENT;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	assert(tr_channel_create_deferred(&config, p->first_connection,
	                                  p->first_connection, NULL, NULL, NULL,
	                                  NULL, &p->first) == TR_OK);
	config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create_deferred(&config, p->second_connection,
	                                  p->second_connection, NULL, NULL,
	                                  NULL, NULL, &p->second) == TR_OK);
	p->first_ret = tr_channel_start(p->first);
	p->second_ret = tr_channel_start(p->second);
	p->retry_ret = tr_channel_start(p->second);
	return TR_OK;
}

static void test_channel_failed_hello_start_is_sticky(void)
{
	struct tr_reactor_config config = {0};
	struct tr_reactor *reactor = NULL;
	struct channel_failed_start_probe probe = {0};
	int a[2], b[2];

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, a) == 0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, b) == 0);
	config.max_connections = 4U;
	config.command_capacity = 2U;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 1U;
	config.rx_buffer_count = 8U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	config.rx_budget_bytes = 64U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, a[0], &probe.first_connection) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(reactor, b[0], &probe.second_connection) ==
	       TR_OK);
	assert(tr_reactor_call(reactor, channel_failed_start_on_owner,
	                       &probe) == TR_OK);
	assert(probe.first_ret == TR_OK);
	assert(probe.second_ret == TR_AGAIN);
	assert(probe.retry_ret == TR_AGAIN);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(probe.first) == TR_OK);
	assert(tr_channel_destroy(probe.second) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(a[1]) == 0);
	assert(close(b[1]) == 0);
}

/* 同一所有者轮次里不会执行 I/O 分派。两个独立 Channel 若共用
 * 唯一普通 CONTROL 发送项，第二个 HELLO 应当明确返回容量背压。
 * 这验证构造失败的真实路径，与下面的 GOAWAY 专用保留不变量区分。
 */
struct channel_hello_contention_probe {
	struct tr_conn_handle first_connection;
	struct tr_conn_handle second_connection;
	struct tr_channel *first;
	struct tr_channel *second;
	int first_ret;
	int second_ret;
	uint32_t pool_in_use_after_first;
};

static int channel_two_hello_on_owner(void *arg)
{
	struct channel_hello_contention_probe *probe = arg;
	struct tr_channel_config config = {0};
	struct tr_reactor_stats stats;

	config.role = TR_CHANNEL_CLIENT;
	config.mode = TR_CHANNEL_SHARED_CONNECTION;
	config.max_streams = 4U;
	config.initial_window_bytes = 4096U;
	probe->first_ret = tr_channel_create(&config, probe->first_connection,
	                                     probe->first_connection, NULL,
	                                     NULL, NULL, NULL, &probe->first);
	if (probe->first_ret != TR_OK)
		return TR_OK;

	assert(tr_reactor_get_stats(probe->first_connection.reactor, &stats) ==
	       TR_OK);
	probe->pool_in_use_after_first = stats.control_tx_item_pool.current;
	config.role = TR_CHANNEL_SERVER;
	probe->second_ret = tr_channel_create(&config, probe->second_connection,
	                                      probe->second_connection, NULL,
	                                      NULL, NULL, NULL, &probe->second);
	return TR_OK;
}

static void test_channel_hello_contention_is_bounded(void)
{
	struct tr_reactor_config config = {0};
	struct tr_reactor *reactor = NULL;
	struct tr_reactor_stats stats;
	struct channel_hello_contention_probe probe = {0};
	int first_fds[2], second_fds[2];

	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, first_fds) ==
	       0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0,
	                  second_fds) == 0);
	config.max_connections = 4U;
	config.command_capacity = 2U;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 1U;
	config.rx_buffer_count = 8U;
	config.rx_buffer_size = 4096U;
	config.max_payload_len = 4096U;
	config.rx_budget_bytes = 64U * 1024U;
	config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, first_fds[0],
	                           &probe.first_connection) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, second_fds[0],
	                           &probe.second_connection) == TR_OK);

	assert(tr_reactor_call(reactor, channel_two_hello_on_owner, &probe) ==
	       TR_OK);
	assert(probe.first_ret == TR_OK);
	assert(probe.pool_in_use_after_first == 1U);
	assert(probe.second_ret == TR_AGAIN);
	assert(probe.second == NULL);

	/* 失败时回滚的发送项只能由原命令或停止回收；不可借此泄漏。 */
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_get_stats(reactor, &stats) == TR_OK);
	assert(stats.control_tx_item_pool.current == 0U);
	assert(tr_channel_destroy(probe.first) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	assert(close(first_fds[1]) == 0);
	assert(close(second_fds[1]) == 0);
}

static void test_channel_deferred_hello_gate(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle server_conn;
	struct channel_test_ctx server_ctx;
	struct channel_ready_wait_probe ready_probe;
	pthread_t ready_thread;
	enum tr_channel_lane_state lane_state;
	uint8_t hello_payload[32] = { 0 };
	uint8_t data_payload[4] = { 'p', 'i', 'n', 'g' };
	uint8_t *wire = NULL;
	size_t wire_len = 0;
	int client_fd;
	int server_fd;

	init_channel_test_ctx(&server_ctx);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 16U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create_deferred(
		       &channel_config, server_conn, server_conn,
		       NULL, NULL, NULL, NULL, &server_channel) == TR_OK);

	/* 对端抢在服务端 RPC 或服务绑定前发送 HELLO。 */
	tr_put_le16(hello_payload + 0, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le16(hello_payload + 2, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le32(hello_payload + 4,
		    TR_CHANNEL_LANE_MASK_CONTROL | TR_CHANNEL_LANE_MASK_BULK);
	tr_put_le32(hello_payload + 8, 4096U);
	tr_put_le32(hello_payload + 12, 4096U);
	tr_put_le64(hello_payload + 16, 0U);
	build_wire_frame(&wire, &wire_len, TR_FRAME_HELLO, 0U, 0U, 0U,
			 hello_payload, sizeof(hello_payload));
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_connection_rx_frames(server_conn, 1U);

	/* 收到对端 HELLO 不等于服务就绪。start() 前，
	 * 延后创建的 Channel 不得回复 ACK 或发布通道 UP。
	 */
	assert(tr_channel_get_lane_state(server_channel, TR_LANE_CONTROL,
					 &lane_state) == TR_OK);
	assert(lane_state != TR_CHANNEL_LANE_UP);
	assert(tr_channel_get_lane_state(server_channel, TR_LANE_BULK,
					 &lane_state) == TR_OK);
	assert(lane_state != TR_CHANNEL_LANE_UP);

	memset(&ready_probe, 0, sizeof(ready_probe));
	ready_probe.channel = server_channel;
	ready_probe.lane = TR_LANE_CONTROL;
	assert(pthread_mutex_init(&ready_probe.lock, NULL) == 0);
	assert(pthread_cond_init(&ready_probe.cond, NULL) == 0);
	assert(pthread_create(
		       &ready_thread, NULL, channel_ready_wait_main,
		       &ready_probe) == 0);
	wait_channel_ready_probe(&ready_probe, &ready_probe.entered);

	assert(tr_channel_set_handler(server_channel, channel_test_on_data,
				      channel_test_on_stream_event,
				      channel_test_on_channel_event,
				      &server_ctx) == TR_OK);
	assert(tr_channel_start(server_channel) == TR_OK);
	wait_channel_ready_probe(&ready_probe, &ready_probe.done);
	assert(pthread_join(ready_thread, NULL) == 0);
	assert(ready_probe.ret == TR_OK);
	pthread_cond_destroy(&ready_probe.cond);
	pthread_mutex_destroy(&ready_probe.lock);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	/* 服务就绪后，同一连接可以继续流 DATA。 */
	build_wire_frame(&wire, &wire_len, TR_FRAME_STREAM_OPEN, 0U, 1U,
			 4096U, NULL, 0U);
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1U);

	build_wire_frame(&wire, &wire_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
			 1U, 1U, data_payload, sizeof(data_payload));
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_channel_counter(&server_ctx, &server_ctx.received, 1U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	assert(close(client_fd) == 0);
	destroy_channel_test_ctx(&server_ctx);
}

static void test_channel_stream_id_index_collision_delete(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle handles[3];
	struct channel_test_ctx server_ctx;
	uint8_t hello_payload[32] = { 0 };
	uint8_t data_payload[4] = { 'h', 'a', 's', 'h' };
	const uint32_t stream_ids[3] = { 1U, 21U, 43U };
	uint8_t *wire = NULL;
	size_t wire_len = 0;
	unsigned i;
	int client_fd;
	int server_fd;

	/* max_streams=3 创建 8 项索引。这三个有效客户端流标识
	 * 有意共享一个初始桶，强制形成线性探测簇，测试后移删除。
	 */
	assert((tr_channel_stream_id_hash(stream_ids[0]) & 7U) ==
	       (tr_channel_stream_id_hash(stream_ids[1]) & 7U));
	assert((tr_channel_stream_id_hash(stream_ids[1]) & 7U) ==
	       (tr_channel_stream_id_hash(stream_ids[2]) & 7U));

	init_channel_test_ctx(&server_ctx);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 16U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 3U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);

	tr_put_le16(hello_payload + 0, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le16(hello_payload + 2, TR_CHANNEL_PROTOCOL_VERSION);
	tr_put_le32(hello_payload + 4,
		    TR_CHANNEL_LANE_MASK_CONTROL | TR_CHANNEL_LANE_MASK_BULK);
	tr_put_le32(hello_payload + 8, 4096U);
	tr_put_le32(hello_payload + 12, 4096U);
	tr_put_le64(hello_payload + 16, 0U);
	build_wire_frame(&wire, &wire_len, TR_FRAME_HELLO, 0U, 0U, 0U,
			 hello_payload, sizeof(hello_payload));
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	for (i = 0; i < 3U; ++i) {
		build_wire_frame(&wire, &wire_len, TR_FRAME_STREAM_OPEN, 0U,
				 stream_ids[i], 4096U, NULL, 0U);
		assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
		free(wire);
		wire = NULL;
		wait_channel_counter(&server_ctx, &server_ctx.opened, i + 1U);
		pthread_mutex_lock(&server_ctx.lock);
		handles[i] = server_ctx.last_stream;
		pthread_mutex_unlock(&server_ctx.lock);
	}
	assert(tr_channel_active_streams(server_channel) == 3U);

	/* 先关闭本地半侧，再关闭对端半侧。中间哈希条目消失时，
	 * stream_id=43 仍位于同一探测簇更后方。
	 */
	assert(tr_stream_close(handles[1]) == TR_OK);
	build_wire_frame(&wire, &wire_len, TR_FRAME_STREAM_CLOSE, 0U,
			 stream_ids[1], 0U, NULL, 0U);
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_channel_active_streams(server_channel, 2U);

	build_wire_frame(&wire, &wire_len, TR_FRAME_DATA,
			 TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
			 stream_ids[2], 1U, data_payload, sizeof(data_payload));
	assert(write(client_fd, wire, wire_len) == (ssize_t)wire_len);
	free(wire);
	wire = NULL;
	wait_channel_counter(&server_ctx, &server_ctx.received, 1U);
	pthread_mutex_lock(&server_ctx.lock);
	assert(server_ctx.last_stream.slot == handles[2].slot);
	assert(server_ctx.last_stream.generation == handles[2].generation);
	assert(server_ctx.last_message_id == 1U);
	pthread_mutex_unlock(&server_ctx.lock);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	assert(close(client_fd) == 0);
	destroy_channel_test_ctx(&server_ctx);
}

static void test_channel_stream_slot_reuse(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle first;
	struct tr_stream_handle second;
	struct tr_stream_handle server_stream;
	struct tr_stream_handle server_second;
	struct tr_stream_flow_state flow;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	int client_fd;
	int server_fd;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 16U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 1U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &client_ctx,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &first) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 1U);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1U);
	assert(tr_channel_active_streams(client_channel) == 1U);
	assert(tr_channel_active_streams(server_channel) == 1U);

	/* 此流退役前，唯一空闲链表项已耗尽。 */
	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &second) ==
	       TR_AGAIN);

	pthread_mutex_lock(&server_ctx.lock);
	server_stream = server_ctx.last_stream;
	pthread_mutex_unlock(&server_ctx.lock);
	assert(tr_stream_close(first) == TR_OK);
	assert(tr_stream_close(server_stream) == TR_OK);
	wait_channel_active_streams(client_channel, 0U);
	wait_channel_active_streams(server_channel, 0U);

	assert(tr_stream_get_flow_state(first, &flow) == TR_ERR_STALE);
	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &second) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 2U);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 2U);
	assert(second.slot == first.slot);
	assert(second.generation != first.generation);
	assert(tr_stream_get_flow_state(first, &flow) == TR_ERR_STALE);

	pthread_mutex_lock(&server_ctx.lock);
	server_second = server_ctx.last_stream;
	pthread_mutex_unlock(&server_ctx.lock);
	assert(tr_stream_close(second) == TR_OK);
	assert(tr_stream_close(server_second) == TR_OK);
	wait_channel_active_streams(client_channel, 0U);
	wait_channel_active_streams(server_channel, 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void test_channel_stream_flow_control(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle client_stream;
	struct tr_stream_flow_state flow;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *first;
	struct tr_buffer *second;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	int client_fd;
	int server_fd;
	unsigned i;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	server_ctx.take_first = 1;
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 8;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 1024;
	channel_config.window_update_threshold_bytes = 512;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &client_ctx,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_stream_open(client_channel, TR_LANE_BULK, &client_stream) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 1);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1);

	assert(tr_buffer_pool_init(&tx_pool, 2, 1024) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 1024, &first) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 1024, &second) == TR_OK);
	for (i = 0; i < 1024; ++i) {
		first->data[i] = (uint8_t)i;
		second->data[i] = (uint8_t)(i ^ 0x5aU);
	}
	first->len = 1024;
	second->len = 1024;

	assert(tr_stream_send(client_stream, first) == TR_OK);
	assert(tr_stream_send(client_stream, second) == TR_AGAIN);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);

	pthread_mutex_lock(&server_ctx.lock);
	assert(server_ctx.held_payload != NULL);
	assert(server_ctx.held_payload->len == 1024);
	pthread_mutex_unlock(&server_ctx.lock);

	assert(tr_stream_get_flow_state(client_stream, &flow) == TR_OK);
	assert(flow.tx_sent_bytes == 1024);
	assert(flow.tx_send_limit == 1024);

	{
		struct tr_stream_handle server_stream;
		pthread_mutex_lock(&server_ctx.lock);
		first = server_ctx.held_payload;
		server_stream = server_ctx.last_stream;
		server_ctx.held_payload = NULL;
		pthread_mutex_unlock(&server_ctx.lock);
		assert(tr_stream_release_payload(server_stream, first) ==
		       TR_OK);
	}

	for (i = 0; i < 500; ++i) {
		if (tr_stream_get_flow_state(client_stream, &flow) == TR_OK &&
		    flow.tx_send_limit >= 2048)
			break;
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 1000000L;
			nanosleep(&pause_time, NULL);
		}
	}
	assert(flow.tx_send_limit >= 2048);
	assert(tr_stream_send(client_stream, second) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 2);

	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	wait_for_pool_full(&tx_pool, 2);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void test_channel_message_fragmentation_reassembly(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle client_stream;
	struct tr_stream_handle server_stream;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer_pool reassembly_pool;
	struct tr_buffer *payload;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	int client_fd;
	int server_fd;
	uint32_t i;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	server_ctx.take_first = 1;
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 16;
	reactor_config.rx_buffer_size = 256;
	reactor_config.max_payload_len = 256;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	assert(tr_buffer_pool_init(&reassembly_pool, 2, 2048) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 4096;
	channel_config.window_update_threshold_bytes = 1024;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &client_ctx,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.max_message_bytes = 2048;
	channel_config.reassembly_pool = &reassembly_pool;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	{
		struct tr_channel_capabilities caps;
		assert(tr_channel_get_capabilities(client_channel, TR_LANE_BULK,
						   &caps) == TR_OK);
		assert(caps.protocol_version == TR_CHANNEL_PROTOCOL_VERSION);
		assert(caps.lane_mask == (TR_CHANNEL_LANE_MASK_CONTROL |
					  TR_CHANNEL_LANE_MASK_BULK));
		assert(caps.max_frame_payload_bytes == 256);
		assert(caps.max_message_bytes == 2048);

		assert(tr_channel_get_capabilities(server_channel, TR_LANE_BULK,
						   &caps) == TR_OK);
		assert(caps.max_frame_payload_bytes == 256);
		assert(caps.max_message_bytes == 256);
	}

	assert(tr_stream_open(client_channel, TR_LANE_BULK, &client_stream) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 1);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1);

	assert(tr_buffer_pool_init(&tx_pool, 1, 1500) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 1500, &payload) == TR_OK);
	for (i = 0; i < 1500; ++i)
		payload->data[i] = (uint8_t)((i * 13U + 7U) & 0xffU);
	payload->len = 1500;

	assert(tr_stream_send(client_stream, payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);

	pthread_mutex_lock(&server_ctx.lock);
	assert(server_ctx.received == 1);
	assert(server_ctx.last_message_id == 1);
	assert(server_ctx.held_payload != NULL);
	assert(server_ctx.held_payload->len == 1500);
	for (i = 0; i < 1500; ++i)
		assert(server_ctx.held_payload->data[i] ==
		       (uint8_t)((i * 13U + 7U) & 0xffU));
	server_stream = server_ctx.last_stream;
	payload = server_ctx.held_payload;
	server_ctx.held_payload = NULL;
	pthread_mutex_unlock(&server_ctx.lock);

	/* 六个传输分片全部重组为一个逻辑消息。 */
	assert(tr_buffer_pool_free_count(&reassembly_pool) == 1);
	assert(tr_stream_release_payload(server_stream, payload) == TR_OK);
	assert(tr_buffer_pool_free_count(&reassembly_pool) == 2);

	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	wait_for_pool_full(&tx_pool, 1);
	tr_buffer_pool_destroy(&tx_pool);
	tr_buffer_pool_destroy(&reassembly_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void test_channel_split_lane_isolation(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_control;
	struct tr_conn_handle server_control;
	struct tr_conn_handle client_bulk;
	struct tr_conn_handle server_bulk;
	struct tr_conn_handle client_bulk2;
	struct tr_conn_handle server_bulk2;
	struct tr_stream_handle control_stream;
	struct tr_stream_handle bulk_stream;
	struct tr_stream_handle bulk_stream2;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *control_payload;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	int ccfd, scfd, cbfd, sbfd;
	int cbfd2, sbfd2;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair(&ccfd, &scfd);
	make_tcp_pair(&cbfd, &sbfd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 8;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, ccfd, &client_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, scfd, &server_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, cbfd, &client_bulk) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sbfd, &server_bulk) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SPLIT_CONNECTIONS;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 4096;
	assert(tr_channel_create(&channel_config, client_control, client_bulk,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &client_ctx,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_control, server_bulk,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_stream_open(client_channel, TR_LANE_CONTROL,
			      &control_stream) == TR_OK);
	assert(tr_stream_open(client_channel, TR_LANE_BULK, &bulk_stream) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 2);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 2);

	/* BULK 丢失不得使 CONTROL 通道不可用。 */
	assert(tr_reactor_close(client_bulk) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.channel_events, 1);
	wait_channel_lane_down(client_channel, TR_LANE_BULK);
	wait_channel_lane_down(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&tx_pool, 1, 128) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 32, &control_payload) == TR_OK);
	memset(control_payload->data, 0x7b, 32);
	control_payload->len = 32;
	assert(tr_stream_send(control_stream, control_payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);

	assert(tr_buffer_acquire(&tx_pool, 32, &control_payload) == TR_OK);
	control_payload->len = 32;
	assert(tr_stream_send(bulk_stream, control_payload) == TR_ERR_STALE);
	tr_buffer_release(control_payload);

	/* BULK 可独立替换，不干扰 CONTROL。 */
	make_tcp_pair(&cbfd2, &sbfd2);
	assert(tr_reactor_adopt_fd(reactor, cbfd2, &client_bulk2) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, sbfd2, &server_bulk2) == TR_OK);
	assert(tr_channel_replace_connection(client_channel, TR_LANE_BULK,
					     client_bulk2) == TR_OK);
	assert(tr_channel_replace_connection(server_channel, TR_LANE_BULK,
					     server_bulk2) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);
	assert(tr_stream_open(client_channel, TR_LANE_BULK, &bulk_stream2) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 3);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 3);

	assert(tr_buffer_acquire(&tx_pool, 32, &control_payload) == TR_OK);
	memset(control_payload->data, 0x5c, 32);
	control_payload->len = 32;
	assert(tr_stream_send(bulk_stream2, control_payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 2);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	wait_for_pool_full(&tx_pool, 1);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

struct channel_wait_owner_probe {
	struct tr_channel *channel;
	int wait_ret;
};

struct channel_drain_open_race_probe {
	struct tr_channel *draining;
	struct tr_channel *peer;
	struct tr_stream_handle raced_stream;
	int drain_ret;
	int open_ret;
};

static int channel_begin_drain_and_race_open_on_owner(void *arg)
{
	struct channel_drain_open_race_probe *probe =
		(struct channel_drain_open_race_probe *)arg;

	/* 两个操作在同一所有者处理轮次执行。此回调返回前对端无法收到 GOAWAY，
	 * 因此其 STREAM_OPEN 确定性地代表与排空屏障竞争的打开操作。
	 */
	probe->drain_ret = tr_channel_begin_drain(probe->draining);
	probe->open_ret = tr_stream_open(
		probe->peer, TR_LANE_CONTROL, &probe->raced_stream);
	return TR_OK;
}

static int channel_wait_drained_from_owner(void *arg)
{
	struct channel_wait_owner_probe *probe =
		(struct channel_wait_owner_probe *)arg;

	probe->wait_ret = tr_channel_wait_drained(probe->channel, 100U);
	return TR_OK;
}

struct channel_wait_thread_probe {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_channel *channel;
	int entered;
	int done;
	int ret;
};

static void *channel_wait_thread_main(void *arg)
{
	struct channel_wait_thread_probe *probe =
		(struct channel_wait_thread_probe *)arg;

	pthread_mutex_lock(&probe->lock);
	probe->entered = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);

	probe->ret = tr_channel_wait_drained(probe->channel, 2000U);

	pthread_mutex_lock(&probe->lock);
	probe->done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
	return NULL;
}

static void wait_channel_wait_probe(
	struct channel_wait_thread_probe *probe, const int *field)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe->lock);
	while (!*field && ret == 0)
		ret = pthread_cond_timedwait(
			&probe->cond, &probe->lock, &deadline);
	assert(*field);
	pthread_mutex_unlock(&probe->lock);
}

struct channel_drain_backpressure_probe {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_channel *channel;
	int owner_entered;
	int run_drain;
	int drain_done;
	int drain_ret;
};

static int channel_drain_backpressure_on_owner(void *arg)
{
	struct channel_drain_backpressure_probe *probe =
		(struct channel_drain_backpressure_probe *)arg;

	pthread_mutex_lock(&probe->lock);
	probe->owner_entered = 1;
	pthread_cond_broadcast(&probe->cond);
	while (!probe->run_drain)
		pthread_cond_wait(&probe->cond, &probe->lock);
	pthread_mutex_unlock(&probe->lock);

	probe->drain_ret = tr_channel_begin_drain(probe->channel);

	pthread_mutex_lock(&probe->lock);
	probe->drain_done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
	return TR_OK;
}

static void *channel_drain_backpressure_thread(void *arg)
{
	struct channel_drain_backpressure_probe *probe =
		(struct channel_drain_backpressure_probe *)arg;

	assert(tr_reactor_call(
		       tr_channel_reactor(probe->channel),
		       channel_drain_backpressure_on_owner, probe) == TR_OK);
	return NULL;
}

static void wait_channel_drain_probe(
	struct channel_drain_backpressure_probe *probe, const int *field)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe->lock);
	while (!*field && ret == 0)
		ret = pthread_cond_timedwait(
			&probe->cond, &probe->lock, &deadline);
	assert(*field);
	pthread_mutex_unlock(&probe->lock);
}

static void test_channel_graceful_drain(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle client_stream;
	struct tr_stream_handle server_stream;
	struct tr_stream_handle rejected;
	struct channel_drain_open_race_probe race_probe;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *payload;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	struct channel_wait_owner_probe wait_probe;
	struct channel_wait_thread_probe drain_wait;
	pthread_t drain_wait_thread;
	enum tr_channel_state channel_state;
	int client_fd;
	int server_fd;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 8;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 4096;
	channel_config.window_update_threshold_bytes = 1024;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &client_ctx,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 channel_test_on_data,
				 channel_test_on_stream_event,
				 channel_test_on_channel_event, &server_ctx,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_stream_open(client_channel, TR_LANE_CONTROL,
			      &client_stream) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 1);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1);
	pthread_mutex_lock(&server_ctx.lock);
	server_stream = server_ctx.last_stream;
	pthread_mutex_unlock(&server_ctx.lock);

	/* 排空与禁用重连现在是同一个反应器所有者事务。
	 * 先启用重连，再进入排空；排空返回后任何重新启用都必须
	 * 看到 local_draining 屏障。
	 */
	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = 1U;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 20U;
	reconnect_config.connect_timeout_ms = 50U;
	assert(tr_channel_enable_client_reconnect(
		       client_channel, &reconnect_config) == TR_OK);

	memset(&race_probe, 0, sizeof(race_probe));
	race_probe.draining = client_channel;
	race_probe.peer = server_channel;
	assert(tr_reactor_call(
		       reactor, channel_begin_drain_and_race_open_on_owner,
		       &race_probe) == TR_OK);
	assert(race_probe.drain_ret == TR_OK);
	assert(race_probe.open_ret == TR_OK);
	assert(tr_channel_enable_client_reconnect(
		       client_channel, &reconnect_config) == TR_ERR_CLOSED);

	/* 在对端能观察到 GOAWAY 前，并发对端 OPEN 已被提交。
	 * 排空中的端点必须拒绝它，不分配本地流；
	 * 对端按 GOAWAY cutoff 直接退役其 OPENING 槽位。
	 */
	wait_channel_counter(&server_ctx, &server_ctx.stream_errors, 1U);
	assert(tr_channel_active_streams(client_channel) == 1U);
	pthread_mutex_lock(&client_ctx.lock);
	assert(client_ctx.opened == 1U);
	pthread_mutex_unlock(&client_ctx.lock);
	assert(tr_stream_close(race_probe.raced_stream) == TR_ERR_STALE);
	wait_channel_active_streams(server_channel, 1U);

	memset(&wait_probe, 0, sizeof(wait_probe));
	wait_probe.channel = client_channel;
	assert(tr_reactor_call(
		       reactor, channel_wait_drained_from_owner,
		       &wait_probe) == TR_OK);
	assert(wait_probe.wait_ret == TR_ERR_STATE);

	assert(tr_channel_wait_drained(client_channel, 0) == TR_AGAIN);
	assert(tr_channel_get_state(client_channel, &channel_state) == TR_OK);
	assert(channel_state == TR_CHANNEL_DRAINING);
	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &rejected) ==
	       TR_ERR_CLOSED);

	wait_channel_counter(&server_ctx, &server_ctx.channel_goaway, 2);
	assert(tr_stream_open(server_channel, TR_LANE_CONTROL, &rejected) ==
	       TR_ERR_CLOSED);

	/* Channel 排空期间，已有流仍可用。 */
	assert(tr_buffer_pool_init(&tx_pool, 1, 64) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 32, &payload) == TR_OK);
	memcpy(payload->data, "drain-still-flows", 17);
	payload->len = 17;
	assert(tr_stream_send(client_stream, payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);
	wait_for_pool_full(&tx_pool, 1);

	assert(tr_channel_begin_drain(server_channel) == TR_OK);
	/* 活跃流使条件变量谓词为假；限时等待证明阻塞路径，
	 * 不依赖调度器时序。
	 */
	assert(tr_channel_wait_drained(client_channel, 10U) == TR_AGAIN);

	/* 让等待者注册与两侧流关闭竞争。若关闭先发生，等待者观察到已排空谓词；
	 * 若注册先发生，最终流释放会广播。两种顺序都不得丢失通知。
	 */
	memset(&drain_wait, 0, sizeof(drain_wait));
	drain_wait.channel = client_channel;
	assert(pthread_mutex_init(&drain_wait.lock, NULL) == 0);
	assert(pthread_cond_init(&drain_wait.cond, NULL) == 0);
	assert(pthread_create(
		       &drain_wait_thread, NULL,
		       channel_wait_thread_main, &drain_wait) == 0);
	wait_channel_wait_probe(&drain_wait, &drain_wait.entered);

	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_stream_close(server_stream) == TR_OK);
	wait_channel_wait_probe(&drain_wait, &drain_wait.done);
	assert(pthread_join(drain_wait_thread, NULL) == 0);
	assert(drain_wait.ret == TR_OK);
	pthread_cond_destroy(&drain_wait.cond);
	pthread_mutex_destroy(&drain_wait.lock);
	assert(tr_channel_wait_drained(server_channel, 2000U) == TR_OK);
	assert(tr_channel_get_state(client_channel, &channel_state) == TR_OK);
	assert(channel_state == TR_CHANNEL_DRAINED);
	assert(tr_channel_active_streams(client_channel) == 0);
	assert(tr_channel_active_streams(server_channel) == 0);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

/* 在给定截止窗口等待 HELLO 真实离开发送队列；这只用于测试编排。 */
static void wait_channel_hello_tx(struct tr_conn_handle connection)
{
	unsigned i;
	for (i = 0; i < 5000U; ++i) {
		struct tr_connection_stats stats;
		assert(tr_reactor_get_connection_stats(connection, &stats) ==
		       TR_OK);
		if (stats.tx_frames >= 1U)
			return;
		{
			struct timespec pause = {0, 1000000L};
			nanosleep(&pause, NULL);
		}
	}
	assert(!"HELLO was not sent");
}

/* 所有者同步读取资源统计，不扩大客户端的控制发送项容量。 */
static void wait_channel_control_pool_empty(struct tr_reactor *reactor,
                                            uint32_t capacity)
{
	unsigned i;
	for (i = 0; i < 5000U; ++i) {
		struct tr_reactor_stats stats;
		assert(tr_reactor_get_stats(reactor, &stats) == TR_OK);
		assert(stats.control_tx_item_pool.capacity == capacity);
		if (stats.control_tx_item_pool.current == 0U)
			return;
		{
			struct timespec pause = {0, 1000000L};
			nanosleep(&pause, NULL);
		}
	}
	assert(!"CONTROL TX items were not reclaimed");
}

static void test_channel_drain_goaway_backpressure(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_reactor_config peer_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_reactor *peer_reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle prebarrier_stream;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	struct channel_drain_backpressure_probe probe;
	pthread_t owner_thread;
	int client_fd;
	int server_fd;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	memset(&probe, 0, sizeof(probe));
	assert(pthread_mutex_init(&probe.lock, NULL) == 0);
	assert(pthread_cond_init(&probe.cond, NULL) == 0);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	/* 被测客户端仍仅有两个命令槽位和一个 CONTROL 发送项。
	 * 对端使用另一 Reactor，使握手阶段不耗尽被测池。
	 */
	reactor_config.command_capacity = 2U;
	reactor_config.tx_item_capacity = 8U;
	reactor_config.control_tx_item_capacity = 1U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	/* 对端的两项只用于 HELLO 重放、ACK 和发送自己的 HELLO。 */
	peer_config = reactor_config;
	peer_config.control_tx_item_capacity = 2U;
	assert(tr_reactor_create(&peer_config, NULL, NULL, NULL,
	                         &peer_reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_start(peer_reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(peer_reactor, server_fd, &server_conn) ==
	       TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create_deferred(
	               &channel_config, client_conn, client_conn,
	               channel_test_on_data, channel_test_on_stream_event,
	               channel_test_on_channel_event, &client_ctx,
	               &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create_deferred(
	               &channel_config, server_conn, server_conn,
	               channel_test_on_data, channel_test_on_stream_event,
	               channel_test_on_channel_event, &server_ctx,
	               &server_channel) == TR_OK);
	/* 双端处理函数均已发布。先完成客户端 HELLO 并释放唯一项，
	 * 对端才重放提前到达的 HELLO 并发送自己的 HELLO 和 ACK。
	 */
	assert(tr_channel_start(client_channel) == TR_OK);
	wait_channel_hello_tx(client_conn);
	wait_channel_control_pool_empty(reactor, 1U);
	assert(tr_channel_start(server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_control_pool_empty(reactor, 1U);
	wait_channel_control_pool_empty(peer_reactor, 2U);

	probe.channel = client_channel;
	assert(pthread_create(
		       &owner_thread, NULL,
		       channel_drain_backpressure_thread, &probe) == 0);
	wait_channel_drain_probe(&probe, &probe.owner_entered);

	/* 所有者被阻塞。屏障前的 STREAM_OPEN 预留唯一普通 CONTROL 发送项；
	 * STREAM_OPEN + RESUME_RX 填满双槽命令环形队列。
	 * begin_drain() 仍必须成功，但 GOAWAY 不得越过已获准 OPEN。
	 */
	assert(tr_stream_open(
		       client_channel, TR_LANE_CONTROL,
		       &prebarrier_stream) == TR_OK);
	/* STREAM_OPEN 使用一个命令槽位与唯一普通 CONTROL 发送项。
	 * RESUME_RX 不使用发送项，在所有者仍阻塞时填满第二个命令槽位。
	 */
	assert(tr_reactor_resume_rx(client_conn) == TR_OK);

	pthread_mutex_lock(&probe.lock);
	probe.run_drain = 1;
	pthread_cond_broadcast(&probe.cond);
	pthread_mutex_unlock(&probe.lock);
	wait_channel_drain_probe(&probe, &probe.drain_done);
	assert(pthread_join(owner_thread, NULL) == 0);
	assert(probe.drain_ret == TR_OK);

	/* 命令序号屏障把屏障前的 OPEN 保留为 FIFO 前驱。
	 * 对端必须先观察 OPENED，再观察 GOAWAY，
	 * 且 GOAWAY 自身不需要第二次 begin_drain() 或轮询重试。
	 */
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1U);
	wait_channel_counter(&server_ctx, &server_ctx.channel_goaway, 2U);
	pthread_mutex_lock(&server_ctx.lock);
	assert(server_ctx.opened_event_seq != 0U);
	assert(server_ctx.goaway_event_seq != 0U);
	assert(server_ctx.opened_event_seq < server_ctx.goaway_event_seq);
	pthread_mutex_unlock(&server_ctx.lock);
	assert(tr_channel_wait_drained(client_channel, 0U) == TR_AGAIN);

	/* 不要让测试清理成为另一个准入要求：STREAM_CLOSE 有意保持为
	 * 普通 CONTROL 流量，此处可能遇到背压。排序不变量被证明后，
	 * 由反应器强制拆除负责剩余流与连接清理。
	 */
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_stop(peer_reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_reactor_destroy(peer_reactor);
	pthread_cond_destroy(&probe.cond);
	pthread_mutex_destroy(&probe.lock);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

struct split_reconnect_close_probe {
	struct tr_conn_handle control;
	struct tr_conn_handle bulk;
};

static int close_split_reconnect_pair_on_owner(void *arg)
{
	struct split_reconnect_close_probe *probe =
	        (struct split_reconnect_close_probe *)arg;
	int ret;

	ret = tr_reactor_close_on_owner(probe->control);
	if (ret != TR_OK)
		return ret;
	return tr_reactor_close_on_owner(probe->bulk);
}

static int accept_reconnect_connection(int listener)
{
	struct pollfd pfd;
	int fd = -1;
	unsigned i;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = listener;
	pfd.events = POLLIN;
	assert(poll(&pfd, 1, 5000) == 1);
	for (i = 0; i < 100U && fd < 0; ++i) {
		int ret = tr_tcp_accept(listener, &fd);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);
	}
	assert(fd >= 0);
	return fd;
}

/* 两条分离通道共享一个重连定时器与 Connector。在同一所有者窗口
 * 断开两个物理连接，仍必须先重连 CONTROL 再重连 BULK，
 * 而不是让某条通道永久断开。
 */
static void test_channel_automatic_reconnect_split_simultaneous(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_control;
	struct tr_conn_handle client_bulk;
	struct tr_conn_handle server_control;
	struct tr_conn_handle server_bulk;
	struct tr_conn_handle server_new_control;
	struct tr_conn_handle server_new_bulk;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	struct tr_channel_stats stats;
	struct split_reconnect_close_probe close_probe;
	enum tr_channel_lane_state lane_state;
	uint16_t control_port;
	uint16_t bulk_port;
	int control_listener = -1;
	int bulk_listener = -1;
	int client_control_fd = -1;
	int server_control_fd = -1;
	int client_bulk_fd = -1;
	int server_bulk_fd = -1;
	int new_server_control_fd = -1;
	int new_server_bulk_fd = -1;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair_keep_listener(&control_listener, &control_port,
	                            &client_control_fd, &server_control_fd);
	make_tcp_pair_keep_listener(&bulk_listener, &bulk_port, &client_bulk_fd,
	                            &server_bulk_fd);
	assert(control_port != bulk_port);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 32U;
	reactor_config.rx_buffer_count = 16U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_control_fd,
	                           &client_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_bulk_fd, &client_bulk) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_control_fd,
	                           &server_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_bulk_fd, &server_bulk) ==
	       TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SPLIT_CONNECTIONS;
	channel_config.max_streams = 8U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&channel_config, client_control, client_bulk,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &client_ctx,
	                         &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_control, server_bulk,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &server_ctx,
	                         &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = control_port;
	reconnect_config.bulk_port = bulk_port;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 40U;
	reconnect_config.connect_timeout_ms = 500U;
	assert(tr_channel_enable_client_reconnect(client_channel,
	                                          &reconnect_config) == TR_OK);

	/* 连续关闭两个旧客户端连接。重连延迟使首次重试开始前，
	 * 两个 DOWN 通知都可见。
	 */
	close_probe.control = client_control;
	close_probe.bulk = client_bulk;
	assert(tr_reactor_call(reactor, close_split_reconnect_pair_on_owner,
	                       &close_probe) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.channel_down, 2U);
	wait_channel_counter(&server_ctx, &server_ctx.channel_down, 2U);

	new_server_control_fd = accept_reconnect_connection(control_listener);
	new_server_bulk_fd = accept_reconnect_connection(bulk_listener);
	assert(tr_reactor_adopt_fd(reactor, new_server_control_fd,
	                           &server_new_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, new_server_bulk_fd,
	                           &server_new_bulk) == TR_OK);
	assert(tr_channel_replace_connection(server_channel, TR_LANE_CONTROL,
	                                     server_new_control) == TR_OK);
	assert(tr_channel_replace_connection(server_channel, TR_LANE_BULK,
	                                     server_new_bulk) == TR_OK);

	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);
	wait_channel_counter(&client_ctx, &client_ctx.channel_up, 4U);
	wait_channel_counter(&server_ctx, &server_ctx.channel_up, 4U);

	assert(tr_channel_get_lane_state(client_channel, TR_LANE_CONTROL,
	                                 &lane_state) == TR_OK);
	assert(lane_state == TR_CHANNEL_LANE_UP);
	assert(tr_channel_get_lane_state(client_channel, TR_LANE_BULK,
	                                 &lane_state) == TR_OK);
	assert(lane_state == TR_CHANNEL_LANE_UP);
	assert(tr_channel_get_stats(client_channel, &stats) == TR_OK);
	assert(stats.reconnect_attempts >= 2U);
	assert(stats.reconnect_successes >= 2U);

	/* 两条通道恢复后，拆除必须使共享定时器与 Connector 静止。 */
	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);
	tr_socket_close(&control_listener);
	tr_socket_close(&bulk_listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

/* 保持 CONTROL 不可达，而 BULK 监听器可用。固定 CONTROL 优先级
 * 会使 BULK 永久饥饿；共享 Connector 必须在每次尝试失败后，
 * 给另一条断开通道一个轮次。
 */
static void
test_channel_reconnect_one_lane_unreachable(enum tr_lane failing_lane,
                                            int both_unreachable)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_control, client_bulk;
	struct tr_conn_handle server_control, server_bulk;
	struct tr_conn_handle server_new;
	struct channel_test_ctx client_ctx, server_ctx;
	struct split_reconnect_close_probe close_probe;
	struct tr_channel_stats stats;
	enum tr_channel_lane_state state;
	uint16_t control_port, bulk_port;
	int control_listener = -1, bulk_listener = -1;
	int ccfd = -1, csfd = -1, cbfd = -1, bsfd = -1;
	int new_fd;
	enum tr_lane healthy_lane = failing_lane == TR_LANE_CONTROL
	                                    ? TR_LANE_BULK
	                                    : TR_LANE_CONTROL;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair_keep_listener(&control_listener, &control_port, &ccfd,
	                            &csfd);
	make_tcp_pair_keep_listener(&bulk_listener, &bulk_port, &cbfd, &bsfd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 32U;
	reactor_config.rx_buffer_count = 16U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, ccfd, &client_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, cbfd, &client_bulk) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, csfd, &server_control) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, bsfd, &server_bulk) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SPLIT_CONNECTIONS;
	channel_config.max_streams = 8U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&channel_config, client_control, client_bulk,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &client_ctx,
	                         &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_control, server_bulk,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &server_ctx,
	                         &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = control_port;
	reconnect_config.bulk_port = bulk_port;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 40U;
	reconnect_config.connect_timeout_ms = 500U;
	assert(tr_channel_enable_client_reconnect(client_channel,
	                                          &reconnect_config) == TR_OK);

	/* 保持一个端口不可达，而另一条通道可恢复。 */
	if (both_unreachable || failing_lane == TR_LANE_CONTROL)
		tr_socket_close(&control_listener);
	if (both_unreachable || failing_lane == TR_LANE_BULK)
		tr_socket_close(&bulk_listener);
	close_probe.control = client_control;
	close_probe.bulk = client_bulk;
	assert(tr_reactor_call(reactor, close_split_reconnect_pair_on_owner,
	                       &close_probe) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.channel_down, 2U);
	if (both_unreachable) {
		unsigned i;

		/* 没有监听器可达。两个重试预算都必须持续取得有界进展，
		 * 不引入忙循环或工作线程。
		 */
		for (i = 0; i < 5000U; ++i) {
			assert(tr_channel_get_stats(client_channel, &stats) ==
			       TR_OK);
			if (stats.reconnect_attempts >= 6U)
				break;
			{
				const struct timespec pause = {0, 1000000L};
				(void)nanosleep(&pause, NULL);
			}
		}
		assert(stats.reconnect_attempts >= 6U);
		assert(stats.reconnect_successes == 0U);
		assert(tr_channel_get_lane_state(client_channel,
		                                 TR_LANE_CONTROL,
		                                 &state) == TR_OK);
		assert(state != TR_CHANNEL_LANE_UP);
		assert(tr_channel_get_lane_state(client_channel, TR_LANE_BULK,
		                                 &state) == TR_OK);
		assert(state != TR_CHANNEL_LANE_UP);
	} else {
		new_fd = accept_reconnect_connection(
		        healthy_lane == TR_LANE_BULK ? bulk_listener
		                                     : control_listener);
		assert(tr_reactor_adopt_fd(reactor, new_fd, &server_new) ==
		       TR_OK);
		assert(tr_channel_replace_connection(server_channel,
		                                     healthy_lane,
		                                     server_new) == TR_OK);
		wait_channel_lane_up(client_channel, healthy_lane);
		wait_channel_lane_up(server_channel, healthy_lane);
		assert(tr_channel_get_lane_state(client_channel, failing_lane,
		                                 &state) == TR_OK);
		/* 不可达通道可能为 DOWN，或正处于 RECONNECTING。 */
		assert(state != TR_CHANNEL_LANE_UP);
		assert(tr_channel_get_stats(client_channel, &stats) == TR_OK);
		/* 健康通道可能在首次轮次就连接；
		 * 它到达 UP 前，另一侧失败不必已完成。
		 */
		assert(stats.reconnect_attempts >= 1U);
		assert(stats.reconnect_successes >= 1U);
	}

	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);
	tr_socket_close(&control_listener);
	tr_socket_close(&bulk_listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

/* 所有者定时器设置失败后没有未来唤醒。验证重连被显式禁用、
 * 发布终止错误，并保留回调源句柄直到正常禁用屏障成功。
 */
static void test_channel_reconnect_timer_arm_failure(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn, server_conn;
	struct channel_test_ctx client_ctx, server_ctx;
	uint16_t port;
	int listener = -1, client_fd = -1, server_fd = -1;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair_keep_listener(&listener, &port, &client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 32U;
	reactor_config.rx_buffer_count = 16U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &client_ctx,
	                         &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &server_ctx,
	                         &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = port;
	reconnect_config.initial_delay_ms = 10U;
	reconnect_config.max_delay_ms = 40U;
	reconnect_config.connect_timeout_ms = 500U;
	assert(tr_channel_enable_client_reconnect(client_channel,
	                                          &reconnect_config) == TR_OK);

	atomic_store(&fail_reconnect_timer_arm, 1U);
	assert(tr_reactor_close(client_conn) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.reconnect_failures, 1U);
	assert(client_ctx.reconnect_failure_status == TR_ERR_SYS);
	assert(client_ctx.reconnect_failure_event ==
	       TR_CHANNEL_EVENT_CONTROL_RECONNECT_FAILED);
	assert(atomic_load(&fail_reconnect_timer_arm) == 0U);
	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);
	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);

	/* 在断开的 Channel 上启用重连会立即设置重试定时。
	 * 第二次注入失败必须返回给调用者并回滚。
	 */
	atomic_store(&fail_reconnect_timer_arm, 1U);
	assert(tr_channel_enable_client_reconnect(
	               client_channel, &reconnect_config) == TR_ERR_SYS);
	wait_channel_counter(&client_ctx, &client_ctx.reconnect_failures, 2U);
	assert(atomic_load(&fail_reconnect_timer_arm) == 0U);
	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);

	tr_socket_close(&listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void test_channel_automatic_reconnect_shared(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_reconnect_config reconnect_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_conn_handle server_new_conn;
	struct tr_stream_handle old_stream;
	struct tr_stream_handle new_stream;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *payload;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
	enum tr_channel_lane_state lane_state;
	struct tr_stream_flow_state old_flow;
	struct pollfd pfd;
	uint16_t port;
	int listener = -1;
	int client_fd = -1;
	int server_fd = -1;
	int new_server_fd = -1;
	unsigned i;
	unsigned nodelay_before;

	init_channel_test_ctx(&client_ctx);
	init_channel_test_ctx(&server_ctx);
	make_tcp_pair_keep_listener(&listener, &port, &client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 32;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 16;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 4096;
	channel_config.window_update_threshold_bytes = 1024;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &client_ctx,
	                         &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
	                         channel_test_on_data,
	                         channel_test_on_stream_event,
	                         channel_test_on_channel_event, &server_ctx,
	                         &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	memset(&reconnect_config, 0, sizeof(reconnect_config));
	reconnect_config.ipv4_address = "127.0.0.1";
	reconnect_config.control_port = port;
	reconnect_config.initial_delay_ms = 10;
	reconnect_config.max_delay_ms = 40;
	reconnect_config.connect_timeout_ms = 500;
	assert(tr_channel_set_reconnect_tcp_nodelay(client_channel, 1) ==
	       TR_OK);
	assert(tr_channel_enable_client_reconnect(client_channel,
	                                          &reconnect_config) == TR_OK);
	/* 重连套接字策略已由反应器所有者串行化；启用发布后不允许
	 * 应用线程再修改本次重连会话的套接字策略。
	 */
	assert(tr_channel_set_reconnect_tcp_nodelay(client_channel, 0) ==
	       TR_ERR_STATE);
	nodelay_before = tcp_nodelay_probe_read();

	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &old_stream) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 1);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 1);

	assert(tr_reactor_close(client_conn) == TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.channel_down, 2);
	wait_channel_counter(&server_ctx, &server_ctx.channel_down, 2);
	wait_channel_counter(&client_ctx, &client_ctx.stream_errors, 1);
	wait_channel_counter(&server_ctx, &server_ctx.stream_errors, 1);

	assert(tr_stream_get_flow_state(old_stream, &old_flow) == TR_ERR_STALE);

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = listener;
	pfd.events = POLLIN;
	assert(poll(&pfd, 1, 5000) == 1);
	for (i = 0; i < 100 && new_server_fd < 0; ++i) {
		int ret = tr_tcp_accept(listener, &new_server_fd);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);
	}
	assert(new_server_fd >= 0);

	assert(tr_reactor_adopt_fd(reactor, new_server_fd, &server_new_conn) ==
	       TR_OK);
	assert(tr_channel_replace_connection(server_channel, TR_LANE_CONTROL,
	                                     server_new_conn) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	assert(tcp_nodelay_probe_read() == nodelay_before + 1U);

	wait_channel_counter(&client_ctx, &client_ctx.channel_up, 2);
	wait_channel_counter(&server_ctx, &server_ctx.channel_up, 2);
	assert(tr_channel_get_lane_state(client_channel, TR_LANE_CONTROL,
	                                 &lane_state) == TR_OK);
	assert(lane_state == TR_CHANNEL_LANE_UP);
	assert(tr_channel_get_lane_state(client_channel, TR_LANE_BULK,
	                                 &lane_state) == TR_OK);
	assert(lane_state == TR_CHANNEL_LANE_UP);

	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &new_stream) ==
	       TR_OK);
	wait_channel_counter(&client_ctx, &client_ctx.opened, 2);
	wait_channel_counter(&server_ctx, &server_ctx.opened, 2);

	assert(tr_buffer_pool_init(&tx_pool, 1, 128) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 32, &payload) == TR_OK);
	memcpy(payload->data, "after-reconnect", 15);
	payload->len = 15;
	assert(tr_stream_send(new_stream, payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);
	wait_for_pool_full(&tx_pool, 1);

	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);
	/* 全部事件源解绑后，终止重连拆除是幂等的。 */
	assert(tr_channel_disable_client_reconnect(client_channel) == TR_OK);
	tr_socket_close(&listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void wait_channel_lane_down(struct tr_channel *channel,
                                   enum tr_lane lane)
{
	unsigned i;
	for (i = 0; i < 5000U; ++i) {
		enum tr_channel_lane_state state;
		assert(tr_channel_get_lane_state(channel, lane, &state) ==
		       TR_OK);
		if (state == TR_CHANNEL_LANE_DOWN)
			return;
		{
			struct timespec pause_time = {0, 1000000L};
			nanosleep(&pause_time, NULL);
		}
	}
	assert(!"channel lane did not go down");
}

static void test_channel_version_negotiation_failure(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle stream;
	struct tr_buffer_pool pool;
	struct tr_buffer *hello;
	int client_fd;
	int server_fd;

	make_tcp_pair(&client_fd, &server_fd);
	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 8;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.min_protocol_version = 1;
	channel_config.max_protocol_version = 1;
	/* 本地配置不再启用旧 GOAWAY 语义；版本 1 必须在发布处理函数前拒绝。 */
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
	                         NULL, NULL, NULL, NULL,
	                         &client_channel) == TR_ERR_INVALID);
	assert(client_channel == NULL);

	channel_config.min_protocol_version = TR_CHANNEL_PROTOCOL_VERSION;
	channel_config.max_protocol_version = TR_CHANNEL_PROTOCOL_VERSION;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
	                         NULL, NULL, NULL, NULL,
	                         &client_channel) == TR_OK);

	/* 原始对端仍用合法 envelope 1 发送 HELLO 1，协商必须失败。 */
	assert(tr_buffer_pool_init(&pool, 1, 32) == TR_OK);
	assert(tr_buffer_acquire(&pool, 32, &hello) == TR_OK);
	memset(hello->data, 0, 32);
	tr_put_le16(hello->data, 1);
	tr_put_le16(hello->data + 2, 1);
	tr_put_le32(hello->data + 4,
	            TR_CHANNEL_LANE_MASK_CONTROL | TR_CHANNEL_LANE_MASK_BULK);
	tr_put_le32(hello->data + 8, 4096);
	tr_put_le32(hello->data + 12, 4096);
	hello->len = 32;
	assert(tr_reactor_send(server_conn, TR_FRAME_HELLO, 0, 0, 0, hello) ==
	       TR_OK);

	wait_channel_lane_down(client_channel, TR_LANE_CONTROL);
	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &stream) ==
	       TR_ERR_CLOSED);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	tr_reactor_destroy(reactor);
	assert(tr_buffer_pool_free_count(&pool) == 1);
	tr_buffer_pool_destroy(&pool);
}

static void test_channel_keepalive_and_diagnostics(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_channel_keepalive_config keepalive;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_channel_stats stats;
	enum tr_channel_lane_state lane_state = TR_CHANNEL_LANE_UP;
	int client_fd;
	int server_fd;
	unsigned i;

	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
	reactor_config.tx_item_capacity = 16;
	reactor_config.control_tx_item_capacity = 16;
	reactor_config.rx_buffer_count = 8;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8;
	channel_config.initial_window_bytes = 4096;
	channel_config.window_update_threshold_bytes = 1024;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	memset(&keepalive, 0, sizeof(keepalive));
	keepalive.interval_ms = 20U;
	keepalive.timeout_ms = 100U;
	assert(tr_channel_enable_keepalive(client_channel, &keepalive) ==
	       TR_OK);

	for (i = 0; i < 2000U; ++i) {
		assert(tr_channel_get_stats(client_channel, &stats) == TR_OK);
		if (stats.keepalive_pongs_received >= 1U)
			break;
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 1000000L;
			nanosleep(&pause_time, NULL);
		}
	}
	assert(stats.keepalive_pings_sent >= 1U);
	assert(stats.keepalive_pongs_received >= 1U);
	assert(stats.control_last_rtt_ns > 0U);
	assert(tr_channel_disable_keepalive(client_channel) == TR_OK);

	/* 抑制对端生成 PONG，以测试超时路径。 */
	assert(tr_reactor_set_handler(server_conn, NULL, NULL, NULL) == TR_OK);
	keepalive.timeout_ms = 40U;
	assert(tr_channel_enable_keepalive(client_channel, &keepalive) ==
	       TR_OK);

	for (i = 0; i < 2000U; ++i) {
		assert(tr_channel_get_lane_state(client_channel,
						 TR_LANE_CONTROL,
						 &lane_state) == TR_OK);
		if (lane_state == TR_CHANNEL_LANE_DOWN)
			break;
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 1000000L;
			nanosleep(&pause_time, NULL);
		}
	}
	assert(lane_state == TR_CHANNEL_LANE_DOWN);
	assert(tr_channel_get_stats(client_channel, &stats) == TR_OK);
	assert(stats.keepalive_pings_sent >= 1U);
	assert(stats.keepalive_timeouts >= 1U);
	assert(stats.control_connection.tx_frames >= 1U);
	assert(stats.control_connection.state == TR_CONN_FREE);

	assert(tr_channel_disable_keepalive(client_channel) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_channel_destroy(client_channel) == TR_OK);
	assert(tr_channel_destroy(server_channel) == TR_OK);
	tr_reactor_destroy(reactor);
}

static void test_client_server_facade_nodelay_policy(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_rpc_method_desc method;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct timespec pause_time = {0, 1000000L};
	uint16_t port = 0;
	unsigned before;
	int ret;

	memset(&method, 0, sizeof(method));
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_RPC_LANE_CONTROL;
	method.max_request_bytes = 1U;
	method.max_response_bytes = 1U;

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.tcp_nodelay = TR_TCP_NODELAY_DISABLED;
	assert(tr_test_server_v2_create(&server_config, &server) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	client_config.tcp_nodelay = TR_TCP_NODELAY_DISABLED;
	assert(tr_client_create(&client_config, &method, 1U, &client) == TR_OK);

	before = tcp_nodelay_probe_read();
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_wait_ready(client, 5000U) == TR_OK);
	assert(tcp_nodelay_probe_read() == before);

	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;
	for (unsigned attempt = 0; attempt < 5000U; ++attempt) {
		ret = tr_server_destroy(server);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);
		assert(nanosleep(&pause_time, NULL) == 0);
	}
	assert(ret == TR_OK);
	server = NULL;

	tr_client_config_init(&client_config);
	client_config.tcp_nodelay = (enum tr_tcp_nodelay_policy)99;
	assert(tr_client_create(&client_config, &method, 1U, &client) ==
	       TR_ERR_INVALID);
	assert(client == NULL);

	tr_server_config_init(&server_config);
	server_config.tcp_nodelay = (enum tr_tcp_nodelay_policy)99;
	assert(tr_test_server_v2_create(&server_config, &server) == TR_ERR_INVALID);
	assert(server == NULL);
}

struct cleanup_order_probe {
	unsigned id;
	unsigned *order;
	unsigned *count;
};

static void cleanup_order_probe_run(struct cleanup_order_probe *probe)
{
	probe->order[(*probe->count)++] = probe->id;
}

static int cleanup_buffer_early_return(struct tr_buffer_pool *pool)
{
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
	int ret;

	ret = tr_buffer_acquire(pool, 1U, &buffer);
	if (ret != TR_OK)
		return ret;
	buffer->len = 1U;
	return TR_OK;
}

static struct tr_buffer *
cleanup_buffer_transfer(struct tr_buffer_pool *pool)
{
	struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;

	if (tr_buffer_acquire(pool, 1U, &buffer) != TR_OK)
		return NULL;
	return tr_buffer_take(&buffer);
}

static int cleanup_fd_early_return(int *observed_fd)
{
	int fd TR_AUTO(tr_fd_cleanup) = dup(STDOUT_FILENO);

	if (fd < 0)
		return TR_ERR_SYS;
	*observed_fd = fd;
	return TR_OK;
}

static int cleanup_fd_transfer(void)
{
	int fd TR_AUTO(tr_fd_cleanup) = dup(STDOUT_FILENO);

	if (fd < 0)
		return -1;
	return tr_fd_take(&fd);
}

static int cleanup_mutex_early_return(pthread_mutex_t *mutex)
{
	struct tr_mutex_guard guard TR_AUTO(tr_mutex_guard_cleanup) = { 0 };

	if (tr_mutex_guard_acquire(&guard, mutex) != 0)
		return TR_ERR_SYS;
	return TR_OK;
}

static void test_refcount_semantics(void)
{
	struct tr_refcount ref;

	assert(tr_refcount_init(&ref, 1U) == TR_OK);
	assert(tr_refcount_read(&ref) == 1U);

	assert(tr_refcount_get(&ref) == TR_OK);
	assert(tr_refcount_read(&ref) == 2U);
	assert(tr_refcount_get_unless_zero(&ref) == 1);
	assert(tr_refcount_read(&ref) == 3U);

	assert(tr_refcount_put(&ref) == 0);
	assert(tr_refcount_read(&ref) == 2U);
	assert(tr_refcount_put(&ref) == 0);
	assert(tr_refcount_read(&ref) == 1U);
	assert(tr_refcount_put(&ref) == 1);
	assert(tr_refcount_read(&ref) == 0U);

	/* 死亡对象不能复活，也不能递减到零以下。 */
	assert(tr_refcount_get(&ref) == TR_ERR_STATE);
	assert(tr_refcount_get_unless_zero(&ref) == 0);
	assert(tr_refcount_put(&ref) == TR_ERR_STATE);

	/* 无效与接近饱和的初始状态须显式表达。 */
	assert(tr_refcount_init(&ref, 0U) == TR_ERR_INVALID);
	assert(tr_refcount_init(&ref, UINT32_MAX) == TR_ERR_INVALID);
	assert(tr_refcount_init(&ref, UINT32_MAX - 1U) == TR_OK);
	assert(tr_refcount_get(&ref) == TR_ERR_STATE);
	assert(tr_refcount_get_unless_zero(&ref) == TR_ERR_STATE);
	assert(tr_refcount_put(&ref) == 0);
}

static void test_scope_cleanup_ownership(void)
{
	struct tr_buffer_pool pool;
	struct tr_buffer *buffer;
	unsigned order[3] = { 0U, 0U, 0U };
	unsigned count = 0U;
	int observed_fd = -1;
	int owned_fd;

	assert(tr_buffer_pool_init(&pool, 2U, 64U) == TR_OK);
	assert(tr_buffer_pool_free_count(&pool) == 2U);
	assert(cleanup_buffer_early_return(&pool) == TR_OK);
	assert(tr_buffer_pool_free_count(&pool) == 2U);

	buffer = cleanup_buffer_transfer(&pool);
	assert(buffer != NULL);
	assert(tr_buffer_pool_free_count(&pool) == 1U);
	tr_buffer_release(buffer);
	assert(tr_buffer_pool_free_count(&pool) == 2U);
	tr_buffer_pool_destroy(&pool);

	assert(cleanup_fd_early_return(&observed_fd) == TR_OK);
	errno = 0;
	assert(fcntl(observed_fd, F_GETFD) == -1);
	assert(errno == EBADF);

	owned_fd = cleanup_fd_transfer();
	assert(owned_fd >= 0);
	assert(fcntl(owned_fd, F_GETFD) >= 0);
	tr_socket_close(&owned_fd);
	assert(owned_fd == -1);

	/* 作用域清理按后进先出顺序执行。因此资源声明必须遵循依赖顺序：
	 * 被依赖资源在前，依赖方在后。
	 */
	{
		struct cleanup_order_probe first
			TR_AUTO(cleanup_order_probe_run) = { 1U, order, &count };
		struct cleanup_order_probe second
			TR_AUTO(cleanup_order_probe_run) = { 2U, order, &count };
		struct cleanup_order_probe third
			TR_AUTO(cleanup_order_probe_run) = { 3U, order, &count };
		(void)first;
		(void)second;
		(void)third;
	}
	assert(count == 3U);
	assert(order[0] == 3U);
	assert(order[1] == 2U);
	assert(order[2] == 1U);

	{
		pthread_mutex_t mutex;
		assert(pthread_mutex_init(&mutex, NULL) == 0);
		assert(cleanup_mutex_early_return(&mutex) == TR_OK);
		assert(pthread_mutex_trylock(&mutex) == 0);
		assert(pthread_mutex_unlock(&mutex) == 0);
		pthread_mutex_destroy(&mutex);
	}
}

int main(void)
{
	test_refcount_semantics();
	test_scope_cleanup_ownership();
	test_endian();
	test_wire_roundtrip();
	test_header_corruption();
	test_parser_one_byte_fragments();
	test_parser_payload_crc_failure();
	test_parser_pool_backpressure();
	test_payload_limit();
	test_zero_payload_control_frame();
	test_invalid_flags_and_reserved();
	test_reactor_local_timer_loop();
	test_reactor_tcp_roundtrip();
	test_reactor_handler_update_is_owner_serialized();
	test_channel_handler_publication_is_owner_serialized();
	test_reactor_rx_pool_backpressure();
	test_channel_split_create_tx_exhaustion_rollback();
	test_channel_failed_hello_start_is_sticky();
	test_channel_hello_contention_is_bounded();
	test_channel_deferred_hello_gate();
	test_channel_stream_id_index_collision_delete();
	test_channel_stream_slot_reuse();
	test_channel_stream_flow_control();
	test_channel_message_fragmentation_reassembly();
	test_channel_split_lane_isolation();
	test_channel_graceful_drain();
	test_channel_drain_goaway_backpressure();
	test_channel_automatic_reconnect_shared();
	test_channel_reconnect_timer_arm_failure();
	test_channel_automatic_reconnect_split_simultaneous();
	test_channel_reconnect_one_lane_unreachable(TR_LANE_CONTROL, 0);
	test_channel_reconnect_one_lane_unreachable(TR_LANE_BULK, 0);
	test_channel_reconnect_one_lane_unreachable(TR_LANE_CONTROL, 1);
	test_channel_version_negotiation_failure();
	test_channel_keepalive_and_diagnostics();
	test_client_server_facade_nodelay_policy();

	puts("all transport tests passed");
	return 0;
}
