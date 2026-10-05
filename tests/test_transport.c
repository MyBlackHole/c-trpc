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
#include "tr/rpc_codec.h"
#include "../src/rpc/rpc_wire.h"
#include "../src/transport/channel/channel.h"
#include "tr/client.h"
#include "tr/server.h"
#include "../src/io/socket.h"
#include "tr/status.h"
#include "../src/transport/protocol/wire.h"
#include "../src/transport/channel/channel_internal.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/facade_tuning_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/rpc/rpc_internal.h"
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

	/* Feed only the second header. The parser then needs a payload buffer. */
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

	/*
	 * 旧 callback 尚未返回时，SET_HANDLER command 只能排队，
	 * 不能从 producer thread 直接修改 handler。
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
	/* 完全 stopped 后允许 teardown-only direct publication。 */
	assert(tr_channel_set_handler(
		       channel, NULL, NULL, NULL, NULL) == TR_OK);
	tr_channel_destroy(channel);
	tr_reactor_destroy(reactor);
	assert(close(fds[1]) == 0);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_method_owner_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int owner_entered;
	int owner_release;
	int registrar_done;
	int registrar_ret;
};

struct rpc_method_register_arg {
	struct tr_rpc_endpoint *endpoint;
	struct tr_channel *channel;
	struct tr_rpc_method_desc method;
	struct rpc_method_owner_ctx *ctx;
};

static int rpc_method_owner_gate(void *arg)
{
	struct rpc_method_owner_ctx *ctx =
		(struct rpc_method_owner_ctx *)arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->owner_entered = 1;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->owner_release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static void *rpc_method_gate_thread(void *arg)
{
	struct rpc_method_register_arg *register_arg =
		(struct rpc_method_register_arg *)arg;

	assert(tr_reactor_call(
		       tr_channel_reactor(register_arg->channel),
		       rpc_method_owner_gate, register_arg->ctx) == TR_OK);
	return NULL;
}

static void *rpc_method_register_thread(void *arg)
{
	struct rpc_method_register_arg *register_arg =
		(struct rpc_method_register_arg *)arg;
	struct rpc_method_owner_ctx *ctx = register_arg->ctx;
	int ret;

	ret = tr_rpc_register_method(
		register_arg->endpoint, &register_arg->method, NULL, NULL);
	pthread_mutex_lock(&ctx->lock);
	ctx->registrar_ret = ret;
	ctx->registrar_done = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return NULL;
}

static void test_rpc_method_registration_is_owner_serialized(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *channel = NULL;
	struct tr_rpc_endpoint *endpoint = NULL;
	struct tr_conn_handle connection;
	struct tr_buffer_pool rpc_pool;
	struct rpc_method_owner_ctx ctx;
	struct rpc_method_register_arg arg;
	struct tr_rpc_endpoint_stats stats;
	struct timespec deadline;
	struct timespec pause_time = { 0, 100000000L };
	pthread_t gate;
	pthread_t registrar;
	int fds[2];
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	memset(&arg, 0, sizeof(arg));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 2U;
	reactor_config.command_capacity = 32U;
	reactor_config.tx_item_capacity = 8U;
	reactor_config.control_tx_item_capacity = 8U;
	reactor_config.rx_buffer_count = 8U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, fds[0], &connection) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create_deferred(
		       &channel_config, connection, connection,
		       NULL, NULL, NULL, NULL, &channel) == TR_OK);

	assert(tr_buffer_pool_init(&rpc_pool, 8U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 4U;
	rpc_config.max_calls = 4U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create(channel, &rpc_config, &endpoint) == TR_OK);

	arg.endpoint = endpoint;
	arg.channel = channel;
	arg.ctx = &ctx;
	arg.method.service_id = 71U;
	arg.method.method_id = 1U;
	arg.method.request_cardinality = TR_RPC_ONE;
	arg.method.response_cardinality = TR_RPC_ONE;
	arg.method.request_codec_id = TR_RPC_CODEC_RAW;
	arg.method.response_codec_id = TR_RPC_CODEC_RAW;
	arg.method.lane = TR_LANE_CONTROL;
	arg.method.max_request_bytes = 128U;
	arg.method.max_response_bytes = 128U;

	assert(pthread_create(
		       &gate, NULL, rpc_method_gate_thread, &arg) == 0);
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx.lock);
	while (!ctx.owner_entered && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	pthread_mutex_unlock(&ctx.lock);

	assert(pthread_create(
		       &registrar, NULL, rpc_method_register_thread, &arg) == 0);
	(void)nanosleep(&pause_time, NULL);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.registrar_done == 0);
	ctx.owner_release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	assert(pthread_join(gate, NULL) == 0);
	assert(pthread_join(registrar, NULL) == 0);
	assert(ctx.registrar_ret == TR_OK);
	memset(&stats, 0, sizeof(stats));
	assert(tr_rpc_endpoint_get_stats(endpoint, &stats) == TR_OK);
	assert(stats.registered_methods == 1U);

	tr_rpc_endpoint_destroy(endpoint);
	tr_channel_destroy(channel);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
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
		 event == TR_CHANNEL_EVENT_BULK_GOAWAY)
		ctx->channel_goaway++;
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
	enum tr_channel_lane_state state = TR_CHANNEL_LANE_DOWN;
	unsigned i;

	for (i = 0; i < 5000; ++i) {
		assert(tr_channel_get_lane_state(channel, lane, &state) ==
		       TR_OK);
		if (state == TR_CHANNEL_LANE_UP)
			return;
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 1000000L;
			nanosleep(&pause_time, NULL);
		}
	}
	assert(state == TR_CHANNEL_LANE_UP);
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

static void test_channel_deferred_hello_gate(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle server_conn;
	struct channel_test_ctx server_ctx;
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

	/* Peer races HELLO ahead of Server RPC/service binding. */
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

	/*
	 * Receiving peer HELLO is not service readiness. Before start(), the
	 * deferred Channel must not ACK it or publish lane UP.
	 */
	assert(tr_channel_get_lane_state(server_channel, TR_LANE_CONTROL,
					 &lane_state) == TR_OK);
	assert(lane_state != TR_CHANNEL_LANE_UP);
	assert(tr_channel_get_lane_state(server_channel, TR_LANE_BULK,
					 &lane_state) == TR_OK);
	assert(lane_state != TR_CHANNEL_LANE_UP);

	assert(tr_channel_set_handler(server_channel, channel_test_on_data,
				      channel_test_on_stream_event,
				      channel_test_on_channel_event,
				      &server_ctx) == TR_OK);
	assert(tr_channel_start(server_channel) == TR_OK);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	/* Once service-ready, the same connection may proceed to Stream DATA. */
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
	tr_channel_destroy(server_channel);
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

	/*
	 * max_streams=3 creates an 8-entry index. These three valid Client Stream
	 * ids intentionally share one initial bucket, forcing a linear-probe
	 * cluster and exercising backward-shift deletion.
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

	/*
	 * Close local half first, then peer half. The middle hash entry disappears
	 * while stream_id=43 remains farther down the same probe cluster.
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
	tr_channel_destroy(server_channel);
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

	/* The single free-list entry is exhausted until this Stream is retired. */
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
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
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
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
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

	/* All six transport fragments were reassembled into one logical message. */
	assert(tr_buffer_pool_free_count(&reassembly_pool) == 1);
	assert(tr_stream_release_payload(server_stream, payload) == TR_OK);
	assert(tr_buffer_pool_free_count(&reassembly_pool) == 2);

	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
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

	/* Losing BULK must not make the CONTROL lane unusable. */
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

	/* BULK can be replaced independently without disturbing CONTROL. */
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
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	wait_for_pool_full(&tx_pool, 1);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

struct rpc_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned server_calls;
	unsigned client_results;
	unsigned client_pre;
	unsigned server_pre;
	unsigned server_post;
	unsigned client_post;
	int client_status;
	uint8_t request[64];
	uint32_t request_len;
	uint8_t response[64];
	uint32_t response_len;
};

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

	/*
	 * Both operations execute in one owner turn. The peer cannot receive the
	 * GOAWAY until this callback returns, so its STREAM_OPEN deterministically
	 * represents an open that raced the drain barrier.
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

	/*
	 * drain 与 reconnect disable 现在是同一个 Reactor owner transaction。
	 * 先启用 reconnect，再进入 drain；drain 返回后任何重新 enable 都必须
	 * 看到 local_draining barrier。
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

	/*
	 * The raced peer OPEN was already submitted before peer GOAWAY could be
	 * observed. The draining endpoint must reject it without allocating a local
	 * Stream; the peer receives an ordinary remote-close for its local slot.
	 */
	wait_channel_counter(&server_ctx, &server_ctx.remote_closed, 1U);
	assert(tr_channel_active_streams(client_channel) == 1U);
	pthread_mutex_lock(&client_ctx.lock);
	assert(client_ctx.opened == 1U);
	pthread_mutex_unlock(&client_ctx.lock);
	assert(tr_stream_close(race_probe.raced_stream) == TR_OK);
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

	/* Existing streams remain usable while the Channel drains. */
	assert(tr_buffer_pool_init(&tx_pool, 1, 64) == TR_OK);
	assert(tr_buffer_acquire(&tx_pool, 32, &payload) == TR_OK);
	memcpy(payload->data, "drain-still-flows", 17);
	payload->len = 17;
	assert(tr_stream_send(client_stream, payload) == TR_OK);
	wait_channel_counter(&server_ctx, &server_ctx.received, 1);
	wait_for_pool_full(&tx_pool, 1);

	assert(tr_channel_begin_drain(server_channel) == TR_OK);
	assert(tr_stream_close(client_stream) == TR_OK);
	assert(tr_stream_close(server_stream) == TR_OK);
	assert(tr_channel_wait_drained(client_channel, 2000) == TR_OK);
	assert(tr_channel_wait_drained(server_channel, 2000) == TR_OK);
	assert(tr_channel_get_state(client_channel, &channel_state) == TR_OK);
	assert(channel_state == TR_CHANNEL_DRAINED);
	assert(tr_channel_active_streams(client_channel) == 0);
	assert(tr_channel_active_streams(server_channel) == 0);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static int rpc_test_unary_handler(struct tr_rpc_call_handle call,
				  const struct tr_rpc_bytes *request,
				  struct tr_rpc_unary_response *response,
				  void *arg)
{
	struct rpc_test_ctx *ctx = (struct rpc_test_ctx *)arg;
	static const uint8_t reply[] = "pong-from-server";
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	assert(request->len <= sizeof(ctx->request));
	memcpy(ctx->request, request->data, request->len);
	ctx->request_len = request->len;
	ctx->server_calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void rpc_test_result(struct tr_rpc_call_handle call, int status,
			    const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_test_ctx *ctx = (struct rpc_test_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->client_status = status;
	if (response) {
		assert(response->len <= sizeof(ctx->response));
		memcpy(ctx->response, response->data, response->len);
		ctx->response_len = response->len;
	}
	ctx->client_results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_counter(struct rpc_test_ctx *ctx, unsigned *value,
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

struct rpc_method_index_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned handler_calls[3];
	unsigned client_results;
};

struct rpc_method_index_handler_arg {
	struct rpc_method_index_ctx *ctx;
	unsigned index;
	uint8_t reply;
};

struct rpc_method_index_result_arg {
	struct rpc_method_index_ctx *ctx;
	uint8_t expected_reply;
};

static int rpc_method_index_handler(struct tr_rpc_call_handle call,
				    const struct tr_rpc_bytes *request,
				    struct tr_rpc_unary_response *response,
				    void *arg)
{
	struct rpc_method_index_handler_arg *handler_arg =
		(struct rpc_method_index_handler_arg *)arg;
	struct rpc_method_index_ctx *ctx = handler_arg->ctx;
	(void)call;

	assert(request && request->len == 1U);
	pthread_mutex_lock(&ctx->lock);
	ctx->handler_calls[handler_arg->index]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = &handler_arg->reply;
	response->message.len = 1U;
	return TR_OK;
}

static void rpc_method_index_result(struct tr_rpc_call_handle call, int status,
				    const struct tr_rpc_bytes *response,
				    void *arg)
{
	struct rpc_method_index_result_arg *result_arg =
		(struct rpc_method_index_result_arg *)arg;
	struct rpc_method_index_ctx *ctx = result_arg->ctx;
	(void)call;

	assert(status == TR_RPC_STATUS_OK);
	assert(response && response->len == 1U);
	assert(response->data[0] == result_arg->expected_reply);

	pthread_mutex_lock(&ctx->lock);
	ctx->client_results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_method_index(struct rpc_method_index_ctx *ctx,
				  unsigned index, unsigned results)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&ctx->lock);
	while ((ctx->handler_calls[index] == 0U ||
		ctx->client_results < results) &&
	       ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->handler_calls[index] != 0U);
	assert(ctx->client_results >= results);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_rpc_method_index_collisions(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc methods[3];
	struct tr_rpc_method_desc extra;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes request;
	struct tr_rpc_endpoint_stats stats;
	struct rpc_method_index_ctx ctx;
	struct rpc_method_index_handler_arg handler_args[3];
	struct rpc_method_index_result_arg result_args[3];
	const uint32_t method_ids[3] = { 1U, 9U, 15U };
	uint8_t request_byte = 0x5aU;
	unsigned i;
	int client_fd;
	int server_fd;

	/* max_methods=3 -> 8 buckets; force one linear-probe cluster. */
	assert((tr_rpc_method_hash(1U, method_ids[0]) & 7U) ==
	       (tr_rpc_method_hash(1U, method_ids[1]) & 7U));
	assert((tr_rpc_method_hash(1U, method_ids[1]) & 7U) ==
	       (tr_rpc_method_hash(1U, method_ids[2]) & 7U));

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 64U;
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
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_buffer_pool_init(&rpc_pool, 16U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 3U;
	rpc_config.max_calls = 8U;
	rpc_config.message_pool = &rpc_pool;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	for (i = 0; i < 3U; ++i) {
		memset(&methods[i], 0, sizeof(methods[i]));
		methods[i].service_id = 1U;
		methods[i].method_id = method_ids[i];
		methods[i].request_cardinality = TR_RPC_ONE;
		methods[i].response_cardinality = TR_RPC_ONE;
		methods[i].request_codec_id = TR_RPC_CODEC_RAW;
		methods[i].response_codec_id = TR_RPC_CODEC_RAW;
		methods[i].lane = TR_LANE_CONTROL;
		methods[i].max_request_bytes = 64U;
		methods[i].max_response_bytes = 64U;

		handler_args[i].ctx = &ctx;
		handler_args[i].index = i;
		handler_args[i].reply = (uint8_t)(0x61U + i);
		result_args[i].ctx = &ctx;
		result_args[i].expected_reply = handler_args[i].reply;

		assert(tr_rpc_register_method(client_rpc, &methods[i],
					      NULL, NULL) == TR_OK);
		assert(tr_rpc_register_method(server_rpc, &methods[i],
					      rpc_method_index_handler,
					      &handler_args[i]) == TR_OK);
	}

	/* Duplicate detection still wins even when the table is at method limit. */
	assert(tr_rpc_register_method(client_rpc, &methods[1], NULL, NULL) ==
	       TR_ERR_STATE);
	assert(tr_rpc_register_method(server_rpc, &methods[1],
				      rpc_method_index_handler,
				      &handler_args[1]) == TR_ERR_STATE);

	extra = methods[0];
	extra.method_id = 99U;
	assert(tr_rpc_register_method(client_rpc, &extra, NULL, NULL) ==
	       TR_AGAIN);
	assert(tr_rpc_register_method(server_rpc, &extra,
				      rpc_method_index_handler,
				      &handler_args[0]) == TR_AGAIN);

	assert(tr_rpc_endpoint_get_stats(client_rpc, &stats) == TR_OK);
	assert(stats.registered_methods == 3U);
	assert(tr_rpc_endpoint_get_stats(server_rpc, &stats) == TR_OK);
	assert(stats.registered_methods == 3U);

	request.data = &request_byte;
	request.len = 1U;
	for (i = 0; i < 3U; ++i) {
		assert(tr_rpc_unary_call(client_rpc, 1U, method_ids[i],
					&request, rpc_method_index_result,
					&result_args[i], &call) == TR_OK);
		wait_rpc_method_index(&ctx, i, i + 1U);
	}

	wait_for_pool_full(&rpc_pool, 16U);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_rpc_deadline_heap_order(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle long_call;
	struct tr_rpc_call_handle short_call;
	struct tr_rpc_call_handle middle_call;
	struct tr_rpc_call_handle root;
	struct tr_rpc_call_options options;
	struct tr_rpc_context call_context;
	uint64_t long_deadline;
	uint64_t root_deadline;
	uint32_t heap_count;
	int client_fd;
	int server_fd;

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
	channel_config.max_streams = 4U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_buffer_pool_init(&rpc_pool, 8U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 1U;
	rpc_config.max_calls = 4U;
	rpc_config.message_pool = &rpc_pool;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config, &client_rpc) ==
	       TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 92U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_MANY;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);

	memset(&options, 0, sizeof(options));
	options.timeout_ms = 5000U;
	assert(tr_rpc_call_start_ex(client_rpc, 92U, 1U, &options, NULL,
				    &long_call) == TR_OK);
	memset(&call_context, 0, sizeof(call_context));
	assert(tr_rpc_call_get_context(long_call, &call_context) == TR_OK);
	assert(call_context.service_id == 92U);
	assert(call_context.method_id == 1U);
	assert(call_context.request_cardinality == TR_RPC_MANY);
	assert(call_context.response_cardinality == TR_RPC_MANY);
	assert(call_context.has_deadline == 1);
	assert(call_context.deadline_remaining_ms > 0U);
	assert(call_context.deadline_remaining_ms <= 5000U);
	assert(call_context.cancelled == 0);
	assert(call_context.cancel_status == TR_RPC_STATUS_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 1U);
	assert(memcmp(&root, &long_call, sizeof(root)) == 0);
	long_deadline = root_deadline;

	options.timeout_ms = 3000U;
	assert(tr_rpc_call_start_ex(client_rpc, 92U, 1U, &options, NULL,
				    &short_call) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 2U);
	assert(memcmp(&root, &short_call, sizeof(root)) == 0);
	assert(root_deadline < long_deadline);

	options.timeout_ms = 4000U;
	assert(tr_rpc_call_start_ex(client_rpc, 92U, 1U, &options, NULL,
				    &middle_call) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 3U);
	assert(memcmp(&root, &short_call, sizeof(root)) == 0);

	/*
	 * The long Call is a non-root, non-last heap entry. Removing it forces
	 * the last entry into the hole and exercises the local heap repair path.
	 */
	assert(tr_rpc_call_cancel(long_call) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 2U);
	assert(memcmp(&root, &short_call, sizeof(root)) == 0);

	assert(tr_rpc_call_cancel(short_call) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 1U);
	assert(memcmp(&root, &middle_call, sizeof(root)) == 0);

	assert(tr_rpc_call_cancel(middle_call) == TR_OK);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 0U);
	assert(memcmp(&root, &(struct tr_rpc_call_handle){ 0 }, sizeof(root)) == 0);
	assert(root_deadline == 0U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
}

struct rpc_slot_reuse_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned entered;
	unsigned results;
	int release;
};

static int rpc_slot_reuse_handler(struct tr_rpc_call_handle call,
				  const struct tr_rpc_bytes *request,
				  struct tr_rpc_unary_response *response,
				  void *arg)
{
	struct rpc_slot_reuse_ctx *ctx = (struct rpc_slot_reuse_ctx *)arg;
	static const uint8_t reply[] = "ok";
	(void)call;
	(void)request;

	pthread_mutex_lock(&ctx->lock);
	ctx->entered++;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void rpc_slot_reuse_result(struct tr_rpc_call_handle call, int status,
				  const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_slot_reuse_ctx *ctx = (struct rpc_slot_reuse_ctx *)arg;
	(void)call;
	assert(status == TR_RPC_STATUS_OK);
	assert(response && response->len == 2U);
	assert(memcmp(response->data, "ok", 2U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_slot_counter(struct rpc_slot_reuse_ctx *ctx,
				  unsigned *value, unsigned target)
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

static void wait_rpc_handle_stale(struct tr_rpc_call_handle handle)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		if (tr_rpc_call_is_cancelled(handle, NULL) == TR_ERR_STALE)
			return;
		{
			struct timespec pause_time = { 0, 1000000L };
			nanosleep(&pause_time, NULL);
		}
	}
	assert(tr_rpc_call_is_cancelled(handle, NULL) == TR_ERR_STALE);
}

static void test_rpc_call_slot_reuse(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle first;
	struct tr_rpc_call_handle second;
	struct tr_rpc_call_handle rejected;
	struct tr_rpc_call_handle root;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_options call_options;
	struct rpc_slot_reuse_ctx ctx;
	uint64_t root_deadline;
	uint32_t heap_count;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
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
	channel_config.max_streams = 2U;
	channel_config.initial_window_bytes = 4096U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_buffer_pool_init(&rpc_pool, 8U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 2U;
	rpc_config.max_calls = 1U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = 16U;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config, &client_rpc) ==
	       TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config, &server_rpc) ==
	       TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 91U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 64U;
	method.max_response_bytes = 64U;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) == TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      rpc_slot_reuse_handler, &ctx) == TR_OK);

	memset(&call_options, 0, sizeof(call_options));
	call_options.timeout_ms = 30000U;
	request.data = (const uint8_t *)"one";
	request.len = 3U;
	assert(tr_rpc_unary_call_ex(client_rpc, 91U, 1U, &request,
				   &call_options, rpc_slot_reuse_result,
				   &ctx, &first) == TR_OK);
	wait_rpc_slot_counter(&ctx, &ctx.entered, 1U);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 1U);
	assert(memcmp(&root, &first, sizeof(root)) == 0);
	assert(root_deadline != 0U);

	/* max_calls=1: the free-call list is empty while the first Call is live. */
	assert(tr_rpc_unary_call_ex(client_rpc, 91U, 1U, &request,
				   &call_options, rpc_slot_reuse_result,
				   &ctx, &rejected) == TR_AGAIN);

	pthread_mutex_lock(&ctx.lock);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_rpc_slot_counter(&ctx, &ctx.results, 1U);
	wait_rpc_handle_stale(first);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 0U);
	assert(memcmp(&root, &(struct tr_rpc_call_handle){ 0 }, sizeof(root)) == 0);
	assert(root_deadline == 0U);

	pthread_mutex_lock(&ctx.lock);
	ctx.release = 0;
	pthread_mutex_unlock(&ctx.lock);

	request.data = (const uint8_t *)"two";
	assert(tr_rpc_unary_call_ex(client_rpc, 91U, 1U, &request,
				   &call_options, rpc_slot_reuse_result,
				   &ctx, &second) == TR_OK);
	wait_rpc_slot_counter(&ctx, &ctx.entered, 2U);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 1U);
	assert(memcmp(&root, &second, sizeof(root)) == 0);
	assert(root_deadline != 0U);

	pthread_mutex_lock(&ctx.lock);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_rpc_slot_counter(&ctx, &ctx.results, 2U);
	wait_rpc_handle_stale(second);
	assert(tr_rpc_deadline_heap_snapshot(client_rpc, &heap_count, &root,
					     &root_deadline) == TR_OK);
	assert(heap_count == 0U);
	/* max_calls=1 guarantees slot reuse; the opaque capability must rotate. */
	assert(memcmp(&second, &first, sizeof(second)) != 0);
	assert(tr_rpc_call_is_cancelled(first, NULL) == TR_ERR_STALE);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
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
	assert(tr_channel_set_reconnect_tcp_nodelay(client_channel, 1) == TR_OK);
	assert(tr_channel_enable_client_reconnect(client_channel,
						  &reconnect_config) == TR_OK);
	/*
	 * reconnect socket policy 已由 Reactor owner 串行化；enable 发布后不允许
	 * application thread 再修改本次 reconnect session 的 socket policy。
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
	tr_socket_close(&listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&tx_pool);
	destroy_channel_test_ctx(&server_ctx);
	destroy_channel_test_ctx(&client_ctx);
}

static void test_rpc_wire_and_raw_codec(void)
{
	struct tr_rpc_wire_header in;
	struct tr_rpc_wire_header out;
	struct tr_rpc_bytes raw;
	struct tr_rpc_bytes decoded;
	const uint8_t *payload;
	uint8_t wire[TR_RPC_WIRE_HEADER_SIZE + 5U];
	uint32_t written = 0;

	memset(&in, 0, sizeof(in));
	in.version = TR_RPC_WIRE_VERSION;
	in.type = TR_RPC_WIRE_REQUEST;
	in.service_id = 10;
	in.method_id = 20;
	in.codec_id = TR_RPC_CODEC_RAW;
	in.status = TR_RPC_STATUS_OK;
	in.payload_len = 5;

	assert(tr_rpc_wire_encode(wire, &in) == TR_OK);
	raw.data = (const uint8_t *)"hello";
	raw.len = 5;
	assert(tr_rpc_raw_encode(&raw, wire + TR_RPC_WIRE_HEADER_SIZE, 5,
				 &written) == TR_OK);
	assert(written == 5);
	assert(tr_rpc_wire_decode(wire, sizeof(wire), &out, &payload) == TR_OK);
	assert(out.service_id == 10);
	assert(out.method_id == 20);
	assert(out.codec_id == TR_RPC_CODEC_RAW);
	assert(out.payload_len == 5);
	assert(tr_rpc_raw_decode(payload, out.payload_len, &decoded) == TR_OK);
	assert(decoded.len == 5);
	assert(memcmp(decoded.data, "hello", 5) == 0);
}

static void test_rpc_unary_raw_roundtrip(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes request;
	struct rpc_test_ctx ctx;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 64;
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
	channel_config.max_streams = 16;
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
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 8, 4096) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 16;
	rpc_config.message_pool = &rpc_pool;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 1;
	method.method_id = 1;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024;
	method.max_response_bytes = 1024;

	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      rpc_test_unary_handler, &ctx) == TR_OK);

	request.data = (const uint8_t *)"ping-from-client";
	request.len = 16;
	assert(tr_rpc_unary_call(client_rpc, 1, 1, &request, rpc_test_result,
				 &ctx, &call) == TR_OK);

	wait_rpc_counter(&ctx, &ctx.server_calls, 1);
	wait_rpc_counter(&ctx, &ctx.client_results, 1);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.request_len == 16);
	assert(memcmp(ctx.request, "ping-from-client", 16) == 0);
	assert(ctx.client_status == TR_RPC_STATUS_OK);
	assert(ctx.response_len == 16);
	assert(memcmp(ctx.response, "pong-from-server", 16) == 0);
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&rpc_pool, 8);

	{
		struct tr_rpc_endpoint_stats client_stats;
		struct tr_rpc_endpoint_stats server_stats;
		assert(tr_rpc_endpoint_get_stats(client_rpc, &client_stats) ==
		       TR_OK);
		assert(tr_rpc_endpoint_get_stats(server_rpc, &server_stats) ==
		       TR_OK);
		assert(client_stats.registered_methods == 1U);
		assert(server_stats.registered_methods == 1U);
		assert(client_stats.calls_started >= 1U);
		assert(server_stats.calls_started >= 1U);
		assert(client_stats.executor_threads >= 1U);
		assert(server_stats.executor_threads >= 1U);
	}

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_fragment_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned server_calls;
	unsigned client_results;
	int client_status;
	uint8_t request[2048];
	uint32_t request_len;
	uint8_t reply[2048];
	uint32_t reply_len;
	uint8_t response[2048];
	uint32_t response_len;
};

static int rpc_fragment_unary_handler(struct tr_rpc_call_handle call,
				      const struct tr_rpc_bytes *request,
				      struct tr_rpc_unary_response *response,
				      void *arg)
{
	struct rpc_fragment_test_ctx *ctx = (struct rpc_fragment_test_ctx *)arg;
	(void)call;

	assert(request->len <= sizeof(ctx->request));
	pthread_mutex_lock(&ctx->lock);
	memcpy(ctx->request, request->data, request->len);
	ctx->request_len = request->len;
	ctx->server_calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = ctx->reply;
	response->message.len = ctx->reply_len;
	return TR_OK;
}

static void rpc_fragment_result(struct tr_rpc_call_handle call, int status,
				const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_fragment_test_ctx *ctx = (struct rpc_fragment_test_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->client_status = status;
	if (response) {
		assert(response->len <= sizeof(ctx->response));
		memcpy(ctx->response, response->data, response->len);
		ctx->response_len = response->len;
	}
	ctx->client_results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_fragment_counter(struct rpc_fragment_test_ctx *ctx,
				      unsigned *value, unsigned target)
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

struct rpc_reconnect_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned server_calls;
	unsigned server_completed;
	unsigned client_results;
	int block_first;
	int statuses[2];
};

static int rpc_reconnect_unary_handler(struct tr_rpc_call_handle call,
				       const struct tr_rpc_bytes *request,
				       struct tr_rpc_unary_response *response,
				       void *arg)
{
	struct rpc_reconnect_test_ctx *ctx =
		(struct rpc_reconnect_test_ctx *)arg;
	static const uint8_t reply[] = "reconnected";
	unsigned ordinal;
	(void)call;
	(void)request;

	pthread_mutex_lock(&ctx->lock);
	ordinal = ++ctx->server_calls;
	pthread_cond_broadcast(&ctx->cond);
	while (ordinal == 1U && ctx->block_first)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	ctx->server_completed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void rpc_reconnect_result(struct tr_rpc_call_handle call, int status,
				 const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_reconnect_test_ctx *ctx =
		(struct rpc_reconnect_test_ctx *)arg;
	(void)call;
	(void)response;

	pthread_mutex_lock(&ctx->lock);
	assert(ctx->client_results < 2U);
	ctx->statuses[ctx->client_results++] = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_reconnect_counter(struct rpc_reconnect_test_ctx *ctx,
				       unsigned *value, unsigned target)
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
			struct timespec pause_time = { 0, 1000000L };
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
	struct tr_channel *server_channel = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_stream_handle stream;
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
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.min_protocol_version = 2;
	channel_config.max_protocol_version = 2;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);

	wait_channel_lane_down(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_down(server_channel, TR_LANE_CONTROL);
	assert(tr_stream_open(client_channel, TR_LANE_CONTROL, &stream) ==
	       TR_ERR_CLOSED);
	assert(tr_stream_open(server_channel, TR_LANE_CONTROL, &stream) ==
	       TR_ERR_CLOSED);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
}

static void test_rpc_connection_replacement_semantics(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_conn_handle client_new_conn;
	struct tr_conn_handle server_new_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes request;
	struct rpc_reconnect_test_ctx ctx;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	ctx.block_first = 1;

	make_tcp_pair(&client_fd, &server_fd);
	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 64;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 32;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 16;
	channel_config.initial_window_bytes = 64U * 1024U;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 32, 4096) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 16;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 2;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 77;
	method.method_id = 1;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024;
	method.max_response_bytes = 1024;
	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      rpc_reconnect_unary_handler,
				      &ctx) == TR_OK);

	request.data = (const uint8_t *)"first";
	request.len = 5;
	assert(tr_rpc_unary_call(client_rpc, 77, 1, &request,
				 rpc_reconnect_result, &ctx, &call) == TR_OK);
	wait_rpc_reconnect_counter(&ctx, &ctx.server_calls, 1);

	/* In-flight RPC is not transparently replayed across a TCP replacement. */
	assert(tr_reactor_close(client_conn) == TR_OK);
	wait_rpc_reconnect_counter(&ctx, &ctx.client_results, 1);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.statuses[0] == TR_RPC_STATUS_UNAVAILABLE);
	ctx.block_first = 0;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	wait_rpc_reconnect_counter(&ctx, &ctx.server_completed, 1);

	wait_channel_lane_down(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_down(server_channel, TR_LANE_CONTROL);
	make_tcp_pair(&client_fd, &server_fd);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_new_conn) ==
	       TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_new_conn) ==
	       TR_OK);
	assert(tr_channel_replace_connection(client_channel, TR_LANE_CONTROL,
					     client_new_conn) == TR_OK);
	assert(tr_channel_replace_connection(server_channel, TR_LANE_CONTROL,
					     server_new_conn) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	/* New Calls use the replacement Connection and work normally. */
	request.data = (const uint8_t *)"second";
	request.len = 6;
	assert(tr_rpc_unary_call(client_rpc, 77, 1, &request,
				 rpc_reconnect_result, &ctx, &call) == TR_OK);
	wait_rpc_reconnect_counter(&ctx, &ctx.client_results, 2);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.statuses[1] == TR_RPC_STATUS_OK);
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&rpc_pool, 32);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_rpc_large_message_fragmentation(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_buffer_pool reassembly_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes request;
	struct rpc_fragment_test_ctx ctx;
	uint8_t request_data[1500];
	int client_fd;
	int server_fd;
	uint32_t i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	for (i = 0; i < sizeof(request_data); ++i)
		request_data[i] = (uint8_t)((i * 17U + 3U) & 0xffU);
	ctx.reply_len = 1700;
	for (i = 0; i < ctx.reply_len; ++i)
		ctx.reply[i] = (uint8_t)((i * 29U + 11U) & 0xffU);

	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 32;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 32;
	reactor_config.rx_buffer_size = 256;
	reactor_config.max_payload_len = 256;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	assert(tr_buffer_pool_init(&reassembly_pool, 4, 4096) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 16;
	channel_config.initial_window_bytes = 8192;
	channel_config.window_update_threshold_bytes = 2048;
	channel_config.max_message_bytes = 4096;
	channel_config.reassembly_pool = &reassembly_pool;
	assert(tr_channel_create(&channel_config, client_conn, client_conn,
				 NULL, NULL, NULL, NULL,
				 &client_channel) == TR_OK);

	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(&channel_config, server_conn, server_conn,
				 NULL, NULL, NULL, NULL,
				 &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 12, 4096) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 16;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 2;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 5;
	method.method_id = 1;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_BULK;
	method.max_request_bytes = 2048;
	method.max_response_bytes = 2048;

	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &method,
				      rpc_fragment_unary_handler,
				      &ctx) == TR_OK);

	request.data = request_data;
	request.len = sizeof(request_data);
	assert(tr_rpc_unary_call(client_rpc, 5, 1, &request,
				 rpc_fragment_result, &ctx, &call) == TR_OK);

	wait_rpc_fragment_counter(&ctx, &ctx.server_calls, 1);
	wait_rpc_fragment_counter(&ctx, &ctx.client_results, 1);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.request_len == sizeof(request_data));
	assert(memcmp(ctx.request, request_data, sizeof(request_data)) == 0);
	assert(ctx.client_status == TR_RPC_STATUS_OK);
	assert(ctx.response_len == ctx.reply_len);
	assert(memcmp(ctx.response, ctx.reply, ctx.reply_len) == 0);
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&rpc_pool, 12);
	wait_for_pool_full(&reassembly_pool, 4);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	tr_buffer_pool_destroy(&reassembly_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_stream_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned client_opened;
	unsigned server_opened;
	unsigned server_messages;
	unsigned server_half_closed;
	unsigned server_closed;
	unsigned client_messages;
	unsigned client_remote_closed;
	unsigned client_finished;
	unsigned client_events_after_finished;
	int client_finish_status;
	uint8_t server_data[3][128];
	uint32_t server_len[3];
	uint8_t client_data[3][128];
	uint32_t client_len[3];
};

static int rpc_stream_server_open(struct tr_rpc_call_handle call, void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_opened++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_rpc_message_disposition
rpc_stream_server_message(struct tr_rpc_call_handle call,
			  const struct tr_rpc_message *message, void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;
	struct tr_rpc_bytes response;
	unsigned index;

	pthread_mutex_lock(&ctx->lock);
	index = ctx->server_messages;
	assert(index < 3U);
	assert(message->bytes.len <= sizeof(ctx->server_data[index]));
	memcpy(ctx->server_data[index], message->bytes.data,
	       message->bytes.len);
	ctx->server_len[index] = message->bytes.len;
	ctx->server_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response = message->bytes;
	assert(tr_rpc_call_send(call, &response) == TR_OK);
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_stream_server_half_close(struct tr_rpc_call_handle call,
					 void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;

	pthread_mutex_lock(&ctx->lock);
	ctx->server_half_closed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	/* Server response termination must go through final STATUS, not close_send. */
	assert(tr_rpc_call_close_send(call) == TR_ERR_INVALID);
	assert(tr_rpc_call_finish(call, 99) == TR_ERR_INVALID);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
}

static void rpc_stream_server_close(struct tr_rpc_call_handle call, int status,
				    void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;
	(void)call;
	assert(status == TR_RPC_STATUS_OK);

	pthread_mutex_lock(&ctx->lock);
	ctx->server_closed++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static enum tr_rpc_message_disposition
rpc_stream_client_message(struct tr_rpc_call_handle call,
			  const struct tr_rpc_message *message, void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;
	unsigned index;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	index = ctx->client_messages;
	assert(index < 3U);
	assert(message->bytes.len <= sizeof(ctx->client_data[index]));
	memcpy(ctx->client_data[index], message->bytes.data,
	       message->bytes.len);
	ctx->client_len[index] = message->bytes.len;
	ctx->client_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	if (index == 0U) {
		struct tr_rpc_message owned = *message;
		assert(tr_rpc_message_release(&owned) == TR_OK);
		return TR_RPC_MESSAGE_TAKE_OWNERSHIP;
	}
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_stream_client_event(struct tr_rpc_call_handle call,
				    enum tr_rpc_call_event event, int status,
				    void *arg)
{
	struct rpc_stream_test_ctx *ctx = (struct rpc_stream_test_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	if (ctx->client_finished != 0U)
		ctx->client_events_after_finished++;
	if (event == TR_RPC_CALL_EVENT_OPENED)
		ctx->client_opened++;
	else if (event == TR_RPC_CALL_EVENT_REMOTE_CLOSED)
		ctx->client_remote_closed++;
	else if (event == TR_RPC_CALL_EVENT_FINISHED) {
		ctx->client_finished++;
		ctx->client_finish_status = status;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_stream_counter(struct rpc_stream_test_ctx *ctx,
				    unsigned *value, unsigned target)
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

static void test_rpc_bidi_streaming_raw_fast_path(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_rpc_call_callbacks callbacks;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_buffer_pool bulk_pool;
	struct tr_rpc_call_handle call;
	struct rpc_stream_test_ctx ctx;
	int client_fd;
	int server_fd;
	unsigned i;

	static const char *messages[3] = { "stream-one", "stream-two-two",
					   "stream-three-three-three" };

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 64;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 32;
	reactor_config.rx_buffer_size = 8192;
	reactor_config.max_payload_len = 8192;
	reactor_config.rx_budget_bytes = 128U * 1024U;
	reactor_config.tx_budget_bytes = 128U * 1024U;

	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 32;
	channel_config.initial_window_bytes = 64U * 1024U;
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
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 32, 4096) == TR_OK);
	assert(tr_buffer_pool_init(&bulk_pool, 8, 2048) == TR_OK);

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 16;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_queue_capacity = 64;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 2;
	method.method_id = 1;
	method.request_cardinality = TR_RPC_MANY;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_BULK;
	method.max_request_bytes = 1024;
	method.max_response_bytes = 1024;

	assert(tr_rpc_register_method(client_rpc, &method, NULL, NULL) ==
	       TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_open = rpc_stream_server_open;
	handlers.on_message = rpc_stream_server_message;
	handlers.on_half_close = rpc_stream_server_half_close;
	handlers.on_close = rpc_stream_server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &method, &handlers,
					     &ctx) == TR_OK);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_message = rpc_stream_client_message;
	callbacks.on_event = rpc_stream_client_event;
	callbacks.arg = &ctx;
	assert(tr_rpc_call_start(client_rpc, 2, 1, &callbacks, &call) == TR_OK);
	wait_rpc_stream_counter(&ctx, &ctx.client_opened, 1);

	for (i = 0; i < 3; ++i) {
		struct tr_buffer *buffer;
		size_t len = strlen(messages[i]);
		assert(tr_buffer_acquire(&bulk_pool, (uint32_t)len, &buffer) ==
		       TR_OK);
		memcpy(buffer->data, messages[i], len);
		buffer->len = (uint32_t)len;
		assert(tr_rpc_call_send_buffer(call, buffer) == TR_OK);
	}

	assert(tr_rpc_call_close_send(call) == TR_OK);

	wait_rpc_stream_counter(&ctx, &ctx.server_opened, 1);
	wait_rpc_stream_counter(&ctx, &ctx.server_messages, 3);
	wait_rpc_stream_counter(&ctx, &ctx.server_half_closed, 1);
	wait_rpc_stream_counter(&ctx, &ctx.client_messages, 3);
	wait_rpc_stream_counter(&ctx, &ctx.client_finished, 1);
	wait_rpc_handle_stale(call);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.client_finish_status == TR_RPC_STATUS_OK);
	assert(ctx.client_remote_closed == 0U);
	assert(ctx.client_events_after_finished == 0U);
	for (i = 0; i < 3; ++i) {
		size_t len = strlen(messages[i]);
		assert(ctx.server_len[i] == len);
		assert(ctx.client_len[i] == len);
		assert(memcmp(ctx.server_data[i], messages[i], len) == 0);
		assert(memcmp(ctx.client_data[i], messages[i], len) == 0);
	}
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&bulk_pool, 8);
	wait_for_pool_full(&rpc_pool, 32);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&bulk_pool);
	tr_buffer_pool_destroy(&rpc_pool);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_shape_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned opened;
	unsigned server_messages;
	unsigned client_messages;
	unsigned remote_closed;
	unsigned finished;
	unsigned events_after_finished;
	unsigned server_context_ok;
	unsigned client_context_ok;
	unsigned initial_metadata_ok;
	unsigned trailing_metadata_ok;
	uint32_t expected_method_id;
	enum tr_rpc_cardinality expected_request_cardinality;
	enum tr_rpc_cardinality expected_response_cardinality;
	const char *initial_value;
	const char *trailing_value;
	int finish_status;
};

static enum tr_rpc_message_disposition
rpc_shape_server_message(struct tr_rpc_call_handle call,
			 const struct tr_rpc_message *message, void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	struct tr_rpc_context context;

	assert(message->bytes.len != 0);
	memset(&context, 0, sizeof(context));
	assert(tr_rpc_call_get_context(call, &context) == TR_OK);
	assert(context.service_id == 3U);
	assert(context.method_id == ctx->expected_method_id);
	assert(context.request_cardinality ==
	       ctx->expected_request_cardinality);
	assert(context.response_cardinality ==
	       ctx->expected_response_cardinality);
	assert(context.cancelled == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->server_context_ok = 1U;
	ctx->server_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_server_stream_half_close(struct tr_rpc_call_handle call,
					 void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	struct tr_rpc_bytes first;
	struct tr_rpc_bytes second;

	first.data = (const uint8_t *)"server-stream-1";
	first.len = 15;
	second.data = (const uint8_t *)"server-stream-2";
	second.len = 15;

	assert(tr_rpc_call_set_metadata(
		       call, "initial-id", ctx->initial_value,
		       (uint16_t)strlen(ctx->initial_value)) == TR_OK);
	assert(tr_rpc_call_send(call, &first) == TR_OK);
	assert(tr_rpc_call_send(call, &second) == TR_OK);
	assert(tr_rpc_call_set_metadata(call, "late-initial", "x", 1U) ==
	       TR_ERR_STATE);
	assert(tr_rpc_call_set_trailing_metadata(
		       call, "trail-id", ctx->trailing_value,
		       (uint16_t)strlen(ctx->trailing_value)) == TR_OK);
	assert(tr_rpc_call_finish(call, 99) == TR_ERR_INVALID);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);

	pthread_mutex_lock(&ctx->lock);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void rpc_client_stream_half_close(struct tr_rpc_call_handle call,
					 void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	struct tr_rpc_bytes only;

	only.data = (const uint8_t *)"client-stream-result";
	only.len = 20;
	assert(tr_rpc_call_close_send(call) == TR_ERR_INVALID);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_ERR_STATE);
	assert(tr_rpc_call_set_metadata(
		       call, "initial-id", ctx->initial_value,
		       (uint16_t)strlen(ctx->initial_value)) == TR_OK);
	assert(tr_rpc_call_send(call, &only) == TR_OK);
	assert(tr_rpc_call_send(call, &only) == TR_ERR_STATE);
	assert(tr_rpc_call_set_trailing_metadata(
		       call, "trail-id", ctx->trailing_value,
		       (uint16_t)strlen(ctx->trailing_value)) == TR_OK);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);

	pthread_mutex_lock(&ctx->lock);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static enum tr_rpc_message_disposition
rpc_shape_client_message(struct tr_rpc_call_handle call,
			 const struct tr_rpc_message *message, void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	uint8_t value[64];
	uint16_t value_len = sizeof(value);

	assert(message->bytes.len != 0);
	if (tr_rpc_call_get_peer_metadata(
		    call, "initial-id", value, &value_len) == TR_OK) {
		assert(value_len == strlen(ctx->initial_value));
		assert(memcmp(value, ctx->initial_value, value_len) == 0);
		pthread_mutex_lock(&ctx->lock);
		ctx->initial_metadata_ok = 1U;
		pthread_mutex_unlock(&ctx->lock);
	}

	pthread_mutex_lock(&ctx->lock);
	ctx->client_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_shape_client_event(struct tr_rpc_call_handle call,
				   enum tr_rpc_call_event event, int status,
				   void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	if (ctx->finished != 0U)
		ctx->events_after_finished++;
	if (event == TR_RPC_CALL_EVENT_OPENED)
		ctx->opened++;
	else if (event == TR_RPC_CALL_EVENT_REMOTE_CLOSED)
		ctx->remote_closed++;
	else if (event == TR_RPC_CALL_EVENT_FINISHED) {
		struct tr_rpc_context context;
		uint8_t value[64];
		uint16_t value_len = sizeof(value);

		memset(&context, 0, sizeof(context));
		assert(tr_rpc_call_get_context(call, &context) == TR_OK);
		assert(context.service_id == 3U);
		assert(context.method_id == ctx->expected_method_id);
		assert(context.request_cardinality ==
		       ctx->expected_request_cardinality);
		assert(context.response_cardinality ==
		       ctx->expected_response_cardinality);
		ctx->client_context_ok = 1U;

		assert(tr_rpc_call_get_peer_trailing_metadata(
			       call, "trail-id", value, &value_len) == TR_OK);
		assert(value_len == strlen(ctx->trailing_value));
		assert(memcmp(value, ctx->trailing_value, value_len) == 0);
		ctx->trailing_metadata_ok = 1U;

		value_len = sizeof(value);
		assert(tr_rpc_call_get_peer_metadata(
			       call, "trail-id", value, &value_len) == TR_ERR_STALE);

		ctx->finished++;
		ctx->finish_status = status;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_shape_counter(struct rpc_shape_ctx *ctx, unsigned *value,
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

static void test_rpc_client_and_server_stream_shapes(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc server_stream_method;
	struct tr_rpc_method_desc client_stream_method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_rpc_call_callbacks callbacks;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_endpoint_stats client_stats;
	struct tr_rpc_endpoint_stats server_stats;
	struct tr_rpc_semantic_stats client_semantic;
	struct tr_rpc_semantic_stats server_semantic;
	struct rpc_shape_ctx server_stream_ctx;
	struct rpc_shape_ctx client_stream_ctx;
	struct tr_rpc_bytes message;
	int client_fd;
	int server_fd;

	memset(&server_stream_ctx, 0, sizeof(server_stream_ctx));
	memset(&client_stream_ctx, 0, sizeof(client_stream_ctx));
	server_stream_ctx.expected_method_id = 1U;
	server_stream_ctx.expected_request_cardinality = TR_RPC_ONE;
	server_stream_ctx.expected_response_cardinality = TR_RPC_MANY;
	server_stream_ctx.initial_value = "ss-initial";
	server_stream_ctx.trailing_value = "ss-trailer";
	client_stream_ctx.expected_method_id = 2U;
	client_stream_ctx.expected_request_cardinality = TR_RPC_MANY;
	client_stream_ctx.expected_response_cardinality = TR_RPC_ONE;
	client_stream_ctx.initial_value = "cs-initial";
	client_stream_ctx.trailing_value = "cs-trailer";
	assert(pthread_mutex_init(&server_stream_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&server_stream_ctx.cond, NULL) == 0);
	assert(pthread_mutex_init(&client_stream_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&client_stream_ctx.cond, NULL) == 0);

	make_tcp_pair(&client_fd, &server_fd);
	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 64;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 32;
	reactor_config.rx_buffer_size = 8192;
	reactor_config.max_payload_len = 8192;
	reactor_config.rx_budget_bytes = 128U * 1024U;
	reactor_config.tx_budget_bytes = 128U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 32;
	channel_config.initial_window_bytes = 64U * 1024U;
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
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 32, 4096) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 16;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_queue_capacity = 64;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&server_stream_method, 0, sizeof(server_stream_method));
	server_stream_method.service_id = 3;
	server_stream_method.method_id = 1;
	server_stream_method.request_cardinality = TR_RPC_ONE;
	server_stream_method.response_cardinality = TR_RPC_MANY;
	server_stream_method.request_codec_id = TR_RPC_CODEC_RAW;
	server_stream_method.response_codec_id = TR_RPC_CODEC_RAW;
	server_stream_method.lane = TR_LANE_CONTROL;
	server_stream_method.max_request_bytes = 1024;
	server_stream_method.max_response_bytes = 1024;

	memset(&client_stream_method, 0, sizeof(client_stream_method));
	client_stream_method = server_stream_method;
	client_stream_method.method_id = 2;
	client_stream_method.request_cardinality = TR_RPC_MANY;
	client_stream_method.response_cardinality = TR_RPC_ONE;

	assert(tr_rpc_register_method(client_rpc, &server_stream_method, NULL,
				      NULL) == TR_OK);
	assert(tr_rpc_register_method(client_rpc, &client_stream_method, NULL,
				      NULL) == TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_message = rpc_shape_server_message;
	handlers.on_half_close = rpc_server_stream_half_close;
	assert(tr_rpc_register_stream_method(server_rpc, &server_stream_method,
					     &handlers,
					     &server_stream_ctx) == TR_OK);

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_message = rpc_shape_server_message;
	handlers.on_half_close = rpc_client_stream_half_close;
	assert(tr_rpc_register_stream_method(server_rpc, &client_stream_method,
					     &handlers,
					     &client_stream_ctx) == TR_OK);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_message = rpc_shape_client_message;
	callbacks.on_event = rpc_shape_client_event;
	callbacks.arg = &server_stream_ctx;
	assert(tr_rpc_call_start(client_rpc, 3, 1, &callbacks, &call) == TR_OK);
	wait_rpc_shape_counter(&server_stream_ctx, &server_stream_ctx.opened,
			       1);

	message.data = (const uint8_t *)"one-request";
	message.len = 11;
	assert(tr_rpc_call_send(call, &message) == TR_OK);
	assert(tr_rpc_call_send(call, &message) == TR_ERR_STATE);
	assert(tr_rpc_call_close_send(call) == TR_OK);
	wait_rpc_shape_counter(&server_stream_ctx,
			       &server_stream_ctx.server_messages, 1);
	wait_rpc_shape_counter(&server_stream_ctx,
			       &server_stream_ctx.client_messages, 2);
	wait_rpc_shape_counter(&server_stream_ctx, &server_stream_ctx.finished,
			       1);
	wait_rpc_handle_stale(call);
	pthread_mutex_lock(&server_stream_ctx.lock);
	assert(server_stream_ctx.finish_status == TR_RPC_STATUS_OK);
	assert(server_stream_ctx.remote_closed == 0U);
	assert(server_stream_ctx.events_after_finished == 0U);
	assert(server_stream_ctx.server_context_ok == 1U);
	assert(server_stream_ctx.client_context_ok == 1U);
	assert(server_stream_ctx.initial_metadata_ok == 1U);
	assert(server_stream_ctx.trailing_metadata_ok == 1U);
	pthread_mutex_unlock(&server_stream_ctx.lock);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_message = rpc_shape_client_message;
	callbacks.on_event = rpc_shape_client_event;
	callbacks.arg = &client_stream_ctx;
	assert(tr_rpc_call_start(client_rpc, 3, 2, &callbacks, &call) == TR_OK);
	wait_rpc_shape_counter(&client_stream_ctx, &client_stream_ctx.opened,
			       1);

	/* V1 has no explicit Method-open envelope: MANY request is 1..N. */
	assert(tr_rpc_call_close_send(call) == TR_ERR_STATE);
	message.data = (const uint8_t *)"many-request";
	message.len = 12;
	assert(tr_rpc_call_send(call, &message) == TR_OK);
	assert(tr_rpc_call_send(call, &message) == TR_OK);
	assert(tr_rpc_call_send(call, &message) == TR_OK);
	assert(tr_rpc_call_close_send(call) == TR_OK);
	wait_rpc_shape_counter(&client_stream_ctx,
			       &client_stream_ctx.server_messages, 3);
	wait_rpc_shape_counter(&client_stream_ctx,
			       &client_stream_ctx.client_messages, 1);
	wait_rpc_shape_counter(&client_stream_ctx, &client_stream_ctx.finished,
			       1);
	wait_rpc_handle_stale(call);
	pthread_mutex_lock(&client_stream_ctx.lock);
	assert(client_stream_ctx.finish_status == TR_RPC_STATUS_OK);
	assert(client_stream_ctx.remote_closed == 0U);
	assert(client_stream_ctx.events_after_finished == 0U);
	assert(client_stream_ctx.server_context_ok == 1U);
	assert(client_stream_ctx.client_context_ok == 1U);
	assert(client_stream_ctx.initial_metadata_ok == 1U);
	assert(client_stream_ctx.trailing_metadata_ok == 1U);
	pthread_mutex_unlock(&client_stream_ctx.lock);

	memset(&client_stats, 0, sizeof(client_stats));
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_rpc_endpoint_get_stats(client_rpc, &client_stats) == TR_OK);
	assert(tr_rpc_endpoint_get_stats(server_rpc, &server_stats) == TR_OK);
	assert(client_stats.calls_completed == 2U);
	assert(server_stats.calls_completed == 2U);

	memset(&client_semantic, 0, sizeof(client_semantic));
	memset(&server_semantic, 0, sizeof(server_semantic));
	assert(tr_rpc_endpoint_get_semantic_stats(
		       client_rpc, &client_semantic) == TR_OK);
	assert(tr_rpc_endpoint_get_semantic_stats(
		       server_rpc, &server_semantic) == TR_OK);
	assert(client_semantic.calls_started == 2U);
	assert(client_semantic.calls_finished == 2U);
	assert(client_semantic.calls_inflight == 0U);
	assert(client_semantic.final_status[TR_RPC_STATUS_OK] == 2U);
	assert(server_semantic.calls_started == 2U);
	assert(server_semantic.calls_finished == 2U);
	assert(server_semantic.calls_inflight == 0U);
	assert(server_semantic.final_status[TR_RPC_STATUS_OK] == 2U);

	wait_for_pool_full(&rpc_pool, 32);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);

	pthread_cond_destroy(&client_stream_ctx.cond);
	pthread_mutex_destroy(&client_stream_ctx.lock);
	pthread_cond_destroy(&server_stream_ctx.cond);
	pthread_mutex_destroy(&server_stream_ctx.lock);
}

struct rpc_control_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;

	unsigned metadata_server_calls;
	unsigned metadata_client_results;
	int metadata_server_ok;
	int metadata_client_ok;

	unsigned cancel_server_messages;
	unsigned cancel_server_closed;
	unsigned cancel_client_opened;
	unsigned cancel_client_finished;
	int cancel_server_status;
	int cancel_client_status;
	int cancel_server_observed;

	unsigned deadline_server_calls;
	unsigned deadline_client_results;
	unsigned deadline_server_observed;
	int deadline_client_status;
	int deadline_server_status;
};

static int rpc_metadata_unary_handler(struct tr_rpc_call_handle call,
				      const struct tr_rpc_bytes *request,
				      struct tr_rpc_unary_response *response,
				      void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	uint8_t value[32];
	uint16_t value_len = sizeof(value);
	static const uint8_t reply[] = "meta-ok";

	assert(request->len == 8);
	assert(memcmp(request->data, "meta-req", 8) == 0);
	assert(tr_rpc_call_get_peer_metadata(call, "trace-id", value,
					     &value_len) == TR_OK);
	assert(value_len == 6);
	assert(memcmp(value, "abc123", 6) == 0);
	assert(tr_rpc_call_set_metadata(call, "server-id", "srv1", 4) == TR_OK);
	assert(tr_rpc_call_set_trailing_metadata(
		       call, "unary-trailer", "x", 1U) == TR_ERR_STATE);

	pthread_mutex_lock(&ctx->lock);
	ctx->metadata_server_calls++;
	ctx->metadata_server_ok = 1;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = sizeof(reply) - 1U;
	return TR_OK;
}

static void rpc_metadata_result(struct tr_rpc_call_handle call, int status,
				const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	uint8_t value[32];
	uint16_t value_len = sizeof(value);
	int ok = 0;

	if (status == TR_RPC_STATUS_OK && response && response->len == 7 &&
	    memcmp(response->data, "meta-ok", 7) == 0 &&
	    tr_rpc_call_get_peer_metadata(call, "server-id", value,
					  &value_len) == TR_OK &&
	    value_len == 4 && memcmp(value, "srv1", 4) == 0)
		ok = 1;

	pthread_mutex_lock(&ctx->lock);
	ctx->metadata_client_results++;
	ctx->metadata_client_ok = ok;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static enum tr_rpc_message_disposition
rpc_cancel_server_message(struct tr_rpc_call_handle call,
			  const struct tr_rpc_message *message, void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	(void)call;
	assert(message->bytes.len == 6);
	assert(memcmp(message->bytes.data, "cancel", 6) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->cancel_server_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_cancel_server_close(struct tr_rpc_call_handle call, int status,
				    void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	int cancel_status = TR_RPC_STATUS_OK;
	int cancelled = tr_rpc_call_is_cancelled(call, &cancel_status);

	pthread_mutex_lock(&ctx->lock);
	ctx->cancel_server_closed++;
	ctx->cancel_server_status = status;
	ctx->cancel_server_observed = cancelled == 1 &&
				      cancel_status == TR_RPC_STATUS_CANCELLED;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void rpc_cancel_client_event(struct tr_rpc_call_handle call,
				    enum tr_rpc_call_event event, int status,
				    void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	if (event == TR_RPC_CALL_EVENT_OPENED)
		ctx->cancel_client_opened++;
	else if (event == TR_RPC_CALL_EVENT_FINISHED) {
		ctx->cancel_client_finished++;
		ctx->cancel_client_status = status;
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int rpc_deadline_unary_handler(struct tr_rpc_call_handle call,
				      const struct tr_rpc_bytes *request,
				      struct tr_rpc_unary_response *response,
				      void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	struct timespec pause;
	int cancel_status = TR_RPC_STATUS_OK;
	int cancelled;
	static const uint8_t late[] = "late";

	assert(request->len == 8);
	assert(memcmp(request->data, "deadline", 8) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->deadline_server_calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	pause.tv_sec = 0;
	pause.tv_nsec = 150 * 1000 * 1000L;
	while (nanosleep(&pause, &pause) != 0 && errno == EINTR)
		;

	cancelled = tr_rpc_call_is_cancelled(call, &cancel_status);
	pthread_mutex_lock(&ctx->lock);
	ctx->deadline_server_observed++;
	ctx->deadline_server_status = cancelled == 1 ? cancel_status :
						       TR_RPC_STATUS_OK;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = late;
	response->message.len = sizeof(late) - 1U;
	return TR_OK;
}

static void rpc_deadline_result(struct tr_rpc_call_handle call, int status,
				const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_control_ctx *ctx = (struct rpc_control_ctx *)arg;
	(void)call;
	assert(response == NULL);

	pthread_mutex_lock(&ctx->lock);
	ctx->deadline_client_results++;
	ctx->deadline_client_status = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_rpc_control_counter(struct rpc_control_ctx *ctx,
				     unsigned *value, unsigned target)
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

static void test_rpc_metadata_cancel_deadline(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc metadata_method;
	struct tr_rpc_method_desc cancel_method;
	struct tr_rpc_method_desc deadline_method;
	struct tr_rpc_stream_handlers stream_handlers;
	struct tr_rpc_call_callbacks callbacks;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes message;
	struct tr_rpc_metadata metadata;
	struct tr_rpc_call_options options;
	struct tr_rpc_semantic_stats client_semantic;
	struct tr_rpc_semantic_stats server_semantic;
	struct rpc_control_ctx ctx;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 128;
	reactor_config.tx_item_capacity = 64;
	reactor_config.control_tx_item_capacity = 32;
	reactor_config.rx_buffer_count = 32;
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
	channel_config.max_streams = 32;
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
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 32, 4096) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 16;
	rpc_config.max_calls = 32;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_queue_capacity = 128;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&metadata_method, 0, sizeof(metadata_method));
	metadata_method.service_id = 4;
	metadata_method.method_id = 1;
	metadata_method.request_cardinality = TR_RPC_ONE;
	metadata_method.response_cardinality = TR_RPC_ONE;
	metadata_method.request_codec_id = TR_RPC_CODEC_RAW;
	metadata_method.response_codec_id = TR_RPC_CODEC_RAW;
	metadata_method.lane = TR_LANE_CONTROL;
	metadata_method.max_request_bytes = 1024;
	metadata_method.max_response_bytes = 1024;
	assert(tr_rpc_register_method(client_rpc, &metadata_method, NULL,
				      NULL) == TR_OK);
	assert(tr_rpc_register_method(server_rpc, &metadata_method,
				      rpc_metadata_unary_handler,
				      &ctx) == TR_OK);

	cancel_method = metadata_method;
	cancel_method.method_id = 2;
	cancel_method.request_cardinality = TR_RPC_MANY;
	cancel_method.response_cardinality = TR_RPC_MANY;
	assert(tr_rpc_register_method(client_rpc, &cancel_method, NULL, NULL) ==
	       TR_OK);
	memset(&stream_handlers, 0, sizeof(stream_handlers));
	stream_handlers.on_message = rpc_cancel_server_message;
	stream_handlers.on_close = rpc_cancel_server_close;
	assert(tr_rpc_register_stream_method(server_rpc, &cancel_method,
					     &stream_handlers, &ctx) == TR_OK);

	deadline_method = metadata_method;
	deadline_method.method_id = 3;
	assert(tr_rpc_register_method(client_rpc, &deadline_method, NULL,
				      NULL) == TR_OK);
	assert(tr_rpc_register_method(server_rpc, &deadline_method,
				      rpc_deadline_unary_handler,
				      &ctx) == TR_OK);

	metadata.key = "trace-id";
	metadata.value = "abc123";
	metadata.value_len = 6;
	memset(&options, 0, sizeof(options));
	options.metadata = &metadata;
	options.metadata_count = 1;
	message.data = (const uint8_t *)"meta-req";
	message.len = 8;
	assert(tr_rpc_unary_call_ex(client_rpc, 4, 1, &message, &options,
				    rpc_metadata_result, &ctx, &call) == TR_OK);
	wait_rpc_control_counter(&ctx, &ctx.metadata_server_calls, 1);
	wait_rpc_control_counter(&ctx, &ctx.metadata_client_results, 1);
	assert(ctx.metadata_server_ok == 1);
	assert(ctx.metadata_client_ok == 1);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_event = rpc_cancel_client_event;
	callbacks.arg = &ctx;
	assert(tr_rpc_call_start(client_rpc, 4, 2, &callbacks, &call) == TR_OK);
	wait_rpc_control_counter(&ctx, &ctx.cancel_client_opened, 1);
	message.data = (const uint8_t *)"cancel";
	message.len = 6;
	assert(tr_rpc_call_send(call, &message) == TR_OK);
	wait_rpc_control_counter(&ctx, &ctx.cancel_server_messages, 1);
	assert(tr_rpc_call_cancel(call) == TR_OK);
	wait_rpc_control_counter(&ctx, &ctx.cancel_client_finished, 1);
	wait_rpc_control_counter(&ctx, &ctx.cancel_server_closed, 1);
	assert(ctx.cancel_client_status == TR_RPC_STATUS_CANCELLED);
	assert(ctx.cancel_server_status == TR_RPC_STATUS_CANCELLED);
	assert(ctx.cancel_server_observed == 1);

	memset(&options, 0, sizeof(options));
	options.timeout_ms = 40;
	message.data = (const uint8_t *)"deadline";
	message.len = 8;
	assert(tr_rpc_unary_call_ex(client_rpc, 4, 3, &message, &options,
				    rpc_deadline_result, &ctx, &call) == TR_OK);
	wait_rpc_control_counter(&ctx, &ctx.deadline_server_calls, 1);
	wait_rpc_control_counter(&ctx, &ctx.deadline_client_results, 1);
	wait_rpc_control_counter(&ctx, &ctx.deadline_server_observed, 1);
	assert(ctx.deadline_client_status == TR_RPC_STATUS_DEADLINE_EXCEEDED);
	assert(ctx.deadline_server_status == TR_RPC_STATUS_DEADLINE_EXCEEDED);

	memset(&client_semantic, 0, sizeof(client_semantic));
	memset(&server_semantic, 0, sizeof(server_semantic));
	assert(tr_rpc_endpoint_get_semantic_stats(
		       client_rpc, &client_semantic) == TR_OK);
	assert(tr_rpc_endpoint_get_semantic_stats(
		       server_rpc, &server_semantic) == TR_OK);
	assert(client_semantic.calls_started == 3U);
	assert(client_semantic.calls_finished == 3U);
	assert(client_semantic.calls_inflight == 0U);
	assert(client_semantic.final_status[TR_RPC_STATUS_OK] == 1U);
	assert(client_semantic.final_status[TR_RPC_STATUS_CANCELLED] == 1U);
	assert(client_semantic.final_status[
		       TR_RPC_STATUS_DEADLINE_EXCEEDED] == 1U);
	assert(server_semantic.calls_started == 3U);
	assert(server_semantic.calls_finished == 3U);
	assert(server_semantic.calls_inflight == 0U);
	assert(server_semantic.final_status[TR_RPC_STATUS_OK] == 1U);
	assert(server_semantic.final_status[TR_RPC_STATUS_CANCELLED] == 1U);
	assert(server_semantic.final_status[
		       TR_RPC_STATUS_DEADLINE_EXCEEDED] == 1U);

	wait_for_pool_full(&rpc_pool, 32);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_interceptor_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned client_pre[3];
	unsigned server_pre[3];
	unsigned server_post[3];
	unsigned client_post[3];
	unsigned stream_opened;
	unsigned stream_messages;
	unsigned stream_finished;
	unsigned unary_handler_calls;
	unsigned unary_results;
	int stream_status;
	int unary_status;
	int auth_seen;
	int trailer_seen;
	int reject_metadata_seen;
	int client_post_before_stream_callback;
	int client_post_before_unary_callback;
};

static void wait_rpc_interceptor_counter(
	struct rpc_interceptor_test_ctx *ctx, unsigned *value, unsigned target)
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

static int rpc_client_interceptor(
	struct tr_rpc_call_handle call, enum tr_rpc_interceptor_phase phase,
	int status, void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	struct tr_rpc_context context;
	uint8_t value[64];
	uint16_t value_len;
	uint32_t method_id;

	memset(&context, 0, sizeof(context));
	assert(tr_rpc_call_get_context(call, &context) == TR_OK);
	assert(context.service_id == 77U);
	method_id = context.method_id;
	assert(method_id == 1U || method_id == 2U);

	if (phase == TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL) {
		assert(status == TR_RPC_STATUS_OK);
		assert(tr_rpc_call_cancel(call) == TR_ERR_STATE);
		assert(tr_rpc_call_set_metadata(
			       call, "auth-token", "token-v1", 8U) == TR_OK);
		pthread_mutex_lock(&ctx->lock);
		ctx->client_pre[method_id]++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_RPC_STATUS_OK;
	}

	assert(phase == TR_RPC_INTERCEPTOR_CLIENT_POST_CALL);
	if (method_id == 1U) {
		value_len = sizeof(value);
		assert(status == TR_RPC_STATUS_OK);
		assert(tr_rpc_call_get_peer_trailing_metadata(
			       call, "server-trailer", value, &value_len) == TR_OK);
		assert(value_len == 7U);
		assert(memcmp(value, "done-v1", 7U) == 0);
		pthread_mutex_lock(&ctx->lock);
		ctx->trailer_seen = 1;
		pthread_mutex_unlock(&ctx->lock);
	} else {
		value_len = sizeof(value);
		assert(status == TR_RPC_STATUS_PERMISSION_DENIED);
		assert(tr_rpc_call_get_peer_metadata(
			       call, "reject-by", value, &value_len) == TR_OK);
		assert(value_len == 11U);
		assert(memcmp(value, "interceptor", 11U) == 0);
		pthread_mutex_lock(&ctx->lock);
		ctx->reject_metadata_seen = 1;
		pthread_mutex_unlock(&ctx->lock);
	}

	pthread_mutex_lock(&ctx->lock);
	ctx->client_post[method_id]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_STATUS_OK;
}

static int rpc_server_interceptor(
	struct tr_rpc_call_handle call, enum tr_rpc_interceptor_phase phase,
	int status, void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	struct tr_rpc_context context;
	uint8_t value[64];
	uint16_t value_len = sizeof(value);
	uint32_t method_id;

	memset(&context, 0, sizeof(context));
	assert(tr_rpc_call_get_context(call, &context) == TR_OK);
	assert(context.service_id == 77U);
	method_id = context.method_id;
	assert(method_id == 1U || method_id == 2U);

	if (phase == TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER) {
		assert(status == TR_RPC_STATUS_OK);
		assert(tr_rpc_call_get_peer_metadata(
			       call, "auth-token", value, &value_len) == TR_OK);
		assert(value_len == 8U);
		assert(memcmp(value, "token-v1", 8U) == 0);

		pthread_mutex_lock(&ctx->lock);
		ctx->auth_seen = 1;
		ctx->server_pre[method_id]++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);

		if (method_id == 2U) {
			assert(tr_rpc_call_set_metadata(
				       call, "reject-by", "interceptor", 11U) == TR_OK);
			return TR_RPC_STATUS_PERMISSION_DENIED;
		}
		return TR_RPC_STATUS_OK;
	}

	assert(phase == TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER);
	assert(status == TR_RPC_STATUS_OK);
	assert(method_id == 1U);
	assert(tr_rpc_call_set_trailing_metadata(
		       call, "server-trailer", "done-v1", 7U) == TR_OK);

	pthread_mutex_lock(&ctx->lock);
	ctx->server_post[method_id]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_STATUS_OK;
}

static enum tr_rpc_message_disposition
rpc_interceptor_stream_message(
	struct tr_rpc_call_handle call, const struct tr_rpc_message *message,
	void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	(void)call;
	assert(message != NULL);
	assert(message->bytes.len == 4U);
	assert(memcmp(message->bytes.data, "ping", 4U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->stream_messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_interceptor_stream_half_close(
	struct tr_rpc_call_handle call, void *arg)
{
	struct tr_rpc_bytes response;
	(void)arg;

	response.data = (const uint8_t *)"pong";
	response.len = 4U;
	assert(tr_rpc_call_send(call, &response) == TR_OK);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
}

static void rpc_interceptor_client_stream_event(
	struct tr_rpc_call_handle call, enum tr_rpc_call_event event,
	int status, void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	(void)call;

	if (event == TR_RPC_CALL_EVENT_OPENED) {
		pthread_mutex_lock(&ctx->lock);
		ctx->stream_opened++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return;
	}
	if (event != TR_RPC_CALL_EVENT_FINISHED)
		return;

	pthread_mutex_lock(&ctx->lock);
	ctx->client_post_before_stream_callback =
		ctx->client_post[1] == 1U;
	ctx->stream_status = status;
	ctx->stream_finished++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int rpc_interceptor_unary_handler(
	struct tr_rpc_call_handle call, const struct tr_rpc_bytes *request,
	struct tr_rpc_unary_response *response, void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	(void)call;
	(void)request;
	(void)response;

	pthread_mutex_lock(&ctx->lock);
	ctx->unary_handler_calls++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static void rpc_interceptor_unary_result(
	struct tr_rpc_call_handle call, int status,
	const struct tr_rpc_bytes *response, void *arg)
{
	struct rpc_interceptor_test_ctx *ctx =
		(struct rpc_interceptor_test_ctx *)arg;
	(void)call;
	assert(response != NULL);
	assert(response->len == 0U);

	pthread_mutex_lock(&ctx->lock);
	ctx->client_post_before_unary_callback =
		ctx->client_post[2] == 1U;
	ctx->unary_status = status;
	ctx->unary_results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_rpc_interceptor_v1(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc stream_method;
	struct tr_rpc_method_desc unary_method;
	struct tr_rpc_stream_handlers stream_handlers;
	struct tr_rpc_call_callbacks callbacks;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle stream_call;
	struct tr_rpc_call_handle unary_call;
	struct tr_rpc_bytes request;
	struct rpc_interceptor_test_ctx ctx;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4U;
	reactor_config.command_capacity = 128U;
	reactor_config.tx_item_capacity = 64U;
	reactor_config.control_tx_item_capacity = 32U;
	reactor_config.rx_buffer_count = 32U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 16U;
	channel_config.initial_window_bytes = 64U * 1024U;
	channel_config.window_update_threshold_bytes = 1024U;
	assert(tr_channel_create(
		       &channel_config, client_conn, client_conn,
		       NULL, NULL, NULL, NULL, &client_channel) == TR_OK);
	channel_config.role = TR_CHANNEL_SERVER;
	assert(tr_channel_create(
		       &channel_config, server_conn, server_conn,
		       NULL, NULL, NULL, NULL, &server_channel) == TR_OK);
	wait_channel_lane_up(client_channel, TR_LANE_CONTROL);
	wait_channel_lane_up(server_channel, TR_LANE_CONTROL);

	assert(tr_buffer_pool_init(&rpc_pool, 32U, 4096U) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 4U;
	rpc_config.max_calls = 8U;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_threads = 1U;
	rpc_config.executor_queue_capacity = 32U;
	rpc_config.interceptor.fn = rpc_client_interceptor;
	rpc_config.interceptor.arg = &ctx;
	assert(tr_rpc_endpoint_create(
		       client_channel, &rpc_config, &client_rpc) == TR_OK);

	rpc_config.role = TR_RPC_SERVER;
	rpc_config.interceptor.fn = rpc_server_interceptor;
	rpc_config.interceptor.arg = &ctx;
	assert(tr_rpc_endpoint_create(
		       server_channel, &rpc_config, &server_rpc) == TR_OK);

	memset(&stream_method, 0, sizeof(stream_method));
	stream_method.service_id = 77U;
	stream_method.method_id = 1U;
	stream_method.request_cardinality = TR_RPC_MANY;
	stream_method.response_cardinality = TR_RPC_ONE;
	stream_method.request_codec_id = TR_RPC_CODEC_RAW;
	stream_method.response_codec_id = TR_RPC_CODEC_RAW;
	stream_method.lane = TR_LANE_CONTROL;
	stream_method.max_request_bytes = 128U;
	stream_method.max_response_bytes = 128U;
	assert(tr_rpc_register_method(
		       client_rpc, &stream_method, NULL, NULL) == TR_OK);
	memset(&stream_handlers, 0, sizeof(stream_handlers));
	stream_handlers.on_message = rpc_interceptor_stream_message;
	stream_handlers.on_half_close = rpc_interceptor_stream_half_close;
	assert(tr_rpc_register_stream_method(
		       server_rpc, &stream_method, &stream_handlers, &ctx) == TR_OK);

	unary_method = stream_method;
	unary_method.method_id = 2U;
	unary_method.request_cardinality = TR_RPC_ONE;
	unary_method.response_cardinality = TR_RPC_ONE;
	assert(tr_rpc_register_method(
		       client_rpc, &unary_method, NULL, NULL) == TR_OK);
	assert(tr_rpc_register_method(
		       server_rpc, &unary_method,
		       rpc_interceptor_unary_handler, &ctx) == TR_OK);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_event = rpc_interceptor_client_stream_event;
	callbacks.arg = &ctx;
	assert(tr_rpc_call_start(
		       client_rpc, 77U, 1U, &callbacks, &stream_call) == TR_OK);
	wait_rpc_interceptor_counter(&ctx, &ctx.stream_opened, 1U);
	request.data = (const uint8_t *)"ping";
	request.len = 4U;
	assert(tr_rpc_call_send(stream_call, &request) == TR_OK);
	assert(tr_rpc_call_close_send(stream_call) == TR_OK);

	wait_rpc_interceptor_counter(&ctx, &ctx.stream_messages, 1U);
	wait_rpc_interceptor_counter(&ctx, &ctx.stream_finished, 1U);
	wait_rpc_handle_stale(stream_call);

	request.data = (const uint8_t *)"deny";
	request.len = 4U;
	assert(tr_rpc_unary_call(
		       client_rpc, 77U, 2U, &request,
		       rpc_interceptor_unary_result, &ctx, &unary_call) == TR_OK);
	wait_rpc_interceptor_counter(&ctx, &ctx.unary_results, 1U);
	wait_rpc_handle_stale(unary_call);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.client_pre[1] == 1U);
	assert(ctx.server_pre[1] == 1U);
	assert(ctx.server_post[1] == 1U);
	assert(ctx.client_post[1] == 1U);
	assert(ctx.stream_status == TR_RPC_STATUS_OK);
	assert(ctx.trailer_seen == 1);
	assert(ctx.client_post_before_stream_callback == 1);

	assert(ctx.client_pre[2] == 1U);
	assert(ctx.server_pre[2] == 1U);
	assert(ctx.server_post[2] == 0U);
	assert(ctx.client_post[2] == 1U);
	assert(ctx.unary_handler_calls == 0U);
	assert(ctx.unary_status == TR_RPC_STATUS_PERMISSION_DENIED);
	assert(ctx.reject_metadata_seen == 1);
	assert(ctx.client_post_before_unary_callback == 1);
	assert(ctx.auth_seen == 1);
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&rpc_pool, 32U);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct rpc_executor_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;

	unsigned unary_active;
	unsigned unary_max_active;
	unsigned unary_results;

	unsigned stream_active;
	unsigned stream_max_active;
	unsigned stream_messages;
	unsigned stream_opened;
	unsigned stream_finished;
	unsigned stream_expected_seq;
	int stream_order_error;
};

static int rpc_executor_parallel_unary_handler(
	struct tr_rpc_call_handle call, const struct tr_rpc_bytes *request,
	struct tr_rpc_unary_response *response, void *arg)
{
	static const uint8_t reply[] = "parallel-ok";
	struct rpc_executor_test_ctx *ctx = (struct rpc_executor_test_ctx *)arg;
	struct timespec deadline;
	int ret = 0;

	(void)call;
	assert(request != NULL);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 2;

	pthread_mutex_lock(&ctx->lock);
	ctx->unary_active++;
	if (ctx->unary_active > ctx->unary_max_active)
		ctx->unary_max_active = ctx->unary_active;
	pthread_cond_broadcast(&ctx->cond);

	while (ctx->unary_active < 2U && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);

	ctx->unary_active--;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void rpc_executor_parallel_result(struct tr_rpc_call_handle call,
					 int status,
					 const struct tr_rpc_bytes *response,
					 void *arg)
{
	struct rpc_executor_test_ctx *ctx = (struct rpc_executor_test_ctx *)arg;

	(void)call;
	assert(status == TR_RPC_STATUS_OK);
	assert(response != NULL);
	assert(response->len == 11U);
	assert(memcmp(response->data, "parallel-ok", 11U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->unary_results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static enum tr_rpc_message_disposition
rpc_executor_serial_message(struct tr_rpc_call_handle call,
			    const struct tr_rpc_message *message, void *arg)
{
	struct rpc_executor_test_ctx *ctx = (struct rpc_executor_test_ctx *)arg;
	struct timespec delay;
	unsigned seq;

	(void)call;
	assert(message != NULL);
	assert(message->bytes.len == 1U);
	seq = message->bytes.data[0];

	pthread_mutex_lock(&ctx->lock);
	ctx->stream_active++;
	if (ctx->stream_active > ctx->stream_max_active)
		ctx->stream_max_active = ctx->stream_active;
	if (seq != ctx->stream_expected_seq)
		ctx->stream_order_error = 1;
	pthread_mutex_unlock(&ctx->lock);

	delay.tv_sec = 0;
	delay.tv_nsec = 20L * 1000L * 1000L;
	(void)nanosleep(&delay, NULL);

	pthread_mutex_lock(&ctx->lock);
	ctx->stream_expected_seq++;
	ctx->stream_messages++;
	assert(ctx->stream_active != 0U);
	ctx->stream_active--;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	return TR_RPC_MESSAGE_RELEASE;
}

static void rpc_executor_serial_half_close(struct tr_rpc_call_handle call,
					   void *arg)
{
	struct rpc_executor_test_ctx *ctx = (struct rpc_executor_test_ctx *)arg;
	struct tr_rpc_bytes reply;

	(void)ctx;
	reply.data = (const uint8_t *)"done";
	reply.len = 4U;
	assert(tr_rpc_call_send(call, &reply) == TR_OK);
	assert(tr_rpc_call_finish(call, TR_RPC_STATUS_OK) == TR_OK);
}

static void rpc_executor_serial_client_event(struct tr_rpc_call_handle call,
					     enum tr_rpc_call_event event,
					     int status, void *arg)
{
	struct rpc_executor_test_ctx *ctx = (struct rpc_executor_test_ctx *)arg;

	(void)call;
	if (event == TR_RPC_CALL_EVENT_OPENED) {
		pthread_mutex_lock(&ctx->lock);
		ctx->stream_opened++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return;
	}

	if (event != TR_RPC_CALL_EVENT_FINISHED)
		return;

	assert(status == TR_RPC_STATUS_OK);
	pthread_mutex_lock(&ctx->lock);
	ctx->stream_finished++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static enum tr_rpc_message_disposition
rpc_executor_serial_client_message(struct tr_rpc_call_handle call,
				   const struct tr_rpc_message *message,
				   void *arg)
{
	(void)call;
	(void)arg;
	assert(message != NULL);
	assert(message->bytes.len == 4U);
	assert(memcmp(message->bytes.data, "done", 4U) == 0);
	return TR_RPC_MESSAGE_RELEASE;
}

static void wait_rpc_executor_counter(struct rpc_executor_test_ctx *ctx,
				      unsigned *value, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 8;

	pthread_mutex_lock(&ctx->lock);
	while (*value < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(*value >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_rpc_multithread_executor_per_call_serialization(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_rpc_method_desc unary_method;
	struct tr_rpc_method_desc stream_method;
	struct tr_rpc_stream_handlers stream_handlers;
	struct tr_rpc_call_callbacks callbacks;
	struct tr_reactor *reactor = NULL;
	struct tr_channel *client_channel = NULL;
	struct tr_channel *server_channel = NULL;
	struct tr_rpc_endpoint *client_rpc = NULL;
	struct tr_rpc_endpoint *server_rpc = NULL;
	struct tr_conn_handle client_conn;
	struct tr_conn_handle server_conn;
	struct tr_buffer_pool rpc_pool;
	struct tr_rpc_call_handle call_a;
	struct tr_rpc_call_handle call_b;
	struct tr_rpc_call_handle stream_call;
	struct tr_rpc_bytes message;
	struct rpc_executor_test_ctx ctx;
	uint8_t seq_payload;
	unsigned i;
	int client_fd;
	int server_fd;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	make_tcp_pair(&client_fd, &server_fd);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 4;
	reactor_config.command_capacity = 256;
	reactor_config.tx_item_capacity = 128;
	reactor_config.control_tx_item_capacity = 64;
	reactor_config.rx_buffer_count = 128;
	reactor_config.rx_buffer_size = 4096;
	reactor_config.max_payload_len = 4096;
	reactor_config.rx_budget_bytes = 256U * 1024U;
	reactor_config.tx_budget_bytes = 256U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL, &reactor) ==
	       TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, client_fd, &client_conn) == TR_OK);
	assert(tr_reactor_adopt_fd(reactor, server_fd, &server_conn) == TR_OK);

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 64;
	channel_config.initial_window_bytes = 64U * 1024U;
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
	wait_channel_lane_up(client_channel, TR_LANE_BULK);
	wait_channel_lane_up(server_channel, TR_LANE_BULK);

	assert(tr_buffer_pool_init(&rpc_pool, 128, 4096) == TR_OK);
	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_CLIENT;
	rpc_config.max_methods = 8;
	rpc_config.max_calls = 32;
	rpc_config.message_pool = &rpc_pool;
	rpc_config.executor_queue_capacity = 128;
	rpc_config.executor_threads = 4;
	assert(tr_rpc_endpoint_create(client_channel, &rpc_config,
				      &client_rpc) == TR_OK);
	rpc_config.role = TR_RPC_SERVER;
	assert(tr_rpc_endpoint_create(server_channel, &rpc_config,
				      &server_rpc) == TR_OK);

	memset(&unary_method, 0, sizeof(unary_method));
	unary_method.service_id = 5;
	unary_method.method_id = 1;
	unary_method.request_cardinality = TR_RPC_ONE;
	unary_method.response_cardinality = TR_RPC_ONE;
	unary_method.request_codec_id = TR_RPC_CODEC_RAW;
	unary_method.response_codec_id = TR_RPC_CODEC_RAW;
	unary_method.lane = TR_LANE_CONTROL;
	unary_method.max_request_bytes = 1024;
	unary_method.max_response_bytes = 1024;
	assert(tr_rpc_register_method(client_rpc, &unary_method, NULL, NULL) ==
	       TR_OK);
	assert(tr_rpc_register_method(server_rpc, &unary_method,
				      rpc_executor_parallel_unary_handler,
				      &ctx) == TR_OK);

	stream_method = unary_method;
	stream_method.method_id = 2;
	stream_method.request_cardinality = TR_RPC_MANY;
	assert(tr_rpc_register_method(client_rpc, &stream_method, NULL, NULL) ==
	       TR_OK);
	memset(&stream_handlers, 0, sizeof(stream_handlers));
	stream_handlers.on_message = rpc_executor_serial_message;
	stream_handlers.on_half_close = rpc_executor_serial_half_close;
	assert(tr_rpc_register_stream_method(server_rpc, &stream_method,
					     &stream_handlers, &ctx) == TR_OK);

	message.data = (const uint8_t *)"work";
	message.len = 4U;
	assert(tr_rpc_unary_call(client_rpc, 5, 1, &message,
				 rpc_executor_parallel_result, &ctx,
				 &call_a) == TR_OK);
	assert(tr_rpc_unary_call(client_rpc, 5, 1, &message,
				 rpc_executor_parallel_result, &ctx,
				 &call_b) == TR_OK);
	wait_rpc_executor_counter(&ctx, &ctx.unary_results, 2);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.unary_max_active >= 2U);
	pthread_mutex_unlock(&ctx.lock);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_message = rpc_executor_serial_client_message;
	callbacks.on_event = rpc_executor_serial_client_event;
	callbacks.arg = &ctx;
	assert(tr_rpc_call_start(client_rpc, 5, 2, &callbacks, &stream_call) ==
	       TR_OK);
	wait_rpc_executor_counter(&ctx, &ctx.stream_opened, 1);

	for (i = 0; i < 8U; ++i) {
		seq_payload = (uint8_t)i;
		message.data = &seq_payload;
		message.len = 1U;
		assert(tr_rpc_call_send(stream_call, &message) == TR_OK);
	}
	assert(tr_rpc_call_close_send(stream_call) == TR_OK);
	wait_rpc_executor_counter(&ctx, &ctx.stream_messages, 8);
	wait_rpc_executor_counter(&ctx, &ctx.stream_finished, 1);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.stream_max_active == 1U);
	assert(ctx.stream_order_error == 0);
	assert(ctx.stream_expected_seq == 8U);
	pthread_mutex_unlock(&ctx.lock);

	wait_for_pool_full(&rpc_pool, 128);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_rpc_endpoint_destroy(client_rpc);
	tr_rpc_endpoint_destroy(server_rpc);
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
	tr_buffer_pool_destroy(&rpc_pool);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
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

	/* Suppress peer PONG generation to exercise the timeout path. */
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
	tr_channel_destroy(client_channel);
	tr_channel_destroy(server_channel);
	tr_reactor_destroy(reactor);
}

struct facade_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_server *server;
	int server_drain_from_worker_ret;
	unsigned server_calls;
	unsigned client_results;
	unsigned client_pre;
	unsigned server_pre;
	unsigned server_post;
	unsigned client_post;
	int client_status;
	char request[64];
	uint32_t request_len;
	char response[64];
	uint32_t response_len;
};

static int facade_test_unary_handler(struct tr_rpc_call_handle call,
				     const struct tr_rpc_bytes *request,
				     struct tr_rpc_unary_response *response,
				     void *arg)
{
	struct facade_test_ctx *ctx = (struct facade_test_ctx *)arg;
	static const uint8_t reply[] = "facade-pong";
	(void)call;

	if (ctx->server)
		ctx->server_drain_from_worker_ret =
			tr_server_drain(ctx->server, 100U);

	pthread_mutex_lock(&ctx->lock);
	ctx->server_calls++;
	ctx->request_len = request->len;
	assert(request->len < sizeof(ctx->request));
	memcpy(ctx->request, request->data, request->len);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void facade_test_result(struct tr_rpc_call_handle call, int status,
			       const struct tr_rpc_bytes *response, void *arg)
{
	struct facade_test_ctx *ctx = (struct facade_test_ctx *)arg;
	(void)call;

	pthread_mutex_lock(&ctx->lock);
	ctx->client_results++;
	ctx->client_status = status;
	ctx->response_len = response ? response->len : 0U;
	assert(ctx->response_len < sizeof(ctx->response));
	if (response && response->len)
		memcpy(ctx->response, response->data, response->len);
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int facade_test_interceptor(
	struct tr_rpc_call_handle call, enum tr_rpc_interceptor_phase phase,
	int status, void *arg)
{
	struct facade_test_ctx *ctx = (struct facade_test_ctx *)arg;
	struct tr_rpc_context context;

	memset(&context, 0, sizeof(context));
	assert(tr_rpc_call_get_context(call, &context) == TR_OK);
	assert(context.service_id == 77U);
	assert(context.method_id == 1U);

	pthread_mutex_lock(&ctx->lock);
	switch (phase) {
	case TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL:
		assert(status == TR_RPC_STATUS_OK);
		ctx->client_pre++;
		break;
	case TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER:
		assert(status == TR_RPC_STATUS_OK);
		ctx->server_pre++;
		break;
	case TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER:
		assert(status == TR_RPC_STATUS_OK);
		ctx->server_post++;
		break;
	case TR_RPC_INTERCEPTOR_CLIENT_POST_CALL:
		assert(status == TR_RPC_STATUS_OK);
		ctx->client_post++;
		break;
	default:
		assert(!"unexpected facade interceptor phase");
	}
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_STATUS_OK;
}

static void test_client_server_facade_unary(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct tr_server_stats server_stats;
	struct tr_rpc_semantic_stats client_semantic;
	struct tr_rpc_semantic_stats server_semantic;
	struct facade_test_ctx ctx;
	struct timespec deadline;
	uint16_t port = 0;
	unsigned nodelay_before;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	server_config.interceptor.fn = facade_test_interceptor;
	server_config.interceptor.arg = &ctx;
	tr_facade_tuning_init(&tuning);
	tuning.observability_flags = TR_OBSERVABILITY_TIMING;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 77U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;
	assert(tr_server_register_method(server, &method,
					 facade_test_unary_handler,
					 &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);
	ctx.server = server;

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;
	client_config.interceptor.fn = facade_test_interceptor;
	client_config.interceptor.arg = &ctx;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	nodelay_before = tcp_nodelay_probe_read();
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tcp_nodelay_probe_read() == nodelay_before + 2U);
	assert(tr_client_register_method(client, &method) == TR_OK);

	request.data = (const uint8_t *)"facade-ping";
	request.len = 11U;
	assert(tr_client_unary_call(client, 77U, 1U, &request,
				    facade_test_result, &ctx, &call) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx.lock);
	while ((ctx.server_calls < 1U || ctx.client_results < 1U) &&
	       ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.server_calls == 1U);
	assert(ctx.client_results == 1U);
	assert(ctx.client_status == TR_RPC_STATUS_OK);
	assert(ctx.server_drain_from_worker_ret == TR_ERR_STATE);
	assert(ctx.request_len == 11U);
	assert(memcmp(ctx.request, "facade-ping", 11U) == 0);
	assert(ctx.response_len == 11U);
	assert(memcmp(ctx.response, "facade-pong", 11U) == 0);
	pthread_mutex_unlock(&ctx.lock);

	/*
	 * max_peers=1 verifies that a disconnected peer is reclaimed at runtime.
	 * Reuse one client object across transient connect rejection as well: a
	 * failed connect attempt must roll all session state back to INIT.
	 */
	tr_client_destroy(client);
	client = NULL;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	{
		unsigned attempt;
		for (attempt = 0; attempt < 100U; ++attempt) {
			ret = tr_client_connect(client, "127.0.0.1", port);
			if (ret == TR_OK)
				break;
			assert(ret != TR_ERR_STATE);
			{
				struct timespec pause_time;
				pause_time.tv_sec = 0;
				pause_time.tv_nsec = 10000000L;
				nanosleep(&pause_time, NULL);
			}
		}
		assert(ret == TR_OK);
	}
	assert(tr_client_register_method(client, &method) == TR_OK);
	assert(tr_client_unary_call(client, 77U, 1U, &request,
				    facade_test_result, &ctx, &call) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	ret = 0;
	pthread_mutex_lock(&ctx.lock);
	while ((ctx.server_calls < 2U || ctx.client_results < 2U) &&
	       ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.server_calls == 2U);
	assert(ctx.client_results == 2U);
	assert(ctx.client_status == TR_RPC_STATUS_OK);
	assert(ctx.client_pre == 2U);
	assert(ctx.server_pre == 2U);
	assert(ctx.server_post == 2U);
	assert(ctx.client_post == 2U);
	pthread_mutex_unlock(&ctx.lock);

	{
		int drain_ret = tr_client_begin_drain(client);
		assert(drain_ret == TR_OK || drain_ret == TR_AGAIN);
	}
	assert(tr_client_wait_drained(client, 2000U) == TR_OK);
	assert(tr_server_drain(server, 2000U) == TR_OK);

	assert(tr_server_get_stats(server, &server_stats) == TR_OK);
	assert(server_stats.max_peers == 1U);
	assert(server_stats.peers_peak == 1U);
	assert(server_stats.peers_current == 1U);
	assert(server_stats.peers_ready_current == 1U);
	assert(server_stats.peers_reaping_current == 0U);
	assert(server_stats.peers_ready_total == 2U);
	assert(server_stats.peers_reaped_total == 1U);
	assert(server_stats.rpc.calls_started == 2U);
	assert(server_stats.rpc.executor_enqueued_tasks >= 2U);
	assert(server_stats.rpc.executor_taken_tasks <=
	       server_stats.rpc.executor_enqueued_tasks);
	assert(server_stats.rpc.executor_queue_wait_ns.samples ==
	       server_stats.rpc.executor_taken_tasks);
	assert(server_stats.rpc.executor_handler_ns.samples <=
	       server_stats.rpc.executor_taken_tasks);
	assert(server_stats.rpc.executor_handler_ns.samples >= 2U);
	assert(server_stats.channel.streams_opened >= 2U);
	assert(server_stats.rpc_message_pool.peak != 0U);
	assert(server_stats.reactor.rx_buffer_pool.peak != 0U);
	assert(server_stats.reactor.tx_item_pool.peak != 0U ||
	       server_stats.reactor.control_tx_item_pool.peak != 0U);
	assert(server_stats.reactor.observability_flags ==
	       TR_OBSERVABILITY_TIMING);
	assert(server_stats.reactor.turn_busy_ns.samples != 0U);

	memset(&client_semantic, 0, sizeof(client_semantic));
	memset(&server_semantic, 0, sizeof(server_semantic));
	assert(tr_client_get_rpc_semantic_stats(
		       client, &client_semantic) == TR_OK);
	assert(tr_server_get_rpc_semantic_stats(
		       server, &server_semantic) == TR_OK);
	/* client is the second Client object; Server aggregates the retired first peer. */
	assert(client_semantic.calls_started == 1U);
	assert(client_semantic.calls_finished == 1U);
	assert(client_semantic.calls_inflight == 0U);
	assert(client_semantic.final_status[TR_RPC_STATUS_OK] == 1U);
	assert(server_semantic.calls_started == 2U);
	assert(server_semantic.calls_finished == 2U);
	assert(server_semantic.calls_inflight == 0U);
	assert(server_semantic.final_status[TR_RPC_STATUS_OK] == 2U);

	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}


static void test_server_multi_shard_reuseport_facade(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *clients[4] = { NULL, NULL, NULL, NULL };
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle calls[4];
	struct tr_server_stats stats;
	struct facade_test_ctx ctx;
	struct timespec deadline;
	uint16_t port = 0U;
	unsigned i;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.shard_count = 2U;
	server_config.max_peers = 8U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;

	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 2U;
	tuning.command_capacity = 32U;
	tuning.tx_item_capacity = 16U;
	tuning.control_tx_item_capacity = 8U;
	tuning.rx_buffer_count = 8U;
	tuning.rpc_message_pool_count = 8U;
	tuning.reassembly_pool_count = 4U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 78U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;
	assert(tr_server_register_method(server, &method,
					 facade_test_unary_handler, &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;

	request.data = (const uint8_t *)"facade-ping";
	request.len = 11U;

	for (i = 0; i < 4U; ++i) {
		assert(tr_client_create(&client_config, &clients[i]) == TR_OK);
		assert(tr_client_connect(clients[i], "127.0.0.1", port) == TR_OK);
		assert(tr_client_register_method(clients[i], &method) == TR_OK);
		assert(tr_client_unary_call(
			       clients[i], 78U, 1U, &request,
			       facade_test_result, &ctx, &calls[i]) == TR_OK);
	}

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx.lock);
	while ((ctx.server_calls < 4U || ctx.client_results < 4U) &&
	       ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.server_calls == 4U);
	assert(ctx.client_results == 4U);
	pthread_mutex_unlock(&ctx.lock);

	memset(&stats, 0, sizeof(stats));
	assert(tr_server_get_stats(server, &stats) == TR_OK);
	assert(stats.shard_count == 2U);
	assert(stats.max_peers == 8U);
	assert(stats.peers_current == 4U);
	assert(stats.peers_ready_current == 4U);
	assert(stats.rpc.executor_threads == 2U);
	assert(stats.rpc.calls_started == 4U);
	assert(stats.rpc_message_pool.capacity == 8U);
	assert(stats.reassembly_pool.capacity == 4U);
	assert(stats.reactor.command_queue.capacity == 32U);
	assert(stats.reactor.tx_item_pool.capacity == 16U);
	assert(stats.reactor.control_tx_item_pool.capacity == 8U);
	assert(stats.reactor.rx_buffer_pool.capacity == 8U);

	for (i = 0; i < 4U; ++i) {
		int drain_ret = tr_client_begin_drain(clients[i]);

		assert(drain_ret == TR_OK || drain_ret == TR_AGAIN);
		assert(tr_client_wait_drained(clients[i], 5000U) == TR_OK);
	}
	assert(tr_server_drain(server, 5000U) == TR_OK);

	for (i = 0; i < 4U; ++i)
		tr_client_destroy(clients[i]);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_client_server_facade_nodelay_policy(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	uint16_t port = 0;
	unsigned before;

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.tcp_nodelay = TR_TCP_NODELAY_DISABLED;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	assert(tr_server_create(&server_config, &server) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	client_config.tcp_nodelay = TR_TCP_NODELAY_DISABLED;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;
	assert(tr_client_create(&client_config, &client) == TR_OK);

	before = tcp_nodelay_probe_read();
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tcp_nodelay_probe_read() == before);

	tr_client_destroy(client);
	client = NULL;
	tr_server_destroy(server);
	server = NULL;

	tr_client_config_init(&client_config);
	client_config.tcp_nodelay = (enum tr_tcp_nodelay_policy)99;
	assert(tr_client_create(&client_config, &client) == TR_ERR_INVALID);
	assert(client == NULL);

	tr_server_config_init(&server_config);
	server_config.tcp_nodelay = (enum tr_tcp_nodelay_policy)99;
	assert(tr_server_create(&server_config, &server) == TR_ERR_INVALID);
	assert(server == NULL);
}

struct shared_executor_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned entered;
	unsigned active;
	unsigned max_active;
	unsigned results;
	int release;
};

struct server_stats_poll_ctx {
	struct tr_server *server;
	_Atomic int stop;
	_Atomic unsigned samples;
	_Atomic unsigned failures;
};

static void *server_stats_poll_main(void *arg)
{
	struct server_stats_poll_ctx *ctx =
		(struct server_stats_poll_ctx *)arg;

	while (!atomic_load_explicit(&ctx->stop, memory_order_acquire)) {
		struct tr_server_stats stats;
		struct timespec pause_time;

		memset(&stats, 0, sizeof(stats));
		if (tr_server_get_stats(ctx->server, &stats) == TR_OK)
			(void)atomic_fetch_add_explicit(
				&ctx->samples, 1U, memory_order_relaxed);
		else
			(void)atomic_fetch_add_explicit(
				&ctx->failures, 1U, memory_order_relaxed);

		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 1000000L;
		nanosleep(&pause_time, NULL);
	}
	return NULL;
}

static int shared_executor_test_handler(struct tr_rpc_call_handle call,
					const struct tr_rpc_bytes *request,
					struct tr_rpc_unary_response *response,
					void *arg)
{
	struct shared_executor_test_ctx *ctx =
		(struct shared_executor_test_ctx *)arg;
	static const uint8_t reply[] = "shared-ok";
	(void)call;
	(void)request;
	assert(tr_rpc_in_worker_context());

	pthread_mutex_lock(&ctx->lock);
	ctx->entered++;
	ctx->active++;
	if (ctx->active > ctx->max_active)
		ctx->max_active = ctx->active;
	pthread_cond_broadcast(&ctx->cond);
	while (!ctx->release)
		pthread_cond_wait(&ctx->cond, &ctx->lock);
	ctx->active--;
	pthread_mutex_unlock(&ctx->lock);

	response->status = TR_RPC_STATUS_OK;
	response->message.data = reply;
	response->message.len = (uint32_t)(sizeof(reply) - 1U);
	return TR_OK;
}

static void shared_executor_test_result(struct tr_rpc_call_handle call,
				       int status,
				       const struct tr_rpc_bytes *response,
				       void *arg)
{
	struct shared_executor_test_ctx *ctx =
		(struct shared_executor_test_ctx *)arg;
	(void)call;
	assert(tr_rpc_in_worker_context());
	assert(status == TR_RPC_STATUS_OK);
	assert(response != NULL);
	assert(response->len == 9U);
	assert(memcmp(response->data, "shared-ok", 9U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static unsigned test_linux_thread_count(void)
{
	DIR *dir;
	struct dirent *entry;
	unsigned count = 0;

	dir = opendir("/proc/self/task");
	assert(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		if (entry->d_name[0] >= '0' && entry->d_name[0] <= '9')
			count++;
	}
	assert(closedir(dir) == 0);
	return count;
}

static int test_connect_ipv4_port(uint16_t port, int *out_fd)
{
	struct pollfd pfd;
	int fd TR_AUTO(tr_fd_cleanup) = -1;
	int ret;

	ret = tr_tcp_connect_ipv4("127.0.0.1", port, &fd);
	if (ret == TR_OK) {
		*out_fd = tr_fd_take(&fd);
		return TR_OK;
	}
	if (ret != TR_IN_PROGRESS)
		return ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLOUT;
	do {
		ret = poll(&pfd, 1, 5000);
	} while (ret < 0 && errno == EINTR);
	if (ret <= 0)
		return ret == 0 ? TR_ERR_TIMEOUT : TR_ERR_SYS;

	ret = tr_tcp_finish_connect(fd);
	if (ret != TR_OK)
		return ret;

	*out_fd = tr_fd_take(&fd);
	return TR_OK;
}

static void test_client_runtime_thread_bound(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning server_tuning;
	struct tr_facade_tuning client_tuning;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	uint16_t port = 0;
	unsigned before;
	unsigned after;

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&server_tuning);
	server_tuning.executor_threads = 1U;
	assert(tr_server_create_with_tuning(
		       &server_config, &server_tuning, &server) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 20U;
	client_config.keepalive_timeout_ms = 200U;
	client_config.enable_reconnect = 0;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&client_tuning);
	client_tuning.executor_threads = 1U;

	assert(tr_client_create_with_tuning(
		       &client_config, &client_tuning, &client) == TR_OK);

	/*
	 * Client create 只启动 Reactor。connect 之后只允许新增一个 RPC
	 * executor worker；deadline 与 keepalive 都是 Reactor-local timer，
	 * 不能再额外创建 timer thread。
	 */
	before = test_linux_thread_count();
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);

	{
		struct timespec pause_time;
		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 100000000L;
		nanosleep(&pause_time, NULL);
	}

	after = test_linux_thread_count();
	assert(after <= before + 1U);

	tr_client_destroy(client);
	tr_server_destroy(server);
}

static void test_server_runtime_thread_bound(void)
{
	struct tr_server_config server_config;
	struct tr_facade_tuning tuning;
	struct tr_reactor_config reactor_config;
	struct tr_channel_config channel_config;
	struct tr_server *server = NULL;
	struct tr_reactor *client_reactor = NULL;
	struct tr_channel *channels[4] = { NULL, NULL, NULL, NULL };
	struct tr_conn_handle connections[4];
	uint16_t port = 0;
	unsigned before;
	unsigned after;
	unsigned i;

	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.keepalive_interval_ms = 20U;
	server_config.keepalive_timeout_ms = 200U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 2U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = 8U;
	reactor_config.command_capacity = 64U;
	reactor_config.tx_item_capacity = 32U;
	reactor_config.control_tx_item_capacity = 16U;
	reactor_config.rx_buffer_count = 32U;
	reactor_config.rx_buffer_size = 4096U;
	reactor_config.max_payload_len = 4096U;
	reactor_config.rx_budget_bytes = 64U * 1024U;
	reactor_config.tx_budget_bytes = 64U * 1024U;
	assert(tr_reactor_create(&reactor_config, NULL, NULL, NULL,
				 &client_reactor) == TR_OK);
	assert(tr_reactor_start(client_reactor) == TR_OK);

	/*
	 * baseline 已包含 Server Reactor/accept/reaper/shared executor 和
	 * Client Reactor。后续只增加 peer/Channel，不应再按 peer 数量创建
	 * deadline/keepalive thread。
	 */
	before = test_linux_thread_count();

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_CLIENT;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = 8U;
	channel_config.initial_window_bytes = 4096U;
	channel_config.window_update_threshold_bytes = 1024U;

	for (i = 0; i < 4U; ++i) {
		int fd = -1;

		assert(test_connect_ipv4_port(port, &fd) == TR_OK);
		assert(tr_reactor_adopt_fd(client_reactor, fd,
					   &connections[i]) == TR_OK);
		assert(tr_channel_create(&channel_config, connections[i],
					 connections[i], NULL, NULL, NULL, NULL,
					 &channels[i]) == TR_OK);
		wait_channel_lane_up(channels[i], TR_LANE_CONTROL);
	}

	{
		struct timespec pause_time;
		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 300000000L;
		nanosleep(&pause_time, NULL);
	}

	after = test_linux_thread_count();
	assert(after <= before + 1U);

	assert(tr_reactor_stop(client_reactor) == TR_OK);
	for (i = 0; i < 4U; ++i)
		tr_channel_destroy(channels[i]);
	tr_reactor_destroy(client_reactor);
	tr_server_destroy(server);
}

static void test_server_shared_rpc_executor(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *clients[4] = { NULL, NULL, NULL, NULL };
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle calls[4];
	struct shared_executor_test_ctx ctx;
	struct timespec deadline;
	uint16_t port = 0;
	unsigned i;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 2U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 88U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;
	assert(tr_server_register_method(server, &method,
					 shared_executor_test_handler,
					 &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 5000U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;

	for (i = 0; i < 4U; ++i) {
		assert(tr_client_create(&client_config, &clients[i]) == TR_OK);
		assert(tr_client_connect(clients[i], "127.0.0.1", port) == TR_OK);
		assert(tr_client_register_method(clients[i], &method) == TR_OK);
	}

	request.data = (const uint8_t *)"work";
	request.len = 4U;
	for (i = 0; i < 4U; ++i)
		assert(tr_client_unary_call(clients[i], 88U, 1U, &request,
					    shared_executor_test_result, &ctx,
					    &calls[i]) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.entered < 2U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.entered == 2U);
	assert(ctx.active == 2U);
	assert(ctx.max_active == 2U);

	/*
	 * Both shared workers are blocked above. No third handler may start until
	 * one of those two workers is released.
	 */
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 1;
	ret = 0;
	while (ctx.entered < 3U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ctx.entered == 2U);

	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	ret = 0;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.results < 4U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.results == 4U);
	assert(ctx.max_active <= 2U);
	pthread_mutex_unlock(&ctx.lock);

	for (i = 0; i < 4U; ++i) {
		int drain_ret = tr_client_begin_drain(clients[i]);
		assert(drain_ret == TR_OK || drain_ret == TR_AGAIN);
		assert(tr_client_wait_drained(clients[i], 5000U) == TR_OK);
	}
	assert(tr_server_drain(server, 5000U) == TR_OK);

	for (i = 0; i < 4U; ++i)
		tr_client_destroy(clients[i]);
	tr_server_destroy(server);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct retained_rpc_message_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_rpc_message retained;
	unsigned retained_count;
};

static enum tr_rpc_message_disposition
retained_rpc_server_message(struct tr_rpc_call_handle call,
			    const struct tr_rpc_message *message, void *arg)
{
	struct retained_rpc_message_ctx *ctx =
		(struct retained_rpc_message_ctx *)arg;

	(void)call;
	assert(message != NULL);
	assert(message->bytes.len == 6U);
	assert(memcmp(message->bytes.data, "retain", 6U) == 0);

	pthread_mutex_lock(&ctx->lock);
	assert(ctx->retained_count == 0U);
	ctx->retained = *message;
	ctx->retained_count = 1U;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_RPC_MESSAGE_TAKE_OWNERSHIP;
}

static void test_retained_rpc_message_survives_peer_disconnect(void)
{
	struct retained_rpc_message_ctx ctx;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_stream_handlers handlers;
	struct tr_rpc_call_handle call;
	struct tr_rpc_bytes bytes;
	struct tr_server_stats stats;
	struct timespec deadline;
	struct timespec pause_time;
	uint16_t port = 0U;
	unsigned attempt;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 1U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 90U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_MANY;
	method.response_cardinality = TR_RPC_MANY;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;

	memset(&handlers, 0, sizeof(handlers));
	handlers.on_message = retained_rpc_server_message;
	assert(tr_server_register_stream_method(
		       server, &method, &handlers, &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 500U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_register_method(client, &method) == TR_OK);
	assert(tr_client_call_start(client, 90U, 1U, NULL, &call) == TR_OK);

	bytes.data = (const uint8_t *)"retain";
	bytes.len = 6U;
	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 10000000L;
	for (attempt = 0; attempt < 500U; ++attempt) {
		ret = tr_rpc_call_send(call, &bytes);
		if (ret == TR_OK)
			break;
		assert(ret == TR_AGAIN);
		nanosleep(&pause_time, NULL);
	}
	assert(ret == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.retained_count == 0U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.retained_count == 1U);
	pthread_mutex_unlock(&ctx.lock);

	/*
	 * Disconnect the peer while application ownership still holds the RX
	 * descriptor.  The retired Endpoint/Channel must stay alive solely because
	 * of the message lifetime pin.
	 */
	tr_client_destroy(client);
	client = NULL;

	memset(&stats, 0, sizeof(stats));
	for (attempt = 0; attempt < 500U; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 0U &&
		    stats.peers_reaping_current == 1U)
			break;
		nanosleep(&pause_time, NULL);
	}
	assert(stats.peers_current == 0U);
	assert(stats.peers_reaping_current == 1U);

	ret = tr_rpc_message_release(&ctx.retained);
	assert(ret == TR_OK || ret == TR_ERR_STALE || ret == TR_ERR_CLOSED);
	assert(ctx.retained._private[0] == (uintptr_t)0);
	assert(ctx.retained._private[1] == (uintptr_t)0);

	memset(&stats, 0, sizeof(stats));
	for (attempt = 0; attempt < 500U; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_reaping_current == 0U)
			break;
		nanosleep(&pause_time, NULL);
	}
	assert(stats.peers_reaping_current == 0U);
	assert(stats.peers_reaped_total >= 1U);

	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_server_reaping_budget_defers_detach(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *first = NULL;
	struct tr_client *second = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct shared_executor_test_ctx ctx;
	struct tr_server_stats stats;
	struct timespec deadline;
	struct timespec pause_time;
	uint16_t port = 0U;
	unsigned attempt;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 1U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 91U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;
	assert(tr_server_register_method(server, &method,
					 shared_executor_test_handler,
					 &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 500U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;

	assert(tr_client_create(&client_config, &first) == TR_OK);
	assert(tr_client_connect(first, "127.0.0.1", port) == TR_OK);
	assert(tr_client_register_method(first, &method) == TR_OK);
	request.data = (const uint8_t *)"hold";
	request.len = 4U;
	assert(tr_client_unary_call(
		       first, 91U, 1U, &request, NULL, NULL, &call) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.entered < 1U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.active == 1U);
	pthread_mutex_unlock(&ctx.lock);

	tr_client_destroy(first);
	first = NULL;

	pause_time.tv_sec = 0;
	pause_time.tv_nsec = 10000000L;
	memset(&stats, 0, sizeof(stats));
	for (attempt = 0; attempt < 500U; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 0U &&
		    stats.peers_reaping_current == 1U)
			break;
		nanosleep(&pause_time, NULL);
	}
	assert(stats.peers_current == 0U);
	assert(stats.peers_reaping_current == 1U);

	/*
	 * Slot reuse remains allowed while the first Endpoint is retiring. The
	 * second disconnected peer must stay published because the shard already
	 * uses its only retiring-object slot.
	 */
	assert(tr_client_create(&client_config, &second) == TR_OK);
	assert(tr_client_connect(second, "127.0.0.1", port) == TR_OK);
	assert(tr_client_register_method(second, &method) == TR_OK);
	tr_client_destroy(second);
	second = NULL;

	memset(&stats, 0, sizeof(stats));
	for (attempt = 0; attempt < 500U; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 1U &&
		    stats.peers_ready_current == 0U &&
		    stats.peers_reaping_current == 1U)
			break;
		nanosleep(&pause_time, NULL);
	}
	assert(stats.peers_current == 1U);
	assert(stats.peers_ready_current == 0U);
	assert(stats.peers_reaping_current == 1U);

	pthread_mutex_lock(&ctx.lock);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	/*
	 * The first finalizer returns one retiring slot and signals peer_event_fd.
	 * Owner retry then detaches/finalizes the already-disconnected second peer.
	 */
	memset(&stats, 0, sizeof(stats));
	for (attempt = 0; attempt < 1000U; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 0U &&
		    stats.peers_reaping_current == 0U &&
		    stats.peers_reaped_total >= 2U)
			break;
		nanosleep(&pause_time, NULL);
	}
	assert(stats.peers_current == 0U);
	assert(stats.peers_reaping_current == 0U);
	assert(stats.peers_reaped_total >= 2U);

	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_server_peer_refcount_drain(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_facade_tuning tuning;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct shared_executor_test_ctx ctx;
	struct server_stats_poll_ctx stats_poll;
	pthread_t stats_thread;
	struct timespec deadline;
	uint16_t port = 0;
	unsigned attempt;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	memset(&stats_poll, 0, sizeof(stats_poll));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	tr_facade_tuning_init(&tuning);
	tuning.executor_threads = 1U;
	assert(tr_server_create_with_tuning(
		       &server_config, &tuning, &server) == TR_OK);

	memset(&method, 0, sizeof(method));
	method.service_id = 89U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1024U;
	method.max_response_bytes = 1024U;
	assert(tr_server_register_method(server, &method,
					 shared_executor_test_handler,
					 &ctx) == TR_OK);
	assert(tr_server_listen(server, "127.0.0.1", 0, &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);
	stats_poll.server = server;
	atomic_init(&stats_poll.stop, 0);
	atomic_init(&stats_poll.samples, 0U);
	atomic_init(&stats_poll.failures, 0U);
	assert(pthread_create(&stats_thread, NULL, server_stats_poll_main,
			      &stats_poll) == 0);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 500U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;

	assert(tr_client_create(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
	assert(tr_client_register_method(client, &method) == TR_OK);

	request.data = (const uint8_t *)"hold";
	request.len = 4U;
	assert(tr_client_unary_call(client, 89U, 1U, &request, NULL, NULL,
				    &call) == TR_OK);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	pthread_mutex_lock(&ctx.lock);
	while (ctx.entered < 1U && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	assert(ret == 0);
	assert(ctx.active == 1U);
	pthread_mutex_unlock(&ctx.lock);

	/*
	 * Close the first peer while its server callback still owns an Endpoint
	 * task reference. Reactor owner detach may immediately retire/reuse the
	 * peer slot, but last-ref finalization must keep detached Endpoint/Channel
	 * state alive until this task releases its strong ref.
	 */
	tr_client_destroy(client);
	client = NULL;

	assert(tr_client_create(&client_config, &client) == TR_OK);
	for (attempt = 0; attempt < 100U; ++attempt) {
		ret = tr_client_connect(client, "127.0.0.1", port);
		if (ret == TR_OK)
			break;
		assert(ret != TR_ERR_STATE);
		{
			struct timespec pause_time;
			pause_time.tv_sec = 0;
			pause_time.tv_nsec = 10000000L;
			nanosleep(&pause_time, NULL);
		}
	}
	assert(ret == TR_OK);
	assert(tr_client_register_method(client, &method) == TR_OK);
	assert(tr_client_unary_call(client, 89U, 1U, &request,
				    shared_executor_test_result, &ctx,
				    &call) == TR_OK);

	/* The only shared worker is still occupied by the retired peer task. */
	{
		struct timespec pause_time;
		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 100000000L;
		nanosleep(&pause_time, NULL);
	}
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.entered == 1U);
	ctx.release = 1;
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 15;
	ret = 0;
	pthread_mutex_lock(&ctx.lock);
	while ((ctx.entered < 2U || ctx.results < 1U) && ret == 0)
		ret = pthread_cond_timedwait(&ctx.cond, &ctx.lock, &deadline);
	if (ret != 0) {
		struct tr_server_stats server_stats;
		struct tr_channel_stats client_channel_stats;
		struct tr_rpc_endpoint_stats client_rpc_stats;
		unsigned entered = ctx.entered;
		unsigned results = ctx.results;
		unsigned active = ctx.active;

		pthread_mutex_unlock(&ctx.lock);
		memset(&server_stats, 0, sizeof(server_stats));
		memset(&client_channel_stats, 0, sizeof(client_channel_stats));
		memset(&client_rpc_stats, 0, sizeof(client_rpc_stats));
		(void)tr_server_get_stats(server, &server_stats);
		(void)tr_client_get_channel_stats(client, &client_channel_stats);
		(void)tr_client_get_rpc_stats(client, &client_rpc_stats);
		fprintf(stderr,
			"peer-refcount timeout: entered=%u results=%u active=%u "
			"peers=%u ready=%u reaping=%u reaped=%llu "
			"server_calls=%llu/%llu server_exec=%llu/%llu/%llu "
			"client_streams=%u client_calls=%u/%u/%u started=%llu completed=%llu\n",
			entered, results, active,
			server_stats.peers_current,
			server_stats.peers_ready_current,
			server_stats.peers_reaping_current,
			(unsigned long long)server_stats.peers_reaped_total,
			(unsigned long long)server_stats.rpc.calls_started,
			(unsigned long long)server_stats.rpc.calls_completed,
			(unsigned long long)
				server_stats.rpc.executor_queued_tasks_current,
			(unsigned long long)
				server_stats.rpc.executor_running_tasks_current,
			(unsigned long long)
				server_stats.rpc.executor_ready_calls_current,
			client_channel_stats.active_streams,
			client_rpc_stats.opening_calls,
			client_rpc_stats.active_calls,
			client_rpc_stats.terminal_calls,
			(unsigned long long)client_rpc_stats.calls_started,
			(unsigned long long)client_rpc_stats.calls_completed);
		pthread_mutex_lock(&ctx.lock);
	}
	assert(ret == 0);
	assert(ctx.entered == 2U);
	assert(ctx.results == 1U);
	assert(ctx.max_active == 1U);
	pthread_mutex_unlock(&ctx.lock);

	{
		int drain_ret = tr_client_begin_drain(client);
		assert(drain_ret == TR_OK || drain_ret == TR_AGAIN);
	}
	assert(tr_client_wait_drained(client, 5000U) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);

	atomic_store_explicit(&stats_poll.stop, 1, memory_order_release);
	assert(pthread_join(stats_thread, NULL) == 0);
	assert(atomic_load_explicit(&stats_poll.samples,
				    memory_order_relaxed) != 0U);
	assert(atomic_load_explicit(&stats_poll.failures,
				    memory_order_relaxed) == 0U);

	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
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

	/* Dead objects cannot be resurrected or decremented below zero. */
	assert(tr_refcount_get(&ref) == TR_ERR_STATE);
	assert(tr_refcount_get_unless_zero(&ref) == 0);
	assert(tr_refcount_put(&ref) == TR_ERR_STATE);

	/* Invalid and saturation-adjacent initial states are explicit. */
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

	/*
	 * Scope cleanup is LIFO. Resource declarations must therefore follow
	 * dependency order: dependencies first, dependents later.
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
	test_rpc_method_registration_is_owner_serialized();
	test_reactor_rx_pool_backpressure();
	test_channel_deferred_hello_gate();
	test_channel_stream_id_index_collision_delete();
	test_channel_stream_slot_reuse();
	test_channel_stream_flow_control();
	test_channel_message_fragmentation_reassembly();
	test_channel_split_lane_isolation();
	test_channel_graceful_drain();
	test_channel_automatic_reconnect_shared();
	test_channel_version_negotiation_failure();
	test_rpc_wire_and_raw_codec();
	test_rpc_method_index_collisions();
	test_rpc_deadline_heap_order();
	test_rpc_call_slot_reuse();
	test_rpc_unary_raw_roundtrip();
	test_rpc_connection_replacement_semantics();
	test_rpc_large_message_fragmentation();
	test_rpc_bidi_streaming_raw_fast_path();
	test_rpc_client_and_server_stream_shapes();
	test_rpc_metadata_cancel_deadline();
	test_rpc_interceptor_v1();
	test_rpc_multithread_executor_per_call_serialization();
	test_channel_keepalive_and_diagnostics();
	test_client_server_facade_unary();
	test_server_multi_shard_reuseport_facade();
	test_client_server_facade_nodelay_policy();
	test_client_runtime_thread_bound();
	test_server_runtime_thread_bound();
	test_server_shared_rpc_executor();
	test_retained_rpc_message_survives_peer_disconnect();
	test_server_reaping_budget_defers_detach();
	test_server_peer_refcount_drain();

	puts("all transport/RPC core tests passed");
	return 0;
}
