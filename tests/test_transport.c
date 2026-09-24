#include "tr/buffer.h"
#include "tr/crc32c.h"
#include "tr/command_queue.h"
#include "tr/endian.h"
#include "tr/frame.h"
#include "tr/parser.h"
#include "tr/reactor.h"
#include "tr/rpc.h"
#include "tr/rpc_codec.h"
#include "tr/rpc_wire.h"
#include "tr/channel.h"
#include "tr/client.h"
#include "tr/server.h"
#include "tr/socket.h"
#include "tr/status.h"
#include "tr/wire.h"

#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_POOL_COUNT 4U
#define TEST_BUF_SIZE (1024U * 1024U)

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

static void test_crc32c(void)
{
	static const char s[] = "123456789";
	assert(tr_crc32c(s, 9) == 0xE3069283U);
	assert(tr_crc32c(NULL, 0) == 0U);
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

static void test_command_queue_bounded(void)
{
	struct tr_command_queue queue;
	struct tr_command in;
	struct tr_command out[2];
	int need_wake;

	assert(tr_command_queue_init(&queue, 2) == TR_OK);

	memset(&in, 0, sizeof(in));
	in.type = TR_CMD_CLOSE;
	in.slot = 1;
	assert(tr_command_queue_push(&queue, &in, &need_wake) == TR_OK);
	assert(need_wake == 1);

	in.slot = 2;
	need_wake = -1;
	assert(tr_command_queue_push(&queue, &in, &need_wake) == TR_OK);
	assert(need_wake == 0);

	in.slot = 3;
	assert(tr_command_queue_push(&queue, &in, &need_wake) == TR_AGAIN);

	assert(tr_command_queue_pop_batch(&queue, out, 1) == 1);
	assert(out[0].slot == 1);

	in.slot = 3;
	assert(tr_command_queue_push(&queue, &in, &need_wake) == TR_OK);
	assert(need_wake == 0);

	assert(tr_command_queue_pop_batch(&queue, out, 2) == 2);
	assert(out[0].slot == 2);
	assert(out[1].slot == 3);

	in.slot = 4;
	need_wake = 0;
	assert(tr_command_queue_push(&queue, &in, &need_wake) == TR_OK);
	assert(need_wake == 1);
	assert(tr_command_queue_pop_batch(&queue, out, 2) == 1);

	tr_command_queue_destroy(&queue);
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
	int client_status;
	uint8_t request[64];
	uint32_t request_len;
	uint8_t response[64];
	uint32_t response_len;
};

static void test_channel_graceful_drain(void)
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
	struct tr_stream_handle rejected;
	struct tr_buffer_pool tx_pool;
	struct tr_buffer *payload;
	struct channel_test_ctx client_ctx;
	struct channel_test_ctx server_ctx;
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

	assert(tr_channel_begin_drain(client_channel) == TR_OK);
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
	assert(tr_channel_enable_client_reconnect(client_channel,
						  &reconnect_config) == TR_OK);

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
	unsigned client_finished;
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
	if (event == TR_RPC_CALL_EVENT_OPENED)
		ctx->client_opened++;
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

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.client_finish_status == TR_RPC_STATUS_OK);
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
	unsigned finished;
	int finish_status;
};

static enum tr_rpc_message_disposition
rpc_shape_server_message(struct tr_rpc_call_handle call,
			 const struct tr_rpc_message *message, void *arg)
{
	struct rpc_shape_ctx *ctx = (struct rpc_shape_ctx *)arg;
	(void)call;
	assert(message->bytes.len != 0);

	pthread_mutex_lock(&ctx->lock);
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

	assert(tr_rpc_call_send(call, &first) == TR_OK);
	assert(tr_rpc_call_send(call, &second) == TR_OK);
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
	assert(tr_rpc_call_send(call, &only) == TR_OK);
	assert(tr_rpc_call_send(call, &only) == TR_ERR_STATE);
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
	(void)call;
	assert(message->bytes.len != 0);

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
	if (event == TR_RPC_CALL_EVENT_OPENED)
		ctx->opened++;
	else if (event == TR_RPC_CALL_EVENT_FINISHED) {
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
	struct rpc_shape_ctx server_stream_ctx;
	struct rpc_shape_ctx client_stream_ctx;
	struct tr_rpc_bytes message;
	int client_fd;
	int server_fd;

	memset(&server_stream_ctx, 0, sizeof(server_stream_ctx));
	memset(&client_stream_ctx, 0, sizeof(client_stream_ctx));
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
	assert(server_stream_ctx.finish_status == TR_RPC_STATUS_OK);

	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.on_message = rpc_shape_client_message;
	callbacks.on_event = rpc_shape_client_event;
	callbacks.arg = &client_stream_ctx;
	assert(tr_rpc_call_start(client_rpc, 3, 2, &callbacks, &call) == TR_OK);
	wait_rpc_shape_counter(&client_stream_ctx, &client_stream_ctx.opened,
			       1);

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
	assert(client_stream_ctx.finish_status == TR_RPC_STATUS_OK);

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
	unsigned server_calls;
	unsigned client_results;
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

static void test_client_server_facade_unary(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_rpc_method_desc method;
	struct tr_rpc_bytes request;
	struct tr_rpc_call_handle call;
	struct facade_test_ctx ctx;
	struct timespec deadline;
	uint16_t port = 0;
	int ret = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	server_config.limits.rpc_message_buffer_bytes = 4096U;
	server_config.limits.rpc_message_pool_count = 32U;
	server_config.limits.reassembly_pool_count = 4U;
	server_config.limits.rx_buffer_count = 32U;
	assert(tr_server_create(&server_config, &server) == TR_OK);

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

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connect_timeout_ms = 1000U;
	client_config.limits.max_frame_payload_bytes = 4096U;
	client_config.limits.max_message_bytes = 16384U;
	client_config.limits.rpc_message_buffer_bytes = 4096U;
	client_config.limits.rpc_message_pool_count = 32U;
	client_config.limits.reassembly_pool_count = 4U;
	client_config.limits.rx_buffer_count = 32U;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_OK);
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
	pthread_mutex_unlock(&ctx.lock);

	{
		int drain_ret = tr_client_begin_drain(client);
		assert(drain_ret == TR_OK || drain_ret == TR_AGAIN);
	}
	assert(tr_client_wait_drained(client, 2000U) == TR_OK);
	assert(tr_server_drain(server, 2000U) == TR_OK);

	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
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
	assert(status == TR_RPC_STATUS_OK);
	assert(response != NULL);
	assert(response->len == 9U);
	assert(memcmp(response->data, "shared-ok", 9U) == 0);

	pthread_mutex_lock(&ctx->lock);
	ctx->results++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_server_shared_rpc_executor(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
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
	server_config.limits.executor_threads = 2U;
	server_config.limits.max_frame_payload_bytes = 4096U;
	server_config.limits.max_message_bytes = 16384U;
	server_config.limits.rpc_message_buffer_bytes = 4096U;
	server_config.limits.rpc_message_pool_count = 64U;
	server_config.limits.reassembly_pool_count = 8U;
	server_config.limits.rx_buffer_count = 64U;
	assert(tr_server_create(&server_config, &server) == TR_OK);

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
	client_config.limits.rpc_message_buffer_bytes = 4096U;
	client_config.limits.rpc_message_pool_count = 32U;
	client_config.limits.reassembly_pool_count = 4U;
	client_config.limits.rx_buffer_count = 32U;

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

int main(void)
{
	test_endian();
	test_crc32c();
	test_wire_roundtrip();
	test_header_corruption();
	test_parser_one_byte_fragments();
	test_parser_payload_crc_failure();
	test_parser_pool_backpressure();
	test_payload_limit();
	test_zero_payload_control_frame();
	test_invalid_flags_and_reserved();
	test_command_queue_bounded();
	test_reactor_tcp_roundtrip();
	test_reactor_rx_pool_backpressure();
	test_channel_stream_flow_control();
	test_channel_message_fragmentation_reassembly();
	test_channel_split_lane_isolation();
	test_channel_graceful_drain();
	test_channel_automatic_reconnect_shared();
	test_channel_version_negotiation_failure();
	test_rpc_wire_and_raw_codec();
	test_rpc_unary_raw_roundtrip();
	test_rpc_connection_replacement_semantics();
	test_rpc_large_message_fragmentation();
	test_rpc_bidi_streaming_raw_fast_path();
	test_rpc_client_and_server_stream_shapes();
	test_rpc_metadata_cancel_deadline();
	test_rpc_multithread_executor_per_call_serialization();
	test_channel_keepalive_and_diagnostics();
	test_client_server_facade_unary();
	test_server_shared_rpc_executor();

	puts("all transport/RPC core tests passed");
	return 0;
}
