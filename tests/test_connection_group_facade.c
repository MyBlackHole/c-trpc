#include "tr/trpc.h"

#include "../src/pipeline_control_wire_internal.h"
#include "../src/pipeline_route_internal.h"

#include "tr/crc32c.h"
#include "tr/status.h"
#include "tr/wire.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_GROUP_ID UINT64_C(0x9911)
#define TEST_GROUP_EPOCH UINT64_C(71)
#define TEST_CONTROL_GENERATION 23U

struct public_group_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned authorized;
	unsigned messages;
	unsigned data_events;
	struct tr_connection_group_message retained;
};

static void send_all(int fd, const void *data, size_t len)
{
	const uint8_t *p = (const uint8_t *)data;

	while (len != 0U) {
		ssize_t n = send(fd, p, len, MSG_NOSIGNAL);

		if (n > 0) {
			p += (size_t)n;
			len -= (size_t)n;
			continue;
		}
		assert(n < 0 && errno == EINTR);
	}
}

static void recv_all_timeout(int fd, void *data, size_t len)
{
	uint8_t *p = (uint8_t *)data;

	while (len != 0U) {
		struct pollfd pfd;
		ssize_t n;
		int ret;

		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = fd;
		pfd.events = POLLIN | POLLHUP;
		do {
			ret = poll(&pfd, 1, 10000);
		} while (ret < 0 && errno == EINTR);
		assert(ret == 1);

		n = recv(fd, p, len, 0);
		assert(n > 0);
		p += (size_t)n;
		len -= (size_t)n;
	}
}

static int connect_loopback(uint16_t port)
{
	struct sockaddr_in addr;
	int fd;

	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
	assert(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
	return fd;
}

static void send_route(int fd, const struct tr_pipeline_route_preface *route)
{
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, route) == TR_OK);
	send_all(fd, raw, sizeof(raw));
}

static struct tr_pipeline_route_preface control_route(void)
{
	struct tr_pipeline_route_preface route;

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	route.owner_shard_id = 0U;
	route.pipeline_id = TEST_GROUP_ID;
	route.epoch = TEST_GROUP_EPOCH;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation = TEST_CONTROL_GENERATION;
	return route;
}

static void recv_control_message(
	int fd, uint64_t expected_message_id,
	struct tr_pipeline_control_wire_message *message)
{
	struct tr_frame_header header;
	struct tr_wire_limits limits;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_PIPELINE_CONTROL_WIRE_SIZE];

	recv_all_timeout(fd, raw_header, sizeof(raw_header));
	memset(&header, 0, sizeof(header));
	assert(tr_wire_header_decode(raw_header, &header) == TR_OK);
	memset(&limits, 0, sizeof(limits));
	limits.max_payload_len = TR_PIPELINE_CONTROL_WIRE_SIZE;
	assert(tr_wire_header_validate(raw_header, &header, &limits) == TR_OK);
	assert(header.type == TR_FRAME_PIPELINE_CONTROL);
	assert(header.message_id == expected_message_id);
	assert(header.payload_len == sizeof(payload));

	recv_all_timeout(fd, payload, sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_pipeline_control_wire_decode(
		       payload, sizeof(payload), message) == TR_OK);
}

static void send_data_message(int fd, uint32_t stream_id,
			      uint64_t message_id,
			      const uint8_t *payload, uint32_t payload_len)
{
	struct tr_frame_header header;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_DATA;
	header.flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
	header.stream_id = stream_id;
	header.message_id = message_id;
	header.payload_len = payload_len;
	header.payload_crc32c = tr_crc32c(payload, payload_len);
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	if (payload_len != 0U)
		send_all(fd, payload, payload_len);
}

static int authorize_group(const struct tr_connection_group_id *group, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	if (!group || group->group_id != TEST_GROUP_ID ||
	    group->epoch != TEST_GROUP_EPOCH)
		return TR_ERR_STALE;

	pthread_mutex_lock(&ctx->lock);
	ctx->authorized++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_connection_group_message_disposition
on_group_message(const struct tr_connection_group_message *message, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(message != NULL);
	assert(message->group.group_id == TEST_GROUP_ID);
	assert(message->group.epoch == TEST_GROUP_EPOCH);
	assert(message->stream_id == 3001U);
	assert(message->message_id == UINT64_C(2001));
	assert(message->flags ==
	       (TR_CONNECTION_GROUP_DATA_FIRST |
		TR_CONNECTION_GROUP_DATA_LAST));

	pthread_mutex_lock(&ctx->lock);
	ctx->retained = *message;
	ctx->messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
}

static void on_group_data_event(
	const struct tr_connection_group_id *group,
	enum tr_connection_group_data_event event, int status, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(event == TR_CONNECTION_GROUP_DATA_CLOSED ||
	       event == TR_CONNECTION_GROUP_DATA_ERROR);
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	ctx->data_events++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_counter(
	struct public_group_ctx *ctx, unsigned *counter, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*counter < target)
		assert(pthread_cond_timedwait(
			       &ctx->cond, &ctx->lock, &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static int wait_data_offer(struct tr_server *server, uint64_t message_id)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		int ret = tr_server_connection_group_send_data_offer(
			server, TEST_GROUP_ID, TEST_GROUP_EPOCH, message_id);

		if (ret == TR_OK)
			return TR_OK;
		assert(ret == TR_ERR_STALE || ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	return TR_ERR_TIMEOUT;
}

static void test_public_connection_group_server(void)
{
	static const uint8_t payload[] = "public-group-data";
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct public_group_ctx ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	uint16_t rpc_port = 0U;
	uint16_t group_port = 0U;
	int control = -1;
	int data = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&config);
	config.max_peers = 1U;
	config.keepalive_interval_ms = 0U;
	config.connection_groups.max_groups = 2U;
	config.connection_groups.max_connections = 4U;
	config.connection_groups.max_data_connections_per_group = 2U;
	config.connection_groups.max_streams_per_group = 8U;
	config.connection_groups.authorize = authorize_group;
	config.connection_groups.on_message = on_group_message;
	config.connection_groups.on_data_event = on_group_data_event;
	config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create(&config, &server) == TR_OK);
	assert(tr_server_listen(
		       server, "127.0.0.1", 0U, &rpc_port) == TR_OK);
	assert(rpc_port != 0U);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	route = control_route();
	control = connect_loopback(group_port);
	send_route(control, &route);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(wait_data_offer(server, UINT64_C(1001)) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(1001), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(offer.pipeline_id == TEST_GROUP_ID);
	assert(offer.epoch == TEST_GROUP_EPOCH);
	assert(tr_pipeline_control_wire_data_route(
		       &offer, &data_route) == TR_OK);

	data = connect_loopback(group_port);
	send_route(data, &data_route);
	send_data_message(data, 3001U, UINT64_C(2001),
			  payload, (uint32_t)(sizeof(payload) - 1U));
	wait_counter(&ctx, &ctx.messages, 1U);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == sizeof(payload) - 1U);
	assert(memcmp(ctx.retained.bytes.data, payload,
		      sizeof(payload) - 1U) == 0);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_connection_group_message_release(&ctx.retained) == TR_OK);
	assert(tr_connection_group_message_release(&ctx.retained) ==
	       TR_ERR_INVALID);

	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U,
		       UINT64_C(1002)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U) == TR_OK);

	assert(close(data) == 0);
	data = -1;
	wait_counter(&ctx, &ctx.data_events, 1U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(close(control) == 0);
	control = -1;
	assert(tr_server_drain(server, 5000U) == TR_OK);
	tr_server_destroy(server);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

int main(void)
{
	test_public_connection_group_server();
	return 0;
}
