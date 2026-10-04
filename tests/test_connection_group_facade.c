#include "tr/trpc.h"

#include "../src/facade_diagnostics_internal.h"
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
	struct tr_client *client;
	unsigned authorized;
	unsigned messages;
	unsigned data_events;
	unsigned transfer_ready;
	uint32_t ready_stream_id;
	uint64_t ready_message_id;
	unsigned client_send_fragments;
	unsigned client_send_first;
	unsigned client_send_last;
	uint32_t client_send_len;
	uint8_t client_send_bytes[256];
	int ready_send_first_ret;
	int ready_send_second_ret;
	unsigned retry_messages;
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

	if (message->stream_id == 3001U) {
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

	assert(message->stream_id == 5001U);
	if (message->message_id == UINT64_C(4202)) {
		assert(message->flags ==
		       (TR_CONNECTION_GROUP_DATA_FIRST |
			TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U);
		assert(message->bytes.data[0] == 0xa5U);
		pthread_mutex_lock(&ctx->lock);
		ctx->retry_messages++;
		ctx->messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	assert(message->message_id == UINT64_C(4201));
	pthread_mutex_lock(&ctx->lock);
	assert((uint64_t)ctx->client_send_len + message->bytes.len <=
	       sizeof(ctx->client_send_bytes));
	if (message->bytes.len != 0U)
		memcpy(ctx->client_send_bytes + ctx->client_send_len,
		       message->bytes.data, message->bytes.len);
	ctx->client_send_len += message->bytes.len;
	ctx->client_send_fragments++;
	if (message->flags & TR_CONNECTION_GROUP_DATA_FIRST)
		ctx->client_send_first++;
	if (message->flags & TR_CONNECTION_GROUP_DATA_LAST)
		ctx->client_send_last++;
	ctx->messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_client_transfer_ready(
	const struct tr_connection_group_id *group, uint32_t stream_id,
	uint64_t message_id, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(stream_id != 0U);

	if (message_id == UINT64_C(4101) && ctx->client) {
		uint8_t payload[130];
		uint8_t retry = 0xa5U;
		struct tr_transport_bytes bytes;
		struct tr_transport_bytes retry_bytes;
		uint32_t i;

		for (i = 0; i < sizeof(payload); ++i)
			payload[i] = (uint8_t)(i ^ 0x5aU);
		bytes.data = payload;
		bytes.len = (uint32_t)sizeof(payload);
		retry_bytes.data = &retry;
		retry_bytes.len = 1U;
		ctx->ready_send_first_ret =
			tr_client_connection_group_send(
				ctx->client, stream_id, UINT64_C(4201), &bytes);
		ctx->ready_send_second_ret =
			tr_client_connection_group_send(
				ctx->client, stream_id, UINT64_C(4202),
				&retry_bytes);
		memset(payload, 0, sizeof(payload));
	}

	pthread_mutex_lock(&ctx->lock);
	ctx->transfer_ready++;
	ctx->ready_stream_id = stream_id;
	ctx->ready_message_id = message_id;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
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

static void wait_data_accepts(struct tr_server *server, uint64_t target)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		struct tr_pipeline_listener_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_get_connection_group_stats_internal(
			       server, &stats) == TR_OK);
		if (stats.data_accepts >= target)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for DATA attach");
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
	wait_data_accepts(server, 1U);

	/*
	 * Physical DATA membership is not transfer authorization. The Server
	 * must publish TRANSFER_READY before this stream is allowed to send DATA.
	 */
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U,
		       UINT64_C(1002)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);

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

static void test_public_connection_group_client_control(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_close(client) == TR_ERR_STATE);

	group.group_id = 0U;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) ==
	       TR_ERR_INVALID);

	group.group_id = TEST_GROUP_ID;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) ==
	       TR_ERR_STATE);
	wait_counter(&ctx, &ctx.authorized, 1U);

	/*
	 * This slice owns only CONTROL. An offered DATA reservation is returned
	 * internally with DATA_CANCEL; once that cancellation reaches the Server,
	 * capacity=1 permits a new offer without exposing index/generation.
	 */
	assert(wait_data_offer(server, UINT64_C(3001)) == TR_OK);
	assert(wait_data_offer(server, UINT64_C(3002)) == TR_OK);

	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_client_connection_group_close(client) == TR_ERR_STATE);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);

	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_client_data_offer(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.limits.max_frame_payload_bytes = 64U;
	client_config.limits.max_message_bytes = 130U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.on_transfer_ready =
		on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(wait_data_offer(server, UINT64_C(4001)) == TR_OK);
	wait_data_accepts(server, 1U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 0U);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 2U);
	assert(server_stats.data_connections_current == 1U);
	assert(server_stats.active_transfers == 0U);
	assert(server_stats.data_accepts == 1U);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.group.group_id == TEST_GROUP_ID);
	assert(client_stats.group.epoch == TEST_GROUP_EPOCH);
	assert(client_stats.control_connected == 1U);
	assert(client_stats.draining == 0U);
	assert(client_stats.data_connections == 1U);
	assert(client_stats.active_transfers == 0U);
	assert(client_stats.send_bytes_inflight == 0U);
	assert(client_stats.send_bytes_limit == 130U);

	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(4002)) == TR_AGAIN);

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_ERR_STALE);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(4101)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5001U);
	assert(ctx.ready_message_id == UINT64_C(4101));
	pthread_mutex_unlock(&ctx.lock);

	{
		uint8_t payload[130];
		uint8_t retry = 0xa5U;
		struct tr_transport_bytes bytes;
		struct tr_transport_bytes retry_bytes;
		uint32_t i;

		for (i = 0; i < sizeof(payload); ++i)
			payload[i] = (uint8_t)(i ^ 0x5aU);
		bytes.data = payload;
		bytes.len = 131U;
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4200), &bytes) ==
		       TR_ERR_BAD_LENGTH);
		bytes.len = (uint32_t)sizeof(payload);
		assert(tr_client_connection_group_send(
			       client, 9999U, UINT64_C(4201), &bytes) ==
		       TR_ERR_STALE);
		assert(ctx.ready_send_first_ret == TR_OK);
		assert(ctx.ready_send_second_ret == TR_AGAIN);

		wait_counter(&ctx, &ctx.messages, 3U);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.client_send_fragments == 3U);
		assert(ctx.client_send_first == 1U);
		assert(ctx.client_send_last == 1U);
		assert(ctx.client_send_len == 130U);
		for (i = 0; i < 130U; ++i)
			assert(ctx.client_send_bytes[i] == (uint8_t)(i ^ 0x5aU));
		pthread_mutex_unlock(&ctx.lock);

		retry_bytes.data = &retry;
		retry_bytes.len = 1U;
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4202),
			       &retry_bytes) == TR_OK);
		wait_counter(&ctx, &ctx.retry_messages, 1U);
	}

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_OK);
	{
		struct tr_transport_bytes empty = { NULL, 0U };
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4202), &empty) ==
		       TR_ERR_STALE);
	}
	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_ERR_STALE);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);

	/* release makes the semantic stream id reusable on both sides. */
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(4102)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5001U);
	assert(ctx.ready_message_id == UINT64_C(4102));
	pthread_mutex_unlock(&ctx.lock);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U);
	assert(client_stats.send_bytes_inflight == 0U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.active_transfers == 1U);
	assert(server_stats.data_connections_current == 1U);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_STATE);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_STATE);

	/*
	 * Client drain is a monotonic local admission barrier. Existing READY work
	 * remains usable, but a READY observed after begin_drain() must not create
	 * a new local affinity. The Server owns that late affinity independently.
	 */
	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.active_transfers == 1U);
	assert(tr_client_connection_group_wait_drained(
		       client, 10U) == TR_ERR_TIMEOUT);

	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U,
		       UINT64_C(4300)) == TR_OK);
	{
		struct timespec pause = { 0, 100000000L };
		(void)nanosleep(&pause, NULL);
	}
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.transfer_ready == 2U);
	pthread_mutex_unlock(&ctx.lock);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.active_transfers == 2U);

	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(4301)) == TR_ERR_CLOSED);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5003U,
		       UINT64_C(4302)) == TR_ERR_CLOSED);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 1U);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 2U);
	assert(server_stats.active_transfers == 2U);

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U) == TR_OK);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.active_transfers == 0U);
	assert(client_stats.send_bytes_inflight == 0U);
	assert(client_stats.data_connections == 1U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	assert(tr_server_connection_group_wait_drained(
		       server, 5000U) == TR_OK);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 1U);
	assert(server_stats.groups_current == 0U);
	assert(server_stats.connections_current == 0U);
	assert(server_stats.data_connections_current == 0U);
	assert(server_stats.active_transfers == 0U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	tr_client_destroy(client);
	tr_server_destroy(server);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_client_drain_cancels_offer(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	assert(tr_client_create(&client_config, &client) == TR_OK);

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(wait_data_offer(server, UINT64_C(4401)) == TR_OK);
	/*
	 * The second offer can be issued only after the first exact reservation
	 * has been returned by the draining Client. No DATA socket is created.
	 */
	assert(wait_data_offer(server, UINT64_C(4402)) == TR_OK);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.data_connections == 0U);
	assert(client_stats.active_transfers == 0U);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 1U);
	assert(server_stats.data_connections_current == 0U);
	assert(server_stats.data_accepts == 0U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);

	tr_client_destroy(client);
	tr_server_destroy(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

int main(void)
{
	test_public_connection_group_server();
	test_public_connection_group_client_control();
	test_public_connection_group_client_data_offer();
	test_public_connection_group_client_drain_cancels_offer();
	return 0;
}
