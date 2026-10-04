#include "../src/group/pipeline_control_wire_internal.h"
#include "../src/transport/group/pipeline_listener_internal.h"
#include "../src/group/pipeline_route_internal.h"

#include "tr/crc32c.h"
#include "tr/reactor.h"
#include "tr/status.h"
#include "tr/wire.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_PIPELINE_ID UINT64_C(0x8101)
#define TEST_EPOCH_1 UINT64_C(61)
#define TEST_EPOCH_2 UINT64_C(62)
#define TEST_CONTROL_GENERATION 17U

struct data_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned frames;
	unsigned events;
	uint16_t last_type;
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

static void wait_peer_close(int fd)
{
	struct pollfd pfd;
	char byte;
	int ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLIN | POLLHUP;
	do {
		ret = poll(&pfd, 1, 10000);
	} while (ret < 0 && errno == EINTR);
	assert(ret == 1);

	ret = (int)recv(fd, &byte, 1U, 0);
	assert(ret == 0 ||
	       (ret < 0 && (errno == ECONNRESET || errno == ENOTCONN)));
}

static int authorize_control(
	const struct tr_pipeline_route_preface *route, void *arg)
{
	(void)arg;
	if (!route || route->role != TR_PIPELINE_ROUTE_CONTROL ||
	    route->owner_shard_id != 0U ||
	    route->pipeline_id != TEST_PIPELINE_ID ||
	    (route->epoch != TEST_EPOCH_1 &&
	     route->epoch != TEST_EPOCH_2) ||
	    route->member_generation != TEST_CONTROL_GENERATION)
		return TR_ERR_STALE;
	return TR_OK;
}

static enum tr_frame_disposition data_frame_cb(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct data_test_ctx *ctx = (struct data_test_ctx *)arg;

	assert(route != NULL);
	assert(route->pipeline_id == TEST_PIPELINE_ID);
	(void)connection;
	pthread_mutex_lock(&ctx->lock);
	ctx->frames++;
	ctx->last_type = frame->header.type;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

static void data_event_cb(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct data_test_ctx *ctx = (struct data_test_ctx *)arg;

	assert(route != NULL);
	assert(route->pipeline_id == TEST_PIPELINE_ID);
	(void)connection;
	(void)event;
	(void)status;
	pthread_mutex_lock(&ctx->lock);
	ctx->events++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static struct tr_pipeline_route_preface control_route(uint64_t epoch)
{
	struct tr_pipeline_route_preface route;

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	route.owner_shard_id = 0U;
	route.pipeline_id = TEST_PIPELINE_ID;
	route.epoch = epoch;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation = TEST_CONTROL_GENERATION;
	return route;
}

static void send_route(int fd, const struct tr_pipeline_route_preface *route)
{
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, route) == TR_OK);
	send_all(fd, raw, sizeof(raw));
}

static void build_ping(uint8_t raw[TR_WIRE_HEADER_SIZE])
{
	struct tr_frame_header header;

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_PING;
	header.payload_crc32c = tr_crc32c(NULL, 0U);
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
}

static void send_empty_data(
	int fd, uint32_t stream_id, uint64_t message_id)
{
	struct tr_frame_header header;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_DATA;
	header.flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
	header.stream_id = stream_id;
	header.message_id = message_id;
	header.payload_crc32c = tr_crc32c(NULL, 0U);
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
	send_all(fd, raw, sizeof(raw));
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
	assert(header.flags == 0U);
	assert(header.stream_id == 0U);
	assert(header.message_id == expected_message_id);
	assert(header.payload_len == TR_PIPELINE_CONTROL_WIRE_SIZE);

	recv_all_timeout(fd, payload, sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_pipeline_control_wire_decode(
		       payload, sizeof(payload), message) == TR_OK);
}

static void send_control_message(
	int fd, uint64_t message_id,
	const struct tr_pipeline_control_wire_message *message)
{
	struct tr_frame_header header;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_PIPELINE_CONTROL_WIRE_SIZE];

	assert(tr_pipeline_control_wire_encode(payload, message) == TR_OK);
	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_PIPELINE_CONTROL;
	header.message_id = message_id;
	header.payload_len = sizeof(payload);
	header.payload_crc32c = tr_crc32c(payload, sizeof(payload));
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	send_all(fd, payload, sizeof(payload));
}

static void wait_listener_counts(
	struct tr_pipeline_listener *listener,
	uint32_t pipelines, uint32_t connections)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		struct tr_pipeline_listener_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
		if (stats.pipelines_current == pipelines &&
		    stats.connections_current == connections)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(0 && "listener counters did not converge");
}

static void wait_data_frames(struct data_test_ctx *ctx, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->frames < target)
		assert(pthread_cond_timedwait(
			       &ctx->cond, &ctx->lock, &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_data_events(struct data_test_ctx *ctx, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->events < target)
		assert(pthread_cond_timedwait(
			       &ctx->cond, &ctx->lock, &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static int wait_offer_retry(
	struct tr_pipeline_listener *listener, uint64_t message_id,
	struct tr_pipeline_route_preface *route)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		int ret = tr_pipeline_listener_send_data_offer(
			listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
			message_id, route);

		if (ret == TR_OK)
			return TR_OK;
		assert(ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	return TR_ERR_TIMEOUT;
}

static void test_pipeline_listener_control_and_data(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct tr_pipeline_listener_stats stats;
	struct data_test_ctx data_ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface offer_route;
	struct tr_pipeline_route_preface offer_route2;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message offer2;
	struct tr_pipeline_control_wire_message offer3;
	struct tr_pipeline_control_wire_message ready;
	struct tr_pipeline_control_wire_message cancel;
	uint8_t ping[TR_WIRE_HEADER_SIZE];
	uint16_t port = 0U;
	int bad_control = -1;
	int control = -1;
	int data = -1;
	int duplicate_data = -1;
	int control2 = -1;

	memset(&data_ctx, 0, sizeof(data_ctx));
	assert(pthread_mutex_init(&data_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&data_ctx.cond, NULL) == 0);

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 2U;
	config.connection_capacity = 8U;
	config.data_capacity_per_pipeline = 2U;
	config.stream_affinity_capacity_per_pipeline = 8U;
	config.control_message_count = 8U;
	config.authorize_control = authorize_control;
	config.data_frame_cb = data_frame_cb;
	config.data_event_cb = data_event_cb;
	config.data_callback_arg = &data_ctx;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 16, &port) == TR_OK);
	assert(port != 0U);

	/* Authorization, not client-provided identity alone, admits CONTROL. */
	route = control_route(TEST_EPOCH_1);
	route.member_generation++;
	bad_control = connect_loopback(port);
	send_route(bad_control, &route);
	wait_peer_close(bad_control);
	close(bad_control);
	bad_control = -1;
	wait_listener_counts(listener, 0U, 0U);

	/* Establish one real CONTROL connection through TRR1. */
	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);

	/* Server reserves DATA and emits the exact capability over TRP1/TRC1. */
	memset(&offer_route, 0, sizeof(offer_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(1001), &offer_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(1001), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(offer.owner_shard_id == 0U);
	assert(offer.pipeline_id == TEST_PIPELINE_ID);
	assert(offer.epoch == TEST_EPOCH_1);
	assert(offer.data_index == offer_route.member_index);
	assert(offer.data_generation == offer_route.member_generation);

	/* A RESERVED capability alone is never enough to publish READY. */
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3000U,
		       UINT64_C(999)) == TR_AGAIN);

	/* Exact DATA socket joins the same listener and normal TRP1 begins after TRR1. */
	data = connect_loopback(port);
	send_route(data, &offer_route);
	build_ping(ping);
	send_all(data, ping, sizeof(ping));
	wait_data_frames(&data_ctx, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.last_type == TR_FRAME_PING);
	pthread_mutex_unlock(&data_ctx.lock);
	wait_listener_counts(listener, 1U, 2U);

	/*
	 * Replaying an already-ATTACHED DATA capability must reject only the new
	 * socket. Failure cleanup exact-cancels RESERVED state, so it cannot retire
	 * the live membership that already consumed this generation.
	 */
	duplicate_data = connect_loopback(port);
	send_route(duplicate_data, &offer_route);
	wait_peer_close(duplicate_data);
	close(duplicate_data);
	duplicate_data = -1;
	wait_listener_counts(listener, 1U, 2U);
	build_ping(ping);
	send_all(data, ping, sizeof(ping));
	wait_data_frames(&data_ctx, 2U);

	/* TRANSFER_READY cannot be emitted until the DATA capability is ATTACHED. */
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3001U,
		       UINT64_C(1002)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	assert(tr_pipeline_listener_release_transfer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3001U) == TR_OK);

	/* A client DATA_CANCEL is handled by the real CONTROL frame callback. */
	memset(&offer_route2, 0, sizeof(offer_route2));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(1003), &offer_route2) == TR_OK);
	memset(&offer2, 0, sizeof(offer2));
	recv_control_message(control, UINT64_C(1003), &offer2);
	cancel = offer2;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	send_control_message(control, UINT64_C(2001), &cancel);

	/*
	 * With one DATA attached and one reservation, another offer is full until
	 * the cancel frame is actually consumed. Retry proves the handler released
	 * only that exact reservation.
	 */
	memset(&offer_route2, 0, sizeof(offer_route2));
	assert(wait_offer_retry(
		       listener, UINT64_C(1004), &offer_route2) == TR_OK);
	memset(&offer3, 0, sizeof(offer3));
	recv_control_message(control, UINT64_C(1004), &offer3);
	assert(offer3.data_index == offer2.data_index);
	assert(offer3.data_generation != offer2.data_generation);

	/*
	 * Fatal CONTROL loss invalidates the whole soft-state group: outstanding
	 * reservations are cancelled, attached DATA is closed, registry entry is
	 * removed, and the same pipeline_id can later start a new epoch.
	 */
	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 1U);
	wait_listener_counts(listener, 0U, 0U);

	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
	assert(stats.control_accepts == 1U);
	assert(stats.data_accepts == 1U);
	assert(stats.route_rejections >= 1U);
	assert(stats.pipelines_peak == 1U);
	assert(stats.connections_peak >= 2U);

	/* New epoch with the same pipeline_id is admitted after old teardown. */
	route = control_route(TEST_EPOCH_2);
	control2 = connect_loopback(port);
	send_route(control2, &route);
	wait_listener_counts(listener, 1U, 1U);

	/*
	 * stop() 本身必须完成 live CONTROL session 的 fatal teardown，而不是要求
	 * 调用方先关闭 peer socket。成功返回后 destroy 只能做纯资源释放。
	 */
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	wait_peer_close(control2);
	close(control2);
	control2 = -1;
	wait_listener_counts(listener, 0U, 0U);
	tr_pipeline_listener_destroy(listener);
	listener = NULL;
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);

	pthread_cond_destroy(&data_ctx.cond);
	pthread_mutex_destroy(&data_ctx.lock);
}

static void test_pipeline_listener_ready_ingress_barrier(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct data_test_ctx data_ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_route_preface data_route2;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message offer2;
	struct tr_pipeline_control_wire_message ready;
	uint16_t port = 0U;
	int control = -1;
	int data = -1;
	int data2 = -1;

	memset(&data_ctx, 0, sizeof(data_ctx));
	assert(pthread_mutex_init(&data_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&data_ctx.cond, NULL) == 0);

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 5U;
	config.data_capacity_per_pipeline = 2U;
	config.stream_affinity_capacity_per_pipeline = 4U;
	config.control_message_count = 4U;
	config.authorize_control = authorize_control;
	config.data_frame_cb = data_frame_cb;
	config.data_event_cb = data_event_cb;
	config.data_callback_arg = &data_ctx;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 16, &port) == TR_OK);

	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);

	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5001), &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(5001), &offer);

	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);

	/*
	 * Physical DATA membership is not transfer authorization. DATA before
	 * TRANSFER_READY is a protocol violation: no application callback and the
	 * offending DATA lane is retired.
	 */
	send_empty_data(data, 7001U, UINT64_C(6001));
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 1U);
	wait_listener_counts(listener, 1U, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* Re-establish DATA, publish READY, then the same stream is admitted. */
	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5002), &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(5002), &offer);
	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);

	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7001U,
		       UINT64_C(5003)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(5003), &ready);
	assert(ready.stream_id == 7001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);

	/*
	 * Add a second DATA membership after READY. The stream is pinned to the
	 * first exact generation, so replaying it on another live lane is also
	 * connection-fatal and must not reach the application.
	 */
	memset(&data_route2, 0, sizeof(data_route2));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5004), &data_route2) == TR_OK);
	memset(&offer2, 0, sizeof(offer2));
	recv_control_message(control, UINT64_C(5004), &offer2);
	data2 = connect_loopback(port);
	send_route(data2, &data_route2);
	wait_listener_counts(listener, 1U, 3U);

	send_empty_data(data2, 7001U, UINT64_C(6002));
	wait_peer_close(data2);
	close(data2);
	data2 = -1;
	wait_data_events(&data_ctx, 2U);
	wait_listener_counts(listener, 1U, 2U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* The READY stream remains valid on its originally selected DATA lane. */
	send_empty_data(data, 7001U, UINT64_C(6003));
	wait_data_frames(&data_ctx, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.last_type == TR_FRAME_DATA);
	pthread_mutex_unlock(&data_ctx.lock);
	assert(tr_pipeline_listener_release_transfer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7001U) == TR_OK);

	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 3U);
	wait_listener_counts(listener, 0U, 0U);

	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	tr_pipeline_listener_destroy(listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	pthread_cond_destroy(&data_ctx.cond);
	pthread_mutex_destroy(&data_ctx.lock);
}

int main(void)
{
	test_pipeline_listener_control_and_data();
	test_pipeline_listener_ready_ingress_barrier();
	puts("pipeline listener/control transport: ok");
	return 0;
}
