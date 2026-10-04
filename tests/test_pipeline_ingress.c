#include "../src/pipeline_ingress_internal.h"
#include "../src/group/pipeline_internal.h"
#include "../src/group/pipeline_registry_internal.h"
#include "../src/pipeline_route_internal.h"
#include "../src/reactor_internal.h"

#include "tr/crc32c.h"
#include "tr/reactor.h"
#include "tr/socket.h"
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
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

struct ingress_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_pipeline_ingress_config ingress;
	unsigned accepted;
	unsigned frames;
	unsigned events;
	uint16_t last_type;
	int last_status;
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

static enum tr_frame_disposition ingress_frame_cb(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct ingress_test_ctx *ctx = (struct ingress_test_ctx *)arg;

	(void)connection;
	pthread_mutex_lock(&ctx->lock);
	ctx->frames++;
	ctx->last_type = frame->header.type;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

static void ingress_event_cb(struct tr_conn_handle connection,
			     enum tr_connection_event event,
			     int status, void *arg)
{
	struct ingress_test_ctx *ctx = (struct ingress_test_ctx *)arg;

	(void)connection;
	(void)event;
	pthread_mutex_lock(&ctx->lock);
	ctx->events++;
	ctx->last_status = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void ingress_listener_cb(int listener, uint32_t events, void *arg)
{
	struct ingress_test_ctx *ctx = (struct ingress_test_ctx *)arg;

	if (!(events & EPOLLIN))
		return;

	for (;;) {
		struct tr_conn_handle connection;
		int fd = -1;
		int ret = tr_tcp_accept(listener, &fd);

		if (ret == TR_AGAIN)
			break;
		assert(ret == TR_OK);
		memset(&connection, 0, sizeof(connection));
		ret = tr_pipeline_ingress_adopt_data_fd_on_owner(
			&ctx->ingress, fd, &connection);
		if (ret != TR_OK)
			tr_socket_close(&fd);

		pthread_mutex_lock(&ctx->lock);
		ctx->accepted++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
	}
}

static struct tr_pipeline *make_pipeline(struct tr_reactor *owner)
{
	struct tr_pipeline_config config;
	struct tr_pipeline *pipeline = NULL;

	memset(&config, 0, sizeof(config));
	config.owner = owner;
	config.owner_shard_id = 0U;
	config.pipeline_id = UINT64_C(0x9001);
	config.epoch = UINT64_C(12);
	config.data_capacity = 2U;
	config.stream_affinity_capacity = 4U;
	assert(tr_pipeline_create(&config, &pipeline) == TR_OK);
	return pipeline;
}

static struct tr_pipeline_route_preface make_route(
	struct tr_pipeline *pipeline, struct tr_pipeline_data_ref data)
{
	struct tr_pipeline_route_preface preface;

	memset(&preface, 0, sizeof(preface));
	preface.version = TR_PIPELINE_ROUTE_VERSION;
	preface.role = TR_PIPELINE_ROUTE_DATA;
	preface.owner_shard_id = tr_pipeline_owner_shard_id(pipeline);
	preface.pipeline_id = tr_pipeline_id(pipeline);
	preface.epoch = tr_pipeline_epoch(pipeline);
	preface.member_index = data.index;
	preface.member_generation = data.generation;
	return preface;
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

static void wait_for_counter(pthread_mutex_t *lock, pthread_cond_t *cond,
			     unsigned *counter, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;

	pthread_mutex_lock(lock);
	while (*counter < target)
		assert(pthread_cond_timedwait(cond, lock, &deadline) == 0);
	pthread_mutex_unlock(lock);
}

static void wait_for_peer_close(int fd)
{
	struct pollfd pfd;
	int ret;
	char byte;

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

static void test_pipeline_ingress_routing(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_registry_config registry_config;
	struct tr_pipeline_registry *registry = NULL;
	struct tr_pipeline *pipeline;
	struct tr_pipeline_data_ref reserved;
	struct tr_pipeline_stats pipeline_stats;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface wrong;
	struct tr_conn_handle control;
	struct ingress_test_ctx ctx;
	uint8_t route_raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint8_t ping[TR_WIRE_HEADER_SIZE];
	uint8_t tail[TR_PIPELINE_ROUTE_PREFACE_SIZE - 13U +
		     TR_WIRE_HEADER_SIZE];
	uint16_t port = 0U;
	int listener = -1;
	int bad_client = -1;
	int good_client = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	pipeline = make_pipeline(reactor);
	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = reactor;
	registry_config.owner_shard_id = 0U;
	registry_config.capacity = 2U;
	assert(tr_pipeline_registry_create(&registry_config, &registry) == TR_OK);
	assert(tr_pipeline_registry_register(registry, pipeline) == TR_OK);
	memset(&control, 0, sizeof(control));
	control.reactor = reactor;
	control.slot = 9U;
	control.generation = 1U;
	assert(tr_pipeline_set_control(pipeline, control) == TR_OK);
	assert(tr_pipeline_reserve_data(pipeline, &reserved) == TR_OK);

	ctx.ingress.registry = registry;
	ctx.ingress.frame_cb = ingress_frame_cb;
	ctx.ingress.event_cb = ingress_event_cb;
	ctx.ingress.callback_arg = &ctx;

	assert(tr_tcp_listen_ipv4("127.0.0.1", 0U, 8, &listener, &port) == TR_OK);
	assert(listener >= 0 && port != 0U);
	assert(tr_reactor_listener_register(
		       reactor, listener, ingress_listener_cb, &ctx) == TR_OK);

	/*
	 * First socket presents a stale generation. Reactor must close it and the
	 * reservation must remain RESERVED for the exact retry capability.
	 */
	wrong = make_route(pipeline, reserved);
	wrong.member_generation++;
	assert(tr_pipeline_route_preface_encode(route_raw, &wrong) == TR_OK);
	bad_client = connect_loopback(port);
	send_all(bad_client, route_raw, sizeof(route_raw));
	wait_for_peer_close(bad_client);
	close(bad_client);
	bad_client = -1;

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_get_stats(pipeline, &pipeline_stats) == TR_OK);
	assert(pipeline_stats.data_reserved_count == 1U);
	assert(pipeline_stats.data_count == 0U);

	/*
	 * Retry the exact capability. Fragment the preface, then coalesce its
	 * remaining bytes with a normal TRP1 PING. The Reactor gate reads exactly
	 * the remaining preface bytes, installs the routed handler, and only then
	 * lets the existing TRP1 parser consume PING.
	 */
	route = make_route(pipeline, reserved);
	assert(tr_pipeline_route_preface_encode(route_raw, &route) == TR_OK);
	build_ping(ping);
	memcpy(tail, route_raw + 13U, TR_PIPELINE_ROUTE_PREFACE_SIZE - 13U);
	memcpy(tail + (TR_PIPELINE_ROUTE_PREFACE_SIZE - 13U), ping,
	       sizeof(ping));

	good_client = connect_loopback(port);
	send_all(good_client, route_raw, 13U);
	send_all(good_client, tail, sizeof(tail));

	wait_for_counter(&ctx.lock, &ctx.cond, &ctx.frames, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.last_type == TR_FRAME_PING);
	pthread_mutex_unlock(&ctx.lock);

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_get_stats(pipeline, &pipeline_stats) == TR_OK);
	assert(pipeline_stats.data_reserved_count == 0U);
	assert(pipeline_stats.data_count == 1U);

	shutdown(good_client, SHUT_RDWR);
	close(good_client);
	good_client = -1;
	wait_for_counter(&ctx.lock, &ctx.cond, &ctx.events, 1U);

	memset(&pipeline_stats, 0, sizeof(pipeline_stats));
	assert(tr_pipeline_get_stats(pipeline, &pipeline_stats) == TR_OK);
	assert(pipeline_stats.data_count == 0U);

	assert(tr_reactor_listener_unregister(reactor, listener) == TR_OK);
	tr_socket_close(&listener);
	assert(tr_pipeline_clear_control(pipeline, control) == TR_OK);
	assert(tr_pipeline_registry_unregister(registry, pipeline) == TR_OK);
	tr_pipeline_registry_destroy(registry);
	tr_pipeline_destroy(pipeline);

	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

int main(void)
{
	test_pipeline_ingress_routing();
	puts("pipeline accepted-ingress routing: ok");
	return 0;
}
