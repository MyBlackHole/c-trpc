#include "pipeline_listener_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>

#include "../../group/pipeline_control_internal.h"
#include "pipeline_control_transport_internal.h"
#include "pipeline_ingress_internal.h"
#include "../../group/pipeline_registry_internal.h"
#include "../../execution/reactor_internal.h"
#include "../../execution/buffer.h"
#include "tr/socket.h"
#include "tr/status.h"

#define TR_PIPELINE_LISTENER_ACCEPT_BATCH 16U

enum tr_pipeline_listener_connection_role {
	TR_PIPELINE_LISTENER_CONN_PENDING = 0,
	TR_PIPELINE_LISTENER_CONN_CONTROL = 1,
	TR_PIPELINE_LISTENER_CONN_DATA = 2
};

struct tr_pipeline_listener_connection {
	struct tr_pipeline_listener *listener;
	struct tr_conn_handle handle;
	struct tr_pipeline_route_preface route;
	uint32_t generation;
	enum tr_pipeline_listener_connection_role role;
	int used;
};

struct tr_pipeline_listener_session {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_control_transport *transport;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t connection_index;
	uint32_t connection_generation;
	int used;
	int closing;
};

struct tr_pipeline_listener {
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_registry *registry;
	struct tr_buffer_pool message_pool;
	int message_pool_ready;

	struct tr_pipeline_listener_session *sessions;
	struct tr_pipeline_listener_connection *connections;

	int listen_fd;
	uint16_t bound_port;
	int listener_registered;
	int draining;

	uint32_t pipelines_current;
	uint32_t pipelines_peak;
	uint32_t connections_current;
	uint32_t connections_peak;
	uint64_t control_accepts;
	uint64_t data_accepts;
	uint64_t route_rejections;
	uint64_t capacity_rejections;
};

struct tr_pipeline_listener_preface {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_route_parser parser;
	uint32_t connection_index;
	uint32_t connection_generation;
	int handed_off;
};

static uint32_t tr_pipeline_listener_next_generation(uint32_t generation)
{
	generation++;
	if (generation == 0U)
		generation = 1U;
	return generation;
}

static struct tr_pipeline_listener_connection *
tr_pipeline_listener_connection_exact(
	struct tr_pipeline_listener *listener, uint32_t index,
	uint32_t generation)
{
	struct tr_pipeline_listener_connection *connection;

	if (!listener || index >= listener->config.connection_capacity)
		return NULL;
	connection = &listener->connections[index];
	if (!connection->used || connection->generation != generation)
		return NULL;
	return connection;
}

static void tr_pipeline_listener_connection_clear(
	struct tr_pipeline_listener_connection *connection)
{
	struct tr_pipeline_listener *listener;

	if (!connection || !connection->used)
		return;
	listener = connection->listener;
	memset(&connection->handle, 0, sizeof(connection->handle));
	memset(&connection->route, 0, sizeof(connection->route));
	connection->role = TR_PIPELINE_LISTENER_CONN_PENDING;
	connection->used = 0;
	if (listener && listener->connections_current != 0U)
		listener->connections_current--;
}

static struct tr_pipeline_listener_connection *
tr_pipeline_listener_connection_reserve(
	struct tr_pipeline_listener *listener, uint32_t *index_out)
{
	uint32_t i;

	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *connection =
			&listener->connections[i];

		if (connection->used)
			continue;
		connection->generation =
			tr_pipeline_listener_next_generation(connection->generation);
		connection->listener = listener;
		connection->used = 1;
		connection->role = TR_PIPELINE_LISTENER_CONN_PENDING;
		memset(&connection->handle, 0, sizeof(connection->handle));
		memset(&connection->route, 0, sizeof(connection->route));
		listener->connections_current++;
		if (listener->connections_current > listener->connections_peak)
			listener->connections_peak = listener->connections_current;
		if (index_out)
			*index_out = i;
		return connection;
	}

	listener->capacity_rejections++;
	return NULL;
}

static struct tr_pipeline_listener_session *
tr_pipeline_listener_session_find(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch)
{
	uint32_t i;

	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		struct tr_pipeline_listener_session *session =
			&listener->sessions[i];

		if (session->used && !session->closing &&
		    session->pipeline_id == pipeline_id &&
		    session->epoch == epoch)
			return session;
	}
	return NULL;
}

static struct tr_pipeline_listener_session *
tr_pipeline_listener_session_reserve(struct tr_pipeline_listener *listener)
{
	uint32_t i;

	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		if (!listener->sessions[i].used)
			return &listener->sessions[i];
	}
	listener->capacity_rejections++;
	return NULL;
}

static void tr_pipeline_listener_control_closing(void *arg)
{
	struct tr_pipeline_listener_session *session =
		(struct tr_pipeline_listener_session *)arg;

	if (session && session->used)
		session->closing = 1;
}

static void tr_pipeline_listener_control_closed(
	uint64_t pipeline_id, uint64_t epoch, int teardown_status, void *arg)
{
	struct tr_pipeline_listener_session *session =
		(struct tr_pipeline_listener_session *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_connection *connection;

	(void)teardown_status;
	if (!session || !session->used)
		return;
	listener = session->listener;
	if (!listener || session->pipeline_id != pipeline_id ||
	    session->epoch != epoch)
		return;

	connection = tr_pipeline_listener_connection_exact(
		listener, session->connection_index,
		session->connection_generation);
	if (connection)
		tr_pipeline_listener_connection_clear(connection);

	if (teardown_status != TR_OK) {
		session->closing = 1;
		return;
	}

	memset(session, 0, sizeof(*session));
	if (listener->pipelines_current != 0U)
		listener->pipelines_current--;
}

static enum tr_frame_disposition tr_pipeline_listener_data_frame(
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct tr_pipeline_listener_connection *tracked =
		(struct tr_pipeline_listener_connection *)arg;
	struct tr_pipeline_listener *listener;

	if (!tracked || !tracked->used)
		return TR_FRAME_RELEASE;
	listener = tracked->listener;
	if (!listener || !listener->config.data_frame_cb)
		return TR_FRAME_RELEASE;
	return listener->config.data_frame_cb(
		&tracked->route, connection, frame,
		listener->config.data_callback_arg);
}

static void tr_pipeline_listener_data_event(
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct tr_pipeline_listener_connection *tracked =
		(struct tr_pipeline_listener_connection *)arg;
	struct tr_pipeline_listener *listener = NULL;
	tr_pipeline_listener_data_event_cb callback = NULL;
	struct tr_pipeline_route_preface route;
	void *callback_arg = NULL;
	int have_route = 0;

	memset(&route, 0, sizeof(route));
	if (tracked && tracked->used) {
		listener = tracked->listener;
		route = tracked->route;
		have_route = 1;
		if (listener) {
			callback = listener->config.data_event_cb;
			callback_arg = listener->config.data_callback_arg;
		}
		tr_pipeline_listener_connection_clear(tracked);
	}

	if (callback && have_route)
		callback(&route, connection, event, status, callback_arg);
}

static int tr_pipeline_listener_accept_control(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_connection *tracked,
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection)
{
	struct tr_pipeline_listener_session *session;

	if (listener->draining)
		return TR_ERR_CLOSED;
	struct tr_pipeline_control_config control_config;
	struct tr_pipeline_control *control = NULL;
	struct tr_pipeline_control_transport_config transport_config;
	struct tr_pipeline_control_transport *transport = NULL;
	int ret;

	if (route->owner_shard_id != listener->config.owner_shard_id)
		return TR_ERR_STALE;
	ret = listener->config.authorize_control(
		route, listener->config.authorize_arg);
	if (ret != TR_OK)
		return ret;

	session = tr_pipeline_listener_session_reserve(listener);
	if (!session)
		return TR_AGAIN;

	memset(&control_config, 0, sizeof(control_config));
	control_config.registry = listener->registry;
	control_config.pipeline_id = route->pipeline_id;
	control_config.epoch = route->epoch;
	control_config.data_capacity =
		listener->config.data_capacity_per_pipeline;
	control_config.stream_affinity_capacity =
		listener->config.stream_affinity_capacity_per_pipeline;
	ret = tr_pipeline_control_create(
		&control_config, connection, &control);
	if (ret != TR_OK)
		return ret;

	memset(session, 0, sizeof(*session));
	session->listener = listener;
	session->pipeline_id = route->pipeline_id;
	session->epoch = route->epoch;
	session->connection_index =
		(uint32_t)(tracked - listener->connections);
	session->connection_generation = tracked->generation;
	session->used = 1;

	memset(&transport_config, 0, sizeof(transport_config));
	transport_config.control = control;
	transport_config.connection = connection;
	transport_config.message_pool = &listener->message_pool;
	transport_config.control_route = *route;
	transport_config.closing_cb =
		tr_pipeline_listener_control_closing;
	transport_config.closed_cb =
		tr_pipeline_listener_control_closed;
	transport_config.closed_arg = session;
	ret = tr_pipeline_control_transport_create(
		&transport_config, &transport);
	if (ret != TR_OK) {
		session->used = 0;
		(void)tr_pipeline_control_close(control, connection);
		return ret;
	}

	session->transport = transport;
	tracked->route = *route;
	tracked->role = TR_PIPELINE_LISTENER_CONN_CONTROL;
	listener->pipelines_current++;
	if (listener->pipelines_current > listener->pipelines_peak)
		listener->pipelines_peak = listener->pipelines_current;
	listener->control_accepts++;
	return TR_OK;
}

static int tr_pipeline_listener_accept_data(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_connection *tracked,
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection)
{
	struct tr_pipeline_ingress_config ingress;
	int ret;

	memset(&ingress, 0, sizeof(ingress));
	ingress.registry = listener->registry;
	ingress.frame_cb = tr_pipeline_listener_data_frame;
	ingress.event_cb = tr_pipeline_listener_data_event;
	ingress.callback_arg = tracked;

	ret = tr_pipeline_ingress_attach_data_route_on_owner(
		&ingress, route, connection);
	if (ret != TR_OK)
		return ret;

	tracked->route = *route;
	tracked->role = TR_PIPELINE_LISTENER_CONN_DATA;
	listener->data_accepts++;
	return TR_OK;
}

static int tr_pipeline_listener_preface_feed(
	struct tr_conn_handle connection, const uint8_t *data, size_t len,
	int *done, void *arg)
{
	struct tr_pipeline_listener_preface *preface =
		(struct tr_pipeline_listener_preface *)arg;
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_route_preface route;
	size_t consumed = 0U;
	int ready = 0;
	int ret;

	if (!preface || !done)
		return TR_ERR_INVALID;
	*done = 0;
	listener = preface->listener;
	tracked = tr_pipeline_listener_connection_exact(
		listener, preface->connection_index,
		preface->connection_generation);
	if (!tracked)
		return TR_ERR_STALE;

	memset(&route, 0, sizeof(route));
	ret = tr_pipeline_route_parser_feed(
		&preface->parser, data, len, &consumed, &route, &ready);
	if (ret != TR_OK)
		goto reject;
	if (consumed != len) {
		ret = TR_ERR_STATE;
		goto reject;
	}
	if (!ready)
		return TR_OK;

	if (route.role == TR_PIPELINE_ROUTE_CONTROL)
		ret = tr_pipeline_listener_accept_control(
			listener, tracked, &route, connection);
	else if (route.role == TR_PIPELINE_ROUTE_DATA)
		ret = tr_pipeline_listener_accept_data(
			listener, tracked, &route, connection);
	else
		ret = TR_ERR_BAD_TYPE;
	if (ret != TR_OK) {
		if (route.role == TR_PIPELINE_ROUTE_DATA)
			(void)tr_pipeline_registry_cancel_data_route(
				listener->registry, &route);
		goto reject;
	}

	preface->handed_off = 1;
	*done = 1;
	return TR_OK;

reject:
	listener->route_rejections++;
	return ret;
}

static void tr_pipeline_listener_preface_release(void *arg)
{
	struct tr_pipeline_listener_preface *preface =
		(struct tr_pipeline_listener_preface *)arg;
	struct tr_pipeline_listener_connection *tracked;

	if (!preface)
		return;
	if (!preface->handed_off && preface->listener) {
		tracked = tr_pipeline_listener_connection_exact(
			preface->listener, preface->connection_index,
			preface->connection_generation);
		if (tracked)
			tr_pipeline_listener_connection_clear(tracked);
	}
	free(preface);
}

static int tr_pipeline_listener_adopt_fd(
	struct tr_pipeline_listener *listener, int fd)
{
	struct tr_pipeline_listener_connection *tracked;
	struct tr_pipeline_listener_preface *preface;
	struct tr_reactor_preface_handler handler;
	struct tr_conn_handle connection;
	uint32_t index = 0U;
	int ret;

	tracked = tr_pipeline_listener_connection_reserve(listener, &index);
	if (!tracked)
		return TR_AGAIN;

	preface = (struct tr_pipeline_listener_preface *)calloc(
		1, sizeof(*preface));
	if (!preface) {
		tr_pipeline_listener_connection_clear(tracked);
		return TR_ERR_NOMEM;
	}
	preface->listener = listener;
	preface->connection_index = index;
	preface->connection_generation = tracked->generation;
	tr_pipeline_route_parser_init(&preface->parser);

	memset(&handler, 0, sizeof(handler));
	handler.byte_count = TR_PIPELINE_ROUTE_PREFACE_SIZE;
	handler.feed = tr_pipeline_listener_preface_feed;
	handler.release = tr_pipeline_listener_preface_release;
	handler.arg = preface;

	memset(&connection, 0, sizeof(connection));
	ret = tr_reactor_adopt_fd_prefaced_on_owner(
		listener->config.owner, fd, &handler, &connection);
	if (ret != TR_OK) {
		tr_pipeline_listener_preface_release(preface);
		return ret;
	}
	tracked->handle = connection;
	return TR_OK;
}

static void tr_pipeline_listener_on_ready(
	int fd, uint32_t events, void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	uint32_t accepted = 0U;

	if (!listener || !(events & EPOLLIN))
		return;

	while (accepted < TR_PIPELINE_LISTENER_ACCEPT_BATCH) {
		int accepted_fd = -1;
		int ret = tr_tcp_accept(fd, &accepted_fd);

		if (ret == TR_AGAIN)
			break;
		if (ret != TR_OK)
			break;

		ret = tr_pipeline_listener_adopt_fd(listener, accepted_fd);
		if (ret != TR_OK)
			tr_socket_close(&accepted_fd);
		accepted++;
	}
}

int tr_pipeline_listener_create(
	const struct tr_pipeline_listener_config *config,
	struct tr_pipeline_listener **out)
{
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_registry_config registry_config;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;
	if (!config || !config->owner ||
	    config->owner_shard_id == UINT32_MAX ||
	    config->pipeline_capacity == 0U ||
	    config->connection_capacity == 0U ||
	    config->data_capacity_per_pipeline == 0U ||
	    config->stream_affinity_capacity_per_pipeline == 0U ||
	    config->control_message_count == 0U ||
	    !config->authorize_control)
		return TR_ERR_INVALID;

	listener = (struct tr_pipeline_listener *)calloc(
		1, sizeof(*listener));
	if (!listener)
		return TR_ERR_NOMEM;
	listener->listen_fd = -1;
	listener->config = *config;

	listener->sessions = (struct tr_pipeline_listener_session *)calloc(
		config->pipeline_capacity, sizeof(*listener->sessions));
	listener->connections =
		(struct tr_pipeline_listener_connection *)calloc(
			config->connection_capacity,
			sizeof(*listener->connections));
	if (!listener->sessions || !listener->connections) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}

	memset(&registry_config, 0, sizeof(registry_config));
	registry_config.owner = config->owner;
	registry_config.owner_shard_id = config->owner_shard_id;
	registry_config.capacity = config->pipeline_capacity;
	ret = tr_pipeline_registry_create(
		&registry_config, &listener->registry);
	if (ret != TR_OK)
		goto fail;

	ret = tr_buffer_pool_init(
		&listener->message_pool, config->control_message_count,
		TR_PIPELINE_CONTROL_WIRE_SIZE);
	if (ret != TR_OK)
		goto fail;
	listener->message_pool_ready = 1;

	*out = listener;
	return TR_OK;

fail:
	if (listener->message_pool_ready)
		tr_buffer_pool_destroy(&listener->message_pool);
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	free(listener->connections);
	free(listener->sessions);
	free(listener);
	return ret;
}

int tr_pipeline_listener_listen_ipv4(
	struct tr_pipeline_listener *listener, const char *address,
	uint16_t port, int backlog, uint16_t *out_bound_port)
{
	int fd = -1;
	uint16_t bound = 0U;
	int ret;

	if (!listener || !address || backlog <= 0)
		return TR_ERR_INVALID;
	if (listener->listen_fd >= 0 || listener->listener_registered)
		return TR_ERR_STATE;

	ret = tr_tcp_listen_ipv4(
		address, port, backlog, &fd, &bound);
	if (ret != TR_OK)
		return ret;

	ret = tr_reactor_listener_register(
		listener->config.owner, fd,
		tr_pipeline_listener_on_ready, listener);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}

	listener->listen_fd = fd;
	listener->bound_port = bound;
	listener->listener_registered = 1;
	listener->draining = 0;
	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

int tr_pipeline_listener_begin_drain(struct tr_pipeline_listener *listener)
{
	int result = TR_OK;
	int ret;

	if (!listener)
		return TR_ERR_INVALID;
	if (listener->draining)
		return TR_OK;

	if (listener->listener_registered) {
		ret = tr_reactor_listener_unregister(
			listener->config.owner, listener->listen_fd);
		if (ret != TR_OK)
			result = ret;
		else
			listener->listener_registered = 0;
	}
	if (!listener->listener_registered && listener->listen_fd >= 0) {
		tr_socket_close(&listener->listen_fd);
		listener->bound_port = 0U;
	}
	if (result == TR_OK)
		listener->draining = 1;
	return result;
}

struct tr_pipeline_listener_stop_request {
	struct tr_pipeline_listener *listener;
};

static int tr_pipeline_listener_stop_on_owner(void *arg)
{
	struct tr_pipeline_listener_stop_request *request =
		(struct tr_pipeline_listener_stop_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;
	uint32_t i;
	int result = TR_OK;

	for (i = 0; i < listener->config.connection_capacity; ++i) {
		struct tr_pipeline_listener_connection *tracked =
			&listener->connections[i];
		struct tr_conn_handle handle;
		int ret;

		if (!tracked->used || !tracked->handle.reactor)
			continue;
		handle = tracked->handle;
		ret = tr_reactor_close_on_owner(handle);
		if (ret != TR_OK && ret != TR_ERR_STALE && result == TR_OK)
			result = ret;
	}

	/*
	 * connection close callback 通常会完成 CONTROL fatal teardown。若某次
	 * teardown 在 closing fence 之后返回错误，connection slot 可能已经清空，
	 * 但 session 仍保留 transport。stop() 必须显式重试这些残留 session，
	 * 不能仅依赖 connections_current 判断是否还需要 owner-side cleanup。
	 */
	for (i = 0; i < listener->config.pipeline_capacity; ++i) {
		struct tr_pipeline_listener_session *session =
			&listener->sessions[i];
		int ret;

		if (!session->used || !session->transport)
			continue;
		ret = tr_pipeline_control_transport_abort(session->transport);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}

	if (listener->connections_current != 0U ||
	    listener->pipelines_current != 0U)
		return result == TR_OK ? TR_ERR_STATE : result;
	return result;
}

int tr_pipeline_listener_stop(struct tr_pipeline_listener *listener)
{
	struct tr_pipeline_listener_stop_request request;
	int result;
	int ret;

	if (!listener)
		return TR_ERR_INVALID;

	result = tr_pipeline_listener_begin_drain(listener);

	if (listener->connections_current != 0U ||
	    listener->pipelines_current != 0U) {
		request.listener = listener;
		ret = tr_reactor_call(
			listener->config.owner,
			tr_pipeline_listener_stop_on_owner, &request);
		if (ret != TR_OK && result == TR_OK)
			result = ret;
	}
	if ((listener->connections_current != 0U ||
	     listener->pipelines_current != 0U) &&
	    result == TR_OK)
		result = TR_ERR_STATE;
	return result;
}

void tr_pipeline_listener_destroy(struct tr_pipeline_listener *listener)
{
	if (!listener)
		return;

	/*
	 * destroy 是纯内存析构，不再隐式执行可能失败的 stop。调用方必须先完成
	 * listener/session quiescence；debug build 在这里直接验证生命周期不变量。
	 */
#ifndef NDEBUG
	assert(!listener->listener_registered);
	assert(listener->listen_fd < 0);
	assert(listener->connections_current == 0U);
	assert(listener->pipelines_current == 0U);
#endif
	if (listener->listener_registered || listener->listen_fd >= 0 ||
	    listener->connections_current != 0U ||
	    listener->pipelines_current != 0U)
		return;

	if (listener->message_pool_ready)
		tr_buffer_pool_destroy(&listener->message_pool);
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	free(listener->connections);
	free(listener->sessions);
	free(listener);
}

uint16_t tr_pipeline_listener_bound_port(
	const struct tr_pipeline_listener *listener)
{
	return listener ? listener->bound_port : 0U;
}

struct tr_pipeline_listener_offer_request {
	struct tr_pipeline_listener *listener;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint64_t message_id;
	struct tr_pipeline_route_preface *route_out;
};

static int tr_pipeline_listener_send_offer_on_owner(void *arg)
{
	struct tr_pipeline_listener_offer_request *request =
		(struct tr_pipeline_listener_offer_request *)arg;
	struct tr_pipeline_listener_session *session;

	if (request->listener->draining)
		return TR_ERR_CLOSED;
	session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_send_data_offer(
		session->transport, request->message_id, request->route_out);
}

int tr_pipeline_listener_send_data_offer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint64_t message_id,
	struct tr_pipeline_route_preface *route_out)
{
	struct tr_pipeline_listener_offer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U)
		return TR_ERR_INVALID;
	if (route_out)
		memset(route_out, 0, sizeof(*route_out));
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.message_id = message_id;
	request.route_out = route_out;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_send_offer_on_owner, &request);
}

struct tr_pipeline_listener_transfer_request {
	struct tr_pipeline_listener *listener;
	uint64_t pipeline_id;
	uint64_t epoch;
	uint32_t stream_id;
	uint64_t message_id;
};

static int tr_pipeline_listener_send_ready_on_owner(void *arg)
{
	struct tr_pipeline_listener_transfer_request *request =
		(struct tr_pipeline_listener_transfer_request *)arg;
	struct tr_pipeline_listener_session *session;

	if (request->listener->draining)
		return TR_ERR_CLOSED;
	session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_send_transfer_ready(
		session->transport, request->stream_id, request->message_id);
}

int tr_pipeline_listener_send_transfer_ready(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id, uint64_t message_id)
{
	struct tr_pipeline_listener_transfer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U ||
	    stream_id == 0U)
		return TR_ERR_INVALID;
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.stream_id = stream_id;
	request.message_id = message_id;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_send_ready_on_owner, &request);
}

static int tr_pipeline_listener_release_transfer_on_owner(void *arg)
{
	struct tr_pipeline_listener_transfer_request *request =
		(struct tr_pipeline_listener_transfer_request *)arg;
	struct tr_pipeline_listener_session *session =
		tr_pipeline_listener_session_find(
			request->listener, request->pipeline_id, request->epoch);

	if (!session || !session->transport)
		return TR_ERR_STALE;
	return tr_pipeline_control_transport_release_transfer(
		session->transport, request->stream_id);
}

int tr_pipeline_listener_release_transfer(
	struct tr_pipeline_listener *listener, uint64_t pipeline_id,
	uint64_t epoch, uint32_t stream_id)
{
	struct tr_pipeline_listener_transfer_request request;

	if (!listener || pipeline_id == 0U || epoch == 0U ||
	    stream_id == 0U)
		return TR_ERR_INVALID;
	memset(&request, 0, sizeof(request));
	request.listener = listener;
	request.pipeline_id = pipeline_id;
	request.epoch = epoch;
	request.stream_id = stream_id;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_release_transfer_on_owner, &request);
}

struct tr_pipeline_listener_stats_request {
	struct tr_pipeline_listener *listener;
	struct tr_pipeline_listener_stats *out;
};

static int tr_pipeline_listener_stats_on_owner(void *arg)
{
	struct tr_pipeline_listener_stats_request *request =
		(struct tr_pipeline_listener_stats_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;

	memset(request->out, 0, sizeof(*request->out));
	request->out->pipeline_capacity =
		listener->config.pipeline_capacity;
	request->out->pipelines_current = listener->pipelines_current;
	request->out->pipelines_peak = listener->pipelines_peak;
	request->out->connection_capacity =
		listener->config.connection_capacity;
	request->out->connections_current = listener->connections_current;
	request->out->connections_peak = listener->connections_peak;
	request->out->draining = listener->draining ? 1U : 0U;
	{
		uint32_t i;

		for (i = 0; i < listener->config.connection_capacity; ++i)
			if (listener->connections[i].used &&
			    listener->connections[i].role ==
				    TR_PIPELINE_LISTENER_CONN_DATA)
				request->out->data_connections_current++;

		for (i = 0; i < listener->config.pipeline_capacity; ++i) {
			struct tr_pipeline_listener_session *session =
				&listener->sessions[i];
			struct tr_pipeline_stats stats;
			int ret;

			if (!session->used || session->closing ||
			    !session->transport)
				continue;
			memset(&stats, 0, sizeof(stats));
			ret = tr_pipeline_control_transport_get_stats(
				session->transport, &stats);
			if (ret == TR_OK)
				request->out->active_transfers +=
					stats.stream_affinity_count;
		}
	}
	request->out->control_accepts = listener->control_accepts;
	request->out->data_accepts = listener->data_accepts;
	request->out->route_rejections = listener->route_rejections;
	request->out->capacity_rejections = listener->capacity_rejections;
	return TR_OK;
}

int tr_pipeline_listener_get_stats(
	struct tr_pipeline_listener *listener,
	struct tr_pipeline_listener_stats *out)
{
	struct tr_pipeline_listener_stats_request request;

	if (!listener || !out)
		return TR_ERR_INVALID;
	request.listener = listener;
	request.out = out;
	return tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_stats_on_owner, &request);
}
