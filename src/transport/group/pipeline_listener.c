#define _GNU_SOURCE
#include "pipeline_listener_internal.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>

#include "../../group/pipeline_control_internal.h"
#include "pipeline_control_transport_internal.h"
#include "pipeline_ingress_internal.h"
#include "../../group/pipeline_registry_internal.h"
#include "../../execution/reactor_internal.h"
#include "../../execution/buffer.h"
#include "../../io/socket.h"
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

	/*
	 * Listener/Pipeline counters remain Reactor-owner only. This lock publishes
	 * only drain lifecycle generations to external waiters.
	 */
	pthread_mutex_t drain_wait_lock;
	pthread_cond_t drain_wait_cond;
	uint64_t drain_generation;
	uint64_t drained_generation;
	uint32_t drain_waiters;
	int drain_wait_active;
	int drain_wait_closed;

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

static int tr_pipeline_listener_drain_complete_on_owner(
	const struct tr_pipeline_listener *listener)
{
	return listener && listener->draining &&
		listener->connections_current == 0U &&
		listener->pipelines_current == 0U;
}

static void tr_pipeline_listener_publish_drain_progress_on_owner(
	struct tr_pipeline_listener *listener)
{
	if (!tr_pipeline_listener_drain_complete_on_owner(listener))
		return;

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_active &&
	    listener->drained_generation != listener->drain_generation) {
		listener->drained_generation = listener->drain_generation;
		pthread_cond_broadcast(&listener->drain_wait_cond);
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);
}

static void tr_pipeline_listener_publish_drain_start_on_owner(
	struct tr_pipeline_listener *listener)
{
	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_generation++;
	if (listener->drain_generation == 0U)
		listener->drain_generation = 1U;
	listener->drained_generation = 0U;
	listener->drain_wait_active = 1;
	pthread_mutex_unlock(&listener->drain_wait_lock);

	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
}

static void tr_pipeline_listener_publish_listening_on_owner(
	struct tr_pipeline_listener *listener)
{
	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_wait_active = 0;
	pthread_cond_broadcast(&listener->drain_wait_cond);
	pthread_mutex_unlock(&listener->drain_wait_lock);
}

static int tr_pipeline_listener_close_wait_admission(
	struct tr_pipeline_listener *listener)
{
	int ret = TR_OK;

	pthread_mutex_lock(&listener->drain_wait_lock);
	listener->drain_wait_closed = 1;
	if (listener->drain_waiters != 0U) {
		pthread_cond_broadcast(&listener->drain_wait_cond);
		ret = TR_ERR_STATE;
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);
	return ret;
}

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
	if (listener)
		tr_pipeline_listener_publish_drain_progress_on_owner(listener);
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
	tr_pipeline_listener_publish_drain_progress_on_owner(listener);
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
	int drain_wait_lock_ready = 0;
	int drain_wait_cond_ready = 0;
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

	if (pthread_mutex_init(&listener->drain_wait_lock, NULL) != 0) {
		ret = TR_ERR_SYS;
		goto fail;
	}
	drain_wait_lock_ready = 1;
	{
		pthread_condattr_t attr;

		if (pthread_condattr_init(&attr) != 0) {
			ret = TR_ERR_SYS;
			goto fail;
		}
		if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
			pthread_condattr_destroy(&attr);
			ret = TR_ERR_SYS;
			goto fail;
		}
		if (pthread_cond_init(&listener->drain_wait_cond, &attr) != 0) {
			pthread_condattr_destroy(&attr);
			ret = TR_ERR_SYS;
			goto fail;
		}
		pthread_condattr_destroy(&attr);
		drain_wait_cond_ready = 1;
	}

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
	if (listener->message_pool_ready) {
		int pool_ret = tr_buffer_pool_destroy(&listener->message_pool);
#ifndef NDEBUG
		assert(pool_ret == TR_OK);
#endif
		if (pool_ret != TR_OK)
			return pool_ret;
		listener->message_pool_ready = 0;
	}
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	free(listener->connections);
	free(listener->sessions);
	if (drain_wait_cond_ready)
		pthread_cond_destroy(&listener->drain_wait_cond);
	if (drain_wait_lock_ready)
		pthread_mutex_destroy(&listener->drain_wait_lock);
	free(listener);
	return ret;
}

struct tr_pipeline_listener_listen_request {
	struct tr_pipeline_listener *listener;
	int fd;
	uint16_t bound_port;
};

static int tr_pipeline_listener_publish_listen(void *arg)
{
	struct tr_pipeline_listener_listen_request *request =
		(struct tr_pipeline_listener_listen_request *)arg;
	struct tr_pipeline_listener *listener = request->listener;

	/*
	 * 这个检查与 source registration 处于同一个 Reactor ownership
	 * transaction。不能在外部线程先读 listener_registered/listen_fd，
	 * 否则会重新引入 lifecycle data race。
	 */
	if (listener->listen_fd >= 0 || listener->listener_registered)
		return TR_ERR_STATE;

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_closed) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_CLOSED;
	}
	pthread_mutex_unlock(&listener->drain_wait_lock);

	listener->listen_fd = request->fd;
	listener->bound_port = request->bound_port;
	listener->listener_registered = 1;
	listener->draining = 0;
	tr_pipeline_listener_publish_listening_on_owner(listener);
	return TR_OK;
}

int tr_pipeline_listener_listen_ipv4(
	struct tr_pipeline_listener *listener, const char *address,
	uint16_t port, int backlog, uint16_t *out_bound_port)
{
	struct tr_pipeline_listener_listen_request request;
	int fd = -1;
	uint16_t bound = 0U;
	int ret;

	if (!listener || !address || backlog <= 0)
		return TR_ERR_INVALID;

	ret = tr_tcp_listen_ipv4(
		address, port, backlog, &fd, &bound);
	if (ret != TR_OK)
		return ret;

	request.listener = listener;
	request.fd = fd;
	request.bound_port = bound;

	/*
	 * source 对 epoll 可见与 Listener owner-state 发布必须是同一个
	 * lifecycle transaction。函数成功返回前 callback 已经能安全观察
	 * listen_fd/bound_port/listener_registered/draining 的完整状态。
	 */
	ret = tr_reactor_listener_register_publish(
		listener->config.owner, fd,
		tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_listen, &request);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}

	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

static int tr_pipeline_listener_publish_drained_admission(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	int was_draining;

	was_draining = listener->draining;
	listener->listener_registered = 0;
	if (listener->listen_fd >= 0) {
		tr_socket_close(&listener->listen_fd);
		listener->bound_port = 0U;
	}
	listener->draining = 1;
	if (!was_draining)
		tr_pipeline_listener_publish_drain_start_on_owner(listener);
	else
		tr_pipeline_listener_publish_drain_progress_on_owner(listener);
	return TR_OK;
}

static int tr_pipeline_listener_begin_drain_on_owner(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;

	if (!listener)
		return TR_ERR_INVALID;

	/*
	 * 即使已经 draining 也经过 Reactor listener ownership 检查。source 已
	 * detach 时 unregister_call 仍执行 publication，因此该操作天然幂等。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_drained_admission, listener);
}

int tr_pipeline_listener_begin_drain(struct tr_pipeline_listener *listener)
{
	if (!listener)
		return TR_ERR_INVALID;

	/*
	 * running/stopped 都只在 Reactor 的串行化域读取和修改 Listener
	 * lifecycle state；外部线程不直接读取 listener_registered/listen_fd。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_publish_drained_admission, listener);
}

int tr_pipeline_listener_wait_drained(
	struct tr_pipeline_listener *listener, uint32_t timeout_ms)
{
	struct timespec deadline;
	uint64_t generation;
	int timed = timeout_ms != 0U;
	int result = TR_OK;

	if (!listener)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context())
		return TR_ERR_STATE;

	if (timed) {
		uint64_t now_ns;
		uint64_t timeout_ns;
		uint64_t deadline_ns;

		if (clock_gettime(CLOCK_MONOTONIC, &deadline) != 0)
			return TR_ERR_SYS;
		now_ns = (uint64_t)deadline.tv_sec * UINT64_C(1000000000) +
			 (uint64_t)deadline.tv_nsec;
		timeout_ns = (uint64_t)timeout_ms * UINT64_C(1000000);
		deadline_ns = UINT64_MAX - now_ns < timeout_ns ?
			UINT64_MAX : now_ns + timeout_ns;
		deadline.tv_sec =
			(time_t)(deadline_ns / UINT64_C(1000000000));
		deadline.tv_nsec =
			(long)(deadline_ns % UINT64_C(1000000000));
	}

	pthread_mutex_lock(&listener->drain_wait_lock);
	if (listener->drain_wait_closed) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_CLOSED;
	}
	if (!listener->drain_wait_active || listener->drain_generation == 0U) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_STATE;
	}

	generation = listener->drain_generation;
	if (listener->drained_generation == generation) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_OK;
	}
	if (listener->drain_waiters == UINT32_MAX) {
		pthread_mutex_unlock(&listener->drain_wait_lock);
		return TR_ERR_STATE;
	}
	listener->drain_waiters++;

	while (listener->drained_generation != generation) {
		int ret;

		if (listener->drain_wait_closed) {
			result = TR_ERR_CLOSED;
			break;
		}
		if (!listener->drain_wait_active ||
		    listener->drain_generation != generation) {
			result = TR_ERR_STALE;
			break;
		}

		if (timed)
			ret = pthread_cond_timedwait(
				&listener->drain_wait_cond,
				&listener->drain_wait_lock, &deadline);
		else
			ret = pthread_cond_wait(
				&listener->drain_wait_cond,
				&listener->drain_wait_lock);
		if (ret == 0)
			continue;
		if (timed && ret == ETIMEDOUT) {
			result = TR_ERR_TIMEOUT;
			break;
		}
		result = TR_ERR_SYS;
		break;
	}

	assert(listener->drain_waiters != 0U);
	listener->drain_waiters--;
	pthread_mutex_unlock(&listener->drain_wait_lock);
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
	int result;

	/*
	 * stop 的整个状态转换都由 owner 串行化：先关闭 listener admission，
	 * 再关闭已有连接/transport，最后在同一个 owner turn 检查计数是否归零。
	 * 外部线程不直接观察 owner-only counters。
	 */
	result = tr_pipeline_listener_begin_drain_on_owner(listener);

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

static int tr_pipeline_listener_stop_stopped(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;
	int ret;

	ret = tr_pipeline_listener_publish_drained_admission(listener);
	if (ret != TR_OK)
		return ret;
	if (listener->connections_current != 0U ||
	    listener->pipelines_current != 0U)
		return TR_ERR_STATE;
	return TR_OK;
}

int tr_pipeline_listener_stop(struct tr_pipeline_listener *listener)
{
	struct tr_pipeline_listener_stop_request request;
	int ret;

	if (!listener)
		return TR_ERR_INVALID;

	request.listener = listener;
	ret = tr_reactor_call(
		listener->config.owner,
		tr_pipeline_listener_stop_on_owner, &request);
	if (ret != TR_ERR_CLOSED)
		return ret;

	/*
	 * tr_reactor_call() 的 CLOSED 同时覆盖“未启动/已停止”和“正在停止”。
	 * unregister_call 只允许前者进入 ctl_lock direct path；正在停止时继续
	 * 返回 CLOSED，绝不越过 owner teardown barrier。detach、状态发布和
	 * counter 检查在同一串行化区间完成。
	 */
	return tr_reactor_listener_unregister_call(
		listener->config.owner, tr_pipeline_listener_on_ready, listener,
		tr_pipeline_listener_stop_stopped, listener);
}

static int tr_pipeline_listener_verify_destroy(void *arg)
{
	struct tr_pipeline_listener *listener =
		(struct tr_pipeline_listener *)arg;

	if (!listener)
		return TR_ERR_INVALID;
	if (listener->listener_registered || listener->listen_fd >= 0 ||
	    listener->connections_current != 0U ||
	    listener->pipelines_current != 0U)
		return TR_ERR_STATE;

	return tr_pipeline_listener_close_wait_admission(listener);
}

int tr_pipeline_listener_destroy(struct tr_pipeline_listener *listener)
{
	int ret;

	if (!listener)
		return TR_OK;

	/*
	 * Validate owner-only lifecycle state in the same serialized domain used by
	 * listener/source teardown. Fully stopped Reactor uses ctl_lock direct path;
	 * an in-progress stop returns CLOSED instead of allowing free to race owner.
	 */
	ret = tr_reactor_call_or_stopped(
		listener->config.owner,
		tr_pipeline_listener_verify_destroy, listener);
#ifndef NDEBUG
	assert(ret == TR_OK);
#endif
	if (ret != TR_OK)
		return ret;

	if (listener->message_pool_ready) {
		ret = tr_buffer_pool_destroy(&listener->message_pool);
		if (ret != TR_OK)
			return ret;
		listener->message_pool_ready = 0;
	}
	if (listener->registry)
		tr_pipeline_registry_destroy(listener->registry);
	free(listener->connections);
	free(listener->sessions);
	(void)pthread_cond_destroy(&listener->drain_wait_cond);
	pthread_mutex_destroy(&listener->drain_wait_lock);
	free(listener);
	return TR_OK;
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
