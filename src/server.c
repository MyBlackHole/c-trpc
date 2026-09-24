#include "tr/server.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "tr/buffer.h"
#include "tr/channel.h"
#include "tr/reactor.h"
#include "tr/rpc_wire.h"
#include "tr/socket.h"
#include "tr/status.h"

enum tr_server_method_kind {
	TR_SERVER_METHOD_UNARY = 1,
	TR_SERVER_METHOD_STREAM = 2
};

struct tr_server_method {
	int used;
	enum tr_server_method_kind kind;
	struct tr_rpc_method_desc desc;
	tr_rpc_unary_handler unary_handler;
	struct tr_rpc_stream_handlers stream_handlers;
	void *handler_arg;
};

struct tr_server_peer {
	int used;
	struct tr_conn_handle connection;
	struct tr_channel *channel;
	struct tr_rpc_endpoint *rpc;
};

struct tr_server {
	struct tr_server_config config;

	struct tr_reactor *reactor;
	struct tr_buffer_pool rpc_message_pool;
	struct tr_buffer_pool reassembly_pool;
	int rpc_pool_ready;
	int reassembly_pool_ready;

	struct tr_server_method *methods;
	uint32_t method_count;

	struct tr_server_peer *peers;
	uint32_t peer_count;

	pthread_mutex_t lock;
	pthread_t accept_thread;
	pthread_t reap_thread;
	int accept_thread_started;
	int reap_thread_started;
	int started;
	int stop_accept;
	int stop_reap;

	int listen_fd;
	uint16_t bound_port;
};

static uint64_t tr_server_now_ms(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
		return 0;
	return (uint64_t)ts.tv_sec * UINT64_C(1000) +
	       (uint64_t)ts.tv_nsec / UINT64_C(1000000);
}

static void tr_server_normalize_config(struct tr_server_config *config)
{
	struct tr_server_config defaults;
	struct tr_facade_limits *l;
	const struct tr_facade_limits *d;

	tr_server_config_init(&defaults);
	if (config->max_peers == 0)
		config->max_peers = defaults.max_peers;
	if (config->listen_backlog <= 0)
		config->listen_backlog = defaults.listen_backlog;

	l = &config->limits;
	d = &defaults.limits;
#define TR_LIMIT_DEFAULT(field)              \
	do {                                 \
		if (l->field == 0)           \
			l->field = d->field; \
	} while (0)
	TR_LIMIT_DEFAULT(max_streams);
	TR_LIMIT_DEFAULT(max_methods);
	TR_LIMIT_DEFAULT(max_calls);
	TR_LIMIT_DEFAULT(max_frame_payload_bytes);
	TR_LIMIT_DEFAULT(max_message_bytes);
	TR_LIMIT_DEFAULT(initial_window_bytes);
	TR_LIMIT_DEFAULT(window_update_threshold_bytes);
	TR_LIMIT_DEFAULT(command_capacity);
	TR_LIMIT_DEFAULT(tx_item_capacity);
	TR_LIMIT_DEFAULT(control_tx_item_capacity);
	TR_LIMIT_DEFAULT(rx_buffer_count);
	TR_LIMIT_DEFAULT(rpc_message_pool_count);
	TR_LIMIT_DEFAULT(rpc_message_buffer_bytes);
	TR_LIMIT_DEFAULT(reassembly_pool_count);
	TR_LIMIT_DEFAULT(executor_threads);
	TR_LIMIT_DEFAULT(executor_queue_capacity);
#undef TR_LIMIT_DEFAULT
}

void tr_server_config_init(struct tr_server_config *config)
{
	if (!config)
		return;

	memset(config, 0, sizeof(*config));
	tr_facade_limits_init(&config->limits);
	config->max_peers = 64U;
	config->listen_backlog = 128;
	config->keepalive_interval_ms = 30000U;
	config->keepalive_timeout_ms = 10000U;
}

static int tr_server_register_methods_on_peer(struct tr_server *server,
					      struct tr_server_peer *peer)
{
	uint32_t i;

	for (i = 0; i < server->method_count; ++i) {
		struct tr_server_method *m = &server->methods[i];
		int ret;

		if (!m->used)
			continue;
		if (m->kind == TR_SERVER_METHOD_UNARY) {
			ret = tr_rpc_register_method(peer->rpc, &m->desc,
						     m->unary_handler,
						     m->handler_arg);
		} else {
			ret = tr_rpc_register_stream_method(peer->rpc, &m->desc,
							    &m->stream_handlers,
							    m->handler_arg);
		}
		if (ret != TR_OK)
			return ret;
	}
	return TR_OK;
}

static int tr_server_peer_handle_equal(const struct tr_server_peer *a,
				       const struct tr_server_peer *b)
{
	return a->connection.reactor == b->connection.reactor &&
	       a->connection.slot == b->connection.slot &&
	       a->connection.generation == b->connection.generation;
}

static int tr_server_peer_disconnected(struct tr_server_peer *peer)
{
	enum tr_connection_state state;
	int ret;

	if (!peer->used || !peer->connection.reactor)
		return 0;

	ret = tr_reactor_get_connection_state(peer->connection, &state);
	if (ret == TR_ERR_STALE)
		return 1;
	if (ret != TR_OK)
		return 0;

	return state == TR_CONN_FREE || state == TR_CONN_CLOSED ||
	       state == TR_CONN_ERROR;
}

static int tr_server_take_reapable_peer(struct tr_server *server,
				       struct tr_server_peer *out)
{
	uint32_t i;

	for (i = 0; i < server->config.max_peers; ++i) {
		struct tr_server_peer snapshot;
		int reap = 0;

		pthread_mutex_lock(&server->lock);
		if (!server->peers[i].used) {
			pthread_mutex_unlock(&server->lock);
			continue;
		}
		snapshot = server->peers[i];
		pthread_mutex_unlock(&server->lock);

		if (!tr_server_peer_disconnected(&snapshot))
			continue;

		pthread_mutex_lock(&server->lock);
		if (server->peers[i].used &&
		    tr_server_peer_handle_equal(&server->peers[i], &snapshot)) {
			*out = server->peers[i];
			memset(&server->peers[i], 0, sizeof(server->peers[i]));
			if (server->peer_count != 0)
				server->peer_count--;
			reap = 1;
		}
		pthread_mutex_unlock(&server->lock);

		if (reap)
			return 1;
	}

	return 0;
}

static void tr_server_destroy_peer(struct tr_server_peer *peer)
{
	if (peer->rpc)
		tr_rpc_endpoint_destroy(peer->rpc);
	if (peer->channel)
		tr_channel_destroy(peer->channel);
	memset(peer, 0, sizeof(*peer));
}

static void *tr_server_reap_main(void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;

	for (;;) {
		struct tr_server_peer peer;
		struct timespec pause_time;
		int stop;

		pthread_mutex_lock(&server->lock);
		stop = server->stop_reap;
		pthread_mutex_unlock(&server->lock);
		if (stop)
			break;

		memset(&peer, 0, sizeof(peer));
		if (tr_server_take_reapable_peer(server, &peer)) {
			tr_server_destroy_peer(&peer);
			continue;
		}

		pause_time.tv_sec = 0;
		pause_time.tv_nsec = 10000000L;
		while (nanosleep(&pause_time, &pause_time) != 0 &&
		       errno == EINTR)
			;
	}

	return NULL;
}

static void tr_server_stop_reaper(struct tr_server *server)
{
	pthread_mutex_lock(&server->lock);
	server->stop_reap = 1;
	pthread_mutex_unlock(&server->lock);

	if (server->reap_thread_started) {
		(void)pthread_join(server->reap_thread, NULL);
		server->reap_thread_started = 0;
	}
}

static int tr_server_adopt_peer(struct tr_server *server, int fd)
{
	struct tr_channel_config channel_config;
	struct tr_rpc_endpoint_config rpc_config;
	struct tr_channel_keepalive_config keepalive_config;
	struct tr_server_peer *peer;
	uint32_t slot;
	int ret;

	pthread_mutex_lock(&server->lock);
	if (server->peer_count >= server->config.max_peers) {
		pthread_mutex_unlock(&server->lock);
		tr_socket_close(&fd);
		return TR_AGAIN;
	}

	for (slot = 0; slot < server->config.max_peers; ++slot)
		if (!server->peers[slot].used)
			break;
	if (slot == server->config.max_peers) {
		pthread_mutex_unlock(&server->lock);
		tr_socket_close(&fd);
		return TR_AGAIN;
	}

	peer = &server->peers[slot];
	memset(peer, 0, sizeof(*peer));
	pthread_mutex_unlock(&server->lock);

	ret = tr_reactor_adopt_fd(server->reactor, fd, &peer->connection);
	if (ret != TR_OK) {
		tr_socket_close(&fd);
		return ret;
	}

	memset(&channel_config, 0, sizeof(channel_config));
	channel_config.role = TR_CHANNEL_SERVER;
	channel_config.mode = TR_CHANNEL_SHARED_CONNECTION;
	channel_config.max_streams = server->config.limits.max_streams;
	channel_config.initial_window_bytes =
		server->config.limits.initial_window_bytes;
	channel_config.window_update_threshold_bytes =
		server->config.limits.window_update_threshold_bytes;
	channel_config.max_message_bytes =
		server->config.limits.max_message_bytes;
	channel_config.reassembly_pool = &server->reassembly_pool;

	ret = tr_channel_create(&channel_config, peer->connection,
				peer->connection, NULL, NULL, NULL, NULL,
				&peer->channel);
	if (ret != TR_OK)
		goto fail_connection;

	memset(&rpc_config, 0, sizeof(rpc_config));
	rpc_config.role = TR_RPC_SERVER;
	rpc_config.max_methods = server->config.limits.max_methods;
	rpc_config.max_calls = server->config.limits.max_calls;
	rpc_config.message_pool = &server->rpc_message_pool;
	rpc_config.executor_threads = server->config.limits.executor_threads;
	rpc_config.executor_queue_capacity =
		server->config.limits.executor_queue_capacity;

	ret = tr_rpc_endpoint_create(peer->channel, &rpc_config, &peer->rpc);
	if (ret != TR_OK)
		goto fail_channel;

	ret = tr_server_register_methods_on_peer(server, peer);
	if (ret != TR_OK)
		goto fail_rpc;

	if (server->config.keepalive_interval_ms != 0) {
		keepalive_config.interval_ms =
			server->config.keepalive_interval_ms;
		keepalive_config.timeout_ms =
			server->config.keepalive_timeout_ms;
		ret = tr_channel_enable_keepalive(peer->channel,
						  &keepalive_config);
		if (ret != TR_OK)
			goto fail_rpc;
	}

	pthread_mutex_lock(&server->lock);
	peer->used = 1;
	server->peer_count++;
	pthread_mutex_unlock(&server->lock);
	return TR_OK;

fail_rpc:
fail_channel:
	/*
     * Do not free Channel/RPC objects while Reactor callbacks may still hold
     * them. Retain the failed peer until server destruction, after Reactor
     * ownership has stopped.
     */
	(void)tr_reactor_close(peer->connection);
	pthread_mutex_lock(&server->lock);
	peer->used = 1;
	server->peer_count++;
	pthread_mutex_unlock(&server->lock);
	return ret;

fail_connection:
	(void)tr_reactor_close(peer->connection);
	memset(&peer->connection, 0, sizeof(peer->connection));
	return ret;
}

static void *tr_server_accept_main(void *arg)
{
	struct tr_server *server = (struct tr_server *)arg;

	for (;;) {
		struct pollfd pfd;
		int stop;
		int listener;
		int ret;

		pthread_mutex_lock(&server->lock);
		stop = server->stop_accept;
		listener = server->listen_fd;
		pthread_mutex_unlock(&server->lock);
		if (stop)
			break;

		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = listener;
		pfd.events = POLLIN;
		do {
			ret = poll(&pfd, 1, 50);
		} while (ret < 0 && errno == EINTR);
		if (ret <= 0)
			continue;

		for (;;) {
			int fd = -1;
			ret = tr_tcp_accept(listener, &fd);
			if (ret == TR_AGAIN)
				break;
			if (ret != TR_OK)
				break;

			/* tr_server_adopt_peer() always consumes the accepted fd. */
			(void)tr_server_adopt_peer(server, fd);
		}
	}

	return NULL;
}

int tr_server_create(const struct tr_server_config *config,
		     struct tr_server **out)
{
	struct tr_server_config effective;
	struct tr_reactor_config reactor_config;
	struct tr_server *server;
	int ret;

	if (!out)
		return TR_ERR_INVALID;
	*out = NULL;

	if (config)
		effective = *config;
	else
		tr_server_config_init(&effective);
	tr_server_normalize_config(&effective);

	if (effective.limits.max_message_bytes <
		    effective.limits.max_frame_payload_bytes ||
	    effective.limits.rpc_message_buffer_bytes <
		    TR_RPC_WIRE_HEADER_SIZE ||
	    effective.max_peers == 0)
		return TR_ERR_INVALID;

	server = (struct tr_server *)calloc(1, sizeof(*server));
	if (!server)
		return TR_ERR_NOMEM;
	server->config = effective;
	server->listen_fd = -1;

	if (pthread_mutex_init(&server->lock, NULL) != 0) {
		free(server);
		return TR_ERR_INVALID;
	}

	server->methods = (struct tr_server_method *)calloc(
		effective.limits.max_methods, sizeof(*server->methods));
	server->peers = (struct tr_server_peer *)calloc(effective.max_peers,
							sizeof(*server->peers));
	if (!server->methods || !server->peers) {
		ret = TR_ERR_NOMEM;
		goto fail;
	}

	ret = tr_buffer_pool_init(&server->rpc_message_pool,
				  effective.limits.rpc_message_pool_count,
				  effective.limits.rpc_message_buffer_bytes);
	if (ret != TR_OK)
		goto fail;
	server->rpc_pool_ready = 1;

	ret = tr_buffer_pool_init(&server->reassembly_pool,
				  effective.limits.reassembly_pool_count,
				  effective.limits.max_message_bytes);
	if (ret != TR_OK)
		goto fail;
	server->reassembly_pool_ready = 1;

	memset(&reactor_config, 0, sizeof(reactor_config));
	reactor_config.max_connections = effective.max_peers + 4U;
	reactor_config.command_capacity = effective.limits.command_capacity;
	reactor_config.tx_item_capacity = effective.limits.tx_item_capacity;
	reactor_config.control_tx_item_capacity =
		effective.limits.control_tx_item_capacity;
	reactor_config.rx_buffer_count = effective.limits.rx_buffer_count;
	reactor_config.rx_buffer_size =
		effective.limits.max_frame_payload_bytes;
	reactor_config.max_payload_len =
		effective.limits.max_frame_payload_bytes;
	reactor_config.rx_budget_bytes =
		effective.limits.max_frame_payload_bytes > UINT32_MAX / 4U ?
			UINT32_MAX :
			effective.limits.max_frame_payload_bytes * 4U;
	reactor_config.tx_budget_bytes = reactor_config.rx_budget_bytes;

	ret = tr_reactor_create(&reactor_config, NULL, NULL, NULL,
				&server->reactor);
	if (ret != TR_OK)
		goto fail;

	*out = server;
	return TR_OK;

fail:
	if (server->reactor)
		tr_reactor_destroy(server->reactor);
	if (server->reassembly_pool_ready)
		tr_buffer_pool_destroy(&server->reassembly_pool);
	if (server->rpc_pool_ready)
		tr_buffer_pool_destroy(&server->rpc_message_pool);
	free(server->peers);
	free(server->methods);
	pthread_mutex_destroy(&server->lock);
	free(server);
	return ret;
}

static int tr_server_validate_method(const struct tr_rpc_method_desc *method,
				     int unary)
{
	if (!method || method->service_id == 0U || method->method_id == 0U ||
	    (method->request_cardinality != TR_RPC_ONE &&
	     method->request_cardinality != TR_RPC_MANY) ||
	    (method->response_cardinality != TR_RPC_ONE &&
	     method->response_cardinality != TR_RPC_MANY) ||
	    method->request_codec_id != TR_RPC_CODEC_RAW ||
	    method->response_codec_id != TR_RPC_CODEC_RAW ||
	    (method->lane != TR_LANE_CONTROL && method->lane != TR_LANE_BULK) ||
	    method->max_request_bytes == 0U || method->max_response_bytes == 0U)
		return TR_ERR_INVALID;

	if (unary && (method->request_cardinality != TR_RPC_ONE ||
		      method->response_cardinality != TR_RPC_ONE))
		return TR_ERR_INVALID;
	return TR_OK;
}

static int tr_server_method_exists(struct tr_server *server,
				   uint32_t service_id, uint32_t method_id)
{
	uint32_t i;

	for (i = 0; i < server->method_count; ++i)
		if (server->methods[i].used &&
		    server->methods[i].desc.service_id == service_id &&
		    server->methods[i].desc.method_id == method_id)
			return 1;
	return 0;
}

int tr_server_register_method(struct tr_server *server,
			      const struct tr_rpc_method_desc *method,
			      tr_rpc_unary_handler handler, void *handler_arg)
{
	struct tr_server_method *entry;

	if (!server || !handler ||
	    tr_server_validate_method(method, 1) != TR_OK)
		return TR_ERR_INVALID;
	if (server->started)
		return TR_ERR_STATE;
	if (server->method_count >= server->config.limits.max_methods)
		return TR_AGAIN;
	if (tr_server_method_exists(server, method->service_id,
				    method->method_id))
		return TR_ERR_STATE;

	entry = &server->methods[server->method_count++];
	memset(entry, 0, sizeof(*entry));
	entry->used = 1;
	entry->kind = TR_SERVER_METHOD_UNARY;
	entry->desc = *method;
	entry->unary_handler = handler;
	entry->handler_arg = handler_arg;
	return TR_OK;
}

int tr_server_register_stream_method(
	struct tr_server *server, const struct tr_rpc_method_desc *method,
	const struct tr_rpc_stream_handlers *handlers, void *handler_arg)
{
	struct tr_server_method *entry;

	if (!server || !handlers ||
	    tr_server_validate_method(method, 0) != TR_OK)
		return TR_ERR_INVALID;
	if (server->started)
		return TR_ERR_STATE;
	if (server->method_count >= server->config.limits.max_methods)
		return TR_AGAIN;
	if (tr_server_method_exists(server, method->service_id,
				    method->method_id))
		return TR_ERR_STATE;

	entry = &server->methods[server->method_count++];
	memset(entry, 0, sizeof(*entry));
	entry->used = 1;
	entry->kind = TR_SERVER_METHOD_STREAM;
	entry->desc = *method;
	entry->stream_handlers = *handlers;
	entry->handler_arg = handler_arg;
	return TR_OK;
}

int tr_server_listen(struct tr_server *server, const char *ipv4_address,
		     uint16_t port, uint16_t *out_bound_port)
{
	int fd = -1;
	uint16_t bound = 0;
	int ret;

	if (!server || !ipv4_address)
		return TR_ERR_INVALID;
	if (server->started || server->listen_fd >= 0)
		return TR_ERR_STATE;

	ret = tr_tcp_listen_ipv4(ipv4_address, port,
				 server->config.listen_backlog, &fd, &bound);
	if (ret != TR_OK)
		return ret;

	server->listen_fd = fd;
	server->bound_port = bound;
	if (out_bound_port)
		*out_bound_port = bound;
	return TR_OK;
}

int tr_server_start(struct tr_server *server)
{
	int ret;

	if (!server || server->listen_fd < 0)
		return TR_ERR_STATE;
	if (server->started)
		return TR_ERR_STATE;

	ret = tr_reactor_start(server->reactor);
	if (ret != TR_OK)
		return ret;

	pthread_mutex_lock(&server->lock);
	server->stop_accept = 0;
	server->stop_reap = 0;
	pthread_mutex_unlock(&server->lock);

	if (pthread_create(&server->reap_thread, NULL, tr_server_reap_main,
			   server) != 0) {
		(void)tr_reactor_stop(server->reactor);
		return TR_ERR_SYS;
	}
	server->reap_thread_started = 1;

	if (pthread_create(&server->accept_thread, NULL, tr_server_accept_main,
			   server) != 0) {
		tr_server_stop_reaper(server);
		(void)tr_reactor_stop(server->reactor);
		return TR_ERR_SYS;
	}

	server->accept_thread_started = 1;
	server->started = 1;
	return TR_OK;
}

static void tr_server_stop_accepting(struct tr_server *server)
{
	pthread_mutex_lock(&server->lock);
	server->stop_accept = 1;
	pthread_mutex_unlock(&server->lock);

	if (server->accept_thread_started) {
		pthread_join(server->accept_thread, NULL);
		server->accept_thread_started = 0;
	}

	tr_socket_close(&server->listen_fd);
}

int tr_server_drain(struct tr_server *server, uint32_t timeout_ms)
{
	uint64_t start;
	uint32_t i;
	int final = TR_OK;

	if (!server || !server->started)
		return TR_ERR_STATE;

	tr_server_stop_accepting(server);
	tr_server_stop_reaper(server);

	for (i = 0; i < server->config.max_peers; ++i)
		if (server->peers[i].used) {
			int ret = tr_channel_begin_drain(
				server->peers[i].channel);
			if (ret != TR_OK && ret != TR_AGAIN && final == TR_OK)
				final = ret;
		}

	start = tr_server_now_ms();
	for (i = 0; i < server->config.max_peers; ++i) {
		uint32_t remaining = timeout_ms;
		int ret;

		if (!server->peers[i].used)
			continue;
		if (timeout_ms != 0) {
			uint64_t elapsed = tr_server_now_ms() - start;
			if (elapsed >= timeout_ms)
				return TR_ERR_TIMEOUT;
			remaining = (uint32_t)(timeout_ms - elapsed);
		}
		ret = tr_channel_wait_drained(server->peers[i].channel,
					      remaining);
		if (ret != TR_OK && final == TR_OK)
			final = ret;
	}

	return final;
}

void tr_server_destroy(struct tr_server *server)
{
	uint32_t i;

	if (!server)
		return;

	if (server->accept_thread_started || server->listen_fd >= 0)
		tr_server_stop_accepting(server);
	if (server->reap_thread_started)
		tr_server_stop_reaper(server);

	if (server->reactor && server->started)
		(void)tr_reactor_stop(server->reactor);

	for (i = 0; i < server->config.max_peers; ++i) {
		struct tr_server_peer *peer = &server->peers[i];
		if (!peer->used && !peer->channel && !peer->rpc)
			continue;
		tr_server_destroy_peer(peer);
	}

	if (server->reactor)
		tr_reactor_destroy(server->reactor);
	if (server->reassembly_pool_ready)
		tr_buffer_pool_destroy(&server->reassembly_pool);
	if (server->rpc_pool_ready)
		tr_buffer_pool_destroy(&server->rpc_message_pool);

	free(server->peers);
	free(server->methods);
	pthread_mutex_destroy(&server->lock);
	free(server);
}
