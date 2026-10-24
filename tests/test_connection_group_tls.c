#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "tr/client.h"
#include "tr/server.h"
#include "tr/status.h"
#include "tr/transport.h"

#include "../src/execution/reactor_internal.h"
#include "../src/execution/reactor.h"
#include "../src/transport/protocol/wire.h"
#include "tls_test_identity.h"

#define TEST_GROUP_ID UINT64_C(0x7a14)
#define TEST_GROUP_EPOCH UINT64_C(19)
#define TEST_STREAM_ID 43U
#define TEST_OFFER_MESSAGE_ID UINT64_C(101)
#define TEST_READY_MESSAGE_ID UINT64_C(102)
#define TEST_DATA_MESSAGE_ID UINT64_C(103)
#define TEST_PROXY_CONNECTIONS 3U
#define TEST_ASYNC_AUTHORIZATIONS 12U
#define TEST_AUTH_WRITE_FAULT_EINTR 1
#define TEST_AUTH_WRITE_FAULT_EAGAIN 2

static _Atomic int test_authorization_event_fd = -1;
static _Atomic int test_last_aux_event_fd = -1;
static _Atomic int test_authorization_write_fault;
static _Atomic unsigned test_authorization_write_fault_hits;

int __real_tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
                                         uint32_t events,
                                         tr_reactor_aux_event_cb callback,
                                         void *arg);
ssize_t __real_write(int fd, const void *buffer, size_t size);

int __wrap_tr_reactor_aux_event_register(struct tr_reactor *reactor, int fd,
                                         uint32_t events,
                                         tr_reactor_aux_event_cb callback,
                                         void *arg)
{
	int ret = __real_tr_reactor_aux_event_register(reactor, fd, events,
	                                               callback, arg);
	if (ret == TR_OK)
		atomic_store_explicit(&test_last_aux_event_fd, fd,
		                      memory_order_relaxed);
	return ret;
}

ssize_t __wrap_write(int fd, const void *buffer, size_t size)
{
	if (fd == atomic_load_explicit(&test_authorization_event_fd,
	                               memory_order_relaxed)) {
		int fault = atomic_exchange_explicit(
		        &test_authorization_write_fault, 0,
		        memory_order_relaxed);

		if (fault == TEST_AUTH_WRITE_FAULT_EINTR) {
			atomic_fetch_add_explicit(
			        &test_authorization_write_fault_hits, 1U,
			        memory_order_relaxed);
			errno = EINTR;
			return -1;
		}
		if (fault == TEST_AUTH_WRITE_FAULT_EAGAIN) {
			uint64_t saturated = UINT64_MAX - 1U;
			ssize_t written;

			/* eventfd 的最大可写计数加一会真实触发饱和 EAGAIN，
			 * 同时保持可读，等价于内核中已有待处理通知。 */
			written =
			        __real_write(fd, &saturated, sizeof(saturated));
			assert(written == (ssize_t)sizeof(saturated));
			written = __real_write(fd, buffer, size);
			assert(written < 0 && errno == EAGAIN);
			atomic_fetch_add_explicit(
			        &test_authorization_write_fault_hits, 1U,
			        memory_order_relaxed);
			errno = EAGAIN;
			return -1;
		}
	}
	return __real_write(fd, buffer, size);
}

struct connection_group_tls_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	_Atomic unsigned control_authorizations;
	_Atomic unsigned data_authorizations;
	_Atomic unsigned ready_callbacks;
	_Atomic unsigned messages;
	_Atomic unsigned authorization_rejections;
	_Atomic unsigned async_authorizations;
	_Atomic unsigned async_cancellations;
	_Atomic int defer_cancel_ack;
	struct tr_server *server;
	struct tr_tls_provider_operation
	        async_operations[TEST_ASYNC_AUTHORIZATIONS];
	enum tr_connection_group_role async_roles[TEST_ASYNC_AUTHORIZATIONS];
	uint64_t async_group_ids[TEST_ASYNC_AUTHORIZATIONS];
};

struct tls_connection_proxy;

struct tls_proxy_bridge {
	struct tls_connection_proxy *proxy;
	pthread_t thread;
	int started;
	int client_fd;
	int backend_fd;
};

struct tls_connection_proxy {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	pthread_t accept_thread;
	int accept_thread_started;
	int listen_fd;
	int errors;
	unsigned accepted;
	unsigned completed;
	unsigned bridge_count;
	uint16_t port;
	uint16_t primary_backend_port;
	uint16_t alternate_backend_port;
	struct tls_proxy_bridge bridges[TEST_PROXY_CONNECTIONS];
};

struct tls_stall_server {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	pthread_t thread;
	int listen_fd;
	int thread_started;
	int accepted;
	int hello_received;
	int peer_closed;
	int first_record_type;
	uint16_t port;
};

static const uint8_t client_uri[] = "spiffe://c-trpc.example/client";
static const uint8_t message_body[] = "client-group-tls-data";
static atomic_uint group_capability_frames_sent;

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload);

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
			   uint32_t flags, uint32_t stream_id,
			   uint64_t message_id, struct tr_buffer *payload)
{
	int ret = __real_tr_reactor_send(connection, type, flags, stream_id,
					 message_id, payload);

	if (ret == TR_OK && type == TR_FRAME_MESSAGE_CAPS)
		atomic_fetch_add_explicit(&group_capability_frames_sent, 1U,
		                          memory_order_relaxed);
	return ret;
}

static void signal_state_changed(struct connection_group_tls_state *state)
{
	pthread_mutex_lock(&state->lock);
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static int connect_ipv4(uint16_t port)
{
	struct sockaddr_in address;
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
		close(fd);
		return -1;
	}
	return fd;
}

static int proxy_send_all(int fd, const uint8_t *bytes, size_t length)
{
	size_t offset = 0U;
	while (offset < length) {
		ssize_t written =
		        send(fd, bytes + offset, length - offset, MSG_NOSIGNAL);
		if (written > 0) {
			offset += (size_t)written;
			continue;
		}
		if (written < 0 && errno == EINTR)
			continue;
		return -1;
	}
	return 0;
}

static void *tls_proxy_bridge_main(void *arg)
{
	struct tls_proxy_bridge *bridge = arg;
	struct tls_connection_proxy *proxy;
	struct pollfd descriptors[2];
	int endpoints[2];
	int read_open[2] = {1, 1};
	int failed = 0;
	if (!bridge)
		return NULL;
	proxy = bridge->proxy;
	endpoints[0] = bridge->client_fd;
	endpoints[1] = bridge->backend_fd;
	while (read_open[0] || read_open[1]) {
		int poll_result;
		descriptors[0].fd = read_open[0] ? endpoints[0] : -1;
		descriptors[0].events = read_open[0] ? POLLIN : 0;
		descriptors[0].revents = 0;
		descriptors[1].fd = read_open[1] ? endpoints[1] : -1;
		descriptors[1].events = read_open[1] ? POLLIN : 0;
		descriptors[1].revents = 0;
		poll_result = poll(descriptors, 2U, -1);
		if (poll_result < 0) {
			if (errno == EINTR)
				continue;
			failed = 1;
			break;
		}
		for (int i = 0; i < 2; ++i) {
			uint8_t buffer[8192];
			ssize_t received;
			int destination = endpoints[1 - i];
			if (!read_open[i] ||
			    !(descriptors[i].revents &
			      (POLLIN | POLLHUP | POLLERR | POLLNVAL)))
				continue;
			received =
			        recv(endpoints[i], buffer, sizeof(buffer), 0);
			if (received > 0) {
				if (proxy_send_all(destination, buffer,
				                   (size_t)received) != 0) {
					/* 对端因 TLS
					 * 主体不匹配关闭时，另一侧仍可能在写告警。
					 */
					read_open[0] = 0;
					read_open[1] = 0;
					break;
				}
				continue;
			}
			if (received < 0 && errno == EINTR)
				continue;
			if (received < 0 &&
			    (errno == EAGAIN || errno == EWOULDBLOCK))
				continue;
			read_open[i] = 0;
			(void)shutdown(destination, SHUT_WR);
		}
		if (failed)
			break;
	}
	if (failed) {
		(void)shutdown(endpoints[0], SHUT_RDWR);
		(void)shutdown(endpoints[1], SHUT_RDWR);
	}
	close(endpoints[0]);
	close(endpoints[1]);
	pthread_mutex_lock(&proxy->lock);
	proxy->completed++;
	if (failed)
		proxy->errors++;
	pthread_cond_broadcast(&proxy->changed);
	pthread_mutex_unlock(&proxy->lock);
	return NULL;
}

static void *tls_proxy_accept_main(void *arg)
{
	struct tls_connection_proxy *proxy = arg;
	unsigned accepted = 0U;
	while (accepted < TEST_PROXY_CONNECTIONS) {
		struct tls_proxy_bridge *bridge;
		struct sockaddr_in peer;
		socklen_t peer_length = sizeof(peer);
		unsigned index;
		uint16_t backend_port;
		int client_fd = accept(proxy->listen_fd,
		                       (struct sockaddr *)&peer, &peer_length);
		int backend_fd;
		if (client_fd < 0) {
			if (errno == EINTR)
				continue;
			pthread_mutex_lock(&proxy->lock);
			if (!proxy->errors)
				proxy->errors = 1;
			pthread_cond_broadcast(&proxy->changed);
			pthread_mutex_unlock(&proxy->lock);
			break;
		}
		pthread_mutex_lock(&proxy->lock);
		index = proxy->accepted++;
		accepted = proxy->accepted;
		pthread_cond_broadcast(&proxy->changed);
		pthread_mutex_unlock(&proxy->lock);
		backend_port = index == 1U ? proxy->alternate_backend_port
		                           : proxy->primary_backend_port;
		backend_fd = connect_ipv4(backend_port);
		if (backend_fd < 0) {
			close(client_fd);
			pthread_mutex_lock(&proxy->lock);
			proxy->errors++;
			proxy->completed++;
			pthread_cond_broadcast(&proxy->changed);
			pthread_mutex_unlock(&proxy->lock);
			continue;
		}
		bridge = &proxy->bridges[index];
		bridge->proxy = proxy;
		bridge->client_fd = client_fd;
		bridge->backend_fd = backend_fd;
		if (pthread_create(&bridge->thread, NULL, tls_proxy_bridge_main,
		                   bridge) != 0) {
			close(client_fd);
			close(backend_fd);
			pthread_mutex_lock(&proxy->lock);
			proxy->errors++;
			proxy->completed++;
			pthread_cond_broadcast(&proxy->changed);
			pthread_mutex_unlock(&proxy->lock);
			continue;
		}
		bridge->started = 1;
		proxy->bridge_count++;
	}
	return NULL;
}

static void tls_connection_proxy_start(struct tls_connection_proxy *proxy,
                                       uint16_t primary_backend_port,
                                       uint16_t alternate_backend_port)
{
	struct sockaddr_in address;
	socklen_t address_length = sizeof(address);
	pthread_condattr_t attr;
	assert(proxy != NULL);
	memset(proxy, 0, sizeof(*proxy));
	proxy->listen_fd = -1;
	for (unsigned i = 0; i < TEST_PROXY_CONNECTIONS; ++i) {
		proxy->bridges[i].client_fd = -1;
		proxy->bridges[i].backend_fd = -1;
	}
	assert(pthread_mutex_init(&proxy->lock, NULL) == 0);
	assert(pthread_condattr_init(&attr) == 0);
	assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&proxy->changed, &attr) == 0);
	assert(pthread_condattr_destroy(&attr) == 0);
	proxy->primary_backend_port = primary_backend_port;
	proxy->alternate_backend_port = alternate_backend_port;
	proxy->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	assert(proxy->listen_fd >= 0);
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0U;
	assert(bind(proxy->listen_fd, (struct sockaddr *)&address,
	            sizeof(address)) == 0);
	assert(listen(proxy->listen_fd, TEST_PROXY_CONNECTIONS) == 0);
	assert(getsockname(proxy->listen_fd, (struct sockaddr *)&address,
	                   &address_length) == 0);
	proxy->port = ntohs(address.sin_port);
	assert(proxy->port != 0U);
	assert(pthread_create(&proxy->accept_thread, NULL,
	                      tls_proxy_accept_main, proxy) == 0);
	proxy->accept_thread_started = 1;
}

static void tls_connection_proxy_wait(struct tls_connection_proxy *proxy,
                                      int wait_completed, unsigned target)
{
	struct timespec deadline;
	unsigned *field = wait_completed ? &proxy->completed : &proxy->accepted;
	int ret = 0;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&proxy->lock);
	while (*field < target && !proxy->errors && ret == 0)
		ret = pthread_cond_timedwait(&proxy->changed, &proxy->lock,
		                             &deadline);
	if (*field < target || proxy->errors != 0)
		fprintf(stderr,
		        "TLS proxy wait failed: completed=%d target=%u "
		        "accepted=%u completed_count=%u errors=%d\n",
		        wait_completed, target, proxy->accepted,
		        proxy->completed, proxy->errors);
	assert(*field >= target);
	assert(proxy->errors == 0);
	pthread_mutex_unlock(&proxy->lock);
}

static void tls_connection_proxy_stop(struct tls_connection_proxy *proxy)
{
	assert(proxy != NULL);
	if (proxy->listen_fd >= 0) {
		(void)shutdown(proxy->listen_fd, SHUT_RDWR);
		close(proxy->listen_fd);
	}
	if (proxy->accept_thread_started)
		assert(pthread_join(proxy->accept_thread, NULL) == 0);
	for (unsigned i = 0; i < TEST_PROXY_CONNECTIONS; ++i)
		if (proxy->bridges[i].started)
			assert(pthread_join(proxy->bridges[i].thread, NULL) ==
			       0);
	assert(proxy->bridge_count == TEST_PROXY_CONNECTIONS);
	assert(proxy->errors == 0);
	pthread_cond_destroy(&proxy->changed);
	pthread_mutex_destroy(&proxy->lock);
}

static void wait_for_counter(struct connection_group_tls_state *state,
                             _Atomic unsigned *counter, unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&state->lock);
	while (atomic_load_explicit(counter, memory_order_relaxed) < expected) {
		int ret = pthread_cond_timedwait(&state->changed, &state->lock,
		                                 &deadline);
		if (ret != 0)
			fprintf(stderr,
			        "counter wait timed out: counter=%p "
			        "expected=%u actual=%u ret=%d\n",
			        (void *)counter, expected,
			        atomic_load_explicit(counter,
			                             memory_order_relaxed),
			        ret);
		assert(ret == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void wait_for_server_group_count(struct tr_server *server,
                                        uint32_t expected)
{
	struct timespec pause = {0, 1000000L};
	for (unsigned attempt = 0U; attempt < 5000U; ++attempt) {
		struct tr_connection_group_server_stats stats;
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.groups_current == expected)
			return;
		assert(nanosleep(&pause, NULL) == 0);
	}
	assert(!"timed out waiting for connection-group count");
}

static void wait_for_server_data_count(struct tr_server *server,
                                       uint32_t expected)
{
	struct timespec pause = {0, 1000000L};

	for (unsigned attempt = 0U; attempt < 5000U; ++attempt) {
		struct tr_connection_group_server_stats stats;
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.data_connections_current == expected)
			return;
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	assert(!"timed out waiting for DATA connection count");
}

static void wait_for_server_route_rejections(struct tr_server *server,
                                             uint64_t expected)
{
	struct timespec pause = {0, 1000000L};
	struct tr_connection_group_server_stats stats;

	for (unsigned attempt = 0U; attempt < 5000U; ++attempt) {
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.route_rejections >= expected)
			return;
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.route_rejections >= expected);
}

static int identity_equals(const struct tr_peer_identity_view *identity,
                           const uint8_t *uri, uint32_t uri_len)
{
	return identity->authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE &&
	       identity->uri_san && identity->uri_san_len == uri_len &&
	       memcmp(identity->uri_san, uri, uri_len) == 0;
}

static int
authorize_group(const struct tr_connection_group_authorization *authorization,
                void *arg)
{
	struct connection_group_tls_state *state = arg;
	if (!authorization || !state)
		return TR_ERR_INVALID;
	if (authorization->group.group_id != TEST_GROUP_ID ||
	    authorization->group.epoch != TEST_GROUP_EPOCH ||
	    !identity_equals(&authorization->peer_identity, client_uri,
	                     (uint32_t)sizeof(client_uri) - 1U)) {
		atomic_fetch_add_explicit(&state->authorization_rejections, 1U,
		                          memory_order_relaxed);
		signal_state_changed(state);
		return TR_ERR_INVALID;
	}

	if (authorization->role == TR_CONNECTION_GROUP_ROLE_CONTROL) {
		if (authorization->control_identity.authentication !=
		            TR_PEER_IDENTITY_NONE ||
		    authorization->control_identity.uri_san != NULL ||
		    authorization->control_identity.uri_san_len != 0U)
			return TR_ERR_INVALID;
		atomic_fetch_add_explicit(&state->control_authorizations, 1U,
		                          memory_order_relaxed);
	} else if (authorization->role == TR_CONNECTION_GROUP_ROLE_DATA) {
		if (!identity_equals(&authorization->control_identity,
		                     client_uri,
		                     (uint32_t)sizeof(client_uri) - 1U))
			return TR_ERR_INVALID;
		atomic_fetch_add_explicit(&state->data_authorizations, 1U,
		                          memory_order_relaxed);
	} else {
		return TR_ERR_INVALID;
	}

	signal_state_changed(state);
	return TR_OK;
}

static void authorize_group_async(
        const struct tr_connection_group_authorization *authorization,
        struct tr_tls_provider_operation operation, void *arg)
{
	struct connection_group_tls_state *state = arg;
	unsigned index;
	int auto_allow;

	assert(authorization != NULL && state != NULL);
	assert(authorization->group.group_id >= TEST_GROUP_ID &&
	       authorization->group.group_id <= TEST_GROUP_ID + 5U);
	assert(authorization->group.epoch == TEST_GROUP_EPOCH);
	assert(authorization->deadline_monotonic_ns != 0U);
	assert(identity_equals(&authorization->peer_identity, client_uri,
	                       (uint32_t)sizeof(client_uri) - 1U));
	if (authorization->role == TR_CONNECTION_GROUP_ROLE_CONTROL) {
		assert(authorization->control_identity.authentication ==
		       TR_PEER_IDENTITY_NONE);
		assert(authorization->control_identity.uri_san == NULL);
		assert(authorization->control_identity.uri_san_len == 0U);
	} else {
		assert(authorization->role == TR_CONNECTION_GROUP_ROLE_DATA);
		assert(identity_equals(&authorization->control_identity,
		                       client_uri,
		                       (uint32_t)sizeof(client_uri) - 1U));
	}
	assert(pthread_mutex_lock(&state->lock) == 0);
	index = atomic_fetch_add_explicit(&state->async_authorizations, 1U,
	                                  memory_order_relaxed);
	assert(index < TEST_ASYNC_AUTHORIZATIONS);
	state->async_operations[index] = operation;
	state->async_roles[index] = authorization->role;
	state->async_group_ids[index] = authorization->group.group_id;
	auto_allow = authorization->group.group_id == TEST_GROUP_ID + 1U;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
	if (auto_allow)
		assert(tr_server_connection_group_authorize_complete(
		               state->server, operation, TR_OK,
		               TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) ==
		       TR_OK);
}

static void cancel_group_async(struct tr_tls_provider_operation operation,
                               void *arg)
{
	struct connection_group_tls_state *state = arg;
	assert(state != NULL && state->server != NULL);
	if (!atomic_load_explicit(&state->defer_cancel_ack,
	                          memory_order_relaxed)) {
		/* 取消后的迟到 ALLOW 只确认 provider 已停止，服务端必须丢弃它。
		 */
		assert(tr_server_connection_group_authorize_complete(
		               state->server, operation, TR_OK,
		               TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) ==
		       TR_OK);
	}
	atomic_fetch_add_explicit(&state->async_cancellations, 1U,
	                          memory_order_relaxed);
	signal_state_changed(state);
}

static enum tr_connection_group_message_disposition
on_group_message(const struct tr_connection_group_message *message, void *arg)
{
	struct connection_group_tls_state *state = arg;
	assert(message != NULL);
	assert(message->group.group_id == TEST_GROUP_ID);
	assert(message->group.epoch == TEST_GROUP_EPOCH);
	assert(message->stream_id == TEST_STREAM_ID);
	assert(message->message_id == TEST_DATA_MESSAGE_ID);
	assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
	                          TR_CONNECTION_GROUP_DATA_LAST));
	assert(message->bytes.len == sizeof(message_body) - 1U);
	assert(memcmp(message->bytes.data, message_body,
	              sizeof(message_body) - 1U) == 0);
	atomic_fetch_add_explicit(&state->messages, 1U, memory_order_relaxed);
	signal_state_changed(state);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_transfer_ready(const struct tr_connection_group_id *group,
                              uint32_t stream_id, uint64_t message_id,
                              void *arg)
{
	struct connection_group_tls_state *state = arg;
	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(stream_id == TEST_STREAM_ID);
	assert(message_id == TEST_READY_MESSAGE_ID);
	atomic_fetch_add_explicit(&state->ready_callbacks, 1U,
	                          memory_order_relaxed);
	signal_state_changed(state);
}

static void
connection_group_tls_state_init(struct connection_group_tls_state *state)
{
	pthread_condattr_t attr;
	memset(state, 0, sizeof(*state));
	atomic_init(&state->control_authorizations, 0U);
	atomic_init(&state->data_authorizations, 0U);
	atomic_init(&state->ready_callbacks, 0U);
	atomic_init(&state->messages, 0U);
	atomic_init(&state->authorization_rejections, 0U);
	atomic_init(&state->async_authorizations, 0U);
	atomic_init(&state->async_cancellations, 0U);
	atomic_init(&state->defer_cancel_ack, 0);
	assert(pthread_mutex_init(&state->lock, NULL) == 0);
	assert(pthread_condattr_init(&attr) == 0);
	assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&state->changed, &attr) == 0);
	assert(pthread_condattr_destroy(&attr) == 0);
}

static void
connection_group_tls_state_destroy(struct connection_group_tls_state *state)
{
	pthread_cond_destroy(&state->changed);
	pthread_mutex_destroy(&state->lock);
}

static void start_test_group_server(const struct tls_test_identity *identity,
                                    struct connection_group_tls_state *state,
                                    const char *server_certificate,
                                    const char *client_ca,
                                    struct tr_server **server, uint16_t *port)
{
	struct tr_server_config config;
	tr_server_config_init(&config);
	config.handshake_timeout_ms = 5000U;
	config.keepalive_interval_ms = 0U;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.certificate_file = server_certificate;
	config.tls.private_key_file = identity->server_private_key_file;
	config.tls.client_ca_file = client_ca;
	config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	config.connection_groups.max_groups = 1U;
	config.connection_groups.max_connections = 3U;
	config.connection_groups.max_data_connections_per_group = 1U;
	config.connection_groups.max_streams_per_group = 4U;
	config.connection_groups.authorize = authorize_group;
	config.connection_groups.on_message = on_group_message;
	config.connection_groups.callback_arg = state;
	assert(tr_server_create_transport(&config, server) == TR_OK);
	assert(tr_server_connection_group_listen(*server, "127.0.0.1", 0U, 16U,
	                                         port) == TR_OK);
	assert(tr_server_start(*server) == TR_OK);
}

static void
wait_for_server_group_attempt_to_close(struct tr_server *server,
                                       uint64_t minimum_route_rejections)
{
	struct tr_connection_group_server_stats stats;
	struct timespec deadline;
	struct timespec pause = {0, 10000000L};
	int ret;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 10;
	for (;;) {
		struct timespec now;
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.connections_peak >= 1U &&
		    stats.connections_current == 0U &&
		    stats.route_rejections >= minimum_route_rejections)
			return;
		assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
		if (now.tv_sec > deadline.tv_sec ||
		    (now.tv_sec == deadline.tv_sec &&
		     now.tv_nsec >= deadline.tv_nsec))
			break;
		ret = nanosleep(&pause, NULL);
		assert(ret == 0 || errno == EINTR);
	}
	assert(stats.connections_peak >= 1U);
	assert(stats.connections_current == 0U);
	assert(stats.route_rejections >= minimum_route_rejections);
}

static void wait_for_client_group_disconnected(struct tr_client *client)
{
	struct tr_connection_group_client_stats stats;
	struct timespec pause = {0, 10000000L};
	unsigned attempt;
	for (attempt = 0U; attempt < 1000U; ++attempt) {
		assert(tr_client_connection_group_get_stats(client, &stats) ==
		       TR_OK);
		if (stats.control_connected == 0U)
			return;
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	assert(stats.control_connected == 0U);
}

static void wait_for_client_group_data_closed(struct tr_client *client)
{
	struct tr_connection_group_client_stats stats;
	struct timespec pause = {0, 10000000L};
	unsigned attempt;
	for (attempt = 0U; attempt < 1000U; ++attempt) {
		assert(tr_client_connection_group_get_stats(client, &stats) ==
		       TR_OK);
		if (stats.control_connected == 0U &&
		    stats.data_connections == 0U)
			return;
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	assert(stats.control_connected == 0U);
	assert(stats.data_connections == 0U);
}

static void destroy_server_after_async_release(struct tr_server *server)
{
	struct timespec pause = {0, 1000000L};
	unsigned attempt;
	int ret = TR_ERR_STATE;

	/* 销毁契约允许异步所有权尚未归还时重试。 */
	for (attempt = 0U; attempt < 5000U; ++attempt) {
		ret = tr_server_destroy(server);
		if (ret == TR_OK)
			return;
		assert(ret == TR_ERR_STATE || ret == TR_AGAIN);
		assert(nanosleep(&pause, NULL) == 0 || errno == EINTR);
	}
	assert(ret == TR_OK);
}

static void *tls_stall_server_main(void *arg)
{
	struct tls_stall_server *server = arg;
	struct sockaddr_in peer;
	socklen_t peer_length = sizeof(peer);
	uint8_t buffer[4096];
	int fd = accept(server->listen_fd, (struct sockaddr *)&peer,
	                &peer_length);
	if (fd < 0)
		return NULL;
	pthread_mutex_lock(&server->lock);
	server->accepted = 1;
	pthread_cond_broadcast(&server->changed);
	pthread_mutex_unlock(&server->lock);
	for (;;) {
		ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
		if (received > 0) {
			pthread_mutex_lock(&server->lock);
			if (!server->hello_received) {
				server->hello_received = 1;
				server->first_record_type = buffer[0];
			}
			pthread_cond_broadcast(&server->changed);
			pthread_mutex_unlock(&server->lock);
			continue;
		}
		if (received < 0 && errno == EINTR)
			continue;
		if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			continue;
		break;
	}
	close(fd);
	pthread_mutex_lock(&server->lock);
	server->peer_closed = 1;
	pthread_cond_broadcast(&server->changed);
	pthread_mutex_unlock(&server->lock);
	return NULL;
}

static void tls_stall_server_start(struct tls_stall_server *server)
{
	struct sockaddr_in address;
	socklen_t address_length = sizeof(address);
	pthread_condattr_t attr;
	memset(server, 0, sizeof(*server));
	server->listen_fd = -1;
	assert(pthread_mutex_init(&server->lock, NULL) == 0);
	assert(pthread_condattr_init(&attr) == 0);
	assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&server->changed, &attr) == 0);
	assert(pthread_condattr_destroy(&attr) == 0);
	server->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
	assert(server->listen_fd >= 0);
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0U;
	assert(bind(server->listen_fd, (struct sockaddr *)&address,
	            sizeof(address)) == 0);
	assert(listen(server->listen_fd, 1) == 0);
	assert(getsockname(server->listen_fd, (struct sockaddr *)&address,
	                   &address_length) == 0);
	server->port = ntohs(address.sin_port);
	assert(server->port != 0U);
	assert(pthread_create(&server->thread, NULL, tls_stall_server_main,
	                      server) == 0);
	server->thread_started = 1;
}

static void tls_stall_server_wait_closed(struct tls_stall_server *server)
{
	struct timespec deadline;
	int ret = 0;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&server->lock);
	while (!server->peer_closed && ret == 0)
		ret = pthread_cond_timedwait(&server->changed, &server->lock,
		                             &deadline);
	assert(server->peer_closed);
	assert(server->accepted);
	assert(server->hello_received);
	assert(server->first_record_type == 0x16);
	pthread_mutex_unlock(&server->lock);
}

static void tls_stall_server_stop(struct tls_stall_server *server)
{
	if (server->listen_fd >= 0)
		close(server->listen_fd);
	if (server->thread_started)
		assert(pthread_join(server->thread, NULL) == 0);
	pthread_cond_destroy(&server->changed);
	pthread_mutex_destroy(&server->lock);
}

static void test_async_connection_group_authorization(void)
{
	struct tls_test_identity identity;
	struct connection_group_tls_state state;
	struct tr_server_config server_config;
	struct tr_tls_server_config rotated_server_tls;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_client *progress_client = NULL;
	struct tr_tls_provider_operation operation;
	struct tr_connection_group_id group = {
	        .group_id = TEST_GROUP_ID,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_id progress_group = {
	        .group_id = TEST_GROUP_ID + 1U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_id revoke_group = {
	        .group_id = TEST_GROUP_ID + 2U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_id deadline_group = {
	        .group_id = TEST_GROUP_ID + 3U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_id drain_group = {
	        .group_id = TEST_GROUP_ID + 4U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_id error_group = {
	        .group_id = TEST_GROUP_ID + 5U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_connection_group_server_stats stats;
	uint64_t generation = 0U;
	uint16_t port = 0U;

	connection_group_tls_state_init(&state);
	assert(tls_test_identity_init(&identity, "connection-group-async") ==
	       0);
	tr_server_config_init(&server_config);
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file = identity.server_certificate_file;
	server_config.tls.private_key_file = identity.server_private_key_file;
	server_config.tls.client_ca_file = identity.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server_config.connection_groups.max_groups = 2U;
	server_config.connection_groups.max_connections = 4U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.authorize_async = authorize_group_async;
	server_config.connection_groups.authorize_cancel = cancel_group_async;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.callback_arg = &state;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	atomic_store_explicit(&test_authorization_event_fd,
	                      atomic_load_explicit(&test_last_aux_event_fd,
	                                           memory_order_relaxed),
	                      memory_order_relaxed);
	state.server = server;
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16U,
	                                         &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.sni_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.connection_groups.max_data_connections = 2U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 1U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_roles[0] == TR_CONNECTION_GROUP_ROLE_CONTROL);
	assert(state.async_operations[0].kind ==
	       TR_TLS_PROVIDER_OPERATION_AUTHORIZATION);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(stats.control_accepts == 0U);
	assert(atomic_load_explicit(&state.messages, memory_order_relaxed) ==
	       0U);
	assert(tr_server_connection_group_send_data_offer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID) == TR_ERR_STALE);
	assert(tr_client_create_transport(&client_config, &progress_client) ==
	       TR_OK);
	assert(tr_client_connection_group_connect(progress_client, "127.0.0.1",
	                                          port,
	                                          &progress_group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 2U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_group_ids[1] == progress_group.group_id);
	assert(state.async_roles[1] == TR_CONNECTION_GROUP_ROLE_CONTROL);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	wait_for_server_group_count(server, 1U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.control_accepts == 1U);
	assert(stats.groups_current == 1U);
	assert(tr_client_connection_group_close(progress_client) == TR_OK);
	assert(tr_client_destroy(progress_client) == TR_OK);
	wait_for_server_group_count(server, 0U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_for_counter(&state, &state.async_cancellations, 1U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(stats.control_accepts == 1U);
	assert(tr_client_destroy(client) == TR_OK);
	wait_for_server_group_count(server, 0U);

	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 3U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	operation = state.async_operations[2];
	assert(pthread_mutex_unlock(&state.lock) == 0);
	atomic_store_explicit(&test_authorization_write_fault_hits, 0U,
	                      memory_order_relaxed);
	atomic_store_explicit(&test_authorization_write_fault,
	                      TEST_AUTH_WRITE_FAULT_EINTR,
	                      memory_order_relaxed);
	assert(tr_server_connection_group_authorize_complete(
	               server, operation, TR_OK,
	               TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) == TR_OK);
	assert(atomic_load_explicit(&test_authorization_write_fault_hits,
	                            memory_order_relaxed) == 1U);
	wait_for_server_group_count(server, 1U);
	assert(tr_server_connection_group_send_data_offer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 4U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_roles[3] == TR_CONNECTION_GROUP_ROLE_DATA);
	operation = state.async_operations[3];
	assert(pthread_mutex_unlock(&state.lock) == 0);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.data_connections_current == 0U);
	assert(stats.data_accepts == 0U);
	assert(atomic_load_explicit(&state.ready_callbacks,
	                            memory_order_relaxed) == 0U);
	assert(atomic_load_explicit(&state.messages, memory_order_relaxed) ==
	       0U);
	{
		uint64_t rejected_before = stats.route_rejections;

		assert(tr_server_connection_group_authorize_complete(
		               server, operation, TR_OK,
		               TR_CONNECTION_GROUP_AUTHORIZATION_DENY) ==
		       TR_OK);
		wait_for_server_route_rejections(server, rejected_before + 1U);
	}
	assert(tr_server_connection_group_send_data_offer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID + 1U) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 5U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_roles[4] == TR_CONNECTION_GROUP_ROLE_DATA);
	operation = state.async_operations[4];
	assert(pthread_mutex_unlock(&state.lock) == 0);
	atomic_store_explicit(&test_authorization_write_fault_hits, 0U,
	                      memory_order_relaxed);
	atomic_store_explicit(&test_authorization_write_fault,
	                      TEST_AUTH_WRITE_FAULT_EAGAIN,
	                      memory_order_relaxed);
	assert(tr_server_connection_group_authorize_complete(
	               server, operation, TR_OK,
	               TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) == TR_OK);
	assert(atomic_load_explicit(&test_authorization_write_fault_hits,
	                            memory_order_relaxed) == 1U);
	wait_for_server_data_count(server, 1U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.control_accepts == 2U);
	assert(stats.data_accepts == 1U);
	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_client_destroy(client) == TR_OK);
	wait_for_server_group_count(server, 0U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	{
		uint64_t rejected_before = stats.route_rejections;

		assert(tr_client_create_transport(&client_config, &client) ==
		       TR_OK);
		assert(tr_client_connection_group_connect(
		               client, "127.0.0.1", port, &group) == TR_OK);
		wait_for_counter(&state, &state.async_authorizations, 6U);
		assert(pthread_mutex_lock(&state.lock) == 0);
		operation = state.async_operations[5];
		assert(pthread_mutex_unlock(&state.lock) == 0);
		assert(tr_server_connection_group_authorize_complete(
		               server, operation, TR_OK,
		               TR_CONNECTION_GROUP_AUTHORIZATION_DENY) ==
		       TR_OK);
		wait_for_server_route_rejections(server, rejected_before + 1U);
	}
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(stats.control_accepts == 2U);
	assert(tr_client_destroy(client) == TR_OK);

	/* Provider error 与明确 DENY 一样失败关闭，不能创建 CONTROL。 */
	{
		uint64_t rejected_before;

		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		rejected_before = stats.route_rejections;
		assert(tr_client_create_transport(&client_config, &client) ==
		       TR_OK);
		assert(tr_client_connection_group_connect(client, "127.0.0.1",
		                                          port, &error_group) ==
		       TR_OK);
		wait_for_counter(&state, &state.async_authorizations, 7U);
		assert(pthread_mutex_lock(&state.lock) == 0);
		operation = state.async_operations[6];
		assert(state.async_group_ids[6] == error_group.group_id);
		assert(pthread_mutex_unlock(&state.lock) == 0);
		assert(tr_server_connection_group_authorize_complete(
		               server, operation, TR_ERR_INVALID,
		               TR_CONNECTION_GROUP_AUTHORIZATION_DENY) ==
		       TR_OK);
		wait_for_server_route_rejections(server, rejected_before + 1U);
	}
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(stats.control_accepts == 2U);
	assert(tr_client_destroy(client) == TR_OK);

	/* REVOKE_OLD 必须取消仍在等待授权的旧 TLS generation。 */
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &revoke_group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 8U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_group_ids[7] == revoke_group.group_id);
	assert(state.async_roles[7] == TR_CONNECTION_GROUP_ROLE_CONTROL);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	rotated_server_tls = server_config.tls;
	rotated_server_tls.certificate_file =
	        identity.rotated_server_certificate_file;
	assert(tr_server_tls_update_profile(server, &rotated_server_tls,
	                                    TR_TLS_ROTATE_REVOKE_OLD,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	wait_for_counter(&state, &state.async_cancellations, 2U);
	wait_for_server_group_count(server, 0U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(stats.control_accepts == 2U);
	assert(tr_client_destroy(client) == TR_OK);

	/* 握手期限也覆盖已完成 TLS、仍挂起远端授权的连接。 */
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &deadline_group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 9U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_group_ids[8] == deadline_group.group_id);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	wait_for_counter(&state, &state.async_cancellations, 3U);
	wait_for_server_group_count(server, 0U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.control_accepts == 2U);
	assert(tr_client_destroy(client) == TR_OK);

	/* facade drain 关闭挂起连接并消费同步返回的取消确认。 */
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &drain_group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 10U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.async_group_ids[9] == drain_group.group_id);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_for_counter(&state, &state.async_cancellations, 4U);
	wait_for_server_group_count(server, 0U);
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.groups_current == 0U);
	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	tls_test_identity_cleanup(&identity);
	connection_group_tls_state_destroy(&state);
}

static void test_async_connection_group_destroy_retry(void)
{
	struct tls_test_identity identity;
	struct connection_group_tls_state state;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_connection_group_id group = {
	        .group_id = TEST_GROUP_ID + 5U,
	        .epoch = TEST_GROUP_EPOCH,
	};
	struct tr_tls_provider_operation operation;
	uint16_t port = 0U;

	connection_group_tls_state_init(&state);
	assert(tls_test_identity_init(&identity, "connection-group-destroy") ==
	       0);
	tr_server_config_init(&server_config);
	server_config.handshake_timeout_ms = 5000U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file = identity.server_certificate_file;
	server_config.tls.private_key_file = identity.server_private_key_file;
	server_config.tls.client_ca_file = identity.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.authorize_async = authorize_group_async;
	server_config.connection_groups.authorize_cancel = cancel_group_async;
	server_config.connection_groups.callback_arg = &state;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	atomic_store_explicit(&test_authorization_event_fd,
	                      atomic_load_explicit(&test_last_aux_event_fd,
	                                           memory_order_relaxed),
	                      memory_order_relaxed);
	state.server = server;
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16U,
	                                         &port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.sni_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &group) == TR_OK);
	wait_for_counter(&state, &state.async_authorizations, 1U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	operation = state.async_operations[0];
	assert(state.async_group_ids[0] == group.group_id);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	atomic_store_explicit(&state.defer_cancel_ack, 1, memory_order_relaxed);
	assert(tr_server_destroy(server) == TR_AGAIN);
	wait_for_counter(&state, &state.async_cancellations, 1U);
	/* destroy 返回可重试后，facade 与完成入口仍然有效。 */
	assert(tr_server_connection_group_authorize_complete(
	               server, operation, TR_OK,
	               TR_CONNECTION_GROUP_AUTHORIZATION_ALLOW) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	assert(tr_client_destroy(client) == TR_OK);
	tls_test_identity_cleanup(&identity);
	connection_group_tls_state_destroy(&state);
}

static void test_public_connection_group_mtls(void)
{
	struct tls_test_identity identity;
	struct connection_group_tls_state state;
	struct tr_server_config server_config;
	struct tr_tls_server_config rotated_server_tls;
	struct tr_client_config client_config;
	struct tr_tls_client_config rotated_client_tls;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct tr_connection_group_id group;
	struct tr_transport_bytes bytes;
	struct tr_connection_group_server_stats server_stats;
	uint16_t port = 0U;
	uint64_t generation = 0U;
	int send_ret = TR_AGAIN;

	atomic_store_explicit(&group_capability_frames_sent, 0U,
	                      memory_order_relaxed);
	memset(&state, 0, sizeof(state));
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attr;
		assert(pthread_condattr_init(&attr) == 0);
		assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attr) == 0);
		assert(pthread_condattr_destroy(&attr) == 0);
	}
	assert(tls_test_identity_init(&identity, "connection-group-tls") == 0);

	tr_server_config_init(&server_config);
	server_config.shard_count = 2U;
	server_config.handshake_timeout_ms = 5000U;
	server_config.keepalive_interval_ms = 0U;
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file = identity.server_certificate_file;
	server_config.tls.private_key_file = identity.server_private_key_file;
	server_config.tls.client_ca_file = identity.ca_file;
	server_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
		TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.callback_arg = &state;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16U,
	                                         &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.keepalive_interval_ms = 0U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.sni_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
		TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready = on_transfer_ready;
	client_config.connection_groups.callback_arg = &state;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &group) == TR_OK);
	wait_for_counter(&state, &state.control_authorizations, 1U);
	assert(tr_server_connection_group_get_stats(server, &server_stats) ==
	       TR_OK);

	rotated_server_tls = server_config.tls;
	rotated_server_tls.certificate_file =
	        identity.rotated_server_certificate_file;
	assert(tr_server_tls_update_profile(server, &rotated_server_tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	rotated_client_tls = client_config.tls;
	rotated_client_tls.client_certificate_file =
	        identity.rotated_client_certificate_file;
	rotated_client_tls.client_private_key_file =
	        identity.rotated_client_private_key_file;
	generation = 0U;
	assert(tr_client_tls_update_profile(client, &rotated_client_tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_OK);
	assert(generation == 2U);
	/* 活动 CONTROL 仍绑定旧 Server generation；准备好的 Client 新证书
	 * 不得被同一 group 的 DATA 提前使用。 */
	assert(tr_server_connection_group_send_data_offer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID) == TR_OK);
	wait_for_server_route_rejections(server,
	                                 server_stats.route_rejections + 1U);
	assert(atomic_load_explicit(&state.data_authorizations,
	                            memory_order_relaxed) == 0U);
	assert(atomic_load_explicit(&state.ready_callbacks,
	                            memory_order_relaxed) == 0U);
	assert(tr_server_connection_group_get_stats(server, &server_stats) ==
	       TR_OK);
	assert(server_stats.groups_current == 1U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_for_server_group_count(server, 0U);
	assert(tr_client_connection_group_connect(client, "127.0.0.1", port,
	                                          &group) == TR_OK);
	wait_for_counter(&state, &state.control_authorizations, 2U);
	assert(tr_server_connection_group_send_data_offer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID) == TR_OK);
	wait_for_counter(&state, &state.data_authorizations, 1U);
	{
		struct timespec pause = {0, 1000000L};
		unsigned i;

		for (i = 0; i < 2000U; ++i) {
			if (atomic_load_explicit(&group_capability_frames_sent,
			                         memory_order_relaxed) >= 2U)
				break;
			(void)nanosleep(&pause, NULL);
		}
		assert(atomic_load_explicit(&group_capability_frames_sent,
		                            memory_order_relaxed) >= 2U);
	}
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, TEST_STREAM_ID,
	               TEST_READY_MESSAGE_ID) == TR_OK);
	wait_for_counter(&state, &state.ready_callbacks, 1U);
	bytes.data = message_body;
	bytes.len = (uint32_t)sizeof(message_body) - 1U;
	for (unsigned attempt = 0U; attempt < 2000U && send_ret == TR_AGAIN;
	     ++attempt) {
		struct timespec pause = {0, 1000000L};

		send_ret = tr_client_connection_group_send_with_compression(
		        client, TEST_STREAM_ID, TEST_DATA_MESSAGE_ID, &bytes,
		        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
		if (send_ret == TR_AGAIN)
			(void)nanosleep(&pause, NULL);
	}
	assert(send_ret == TR_OK);
	wait_for_counter(&state, &state.messages, 1U);

	/* 显式撤销旧 TLS 代次必须退休已获准压缩的 DATA 路由；其后同一
	 * stream 的压缩发送不能继续借用已撤销代次的能力或身份。
	 */
	generation = 0U;
	assert(tr_server_tls_update_profile(
	               server, &rotated_server_tls, TR_TLS_ROTATE_REVOKE_OLD,
	               &generation) == TR_OK);
	assert(generation == 3U);
	wait_for_server_group_count(server, 0U);
	wait_for_client_group_data_closed(client);
	generation = 0U;
	assert(tr_client_tls_update_profile(
	               client, &rotated_client_tls, TR_TLS_ROTATE_REVOKE_OLD,
	               &generation) == TR_OK);
	assert(generation == 3U);
	send_ret = tr_client_connection_group_send_with_compression(
	        client, TEST_STREAM_ID, TEST_DATA_MESSAGE_ID, &bytes,
	        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
	assert(send_ret == TR_ERR_CLOSED);

	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	generation = UINT64_C(0xfeedface);
	assert(tr_server_tls_update_profile(server, &server_config.tls,
	                                    TR_TLS_ROTATE_GRACEFUL,
	                                    &generation) == TR_ERR_CLOSED);
	assert(generation == UINT64_C(0xfeedface));
	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	destroy_server_after_async_release(server);
	tls_test_identity_cleanup(&identity);
	pthread_cond_destroy(&state.changed);
	pthread_mutex_destroy(&state.lock);
}

static void test_data_server_uri_mismatch_is_rejected(void)
{
	struct tls_test_identity identity;
	struct connection_group_tls_state primary_state;
	struct connection_group_tls_state alternate_state;
	struct tls_connection_proxy proxy;
	struct tr_server_config primary_config;
	struct tr_server_config alternate_config;
	struct tr_client_config client_config;
	struct tr_server *primary_server = NULL;
	struct tr_server *alternate_server = NULL;
	struct tr_client *client = NULL;
	struct tr_connection_group_id group;
	struct tr_connection_group_server_stats stats;
	struct tr_transport_bytes bytes;
	uint16_t primary_port = 0U;
	uint16_t alternate_port = 0U;
	int ret;

	memset(&primary_state, 0, sizeof(primary_state));
	memset(&alternate_state, 0, sizeof(alternate_state));
	assert(pthread_mutex_init(&primary_state.lock, NULL) == 0);
	assert(pthread_mutex_init(&alternate_state.lock, NULL) == 0);
	{
		pthread_condattr_t attr;
		assert(pthread_condattr_init(&attr) == 0);
		assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&primary_state.changed, &attr) == 0);
		assert(pthread_cond_init(&alternate_state.changed, &attr) == 0);
		assert(pthread_condattr_destroy(&attr) == 0);
	}
	assert(tls_test_identity_init(&identity,
	                              "connection-group-tls-mismatch") == 0);

	tr_server_config_init(&primary_config);
	primary_config.handshake_timeout_ms = 5000U;
	primary_config.keepalive_interval_ms = 0U;
	primary_config.tls.mode = TR_TLS_REQUIRED;
	primary_config.tls.certificate_file = identity.server_certificate_file;
	primary_config.tls.private_key_file = identity.server_private_key_file;
	primary_config.tls.client_ca_file = identity.ca_file;
	primary_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	primary_config.connection_groups.max_groups = 1U;
	primary_config.connection_groups.max_connections = 3U;
	primary_config.connection_groups.max_data_connections_per_group = 1U;
	primary_config.connection_groups.max_streams_per_group = 4U;
	primary_config.connection_groups.authorize = authorize_group;
	primary_config.connection_groups.on_message = on_group_message;
	primary_config.connection_groups.callback_arg = &primary_state;
	assert(tr_server_create_transport(&primary_config, &primary_server) ==
	       TR_OK);
	assert(tr_server_connection_group_listen(primary_server, "127.0.0.1",
	                                         0U, 16U,
	                                         &primary_port) == TR_OK);
	assert(tr_server_start(primary_server) == TR_OK);

	tr_server_config_init(&alternate_config);
	alternate_config.handshake_timeout_ms = 5000U;
	alternate_config.keepalive_interval_ms = 0U;
	alternate_config.tls.mode = TR_TLS_REQUIRED;
	alternate_config.tls.certificate_file =
	        identity.server_alternate_uri_certificate_file;
	alternate_config.tls.private_key_file =
	        identity.server_private_key_file;
	alternate_config.tls.client_ca_file = identity.ca_file;
	alternate_config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	alternate_config.connection_groups.max_groups = 1U;
	alternate_config.connection_groups.max_connections = 3U;
	alternate_config.connection_groups.max_data_connections_per_group = 1U;
	alternate_config.connection_groups.max_streams_per_group = 4U;
	alternate_config.connection_groups.authorize = authorize_group;
	alternate_config.connection_groups.on_message = on_group_message;
	alternate_config.connection_groups.callback_arg = &alternate_state;
	assert(tr_server_create_transport(&alternate_config,
	                                  &alternate_server) == TR_OK);
	assert(tr_server_connection_group_listen(alternate_server, "127.0.0.1",
	                                         0U, 16U,
	                                         &alternate_port) == TR_OK);
	assert(tr_server_start(alternate_server) == TR_OK);

	tls_connection_proxy_start(&proxy, primary_port, alternate_port);
	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 5000U;
	client_config.keepalive_interval_ms = 0U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.sni_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.on_transfer_ready = on_transfer_ready;
	client_config.connection_groups.callback_arg = &primary_state;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          proxy.port, &group) == TR_OK);
	wait_for_counter(&primary_state, &primary_state.control_authorizations,
	                 1U);
	tls_connection_proxy_wait(&proxy, 0, 1U);
	assert(tr_server_connection_group_send_data_offer(
	               primary_server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_OFFER_MESSAGE_ID) == TR_OK);
	tls_connection_proxy_wait(&proxy, 0, 2U);
	tls_connection_proxy_wait(&proxy, 1, 1U);

	assert(tr_server_connection_group_get_stats(alternate_server, &stats) ==
	       TR_OK);
	assert(stats.data_accepts == 0U);
	assert(stats.data_connections_current == 0U);
	assert(atomic_load_explicit(&alternate_state.control_authorizations,
	                            memory_order_relaxed) == 0U);
	assert(atomic_load_explicit(&alternate_state.data_authorizations,
	                            memory_order_relaxed) == 0U);
	assert(atomic_load_explicit(&primary_state.ready_callbacks,
	                            memory_order_relaxed) == 0U);
	assert(atomic_load_explicit(&primary_state.messages,
	                            memory_order_relaxed) == 0U);
	assert(tr_server_connection_group_send_transfer_ready(
	               primary_server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_STREAM_ID, TEST_READY_MESSAGE_ID) == TR_AGAIN);

	/* DATA 主体校验失败只撤销该预留；原 CONTROL 仍可继续建立 DATA。 */
	{
		struct timespec pause = {0, 10000000L};
		unsigned attempt;
		for (attempt = 0U; attempt < 500U; ++attempt) {
			ret = tr_server_connection_group_send_data_offer(
			        primary_server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
			        TEST_OFFER_MESSAGE_ID + 1U);
			if (ret == TR_OK)
				break;
			assert(ret == TR_AGAIN);
			assert(nanosleep(&pause, NULL) == 0);
		}
		assert(ret == TR_OK);
	}
	tls_connection_proxy_wait(&proxy, 0, 3U);
	wait_for_counter(&primary_state, &primary_state.data_authorizations,
	                 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               primary_server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               TEST_STREAM_ID, TEST_READY_MESSAGE_ID) == TR_OK);
	wait_for_counter(&primary_state, &primary_state.ready_callbacks, 1U);
	bytes.data = message_body;
	bytes.len = (uint32_t)sizeof(message_body) - 1U;
	assert(tr_client_connection_group_send(client, TEST_STREAM_ID,
	                                       TEST_DATA_MESSAGE_ID,
	                                       &bytes) == TR_OK);
	wait_for_counter(&primary_state, &primary_state.messages, 1U);

	assert(tr_server_connection_group_stop(primary_server) == TR_OK);
	wait_for_client_group_data_closed(client);
	assert(tr_client_destroy(client) == TR_OK);
	tls_connection_proxy_wait(&proxy, 1, TEST_PROXY_CONNECTIONS);
	tls_connection_proxy_stop(&proxy);
	assert(tr_server_connection_group_stop(alternate_server) == TR_OK);
	assert(tr_server_drain(alternate_server, 5000U) == TR_OK);
	assert(tr_server_destroy(alternate_server) == TR_OK);
	assert(tr_server_drain(primary_server, 5000U) == TR_OK);
	assert(tr_server_destroy(primary_server) == TR_OK);
	tls_test_identity_cleanup(&identity);
	pthread_cond_destroy(&alternate_state.changed);
	pthread_mutex_destroy(&alternate_state.lock);
	pthread_cond_destroy(&primary_state.changed);
	pthread_mutex_destroy(&primary_state.lock);
}

static void test_client_certificate_authorization_failures(void)
{
	struct tls_test_identity identity;
	assert(tls_test_identity_init(
	               &identity, "connection-group-tls-client-errors") == 0);
	for (unsigned wrong_uri = 0U; wrong_uri <= 1U; ++wrong_uri) {
		struct connection_group_tls_state state;
		struct tr_server *server = NULL;
		struct tr_client *client = NULL;
		struct tr_client_config client_config;
		struct tr_connection_group_client_stats client_stats;
		struct tr_connection_group_server_stats server_stats;
		uint16_t port = 0U;
		const char *client_ca = wrong_uri ? identity.ca_file
		                                  : identity.alternate_ca_file;
		const char *client_certificate =
		        wrong_uri
		                ? identity.client_alternate_uri_certificate_file
		                : identity.client_certificate_file;
		connection_group_tls_state_init(&state);
		start_test_group_server(&identity, &state,
		                        identity.server_certificate_file,
		                        client_ca, &server, &port);

		tr_client_config_init(&client_config);
		client_config.connect_timeout_ms = 2000U;
		client_config.keepalive_interval_ms = 0U;
		client_config.tls.mode = TR_TLS_REQUIRED;
		client_config.tls.ca_file = identity.ca_file;
		client_config.tls.expected_peer_name = "server.example.test";
		client_config.tls.sni_name = "server.example.test";
		client_config.tls.client_certificate_file = client_certificate;
		client_config.tls.client_private_key_file =
		        identity.client_private_key_file;
		client_config.connection_groups.max_data_connections = 1U;
		client_config.connection_groups.max_active_transfers = 1U;
		client_config.connection_groups.callback_arg = &state;
		assert(tr_client_create_transport(&client_config, &client) ==
		       TR_OK);
		{
			struct tr_connection_group_id group = {
			        TEST_GROUP_ID, TEST_GROUP_EPOCH};
			assert(tr_client_connection_group_connect(
			               client, "127.0.0.1", port, &group) ==
			       TR_OK);
		}
		wait_for_server_group_attempt_to_close(server,
		                                       wrong_uri ? 1U : 0U);
		wait_for_client_group_disconnected(client);
		assert(tr_client_connection_group_get_stats(
		               client, &client_stats) == TR_OK);
		assert(client_stats.control_connected == 0U);
		assert(atomic_load_explicit(&state.control_authorizations,
		                            memory_order_relaxed) == 0U);
		if (wrong_uri) {
			assert(atomic_load_explicit(
			               &state.authorization_rejections,
			               memory_order_relaxed) == 1U);
		} else {
			assert(atomic_load_explicit(
			               &state.authorization_rejections,
			               memory_order_relaxed) == 0U);
		}
		assert(tr_server_connection_group_get_stats(
		               server, &server_stats) == TR_OK);
		assert(server_stats.control_accepts == 0U);
		assert(server_stats.data_accepts == 0U);
		assert(tr_client_destroy(client) == TR_OK);
		assert(tr_server_connection_group_stop(server) == TR_OK);
		assert(tr_server_drain(server, 5000U) == TR_OK);
		assert(tr_server_destroy(server) == TR_OK);
		connection_group_tls_state_destroy(&state);
	}
	tls_test_identity_cleanup(&identity);
}

static void test_client_tls_handshake_uses_connect_deadline(void)
{
	struct tls_test_identity identity;
	struct tls_stall_server stall_server;
	struct tr_client_config client_config;
	struct tr_client *client = NULL;
	assert(tls_test_identity_init(&identity,
	                              "connection-group-tls-deadline") == 0);
	tls_stall_server_start(&stall_server);
	tr_client_config_init(&client_config);
	client_config.connect_timeout_ms = 200U;
	client_config.keepalive_interval_ms = 0U;
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "server.example.test";
	client_config.tls.sni_name = "server.example.test";
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.connection_groups.max_data_connections = 1U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	{
		struct tr_connection_group_id group = {TEST_GROUP_ID,
		                                       TEST_GROUP_EPOCH};
		assert(tr_client_connection_group_connect(client, "127.0.0.1",
		                                          stall_server.port,
		                                          &group) == TR_OK);
	}
	tls_stall_server_wait_closed(&stall_server);
	wait_for_client_group_disconnected(client);
	assert(tr_client_destroy(client) == TR_OK);
	tls_stall_server_stop(&stall_server);
	tls_test_identity_cleanup(&identity);
}

int main(void)
{
	test_async_connection_group_authorization();
	test_async_connection_group_destroy_retry();
	test_public_connection_group_mtls();
	test_data_server_uri_mismatch_is_rejected();
	test_client_certificate_authorization_failures();
	test_client_tls_handshake_uses_connect_deadline();
	return 0;
}
