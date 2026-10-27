#define _POSIX_C_SOURCE 200809L

#include <tr/trpc.h>

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../src/facade_diagnostics_internal.h"
#include "tls_test_identity.h"

#define TEST_SERVICE_ID 0x234U
#define TEST_METHOD_ID 1U
#define TEST_BODY_SIZE (64U * 1024U)
#define TEST_WAIT_MS 15000U
#define TEST_TLS_HANDSHAKE_TIMEOUT_MS 1500U
#define TEST_TLS_FLOOD_MAX_PEERS 16U
#define TEST_TLS_FLOOD_EXCESS_PEERS 48U

struct test_state {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_server *server;
	struct tr_client *client;
	struct tls_test_identity identity;
	uint16_t server_port;
	unsigned server_messages;
	unsigned server_terminals;
	unsigned client_messages;
	unsigned client_terminals;
	uint8_t response[TEST_BODY_SIZE];
	uint32_t response_len;
	enum tr_rpc_status client_status;
	uint64_t order;
	uint64_t work_order;
	uint64_t server_terminal_order;
	uint64_t client_message_order;
	uint64_t client_terminal_order;
};

struct test_proxy {
	int listener;
	uint16_t listen_port;
	uint16_t upstream_port;
	pthread_t thread;
	int thread_started;
	int result;
};

static _Atomic uint64_t wrapped_sendmsg_calls;

ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);
int __real_connect(int fd, const struct sockaddr *address, socklen_t length);

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	(void)atomic_fetch_add_explicit(&wrapped_sendmsg_calls, UINT64_C(1),
	                                memory_order_relaxed);
	return __real_sendmsg(fd, message, flags);
}

int __wrap_connect(int fd, const struct sockaddr *address, socklen_t length)
{
	int send_buffer = 4096;
	(void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_buffer,
	                 (socklen_t)sizeof(send_buffer));
	return __real_connect(fd, address, length);
}

static struct timespec test_deadline(unsigned milliseconds)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += (time_t)(milliseconds / 1000U);
	deadline.tv_nsec += (long)(milliseconds % 1000U) * 1000000L;
	if (deadline.tv_nsec >= 1000000000L) {
		++deadline.tv_sec;
		deadline.tv_nsec -= 1000000000L;
	}
	return deadline;
}

static void test_state_init(struct test_state *state)
{
	pthread_condattr_t attributes;
	memset(state, 0, sizeof(*state));
	assert(pthread_mutex_init(&state->lock, NULL) == 0);
	assert(pthread_condattr_init(&attributes) == 0);
	assert(pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&state->cond, &attributes) == 0);
	assert(pthread_condattr_destroy(&attributes) == 0);
	assert(tls_test_identity_init(&state->identity, "tls-c-reactor") == 0);
}

static void test_state_destroy(struct test_state *state)
{
	if (state->client) {
		int result = tr_client_destroy(state->client);
		assert(result == TR_OK);
		state->client = NULL;
	}
	if (state->server) {
		int result;
		do {
			result = tr_server_destroy(state->server);
			if (result == TR_AGAIN) {
				struct timespec pause = {0, 1000000L};
				assert(nanosleep(&pause, NULL) == 0);
			}
		} while (result == TR_AGAIN);
		assert(result == TR_OK);
		state->server = NULL;
	}
	tls_test_identity_cleanup(&state->identity);
	assert(pthread_cond_destroy(&state->cond) == 0);
	assert(pthread_mutex_destroy(&state->lock) == 0);
}

static void test_methods_init(struct tr_rpc_v2_method *method,
                              struct tr_rpc_method_desc *descriptor,
                              struct test_state *state)
{
	memset(method, 0, sizeof(*method));
	memset(descriptor, 0, sizeof(*descriptor));
	descriptor->service_id = TEST_SERVICE_ID;
	descriptor->method_id = TEST_METHOD_ID;
	descriptor->request_cardinality = TR_RPC_ONE;
	descriptor->response_cardinality = TR_RPC_ONE;
	descriptor->request_codec_id = TR_RPC_CODEC_RAW;
	descriptor->response_codec_id = TR_RPC_CODEC_RAW;
	descriptor->lane = TR_RPC_LANE_CONTROL;
	descriptor->max_request_bytes = TEST_BODY_SIZE;
	descriptor->max_response_bytes = TEST_BODY_SIZE;
	method->descriptor = *descriptor;
	method->work = NULL;
	method->terminal = NULL;
	method->arg = state;
}

static enum tr_rpc_status
test_server_work(struct tr_rpc_v2_call_id call,
                 const struct tr_rpc_v2_request *request,
                 const struct tr_rpc_v2_cancel *cancel,
                 struct tr_rpc_v2_reply *reply, void *arg)
{
	struct test_state *state = arg;
	(void)call;
	(void)cancel;
	if (request->kind == TR_RPC_V2_REQUEST_MESSAGE) {
		pthread_mutex_lock(&state->lock);
		state->server_messages++;
		state->work_order = ++state->order;
		pthread_mutex_unlock(&state->lock);
		if (tr_rpc_v2_reply_append(reply, &request->body) != TR_OK)
			return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	}
	return TR_RPC_STATUS_OK;
}

static void test_server_terminal(struct tr_rpc_v2_call_id call,
                                 enum tr_rpc_status status,
                                 enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct test_state *state = arg;
	(void)call;
	(void)status;
	(void)delivery;
	pthread_mutex_lock(&state->lock);
	state->server_terminals++;
	state->server_terminal_order = ++state->order;
	pthread_cond_broadcast(&state->cond);
	pthread_mutex_unlock(&state->lock);
}

static void test_client_message(struct tr_rpc_v2_call call,
                                const struct tr_rpc_bytes *body, void *arg)
{
	struct test_state *state = arg;
	(void)call;
	assert(body->len <= sizeof(state->response));
	pthread_mutex_lock(&state->lock);
	memcpy(state->response, body->data, body->len);
	state->response_len = body->len;
	state->client_messages++;
	state->client_message_order = ++state->order;
	pthread_cond_broadcast(&state->cond);
	pthread_mutex_unlock(&state->lock);
}

static void test_client_terminal(struct tr_rpc_v2_call call,
                                 enum tr_rpc_status status,
                                 enum tr_rpc_v2_delivery delivery, void *arg)
{
	struct test_state *state = arg;
	(void)call;
	(void)delivery;
	pthread_mutex_lock(&state->lock);
	state->client_status = status;
	state->client_terminals++;
	state->client_terminal_order = ++state->order;
	pthread_cond_broadcast(&state->cond);
	pthread_mutex_unlock(&state->lock);
}

static int test_server_start_with_limits(struct test_state *state,
                                         int tls_required, uint32_t max_peers,
                                         uint32_t handshake_timeout_ms)
{
	struct tr_rpc_v2_method method;
	struct tr_rpc_method_desc descriptor;
	struct tr_server_config config;
	int result;
	test_methods_init(&method, &descriptor, state);
	method.work = test_server_work;
	method.terminal = test_server_terminal;
	tr_server_config_init(&config);
	config.max_peers = max_peers;
	config.handshake_timeout_ms = handshake_timeout_ms;
	config.limits.max_message_bytes = TEST_BODY_SIZE;
	config.limits.max_frame_payload_bytes = 4096U;
	config.limits.initial_window_bytes = TEST_BODY_SIZE;
	if (tls_required) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.certificate_file =
		        state->identity.server_certificate_file;
		config.tls.private_key_file =
		        state->identity.server_private_key_file;
	}
	result = tr_server_create(&config, &method, 1U, &state->server);
	if (result != TR_OK)
		return result;
	result = tr_server_listen(state->server, "127.0.0.1", 0,
	                          &state->server_port);
	if (result != TR_OK)
		return result;
	return tr_server_start(state->server);
}

static int test_server_start(struct test_state *state, int tls_required)
{
	return test_server_start_with_limits(state, tls_required, 4U, 5000U);
}

static int test_client_create(struct test_state *state, int tls_required,
                              const char *ca_file, const char *peer_name)
{
	struct tr_rpc_method_desc descriptor;
	struct tr_client_config config;
	test_methods_init(&(struct tr_rpc_v2_method){0}, &descriptor, state);
	tr_client_config_init(&config);
	config.connect_timeout_ms = 5000U;
	config.limits.max_message_bytes = TEST_BODY_SIZE;
	config.limits.max_frame_payload_bytes = 4096U;
	config.limits.initial_window_bytes = TEST_BODY_SIZE;
	if (tls_required) {
		config.tls.mode = TR_TLS_REQUIRED;
		config.tls.ca_file = ca_file;
		config.tls.expected_peer_name = peer_name;
	}
	return tr_client_create(&config, &descriptor, 1U, &state->client);
}

static int socket_connect_ipv4(const char *address, uint16_t port)
{
	struct sockaddr_in peer;
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0)
		return -1;
	memset(&peer, 0, sizeof(peer));
	peer.sin_family = AF_INET;
	peer.sin_port = htons(port);
	if (inet_pton(AF_INET, address, &peer.sin_addr) != 1 ||
	    connect(fd, (struct sockaddr *)&peer, sizeof(peer)) != 0) {
		close(fd);
		return -1;
	}
	return fd;
}

static int proxy_send_fragments(int fd, const uint8_t *data, size_t length,
                                unsigned *sequence)
{
	size_t offset = 0U;
	while (offset < length) {
		size_t part = (size_t)(*sequence % 7U) + 1U;
		size_t sent = 0U;
		if (part > length - offset)
			part = length - offset;
		while (sent < part) {
			ssize_t count = send(fd, data + offset + sent,
			                     part - sent, MSG_NOSIGNAL);
			if (count > 0) {
				sent += (size_t)count;
				continue;
			}
			if (count < 0 && errno == EINTR)
				continue;
			return -1;
		}
		offset += part;
		++*sequence;
	}
	return 0;
}

static void *test_proxy_thread(void *arg)
{
	struct test_proxy *proxy = arg;
	struct pollfd descriptors[2];
	uint8_t buffer[4096];
	unsigned sequence = 0U;
	int client_fd = accept(proxy->listener, NULL, NULL);
	int upstream_fd = -1;
	if (client_fd < 0)
		goto fail;
	upstream_fd = socket_connect_ipv4("127.0.0.1", proxy->upstream_port);
	if (upstream_fd < 0)
		goto fail;
	descriptors[0].fd = client_fd;
	descriptors[1].fd = upstream_fd;
	for (;;) {
		int result;
		descriptors[0].events = POLLIN;
		descriptors[1].events = POLLIN;
		descriptors[0].revents = 0;
		descriptors[1].revents = 0;
		result = poll(descriptors, 2U, 5000);
		if (result == 0) {
			errno = ETIMEDOUT;
			goto fail;
		}
		if (result < 0 && errno == EINTR)
			continue;
		if (result < 0)
			goto fail;
		for (unsigned i = 0; i < 2U; ++i) {
			ssize_t count;
			int destination;
			if (!(descriptors[i].revents &
			      (POLLIN | POLLHUP | POLLERR)))
				continue;
			count = recv(descriptors[i].fd, buffer, sizeof(buffer),
			             0);
			if (count == 0)
				goto done;
			if (count < 0 && errno == EINTR)
				continue;
			if (count < 0)
				goto fail;
			destination = descriptors[1U - i].fd;
			if (proxy_send_fragments(destination, buffer,
			                         (size_t)count, &sequence) != 0)
				goto fail;
		}
	}

done:
	proxy->result = 0;
	close(client_fd);
	close(upstream_fd);
	return NULL;
fail:
	proxy->result = -1;
	if (client_fd >= 0)
		close(client_fd);
	if (upstream_fd >= 0)
		close(upstream_fd);
	return NULL;
}

static int test_proxy_start(struct test_proxy *proxy, uint16_t upstream_port)
{
	struct sockaddr_in address;
	socklen_t address_length = (socklen_t)sizeof(address);
	proxy->listener = socket(AF_INET, SOCK_STREAM, 0);
	if (proxy->listener < 0)
		return -1;
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0;
	if (bind(proxy->listener, (struct sockaddr *)&address,
	         (socklen_t)sizeof(address)) != 0 ||
	    listen(proxy->listener, 1) != 0 ||
	    getsockname(proxy->listener, (struct sockaddr *)&address,
	                &address_length) != 0)
		return -1;
	proxy->listen_port = ntohs(address.sin_port);
	proxy->upstream_port = upstream_port;
	proxy->result = -1;
	if (pthread_create(&proxy->thread, NULL, test_proxy_thread, proxy) != 0)
		return -1;
	proxy->thread_started = 1;
	return 0;
}

static void test_proxy_stop(struct test_proxy *proxy)
{
	if (proxy->thread_started) {
		assert(pthread_join(proxy->thread, NULL) == 0);
		assert(proxy->result == 0);
		proxy->thread_started = 0;
	}
	if (proxy->listener >= 0) {
		close(proxy->listener);
		proxy->listener = -1;
	}
}

static void wait_client_terminal(struct test_state *state)
{
	struct timespec deadline = test_deadline(TEST_WAIT_MS);
	pthread_mutex_lock(&state->lock);
	while (state->client_terminals == 0U) {
		int result = pthread_cond_timedwait(&state->cond, &state->lock,
		                                    &deadline);
		assert(result == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static void test_roundtrip(int tls_required, const char *peer_name,
                           int use_proxy)
{
	struct test_state state;
	struct test_proxy proxy;
	struct tr_rpc_v2_client_callbacks callbacks;
	struct tr_rpc_bytes request;
	struct tr_rpc_v2_call call;
	uint8_t request_data[TEST_BODY_SIZE];
	uint16_t destination_port;
	int result;
	test_state_init(&state);
	memset(&proxy, 0, sizeof(proxy));
	proxy.listener = -1;
	assert(test_server_start(&state, tls_required) == TR_OK);
	assert(test_client_create(&state, tls_required,
	                          tls_required ? state.identity.ca_file : NULL,
	                          peer_name) == TR_OK);
	if (use_proxy) {
		assert(test_proxy_start(&proxy, state.server_port) == 0);
		destination_port = proxy.listen_port;
	} else {
		destination_port = state.server_port;
	}
	atomic_store_explicit(&wrapped_sendmsg_calls, 0U, memory_order_relaxed);
	result = tr_client_connect(state.client, "127.0.0.1", destination_port);
	if (result != TR_OK)
		fprintf(stderr,
		        "tls-reactor: client connect failed status=%d\n",
		        result);
	assert(result == TR_OK);
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.message = test_client_message;
	callbacks.terminal = test_client_terminal;
	callbacks.arg = &state;
	for (uint32_t i = 0U; i < TEST_BODY_SIZE; ++i)
		request_data[i] = (uint8_t)((i * 37U + 11U) & 0xffU);
	request.data = request_data;
	request.len = sizeof(request_data);
	assert(tr_client_unary_call(state.client, TEST_SERVICE_ID,
	                            TEST_METHOD_ID, &request, NULL, &callbacks,
	                            &call) == TR_OK);
	wait_client_terminal(&state);
	pthread_mutex_lock(&state.lock);
	assert(state.client_status == TR_RPC_STATUS_OK);
	assert(state.server_messages == 1U);
	assert(state.server_terminals == 1U);
	assert(state.client_messages == 1U);
	assert(state.response_len == sizeof(request_data));
	assert(memcmp(state.response, request_data, sizeof(request_data)) == 0);
	assert(state.work_order < state.server_terminal_order);
	assert(state.client_message_order < state.client_terminal_order);
	pthread_mutex_unlock(&state.lock);
	assert(tr_client_begin_drain(state.client) == TR_OK);
	assert(tr_client_wait_drained(state.client, TEST_WAIT_MS) == TR_OK);
	if (tls_required)
		assert(atomic_load_explicit(&wrapped_sendmsg_calls,
		                            memory_order_relaxed) == 0U);
	else
		assert(atomic_load_explicit(&wrapped_sendmsg_calls,
		                            memory_order_relaxed) > 0U);
	assert(tr_client_destroy(state.client) == TR_OK);
	state.client = NULL;
	if (use_proxy)
		test_proxy_stop(&proxy);
	test_state_destroy(&state);
	printf("tls-reactor: roundtrip mode=%s proxy=%d passed\n",
	       tls_required ? "required" : "disabled", use_proxy);
}

static void test_identity_failure(int bad_ca)
{
	struct test_state state;
	const char *ca_file;
	const char *peer_name;
	int result;
	test_state_init(&state);
	ca_file = bad_ca ? state.identity.alternate_ca_file
	                 : state.identity.ca_file;
	peer_name = bad_ca ? "127.0.0.1" : "wrong.example";
	assert(test_server_start(&state, 1) == TR_OK);
	assert(test_client_create(&state, 1, ca_file, peer_name) == TR_OK);
	result =
	        tr_client_connect(state.client, "127.0.0.1", state.server_port);
	assert(result != TR_OK);
	pthread_mutex_lock(&state.lock);
	assert(state.server_messages == 0U);
	assert(state.server_terminals == 0U);
	assert(state.client_messages == 0U);
	assert(state.client_terminals == 0U);
	pthread_mutex_unlock(&state.lock);
	test_state_destroy(&state);
}

static void test_plaintext_is_rejected_by_tls_listener(void)
{
	struct test_state state;
	struct sockaddr_in address;
	const uint8_t plaintext[] = {0x54U, 0x52U, 0x50U, 0x31U};
	int fd;
	test_state_init(&state);
	assert(test_server_start(&state, 1) == TR_OK);
	fd = socket_connect_ipv4("127.0.0.1", state.server_port);
	assert(fd >= 0);
	assert(send(fd, plaintext, sizeof(plaintext), MSG_NOSIGNAL) ==
	       (ssize_t)sizeof(plaintext));
	memset(&address, 0, sizeof(address));
	{
		struct timespec pause = {0, 100000000L};
		assert(nanosleep(&pause, NULL) == 0);
	}
	close(fd);
	pthread_mutex_lock(&state.lock);
	assert(state.server_messages == 0U);
	assert(state.server_terminals == 0U);
	pthread_mutex_unlock(&state.lock);
	test_state_destroy(&state);
}

static uint64_t test_monotonic_ms(void)
{
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

static void wait_server_peer_counts(struct tr_server *server, uint32_t current,
                                    uint32_t ready)
{
	struct timespec pause = {0, 1000000L};
	struct tr_server_stats stats;
	unsigned attempt;

	for (attempt = 0U; attempt < TEST_WAIT_MS; ++attempt) {
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == current &&
		    stats.peers_ready_current == ready)
			return;
		assert(nanosleep(&pause, NULL) == 0);
	}
	assert(tr_server_get_stats(server, &stats) == TR_OK);
	assert(stats.peers_current == current);
	assert(stats.peers_ready_current == ready);
}

static void wait_socket_closed(int fd)
{
	uint64_t deadline = test_monotonic_ms() + TEST_WAIT_MS;
	uint8_t buffer[256];

	for (;;) {
		struct pollfd descriptor = {
		        .fd = fd,
		        .events = POLLIN | POLLHUP | POLLERR,
		};
		uint64_t now = test_monotonic_ms();
		int timeout;
		int result;
		ssize_t amount;

		assert(now < deadline);
		timeout = (int)(deadline - now);
		result = poll(&descriptor, 1U, timeout);
		if (result < 0 && errno == EINTR)
			continue;
		assert(result > 0);
		amount = recv(fd, buffer, sizeof(buffer), 0);
		if (amount == 0 || (amount < 0 && errno == ECONNRESET))
			return;
		assert(amount > 0 || errno == EINTR);
	}
}

static void test_tls_peer_capacity_burst_is_bounded(void)
{
	static const uint8_t partial_tls_record[] = {
	        0x16U, 0x03U, 0x03U, 0x00U, 0x40U,
	};
	struct test_state state;
	struct tr_server_stats stats;
	int slow_peers[TEST_TLS_FLOOD_MAX_PEERS];
	int excess_peers[TEST_TLS_FLOOD_EXCESS_PEERS];
	uint64_t start_ms;
	uint64_t elapsed_ms;
	unsigned i;

	for (i = 0U; i < TEST_TLS_FLOOD_MAX_PEERS; ++i)
		slow_peers[i] = -1;
	for (i = 0U; i < TEST_TLS_FLOOD_EXCESS_PEERS; ++i)
		excess_peers[i] = -1;
	test_state_init(&state);
	assert(test_server_start_with_limits(
	               &state, 1, TEST_TLS_FLOOD_MAX_PEERS,
	               TEST_TLS_HANDSHAKE_TIMEOUT_MS) == TR_OK);
	start_ms = test_monotonic_ms();
	for (i = 0U; i < TEST_TLS_FLOOD_MAX_PEERS; ++i) {
		slow_peers[i] =
		        socket_connect_ipv4("127.0.0.1", state.server_port);
		assert(slow_peers[i] >= 0);
		assert(send(slow_peers[i], partial_tls_record,
		            sizeof(partial_tls_record), MSG_NOSIGNAL) ==
		       (ssize_t)sizeof(partial_tls_record));
	}
	wait_server_peer_counts(state.server, TEST_TLS_FLOOD_MAX_PEERS, 0U);
	for (i = 0U; i < TEST_TLS_FLOOD_EXCESS_PEERS; ++i) {
		excess_peers[i] =
		        socket_connect_ipv4("127.0.0.1", state.server_port);
		assert(excess_peers[i] >= 0);
	}
	for (i = 0U; i < TEST_TLS_FLOOD_EXCESS_PEERS; ++i) {
		wait_socket_closed(excess_peers[i]);
		assert(close(excess_peers[i]) == 0);
		excess_peers[i] = -1;
	}
	wait_server_peer_counts(state.server, TEST_TLS_FLOOD_MAX_PEERS, 0U);
	for (i = 0U; i < TEST_TLS_FLOOD_MAX_PEERS; ++i) {
		wait_socket_closed(slow_peers[i]);
		assert(close(slow_peers[i]) == 0);
		slow_peers[i] = -1;
	}
	elapsed_ms = test_monotonic_ms() - start_ms;
	assert(elapsed_ms >= TEST_TLS_HANDSHAKE_TIMEOUT_MS / 2U);
	wait_server_peer_counts(state.server, 0U, 0U);
	assert(tr_server_get_stats(state.server, &stats) == TR_OK);
	assert(stats.peers_peak <= TEST_TLS_FLOOD_MAX_PEERS);
	assert(stats.peer_capacity_rejections >= TEST_TLS_FLOOD_EXCESS_PEERS);
	assert(stats.peers_reaped_total >= TEST_TLS_FLOOD_MAX_PEERS);

	assert(test_client_create(&state, 1, state.identity.ca_file,
	                          "127.0.0.1") == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1",
	                         state.server_port) == TR_OK);
	wait_server_peer_counts(state.server, 1U, 1U);
	test_state_destroy(&state);
	puts("tls-reactor: TLS-required connection burst stayed within the "
	     "16-peer capacity and recovered");
}

static void test_transport_only_tls_support_boundary(void)
{
	struct tr_client_config client_config;
	struct tr_server_config server_config;
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;
	struct tls_test_identity identity;
	assert(tls_test_identity_init(&identity, "tls-c-transport") == 0);
	tr_client_config_init(&client_config);
	client_config.tls.mode = TR_TLS_REQUIRED;
	client_config.tls.ca_file = identity.ca_file;
	client_config.tls.expected_peer_name = "localhost";
	assert(tr_client_create_transport(&client_config, &client) ==
	       TR_ERR_INVALID);
	assert(client == NULL);
	client_config.tls.client_certificate_file =
	        identity.client_certificate_file;
	client_config.tls.client_private_key_file =
	        identity.client_private_key_file;
	client_config.tls.expected_peer_name = NULL;
	assert(tr_client_create_transport(&client_config, &client) ==
	       TR_ERR_INVALID);
	assert(client == NULL);
	client_config.tls.expected_peer_name = "localhost";
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(client != NULL);
	assert(tr_client_destroy(client) == TR_OK);
	tr_server_config_init(&server_config);
	server_config.tls.mode = TR_TLS_REQUIRED;
	server_config.tls.certificate_file = identity.server_certificate_file;
	server_config.tls.private_key_file = identity.server_private_key_file;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_UNSUPPORTED);
	assert(server == NULL);
	tls_test_identity_cleanup(&identity);
}

static void test_server_initiated_tls_close(void)
{
	struct test_state state;
	test_state_init(&state);
	assert(test_server_start(&state, 1) == TR_OK);
	assert(test_client_create(&state, 1, state.identity.ca_file,
	                          "127.0.0.1") == TR_OK);
	assert(tr_client_connect(state.client, "127.0.0.1",
	                         state.server_port) == TR_OK);
	assert(tr_server_drain(state.server, TEST_WAIT_MS) == TR_OK);
	{
		unsigned attempt;
		int ret;
		for (attempt = 0; attempt < TEST_WAIT_MS; ++attempt) {
			struct timespec pause = {0, 1000000L};
			ret = tr_server_destroy(state.server);
			if (ret != TR_AGAIN)
				break;
			assert(nanosleep(&pause, NULL) == 0);
		}
		if (ret != TR_OK)
			fprintf(stderr, "server TLS destroy failed: %d (%s)\n",
			        ret, tr_status_str(ret));
		assert(ret == TR_OK);
	}
	state.server = NULL;
	assert(tr_client_destroy(state.client) == TR_OK);
	state.client = NULL;
	test_state_destroy(&state);
	puts("tls-reactor: server-initiated graceful TLS close passed");
}

int main(int argc, char **argv)
{
	if (argc == 2 && strcmp(argv[1], "disabled") == 0) {
		test_roundtrip(0, NULL, 0);
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "tls") == 0) {
		test_roundtrip(1, "127.0.0.1", 1);
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "bad-ca") == 0) {
		test_identity_failure(1);
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "bad-san") == 0) {
		test_identity_failure(0);
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "plaintext") == 0) {
		test_plaintext_is_rejected_by_tls_listener();
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "transport") == 0) {
		test_transport_only_tls_support_boundary();
		return 0;
	}
	if (argc == 2 && strcmp(argv[1], "server-close") == 0) {
		test_server_initiated_tls_close();
		return 0;
	}
	assert(argc == 1);
	test_transport_only_tls_support_boundary();
	test_plaintext_is_rejected_by_tls_listener();
	test_tls_peer_capacity_burst_is_bounded();
	test_identity_failure(1);
	test_identity_failure(0);
	test_roundtrip(0, NULL, 0);
	test_roundtrip(1, "127.0.0.1", 1);
	test_server_initiated_tls_close();
	return 0;
}
