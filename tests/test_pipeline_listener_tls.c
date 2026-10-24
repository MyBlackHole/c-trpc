#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "tr/server.h"
#include "tr/status.h"
#include "tr/transport.h"

#include "../src/crc32c.h"
#include "../src/group/pipeline_control_wire_internal.h"
#include "../src/group/pipeline_route_internal.h"
#include "../src/transport/protocol/wire.h"
#include "tls_test_identity.h"

#define TEST_GROUP_ID UINT64_C(0x8d31)
#define TEST_GROUP_EPOCH UINT64_C(204)
#define TEST_CONTROL_GENERATION 7U

struct group_tls_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct tr_server *server;
	_Atomic unsigned control_authorizations;
	_Atomic unsigned data_authorizations;
	_Atomic unsigned messages;
	_Atomic unsigned data_events;
};

struct tls_peer {
	int fd;
	SSL_CTX *context;
	SSL *ssl;
};

struct preface_defer_test_state {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	struct tr_reactor *reactor;
	struct tr_conn_handle connection;
	int resume_fd;
	int resume_status;
	int resume_result;
	int check_duplicate;
	int duplicate_result;
	int check_stale;
	struct tr_conn_handle stale_connection;
	int stale_result;
	int connection_ready;
	uint32_t preface_bytes;
	unsigned deferred;
	unsigned frames;
	unsigned releases;
	unsigned connection_events;
	unsigned resumed;
};

static const uint8_t expected_client_uri[] = "spiffe://c-trpc.example/client";
static const uint8_t message_body[] = "tls-group-data";

static int connect_loopback(uint16_t port);

static void wait_preface_test_counter(struct preface_defer_test_state *state,
                                      unsigned *counter, unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	assert(pthread_mutex_lock(&state->lock) == 0);
	while (*counter < expected)
		assert(pthread_cond_timedwait(&state->changed, &state->lock,
		                              &deadline) == 0);
	assert(pthread_mutex_unlock(&state->lock) == 0);
}

static int defer_test_preface(struct tr_conn_handle connection,
                              const uint8_t *data, size_t len, int *done,
                              void *arg)
{
	struct preface_defer_test_state *state = arg;
	(void)connection;
	assert(data != NULL && len != 0U && done != NULL);
	*done = 0;
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->preface_bytes += (uint32_t)len;
	if (state->preface_bytes == TR_PIPELINE_ROUTE_PREFACE_SIZE) {
		*done = 1;
		state->deferred++;
		pthread_cond_broadcast(&state->changed);
	}
	assert(state->preface_bytes <= TR_PIPELINE_ROUTE_PREFACE_SIZE);
	assert(pthread_mutex_unlock(&state->lock) == 0);
	return *done ? TR_IN_PROGRESS : TR_OK;
}

static void release_test_preface(void *arg)
{
	struct preface_defer_test_state *state = arg;
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->releases++;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
}

static enum tr_frame_disposition
on_deferred_test_frame(struct tr_conn_handle connection, struct tr_frame *frame,
                       void *arg)
{
	struct preface_defer_test_state *state = arg;
	(void)connection;
	assert(frame != NULL);
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->frames++;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
	return TR_FRAME_RELEASE;
}

static void on_deferred_test_connection_event(struct tr_conn_handle connection,
                                              enum tr_connection_event event,
                                              int status, void *arg)
{
	struct preface_defer_test_state *state = arg;
	(void)connection;
	(void)event;
	(void)status;
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->connection_events++;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
}

static void resume_deferred_test_connection(int fd, uint32_t events, void *arg)
{
	struct preface_defer_test_state *state = arg;
	struct tr_conn_handle connection;
	struct tr_conn_handle stale_connection;
	uint64_t value;
	ssize_t result;
	int status;
	int check_duplicate;
	int check_stale;
	int duplicate_result = TR_ERR_STATE;
	int stale_result = TR_ERR_STATE;
	int ret;
	(void)events;
	result = read(fd, &value, sizeof(value));
	assert(result == (ssize_t)sizeof(value));
	assert(value == 1U);
	assert(pthread_mutex_lock(&state->lock) == 0);
	connection = state->connection;
	stale_connection = state->stale_connection;
	status = state->resume_status;
	check_duplicate = state->check_duplicate;
	check_stale = state->check_stale;
	assert(pthread_mutex_unlock(&state->lock) == 0);
	if (check_stale)
		stale_result = tr_reactor_preface_resume_on_owner(
		        stale_connection, TR_OK);
	ret = tr_reactor_preface_resume_on_owner(connection, status);
	if (check_duplicate)
		duplicate_result =
		        tr_reactor_preface_resume_on_owner(connection, TR_OK);
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->resume_result = ret;
	state->duplicate_result = duplicate_result;
	state->stale_result = stale_result;
	state->resumed++;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
}

static void accept_deferred_test_connection(int fd, uint32_t events, void *arg)
{
	struct preface_defer_test_state *state = arg;
	struct tr_reactor_preface_handler preface;
	struct tr_conn_handle connection;
	int accepted;
	int ret;
	(void)events;
	accepted = accept(fd, NULL, NULL);
	assert(accepted >= 0);
	memset(&preface, 0, sizeof(preface));
	preface.byte_count = TR_PIPELINE_ROUTE_PREFACE_SIZE;
	preface.feed = defer_test_preface;
	preface.release = release_test_preface;
	preface.arg = state;
	ret = tr_reactor_adopt_fd_prefaced_on_owner(state->reactor, accepted,
	                                            &preface, &connection);
	assert(ret == TR_OK);
	assert(pthread_mutex_lock(&state->lock) == 0);
	state->preface_bytes = 0U;
	state->connection = connection;
	state->connection_ready = 1;
	pthread_cond_broadcast(&state->changed);
	assert(pthread_mutex_unlock(&state->lock) == 0);
}

static int make_preface_test_listener(uint16_t *port)
{
	struct sockaddr_in address;
	socklen_t address_len = sizeof(address);
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0;
	assert(bind(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	assert(getsockname(fd, (struct sockaddr *)&address, &address_len) == 0);
	assert(listen(fd, 4) == 0);
	*port = ntohs(address.sin_port);
	return fd;
}

static void send_preface_and_test_frame(int fd)
{
	struct tr_pipeline_route_preface route = {
	        .version = TR_PIPELINE_ROUTE_VERSION,
	        .role = TR_PIPELINE_ROUTE_CONTROL,
	        .owner_shard_id = 0U,
	        .pipeline_id = TEST_GROUP_ID,
	        .epoch = TEST_GROUP_EPOCH,
	        .member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL,
	        .member_generation = TEST_CONTROL_GENERATION,
	};
	struct tr_frame_header header = {
	        .version = TR_WIRE_ENV_VERSION,
	        .type = TR_FRAME_DATA,
	        .flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        .stream_id = 3U,
	        .message_id = 5U,
	        .payload_len = sizeof(message_body) - 1U,
	        .payload_crc32c = 0U,
	};
	uint8_t preface[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint8_t encoded[TR_WIRE_HEADER_SIZE];
	const uint8_t *parts[] = {preface, encoded, message_body};
	size_t sizes[] = {sizeof(preface), sizeof(encoded),
	                  sizeof(message_body) - 1U};
	header.payload_crc32c =
	        tr_crc32c(message_body, sizeof(message_body) - 1U);
	assert(tr_pipeline_route_preface_encode(preface, &route) == TR_OK);
	assert(tr_wire_header_encode(encoded, &header) == TR_OK);
	for (size_t part = 0U; part < sizeof(parts) / sizeof(parts[0]);
	     ++part) {
		const uint8_t *cursor = parts[part];
		size_t left = sizes[part];
		while (left != 0U) {
			ssize_t written = send(fd, cursor, left, MSG_NOSIGNAL);
			if (written > 0) {
				cursor += (size_t)written;
				left -= (size_t)written;
				continue;
			}
			assert(written < 0 && errno == EINTR);
		}
	}
}

static void run_reactor_preface_resume_case(int status, int expect_frame,
                                            int close_before_resume,
                                            int check_duplicate)
{
	struct preface_defer_test_state state;
	struct tr_reactor_config config = {.max_connections = 1U};
	struct tr_reactor *reactor = NULL;
	uint16_t port = 0U;
	int listen_fd;
	int client_fd;
	uint64_t resume_signal = 1U;
	struct pollfd ready;

	memset(&state, 0, sizeof(state));
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	assert(pthread_cond_init(&state.changed, NULL) == 0);
	state.resume_status = status;
	state.check_duplicate = check_duplicate;
	listen_fd = make_preface_test_listener(&port);
	state.resume_fd = eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
	assert(state.resume_fd >= 0);
	assert(tr_reactor_create(&config, on_deferred_test_frame,
	                         on_deferred_test_connection_event, &state,
	                         &reactor) == TR_OK);
	state.reactor = reactor;
	assert(tr_reactor_listener_register(reactor, listen_fd,
	                                    accept_deferred_test_connection,
	                                    &state) == TR_OK);
	assert(tr_reactor_aux_event_register(reactor, state.resume_fd, EPOLLIN,
	                                     resume_deferred_test_connection,
	                                     &state) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	client_fd = connect_loopback(port);
	assert(pthread_mutex_lock(&state.lock) == 0);
	while (!state.connection_ready)
		assert(pthread_cond_wait(&state.changed, &state.lock) == 0);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	send_preface_and_test_frame(client_fd);
	wait_preface_test_counter(&state, &state.deferred, 1U);
	ready.fd = client_fd;
	ready.events = POLLIN | POLLHUP;
	ready.revents = 0;
	assert(poll(&ready, 1U, 100) == 0);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.frames == 0U);
	assert(state.releases == 0U);
	assert(state.connection_events == 0U);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	if (close_before_resume) {
		close(client_fd);
		client_fd = -1;
		wait_preface_test_counter(&state, &state.connection_events, 1U);
		assert(pthread_mutex_lock(&state.lock) == 0);
		assert(state.frames == 0U);
		assert(state.releases == 1U);
		assert(state.resumed == 0U);
		assert(pthread_mutex_unlock(&state.lock) == 0);
	} else {
		assert(write(state.resume_fd, &resume_signal,
		             sizeof(resume_signal)) ==
		       (ssize_t)sizeof(resume_signal));
		wait_preface_test_counter(&state, &state.resumed, 1U);
		assert(pthread_mutex_lock(&state.lock) == 0);
		assert(state.resume_result == TR_OK);
		assert(state.releases == 1U);
		assert(pthread_mutex_unlock(&state.lock) == 0);
		if (check_duplicate) {
			assert(pthread_mutex_lock(&state.lock) == 0);
			assert(state.duplicate_result == TR_ERR_STATE);
			assert(pthread_mutex_unlock(&state.lock) == 0);
		}
		if (expect_frame) {
			wait_preface_test_counter(&state, &state.frames, 1U);
			assert(pthread_mutex_lock(&state.lock) == 0);
			assert(state.connection_events == 0U);
			assert(pthread_mutex_unlock(&state.lock) == 0);
		} else {
			wait_preface_test_counter(&state,
			                          &state.connection_events, 1U);
			assert(pthread_mutex_lock(&state.lock) == 0);
			assert(state.frames == 0U);
			assert(state.releases == 1U);
			assert(pthread_mutex_unlock(&state.lock) == 0);
		}
	}
	if (client_fd >= 0)
		close(client_fd);
	if (expect_frame && !close_before_resume)
		wait_preface_test_counter(&state, &state.connection_events, 1U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.frames == (expect_frame ? 1U : 0U));
	assert(state.releases == 1U);
	assert(state.connection_events == 1U);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	assert(tr_reactor_aux_event_unregister(reactor, state.resume_fd) ==
	       TR_OK);
	assert(tr_reactor_listener_unregister(reactor, listen_fd) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	close(state.resume_fd);
	close(listen_fd);
	assert(pthread_cond_destroy(&state.changed) == 0);
	assert(pthread_mutex_destroy(&state.lock) == 0);
}

static void test_reactor_preface_defer_pauses_and_resumes(void)
{
	run_reactor_preface_resume_case(TR_OK, 1, 0, 1);
	run_reactor_preface_resume_case(TR_ERR_CANCELLED, 0, 0, 0);
	run_reactor_preface_resume_case(TR_OK, 0, 1, 0);
}

static void test_reactor_preface_resume_rejects_reused_generation(void)
{
	struct preface_defer_test_state state;
	struct tr_reactor_config config = {.max_connections = 1U};
	struct tr_reactor *reactor = NULL;
	struct tr_conn_handle old_connection;
	struct tr_conn_handle current_connection;
	uint16_t port = 0U;
	uint64_t resume_signal = 1U;
	int listen_fd;
	int first_fd;
	int second_fd;

	memset(&state, 0, sizeof(state));
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	assert(pthread_cond_init(&state.changed, NULL) == 0);
	state.resume_status = TR_OK;
	state.check_stale = 1;
	listen_fd = make_preface_test_listener(&port);
	state.resume_fd = eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK);
	assert(state.resume_fd >= 0);
	assert(tr_reactor_create(&config, on_deferred_test_frame,
	                         on_deferred_test_connection_event, &state,
	                         &reactor) == TR_OK);
	state.reactor = reactor;
	assert(tr_reactor_listener_register(reactor, listen_fd,
	                                    accept_deferred_test_connection,
	                                    &state) == TR_OK);
	assert(tr_reactor_aux_event_register(reactor, state.resume_fd, EPOLLIN,
	                                     resume_deferred_test_connection,
	                                     &state) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	first_fd = connect_loopback(port);
	assert(pthread_mutex_lock(&state.lock) == 0);
	while (!state.connection_ready)
		assert(pthread_cond_wait(&state.changed, &state.lock) == 0);
	old_connection = state.connection;
	assert(pthread_mutex_unlock(&state.lock) == 0);
	send_preface_and_test_frame(first_fd);
	wait_preface_test_counter(&state, &state.deferred, 1U);
	close(first_fd);
	wait_preface_test_counter(&state, &state.connection_events, 1U);
	wait_preface_test_counter(&state, &state.releases, 1U);

	assert(pthread_mutex_lock(&state.lock) == 0);
	state.connection_ready = 0;
	state.stale_connection = old_connection;
	assert(pthread_mutex_unlock(&state.lock) == 0);
	second_fd = connect_loopback(port);
	assert(pthread_mutex_lock(&state.lock) == 0);
	while (!state.connection_ready)
		assert(pthread_cond_wait(&state.changed, &state.lock) == 0);
	current_connection = state.connection;
	assert(pthread_mutex_unlock(&state.lock) == 0);
	assert(current_connection.slot == old_connection.slot);
	assert(current_connection.generation != old_connection.generation);
	send_preface_and_test_frame(second_fd);
	wait_preface_test_counter(&state, &state.deferred, 2U);
	assert(write(state.resume_fd, &resume_signal, sizeof(resume_signal)) ==
	       (ssize_t)sizeof(resume_signal));
	wait_preface_test_counter(&state, &state.resumed, 1U);
	wait_preface_test_counter(&state, &state.frames, 1U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.stale_result == TR_ERR_STALE);
	assert(state.resume_result == TR_OK);
	assert(state.releases == 2U);
	assert(state.connection_events == 1U);
	assert(pthread_mutex_unlock(&state.lock) == 0);
	close(second_fd);
	wait_preface_test_counter(&state, &state.connection_events, 2U);
	assert(pthread_mutex_lock(&state.lock) == 0);
	assert(state.frames == 1U);
	assert(state.releases == 2U);
	assert(state.connection_events == 2U);
	assert(pthread_mutex_unlock(&state.lock) == 0);

	assert(tr_reactor_aux_event_unregister(reactor, state.resume_fd) ==
	       TR_OK);
	assert(tr_reactor_listener_unregister(reactor, listen_fd) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	close(state.resume_fd);
	close(listen_fd);
	assert(pthread_cond_destroy(&state.changed) == 0);
	assert(pthread_mutex_destroy(&state.lock) == 0);
}

static int peer_identity_equals(const struct tr_peer_identity_view *identity,
                                const uint8_t *uri, uint32_t uri_len)
{
	return identity->authentication == TR_PEER_IDENTITY_TLS_CERTIFICATE &&
	       identity->uri_san && identity->uri_san_len == uri_len &&
	       memcmp(identity->uri_san, uri, uri_len) == 0;
}

static int peer_identity_is_none(const struct tr_peer_identity_view *identity)
{
	return identity->authentication == TR_PEER_IDENTITY_NONE &&
	       identity->uri_san == NULL && identity->uri_san_len == 0U;
}

static int
authorize_group(const struct tr_connection_group_authorization *authorization,
                void *arg)
{
	struct group_tls_state *state = arg;
	if (!authorization || !state ||
	    authorization->group.group_id != TEST_GROUP_ID ||
	    authorization->group.epoch != TEST_GROUP_EPOCH)
		return TR_ERR_STALE;

	if (authorization->role == TR_CONNECTION_GROUP_ROLE_CONTROL) {
		if (!peer_identity_equals(
		            &authorization->peer_identity, expected_client_uri,
		            (uint32_t)sizeof(expected_client_uri) - 1U) ||
		    !peer_identity_is_none(&authorization->control_identity))
			return TR_ERR_INVALID;
		pthread_mutex_lock(&state->lock);
		state->control_authorizations++;
		pthread_cond_broadcast(&state->changed);
		pthread_mutex_unlock(&state->lock);
		return TR_OK;
	}
	if (authorization->role == TR_CONNECTION_GROUP_ROLE_DATA) {
		if (!peer_identity_equals(
		            &authorization->peer_identity, expected_client_uri,
		            (uint32_t)sizeof(expected_client_uri) - 1U) ||
		    !peer_identity_equals(
		            &authorization->control_identity,
		            expected_client_uri,
		            (uint32_t)sizeof(expected_client_uri) - 1U))
			return TR_ERR_INVALID;
		pthread_mutex_lock(&state->lock);
		state->data_authorizations++;
		pthread_cond_broadcast(&state->changed);
		pthread_mutex_unlock(&state->lock);
		return TR_OK;
	}
	return TR_ERR_INVALID;
}

static enum tr_connection_group_message_disposition
on_group_message(const struct tr_connection_group_message *message, void *arg)
{
	struct group_tls_state *state = arg;
	assert(message != NULL);
	assert(message->group.group_id == TEST_GROUP_ID);
	assert(message->group.epoch == TEST_GROUP_EPOCH);
	assert(message->stream_id == 31U);
	assert(message->message_id == 81U);
	assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
	                          TR_CONNECTION_GROUP_DATA_LAST));
	assert(message->bytes.len == sizeof(message_body) - 1U);
	assert(memcmp(message->bytes.data, message_body,
	              sizeof(message_body) - 1U) == 0);
	pthread_mutex_lock(&state->lock);
	state->messages++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_group_data_event(const struct tr_connection_group_id *group,
                                enum tr_connection_group_data_event event,
                                int status, void *arg)
{
	struct group_tls_state *state = arg;
	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(event == TR_CONNECTION_GROUP_DATA_CLOSED ||
	       event == TR_CONNECTION_GROUP_DATA_ERROR);
	(void)status;
	pthread_mutex_lock(&state->lock);
	state->data_events++;
	pthread_cond_broadcast(&state->changed);
	pthread_mutex_unlock(&state->lock);
}

static void wait_counter(struct group_tls_state *state,
                         _Atomic unsigned *counter, unsigned expected)
{
	struct timespec deadline;
	assert(clock_gettime(CLOCK_MONOTONIC, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&state->lock);
	while (atomic_load_explicit(counter, memory_order_relaxed) < expected) {
		int ret = pthread_cond_timedwait(&state->changed, &state->lock,
		                                 &deadline);
		assert(ret == 0);
	}
	pthread_mutex_unlock(&state->lock);
}

static int connect_loopback(uint16_t port)
{
	struct sockaddr_in address;
	struct timeval timeout = {.tv_sec = 10, .tv_usec = 0};
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);
	assert(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
	                  sizeof(timeout)) == 0);
	assert(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
	                  sizeof(timeout)) == 0);
	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	assert(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
	assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	return fd;
}

static int tls_peer_open(struct tls_peer *peer, uint16_t port,
                         const struct tls_test_identity *identity,
                         const char *certificate_file)
{
	X509_VERIFY_PARAM *parameters;
	memset(peer, 0, sizeof(*peer));
	peer->fd = -1;
	peer->fd = connect_loopback(port);
	peer->context = SSL_CTX_new(TLS_client_method());
	if (!peer->context ||
	    SSL_CTX_set_min_proto_version(peer->context, TLS1_3_VERSION) != 1 ||
	    SSL_CTX_set_max_proto_version(peer->context, TLS1_3_VERSION) != 1 ||
	    SSL_CTX_load_verify_locations(peer->context, identity->ca_file,
	                                  NULL) != 1)
		return 0;
	parameters = SSL_CTX_get0_param(peer->context);
	if (!parameters ||
	    X509_VERIFY_PARAM_set_purpose(parameters,
	                                  X509_PURPOSE_SSL_SERVER) != 1 ||
	    X509_VERIFY_PARAM_set1_host(parameters, "server.example.test",
	                                0U) != 1)
		return 0;
	SSL_CTX_set_verify(peer->context, SSL_VERIFY_PEER, NULL);
	if (certificate_file &&
	    (SSL_CTX_use_certificate_chain_file(peer->context,
	                                        certificate_file) != 1 ||
	     SSL_CTX_use_PrivateKey_file(peer->context,
	                                 identity->client_private_key_file,
	                                 SSL_FILETYPE_PEM) != 1 ||
	     SSL_CTX_check_private_key(peer->context) != 1))
		return 0;
	peer->ssl = SSL_new(peer->context);
	if (!peer->ssl || SSL_set_fd(peer->ssl, peer->fd) != 1 ||
	    SSL_set_tlsext_host_name(peer->ssl, "server.example.test") != 1)
		return 0;
	return SSL_connect(peer->ssl) == 1;
}

static void tls_peer_close(struct tls_peer *peer)
{
	if (!peer)
		return;
	if (peer->ssl)
		SSL_free(peer->ssl);
	if (peer->context)
		SSL_CTX_free(peer->context);
	if (peer->fd >= 0) {
		shutdown(peer->fd, SHUT_RDWR);
		close(peer->fd);
	}
	memset(peer, 0, sizeof(*peer));
	peer->fd = -1;
}

static void ssl_write_all(SSL *ssl, const void *data, size_t size)
{
	const uint8_t *cursor = data;
	while (size != 0U) {
		size_t written = 0U;
		assert(SSL_write_ex(ssl, cursor, size, &written) == 1);
		assert(written != 0U && written <= size);
		cursor += written;
		size -= written;
	}
}

static void ssl_read_all_timeout(SSL *ssl, int fd, void *data, size_t size)
{
	uint8_t *cursor = data;
	while (size != 0U) {
		struct pollfd ready = {.fd = fd, .events = POLLIN | POLLHUP};
		size_t received = 0U;
		int result;
		do {
			result = poll(&ready, 1U, 10000);
		} while (result < 0 && errno == EINTR);
		assert(result == 1);
		assert(SSL_read_ex(ssl, cursor, size, &received) == 1);
		assert(received != 0U && received <= size);
		cursor += received;
		size -= received;
	}
}

static void wait_tls_peer_closed(struct tls_peer *peer)
{
	struct pollfd ready = {.fd = peer->fd, .events = POLLIN | POLLHUP};
	int result;
	do {
		result = poll(&ready, 1U, 10000);
	} while (result < 0 && errno == EINTR);
	assert(result == 1);
	{
		uint8_t byte;
		size_t received = 0U;
		int ret =
		        SSL_read_ex(peer->ssl, &byte, sizeof(byte), &received);
		assert(ret != 1);
	}
}

static void send_raw_route(int fd,
                           const struct tr_pipeline_route_preface *route)
{
	uint8_t encoded[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	size_t written = 0U;
	assert(tr_pipeline_route_preface_encode(encoded, route) == TR_OK);
	while (written < sizeof(encoded)) {
		ssize_t ret = send(fd, encoded + written,
		                   sizeof(encoded) - written, MSG_NOSIGNAL);
		if (ret > 0) {
			written += (size_t)ret;
			continue;
		}
		assert(ret < 0 && errno == EINTR);
	}
}

static void send_tls_route(SSL *ssl,
                           const struct tr_pipeline_route_preface *route)
{
	uint8_t encoded[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	assert(tr_pipeline_route_preface_encode(encoded, route) == TR_OK);
	ssl_write_all(ssl, encoded, sizeof(encoded));
}

static struct tr_pipeline_route_preface control_route(void)
{
	return (struct tr_pipeline_route_preface){
	        .version = TR_PIPELINE_ROUTE_VERSION,
	        .role = TR_PIPELINE_ROUTE_CONTROL,
	        .owner_shard_id = 0U,
	        .pipeline_id = TEST_GROUP_ID,
	        .epoch = TEST_GROUP_EPOCH,
	        .member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL,
	        .member_generation = TEST_CONTROL_GENERATION,
	};
}

static void recv_control_offer(struct tls_peer *control,
                               uint64_t expected_message_id,
                               struct tr_pipeline_route_preface *route)
{
	struct tr_frame_header header;
	struct tr_wire_limits limits = {.max_payload_len =
	                                        TR_PIPELINE_CONTROL_WIRE_SIZE};
	struct tr_pipeline_control_wire_message message;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_PIPELINE_CONTROL_WIRE_SIZE];
	ssl_read_all_timeout(control->ssl, control->fd, raw_header,
	                     sizeof(raw_header));
	assert(tr_wire_header_decode(raw_header, &header) == TR_OK);
	assert(tr_wire_header_validate(raw_header, &header, &limits) == TR_OK);
	assert(header.type == TR_FRAME_PIPELINE_CONTROL);
	assert(header.message_id == expected_message_id);
	assert(header.payload_len == sizeof(payload));
	ssl_read_all_timeout(control->ssl, control->fd, payload,
	                     sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_pipeline_control_wire_decode(payload, sizeof(payload),
	                                       &message) == TR_OK);
	assert(message.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(tr_pipeline_control_wire_data_route(&message, route) == TR_OK);
}

static void send_data_frame(struct tls_peer *data)
{
	struct tr_frame_header header = {
	        .version = TR_WIRE_ENV_VERSION,
	        .type = TR_FRAME_DATA,
	        .flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        .stream_id = 31U,
	        .message_id = 81U,
	        .payload_len = sizeof(message_body) - 1U,
	        .payload_crc32c = 0U,
	};
	uint8_t encoded[TR_WIRE_HEADER_SIZE];
	header.payload_crc32c =
	        tr_crc32c(message_body, sizeof(message_body) - 1U);
	assert(tr_wire_header_encode(encoded, &header) == TR_OK);
	ssl_write_all(data->ssl, encoded, sizeof(encoded));
	ssl_write_all(data->ssl, message_body, sizeof(message_body) - 1U);
}

static void test_tls_group_routes_only_after_authentication(void)
{
	struct tls_test_identity identity;
	struct group_tls_state state;
	struct tr_server_config config;
	struct tr_server_config invalid_config;
	struct tr_server *rejected_server = NULL;
	struct tr_connection_group_server_stats stats;
	struct tr_pipeline_route_preface route;
	struct tls_peer control = {.fd = -1};
	struct tls_peer data = {.fd = -1};
	uint16_t port = 0U;
	int raw_fd = -1;

	memset(&state, 0, sizeof(state));
	assert(pthread_mutex_init(&state.lock, NULL) == 0);
	{
		pthread_condattr_t attr;
		assert(pthread_condattr_init(&attr) == 0);
		assert(pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0);
		assert(pthread_cond_init(&state.changed, &attr) == 0);
		assert(pthread_condattr_destroy(&attr) == 0);
	}
	assert(tls_test_identity_init(&identity, "pipeline-listener-tls") == 0);

	tr_server_config_init(&config);
	config.handshake_timeout_ms = 5000U;
	config.tls.mode = TR_TLS_REQUIRED;
	config.tls.certificate_file = identity.server_certificate_file;
	config.tls.private_key_file = identity.server_private_key_file;
	config.tls.client_ca_file = identity.ca_file;
	config.tls.client_auth = TR_TLS_CLIENT_AUTH_REQUIRED;
	config.connection_groups.max_groups = 1U;
	config.connection_groups.max_connections = 3U;
	config.connection_groups.max_data_connections_per_group = 2U;
	config.connection_groups.max_streams_per_group = 4U;
	config.connection_groups.authorize = authorize_group;
	config.connection_groups.on_message = on_group_message;
	config.connection_groups.on_data_event = on_group_data_event;
	config.connection_groups.callback_arg = &state;
	invalid_config = config;
	invalid_config.tls.client_auth = TR_TLS_CLIENT_AUTH_OPTIONAL;
	assert(tr_server_create_transport(&invalid_config, &rejected_server) ==
	       TR_ERR_INVALID);
	assert(rejected_server == NULL);
	assert(tr_server_create_transport(&config, &state.server) == TR_OK);
	assert(tr_server_connection_group_listen(state.server, "127.0.0.1", 0U,
	                                         16, &port) == TR_OK);
	assert(tr_server_start(state.server) == TR_OK);

	/* 明文 TRR1 不是可见的路由尝试；TLS 未认证前不触发授权或应用回调。 */
	raw_fd = connect_loopback(port);
	route = control_route();
	send_raw_route(raw_fd, &route);
	{
		uint8_t bytes[256];
		for (;;) {
			struct pollfd ready = {.fd = raw_fd,
			                       .events = POLLIN | POLLHUP};
			int polled;
			do {
				polled = poll(&ready, 1U, 10000);
			} while (polled < 0 && errno == EINTR);
			assert(polled == 1);
			ssize_t received =
			        recv(raw_fd, bytes, sizeof(bytes), 0);
			if (received == 0)
				break;
			if (received < 0) {
				assert(errno == ECONNRESET ||
				       errno == ENOTCONN);
				break;
			}
		}
	}
	close(raw_fd);
	raw_fd = -1;
	if (tls_peer_open(&control, port, &identity, NULL))
		wait_tls_peer_closed(&control);
	tls_peer_close(&control);
	assert(atomic_load(&state.control_authorizations) == 0U);
	assert(atomic_load(&state.data_authorizations) == 0U);
	assert(atomic_load(&state.messages) == 0U);

	/* A trusted CONTROL identity is captured from TLS, before TRR1
	 * authorization. */
	assert(tls_peer_open(&control, port, &identity,
	                     identity.client_certificate_file));
	route = control_route();
	send_tls_route(control.ssl, &route);
	wait_counter(&state, &state.control_authorizations, 1U);

	assert(tr_server_connection_group_send_data_offer(
	               state.server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               UINT64_C(9001)) == TR_OK);
	recv_control_offer(&control, UINT64_C(9001), &route);
	{
		struct tr_pipeline_route_preface invalid;
		for (unsigned variant = 0U; variant < 3U; ++variant) {
			invalid = route;
			if (variant == 0U)
				invalid.pipeline_id++;
			else if (variant == 1U)
				invalid.epoch++;
			else
				invalid.member_generation++;
			assert(tls_peer_open(&data, port, &identity,
			                     identity.client_certificate_file));
			send_tls_route(data.ssl, &invalid);
			wait_tls_peer_closed(&data);
			tls_peer_close(&data);
			assert(atomic_load(&state.control_authorizations) ==
			       1U);
			assert(atomic_load(&state.data_authorizations) == 0U);
			assert(atomic_load(&state.messages) == 0U);
		}
	}

	/* Same CA is not enough: another valid URI SAN cannot join this
	 * CONTROL. */
	assert(tls_peer_open(&data, port, &identity,
	                     identity.client_alternate_uri_certificate_file));
	send_tls_route(data.ssl, &route);
	wait_tls_peer_closed(&data);
	tls_peer_close(&data);
	assert(atomic_load(&state.data_authorizations) == 0U);
	assert(atomic_load(&state.messages) == 0U);

	/* The failed DATA attempts do not consume another member's route or
	 * CONTROL. */
	assert(tr_server_connection_group_send_data_offer(
	               state.server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	               UINT64_C(9002)) == TR_OK);
	recv_control_offer(&control, UINT64_C(9002), &route);
	assert(tls_peer_open(&data, port, &identity,
	                     identity.client_certificate_file));
	send_tls_route(data.ssl, &route);
	wait_counter(&state, &state.data_authorizations, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               state.server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 31U,
	               UINT64_C(9101)) == TR_OK);
	send_data_frame(&data);
	wait_counter(&state, &state.messages, 1U);

	memset(&stats, 0, sizeof(stats));
	assert(tr_server_connection_group_get_stats(state.server, &stats) ==
	       TR_OK);
	assert(stats.control_accepts == 1U);
	assert(stats.data_accepts == 1U);
	assert(stats.route_rejections >= 4U);

	tls_peer_close(&data);
	tls_peer_close(&control);
	assert(tr_server_connection_group_stop(state.server) == TR_OK);
	assert(tr_server_destroy(state.server) == TR_OK);
	tls_test_identity_cleanup(&identity);
	pthread_cond_destroy(&state.changed);
	pthread_mutex_destroy(&state.lock);
}

int main(void)
{
	test_reactor_preface_defer_pauses_and_resumes();
	test_reactor_preface_resume_rejects_reused_generation();
	test_tls_group_routes_only_after_authentication();
	return 0;
}
