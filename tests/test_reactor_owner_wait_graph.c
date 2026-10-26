#define _GNU_SOURCE
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/io/tls_openssl.h"
#include "tr/status.h"
#include "tls_test_identity.h"

#include <assert.h>
#include <fcntl.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

struct reactor_fixture {
	struct tr_reactor *reactors[3];
	unsigned count;
	int peer_fd;
	struct tr_conn_handle target_connection;
};

static struct tr_reactor *create_started_reactor(void)
{
	struct tr_reactor_config config;
	struct tr_reactor *reactor = NULL;

	memset(&config, 0, sizeof(config));
	config.command_capacity = 8U;
	config.max_connections = 4U;
	config.rx_buffer_count = 8U;
	config.rx_buffer_size = 1024U;
	config.max_payload_len = 1024U;
	config.tx_item_capacity = 8U;
	config.control_tx_item_capacity = 4U;
	assert(tr_reactor_create(&config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(reactor != NULL);
	assert(tr_reactor_start(reactor) == TR_OK);
	return reactor;
}

static void fixture_start(struct reactor_fixture *fixture, unsigned count,
                          int with_target_connection)
{
	int sockets[2];
	unsigned i;

	memset(fixture, 0, sizeof(*fixture));
	fixture->peer_fd = -1;
	assert(count >= 2U && count <= 3U);
	fixture->count = count;
	for (i = 0; i < count; ++i)
		fixture->reactors[i] = create_started_reactor();

	if (with_target_connection) {
		assert(socketpair(AF_UNIX,
		                  SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
		                  sockets) == 0);
		fixture->peer_fd = sockets[1];
		assert(tr_reactor_adopt_fd(fixture->reactors[1], sockets[0],
		                           &fixture->target_connection) ==
		       TR_OK);
	}
}

static void fixture_stop_destroy(struct reactor_fixture *fixture)
{
	unsigned i;

	for (i = fixture->count; i > 0U; --i) {
		assert(tr_reactor_stop(fixture->reactors[i - 1U]) == TR_OK);
		assert(tr_reactor_destroy(fixture->reactors[i - 1U]) == TR_OK);
	}
	if (fixture->peer_fd >= 0)
		assert(close(fixture->peer_fd) == 0);
}

static int no_op(void *arg)
{
	(void)arg;
	return TR_OK;
}

struct call_chain {
	struct tr_reactor *reactors[3];
	int reverse_result;
};

static int call_a_from_b(void *arg)
{
	struct call_chain *chain = arg;

	chain->reverse_result =
	        tr_reactor_call(chain->reactors[0], no_op, NULL);
	return TR_OK;
}

static int call_b_leaf(void *arg)
{
	struct call_chain *chain = arg;

	chain->reverse_result = TR_OK;
	return TR_OK;
}

static int acyclic_call_b_from_a(void *arg)
{
	struct call_chain *chain = arg;

	return tr_reactor_call(chain->reactors[1], call_b_leaf, chain);
}

static int call_a_from_c(void *arg)
{
	struct call_chain *chain = arg;

	chain->reverse_result =
	        tr_reactor_call(chain->reactors[0], no_op, NULL);
	return TR_OK;
}

static int call_c_from_b(void *arg)
{
	struct call_chain *chain = arg;

	return tr_reactor_call(chain->reactors[2], call_a_from_c, chain);
}

static int call_b_from_a(void *arg)
{
	struct call_chain *chain = arg;

	return tr_reactor_call(chain->reactors[1], call_a_from_b, chain);
}

static int call_b_then_c_from_a(void *arg)
{
	struct call_chain *chain = arg;

	return tr_reactor_call(chain->reactors[1], call_c_from_b, chain);
}

static void test_acyclic_owner_call(void)
{
	struct reactor_fixture fixture;
	struct call_chain chain;

	fixture_start(&fixture, 2U, 0);
	memset(&chain, 0, sizeof(chain));
	chain.reactors[0] = fixture.reactors[0];
	chain.reactors[1] = fixture.reactors[1];
	assert(tr_reactor_call(fixture.reactors[0], acyclic_call_b_from_a,
	                       &chain) == TR_OK);
	assert(chain.reverse_result == TR_OK);

	/* A completed wait must leave the source free to make another call. */
	assert(tr_reactor_call(fixture.reactors[0], no_op, NULL) == TR_OK);
	fixture_stop_destroy(&fixture);
}

static void test_two_reactor_cycle(void)
{
	struct reactor_fixture fixture;
	struct call_chain chain;

	fixture_start(&fixture, 2U, 0);
	memset(&chain, 0, sizeof(chain));
	chain.reactors[0] = fixture.reactors[0];
	chain.reactors[1] = fixture.reactors[1];
	assert(tr_reactor_call(fixture.reactors[0], call_b_from_a, &chain) ==
	       TR_OK);
	assert(chain.reverse_result == TR_ERR_STATE);

	/* The rejected edge is removed, so an acyclic call still succeeds. */
	assert(tr_reactor_call(fixture.reactors[0], no_op, NULL) == TR_OK);
	fixture_stop_destroy(&fixture);
}

static void test_three_reactor_cycle(void)
{
	struct reactor_fixture fixture;
	struct call_chain chain;

	fixture_start(&fixture, 3U, 0);
	memset(&chain, 0, sizeof(chain));
	chain.reactors[0] = fixture.reactors[0];
	chain.reactors[1] = fixture.reactors[1];
	chain.reactors[2] = fixture.reactors[2];
	assert(tr_reactor_call(fixture.reactors[0], call_b_then_c_from_a,
	                       &chain) == TR_OK);
	assert(chain.reverse_result == TR_ERR_STATE);
	fixture_stop_destroy(&fixture);
}

enum wait_operation {
	WAIT_QUIESCE,
	WAIT_SET_HANDLER,
	WAIT_GRACEFUL_CLOSE,
	WAIT_STOP
};

struct cycle_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned arrived;
};

struct api_cycle {
	struct reactor_fixture fixture;
	struct cycle_gate gate;
	enum wait_operation operation;
	int outer_result;
	int reverse_result;
	int remote_call_result;
};

static void cycle_gate_arrive(struct cycle_gate *gate)
{
	assert(pthread_mutex_lock(&gate->lock) == 0);
	gate->arrived++;
	assert(pthread_cond_broadcast(&gate->cond) == 0);
	while (gate->arrived < 2U)
		assert(pthread_cond_wait(&gate->cond, &gate->lock) == 0);
	assert(pthread_mutex_unlock(&gate->lock) == 0);
}

static int owner_wait_on_target(void *arg)
{
	struct api_cycle *cycle = arg;

	cycle_gate_arrive(&cycle->gate);
	switch (cycle->operation) {
	case WAIT_QUIESCE:
		cycle->outer_result =
		        tr_reactor_quiesce(cycle->fixture.reactors[1]);
		break;
	case WAIT_SET_HANDLER:
		cycle->outer_result = tr_reactor_set_handler(
		        cycle->fixture.target_connection, NULL, NULL, NULL);
		break;
	case WAIT_GRACEFUL_CLOSE:
		cycle->outer_result = tr_reactor_close_graceful(
		        cycle->fixture.target_connection);
		break;
	case WAIT_STOP:
		cycle->outer_result =
		        tr_reactor_stop(cycle->fixture.reactors[1]);
		break;
	default:
		assert(0);
	}
	return TR_OK;
}

static int owner_call_back_to_source(void *arg)
{
	struct api_cycle *cycle = arg;

	cycle_gate_arrive(&cycle->gate);
	cycle->reverse_result =
	        tr_reactor_call(cycle->fixture.reactors[0], no_op, NULL);
	return TR_OK;
}

static void *run_remote_cycle_callback(void *arg)
{
	struct api_cycle *cycle = arg;

	cycle->remote_call_result = tr_reactor_call(
	        cycle->fixture.reactors[1], owner_call_back_to_source, cycle);
	return NULL;
}

static void test_wait_api_cycle(enum wait_operation operation)
{
	struct api_cycle cycle;
	pthread_t remote;

	memset(&cycle, 0, sizeof(cycle));
	cycle.operation = operation;
	cycle.fixture.peer_fd = -1;
	fixture_start(&cycle.fixture, 2U, 1);
	assert(pthread_mutex_init(&cycle.gate.lock, NULL) == 0);
	assert(pthread_cond_init(&cycle.gate.cond, NULL) == 0);

	assert(pthread_create(&remote, NULL, run_remote_cycle_callback,
	                      &cycle) == 0);
	assert(tr_reactor_call(cycle.fixture.reactors[0], owner_wait_on_target,
	                       &cycle) == TR_OK);
	assert(pthread_join(remote, NULL) == 0);
	assert(cycle.remote_call_result == TR_OK);

	if (operation == WAIT_GRACEFUL_CLOSE) {
		if (cycle.outer_result == TR_ERR_STATE) {
			assert(cycle.reverse_result == TR_OK);
		} else {
			assert(cycle.outer_result == TR_ERR_UNSUPPORTED);
			assert(cycle.reverse_result == TR_ERR_STATE);
		}
	} else {
		assert((cycle.outer_result == TR_ERR_STATE &&
		        cycle.reverse_result == TR_OK) ||
		       (cycle.outer_result == TR_OK &&
		        cycle.reverse_result == TR_ERR_STATE));
	}

	fixture_stop_destroy(&cycle.fixture);
	assert(pthread_cond_destroy(&cycle.gate.cond) == 0);
	assert(pthread_mutex_destroy(&cycle.gate.lock) == 0);
}

struct destroy_wait {
	struct tr_reactor *source;
	struct tr_reactor *target;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	pthread_t source_owner;
	_Atomic int pause_after_signal;
	int waiter_paused;
	int release_waiter;
	int source_result;
};

static struct destroy_wait *active_destroy_wait;

int __real_pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *lock);

int __wrap_pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *lock)
{
	struct destroy_wait *wait = active_destroy_wait;
	int ret = __real_pthread_cond_wait(cond, lock);

	if (ret != 0 || !wait ||
	    !atomic_load_explicit(&wait->pause_after_signal,
	                          memory_order_acquire) ||
	    !pthread_equal(pthread_self(), wait->source_owner))
		return ret;

	assert(pthread_mutex_lock(&wait->lock) == 0);
	if (!wait->waiter_paused) {
		wait->waiter_paused = 1;
		assert(pthread_cond_broadcast(&wait->cond) == 0);
	}
	while (!wait->release_waiter)
		assert(__real_pthread_cond_wait(&wait->cond, &wait->lock) == 0);
	assert(pthread_mutex_unlock(&wait->lock) == 0);
	return ret;
}

static int owner_call_target_and_pause_wait(void *arg)
{
	struct destroy_wait *wait = arg;

	wait->source_owner = pthread_self();
	return tr_reactor_call(wait->target, no_op, NULL);
}

static void *call_source_reactor(void *arg)
{
	struct destroy_wait *wait = arg;

	wait->source_result = tr_reactor_call(
	        wait->source, owner_call_target_and_pause_wait, wait);
	return NULL;
}

static void test_destroy_waits_for_inbound_scope(void)
{
	struct reactor_fixture fixture;
	struct destroy_wait wait;
	pthread_t source_thread;
	int destroy_result;

	memset(&wait, 0, sizeof(wait));
	fixture_start(&fixture, 2U, 0);
	wait.source = fixture.reactors[0];
	wait.target = fixture.reactors[1];
	assert(pthread_mutex_init(&wait.lock, NULL) == 0);
	assert(pthread_cond_init(&wait.cond, NULL) == 0);
	atomic_init(&wait.pause_after_signal, 1);
	active_destroy_wait = &wait;

	assert(pthread_create(&source_thread, NULL, call_source_reactor,
	                      &wait) == 0);
	assert(pthread_mutex_lock(&wait.lock) == 0);
	while (!wait.waiter_paused)
		assert(pthread_cond_wait(&wait.cond, &wait.lock) == 0);
	assert(pthread_mutex_unlock(&wait.lock) == 0);

	/* The target owner is stopped, but the source has not released its
	 * edge. */
	destroy_result = tr_reactor_destroy(wait.target);
	assert(destroy_result == TR_AGAIN);
	assert(tr_reactor_start(wait.target) == TR_ERR_STATE);

	assert(pthread_mutex_lock(&wait.lock) == 0);
	wait.release_waiter = 1;
	assert(pthread_cond_broadcast(&wait.cond) == 0);
	assert(pthread_mutex_unlock(&wait.lock) == 0);
	assert(pthread_join(source_thread, NULL) == 0);
	assert(wait.source_result == TR_OK);

	active_destroy_wait = NULL;
	assert(tr_reactor_destroy(wait.target) == TR_OK);
	assert(tr_reactor_stop(wait.source) == TR_OK);
	assert(tr_reactor_destroy(wait.source) == TR_OK);
	assert(pthread_cond_destroy(&wait.cond) == 0);
	assert(pthread_mutex_destroy(&wait.lock) == 0);
}

struct tls_wait_cycle {
	struct reactor_fixture fixture;
	struct tls_test_identity identity;
	struct tr_tls_openssl_profile *server_profile;
	SSL_CTX *client_context;
	SSL *client_session;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int peer_received_close_notify;
	int allow_peer_close_notify;
	int peer_result;
	int reverse_result;
	int close_result;
};

struct tls_adopt_request {
	struct tr_reactor *reactor;
	struct tr_tls_openssl_profile *profile;
	int fd;
	uint64_t deadline_ns;
	struct tr_conn_handle connection;
};

static uint64_t tls_handshake_deadline_ns(void)
{
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec + UINT64_C(10000000000);
}

static int adopt_tls_on_owner(void *arg)
{
	struct tls_adopt_request *request = arg;

	return tr_reactor_adopt_tls_fd_on_owner(
	        request->reactor, request->fd, request->profile,
	        request->deadline_ns, &request->connection);
}

static int query_tls_authentication_on_owner(void *arg)
{
	struct tls_adopt_request *request = arg;
	struct tr_peer_identity_snapshot *snapshot = NULL;
	int ret = tr_reactor_peer_identity_snapshot_on_owner(
	        request->connection, &snapshot);

	assert(snapshot == NULL);
	return ret;
}

static int graceful_close_on_owner(void *arg)
{
	struct tls_wait_cycle *cycle = arg;

	return tr_reactor_close_graceful(cycle->fixture.target_connection);
}

static int call_back_during_tls_close(void *arg)
{
	struct tls_wait_cycle *cycle = arg;

	cycle->reverse_result =
	        tr_reactor_call(cycle->fixture.reactors[0], no_op, NULL);
	return TR_OK;
}

static void *call_graceful_close(void *arg)
{
	struct tls_wait_cycle *cycle = arg;

	cycle->close_result = tr_reactor_call(cycle->fixture.reactors[0],
	                                      graceful_close_on_owner, cycle);
	return NULL;
}

static void *tls_peer_wait_for_close(void *arg)
{
	struct tls_wait_cycle *cycle = arg;
	uint8_t byte;
	int ret = SSL_read(cycle->client_session, &byte, sizeof(byte));

	assert(ret <= 0);
	assert(SSL_get_error(cycle->client_session, ret) ==
	       SSL_ERROR_ZERO_RETURN);
	assert(pthread_mutex_lock(&cycle->lock) == 0);
	cycle->peer_received_close_notify = 1;
	assert(pthread_cond_broadcast(&cycle->cond) == 0);
	while (!cycle->allow_peer_close_notify)
		assert(pthread_cond_wait(&cycle->cond, &cycle->lock) == 0);
	assert(pthread_mutex_unlock(&cycle->lock) == 0);

	ret = SSL_shutdown(cycle->client_session);
	if (ret == 0)
		ret = SSL_shutdown(cycle->client_session);
	assert(ret == 1);
	cycle->peer_result = TR_OK;
	return NULL;
}

static void test_graceful_close_cycle_through_tls_completion(void)
{
	struct tls_wait_cycle cycle;
	struct tr_tls_server_config server_config;
	struct tls_adopt_request request;
	pthread_t close_thread;
	pthread_t peer_thread;
	int sockets[2];
	int flags;
	unsigned attempt;
	int ret;

	memset(&cycle, 0, sizeof(cycle));
	cycle.fixture.peer_fd = -1;
	fixture_start(&cycle.fixture, 2U, 0);
	assert(pthread_mutex_init(&cycle.lock, NULL) == 0);
	assert(pthread_cond_init(&cycle.cond, NULL) == 0);
	assert(tls_test_identity_init(&cycle.identity, "wait-graph-tls") == 0);

	memset(&server_config, 0, sizeof(server_config));
	server_config.mode = TR_TLS_REQUIRED;
	server_config.certificate_file = cycle.identity.server_certificate_file;
	server_config.private_key_file = cycle.identity.server_private_key_file;
	server_config.client_auth = TR_TLS_CLIENT_AUTH_NONE;
	assert(tr_tls_openssl_server_profile_create(
	               &server_config, &cycle.server_profile) == TR_OK);
	assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) ==
	       0);
	cycle.fixture.peer_fd = sockets[1];
	flags = fcntl(sockets[0], F_GETFL, 0);
	assert(flags >= 0);
	assert(fcntl(sockets[0], F_SETFL, flags | O_NONBLOCK) == 0);

	memset(&request, 0, sizeof(request));
	request.reactor = cycle.fixture.reactors[1];
	request.profile = cycle.server_profile;
	request.fd = sockets[0];
	request.deadline_ns = tls_handshake_deadline_ns();
	assert(tr_reactor_call(request.reactor, adopt_tls_on_owner, &request) ==
	       TR_OK);
	cycle.fixture.target_connection = request.connection;

	cycle.client_context = SSL_CTX_new(TLS_client_method());
	assert(cycle.client_context != NULL);
	assert(SSL_CTX_set_min_proto_version(cycle.client_context,
	                                     TLS1_3_VERSION) == 1);
	assert(SSL_CTX_set_max_proto_version(cycle.client_context,
	                                     TLS1_3_VERSION) == 1);
	SSL_CTX_set_verify(cycle.client_context, SSL_VERIFY_NONE, NULL);
	cycle.client_session = SSL_new(cycle.client_context);
	assert(cycle.client_session != NULL);
	assert(SSL_set_fd(cycle.client_session, cycle.fixture.peer_fd) == 1);
	assert(SSL_connect(cycle.client_session) == 1);

	for (attempt = 0U; attempt < 128U; ++attempt) {
		ret = tr_reactor_call(request.reactor,
		                      query_tls_authentication_on_owner,
		                      &request);
		if (ret != TR_AGAIN)
			break;
		assert(tr_reactor_quiesce(request.reactor) == TR_OK);
	}
	assert(ret == TR_OK);

	assert(pthread_create(&peer_thread, NULL, tls_peer_wait_for_close,
	                      &cycle) == 0);
	assert(pthread_create(&close_thread, NULL, call_graceful_close,
	                      &cycle) == 0);
	assert(pthread_mutex_lock(&cycle.lock) == 0);
	while (!cycle.peer_received_close_notify)
		assert(pthread_cond_wait(&cycle.cond, &cycle.lock) == 0);
	assert(pthread_mutex_unlock(&cycle.lock) == 0);

	/* A 已在等待 TLS close_notify 完成；B 的反向同步调用必须在入队前拒绝。
	 */
	assert(tr_reactor_call(cycle.fixture.reactors[1],
	                       call_back_during_tls_close, &cycle) == TR_OK);
	assert(cycle.reverse_result == TR_ERR_STATE);
	assert(pthread_mutex_lock(&cycle.lock) == 0);
	cycle.allow_peer_close_notify = 1;
	assert(pthread_cond_broadcast(&cycle.cond) == 0);
	assert(pthread_mutex_unlock(&cycle.lock) == 0);
	assert(pthread_join(peer_thread, NULL) == 0);
	assert(cycle.peer_result == TR_OK);
	assert(pthread_join(close_thread, NULL) == 0);
	assert(cycle.close_result == TR_OK);

	SSL_free(cycle.client_session);
	SSL_CTX_free(cycle.client_context);
	tr_tls_openssl_profile_release(cycle.server_profile);
	fixture_stop_destroy(&cycle.fixture);
	tls_test_identity_cleanup(&cycle.identity);
	assert(pthread_cond_destroy(&cycle.cond) == 0);
	assert(pthread_mutex_destroy(&cycle.lock) == 0);
}

static void run_with_watchdog(void (*test)(void), const char *name)
{
	pid_t child = fork();
	int status;

	assert(child >= 0);
	if (child == 0) {
		(void)signal(SIGALRM, SIG_DFL);
		alarm(8U);
		test();
		alarm(0U);
		exit(0);
	}
	assert(waitpid(child, &status, 0) == child);
	if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
		fprintf(stderr, "wait graph case failed: %s, status=%d\n", name,
		        status);
		exit(1);
	}
}

static void run_quiesce_cycle(void)
{
	test_wait_api_cycle(WAIT_QUIESCE);
}

static void run_set_handler_cycle(void)
{
	test_wait_api_cycle(WAIT_SET_HANDLER);
}

static void run_graceful_close_cycle(void)
{
	test_wait_api_cycle(WAIT_GRACEFUL_CLOSE);
}

static void run_stop_cycle(void)
{
	test_wait_api_cycle(WAIT_STOP);
}

int main(void)
{
	run_with_watchdog(test_acyclic_owner_call, "acyclic A->B");
	run_with_watchdog(test_two_reactor_cycle, "cycle A->B->A");
	run_with_watchdog(test_three_reactor_cycle, "cycle A->B->C->A");
	run_with_watchdog(run_quiesce_cycle, "quiesce cycle");
	run_with_watchdog(run_set_handler_cycle, "set-handler cycle");
	run_with_watchdog(run_graceful_close_cycle, "graceful-close cycle");
	run_with_watchdog(run_stop_cycle, "stop/join cycle");
	run_with_watchdog(test_graceful_close_cycle_through_tls_completion,
	                  "graceful-close TLS completion cycle");
	run_with_watchdog(test_destroy_waits_for_inbound_scope,
	                  "destroy with inbound waiter");
	puts("Reactor cross-owner wait graph: ok");
	return 0;
}
