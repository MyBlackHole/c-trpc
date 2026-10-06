#include "../src/execution/reactor.h"
#include "../src/io/connector_internal.h"
#include "../src/io/socket.h"
#include "tr/status.h"

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

struct connector_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_connector *connector;
	int callbacks;
	int status;
	int destroy_ret;
};

static void connector_destroy_from_completion(int status, int fd, void *arg)
{
	struct connector_test_ctx *ctx =
		(struct connector_test_ctx *)arg;
	int destroy_ret;

	tr_socket_close(&fd);
	destroy_ret = tr_connector_destroy(ctx->connector);

	pthread_mutex_lock(&ctx->lock);
	ctx->status = status;
	ctx->destroy_ret = destroy_ret;
	if (destroy_ret == TR_OK)
		ctx->connector = NULL;
	ctx->callbacks++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_connector_callback(struct connector_test_ctx *ctx)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;

	pthread_mutex_lock(&ctx->lock);
	while (ctx->callbacks == 0 && ret == 0)
		ret = pthread_cond_timedwait(
			&ctx->cond, &ctx->lock, &deadline);
	assert(ctx->callbacks == 1);
	pthread_mutex_unlock(&ctx->lock);
}

static void test_completion_may_destroy_connector(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct connector_test_ctx ctx;
	uint16_t port = 0U;
	int listener = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	assert(tr_tcp_listen_ipv4(
		       "127.0.0.1", 0U, 8, &listener, &port) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 5000U;
	connector_config.tcp_nodelay = 1;
	connector_config.complete_cb = connector_destroy_from_completion;
	connector_config.callback_arg = &ctx;
	assert(tr_connector_create(
		       &connector_config, &ctx.connector) == TR_OK);

	/*
	 * completion may run synchronously inside tr_connector_start() or later
	 * from EPOLLOUT. In both cases callback destroys Connector before the
	 * connector stack unwinds; no code may dereference it afterwards.
	 */
	assert(tr_connector_start(
		       ctx.connector, "127.0.0.1", port, NULL, 0U) == TR_OK);
	wait_connector_callback(&ctx);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.status == TR_OK);
	assert(ctx.destroy_ret == TR_OK);
	assert(ctx.connector == NULL);
	pthread_mutex_unlock(&ctx.lock);

	tr_socket_close(&listener);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void connector_noop_completion(int status, int fd, void *arg)
{
	(void)status;
	(void)arg;
	tr_socket_close(&fd);
}

static void test_inactive_cancel_and_destroy_are_idempotent(void)
{
	struct tr_reactor_config reactor_config;
	struct tr_connector_config connector_config;
	struct tr_reactor *reactor = NULL;
	struct tr_connector *connector = NULL;

	memset(&reactor_config, 0, sizeof(reactor_config));
	assert(tr_reactor_create(
		       &reactor_config, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&connector_config, 0, sizeof(connector_config));
	connector_config.owner = reactor;
	connector_config.timeout_ms = 1000U;
	connector_config.complete_cb = connector_noop_completion;
	assert(tr_connector_create(&connector_config, &connector) == TR_OK);

	assert(tr_connector_cancel(connector) == TR_OK);
	assert(tr_connector_cancel(connector) == TR_OK);
	assert(tr_connector_destroy(connector) == TR_OK);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

int main(void)
{
	test_completion_may_destroy_connector();
	test_inactive_cancel_and_destroy_are_idempotent();
	return 0;
}
