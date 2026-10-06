#define _GNU_SOURCE
#include "tr/status.h"

#include "../src/execution/buffer.h"
#include "../src/execution/reactor.h"
#include "../src/io/connector_internal.h"
#include "../src/runtime/runtime_internal.h"
#include "../src/transport/group/client_group_internal.h"
#include "../src/transport/group/pipeline_listener_internal.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static atomic_uint reactor_create_attempts;
static atomic_uint reactor_destroy_attempts;
static atomic_uint connector_create_attempts;
static atomic_uint pool_destroy_attempts;

static atomic_uint fail_reactor_create_at;
static atomic_int fail_reactor_destroy_once;
static atomic_int fail_connector_create_once;
static atomic_int fail_pool_destroy_once;

int __real_tr_reactor_create(
	const struct tr_reactor_config *config,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg,
	struct tr_reactor **out);
int __real_tr_reactor_destroy(struct tr_reactor *reactor);
int __real_tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out);
int __real_tr_buffer_pool_destroy(struct tr_buffer_pool *pool);

int __wrap_tr_reactor_create(
	const struct tr_reactor_config *config,
	tr_reactor_frame_cb frame_cb,
	tr_reactor_event_cb event_cb, void *callback_arg,
	struct tr_reactor **out)
{
	unsigned attempt = atomic_fetch_add(&reactor_create_attempts, 1U) + 1U;

	if (attempt == atomic_load(&fail_reactor_create_at)) {
		if (out)
			*out = NULL;
		return TR_ERR_BAD_LENGTH;
	}
	return __real_tr_reactor_create(
		config, frame_cb, event_cb, callback_arg, out);
}

int __wrap_tr_reactor_destroy(struct tr_reactor *reactor)
{
	atomic_fetch_add(&reactor_destroy_attempts, 1U);
	if (atomic_exchange(&fail_reactor_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_reactor_destroy(reactor);
}

int __wrap_tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out)
{
	atomic_fetch_add(&connector_create_attempts, 1U);
	if (atomic_exchange(&fail_connector_create_once, 0)) {
		if (out)
			*out = NULL;
		return TR_ERR_BAD_LENGTH;
	}
	return __real_tr_connector_create(config, out);
}

int __wrap_tr_buffer_pool_destroy(struct tr_buffer_pool *pool)
{
	atomic_fetch_add(&pool_destroy_attempts, 1U);
	if (atomic_exchange(&fail_pool_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_buffer_pool_destroy(pool);
}

static void reset_faults(void)
{
	atomic_store(&reactor_create_attempts, 0U);
	atomic_store(&reactor_destroy_attempts, 0U);
	atomic_store(&connector_create_attempts, 0U);
	atomic_store(&pool_destroy_attempts, 0U);
	atomic_store(&fail_reactor_create_at, 0U);
	atomic_store(&fail_reactor_destroy_once, 0);
	atomic_store(&fail_connector_create_once, 0);
	atomic_store(&fail_pool_destroy_once, 0);
}

static void test_runtime_constructor_rollback_keeps_owner(void)
{
	struct tr_runtime_shard_config shards[2];
	struct tr_runtime_config config;
	struct tr_runtime *runtime = NULL;

	memset(shards, 0, sizeof(shards));
	memset(&config, 0, sizeof(config));
	config.shard_count = 2U;
	config.shards = shards;

	reset_faults();
	atomic_store(&fail_reactor_create_at, 2U);
	atomic_store(&fail_reactor_destroy_once, 1);

	/*
	 * shard0 Reactor 已构造，shard1 Reactor create 返回 BAD_LENGTH。
	 * constructor rollback 随后在销毁 shard0 Reactor 时再失败一次。
	 * lifecycle error 必须优先，同时通过 *out 保留 partial Runtime owner。
	 */
	assert(tr_runtime_create(&config, &runtime) == TR_ERR_SYS);
	assert(runtime != NULL);
	assert(atomic_load(&reactor_create_attempts) == 2U);
	assert(atomic_load(&reactor_destroy_attempts) == 1U);

	/* destroy fault 已消费，调用方继续使用保留 ownership 即可完成收敛。 */
	assert(tr_runtime_destroy(runtime) == TR_OK);
	runtime = NULL;
	assert(atomic_load(&reactor_destroy_attempts) == 2U);
}

static void noop_transfer_ready(
	const struct tr_connection_group_id *group,
	uint32_t stream_id, uint64_t message_id, void *arg)
{
	(void)group;
	(void)stream_id;
	(void)message_id;
	(void)arg;
}

static void test_client_group_constructor_rollback_keeps_owner(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_client_group_config config;
	struct tr_client_group *group = NULL;

	reset_faults();
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.max_data_connections = 1U;
	config.max_transfers = 2U;
	config.max_message_bytes = 4096U;
	config.max_frame_payload_bytes = 1024U;
	config.connect_timeout_ms = 100U;
	config.tcp_nodelay = 0;
	config.on_transfer_ready = noop_transfer_ready;

	/*
	 * connector create 是 Group constructor 最后一个可失败步骤。强制它失败，
	 * 随后再让 control pool rollback destroy 失败一次。
	 */
	atomic_store(&fail_connector_create_once, 1);
	atomic_store(&fail_pool_destroy_once, 1);
	assert(tr_client_group_create(&config, &group) == TR_ERR_SYS);
	assert(group != NULL);
	assert(atomic_load(&connector_create_attempts) == 1U);
	assert(atomic_load(&pool_destroy_attempts) == 1U);

	assert(tr_client_group_destroy(group) == TR_OK);
	group = NULL;
	assert(atomic_load(&pool_destroy_attempts) == 2U);

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static int authorize_control(
	const struct tr_pipeline_route_preface *route, void *arg)
{
	(void)route;
	(void)arg;
	return TR_OK;
}

static void test_pipeline_listener_destroy_is_retryable(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	uint16_t port = 0U;

	reset_faults();
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 2U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 2U;
	config.control_message_count = 2U;
	config.authorize_control = authorize_control;

	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(listener != NULL);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 4, &port) == TR_OK);
	assert(port != 0U);

	/*
	 * listener source 仍然发布时 destroy 是正常可恢复错误。debug/release 都必须
	 * 返回 TR_ERR_STATE，而不是 assert-abort。
	 */
	assert(tr_pipeline_listener_destroy(listener) == TR_ERR_STATE);
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	listener = NULL;

	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

int main(void)
{
	test_runtime_constructor_rollback_keeps_owner();
	test_client_group_constructor_rollback_keeps_owner();
	test_pipeline_listener_destroy_is_retryable();
	return 0;
}
