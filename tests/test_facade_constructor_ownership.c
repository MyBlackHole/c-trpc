#define _GNU_SOURCE
#include "tr/client.h"
#include "tr/server.h"
#include "tr/status.h"

#include "../src/execution/buffer_internal.h"
#include "../src/memory_budget.h"
#include "../src/runtime/runtime_internal.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>

static atomic_int force_runtime_start_error;
static atomic_int fail_runtime_stop_once;
static atomic_int fail_runtime_destroy_once;
static atomic_int fail_server_rpc_pool_once;

static atomic_uint runtime_start_attempts;
static atomic_uint runtime_stop_attempts;
static atomic_uint runtime_destroy_attempts;
static atomic_uint server_pool_attempts;

int __real_tr_runtime_start(struct tr_runtime *runtime);
int __real_tr_runtime_stop(struct tr_runtime *runtime);
int __real_tr_runtime_destroy(struct tr_runtime *runtime);
int __real_tr_buffer_pool_init_dynamic_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t max_buffer_size, struct tr_memory_budget *budget);

int __wrap_tr_runtime_start(struct tr_runtime *runtime)
{
	int ret;

	atomic_fetch_add(&runtime_start_attempts, 1U);
	ret = __real_tr_runtime_start(runtime);
	if (ret == TR_OK && atomic_exchange(&force_runtime_start_error, 0))
		return TR_ERR_BAD_LENGTH;
	return ret;
}

int __wrap_tr_runtime_stop(struct tr_runtime *runtime)
{
	atomic_fetch_add(&runtime_stop_attempts, 1U);
	if (atomic_exchange(&fail_runtime_stop_once, 0))
		return TR_ERR_SYS;
	return __real_tr_runtime_stop(runtime);
}

int __wrap_tr_runtime_destroy(struct tr_runtime *runtime)
{
	atomic_fetch_add(&runtime_destroy_attempts, 1U);
	if (atomic_exchange(&fail_runtime_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_runtime_destroy(runtime);
}

int __wrap_tr_buffer_pool_init_dynamic_budgeted(
	struct tr_buffer_pool *pool, uint32_t buffer_count,
	uint32_t max_buffer_size, struct tr_memory_budget *budget)
{
	atomic_fetch_add(&server_pool_attempts, 1U);
	if (atomic_exchange(&fail_server_rpc_pool_once, 0))
		return TR_ERR_NOMEM;
	return __real_tr_buffer_pool_init_dynamic_budgeted(
		pool, buffer_count, max_buffer_size, budget);
}

static void reset_faults(void)
{
	atomic_store(&force_runtime_start_error, 0);
	atomic_store(&fail_runtime_stop_once, 0);
	atomic_store(&fail_runtime_destroy_once, 0);
	atomic_store(&fail_server_rpc_pool_once, 0);
	atomic_store(&runtime_start_attempts, 0U);
	atomic_store(&runtime_stop_attempts, 0U);
	atomic_store(&runtime_destroy_attempts, 0U);
	atomic_store(&server_pool_attempts, 0U);
}

static void small_server_config(struct tr_server_config *config)
{
	tr_server_config_init(config);
	config->shard_count = 1U;
	config->max_peers = 1U;
	config->listen_backlog = 4;
	config->limits.max_streams = 4U;
	config->limits.max_methods = 2U;
	config->limits.max_calls = 4U;
	config->limits.max_frame_payload_bytes = 1024U;
	config->limits.max_message_bytes = 4096U;
	config->limits.initial_window_bytes = 4096U;
	config->limits.window_update_threshold_bytes = 1024U;
}

static void test_client_normal_constructor_rollback_clears_out(void)
{
	struct tr_client *client = NULL;

	reset_faults();
	/*
	 * real runtime_start 已经成功建立 Reactor epoch，然后人为把 create 后续结果
	 * 改成失败。rollback 正常收敛时仍保持传统 create 契约：out == NULL。
	 */
	atomic_store(&force_runtime_start_error, 1);
	assert(tr_client_create(NULL, &client) == TR_ERR_BAD_LENGTH);
	assert(client == NULL);
	assert(atomic_load(&runtime_start_attempts) == 1U);
	assert(atomic_load(&runtime_stop_attempts) >= 1U);
}

static void test_client_rollback_failure_keeps_owner(void)
{
	struct tr_client *client = NULL;

	reset_faults();
	atomic_store(&force_runtime_start_error, 1);
	atomic_store(&fail_runtime_stop_once, 1);

	/*
	 * 原始 constructor cause 是 BAD_LENGTH，但 terminal rollback 的 stop barrier
	 * 再失败一次。生命周期错误必须优先，并把 partial Client 留在 out 中。
	 */
	assert(tr_client_create(NULL, &client) == TR_ERR_SYS);
	assert(client != NULL);
	assert(atomic_load(&runtime_start_attempts) == 1U);
	assert(atomic_load(&runtime_stop_attempts) == 1U);

	/* fault 已消费，caller 使用保留 ownership 继续 destroy 即可完成收敛。 */
	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;
	assert(atomic_load(&runtime_stop_attempts) >= 2U);
	assert(atomic_load(&runtime_destroy_attempts) == 1U);
}

static void test_server_normal_constructor_rollback_clears_out(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;

	small_server_config(&config);
	reset_faults();
	atomic_store(&fail_server_rpc_pool_once, 1);

	assert(tr_server_create(&config, &server) == TR_ERR_NOMEM);
	assert(server == NULL);
	assert(atomic_load(&server_pool_attempts) == 1U);
	assert(atomic_load(&runtime_destroy_attempts) == 1U);
}

static void test_server_rollback_failure_keeps_owner(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;

	small_server_config(&config);
	reset_faults();
	atomic_store(&fail_server_rpc_pool_once, 1);
	atomic_store(&fail_runtime_destroy_once, 1);

	/*
	 * pool init 是原始 constructor failure；随后 Runtime terminal destroy
	 * 再失败一次。Server storage 不得由 void cleanup 丢失，而必须留在 out。
	 */
	assert(tr_server_create(&config, &server) == TR_ERR_SYS);
	assert(server != NULL);
	assert(atomic_load(&server_pool_attempts) == 1U);
	assert(atomic_load(&runtime_destroy_attempts) == 1U);

	assert(tr_server_destroy(server) == TR_OK);
	server = NULL;
	assert(atomic_load(&runtime_destroy_attempts) == 2U);
}

int main(void)
{
	test_client_normal_constructor_rollback_clears_out();
	test_client_rollback_failure_keeps_owner();
	test_server_normal_constructor_rollback_clears_out();
	test_server_rollback_failure_keeps_owner();
	return 0;
}
