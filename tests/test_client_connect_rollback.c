#define _GNU_SOURCE
#include "../src/client_record_internal.h"
#include "../src/memory_budget.h"
#include "tr/client.h"
#include "../src/io/socket.h"
#include "../src/transport/channel/channel.h"
#include "tr/status.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static atomic_int fail_record_create_once;
static atomic_int fail_channel_destroy_once;
static atomic_uint record_create_attempts;
static atomic_uint channel_destroy_attempts;

int __real_tr_rpc_record_client_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_method_desc *methods,
        size_t method_count, const struct tr_rpc_record_client_config *config,
        struct tr_memory_budget *budget, struct tr_rpc_record_client **out);
int __real_tr_channel_destroy(struct tr_channel *channel);

int __wrap_tr_rpc_record_client_create_on_owner(
        struct tr_channel *channel, const struct tr_rpc_method_desc *methods,
        size_t method_count, const struct tr_rpc_record_client_config *config,
        struct tr_memory_budget *budget, struct tr_rpc_record_client **out)
{
	atomic_fetch_add(&record_create_attempts, 1U);
	if (atomic_exchange(&fail_record_create_once, 0)) {
		if (out)
			*out = NULL;
		return TR_ERR_BAD_LENGTH;
	}
	return __real_tr_rpc_record_client_create_on_owner(
	        channel, methods, method_count, config, budget, out);
}

int __wrap_tr_channel_destroy(struct tr_channel *channel)
{
	atomic_fetch_add(&channel_destroy_attempts, 1U);
	if (atomic_exchange(&fail_channel_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_channel_destroy(channel);
}

static void reset_faults(void)
{
	atomic_store(&fail_record_create_once, 0);
	atomic_store(&fail_channel_destroy_once, 0);
	atomic_store(&record_create_attempts, 0U);
	atomic_store(&channel_destroy_attempts, 0U);
}

static void create_listener(int *fd_out, uint16_t *port_out)
{
	assert(tr_tcp_listen_ipv4(
		       "127.0.0.1", 0U, 8, fd_out, port_out) == TR_OK);
	assert(*fd_out >= 0);
	assert(*port_out != 0U);
}

static struct tr_client *create_client(void)
{
	struct tr_client_config config;
	struct tr_rpc_method_desc method = {0};
	struct tr_client *client = NULL;

	tr_client_config_init(&config);
	config.keepalive_interval_ms = 0U;
	config.enable_reconnect = 0;
	method.service_id = 1U;
	method.method_id = 1U;
	method.request_cardinality = TR_RPC_ONE;
	method.response_cardinality = TR_RPC_ONE;
	method.request_codec_id = TR_RPC_CODEC_RAW;
	method.response_codec_id = TR_RPC_CODEC_RAW;
	method.lane = TR_LANE_CONTROL;
	method.max_request_bytes = 1U;
	method.max_response_bytes = 1U;
	assert(tr_client_create(&config, &method, 1U, &client) == TR_OK);
	assert(client != NULL);
	return client;
}

static void test_successful_rollback_restores_connect_state(void)
{
	struct tr_client *client = create_client();
	uint16_t port = 0U;
	int listener = -1;

	create_listener(&listener, &port);
	reset_faults();

	/* RPC 发布前注入失败。第一次连接的显式回滚必须完整
	 * 清掉连接与 Channel，使第二次连接不会被残留会话拒绝。
	 */
	atomic_store(&fail_record_create_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) ==
	       TR_ERR_BAD_LENGTH);
	assert(atomic_load(&record_create_attempts) == 1U);
	assert(atomic_load(&channel_destroy_attempts) == 1U);

	atomic_store(&fail_record_create_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) ==
	       TR_ERR_BAD_LENGTH);
	assert(atomic_load(&record_create_attempts) == 2U);
	assert(atomic_load(&channel_destroy_attempts) == 2U);

	assert(tr_client_destroy(client) == TR_OK);
	tr_socket_close(&listener);
}

static void test_rollback_failure_wins_and_destroy_can_retry(void)
{
	struct tr_client *client = create_client();
	uint16_t port = 0U;
	int listener = -1;

	create_listener(&listener, &port);
	reset_faults();

	/* 原始 RPC 创建错误是 BAD_LENGTH，再让 Channel 销毁回滚
	 * 失败一次。连接必须返回回滚的 TR_ERR_SYS，而不是把对象伪装
	 * 成已经恢复到已断开状态。
	 */
	atomic_store(&fail_record_create_once, 1);
	atomic_store(&fail_channel_destroy_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_ERR_SYS);
	assert(atomic_load(&record_create_attempts) == 1U);
	assert(atomic_load(&channel_destroy_attempts) == 1U);
	assert(atomic_load(&fail_channel_destroy_once) == 0);

	/* 部分会话仍归 Client 所有，禁止开始新的连接轮次。 */
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_ERR_STATE);

	/* 故障已消耗，终止销毁必须从保留的 Channel 所有权继续收敛，
	 * 而不是因为前一次连接返回错误就丢失该对象。
	 */
	assert(tr_client_destroy(client) == TR_OK);
	assert(atomic_load(&channel_destroy_attempts) == 2U);
	tr_socket_close(&listener);
}

int main(void)
{
	test_successful_rollback_restores_connect_state();
	test_rollback_failure_wins_and_destroy_can_retry();
	return 0;
}
