#define _GNU_SOURCE
#include "tr/client.h"
#include "../src/io/socket.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"
#include "tr/status.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static atomic_int fail_rpc_create_once;
static atomic_int fail_channel_destroy_once;
static atomic_uint rpc_create_attempts;
static atomic_uint channel_destroy_attempts;

int __real_tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel,
	const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group,
	struct tr_rpc_endpoint **out);
int __real_tr_channel_destroy(struct tr_channel *channel);

int __wrap_tr_rpc_endpoint_create_with_executor_group(
	struct tr_channel *channel,
	const struct tr_rpc_endpoint_config *config,
	struct tr_rpc_executor_group *group,
	struct tr_rpc_endpoint **out)
{
	atomic_fetch_add(&rpc_create_attempts, 1U);
	if (atomic_exchange(&fail_rpc_create_once, 0)) {
		if (out)
			*out = NULL;
		return TR_ERR_BAD_LENGTH;
	}
	return __real_tr_rpc_endpoint_create_with_executor_group(
		channel, config, group, out);
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
	atomic_store(&fail_rpc_create_once, 0);
	atomic_store(&fail_channel_destroy_once, 0);
	atomic_store(&rpc_create_attempts, 0U);
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
	struct tr_client *client = NULL;

	tr_client_config_init(&config);
	config.keepalive_interval_ms = 0U;
	config.enable_reconnect = 0;
	assert(tr_client_create(&config, &client) == TR_OK);
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

	/*
	 * RPC publication 前注入失败。第一次 connect 的显式 rollback 必须完整
	 * 清掉 connection + Channel，使第二次 connect 不会被残留 session 拒绝。
	 */
	atomic_store(&fail_rpc_create_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) ==
	       TR_ERR_BAD_LENGTH);
	assert(atomic_load(&rpc_create_attempts) == 1U);
	assert(atomic_load(&channel_destroy_attempts) == 1U);

	atomic_store(&fail_rpc_create_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) ==
	       TR_ERR_BAD_LENGTH);
	assert(atomic_load(&rpc_create_attempts) == 2U);
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

	/*
	 * 原始 RPC create 错误是 BAD_LENGTH，再让 Channel destroy rollback
	 * 失败一次。connect 必须返回 rollback 的 TR_ERR_SYS，而不是把对象伪装
	 * 成已经恢复到 disconnected 状态。
	 */
	atomic_store(&fail_rpc_create_once, 1);
	atomic_store(&fail_channel_destroy_once, 1);
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_ERR_SYS);
	assert(atomic_load(&rpc_create_attempts) == 1U);
	assert(atomic_load(&channel_destroy_attempts) == 1U);
	assert(atomic_load(&fail_channel_destroy_once) == 0);

	/* partial session 仍归 Client 所有，禁止开始新的 connect epoch。 */
	assert(tr_client_connect(client, "127.0.0.1", port) == TR_ERR_STATE);

	/*
	 * 故障已消费，terminal destroy 必须从保留的 Channel ownership 继续收敛，
	 * 而不是因为前一次 connect 返回错误就丢失该对象。
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
