#define _GNU_SOURCE
#include "tr/server.h"
#include "tr/status.h"

#include "../src/facade_diagnostics_internal.h"
#include "../src/rpc/rpc_internal.h"
#include "../src/transport/channel/channel.h"

#include <arpa/inet.h>
#include <assert.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static atomic_int fail_rpc_destroy_once;
static atomic_int fail_channel_destroy_once;
static atomic_uint rpc_destroy_attempts;
static atomic_uint channel_destroy_attempts;

int __real_tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint);
int __real_tr_channel_destroy(struct tr_channel *channel);

int __wrap_tr_rpc_endpoint_destroy(struct tr_rpc_endpoint *endpoint)
{
	atomic_fetch_add(&rpc_destroy_attempts, 1U);
	if (atomic_exchange(&fail_rpc_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_rpc_endpoint_destroy(endpoint);
}

int __wrap_tr_channel_destroy(struct tr_channel *channel)
{
	atomic_fetch_add(&channel_destroy_attempts, 1U);
	if (atomic_exchange(&fail_channel_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_channel_destroy(channel);
}

static void small_server_config(struct tr_server_config *config)
{
	tr_server_config_init(config);
	config->shard_count = 1U;
	config->max_peers = 1U;
	config->listen_backlog = 4;
	config->keepalive_interval_ms = 0U;
	config->limits.max_streams = 4U;
	config->limits.max_methods = 2U;
	config->limits.max_calls = 4U;
	config->limits.max_frame_payload_bytes = 1024U;
	config->limits.max_message_bytes = 4096U;
	config->limits.initial_window_bytes = 4096U;
	config->limits.window_update_threshold_bytes = 1024U;
}

static int connect_loopback(uint16_t port)
{
	struct sockaddr_in address;
	int fd;

	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);

	memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	return fd;
}

static void wait_for_peer(struct tr_server *server)
{
	unsigned i;

	for (i = 0; i < 10000U; ++i) {
		struct tr_server_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 1U)
			return;
		sched_yield();
	}
	assert(!"server peer was not published");
}

int main(void)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	uint16_t port = 0U;
	int client_fd;

	small_server_config(&config);
	atomic_store(&fail_rpc_destroy_once, 0);
	atomic_store(&fail_channel_destroy_once, 0);
	atomic_store(&rpc_destroy_attempts, 0U);
	atomic_store(&channel_destroy_attempts, 0U);

	assert(tr_server_create(&config, &server) == TR_OK);
	assert(server != NULL);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	client_fd = connect_loopback(port);
	wait_for_peer(server);

	/*
	 * 第一次 terminal destroy 在 RPC barrier 处失败。旧实现 debug build 会
	 * assert-abort；新实现必须返回错误并保留 Server/peer ownership。
	 */
	atomic_store(&fail_rpc_destroy_once, 1);
	assert(tr_server_destroy(server) == TR_ERR_SYS);
	assert(atomic_load(&rpc_destroy_attempts) == 1U);
	assert(atomic_load(&channel_destroy_attempts) == 0U);

	/*
	 * 第二次重试让 RPC 正常收敛，再在 Channel destroy 处失败。
	 * peer->rpc 已清空，但 peer->channel 与 Server storage 必须继续保留。
	 */
	atomic_store(&fail_channel_destroy_once, 1);
	assert(tr_server_destroy(server) == TR_ERR_SYS);
	assert(atomic_load(&rpc_destroy_attempts) == 2U);
	assert(atomic_load(&channel_destroy_attempts) == 1U);

	/* 第三次从剩余 Channel ownership 继续，最终完整释放 Server。 */
	assert(tr_server_destroy(server) == TR_OK);
	server = NULL;
	assert(atomic_load(&rpc_destroy_attempts) == 2U);
	assert(atomic_load(&channel_destroy_attempts) == 2U);

	assert(close(client_fd) == 0);
	return 0;
}
