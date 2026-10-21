#define _GNU_SOURCE
#include "tr/rpc_v2.h"
#include "tr/server.h"
#include "tr/status.h"

#include "../src/facade_diagnostics_internal.h"
#include "../src/transport/channel/channel.h"

#include <arpa/inet.h>
#include <assert.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static atomic_int fail_channel_destroy_once;
static atomic_uint channel_destroy_attempts;

int __real_tr_channel_destroy(struct tr_channel *channel);

int __wrap_tr_channel_destroy(struct tr_channel *channel)
{
	atomic_fetch_add(&channel_destroy_attempts, 1U);
	if (atomic_exchange(&fail_channel_destroy_once, 0))
		return TR_ERR_SYS;
	return __real_tr_channel_destroy(channel);
}

static enum tr_rpc_status unused_work(struct tr_rpc_v2_call_id call,
                                      const struct tr_rpc_v2_request *request,
                                      const struct tr_rpc_v2_cancel *cancel,
                                      struct tr_rpc_v2_reply *reply, void *arg)
{
	(void)call;
	(void)request;
	(void)cancel;
	(void)reply;
	(void)arg;
	return TR_RPC_STATUS_OK;
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
	struct tr_rpc_v2_method method;
	struct tr_server *server = NULL;
	struct tr_server_stats stats;
	struct tr_rpc_semantic_stats semantic;
	uint16_t port = 0U;
	int client_fd;

	small_server_config(&config);
	memset(&method, 0, sizeof(method));
	method.descriptor.service_id = 1U;
	method.descriptor.method_id = 1U;
	method.descriptor.request_cardinality = TR_RPC_ONE;
	method.descriptor.response_cardinality = TR_RPC_ONE;
	method.descriptor.request_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.response_codec_id = TR_RPC_CODEC_RAW;
	method.descriptor.lane = TR_RPC_LANE_CONTROL;
	method.descriptor.max_request_bytes = 16U;
	method.descriptor.max_response_bytes = 16U;
	method.work = unused_work;
	atomic_store(&fail_channel_destroy_once, 0);
	atomic_store(&channel_destroy_attempts, 0U);

	assert(tr_server_create(&config, &method, 1U, &server) == TR_OK);
	assert(server != NULL);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);

	client_fd = connect_loopback(port);
	wait_for_peer(server);

	/* 第一次终止销毁在 Channel 销毁处失败；Server 必须保留
	 * Channel 与自身存储的所有权，供调用方重试。
	 */
	atomic_store(&fail_channel_destroy_once, 1);
	int destroy_ret;
	unsigned destroy_retries = 0U;
	do {
		destroy_ret = tr_server_destroy(server);
		if (destroy_ret == TR_AGAIN) {
			assert(atomic_load(&channel_destroy_attempts) == 0U);
			assert(++destroy_retries < 10000U);
			sched_yield();
		}
	} while (destroy_ret == TR_AGAIN);
	assert(destroy_ret == TR_ERR_SYS);
	assert(atomic_load(&channel_destroy_attempts) == 1U);
	/* 进入终止销毁后，即使屏障失败并保留对象也不得读取
	 * 已可能拆除部分事件源的跨分片统计。
	 */
	assert(tr_server_get_stats(server, &stats) == TR_ERR_STATE);
	assert(tr_server_get_rpc_semantic_stats(server, &semantic) ==
	       TR_ERR_STATE);

	/* 第二次从仍由 Server 持有的 Channel 继续，最终释放 Server。 */
	assert(tr_server_destroy(server) == TR_OK);
	server = NULL;
	assert(atomic_load(&channel_destroy_attempts) == 2U);

	assert(close(client_fd) == 0);
	return 0;
}
