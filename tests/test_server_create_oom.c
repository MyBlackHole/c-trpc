#define _GNU_SOURCE
#include "tr/server.h"
#include "tr/status.h"

#include "../src/facade_diagnostics_internal.h"
#include "../src/runtime/runtime_internal.h"
#include "server_v2_test_fixture.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct allocation_probe {
	int enabled;
	int runtime_ready;
	size_t allocations;
	size_t fail_at;
	unsigned failures;
	unsigned destroy_attempts;
	unsigned destroy_failures;
	int fail_destroy_once;
};

/* 仅当前构造线程注入失败，不影响后续启动的反应器或工作线程。 */
static _Thread_local struct allocation_probe probe;

void *__real_calloc(size_t count, size_t size);
void *__real_malloc(size_t size);
int __real_tr_runtime_create(const struct tr_runtime_config *config,
			     struct tr_runtime **out);
int __real_tr_runtime_destroy(struct tr_runtime *runtime);

static int allocation_fails(void)
{
	if (!probe.enabled || !probe.runtime_ready)
		return 0;

	probe.allocations++;
	if (probe.fail_at == 0U || probe.allocations != probe.fail_at)
		return 0;

	probe.failures++;
	errno = ENOMEM;
	return 1;
}

void *__wrap_calloc(size_t count, size_t size)
{
	if (allocation_fails())
		return NULL;
	return __real_calloc(count, size);
}

void *__wrap_malloc(size_t size)
{
	if (allocation_fails())
		return NULL;
	return __real_malloc(size);
}

int __wrap_tr_runtime_create(const struct tr_runtime_config *config,
			     struct tr_runtime **out)
{
	int ret = __real_tr_runtime_create(config, out);

	/* 返回后的第一处分配就是服务端分片数组，不依赖私有结构体大小。 */
	if (probe.enabled && ret == TR_OK)
		probe.runtime_ready = 1;
	return ret;
}

int __wrap_tr_runtime_destroy(struct tr_runtime *runtime)
{
	if (probe.enabled && probe.runtime_ready) {
		probe.destroy_attempts++;
		if (probe.fail_destroy_once && probe.failures != 0U) {
			probe.fail_destroy_once = 0;
			probe.destroy_failures++;
			return TR_ERR_SYS;
		}
	}
	return __real_tr_runtime_destroy(runtime);
}

static unsigned open_fd_count(void)
{
	struct dirent *entry;
	DIR *directory;
	unsigned count = 0U;

	assert(!probe.enabled);
	directory = opendir("/proc/self/fd");
	assert(directory != NULL);
	while ((entry = readdir(directory)) != NULL)
		if (strcmp(entry->d_name, ".") != 0 &&
		    strcmp(entry->d_name, "..") != 0)
			count++;
	assert(closedir(directory) == 0);
	return count;
}

static void config_init(struct tr_server_config *config, uint32_t shards)
{
	tr_server_config_init(config);
	config->shard_count = shards;
	config->max_peers = shards;
	config->listen_backlog = (int)shards;
	config->keepalive_interval_ms = 0U;
	config->limits.max_streams = 4U;
	config->limits.max_methods = 2U;
	config->limits.max_calls = 4U;
	config->limits.max_frame_payload_bytes = 1024U;
	config->limits.max_message_bytes = 4096U;
	config->limits.initial_window_bytes = 4096U;
	config->limits.window_update_threshold_bytes = 1024U;
}

static void probe_begin(size_t fail_at, int fail_destroy)
{
	memset(&probe, 0, sizeof(probe));
	probe.fail_at = fail_at;
	probe.fail_destroy_once = fail_destroy;
	probe.enabled = 1;
}

static void expect_no_fd_change(unsigned count, int sentinel)
{
	assert(!probe.enabled);
	assert(fcntl(sentinel, F_GETFD) >= 0);
	assert(open_fd_count() == count);
}

static int destroy_server_until_complete(struct tr_server *server)
{
	unsigned retries = 0U;
	int ret;

	do {
		ret = tr_server_destroy(server);
		if (ret == TR_AGAIN) {
			if (++retries >= 10000U)
				return TR_ERR_TIMEOUT;
			sched_yield();
		}
	} while (ret == TR_AGAIN);
	return ret;
}

static void test_failure(uint32_t shards, size_t fail_at,
			 int fail_destroy, int sentinel)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	unsigned fds = open_fd_count();
	int ret;

	config_init(&config, shards);
	printf("Server OOM: shards=%u allocation=%zu rollback_fault=%d\n",
	       shards, fail_at, fail_destroy);
	fflush(stdout);
	probe_begin(fail_at, fail_destroy);
	ret = tr_test_server_v2_create(&config, &server);
	assert(probe.runtime_ready);
	assert(probe.failures == 1U);
	assert(probe.destroy_attempts == 1U);

	if (fail_destroy) {
		struct tr_server_stats stats;
		struct tr_rpc_semantic_stats semantic;

		assert(ret == TR_ERR_SYS);
		assert(server != NULL);
		assert(probe.destroy_failures == 1U);
		/* 构造回滚失败后只能继续销毁；不能把部分 Server 当成
		 * 已发布分片集合来读取 RPC 或内存池状态。
		 */
		assert(tr_server_get_stats(server, &stats) == TR_ERR_STATE);
		assert(tr_server_get_rpc_semantic_stats(server, &semantic) ==
		       TR_ERR_STATE);
		assert(destroy_server_until_complete(server) == TR_OK);
		assert(probe.destroy_attempts == 2U);
	} else {
		assert(ret == TR_ERR_NOMEM);
		assert(server == NULL);
	}
	probe.enabled = 0;
	expect_no_fd_change(fds, sentinel);
}

static size_t test_success(uint32_t shards, int sentinel)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	unsigned fds = open_fd_count();
	size_t allocations;
	uint16_t port = 0U;

	config_init(&config, shards);
	probe_begin(0U, 0);
	assert(tr_test_server_v2_create(&config, &server) == TR_OK);
	assert(server != NULL);
	assert(probe.runtime_ready);
	assert(probe.failures == 0U);
	allocations = probe.allocations;
	probe.enabled = 0;

	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);
	assert(destroy_server_until_complete(server) == TR_OK);
	expect_no_fd_change(fds, sentinel);
	return allocations;
}

int main(void)
{
	static const uint32_t shard_counts[] = { 1U, 3U };
	int sentinel = open("/dev/null", O_RDONLY | O_CLOEXEC);
	size_t i;

	assert(sentinel >= 0);
	/* 旧代码必须首先在这里失败，日志明确对应分片数组分配，而非其他故障。 */
	test_failure(3U, 1U, 0, sentinel);

	for (i = 0U; i < sizeof(shard_counts) / sizeof(shard_counts[0]); ++i) {
		uint32_t shards = shard_counts[i];
		size_t allocations = test_success(shards, sentinel);
		size_t n;

		/* 数组与每个分片至少一处分配；防止注入未覆盖目标却误通过。 */
		assert(allocations > (size_t)shards);
		for (n = 1U; n <= allocations; ++n) {
			test_failure(shards, n, 0, sentinel);
			test_failure(shards, n, 1, sentinel);
		}
		printf("Server OOM: shards=%u, %zu allocation points checked\n",
		       shards, allocations);
	}
	assert(close(sentinel) == 0);
	puts("Server constructor OOM/retry: ok");
	return 0;
}
