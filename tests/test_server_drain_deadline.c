#define _GNU_SOURCE
#include "tr/server.h"
#include "tr/status.h"

#include "../src/execution/deadline_internal.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/transport/channel/channel_internal.h"

#include <arpa/inet.h>
#include <assert.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 测试链接期只替换 Server 到 Channel 的排空等待边界：
 * 使用真实反应器、Server 和两个已发布对端，保持 begin_drain 原样执行。
 * 针对未到期或已到期等状态注入确定性等待结果，不依赖实际睡满超时。
 */
enum drain_case {
	DRAIN_CASE_NONE,
	DRAIN_CASE_SUCCESS,
	DRAIN_CASE_PEER_AGAIN,
	DRAIN_CASE_GLOBAL_TIMEOUT,
	DRAIN_CASE_ZERO_CHECK,
	DRAIN_CASE_CLOCK_ERROR
};

static enum drain_case active_case;
static unsigned int begin_calls;
static unsigned int wait_calls;
static unsigned int deadline_inits;
static unsigned int deadline_checks;
static struct tr_deadline *shared_deadline;
static struct timespec shared_absolute;
static struct tr_channel *first_wait_channel;

int __real_tr_channel_begin_drain(struct tr_channel *channel);
int __real_tr_channel_wait_drained_deadline(
	struct tr_channel *channel, const struct tr_deadline *deadline);
int __real_tr_deadline_init_ms(struct tr_deadline *deadline,
			       uint32_t timeout_ms);
int __real_tr_deadline_expired(const struct tr_deadline *deadline,
			       int *expired);

int __wrap_tr_channel_begin_drain(struct tr_channel *channel)
{
	if (active_case != DRAIN_CASE_NONE)
		begin_calls++;
	return __real_tr_channel_begin_drain(channel);
}

int __wrap_tr_deadline_init_ms(struct tr_deadline *deadline,
			      uint32_t timeout_ms)
{
	int ret;

	if (active_case == DRAIN_CASE_NONE)
		return __real_tr_deadline_init_ms(deadline, timeout_ms);

	/* 期限只在所有对端发布 GOAWAY 屏障后构造一次。 */
	assert(begin_calls == 2U);
	assert(deadline_inits == 0U);
	assert(timeout_ms == 30000U);
	ret = __real_tr_deadline_init_ms(deadline, timeout_ms);
	assert(ret == TR_OK);
	deadline_inits++;
	shared_deadline = deadline;
	shared_absolute = deadline->absolute;
	assert(deadline->infinite == 0);
	return ret;
}

int __wrap_tr_deadline_expired(const struct tr_deadline *deadline,
			      int *expired)
{
	if (active_case == DRAIN_CASE_NONE)
		return __real_tr_deadline_expired(deadline, expired);

	assert(deadline == shared_deadline);
	assert(expired != NULL);
	assert(deadline->absolute.tv_sec == shared_absolute.tv_sec);
	assert(deadline->absolute.tv_nsec == shared_absolute.tv_nsec);
	deadline_checks++;

	/* 第一个 Channel 等待返回后，模拟总期限恰好耗尽。 */
	if (active_case == DRAIN_CASE_GLOBAL_TIMEOUT && wait_calls == 1U) {
		*expired = 1;
		return TR_OK;
	}
	/* 时钟读失败必须显式传给 Server，而非当成 elapsed=0。 */
	if (active_case == DRAIN_CASE_CLOCK_ERROR)
		return TR_ERR_SYS;
	return __real_tr_deadline_expired(deadline, expired);
}

int __wrap_tr_channel_wait_drained_deadline(
	struct tr_channel *channel, const struct tr_deadline *deadline)
{
	if (active_case == DRAIN_CASE_NONE)
		return __real_tr_channel_wait_drained_deadline(channel,
							     deadline);

	assert(channel != NULL);
	assert(begin_calls == 2U);
	assert(wait_calls < 2U);
	if (wait_calls == 0U)
		first_wait_channel = channel;
	else
		assert(first_wait_channel != channel);

	if (active_case == DRAIN_CASE_ZERO_CHECK) {
		assert(deadline == NULL);
		assert(deadline_inits == 0U);
	} else {
		assert(deadline == shared_deadline);
		assert(deadline != NULL && !deadline->infinite);
		assert(deadline->absolute.tv_sec == shared_absolute.tv_sec);
		assert(deadline->absolute.tv_nsec == shared_absolute.tv_nsec);
	}
	wait_calls++;

	switch (active_case) {
	case DRAIN_CASE_SUCCESS:
		/* 真实 Channel，无流时立即完成。 */
		return __real_tr_channel_wait_drained_deadline(channel, deadline);
	case DRAIN_CASE_PEER_AGAIN:
	case DRAIN_CASE_GLOBAL_TIMEOUT:
		/* 模拟尚未耗尽总期限的单个 Channel 超时。 */
		return TR_AGAIN;
	case DRAIN_CASE_ZERO_CHECK:
		/* 验证一次真实 0ms 状态检查及聚合 TR_AGAIN 路径。 */
		if (wait_calls == 1U)
			return __real_tr_channel_wait_drained_deadline(channel,
								       NULL);
		return TR_AGAIN;
	case DRAIN_CASE_CLOCK_ERROR:
	case DRAIN_CASE_NONE:
		break;
	}
	assert(!"unexpected drain wait");
	return TR_ERR_STATE;
}

static void small_server_config(struct tr_server_config *config)
{
	tr_server_config_init(config);
	config->shard_count = 1U;
	config->max_peers = 2U;
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

static void wait_two_peers(struct tr_server *server)
{
	struct timespec retry = { .tv_sec = 0, .tv_nsec = 1000000L };
	unsigned int i;

	for (i = 0U; i < 5000U; ++i) {
		struct tr_server_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_get_stats(server, &stats) == TR_OK);
		if (stats.peers_current == 2U)
			return;
		(void)nanosleep(&retry, NULL);
	}
	assert(!"two Server peers were not published");
}

static void reset_probe(enum drain_case test_case)
{
	active_case = test_case;
	begin_calls = 0U;
	wait_calls = 0U;
	deadline_inits = 0U;
	deadline_checks = 0U;
	shared_deadline = NULL;
	first_wait_channel = NULL;
	memset(&shared_absolute, 0, sizeof(shared_absolute));
}

static void run_case(enum drain_case test_case, uint32_t timeout_ms,
		     int expected_ret, unsigned int expected_waits,
		     unsigned int expected_checks)
{
	struct tr_server_config config;
	struct tr_server *server = NULL;
	uint16_t port = 0U;
	int peers[2];
	int ret;

	small_server_config(&config);
	assert(tr_server_create(&config, &server) == TR_OK);
	assert(server != NULL);
	assert(tr_server_listen(server, "127.0.0.1", 0U, &port) == TR_OK);
	assert(port != 0U);
	assert(tr_server_start(server) == TR_OK);
	peers[0] = connect_loopback(port);
	peers[1] = connect_loopback(port);
	wait_two_peers(server);

	reset_probe(test_case);
	ret = tr_server_drain(server, timeout_ms);
	active_case = DRAIN_CASE_NONE;

	assert(ret == expected_ret);
	assert(begin_calls == 2U);
	assert(wait_calls == expected_waits);
	assert(deadline_checks == expected_checks);
	assert(deadline_inits == (timeout_ms != 0U ? 1U : 0U));

	/* 拆除必须保留真实 Server/Channel 所有权，不能由测试跳过。 */
	assert(tr_server_destroy(server) == TR_OK);
	assert(close(peers[0]) == 0);
	assert(close(peers[1]) == 0);
}

int main(void)
{
	run_case(DRAIN_CASE_SUCCESS, 30000U, TR_OK, 2U, 2U);
	run_case(DRAIN_CASE_PEER_AGAIN, 30000U, TR_AGAIN, 2U, 2U);
	run_case(DRAIN_CASE_GLOBAL_TIMEOUT, 30000U, TR_ERR_TIMEOUT,
		 1U, 2U);
	run_case(DRAIN_CASE_ZERO_CHECK, 0U, TR_AGAIN, 2U, 0U);
	run_case(DRAIN_CASE_CLOCK_ERROR, 30000U, TR_ERR_SYS, 0U, 1U);
	puts("server shared drain deadline: ok");
	return 0;
}
