#define _GNU_SOURCE
#include "tr/trpc.h"

#include "server_v2_test_fixture.h"
#include "../src/execution/deadline_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/facade_diagnostics_internal.h"
#include "../src/endian.h"
#include "../src/crc32c.h"
#include "../src/transport/group/pipeline_ingress_internal.h"
#include "../src/transport/channel/channel.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* 只在测试链接期捕获真实所有者定时器；人工推进仍在原反应器上执行。 */
static pthread_mutex_t probe_lock = PTHREAD_MUTEX_INITIALIZER;
static tr_reactor_timer_cb probe_callback;
static void *probe_arg;
static pthread_t probe_owner;
static struct tr_reactor_timer_handle probe_timer;
static uint64_t probe_deadline;
static atomic_int capture_timer;
static atomic_int fail_clock;
static atomic_int fail_arm;
static atomic_int fail_register;
static atomic_int fail_unregister;
static atomic_uint clock_failures;
static atomic_uint arm_failures;
static atomic_uint register_failures;
static atomic_uint unregister_failures;
static atomic_uint timeout_aborts;
static atomic_uint handshake_inits;
static atomic_uint observed_timeout;
static atomic_uint authorized;
static atomic_uint_fast64_t expired_clock;
static atomic_int fail_expiry_clock;
static atomic_int fail_abort;
static atomic_int expire_after_authorize;
static atomic_int expire_after_data_attach;
static _Thread_local int local_clock_failure;
static _Thread_local uint64_t local_clock_override;

int __real_clock_gettime(clockid_t id, struct timespec *out);
int __real_tr_deadline_init_ms(struct tr_deadline *deadline, uint32_t timeout);
int __real_tr_deadline_expired(const struct tr_deadline *deadline,
                               int *expired);
int __real_tr_reactor_timer_register(struct tr_reactor *reactor,
                                     tr_reactor_timer_cb callback, void *arg,
                                     struct tr_reactor_timer_handle *out);
int __real_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t ns);
int __real_tr_reactor_timer_unregister(struct tr_reactor_timer_handle timer);
int __real_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
                                     int status);

int __wrap_clock_gettime(clockid_t id, struct timespec *out)
{
	if (id == CLOCK_MONOTONIC && local_clock_failure) {
		local_clock_failure = 0;
		errno = EIO;
		return -1;
	}
	if (id == CLOCK_MONOTONIC && local_clock_override != 0U) {
		out->tv_sec =
		        (time_t)(local_clock_override / UINT64_C(1000000000));
		out->tv_nsec =
		        (long)(local_clock_override % UINT64_C(1000000000));
		return 0;
	}
	return __real_clock_gettime(id, out);
}

int __wrap_tr_deadline_init_ms(struct tr_deadline *deadline, uint32_t timeout)
{
	/* 当前夹具只有服务端接受连接时在所有者上建立这些期限。 */
	if (tr_reactor_in_owner_context() &&
	    (timeout == 10000U || timeout == 80U || timeout == 1000U)) {
		atomic_fetch_add(&handshake_inits, 1U);
		atomic_store(&observed_timeout, timeout);
		if (atomic_exchange(&fail_clock, 0)) {
			local_clock_failure = 1;
			atomic_fetch_add(&clock_failures, 1U);
		}
	}
	return __real_tr_deadline_init_ms(deadline, timeout);
}

int __wrap_tr_deadline_expired(const struct tr_deadline *deadline, int *expired)
{
	int on_owner;
	int ret;

	pthread_mutex_lock(&probe_lock);
	on_owner = probe_timer.reactor &&
	           pthread_equal(pthread_self(), probe_owner);
	pthread_mutex_unlock(&probe_lock);
	if (on_owner) {
		local_clock_override = atomic_load(&expired_clock);
		if (atomic_exchange(&fail_expiry_clock, 0))
			local_clock_failure = 1;
	}
	ret = __real_tr_deadline_expired(deadline, expired);
	local_clock_override = 0U;
	return ret;
}

static int is_probe_timer(struct tr_reactor_timer_handle timer)
{
	return timer.reactor == probe_timer.reactor &&
	       timer.slot == probe_timer.slot &&
	       timer.generation == probe_timer.generation;
}

int __wrap_tr_reactor_timer_register(struct tr_reactor *reactor,
                                     tr_reactor_timer_cb callback, void *arg,
                                     struct tr_reactor_timer_handle *out)
{
	int ret;

	if (tr_reactor_in_owner_context() && atomic_load(&capture_timer) &&
	    atomic_exchange(&fail_register, 0)) {
		atomic_fetch_add(&register_failures, 1U);
		return TR_ERR_SYS;
	}
	ret = __real_tr_reactor_timer_register(reactor, callback, arg, out);
	if (ret == TR_OK && tr_reactor_in_owner_context() &&
	    atomic_exchange(&capture_timer, 0)) {
		pthread_mutex_lock(&probe_lock);
		probe_owner = pthread_self();
		probe_callback = callback;
		probe_arg = arg;
		probe_timer = *out;
		pthread_mutex_unlock(&probe_lock);
	}
	return ret;
}

int __wrap_tr_reactor_timer_arm(struct tr_reactor_timer_handle timer,
                                uint64_t ns)
{
	int match;

	pthread_mutex_lock(&probe_lock);
	match = is_probe_timer(timer);
	if (match)
		probe_deadline = ns;
	pthread_mutex_unlock(&probe_lock);
	if (match && ns != 0U && atomic_exchange(&fail_arm, 0)) {
		atomic_fetch_add(&arm_failures, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_arm(timer, ns);
}

int __wrap_tr_reactor_timer_unregister(struct tr_reactor_timer_handle timer)
{
	int match;

	pthread_mutex_lock(&probe_lock);
	match = is_probe_timer(timer);
	pthread_mutex_unlock(&probe_lock);
	if (match && atomic_exchange(&fail_unregister, 0)) {
		atomic_fetch_add(&unregister_failures, 1U);
		return TR_ERR_SYS;
	}
	return __real_tr_reactor_timer_unregister(timer);
}

int __wrap_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
                                     int status)
{
	if (status == TR_ERR_TIMEOUT) {
		atomic_fetch_add(&timeout_aborts, 1U);
		if (atomic_exchange(&fail_abort, 0))
			return TR_ERR_SYS;
	}
	return __real_tr_reactor_abort_on_owner(connection, status);
}

int __real_tr_pipeline_ingress_attach_data_route_on_owner(
        const struct tr_pipeline_ingress_config *config,
        const struct tr_pipeline_route_preface *preface,
        struct tr_conn_handle connection);

int __wrap_tr_pipeline_ingress_attach_data_route_on_owner(
        const struct tr_pipeline_ingress_config *config,
        const struct tr_pipeline_route_preface *preface,
        struct tr_conn_handle connection)
{
	int ret = __real_tr_pipeline_ingress_attach_data_route_on_owner(
	        config, preface, connection);

	if (ret == TR_OK) {
		int mode = atomic_exchange(&expire_after_data_attach, 0);

		if (mode == 1) {
			pthread_mutex_lock(&probe_lock);
			atomic_store(&expired_clock, probe_deadline);
			pthread_mutex_unlock(&probe_lock);
		} else if (mode == 2) {
			atomic_store(&fail_expiry_clock, 1);
		}
	}
	return ret;
}

struct fixture {
	struct tr_server_config config;
	struct tr_server *server;
	uint16_t port;
	int group;
};

static int authorize_group(const struct tr_connection_group_id *group,
                           void *arg)
{
	(void)arg;
	assert(group->group_id == 42U && group->epoch == 1U);
	atomic_fetch_add(&authorized, 1U);
	{
		int mode = atomic_exchange(&expire_after_authorize, 0);

		if (mode == 1) {
			pthread_mutex_lock(&probe_lock);
			atomic_store(&expired_clock, probe_deadline);
			pthread_mutex_unlock(&probe_lock);
		} else if (mode == 2) {
			atomic_store(&fail_expiry_clock, 1);
		}
	}
	return TR_OK;
}

static void reset_probe(void)
{
	pthread_mutex_lock(&probe_lock);
	probe_callback = NULL;
	probe_arg = NULL;
	memset(&probe_timer, 0, sizeof(probe_timer));
	probe_deadline = 0U;
	pthread_mutex_unlock(&probe_lock);
	atomic_store(&capture_timer, 1);
	atomic_store(&expired_clock, 0U);
	atomic_store(&handshake_inits, 0U);
	atomic_store(&authorized, 0U);
}

static void fixture_init_capacity(struct fixture *f, int group,
                                  uint32_t timeout, uint32_t capacity,
                                  uint32_t shards)
{
	memset(f, 0, sizeof(*f));
	f->group = group;
	reset_probe();
	/* V2 Server start 注册重组任务的周期清理定时器；本用例要捕获
	 * 随后由入站 peer 注册的握手租约定时器。
	 */
	atomic_store(&capture_timer, 0);
	tr_server_config_init(&f->config);
	assert(f->config.handshake_timeout_ms == 10000U);
	f->config.handshake_timeout_ms = timeout;
	f->config.max_peers = capacity;
	f->config.shard_count = shards;
	f->config.keepalive_interval_ms = timeout == 80U ? 10U : 0U;
	f->config.keepalive_timeout_ms = 10U;
	f->config.limits.max_streams = 4U;
	f->config.limits.max_calls = 4U;
	f->config.limits.max_methods = 2U;
	f->config.limits.max_frame_payload_bytes = 1024U;
	f->config.limits.max_message_bytes = 4096U;
	if (group) {
		f->config.connection_groups.max_groups = 1U;
		f->config.connection_groups.max_connections = capacity;
		f->config.connection_groups.max_data_connections_per_group = 1U;
		f->config.connection_groups.max_streams_per_group = 4U;
		f->config.connection_groups.authorize = authorize_group;
	}
	if (group)
		assert(tr_server_create_transport(&f->config, &f->server) ==
		       TR_OK);
	else
		assert(tr_test_server_v2_create(&f->config, &f->server) ==
		       TR_OK);
	if (group)
		assert(tr_server_connection_group_listen(f->server, "127.0.0.1",
		                                         0U, 4,
		                                         &f->port) == TR_OK);
	else
		assert(tr_server_listen(f->server, "127.0.0.1", 0U, &f->port) ==
		       TR_OK);
	assert(tr_server_start(f->server) == TR_OK);
	reset_probe();
}

static void fixture_init(struct fixture *f, int group, uint32_t timeout)
{
	fixture_init_capacity(f, group, timeout, 1U, 1U);
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

static int raw_connect(uint16_t port)
{
	struct sockaddr_in address = {0};
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

	assert(fd >= 0);
	address.sin_family = AF_INET;
	address.sin_port = htons(port);
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	assert(connect(fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	return fd;
}

static uint64_t now_ns(void)
{
	struct timespec now;

	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}

static void wait_counts(struct fixture *f, uint32_t count, uint32_t ready)
{
	uint64_t end = now_ns() + UINT64_C(5000000000);

	for (;;) {
		if (f->group) {
			struct tr_pipeline_listener_stats stats;

			assert(tr_server_get_connection_group_stats_internal(
			               f->server, &stats) == TR_OK);
			if (stats.connections_current == count &&
			    stats.pipelines_current == ready)
				return;
		} else {
			struct tr_server_stats stats;

			assert(tr_server_get_stats(f->server, &stats) == TR_OK);
			if (stats.peers_current == count &&
			    stats.peers_ready_current == ready &&
			    (count != 0U || stats.peers_reaping_current == 0U))
				return;
		}
		assert(now_ns() < end);
		usleep(1000);
	}
}

static void wait_rx(struct fixture *f, uint64_t bytes)
{
	uint64_t end = now_ns() + UINT64_C(5000000000);

	for (;;) {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(f->server, &stats) == TR_OK);
		if (stats.reactor.total.rx_bytes >= bytes)
			return;
		assert(now_ns() < end);
		usleep(1000);
	}
}

static void wait_eof(int fd)
{
	uint64_t end = now_ns() + UINT64_C(5000000000);

	for (;;) {
		struct pollfd pollfd = {.fd = fd, .events = POLLIN | POLLHUP};
		uint8_t bytes[256];
		ssize_t n;

		assert(now_ns() < end);
		assert(poll(&pollfd, 1, 5000) == 1);
		n = recv(fd, bytes, sizeof(bytes), 0);
		if (n == 0 || (n < 0 && errno == ECONNRESET))
			return;
		assert(n > 0 || errno == EINTR);
	}
}

static uint64_t captured_deadline(void)
{
	uint64_t result;

	pthread_mutex_lock(&probe_lock);
	result = probe_deadline;
	pthread_mutex_unlock(&probe_lock);
	assert(result != 0U);
	return result;
}

struct tick_request {
	tr_reactor_timer_cb callback;
	void *arg;
	uint64_t now;
	uint64_t next;
};

static int tick_on_owner(void *arg)
{
	struct tick_request *request = arg;

	request->next = request->callback(request->arg, request->now);
	return TR_OK;
}

static uint64_t tick(uint64_t now)
{
	struct tick_request request;
	struct tr_reactor *reactor;

	pthread_mutex_lock(&probe_lock);
	request.callback = probe_callback;
	request.arg = probe_arg;
	reactor = probe_timer.reactor;
	pthread_mutex_unlock(&probe_lock);
	assert(request.callback && reactor);
	request.now = now;
	request.next = 0U;
	assert(tr_reactor_call(reactor, tick_on_owner, &request) == TR_OK);
	return request.next;
}

static void normal_client(struct fixture *f)
{
	struct tr_client_config config;
	struct tr_client *transport = NULL;
	struct tr_client *client = NULL;
	struct tr_connection_group_id group = {.group_id = 42U, .epoch = 1U};
	struct tr_rpc_method_desc method = {
	        .service_id = 1U,
	        .method_id = 1U,
	        .request_cardinality = TR_RPC_ONE,
	        .response_cardinality = TR_RPC_MANY,
	        .request_codec_id = TR_RPC_CODEC_RAW,
	        .response_codec_id = TR_RPC_CODEC_RAW,
	        .lane = TR_RPC_LANE_CONTROL,
	        .max_request_bytes = 1U,
	        .max_response_bytes = 1U};

	tr_client_config_init(&config);
	config.limits = f->config.limits;
	config.keepalive_interval_ms = 0U;
	if (f->group) {
		/* Group API remains transport-only; this fixture tests Server
		 * ingress. */
		assert(tr_client_create_transport(&config, &transport) ==
		       TR_OK);
		assert(tr_client_connection_group_connect(transport,
		                                          "127.0.0.1", f->port,
		                                          &group) == TR_OK);
	} else {
		assert(tr_client_create(&config, &method, 1U, &client) ==
		       TR_OK);
		assert(tr_client_connect(client, "127.0.0.1", f->port) ==
		       TR_OK);
		assert(tr_client_wait_ready(client, 5000U) == TR_OK);
	}
	wait_counts(f, 1U, 1U);
	if (!f->group) {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(f->server, &stats) == TR_OK);
		assert(stats.peers_ready_total == 1U);
	}
	/* 真正就绪后旧租约失效，迟到的定时器不得关闭新槽位或活动连接。 */
	assert(tick(UINT64_MAX) == 0U);
	wait_counts(f, 1U, 1U);
	if (transport)
		assert(tr_client_destroy(transport) == TR_OK);
	if (client)
		assert(tr_client_destroy(client) == TR_OK);
	wait_counts(f, 0U, 0U);
}

static void test_exact_deadline(int group, int partial)
{
	struct fixture f;
	uint64_t deadline;
	uint64_t next;
	unsigned before = atomic_load(&timeout_aborts);
	int fd;

	/* 旧代码的尾部零初始化仍获得默认有限租约。 */
	fixture_init(&f, group, 0U);
	fd = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	assert(atomic_load(&observed_timeout) == 10000U);
	deadline = captured_deadline();
	{
		int extra = raw_connect(f.port);

		/* 满槽拒绝不能重置原连接的期限，也不能偷占就绪计数。 */
		wait_eof(extra);
		assert(close(extra) == 0);
		wait_counts(&f, 1U, 0U);
		assert(captured_deadline() == deadline);
	}
	assert(tick(deadline - 1U) == deadline);
	if (partial) {
		unsigned i;
		const char prefix[] = "TR";

		for (i = 0; i < 2U; ++i) {
			assert(send(fd, &prefix[i], 1U, MSG_NOSIGNAL) == 1);
			wait_rx(&f, i + 1U);
			assert(captured_deadline() == deadline);
			assert(tick(deadline - 1U) == deadline);
		}
	}
	assert(atomic_load(&handshake_inits) == 1U);
	if (!group) {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(f.server, &stats) == TR_OK);
		assert(stats.peers_ready_total == 0U);
		assert(stats.channel.keepalive_timeouts == 0U);
	}
	assert(tick(deadline) == 0U);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(atomic_load(&timeout_aborts) == before + 1U);
	assert(close(fd) == 0);

	fd = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	next = captured_deadline();
	assert(next > deadline);
	assert(tick(deadline) == next);
	wait_counts(&f, 1U, 0U);
	assert(close(fd) == 0);
	wait_counts(&f, 0U, 0U);
	normal_client(&f);
	atomic_store(&fail_unregister, 1);
	assert(tr_server_destroy(f.server) == TR_ERR_SYS);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void test_faults(int group)
{
	struct fixture f;
	int fd;

	fixture_init(&f, group, 0U);
	atomic_store(&fail_clock, 1);
	fd = raw_connect(f.port);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(close(fd) == 0);
	atomic_store(&fail_register, 1);
	fd = raw_connect(f.port);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(close(fd) == 0);
	atomic_store(&fail_arm, 1);
	fd = raw_connect(f.port);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(close(fd) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void send_valid_handshake(int fd, int group)
{
	uint8_t bytes[TR_WIRE_HEADER_SIZE + 32U] = {0};
	size_t len;

	if (group) {
		struct tr_pipeline_route_preface route = {0};

		route.version = TR_PIPELINE_ROUTE_VERSION;
		route.role = TR_PIPELINE_ROUTE_CONTROL;
		route.pipeline_id = 42U;
		route.epoch = 1U;
		route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
		route.member_generation = 1U;
		assert(tr_pipeline_route_preface_encode(bytes, &route) ==
		       TR_OK);
		len = TR_PIPELINE_ROUTE_PREFACE_SIZE;
	} else {
		struct tr_frame_header header = {0};
		uint8_t *hello = bytes + TR_WIRE_HEADER_SIZE;

		tr_put_le16(hello, TR_CHANNEL_PROTOCOL_VERSION);
		tr_put_le16(hello + 2U, TR_CHANNEL_PROTOCOL_VERSION);
		tr_put_le32(hello + 4U, 3U);
		tr_put_le32(hello + 8U, 1024U);
		tr_put_le32(hello + 12U, 4096U);
		tr_put_le64(hello + 16U, TR_CHANNEL_FEATURE_STREAM_RESET);
		header.version = TR_WIRE_ENV_VERSION;
		header.type = TR_FRAME_HELLO;
		header.payload_len = 32U;
		header.payload_crc32c = tr_crc32c(hello, 32U);
		assert(tr_wire_header_encode(bytes, &header) == TR_OK);
		len = sizeof(bytes);
	}
	assert(send(fd, bytes, len, MSG_NOSIGNAL) == (ssize_t)len);
}

static void test_late_handshake(int group, int clock_error)
{
	struct fixture f;
	int fd;

	fixture_init(&f, group, 0U);
	fd = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	/* 只在完成期限判定时推进时间，让接收事件确定性先于到期定时器分发。 */
	if (clock_error)
		atomic_store(&fail_expiry_clock, 1);
	else
		atomic_store(&expired_clock, captured_deadline());
	send_valid_handshake(fd, group);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	if (group) {
		assert(atomic_load(&authorized) == 0U);
	} else {
		struct tr_server_stats stats;

		assert(tr_server_get_stats(f.server, &stats) == TR_OK);
		assert(stats.peers_ready_total == 0U);
	}
	atomic_store(&expired_clock, 0U);
	assert(atomic_load(&fail_expiry_clock) == 0);
	assert(close(fd) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void test_authorize_crosses_deadline(int mode)
{
	struct fixture f;
	struct tr_pipeline_listener_stats stats;
	int fd;

	fixture_init(&f, 1, 0U);
	fd = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	atomic_store(&expire_after_authorize, mode);
	send_valid_handshake(fd, 1);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(atomic_load(&authorized) == 1U);
	assert(atomic_load(&expire_after_authorize) == 0);
	assert(atomic_load(&fail_expiry_clock) == 0);
	assert(tr_server_get_connection_group_stats_internal(f.server,
	                                                     &stats) == TR_OK);
	assert(stats.control_accepts == 0U);
	assert(stats.route_rejections == 1U);
	atomic_store(&expired_clock, 0U);
	assert(close(fd) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void test_existing_timer_arm_failure(int group)
{
	struct fixture f;
	unsigned before = atomic_load(&arm_failures);
	uint64_t deadline;
	int first;
	int second;

	fixture_init_capacity(&f, group, 1000U, 2U, 1U);
	first = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	deadline = captured_deadline();
	atomic_store(&fail_arm, 1);
	second = raw_connect(f.port);
	wait_eof(second);
	assert(atomic_load(&arm_failures) == before + 1U);
	assert(captured_deadline() == deadline);
	/* 不人工推进：第二次设置定时失败后，第一条连接仍由原真实定时器唤醒。 */
	wait_eof(first);
	wait_counts(&f, 0U, 0U);
	assert(close(first) == 0);
	assert(close(second) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void test_shard_capacity(void)
{
	struct fixture f;
	struct tr_server_stats stats;
	int fds[16];
	unsigned i;

	fixture_init_capacity(&f, 0, 1000U, 2U, 2U);
	for (i = 0; i < 16U; ++i)
		fds[i] = raw_connect(f.port);
	/* 每分片容量为 1，多个四元组迫使至少一个分片出现容量拒绝。 */
	for (i = 0; i < 16U; ++i) {
		wait_eof(fds[i]);
		assert(close(fds[i]) == 0);
	}
	wait_counts(&f, 0U, 0U);
	assert(tr_server_get_stats(f.server, &stats) == TR_OK);
	assert(stats.max_peers == 2U);
	assert(stats.peers_peak <= 2U);
	assert(stats.peer_capacity_rejections != 0U);
	assert(stats.peers_ready_total == 0U);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static int authorize_route(const struct tr_pipeline_route_preface *route,
                           void *arg)
{
	(void)route;
	(void)arg;
	return TR_OK;
}

static void wait_listener_count(struct tr_pipeline_listener *listener,
                                uint32_t count)
{
	uint64_t end = now_ns() + UINT64_C(5000000000);

	for (;;) {
		struct tr_pipeline_listener_stats stats;

		assert(tr_pipeline_listener_get_stats(listener, &stats) ==
		       TR_OK);
		if (stats.connections_current == count)
			return;
		assert(now_ns() < end);
		usleep(1000);
	}
}

static void test_listener_restart(void)
{
	struct tr_pipeline_listener_config config = {0};
	struct tr_pipeline_listener *listener = NULL;
	struct tr_reactor *reactor = NULL;
	uint64_t deadline;
	uint64_t next;
	uint16_t port;
	int fd;

	reset_probe();
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	config.owner = reactor;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 1U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 1U;
	config.control_message_count = 1U;
	config.authorize_control = authorize_route;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(listener, "127.0.0.1", 0U, 4,
	                                        &port) == TR_OK);
	fd = raw_connect(port);
	wait_listener_count(listener, 1U);
	deadline = captured_deadline();
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	wait_eof(fd);
	assert(close(fd) == 0);
	assert(tick(deadline) == 0U);

	/* 停止或监听的新周期继续复用有界表，旧到期通知不得清除新连接。 */
	assert(tr_pipeline_listener_listen_ipv4(listener, "127.0.0.1", 0U, 4,
	                                        &port) == TR_OK);
	fd = raw_connect(port);
	wait_listener_count(listener, 1U);
	next = captured_deadline();
	assert(next > deadline);
	assert(tick(deadline) == next);
	wait_listener_count(listener, 1U);
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	wait_eof(fd);
	assert(close(fd) == 0);
	assert(tr_reactor_stop(reactor) == TR_OK);
	/* 已停止路径仍须保留失败的定时器解绑能力，允许再次销毁。 */
	atomic_store(&fail_unregister, 1);
	assert(tr_pipeline_listener_destroy(listener) == TR_ERR_SYS);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

struct data_handoff_probe {
	atomic_uint events;
	atomic_int last_status;
	atomic_uint last_generation;
};

static void
record_data_handoff_event(const struct tr_pipeline_route_preface *route,
                          struct tr_conn_handle connection,
                          enum tr_connection_event event, int status, void *arg)
{
	struct data_handoff_probe *probe = arg;

	(void)connection;
	(void)event;
	/* preface_release 必须保留 tracked，终止处理函数才能看到精确路由。 */
	assert(route->role == TR_PIPELINE_ROUTE_DATA);
	assert(route->pipeline_id == 42U && route->epoch == 1U);
	atomic_store(&probe->last_status, status);
	atomic_store(&probe->last_generation, route->member_generation);
	atomic_fetch_add(&probe->events, 1U);
}

static void test_data_handoff_crosses_deadline(int mode)
{
	struct tr_pipeline_listener_config config = {0};
	struct tr_pipeline_listener *listener = NULL;
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_stats stats;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface next_route;
	struct data_handoff_probe probe;
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];
	uint64_t end;
	uint16_t port;
	int control;
	int data;

	atomic_init(&probe.events, 0U);
	atomic_init(&probe.last_status, TR_OK);
	atomic_init(&probe.last_generation, 0U);
	reset_probe();
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	config.owner = reactor;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 2U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 1U;
	config.control_message_count = 2U;
	config.authorize_control = authorize_route;
	config.data_event_cb = record_data_handoff_event;
	config.data_callback_arg = &probe;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(listener, "127.0.0.1", 0U, 4,
	                                        &port) == TR_OK);
	control = raw_connect(port);
	send_valid_handshake(control, 1);
	end = now_ns() + UINT64_C(5000000000);
	do {
		assert(tr_pipeline_listener_get_stats(listener, &stats) ==
		       TR_OK);
		assert(now_ns() < end);
		if (stats.control_accepts != 1U)
			usleep(1000);
	} while (stats.control_accepts != 1U);
	assert(tr_pipeline_listener_send_data_offer(listener, 42U, 1U, 1U,
	                                            &route) == TR_OK);

	data = raw_connect(port);
	wait_listener_count(listener, 2U);
	atomic_store(&expire_after_data_attach, mode);
	assert(tr_pipeline_route_preface_encode(raw, &route) == TR_OK);
	assert(send(data, raw, sizeof(raw), MSG_NOSIGNAL) ==
	       (ssize_t)sizeof(raw));
	wait_eof(data);
	wait_listener_count(listener, 1U);
	assert(atomic_load(&expire_after_data_attach) == 0);
	assert(atomic_load(&fail_expiry_clock) == 0);
	assert(atomic_load(&probe.events) == 1U);
	assert(atomic_load(&probe.last_status) ==
	       (mode == 1 ? TR_ERR_TIMEOUT : TR_ERR_SYS));
	assert(atomic_load(&probe.last_generation) == route.member_generation);
	assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
	assert(stats.pipelines_current == 1U);
	assert(stats.control_accepts == 1U && stats.data_accepts == 0U);
	assert(stats.data_connections_current == 0U);
	assert(stats.route_rejections == 1U);
	assert(close(data) == 0);
	atomic_store(&expired_clock, 0U);

	/* 容量为 1：新提议成功证明失败交接的 ATTACHED 成员关系已退役。 */
	assert(tr_pipeline_listener_send_data_offer(listener, 42U, 1U, 2U,
	                                            &next_route) == TR_OK);
	assert(next_route.member_index == route.member_index);
	assert(next_route.member_generation != route.member_generation);
	data = raw_connect(port);
	assert(tr_pipeline_route_preface_encode(raw, &next_route) == TR_OK);
	assert(send(data, raw, sizeof(raw), MSG_NOSIGNAL) ==
	       (ssize_t)sizeof(raw));
	end = now_ns() + UINT64_C(5000000000);
	do {
		assert(tr_pipeline_listener_get_stats(listener, &stats) ==
		       TR_OK);
		assert(now_ns() < end);
		if (stats.data_accepts != 1U)
			usleep(1000);
	} while (stats.data_accepts != 1U);
	assert(stats.connections_current == 2U);
	assert(stats.data_connections_current == 1U);
	assert(tick(UINT64_MAX) == 0U);
	wait_listener_count(listener, 2U);
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	wait_eof(data);
	wait_eof(control);
	assert(atomic_load(&probe.events) == 2U);
	assert(close(data) == 0);
	assert(close(control) == 0);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static void test_timeout_abort_retry(int group)
{
	struct fixture f;
	uint64_t deadline;
	int fd;

	fixture_init(&f, group, 0U);
	fd = raw_connect(f.port);
	wait_counts(&f, 1U, 0U);
	deadline = captured_deadline();
	atomic_store(&fail_abort, 1);
	assert(tick(deadline) == deadline + 1U);
	wait_counts(&f, 1U, 0U);
	assert(tick(deadline + 1U) == 0U);
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(close(fd) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

static void test_real_timeout(int group)
{
	struct fixture f;
	unsigned before = atomic_load(&timeout_aborts);
	int fd;

	fixture_init(&f, group, 80U);
	fd = raw_connect(f.port);
	/* 原始 fd 保持打开，由真实反应器时钟与定时器独立回收。 */
	wait_eof(fd);
	wait_counts(&f, 0U, 0U);
	assert(atomic_load(&timeout_aborts) == before + 1U);
	assert(close(fd) == 0);
	normal_client(&f);
	assert(destroy_server_until_complete(f.server) == TR_OK);
}

int main(void)
{
	int group;

	for (group = 0; group < 2; ++group) {
		test_exact_deadline(group, 0);
		test_exact_deadline(group, 1);
		test_faults(group);
		test_late_handshake(group, 0);
		test_late_handshake(group, 1);
		test_real_timeout(group);
		test_existing_timer_arm_failure(group);
		test_timeout_abort_retry(group);
	}
	test_authorize_crosses_deadline(1);
	test_authorize_crosses_deadline(2);
	test_data_handoff_crosses_deadline(1);
	test_data_handoff_crosses_deadline(2);
	test_shard_capacity();
	test_listener_restart();
	assert(atomic_load(&clock_failures) == 2U);
	assert(atomic_load(&register_failures) == 2U);
	assert(atomic_load(&arm_failures) == 4U);
	assert(atomic_load(&unregister_failures) == 5U);
	puts("inbound handshake deadline tests passed");
	return 0;
}
