#include "tr/trpc.h"
#include "drain_deadline_probe.h"

#include "../src/facade_diagnostics_internal.h"
#include "../src/execution/reactor_internal.h"
#include "../src/group/pipeline_control_wire_internal.h"
#include "../src/group/pipeline_route_internal.h"
#include "../src/io/connector_internal.h"
#include "../src/memory_budget.h"
#include "../src/transport/group/message_budget.h"
#include "../src/transport/group/pipeline_listener_internal.h"

#include "../src/crc32c.h"
#include "../src/transport/protocol/message_envelope.h"
#include "tr/status.h"
#include "../src/transport/protocol/wire.h"

#include <zstd.h>

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define TEST_GROUP_ID UINT64_C(0x9911)
#define TEST_GROUP_EPOCH UINT64_C(71)
#define TEST_GROUP_PAIR_ID_FIRST UINT64_C(0x9921)
#define TEST_GROUP_PAIR_ID_SECOND UINT64_C(0x9922)
#define TEST_GROUP_PAIR_EPOCH_FIRST UINT64_C(73)
#define TEST_GROUP_PAIR_EPOCH_SECOND UINT64_C(74)
#define TEST_CONTROL_GENERATION 23U
#define TEST_GROUP_LARGE_DECODED_BYTES (1024U * 1024U)
#define TEST_GROUP_BUDGET_TAIL_BYTES (64U * 1024U)
#define TEST_GROUP_BUDGET_HOLD_COUNT 5U
#define TEST_GROUP_RAW_BURST_COUNT 64U
#define TEST_GROUP_RAW_BURST_FIRST_ID UINT64_C(6800)
#define TEST_GROUP_NATURAL_BUDGET_HOLD_CAPACITY 64U
#define TEST_GROUP_NATURAL_BUDGET_FIRST_ID UINT64_C(7000)
#define TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID UINT64_C(7400)
#define TEST_GROUP_FAIRNESS_ZSTD_BATCHES 8U
#define TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH 32U
#define TEST_GROUP_FAIRNESS_ZSTD_COUNT                                         \
	(TEST_GROUP_FAIRNESS_ZSTD_BATCHES * TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH)
#define TEST_GROUP_FAIRNESS_RAW_FIRST_ID UINT64_C(8000)
#define TEST_GROUP_FAIRNESS_RAW_WINDOW 32U
#define TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES 65536U
#define TEST_GROUP_FAIRNESS_MIN_RAW_DURING_ZSTD 32U

static atomic_int fail_group_control_close_once;
static atomic_uint group_control_close_attempts;
static atomic_int defer_group_connector_start;
static atomic_int fail_group_connector_cancel_once;
static atomic_uint group_state_aborts;
static pthread_mutex_t group_connector_probe_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t group_connector_probe_cond = PTHREAD_COND_INITIALIZER;
static struct tr_reactor *group_connector_probe_owner;
static tr_connector_complete_cb group_connector_probe_complete;
static void *group_connector_probe_arg;
static unsigned group_connector_probe_starts;
static atomic_int block_group_data_tx;
static pthread_mutex_t group_data_tx_probe_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t group_data_tx_probe_cond = PTHREAD_COND_INITIALIZER;
static unsigned group_data_tx_blocked_attempts;
static atomic_uint group_capability_frames_sent;
static atomic_uint group_compressed_multiframe_submissions;
static atomic_uint group_multi_data_compressed_submissions;
static atomic_uint group_large_compressed_submissions;
static atomic_uint group_large_raw_submissions;
static atomic_uint group_fairness_compressed_submissions;
static atomic_uint group_fairness_raw_submissions;
static atomic_int capture_group_message_budget;
static struct tr_memory_budget *captured_group_message_budget;
static pthread_mutex_t group_route_probe_lock = PTHREAD_MUTEX_INITIALIZER;
static int group_route_probe_enabled;
static uint64_t group_route_probe_message_ids[2];
static struct tr_conn_handle group_route_probe_connections[2];
static unsigned group_route_probe_seen;
static atomic_int hold_group_zstd_completion;
static atomic_int hold_group_zstd_decompression;
static pthread_mutex_t group_zstd_gate_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t group_zstd_gate_cond = PTHREAD_COND_INITIALIZER;
static int group_zstd_gate_entered;
static int group_zstd_gate_released;

static void wait_group_zstd_decompression_entered(void);
static void release_group_zstd_gate(void);
static void wait_group_client_send_bytes_empty(struct tr_client *client);

int __real_tr_pipeline_listener_create(
        const struct tr_pipeline_listener_config *config,
        struct tr_pipeline_listener **out);

int __wrap_tr_pipeline_listener_create(
        const struct tr_pipeline_listener_config *config,
        struct tr_pipeline_listener **out)
{
	if (atomic_load_explicit(&capture_group_message_budget,
	                         memory_order_acquire)) {
		assert(config != NULL);
		captured_group_message_budget = config->message_budget;
	}
	return __real_tr_pipeline_listener_create(config, out);
}

int __real_tr_reactor_close_on_owner(struct tr_conn_handle connection);

int __wrap_tr_reactor_close_on_owner(struct tr_conn_handle connection)
{
	atomic_fetch_add(&group_control_close_attempts, 1U);
	if (atomic_exchange(&fail_group_control_close_once, 0))
		return TR_ERR_SYS;
	return __real_tr_reactor_close_on_owner(connection);
}

int __real_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
                                     int status);

int __wrap_tr_reactor_abort_on_owner(struct tr_conn_handle connection,
                                     int status)
{
	if (status == TR_ERR_STATE)
		atomic_fetch_add(&group_state_aborts, 1U);
	return __real_tr_reactor_abort_on_owner(connection, status);
}

int __real_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload);

int __wrap_tr_reactor_send(struct tr_conn_handle connection, uint16_t type,
                           uint32_t flags, uint32_t stream_id,
                           uint64_t message_id, struct tr_buffer *payload)
{
	int ret = __real_tr_reactor_send(connection, type, flags, stream_id,
	                                 message_id, payload);

	if (ret == TR_OK && type == TR_FRAME_MESSAGE_CAPS)
		atomic_fetch_add(&group_capability_frames_sent, 1U);
	return ret;
}

int __real_tr_reactor_sendv_limited(struct tr_conn_handle connection,
                                    uint16_t type, uint32_t flags,
                                    uint32_t stream_id, uint64_t message_id,
                                    struct tr_buffer *const *payloads,
                                    uint32_t payload_count,
                                    uint32_t max_frame_payload_len);

int __wrap_tr_reactor_sendv_limited(struct tr_conn_handle connection,
                                    uint16_t type, uint32_t flags,
                                    uint32_t stream_id, uint64_t message_id,
                                    struct tr_buffer *const *payloads,
                                    uint32_t payload_count,
                                    uint32_t max_frame_payload_len)
{
	uint64_t payload_len = 0U;
	uint32_t i;
	int ret;

	for (i = 0U; i < payload_count; ++i)
		if (payloads[i])
			payload_len += payloads[i]->len;
	if (type == TR_FRAME_DATA && (flags & TR_FRAME_F_COMPRESSED) != 0U &&
	    payload_len > max_frame_payload_len)
		atomic_fetch_add(&group_compressed_multiframe_submissions, 1U);
	ret = __real_tr_reactor_sendv_limited(
	        connection, type, flags, stream_id, message_id, payloads,
	        payload_count, max_frame_payload_len);
	if (ret == TR_OK && type == TR_FRAME_DATA &&
	    (flags & TR_FRAME_F_COMPRESSED) != 0U &&
	    (message_id == UINT64_C(6611) || message_id == UINT64_C(6612)))
		atomic_fetch_add(&group_multi_data_compressed_submissions, 1U);
	if (ret == TR_OK && type == TR_FRAME_DATA &&
	    message_id == UINT64_C(6711) &&
	    (flags & TR_FRAME_F_COMPRESSED) != 0U)
		atomic_fetch_add(&group_large_compressed_submissions, 1U);
	if (ret == TR_OK && type == TR_FRAME_DATA &&
	    message_id >= TEST_GROUP_RAW_BURST_FIRST_ID &&
	    message_id < TEST_GROUP_RAW_BURST_FIRST_ID +
	                         TEST_GROUP_RAW_BURST_COUNT &&
	    (flags & TR_FRAME_F_COMPRESSED) == 0U)
		atomic_fetch_add(&group_large_raw_submissions, 1U);
	if (ret == TR_OK && type == TR_FRAME_DATA &&
	    message_id >= TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID &&
	    message_id < TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID +
	                         TEST_GROUP_FAIRNESS_ZSTD_COUNT &&
	    (flags & TR_FRAME_F_COMPRESSED) != 0U)
		atomic_fetch_add(&group_fairness_compressed_submissions, 1U);
	if (ret == TR_OK && type == TR_FRAME_DATA &&
	    message_id >= TEST_GROUP_FAIRNESS_RAW_FIRST_ID &&
	    message_id < TEST_GROUP_FAIRNESS_RAW_FIRST_ID +
	                         TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES &&
	    (flags & TR_FRAME_F_COMPRESSED) == 0U)
		atomic_fetch_add(&group_fairness_raw_submissions, 1U);
	if (ret == TR_OK && type == TR_FRAME_DATA) {
		unsigned i;
		int later_raw_burst_message =
		        message_id > TEST_GROUP_RAW_BURST_FIRST_ID &&
		        message_id < TEST_GROUP_RAW_BURST_FIRST_ID +
		                             TEST_GROUP_RAW_BURST_COUNT;
		int later_fairness_raw_message =
		        message_id > TEST_GROUP_FAIRNESS_RAW_FIRST_ID &&
		        message_id <
		                TEST_GROUP_FAIRNESS_RAW_FIRST_ID +
		                        TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES;

		pthread_mutex_lock(&group_route_probe_lock);
		if (group_route_probe_enabled &&
		    (later_raw_burst_message || later_fairness_raw_message) &&
		    (group_route_probe_seen & (1U << 1)) != 0U) {
			assert(connection.reactor ==
			       group_route_probe_connections[1].reactor);
			assert(connection.slot ==
			       group_route_probe_connections[1].slot);
			assert(connection.generation ==
			       group_route_probe_connections[1].generation);
		}
		if (group_route_probe_enabled)
			for (i = 0U; i < 2U; ++i)
				if (message_id ==
			            group_route_probe_message_ids[i]) {
					group_route_probe_connections[i] = connection;
					group_route_probe_seen |= 1U << i;
				}
		pthread_mutex_unlock(&group_route_probe_lock);
	}
	return ret;
}

int __real_tr_connector_create(const struct tr_connector_config *config,
                               struct tr_connector **out);

int __wrap_tr_connector_create(const struct tr_connector_config *config,
                               struct tr_connector **out)
{
	int ret = __real_tr_connector_create(config, out);

	if (ret == TR_OK) {
		pthread_mutex_lock(&group_connector_probe_lock);
		group_connector_probe_owner = config->owner;
		group_connector_probe_complete = config->complete_cb;
		group_connector_probe_arg = config->callback_arg;
		pthread_mutex_unlock(&group_connector_probe_lock);
	}
	return ret;
}

int __real_tr_connector_start(struct tr_connector *connector,
                              const char *ipv4_address, uint16_t port,
                              const uint8_t *preface, uint32_t preface_len);

int __wrap_tr_connector_start(struct tr_connector *connector,
                              const char *ipv4_address, uint16_t port,
                              const uint8_t *preface, uint32_t preface_len)
{
	if (atomic_load(&defer_group_connector_start)) {
		pthread_mutex_lock(&group_connector_probe_lock);
		group_connector_probe_starts++;
		pthread_cond_broadcast(&group_connector_probe_cond);
		pthread_mutex_unlock(&group_connector_probe_lock);
		/* 该目标中的假连接器接受异步尝试，测试稍后显式投递完成回调。 */
		return TR_OK;
	}
	return __real_tr_connector_start(connector, ipv4_address, port, preface,
	                                 preface_len);
}

int __real_tr_connector_cancel(struct tr_connector *connector);

int __wrap_tr_connector_cancel(struct tr_connector *connector)
{
	if (atomic_exchange(&fail_group_connector_cancel_once, 0))
		return TR_ERR_SYS;
	return __real_tr_connector_cancel(connector);
}

ssize_t __real_sendmsg(int fd, const struct msghdr *message, int flags);

ssize_t __wrap_sendmsg(int fd, const struct msghdr *message, int flags)
{
	struct tr_frame_header header;

	if (atomic_load(&block_group_data_tx) && message &&
	    message->msg_iovlen != 0U && message->msg_iov &&
	    message->msg_iov[0].iov_len >= TR_WIRE_HEADER_SIZE &&
	    tr_wire_header_decode((const uint8_t *)message->msg_iov[0].iov_base,
	                          &header) == TR_OK &&
	    header.type == TR_FRAME_DATA) {
		pthread_mutex_lock(&group_data_tx_probe_lock);
		group_data_tx_blocked_attempts++;
		pthread_cond_broadcast(&group_data_tx_probe_cond);
		pthread_mutex_unlock(&group_data_tx_probe_lock);
		errno = EAGAIN;
		return -1;
	}
	return __real_sendmsg(fd, message, flags);
}

size_t __real_ZSTD_compressStream2(ZSTD_CCtx *ctx, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end);

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *ctx, ZSTD_outBuffer *out,
                                   ZSTD_inBuffer *in, ZSTD_EndDirective end)
{
	size_t ret = __real_ZSTD_compressStream2(ctx, out, in, end);

	if (atomic_exchange(&hold_group_zstd_completion, 0)) {
		pthread_mutex_lock(&group_zstd_gate_lock);
		group_zstd_gate_entered = 1;
		pthread_cond_broadcast(&group_zstd_gate_cond);
		while (!group_zstd_gate_released)
			pthread_cond_wait(&group_zstd_gate_cond,
			                  &group_zstd_gate_lock);
		pthread_mutex_unlock(&group_zstd_gate_lock);
	}
	return ret;
}

size_t __real_ZSTD_decompressStream(ZSTD_DCtx *ctx, ZSTD_outBuffer *out,
                                    ZSTD_inBuffer *in);

size_t __wrap_ZSTD_decompressStream(ZSTD_DCtx *ctx, ZSTD_outBuffer *out,
                                    ZSTD_inBuffer *in)
{
	size_t ret = __real_ZSTD_decompressStream(ctx, out, in);

	if (atomic_exchange(&hold_group_zstd_decompression, 0)) {
		pthread_mutex_lock(&group_zstd_gate_lock);
		group_zstd_gate_entered = 1;
		pthread_cond_broadcast(&group_zstd_gate_cond);
		while (!group_zstd_gate_released)
			pthread_cond_wait(&group_zstd_gate_cond,
			                  &group_zstd_gate_lock);
		pthread_mutex_unlock(&group_zstd_gate_lock);
	}
	return ret;
}

struct public_group_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_client *client;
	unsigned authorized;
	unsigned messages;
	unsigned data_events;
	int last_data_status;
	unsigned fairness_zstd_messages;
	unsigned fairness_zstd_batches;
	unsigned fairness_raw_sent;
	unsigned fairness_raw_messages;
	unsigned fairness_raw_at_batch_start[TEST_GROUP_FAIRNESS_ZSTD_BATCHES];
	unsigned fairness_raw_at_batch_end[TEST_GROUP_FAIRNESS_ZSTD_BATCHES];
	unsigned fairness_zstd_held;
	unsigned fairness_raw_sender_exhausted;
	int fairness_raw_sender_status;
	struct tr_connection_group_message
	        fairness_retained[TEST_GROUP_BUDGET_HOLD_COUNT];
	unsigned transfer_ready;
	uint32_t ready_stream_id;
	uint64_t ready_message_id;
	unsigned client_send_fragments;
	unsigned client_send_first;
	unsigned client_send_last;
	uint32_t client_send_len;
	uint8_t client_send_bytes[256];
	int ready_send_first_ret;
	int ready_send_second_ret;
	unsigned retry_messages;
	unsigned multi_drain_messages;
	unsigned raw_burst_messages;
	unsigned member_replacement_messages;
	unsigned inflight_drain_messages;
	unsigned compressed_drain_messages;
	unsigned compressed_frames;
	unsigned compressed_first;
	unsigned compressed_last;
	unsigned compressed_done;
	uint64_t compressed_message_ids[16];
	uint32_t compressed_decoded_len;
	uint8_t compressed_decoded[128];
	struct tr_connection_group_message retained;
	struct tr_connection_group_message compressed_retained;
	struct tr_connection_group_message
	        budget_retained[TEST_GROUP_BUDGET_HOLD_COUNT];
	unsigned budget_hold_messages;
	struct tr_connection_group_message natural_budget_retained
	        [TEST_GROUP_NATURAL_BUDGET_HOLD_CAPACITY];
	unsigned natural_budget_hold_messages;
};

struct fairness_raw_sender {
	struct public_group_ctx *ctx;
	struct tr_client *client;
	atomic_int stop;
};

struct group_pair_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned authorized[2];
	unsigned data_messages[2];
	unsigned data_events[2];
};

enum { GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS = 32U };

struct group_pair_abort_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned ready;
	int open;
};

struct group_pair_abort_task {
	struct group_pair_abort_gate *gate;
	struct tr_conn_handle connection;
	int result;
};

static void *abort_group_pair_data_route(void *arg)
{
	struct group_pair_abort_task *task = arg;

	pthread_mutex_lock(&task->gate->lock);
	task->gate->ready++;
	pthread_cond_broadcast(&task->gate->cond);
	while (!task->gate->open)
		pthread_cond_wait(&task->gate->cond, &task->gate->lock);
	pthread_mutex_unlock(&task->gate->lock);
	task->result = tr_reactor_abort(task->connection, TR_ERR_SYS);
	return NULL;
}

static void abort_group_pair_data_routes_concurrently(
	const struct tr_conn_handle connections[2])
{
	struct group_pair_abort_gate gate;
	struct group_pair_abort_task tasks[2];
	pthread_t threads[2];
	unsigned i;

	memset(&gate, 0, sizeof(gate));
	assert(pthread_mutex_init(&gate.lock, NULL) == 0);
	assert(pthread_cond_init(&gate.cond, NULL) == 0);
	for (i = 0U; i < 2U; ++i) {
		tasks[i].gate = &gate;
		tasks[i].connection = connections[i];
		tasks[i].result = TR_ERR_STATE;
		assert(pthread_create(&threads[i], NULL,
		                      abort_group_pair_data_route,
		                      &tasks[i]) == 0);
	}
	pthread_mutex_lock(&gate.lock);
	while (gate.ready != 2U)
		pthread_cond_wait(&gate.cond, &gate.lock);
	gate.open = 1;
	pthread_cond_broadcast(&gate.cond);
	pthread_mutex_unlock(&gate.lock);
	for (i = 0U; i < 2U; ++i) {
		assert(pthread_join(threads[i], NULL) == 0);
		assert(tasks[i].result == TR_OK);
	}
	assert(pthread_cond_destroy(&gate.cond) == 0);
	assert(pthread_mutex_destroy(&gate.lock) == 0);
}

static void send_all(int fd, const void *data, size_t len)
{
	const uint8_t *p = (const uint8_t *)data;

	while (len != 0U) {
		ssize_t n = send(fd, p, len, MSG_NOSIGNAL);

		if (n > 0) {
			p += (size_t)n;
			len -= (size_t)n;
			continue;
		}
		assert(n < 0 && errno == EINTR);
	}
}

static void recv_all_timeout(int fd, void *data, size_t len)
{
	uint8_t *p = (uint8_t *)data;

	while (len != 0U) {
		struct pollfd pfd;
		ssize_t n;
		int ret;

		memset(&pfd, 0, sizeof(pfd));
		pfd.fd = fd;
		pfd.events = POLLIN | POLLHUP;
		do {
			ret = poll(&pfd, 1, 10000);
		} while (ret < 0 && errno == EINTR);
		assert(ret == 1);

		n = recv(fd, p, len, 0);
		assert(n > 0);
		p += (size_t)n;
		len -= (size_t)n;
	}
}

static void expect_socket_closed(int fd)
{
	struct pollfd pfd;
	uint8_t byte;
	ssize_t n;
	int ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLIN | POLLHUP;
	do {
		ret = poll(&pfd, 1, 10000);
	} while (ret < 0 && errno == EINTR);
	assert(ret == 1);
	assert((pfd.revents & (POLLIN | POLLHUP | POLLERR)) != 0);
	do {
		n = recv(fd, &byte, sizeof(byte), 0);
	} while (n < 0 && errno == EINTR);
	assert(n == 0 || (n < 0 && errno == ECONNRESET));
}

static int connect_loopback(uint16_t port)
{
	struct sockaddr_in addr;
	int fd;

	fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
	assert(fd >= 0);
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
	assert(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0);
	return fd;
}

static void send_route(int fd, const struct tr_pipeline_route_preface *route)
{
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, route) == TR_OK);
	send_all(fd, raw, sizeof(raw));
}

static struct tr_pipeline_route_preface control_route(void)
{
	struct tr_pipeline_route_preface route;

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	route.owner_shard_id = 0U;
	route.pipeline_id = TEST_GROUP_ID;
	route.epoch = TEST_GROUP_EPOCH;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation = TEST_CONTROL_GENERATION;
	return route;
}

static void recv_control_message(
	int fd, uint64_t expected_message_id,
	struct tr_pipeline_control_wire_message *message)
{
	struct tr_frame_header header;
	struct tr_wire_limits limits;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_PIPELINE_CONTROL_WIRE_SIZE];

	recv_all_timeout(fd, raw_header, sizeof(raw_header));
	memset(&header, 0, sizeof(header));
	assert(tr_wire_header_decode(raw_header, &header) == TR_OK);
	memset(&limits, 0, sizeof(limits));
	limits.max_payload_len = TR_PIPELINE_CONTROL_WIRE_SIZE;
	assert(tr_wire_header_validate(raw_header, &header, &limits) == TR_OK);
	assert(header.type == TR_FRAME_PIPELINE_CONTROL);
	assert(header.message_id == expected_message_id);
	assert(header.payload_len == sizeof(payload));

	recv_all_timeout(fd, payload, sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_pipeline_control_wire_decode(
		       payload, sizeof(payload), message) == TR_OK);
}

static void send_data_message(int fd, uint32_t stream_id,
			      uint64_t message_id,
			      const uint8_t *payload, uint32_t payload_len)
{
	struct tr_frame_header header;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_DATA;
	header.flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
	header.stream_id = stream_id;
	header.message_id = message_id;
	header.payload_len = payload_len;
	header.payload_crc32c = tr_crc32c(payload, payload_len);
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	if (payload_len != 0U)
		send_all(fd, payload, payload_len);
}

static int
authorize_group(const struct tr_connection_group_authorization *authorization,
                void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	if (!authorization || authorization->group.group_id != TEST_GROUP_ID ||
	    authorization->group.epoch != TEST_GROUP_EPOCH ||
	    (authorization->role != TR_CONNECTION_GROUP_ROLE_CONTROL &&
	     authorization->role != TR_CONNECTION_GROUP_ROLE_DATA) ||
	    authorization->peer_identity.authentication !=
	            TR_PEER_IDENTITY_NONE ||
	    authorization->control_identity.authentication !=
	            TR_PEER_IDENTITY_NONE)
		return TR_ERR_STALE;

	pthread_mutex_lock(&ctx->lock);
	ctx->authorized++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static int authorize_group_dual_epoch(
        const struct tr_connection_group_authorization *authorization,
        void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;
	uint64_t epoch;

	if (!authorization || authorization->group.group_id != TEST_GROUP_ID ||
	    (authorization->role != TR_CONNECTION_GROUP_ROLE_CONTROL &&
	     authorization->role != TR_CONNECTION_GROUP_ROLE_DATA) ||
	    authorization->peer_identity.authentication !=
	            TR_PEER_IDENTITY_NONE ||
	    authorization->control_identity.authentication !=
	            TR_PEER_IDENTITY_NONE)
		return TR_ERR_STALE;
	epoch = authorization->group.epoch;
	if (epoch != TEST_GROUP_EPOCH && epoch != TEST_GROUP_EPOCH + 1U)
		return TR_ERR_STALE;

	pthread_mutex_lock(&ctx->lock);
	ctx->authorized++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_connection_group_message_disposition
on_group_message(const struct tr_connection_group_message *message, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(message != NULL);
	assert(message->group.group_id == TEST_GROUP_ID);
	assert(message->group.epoch == TEST_GROUP_EPOCH);

	if (message->stream_id == 3001U) {
		assert(message->message_id == UINT64_C(2001));
		assert(message->flags ==
		       (TR_CONNECTION_GROUP_DATA_FIRST |
			TR_CONNECTION_GROUP_DATA_LAST));

		pthread_mutex_lock(&ctx->lock);
		ctx->retained = *message;
		ctx->messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
	}
	if (message->message_id >= UINT64_C(4301) &&
	    message->message_id <= UINT64_C(4310)) {
		assert(message->stream_id == 5001U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == sizeof(ctx->compressed_decoded));
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] ==
			       (uint8_t)(i * 73U + 19U));
		if (message->message_id == UINT64_C(4302)) {
			pthread_mutex_lock(&ctx->lock);
			ctx->compressed_retained = *message;
			pthread_mutex_unlock(&ctx->lock);
		}
		pthread_mutex_lock(&ctx->lock);
		memcpy(ctx->compressed_decoded, message->bytes.data,
		       message->bytes.len);
		ctx->compressed_decoded_len = message->bytes.len;
		assert(ctx->compressed_done <
		       sizeof(ctx->compressed_message_ids) /
		               sizeof(ctx->compressed_message_ids[0]));
		ctx->compressed_message_ids[ctx->compressed_done] =
		        message->message_id;
		ctx->compressed_frames++;
		ctx->compressed_first++;
		ctx->compressed_last++;
		ctx->compressed_done++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		if (message->message_id == UINT64_C(4302))
			return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6611) ||
	    message->message_id == UINT64_C(6612)) {
		uint32_t expected_stream =
		        message->message_id == UINT64_C(6611) ? 5001U : 5002U;

		assert(message->stream_id == expected_stream);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 128U);
		for (uint32_t i = 0U; i < message->bytes.len; ++i) {
			uint8_t expected_byte =
			        message->message_id == UINT64_C(6611)
			                ? (uint8_t)(i * 73U + 19U)
			                : (uint8_t)(i * 53U + 71U);
			assert(message->bytes.data[i] == expected_byte);
		}
		pthread_mutex_lock(&ctx->lock);
		if (message->message_id == UINT64_C(6611))
			ctx->retained = *message;
		ctx->multi_drain_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		if (message->message_id == UINT64_C(6611))
			return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6613)) {
		assert(message->stream_id == 5002U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == 0x34U);
		pthread_mutex_lock(&ctx->lock);
		ctx->multi_drain_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6711)) {
		assert(message->stream_id == 5101U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == TEST_GROUP_LARGE_DECODED_BYTES);
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] == 0x5aU);
		pthread_mutex_lock(&ctx->lock);
		ctx->retained = *message;
		ctx->multi_drain_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
	}
	if (message->message_id >= UINT64_C(6721) &&
	    message->message_id <
	            UINT64_C(6721) + TEST_GROUP_BUDGET_HOLD_COUNT) {
		unsigned index =
		        (unsigned)(message->message_id - UINT64_C(6721));
		uint32_t expected_bytes =
		        index + 1U == TEST_GROUP_BUDGET_HOLD_COUNT
		                ? TEST_GROUP_BUDGET_TAIL_BYTES
		                : TEST_GROUP_LARGE_DECODED_BYTES;
		uint8_t expected_byte = (uint8_t)(0x61U + index);

		assert(message->stream_id == 5101U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == expected_bytes);
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		assert(ctx->budget_retained[index].bytes.data == NULL);
		ctx->budget_retained[index] = *message;
		ctx->budget_hold_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
	}
	if (message->message_id >= TEST_GROUP_NATURAL_BUDGET_FIRST_ID &&
	    message->message_id <
	            TEST_GROUP_NATURAL_BUDGET_FIRST_ID +
	                    TEST_GROUP_NATURAL_BUDGET_HOLD_CAPACITY) {
		unsigned index = (unsigned)(message->message_id -
		                            TEST_GROUP_NATURAL_BUDGET_FIRST_ID);
		uint8_t expected_byte = (uint8_t)(0x31U + index);

		assert(message->stream_id == 5101U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == TEST_GROUP_LARGE_DECODED_BYTES);
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		assert(ctx->natural_budget_retained[index].bytes.data == NULL);
		ctx->natural_budget_retained[index] = *message;
		ctx->natural_budget_hold_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP;
	}
	if (message->message_id >= TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID &&
	    message->message_id < TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID +
	                                  TEST_GROUP_FAIRNESS_ZSTD_COUNT) {
		unsigned index = (unsigned)(message->message_id -
		                            TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID);
		unsigned batch = index / TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH;
		uint8_t expected_byte = (uint8_t)(0x40U + index);

		assert(message->stream_id == 5101U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == TEST_GROUP_LARGE_DECODED_BYTES);
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		assert(index == ctx->fairness_zstd_messages);
		if (index % TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH == 0U)
			ctx->fairness_raw_at_batch_start[batch] =
			        ctx->fairness_raw_messages;
		if (index < TEST_GROUP_BUDGET_HOLD_COUNT) {
			assert(ctx->fairness_retained[index].bytes.data ==
			       NULL);
			ctx->fairness_retained[index] = *message;
			ctx->fairness_zstd_held++;
		}
		ctx->fairness_zstd_messages++;
		if ((index + 1U) % TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH == 0U) {
			ctx->fairness_raw_at_batch_end[batch] =
			        ctx->fairness_raw_messages;
			ctx->fairness_zstd_batches++;
		}
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return index < TEST_GROUP_BUDGET_HOLD_COUNT
		               ? TR_CONNECTION_GROUP_MESSAGE_TAKE_OWNERSHIP
			       : TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id >= TEST_GROUP_FAIRNESS_RAW_FIRST_ID &&
	    message->message_id <
	            TEST_GROUP_FAIRNESS_RAW_FIRST_ID +
	                    TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES) {
		uint8_t expected_byte =
		        (uint8_t)(0x80U + (message->message_id -
			                   TEST_GROUP_FAIRNESS_RAW_FIRST_ID));

		assert(message->stream_id == 5102U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		assert(message->message_id ==
		       TEST_GROUP_FAIRNESS_RAW_FIRST_ID +
		               ctx->fairness_raw_messages);
		assert(ctx->fairness_raw_messages < ctx->fairness_raw_sent);
		ctx->fairness_raw_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id >= TEST_GROUP_RAW_BURST_FIRST_ID &&
	    message->message_id < TEST_GROUP_RAW_BURST_FIRST_ID +
	                                  TEST_GROUP_RAW_BURST_COUNT) {
		unsigned index = (unsigned)(message->message_id -
		                            TEST_GROUP_RAW_BURST_FIRST_ID);
		uint8_t expected_byte = (uint8_t)(0x6aU + index);

		assert(message->stream_id == 5102U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		ctx->multi_drain_messages++;
		ctx->raw_burst_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6201) ||
	    message->message_id == UINT64_C(6202)) {
		uint32_t expected_stream =
		        message->message_id == UINT64_C(6201) ? 5001U : 5002U;
		uint8_t expected_byte =
		        message->message_id == UINT64_C(6201) ? 0x31U : 0x32U;

		assert(message->stream_id == expected_stream);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == expected_byte);
		pthread_mutex_lock(&ctx->lock);
		ctx->multi_drain_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6203)) {
		assert(message->stream_id == 5001U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == 0x33U);
		pthread_mutex_lock(&ctx->lock);
		ctx->inflight_drain_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6204)) {
		assert(message->stream_id == 5003U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == 0x34U);
		pthread_mutex_lock(&ctx->lock);
		ctx->member_replacement_messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	if (message->message_id == UINT64_C(6503)) {
		assert(message->stream_id == 5001U);
		assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
		                          TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 64U);
		for (uint32_t i = 0U; i < message->bytes.len; ++i)
			assert(message->bytes.data[i] ==
			       (uint8_t)(i * 29U + 7U));
		pthread_mutex_lock(&ctx->lock);
		ctx->compressed_drain_messages++;
		ctx->messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}

	assert(message->stream_id == 5001U);
	if (message->message_id == UINT64_C(4202)) {
		assert(message->flags ==
		       (TR_CONNECTION_GROUP_DATA_FIRST |
			TR_CONNECTION_GROUP_DATA_LAST));
		assert(message->bytes.len == 1U);
		assert(message->bytes.data[0] == 0xa5U);
		pthread_mutex_lock(&ctx->lock);
		ctx->retry_messages++;
		ctx->messages++;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
	}
	assert(message->message_id == UINT64_C(4201));
	pthread_mutex_lock(&ctx->lock);
	assert((uint64_t)ctx->client_send_len + message->bytes.len <=
	       sizeof(ctx->client_send_bytes));
	if (message->bytes.len != 0U)
		memcpy(ctx->client_send_bytes + ctx->client_send_len,
		       message->bytes.data, message->bytes.len);
	ctx->client_send_len += message->bytes.len;
	ctx->client_send_fragments++;
	if (message->flags & TR_CONNECTION_GROUP_DATA_FIRST)
		ctx->client_send_first++;
	if (message->flags & TR_CONNECTION_GROUP_DATA_LAST)
		ctx->client_send_last++;
	ctx->messages++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_client_transfer_ready(
	const struct tr_connection_group_id *group, uint32_t stream_id,
	uint64_t message_id, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(stream_id != 0U);

	if (message_id == UINT64_C(4101) && ctx->client) {
		uint8_t payload[130];
		uint8_t retry = 0xa5U;
		struct tr_transport_bytes bytes;
		struct tr_transport_bytes retry_bytes;
		uint32_t i;

		for (i = 0; i < sizeof(payload); ++i)
			payload[i] = (uint8_t)(i ^ 0x5aU);
		bytes.data = payload;
		bytes.len = (uint32_t)sizeof(payload);
		retry_bytes.data = &retry;
		retry_bytes.len = 1U;
		ctx->ready_send_first_ret =
			tr_client_connection_group_send(
				ctx->client, stream_id, UINT64_C(4201), &bytes);
		ctx->ready_send_second_ret =
			tr_client_connection_group_send(
				ctx->client, stream_id, UINT64_C(4202),
				&retry_bytes);
		memset(payload, 0, sizeof(payload));
	}

	pthread_mutex_lock(&ctx->lock);
	ctx->transfer_ready++;
	ctx->ready_stream_id = stream_id;
	ctx->ready_message_id = message_id;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void on_group_data_event(
	const struct tr_connection_group_id *group,
	enum tr_connection_group_data_event event, int status, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(event == TR_CONNECTION_GROUP_DATA_CLOSED ||
	       event == TR_CONNECTION_GROUP_DATA_ERROR);
	(void)status;

	pthread_mutex_lock(&ctx->lock);
	ctx->data_events++;
	ctx->last_data_status = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void
on_group_data_event_dual_epoch(const struct tr_connection_group_id *group,
                               enum tr_connection_group_data_event event,
                               int status, void *arg)
{
	struct public_group_ctx *ctx = (struct public_group_ctx *)arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH ||
	       group->epoch == TEST_GROUP_EPOCH + 1U);
	assert(event == TR_CONNECTION_GROUP_DATA_CLOSED ||
	       event == TR_CONNECTION_GROUP_DATA_ERROR);

	pthread_mutex_lock(&ctx->lock);
	ctx->data_events++;
	ctx->last_data_status = status;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int group_pair_index(const struct tr_connection_group_id *group)
{
	if (!group)
		return -1;
	if (group->group_id == TEST_GROUP_PAIR_ID_FIRST &&
	    group->epoch == TEST_GROUP_PAIR_EPOCH_FIRST)
		return 0;
	if (group->group_id == TEST_GROUP_PAIR_ID_SECOND &&
	    group->epoch == TEST_GROUP_PAIR_EPOCH_SECOND)
		return 1;
	return -1;
}

static int authorize_group_pair(
	const struct tr_connection_group_authorization *authorization,
	void *arg)
{
	struct group_pair_ctx *ctx = (struct group_pair_ctx *)arg;
	int index;

	if (!authorization ||
	    (authorization->role != TR_CONNECTION_GROUP_ROLE_CONTROL &&
	     authorization->role != TR_CONNECTION_GROUP_ROLE_DATA) ||
	    authorization->peer_identity.authentication !=
	            TR_PEER_IDENTITY_NONE ||
	    authorization->control_identity.authentication !=
	            TR_PEER_IDENTITY_NONE)
		return TR_ERR_STALE;
	index = group_pair_index(&authorization->group);
	if (index < 0)
		return TR_ERR_STALE;

	pthread_mutex_lock(&ctx->lock);
	ctx->authorized[index]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_connection_group_message_disposition on_group_pair_message(
	const struct tr_connection_group_message *message, void *arg)
{
	struct group_pair_ctx *ctx = (struct group_pair_ctx *)arg;
	int index;

	assert(message != NULL);
	index = group_pair_index(&message->group);
	assert(index >= 0);
	if (message->message_id >= UINT64_C(7209)) {
		uint64_t offset = message->message_id - UINT64_C(7209);
		uint64_t expected_stream_id =
		        (index == 0 ? UINT64_C(7104) : UINT64_C(8104)) +
		        offset / 2U;

		assert(offset % 2U == (uint64_t)index);
		assert(message->stream_id == expected_stream_id);
		assert(message->bytes.len == 1U &&
		       message->bytes.data[0] == 0x53U);
	} else if (index == 0) {
		if (message->message_id == UINT64_C(7201)) {
			assert(message->stream_id == 7101U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x51U);
		} else if (message->message_id == UINT64_C(7203)) {
			assert(message->stream_id == 7102U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x53U);
		} else {
			assert(message->message_id == UINT64_C(7206));
			assert(message->stream_id == 7103U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x53U);
		}
	} else {
		if (message->message_id == UINT64_C(7202)) {
			assert(message->stream_id == 8101U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x52U);
		} else if (message->message_id == UINT64_C(7204)) {
			assert(message->stream_id == 8102U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x53U);
		} else if (message->message_id == UINT64_C(7207)) {
			assert(message->stream_id == 8103U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x53U);
		} else if (message->message_id == UINT64_C(7208)) {
			assert(message->stream_id ==
			       UINT64_C(8104) +
			               GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS -
			               1U);
			assert(message->bytes.len == 1U &&
			       message->bytes.data[0] == 0x53U);
		} else {
			assert(!"unexpected group pair message id");
		}
	}
	assert(message->flags == (TR_CONNECTION_GROUP_DATA_FIRST |
	                          TR_CONNECTION_GROUP_DATA_LAST));

	pthread_mutex_lock(&ctx->lock);
	ctx->data_messages[index]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_group_pair_data_event(
	const struct tr_connection_group_id *group,
	enum tr_connection_group_data_event event, int status, void *arg)
{
	struct group_pair_ctx *ctx = (struct group_pair_ctx *)arg;
	int index = group_pair_index(group);

	assert(index >= 0);
	assert(event == TR_CONNECTION_GROUP_DATA_CLOSED ||
	       event == TR_CONNECTION_GROUP_DATA_ERROR);
	(void)status;
	pthread_mutex_lock(&ctx->lock);
	ctx->data_events[index]++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static const char *counter_name(
	struct public_group_ctx *ctx, const unsigned *counter)
{
	if (counter == &ctx->authorized)
		return "authorized";
	if (counter == &ctx->messages)
		return "messages";
	if (counter == &ctx->retry_messages)
		return "retry_messages";
	if (counter == &ctx->multi_drain_messages)
		return "multi_drain_messages";
	if (counter == &ctx->fairness_zstd_messages)
		return "fairness_zstd_messages";
	if (counter == &ctx->fairness_raw_sent)
		return "fairness_raw_sent";
	if (counter == &ctx->fairness_raw_messages)
		return "fairness_raw_messages";
	if (counter == &ctx->natural_budget_hold_messages)
		return "natural_budget_hold_messages";
	if (counter == &ctx->member_replacement_messages)
		return "member_replacement_messages";
	if (counter == &ctx->inflight_drain_messages)
		return "inflight_drain_messages";
	if (counter == &ctx->compressed_drain_messages)
		return "compressed_drain_messages";
	if (counter == &ctx->compressed_done)
		return "compressed_done";
	if (counter == &ctx->transfer_ready)
		return "transfer_ready";
	if (counter == &ctx->data_events)
		return "data_events";
	return "unknown";
}

static void wait_counter(
	struct public_group_ctx *ctx, unsigned *counter, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*counter < target) {
		int ret = pthread_cond_timedwait(
			&ctx->cond, &ctx->lock, &deadline);

		if (ret != 0) {
			fprintf(
				stderr,
				"timeout waiting for %s: current=%u target=%u\n",
				counter_name(ctx, counter), *counter, target);
			assert(ret == 0);
		}
	}
	pthread_mutex_unlock(&ctx->lock);
}

static void *fairness_raw_sender_main(void *arg)
{
	struct fairness_raw_sender *sender = (struct fairness_raw_sender *)arg;
	struct public_group_ctx *ctx = sender->ctx;
	struct timespec retry_delay = {0, 1000000L};

	for (;;) {
		struct tr_transport_bytes bytes;
		uint64_t message_id;
		uint8_t payload;
		int ret;

		pthread_mutex_lock(&ctx->lock);
		while (!atomic_load(&sender->stop) &&
		       ctx->fairness_raw_sent - ctx->fairness_raw_messages >=
		               TEST_GROUP_FAIRNESS_RAW_WINDOW &&
		       ctx->fairness_raw_sent <
		               TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES) {
			int wait_ret =
			        pthread_cond_wait(&ctx->cond, &ctx->lock);

			assert(wait_ret == 0);
		}
		if (atomic_load(&sender->stop)) {
			pthread_mutex_unlock(&ctx->lock);
			break;
		}
		if (ctx->fairness_raw_sent >=
		    TEST_GROUP_FAIRNESS_RAW_MAX_MESSAGES) {
			ctx->fairness_raw_sender_exhausted = 1U;
			pthread_cond_broadcast(&ctx->cond);
			pthread_mutex_unlock(&ctx->lock);
			break;
		}
		message_id = TEST_GROUP_FAIRNESS_RAW_FIRST_ID +
		             ctx->fairness_raw_sent;
		ctx->fairness_raw_sent++;
		pthread_mutex_unlock(&ctx->lock);

		payload = (uint8_t)(0x80U + (message_id -
		                             TEST_GROUP_FAIRNESS_RAW_FIRST_ID));
		bytes.data = &payload;
		bytes.len = 1U;
		ret = tr_client_connection_group_send(sender->client, 5102U,
		                                      message_id, &bytes);
		if (ret == TR_OK)
			continue;

		pthread_mutex_lock(&ctx->lock);
		assert(ctx->fairness_raw_sent != 0U);
		ctx->fairness_raw_sent--;
		if (ret != TR_AGAIN)
			ctx->fairness_raw_sender_status = ret;
		pthread_cond_broadcast(&ctx->cond);
		pthread_mutex_unlock(&ctx->lock);
		if (ret != TR_AGAIN)
			break;
		(void)nanosleep(&retry_delay, NULL);
	}
	return NULL;
}

static uint64_t group_monotonic_now_ns(void)
{
	struct timespec now;

	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
	       (uint64_t)now.tv_nsec;
}

static int wait_natural_budget_hold_or_data_close(struct public_group_ctx *ctx,
                                                  unsigned hold_target)
{
	struct timespec deadline;
	int received;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->natural_budget_hold_messages < hold_target &&
	       ctx->data_events == 0U) {
		int ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock,
		                                 &deadline);

		if (ret != 0) {
			fprintf(stderr,
			        "timeout waiting for natural budget result: "
			        "holds=%u target=%u events=%u\n",
			        ctx->natural_budget_hold_messages, hold_target,
			        ctx->data_events);
			assert(ret == 0);
		}
	}
	received = ctx->natural_budget_hold_messages >= hold_target;
	pthread_mutex_unlock(&ctx->lock);
	return received;
}

static void wait_group_capability_frames(unsigned target)
{
	struct timespec pause = {0, 1000000L};
	unsigned i;

	for (i = 0; i < 2000U; ++i) {
		if (atomic_load(&group_capability_frames_sent) >= target)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(atomic_load(&group_capability_frames_sent) >= target);
}

static void wait_server_destroy_after_release(struct tr_server *server)
{
	struct timespec pause = {0, 1000000L};
	unsigned attempt;
	int ret = TR_ERR_STATE;

	for (attempt = 0U; attempt < 5000U; ++attempt) {
		ret = tr_server_destroy(server);
		if (ret == TR_OK)
			return;
		assert(ret == TR_ERR_STATE || ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	assert(ret == TR_OK);
}

static void wait_client_destroy_after_group_close(struct tr_client *client)
{
	struct timespec pause = {0, 1000000L};
	unsigned attempt;
	int ret = TR_ERR_STATE;

	for (attempt = 0U; attempt < 5000U; ++attempt) {
		ret = tr_client_destroy(client);
		if (ret == TR_OK)
			return;
		assert(ret == TR_ERR_STATE || ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	assert(ret == TR_OK);
}

static int wait_data_offer(struct tr_server *server, uint64_t message_id)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		int ret = tr_server_connection_group_send_data_offer(
			server, TEST_GROUP_ID, TEST_GROUP_EPOCH, message_id);

		if (ret == TR_OK)
			return TR_OK;
		assert(ret == TR_ERR_STALE || ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	return TR_ERR_TIMEOUT;
}

static void wait_data_accepts(struct tr_server *server, uint64_t target)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		struct tr_pipeline_listener_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_get_connection_group_stats_internal(
			       server, &stats) == TR_OK);
		if (stats.data_accepts >= target)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for DATA attach");
}

static int wait_group_pair_offer(
	struct tr_server *server, uint64_t group_id, uint64_t epoch,
	uint64_t message_id)
{
	struct timespec pause = {0, 10000000L};
	unsigned i;

	for (i = 0U; i < 1000U; ++i) {
		int ret = tr_server_connection_group_send_data_offer(
			server, group_id, epoch, message_id);

		if (ret == TR_OK)
			return TR_OK;
		assert(ret == TR_ERR_STALE || ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	return TR_ERR_TIMEOUT;
}

static void wait_group_pair_counter(
	struct group_pair_ctx *ctx, unsigned *counter, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*counter < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	if (ret != 0)
		fprintf(stderr,
		        "timeout waiting for group pair counter: current=%u target=%u\n",
		        *counter, target);
	assert(ret == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_group_pair_server_stats(
	struct tr_server *server, uint32_t groups, uint32_t data_connections,
	uint64_t active_transfers)
{
	struct timespec pause = {0, 10000000L};
	unsigned i;

	for (i = 0U; i < 1000U; ++i) {
		struct tr_connection_group_server_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.groups_current == groups &&
		    stats.data_connections_current == data_connections &&
		    stats.active_transfers == active_transfers)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for group pair server stats");
}

static void wait_group_pair_client_stats(
	struct tr_client *client, uint32_t data_connections,
	uint32_t active_transfers)
{
	struct timespec pause = {0, 10000000L};
	unsigned i;

	for (i = 0U; i < 1000U; ++i) {
		struct tr_connection_group_client_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_client_connection_group_get_stats(client, &stats) ==
		       TR_OK);
		if (stats.control_connected == 1U &&
		    stats.data_connections == data_connections &&
		    stats.active_transfers == active_transfers)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for group pair client stats");
}

static void stress_group_pair_replacement_round(
        struct tr_server *server, struct tr_client *clients[2],
        struct group_pair_ctx *ctx, const uint64_t group_ids[2],
        const uint64_t epochs[2], struct tr_conn_handle current_routes[2],
        uint64_t current_transfer_ids[2], uint64_t current_message_ids[2],
        unsigned round)
{
	struct tr_conn_handle next_routes[2];
	uint64_t next_transfer_ids[2] = {UINT64_C(7104) + round,
	                                 UINT64_C(8104) + round};
	uint64_t next_message_ids[2] = {UINT64_C(7209) + (uint64_t)round * 2U,
	                                UINT64_C(7210) + (uint64_t)round * 2U};
	uint64_t offer_ids[2] = {UINT64_C(8107) + (uint64_t)round * 2U,
	                         UINT64_C(8108) + (uint64_t)round * 2U};
	uint64_t ready_ids[2] = {UINT64_C(7304) + round,
	                         UINT64_C(8304) + round};
	struct tr_transport_bytes payload = {(const uint8_t *)"S", 1U};
	unsigned expected_accepts = 8U + round * 2U;
	unsigned expected_event_count = 3U + round;
	unsigned expected_message_count = 4U + round;
	unsigned i;

	abort_group_pair_data_routes_concurrently(current_routes);
	wait_group_pair_counter(ctx, &ctx->data_events[0],
	                        expected_event_count);
	wait_group_pair_counter(ctx, &ctx->data_events[1],
	                        expected_event_count);
	wait_group_pair_client_stats(clients[0], 0U, 0U);
	wait_group_pair_client_stats(clients[1], 0U, 0U);
	wait_group_pair_server_stats(server, 2U, 0U, 0U);
	assert(tr_client_connection_group_send(
	               clients[0], current_transfer_ids[0],
	               current_message_ids[0], &payload) == TR_ERR_STALE);
	assert(tr_client_connection_group_send(
	               clients[1], current_transfer_ids[1],
	               current_message_ids[1], &payload) == TR_ERR_STALE);

	assert(wait_group_pair_offer(server, group_ids[0], epochs[0],
	                             offer_ids[0]) == TR_OK);
	wait_data_accepts(server, expected_accepts - 1U);
	assert(wait_group_pair_offer(server, group_ids[1], epochs[1],
	                             offer_ids[1]) == TR_OK);
	wait_data_accepts(server, expected_accepts);
	for (i = 0U; i < 2U; ++i) {
		assert(tr_server_connection_group_send_transfer_ready(
		               server, group_ids[i], epochs[i],
		               next_transfer_ids[i], ready_ids[i]) == TR_OK);
		wait_group_pair_client_stats(clients[i], 1U, 1U);
	}

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = next_message_ids[0];
	group_route_probe_message_ids[1] = next_message_ids[1];
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	for (i = 0U; i < 2U; ++i)
		assert(tr_client_connection_group_send(
		               clients[i], next_transfer_ids[i],
		               next_message_ids[i], &payload) == TR_OK);
	for (i = 0U; i < 2U; ++i)
		wait_group_pair_counter(ctx, &ctx->data_messages[i],
		                        expected_message_count);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	next_routes[0] = group_route_probe_connections[0];
	next_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	for (i = 0U; i < 2U; ++i) {
		assert(next_routes[i].reactor == current_routes[i].reactor);
		assert(next_routes[i].slot != current_routes[i].slot ||
		       next_routes[i].generation !=
		               current_routes[i].generation);
		current_routes[i] = next_routes[i];
		current_transfer_ids[i] = next_transfer_ids[i];
		current_message_ids[i] = next_message_ids[i];
	}
	wait_group_pair_server_stats(server, 2U, 2U, 2U);
}

static void wait_group_data_tx_blocked(void)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&group_data_tx_probe_lock);
	while (group_data_tx_blocked_attempts == 0U && ret == 0)
		ret = pthread_cond_timedwait(&group_data_tx_probe_cond,
		                             &group_data_tx_probe_lock,
		                             &deadline);
	pthread_mutex_unlock(&group_data_tx_probe_lock);
	assert(ret == 0);
}

static void
test_public_connection_group_client_destroy_with_data_tx_inflight(void)
{
	static const uint32_t payload_len = 8U * 1024U * 1024U;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_transport_bytes bytes;
	uint8_t *payload;
	uint16_t group_port = 0U;

	atomic_store(&group_capability_frames_sent, 0U);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.limits.max_frame_payload_bytes = 64U * 1024U;
	server_config.limits.max_message_bytes = payload_len;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.limits.max_frame_payload_bytes = 64U * 1024U;
	client_config.limits.max_message_bytes = payload_len;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 1U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(5100)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(atomic_load(&group_capability_frames_sent) == 0U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 6001U,
	               UINT64_C(5101)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);

	pthread_mutex_lock(&group_data_tx_probe_lock);
	group_data_tx_blocked_attempts = 0U;
	pthread_mutex_unlock(&group_data_tx_probe_lock);
	atomic_store(&block_group_data_tx, 1);
	payload = (uint8_t *)malloc(payload_len);
	assert(payload != NULL);
	memset(payload, 0x5a, payload_len);
	bytes.data = payload;
	bytes.len = payload_len;
	assert(tr_client_connection_group_send(client, 6001U, UINT64_C(5102),
	                                       &bytes) == TR_OK);
	free(payload);

	/* 等到 DATA 帧确实进入 Reactor 发送路径并被确定性阻塞。 */
	wait_group_data_tx_blocked();
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(client, &client_stats) ==
	       TR_OK);
	assert(client_stats.send_bytes_inflight == payload_len);

	/* 直接销毁，不预先 close()、begin_drain() 或 wait_drained()。
	 * Client 所有者屏障必须同步释放 DATA TX 队列和组持有负载。 */
	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;
	atomic_store(&block_group_data_tx, 0);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_server(void)
{
	static const uint8_t payload[] = "public-group-data";
	struct tr_server_config config;
	struct tr_server *server = NULL;
	struct tr_server_stats server_stats;
	struct public_group_ctx ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	uint16_t group_port = 0U;
	int control = -1;
	int data = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&config);
	config.max_peers = 1U;
	config.keepalive_interval_ms = 0U;
	config.connection_groups.max_groups = 2U;
	config.connection_groups.max_connections = 4U;
	config.connection_groups.max_data_connections_per_group = 2U;
	config.connection_groups.max_streams_per_group = 8U;
	config.connection_groups.authorize = authorize_group;
	config.connection_groups.on_message = on_group_message;
	config.connection_groups.on_data_event = on_group_data_event;
	config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&config, &server) == TR_OK);
	/* 传输工厂不能成为重新启用 V1 RPC 的后门，且失败不发布监听端口。 */
	uint16_t rejected_port = 1234;
	assert(tr_server_listen(server, "127.0.0.1", 0, &rejected_port) ==
	       TR_ERR_UNSUPPORTED);
	assert(rejected_port == 1234);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);
	assert(tr_server_get_stats(server, &server_stats) == TR_OK);
	assert(server_stats.rpc.executor_threads == 0U);
	assert(server_stats.reassembly_pool.capacity == 0U);

	route = control_route();
	control = connect_loopback(group_port);
	send_route(control, &route);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(wait_data_offer(server, UINT64_C(1001)) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(1001), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(offer.pipeline_id == TEST_GROUP_ID);
	assert(offer.epoch == TEST_GROUP_EPOCH);
	assert(tr_pipeline_control_wire_data_route(
		       &offer, &data_route) == TR_OK);

	data = connect_loopback(group_port);
	send_route(data, &data_route);
	wait_data_accepts(server, 1U);

	/* 物理 DATA 成员关系本身不是传输授权。服务端必须先在
	 * 所有者上建立流到精确 DATA 代次的亲和绑定，并通过
	 * TRANSFER_READY 把该屏障告知对端，之后 DATA 帧才允许进入业务回调。
	 */
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U,
		       UINT64_C(1002)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);

	send_data_message(data, 3001U, UINT64_C(2001),
			  payload, (uint32_t)(sizeof(payload) - 1U));
	wait_counter(&ctx, &ctx.messages, 1U);

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == sizeof(payload) - 1U);
	assert(memcmp(ctx.retained.bytes.data, payload,
		      sizeof(payload) - 1U) == 0);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U) == TR_OK);

	assert(close(data) == 0);
	data = -1;
	wait_counter(&ctx, &ctx.data_events, 1U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(close(control) == 0);
	control = -1;
	assert(tr_server_drain(server, 5000U) == TR_OK);

	/* 保留的 DATA 负载仍拥有一个运行时反应器接收缓冲区。
	 * 终止销毁必须停止或关闭准入，但保持 Runtime/Server 存储存活。
	 */
	assert(tr_server_destroy(server) == TR_ERR_STATE);
	assert(tr_connection_group_message_release(&ctx.retained) == TR_OK);
	assert(tr_connection_group_message_release(&ctx.retained) ==
	       TR_ERR_INVALID);

	/* 最终持有者归还后，同一终止操作可重试。 */
	assert(tr_server_destroy(server) == TR_OK);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_client_control(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connect(client, "127.0.0.1", 9) == TR_ERR_UNSUPPORTED);
	assert(tr_client_connection_group_close(client) == TR_ERR_STATE);

	group.group_id = 0U;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) ==
	       TR_ERR_INVALID);

	group.group_id = TEST_GROUP_ID;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) ==
	       TR_ERR_STATE);
	wait_counter(&ctx, &ctx.authorized, 1U);

	/* 此部分仅拥有 CONTROL。提供的 DATA 预留通过 DATA_CANCEL
	 * 在内部归还；取消到达服务端后，capacity=1 允许新的提议，
	 * 无需暴露索引或代次。
	 */
	assert(wait_data_offer(server, UINT64_C(3001)) == TR_OK);
	assert(wait_data_offer(server, UINT64_C(3002)) == TR_OK);

	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_client_connection_group_close(client) == TR_ERR_STATE);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);

	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void
test_public_connection_group_control_loss_fences_old_epoch_data(void)
{
	static const uint8_t payload[] = "control-loss-data";
	struct tr_server_config server_config;
	struct tr_server *server = NULL;
	struct public_group_ctx ctx;
	struct tr_pipeline_route_preface old_control_route;
	struct tr_pipeline_route_preface old_data_route;
	struct tr_pipeline_route_preface idle_data_route;
	struct tr_pipeline_route_preface reserved_data_route;
	struct tr_pipeline_route_preface new_control_route;
	struct tr_pipeline_route_preface new_data_route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	struct tr_connection_group_server_stats stats;
	uint16_t group_port = 0U;
	int old_control = -1;
	int old_data = -1;
	int idle_data = -1;
	int new_control = -1;
	int stale_data = -1;
	int stale_idle_data = -1;
	int stale_reserved_data = -1;
	int new_data = -1;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 4U;
	server_config.connection_groups.max_data_connections_per_group = 3U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.authorize = authorize_group_dual_epoch;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event =
	        on_group_data_event_dual_epoch;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	old_control_route = control_route();
	old_control = connect_loopback(group_port);
	send_route(old_control, &old_control_route);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_group_pair_offer(server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	                             UINT64_C(1001)) == TR_OK);
	recv_control_message(old_control, UINT64_C(1001), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(tr_pipeline_control_wire_data_route(&offer, &old_data_route) ==
	       TR_OK);

	old_data = connect_loopback(group_port);
	send_route(old_data, &old_data_route);
	wait_counter(&ctx, &ctx.authorized, 2U);
	wait_data_accepts(server, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 3001U,
	               UINT64_C(1002)) == TR_OK);
	recv_control_message(old_control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	send_data_message(old_data, 3001U, UINT64_C(2001), payload,
	                  (uint32_t)(sizeof(payload) - 1U));
	wait_counter(&ctx, &ctx.messages, 1U);
	assert(tr_connection_group_message_release(&ctx.retained) == TR_OK);
	wait_group_pair_server_stats(server, 1U, 1U, 1U);

	/* 为第二条已附着但没有 transfer 的 DATA 建立不同成员状态。 */
	assert(wait_group_pair_offer(server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	                             UINT64_C(1003)) == TR_OK);
	recv_control_message(old_control, UINT64_C(1003), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(tr_pipeline_control_wire_data_route(&offer, &idle_data_route) ==
	       TR_OK);
	idle_data = connect_loopback(group_port);
	send_route(idle_data, &idle_data_route);
	wait_counter(&ctx, &ctx.authorized, 3U);
	wait_data_accepts(server, 2U);

	/* 再保留第三条 DATA route，但不连接 socket。 */
	assert(wait_group_pair_offer(server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
	                             UINT64_C(1004)) == TR_OK);
	recv_control_message(old_control, UINT64_C(1004), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(tr_pipeline_control_wire_data_route(
	               &offer, &reserved_data_route) == TR_OK);
	wait_group_pair_server_stats(server, 1U, 2U, 1U);

	/* CONTROL 的真实 TCP 关闭必须退役活动 DATA 和 transfer affinity。 */
	assert(close(old_control) == 0);
	old_control = -1;
	wait_counter(&ctx, &ctx.data_events, 2U);
	expect_socket_closed(old_data);
	assert(close(old_data) == 0);
	old_data = -1;
	expect_socket_closed(idle_data);
	assert(close(idle_data) == 0);
	idle_data = -1;
	wait_group_pair_server_stats(server, 0U, 0U, 0U);
	memset(&stats, 0, sizeof(stats));
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.connections_current == 0U);

	/* 新 epoch 接管相同 group ID 后，旧 DATA 前导仍须被拒绝。 */
	new_control_route = control_route();
	new_control_route.epoch = TEST_GROUP_EPOCH + 1U;
	new_control = connect_loopback(group_port);
	send_route(new_control, &new_control_route);
	wait_counter(&ctx, &ctx.authorized, 4U);
	assert(wait_group_pair_offer(server, TEST_GROUP_ID,
	                             TEST_GROUP_EPOCH + 1U,
	                             UINT64_C(1005)) == TR_OK);
	recv_control_message(new_control, UINT64_C(1005), &offer);
	assert(tr_pipeline_control_wire_data_route(&offer, &new_data_route) ==
	       TR_OK);
	assert(new_data_route.epoch == TEST_GROUP_EPOCH + 1U);
	assert(new_data_route.member_generation !=
	       old_data_route.member_generation);

	stale_data = connect_loopback(group_port);
	send_route(stale_data, &old_data_route);
	expect_socket_closed(stale_data);
	assert(close(stale_data) == 0);
	stale_data = -1;
	stale_idle_data = connect_loopback(group_port);
	send_route(stale_idle_data, &idle_data_route);
	expect_socket_closed(stale_idle_data);
	assert(close(stale_idle_data) == 0);
	stale_idle_data = -1;
	stale_reserved_data = connect_loopback(group_port);
	send_route(stale_reserved_data, &reserved_data_route);
	expect_socket_closed(stale_reserved_data);
	assert(close(stale_reserved_data) == 0);
	stale_reserved_data = -1;
	wait_group_pair_server_stats(server, 1U, 0U, 0U);

	new_data = connect_loopback(group_port);
	send_route(new_data, &new_data_route);
	wait_counter(&ctx, &ctx.authorized, 5U);
	wait_data_accepts(server, 3U);
	wait_group_pair_server_stats(server, 1U, 1U, 0U);

	assert(close(new_control) == 0);
	new_control = -1;
	wait_counter(&ctx, &ctx.data_events, 3U);
	expect_socket_closed(new_data);
	assert(close(new_data) == 0);
	new_data = -1;
	wait_group_pair_server_stats(server, 0U, 0U, 0U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_control_loss_preserves_peer_group(void)
{
	static const uint8_t survivor_payload = 0x52U;
	struct tr_server_config server_config;
	struct tr_server *server = NULL;
	struct group_pair_ctx ctx;
	struct tr_pipeline_route_preface control_routes[2];
	struct tr_pipeline_route_preface data_routes[2];
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	struct tr_connection_group_server_stats stats;
	const uint64_t group_ids[2] = {TEST_GROUP_PAIR_ID_FIRST,
	                               TEST_GROUP_PAIR_ID_SECOND};
	const uint64_t epochs[2] = {TEST_GROUP_PAIR_EPOCH_FIRST,
	                            TEST_GROUP_PAIR_EPOCH_SECOND};
	const uint64_t offer_ids[2] = {UINT64_C(9101), UINT64_C(9102)};
	const uint64_t ready_ids[2] = {UINT64_C(9301), UINT64_C(9302)};
	const uint32_t stream_ids[2] = {7101U, 8101U};
	const uint64_t message_ids[2] = {UINT64_C(7201), UINT64_C(7202)};
	int controls[2] = {-1, -1};
	int data[2] = {-1, -1};
	uint16_t group_port = 0U;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 2U;
	server_config.connection_groups.max_connections = 4U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group_pair;
	server_config.connection_groups.on_message = on_group_pair_message;
	server_config.connection_groups.on_data_event =
	        on_group_pair_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	for (i = 0U; i < 2U; ++i) {
		control_routes[i] = control_route();
		control_routes[i].pipeline_id = group_ids[i];
		control_routes[i].epoch = epochs[i];
		controls[i] = connect_loopback(group_port);
		send_route(controls[i], &control_routes[i]);
		wait_group_pair_counter(&ctx, &ctx.authorized[i], 1U);
	}

	for (i = 0U; i < 2U; ++i) {
		assert(wait_group_pair_offer(server, group_ids[i], epochs[i],
		                             offer_ids[i]) == TR_OK);
		recv_control_message(controls[i], offer_ids[i], &offer);
		assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
		assert(tr_pipeline_control_wire_data_route(
		               &offer, &data_routes[i]) == TR_OK);
		data[i] = connect_loopback(group_port);
		send_route(data[i], &data_routes[i]);
		wait_group_pair_counter(&ctx, &ctx.authorized[i], 2U);
		wait_data_accepts(server, i + 1U);
		assert(tr_server_connection_group_send_transfer_ready(
		               server, group_ids[i], epochs[i], stream_ids[i],
		               ready_ids[i]) == TR_OK);
		recv_control_message(controls[i], ready_ids[i], &ready);
		assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
		assert(ready.stream_id == stream_ids[i]);
	}
	wait_group_pair_server_stats(server, 2U, 2U, 2U);

	/* 真实关闭第一组 CONTROL，第二组的 CONTROL、DATA 与 transfer 必须存活。
	 */
	assert(close(controls[0]) == 0);
	controls[0] = -1;
	wait_group_pair_counter(&ctx, &ctx.data_events[0], 1U);
	expect_socket_closed(data[0]);
	assert(close(data[0]) == 0);
	data[0] = -1;
	wait_group_pair_server_stats(server, 1U, 1U, 1U);
	memset(&stats, 0, sizeof(stats));
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.connections_current == 2U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.data_events[1] == 0U);
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_server_connection_group_send_transfer_ready(
	               server, group_ids[1], epochs[1], 8102U,
	               UINT64_C(9303)) == TR_OK);
	recv_control_message(controls[1], UINT64_C(9303), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 8102U);
	send_data_message(data[1], stream_ids[1], message_ids[1],
	                  &survivor_payload,
	                  (uint32_t)sizeof(survivor_payload));
	wait_group_pair_counter(&ctx, &ctx.data_messages[1], 1U);
	wait_group_pair_server_stats(server, 1U, 1U, 2U);

	assert(close(controls[1]) == 0);
	controls[1] = -1;
	wait_group_pair_counter(&ctx, &ctx.data_events[1], 1U);
	expect_socket_closed(data[1]);
	assert(close(data[1]) == 0);
	data[1] = -1;
	wait_group_pair_server_stats(server, 0U, 0U, 0U);
	memset(&stats, 0, sizeof(stats));
	assert(tr_server_connection_group_get_stats(server, &stats) == TR_OK);
	assert(stats.connections_current == 0U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_client_group_destroy_retry_preserves_control(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	atomic_store(&group_control_close_attempts, 0U);
	atomic_store(&fail_group_control_close_once, 1);
	assert(tr_client_destroy(client) == TR_ERR_SYS);

	/* 失败的终局屏障不能清空 CONTROL 能力凭证，销毁必须可重试。 */
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(client, &client_stats) ==
	       TR_OK);
	assert(client_stats.control_connected == 1U);
	assert(atomic_load(&group_control_close_attempts) == 1U);

	assert(tr_client_destroy(client) == TR_OK);
	client = NULL;
	assert(atomic_load(&group_control_close_attempts) == 2U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_client_group_close_retry_preserves_control(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	atomic_store(&group_control_close_attempts, 0U);
	atomic_store(&fail_group_control_close_once, 1);
	assert(tr_client_connection_group_close(client) == TR_ERR_SYS);

	/* 失败的显式关闭必须保留 CONTROL 能力凭证，后续调用才能重试。 */
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(client, &client_stats) ==
	       TR_OK);
	assert(client_stats.control_connected == 1U);
	assert(atomic_load(&group_control_close_attempts) == 1U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(atomic_load(&group_control_close_attempts) == 2U);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(client, &client_stats) ==
	       TR_OK);
	assert(client_stats.control_connected == 0U);

	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct client_group_wait_probe {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_client *client;
	uint32_t timeout_ms;
	int entered;
	int done;
	int ret;
};

static void *client_group_wait_main(void *arg)
{
	struct client_group_wait_probe *probe =
		(struct client_group_wait_probe *)arg;

	pthread_mutex_lock(&probe->lock);
	probe->entered = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);

	probe->ret = tr_client_connection_group_wait_drained(
	        probe->client,
	        probe->timeout_ms != 0U ? probe->timeout_ms : 5000U);

	pthread_mutex_lock(&probe->lock);
	probe->done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
	return NULL;
}

static void wait_client_group_probe(
	struct client_group_wait_probe *probe, int *field)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe->lock);
	while (!*field && ret == 0)
		ret = pthread_cond_timedwait(
			&probe->cond, &probe->lock, &deadline);
	assert(*field);
	pthread_mutex_unlock(&probe->lock);
}

struct group_connector_complete_request {
	tr_connector_complete_cb complete;
	void *callback_arg;
};

static int group_connector_complete_failure_on_owner(void *arg)
{
	struct group_connector_complete_request *request =
	        (struct group_connector_complete_request *)arg;

	request->complete(TR_ERR_SYS, -1, 0U, request->callback_arg);
	return TR_OK;
}

static void wait_group_connector_start(unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&group_connector_probe_lock);
	while (group_connector_probe_starts < target && ret == 0)
		ret = pthread_cond_timedwait(&group_connector_probe_cond,
		                             &group_connector_probe_lock,
		                             &deadline);
	assert(group_connector_probe_starts >= target);
	pthread_mutex_unlock(&group_connector_probe_lock);
}

static void dispatch_group_connector_failure(void)
{
	struct group_connector_complete_request request;
	struct tr_reactor *owner;

	pthread_mutex_lock(&group_connector_probe_lock);
	owner = group_connector_probe_owner;
	request.complete = group_connector_probe_complete;
	request.callback_arg = group_connector_probe_arg;
	pthread_mutex_unlock(&group_connector_probe_lock);
	assert(owner != NULL);
	assert(request.complete != NULL);
	assert(tr_reactor_call(owner, group_connector_complete_failure_on_owner,
	                       &request) == TR_OK);
}

struct server_group_wait_probe {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_server *server;
	int entered;
	int done;
	int ret;
};

static void *server_group_wait_main(void *arg)
{
	struct server_group_wait_probe *probe =
		(struct server_group_wait_probe *)arg;

	pthread_mutex_lock(&probe->lock);
	probe->entered = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);

	probe->ret = tr_server_connection_group_wait_drained(
		probe->server, 5000U);

	pthread_mutex_lock(&probe->lock);
	probe->done = 1;
	pthread_cond_broadcast(&probe->cond);
	pthread_mutex_unlock(&probe->lock);
	return NULL;
}

static void wait_server_group_probe(
	struct server_group_wait_probe *probe, int *field)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 5;
	pthread_mutex_lock(&probe->lock);
	while (!*field && ret == 0)
		ret = pthread_cond_timedwait(
			&probe->cond, &probe->lock, &deadline);
	assert(*field);
	pthread_mutex_unlock(&probe->lock);
}

static void test_public_connection_group_client_data_offer(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	struct client_group_wait_probe wait_probe;
	struct server_group_wait_probe server_wait;
	struct tr_test_drain_deadline_probe deadline_probe;
	pthread_t wait_thread;
	pthread_t server_wait_thread;
	uint16_t group_port = 0U;

	atomic_store(&group_capability_frames_sent, 0U);
	atomic_store(&group_compressed_multiframe_submissions, 0U);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 8U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.limits.max_frame_payload_bytes = 64U;
	client_config.limits.max_message_bytes = 130U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 3U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready =
		on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(wait_data_offer(server, UINT64_C(4001)) == TR_OK);
	wait_data_accepts(server, 1U);
	wait_group_capability_frames(2U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 0U);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 2U);
	assert(server_stats.data_connections_current == 1U);
	assert(server_stats.active_transfers == 0U);
	assert(server_stats.data_accepts == 1U);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.group.group_id == TEST_GROUP_ID);
	assert(client_stats.group.epoch == TEST_GROUP_EPOCH);
	assert(client_stats.control_connected == 1U);
	assert(client_stats.draining == 0U);
	assert(client_stats.data_connections == 1U);
	assert(client_stats.active_transfers == 0U);
	assert(client_stats.active_transfer_limit == 3U);
	assert(client_stats.send_bytes_inflight == 0U);
	assert(client_stats.send_bytes_limit == 130U);

	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(4002)) == TR_AGAIN);

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_ERR_STALE);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(4101)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5001U);
	assert(ctx.ready_message_id == UINT64_C(4101));
	pthread_mutex_unlock(&ctx.lock);

	{
		uint8_t payload[130];
		uint8_t retry = 0xa5U;
		struct tr_transport_bytes bytes;
		struct tr_transport_bytes retry_bytes;
		uint32_t i;

		for (i = 0; i < sizeof(payload); ++i)
			payload[i] = (uint8_t)(i ^ 0x5aU);
		bytes.data = payload;
		bytes.len = 131U;
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4200), &bytes) ==
		       TR_ERR_BAD_LENGTH);
		bytes.len = (uint32_t)sizeof(payload);
		assert(tr_client_connection_group_send(
			       client, 9999U, UINT64_C(4201), &bytes) ==
		       TR_ERR_STALE);
		assert(ctx.ready_send_first_ret == TR_OK);
		assert(ctx.ready_send_second_ret == TR_AGAIN);

		wait_counter(&ctx, &ctx.messages, 3U);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.client_send_fragments == 3U);
		assert(ctx.client_send_first == 1U);
		assert(ctx.client_send_last == 1U);
		assert(ctx.client_send_len == 130U);
		for (i = 0; i < 130U; ++i)
			assert(ctx.client_send_bytes[i] == (uint8_t)(i ^ 0x5aU));
		pthread_mutex_unlock(&ctx.lock);

		retry_bytes.data = &retry;
		retry_bytes.len = 1U;
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4202),
			       &retry_bytes) == TR_OK);
		wait_counter(&ctx, &ctx.retry_messages, 1U);
	}
	{
		uint8_t source[128];
		uint8_t expected[128];
		struct tr_transport_bytes bytes;
		uint32_t i;

		for (i = 0; i < sizeof(source); ++i)
			expected[i] = source[i] = (uint8_t)(i * 73U + 19U);
		bytes.data = source;
		bytes.len = sizeof(source);
		assert(tr_client_connection_group_send_with_compression(
		               client, 5001U, UINT64_C(4301), &bytes,
		               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
		memset(source, 0, sizeof(source));
		wait_counter(&ctx, &ctx.compressed_done, 1U);

		pthread_mutex_lock(&ctx.lock);
		assert(ctx.compressed_frames == 1U);
		assert(ctx.compressed_first == 1U);
		assert(ctx.compressed_last == 1U);
		assert(ctx.compressed_decoded_len == sizeof(expected));
		assert(memcmp(ctx.compressed_decoded, expected,
		              sizeof(expected)) == 0);
		pthread_mutex_unlock(&ctx.lock);

		bytes.data = source;
		bytes.len = sizeof(source);
		for (i = 0; i < sizeof(source); ++i)
			source[i] = (uint8_t)(i * 73U + 19U);
		assert(tr_client_connection_group_send_with_compression(
		               client, 5001U, UINT64_C(4302), &bytes,
		               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
		wait_counter(&ctx, &ctx.compressed_done, 2U);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.compressed_frames == 2U);
		assert(ctx.compressed_retained.bytes.len == sizeof(expected));
		assert(memcmp(ctx.compressed_retained.bytes.data, expected,
		              sizeof(expected)) == 0);
		pthread_mutex_unlock(&ctx.lock);

		/* Hold the first receive decode while a burst of messages
		 * arrives on the same stream. Task capacity must not be limited
		 * by stream slots.
		 */
		pthread_mutex_lock(&group_zstd_gate_lock);
		group_zstd_gate_entered = 0;
		group_zstd_gate_released = 0;
		pthread_mutex_unlock(&group_zstd_gate_lock);
		atomic_store(&hold_group_zstd_decompression, 1);
		assert(tr_client_connection_group_send_with_compression(
		               client, 5001U, UINT64_C(4303), &bytes,
		               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
		wait_group_zstd_decompression_entered();
		for (i = 1U; i < 8U; ++i) {
			struct timespec pause = {0, 1000000L};
			unsigned attempt;
			int ret = TR_AGAIN;

			for (attempt = 0U; attempt < 5000U && ret == TR_AGAIN;
			     ++attempt) {
				ret = tr_client_connection_group_send_with_compression(
				        client, 5001U, UINT64_C(4303) + i,
				        &bytes,
				        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
				if (ret == TR_AGAIN)
					(void)nanosleep(&pause, NULL);
			}
			assert(ret == TR_OK);
		}
		release_group_zstd_gate();
		wait_counter(&ctx, &ctx.compressed_done, 10U);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.data_events == 0U);
		for (i = 0U; i < 10U; ++i)
			assert(ctx.compressed_message_ids[i] ==
			       UINT64_C(4301) + i);
		pthread_mutex_unlock(&ctx.lock);
		assert(atomic_load(&group_compressed_multiframe_submissions) >=
		       2U);
	}

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_OK);
	{
		struct tr_transport_bytes empty = { NULL, 0U };
		assert(tr_client_connection_group_send(
			       client, 5001U, UINT64_C(4202), &empty) ==
		       TR_ERR_STALE);
	}
	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_ERR_STALE);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);

	/* 释放使语义流标识在两端均可复用。 */
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(4102)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5001U);
	assert(ctx.ready_message_id == UINT64_C(4102));
	pthread_mutex_unlock(&ctx.lock);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U);
	assert(client_stats.send_bytes_inflight == 0U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.active_transfers == 1U);
	assert(server_stats.data_connections_current == 1U);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_STATE);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_STATE);

	/* 客户端排空是单调的本地准入屏障。已有 READY 工作仍可用，
	 * 但 begin_drain() 后观察到的 READY 不得创建新本地亲和绑定。
	 * 服务端独立拥有该迟到亲和绑定。
	 */
	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.active_transfers == 1U);
	/* 真实客户端组仍有活跃传输：定点拦截有限条件等待，
	 * 确认超时由公共辅助函数计算且原始 CLOCK_MONOTONIC timespec 直传。
	 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, 10U, 0);
	assert(tr_client_connection_group_wait_drained(
		       client, 10U) == TR_ERR_TIMEOUT);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 1U);

	/* UINT32_MAX 毫秒也必须保持有限，且原绝对截止时间不可重算。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, UINT32_MAX, 0);
	assert(tr_client_connection_group_wait_drained(
		       client, UINT32_MAX) == TR_ERR_TIMEOUT);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 1U);

	/* 时钟或截止时间初始化失败必须显式返回；不能误入条件等待。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, 10U, 1);
	assert(tr_client_connection_group_wait_drained(
		       client, 10U) == TR_ERR_SYS);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 0U);

	/* 让等待者注册与 release_transfer()
	 * 竞争。此前的限时等待已证明阻塞条件变量路径；
	 * 这里两种顺序都是有意的，且必须安全：
	 *
	 *   等待者注册 -> 释放发布并唤醒
	 *   释放发布 -> 等待者观察已完成代次
	 *
	 * 两种顺序都不得丢失唤醒。
	 */
	memset(&wait_probe, 0, sizeof(wait_probe));
	wait_probe.client = client;
	assert(pthread_mutex_init(&wait_probe.lock, NULL) == 0);
	assert(pthread_cond_init(&wait_probe.cond, NULL) == 0);
	assert(pthread_create(
		       &wait_thread, NULL, client_group_wait_main,
		       &wait_probe) == 0);
	wait_client_group_probe(&wait_probe, &wait_probe.entered);

	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U,
		       UINT64_C(4300)) == TR_OK);
	{
		struct timespec pause = { 0, 100000000L };
		(void)nanosleep(&pause, NULL);
	}
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.transfer_ready == 2U);
	pthread_mutex_unlock(&ctx.lock);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.active_transfers == 2U);

	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(4301)) == TR_ERR_CLOSED);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5003U,
		       UINT64_C(4302)) == TR_ERR_CLOSED);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 1U);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 2U);
	assert(server_stats.active_transfers == 2U);
	/* 活跃组与连接使事件驱动的等待者保持阻塞。 */
	assert(tr_server_connection_group_wait_drained(
		       server, 10U) == TR_ERR_TIMEOUT);

	assert(tr_client_connection_group_release_transfer(
		       client, 5001U) == TR_OK);
	wait_client_group_probe(&wait_probe, &wait_probe.done);
	assert(pthread_join(wait_thread, NULL) == 0);
	assert(wait_probe.ret == TR_OK);
	pthread_cond_destroy(&wait_probe.cond);
	pthread_mutex_destroy(&wait_probe.lock);

	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U) == TR_OK);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.active_transfers == 0U);
	assert(client_stats.send_bytes_inflight == 0U);
	assert(client_stats.data_connections == 1U);

	/* 让等待者注册与客户端终止关闭竞争。两种顺序都有效：
	 * 已注册等待者必须被广播唤醒，迟到等待者必须观察到已完成代次。不得丢失唤醒。
	 */
	memset(&server_wait, 0, sizeof(server_wait));
	server_wait.server = server;
	assert(pthread_mutex_init(&server_wait.lock, NULL) == 0);
	assert(pthread_cond_init(&server_wait.cond, NULL) == 0);
	assert(pthread_create(
		       &server_wait_thread, NULL,
		       server_group_wait_main, &server_wait) == 0);
	wait_server_group_probe(&server_wait, &server_wait.entered);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	wait_server_group_probe(&server_wait, &server_wait.done);
	assert(pthread_join(server_wait_thread, NULL) == 0);
	assert(server_wait.ret == TR_OK);
	pthread_cond_destroy(&server_wait.cond);
	pthread_mutex_destroy(&server_wait.lock);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.draining == 1U);
	assert(server_stats.groups_current == 0U);
	assert(server_stats.connections_current == 0U);
	assert(server_stats.data_connections_current == 0U);
	assert(server_stats.active_transfers == 0U);

	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	/* 已转移的解码消息继续持有监听器预算，必须先等应用释放再销毁。 */
	assert(tr_server_destroy(server) == TR_ERR_STATE);
	assert(tr_connection_group_message_release(&ctx.compressed_retained) ==
	       TR_OK);
	wait_server_destroy_after_release(server);

	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_multi_data_drain(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	struct tr_conn_handle data_routes[2];
	uint8_t first_payload[128];
	uint8_t second_payload[128];
	uint8_t raw_peer_payload = 0x34U;
	uint8_t inflight_payload = 0x33U;
	struct tr_transport_bytes first_bytes;
	struct tr_transport_bytes second_bytes;
	struct tr_transport_bytes raw_peer_bytes = {&raw_peer_payload, 1U};
	struct tr_transport_bytes inflight_bytes = {&inflight_payload, 1U};
	uint16_t group_port = 0U;
	uint32_t i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	for (i = 0U; i < sizeof(first_payload); ++i) {
		first_payload[i] = (uint8_t)(i * 73U + 19U);
		second_payload[i] = (uint8_t)(i * 53U + 71U);
	}
	first_bytes.data = first_payload;
	first_bytes.len = (uint32_t)sizeof(first_payload);
	second_bytes.data = second_payload;
	second_bytes.len = (uint32_t)sizeof(second_payload);
	atomic_store(&group_multi_data_compressed_submissions, 0U);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 2U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 2U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 2U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 4096U;
	client_config.connection_groups.on_transfer_ready =
		on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6101)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_data_offer(server, UINT64_C(6102)) == TR_OK);
	wait_data_accepts(server, 2U);

	/* 服务端按 DATA 成员轮转分配两个传输；客户端校验并接纳各自代次。 */
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(6301)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5001U &&
	       ctx.ready_message_id == UINT64_C(6301));
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U,
		       UINT64_C(6302)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.ready_stream_id == 5002U &&
	       ctx.ready_message_id == UINT64_C(6302));
	pthread_mutex_unlock(&ctx.lock);

	/* 记录真实 DATA 帧的物理连接，确认两个传输落在不同成员。 */
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(6611);
	group_route_probe_message_ids[1] = UINT64_C(6612);
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send_with_compression(
	               client, 5001U, UINT64_C(6611), &first_bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	/* 第一成员的应用保留正文仍持有接收缓冲时，第二 DATA 成员必须能
	 * 独立进入真实 Server 消息回调并释放自己的正文。 */
	wait_counter(&ctx, &ctx.multi_drain_messages, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == sizeof(first_payload) &&
	       memcmp(ctx.retained.bytes.data, first_payload,
	              sizeof(first_payload)) == 0);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_client_connection_group_send_with_compression(
	               client, 5002U, UINT64_C(6612), &second_bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_counter(&ctx, &ctx.multi_drain_messages, 2U);
	assert(atomic_load(&group_multi_data_compressed_submissions) == 2U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].reactor == data_routes[1].reactor &&
	       (data_routes[0].slot != data_routes[1].slot ||
	        data_routes[0].generation != data_routes[1].generation));
	/* 解压缓冲仍被持有时，第二成员再接收一条 RAW 消息也必须进展。 */
	assert(tr_client_connection_group_send(client, 5002U, UINT64_C(6613),
	                                       &raw_peer_bytes) == TR_OK);
	wait_counter(&ctx, &ctx.multi_drain_messages, 3U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == sizeof(first_payload) &&
	       memcmp(ctx.retained.bytes.data, first_payload,
	              sizeof(first_payload)) == 0);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_connection_group_message_release(&ctx.retained) == TR_OK);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.data_connections == 2U &&
	       client_stats.active_transfers == 2U &&
	       client_stats.send_bytes_inflight == 0U);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.data_connections_current == 2U &&
	       server_stats.active_transfers == 2U);

	/* 已接受的 DATA 发送即使遇到 socket EAGAIN 也由 drain 等待；
	 * 先将字节留在 Reactor TX 队列，再释放两个逻辑 transfer。 */
	pthread_mutex_lock(&group_data_tx_probe_lock);
	group_data_tx_blocked_attempts = 0U;
	pthread_mutex_unlock(&group_data_tx_probe_lock);
	atomic_store(&block_group_data_tx, 1);
	assert(tr_client_connection_group_send(
		       client, 5001U, UINT64_C(6203), &inflight_bytes) == TR_OK);
	wait_group_data_tx_blocked();
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 2U &&
	       client_stats.send_bytes_inflight == 1U);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(6103)) == TR_ERR_CLOSED);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5003U,
		       UINT64_C(6303)) == TR_ERR_CLOSED);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	/* 只释放一个 DATA 成员上的传输时，两端排空仍等待另一个传输。 */
	assert(tr_client_connection_group_release_transfer(client, 5001U) ==
	       TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.active_transfers == 2U &&
	       server_stats.data_connections_current == 2U);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	/* 两个 transfer 都释放后，唯一剩余的 Client drain 责任是
	 * 仍在队列中的已接受 DATA 字节；放行 Reactor 后它才完成。 */
	assert(tr_client_connection_group_release_transfer(client, 5002U) ==
	       TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 0U &&
	       client_stats.send_bytes_inflight == 1U);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);
	atomic_store(&block_group_data_tx, 0);
	wait_counter(&ctx, &ctx.inflight_drain_messages, 1U);
	wait_group_client_send_bytes_empty(client);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U) == TR_OK);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 0U &&
	       client_stats.data_connections == 2U);

	/* CONTROL 关闭带走两条已附着 DATA 连接后，服务端才报告完整排空。 */
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 2U);
	assert(tr_server_connection_group_wait_drained(
		       server, 5000U) == TR_OK);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 0U &&
	       server_stats.connections_current == 0U &&
	       server_stats.data_connections_current == 0U &&
	       server_stats.active_transfers == 0U);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	/* ZSTD 回调返回后仍可能有解码任务完成清理；销毁按既有重试屏障收敛。 */
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: multi-DATA drain passed");
}

static void test_public_connection_group_large_decode_hold_raw_peer(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_conn_handle data_routes[2];
	uint8_t *decoded_payload;
	uint8_t raw_payload = 0x6aU;
	struct tr_transport_bytes compressed_bytes;
	struct tr_transport_bytes raw_bytes = {&raw_payload, 1U};
	uint16_t group_port = 0U;
	unsigned i;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	atomic_store(&group_large_compressed_submissions, 0U);
	atomic_store(&group_large_raw_submissions, 0U);

	tr_server_config_init(&server_config);
	server_config.limits.max_frame_payload_bytes = 64U * 1024U;
	server_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 2U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages =
	        TEST_GROUP_BUDGET_HOLD_COUNT + 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.limits.max_frame_payload_bytes = 64U * 1024U;
	client_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 2U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 4096U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6761)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_data_offer(server, UINT64_C(6762)) == TR_OK);
	wait_data_accepts(server, 2U);

	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5101U,
	               UINT64_C(6771)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U,
	               UINT64_C(6772)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(6711);
	group_route_probe_message_ids[1] = TEST_GROUP_RAW_BURST_FIRST_ID;
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);

	decoded_payload = (uint8_t *)malloc(TEST_GROUP_LARGE_DECODED_BYTES);
	assert(decoded_payload != NULL);
	memset(decoded_payload, 0x5a, TEST_GROUP_LARGE_DECODED_BYTES);
	compressed_bytes.data = decoded_payload;
	compressed_bytes.len = TEST_GROUP_LARGE_DECODED_BYTES;
	assert(tr_client_connection_group_send_with_compression(
	               client, 5101U, UINT64_C(6711), &compressed_bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	free(decoded_payload);
	wait_counter(&ctx, &ctx.multi_drain_messages, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == TEST_GROUP_LARGE_DECODED_BYTES);
	for (uint32_t i = 0U; i < ctx.retained.bytes.len; ++i)
		assert(ctx.retained.bytes.data[i] == 0x5aU);
	pthread_mutex_unlock(&ctx.lock);

	/* 连续持有其余真实解码输出，实际应用持有正文约 5.06 MiB；
	 * 不通过测试夹具手工预约预算。 */
	for (i = 0U; i < TEST_GROUP_BUDGET_HOLD_COUNT; ++i) {
		uint32_t decoded_bytes =
		        i + 1U == TEST_GROUP_BUDGET_HOLD_COUNT
		                ? TEST_GROUP_BUDGET_TAIL_BYTES
		                : TEST_GROUP_LARGE_DECODED_BYTES;
		uint8_t expected_byte = (uint8_t)(0x61U + i);

		decoded_payload = (uint8_t *)malloc(decoded_bytes);
		assert(decoded_payload != NULL);
		memset(decoded_payload, expected_byte, decoded_bytes);
		compressed_bytes.data = decoded_payload;
		compressed_bytes.len = decoded_bytes;
		assert(tr_client_connection_group_send_with_compression(
		               client, 5101U, UINT64_C(6721) + i,
		               &compressed_bytes,
		               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
		free(decoded_payload);
		wait_counter(&ctx, &ctx.budget_hold_messages, i + 1U);
	}

	/* 多条大解码输出仍由应用持有；另一 DATA 成员必须完成一个有界
	 * RAW 突发，而不是只证明单条消息偶然穿过。 */
	for (i = 0U; i < TEST_GROUP_RAW_BURST_COUNT; ++i) {
		raw_payload = (uint8_t)(0x6aU + i);
		raw_bytes.data = &raw_payload;
		assert(tr_client_connection_group_send(
		               client, 5102U, TEST_GROUP_RAW_BURST_FIRST_ID + i,
		               &raw_bytes) == TR_OK);
	}
	wait_counter(&ctx, &ctx.raw_burst_messages, TEST_GROUP_RAW_BURST_COUNT);
	wait_counter(&ctx, &ctx.multi_drain_messages,
	             1U + TEST_GROUP_RAW_BURST_COUNT);
	assert(atomic_load(&group_large_compressed_submissions) == 1U);
	assert(atomic_load(&group_large_raw_submissions) ==
	       TEST_GROUP_RAW_BURST_COUNT);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].reactor == data_routes[1].reactor &&
	       (data_routes[0].slot != data_routes[1].slot ||
	        data_routes[0].generation != data_routes[1].generation));
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.retained.bytes.len == TEST_GROUP_LARGE_DECODED_BYTES);
	for (uint32_t i = 0U; i < ctx.retained.bytes.len; ++i)
		assert(ctx.retained.bytes.data[i] == 0x5aU);
	assert(ctx.budget_hold_messages == TEST_GROUP_BUDGET_HOLD_COUNT);
	for (unsigned message = 0U; message < TEST_GROUP_BUDGET_HOLD_COUNT;
	     ++message) {
		uint32_t expected_bytes =
		        message + 1U == TEST_GROUP_BUDGET_HOLD_COUNT
		                ? TEST_GROUP_BUDGET_TAIL_BYTES
		                : TEST_GROUP_LARGE_DECODED_BYTES;
		uint8_t expected_byte = (uint8_t)(0x61U + message);

		assert(ctx.budget_retained[message].bytes.len ==
		       expected_bytes);
		for (uint32_t byte = 0U; byte < expected_bytes; ++byte)
			assert(ctx.budget_retained[message].bytes.data[byte] ==
			       expected_byte);
	}
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_connection_group_message_release(&ctx.retained) == TR_OK);
	for (i = 0U; i < TEST_GROUP_BUDGET_HOLD_COUNT; ++i)
		assert(tr_connection_group_message_release(
		               &ctx.budget_retained[i]) == TR_OK);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5101U) ==
	       TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5102U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5101U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U) ==
	       TR_OK);
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 2U);
	assert(tr_server_connection_group_wait_drained(server, 5000U) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: multi-message budget hold/raw peer "
	     "passed");
}

static void test_public_connection_group_sustained_raw_zstd_progress(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_conn_handle data_routes[2];
	struct fairness_raw_sender raw_sender;
	struct tr_transport_bytes compressed_bytes;
	uint8_t *decoded_payload;
	uint16_t group_port = 0U;
	unsigned raw_sent_target;
	unsigned raw_during_zstd_total = 0U;
	uint64_t zstd_batch_duration_ns[TEST_GROUP_FAIRNESS_ZSTD_BATCHES];
	uint64_t zstd_started_ns;
	uint64_t zstd_elapsed_ns;
	pthread_t raw_thread;
	unsigned batch;
	unsigned i;
	int ret;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	atomic_store(&group_fairness_compressed_submissions, 0U);
	atomic_store(&group_fairness_raw_submissions, 0U);

	tr_server_config_init(&server_config);
	server_config.limits.max_frame_payload_bytes = 64U * 1024U;
	server_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 2U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages =
	        TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.limits.max_frame_payload_bytes = 64U * 1024U;
	client_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 2U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 4096U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(7161)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_data_offer(server, UINT64_C(7162)) == TR_OK);
	wait_data_accepts(server, 2U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5101U,
	               UINT64_C(7171)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U,
	               UINT64_C(7172)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID;
	group_route_probe_message_ids[1] = TEST_GROUP_FAIRNESS_RAW_FIRST_ID;
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);

	memset(&raw_sender, 0, sizeof(raw_sender));
	raw_sender.ctx = &ctx;
	raw_sender.client = client;
	atomic_init(&raw_sender.stop, 0);
	assert(pthread_create(&raw_thread, NULL, fairness_raw_sender_main,
	                      &raw_sender) == 0);
	wait_counter(&ctx, &ctx.fairness_raw_sent,
	             TEST_GROUP_FAIRNESS_RAW_WINDOW);

	decoded_payload = (uint8_t *)malloc(TEST_GROUP_LARGE_DECODED_BYTES);
	assert(decoded_payload != NULL);
	compressed_bytes.data = decoded_payload;
	compressed_bytes.len = TEST_GROUP_LARGE_DECODED_BYTES;
	zstd_started_ns = group_monotonic_now_ns();
	for (batch = 0U; batch < TEST_GROUP_FAIRNESS_ZSTD_BATCHES; ++batch) {
		unsigned batch_first =
		        batch * TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH;
		unsigned batch_end =
		        batch_first + TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH;
		uint64_t batch_started_ns = group_monotonic_now_ns();

		for (i = batch_first; i < batch_end; ++i) {
			unsigned attempt;

			memset(decoded_payload, (uint8_t)(0x40U + i),
			       TEST_GROUP_LARGE_DECODED_BYTES);
			ret = TR_AGAIN;
			for (attempt = 0U; attempt < 5000U && ret == TR_AGAIN;
			     ++attempt) {
				ret = tr_client_connection_group_send_with_compression(
				        client, 5101U,
				        TEST_GROUP_FAIRNESS_ZSTD_FIRST_ID + i,
				        &compressed_bytes,
				        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
				if (ret == TR_AGAIN) {
					struct timespec pause = {0, 1000000L};

					(void)nanosleep(&pause, NULL);
				}
			}
			if (ret != TR_OK) {
				unsigned raw_sent;
				unsigned raw_received;
				unsigned data_events;
				int last_data_status;
				int raw_sender_status;

				pthread_mutex_lock(&ctx.lock);
				raw_sent = ctx.fairness_raw_sent;
				raw_received = ctx.fairness_raw_messages;
				data_events = ctx.data_events;
				last_data_status = ctx.last_data_status;
				raw_sender_status =
				        ctx.fairness_raw_sender_status;
				pthread_mutex_unlock(&ctx.lock);
				fprintf(stderr,
				        "sustained RAW/Zstandard send failed: "
				        "index=%u "
				        "attempt=%u status=%d (%s) raw=%u/%u "
				        "events=%u "
				        "event_status=%d (%s) "
				        "sender_status=%d\n",
				        i, attempt, ret, tr_status_str(ret),
				        raw_received, raw_sent, data_events,
				        last_data_status,
				        tr_status_str(last_data_status),
				        raw_sender_status);
			}
			assert(ret == TR_OK);
		}
		wait_counter(&ctx, &ctx.fairness_zstd_messages,
		             (batch + 1U) * TEST_GROUP_FAIRNESS_ZSTD_PER_BATCH);
		pthread_mutex_lock(&ctx.lock);
		assert(ctx.fairness_zstd_batches == batch + 1U);
		assert(ctx.fairness_raw_at_batch_end[batch] >=
		       ctx.fairness_raw_at_batch_start[batch] +
		               TEST_GROUP_FAIRNESS_MIN_RAW_DURING_ZSTD);
		pthread_mutex_unlock(&ctx.lock);
		zstd_batch_duration_ns[batch] =
		        group_monotonic_now_ns() - batch_started_ns;
		assert(zstd_batch_duration_ns[batch] != 0U);
	}
	zstd_elapsed_ns = group_monotonic_now_ns() - zstd_started_ns;
	assert(zstd_elapsed_ns != 0U);
	free(decoded_payload);

	atomic_store(&raw_sender.stop, 1);
	pthread_mutex_lock(&ctx.lock);
	pthread_cond_broadcast(&ctx.cond);
	pthread_mutex_unlock(&ctx.lock);
	assert(pthread_join(raw_thread, NULL) == 0);
	pthread_mutex_lock(&ctx.lock);
	raw_sent_target = ctx.fairness_raw_sent;
	assert(ctx.fairness_raw_sender_exhausted == 0U);
	assert(ctx.fairness_raw_sender_status == 0);
	assert(ctx.fairness_zstd_messages == TEST_GROUP_FAIRNESS_ZSTD_COUNT);
	assert(ctx.fairness_zstd_batches == TEST_GROUP_FAIRNESS_ZSTD_BATCHES);
	assert(ctx.fairness_zstd_held == TEST_GROUP_BUDGET_HOLD_COUNT);
	for (batch = 0U; batch < TEST_GROUP_FAIRNESS_ZSTD_BATCHES; ++batch) {
		unsigned raw_during_batch =
		        ctx.fairness_raw_at_batch_end[batch] -
		        ctx.fairness_raw_at_batch_start[batch];

		assert(raw_during_batch >=
		       TEST_GROUP_FAIRNESS_MIN_RAW_DURING_ZSTD);
		raw_during_zstd_total += raw_during_batch;
	}
	assert(ctx.data_events == 0U);
	pthread_mutex_unlock(&ctx.lock);
	wait_counter(&ctx, &ctx.fairness_raw_messages, raw_sent_target);
	assert(atomic_load(&group_fairness_compressed_submissions) ==
	       TEST_GROUP_FAIRNESS_ZSTD_COUNT);
	assert(atomic_load(&group_fairness_raw_submissions) == raw_sent_target);

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].reactor == data_routes[1].reactor &&
	       (data_routes[0].slot != data_routes[1].slot ||
	        data_routes[0].generation != data_routes[1].generation));

	pthread_mutex_lock(&ctx.lock);
	for (i = 0U; i < TEST_GROUP_BUDGET_HOLD_COUNT; ++i) {
		uint8_t expected_byte = (uint8_t)(0x40U + i);

		assert(ctx.fairness_retained[i].bytes.len ==
		       TEST_GROUP_LARGE_DECODED_BYTES);
		for (uint32_t byte = 0U; byte < TEST_GROUP_LARGE_DECODED_BYTES;
		     ++byte)
			assert(ctx.fairness_retained[i].bytes.data[byte] ==
			       expected_byte);
	}
	printf("sustained RAW/Zstd progress: zstd=%u raw_per_batch=[",
	       ctx.fairness_zstd_messages);
	for (batch = 0U; batch < TEST_GROUP_FAIRNESS_ZSTD_BATCHES; ++batch) {
		if (batch != 0U)
			putchar(',');
		printf("%u", ctx.fairness_raw_at_batch_end[batch] -
		                     ctx.fairness_raw_at_batch_start[batch]);
	}
	printf("] zstd_batch_ms=[");
	for (batch = 0U; batch < TEST_GROUP_FAIRNESS_ZSTD_BATCHES; ++batch) {
		if (batch != 0U)
			putchar(',');
		printf("%llu",
		       (unsigned long long)(zstd_batch_duration_ns[batch] /
		                            UINT64_C(1000000)));
	}
	printf("] zstd_elapsed_ms=%llu raw_during_zstd=%u raw_total=%u\n",
	       (unsigned long long)(zstd_elapsed_ns / UINT64_C(1000000)),
	       raw_during_zstd_total, ctx.fairness_raw_messages);
	pthread_mutex_unlock(&ctx.lock);
	for (i = 0U; i < TEST_GROUP_BUDGET_HOLD_COUNT; ++i)
		assert(tr_connection_group_message_release(
		               &ctx.fairness_retained[i]) == TR_OK);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5101U) ==
	       TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5102U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5101U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U) ==
	       TR_OK);
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 2U);
	assert(tr_server_connection_group_wait_drained(server, 5000U) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: sustained RAW/Zstd progress passed");
}

static void test_public_connection_group_natural_budget_rejection_raw_peer(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_memory_budget_stats budget_before;
	struct tr_memory_budget_stats budget_after;
	struct tr_memory_budget *message_budget;
	struct tr_conn_handle data_routes[2];
	uint8_t *decoded_payload;
	uint8_t raw_payload = 0U;
	struct tr_transport_bytes compressed_bytes;
	struct tr_transport_bytes raw_bytes = {&raw_payload, 1U};
	uint16_t group_port = 0U;
	unsigned retained = 0U;
	unsigned i;
	int budget_rejected = 0;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);
	atomic_store(&group_large_raw_submissions, 0U);
	captured_group_message_budget = NULL;
	atomic_store(&capture_group_message_budget, 1);

	tr_server_config_init(&server_config);
	server_config.limits.max_frame_payload_bytes = 64U * 1024U;
	server_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 2U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 8U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	atomic_store(&capture_group_message_budget, 0);
	message_budget = captured_group_message_budget;
	assert(message_budget != NULL);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.limits.max_frame_payload_bytes = 64U * 1024U;
	client_config.limits.max_message_bytes = TEST_GROUP_LARGE_DECODED_BYTES;
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 2U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 4096U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6961)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_data_offer(server, UINT64_C(6962)) == TR_OK);
	wait_data_accepts(server, 2U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5101U,
	               UINT64_C(6971)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U,
	               UINT64_C(6972)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = TEST_GROUP_NATURAL_BUDGET_FIRST_ID;
	group_route_probe_message_ids[1] = TEST_GROUP_RAW_BURST_FIRST_ID;
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);

	tr_memory_budget_get_stats(message_budget, &budget_before);
	assert(budget_before.limit_bytes != 0U);
	for (i = 0U; i < TEST_GROUP_NATURAL_BUDGET_HOLD_CAPACITY; ++i) {
		uint8_t expected_byte = (uint8_t)(0x31U + i);
		int ret;

		decoded_payload =
		        (uint8_t *)malloc(TEST_GROUP_LARGE_DECODED_BYTES);
		assert(decoded_payload != NULL);
		memset(decoded_payload, expected_byte,
		       TEST_GROUP_LARGE_DECODED_BYTES);
		compressed_bytes.data = decoded_payload;
		compressed_bytes.len = TEST_GROUP_LARGE_DECODED_BYTES;
		ret = tr_client_connection_group_send_with_compression(
		        client, 5101U, TEST_GROUP_NATURAL_BUDGET_FIRST_ID + i,
		        &compressed_bytes,
		        TR_CONNECTION_GROUP_COMPRESSION_ZSTD);
		free(decoded_payload);
		if (ret != TR_OK) {
			wait_counter(&ctx, &ctx.data_events, 1U);
			budget_rejected = 1;
			break;
		}
		if (!wait_natural_budget_hold_or_data_close(&ctx, i + 1U)) {
			budget_rejected = 1;
			break;
		}
		retained = i + 1U;
	}
	assert(budget_rejected);
	assert(retained != 0U);
	wait_counter(&ctx, &ctx.data_events, 1U);
	tr_memory_budget_get_stats(message_budget, &budget_after);
	assert(budget_after.rejection_events ==
	       budget_before.rejection_events + 1U);
	assert(budget_after.current_bytes <= budget_after.limit_bytes);
	assert(budget_after.current_bytes > budget_before.current_bytes);
	assert(budget_after.limit_bytes - budget_after.current_bytes <
	       (uint64_t)TEST_GROUP_LARGE_DECODED_BYTES +
	               TR_GROUP_MESSAGE_WORKSPACE_BYTES);
	assert(ctx.last_data_status != TR_OK);
	printf("natural server message budget: held=%u current=%llu limit=%llu "
	       "rejections=%llu\n",
	       retained, (unsigned long long)budget_after.current_bytes,
	       (unsigned long long)budget_after.limit_bytes,
	       (unsigned long long)budget_after.rejection_events);

	/* 全局消息预算已拒绝真实压缩任务且压缩成员已关闭；另一 DATA
	 * 成员上的 RAW 突发仍须全部交付，期间不释放任何解码持有缓冲。 */
	for (i = 0U; i < TEST_GROUP_RAW_BURST_COUNT; ++i) {
		raw_payload = (uint8_t)(0x6aU + i);
		raw_bytes.data = &raw_payload;
		assert(tr_client_connection_group_send(
		               client, 5102U, TEST_GROUP_RAW_BURST_FIRST_ID + i,
		               &raw_bytes) == TR_OK);
	}
	wait_counter(&ctx, &ctx.raw_burst_messages, TEST_GROUP_RAW_BURST_COUNT);
	assert(atomic_load(&group_large_raw_submissions) ==
	       TEST_GROUP_RAW_BURST_COUNT);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].reactor == data_routes[1].reactor &&
	       (data_routes[0].slot != data_routes[1].slot ||
	        data_routes[0].generation != data_routes[1].generation));

	pthread_mutex_lock(&ctx.lock);
	assert(ctx.natural_budget_hold_messages == retained);
	for (unsigned message = 0U; message < retained; ++message) {
		uint8_t expected_byte = (uint8_t)(0x31U + message);

		assert(ctx.natural_budget_retained[message].bytes.len ==
		       TEST_GROUP_LARGE_DECODED_BYTES);
		for (uint32_t byte = 0U; byte < TEST_GROUP_LARGE_DECODED_BYTES;
		     ++byte)
			assert(ctx.natural_budget_retained[message]
			               .bytes.data[byte] == expected_byte);
	}
	pthread_mutex_unlock(&ctx.lock);
	for (i = 0U; i < retained; ++i)
		assert(tr_connection_group_message_release(
		               &ctx.natural_budget_retained[i]) == TR_OK);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5102U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5102U) ==
	       TR_OK);
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 2U);
	assert(tr_server_connection_group_wait_drained(server, 5000U) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: natural server budget rejection/raw "
	     "peer passed");
}

static void test_public_connection_group_member_failure_replacement(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	struct tr_conn_handle data_routes[2];
	struct tr_conn_handle replacement_route;
	uint8_t first_payload = 0x31U;
	uint8_t second_payload = 0x32U;
	uint8_t replacement_payload = 0x34U;
	struct tr_transport_bytes first_bytes = {&first_payload, 1U};
	struct tr_transport_bytes second_bytes = {&second_payload, 1U};
	struct tr_transport_bytes replacement_bytes = {
		&replacement_payload, 1U};
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 3U;
	server_config.connection_groups.max_data_connections_per_group = 2U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 2U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.on_transfer_ready =
		on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6101)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_data_offer(server, UINT64_C(6102)) == TR_OK);
	wait_data_accepts(server, 2U);

	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(6301)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U,
		       UINT64_C(6302)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 2U);

	/* 捕获两条真实 DATA 帧对应的客户端物理成员。 */
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(6201);
	group_route_probe_message_ids[1] = UINT64_C(6202);
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send(
		       client, 5001U, UINT64_C(6201), &first_bytes) == TR_OK);
	assert(tr_client_connection_group_send(
		       client, 5002U, UINT64_C(6202), &second_bytes) == TR_OK);
	wait_counter(&ctx, &ctx.multi_drain_messages, 2U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].slot != data_routes[1].slot ||
	       data_routes[0].generation != data_routes[1].generation);

	/* 一条成员故障只撤销其亲和关系；另一成员仍可发送。 */
	assert(tr_reactor_abort(data_routes[0], TR_ERR_SYS) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.data_connections == 1U &&
	       client_stats.active_transfers == 1U);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.data_connections_current == 1U &&
	       server_stats.active_transfers == 1U);
	assert(tr_client_connection_group_send(
		       client, 5001U, UINT64_C(6203), &first_bytes) ==
	       TR_ERR_STALE);
	assert(tr_client_connection_group_send(
		       client, 5002U, UINT64_C(6202), &second_bytes) == TR_OK);
	wait_counter(&ctx, &ctx.multi_drain_messages, 3U);

	/* 相同 group/epoch 上重建 DATA 成员并交付新亲和流。 */
	assert(wait_data_offer(server, UINT64_C(6103)) == TR_OK);
	wait_data_accepts(server, 3U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5003U,
		       UINT64_C(6303)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 3U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(6204);
	group_route_probe_message_ids[1] = 0U;
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send(
		       client, 5003U, UINT64_C(6204), &replacement_bytes) == TR_OK);
	wait_counter(&ctx, &ctx.member_replacement_messages, 1U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 1U);
	replacement_route = group_route_probe_connections[0];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(replacement_route.reactor == data_routes[0].reactor);
	assert(replacement_route.slot != data_routes[0].slot ||
	       replacement_route.generation != data_routes[0].generation);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.data_connections == 2U &&
	       client_stats.active_transfers == 2U);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.data_connections_current == 2U &&
	       server_stats.active_transfers == 2U);

	assert(tr_client_connection_group_release_transfer(client, 5002U) ==
	       TR_OK);
	assert(tr_client_connection_group_release_transfer(client, 5003U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5003U) == TR_OK);
	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(6104)) == TR_ERR_CLOSED);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);
	assert(tr_client_connection_group_wait_drained(client, 5000U) == TR_OK);
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 3U);
	assert(tr_server_connection_group_wait_drained(
		       server, 5000U) == TR_OK);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 0U &&
	       server_stats.connections_current == 0U &&
	       server_stats.data_connections_current == 0U &&
	       server_stats.active_transfers == 0U);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: member failure replacement passed");
}

static void test_public_connection_group_multi_group_repeated_failure_replacement(
	void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *clients[2] = {NULL, NULL};
	struct group_pair_ctx ctx;
	struct tr_conn_handle data_routes[2];
	struct tr_conn_handle replacement_routes[2];
	struct tr_conn_handle second_replacement_routes[2];
	struct tr_conn_handle current_routes[2];
	uint8_t payload_a = 0x51U;
	uint8_t payload_b = 0x52U;
	uint8_t replacement_payload = 0x53U;
	struct tr_transport_bytes bytes_a = {&payload_a, 1U};
	struct tr_transport_bytes bytes_b = {&payload_b, 1U};
	struct tr_transport_bytes replacement_bytes = {&replacement_payload,
	                                               1U};
	uint64_t group_ids[2] = {TEST_GROUP_PAIR_ID_FIRST,
	                         TEST_GROUP_PAIR_ID_SECOND};
	uint64_t epochs[2] = {TEST_GROUP_PAIR_EPOCH_FIRST,
	                      TEST_GROUP_PAIR_EPOCH_SECOND};
	uint64_t current_transfer_ids[2] = {7103U, 8103U};
	uint64_t current_message_ids[2] = {7206U, 7207U};
	uint16_t group_port = 0U;
	unsigned i;
	unsigned round;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 4U;
	server_config.shard_count = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 2U;
	server_config.connection_groups.max_connections = 4U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group_pair;
	server_config.connection_groups.on_message = on_group_pair_message;
	server_config.connection_groups.on_data_event =
		on_group_pair_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	for (i = 0U; i < 2U; ++i) {
		tr_client_config_init(&client_config);
		client_config.keepalive_interval_ms = 0U;
		client_config.connection_groups.max_data_connections = 1U;
		client_config.connection_groups.max_active_transfers = 2U;
		assert(tr_client_create_transport(&client_config, &clients[i]) ==
		       TR_OK);
		{
			struct tr_connection_group_id group = {
				group_ids[i], epochs[i]};

			assert(tr_client_connection_group_connect(
				       clients[i], "127.0.0.1", group_port,
				       &group) == TR_OK);
		}
	}
	wait_group_pair_counter(&ctx, &ctx.authorized[0], 1U);
	wait_group_pair_counter(&ctx, &ctx.authorized[1], 1U);
	wait_group_pair_server_stats(server, 2U, 0U, 0U);

	assert(wait_group_pair_offer(server, group_ids[0], epochs[0],
	                             UINT64_C(8101)) == TR_OK);
	wait_data_accepts(server, 1U);
	assert(wait_group_pair_offer(server, group_ids[1], epochs[1],
	                             UINT64_C(8102)) == TR_OK);
	wait_data_accepts(server, 2U);
	wait_group_pair_client_stats(clients[0], 1U, 0U);
	wait_group_pair_client_stats(clients[1], 1U, 0U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[0], epochs[0], 7101U,
		       UINT64_C(7301)) == TR_OK);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[1], epochs[1], 8101U,
		       UINT64_C(8301)) == TR_OK);
	wait_group_pair_client_stats(clients[0], 1U, 1U);
	wait_group_pair_client_stats(clients[1], 1U, 1U);

	/* 两个并行 group 的 DATA 各来自独立 Client Reactor。 */
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(7201);
	group_route_probe_message_ids[1] = UINT64_C(7202);
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send(
		       clients[0], 7101U, UINT64_C(7201), &bytes_a) == TR_OK);
	assert(tr_client_connection_group_send(
		       clients[1], 8101U, UINT64_C(7202), &bytes_b) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_messages[0], 1U);
	wait_group_pair_counter(&ctx, &ctx.data_messages[1], 1U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	data_routes[0] = group_route_probe_connections[0];
	data_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(data_routes[0].reactor != data_routes[1].reactor);

	/* 通过屏障并发中止两个独立 Reactor 上的 DATA 成员。 */
	abort_group_pair_data_routes_concurrently(data_routes);
	wait_group_pair_counter(&ctx, &ctx.data_events[0], 1U);
	wait_group_pair_counter(&ctx, &ctx.data_events[1], 1U);
	wait_group_pair_client_stats(clients[0], 0U, 0U);
	wait_group_pair_client_stats(clients[1], 0U, 0U);
	wait_group_pair_server_stats(server, 2U, 0U, 0U);
	assert(tr_client_connection_group_send(
		       clients[0], 7101U, UINT64_C(7201), &bytes_a) ==
	       TR_ERR_STALE);
	assert(tr_client_connection_group_send(
		       clients[1], 8101U, UINT64_C(7202), &bytes_b) ==
	       TR_ERR_STALE);

	/* 两组分别替代成员，不复用已故障的物理连接。 */
	assert(wait_group_pair_offer(server, group_ids[0], epochs[0],
	                             UINT64_C(8103)) == TR_OK);
	wait_data_accepts(server, 3U);
	assert(wait_group_pair_offer(server, group_ids[1], epochs[1],
	                             UINT64_C(8104)) == TR_OK);
	wait_data_accepts(server, 4U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[0], epochs[0], 7102U,
		       UINT64_C(7302)) == TR_OK);
	wait_group_pair_client_stats(clients[0], 1U, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[1], epochs[1], 8102U,
		       UINT64_C(8302)) == TR_OK);
	wait_group_pair_client_stats(clients[1], 1U, 1U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(7203);
	group_route_probe_message_ids[1] = UINT64_C(7204);
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send(
		       clients[0], 7102U, UINT64_C(7203),
		       &replacement_bytes) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_messages[0], 2U);
	assert(tr_client_connection_group_send(
		       clients[1], 8102U, UINT64_C(7204),
		       &replacement_bytes) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_messages[1], 2U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	replacement_routes[0] = group_route_probe_connections[0];
	replacement_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	for (i = 0U; i < 2U; ++i) {
		assert(replacement_routes[i].reactor == data_routes[i].reactor);
		assert(replacement_routes[i].slot != data_routes[i].slot ||
		       replacement_routes[i].generation !=
		               data_routes[i].generation);
	}
	wait_group_pair_server_stats(server, 2U, 2U, 2U);

	/* 在第一轮替代后立即再次并发故障，检验快速重复换代。 */
	abort_group_pair_data_routes_concurrently(replacement_routes);
	wait_group_pair_counter(&ctx, &ctx.data_events[0], 2U);
	wait_group_pair_counter(&ctx, &ctx.data_events[1], 2U);
	wait_group_pair_client_stats(clients[0], 0U, 0U);
	wait_group_pair_client_stats(clients[1], 0U, 0U);
	wait_group_pair_server_stats(server, 2U, 0U, 0U);
	assert(tr_client_connection_group_send(
		       clients[0], 7102U, UINT64_C(7203), &replacement_bytes) ==
	       TR_ERR_STALE);
	assert(tr_client_connection_group_send(
		       clients[1], 8102U, UINT64_C(7204), &replacement_bytes) ==
	       TR_ERR_STALE);

	assert(wait_group_pair_offer(server, group_ids[0], epochs[0],
	                             UINT64_C(8105)) == TR_OK);
	wait_data_accepts(server, 5U);
	assert(wait_group_pair_offer(server, group_ids[1], epochs[1],
	                             UINT64_C(8106)) == TR_OK);
	wait_data_accepts(server, 6U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[0], epochs[0], 7103U,
		       UINT64_C(7303)) == TR_OK);
	wait_group_pair_client_stats(clients[0], 1U, 1U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, group_ids[1], epochs[1], 8103U,
		       UINT64_C(8303)) == TR_OK);
	wait_group_pair_client_stats(clients[1], 1U, 1U);

	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_message_ids[0] = UINT64_C(7206);
	group_route_probe_message_ids[1] = UINT64_C(7207);
	memset(group_route_probe_connections, 0,
	       sizeof(group_route_probe_connections));
	group_route_probe_seen = 0U;
	group_route_probe_enabled = 1;
	pthread_mutex_unlock(&group_route_probe_lock);
	assert(tr_client_connection_group_send(
		       clients[0], 7103U, UINT64_C(7206),
		       &replacement_bytes) == TR_OK);
	assert(tr_client_connection_group_send(
		       clients[1], 8103U, UINT64_C(7207),
		       &replacement_bytes) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_messages[0], 3U);
	wait_group_pair_counter(&ctx, &ctx.data_messages[1], 3U);
	pthread_mutex_lock(&group_route_probe_lock);
	group_route_probe_enabled = 0;
	assert(group_route_probe_seen == 3U);
	second_replacement_routes[0] = group_route_probe_connections[0];
	second_replacement_routes[1] = group_route_probe_connections[1];
	pthread_mutex_unlock(&group_route_probe_lock);
	for (i = 0U; i < 2U; ++i) {
		assert(second_replacement_routes[i].reactor ==
		       replacement_routes[i].reactor);
		assert(second_replacement_routes[i].slot !=
		               replacement_routes[i].slot ||
		       second_replacement_routes[i].generation !=
		               replacement_routes[i].generation);
	}
	wait_group_pair_server_stats(server, 2U, 2U, 2U);
	for (i = 0U; i < 2U; ++i)
		current_routes[i] = second_replacement_routes[i];
	for (round = 0U; round < GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS; ++round)
		stress_group_pair_replacement_round(
		        server, clients, &ctx, group_ids, epochs,
		        current_routes, current_transfer_ids,
		        current_message_ids, round);

	/* 关闭 A 组不关闭 B 组；B 组继续发完在途工作。 */
	assert(tr_client_connection_group_close(clients[0]) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_events[0],
	                        GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS + 3U);
	wait_group_pair_server_stats(server, 1U, 1U, 1U);
	wait_group_pair_client_stats(clients[1], 1U, 1U);
	assert(tr_client_connection_group_send(
	               clients[1], current_transfer_ids[1], UINT64_C(7208),
	               &replacement_bytes) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_messages[1],
	                        GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS + 4U);
	wait_client_destroy_after_group_close(clients[0]);

	assert(tr_client_connection_group_release_transfer(
	               clients[1], current_transfer_ids[1]) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
	               server, group_ids[1], epochs[1],
	               current_transfer_ids[1]) == TR_OK);
	assert(tr_client_connection_group_begin_drain(clients[1]) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
	               server, group_ids[1], epochs[1], UINT64_C(8104)) ==
	       TR_ERR_CLOSED);
	assert(tr_client_connection_group_wait_drained(clients[1], 5000U) ==
	       TR_OK);
	assert(tr_server_connection_group_wait_drained(server, 1U) ==
	       TR_ERR_TIMEOUT);
	assert(tr_client_connection_group_close(clients[1]) == TR_OK);
	wait_group_pair_counter(&ctx, &ctx.data_events[1],
	                        GROUP_PAIR_REPLACEMENT_STRESS_ROUNDS + 3U);
	assert(tr_server_connection_group_wait_drained(server, 5000U) == TR_OK);
	wait_group_pair_server_stats(server, 0U, 0U, 0U);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(clients[1]);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: 32-round multi-group failure "
	     "replacement stress passed");
}

static void test_public_connection_group_client_connector_failure_wakes(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct client_group_wait_probe wait_probe;
	pthread_t wait_thread;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	/* 假连接器保持 DATA 槽 CONNECTING，取消先失败，再投递终结回调，
	 * 模拟排空取消与可重试完成清理竞争的窗口。
	 */
	pthread_mutex_lock(&group_connector_probe_lock);
	group_connector_probe_starts = 0U;
	pthread_mutex_unlock(&group_connector_probe_lock);
	atomic_store(&defer_group_connector_start, 1);
	assert(wait_data_offer(server, UINT64_C(4501)) == TR_OK);
	wait_group_connector_start(1U);
	atomic_store(&fail_group_connector_cancel_once, 1);
	assert(tr_client_connection_group_begin_drain(client) == TR_ERR_SYS);

	memset(&wait_probe, 0, sizeof(wait_probe));
	wait_probe.client = client;
	wait_probe.timeout_ms = 1000U;
	assert(pthread_mutex_init(&wait_probe.lock, NULL) == 0);
	assert(pthread_cond_init(&wait_probe.cond, NULL) == 0);
	assert(pthread_create(&wait_thread, NULL, client_group_wait_main,
	                      &wait_probe) == 0);
	wait_client_group_probe(&wait_probe, &wait_probe.entered);

	/* 完成回调释放最后一个 CONNECTING 槽后，wait_drained 必须被唤醒。 */
	dispatch_group_connector_failure();
	wait_client_group_probe(&wait_probe, &wait_probe.done);
	assert(pthread_join(wait_thread, NULL) == 0);
	assert(wait_probe.ret == TR_OK);
	pthread_cond_destroy(&wait_probe.cond);
	pthread_mutex_destroy(&wait_probe.lock);

	atomic_store(&defer_group_connector_start, 0);
	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

struct group_compression_retirement_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned authorizations;
	unsigned ready;
	unsigned messages;
	uint64_t last_message_id;
};

static int authorize_group_compression_retirement(
        const struct tr_connection_group_authorization *authorization,
        void *arg)
{
	struct group_compression_retirement_ctx *ctx = arg;

	assert(authorization != NULL);
	assert(authorization->group.group_id == TEST_GROUP_ID);
	assert(authorization->group.epoch == TEST_GROUP_EPOCH);
	assert(authorization->peer_identity.authentication ==
	       TR_PEER_IDENTITY_NONE);
	pthread_mutex_lock(&ctx->lock);
	ctx->authorizations++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_OK;
}

static enum tr_connection_group_message_disposition
on_group_compression_retirement_message(
        const struct tr_connection_group_message *message, void *arg)
{
	struct group_compression_retirement_ctx *ctx = arg;

	assert(message != NULL);
	assert(message->stream_id == 7001U);
	pthread_mutex_lock(&ctx->lock);
	ctx->messages++;
	ctx->last_message_id = message->message_id;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_CONNECTION_GROUP_MESSAGE_RELEASE;
}

static void on_group_compression_retirement_ready(
        const struct tr_connection_group_id *group, uint32_t stream_id,
        uint64_t message_id, void *arg)
{
	struct group_compression_retirement_ctx *ctx = arg;

	assert(group != NULL);
	assert(group->group_id == TEST_GROUP_ID);
	assert(group->epoch == TEST_GROUP_EPOCH);
	assert(stream_id == 7001U);
	assert(message_id == 4401U);
	pthread_mutex_lock(&ctx->lock);
	ctx->ready++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_group_compression_retirement_counter(
        struct group_compression_retirement_ctx *ctx, unsigned *counter,
        unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (*counter < target && ret == 0)
		ret = pthread_cond_timedwait(&ctx->cond, &ctx->lock, &deadline);
	assert(*counter >= target);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_group_compression_worker_entered(void)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&group_zstd_gate_lock);
	while (!group_zstd_gate_entered && ret == 0)
		ret = pthread_cond_timedwait(&group_zstd_gate_cond,
		                             &group_zstd_gate_lock, &deadline);
	assert(group_zstd_gate_entered);
	pthread_mutex_unlock(&group_zstd_gate_lock);
}

static void wait_group_zstd_decompression_entered(void)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&group_zstd_gate_lock);
	while (!group_zstd_gate_entered && ret == 0)
		ret = pthread_cond_timedwait(&group_zstd_gate_cond,
		                             &group_zstd_gate_lock, &deadline);
	assert(group_zstd_gate_entered);
	pthread_mutex_unlock(&group_zstd_gate_lock);
}

static void release_group_zstd_gate(void)
{
	pthread_mutex_lock(&group_zstd_gate_lock);
	group_zstd_gate_released = 1;
	pthread_cond_broadcast(&group_zstd_gate_cond);
	pthread_mutex_unlock(&group_zstd_gate_lock);
}

static void wait_group_client_send_bytes_empty(struct tr_client *client)
{
	struct timespec pause = {0, 1000000L};
	struct tr_connection_group_client_stats stats;
	unsigned attempt;

	for (attempt = 0; attempt < 10000U; ++attempt) {
		assert(tr_client_connection_group_get_stats(client, &stats) ==
		       TR_OK);
		if (stats.send_bytes_inflight == 0U)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for client compression retirement");
}

static void wait_for_server_group_count(struct tr_server *server,
                                        uint32_t expected)
{
	struct timespec pause = {0, 1000000L};
	unsigned attempt;

	for (attempt = 0; attempt < 10000U; ++attempt) {
		struct tr_connection_group_server_stats stats;
		assert(tr_server_connection_group_get_stats(server, &stats) ==
		       TR_OK);
		if (stats.groups_current == expected)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for server connection-group count");
}

static void test_client_compression_completion_after_route_replacement(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct group_compression_retirement_ctx ctx;
	struct tr_connection_group_id group = {TEST_GROUP_ID, TEST_GROUP_EPOCH};
	struct tr_connection_group_client_stats client_stats;
	struct tr_transport_bytes bytes;
	struct tr_transport_bytes retry_bytes;
	uint8_t source[64];
	uint8_t retry_byte = 0x6dU;
	uint16_t group_port = 0U;
	unsigned i;

	atomic_store(&group_capability_frames_sent, 0U);
	atomic_store(&hold_group_zstd_completion, 0);
	pthread_mutex_lock(&group_zstd_gate_lock);
	group_zstd_gate_entered = 0;
	group_zstd_gate_released = 0;
	pthread_mutex_unlock(&group_zstd_gate_lock);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	server_config.connection_groups.authorize =
	        authorize_group_compression_retirement;
	server_config.connection_groups.on_message =
	        on_group_compression_retirement_message;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 2U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready =
	        on_group_compression_retirement_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_group_compression_retirement_counter(&ctx, &ctx.authorizations,
	                                          1U);
	assert(wait_data_offer(server, UINT64_C(4501)) == TR_OK);
	wait_data_accepts(server, 1U);
	wait_group_capability_frames(2U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 7001U,
	               UINT64_C(4401)) == TR_OK);
	wait_group_compression_retirement_counter(&ctx, &ctx.ready, 1U);

	for (i = 0; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i * 29U + 7U);
	bytes.data = source;
	bytes.len = sizeof(source);
	atomic_store(&hold_group_zstd_completion, 1);
	assert(tr_client_connection_group_send_with_compression(
	               client, 7001U, UINT64_C(4401), &bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_group_compression_worker_entered();
	retry_bytes.data = &retry_byte;
	retry_bytes.len = 1U;
	assert(tr_client_connection_group_send_with_compression(
	               client, 7001U, UINT64_C(4402), &retry_bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_AGAIN);
	assert(retry_bytes.data == &retry_byte && retry_bytes.len == 1U &&
	       retry_byte == 0x6dU);

	/* 旧 DATA 路由关闭时取消运行中的编码任务；新路由在旧完成回调
	 * 尚未返回前建立，以覆盖相同 group/stream 上的代际复用。
	 */
	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_for_server_group_count(server, 0U);
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_group_compression_retirement_counter(&ctx, &ctx.authorizations,
	                                          2U);
	assert(wait_data_offer(server, UINT64_C(4502)) == TR_OK);
	wait_data_accepts(server, 2U);
	wait_group_capability_frames(4U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 7001U,
	               UINT64_C(4401)) == TR_OK);
	wait_group_compression_retirement_counter(&ctx, &ctx.ready, 2U);

	pthread_mutex_lock(&group_zstd_gate_lock);
	group_zstd_gate_released = 1;
	pthread_cond_broadcast(&group_zstd_gate_cond);
	pthread_mutex_unlock(&group_zstd_gate_lock);
	wait_group_client_send_bytes_empty(client);
	assert(tr_client_connection_group_get_stats(client, &client_stats) ==
	       TR_OK);
	assert(client_stats.active_transfers == 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.messages == 0U);
	pthread_mutex_unlock(&ctx.lock);

	/* 迟到的旧代次完成不得破坏新路由；新代次仍能独立完成压缩发送。 */
	assert(tr_client_connection_group_send_with_compression(
	               client, 7001U, UINT64_C(4402), &bytes,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_group_compression_retirement_counter(&ctx, &ctx.messages, 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.last_message_id == UINT64_C(4402));
	pthread_mutex_unlock(&ctx.lock);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_for_server_group_count(server, 0U);
	wait_client_destroy_after_group_close(client);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_drain_with_compression_inflight(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group = {TEST_GROUP_ID, TEST_GROUP_EPOCH};
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	struct tr_transport_bytes bytes;
	uint8_t source[64];
	uint16_t group_port = 0U;
	unsigned i;

	atomic_store(&group_capability_frames_sent, 0U);
	atomic_store(&hold_group_zstd_completion, 0);
	pthread_mutex_lock(&group_zstd_gate_lock);
	group_zstd_gate_entered = 0;
	group_zstd_gate_released = 0;
	pthread_mutex_unlock(&group_zstd_gate_lock);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 1U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6401)) == TR_OK);
	wait_data_accepts(server, 1U);
	wait_group_capability_frames(2U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(6402)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);

	for (i = 0U; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i * 29U + 7U);
	bytes.data = source;
	bytes.len = sizeof(source);
	atomic_store(&hold_group_zstd_completion, 1);
	assert(tr_client_connection_group_send_with_compression(
		       client, 5001U, UINT64_C(6403), &bytes,
		       TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_group_compression_worker_entered();
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 1U &&
	       client_stats.send_bytes_inflight == sizeof(source));

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_server_connection_group_send_data_offer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH,
		       UINT64_C(6404)) == TR_ERR_CLOSED);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5002U,
		       UINT64_C(6405)) == TR_ERR_CLOSED);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	/* 释放逻辑 transfer 后，客户端仍拥有压缩任务及输入字节；完成回调
	 * 必须收敛这些所有权，drain 才能推进。 */
	assert(tr_client_connection_group_release_transfer(client, 5001U) ==
	       TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 0U &&
	       client_stats.send_bytes_inflight == sizeof(source));
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);

	release_group_zstd_gate();
	wait_group_client_send_bytes_empty(client);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 0U &&
	       client_stats.send_bytes_inflight == 0U &&
	       client_stats.data_connections == 1U);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.messages == 0U);
	pthread_mutex_unlock(&ctx.lock);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	assert(tr_server_connection_group_wait_drained(
		       server, 5000U) == TR_OK);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 0U &&
	       server_stats.connections_current == 0U &&
	       server_stats.active_transfers == 0U);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: compression drain passed");
}

static void test_public_connection_group_drain_with_compressed_wire_inflight(
        void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group = {TEST_GROUP_ID, TEST_GROUP_EPOCH};
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	struct tr_transport_bytes bytes;
	uint8_t source[64];
	uint16_t group_port = 0U;
	unsigned i;

	atomic_store(&group_capability_frames_sent, 0U);
	atomic_store(&block_group_data_tx, 0);
	atomic_store(&hold_group_zstd_completion, 0);
	pthread_mutex_lock(&group_zstd_gate_lock);
	group_zstd_gate_entered = 0;
	group_zstd_gate_released = 0;
	pthread_mutex_unlock(&group_zstd_gate_lock);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 1U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(6501)) == TR_OK);
	wait_data_accepts(server, 1U);
	wait_group_capability_frames(2U);
	assert(tr_server_connection_group_send_transfer_ready(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
		       UINT64_C(6502)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);

	for (i = 0U; i < sizeof(source); ++i)
		source[i] = (uint8_t)(i * 29U + 7U);
	bytes.data = source;
	bytes.len = sizeof(source);
	atomic_store(&hold_group_zstd_completion, 1);
	assert(tr_client_connection_group_send_with_compression(
		       client, 5001U, UINT64_C(6503), &bytes,
		       TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_group_compression_worker_entered();

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(tr_server_connection_group_begin_drain(server) == TR_OK);
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	/* 编码完成后把压缩 wire 保留在 Reactor TX 队列；此时释放逻辑
	 * transfer，排空仍必须等待被接受的 wire 真正发送并归还缓冲。 */
	pthread_mutex_lock(&group_data_tx_probe_lock);
	group_data_tx_blocked_attempts = 0U;
	pthread_mutex_unlock(&group_data_tx_probe_lock);
	atomic_store(&block_group_data_tx, 1);
	release_group_zstd_gate();
	wait_group_data_tx_blocked();
	assert(tr_client_connection_group_release_transfer(client, 5001U) ==
	       TR_OK);
	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.active_transfers == 0U &&
	       client_stats.send_bytes_inflight == sizeof(source));
	assert(tr_client_connection_group_wait_drained(
		       client, 1U) == TR_ERR_TIMEOUT);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	atomic_store(&block_group_data_tx, 0);
	wait_group_client_send_bytes_empty(client);
	wait_counter(&ctx, &ctx.compressed_drain_messages, 1U);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);
	assert(tr_server_connection_group_release_transfer(
		       server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U) == TR_OK);
	assert(tr_server_connection_group_wait_drained(
		       server, 1U) == TR_ERR_TIMEOUT);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	assert(tr_server_connection_group_wait_drained(
		       server, 5000U) == TR_OK);
	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 0U &&
	       server_stats.connections_current == 0U &&
	       server_stats.data_connections_current == 0U &&
	       server_stats.active_transfers == 0U);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_client_destroy_after_group_close(client);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
	puts("connection_group_facade: compressed wire drain passed");
}

static void test_client_compression_reactor_tx_pool_saturation(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group = {TEST_GROUP_ID, TEST_GROUP_EPOCH};
	struct tr_transport_bytes empty = {NULL, 0U};
	struct tr_transport_bytes compressed;
	uint8_t source[] = "bounded tx pool pressure";
	uint16_t group_port = 0U;
	unsigned accepted = 0U;
	unsigned attempt;
	int ret = TR_OK;

	atomic_store(&group_capability_frames_sent, 0U);
	atomic_store(&block_group_data_tx, 0);
	atomic_store(&group_state_aborts, 0U);
	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 2U;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.on_message = on_group_message;
	server_config.connection_groups.on_data_event = on_group_data_event;
	server_config.connection_groups.callback_arg = &ctx;
	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(server, "127.0.0.1", 0U, 16,
	                                         &group_port) == TR_OK);
	assert(group_port != 0U);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.max_active_transfers = 1U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 3072U;
	client_config.connection_groups.on_transfer_ready =
	        on_client_transfer_ready;
	client_config.connection_groups.callback_arg = &ctx;
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	ctx.client = client;
	assert(tr_client_connection_group_connect(client, "127.0.0.1",
	                                          group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);
	assert(wait_data_offer(server, UINT64_C(4601)) == TR_OK);
	wait_data_accepts(server, 1U);
	wait_group_capability_frames(2U);
	assert(tr_server_connection_group_send_transfer_ready(
	               server, TEST_GROUP_ID, TEST_GROUP_EPOCH, 5001U,
	               UINT64_C(4602)) == TR_OK);
	wait_counter(&ctx, &ctx.transfer_ready, 1U);

	/* 保持一个 DATA 发送项在 Reactor 内，再填满有限发送池。 */
	pthread_mutex_lock(&group_data_tx_probe_lock);
	group_data_tx_blocked_attempts = 0U;
	pthread_mutex_unlock(&group_data_tx_probe_lock);
	atomic_store(&block_group_data_tx, 1);
	assert(tr_client_connection_group_send(client, 5001U, UINT64_C(4603),
	                                       &empty) == TR_OK);
	wait_group_data_tx_blocked();
	for (attempt = 0U; attempt < 1024U; ++attempt) {
		ret = tr_client_connection_group_send(
		        client, 5001U, UINT64_C(4700) + attempt, &empty);
		if (ret == TR_AGAIN)
			break;
		assert(ret == TR_OK);
		accepted++;
	}
	assert(ret == TR_AGAIN);
	assert(accepted != 0U);

	/* MessageService 已接受压缩任务后才发现 Reactor TX 池已满。
	 * 整个压缩 DATA 必须失败关闭，不得发出部分消息或遗留预算。
	 */
	compressed.data = source;
	compressed.len = (uint32_t)sizeof(source) - 1U;
	assert(tr_client_connection_group_send_with_compression(
	               client, 5001U, UINT64_C(4800), &compressed,
	               TR_CONNECTION_GROUP_COMPRESSION_ZSTD) == TR_OK);
	wait_counter(&ctx, &ctx.data_events, 1U);
	atomic_store(&block_group_data_tx, 0);
	pthread_mutex_lock(&ctx.lock);
	assert(ctx.last_data_status == TR_OK);
	assert(ctx.messages == 0U);
	pthread_mutex_unlock(&ctx.lock);
	assert(atomic_load(&group_state_aborts) == 1U);
	wait_group_client_send_bytes_empty(client);

	assert(tr_client_connection_group_close(client) == TR_OK);
	wait_for_server_group_count(server, 0U);
	wait_client_destroy_after_group_close(client);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);
	wait_server_destroy_after_release(server);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_public_connection_group_client_drain_cancels_offer(void)
{
	struct tr_test_drain_deadline_probe deadline_probe;
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	struct tr_connection_group_client_stats client_stats;
	struct tr_connection_group_server_stats server_stats;
	uint16_t group_port = 0U;

	memset(&ctx, 0, sizeof(ctx));
	assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&ctx.cond, NULL) == 0);

	tr_server_config_init(&server_config);
	server_config.max_peers = 1U;
	server_config.keepalive_interval_ms = 0U;
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 4U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.callback_arg = &ctx;

	assert(tr_server_create_transport(&server_config, &server) == TR_OK);
	assert(tr_server_connection_group_listen(
		       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
	assert(tr_server_start(server) == TR_OK);

	tr_client_config_init(&client_config);
	client_config.keepalive_interval_ms = 0U;
	client_config.connection_groups.max_data_connections = 1U;
	/* max_active_transfers == 0 继承现有 max_streams 默认值。 */
	assert(client_config.connection_groups.max_active_transfers == 0U);
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);

	group.group_id = TEST_GROUP_ID;
	group.epoch = TEST_GROUP_EPOCH;
	assert(tr_client_connection_group_connect(
		       client, "127.0.0.1", group_port, &group) == TR_OK);
	wait_counter(&ctx, &ctx.authorized, 1U);

	assert(tr_client_connection_group_begin_drain(client) == TR_OK);
	assert(wait_data_offer(server, UINT64_C(4401)) == TR_OK);
	/* 只有排空中的客户端归还第一个精确预留后，才能签发第二个提议。
	 * 不创建 DATA 套接字。
	 */
	assert(wait_data_offer(server, UINT64_C(4402)) == TR_OK);

	memset(&client_stats, 0, sizeof(client_stats));
	assert(tr_client_connection_group_get_stats(
		       client, &client_stats) == TR_OK);
	assert(client_stats.draining == 1U);
	assert(client_stats.data_connections == 0U);
	assert(client_stats.active_transfers == 0U);
	assert(client_stats.active_transfer_limit == client_config.limits.max_streams);
	assert(tr_client_connection_group_wait_drained(
		       client, 5000U) == TR_OK);

	/* 0ms 仍表示无限等待；已排空时直接返回，不读取时钟。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, 0U, 0);
	assert(tr_client_connection_group_wait_drained(
		       client, 0U) == TR_OK);
	tr_test_drain_deadline_probe_end(&deadline_probe, 0U, 0U);

	memset(&server_stats, 0, sizeof(server_stats));
	assert(tr_server_connection_group_get_stats(
		       server, &server_stats) == TR_OK);
	assert(server_stats.groups_current == 1U);
	assert(server_stats.connections_current == 1U);
	assert(server_stats.data_connections_current == 0U);
	assert(server_stats.data_accepts == 0U);

	assert(tr_client_connection_group_close(client) == TR_OK);
	assert(tr_server_connection_group_stop(server) == TR_OK);
	assert(tr_server_drain(server, 5000U) == TR_OK);

	assert(tr_client_destroy(client) == TR_OK);
	assert(tr_server_destroy(server) == TR_OK);
	pthread_cond_destroy(&ctx.cond);
	pthread_mutex_destroy(&ctx.lock);
}

static void test_client_group_destroy_after_remote_control_close(void)
{
	struct tr_server_config server_config;
	struct tr_client_config client_config;
	struct tr_server *server = NULL;
	struct tr_client *client = NULL;
	struct public_group_ctx ctx;
	struct tr_connection_group_id group;
	uint16_t group_port = 0U;
	unsigned i;

	/* 反复覆盖 CONTROL 对端关闭与客户端销毁的交叠窗口。
	 * 销毁不能通过外部线程读取 group->control.reactor 来判断是否需要
	 * 所有者屏障；即使关闭回调已经清空句柄但尚未返回，
	 * 客户端组存储也必须继续存活到回调静止。
	 */
	for (i = 0; i < 16U; ++i) {
		memset(&ctx, 0, sizeof(ctx));
		assert(pthread_mutex_init(&ctx.lock, NULL) == 0);
		assert(pthread_cond_init(&ctx.cond, NULL) == 0);

		tr_server_config_init(&server_config);
		server_config.max_peers = 1U;
		server_config.keepalive_interval_ms = 0U;
		server_config.connection_groups.max_groups = 1U;
		server_config.connection_groups.max_connections = 2U;
		server_config.connection_groups.max_data_connections_per_group = 1U;
		server_config.connection_groups.max_streams_per_group = 1U;
		server_config.connection_groups.authorize = authorize_group;
		server_config.connection_groups.callback_arg = &ctx;
		assert(tr_server_create_transport(&server_config, &server) ==
		       TR_OK);
		assert(tr_server_connection_group_listen(
			       server, "127.0.0.1", 0U, 16, &group_port) == TR_OK);
		assert(tr_server_start(server) == TR_OK);

		tr_client_config_init(&client_config);
		client_config.keepalive_interval_ms = 0U;
		assert(tr_client_create_transport(&client_config, &client) ==
		       TR_OK);

		group.group_id = TEST_GROUP_ID;
		group.epoch = TEST_GROUP_EPOCH;
		assert(tr_client_connection_group_connect(
			       client, "127.0.0.1", group_port, &group) == TR_OK);
		wait_counter(&ctx, &ctx.authorized, 1U);

		/* 从客户端反应器角度看，远端关闭是异步的。 */
		assert(tr_server_connection_group_stop(server) == TR_OK);
		assert(tr_client_destroy(client) == TR_OK);
		client = NULL;

		assert(tr_server_drain(server, 5000U) == TR_OK);
		assert(tr_server_destroy(server) == TR_OK);
		server = NULL;
		pthread_cond_destroy(&ctx.cond);
		pthread_mutex_destroy(&ctx.lock);
	}
}

static void test_transport_constructor_rejects_reconnect(void)
{
	struct tr_client_config cc;
	struct tr_client *client = NULL;

	tr_client_config_init(&cc);
	cc.enable_reconnect = 1;
	assert(tr_client_create_transport(&cc, &client) == TR_ERR_UNSUPPORTED);
	assert(!client);
}

static void test_connection_group_compression_policy_validation(void)
{
	struct tr_client_config client_config;
	struct tr_server_config server_config;
	struct tr_client *client = NULL;
	struct tr_server *server = NULL;
	struct tr_transport_bytes bytes = {(const uint8_t *)"x", 1U};

	tr_client_config_init(&client_config);
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.send_compression_algorithms =
	        UINT32_C(0x80000000);
	client_config.connection_groups.max_encoded_message_bytes = 64U;
	assert(tr_client_create_transport(&client_config, &client) ==
	       TR_ERR_INVALID);
	assert(!client);

	tr_client_config_init(&client_config);
	client_config.connection_groups.max_data_connections = 1U;
	client_config.connection_groups.send_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	client_config.connection_groups.max_encoded_message_bytes = 16U;
	assert(tr_client_create_transport(&client_config, &client) ==
	       TR_ERR_INVALID);
	assert(!client);

	tr_server_config_init(&server_config);
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.receive_compression_algorithms =
	        UINT32_C(0x80000000);
	server_config.connection_groups.max_encoded_message_bytes = 64U;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_INVALID);
	assert(!server);

	tr_server_config_init(&server_config);
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = 1U;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = 4096U;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_INVALID);
	assert(!server);
	server_config.connection_groups.max_inflight_compressed_messages =
	        UINT32_MAX;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_INVALID);
	assert(!server);

	/* Compression budgets are derived before Runtime publication. A receive
	 * builder ceiling whose checked product overflows must fail
	 * transactionally.
	 */
	tr_server_config_init(&server_config);
	server_config.connection_groups.max_groups = 1U;
	server_config.connection_groups.max_connections = 2U;
	server_config.connection_groups.max_data_connections_per_group = 1U;
	server_config.connection_groups.max_streams_per_group = UINT32_MAX;
	server_config.connection_groups.authorize = authorize_group;
	server_config.connection_groups.receive_compression_algorithms =
	        TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD;
	server_config.connection_groups.max_encoded_message_bytes = UINT32_MAX;
	server_config.connection_groups.max_inflight_compressed_messages = 1U;
	assert(tr_server_create_transport(&server_config, &server) ==
	       TR_ERR_BAD_LENGTH);
	assert(!server);

	tr_client_config_init(&client_config);
	assert(tr_client_create_transport(&client_config, &client) == TR_OK);
	assert(tr_client_connection_group_send_with_compression(
	               client, 1U, 1U, &bytes,
	               (enum tr_connection_group_compression)99) ==
	       TR_ERR_INVALID);
	assert(tr_client_destroy(client) == TR_OK);
}

int main(void)
{
	test_transport_constructor_rejects_reconnect();
	test_connection_group_compression_policy_validation();
	test_public_connection_group_server();
	test_public_connection_group_control_loss_fences_old_epoch_data();
	test_public_connection_group_control_loss_preserves_peer_group();
	test_public_connection_group_client_control();
	test_client_group_destroy_retry_preserves_control();
	test_client_group_close_retry_preserves_control();
	test_public_connection_group_client_destroy_with_data_tx_inflight();
	test_public_connection_group_client_data_offer();
	test_public_connection_group_multi_data_drain();
	test_public_connection_group_large_decode_hold_raw_peer();
	test_public_connection_group_sustained_raw_zstd_progress();
	test_public_connection_group_natural_budget_rejection_raw_peer();
	test_public_connection_group_member_failure_replacement();
	test_public_connection_group_multi_group_repeated_failure_replacement();
	test_client_compression_completion_after_route_replacement();
	test_public_connection_group_drain_with_compression_inflight();
	test_public_connection_group_drain_with_compressed_wire_inflight();
	test_client_compression_reactor_tx_pool_saturation();
	test_public_connection_group_client_connector_failure_wakes();
	test_public_connection_group_client_drain_cancels_offer();
	test_client_group_destroy_after_remote_control_close();
	return 0;
}
