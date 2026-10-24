#include "../src/group/pipeline_control_wire_internal.h"
#include "drain_deadline_probe.h"
#include "../src/transport/group/pipeline_listener_internal.h"
#include "../src/group/pipeline_route_internal.h"

#include "../src/crc32c.h"
#include "../src/execution/reactor.h"
#include "../src/execution/reactor_internal.h"
#include "../src/execution/command_queue.h"
#include "../src/memory_budget.h"
#include "../src/message/storage.h"
#include "../src/message/transform.h"
#include "tr/status.h"
#include "../src/transport/protocol/message_caps.h"
#include "../src/transport/protocol/message_envelope.h"
#include "../src/transport/protocol/wire.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <zstd.h>

#define TEST_PIPELINE_ID UINT64_C(0x8101)
#define TEST_EPOCH_1 UINT64_C(61)
#define TEST_EPOCH_2 UINT64_C(62)
#define TEST_CONTROL_GENERATION 17U

struct data_test_ctx {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned frames;
	unsigned events;
	uint16_t last_type;
};

struct held_message_task {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct tr_message input;
	tr_message_service_done_fn done;
	void *done_arg;
	struct tr_task_handle handle;
	unsigned submissions;
	unsigned cancellations;
	unsigned completions;
	struct tr_message_transform_config transform;
	struct tr_memory_budget *budget;
};

/* 只在该测试目标通过 --wrap=tr_command_queue_push 生效。强制下一次 SEND
 * 准入返回 TR_AGAIN，验证 Pipeline 软状态回滚；生产库不包含
 * 故障注入钩子。
 */
static atomic_int fail_next_send_push;
static atomic_uint injected_send_push_failures;
static int fail_next_listener_publish;
static int injected_listener_fd = -1;
static int fail_listener_del_fd = -1;

int __real_tr_reactor_listener_register_publish(
	struct tr_reactor *reactor, int fd,
	tr_reactor_listener_cb callback, void *arg,
	int (*publish)(void *arg), void *publish_arg,
	void (*on_retained)(void *arg), void *retained_arg,
	int *out_retained);

int __real_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event);

static int injected_listener_publish_failure(void *arg)
{
	(void)arg;
	return TR_ERR_STATE;
}

int __wrap_tr_reactor_listener_register_publish(
	struct tr_reactor *reactor, int fd,
	tr_reactor_listener_cb callback, void *arg,
	int (*publish)(void *arg), void *publish_arg,
	void (*on_retained)(void *arg), void *retained_arg,
	int *out_retained)
{
	if (!fail_next_listener_publish)
		return __real_tr_reactor_listener_register_publish(
			reactor, fd, callback, arg, publish, publish_arg,
			on_retained, retained_arg, out_retained);

	fail_next_listener_publish = 0;
	injected_listener_fd = fd;
	fail_listener_del_fd = fd;
	return __real_tr_reactor_listener_register_publish(
		reactor, fd, callback, arg,
		injected_listener_publish_failure, NULL,
		on_retained, retained_arg, out_retained);
}

int __wrap_epoll_ctl(
	int epfd, int op, int fd, struct epoll_event *event)
{
	if (op == EPOLL_CTL_DEL && fd == fail_listener_del_fd) {
		fail_listener_del_fd = -1;
		errno = EIO;
		return -1;
	}
	return __real_epoll_ctl(epfd, op, fd, event);
}

int __real_tr_command_queue_push(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake);

int __wrap_tr_command_queue_push(
	struct tr_command_queue *queue, const struct tr_command *command,
	int *need_wake)
{
	if (command && command->type == TR_CMD_SEND &&
	    atomic_exchange(&fail_next_send_push, 0)) {
		atomic_fetch_add(&injected_send_push_failures, 1U);
		return TR_AGAIN;
	}

	return __real_tr_command_queue_push(queue, command, need_wake);
}

static void fail_next_send_admission(void)
{
	assert(atomic_exchange(&fail_next_send_push, 1) == 0);
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

static void wait_peer_close(int fd)
{
	struct pollfd pfd;
	char byte;
	int ret;

	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = fd;
	pfd.events = POLLIN | POLLHUP;
	do {
		ret = poll(&pfd, 1, 10000);
	} while (ret < 0 && errno == EINTR);
	assert(ret == 1);

	ret = (int)recv(fd, &byte, 1U, 0);
	assert(ret == 0 ||
	       (ret < 0 && (errno == ECONNRESET || errno == ENOTCONN)));
}

static int
authorize_control(const struct tr_connection_group_authorization *authorization,
                  void *arg)
{
	(void)arg;
	if (!authorization ||
	    (authorization->role != TR_CONNECTION_GROUP_ROLE_CONTROL &&
	     authorization->role != TR_CONNECTION_GROUP_ROLE_DATA) ||
	    authorization->group.group_id != TEST_PIPELINE_ID ||
	    (authorization->group.epoch != TEST_EPOCH_1 &&
	     authorization->group.epoch != TEST_EPOCH_2) ||
	    authorization->peer_identity.authentication !=
	            TR_PEER_IDENTITY_NONE ||
	    authorization->peer_identity.uri_san != NULL ||
	    authorization->peer_identity.uri_san_len != 0U ||
	    authorization->control_identity.authentication !=
	            TR_PEER_IDENTITY_NONE)
		return TR_ERR_STALE;
	return TR_OK;
}

static enum tr_frame_disposition data_frame_cb(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, struct tr_frame *frame, void *arg)
{
	struct data_test_ctx *ctx = (struct data_test_ctx *)arg;

	assert(route != NULL);
	assert(route->pipeline_id == TEST_PIPELINE_ID);
	(void)connection;
	pthread_mutex_lock(&ctx->lock);
	ctx->frames++;
	ctx->last_type = frame->header.type;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
	return TR_FRAME_RELEASE;
}

static void data_event_cb(
	const struct tr_pipeline_route_preface *route,
	struct tr_conn_handle connection, enum tr_connection_event event,
	int status, void *arg)
{
	struct data_test_ctx *ctx = (struct data_test_ctx *)arg;

	assert(route != NULL);
	assert(route->pipeline_id == TEST_PIPELINE_ID);
	(void)connection;
	(void)event;
	(void)status;
	pthread_mutex_lock(&ctx->lock);
	ctx->events++;
	pthread_cond_broadcast(&ctx->cond);
	pthread_mutex_unlock(&ctx->lock);
}

static int hold_message_submit(void *context, struct tr_message *input,
                               const struct tr_message_transform_config *config,
                               tr_message_service_done_fn done, void *arg,
                               struct tr_task_handle *handle)
{
	struct held_message_task *task = context;

	assert(task != NULL);
	assert(input != NULL);
	assert(config != NULL);
	assert(config->kind == TR_MESSAGE_ZSTD_DECODE);
	assert(done != NULL);
	assert(handle != NULL);
	pthread_mutex_lock(&task->lock);
	assert(task->done == NULL);
	task->input = *input;
	memset(input, 0, sizeof(*input));
	task->done = done;
	task->done_arg = arg;
	task->handle =
	        (struct tr_task_handle){UINT64_C(0x51), UINT64_C(0x91), 0U};
	task->transform = *config;
	*handle = task->handle;
	task->submissions++;
	pthread_cond_broadcast(&task->cond);
	pthread_mutex_unlock(&task->lock);
	return TR_OK;
}

static int hold_message_cancel(void *context, struct tr_task_handle handle)
{
	struct held_message_task *task = context;

	assert(task != NULL);
	pthread_mutex_lock(&task->lock);
	if (!task->done) {
		pthread_mutex_unlock(&task->lock);
		return TR_ERR_STATE;
	}
	assert(handle.instance == task->handle.instance);
	assert(handle.generation == task->handle.generation);
	assert(handle.slot == task->handle.slot);
	task->cancellations++;
	pthread_cond_broadcast(&task->cond);
	pthread_mutex_unlock(&task->lock);
	return TR_OK;
}

static void wait_held_message_count(struct held_message_task *task,
                                    unsigned *field, unsigned target)
{
	struct timespec deadline;
	int ret = 0;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&task->lock);
	while (*field < target && ret == 0)
		ret = pthread_cond_timedwait(&task->cond, &task->lock,
		                             &deadline);
	assert(*field >= target);
	pthread_mutex_unlock(&task->lock);
}

struct held_message_completion_request {
	struct tr_reactor *reactor;
	struct held_message_task *task;
};

static int complete_held_message_on_owner(void *arg)
{
	struct held_message_completion_request *request = arg;
	struct held_message_task *task = request->task;
	tr_message_service_done_fn done;
	struct tr_task_handle handle;
	struct tr_message input;
	struct tr_message output = {0};
	void *done_arg;
	struct tr_message_service_result result = {TR_MESSAGE_TRANSFORM_OK,
	                                           TR_MESSAGE_SERVICE_ON_OWNER};

	assert(tr_reactor_in_owner_context_for(request->reactor));
	pthread_mutex_lock(&task->lock);
	assert(task->done != NULL);
	done = task->done;
	done_arg = task->done_arg;
	handle = task->handle;
	input = task->input;
	memset(&task->input, 0, sizeof(task->input));
	task->done = NULL;
	task->done_arg = NULL;
	pthread_mutex_unlock(&task->lock);

	assert(tr_message_clone(&input, &output) == TR_OK);
	assert(tr_message_release(&input) == TR_OK);
	done(done_arg, handle, result, &output);
	assert(tr_message_release(&output) == TR_OK);

	pthread_mutex_lock(&task->lock);
	task->completions++;
	pthread_cond_broadcast(&task->cond);
	pthread_mutex_unlock(&task->lock);
	return TR_OK;
}

static int
run_held_message_transform(struct held_message_task *task,
                           enum tr_message_transform_outcome *outcome,
                           struct tr_message *output)
{
	struct tr_message input = {0};
	struct tr_message_transform_config config;
	struct tr_message_transform *operation = NULL;
	struct tr_memory_budget *budget;
	int ret;

	pthread_mutex_lock(&task->lock);
	assert(task->done != NULL);
	input = task->input;
	memset(&task->input, 0, sizeof(task->input));
	config = task->transform;
	budget = task->budget;
	pthread_mutex_unlock(&task->lock);
	assert(budget != NULL);
	ret = tr_message_transform_prepare(&input, &config, budget, &operation);
	if (ret != TR_OK) {
		if (tr_message_view(&input, &(struct tr_message_view){0}) ==
		    TR_OK)
			assert(tr_message_release(&input) == TR_OK);
		return ret;
	}
	assert(tr_message_view(&input, &(struct tr_message_view){0}) ==
	       TR_ERR_STATE);
	ret = tr_message_transform_run(operation);
	if (ret != TR_OK)
		return ret;
	return tr_message_transform_finish(&operation, outcome, output);
}

struct held_transform_completion_request {
	struct tr_reactor *reactor;
	struct held_message_task *task;
	enum tr_message_transform_outcome outcome;
	struct tr_message output;
};

static int complete_held_transform_on_owner(void *arg)
{
	struct held_transform_completion_request *request = arg;
	struct held_message_task *task = request->task;
	tr_message_service_done_fn done;
	struct tr_task_handle handle;
	struct tr_message empty_input;
	void *done_arg;
	struct tr_message_service_result result = {request->outcome,
	                                           TR_MESSAGE_SERVICE_ON_OWNER};

	assert(tr_reactor_in_owner_context_for(request->reactor));
	pthread_mutex_lock(&task->lock);
	assert(task->done != NULL);
	done = task->done;
	done_arg = task->done_arg;
	handle = task->handle;
	empty_input = task->input;
	memset(&task->input, 0, sizeof(task->input));
	task->done = NULL;
	task->done_arg = NULL;
	pthread_mutex_unlock(&task->lock);
	assert(tr_message_view(&empty_input, &(struct tr_message_view){0}) ==
	       TR_ERR_STATE);
	done(done_arg, handle, result,
	     request->outcome == TR_MESSAGE_TRANSFORM_OK ? &request->output
	                                                 : NULL);
	if (request->outcome == TR_MESSAGE_TRANSFORM_OK)
		assert(tr_message_release(&request->output) == TR_OK);

	pthread_mutex_lock(&task->lock);
	task->completions++;
	pthread_cond_broadcast(&task->cond);
	pthread_mutex_unlock(&task->lock);
	return TR_OK;
}

static struct tr_pipeline_route_preface control_route(uint64_t epoch)
{
	struct tr_pipeline_route_preface route;

	memset(&route, 0, sizeof(route));
	route.version = TR_PIPELINE_ROUTE_VERSION;
	route.role = TR_PIPELINE_ROUTE_CONTROL;
	route.owner_shard_id = 0U;
	route.pipeline_id = TEST_PIPELINE_ID;
	route.epoch = epoch;
	route.member_index = TR_PIPELINE_ROUTE_MEMBER_CONTROL;
	route.member_generation = TEST_CONTROL_GENERATION;
	return route;
}

static void send_route(int fd, const struct tr_pipeline_route_preface *route)
{
	uint8_t raw[TR_PIPELINE_ROUTE_PREFACE_SIZE];

	assert(tr_pipeline_route_preface_encode(raw, route) == TR_OK);
	send_all(fd, raw, sizeof(raw));
}

static void build_ping(uint8_t raw[TR_WIRE_HEADER_SIZE])
{
	struct tr_frame_header header;

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_PING;
	header.payload_crc32c = tr_crc32c(NULL, 0U);
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
}

static void send_empty_data(
	int fd, uint32_t stream_id, uint64_t message_id)
{
	struct tr_frame_header header;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_DATA;
	header.flags = TR_FRAME_F_FIRST | TR_FRAME_F_LAST;
	header.stream_id = stream_id;
	header.message_id = message_id;
	header.payload_crc32c = tr_crc32c(NULL, 0U);
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
	send_all(fd, raw, sizeof(raw));
}

static void send_data_frame(int fd, uint32_t flags, uint32_t stream_id,
                            uint64_t message_id, const uint8_t *payload,
                            uint32_t payload_len)
{
	struct tr_frame_header header;
	uint8_t raw[TR_WIRE_HEADER_SIZE];

	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_DATA;
	header.flags = flags;
	header.stream_id = stream_id;
	header.message_id = message_id;
	header.payload_len = payload_len;
	header.payload_crc32c = tr_crc32c(payload, payload_len);
	assert(tr_wire_header_encode(raw, &header) == TR_OK);
	send_all(fd, raw, sizeof(raw));
	if (payload_len != 0U)
		send_all(fd, payload, payload_len);
}

static void send_test_compressed_message(int fd, uint32_t stream_id,
                                         uint64_t message_id)
{
	struct tr_message_envelope envelope;
	uint8_t payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U];

	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 1U;
	envelope.decoded_message_bytes = 1U;
	assert(tr_message_envelope_encode(payload, &envelope) == TR_OK);
	payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE] = 0U;
	send_data_frame(
	        fd, TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        stream_id, message_id, payload, sizeof(payload));
}

static void send_compressed_first_fragment(int fd, uint32_t stream_id,
                                           uint64_t message_id)
{
	struct tr_message_envelope envelope;
	uint8_t payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U];

	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 2U;
	envelope.decoded_message_bytes = 1U;
	assert(tr_message_envelope_encode(payload, &envelope) == TR_OK);
	payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE] = 0U;
	send_data_frame(fd, TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST, stream_id,
	                message_id, payload, sizeof(payload));
}

static void send_compressed_last_fragment(int fd, uint32_t stream_id,
                                          uint64_t message_id)
{
	static const uint8_t last_byte = 1U;

	send_data_frame(fd, TR_FRAME_F_COMPRESSED | TR_FRAME_F_LAST, stream_id,
	                message_id, &last_byte, sizeof(last_byte));
}

static void send_trailing_zstd_message(int fd, uint32_t stream_id,
                                       uint64_t message_id)
{
	struct tr_message_envelope envelope;
	uint8_t source = 0x5aU;
	uint8_t encoded[128];
	uint8_t payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE + sizeof(encoded) + 1U];
	size_t encoded_bytes = ZSTD_compress(encoded, sizeof(encoded), &source,
	                                     sizeof(source), 1);

	assert(!ZSTD_isError(encoded_bytes));
	assert(encoded_bytes + 1U <= sizeof(encoded));
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = (uint32_t)(encoded_bytes + 1U);
	envelope.decoded_message_bytes = sizeof(source);
	assert(tr_message_envelope_encode(payload, &envelope) == TR_OK);
	memcpy(payload + TR_MESSAGE_ENVELOPE_WIRE_SIZE, encoded, encoded_bytes);
	payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE + encoded_bytes] = 0xa5U;
	send_data_frame(
	        fd, TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        stream_id, message_id, payload,
	        (uint32_t)(TR_MESSAGE_ENVELOPE_WIRE_SIZE + encoded_bytes + 1U));
}

static void send_truncated_zstd_message(int fd, uint32_t stream_id,
                                        uint64_t message_id)
{
	struct tr_message_envelope envelope;
	uint8_t source = 0x6bU;
	uint8_t encoded[128];
	uint8_t payload[TR_MESSAGE_ENVELOPE_WIRE_SIZE + sizeof(encoded)];
	size_t encoded_bytes = ZSTD_compress(encoded, sizeof(encoded), &source,
	                                     sizeof(source), 1);
	uint32_t truncated_bytes;

	assert(!ZSTD_isError(encoded_bytes));
	assert(encoded_bytes > 1U);
	truncated_bytes = (uint32_t)encoded_bytes - 1U;
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = truncated_bytes;
	envelope.decoded_message_bytes = sizeof(source);
	assert(tr_message_envelope_encode(payload, &envelope) == TR_OK);
	memcpy(payload + TR_MESSAGE_ENVELOPE_WIRE_SIZE, encoded,
	       truncated_bytes);
	send_data_frame(
	        fd, TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        stream_id, message_id, payload,
	        TR_MESSAGE_ENVELOPE_WIRE_SIZE + truncated_bytes);
}

static void send_message_caps_request(int fd)
{
	struct tr_frame_header header;
	struct tr_message_caps caps = {0U, 0U};
	uint8_t payload[TR_MESSAGE_CAPS_WIRE_SIZE];
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];

	assert(tr_message_caps_encode(payload, &caps) == TR_OK);
	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_MESSAGE_CAPS;
	header.payload_len = sizeof(payload);
	header.payload_crc32c = tr_crc32c(payload, sizeof(payload));
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	send_all(fd, payload, sizeof(payload));
}

static void send_malformed_message_caps_request(int fd)
{
	struct tr_frame_header header;
	uint8_t payload[TR_MESSAGE_CAPS_WIRE_SIZE] = {0};
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];

	/* schema=1, receive mask=0, max encoded size=16 is an invalid pair. */
	payload[0] = 1U;
	payload[8] = 16U;
	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_MESSAGE_CAPS;
	header.payload_len = sizeof(payload);
	header.payload_crc32c = tr_crc32c(payload, sizeof(payload));
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	send_all(fd, payload, sizeof(payload));
}

static void recv_message_caps_response(int fd, struct tr_message_caps *caps)
{
	struct tr_frame_header header;
	struct tr_wire_limits limits = {0};
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_MESSAGE_CAPS_WIRE_SIZE];

	recv_all_timeout(fd, raw_header, sizeof(raw_header));
	assert(tr_wire_header_decode(raw_header, &header) == TR_OK);
	limits.max_payload_len = sizeof(payload);
	assert(tr_wire_header_validate(raw_header, &header, &limits) == TR_OK);
	assert(header.type == TR_FRAME_MESSAGE_CAPS);
	assert(header.flags == 0U);
	assert(header.stream_id == 0U);
	assert(header.message_id == 0U);
	assert(header.payload_len == sizeof(payload));
	recv_all_timeout(fd, payload, sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_message_caps_decode(payload, sizeof(payload), caps) == TR_OK);
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
	assert(header.flags == 0U);
	assert(header.stream_id == 0U);
	assert(header.message_id == expected_message_id);
	assert(header.payload_len == TR_PIPELINE_CONTROL_WIRE_SIZE);

	recv_all_timeout(fd, payload, sizeof(payload));
	assert(tr_crc32c(payload, sizeof(payload)) == header.payload_crc32c);
	assert(tr_pipeline_control_wire_decode(
		       payload, sizeof(payload), message) == TR_OK);
}

static void send_control_message(
	int fd, uint64_t message_id,
	const struct tr_pipeline_control_wire_message *message)
{
	struct tr_frame_header header;
	uint8_t raw_header[TR_WIRE_HEADER_SIZE];
	uint8_t payload[TR_PIPELINE_CONTROL_WIRE_SIZE];

	assert(tr_pipeline_control_wire_encode(payload, message) == TR_OK);
	memset(&header, 0, sizeof(header));
	header.version = TR_WIRE_ENV_VERSION;
	header.type = TR_FRAME_PIPELINE_CONTROL;
	header.message_id = message_id;
	header.payload_len = sizeof(payload);
	header.payload_crc32c = tr_crc32c(payload, sizeof(payload));
	assert(tr_wire_header_encode(raw_header, &header) == TR_OK);
	send_all(fd, raw_header, sizeof(raw_header));
	send_all(fd, payload, sizeof(payload));
}

static void wait_listener_counts(
	struct tr_pipeline_listener *listener,
	uint32_t pipelines, uint32_t connections)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		struct tr_pipeline_listener_stats stats;

		memset(&stats, 0, sizeof(stats));
		assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
		if (stats.pipelines_current == pipelines &&
		    stats.connections_current == connections)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(0 && "listener counters did not converge");
}

static void wait_data_frames(struct data_test_ctx *ctx, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->frames < target)
		assert(pthread_cond_timedwait(
			       &ctx->cond, &ctx->lock, &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static void wait_data_events(struct data_test_ctx *ctx, unsigned target)
{
	struct timespec deadline;

	assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
	deadline.tv_sec += 10;
	pthread_mutex_lock(&ctx->lock);
	while (ctx->events < target)
		assert(pthread_cond_timedwait(
			       &ctx->cond, &ctx->lock, &deadline) == 0);
	pthread_mutex_unlock(&ctx->lock);
}

static int open_ready_data_connection(struct tr_pipeline_listener *listener,
                                      int control, uint16_t port,
                                      uint64_t offer_message_id,
                                      uint64_t ready_message_id,
                                      uint32_t stream_id, int negotiate_caps)
{
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	int data;

	memset(&route, 0, sizeof(route));
	assert(tr_pipeline_listener_send_data_offer(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
	               offer_message_id, &route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, offer_message_id, &offer);
	data = connect_loopback(port);
	send_route(data, &route);
	wait_listener_counts(listener, 1U, 2U);
	assert(tr_pipeline_listener_send_transfer_ready(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1, stream_id,
	               ready_message_id) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, ready_message_id, &ready);
	assert(ready.stream_id == stream_id);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	if (negotiate_caps) {
		struct tr_message_caps peer_caps;

		send_message_caps_request(data);
		recv_message_caps_response(data, &peer_caps);
		assert(peer_caps.receive_algorithm_bits ==
		       TR_MESSAGE_ALGORITHM_ZSTD);
	}
	return data;
}

static int wait_offer_retry(
	struct tr_pipeline_listener *listener, uint64_t message_id,
	struct tr_pipeline_route_preface *route)
{
	struct timespec pause;
	unsigned i;

	pause.tv_sec = 0;
	pause.tv_nsec = 10000000L;
	for (i = 0; i < 1000U; ++i) {
		int ret = tr_pipeline_listener_send_data_offer(
			listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
			message_id, route);

		if (ret == TR_OK)
			return TR_OK;
		assert(ret == TR_AGAIN);
		(void)nanosleep(&pause, NULL);
	}
	return TR_ERR_TIMEOUT;
}

static void test_pipeline_listener_control_and_data(void)
{
	struct tr_test_drain_deadline_probe deadline_probe;
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct tr_pipeline_listener_stats stats;
	struct data_test_ctx data_ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface offer_route;
	struct tr_pipeline_route_preface offer_route2;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message offer2;
	struct tr_pipeline_control_wire_message offer3;
	struct tr_pipeline_control_wire_message ready;
	struct tr_pipeline_control_wire_message cancel;
	uint8_t ping[TR_WIRE_HEADER_SIZE];
	uint16_t port = 0U;
	int bad_control = -1;
	int control = -1;
	int data = -1;
	int duplicate_data = -1;
	int control2 = -1;

	memset(&data_ctx, 0, sizeof(data_ctx));
	assert(pthread_mutex_init(&data_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&data_ctx.cond, NULL) == 0);

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 2U;
	config.connection_capacity = 8U;
	config.data_capacity_per_pipeline = 2U;
	config.stream_affinity_capacity_per_pipeline = 8U;
	config.control_message_count = 8U;
	config.authorize_control = authorize_control;
	config.data_frame_cb = data_frame_cb;
	config.data_event_cb = data_event_cb;
	config.data_callback_arg = &data_ctx;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 16, &port) == TR_OK);
	assert(port != 0U);

	/* 由授权接纳 CONTROL，而非只靠客户端提供的标识。 */
	route = control_route(TEST_EPOCH_1);
	route.pipeline_id++;
	bad_control = connect_loopback(port);
	send_route(bad_control, &route);
	wait_peer_close(bad_control);
	close(bad_control);
	bad_control = -1;
	wait_listener_counts(listener, 0U, 0U);

	/* 通过 TRR1 建立一个真实 CONTROL 连接。 */
	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);

	/* 服务端预留 DATA，并通过 TRP1/TRC1 发出精确能力凭证。 */
	memset(&offer_route, 0, sizeof(offer_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(1001), &offer_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(1001), &offer);
	assert(offer.type == TR_PIPELINE_CONTROL_DATA_OFFER);
	assert(offer.owner_shard_id == 0U);
	assert(offer.pipeline_id == TEST_PIPELINE_ID);
	assert(offer.epoch == TEST_EPOCH_1);
	assert(offer.data_index == offer_route.member_index);
	assert(offer.data_generation == offer_route.member_generation);

	/* 仅有 RESERVED 能力凭证绝不足以发布 READY。 */
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3000U,
		       UINT64_C(999)) == TR_AGAIN);

	/* 精确 DATA 套接字加入同一监听器，TRR1 后开始正常 TRP1。 */
	data = connect_loopback(port);
	send_route(data, &offer_route);
	build_ping(ping);
	send_all(data, ping, sizeof(ping));
	wait_data_frames(&data_ctx, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.last_type == TR_FRAME_PING);
	pthread_mutex_unlock(&data_ctx.lock);
	wait_listener_counts(listener, 1U, 2U);

	/* 重放已为 ATTACHED 的 DATA 能力凭证必须只拒绝新套接字。
	 * 失败清理精确取消 RESERVED 状态，因此不能退役
	 * 已消耗该代次的存活成员关系。
	 */
	duplicate_data = connect_loopback(port);
	send_route(duplicate_data, &offer_route);
	wait_peer_close(duplicate_data);
	close(duplicate_data);
	duplicate_data = -1;
	wait_listener_counts(listener, 1U, 2U);
	build_ping(ping);
	send_all(data, ping, sizeof(ping));
	wait_data_frames(&data_ctx, 2U);

	/* DATA 能力凭证成为 ATTACHED 前不能发出 TRANSFER_READY。 */
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3001U,
		       UINT64_C(1002)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(1002), &ready);
	assert(ready.type == TR_PIPELINE_CONTROL_TRANSFER_READY);
	assert(ready.stream_id == 3001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	assert(tr_pipeline_listener_release_transfer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 3001U) == TR_OK);

	/* 客户端 DATA_CANCEL 由真实 CONTROL 帧回调处理。 */
	memset(&offer_route2, 0, sizeof(offer_route2));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(1003), &offer_route2) == TR_OK);
	memset(&offer2, 0, sizeof(offer2));
	recv_control_message(control, UINT64_C(1003), &offer2);
	cancel = offer2;
	cancel.type = TR_PIPELINE_CONTROL_DATA_CANCEL;
	send_control_message(control, UINT64_C(2001), &cancel);

	/* 一个 DATA 已附接且另有一个预留时，取消帧被实际消耗前，
	 * 另一提议会因容量满而失败。重试证明处理函数只释放该精确预留。
	 */
	memset(&offer_route2, 0, sizeof(offer_route2));
	assert(wait_offer_retry(
		       listener, UINT64_C(1004), &offer_route2) == TR_OK);
	memset(&offer3, 0, sizeof(offer3));
	recv_control_message(control, UINT64_C(1004), &offer3);
	assert(offer3.data_index == offer2.data_index);
	assert(offer3.data_generation != offer2.data_generation);

	/* 致命 CONTROL 丢失使整个软状态组失效：未完成预留被取消，
	 * 已附接 DATA 被关闭，注册表条目被移除，
	 * 之后相同 pipeline_id 可以开始新轮次。
	 */
	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 1U);
	wait_listener_counts(listener, 0U, 0U);

	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
	assert(stats.control_accepts == 1U);
	assert(stats.data_accepts == 1U);
	assert(stats.route_rejections >= 1U);
	assert(stats.pipelines_peak == 1U);
	assert(stats.connections_peak >= 2U);

	/* 旧拆除完成后，接纳相同 pipeline_id 的新轮次。 */
	route = control_route(TEST_EPOCH_2);
	control2 = connect_loopback(port);
	send_route(control2, &route);
	wait_listener_counts(listener, 1U, 1U);

	/* 保留真实 CONTROL 会话进入排空。限时等待必须使用公共
	 * CLOCK_MONOTONIC 绝对期限，不得再次进行 uint64 纳秒转换。
	 */
	assert(tr_pipeline_listener_begin_drain(listener) == TR_OK);
	tr_test_drain_deadline_probe_begin(&deadline_probe, 10U, 0);
	assert(tr_pipeline_listener_wait_drained(listener, 10U) ==
	       TR_ERR_TIMEOUT);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 1U);

	/* 超大有限期限不能被有符号 time_t 转换误认为无限等待。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, UINT32_MAX, 0);
	assert(tr_pipeline_listener_wait_drained(listener, UINT32_MAX) ==
	       TR_ERR_TIMEOUT);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 1U);

	/* 截止时间初始化失败不进入条件等待，也不能清除会话所有权。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, 10U, 1);
	assert(tr_pipeline_listener_wait_drained(listener, 10U) ==
	       TR_ERR_SYS);
	tr_test_drain_deadline_probe_end(&deadline_probe, 1U, 0U);

	/* stop() 本身必须完成存活 CONTROL 会话的致命拆除，而不是要求
	 * 调用方先关闭对端套接字。成功返回后销毁只能做纯资源释放。
	 */
	assert(tr_pipeline_listener_stop(listener) == TR_OK);

	/* 0ms 仍为无限等待；排空完成后无需创建截止时间或调用条件等待。 */
	tr_test_drain_deadline_probe_begin(&deadline_probe, 0U, 0);
	assert(tr_pipeline_listener_wait_drained(listener, 0U) == TR_OK);
	tr_test_drain_deadline_probe_end(&deadline_probe, 0U, 0U);
	wait_peer_close(control2);
	close(control2);
	control2 = -1;
	wait_listener_counts(listener, 0U, 0U);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	listener = NULL;
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);

	pthread_cond_destroy(&data_ctx.cond);
	pthread_mutex_destroy(&data_ctx.lock);
}

static void test_pipeline_listener_ready_ingress_barrier(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct data_test_ctx data_ctx;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_route_preface data_route2;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message offer2;
	struct tr_pipeline_control_wire_message ready;
	struct tr_message_caps peer_caps;
	uint16_t port = 0U;
	int control = -1;
	int data = -1;
	int data2 = -1;

	memset(&data_ctx, 0, sizeof(data_ctx));
	assert(pthread_mutex_init(&data_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&data_ctx.cond, NULL) == 0);

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 5U;
	config.data_capacity_per_pipeline = 2U;
	config.stream_affinity_capacity_per_pipeline = 4U;
	config.control_message_count = 4U;
	config.receive_compression_algorithms = TR_MESSAGE_ALGORITHM_ZSTD;
	config.max_encoded_message_bytes = 4096U;
	config.authorize_control = authorize_control;
	config.data_frame_cb = data_frame_cb;
	config.data_event_cb = data_event_cb;
	config.data_callback_arg = &data_ctx;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 16, &port) == TR_OK);

	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);

	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5001), &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(5001), &offer);

	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);

	/* 物理 DATA 成员关系不是传输授权。TRANSFER_READY 前的 DATA
	 * 是协议违规：不调用应用回调，违规 DATA 通道被退役。
	 */
	send_empty_data(data, 7001U, UINT64_C(6001));
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 1U);
	wait_listener_counts(listener, 1U, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* 重新建立 DATA、发布 READY，然后接纳同一流。 */
	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5002), &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(5002), &offer);
	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);

	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7001U,
		       UINT64_C(5003)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(5003), &ready);
	assert(ready.stream_id == 7001U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	send_message_caps_request(data);
	recv_message_caps_response(data, &peer_caps);
	assert(peer_caps.receive_algorithm_bits == TR_MESSAGE_ALGORITHM_ZSTD);
	assert(peer_caps.max_encoded_wire_bytes == 4096U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* READY 后增加第二个 DATA 成员关系。该流固定绑定到第一个精确代次，
	 * 因此在另一存活通道上重放它同样对连接致命，不得到达应用。
	 */
	memset(&data_route2, 0, sizeof(data_route2));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(5004), &data_route2) == TR_OK);
	memset(&offer2, 0, sizeof(offer2));
	recv_control_message(control, UINT64_C(5004), &offer2);
	data2 = connect_loopback(port);
	send_route(data2, &data_route2);
	wait_listener_counts(listener, 1U, 3U);
	send_malformed_message_caps_request(data2);
	wait_peer_close(data2);
	close(data2);
	data2 = -1;
	wait_data_events(&data_ctx, 2U);
	wait_listener_counts(listener, 1U, 2U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* Malformed negotiation retires only that DATA route. Reuse the route
	 * slot and verify it still cannot replay the stream bound to data.
	 */
	memset(&data_route2, 0, sizeof(data_route2));
	assert(tr_pipeline_listener_send_data_offer(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1, UINT64_C(5005),
	               &data_route2) == TR_OK);
	memset(&offer2, 0, sizeof(offer2));
	recv_control_message(control, UINT64_C(5005), &offer2);
	data2 = connect_loopback(port);
	send_route(data2, &data_route2);
	wait_listener_counts(listener, 1U, 3U);
	send_empty_data(data2, 7001U, UINT64_C(6002));
	wait_peer_close(data2);
	close(data2);
	data2 = -1;
	wait_data_events(&data_ctx, 3U);
	wait_listener_counts(listener, 1U, 2U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* READY 流在其最初选择的 DATA 通道上仍有效。 */
	send_empty_data(data, 7001U, UINT64_C(6003));
	wait_data_frames(&data_ctx, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.last_type == TR_FRAME_DATA);
	pthread_mutex_unlock(&data_ctx.lock);
	assert(tr_pipeline_listener_release_transfer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7001U) == TR_OK);

	/* A duplicate request is a protocol error on this DATA generation only.
	 */
	send_message_caps_request(data);
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 4U);
	wait_listener_counts(listener, 1U, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 1U);
	pthread_mutex_unlock(&data_ctx.lock);

	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_listener_counts(listener, 0U, 0U);

	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	tr_reactor_destroy(reactor);
	pthread_cond_destroy(&data_ctx.cond);
	pthread_mutex_destroy(&data_ctx.lock);
}

static void test_pipeline_listener_compressed_route_retirement(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct tr_pipeline_listener_stats stats;
	struct tr_memory_budget message_budget;
	struct tr_memory_budget_stats budget_stats;
	struct held_message_task task = {0};
	struct held_message_completion_request completion;
	struct held_transform_completion_request transform_completion = {0};
	struct data_test_ctx data_ctx = {0};
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	struct tr_message_caps peer_caps;
	struct tr_message_envelope envelope;
	uint8_t bad_envelope[TR_MESSAGE_ENVELOPE_WIRE_SIZE];
	uint8_t malformed_data[TR_MESSAGE_ENVELOPE_WIRE_SIZE + 2U];
	uint64_t listener_budget_bytes;
	uint16_t port = 0U;
	int control = -1;
	int data = -1;

	assert(pthread_mutex_init(&task.lock, NULL) == 0);
	assert(pthread_cond_init(&task.cond, NULL) == 0);
	assert(pthread_mutex_init(&data_ctx.lock, NULL) == 0);
	assert(pthread_cond_init(&data_ctx.cond, NULL) == 0);
	tr_memory_budget_init(&message_budget, 4U * 1024U * 1024U);
	task.budget = &message_budget;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);
	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 2U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 2U;
	config.control_message_count = 4U;
	config.receive_compression_algorithms = TR_MESSAGE_ALGORITHM_ZSTD;
	config.max_message_bytes = 128U;
	config.max_encoded_message_bytes = 128U;
	config.message_task_capacity = 2U;
	config.message_submit = hold_message_submit;
	config.message_cancel = hold_message_cancel;
	config.message_context = &task;
	config.message_budget = &message_budget;
	config.authorize_control = authorize_control;
	config.data_frame_cb = data_frame_cb;
	config.data_event_cb = data_event_cb;
	config.data_callback_arg = &data_ctx;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	listener_budget_bytes = atomic_load(&message_budget.current_bytes);
	assert(listener_budget_bytes != 0U);
	assert(tr_pipeline_listener_listen_ipv4(listener, "127.0.0.1", 0U, 8,
	                                        &port) == TR_OK);

	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);
	assert(tr_pipeline_listener_send_data_offer(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1, UINT64_C(7101),
	               &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(7101), &offer);
	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);
	assert(tr_pipeline_listener_send_transfer_ready(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7201U,
	               UINT64_C(7102)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(7102), &ready);
	assert(ready.stream_id == 7201U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	send_message_caps_request(data);
	recv_message_caps_response(data, &peer_caps);
	assert(peer_caps.receive_algorithm_bits == TR_MESSAGE_ALGORITHM_ZSTD);
	assert(tr_pipeline_listener_send_transfer_ready(
	               listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 7202U,
	               UINT64_C(7103)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(7103), &ready);
	assert(ready.stream_id == 7202U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);

	/* 两条流的压缩消息交错分片。第一条完成后产生一个挂起任务，关闭
	 * DATA 连接还要回收第二条流的部分编码体。
	 */
	send_compressed_first_fragment(data, 7201U, UINT64_C(7203));
	send_compressed_first_fragment(data, 7202U, UINT64_C(7204));
	send_compressed_last_fragment(data, 7201U, UINT64_C(7203));
	wait_held_message_count(&task, &task.submissions, 1U);
	tr_memory_budget_get_stats(&message_budget, &budget_stats);
	assert(budget_stats.current_bytes > listener_budget_bytes);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);
	assert(shutdown(data, SHUT_RDWR) == 0);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 1U);
	wait_listener_counts(listener, 1U, 1U);
	wait_held_message_count(&task, &task.cancellations, 1U);
	/* 在旧任务完成前让新 DATA 代次占用复用后的连接槽。旧任务不能把
	 * 成功结果投递到这个新路由。
	 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7104), UINT64_C(7105), 7210U,
	                                  0);

	completion.reactor = reactor;
	completion.task = &task;
	assert(tr_reactor_call(reactor, complete_held_message_on_owner,
	                       &completion) == TR_OK);
	wait_held_message_count(&task, &task.completions, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);
	tr_memory_budget_get_stats(&message_budget, &budget_stats);
	assert(budget_stats.current_bytes == listener_budget_bytes);
	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
	assert(stats.data_connections_current == 1U);
	/* 即使 DATA route 已发送 READY，没有该路由代次的 CAPS 请求仍不能
	 * 接受压缩帧；拒绝只关闭这个 DATA 连接。
	 */
	send_test_compressed_message(data, 7210U, UINT64_C(7206));
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 2U);
	wait_listener_counts(listener, 1U, 1U);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* 正确协商后，未知 codec 的信封仍是协议错误，且绝不提交解码任务。 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7107), UINT64_C(7108), 7203U,
	                                  1);
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 1U;
	envelope.decoded_message_bytes = 1U;
	assert(tr_message_envelope_encode(bad_envelope, &envelope) == TR_OK);
	bad_envelope[2] = 2U;
	send_data_frame(
	        data,
	        TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        7203U, UINT64_C(7209), bad_envelope, sizeof(bad_envelope));
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 3U);
	wait_listener_counts(listener, 1U, 1U);
	pthread_mutex_lock(&task.lock);
	assert(task.submissions == 1U);
	pthread_mutex_unlock(&task.lock);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* LAST 提前到达时，已收编码长度必须等于信封声明值；接收槽和任务
	 * 预约都要随连接回收，不能留下预算占用。
	 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7110), UINT64_C(7111), 7204U,
	                                  1);
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 2U;
	envelope.decoded_message_bytes = 1U;
	assert(tr_message_envelope_encode(malformed_data, &envelope) == TR_OK);
	malformed_data[TR_MESSAGE_ENVELOPE_WIRE_SIZE] = 0U;
	send_data_frame(data,
	                TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST |
	                        TR_FRAME_F_LAST,
	                7204U, UINT64_C(7212), malformed_data,
	                TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U);
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 4U);
	wait_listener_counts(listener, 1U, 1U);

	/* 解码长度上限在分配编码体之前检查。 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7113), UINT64_C(7114), 7205U,
	                                  1);
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 1U;
	envelope.decoded_message_bytes = 129U;
	assert(tr_message_envelope_encode(bad_envelope, &envelope) == TR_OK);
	memcpy(malformed_data, bad_envelope, sizeof(bad_envelope));
	malformed_data[TR_MESSAGE_ENVELOPE_WIRE_SIZE] = 0U;
	send_data_frame(data,
	                TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST |
	                        TR_FRAME_F_LAST,
	                7205U, UINT64_C(7215), malformed_data,
	                TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U);
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 5U);
	wait_listener_counts(listener, 1U, 1U);

	/* 实际编码体也不能超出声明长度；尾随字节不能被忽略或拼入下一条
	 * 消息。
	 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7116), UINT64_C(7117), 7206U,
	                                  1);
	memset(&envelope, 0, sizeof(envelope));
	envelope.codec_id = TR_MESSAGE_CODEC_ZSTD;
	envelope.encoded_body_bytes = 1U;
	envelope.decoded_message_bytes = 1U;
	assert(tr_message_envelope_encode(malformed_data, &envelope) == TR_OK);
	malformed_data[TR_MESSAGE_ENVELOPE_WIRE_SIZE] = 0U;
	malformed_data[TR_MESSAGE_ENVELOPE_WIRE_SIZE + 1U] = 1U;
	send_data_frame(
	        data,
	        TR_FRAME_F_COMPRESSED | TR_FRAME_F_FIRST | TR_FRAME_F_LAST,
	        7206U, UINT64_C(7218), malformed_data, sizeof(malformed_data));
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 6U);
	wait_listener_counts(listener, 1U, 1U);
	tr_memory_budget_get_stats(&message_budget, &budget_stats);
	assert(budget_stats.current_bytes == listener_budget_bytes);
	pthread_mutex_lock(&task.lock);
	assert(task.submissions == 1U);
	pthread_mutex_unlock(&task.lock);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* 完整帧后附带尾随字节仍必须由真正的 Zstandard 解码拒绝，不能
	 * 因信封累计长度正确而交付消息。
	 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7119), UINT64_C(7120), 7207U,
	                                  1);
	send_trailing_zstd_message(data, 7207U, UINT64_C(7221));
	wait_held_message_count(&task, &task.submissions, 2U);
	transform_completion.reactor = reactor;
	transform_completion.task = &task;
	transform_completion.output = (struct tr_message){0};
	assert(run_held_message_transform(&task, &transform_completion.outcome,
	                                  &transform_completion.output) ==
	       TR_OK);
	assert(transform_completion.outcome == TR_MESSAGE_TRANSFORM_BAD_DATA);
	assert(tr_reactor_call(reactor, complete_held_transform_on_owner,
	                       &transform_completion) == TR_OK);
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 7U);
	wait_listener_counts(listener, 1U, 1U);
	tr_memory_budget_get_stats(&message_budget, &budget_stats);
	assert(budget_stats.current_bytes == listener_budget_bytes);
	pthread_mutex_lock(&task.lock);
	assert(task.submissions == 2U);
	assert(task.completions == 2U);
	pthread_mutex_unlock(&task.lock);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	/* 信封长度完整但 Zstandard 帧被截断时，异步解码也必须拒绝并关闭
	 * 当前 DATA 代次。
	 */
	data = open_ready_data_connection(listener, control, port,
	                                  UINT64_C(7122), UINT64_C(7123), 7208U,
	                                  1);
	send_truncated_zstd_message(data, 7208U, UINT64_C(7224));
	wait_held_message_count(&task, &task.submissions, 3U);
	transform_completion.outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
	transform_completion.output = (struct tr_message){0};
	assert(run_held_message_transform(&task, &transform_completion.outcome,
	                                  &transform_completion.output) ==
	       TR_OK);
	assert(transform_completion.outcome == TR_MESSAGE_TRANSFORM_BAD_DATA);
	assert(tr_reactor_call(reactor, complete_held_transform_on_owner,
	                       &transform_completion) == TR_OK);
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_data_events(&data_ctx, 8U);
	wait_listener_counts(listener, 1U, 1U);
	tr_memory_budget_get_stats(&message_budget, &budget_stats);
	assert(budget_stats.current_bytes == listener_budget_bytes);
	pthread_mutex_lock(&task.lock);
	assert(task.submissions == 3U);
	assert(task.completions == 3U);
	pthread_mutex_unlock(&task.lock);
	pthread_mutex_lock(&data_ctx.lock);
	assert(data_ctx.frames == 0U);
	pthread_mutex_unlock(&data_ctx.lock);

	memset(&stats, 0, sizeof(stats));
	assert(tr_pipeline_listener_get_stats(listener, &stats) == TR_OK);
	assert(stats.data_connections_current == 0U);
	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_listener_counts(listener, 0U, 0U);
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	listener = NULL;
	assert(atomic_load(&message_budget.current_bytes) == 0U);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
	pthread_cond_destroy(&data_ctx.cond);
	pthread_mutex_destroy(&data_ctx.lock);
	pthread_cond_destroy(&task.cond);
	pthread_mutex_destroy(&task.lock);
}

static void test_pipeline_send_failure_rolls_back_soft_state(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	struct tr_pipeline_route_preface route;
	struct tr_pipeline_route_preface data_route;
	struct tr_pipeline_control_wire_message offer;
	struct tr_pipeline_control_wire_message ready;
	uint16_t port = 0U;
	int control = -1;
	int data = -1;
	unsigned failures_before;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 3U;
	/* capacity=1 使失败提议的泄漏立即可见：
	 * 若回滚留下一个 RESERVED 能力凭证，下一个提议就不可能成功。
	 */
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 1U;
	config.control_message_count = 4U;
	config.authorize_control = authorize_control;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 8, &port) == TR_OK);

	route = control_route(TEST_EPOCH_1);
	control = connect_loopback(port);
	send_route(control, &route);
	wait_listener_counts(listener, 1U, 1U);

	failures_before = atomic_load(&injected_send_push_failures);
	fail_next_send_admission();
	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(9001), &data_route) == TR_AGAIN);
	assert(atomic_load(&injected_send_push_failures) ==
	       failures_before + 1U);
	assert(data_route.member_generation == 0U);

	/* 失败 SEND 必须已取消精确预留。只有一个 DATA 槽位时，
	 * 立即成功证明槽位已回到 FREE。
	 */
	memset(&data_route, 0, sizeof(data_route));
	assert(tr_pipeline_listener_send_data_offer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1,
		       UINT64_C(9002), &data_route) == TR_OK);
	memset(&offer, 0, sizeof(offer));
	recv_control_message(control, UINT64_C(9002), &offer);
	assert(offer.data_index == data_route.member_index);
	assert(offer.data_generation == data_route.member_generation);

	data = connect_loopback(port);
	send_route(data, &data_route);
	wait_listener_counts(listener, 1U, 2U);

	failures_before = atomic_load(&injected_send_push_failures);
	fail_next_send_admission();
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 9101U,
		       UINT64_C(9003)) == TR_AGAIN);
	assert(atomic_load(&injected_send_push_failures) ==
	       failures_before + 1U);

	/* prepare_transfer() 在 SEND 准入前安装了流亲和绑定。
	 * 同一流的重试成功证明失败 SEND 已移除该绑定。
	 */
	assert(tr_pipeline_listener_send_transfer_ready(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 9101U,
		       UINT64_C(9004)) == TR_OK);
	memset(&ready, 0, sizeof(ready));
	recv_control_message(control, UINT64_C(9004), &ready);
	assert(ready.stream_id == 9101U);
	assert(ready.data_index == offer.data_index);
	assert(ready.data_generation == offer.data_generation);
	assert(tr_pipeline_listener_release_transfer(
		       listener, TEST_PIPELINE_ID, TEST_EPOCH_1, 9101U) == TR_OK);

	/* CONTROL 关闭是正常传输终止边界。它使已附接 DATA 成员关系失效，
	 * 并在监听器存储销毁前关闭该 DATA 连接。
	 */
	shutdown(control, SHUT_RDWR);
	close(control);
	control = -1;
	wait_peer_close(data);
	close(data);
	data = -1;
	wait_listener_counts(listener, 0U, 0U);

	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

static void test_pipeline_listener_prestart_teardown(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	uint16_t port = 0U;

	/* 服务端允许先监听再启动 Runtime。此构造阶段没有所有者线程，
	 * 但监听器事件源已经注册进 epoll；begin_drain/stop 必须能够在不启动
	 * 反应器的情况下完整撤销注册并完成纯内存析构。
	 */
	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 2U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 1U;
	config.control_message_count = 2U;
	config.authorize_control = authorize_control;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);
	assert(tr_pipeline_listener_listen_ipv4(
		       listener, "127.0.0.1", 0U, 8, &port) == TR_OK);
	assert(port != 0U);

	assert(tr_pipeline_listener_begin_drain(listener) == TR_OK);
	assert(tr_pipeline_listener_stop(listener) == TR_OK);
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	tr_reactor_destroy(reactor);
}

static void test_listener_publish_rollback_keeps_fd_until_drain(void)
{
	struct tr_reactor *reactor = NULL;
	struct tr_pipeline_listener_config config;
	struct tr_pipeline_listener *listener = NULL;
	uint16_t port = 0U;
	int ret;

	assert(tr_reactor_create(NULL, NULL, NULL, NULL, &reactor) == TR_OK);
	assert(tr_reactor_start(reactor) == TR_OK);

	memset(&config, 0, sizeof(config));
	config.owner = reactor;
	config.owner_shard_id = 0U;
	config.pipeline_capacity = 1U;
	config.connection_capacity = 2U;
	config.data_capacity_per_pipeline = 1U;
	config.stream_affinity_capacity_per_pipeline = 1U;
	config.control_message_count = 2U;
	config.authorize_control = authorize_control;
	assert(tr_pipeline_listener_create(&config, &listener) == TR_OK);

	fail_next_listener_publish = 1;
	ret = tr_pipeline_listener_listen_ipv4(
		listener, "127.0.0.1", 0U, 8, &port);
	assert(ret == TR_ERR_SYS);
	assert(injected_listener_fd >= 0);
	assert(fail_listener_del_fd == -1);
	assert(fcntl(injected_listener_fd, F_GETFD) >= 0);

	/* 反应器仍发布事件源时，监听器必须保留 fd，直到排空成功。 */
	assert(tr_pipeline_listener_begin_drain(listener) == TR_OK);
	errno = 0;
	assert(fcntl(injected_listener_fd, F_GETFD) < 0 && errno == EBADF);
	injected_listener_fd = -1;
	assert(tr_pipeline_listener_destroy(listener) == TR_OK);
	assert(tr_reactor_stop(reactor) == TR_OK);
	assert(tr_reactor_destroy(reactor) == TR_OK);
}

int main(void)
{
	test_pipeline_listener_control_and_data();
	test_pipeline_listener_ready_ingress_barrier();
	test_pipeline_listener_compressed_route_retirement();
	test_pipeline_send_failure_rolls_back_soft_state();
	test_pipeline_listener_prestart_teardown();
	test_listener_publish_rollback_keeps_fd_until_drain();
	assert(atomic_load(&fail_next_send_push) == 0);
	puts("pipeline listener/control transport: ok");
	return 0;
}
