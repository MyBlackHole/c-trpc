#include "../src/rpc/rpc_record.h"
#include "../src/rpc/rpc_wire.h"
#include "../src/message/transform.h"
#include "../src/message/storage.h"
#include "../src/memory_budget.h"
#include "../src/endian.h"
#include "tr/status.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #x);                                           \
			abort();                                               \
		}                                                              \
	} while (0)
#define REQ TR_RPC_RECORD_REQUEST_DIRECTION
#define RESP TR_RPC_RECORD_RESPONSE_DIRECTION
#define H TR_RPC_RECORD_HEADER_SIZE

static const struct tr_rpc_record_limits limits = {
        1024 * 1024, 1024 * 1024, TR_RPC_METADATA_MAX_BYTES,
        TR_RPC_RECORD_ACCEPT_NONE | TR_RPC_RECORD_ACCEPT_ZSTD};
static const struct tr_rpc_record_header open_record = {
        .kind = TR_RPC_RECORD_OPEN};
static const struct tr_rpc_record_header initial = {
        .kind = TR_RPC_RECORD_INITIAL_METADATA};
static const struct tr_rpc_record_header empty_message = {
        .kind = TR_RPC_RECORD_MESSAGE, .codec_id = TR_RPC_CODEC_RAW};
static const struct tr_rpc_record_header half = {
        .kind = TR_RPC_RECORD_HALF_CLOSE};
static const struct tr_rpc_record_header ok = {
        .kind = TR_RPC_RECORD_FINAL_STATUS, .status = TR_RPC_STATUS_OK};
static const struct tr_rpc_record_header denied = {
        .kind = TR_RPC_RECORD_FINAL_STATUS,
        .status = TR_RPC_STATUS_PERMISSION_DENIED};

/* 核心编码/解析/状态转换不得分配；真实消息变换在此检查区间之外运行。 */
void *__real_malloc(size_t);
void *__real_calloc(size_t, size_t);
void *__real_realloc(void *, size_t);
static bool forbid_allocations;
void *__wrap_malloc(size_t size)
{
	CHECK(!forbid_allocations);
	return __real_malloc(size);
}
void *__wrap_calloc(size_t count, size_t size)
{
	CHECK(!forbid_allocations);
	return __real_calloc(count, size);
}
void *__wrap_realloc(void *pointer, size_t size)
{
	CHECK(!forbid_allocations);
	return __real_realloc(pointer, size);
}

static size_t encode(uint8_t *out, const struct tr_rpc_record_header *header,
                     const uint8_t *metadata, const uint8_t *body)
{
	CHECK(tr_rpc_record_encode_header(out, H, header, &limits) == TR_OK);
	if (header->metadata_size)
		memcpy(out + H, metadata, header->metadata_size);
	if (header->encoded_size)
		memcpy(out + H + header->metadata_size, body,
		       header->encoded_size);
	return H + header->metadata_size + (size_t)header->encoded_size;
}

static void reject_decode(const uint8_t *bytes, size_t len,
                          const struct tr_rpc_record_limits *policy,
                          int expected)
{
	struct tr_rpc_record_view out;
	unsigned char saved[sizeof(out)];
	size_t consumed = 73;
	memset(&out, 0xa5, sizeof(out));
	memcpy(saved, &out, sizeof(out));
	CHECK(tr_rpc_record_decode(bytes, len, policy, &out, &consumed) ==
	      expected);
	CHECK(consumed == 73 && !memcmp(&out, saved, sizeof(out)));
}

static void test_golden_and_framing(void)
{
	/* 独立固定向量，避免编码/解码犯同一个偏移错误仍通过往返测试。 */
	static const uint8_t golden[H] = {
	        'T',  'R',  'P',  'C',  2,    0,    1, 0, 0x44, 0x33,
	        0x22, 0x11, 0xdd, 0xcc, 0xbb, 0xaa, 0, 0, 0,    0,
	        0,    0,    0,    0,    0,    0,    0, 0, 0,    0,
	        0,    0,    2,    0,    0,    0,    0, 0, 0,    0,
	        0x15, 0xcd, 0x5b, 7,    0,    0,    0, 0};
	uint8_t bytes[4 * H + 32] = {0};
	struct tr_rpc_record_header request = open_record;
	struct tr_rpc_record_view view;
	struct tr_rpc_record_header decoded;
	size_t size, used, total, offset;

	request.service_id = 0x11223344;
	request.method_id = 0xaabbccdd;
	request.metadata_size = 2;
	request.remaining_timeout_ns = 123456789;
	size = encode(bytes + 1, &request, (const uint8_t *)"kv", NULL);
	CHECK(!memcmp(bytes + 1, golden, H));
	/* 非对齐头和任意拆分不改变解析结果；短读时输出保持原样。 */
	for (size_t i = 0; i < size; ++i)
		reject_decode(bytes + 1, i, &limits, TR_AGAIN);
	CHECK(tr_rpc_record_decode_header(bytes + 1, H, &limits, &decoded,
	                                  &total) == TR_OK);
	CHECK(total == size && decoded.metadata_size == 2);
	CHECK(tr_rpc_record_decode(bytes + 1, size, &limits, &view, &used) ==
	      TR_OK);
	CHECK(used == size && !view.body && view.metadata == bytes + 1 + H);
	CHECK(view.header.remaining_timeout_ns == 123456789);
	offset = size;
	offset += encode(bytes + 1 + offset, &empty_message, NULL, NULL);
	offset += encode(bytes + 1 + offset, &half, NULL, NULL);
	CHECK(tr_rpc_record_decode(bytes + 1, offset, &limits, &view, &used) ==
	              TR_OK &&
	      used == size);
	CHECK(tr_rpc_record_decode(bytes + 1 + used, offset - used, &limits,
	                           &view, &total) == TR_OK);
	CHECK(view.header.kind == TR_RPC_RECORD_MESSAGE && total == H &&
	      !view.body && !view.metadata && !view.header.decoded_size);
	used += total;
	CHECK(tr_rpc_record_decode(bytes + 1 + used, offset - used, &limits,
	                           &view, &total) == TR_OK);
	CHECK(view.header.kind == TR_RPC_RECORD_HALF_CLOSE && total == H);
	CHECK(used + total == offset);
}

static void test_bad_headers(void)
{
	uint8_t bytes[H + 8] = {0}, output[H];
	struct tr_rpc_record_header message = empty_message, modified;
	struct tr_rpc_record_limits policy = limits;
	struct tr_rpc_wire_header legacy = {.version = TR_RPC_WIRE_VERSION,
	                                    .type = TR_RPC_WIRE_REQUEST};
	const uint8_t *payload;
	uint32_t *fields[] = {&modified.service_id,    &modified.method_id,
	                      &modified.codec_id,      &modified.compression,
	                      &modified.encoded_size,  &modified.decoded_size,
	                      &modified.metadata_size, &modified.status};

	CHECK(tr_rpc_wire_encode(bytes, &legacy) == TR_OK);
	reject_decode(bytes, TR_RPC_WIRE_HEADER_SIZE, &limits,
	              TR_ERR_BAD_VERSION);
	encode(bytes, &open_record, NULL, NULL);
	CHECK(tr_rpc_wire_decode(bytes, H, &legacy, &payload) ==
	      TR_ERR_BAD_VERSION);
	bytes[0] = '?';
	reject_decode(bytes, 4, &limits, TR_ERR_BAD_MAGIC);
	bytes[0] = 'T';
	bytes[4] = 3;
	reject_decode(bytes, 6, &limits, TR_ERR_BAD_VERSION);
	bytes[4] = 2;
	bytes[6] = 255;
	reject_decode(bytes, 8, &limits, TR_ERR_BAD_TYPE);

	for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
		modified = half;
		*fields[i] = 1;
		memset(output, 0xab, sizeof(output));
		CHECK(tr_rpc_record_encode_header(output, H, &modified,
		                                  &limits) != TR_OK);
		for (size_t j = 0; j < sizeof(output); ++j)
			CHECK(output[j] == 0xab);
	}
	modified = half;
	modified.remaining_timeout_ns = 1;
	CHECK(tr_rpc_record_encode_header(output, H, &modified, &limits) ==
	      TR_ERR_BAD_FLAGS);
	modified = empty_message;
	modified.metadata_size = 1;
	CHECK(tr_rpc_record_encode_header(output, H, &modified, &limits) ==
	      TR_ERR_BAD_FLAGS);
	modified = denied;
	modified.compression = TR_RPC_RECORD_ZSTD;
	CHECK(tr_rpc_record_encode_header(output, H, &modified, &limits) ==
	      TR_ERR_BAD_FLAGS);

	message.encoded_size = message.decoded_size = 3;
	encode(bytes, &message, NULL, (const uint8_t *)"abc");
	/* 只给固定头，必须先拒绝策略超限，不能等正文到齐才发现。 */
	policy.max_encoded_size = 2;
	reject_decode(bytes, H, &policy, TR_ERR_BAD_LENGTH);
	policy = limits;
	policy.max_decoded_size = 2;
	reject_decode(bytes, H, &policy, TR_ERR_BAD_LENGTH);
	policy = limits;
	tr_put_le32(bytes + 20, UINT32_MAX);
	reject_decode(bytes, H, &policy, TR_ERR_UNSUPPORTED);
	tr_put_le32(bytes + 20, TR_RPC_RECORD_ZSTD);
	policy.accepted_compression = TR_RPC_RECORD_ACCEPT_NONE;
	reject_decode(bytes, H, &policy, TR_ERR_UNSUPPORTED);
	policy = limits;
	tr_put_le32(bytes + 24, 0);
	reject_decode(bytes, H, &policy, TR_ERR_BAD_LENGTH);
	tr_put_le32(bytes + 20, TR_RPC_RECORD_NONE);
	tr_put_le32(bytes + 24, 2);
	reject_decode(bytes, H, &policy, TR_ERR_BAD_LENGTH);
	tr_put_le32(bytes + 24, UINT32_MAX);
	reject_decode(bytes, H, &policy, TR_ERR_BAD_LENGTH);
	encode(bytes, &open_record, NULL, NULL);
	tr_put_le32(bytes + 32, UINT32_MAX);
	reject_decode(bytes, H, &limits, TR_ERR_BAD_LENGTH);
	encode(bytes, &ok, NULL, NULL);
	tr_put_le32(bytes + 36, TR_RPC_STATUS_COUNT);
	reject_decode(bytes, H, &limits, TR_ERR_BAD_TYPE);
	policy.accepted_compression = 0;
	reject_decode(bytes, H, &policy, TR_ERR_INVALID);
	reject_decode(NULL, 0, &policy, TR_ERR_INVALID);
	policy = limits;
	policy.max_metadata_size = TR_RPC_METADATA_MAX_BYTES + 1;
	reject_decode(bytes, H, &policy, TR_ERR_INVALID);
	reject_decode(NULL, 1, &limits, TR_ERR_INVALID);
	reject_decode(NULL, 0, &limits, TR_AGAIN);
	CHECK(tr_rpc_record_encode_header(output, H - 1, &ok, &limits) ==
	      TR_ERR_BAD_LENGTH);
	/* 在32位平台总长溢出也必须拒绝；64位平台不得错误回绕。 */
	policy = (struct tr_rpc_record_limits){UINT32_MAX, UINT32_MAX, 0,
	                                       TR_RPC_RECORD_ACCEPT_NONE};
	message.encoded_size = message.decoded_size = UINT32_MAX;
	CHECK(tr_rpc_record_encode_header(output, H, &message, &policy) ==
	      (SIZE_MAX > UINT32_MAX ? TR_OK : TR_ERR_BAD_LENGTH));
}

static void apply(struct tr_rpc_record_state *s,
                  enum tr_rpc_record_direction direction,
                  const struct tr_rpc_record_header *record, int expected)
{
	struct tr_rpc_record_state previous = *s;
	CHECK(tr_rpc_record_state_apply(s, direction, record) == expected);
	if (expected != TR_OK)
		CHECK(!memcmp(s, &previous, sizeof(*s)));
}

static void test_cardinality_and_terminal(void)
{
	for (int request = TR_RPC_ONE; request <= TR_RPC_MANY; ++request) {
		for (int response = TR_RPC_ONE; response <= TR_RPC_MANY;
		     ++response) {
			struct tr_rpc_record_state s, pending;
			CHECK(tr_rpc_record_state_init(&s, request, response) ==
			      TR_OK);
			apply(&s, REQ, &empty_message, TR_ERR_STATE);
			apply(&s, RESP, &open_record, TR_ERR_STATE);
			apply(&s, REQ, &open_record, TR_OK);
			apply(&s, REQ, &open_record, TR_ERR_STATE);
			apply(&s, RESP, &empty_message, TR_ERR_STATE);
			apply(&s, RESP, &half, TR_ERR_STATE);
			/* 下游背压时只改预检副本，不能推进真实消息计数。 */
			pending = s;
			apply(&pending, REQ, &empty_message, TR_OK);
			CHECK(s.request_messages == 0);
			apply(&s, REQ, &empty_message, TR_OK);
			CHECK(s.request_messages == 1); /* 长度为0也算一条。 */
			apply(&s, REQ, &empty_message,
			      request == TR_RPC_ONE ? TR_ERR_STATE : TR_OK);
			apply(&s, REQ, &half, TR_OK);
			CHECK(s.request_closed && !s.terminal);
			apply(&s, REQ, &empty_message, TR_ERR_STATE);
			apply(&s, RESP, &initial, TR_OK);
			apply(&s, RESP, &initial, TR_ERR_STATE);
			apply(&s, RESP, &empty_message, TR_OK);
			CHECK(!s.terminal); /* 收到响应不能提前报告成功。 */
			apply(&s, RESP, &empty_message,
			      response == TR_RPC_ONE ? TR_ERR_STATE : TR_OK);
			apply(&s, RESP, &ok, TR_OK);
			CHECK(s.terminal && s.final_status == TR_RPC_STATUS_OK);
			apply(&s, RESP, &denied, TR_ERR_STATE);
			CHECK(tr_rpc_record_state_abort(
			              &s, TR_RPC_STATUS_CANCELLED) ==
			      TR_ERR_STATE);
			CHECK(s.final_status == TR_RPC_STATUS_OK);

			CHECK(tr_rpc_record_state_init(&s, request, response) ==
			      TR_OK);
			apply(&s, REQ, &open_record, TR_OK);
			apply(&s, REQ, &half,
			      request == TR_RPC_ONE ? TR_ERR_STATE : TR_OK);
			apply(&s, RESP, &ok,
			      request == TR_RPC_MANY && response == TR_RPC_MANY
			              ? TR_OK
			              : TR_ERR_STATE);
			if (!s.terminal) {
				apply(&s, RESP, &denied, TR_OK);
				CHECK(s.final_status ==
				      TR_RPC_STATUS_PERMISSION_DENIED);
			}
			/* 错误可在无请求体时提前返回，不要求先半关闭。 */
			CHECK(tr_rpc_record_state_init(&s, request, response) ==
			      TR_OK);
			apply(&s, REQ, &open_record, TR_OK);
			apply(&s, RESP, &denied, TR_OK);
			CHECK(s.terminal && !s.request_messages);
		}
	}
	for (int status = 1; status < (int)TR_RPC_STATUS_COUNT; ++status) {
		struct tr_rpc_record_state s, previous;
		CHECK(tr_rpc_record_state_init(&s, TR_RPC_ONE, TR_RPC_ONE) ==
		      TR_OK);
		CHECK(tr_rpc_record_state_abort(&s, status) == TR_OK);
		previous = s;
		CHECK(tr_rpc_record_state_abort(&s, TR_RPC_STATUS_UNKNOWN) ==
		      TR_ERR_STATE);
		apply(&s, REQ, &open_record, TR_ERR_STATE);
		CHECK(!memcmp(&s, &previous, sizeof(s)));
	}
}

static void test_preopen_rejection(void)
{
	for (int request = TR_RPC_ONE; request <= TR_RPC_MANY; ++request) {
		for (int response = TR_RPC_ONE; response <= TR_RPC_MANY;
		     ++response) {
			struct tr_rpc_record_state s, pending, unopened;
			struct tr_rpc_record_header invalid = denied;
			CHECK(tr_rpc_record_state_init(&s, request, response) ==
			      TR_OK);
			unopened = s;
			/* 早拒绝只放宽响应错误终局，不放宽其他记录或字段。 */
			apply(&s, REQ, &initial, TR_ERR_STATE);
			apply(&s, REQ, &empty_message, TR_ERR_STATE);
			apply(&s, REQ, &half, TR_ERR_STATE);
			apply(&s, REQ, &ok, TR_ERR_STATE);
			apply(&s, RESP, &open_record, TR_ERR_STATE);
			apply(&s, RESP, &initial, TR_ERR_STATE);
			apply(&s, RESP, &empty_message, TR_ERR_STATE);
			apply(&s, RESP, &half, TR_ERR_STATE);
			apply(&s, RESP, &ok, TR_ERR_STATE);
			invalid.status = TR_RPC_STATUS_COUNT;
			apply(&s, RESP, &invalid, TR_ERR_BAD_TYPE);
			invalid = denied;
			invalid.service_id = 1;
			apply(&s, RESP, &invalid, TR_ERR_BAD_FLAGS);
			invalid = denied;
			invalid.metadata_size = TR_RPC_METADATA_MAX_BYTES + 1;
			apply(&s, RESP, &invalid, TR_ERR_BAD_LENGTH);
			/* OPEN预检成功但下游尚未接受：真实状态仍可收到拒绝。 */
			pending = s;
			apply(&pending, REQ, &open_record, TR_OK);
			CHECK(pending.opened && !s.opened);
			CHECK(!memcmp(&s, &unopened, sizeof(s)));
			for (unsigned status = 1; status < TR_RPC_STATUS_COUNT;
			     ++status) {
				struct tr_rpc_record_header error = denied;
				error.status = status;
				s = unopened;
				apply(&s, REQ, &error, TR_ERR_STATE);
				apply(&s, RESP, &error, TR_OK);
				CHECK(s.terminal && s.final_status == status);
				CHECK(!s.opened && !s.request_messages &&
				      !s.response_messages &&
				      !s.request_closed && !s.response_headers);
				CHECK(s.initialized);
				CHECK(s.request_cardinality ==
				      unopened.request_cardinality);
				CHECK(s.response_cardinality ==
				      unopened.response_cardinality);
				apply(&s, RESP, &error, TR_ERR_STATE);
				apply(&s, RESP, &ok, TR_ERR_STATE);
				apply(&s, REQ, &open_record, TR_ERR_STATE);
				CHECK(tr_rpc_record_state_abort(
				              &s, TR_RPC_STATUS_CANCELLED) ==
				      TR_ERR_STATE);
				CHECK(s.final_status == status && !s.opened);
				/* 取消先选终局，迟到拒绝不能覆盖它。 */
				s = unopened;
				CHECK(tr_rpc_record_state_abort(
				              &s, TR_RPC_STATUS_CANCELLED) ==
				      TR_OK);
				apply(&s, RESP, &error, TR_ERR_STATE);
				CHECK(s.final_status ==
				              TR_RPC_STATUS_CANCELLED &&
				      !s.opened);
			}
		}
	}
}

static unsigned long explored;
static void explore(struct tr_rpc_record_state state, unsigned depth)
{
	const struct tr_rpc_record_header *events[] = {
	        &open_record, &initial, &empty_message, &half, &ok, &denied};
	if (!depth)
		return;
	/* 每个可达前缀都和取消/超时/断连终局交错；先到终局保持不变。 */
	for (unsigned status = 1; status < TR_RPC_STATUS_COUNT; ++status) {
		struct tr_rpc_record_state aborted = state;
		int ret = tr_rpc_record_state_abort(&aborted,
		                                    (enum tr_rpc_status)status);
		CHECK(ret == (state.terminal ? TR_ERR_STATE : TR_OK));
		CHECK(aborted.terminal);
		CHECK(aborted.final_status ==
		      (state.terminal ? state.final_status : status));
		apply(&aborted, RESP, &ok, TR_ERR_STATE);
		apply(&aborted, REQ, &empty_message, TR_ERR_STATE);
	}
	for (unsigned direction = 0; direction < 2; ++direction) {
		for (size_t i = 0; i < sizeof(events) / sizeof(events[0]);
		     ++i) {
			struct tr_rpc_record_state next = state;
			int ret = tr_rpc_record_state_apply(&next, direction,
			                                    events[i]);
			++explored;
			if (ret != TR_OK) {
				CHECK(!memcmp(&state, &next, sizeof(state)));
				continue;
			}
			CHECK(!state.terminal);
			CHECK(next.opened ==
			      (state.opened ||
			       (direction == REQ &&
			        events[i]->kind == TR_RPC_RECORD_OPEN)));
			if (!next.opened) {
				CHECK(next.terminal && direction == RESP &&
				      events[i]->kind ==
				              TR_RPC_RECORD_FINAL_STATUS &&
				      next.final_status != TR_RPC_STATUS_OK);
				CHECK(!next.request_messages &&
				      !next.response_messages &&
				      !next.request_closed &&
				      !next.response_headers);
			}
			CHECK(next.request_messages >= state.request_messages);
			CHECK(next.response_messages >=
			      state.response_messages);
			CHECK(next.request_cardinality != TR_RPC_ONE ||
			      next.request_messages <= 1);
			CHECK(next.response_cardinality != TR_RPC_ONE ||
			      next.response_messages <= 1);
			if (next.terminal) {
				CHECK(direction == RESP &&
				      events[i]->kind ==
				              TR_RPC_RECORD_FINAL_STATUS);
				CHECK(next.final_status == events[i]->status);
			}
			explore(next, depth - 1);
		}
	}
}

static void test_bounded_sequences(void)
{
	for (int request = TR_RPC_ONE; request <= TR_RPC_MANY; ++request) {
		for (int response = TR_RPC_ONE; response <= TR_RPC_MANY;
		     ++response) {
			struct tr_rpc_record_state state;
			CHECK(tr_rpc_record_state_init(&state, request,
			                               response) == TR_OK);
			explore(state, 7);
		}
	}
	CHECK(explored > 1000);
}

struct input_storage {
	struct tr_message_storage storage;
	unsigned releases;
};
static void released(void *arg)
{
	struct input_storage *input = arg;
	++input->releases;
}
static void input_message(struct input_storage *storage,
                          struct tr_memory_budget *budget, const void *data,
                          size_t size, struct tr_message *message)
{
	*storage = (struct input_storage){0};
	CHECK(tr_message_storage_reserve(&storage->storage, budget, size) ==
	      TR_OK);
	CHECK(tr_message_storage_publish(&storage->storage, data, size,
	                                 released, storage, message) == TR_OK);
}
static void transform_message(struct tr_message *input,
                              const struct tr_message_transform_config *config,
                              struct tr_memory_budget *budget,
                              struct tr_message *output)
{
	struct tr_message_transform *operation = NULL;
	enum tr_message_transform_outcome outcome;
	CHECK(tr_message_transform_prepare(input, config, budget, &operation) ==
	      TR_OK);
	CHECK(tr_message_transform_run(operation) == TR_OK);
	CHECK(tr_message_transform_finish(&operation, &outcome, output) ==
	      TR_OK);
	CHECK(!operation && outcome == TR_MESSAGE_TRANSFORM_OK);
}

static void test_real_message_body_transform(void)
{
	static const uint8_t credentials[] =
	        "authorization: confidential-token";
	uint8_t body[65537], control[H + sizeof(credentials)];
	struct tr_memory_budget budget;
	struct input_storage input_owner, encoded_owner;
	struct tr_message input = {0}, encoded = {0}, decoder_input = {0},
	                  decoded = {0};
	struct tr_message_view compressed, plaintext;
	struct tr_rpc_record_header h = open_record;
	struct tr_rpc_record_view view;
	struct tr_message_transform_config config = {
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {1024 * 1024, 1024 * 1024},
	        .max_workspace_bytes = 16 * 1024 * 1024,
	        .max_window_log = 20,
	        .compression_level = 3};
	size_t consumed, encoded_bytes;
	uint8_t *wire;

	for (size_t i = 0; i < sizeof(body); ++i)
		body[i] = (uint8_t)(i % 251);
	h.metadata_size = sizeof(credentials);
	encode(control, &h, credentials, NULL);
	tr_memory_budget_init(&budget, 32 * 1024 * 1024);
	input_message(&input_owner, &budget, body, sizeof(body), &input);
	transform_message(&input, &config, &budget, &encoded);
	CHECK(input_owner.releases == 1);
	CHECK(tr_message_view(&encoded, &compressed) == TR_OK);
	CHECK(compressed.len <= UINT32_MAX);
	wire = malloc(H + compressed.len);
	CHECK(wire);
	h = empty_message;
	h.compression = TR_RPC_RECORD_ZSTD;
	h.encoded_size = (uint32_t)compressed.len;
	h.decoded_size = sizeof(body);
	encoded_bytes = encode(wire, &h, NULL, compressed.data);
	CHECK(tr_rpc_record_decode(wire, encoded_bytes, &limits, &view,
	                           &consumed) == TR_OK);
	CHECK(!view.metadata && view.body == wire + H);
	CHECK(consumed == encoded_bytes);
	/* 只把已定位的业务体交给真实解压器，不传入控制头、元数据或状态。 */
	input_message(&encoded_owner, &budget, view.body,
	              view.header.encoded_size, &decoder_input);
	config.kind = TR_MESSAGE_ZSTD_DECODE;
	config.decoded_size = view.header.decoded_size;
	transform_message(&decoder_input, &config, &budget, &decoded);
	CHECK(encoded_owner.releases == 1);
	CHECK(tr_message_view(&decoded, &plaintext) == TR_OK);
	CHECK(plaintext.len == sizeof(body) &&
	      !memcmp(plaintext.data, body, sizeof(body)));
	CHECK(tr_rpc_record_decode(control, sizeof(control), &limits, &view,
	                           &consumed) == TR_OK);
	CHECK(!view.body && view.header.kind == TR_RPC_RECORD_OPEN);
	CHECK(!memcmp(view.metadata, credentials, sizeof(credentials)));
	CHECK(tr_message_release(&decoded) == TR_OK);
	CHECK(tr_message_release(&encoded) == TR_OK);
	CHECK(atomic_load(&budget.current_bytes) == 0);
	free(wire);
}

static uint32_t random_word(uint32_t *seed)
{
	*seed ^= *seed << 13;
	*seed ^= *seed >> 17;
	*seed ^= *seed << 5;
	return *seed;
}
static void test_mutated_headers(void)
{
	uint8_t bytes[H + 64], encoded[H];
	uint32_t seed = 0x27182818;
	for (unsigned iteration = 0; iteration < 20000; ++iteration) {
		struct tr_rpc_record_header h = empty_message;
		struct tr_rpc_record_view view;
		size_t consumed = 0, len;
		h.encoded_size = h.decoded_size = 64;
		CHECK(tr_rpc_record_encode_header(bytes, H, &h, &limits) ==
		      TR_OK);
		memset(bytes + H, 0x5a, 64);
		bytes[random_word(&seed) % H] ^= (uint8_t)random_word(&seed);
		len = random_word(&seed) % (sizeof(bytes) + 1);
		if (tr_rpc_record_decode(bytes, len, &limits, &view,
		                         &consumed) == TR_OK) {
			CHECK(consumed <= len && consumed >= H);
			CHECK(tr_rpc_record_encode_header(encoded, H,
			                                  &view.header,
			                                  &limits) == TR_OK);
			CHECK(!memcmp(encoded, bytes, H));
			CHECK(!view.metadata);
			CHECK(view.body == bytes + H);
		}
	}
}

int main(void)
{
	forbid_allocations = true;
	test_golden_and_framing();
	test_bad_headers();
	test_cardinality_and_terminal();
	test_preopen_rejection();
	test_bounded_sequences();
	test_mutated_headers();
	forbid_allocations = false;
	test_real_message_body_transform();
	printf("rpc_record_sequences=%lu\n", explored);
	puts("rpc_record: ok");
	return 0;
}
