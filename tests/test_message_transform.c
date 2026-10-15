#include "../src/message/transform.h"
#include "../src/message/storage.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <zstd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expression)                                                      \
	do {                                                                   \
		if (!(expression)) {                                           \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #expression);                                  \
			abort();                                               \
		}                                                              \
	} while (0)

#define MIB ((size_t)1024 * 1024)

void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
size_t __real_ZSTD_compressStream2(ZSTD_CCtx *context, ZSTD_outBuffer *output,
                                   ZSTD_inBuffer *input, ZSTD_EndDirective end);

size_t __real_ZSTD_decompressStream(ZSTD_DCtx *context, ZSTD_outBuffer *output,
                                    ZSTD_inBuffer *input);

/* 故障只在调用线程生效，不拦截 pthread 自身的分配。 */
static _Thread_local size_t allocation_calls;
static _Thread_local size_t fail_allocation;
static _Thread_local bool allocation_failed;
static _Thread_local struct tr_memory_budget *allocation_budget;

static uint64_t budget_current(const struct tr_memory_budget *budget)
{
	struct tr_memory_budget_stats stats;

	tr_memory_budget_get_stats(budget, &stats);
	CHECK(stats.current_bytes <= stats.limit_bytes);
	CHECK(stats.peak_bytes <= stats.limit_bytes);
	return stats.current_bytes;
}

static bool allocation_rejected(size_t size)
{
	if (!allocation_budget)
		return false;
	CHECK(budget_current(allocation_budget) >= size);
	allocation_calls++;
	if (fail_allocation != 0 && allocation_calls == fail_allocation) {
		allocation_failed = true;
		return true;
	}
	return false;
}

void *__wrap_malloc(size_t size)
{
	return allocation_rejected(size) ? NULL : __real_malloc(size);
}

void *__wrap_calloc(size_t count, size_t size)
{
	CHECK(count == 0 || size <= SIZE_MAX / count);
	return allocation_rejected(count * size) ? NULL
	                                         : __real_calloc(count, size);
}

struct gate {
	pthread_mutex_t mutex;
	pthread_cond_t condition;
	bool entered;
	bool released;
};

static void gate_init(struct gate *gate)
{
	*gate = (struct gate){0};
	CHECK(pthread_mutex_init(&gate->mutex, NULL) == 0);
	CHECK(pthread_cond_init(&gate->condition, NULL) == 0);
}

static void gate_enter(struct gate *gate)
{
	CHECK(pthread_mutex_lock(&gate->mutex) == 0);
	gate->entered = true;
	CHECK(pthread_cond_broadcast(&gate->condition) == 0);
	while (!gate->released)
		CHECK(pthread_cond_wait(&gate->condition, &gate->mutex) == 0);
	CHECK(pthread_mutex_unlock(&gate->mutex) == 0);
}

static void gate_wait(struct gate *gate)
{
	CHECK(pthread_mutex_lock(&gate->mutex) == 0);
	while (!gate->entered)
		CHECK(pthread_cond_wait(&gate->condition, &gate->mutex) == 0);
	CHECK(pthread_mutex_unlock(&gate->mutex) == 0);
}

static void gate_release(struct gate *gate)
{
	CHECK(pthread_mutex_lock(&gate->mutex) == 0);
	gate->released = true;
	CHECK(pthread_cond_broadcast(&gate->condition) == 0);
	CHECK(pthread_mutex_unlock(&gate->mutex) == 0);
}

static void gate_destroy(struct gate *gate)
{
	CHECK(pthread_cond_destroy(&gate->condition) == 0);
	CHECK(pthread_mutex_destroy(&gate->mutex) == 0);
}

static _Thread_local struct gate *compression_gate;
static _Thread_local struct gate *decompression_gate;
static _Thread_local bool bytewise_decode;
static _Thread_local size_t decode_probe_calls;

size_t __wrap_ZSTD_decompressStream(ZSTD_DCtx *context, ZSTD_outBuffer *output,
                                    ZSTD_inBuffer *input)
{
	size_t available = input->size;
	size_t result;

	if (bytewise_decode) {
		if (input->size > input->pos)
			input->size = input->pos + 1;
		if (output->size == 1 && output->pos == 0)
			decode_probe_calls++;
	}
	result = __real_ZSTD_decompressStream(context, output, input);
	input->size = available;
	if (decompression_gate) {
		struct gate *gate = decompression_gate;

		decompression_gate = NULL;
		gate_enter(gate);
	}
	return result;
}

size_t __wrap_ZSTD_compressStream2(ZSTD_CCtx *context, ZSTD_outBuffer *output,
                                   ZSTD_inBuffer *input, ZSTD_EndDirective end)
{
	size_t result =
	        __real_ZSTD_compressStream2(context, output, input, end);

	if (compression_gate) {
		struct gate *gate = compression_gate;

		compression_gate = NULL;
		gate_enter(gate);
	}
	return result;
}

struct source_owner {
	struct tr_message_storage storage;
	_Atomic unsigned releases;
	struct gate *release_gate;
};

static void source_release(void *arg)
{
	struct source_owner *owner = arg;

	if (owner->release_gate)
		gate_enter(owner->release_gate);
	(void)atomic_fetch_add_explicit(&owner->releases, 1,
	                                memory_order_relaxed);
}

static void source_init(struct source_owner *owner,
                        struct tr_memory_budget *budget, const void *data,
                        size_t size, struct tr_message *message)
{
	*owner = (struct source_owner){0};
	atomic_init(&owner->releases, 0);
	*message = (struct tr_message){0};
	CHECK(tr_message_storage_reserve(&owner->storage, budget, size) ==
	      TR_OK);
	CHECK(tr_message_storage_publish(&owner->storage, data, size,
	                                 source_release, owner,
	                                 message) == TR_OK);
}

static unsigned source_releases(const struct source_owner *owner)
{
	return atomic_load_explicit(&owner->releases, memory_order_relaxed);
}

static struct tr_message_transform_config encode_config(void)
{
	return (struct tr_message_transform_config){
	        .kind = TR_MESSAGE_ZSTD_ENCODE,
	        .limits = {4 * MIB, 4 * MIB},
	        .max_workspace_bytes = 16 * MIB,
	        .max_window_log = 20,
	        .compression_level = 3,
	};
}

static enum tr_message_transform_outcome
perform(struct tr_message *input,
        const struct tr_message_transform_config *config,
        struct tr_memory_budget *budget, struct tr_message *output)
{
	struct tr_message_transform *operation = NULL;
	enum tr_message_transform_outcome outcome =
	        TR_MESSAGE_TRANSFORM_INTERNAL;
	struct tr_message_view view;

	CHECK(tr_message_transform_prepare(input, config, budget, &operation) ==
	      TR_OK);
	CHECK(tr_message_view(input, &view) == TR_ERR_STATE);
	CHECK(tr_message_transform_finish(&operation, &outcome, output) ==
	      TR_AGAIN);
	CHECK(tr_message_transform_run(operation) == TR_OK);
	CHECK(tr_message_transform_run(operation) == TR_ERR_STATE);
	CHECK(tr_message_transform_cancel(operation) == TR_ERR_STATE);
	CHECK(tr_message_transform_finish(&operation, &outcome, output) ==
	      TR_OK);
	CHECK(operation == NULL);
	CHECK(tr_message_transform_finish(&operation, &outcome, output) ==
	      TR_ERR_INVALID);
	CHECK(tr_message_view(output, &view) ==
	      (outcome == TR_MESSAGE_TRANSFORM_OK ? TR_OK : TR_ERR_STATE));
	return outcome;
}

static void roundtrip(const uint8_t *data, size_t size)
{
	struct tr_memory_budget input_budget;
	struct tr_memory_budget budget;
	struct source_owner owner;
	struct tr_message input = {0}, encoded = {0}, decoded = {0},
	                  slice = {0};
	struct tr_message_transform_config config = encode_config();
	struct tr_message_view view;
	uint64_t held;
	uint8_t *independent;
	size_t result;

	tr_memory_budget_init(&input_budget, 8 * MIB);
	tr_memory_budget_init(&budget, 64 * MIB);
	source_init(&owner, &input_budget, data, size, &input);
	CHECK(perform(&input, &config, &budget, &encoded) ==
	      TR_MESSAGE_TRANSFORM_OK);
	CHECK(source_releases(&owner) == 1);
	CHECK(budget_current(&input_budget) == 0);
	CHECK(tr_message_view(&encoded, &view) == TR_OK);
	independent = malloc(size != 0 ? size : 1);
	CHECK(independent != NULL);
	result = ZSTD_decompress(independent, size, view.data, view.len);
	CHECK(!ZSTD_isError(result) && result == size);
	CHECK(size == 0 || memcmp(independent, data, size) == 0);
	free(independent);

	config.kind = TR_MESSAGE_ZSTD_DECODE;
	config.decoded_size = size;
	CHECK(perform(&encoded, &config, &budget, &decoded) ==
	      TR_MESSAGE_TRANSFORM_OK);
	CHECK(tr_message_view(&decoded, &view) == TR_OK);
	CHECK(view.len == size &&
	      (size == 0 || memcmp(view.data, data, size) == 0));
	held = budget_current(&budget);
	CHECK(held >= size && held < config.max_workspace_bytes);
	CHECK(tr_message_slice(&decoded, size / 2, size - size / 2, &slice) ==
	      TR_OK);
	CHECK(tr_message_release(&decoded) == TR_OK);
	CHECK(budget_current(&budget) == held);
	CHECK(tr_message_view(&slice, &view) == TR_OK);
	CHECK(view.len == size - size / 2);
	CHECK(tr_message_release(&slice) == TR_OK);
	CHECK(budget_current(&budget) == 0);
}

static void test_roundtrips(void)
{
	static const size_t sizes[] = {0,    1,     2,     15,    63,     255,
	                               1023, 65535, 65536, 65537, 262145, MIB};
	uint8_t *data = malloc(MIB);
	uint32_t random = 0x94189233;

	CHECK(data != NULL);
	for (unsigned pattern = 0; pattern < 2; pattern++) {
		for (size_t i = 0; i < MIB; i++) {
			random ^= random << 13;
			random ^= random >> 17;
			random ^= random << 5;
			data[i] = pattern == 0 ? (uint8_t)random
			                       : (uint8_t)(i % 7);
		}
		for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++)
			roundtrip(data, sizes[i]);
	}
	roundtrip(NULL, 0);
	free(data);
}

static void test_rejection_and_limits(void)
{
	struct tr_memory_budget input_budget, budget;
	struct source_owner owner;
	struct tr_message input = {0}, output = {0};
	struct tr_message_transform *operation = NULL;
	struct tr_message_transform_config config = encode_config();
	struct tr_message_view before, after;
	const uint8_t data[] = "bounded message";

	tr_memory_budget_init(&input_budget, MIB);
	tr_memory_budget_init(&budget, 1);
	source_init(&owner, &input_budget, data, sizeof(data), &input);
	CHECK(tr_message_view(&input, &before) == TR_OK);
	for (unsigned i = 0; i < 64; i++)
		CHECK(tr_message_transform_prepare(&input, &config, &budget,
		                                   &operation) == TR_AGAIN);
	CHECK(operation == NULL && source_releases(&owner) == 0);
	CHECK(budget_current(&budget) == 0);
	CHECK(tr_message_view(&input, &after) == TR_OK);
	CHECK(before.data == after.data && before.len == after.len);

	tr_memory_budget_init(&budget, 0);
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_ERR_INVALID);
	tr_memory_budget_init(&budget, 64 * MIB);
	config.max_workspace_bytes = SIZE_MAX;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_ERR_BAD_LENGTH);
	config = encode_config();
	config.limits.max_decoded_bytes = sizeof(data) - 1;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_ERR_BAD_LENGTH);
	config = encode_config();
	config.max_window_log = 64;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_ERR_INVALID);
	config = encode_config();
	config.kind = 0;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_ERR_UNSUPPORTED);
	CHECK(source_releases(&owner) == 0 && budget_current(&budget) == 0);
	config = encode_config();
	config.limits.max_encoded_bytes = 1;
	CHECK(perform(&input, &config, &budget, &output) ==
	      TR_MESSAGE_TRANSFORM_LIMIT);
	CHECK(source_releases(&owner) == 1 && budget_current(&budget) == 0);

	source_init(&owner, &input_budget, data, sizeof(data), &input);
	config = encode_config();
	config.max_workspace_bytes = 1;
	CHECK(perform(&input, &config, &budget, &output) ==
	      TR_MESSAGE_TRANSFORM_LIMIT);
	CHECK(source_releases(&owner) == 1);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
}

static size_t make_frame(const uint8_t *data, size_t size, uint8_t *output,
                         size_t capacity, bool known_size)
{
	ZSTD_CCtx *context = ZSTD_createCCtx();
	size_t result;

	CHECK(context != NULL);
	CHECK(!ZSTD_isError(ZSTD_CCtx_setParameter(
	        context, ZSTD_c_contentSizeFlag, known_size ? 1 : 0)));
	CHECK(!ZSTD_isError(
	        ZSTD_CCtx_setParameter(context, ZSTD_c_checksumFlag, 1)));
	CHECK(!ZSTD_isError(
	        ZSTD_CCtx_setParameter(context, ZSTD_c_windowLog, 20)));
	result = ZSTD_compress2(context, output, capacity, data, size);
	CHECK(!ZSTD_isError(result));
	CHECK(!ZSTD_isError(ZSTD_freeCCtx(context)));
	return result;
}

static void decode_case(const uint8_t *data, size_t size, size_t decoded_size,
                        unsigned window_log,
                        enum tr_message_transform_outcome expected)
{
	struct tr_memory_budget budget, input_budget;
	struct source_owner owner;
	struct tr_message input = {0}, output = {0};
	struct tr_message_transform_config config = encode_config();

	tr_memory_budget_init(&budget, 64 * MIB);
	tr_memory_budget_init(&input_budget, 16 * MIB);
	config.kind = TR_MESSAGE_ZSTD_DECODE;
	config.decoded_size = decoded_size;
	config.max_window_log = window_log;
	source_init(&owner, &input_budget, data, size, &input);
	CHECK(perform(&input, &config, &budget, &output) == expected);
	CHECK(source_releases(&owner) == 1);
	if (expected == TR_MESSAGE_TRANSFORM_OK)
		CHECK(tr_message_release(&output) == TR_OK);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
}

static void test_decode_boundaries(void)
{
	static const uint8_t skippable[] = {0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0};
	static const uint8_t dictionary[] = {0x28, 0xb5, 0x2f, 0xfd, 0x21, 1,
	                                     1,    9,    0,    0,    'x'};
	uint8_t *data = calloc(1, MIB);
	uint8_t *frame = malloc(2 * ZSTD_compressBound(MIB) + 1);
	size_t length;

	CHECK(data && frame);
	decode_case(NULL, 0, 0, 20, TR_MESSAGE_TRANSFORM_BAD_DATA);
	decode_case(skippable, sizeof(skippable), 0, 20,
	            TR_MESSAGE_TRANSFORM_BAD_DATA);
	decode_case(dictionary, sizeof(dictionary), 1, 20,
	            TR_MESSAGE_TRANSFORM_BAD_DATA);
	for (unsigned known = 0; known < 2; known++) {
		length = make_frame(data, MIB, frame, ZSTD_compressBound(MIB),
		                    known != 0);
		decode_case(frame, length, MIB, 20, TR_MESSAGE_TRANSFORM_OK);
		decode_case(frame, length, MIB, 10, TR_MESSAGE_TRANSFORM_LIMIT);
		decode_case(frame, length - 1, MIB, 20,
		            TR_MESSAGE_TRANSFORM_BAD_DATA);
		decode_case(frame, 4, MIB, 20, TR_MESSAGE_TRANSFORM_BAD_DATA);
		frame[length] = 0;
		decode_case(frame, length + 1, MIB, 20,
		            TR_MESSAGE_TRANSFORM_BAD_DATA);
		memcpy(frame + length, frame, length);
		decode_case(frame, length * 2, MIB, 20,
		            TR_MESSAGE_TRANSFORM_BAD_DATA);
		frame[length - 1] ^= 0x80;
		decode_case(frame, length, MIB, 20,
		            TR_MESSAGE_TRANSFORM_BAD_DATA);
		frame[length - 1] ^= 0x80;
		decode_case(frame, length, MIB + 1, 20,
		            TR_MESSAGE_TRANSFORM_BAD_DATA);
		decode_case(frame, length, MIB - 1, 20,
		            known ? TR_MESSAGE_TRANSFORM_BAD_DATA
		                  : TR_MESSAGE_TRANSFORM_LIMIT);
		decode_case(frame, length, 0, 20,
		            known ? TR_MESSAGE_TRANSFORM_BAD_DATA
		                  : TR_MESSAGE_TRANSFORM_LIMIT);
	}
	length = make_frame(NULL, 0, frame, ZSTD_compressBound(MIB), false);
	decode_case(frame, length, 0, 20, TR_MESSAGE_TRANSFORM_OK);
	free(frame);
	free(data);
}

static size_t allocation_case(const uint8_t *data, size_t size,
                              const struct tr_message_transform_config *config,
                              size_t fail_at)
{
	struct tr_memory_budget budget, input_budget;
	struct source_owner owner;
	struct tr_message input = {0}, output = {0};
	struct tr_message_transform *operation = NULL;
	enum tr_message_transform_outcome outcome;
	int ret;

	tr_memory_budget_init(&budget, 64 * MIB);
	tr_memory_budget_init(&input_budget, 16 * MIB);
	source_init(&owner, &input_budget, data, size, &input);
	allocation_calls = 0;
	allocation_failed = false;
	fail_allocation = fail_at;
	allocation_budget = &budget;
	ret = tr_message_transform_prepare(&input, config, &budget, &operation);
	if (ret == TR_OK) {
		CHECK(tr_message_transform_run(operation) == TR_OK);
		CHECK(tr_message_transform_finish(&operation, &outcome,
		                                  &output) == TR_OK);
		CHECK(outcome == (fail_at ? TR_MESSAGE_TRANSFORM_NOMEM
		                          : TR_MESSAGE_TRANSFORM_OK));
		if (outcome == TR_MESSAGE_TRANSFORM_OK)
			CHECK(tr_message_release(&output) == TR_OK);
	} else {
		CHECK(fail_at && ret == TR_ERR_NOMEM && operation == NULL);
		CHECK(source_releases(&owner) == 0);
		CHECK(tr_message_release(&input) == TR_OK);
	}
	allocation_budget = NULL;
	fail_allocation = 0;
	CHECK(allocation_failed == (fail_at != 0));
	CHECK(source_releases(&owner) == 1);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
	return allocation_calls;
}

static void test_allocation_failures(void)
{
	struct tr_message_transform_config config = encode_config();
	uint8_t *data = calloc(1, MIB);
	uint8_t *frame = malloc(ZSTD_compressBound(MIB));
	size_t count;
	size_t length;

	CHECK(data && frame);
	count = allocation_case(data, MIB, &config, 0);
	CHECK(count >= 5);
	for (size_t i = 1; i <= count; i++)
		(void)allocation_case(data, MIB, &config, i);
	printf("encode_allocation_points=%zu\n", count);
	length = make_frame(data, MIB, frame, ZSTD_compressBound(MIB), false);
	config.kind = TR_MESSAGE_ZSTD_DECODE;
	config.decoded_size = MIB;
	count = allocation_case(frame, length, &config, 0);
	CHECK(count >= 4);
	for (size_t i = 1; i <= count; i++)
		(void)allocation_case(frame, length, &config, i);
	printf("decode_allocation_points=%zu\n", count);
	free(frame);
	free(data);
}

struct runner {
	struct tr_message_transform *operation;
	struct gate *gate;
	struct gate *decode_gate;
};

static void *run_thread(void *arg)
{
	struct runner *runner = arg;

	compression_gate = runner->gate;
	decompression_gate = runner->decode_gate;
	CHECK(tr_message_transform_run(runner->operation) == TR_OK);
	return NULL;
}

static void *cancel_thread(void *arg)
{
	CHECK(tr_message_transform_cancel(arg) == TR_OK);
	return NULL;
}

static void test_cancellation(unsigned phase)
{
	struct tr_memory_budget budget, input_budget;
	struct source_owner owner;
	struct tr_message input = {0}, output = {0};
	struct tr_message_transform *operation = NULL;
	struct tr_message_transform_config config = encode_config();
	enum tr_message_transform_outcome outcome;
	struct gate gate;
	pthread_t worker, cancellers[4];
	struct runner runner;
	uint8_t *data = calloc(1, MIB);
	uint64_t held;

	CHECK(data != NULL);
	gate_init(&gate);
	tr_memory_budget_init(&budget, 64 * MIB);
	tr_memory_budget_init(&input_budget, 16 * MIB);
	source_init(&owner, &input_budget, data, MIB, &input);
	if (phase == 2)
		owner.release_gate = &gate;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_OK);
	if (phase == 0) {
		held = budget_current(&budget);
		CHECK(tr_message_transform_cancel(operation) == TR_OK);
		CHECK(tr_message_transform_cancel(operation) == TR_OK);
		CHECK(budget_current(&budget) == held &&
		      source_releases(&owner) == 0);
		CHECK(tr_message_transform_run(operation) == TR_OK);
	} else {
		runner = (struct runner){.operation = operation,
		                         .gate = phase == 1 ? &gate : NULL};
		CHECK(pthread_create(&worker, NULL, run_thread, &runner) == 0);
		gate_wait(&gate);
		held = budget_current(&budget);
		CHECK(held != 0 && budget_current(&input_budget) == MIB);
		for (size_t i = 0; i < 4; i++)
			CHECK(pthread_create(&cancellers[i], NULL,
			                     cancel_thread, operation) == 0);
		for (size_t i = 0; i < 4; i++)
			CHECK(pthread_join(cancellers[i], NULL) == 0);
		CHECK(budget_current(&budget) == held &&
		      source_releases(&owner) == 0);
		gate_release(&gate);
		CHECK(pthread_join(worker, NULL) == 0);
	}
	CHECK(tr_message_transform_cancel(operation) == TR_ERR_STATE);
	CHECK(tr_message_transform_finish(&operation, &outcome, &output) ==
	      TR_OK);
	CHECK(outcome == TR_MESSAGE_TRANSFORM_CANCELLED && operation == NULL);
	CHECK(source_releases(&owner) == 1);
	CHECK(tr_message_view(&output, &(struct tr_message_view){0}) ==
	      TR_ERR_STATE);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
	gate_destroy(&gate);
	free(data);
}

static void test_output_and_admission(void)
{
	const uint8_t data[] = "retained output";
	struct tr_memory_budget input_budget, budget;
	struct source_owner owners[3];
	struct tr_message inputs[3] = {{0}}, output = {0}, retained = {0};
	struct tr_message_transform_config config = encode_config();
	struct tr_message_transform *operation = NULL, *next = NULL;
	enum tr_message_transform_outcome outcome;
	uint64_t reservation;
	uint64_t held;

	tr_memory_budget_init(&input_budget, MIB);
	tr_memory_budget_init(&budget, 64 * MIB);
	source_init(&owners[0], &input_budget, data, sizeof(data), &inputs[0]);
	CHECK(tr_message_transform_prepare(&inputs[0], &config, &budget,
	                                   &operation) == TR_OK);
	reservation = budget_current(&budget);
	CHECK(tr_message_transform_cancel(operation) == TR_OK);
	CHECK(tr_message_transform_run(operation) == TR_OK);
	CHECK(tr_message_transform_finish(&operation, &outcome, &output) ==
	      TR_OK);
	CHECK(budget_current(&budget) == 0);

	tr_memory_budget_init(&budget, reservation);
	for (size_t i = 0; i < 3; i++)
		source_init(&owners[i], &input_budget, data, sizeof(data),
		            &inputs[i]);
	CHECK(tr_message_transform_prepare(&inputs[0], &config, &budget,
	                                   &operation) == TR_OK);
	CHECK(tr_message_transform_prepare(&inputs[1], &config, &budget,
	                                   &operation) == TR_ERR_STATE);
	CHECK(tr_message_transform_prepare(&inputs[1], &config, &budget,
	                                   &next) == TR_AGAIN);
	CHECK(next == NULL && source_releases(&owners[1]) == 0);
	CHECK(tr_message_transform_cancel(operation) == TR_OK);
	CHECK(tr_message_transform_prepare(&inputs[1], &config, &budget,
	                                   &next) == TR_AGAIN);
	CHECK(tr_message_transform_run(operation) == TR_OK);
	CHECK(tr_message_transform_finish(&operation, &outcome, &output) ==
	      TR_OK);
	CHECK(outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
	CHECK(budget_current(&budget) == 0);

	CHECK(tr_message_transform_prepare(&inputs[1], &config, &budget,
	                                   &operation) == TR_OK);
	CHECK(tr_message_transform_run(operation) == TR_OK);
	/* 已完成也不能覆盖调用方的存活输出；拒绝后同一操作可正确取走。 */
	CHECK(tr_message_transform_finish(&operation, &outcome, &inputs[2]) ==
	      TR_ERR_STATE);
	CHECK(operation != NULL && source_releases(&owners[2]) == 0);
	CHECK(tr_message_transform_finish(&operation, &outcome, &output) ==
	      TR_OK);
	CHECK(outcome == TR_MESSAGE_TRANSFORM_OK);
	held = budget_current(&budget);
	CHECK(held > 0);
	CHECK(tr_message_clone(&output, &retained) == TR_OK);
	CHECK(tr_message_release(&output) == TR_OK);
	CHECK(budget_current(&budget) == held);
	CHECK(tr_message_transform_prepare(&inputs[2], &config, &budget,
	                                   &next) == TR_AGAIN);
	CHECK(tr_message_release(&retained) == TR_OK);
	CHECK(budget_current(&budget) == 0);
	CHECK(perform(&inputs[2], &config, &budget, &output) ==
	      TR_MESSAGE_TRANSFORM_OK);
	CHECK(tr_message_release(&output) == TR_OK);
	for (size_t i = 0; i < 3; i++)
		CHECK(source_releases(&owners[i]) == 1);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
}

static void test_bytewise_trailer(void)
{
	const uint8_t data[] = "a trailer after a full output buffer";
	uint8_t frame[256];
	size_t length =
	        make_frame(data, sizeof(data), frame, sizeof(frame), false);

	bytewise_decode = true;
	decode_probe_calls = 0;
	decode_case(frame, length, sizeof(data), 20, TR_MESSAGE_TRANSFORM_OK);
	CHECK(decode_probe_calls > 0);
	decode_case(frame, length - 1, sizeof(data), 20,
	            TR_MESSAGE_TRANSFORM_BAD_DATA);
	decode_case(frame, length, sizeof(data) - 1, 20,
	            TR_MESSAGE_TRANSFORM_LIMIT);
	bytewise_decode = false;
}

static void test_decode_cancellation(void)
{
	struct tr_memory_budget budget, input_budget;
	struct source_owner owner;
	struct tr_message input = {0}, output = {0};
	struct tr_message_transform *operation = NULL;
	struct tr_message_transform_config config = encode_config();
	enum tr_message_transform_outcome outcome;
	struct gate gate;
	struct runner runner;
	pthread_t worker;
	uint8_t *data = calloc(1, MIB);
	uint8_t *frame = malloc(ZSTD_compressBound(MIB));
	size_t length;

	CHECK(data && frame);
	length = make_frame(data, MIB, frame, ZSTD_compressBound(MIB), false);
	tr_memory_budget_init(&budget, 64 * MIB);
	tr_memory_budget_init(&input_budget, 16 * MIB);
	source_init(&owner, &input_budget, frame, length, &input);
	config.kind = TR_MESSAGE_ZSTD_DECODE;
	config.decoded_size = MIB;
	CHECK(tr_message_transform_prepare(&input, &config, &budget,
	                                   &operation) == TR_OK);
	gate_init(&gate);
	runner = (struct runner){.operation = operation, .decode_gate = &gate};
	CHECK(pthread_create(&worker, NULL, run_thread, &runner) == 0);
	gate_wait(&gate);
	CHECK(source_releases(&owner) == 0);
	CHECK(tr_message_transform_cancel(operation) == TR_OK);
	gate_release(&gate);
	CHECK(pthread_join(worker, NULL) == 0);
	CHECK(tr_message_transform_finish(&operation, &outcome, &output) ==
	      TR_OK);
	CHECK(outcome == TR_MESSAGE_TRANSFORM_CANCELLED);
	CHECK(source_releases(&owner) == 1);
	CHECK(budget_current(&budget) == 0 &&
	      budget_current(&input_budget) == 0);
	gate_destroy(&gate);
	free(frame);
	free(data);
}

int main(void)
{
	CHECK(ZSTD_versionNumber() == 10507);
	test_roundtrips();
	test_rejection_and_limits();
	test_decode_boundaries();
	test_allocation_failures();
	test_output_and_admission();
	test_bytewise_trailer();
	test_decode_cancellation();
	for (unsigned phase = 0; phase < 3; phase++)
		test_cancellation(phase);
	puts("message_transform: ok");
	return 0;
}
