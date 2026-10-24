#define _POSIX_C_SOURCE 200809L
#include "../src/message/message.h"
#include "../src/message/builder.h"
#include "../src/message/storage.h"
#include "../src/memory_budget.h"
#include "tr/status.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 测试操作不放在 assert 中，NDEBUG 不得删除实际验证。 */
#define CHECK(expr)                                                            \
	do {                                                                   \
		if (!(expr)) {                                                 \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,     \
			        #expr);                                        \
			abort();                                               \
		}                                                              \
	} while (0)
#define STATUS(expr, expected) CHECK((expr) == (expected))

static atomic_int deny_allocations;
static atomic_uint denied_allocations;

void *__real_malloc(size_t size);
void *__real_calloc(size_t count, size_t size);
void *__real_realloc(void *ptr, size_t size);

void *__wrap_malloc(size_t size)
{
	if (atomic_load(&deny_allocations)) {
		atomic_fetch_add(&denied_allocations, 1U);
		return NULL;
	}
	return __real_malloc(size);
}

void *__wrap_calloc(size_t count, size_t size)
{
	if (atomic_load(&deny_allocations)) {
		atomic_fetch_add(&denied_allocations, 1U);
		return NULL;
	}
	return __real_calloc(count, size);
}

void *__wrap_realloc(void *ptr, size_t size)
{
	if (atomic_load(&deny_allocations)) {
		atomic_fetch_add(&denied_allocations, 1U);
		return NULL;
	}
	return __real_realloc(ptr, size);
}

struct release_record {
	atomic_uint calls;
};

static void note_release(void *arg)
{
	struct release_record *record = arg;

	atomic_fetch_add(&record->calls, 1U);
}

static uint64_t current_bytes(struct tr_memory_budget *budget)
{
	struct tr_memory_budget_stats stats;

	tr_memory_budget_get_stats(budget, &stats);
	return stats.current_bytes;
}

static void check_view(const struct tr_message *message, const uint8_t *data,
                       size_t len)
{
	struct tr_message_view view;

	STATUS(tr_message_view(message, &view), TR_OK);
	CHECK(view.data == data);
	CHECK(view.len == len);
}

static void test_reservation(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage storage = {0};
	struct tr_message_storage blocked = {0};
	struct tr_memory_budget_stats stats;
	unsigned i;
	void *failed;
	void *(*volatile allocate)(size_t) = malloc;

	tr_memory_budget_init(&budget, 8);
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_OK);
	STATUS(tr_message_storage_reserve(&storage, &budget, 1), TR_ERR_STATE);
	for (i = 0; i < 32; ++i)
		STATUS(tr_message_storage_reserve(&blocked, &budget, 1),
		       TR_AGAIN);
	tr_memory_budget_get_stats(&budget, &stats);
	CHECK(stats.current_bytes == 8 && stats.peak_bytes == 8);
	CHECK(stats.rejection_events == 32);
	STATUS(tr_message_storage_cancel(&blocked), TR_ERR_STATE);
	STATUS(tr_message_storage_cancel(&storage), TR_OK);
	STATUS(tr_message_storage_cancel(&storage), TR_ERR_STATE);
	CHECK(current_bytes(&budget) == 0);

	/* 在真实分配前预约，分配持续失败时由提供方取消预约。 */
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_OK);
	atomic_store(&deny_allocations, 1);
	failed = allocate(8);
	CHECK(failed == NULL);
	STATUS(tr_message_storage_cancel(&storage), TR_OK);
	atomic_store(&deny_allocations, 0);
	CHECK(current_bytes(&budget) == 0);
	CHECK(atomic_exchange(&denied_allocations, 0U) == 1U);

	/* 原预算允许只计账，但 uint64_t 溢出必须拒绝，不能静默回绕。 */
	tr_memory_budget_init(&budget, 0);
	STATUS(tr_memory_budget_reserve(&budget, UINT64_MAX - 1U), TR_OK);
	STATUS(tr_message_storage_reserve(&storage, &budget, 2), TR_ERR_STATE);
	CHECK(current_bytes(&budget) == UINT64_MAX - 1U);
	STATUS(tr_memory_budget_release(&budget, UINT64_MAX - 1U), TR_OK);
	STATUS(tr_message_storage_reserve(&storage, &budget, 2), TR_OK);
	STATUS(tr_message_storage_cancel(&storage), TR_OK);
}

static void test_bounded_builder_publishes_exact_message(void)
{
	struct tr_memory_budget budget;
	struct tr_message_builder *builder = NULL;
	struct tr_message message = {0};
	struct tr_message_view view;

	tr_memory_budget_init(&budget, 256U);
	STATUS(tr_message_builder_create(&budget, 8U, &builder), TR_OK);
	CHECK(current_bytes(&budget) > 8U);
	STATUS(tr_message_builder_append(builder, "abc", 3U), TR_OK);
	STATUS(tr_message_builder_publish(&builder, &message), TR_AGAIN);
	CHECK(builder != NULL && message._storage == NULL);
	STATUS(tr_message_builder_append(builder, "defgh", 5U), TR_OK);
	STATUS(tr_message_builder_append(builder, "x", 1U), TR_ERR_BAD_LENGTH);
	STATUS(tr_message_builder_publish(&builder, &message), TR_OK);
	CHECK(builder == NULL);
	STATUS(tr_message_view(&message, &view), TR_OK);
	CHECK(view.len == 8U);
	CHECK(memcmp(view.data, "abcdefgh", 8U) == 0);
	CHECK(current_bytes(&budget) > 8U);
	STATUS(tr_message_release(&message), TR_OK);
	CHECK(current_bytes(&budget) == 0U);
}

static void test_bounded_builder_cancel_and_budget_failure(void)
{
	struct tr_memory_budget budget;
	struct tr_message_builder *builder = NULL;

	tr_memory_budget_init(&budget, 8U);
	STATUS(tr_message_builder_create(&budget, 8U, &builder), TR_AGAIN);
	CHECK(builder == NULL);
	CHECK(current_bytes(&budget) == 0U);

	tr_memory_budget_init(&budget, 256U);
	STATUS(tr_message_builder_create(&budget, 8U, &builder), TR_OK);
	STATUS(tr_message_builder_append(builder, "abc", 3U), TR_OK);
	STATUS(tr_message_builder_cancel(&builder), TR_OK);
	CHECK(builder == NULL);
	CHECK(current_bytes(&budget) == 0U);
}

static void test_bounded_builder_empty_message(void)
{
	struct tr_memory_budget budget;
	struct tr_message_builder *builder = NULL;
	struct tr_message message = {0};
	struct tr_message_view view;

	tr_memory_budget_init(&budget, 256U);
	STATUS(tr_message_builder_create(&budget, 0U, &builder), TR_OK);
	STATUS(tr_message_builder_append(builder, NULL, 0U), TR_OK);
	STATUS(tr_message_builder_publish(&builder, &message), TR_OK);
	CHECK(builder == NULL);
	STATUS(tr_message_view(&message, &view), TR_OK);
	CHECK(view.len == 0U);
	STATUS(tr_message_release(&message), TR_OK);
	CHECK(current_bytes(&budget) == 0U);
}

static void test_publish_rollback(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage storage = {0};
	struct tr_message_storage second = {0};
	struct tr_message message = {0};
	struct release_record record = {0};
	uint8_t data[8] = {1};

	tr_memory_budget_init(&budget, 16);
	STATUS(tr_message_storage_publish(&storage, data, 8, note_release,
	                                  &record, &message),
	       TR_ERR_STATE);
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_OK);
	STATUS(tr_message_storage_publish(&storage, NULL, 0, note_release,
	                                  &record, &message),
	       TR_ERR_INVALID);
	STATUS(tr_message_storage_publish(&storage, data, 9, note_release,
	                                  &record, &message),
	       TR_ERR_BAD_LENGTH);
	STATUS(tr_message_storage_publish(&storage, data, 8, NULL, &record,
	                                  &message),
	       TR_ERR_INVALID);
	CHECK(atomic_load(&record.calls) == 0);
	CHECK(current_bytes(&budget) == 8);
	STATUS(tr_message_storage_publish(&storage, data, 8, note_release,
	                                  &record, &message),
	       TR_OK);
	STATUS(tr_message_storage_cancel(&storage), TR_ERR_STATE);
	STATUS(tr_message_storage_reserve(&second, &budget, 8), TR_OK);
	STATUS(tr_message_storage_publish(&second, data, 8, note_release,
	                                  &record, &message),
	       TR_ERR_STATE);
	STATUS(tr_message_storage_cancel(&second), TR_OK);
	check_view(&message, data, 8);
	STATUS(tr_message_release(&message), TR_OK);
	STATUS(tr_message_release(&message), TR_ERR_STATE);
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_ERR_STATE);
	CHECK(atomic_load(&record.calls) == 1);
	CHECK(current_bytes(&budget) == 0);
}

static void test_none_and_aliases(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage storage = {0};
	struct tr_message message = {0}, clone = {0}, slice = {0}, moved = {0};
	struct tr_message_limits limits = {8, 8};
	struct release_record record = {0};
	uint8_t data[8] = {0, 1, 2, 3, 4, 5, 6, 7};

	tr_memory_budget_init(&budget, sizeof(data));
	atomic_store(&deny_allocations, 1);
	STATUS(tr_message_storage_reserve(&storage, &budget, sizeof(data)),
	       TR_OK);
	STATUS(tr_message_storage_publish(&storage, data, 8, note_release,
	                                  &record, &message),
	       TR_OK);
	limits.max_decoded_bytes = 7;
	STATUS(tr_message_none_move(&message, &limits, &moved),
	       TR_ERR_BAD_LENGTH);
	limits.max_decoded_bytes = 8;
	limits.max_encoded_bytes = 7;
	STATUS(tr_message_none_move(&message, &limits, &moved),
	       TR_ERR_BAD_LENGTH);
	limits.max_encoded_bytes = 8;
	STATUS(tr_message_none_move(&message, &limits, &message),
	       TR_ERR_INVALID);
	check_view(&message, data, 8);
	STATUS(tr_message_clone(&message, &clone), TR_OK);
	STATUS(tr_message_slice(&clone, 2, 3, &slice), TR_OK);
	STATUS(tr_message_none_move(&message, &limits, &clone), TR_ERR_STATE);
	STATUS(tr_message_none_move(&message, &limits, &moved), TR_OK);
	STATUS(tr_message_release(&message), TR_ERR_STATE);
	check_view(&moved, data, 8);
	check_view(&slice, data + 2, 3);
	CHECK(current_bytes(&budget) == 8);
	STATUS(tr_message_release(&moved), TR_OK);
	STATUS(tr_message_release(&clone), TR_OK);
	CHECK(atomic_load(&record.calls) == 0);
	CHECK(current_bytes(&budget) == 8);
	check_view(&slice, data + 2, 3);
	STATUS(tr_message_release(&slice), TR_OK);
	CHECK(atomic_load(&record.calls) == 1);
	CHECK(current_bytes(&budget) == 0);
	CHECK(atomic_load(&denied_allocations) == 0);
	atomic_store(&deny_allocations, 0);
}

static void test_empty_and_ranges(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage empty_storage = {0}, storage = {0};
	struct tr_message empty = {0}, alias = {0}, moved = {0}, message = {0};
	struct tr_message_limits limits = {0, 0};
	struct tr_message_view untouched = {(const uint8_t *)"x", 123};
	struct release_record record = {0};
	uint8_t data[4] = {1, 2, 3, 4};

	tr_memory_budget_init(&budget, 4);
	STATUS(tr_message_view(&empty, &untouched), TR_ERR_STATE);
	CHECK(untouched.len == 123);
	STATUS(tr_message_storage_reserve(&empty_storage, &budget, 0), TR_OK);
	STATUS(tr_message_storage_publish(&empty_storage, NULL, 0, note_release,
	                                  &record, &empty),
	       TR_OK);
	STATUS(tr_message_slice(&empty, 0, 0, &alias), TR_OK);
	STATUS(tr_message_none_move(&empty, &limits, &moved), TR_OK);
	check_view(&alias, NULL, 0);
	STATUS(tr_message_release(&moved), TR_OK);
	STATUS(tr_message_release(&alias), TR_OK);
	CHECK(atomic_load(&record.calls) == 1);

	STATUS(tr_message_storage_reserve(&storage, &budget, 4), TR_OK);
	STATUS(tr_message_storage_publish(&storage, data, 4, note_release,
	                                  &record, &message),
	       TR_OK);
	STATUS(tr_message_slice(&message, SIZE_MAX, 1, &alias),
	       TR_ERR_BAD_LENGTH);
	STATUS(tr_message_slice(&message, 1, SIZE_MAX, &alias),
	       TR_ERR_BAD_LENGTH);
	STATUS(tr_message_slice(&message, 4, 1, &alias), TR_ERR_BAD_LENGTH);
	STATUS(tr_message_slice(&message, 4, 0, &alias), TR_OK);
	check_view(&alias, data + 4, 0);
	STATUS(tr_message_release(&alias), TR_OK);
	STATUS(tr_message_slice(&message, 1, 3, &alias), TR_OK);
	STATUS(tr_message_slice(&alias, 1, 1, &moved), TR_OK);
	check_view(&moved, data + 2, 1);
	STATUS(tr_message_release(&moved), TR_OK);
	STATUS(tr_message_release(&alias), TR_OK);

	/* 白盒注入引用上限，不需要创建数十亿个别名；失败不得发布输出。 */
	atomic_store(&storage._refs.value, UINT32_MAX - 1U);
	STATUS(tr_message_clone(&message, &alias), TR_ERR_STATE);
	STATUS(tr_message_release(&alias), TR_ERR_STATE);
	atomic_store(&storage._refs.value, 1U);
	STATUS(tr_message_release(&message), TR_OK);
	CHECK(current_bytes(&budget) == 0);
	CHECK(atomic_load(&record.calls) == 2);
}

struct heap_owner {
	struct tr_message_storage storage;
	struct tr_message message;
	struct release_record *record;
	struct tr_memory_budget *budget;
	uint8_t *bytes;
};

static void free_owner(void *arg)
{
	struct heap_owner *owner = arg;

	CHECK(current_bytes(owner->budget) == 8);
	atomic_fetch_add(&owner->record->calls, 1U);
	free(owner->bytes);
	free(owner);
}

static void test_embedded_descriptor_free(void)
{
	struct tr_memory_budget budget;
	struct release_record record = {0};
	struct heap_owner *owner = calloc(1, sizeof(*owner));

	CHECK(owner != NULL);
	tr_memory_budget_init(&budget, 8);
	owner->budget = &budget;
	owner->record = &record;
	STATUS(tr_message_storage_reserve(&owner->storage, &budget, 8), TR_OK);
	owner->bytes = malloc(8);
	CHECK(owner->bytes != NULL);
	STATUS(tr_message_storage_publish(&owner->storage, owner->bytes, 8,
	                                  free_owner, owner, &owner->message),
	       TR_OK);
	/* 最后释放会同时销毁 message 和 storage；返回后只可使用外部记录。 */
	STATUS(tr_message_release(&owner->message), TR_OK);
	CHECK(current_bytes(&budget) == 0);
	CHECK(atomic_load(&record.calls) == 1);
}

#define THREADS 8U
struct thread_arg {
	struct tr_message message;
	pthread_barrier_t *barrier;
};

static void barrier_wait(pthread_barrier_t *barrier)
{
	int ret = pthread_barrier_wait(barrier);

	CHECK(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
}

static void *clone_worker(void *arg)
{
	struct thread_arg *worker = arg;
	unsigned i;

	barrier_wait(worker->barrier);
	for (i = 0; i < 2000; ++i) {
		struct tr_message copy = {0};

		STATUS(tr_message_clone(&worker->message, &copy), TR_OK);
		STATUS(tr_message_release(&copy), TR_OK);
	}
	STATUS(tr_message_release(&worker->message), TR_OK);
	return NULL;
}

static void test_concurrent_held_references(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage storage = {0};
	struct tr_message message = {0};
	struct release_record record = {0};
	struct thread_arg args[THREADS] = {0};
	pthread_t threads[THREADS];
	pthread_barrier_t barrier;
	uint8_t data[8] = {0};
	unsigned i;

	tr_memory_budget_init(&budget, 8);
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_OK);
	STATUS(tr_message_storage_publish(&storage, data, 8, note_release,
	                                  &record, &message),
	       TR_OK);
	CHECK(pthread_barrier_init(&barrier, NULL, THREADS + 1U) == 0);
	for (i = 0; i < THREADS; ++i) {
		args[i].barrier = &barrier;
		STATUS(tr_message_clone(&message, &args[i].message), TR_OK);
		CHECK(pthread_create(&threads[i], NULL, clone_worker,
		                     &args[i]) == 0);
	}
	STATUS(tr_message_release(&message), TR_OK);
	CHECK(current_bytes(&budget) == 8);
	barrier_wait(&barrier);
	for (i = 0; i < THREADS; ++i)
		CHECK(pthread_join(threads[i], NULL) == 0);
	CHECK(pthread_barrier_destroy(&barrier) == 0);
	CHECK(atomic_load(&record.calls) == 1);
	CHECK(current_bytes(&budget) == 0);
}

struct release_gate {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int entered;
	int proceed;
	struct tr_message message;
};

static void gated_release(void *arg)
{
	struct release_gate *gate = arg;

	CHECK(pthread_mutex_lock(&gate->lock) == 0);
	gate->entered = 1;
	CHECK(pthread_cond_broadcast(&gate->cond) == 0);
	while (!gate->proceed)
		CHECK(pthread_cond_wait(&gate->cond, &gate->lock) == 0);
	CHECK(pthread_mutex_unlock(&gate->lock) == 0);
}

static void *release_worker(void *arg)
{
	struct release_gate *gate = arg;

	STATUS(tr_message_release(&gate->message), TR_OK);
	return NULL;
}

static void test_credit_after_callback(void)
{
	struct tr_memory_budget budget;
	struct tr_message_storage storage = {0}, next = {0};
	struct release_gate gate = {0};
	pthread_t thread;
	uint8_t data[8] = {0};

	tr_memory_budget_init(&budget, 8);
	CHECK(pthread_mutex_init(&gate.lock, NULL) == 0);
	CHECK(pthread_cond_init(&gate.cond, NULL) == 0);
	STATUS(tr_message_storage_reserve(&storage, &budget, 8), TR_OK);
	STATUS(tr_message_storage_publish(&storage, data, 8, gated_release,
	                                  &gate, &gate.message),
	       TR_OK);
	CHECK(pthread_create(&thread, NULL, release_worker, &gate) == 0);
	CHECK(pthread_mutex_lock(&gate.lock) == 0);
	while (!gate.entered)
		CHECK(pthread_cond_wait(&gate.cond, &gate.lock) == 0);
	CHECK(current_bytes(&budget) == 8);
	STATUS(tr_message_storage_reserve(&next, &budget, 8), TR_AGAIN);
	gate.proceed = 1;
	CHECK(pthread_cond_broadcast(&gate.cond) == 0);
	CHECK(pthread_mutex_unlock(&gate.lock) == 0);
	CHECK(pthread_join(thread, NULL) == 0);
	STATUS(tr_message_storage_reserve(&next, &budget, 8), TR_OK);
	STATUS(tr_message_storage_cancel(&next), TR_OK);
	CHECK(current_bytes(&budget) == 0);
	CHECK(pthread_cond_destroy(&gate.cond) == 0);
	CHECK(pthread_mutex_destroy(&gate.lock) == 0);
}

static void test_null_arguments(void)
{
	struct tr_message message = {0};
	struct tr_message_storage storage = {0};
	struct tr_message_limits limits = {0, 0};
	struct tr_memory_budget budget;
	struct tr_message_view view;

	tr_memory_budget_init(&budget, 1);
	STATUS(tr_message_storage_reserve(NULL, &budget, 1), TR_ERR_INVALID);
	STATUS(tr_message_storage_reserve(&storage, NULL, 1), TR_ERR_INVALID);
	STATUS(tr_message_storage_cancel(NULL), TR_ERR_INVALID);
	STATUS(tr_message_storage_publish(NULL, NULL, 0, note_release, NULL,
	                                  &message),
	       TR_ERR_INVALID);
	STATUS(tr_message_view(NULL, &view), TR_ERR_INVALID);
	STATUS(tr_message_view(&message, NULL), TR_ERR_INVALID);
	STATUS(tr_message_clone(NULL, &message), TR_ERR_INVALID);
	STATUS(tr_message_clone(&message, NULL), TR_ERR_INVALID);
	STATUS(tr_message_slice(&message, 0, 0, &message), TR_ERR_INVALID);
	STATUS(tr_message_none_move(&message, NULL, &message), TR_ERR_INVALID);
	STATUS(tr_message_none_move(NULL, &limits, &message), TR_ERR_INVALID);
	STATUS(tr_message_release(NULL), TR_ERR_INVALID);
}

int main(void)
{
	test_reservation();
	test_bounded_builder_publishes_exact_message();
	test_bounded_builder_cancel_and_budget_failure();
	test_bounded_builder_empty_message();
	test_publish_rollback();
	test_none_and_aliases();
	test_empty_and_ranges();
	test_embedded_descriptor_free();
	test_concurrent_held_references();
	test_credit_after_callback();
	test_null_arguments();
	puts("message_storage_none: ok");
	return 0;
}
