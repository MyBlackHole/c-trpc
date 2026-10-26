#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "../include/tr/status.h"
#include "../src/execution/async_operation_internal.h"
#include "../src/memory_budget.h"

struct test_payload {
	int value;
};

struct cancel_race_context {
	struct tr_async_operation_pool *pool;
	struct tr_async_operation operation;
	pthread_barrier_t *barrier;
	int status;
	int notify;
};

struct publish_race_context {
	struct tr_async_operation_pool *pool;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref completion_ref;
	pthread_barrier_t *barrier;
	int status;
	int ready;
};

struct owner_handoff_context {
	struct tr_async_operation_pool *pool;
	int unproven_handoff_status;
	int handoff_status;
	int stats_status;
};

static unsigned int test_payload_destroyed;

static void destroy_test_payload(void *payload)
{
	free(payload);
	++test_payload_destroyed;
}

static struct test_payload *new_test_payload(int value)
{
	struct test_payload *payload = malloc(sizeof(*payload));
	assert(payload != NULL);
	payload->value = value;
	return payload;
}

static void wait_barrier(pthread_barrier_t *barrier)
{
	int ret = pthread_barrier_wait(barrier);
	assert(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
}

static void *cancel_race_thread(void *arg)
{
	struct cancel_race_context *context = arg;
	wait_barrier(context->barrier);
	context->status = tr_async_operation_request_cancel(
	        context->pool, context->operation, TR_ERR_CANCELLED,
	        &context->notify);
	return NULL;
}

static void *publish_race_thread(void *arg)
{
	struct publish_race_context *context = arg;
	wait_barrier(context->barrier);
	context->status = tr_async_operation_publish(
	        context->pool, &context->producer_ref,
	        (struct tr_async_operation_completion){.status = TR_OK},
	        &context->completion_ref, &context->ready);
	return NULL;
}

static void *owner_handoff_thread(void *arg)
{
	struct owner_handoff_context *context = arg;
	struct tr_async_operation_stats stats;
	context->unproven_handoff_status =
	        tr_async_operation_pool_handoff_owner(context->pool, 0);
	context->handoff_status =
	        tr_async_operation_pool_handoff_owner(context->pool, 1);
	context->stats_status =
	        tr_async_operation_get_stats(context->pool, &stats);
	return NULL;
}

static void test_capacity_reject_and_reuse(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {
	        .kind = 1U,
	        .domain_generation = 10U,
	        .object_generation = 20U,
	        .deadline_ns = 30U,
	};
	struct tr_async_operation first = {0};
	struct tr_async_operation second = {91U, 92U, 93U};
	struct tr_async_operation unchanged_operation;
	struct tr_async_operation_ref first_ref = {0};
	struct tr_async_operation_ref second_ref = {81U, 82U, 83U, 84U};
	struct tr_async_operation_ref unchanged_ref;
	struct tr_async_operation_stats stats;
	struct tr_memory_budget_stats budget_stats;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	tr_memory_budget_get_stats(&budget, &budget_stats);
	assert(budget_stats.current_bytes != 0U);
	assert(tr_async_operation_reserve(pool, &spec, &first, &first_ref) ==
	       TR_OK);
	assert(first.instance != 0U && first.generation == 1U &&
	       first.slot == 0U);
	assert(first_ref.instance == first.instance &&
	       first_ref.generation == first.generation &&
	       first_ref.token != 0U && first_ref.slot == first.slot);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.capacity == 1U && stats.reserved == 1U &&
	       stats.accepted == 0U && stats.published == 0U &&
	       stats.consumed == 0U && stats.reserve_full == 0U &&
	       stats.accepting != 0);

	unchanged_ref = second_ref;
	assert(tr_async_operation_reserve(pool, &spec, &second, &second_ref) ==
	       TR_AGAIN);
	assert(second.instance == 91U && second.generation == 92U &&
	       second.slot == 93U);
	assert(second_ref.instance == unchanged_ref.instance &&
	       second_ref.generation == unchanged_ref.generation &&
	       second_ref.token == unchanged_ref.token &&
	       second_ref.slot == unchanged_ref.slot);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.reserve_full == 1U);
	assert(tr_async_operation_reject(pool, first, &first_ref) == TR_OK);
	assert(first_ref.instance == 0U && first_ref.generation == 0U &&
	       first_ref.token == 0U && first_ref.slot == 0U);

	assert(tr_async_operation_reserve(pool, &spec, &second, &second_ref) ==
	       TR_OK);
	assert(second.instance == first.instance && second.slot == first.slot &&
	       second.generation == first.generation + 1U);
	assert(tr_async_operation_reject(pool, first, &second_ref) ==
	       TR_ERR_STALE);
	assert(second_ref.instance == second.instance &&
	       second_ref.generation == second.generation &&
	       second_ref.token != 0U && second_ref.slot == second.slot);
	assert(tr_async_operation_reject(pool, second, &second_ref) == TR_OK);

	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.accepting == 0 && stats.reserved == 0U);
	unchanged_operation = second;
	unchanged_ref = second_ref;
	assert(tr_async_operation_reserve(pool, &spec, &second, &second_ref) ==
	       TR_ERR_CLOSED);
	assert(second.instance == unchanged_operation.instance &&
	       second.generation == unchanged_operation.generation &&
	       second.slot == unchanged_operation.slot);
	assert(second_ref.instance == unchanged_ref.instance &&
	       second_ref.generation == unchanged_ref.generation &&
	       second_ref.token == unchanged_ref.token &&
	       second_ref.slot == unchanged_ref.slot);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
	assert(pool == NULL);
	tr_memory_budget_get_stats(&budget, &budget_stats);
	assert(budget_stats.current_bytes == 0U);
}

static void test_accept_publish_consume_retire(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 7U};
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref stale_ref;
	struct tr_async_operation_ref completion_ref = {51U, 52U, 53U, 54U};
	struct tr_async_operation_ref unchanged_completion_ref;
	struct tr_async_operation_spec received_spec = {0};
	struct tr_async_operation_completion completion = {0};
	struct tr_async_operation_completion received = {0};
	struct test_payload *payload = new_test_payload(42);
	int ready = -1;
	int cancelled = -1;
	int cancel_reason = 99;

	test_payload_destroyed = 0U;
	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_OK);
	unchanged_completion_ref = completion_ref;
	assert(tr_async_operation_accept(pool, operation, &completion_ref,
	                                 &ready) == TR_OK);
	assert(ready == 0 &&
	       completion_ref.instance == unchanged_completion_ref.instance &&
	       completion_ref.generation ==
	               unchanged_completion_ref.generation &&
	       completion_ref.token == unchanged_completion_ref.token &&
	       completion_ref.slot == unchanged_completion_ref.slot);
	stale_ref = producer_ref;
	assert(tr_async_operation_publish(
	               pool, &producer_ref,
	               (struct tr_async_operation_completion){
	                       .status = TR_OK,
	                       .value = UINT64_C(0xfeed1234),
	                       .payload = payload,
	                       .destroy = destroy_test_payload,
	               },
	               &completion_ref, &ready) == TR_OK);
	assert(ready == 1 && producer_ref.token == 0U &&
	       completion_ref.token != 0U);
	received_spec.kind = 99U;
	assert(tr_async_operation_consume(pool, &stale_ref, &received_spec,
	                                  &received, &cancelled,
	                                  &cancel_reason) == TR_ERR_STALE);
	assert(received_spec.kind == 99U && received.payload == NULL &&
	       cancelled == -1 && cancel_reason == 99);
	assert(tr_async_operation_consume(pool, &completion_ref, &received_spec,
	                                  &received, &cancelled,
	                                  &cancel_reason) == TR_OK);
	assert(received_spec.kind == spec.kind &&
	       received_spec.domain_generation == spec.domain_generation &&
	       received_spec.object_generation == spec.object_generation &&
	       received_spec.deadline_ns == spec.deadline_ns);
	assert(received.status == TR_OK &&
	       received.value == UINT64_C(0xfeed1234) &&
	       received.payload == payload &&
	       ((struct test_payload *)received.payload)->value == 42);
	assert(cancelled == 0 && cancel_reason == TR_OK);
	assert(test_payload_destroyed == 0U);
	assert(tr_async_operation_consume(pool, &completion_ref, &received_spec,
	                                  &completion, &ready,
	                                  &cancel_reason) == TR_ERR_STATE);
	assert(completion.payload == NULL);
	assert(tr_async_operation_retire(pool, &completion_ref) == TR_OK);
	assert(completion_ref.token == 0U);
	assert(test_payload_destroyed == 0U);
	received.destroy(received.payload);
	assert(test_payload_destroyed == 1U);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_publish_before_accept_is_retained(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 8U};
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref completion_ref = {0};
	struct tr_async_operation_spec received_spec = {0};
	struct tr_async_operation_completion received = {0};
	struct test_payload *payload = new_test_payload(81);
	int ready = -1;
	int cancelled = -1;
	int cancel_reason = 99;

	test_payload_destroyed = 0U;
	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_OK);
	assert(tr_async_operation_publish(
	               pool, &producer_ref,
	               (struct tr_async_operation_completion){
	                       .status = TR_OK,
	                       .payload = payload,
	                       .destroy = destroy_test_payload,
	               },
	               &completion_ref, &ready) == TR_OK);
	assert(ready == 0 && producer_ref.token == 0U &&
	       completion_ref.token == 0U);
	assert(tr_async_operation_reject(pool, operation, &producer_ref) ==
	       TR_ERR_STATE);
	assert(producer_ref.token == 0U);
	assert(tr_async_operation_accept(pool, operation, &completion_ref,
	                                 &ready) == TR_OK);
	assert(ready == 1 && completion_ref.token != 0U);
	assert(tr_async_operation_consume(pool, &completion_ref, &received_spec,
	                                  &received, &cancelled,
	                                  &cancel_reason) == TR_OK);
	assert(received_spec.kind == spec.kind && received.status == TR_OK &&
	       received.payload == payload &&
	       ((struct test_payload *)received.payload)->value == 81);
	assert(tr_async_operation_retire(pool, &completion_ref) == TR_OK);
	received.destroy(received.payload);
	assert(test_payload_destroyed == 1U);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_cancel_and_publish_race(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 9U};
	unsigned int iteration;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	for (iteration = 0U; iteration < 100U; ++iteration) {
		pthread_barrier_t barrier;
		pthread_t cancel_thread;
		pthread_t publish_thread;
		struct tr_async_operation operation;
		struct tr_async_operation_ref producer_ref;
		struct tr_async_operation_ref completion_ref = {0};
		struct tr_async_operation_spec received_spec = {0};
		struct tr_async_operation_completion received = {0};
		struct cancel_race_context cancel_context = {0};
		struct publish_race_context publish_context = {0};
		int ready = -1;
		int cancelled = -1;
		int cancel_reason = 123;

		assert(tr_async_operation_reserve(pool, &spec, &operation,
		                                  &producer_ref) == TR_OK);
		assert(tr_async_operation_accept(pool, operation,
		                                 &completion_ref,
		                                 &ready) == TR_OK);
		assert(ready == 0);
		assert(pthread_barrier_init(&barrier, NULL, 2U) == 0);
		cancel_context.pool = pool;
		cancel_context.operation = operation;
		cancel_context.barrier = &barrier;
		cancel_context.notify = -1;
		publish_context.pool = pool;
		publish_context.producer_ref = producer_ref;
		producer_ref = (struct tr_async_operation_ref){0};
		publish_context.barrier = &barrier;
		publish_context.ready = -1;
		assert(pthread_create(&cancel_thread, NULL, cancel_race_thread,
		                      &cancel_context) == 0);
		assert(pthread_create(&publish_thread, NULL,
		                      publish_race_thread,
		                      &publish_context) == 0);
		assert(pthread_join(cancel_thread, NULL) == 0);
		assert(pthread_join(publish_thread, NULL) == 0);
		assert(pthread_barrier_destroy(&barrier) == 0);
		assert(publish_context.status == TR_OK &&
		       publish_context.ready == 1 &&
		       publish_context.producer_ref.token == 0U);
		assert(cancel_context.status == TR_OK ||
		       cancel_context.status == TR_ERR_STATE);
		if (cancel_context.status == TR_OK)
			assert(cancel_context.notify == 1);
		else
			assert(cancel_context.notify == -1);
		completion_ref = publish_context.completion_ref;
		assert(tr_async_operation_consume(
		               pool, &completion_ref, &received_spec, &received,
		               &cancelled, &cancel_reason) == TR_OK);
		assert(received_spec.kind == spec.kind &&
		       received.status == TR_OK && received.payload == NULL);
		if (cancel_context.status == TR_OK) {
			assert(cancelled == 1 &&
			       cancel_reason == TR_ERR_CANCELLED);
		} else {
			assert(cancelled == 0 && cancel_reason == TR_OK);
		}
		assert(tr_async_operation_retire(pool, &completion_ref) ==
		       TR_OK);
	}
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_stopped_reap_and_generation_exhaustion(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 10U};
	struct tr_async_operation operation = {91U, 92U, 93U};
	struct tr_async_operation_ref producer_ref = {81U, 82U, 83U, 84U};
	struct tr_async_operation_ref completion_ref = {0};
	struct test_payload *payload;
	struct tr_async_operation_stats stats;
	int ready = -1;
	uint32_t reaped = 99U;

	test_payload_destroyed = 0U;
	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	payload = new_test_payload(101);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_OK);
	assert(tr_async_operation_publish(
	               pool, &producer_ref,
	               (struct tr_async_operation_completion){
	                       .status = TR_OK,
	                       .payload = payload,
	                       .destroy = destroy_test_payload,
	               },
	               &completion_ref, &ready) == TR_OK);
	assert(ready == 0 && producer_ref.token == 0U &&
	       completion_ref.token == 0U);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_AGAIN);
	assert(tr_async_operation_reap_stopped(pool, 0, &reaped) ==
	       TR_ERR_STATE);
	assert(reaped == 99U && test_payload_destroyed == 0U);
	assert(tr_async_operation_reap_stopped(pool, 1, &reaped) == TR_OK);
	assert(reaped == 1U && test_payload_destroyed == 1U);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.reserved == 0U && stats.accepted == 0U &&
	       stats.published == 0U && stats.consumed == 0U);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);

	pool = NULL;
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	assert(tr_async_operation_test_set_generation(pool, 0U, UINT64_MAX) ==
	       TR_OK);
	operation = (struct tr_async_operation){91U, 92U, 93U};
	producer_ref = (struct tr_async_operation_ref){81U, 82U, 83U, 84U};
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_ERR_STATE);
	assert(operation.instance == 91U && operation.generation == 92U &&
	       operation.slot == 93U);
	assert(producer_ref.instance == 81U && producer_ref.generation == 82U &&
	       producer_ref.token == 83U && producer_ref.slot == 84U);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.generation_exhausted == 1U);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_owner_must_bind_before_pool_use(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 11U};
	struct tr_async_operation operation = {91U, 92U, 93U};
	struct tr_async_operation_ref producer_ref = {81U, 82U, 83U, 84U};

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_ERR_STATE);
	assert(operation.instance == 91U && operation.generation == 92U &&
	       operation.slot == 93U);
	assert(producer_ref.instance == 81U && producer_ref.generation == 82U &&
	       producer_ref.token == 83U && producer_ref.slot == 84U);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_OK);
	assert(tr_async_operation_reject(pool, operation, &producer_ref) ==
	       TR_OK);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_stopped_reap_closes_admission(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct tr_async_operation_spec spec = {.kind = 12U};
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	struct tr_async_operation_ref completion_ref = {0};
	struct test_payload *payload = new_test_payload(202);
	struct tr_async_operation_stats stats;
	uint32_t reaped = 99U;
	int ready = -1;

	test_payload_destroyed = 0U;
	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	assert(tr_async_operation_reserve(pool, &spec, &operation,
	                                  &producer_ref) == TR_OK);
	assert(tr_async_operation_publish(
	               pool, &producer_ref,
	               (struct tr_async_operation_completion){
	                       .status = TR_OK,
	                       .payload = payload,
	                       .destroy = destroy_test_payload,
	               },
	               &completion_ref, &ready) == TR_OK);
	assert(ready == 0);
	assert(tr_async_operation_reap_stopped(pool, 1, &reaped) == TR_OK);
	assert(reaped == 1U && test_payload_destroyed == 1U);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(stats.accepting == 0 && stats.reserved == 0U);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

static void test_serialized_owner_handoff(void)
{
	struct tr_async_operation_pool *pool = NULL;
	struct tr_memory_budget budget;
	struct owner_handoff_context context = {0};
	struct tr_async_operation_stats stats;
	pthread_t thread;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_async_operation_pool_create(1U, &budget, &pool) == TR_OK);
	assert(tr_async_operation_pool_bind_owner(pool) == TR_OK);
	context.pool = pool;
	assert(pthread_create(&thread, NULL, owner_handoff_thread, &context) ==
	       0);
	assert(pthread_join(thread, NULL) == 0);
	assert(context.unproven_handoff_status == TR_ERR_STATE);
	assert(context.handoff_status == TR_OK &&
	       context.stats_status == TR_OK);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_ERR_STATE);
	assert(tr_async_operation_pool_handoff_owner(pool, 1) == TR_OK);
	assert(tr_async_operation_get_stats(pool, &stats) == TR_OK);
	assert(tr_async_operation_pool_close(pool) == TR_OK);
	assert(tr_async_operation_pool_destroy(&pool) == TR_OK);
}

int main(void)
{
	test_capacity_reject_and_reuse();
	test_accept_publish_consume_retire();
	test_publish_before_accept_is_retained();
	test_cancel_and_publish_race();
	test_stopped_reap_and_generation_exhaustion();
	test_owner_must_bind_before_pool_use();
	test_stopped_reap_closes_admission();
	test_serialized_owner_handoff();
	return 0;
}
