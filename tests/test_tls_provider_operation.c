#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

#include "../include/tr/status.h"
#include "../include/tr/tls.h"
#include "../src/io/tls_provider_operation_internal.h"
#include "../src/memory_budget.h"

struct race_context {
	struct tr_tls_provider_operation_table *table;
	struct tr_tls_provider_operation operation;
	pthread_barrier_t *barrier;
	int status;
	int notify;
};

static void *complete_racer(void *arg)
{
	struct race_context *context = arg;
	const uint8_t payload[] = {0x5aU};
	int barrier_status = pthread_barrier_wait(context->barrier);
	assert(barrier_status == 0 ||
	       barrier_status == PTHREAD_BARRIER_SERIAL_THREAD);
	context->status = tr_tls_provider_operation_complete(
	        context->table, context->operation, TR_OK, payload,
	        sizeof(payload));
	return NULL;
}

static void *cancel_racer(void *arg)
{
	struct race_context *context = arg;
	int barrier_status = pthread_barrier_wait(context->barrier);
	assert(barrier_status == 0 ||
	       barrier_status == PTHREAD_BARRIER_SERIAL_THREAD);
	context->status = tr_tls_provider_operation_request_cancel(
	        context->table, context->operation, TR_ERR_CANCELLED,
	        &context->notify);
	return NULL;
}

static void test_capacity_and_accounting(void)
{
	struct tr_tls_provider_operation_table table = {0};
	struct tr_memory_budget budget;
	struct tr_memory_budget_stats stats;
	struct tr_tls_provider_operation operation = {
	        .instance = 99U,
	        .generation = 88U,
	        .slot = 77U,
	        .kind = TR_TLS_PROVIDER_OPERATION_AUTHORIZATION,
	};
	struct tr_tls_provider_operation stale;
	const uint8_t result[] = {1U, 2U, 3U, 4U};
	uint8_t output[sizeof(result)] = {0};
	size_t output_len = 99U;
	int result_status = 99;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_tls_provider_operation_table_init(&table, 1U, &budget) ==
	       TR_OK);
	assert(tr_tls_provider_operation_reserve(
	               &table, TR_TLS_PROVIDER_OPERATION_CREDENTIALS, 64U,
	               &operation) == TR_OK);
	stale = operation;
	assert(operation.instance != 0U);
	assert(operation.generation == 1U);
	assert(operation.slot == 0U);
	assert(tr_tls_provider_operation_reserve(
	               &table, TR_TLS_PROVIDER_OPERATION_AUTHORIZATION, 0U,
	               &operation) == TR_AGAIN);
	assert(operation.instance == stale.instance);
	assert(operation.generation == stale.generation);
	assert(operation.slot == stale.slot);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == table.slot_storage_bytes + 64U);
	assert(tr_tls_provider_operation_accept(&table, stale) == TR_OK);
	assert(tr_tls_provider_operation_result(&table, stale, &result_status,
	                                        output, sizeof(output),
	                                        &output_len) == TR_AGAIN);
	assert(result_status == 99);
	assert(output_len == 99U);
	assert(tr_tls_provider_operation_complete(&table, stale, TR_OK, result,
	                                          sizeof(result)) == TR_OK);
	result_status = 98;
	output_len = 99U;
	assert(tr_tls_provider_operation_result(&table, stale, &result_status,
	                                        output, 1U, &output_len) ==
	       TR_ERR_BAD_LENGTH);
	assert(result_status == 98);
	assert(output_len == 99U);
	assert(tr_tls_provider_operation_result(&table, stale, &result_status,
	                                        output, sizeof(output),
	                                        &output_len) == TR_OK);
	assert(result_status == TR_OK);
	assert(output_len == sizeof(result));
	assert(memcmp(output, result, sizeof(result)) == 0);
	assert(tr_tls_provider_operation_retire(&table, stale) == TR_OK);
	assert(tr_tls_provider_operation_result(&table, stale, &result_status,
	                                        output, sizeof(output),
	                                        &output_len) == TR_ERR_STALE);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == table.slot_storage_bytes);
	assert(tr_tls_provider_operation_table_destroy(&table) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
}

static void test_generation_exhaustion_and_foreign_handles(void)
{
	struct tr_tls_provider_operation_table first = {0};
	struct tr_tls_provider_operation_table second = {0};
	struct tr_memory_budget first_budget;
	struct tr_memory_budget second_budget;
	struct tr_tls_provider_operation operation;
	struct tr_tls_provider_operation unchanged = {
	        .instance = 123U,
	        .generation = 456U,
	        .slot = 7U,
	        .kind = TR_TLS_PROVIDER_OPERATION_CREDENTIALS,
	};
	uint64_t generation = UINT64_MAX;
	int result_status = 0;
	size_t result_len = 0U;

	tr_memory_budget_init(&first_budget, 1024U * 1024U);
	tr_memory_budget_init(&second_budget, 1024U * 1024U);
	assert(tr_tls_provider_operation_table_init(&first, 1U,
	                                            &first_budget) == TR_OK);
	assert(tr_tls_provider_operation_table_init(&second, 1U,
	                                            &second_budget) == TR_OK);
	assert(tr_tls_provider_operation_reserve(
	               &first, TR_TLS_PROVIDER_OPERATION_AUTHORIZATION, 0U,
	               &operation) == TR_OK);
	assert(tr_tls_provider_operation_accept(&first, operation) == TR_OK);
	assert(tr_tls_provider_operation_complete(&first, operation, TR_OK,
	                                          NULL, 0U) == TR_OK);
	assert(tr_tls_provider_operation_retire(&first, operation) == TR_OK);
	assert(tr_tls_provider_operation_result(&second, operation,
	                                        &result_status, NULL, 0U,
	                                        &result_len) == TR_ERR_STALE);
	assert(tr_tls_provider_operation_test_set_generation(
	               &first, operation.slot, generation) == TR_OK);
	assert(tr_tls_provider_operation_reserve(
	               &first, TR_TLS_PROVIDER_OPERATION_AUTHORIZATION, 0U,
	               &unchanged) == TR_ERR_STATE);
	assert(unchanged.instance == 123U);
	assert(unchanged.generation == 456U);
	assert(tr_tls_provider_operation_table_destroy(&first) == TR_OK);
	assert(tr_tls_provider_operation_table_destroy(&second) == TR_OK);
}

static void test_credentials_copy_and_retire_accounting(void)
{
	struct tr_tls_provider_operation_table table = {0};
	struct tr_memory_budget budget;
	struct tr_memory_budget_stats stats;
	struct tr_tls_provider_operation operation;
	struct tr_tls_credential_bundle input = {0};
	struct tr_tls_credential_bundle output = {0};
	uint8_t certificate[] = {'c', 'e', 'r', 't'};
	uint8_t private_key[] = {'k', 'e', 'y'};
	uint8_t password[] = {'p', 'w'};
	int result_status = 99;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_tls_provider_operation_table_init(&table, 1U, &budget) ==
	       TR_OK);
	assert(tr_tls_provider_operation_reserve(
	               &table, TR_TLS_PROVIDER_OPERATION_CREDENTIALS, 9U,
	               &operation) == TR_OK);
	assert(tr_tls_provider_operation_accept(&table, operation) == TR_OK);
	output.certificate_chain_pem.data = (const uint8_t *)"sentinel";
	output.certificate_chain_pem.len = 8U;
	assert(tr_tls_provider_operation_credentials_result(
	               &table, operation, &result_status, &output) == TR_AGAIN);
	assert(result_status == 99);
	assert(output.certificate_chain_pem.len == 8U &&
	       memcmp(output.certificate_chain_pem.data, "sentinel", 8U) == 0);
	input.certificate_chain_pem.data = certificate;
	input.certificate_chain_pem.len = sizeof(certificate);
	input.private_key_pem.data = private_key;
	input.private_key_pem.len = sizeof(private_key);
	input.private_key_password.data = password;
	input.private_key_password.len = sizeof(password);
	assert(tr_tls_provider_operation_complete_credentials(
	               &table, operation, TR_OK, &input) == TR_OK);
	memset(certificate, 'x', sizeof(certificate));
	memset(private_key, 'x', sizeof(private_key));
	memset(password, 'x', sizeof(password));
	assert(tr_tls_provider_operation_credentials_result(
	               &table, operation, &result_status, &output) == TR_OK);
	assert(result_status == TR_OK);
	assert(output.certificate_chain_pem.len == 4U);
	assert(memcmp(output.certificate_chain_pem.data, "cert", 4U) == 0);
	assert(output.private_key_pem.len == 3U);
	assert(memcmp(output.private_key_pem.data, "key", 3U) == 0);
	assert(output.private_key_password.len == 2U);
	assert(memcmp(output.private_key_password.data, "pw", 2U) == 0);
	assert(tr_tls_provider_operation_retire(&table, operation) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == table.slot_storage_bytes);
	assert(tr_tls_provider_operation_table_destroy(&table) == TR_OK);
	tr_memory_budget_get_stats(&budget, &stats);
	assert(stats.current_bytes == 0U);
}

static void test_cancel_and_complete_race(void)
{
	struct tr_tls_provider_operation_table table = {0};
	struct tr_memory_budget budget;
	unsigned int iteration;

	tr_memory_budget_init(&budget, 1024U * 1024U);
	assert(tr_tls_provider_operation_table_init(&table, 1U, &budget) ==
	       TR_OK);
	for (iteration = 0U; iteration < 100U; ++iteration) {
		pthread_barrier_t barrier;
		pthread_t complete_thread;
		pthread_t cancel_thread;
		struct race_context complete_context = {0};
		struct race_context cancel_context = {0};
		struct tr_tls_provider_operation operation;
		uint8_t result = 0U;
		size_t result_len = 99U;
		int result_status = 99;

		assert(tr_tls_provider_operation_reserve(
		               &table, TR_TLS_PROVIDER_OPERATION_CREDENTIALS,
		               8U, &operation) == TR_OK);
		assert(tr_tls_provider_operation_accept(&table, operation) ==
		       TR_OK);
		assert(pthread_barrier_init(&barrier, NULL, 2U) == 0);
		complete_context.table = &table;
		complete_context.operation = operation;
		complete_context.barrier = &barrier;
		cancel_context.table = &table;
		cancel_context.operation = operation;
		cancel_context.barrier = &barrier;
		assert(pthread_create(&complete_thread, NULL, complete_racer,
		                      &complete_context) == 0);
		assert(pthread_create(&cancel_thread, NULL, cancel_racer,
		                      &cancel_context) == 0);
		assert(pthread_join(complete_thread, NULL) == 0);
		assert(pthread_join(cancel_thread, NULL) == 0);
		assert(pthread_barrier_destroy(&barrier) == 0);
		assert(complete_context.status == TR_OK);
		assert(cancel_context.status == TR_OK);
		assert(tr_tls_provider_operation_result(
		               &table, operation, &result_status, &result,
		               sizeof(result), &result_len) == TR_OK);
		if (cancel_context.notify) {
			assert(result_status == TR_ERR_CANCELLED);
			assert(result_len == 0U);
		} else {
			assert(result_status == TR_OK);
			assert(result_len == 1U);
			assert(result == 0x5aU);
		}
		assert(tr_tls_provider_operation_retire(&table, operation) ==
		       TR_OK);
	}
	assert(tr_tls_provider_operation_table_destroy(&table) == TR_OK);
}

int main(void)
{
	test_capacity_and_accounting();
	test_generation_exhaustion_and_foreign_handles();
	test_credentials_copy_and_retire_accounting();
	test_cancel_and_complete_race();
	return 0;
}
