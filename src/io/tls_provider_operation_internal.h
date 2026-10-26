#ifndef TR_TLS_PROVIDER_OPERATION_INTERNAL_H
#define TR_TLS_PROVIDER_OPERATION_INTERNAL_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "tr/tls.h"

struct tr_memory_budget;
struct tr_async_operation_pool;
struct tr_tls_provider_operation_slot;

struct tr_tls_provider_operation_table {
	pthread_mutex_t lock;
	struct tr_tls_provider_operation_slot *slots;
	struct tr_async_operation_pool *operations;
	struct tr_memory_budget *budget;
	uint64_t instance;
	uint64_t slot_storage_bytes;
	uint32_t capacity;
	int lock_ready;
};

/* table 在 init 前必须清零；其寿命与 budget 覆盖所有已准入操作。 */
int tr_tls_provider_operation_table_init(
        struct tr_tls_provider_operation_table *table, uint32_t capacity,
        struct tr_memory_budget *budget);
int tr_tls_provider_operation_table_destroy(
        struct tr_tls_provider_operation_table *table);

/* reserve 预留结果缓冲和预算；只有 TR_OK 才写入 out_operation。 */
int tr_tls_provider_operation_reserve(
        struct tr_tls_provider_operation_table *table,
        enum tr_tls_provider_operation_kind kind, uint32_t result_capacity,
        struct tr_tls_provider_operation *out_operation);
int tr_tls_provider_operation_accept(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation);
int tr_tls_provider_operation_complete(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int status,
        const void *result, size_t result_len);
int tr_tls_provider_operation_complete_credentials(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int status,
        const struct tr_tls_credential_bundle *bundle);
int tr_tls_provider_operation_request_cancel(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int reason,
        int *out_notify_provider);
int tr_tls_provider_operation_result(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int *out_status,
        void *out_result, size_t result_capacity, size_t *out_result_len);
int tr_tls_provider_operation_credentials_result(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation, int *out_status,
        struct tr_tls_credential_bundle *out_bundle);
int tr_tls_provider_operation_retire(
        struct tr_tls_provider_operation_table *table,
        struct tr_tls_provider_operation operation);

/* 仅供内部单元测试模拟代次耗尽；目标槽必须空闲。 */
int tr_tls_provider_operation_test_set_generation(
        struct tr_tls_provider_operation_table *table, uint32_t slot,
        uint64_t generation);

#endif
