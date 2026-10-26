#include "async_operation_internal.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "../memory_budget.h"

enum tr_async_operation_state {
	TR_ASYNC_OPERATION_FREE = 0,
	TR_ASYNC_OPERATION_RESERVED,
	TR_ASYNC_OPERATION_RESERVED_READY,
	TR_ASYNC_OPERATION_ACCEPTED,
	TR_ASYNC_OPERATION_PUBLISHED,
	TR_ASYNC_OPERATION_CONSUMED
};

struct tr_async_operation_slot {
	struct tr_async_operation_spec spec;
	struct tr_async_operation_completion completion;
	uint64_t generation;
	uint64_t token;
	int cancel_reason;
	enum tr_async_operation_state state;
	int cancel_requested;
};

struct tr_async_operation_pool {
	pthread_mutex_t lock;
	pthread_t owner;
	struct tr_async_operation_slot *slots;
	struct tr_memory_budget *budget;
	uint64_t instance;
	uint64_t storage_bytes;
	uint64_t reserve_full;
	uint32_t capacity;
	int accepting;
	int owner_bound;
	int lock_ready;
};

static _Atomic uint64_t tr_async_operation_next_instance = 1U;

static int tr_async_operation_size_multiply(size_t left, size_t right,
                                            size_t *out)
{
	if (left != 0U && right > SIZE_MAX / left)
		return TR_ERR_STATE;
	*out = left * right;
	return TR_OK;
}

static int tr_async_operation_allocate_instance(uint64_t *out_instance)
{
	uint64_t current = atomic_load_explicit(
	        &tr_async_operation_next_instance, memory_order_relaxed);

	for (;;) {
		if (current == 0U || current == UINT64_MAX)
			return TR_ERR_STATE;
		if (atomic_compare_exchange_weak_explicit(
		            &tr_async_operation_next_instance, &current,
		            current + 1U, memory_order_relaxed,
		            memory_order_relaxed)) {
			*out_instance = current;
			return TR_OK;
		}
	}
}

static int tr_async_operation_lock(struct tr_async_operation_pool *pool)
{
	if (!pool || !pool->lock_ready)
		return TR_ERR_INVALID;
	return pthread_mutex_lock(&pool->lock) == 0 ? TR_OK : TR_ERR_SYS;
}

static int tr_async_operation_require_owner_locked(
        const struct tr_async_operation_pool *pool)
{
	if (!pool->owner_bound ||
	    pthread_equal(pool->owner, pthread_self()) == 0)
		return TR_ERR_STATE;
	return TR_OK;
}

static int
tr_async_operation_lookup_locked(struct tr_async_operation_pool *pool,
                                 struct tr_async_operation operation,
                                 struct tr_async_operation_slot **out_slot)
{
	struct tr_async_operation_slot *slot;

	if (operation.instance == 0U || operation.generation == 0U)
		return TR_ERR_INVALID;
	if (operation.instance != pool->instance ||
	    operation.slot >= pool->capacity)
		return TR_ERR_STALE;
	slot = &pool->slots[operation.slot];
	if (slot->state == TR_ASYNC_OPERATION_FREE ||
	    slot->generation != operation.generation)
		return TR_ERR_STALE;
	if (out_slot)
		*out_slot = slot;
	return TR_OK;
}

static int
tr_async_operation_ref_lookup_locked(struct tr_async_operation_pool *pool,
                                     const struct tr_async_operation_ref *ref,
                                     struct tr_async_operation_slot **out_slot)
{
	struct tr_async_operation operation;
	struct tr_async_operation_slot *slot;
	int ret;

	if (!ref || ref->instance == 0U || ref->generation == 0U ||
	    ref->token == 0U)
		return TR_ERR_INVALID;
	operation = (struct tr_async_operation){
	        .instance = ref->instance,
	        .generation = ref->generation,
	        .slot = ref->slot,
	};
	ret = tr_async_operation_lookup_locked(pool, operation, &slot);
	if (ret != TR_OK)
		return ret;
	if (slot->token != ref->token)
		return TR_ERR_STALE;
	if (out_slot)
		*out_slot = slot;
	return TR_OK;
}

int tr_async_operation_pool_create(uint32_t capacity,
                                   struct tr_memory_budget *budget,
                                   struct tr_async_operation_pool **out)
{
	struct tr_async_operation_pool *pool;
	size_t slot_bytes_size;
	uint64_t slot_bytes;
	uint64_t storage_bytes;
	int ret;

	if (!out || *out || !budget || capacity == 0U)
		return TR_ERR_INVALID;
	ret = tr_async_operation_size_multiply(
	        (size_t)capacity, sizeof(struct tr_async_operation_slot),
	        &slot_bytes_size);
	if (ret != TR_OK)
		return ret;
	slot_bytes = (uint64_t)slot_bytes_size;
	if (slot_bytes >
	    UINT64_MAX - (uint64_t)sizeof(struct tr_async_operation_pool))
		return TR_ERR_STATE;
	storage_bytes =
	        (uint64_t)sizeof(struct tr_async_operation_pool) + slot_bytes;
	ret = tr_memory_budget_reserve(budget, storage_bytes);
	if (ret != TR_OK)
		return ret;
	pool = calloc(1U, sizeof(*pool));
	if (!pool) {
		(void)tr_memory_budget_release(budget, storage_bytes);
		return TR_ERR_NOMEM;
	}
	pool->slots = calloc((size_t)capacity, sizeof(*pool->slots));
	if (!pool->slots) {
		free(pool);
		(void)tr_memory_budget_release(budget, storage_bytes);
		return TR_ERR_NOMEM;
	}
	if (pthread_mutex_init(&pool->lock, NULL) != 0) {
		free(pool->slots);
		free(pool);
		(void)tr_memory_budget_release(budget, storage_bytes);
		return TR_ERR_SYS;
	}
	ret = tr_async_operation_allocate_instance(&pool->instance);
	if (ret != TR_OK) {
		(void)pthread_mutex_destroy(&pool->lock);
		free(pool->slots);
		free(pool);
		(void)tr_memory_budget_release(budget, storage_bytes);
		return ret;
	}
	pool->budget = budget;
	pool->storage_bytes = storage_bytes;
	pool->capacity = capacity;
	pool->accepting = 1;
	pool->lock_ready = 1;
	*out = pool;
	return TR_OK;
}

int tr_async_operation_pool_bind_owner(struct tr_async_operation_pool *pool)
{
	int ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	if (pool->owner_bound) {
		ret = TR_ERR_STATE;
	} else {
		pool->owner = pthread_self();
		pool->owner_bound = 1;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_async_operation_pool_handoff_owner(struct tr_async_operation_pool *pool,
                                          int external_serialization_proven)
{
	int ret;
	if (!external_serialization_proven)
		return TR_ERR_STATE;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	pool->owner = pthread_self();
	pool->owner_bound = 1;
	return pthread_mutex_unlock(&pool->lock) == 0 ? TR_OK : TR_ERR_SYS;
}

int tr_async_operation_reserve(struct tr_async_operation_pool *pool,
                               const struct tr_async_operation_spec *spec,
                               struct tr_async_operation *out_operation,
                               struct tr_async_operation_ref *out_producer_ref)
{
	struct tr_async_operation_slot *slot = NULL;
	struct tr_async_operation operation;
	struct tr_async_operation_ref producer_ref;
	uint32_t index;
	uint32_t exhausted = 0U;
	int ret;

	if (!pool || !spec || !out_operation || !out_producer_ref)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	if (!pool->accepting) {
		(void)pthread_mutex_unlock(&pool->lock);
		return TR_ERR_CLOSED;
	}
	for (index = 0U; index < pool->capacity; ++index) {
		struct tr_async_operation_slot *candidate = &pool->slots[index];
		if (candidate->state != TR_ASYNC_OPERATION_FREE)
			continue;
		if (candidate->generation == UINT64_MAX) {
			++exhausted;
			continue;
		}
		slot = candidate;
		break;
	}
	if (!slot) {
		if (exhausted == pool->capacity) {
			(void)pthread_mutex_unlock(&pool->lock);
			return TR_ERR_STATE;
		}
		if (pool->reserve_full != UINT64_MAX)
			++pool->reserve_full;
		(void)pthread_mutex_unlock(&pool->lock);
		return TR_AGAIN;
	}
	++slot->generation;
	slot->token = 1U;
	slot->spec = *spec;
	slot->completion = (struct tr_async_operation_completion){0};
	slot->cancel_reason = TR_OK;
	slot->cancel_requested = 0;
	slot->state = TR_ASYNC_OPERATION_RESERVED;
	operation = (struct tr_async_operation){
	        .instance = pool->instance,
	        .generation = slot->generation,
	        .slot = index,
	};
	producer_ref = (struct tr_async_operation_ref){
	        .instance = operation.instance,
	        .generation = operation.generation,
	        .token = slot->token,
	        .slot = operation.slot,
	};
	if (pthread_mutex_unlock(&pool->lock) != 0)
		return TR_ERR_SYS;
	*out_operation = operation;
	*out_producer_ref = producer_ref;
	return TR_OK;
}

int tr_async_operation_reject(struct tr_async_operation_pool *pool,
                              struct tr_async_operation operation,
                              struct tr_async_operation_ref *producer_ref)
{
	struct tr_async_operation_slot *slot;
	int ret;

	if (!pool || !producer_ref)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	ret = tr_async_operation_lookup_locked(pool, operation, &slot);
	if (ret == TR_OK && slot->state != TR_ASYNC_OPERATION_RESERVED)
		ret = TR_ERR_STATE;
	if (ret == TR_OK)
		ret = tr_async_operation_ref_lookup_locked(pool, producer_ref,
		                                           NULL);
	if (ret == TR_OK) {
		uint64_t generation = slot->generation;
		memset(slot, 0, sizeof(*slot));
		slot->generation = generation;
		memset(producer_ref, 0, sizeof(*producer_ref));
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_async_operation_pool_close(struct tr_async_operation_pool *pool)
{
	int ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	pool->accepting = 0;
	return pthread_mutex_unlock(&pool->lock) == 0 ? TR_OK : TR_ERR_SYS;
}

int tr_async_operation_accept(struct tr_async_operation_pool *pool,
                              struct tr_async_operation operation,
                              struct tr_async_operation_ref *out_completion_ref,
                              int *out_ready)
{
	struct tr_async_operation_slot *slot;
	struct tr_async_operation_ref completion_ref = {0};
	int ready = 0;
	int ret;

	if (!pool || !out_completion_ref || !out_ready)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	ret = tr_async_operation_lookup_locked(pool, operation, &slot);
	if (ret == TR_OK && slot->state == TR_ASYNC_OPERATION_RESERVED) {
		slot->state = TR_ASYNC_OPERATION_ACCEPTED;
	} else if (ret == TR_OK &&
	           slot->state == TR_ASYNC_OPERATION_RESERVED_READY) {
		if (slot->token == UINT64_MAX) {
			ret = TR_ERR_STATE;
		} else {
			++slot->token;
			slot->state = TR_ASYNC_OPERATION_PUBLISHED;
			completion_ref = (struct tr_async_operation_ref){
			        .instance = pool->instance,
			        .generation = slot->generation,
			        .token = slot->token,
			        .slot = operation.slot,
			};
			ready = 1;
		}
	} else if (ret == TR_OK) {
		ret = TR_ERR_STATE;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	if (ret == TR_OK) {
		if (ready)
			*out_completion_ref = completion_ref;
		*out_ready = ready;
	}
	return ret;
}

int tr_async_operation_publish(
        struct tr_async_operation_pool *pool,
        struct tr_async_operation_ref *producer_ref,
        struct tr_async_operation_completion completion,
        struct tr_async_operation_ref *out_completion_ref, int *out_ready)
{
	struct tr_async_operation_slot *slot;
	struct tr_async_operation_ref completion_ref = {0};
	int ready = 0;
	int ret;

	if (!pool || !producer_ref || !out_completion_ref || !out_ready)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_ref_lookup_locked(pool, producer_ref, &slot);
	if (ret == TR_OK && slot->state == TR_ASYNC_OPERATION_RESERVED) {
		slot->completion = completion;
		slot->state = TR_ASYNC_OPERATION_RESERVED_READY;
		memset(producer_ref, 0, sizeof(*producer_ref));
	} else if (ret == TR_OK && slot->state == TR_ASYNC_OPERATION_ACCEPTED) {
		if (slot->token == UINT64_MAX) {
			ret = TR_ERR_STATE;
		} else {
			slot->completion = completion;
			++slot->token;
			slot->state = TR_ASYNC_OPERATION_PUBLISHED;
			completion_ref = (struct tr_async_operation_ref){
			        .instance = pool->instance,
			        .generation = slot->generation,
			        .token = slot->token,
			        .slot = producer_ref->slot,
			};
			memset(producer_ref, 0, sizeof(*producer_ref));
			ready = 1;
		}
	} else if (ret == TR_OK) {
		ret = TR_ERR_STATE;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	if (ret == TR_OK) {
		*out_completion_ref = completion_ref;
		*out_ready = ready;
	}
	return ret;
}

int tr_async_operation_consume(
        struct tr_async_operation_pool *pool,
        const struct tr_async_operation_ref *completion_ref,
        struct tr_async_operation_spec *out_spec,
        struct tr_async_operation_completion *out_completion,
        int *out_cancelled, int *out_cancel_reason)
{
	struct tr_async_operation_slot *slot;
	struct tr_async_operation_spec spec;
	struct tr_async_operation_completion completion;
	int cancelled;
	int cancel_reason;
	int ret;

	if (!pool || !completion_ref || !out_spec || !out_completion ||
	    !out_cancelled || !out_cancel_reason)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	ret = tr_async_operation_ref_lookup_locked(pool, completion_ref, &slot);
	if (ret == TR_OK && slot->state != TR_ASYNC_OPERATION_PUBLISHED)
		ret = TR_ERR_STATE;
	if (ret == TR_OK) {
		spec = slot->spec;
		completion = slot->completion;
		cancelled = slot->cancel_requested;
		cancel_reason = slot->cancel_reason;
		slot->completion = (struct tr_async_operation_completion){0};
		slot->state = TR_ASYNC_OPERATION_CONSUMED;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	if (ret == TR_OK) {
		*out_spec = spec;
		*out_completion = completion;
		*out_cancelled = cancelled;
		*out_cancel_reason = cancel_reason;
	}
	return ret;
}

int tr_async_operation_retire(struct tr_async_operation_pool *pool,
                              struct tr_async_operation_ref *completion_ref)
{
	struct tr_async_operation_slot *slot;
	uint64_t generation;
	int ret;

	if (!pool || !completion_ref)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	ret = tr_async_operation_ref_lookup_locked(pool, completion_ref, &slot);
	if (ret == TR_OK && slot->state != TR_ASYNC_OPERATION_CONSUMED)
		ret = TR_ERR_STATE;
	if (ret == TR_OK) {
		generation = slot->generation;
		memset(slot, 0, sizeof(*slot));
		slot->generation = generation;
		memset(completion_ref, 0, sizeof(*completion_ref));
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}

int tr_async_operation_request_cancel(struct tr_async_operation_pool *pool,
                                      struct tr_async_operation operation,
                                      int reason, int *out_notify)
{
	struct tr_async_operation_slot *slot;
	int notify = 0;
	int ret;

	if (!pool || !out_notify)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_lookup_locked(pool, operation, &slot);
	if (ret == TR_OK && slot->state != TR_ASYNC_OPERATION_ACCEPTED)
		ret = TR_ERR_STATE;
	if (ret == TR_OK && !slot->cancel_requested) {
		slot->cancel_requested = 1;
		slot->cancel_reason = reason;
		notify = 1;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	if (ret == TR_OK)
		*out_notify = notify;
	return ret;
}

int tr_async_operation_get_stats(struct tr_async_operation_pool *pool,
                                 struct tr_async_operation_stats *out)
{
	struct tr_async_operation_stats stats = {0};
	uint32_t index;
	int ret;

	if (!pool || !out)
		return TR_ERR_INVALID;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	stats.capacity = pool->capacity;
	stats.reserve_full = pool->reserve_full;
	stats.accepting = pool->accepting;
	for (index = 0U; index < pool->capacity; ++index) {
		const struct tr_async_operation_slot *slot =
		        &pool->slots[index];
		switch (slot->state) {
		case TR_ASYNC_OPERATION_RESERVED:
		case TR_ASYNC_OPERATION_RESERVED_READY:
			++stats.reserved;
			break;
		case TR_ASYNC_OPERATION_ACCEPTED:
			++stats.accepted;
			break;
		case TR_ASYNC_OPERATION_PUBLISHED:
			++stats.published;
			break;
		case TR_ASYNC_OPERATION_CONSUMED:
			++stats.consumed;
			break;
		case TR_ASYNC_OPERATION_FREE:
			break;
		}
		if (slot->generation == UINT64_MAX)
			++stats.generation_exhausted;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0)
		return TR_ERR_SYS;
	*out = stats;
	return TR_OK;
}

int tr_async_operation_reap_stopped(struct tr_async_operation_pool *pool,
                                    int owner_and_producers_permanently_stopped,
                                    uint32_t *out_reaped)
{
	uint32_t index;
	uint32_t reaped = 0U;
	int ret;

	if (!pool || !out_reaped)
		return TR_ERR_INVALID;
	if (!owner_and_producers_permanently_stopped)
		return TR_ERR_STATE;
	ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	/* 外层静默证明排除并发准入；此处关闭池并接管清理责任。 */
	pool->accepting = 0;
	pool->owner = pthread_self();
	pool->owner_bound = 1;
	if (pthread_mutex_unlock(&pool->lock) != 0)
		return TR_ERR_SYS;

	/* 调用者已证明旧 owner 与 producer 永久静默并排除池 API 并发。 */
	for (index = 0U; index < pool->capacity; ++index) {
		struct tr_async_operation_slot *slot;
		struct tr_async_operation_completion completion;
		uint64_t generation;

		ret = tr_async_operation_lock(pool);
		if (ret != TR_OK)
			return ret;
		slot = &pool->slots[index];
		if (slot->state == TR_ASYNC_OPERATION_FREE) {
			(void)pthread_mutex_unlock(&pool->lock);
			continue;
		}
		completion = slot->completion;
		generation = slot->generation;
		memset(slot, 0, sizeof(*slot));
		slot->generation = generation;
		if (pthread_mutex_unlock(&pool->lock) != 0)
			return TR_ERR_SYS;
		if (completion.destroy)
			completion.destroy(completion.payload);
		++reaped;
	}
	*out_reaped = reaped;
	return TR_OK;
}

int tr_async_operation_pool_destroy(struct tr_async_operation_pool **pointer)
{
	struct tr_async_operation_pool *pool;
	uint32_t index;
	int ret;

	if (!pointer || !*pointer)
		return TR_ERR_INVALID;
	pool = *pointer;
	if (pool->lock_ready) {
		ret = tr_async_operation_lock(pool);
		if (ret != TR_OK)
			return ret;
		if (pool->owner_bound &&
		    pthread_equal(pool->owner, pthread_self()) == 0) {
			(void)pthread_mutex_unlock(&pool->lock);
			return TR_ERR_STATE;
		}
		if (pool->accepting) {
			(void)pthread_mutex_unlock(&pool->lock);
			return TR_ERR_STATE;
		}
		for (index = 0U; index < pool->capacity; ++index) {
			if (pool->slots[index].state !=
			    TR_ASYNC_OPERATION_FREE) {
				(void)pthread_mutex_unlock(&pool->lock);
				return TR_AGAIN;
			}
		}
		if (pthread_mutex_unlock(&pool->lock) != 0)
			return TR_ERR_SYS;
		if (pthread_mutex_destroy(&pool->lock) != 0)
			return TR_ERR_SYS;
		pool->lock_ready = 0;
	}
	ret = tr_memory_budget_release(pool->budget, pool->storage_bytes);
	if (ret != TR_OK)
		return ret;
	free(pool->slots);
	free(pool);
	*pointer = NULL;
	return TR_OK;
}

int tr_async_operation_test_set_generation(struct tr_async_operation_pool *pool,
                                           uint32_t slot_index,
                                           uint64_t generation)
{
	struct tr_async_operation_slot *slot;
	int ret = tr_async_operation_lock(pool);
	if (ret != TR_OK)
		return ret;
	ret = tr_async_operation_require_owner_locked(pool);
	if (ret != TR_OK) {
		(void)pthread_mutex_unlock(&pool->lock);
		return ret;
	}
	if (slot_index >= pool->capacity) {
		ret = TR_ERR_INVALID;
	} else {
		slot = &pool->slots[slot_index];
		if (slot->state != TR_ASYNC_OPERATION_FREE)
			ret = TR_ERR_STATE;
		else
			slot->generation = generation;
	}
	if (pthread_mutex_unlock(&pool->lock) != 0 && ret == TR_OK)
		return TR_ERR_SYS;
	return ret;
}
