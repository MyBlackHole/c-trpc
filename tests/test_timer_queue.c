#include "../src/timer_queue.h"
#include "tr/status.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#define MODEL_CAPACITY 31U

struct timer_ctx {
	struct tr_timer_token token;
	uint64_t deadline;
	unsigned fired;
	int registered;
};

static uint64_t count_callback(void *arg, uint64_t now_ns)
{
	struct timer_ctx *ctx = arg;

	(void)now_ns;
	ctx->fired++;
	ctx->deadline = 0;
	return 0;
}

/* Every slot belongs to exactly one of the free list or registered set. */
static void check_queue(const struct tr_timer_queue *queue)
{
	unsigned char free_seen[MODEL_CAPACITY] = {0};
	unsigned char heap_seen[MODEL_CAPACITY] = {0};
	uint32_t free_count = 0;
	uint32_t used_count = 0;
	uint32_t slot = queue->free_head;
	uint32_t i;

	assert(queue->capacity <= MODEL_CAPACITY);
	assert(queue->size <= queue->capacity);
	while (slot != UINT32_MAX) {
		assert(slot < queue->capacity);
		assert(!free_seen[slot]);
		assert(!queue->entries[slot].used);
		free_seen[slot] = 1;
		free_count++;
		slot = queue->entries[slot].next_free;
	}
	for (i = 0; i < queue->size; ++i) {
		const struct tr_timer_entry *entry;

		slot = queue->heap[i];
		assert(slot < queue->capacity);
		assert(!heap_seen[slot]);
		heap_seen[slot] = 1;
		entry = &queue->entries[slot];
		assert(entry->used && entry->deadline_ns != 0);
		assert(entry->heap_pos == i);
		if (i != 0) {
			uint32_t parent_slot = queue->heap[(i - 1U) / 2U];
			uint64_t parent_deadline =
				queue->entries[parent_slot].deadline_ns;

			assert(parent_deadline < entry->deadline_ns ||
			       (parent_deadline == entry->deadline_ns &&
				parent_slot < slot));
		}
	}
	for (i = 0; i < queue->capacity; ++i) {
		const struct tr_timer_entry *entry = &queue->entries[i];

		assert((entry->used != 0) != (free_seen[i] != 0));
		if (!entry->used)
			continue;
		used_count++;
		assert(entry->generation != 0 && !entry->running);
		assert((entry->deadline_ns != 0) == (heap_seen[i] != 0));
		if (!heap_seen[i])
			assert(entry->heap_pos == UINT32_MAX);
	}
	assert(free_count + used_count == queue->capacity);
}

static void test_registration_recycling(void)
{
	struct tr_timer_queue queue = {0};
	struct timer_ctx ctx[4] = {0};
	struct tr_timer_token recycled;
	struct tr_timer_token extra = {42U, 43U};
	uint32_t i;

	assert(tr_timer_queue_init(NULL, 4U) == TR_ERR_INVALID);
	assert(tr_timer_queue_init(&queue, 0U) == TR_ERR_INVALID);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &extra) ==
	       TR_AGAIN);
	assert(tr_timer_queue_init(&queue, 4U) == TR_OK);
	assert(tr_timer_queue_register(NULL, count_callback, ctx, &extra) ==
	       TR_ERR_INVALID);
	assert(tr_timer_queue_register(&queue, NULL, ctx, &extra) == TR_ERR_INVALID);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, NULL) ==
	       TR_ERR_INVALID);
	for (i = 0; i < 4U; ++i) {
		assert(tr_timer_queue_register(&queue, count_callback, &ctx[i],
					       &ctx[i].token) == TR_OK);
		assert(tr_timer_queue_arm(&queue, ctx[i].token, 10U + i) == TR_OK);
	}
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &extra) ==
	       TR_AGAIN);
	assert(extra.slot == 42U && extra.generation == 43U);
	assert(tr_timer_queue_arm(&queue, ctx[1].token, 0U) == TR_OK);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &extra) ==
	       TR_AGAIN); /* Disarming is not unregistering. */
	assert(tr_timer_queue_unregister(&queue, ctx[1].token) == TR_OK);
	assert(tr_timer_queue_unregister(&queue, ctx[3].token) == TR_OK);
	check_queue(&queue);
	assert(tr_timer_queue_unregister(&queue, ctx[3].token) == TR_ERR_STALE);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &recycled) ==
	       TR_OK);
	assert(recycled.slot == ctx[3].token.slot);
	assert(recycled.generation != ctx[3].token.generation);
	assert(tr_timer_queue_arm(&queue, ctx[3].token, 1U) == TR_ERR_STALE);
	assert(tr_timer_queue_unregister(&queue, ctx[3].token) == TR_ERR_STALE);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &recycled) ==
	       TR_OK);
	assert(recycled.slot == ctx[1].token.slot);
	assert(recycled.generation != ctx[1].token.generation);
	check_queue(&queue);
	tr_timer_queue_destroy(&queue);
	assert(tr_timer_queue_register(&queue, count_callback, ctx, &extra) ==
	       TR_AGAIN);
	tr_timer_queue_destroy(&queue);
}

static void test_zero_budget(void)
{
	struct tr_timer_queue queue;
	struct timer_ctx ctx = {0};
	int more = -1;

	assert(tr_timer_queue_run_due(NULL, 10U, 0U, &more) == 0U);
	assert(more == 0);
	assert(tr_timer_queue_init(&queue, 1U) == TR_OK);
	more = -1;
	assert(tr_timer_queue_run_due(&queue, 10U, 0U, &more) == 0U);
	assert(more == 0);
	assert(tr_timer_queue_register(&queue, count_callback, &ctx, &ctx.token) ==
	       TR_OK);
	assert(tr_timer_queue_arm(&queue, ctx.token, 10U) == TR_OK);
	more = -1;
	assert(tr_timer_queue_run_due(&queue, 9U, 0U, &more) == 0U);
	assert(more == 0);
	assert(tr_timer_queue_run_due(&queue, 10U, 0U, &more) == 0U);
	assert(more == 1 && ctx.fired == 0U);
	assert(tr_timer_queue_next_deadline(&queue) == 10U);
	assert(tr_timer_queue_run_due(&queue, 10U, 1U, &more) == 1U);
	assert(more == 0 && ctx.fired == 1U);
	assert(tr_timer_queue_run_due(&queue, 10U, 0U, NULL) == 0U);
	check_queue(&queue);
	tr_timer_queue_destroy(&queue);
}

enum mutation {
	MUTATION_REARM,
	MUTATION_DISARM,
	MUTATION_UNREGISTER,
	MUTATION_REPLACE
};

struct mutation_ctx {
	struct tr_timer_queue *queue;
	struct tr_timer_token original;
	struct timer_ctx replacement;
	enum mutation mutation;
	unsigned fired;
};

static uint64_t mutate_callback(void *arg, uint64_t now_ns)
{
	struct mutation_ctx *ctx = arg;

	assert(now_ns == 10U);
	ctx->fired++;
	switch (ctx->mutation) {
	case MUTATION_REARM:
		assert(tr_timer_queue_arm(ctx->queue, ctx->original, 50U) == TR_OK);
		break;
	case MUTATION_DISARM:
		assert(tr_timer_queue_arm(ctx->queue, ctx->original, 0U) == TR_OK);
		break;
	case MUTATION_UNREGISTER:
	case MUTATION_REPLACE:
		assert(tr_timer_queue_unregister(ctx->queue, ctx->original) == TR_OK);
		if (ctx->mutation == MUTATION_REPLACE) {
			assert(tr_timer_queue_register(ctx->queue, count_callback,
				&ctx->replacement, &ctx->replacement.token) == TR_OK);
			assert(tr_timer_queue_arm(ctx->queue, ctx->replacement.token,
						  50U) == TR_OK);
		}
		break;
	}
	return 1U; /* Must not override the explicit mutation above. */
}

static void test_callback_mutations(void)
{
	unsigned mode;

	for (mode = MUTATION_REARM; mode <= MUTATION_REPLACE; ++mode) {
		struct tr_timer_queue queue;
		struct mutation_ctx ctx = {0};
		int more = -1;

		assert(tr_timer_queue_init(&queue, 1U) == TR_OK);
		ctx.queue = &queue;
		ctx.mutation = (enum mutation)mode;
		assert(tr_timer_queue_register(&queue, mutate_callback, &ctx,
					       &ctx.original) == TR_OK);
		assert(tr_timer_queue_arm(&queue, ctx.original, 10U) == TR_OK);
		assert(tr_timer_queue_run_due(&queue, 10U, 4U, &more) == 1U);
		assert(more == 0 && ctx.fired == 1U);
		assert(tr_timer_queue_next_deadline(&queue) ==
		       (mode == MUTATION_REARM || mode == MUTATION_REPLACE ? 50U : 0U));
		if (mode == MUTATION_REPLACE) {
			assert(ctx.original.slot == ctx.replacement.token.slot);
			assert(ctx.original.generation != ctx.replacement.token.generation);
			assert(tr_timer_queue_unregister(&queue, ctx.original) == TR_ERR_STALE);
			assert(tr_timer_queue_run_due(&queue, 50U, 4U, &more) == 1U);
			assert(ctx.replacement.fired == 1U);
		}
		check_queue(&queue);
		tr_timer_queue_destroy(&queue);
	}
}

static void test_equal_deadlines_budget(void)
{
	struct tr_timer_queue queue;
	struct timer_ctx ctx[4] = {0};
	uint32_t i;
	int more = -1;

	assert(tr_timer_queue_init(&queue, 4U) == TR_OK);
	for (i = 0; i < 4U; ++i)
		assert(tr_timer_queue_register(&queue, count_callback, &ctx[i],
					       &ctx[i].token) == TR_OK);
	/* Insert in reverse order; equal deadlines must still follow slot order. */
	for (i = 4U; i != 0; --i)
		assert(tr_timer_queue_arm(&queue, ctx[i - 1U].token, 10U) == TR_OK);
	assert(tr_timer_queue_run_due(&queue, 10U, 2U, &more) == 2U);
	assert(more == 1 && ctx[0].fired == 1U && ctx[1].fired == 1U);
	assert(ctx[2].fired == 0U && ctx[3].fired == 0U);
	check_queue(&queue);
	assert(tr_timer_queue_run_due(&queue, 10U, 2U, &more) == 2U);
	assert(more == 0 && ctx[2].fired == 1U && ctx[3].fired == 1U);
	check_queue(&queue);
	tr_timer_queue_destroy(&queue);
}

static uint32_t random_next(uint32_t *state)
{
	*state ^= *state << 13;
	*state ^= *state >> 17;
	*state ^= *state << 5;
	return *state;
}

static void test_randomized_lifecycle(void)
{
	static const uint32_t capacities[] = {1U, 7U, MODEL_CAPACITY};
	size_t c;

	for (c = 0; c < sizeof(capacities) / sizeof(capacities[0]); ++c) {
		struct tr_timer_queue queue;
		struct timer_ctx model[MODEL_CAPACITY] = {0};
		uint32_t state = UINT32_C(0x95c43ab7) ^ capacities[c];
		unsigned step;

		assert(tr_timer_queue_init(&queue, capacities[c]) == TR_OK);
		for (step = 0; step < 20000U; ++step) {
			struct timer_ctx *ctx = &model[random_next(&state) % queue.capacity];
			uint32_t op = random_next(&state) % 7U;
			uint64_t deadline = 1U + random_next(&state) % 1000U;
			uint64_t nearest = 0;
			uint32_t i;

			if (op == 0U && !ctx->registered) {
				assert(tr_timer_queue_register(&queue, count_callback, ctx,
							       &ctx->token) == TR_OK);
				ctx->registered = 1;
			} else if (op == 1U && ctx->registered) {
				assert(tr_timer_queue_unregister(&queue, ctx->token) == TR_OK);
				assert(tr_timer_queue_unregister(&queue, ctx->token) == TR_ERR_STALE);
				assert(tr_timer_queue_arm(&queue, ctx->token, deadline) == TR_ERR_STALE);
				ctx->registered = 0;
				ctx->deadline = 0;
			} else if (op >= 2U && op <= 4U && ctx->registered) {
				ctx->deadline = op == 4U ? 0U : deadline;
				assert(tr_timer_queue_arm(&queue, ctx->token, ctx->deadline) == TR_OK);
			} else if (op >= 5U) {
				size_t budget = op == 6U ? 0U : random_next(&state) % 5U;
				size_t due = 0;
				int more = -1;

				for (i = 0; i < queue.capacity; ++i)
					if (model[i].deadline != 0 && model[i].deadline <= deadline)
						due++;
				assert(tr_timer_queue_run_due(&queue, deadline, budget, &more) ==
				       (due < budget ? due : budget));
				assert(more == (due > budget));
			}
			for (i = 0; i < queue.capacity; ++i) {
				const struct timer_ctx *item = &model[i];

				if (!item->registered)
					continue;
				assert(queue.entries[item->token.slot].used);
				assert(queue.entries[item->token.slot].generation == item->token.generation);
				assert(queue.entries[item->token.slot].deadline_ns == item->deadline);
				if (item->deadline != 0 && (nearest == 0 || item->deadline < nearest))
					nearest = item->deadline;
			}
			assert(tr_timer_queue_next_deadline(&queue) == nearest);
			check_queue(&queue);
		}
		tr_timer_queue_destroy(&queue);
	}
}

static void test_high_occupancy_reuse(void)
{
	struct tr_timer_queue queue;
	struct timer_ctx ctx[MODEL_CAPACITY] = {0};
	struct tr_timer_token old;
	uint32_t i;

	assert(tr_timer_queue_init(&queue, MODEL_CAPACITY) == TR_OK);
	for (i = 0; i < MODEL_CAPACITY; ++i)
		assert(tr_timer_queue_register(&queue, count_callback, &ctx[i],
					       &ctx[i].token) == TR_OK);
	for (i = 0; i < 10000U; ++i) {
		old = ctx[MODEL_CAPACITY - 1U].token;
		assert(tr_timer_queue_unregister(&queue, old) == TR_OK);
		assert(tr_timer_queue_register(&queue, count_callback,
			&ctx[MODEL_CAPACITY - 1U], &ctx[MODEL_CAPACITY - 1U].token) == TR_OK);
		assert(ctx[MODEL_CAPACITY - 1U].token.slot == old.slot);
		assert(ctx[MODEL_CAPACITY - 1U].token.generation != old.generation);
		assert(tr_timer_queue_unregister(&queue, old) == TR_ERR_STALE);
		check_queue(&queue);
	}
	tr_timer_queue_destroy(&queue);
}

#define RUN_TEST(fn) do { fn(); puts(#fn ": ok"); } while (0)

int main(void)
{
	RUN_TEST(test_registration_recycling);
	RUN_TEST(test_zero_budget);
	RUN_TEST(test_callback_mutations);
	RUN_TEST(test_equal_deadlines_budget);
	RUN_TEST(test_randomized_lifecycle);
	RUN_TEST(test_high_occupancy_reuse);
	return 0;
}
