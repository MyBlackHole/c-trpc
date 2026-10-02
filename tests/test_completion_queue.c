#include "../src/completion_queue.h"

#include "tr/status.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void noop(void *arg)
{
	(void)arg;
}

static void test_completion_admission_and_wake_coalescing(void)
{
	struct tr_completion_queue queue;
	struct tr_completion completion = { noop, NULL };
	struct tr_completion out[2];
	int need_wake;
	int has_more;
	size_t count;

	assert(tr_completion_queue_init(&queue, 2U) == TR_OK);

	need_wake = -1;
	assert(tr_completion_queue_push(&queue, &completion, &need_wake) ==
	       TR_ERR_CLOSED);

	assert(tr_completion_queue_open(&queue) == TR_OK);

	need_wake = 0;
	assert(tr_completion_queue_push(&queue, &completion, &need_wake) == TR_OK);
	assert(need_wake == 1);

	need_wake = 1;
	assert(tr_completion_queue_push(&queue, &completion, &need_wake) == TR_OK);
	assert(need_wake == 0);

	assert(tr_completion_queue_push(&queue, &completion, NULL) == TR_AGAIN);

	memset(out, 0, sizeof(out));
	has_more = 0;
	count = tr_completion_queue_pop_batch(&queue, out, 1U, &has_more);
	assert(count == 1U);
	assert(has_more == 1);

	/* Admission close is independent of remaining queued completions. */
	tr_completion_queue_close(&queue);
	assert(tr_completion_queue_push(&queue, &completion, NULL) ==
	       TR_ERR_CLOSED);

	has_more = 1;
	count = tr_completion_queue_pop_batch(&queue, out, 2U, &has_more);
	assert(count == 1U);
	assert(has_more == 0);

	/* Empty-drain clears wake coalescing; reopening starts a new wake epoch. */
	assert(tr_completion_queue_open(&queue) == TR_OK);
	need_wake = 0;
	assert(tr_completion_queue_push(&queue, &completion, &need_wake) == TR_OK);
	assert(need_wake == 1);
	tr_completion_queue_close(&queue);

	has_more = 1;
	count = tr_completion_queue_pop_batch(&queue, out, 2U, &has_more);
	assert(count == 1U);
	assert(has_more == 0);

	tr_completion_queue_destroy(&queue);
}

int main(void)
{
	test_completion_admission_and_wake_coalescing();
	puts("completion admission/wake coalescing: ok");
	return 0;
}
