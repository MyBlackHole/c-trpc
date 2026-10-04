#include "../src/completion_queue.h"

#include "tr/status.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static void noop(void *arg)
{
	(void)arg;
}

struct push_wait_ctx {
	struct tr_completion_queue *queue;
	struct tr_completion completion;
	int ret;
	int need_wake;
};

static void *push_wait_main(void *arg)
{
	struct push_wait_ctx *ctx = (struct push_wait_ctx *)arg;

	ctx->need_wake = -1;
	ctx->ret = tr_completion_queue_push_wait(
		ctx->queue, &ctx->completion, &ctx->need_wake);
	return NULL;
}

static void wait_for_waiters(
	struct tr_completion_queue *queue, uint32_t target)
{
	struct timespec pause = { 0, 1000000L };
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		uint32_t waiters;

		pthread_mutex_lock(&queue->lock);
		waiters = queue->waiters;
		pthread_mutex_unlock(&queue->lock);
		if (waiters == target)
			return;
		(void)nanosleep(&pause, NULL);
	}
	assert(!"timed out waiting for completion queue waiter");
}

static void test_completion_capacity_wait_and_close(void)
{
	struct tr_completion_queue queue;
	struct tr_completion first = { noop, (void *)(uintptr_t)1U };
	struct tr_completion out[2];
	struct push_wait_ctx producer;
	pthread_t thread;
	int need_wake = 0;
	int has_more = 0;
	size_t count;

	memset(&producer, 0, sizeof(producer));
	assert(tr_completion_queue_init(&queue, 1U) == TR_OK);
	assert(tr_completion_queue_open(&queue) == TR_OK);

	assert(tr_completion_queue_push(&queue, &first, &need_wake) == TR_OK);
	assert(need_wake == 1);

	producer.queue = &queue;
	producer.completion.fn = noop;
	producer.completion.arg = (void *)(uintptr_t)2U;
	assert(pthread_create(&thread, NULL, push_wait_main, &producer) == 0);
	wait_for_waiters(&queue, 1U);

	memset(out, 0, sizeof(out));
	count = tr_completion_queue_pop_batch(&queue, out, 1U, &has_more);
	assert(count == 1U);
	assert(has_more == 0);
	assert(out[0].arg == (void *)(uintptr_t)1U);
	assert(pthread_join(thread, NULL) == 0);
	assert(producer.ret == TR_OK);
	/* pop 清空了旧 wake epoch，blocked producer 必须重新请求 wake。 */
	assert(producer.need_wake == 1);
	wait_for_waiters(&queue, 0U);

	/*
	 * queue 再次满时，close 必须直接唤醒 waiter 返回 CLOSED；producer
	 * ownership 未转移，已有 completion 仍可由 consumer drain。
	 */
	producer.completion.arg = (void *)(uintptr_t)3U;
	producer.ret = TR_OK;
	producer.need_wake = -1;
	assert(pthread_create(&thread, NULL, push_wait_main, &producer) == 0);
	wait_for_waiters(&queue, 1U);
	tr_completion_queue_close(&queue);
	{
		uint64_t closed_generation;

		pthread_mutex_lock(&queue.lock);
		closed_generation = queue.admission_generation;
		pthread_mutex_unlock(&queue.lock);

		/*
		 * 故意在旧 waiter join 前 reopen。旧 generation 无论何时重新取得
		 * queue lock，都不能把上一运行期的 completion 投进新 epoch。
		 */
		assert(tr_completion_queue_open(&queue) == TR_OK);
		pthread_mutex_lock(&queue.lock);
		assert(queue.admission_generation != closed_generation);
		pthread_mutex_unlock(&queue.lock);
	}
	assert(pthread_join(thread, NULL) == 0);
	assert(producer.ret == TR_ERR_CLOSED);
	assert(producer.need_wake == -1);
	wait_for_waiters(&queue, 0U);

	memset(out, 0, sizeof(out));
	count = tr_completion_queue_pop_batch(&queue, out, 1U, &has_more);
	assert(count == 1U);
	assert(out[0].arg == (void *)(uintptr_t)2U);
	assert(has_more == 0);

	pthread_mutex_lock(&queue.lock);
	assert(queue.full_events == 2U);
	assert(queue.waiters == 0U);
	pthread_mutex_unlock(&queue.lock);
	tr_completion_queue_close(&queue);
	tr_completion_queue_destroy(&queue);
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
	test_completion_capacity_wait_and_close();
	puts("completion admission/wake coalescing/capacity wait: ok");
	return 0;
}
