#define _POSIX_C_SOURCE 200809L
#include "../src/execution/command_queue.h"
#include "tr/status.h"

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

struct wait_ctx {
	struct tr_command_queue *queue;
	struct tr_command command;
	uint64_t generation;
	int force;
	int ret;
	int need_wake;
};

static void pause_1ms(void)
{
	struct timespec pause = { 0, 1000000L };

	(void)nanosleep(&pause, NULL);
}

static void wait_for_waiters(
	struct tr_command_queue *queue, uint32_t target)
{
	unsigned i;

	for (i = 0; i < 5000U; ++i) {
		uint32_t waiters;

		pthread_mutex_lock(&queue->lock);
		waiters = queue->waiters;
		pthread_mutex_unlock(&queue->lock);
		if (waiters == target)
			return;
		pause_1ms();
	}
	assert(!"timed out waiting for command queue waiter");
}

static void *wait_main(void *arg)
{
	struct wait_ctx *ctx = (struct wait_ctx *)arg;

	ctx->need_wake = -1;
	if (ctx->force)
		ctx->ret = tr_command_queue_push_wait_force(
			ctx->queue, &ctx->command, &ctx->need_wake);
	else
		ctx->ret = tr_command_queue_push_wait(
			ctx->queue, &ctx->command, ctx->generation,
			&ctx->need_wake);
	return NULL;
}

static void test_immediate_push_stays_nonblocking(void)
{
	struct tr_command_queue queue;
	struct tr_command send;
	struct tr_command resume;
	struct tr_command out[2];
	int need_wake;
	size_t count;

	memset(&send, 0, sizeof(send));
	memset(&resume, 0, sizeof(resume));
	send.type = TR_CMD_SEND;
	resume.type = TR_CMD_RESUME_RX;

	assert(tr_command_queue_is_empty(NULL) == 0);
	assert(tr_command_queue_init(&queue, 2U) == TR_OK);
	assert(tr_command_queue_is_empty(&queue) == 1);
	need_wake = 0;
	assert(tr_command_queue_push(&queue, &send, &need_wake) == TR_OK);
	assert(need_wake == 1);
	assert(tr_command_queue_is_empty(&queue) == 0);
	need_wake = 1;
	assert(tr_command_queue_push(&queue, &resume, &need_wake) == TR_OK);
	assert(need_wake == 0);
	assert(tr_command_queue_last_sequence(&queue) == 2U);
	assert(tr_command_queue_push(&queue, &send, NULL) == TR_AGAIN);
	assert(tr_command_queue_last_sequence(&queue) == 2U);

	pthread_mutex_lock(&queue.lock);
	assert(queue.count == 2U);
	assert(queue.full_events == 1U);
	assert(queue.full_send == 1U);
	assert(queue.pushed_send == 1U);
	assert(queue.pushed_resume_rx == 1U);
	pthread_mutex_unlock(&queue.lock);

	count = tr_command_queue_pop_batch(&queue, out, 2U);
	assert(count == 2U);
	assert(out[0].sequence == 1U);
	assert(out[1].sequence == 2U);
	assert(out[0].sequence < out[1].sequence);
	assert(tr_command_queue_is_empty(&queue) == 1);
	tr_command_queue_destroy(&queue);
}

static void test_batch_pop_wakes_sync_waiters(void)
{
	struct tr_command_queue queue;
	struct tr_command seed;
	struct tr_command out[2];
	struct wait_ctx waiters[2];
	pthread_t threads[2];
	uint64_t generation;
	size_t count;
	unsigned i;

	memset(&seed, 0, sizeof(seed));
	seed.type = TR_CMD_RESUME_RX;
	memset(waiters, 0, sizeof(waiters));

	assert(tr_command_queue_init(&queue, 2U) == TR_OK);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	generation = tr_command_queue_wait_generation(&queue);
	assert(generation != 0U);
	assert(tr_command_queue_push(&queue, &seed, NULL) == TR_OK);
	assert(tr_command_queue_push(&queue, &seed, NULL) == TR_OK);

	waiters[0].queue = &queue;
	waiters[0].generation = generation;
	waiters[0].command.type = TR_CMD_CALL;
	waiters[1].queue = &queue;
	waiters[1].generation = generation;
	waiters[1].command.type = TR_CMD_QUIESCE;
	for (i = 0; i < 2U; ++i)
		assert(pthread_create(
			       &threads[i], NULL, wait_main,
			       &waiters[i]) == 0);
	wait_for_waiters(&queue, 2U);

	count = tr_command_queue_pop_batch(&queue, out, 2U);
	assert(count == 2U);
	for (i = 0; i < 2U; ++i) {
		assert(pthread_join(threads[i], NULL) == 0);
		assert(waiters[i].ret == TR_OK);
	}
	wait_for_waiters(&queue, 0U);

	pthread_mutex_lock(&queue.lock);
	assert(queue.count == 2U);
	assert(queue.full_events == 2U);
	assert(queue.full_call == 1U);
	assert(queue.full_other == 1U);
	pthread_mutex_unlock(&queue.lock);

	tr_command_queue_wait_close(&queue);
	count = tr_command_queue_pop_batch(&queue, out, 2U);
	assert(count == 2U);
	tr_command_queue_destroy(&queue);
}

static void test_close_cancels_waiter_and_stop_uses_reserve(void)
{
	struct tr_command_queue queue;
	struct tr_command seed = { .type = TR_CMD_RESUME_RX };
	struct tr_command stop = { .type = TR_CMD_STOP };
	struct tr_command out;
	struct wait_ctx normal = { 0 };
	pthread_t normal_thread;
	uint64_t generation;
	int need_wake = -1;

	assert(tr_command_queue_init(&queue, 1U) == TR_OK);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	generation = tr_command_queue_wait_generation(&queue);
	assert(generation != 0U);
	assert(tr_command_queue_push(&queue, &seed, NULL) == TR_OK);

	normal.queue = &queue;
	normal.generation = generation;
	normal.command.type = TR_CMD_CALL;
	assert(pthread_create(&normal_thread, NULL, wait_main, &normal) == 0);
	wait_for_waiters(&queue, 1U);

	tr_command_queue_wait_close(&queue);
	assert(pthread_join(normal_thread, NULL) == 0);
	assert(normal.ret == TR_ERR_CLOSED);
	assert(normal.need_wake == -1);

	/* 尚未消费普通命令；STOP 必须不等待、不挤占普通容量。 */
	assert(tr_command_queue_push_wait_force(&queue, &stop, &need_wake) == TR_OK);
	assert(need_wake == 0);
	wait_for_waiters(&queue, 0U);
	assert(queue.count == 1U && queue.peak_count == 1U);
	assert(queue.full_call == 1U && queue.full_other == 0U);
	assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
	assert(out.type == TR_CMD_RESUME_RX);
	assert(!tr_command_queue_is_empty(&queue));
	assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
	assert(out.type == TR_CMD_STOP);
	assert(tr_command_queue_is_empty(&queue));
	tr_command_queue_destroy(&queue);
}

static void test_stop_fifo_budget_and_epoch(void)
{
	struct tr_command_queue queue;
	struct tr_command command = { .type = TR_CMD_RESUME_RX };
	struct tr_command stop = { .type = TR_CMD_STOP };
	struct tr_command out[2];
	uint64_t old_generation;
	int need_wake = -1;

	assert(tr_command_queue_init(&queue, 2U) == TR_OK);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	old_generation = tr_command_queue_wait_generation(&queue);
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
	assert(tr_command_queue_push_wait_force(&queue, &stop, NULL) == TR_OK);
	assert(tr_command_queue_last_sequence(&queue) == 3U);
	assert(tr_command_queue_push_wait_force(&queue, &stop, &need_wake) ==
	       TR_ERR_STATE);
	assert(need_wake == -1);
	assert(tr_command_queue_push_wait_force(&queue, &command, NULL) ==
	       TR_ERR_INVALID);
	assert(tr_command_queue_last_sequence(&queue) == 3U);
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_ERR_CLOSED);
	assert(tr_command_queue_push_wait(
		       &queue, &command, old_generation, NULL) == TR_ERR_CLOSED);
	assert(tr_command_queue_wait_open(&queue) == TR_ERR_STATE);

	/* 满 batch 只能弹出两个前驱，STOP 留到下一 batch，不能突破预算。 */
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 2U);
	assert(out[0].sequence == 1U && out[1].sequence == 2U);
	assert(queue.count == 0U && queue.stop_pending);
	assert(queue.wake_pending && !tr_command_queue_is_empty(&queue));
	assert(tr_command_queue_pop_batch(&queue, out, 0U) == 0U);
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 1U);
	assert(out[0].type == TR_CMD_STOP && out[0].sequence == 3U);
	assert(tr_command_queue_is_empty(&queue) && !queue.wake_pending);
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_ERR_CLOSED);
	assert(tr_command_queue_push_wait_force(&queue, &stop, NULL) == TR_ERR_STATE);

	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	assert(tr_command_queue_push_wait(
		       &queue, &command, old_generation, NULL) == TR_ERR_CLOSED);
	assert(tr_command_queue_push_wait(
		       &queue, &command, tr_command_queue_wait_generation(&queue),
		       &need_wake) == TR_OK);
	assert(need_wake == 1);
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 1U);
	assert(out[0].type == TR_CMD_RESUME_RX && out[0].sequence == 4U);
	tr_command_queue_destroy(&queue);
}

static void test_stop_empty_and_sequence_wrap(void)
{
	struct tr_command_queue queue;
	struct tr_command stop = { .type = TR_CMD_STOP };
	struct tr_command command = { .type = TR_CMD_RESUME_RX };
	struct tr_command out[2];
	int need_wake = 0;

	assert(tr_command_queue_init(&queue, 1U) == TR_OK);
	assert(tr_command_queue_push_wait_force(&queue, &stop, &need_wake) == TR_OK);
	assert(need_wake == 1 && !tr_command_queue_is_empty(&queue));
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 1U);
	assert(out[0].type == TR_CMD_STOP);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);

	queue.next_sequence = UINT64_MAX - 1U;
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
	assert(tr_command_queue_push_wait_force(&queue, &stop, NULL) == TR_OK);
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 2U);
	assert(out[0].type == TR_CMD_RESUME_RX && out[0].sequence == UINT64_MAX);
	assert(out[1].type == TR_CMD_STOP && out[1].sequence == 1U);
	assert(tr_command_sequence_after_eq(out[1].sequence, out[0].sequence));
	tr_command_queue_destroy(&queue);
}

static void test_wait_generation_fences_reopen(void)
{
	struct tr_command_queue queue;
	struct tr_command seed;
	struct tr_command out;
	struct wait_ctx old_waiter;
	pthread_t thread;
	uint64_t old_generation;

	memset(&seed, 0, sizeof(seed));
	memset(&old_waiter, 0, sizeof(old_waiter));
	seed.type = TR_CMD_RESUME_RX;

	assert(tr_command_queue_init(&queue, 1U) == TR_OK);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	old_generation = tr_command_queue_wait_generation(&queue);
	assert(old_generation != 0U);
	assert(tr_command_queue_push(&queue, &seed, NULL) == TR_OK);

	old_waiter.queue = &queue;
	old_waiter.generation = old_generation;
	old_waiter.command.type = TR_CMD_CALL;
	assert(pthread_create(&thread, NULL, wait_main, &old_waiter) == 0);
	wait_for_waiters(&queue, 1U);

	tr_command_queue_wait_close(&queue);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	assert(tr_command_queue_wait_generation(&queue) != old_generation);
	assert(pthread_join(thread, NULL) == 0);
	assert(old_waiter.ret == TR_ERR_CLOSED);

	assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
	assert(out.type == TR_CMD_RESUME_RX);
	tr_command_queue_wait_close(&queue);
	tr_command_queue_destroy(&queue);
}

static void test_sequence_wrap_order(void)
{
	struct tr_command_queue queue;
	struct tr_command command;
	struct tr_command out[2];

	memset(&command, 0, sizeof(command));
	command.type = TR_CMD_RESUME_RX;
	assert(tr_command_queue_init(&queue, 2U) == TR_OK);

	/* Force the only interesting boundary without executing 2^64 pushes. */
	pthread_mutex_lock(&queue.lock);
	queue.next_sequence = UINT64_MAX - 1U;
	pthread_mutex_unlock(&queue.lock);

	assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
	assert(tr_command_queue_push(&queue, &command, NULL) == TR_OK);
	assert(tr_command_queue_last_sequence(&queue) == 1U);
	assert(tr_command_queue_pop_batch(&queue, out, 2U) == 2U);
	assert(out[0].sequence == UINT64_MAX);
	assert(out[1].sequence == 1U);

	assert(tr_command_sequence_after_eq(UINT64_MAX, UINT64_MAX));
	assert(tr_command_sequence_after_eq(1U, UINT64_MAX));
	assert(!tr_command_sequence_after_eq(UINT64_MAX, 1U));
	assert(tr_command_sequence_after_eq(0U, 0U));

	tr_command_queue_destroy(&queue);
}

int main(void)
{
	test_immediate_push_stays_nonblocking();
	test_batch_pop_wakes_sync_waiters();
	test_close_cancels_waiter_and_stop_uses_reserve();
	test_stop_fifo_budget_and_epoch();
	test_stop_empty_and_sequence_wrap();
	test_wait_generation_fences_reopen();
	test_sequence_wrap_order();
	puts("command queue selective capacity waits: ok");
	return 0;
}
