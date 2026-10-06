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
	assert(tr_command_queue_push(&queue, &send, NULL) == TR_AGAIN);

	pthread_mutex_lock(&queue.lock);
	assert(queue.count == 2U);
	assert(queue.full_events == 1U);
	assert(queue.full_send == 1U);
	assert(queue.pushed_send == 1U);
	assert(queue.pushed_resume_rx == 1U);
	pthread_mutex_unlock(&queue.lock);

	count = tr_command_queue_pop_batch(&queue, out, 2U);
	assert(count == 2U);
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

static void test_close_cancels_sync_waiter_but_not_forced_stop(void)
{
	struct tr_command_queue queue;
	struct tr_command seed;
	struct tr_command out;
	struct wait_ctx normal;
	struct wait_ctx forced;
	pthread_t normal_thread;
	pthread_t forced_thread;
	uint64_t generation;

	memset(&seed, 0, sizeof(seed));
	memset(&normal, 0, sizeof(normal));
	memset(&forced, 0, sizeof(forced));
	seed.type = TR_CMD_RESUME_RX;

	assert(tr_command_queue_init(&queue, 1U) == TR_OK);
	assert(tr_command_queue_wait_open(&queue) == TR_OK);
	generation = tr_command_queue_wait_generation(&queue);
	assert(generation != 0U);
	assert(tr_command_queue_push(&queue, &seed, NULL) == TR_OK);

	normal.queue = &queue;
	normal.generation = generation;
	normal.command.type = TR_CMD_CALL;
	forced.queue = &queue;
	forced.force = 1;
	forced.command.type = TR_CMD_STOP;

	assert(pthread_create(
		       &normal_thread, NULL, wait_main, &normal) == 0);
	assert(pthread_create(
		       &forced_thread, NULL, wait_main, &forced) == 0);
	wait_for_waiters(&queue, 2U);

	tr_command_queue_wait_close(&queue);
	assert(pthread_join(normal_thread, NULL) == 0);
	assert(normal.ret == TR_ERR_CLOSED);
	assert(normal.need_wake == -1);

	/*
	 * close 只取消普通同步 waiter。STOP force waiter 会重新检查 ring，
	 * 发现仍满后继续睡眠，直到 owner pop 释放真实容量。
	 */
	wait_for_waiters(&queue, 1U);
	assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
	assert(out.type == TR_CMD_RESUME_RX);
	assert(pthread_join(forced_thread, NULL) == 0);
	assert(forced.ret == TR_OK);
	assert(tr_command_queue_pop_batch(&queue, &out, 1U) == 1U);
	assert(out.type == TR_CMD_STOP);
	wait_for_waiters(&queue, 0U);

	pthread_mutex_lock(&queue.lock);
	assert(queue.full_call == 1U);
	assert(queue.full_other == 1U);
	pthread_mutex_unlock(&queue.lock);
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

int main(void)
{
	test_immediate_push_stays_nonblocking();
	test_batch_pop_wakes_sync_waiters();
	test_close_cancels_sync_waiter_but_not_forced_stop();
	test_wait_generation_fences_reopen();
	puts("command queue selective capacity waits: ok");
	return 0;
}
