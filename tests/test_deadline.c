#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "../src/execution/deadline_internal.h"
#include "tr/status.h"

struct poll_step {
	int expected_ms;
	int result;
	int error;
	uint32_t advance_ms;
};

static struct timespec mock_now;
static struct poll_step steps[12];
static unsigned int step_count;
static unsigned int step_index;
static int clock_fails;

static void set_now(time_t sec, long nsec)
{
	mock_now.tv_sec = sec;
	mock_now.tv_nsec = nsec;
	step_count = 0;
	step_index = 0;
	clock_fails = 0;
}

static void add_step(int expected_ms, int result, int error,
		     uint32_t advance_ms)
{
	assert(step_count < sizeof(steps) / sizeof(steps[0]));
	steps[step_count++] = (struct poll_step){
		expected_ms, result, error, advance_ms
	};
}

int __wrap_clock_gettime(clockid_t clock_id, struct timespec *out)
{
	assert(clock_id == CLOCK_MONOTONIC);
	if (clock_fails) {
		errno = EIO;
		return -1;
	}
	*out = mock_now;
	return 0;
}

int __wrap_poll(struct pollfd *fds, nfds_t count, int timeout_ms)
{
	struct poll_step *step;
	uint64_t nanos;

	assert(count == 1U);
	assert(fds[0].fd == 17);
	assert(fds[0].events == POLLOUT);
	assert(step_index < step_count);
	step = &steps[step_index++];
	assert(timeout_ms == step->expected_ms);

	nanos = (uint64_t)mock_now.tv_nsec +
		(uint64_t)step->advance_ms * UINT64_C(1000000);
	mock_now.tv_sec += (time_t)(nanos / UINT64_C(1000000000));
	mock_now.tv_nsec = (long)(nanos % UINT64_C(1000000000));
	errno = step->error;
	return step->result;
}

static void check_consumed_at(const char *test, int line)
{
	if (step_index != step_count)
		fprintf(stderr, "%s:%d: consumed %u/%u poll steps\n",
			test, line, step_index, step_count);
	assert(step_index == step_count);
}

#define check_consumed() check_consumed_at(__func__, __LINE__)

static void test_zero_and_infinite(void)
{
	struct tr_deadline deadline;
	int expired;

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 0) == TR_OK);
	assert(tr_deadline_expired(&deadline, &expired) == TR_OK);
	assert(expired == 1);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) ==
	       TR_ERR_TIMEOUT);
	add_step(0, 1, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 1) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 0) == TR_OK);
	add_step(0, 0, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 1) ==
	       TR_ERR_TIMEOUT);
	check_consumed();

	set_now(100, 0);
	tr_deadline_init_infinite(&deadline);
	add_step(-1, 1, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();
}

static void test_eintr_and_early_return(void)
{
	struct tr_deadline deadline;

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 100) == TR_OK);
	add_step(100, -1, EINTR, 25);
	add_step(75, -1, EINTR, 20);
	add_step(55, 1, 0, 5);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 8) == TR_OK);
	add_step(8, 0, 0, 2);
	add_step(6, 1, 0, 1);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 10) == TR_OK);
	add_step(10, 1, 0, 11);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) ==
	       TR_ERR_TIMEOUT);
	check_consumed();
}

/*
 * 用两个等待模拟 TCP → 握手 / TCP → 前导数据：第二阶段只能看到剩余预算，
 * 不能重新使用完整 100ms。
 */
static void test_shared_deadline_stages(void)
{
	struct tr_deadline deadline;

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 100) == TR_OK);
	add_step(100, 1, 0, 80);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	add_step(20, 1, 0, 5);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 100) == TR_OK);
	add_step(100, 1, 0, 80);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	add_step(20, 0, 0, 20);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) ==
	       TR_ERR_TIMEOUT);
	check_consumed();
}

static void test_large_timeouts(void)
{
	struct tr_deadline deadline;
	int expired;

	set_now(100, 1);
	assert(tr_deadline_init_ms(&deadline, 1) == TR_OK);
	add_step(1, 1, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, INT_MAX) == TR_OK);
	add_step(INT_MAX, 1, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, (uint32_t)INT_MAX + 1U) == TR_OK);
	add_step(INT_MAX, 1, 0, 0);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) == TR_OK);
	check_consumed();

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, UINT32_MAX) == TR_OK);
	add_step(INT_MAX, 0, 0, INT_MAX);
	add_step(INT_MAX, 0, 0, INT_MAX);
	add_step(1, 0, 0, 1);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) ==
	       TR_ERR_TIMEOUT);
	check_consumed();
	assert(tr_deadline_expired(&deadline, &expired) == TR_OK);
	assert(expired == 1);
}

static void test_saturation_and_failure(void)
{
	struct tr_deadline deadline;
	uintmax_t seconds_max;
	unsigned int bits = sizeof(time_t) * CHAR_BIT;
	unsigned int max_bits = sizeof(uintmax_t) * CHAR_BIT;
	int expired;

	if (bits >= max_bits)
		seconds_max = (time_t)-1 < (time_t)0 ?
				      UINTMAX_MAX >> 1 : UINTMAX_MAX;
	else
		seconds_max = (time_t)-1 < (time_t)0 ?
				      (UINTMAX_C(1) << (bits - 1U)) - 1U :
				      (UINTMAX_C(1) << bits) - 1U;

	set_now((time_t)(seconds_max - 1U), 990000000L);
	assert(tr_deadline_init_ms(&deadline, UINT32_MAX) == TR_OK);
	assert((uintmax_t)deadline.absolute.tv_sec == seconds_max);
	assert(deadline.absolute.tv_nsec == 999999999L);
	assert(tr_deadline_expired(&deadline, &expired) == TR_OK);
	assert(expired == 0);

	set_now(100, 0);
	clock_fails = 1;
	assert(tr_deadline_init_ms(&deadline, 1) == TR_ERR_SYS);
	clock_fails = 0;
	assert(tr_deadline_init_ms(&deadline, 1) == TR_OK);
	clock_fails = 1;
	assert(tr_deadline_expired(&deadline, &expired) == TR_ERR_SYS);
}

static void test_poll_error(void)
{
	struct tr_deadline deadline;

	set_now(100, 0);
	assert(tr_deadline_init_ms(&deadline, 100) == TR_OK);
	add_step(100, -1, EIO, 1);
	assert(tr_deadline_poll_fd(17, POLLOUT, &deadline, 0) ==
	       TR_ERR_SYS);
	check_consumed();
}

int main(void)
{
	test_zero_and_infinite();
	test_eintr_and_early_return();
	test_shared_deadline_stages();
	test_large_timeouts();
	test_saturation_and_failure();
	test_poll_error();
	puts("deadline tests passed");
	return 0;
}
