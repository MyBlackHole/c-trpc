#define _POSIX_C_SOURCE 200809L
#include "deadline_internal.h"

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdint.h>

#include "tr/status.h"

#define TR_NSEC_PER_SEC UINT64_C(1000000000)
#define TR_NSEC_PER_MSEC UINT64_C(1000000)

/* 不假设 time_t 在 32 位目标上也有 64 位宽度。 */
static uintmax_t tr_deadline_time_t_max(void)
{
	unsigned int bits = (unsigned int)(sizeof(time_t) * CHAR_BIT);
	unsigned int max_bits = (unsigned int)(sizeof(uintmax_t) * CHAR_BIT);
	int is_signed = (time_t)-1 < (time_t)0;

	if (bits >= max_bits)
		return is_signed ? UINTMAX_MAX >> 1 : UINTMAX_MAX;
	return is_signed ? (UINTMAX_C(1) << (bits - 1U)) - 1U :
			   (UINTMAX_C(1) << bits) - 1U;
}

int tr_deadline_init_ms(struct tr_deadline *deadline, uint32_t timeout_ms)
{
	struct timespec now;
	uintmax_t seconds;
	uintmax_t max_seconds;
	uintmax_t add_seconds;
	uint64_t nanos;

	if (!deadline)
		return TR_ERR_INVALID;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
	    now.tv_sec < 0 || now.tv_nsec < 0 ||
	    now.tv_nsec >= (long)TR_NSEC_PER_SEC)
		return TR_ERR_SYS;

	max_seconds = tr_deadline_time_t_max();
	seconds = (uintmax_t)now.tv_sec;
	nanos = (uint64_t)now.tv_nsec +
		(uint64_t)(timeout_ms % 1000U) * TR_NSEC_PER_MSEC;
	add_seconds = (uintmax_t)(timeout_ms / 1000U) +
		      (uintmax_t)(nanos / TR_NSEC_PER_SEC);
	nanos %= TR_NSEC_PER_SEC;

	if (add_seconds > max_seconds - seconds) {
		deadline->absolute.tv_sec = (time_t)max_seconds;
		deadline->absolute.tv_nsec = 999999999L;
	} else {
		deadline->absolute.tv_sec = (time_t)(seconds + add_seconds);
		deadline->absolute.tv_nsec = (long)nanos;
	}
	deadline->infinite = 0;
	return TR_OK;
}

void tr_deadline_init_infinite(struct tr_deadline *deadline)
{
	if (!deadline)
		return;
	deadline->absolute.tv_sec = 0;
	deadline->absolute.tv_nsec = 0;
	deadline->infinite = 1;
}

/*
 * 向上取整剩余毫秒，且有限等待只返回 [0, INT_MAX]。
 * 先比较/借位再乘 1000，避免超大 time_t 差值算术溢出。
 */
static int tr_deadline_poll_ms(const struct tr_deadline *deadline,
			      int *wait_ms, int *expired)
{
	struct timespec now;
	uintmax_t seconds;
	uintmax_t millis;
	int64_t nanos;

	if (!deadline || !wait_ms || !expired)
		return TR_ERR_INVALID;
	*expired = 0;
	if (deadline->infinite) {
		*wait_ms = -1;
		return TR_OK;
	}
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
	    now.tv_sec < 0 || now.tv_nsec < 0 ||
	    now.tv_nsec >= (long)TR_NSEC_PER_SEC)
		return TR_ERR_SYS;
	if (now.tv_sec > deadline->absolute.tv_sec ||
	    (now.tv_sec == deadline->absolute.tv_sec &&
	     now.tv_nsec >= deadline->absolute.tv_nsec)) {
		*wait_ms = 0;
		*expired = 1;
		return TR_OK;
	}
	seconds = (uintmax_t)deadline->absolute.tv_sec -
		  (uintmax_t)now.tv_sec;
	nanos = (int64_t)deadline->absolute.tv_nsec -
		(int64_t)now.tv_nsec;
	if (nanos < 0) {
		seconds--;
		nanos += (int64_t)TR_NSEC_PER_SEC;
	}
	if (seconds > (uintmax_t)INT_MAX / 1000U) {
		*wait_ms = INT_MAX;
		return TR_OK;
	}
	millis = seconds * 1000U +
		 ((uint64_t)nanos + TR_NSEC_PER_MSEC - 1U) /
			 TR_NSEC_PER_MSEC;
	*wait_ms = (int)(millis > INT_MAX ? INT_MAX : millis);
	return TR_OK;
}

int tr_deadline_expired(const struct tr_deadline *deadline, int *expired)
{
	int wait_ms;

	return tr_deadline_poll_ms(deadline, &wait_ms, expired);
}

int tr_deadline_poll_fd(int fd, short events,
			const struct tr_deadline *deadline,
			int allow_zero_probe)
{
	struct pollfd pfd;
	int first = 1;

	if (fd < 0 || !deadline)
		return TR_ERR_INVALID;

	pfd.fd = fd;
	pfd.events = events;
	for (;;) {
		int expired;
		int wait_ms;
		int ret;

		ret = tr_deadline_poll_ms(deadline, &wait_ms, &expired);
		if (ret != TR_OK)
			return ret;
		if (expired && !(first && allow_zero_probe))
			return TR_ERR_TIMEOUT;

		pfd.revents = 0;
		ret = poll(&pfd, 1, wait_ms);
		if (ret > 0) {
			/* 仅显式 0ms connect 保留一次立即就绪探测。 */
			if (first && expired && allow_zero_probe)
				return TR_OK;
			ret = tr_deadline_expired(deadline, &expired);
			if (ret != TR_OK)
				return ret;
			return expired ? TR_ERR_TIMEOUT : TR_OK;
		}
		first = 0;
		if (ret == 0 || (ret < 0 && errno == EINTR))
			continue;
		return TR_ERR_SYS;
	}
}
