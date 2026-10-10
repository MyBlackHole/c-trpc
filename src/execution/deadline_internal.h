#ifndef TR_DEADLINE_INTERNAL_H
#define TR_DEADLINE_INTERNAL_H

#include <stdint.h>
#include <time.h>

/*
 * 内部绝对截止时间。有限值包括 0ms 立即到期；无限等待必须显式声明，
 * 避免不同公开 API 的 timeout_ms == 0 语义互相污染。
 */
struct tr_deadline {
	struct timespec absolute;
	int infinite;
};

int tr_deadline_init_ms(struct tr_deadline *deadline, uint32_t timeout_ms);
void tr_deadline_init_infinite(struct tr_deadline *deadline);

/* 有限截止时间到期返回 *expired=1；clock_gettime 失败返回 TR_ERR_SYS。 */
int tr_deadline_expired(const struct tr_deadline *deadline, int *expired);

/*
 * 使用同一个绝对截止时间循环处理 EINTR、提前 poll(0) 和 INT_MAX 分段。
 * allow_zero_probe 仅用于原本超时为 0 的非阻塞 connect：允许一次 poll(0)。
 * 返回 TR_OK 表示就绪，TR_ERR_TIMEOUT 表示总期限耗尽。
 */
int tr_deadline_poll_fd(int fd, short events,
			const struct tr_deadline *deadline,
			int allow_zero_probe);

#endif
