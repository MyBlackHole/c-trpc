#ifndef TR_MAINTENANCE_H
#define TR_MAINTENANCE_H

#include <stdint.h>

struct tr_maintenance_scheduler;

struct tr_maintenance_handle {
	struct tr_maintenance_scheduler *scheduler;
	uint32_t slot;
	uint32_t generation;
};

/*
 * callback 在 scheduler thread 上执行，不能长时间阻塞。
 * 返回下一次绝对 CLOCK_MONOTONIC 时间（ns）；返回 0 表示暂时 disarm。
 */
typedef uint64_t (*tr_maintenance_cb)(void *arg, uint64_t now_ns);

int tr_maintenance_scheduler_create(uint32_t capacity,
				    struct tr_maintenance_scheduler **out);
void tr_maintenance_scheduler_destroy(struct tr_maintenance_scheduler *scheduler);

int tr_maintenance_register(struct tr_maintenance_scheduler *scheduler,
			    tr_maintenance_cb callback, void *arg,
			    struct tr_maintenance_handle *out);
int tr_maintenance_arm(struct tr_maintenance_handle handle,
		       uint64_t deadline_ns);
int tr_maintenance_unregister(struct tr_maintenance_handle handle);

uint64_t tr_maintenance_now_ns(void);

#endif
