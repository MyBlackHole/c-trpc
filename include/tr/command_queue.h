#ifndef TR_COMMAND_QUEUE_H
#define TR_COMMAND_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum tr_command_type {
	TR_CMD_ADOPT_FD = 1,
	TR_CMD_SEND,
	TR_CMD_RESUME_RX,
	TR_CMD_CLOSE,
	TR_CMD_ABORT,
	TR_CMD_STOP
};

struct tr_tx_item;

struct tr_command {
	uint16_t type;
	uint16_t reserved;
	uint32_t slot;
	uint32_t generation;

	union {
		struct {
			int fd;
		} adopt;

		struct {
			struct tr_tx_item *item;
		} send;

		struct {
			int status;
		} abort;
	} u;
};

struct tr_command_queue {
	pthread_mutex_t lock;
	struct tr_command *items;
	uint32_t capacity;
	uint32_t head;
	uint32_t tail;
	uint32_t count;
	int wake_pending;
};

int tr_command_queue_init(struct tr_command_queue *queue, uint32_t capacity);
void tr_command_queue_destroy(struct tr_command_queue *queue);

/*
 * need_wake is set when the caller should signal the reactor eventfd.
 * TR_AGAIN means the bounded queue is full and ownership did not transfer.
 */
int tr_command_queue_push(struct tr_command_queue *queue,
			  const struct tr_command *command, int *need_wake);

/* Returns the number of commands copied to out. */
size_t tr_command_queue_pop_batch(struct tr_command_queue *queue,
				  struct tr_command *out, size_t max_commands);

#ifdef __cplusplus
}
#endif

#endif
