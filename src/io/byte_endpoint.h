#ifndef TR_BYTE_ENDPOINT_H
#define TR_BYTE_ENDPOINT_H

#include <stddef.h>
#include <stdint.h>

#include "../../include/tr/status.h"

#ifdef __cplusplus
extern "C" {
#endif

enum tr_byte_endpoint_mode {
	TR_BYTE_ENDPOINT_PLAIN = 1,
	TR_BYTE_ENDPOINT_TLS_REQUIRED = 2
};

enum tr_byte_endpoint_state {
	TR_BYTE_ENDPOINT_UNINITIALIZED = 0,
	TR_BYTE_ENDPOINT_TCP_READY,
	TR_BYTE_ENDPOINT_NEGOTIATING,
	TR_BYTE_ENDPOINT_AUTHENTICATED,
	TR_BYTE_ENDPOINT_CLOSING,
	TR_BYTE_ENDPOINT_DETACHED,
	TR_BYTE_ENDPOINT_RELEASED
};

enum tr_byte_endpoint_event {
	TR_BYTE_ENDPOINT_EVENT_READ = 1U,
	TR_BYTE_ENDPOINT_EVENT_WRITE = 2U
};

/* TLS 驱动须由连接所属 Reactor 所有者串行调用。TR_AGAIN 必须给出
 * 非零等待方向；失败须返回负错误码。read 仅以 TR_ERR_CLOSED 表示收到
 * 对端 close_notify；shutdown 只负责尝试回送 close_notify。
 */
struct tr_byte_endpoint_driver {
	int (*handshake)(void *arg, uint32_t *wait_events);
	int (*read)(void *arg, void *buffer, size_t capacity,
	            size_t *read_bytes, uint32_t *wait_events);
	int (*write)(void *arg, const void *buffer, size_t length,
	             size_t *written_bytes, uint32_t *wait_events);
	int (*shutdown)(void *arg, uint32_t *wait_events);
	void (*release)(void *arg);
};

/* fd 由 Reactor 连接持有；此结构只借用它，不关闭 fd。
 * TR_AGAIN 读写时调用者须保持原缓冲区与长度，直至重试完成或关闭。
 * TLS read 返回 TR_ERR_CLOSED 后，对端已发 close_notify；调用者可以排空
 * 本端待发数据，再用 begin_close 提供绝对关闭期限并回送 close_notify。
 */
struct tr_byte_endpoint {
	int fd;
	enum tr_byte_endpoint_mode mode;
	enum tr_byte_endpoint_state state;
	uint64_t handshake_deadline_ns;
	uint64_t shutdown_deadline_ns;
	uint32_t handshake_wait;
	uint32_t read_wait;
	uint32_t write_wait;
	uint32_t shutdown_wait;
	const struct tr_byte_endpoint_driver *driver;
	void *driver_arg;
	const void *write_buffer;
	size_t write_length;
	void *read_buffer;
	size_t read_capacity;
	int terminal_status;
	int events_bound;
	int shutdown_done;
	int peer_closed;
};

int tr_byte_endpoint_init(struct tr_byte_endpoint *endpoint,
                          enum tr_byte_endpoint_mode mode, int fd,
                          uint64_t handshake_deadline_ns,
                          const struct tr_byte_endpoint_driver *driver,
                          void *driver_arg);
/* 成功注册 epoll 事件源后调用；握手、I/O 和关闭推进都要求已绑定。 */
int tr_byte_endpoint_event_bind(struct tr_byte_endpoint *endpoint);
int tr_byte_endpoint_step(struct tr_byte_endpoint *endpoint,
                          uint32_t ready_events, uint64_t now_ns);
int tr_byte_endpoint_read(struct tr_byte_endpoint *endpoint, void *buffer,
                          size_t capacity, uint32_t ready_events,
                          size_t *read_bytes, uint32_t *wait_events);
int tr_byte_endpoint_write(struct tr_byte_endpoint *endpoint,
                           const void *buffer, size_t length,
                           uint32_t ready_events, size_t *written_bytes,
                           uint32_t *wait_events);
uint32_t tr_byte_endpoint_interest(const struct tr_byte_endpoint *endpoint);
int tr_byte_endpoint_begin_close(struct tr_byte_endpoint *endpoint,
                                 int terminal_status,
                                 uint64_t shutdown_deadline_ns);
int tr_byte_endpoint_abort(struct tr_byte_endpoint *endpoint,
                           int terminal_status);
/* close_notify 完成或 abort 后，外部注销事件并等待回调静默，再调用。
 * 关闭仍等待事件时返回 TR_AGAIN；调用方须继续保持事件监听。
 */
int tr_byte_endpoint_event_unbind(struct tr_byte_endpoint *endpoint);
/* 仅在 DETACHED 后释放后端；此接口不关闭借用的 fd。 */
int tr_byte_endpoint_release(struct tr_byte_endpoint *endpoint);

#ifdef __cplusplus
}
#endif

#endif
