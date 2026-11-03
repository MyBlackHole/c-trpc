#ifndef TR_CONNECTOR_INTERNAL_H
#define TR_CONNECTOR_INTERNAL_H

#include <stdint.h>

struct tr_reactor;
struct tr_connector;

typedef void (*tr_connector_complete_cb)(int status, int fd,
                                         uint64_t deadline_ns, void *arg);

struct tr_connector_config {
	struct tr_reactor *owner;
	uint32_t timeout_ms;
	int tcp_nodelay;
	tr_connector_complete_cb complete_cb;
	void *callback_arg;
};

/* 由反应器所有者管理的非阻塞 Connector。
 *
 * start() 只启动状态机，不阻塞等待连接或前导。
 *
 * 所有权约定：
 * - 返回 TR_OK：Connector 已接管本次尝试，complete_cb 必定调用一次（允许在
 *   start() 返回前同步调用）；
 * - 返回错误：本次尝试未被接管，不会调用 complete_cb。
 *
 * 成功完成时，complete_cb 在所属反应器上收到 status=TR_OK 和一个已接管的 fd，
 * fd 所有权转移给回调；失败时 Connector 自行关闭 fd，并以 fd=-1 回调。
 *
 * 首次在所有者上提交的完成或取消操作决定本次终态。完成先提交时，状态码会被锁存，
 * 清理期间后续 cancel() 返回
 * TR_ERR_STATE，不能覆盖完成或阻止回调。取消先提交时， 后续 fd
 * 或定时器事件只重试取消清理，不触发完成回调。cancel() 返回 TR_OK 表示
 * Connector 已不再观察任何 fd 或已设置定时的定时器；失败时所有权保持可重试。
 */
int tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out);

/* 销毁 Connector。返回 TR_OK
 * 表示所有反应器回调源均已解绑且存储已释放。返回错误时 Connector
 * 所有权仍归调用方；只要所属反应器仍运行，就可以重试销毁。
 */
int tr_connector_destroy(struct tr_connector *connector);

int tr_connector_start(
	struct tr_connector *connector, const char *ipv4_address, uint16_t port,
	const uint8_t *preface, uint32_t preface_len);
int tr_connector_cancel(struct tr_connector *connector);

#endif
