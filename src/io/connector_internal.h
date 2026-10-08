#ifndef TR_CONNECTOR_INTERNAL_H
#define TR_CONNECTOR_INTERNAL_H

#include <stdint.h>

struct tr_reactor;
struct tr_connector;

typedef void (*tr_connector_complete_cb)(
	int status, int fd, void *arg);

struct tr_connector_config {
	struct tr_reactor *owner;
	uint32_t timeout_ms;
	int tcp_nodelay;
	tr_connector_complete_cb complete_cb;
	void *callback_arg;
};

/*
 * 由 Reactor owner 管理的非阻塞 Connector。
 *
 * start() 只启动状态机，不阻塞等待 connect/preface。
 *
 * 所有权约定：
 * - 返回 TR_OK：Connector 已接管本次 attempt，complete_cb 必定调用一次（允许在
 *   start() 返回前同步调用）；
 * - 返回错误：attempt 未被接管，不会调用 complete_cb。
 *
 * 成功完成时，complete_cb 在 owner Reactor 上收到 status=TR_OK 和一个已接管的 fd，
 * fd 所有权转移给 callback；失败时 Connector 自行关闭 fd，并以 fd=-1 回调。
 *
 * 首次在 owner 上提交的完成或取消操作决定本次终态。完成先提交时，状态码会被锁存，
 * 清理期间后续 cancel() 返回 TR_ERR_STATE，不能覆盖完成或阻止回调。取消先提交时，
 * 后续 fd/timer 事件只重试取消清理，不触发 completion callback。cancel() 返回 TR_OK
 * 表示 Connector 已不再观察任何 fd 或已 arm 的 timer；失败时所有权保持可重试。
 */
int tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out);

/*
 * 销毁 Connector。返回 TR_OK 表示所有 Reactor 回调源均已解绑且存储已释放。返回错误时
 * Connector 所有权仍归调用方；只要 owner Reactor 仍运行，就可以重试 destroy。
 */
int tr_connector_destroy(struct tr_connector *connector);

int tr_connector_start(
	struct tr_connector *connector, const char *ipv4_address, uint16_t port,
	const uint8_t *preface, uint32_t preface_len);
int tr_connector_cancel(struct tr_connector *connector);

#endif
