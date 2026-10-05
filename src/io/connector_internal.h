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
 * 由 Reactor 拥有的非阻塞连接器。
 *
 * start() 只启动状态机，不阻塞等待 connect/preface。
 *
 * 所有权契约：
 * - TR_OK：connector 已接管本次 attempt，complete_cb 必定调用一次（允许在
 *   start() 返回前同步调用）；
 * - error：attempt 未被接管，不会调用 complete_cb。
 *
 * 成功完成时 complete_cb 在 owner Reactor 上收到 status=TR_OK 和一个 owned fd，
 * 此时 fd ownership 转移给 callback；失败时 connector 自行 close fd，并以 fd=-1
 * 回调。
 *
 * cancel() 不触发 completion callback；返回后 connector 已不再观察任何 fd/timer。
 */
int tr_connector_create(
	const struct tr_connector_config *config,
	struct tr_connector **out);
void tr_connector_destroy(struct tr_connector *connector);

int tr_connector_start(
	struct tr_connector *connector, const char *ipv4_address, uint16_t port,
	const uint8_t *preface, uint32_t preface_len);
int tr_connector_cancel(struct tr_connector *connector);

#endif
