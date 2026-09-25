#ifndef TR_REACTOR_INTERNAL_H
#define TR_REACTOR_INTERNAL_H

#include "tr/reactor.h"

/*
 * 仅供 c-trpc 内部模块把一小段状态提交回 Reactor owner thread。
 *
 * fn 必须是非阻塞的短任务，禁止在回调里执行磁盘、网络、KMS 等阻塞工作。
 * TR_OK 表示 arg 的生命周期责任已经转移给 fn；提交失败时仍由调用方负责。
 */
int tr_reactor_post(struct tr_reactor *reactor, void (*fn)(void *arg),
		    void *arg);

#endif
