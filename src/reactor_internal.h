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

/*
 * 在 Reactor owner thread 执行一个短小、非阻塞的同步操作，并把 fn 的
 * 返回值传回调用方。owner thread 内调用时直接执行，避免自等待。
 *
 * arg 只在本函数返回前被访问，因此调用方可以安全传递栈上 request。
 */
int tr_reactor_call(struct tr_reactor *reactor, int (*fn)(void *arg),
		    void *arg);

#endif
