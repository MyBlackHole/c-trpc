#ifndef TR_RPC_RECORD_TRANSPORT_INTERNAL_H
#define TR_RPC_RECORD_TRANSPORT_INTERNAL_H

#include "rpc_record.h"
#include "../transport/channel/channel.h"
#include "tr/status.h"

/* RESET 和未受理拒绝只携带传输原因。映射属于 RPC 适配边界；
 * 已被记录状态机接纳的 FINAL 不受随后传输终止影响。 */
static inline enum tr_rpc_status tr_rpc_record_status_from_reset(int reason)
{
	switch (reason) {
	case TR_ERR_CLOSED:
		return TR_RPC_STATUS_CANCELLED;
	case TR_ERR_TIMEOUT:
		return TR_RPC_STATUS_DEADLINE_EXCEEDED;
	case TR_ERR_NOMEM:
		return TR_RPC_STATUS_RESOURCE_EXHAUSTED;
	case TR_ERR_STATE:
		return TR_RPC_STATUS_INVALID_ARGUMENT;
	case TR_ERR_SYS:
		return TR_RPC_STATUS_INTERNAL;
	default:
		return (enum tr_rpc_status)TR_RPC_STATUS_COUNT;
	}
}

/* 没有被接受的错误 FINAL 不能保证其详细业务状态送达。RESET 使用有限
 * 传输原因完成停止；具体业务状态仍只由合法 FINAL 表达。 */
static inline int tr_rpc_record_reset_reason(enum tr_rpc_status status)
{
	switch (status) {
	case TR_RPC_STATUS_CANCELLED:
	case TR_RPC_STATUS_UNAVAILABLE:
		return TR_ERR_CLOSED;
	case TR_RPC_STATUS_DEADLINE_EXCEEDED:
		return TR_ERR_TIMEOUT;
	case TR_RPC_STATUS_RESOURCE_EXHAUSTED:
		return TR_ERR_NOMEM;
	case TR_RPC_STATUS_INVALID_ARGUMENT:
	case TR_RPC_STATUS_OUT_OF_RANGE:
	case TR_RPC_STATUS_DATA_LOSS:
		return TR_ERR_STATE;
	default:
		return TR_ERR_SYS;
	}
}

static inline int tr_rpc_record_is_terminal_event(enum tr_stream_event event)
{
	return event == TR_STREAM_EVENT_REMOTE_CLOSED ||
	       event == TR_STREAM_EVENT_CLOSED ||
	       event == TR_STREAM_EVENT_ERROR ||
	       event == TR_STREAM_EVENT_RESET ||
	       event == TR_STREAM_EVENT_REFUSED;
}

#endif
