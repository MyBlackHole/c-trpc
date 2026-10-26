#include "../src/transport/channel/channel.h"
#include "tr/status.h"

#include <stdbool.h>
#include <stdatomic.h>

static _Atomic bool tr_test_fail_stream_open;
static _Atomic unsigned tr_test_stream_open_failures;

void tr_rpc_v2_test_fail_next_stream_open(void)
{
	atomic_store_explicit(&tr_test_fail_stream_open, true,
	                      memory_order_release);
}

unsigned tr_rpc_v2_test_stream_open_failures(void)
{
	return atomic_load_explicit(&tr_test_stream_open_failures,
	                            memory_order_acquire);
}

int __real_tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
                          struct tr_stream_handle *out);

int __wrap_tr_stream_open(struct tr_channel *channel, enum tr_lane lane,
                          struct tr_stream_handle *out)
{
	if (atomic_exchange_explicit(&tr_test_fail_stream_open, false,
	                             memory_order_acq_rel)) {
		(void)atomic_fetch_add_explicit(&tr_test_stream_open_failures,
		                                1, memory_order_relaxed);
		return TR_ERR_NOMEM;
	}
	return __real_tr_stream_open(channel, lane, out);
}
