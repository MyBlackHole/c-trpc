#include "worker_context.h"

static _Thread_local bool tr_execution_worker;

struct tr_worker_context tr_execution_enter_worker(void)
{
	struct tr_worker_context saved = {tr_execution_worker};

	tr_execution_worker = true;
	return saved;
}

void tr_execution_leave_worker(struct tr_worker_context saved)
{
	tr_execution_worker = saved.previous;
}

int tr_execution_in_worker_context(void)
{
	return tr_execution_worker;
}
