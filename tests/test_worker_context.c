#define _POSIX_C_SOURCE 200809L
#include "../src/execution/worker_context.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(x)                                                               \
	do {                                                                   \
		if (!(x)) {                                                    \
			fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);  \
			abort();                                               \
		}                                                              \
	} while (0)

#define THREADS 8U
#define ROUNDS 2000U

static pthread_barrier_t barrier;

static void meet(void)
{
	int ret = pthread_barrier_wait(&barrier);

	CHECK(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD);
}

static void *check_thread(void *arg)
{
	uintptr_t index = (uintptr_t)arg;

	CHECK(!tr_execution_in_worker_context());
	for (unsigned i = 0; i < ROUNDS; ++i) {
		struct tr_worker_context outer = {false};
		struct tr_worker_context inner, nested;
		int marked = (index & 1U) == 0;

		if (marked)
			outer = tr_execution_enter_worker();
		meet();
		/* 另一组线程保持外部身份；全局布尔值不能满足此同时观测。 */
		CHECK(tr_execution_in_worker_context() == marked);
		inner = tr_execution_enter_worker();
		nested = tr_execution_enter_worker();
		CHECK(tr_execution_in_worker_context());
		tr_execution_leave_worker(nested);
		CHECK(tr_execution_in_worker_context());
		tr_execution_leave_worker(inner);
		CHECK(tr_execution_in_worker_context() == marked);
		if (marked)
			tr_execution_leave_worker(outer);
		CHECK(!tr_execution_in_worker_context());
		meet();
	}
	return NULL;
}

int main(void)
{
	pthread_t threads[THREADS];

	CHECK(!tr_execution_in_worker_context());
	CHECK(pthread_barrier_init(&barrier, NULL, THREADS + 1U) == 0);
	for (uintptr_t i = 0; i < THREADS; ++i)
		CHECK(pthread_create(&threads[i], NULL, check_thread,
		                     (void *)i) == 0);
	for (unsigned i = 0; i < ROUNDS; ++i) {
		meet();
		CHECK(!tr_execution_in_worker_context());
		meet();
	}
	for (unsigned i = 0; i < THREADS; ++i)
		CHECK(pthread_join(threads[i], NULL) == 0);
	CHECK(pthread_barrier_destroy(&barrier) == 0);
	CHECK(!tr_execution_in_worker_context());
	puts("worker_context: ok");
	return 0;
}
