#define _POSIX_C_SOURCE 200809L
#include "../execution/worker_context.h"
#include "rpc_record_producer.h"
#include "rpc_record.h"
#include "rpc_record_client.h"
#include "rpc_record_dispatch.h"
#include "rpc_v2_internal.h"
#include "../execution/deadline_internal.h"
#include "../execution/reactor_internal.h"
#include "../refcount.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct tr_rpc_record_producer_domain {
	struct tr_refcount refs;
	pthread_mutex_t lock;
	struct tr_memory_budget *upstream;
	uint64_t current_bytes;
	size_t capacity, leases;
	bool closed;
};

struct tr_producer_body {
	struct tr_buffer buffer;
	struct tr_rpc_record_producer_domain *domain;
	size_t bytes;
	uint8_t data[];
};

struct tr_rpc_v2_producer {
	struct tr_refcount refs;
	struct tr_rpc_record_producer_domain *domain;
	pthread_mutex_t lock;
	pthread_cond_t changed;
	uint32_t app_refs, max_message;
	_Atomic bool abandoned;
	struct tr_buffer *body;
	unsigned kind;
	enum tr_rpc_status status;
	enum tr_rpc_compression compression;
	uint32_t response_compression_algorithms;
	uint32_t metadata_size;
	uint8_t metadata[TR_RPC_METADATA_MAX_BYTES];
	bool active, closed, finished, initial_closed, building, in_send;
};

static void tr_producer_domain_put(struct tr_rpc_record_producer_domain *d)
{
	/* 最后一个引用只可能在宿主 close 后归还。域自身的计费已在
	 * close 中转出，剩余控制和正文也已随各自引用归还。 */
	if (tr_refcount_put(&d->refs) == 1) {
		(void)pthread_mutex_destroy(&d->lock);
		free(d);
	}
}

static int tr_producer_domain_reserve(struct tr_rpc_record_producer_domain *d,
                                      size_t bytes, bool lease)
{
	int ret;
	pthread_mutex_lock(&d->lock);
	if (d->closed) {
		ret = TR_ERR_CLOSED;
	} else if (lease && d->leases == d->capacity) {
		ret = TR_AGAIN;
	} else if (bytes > UINT64_MAX - d->current_bytes) {
		ret = TR_ERR_NOMEM;
	} else {
		ret = tr_refcount_get(&d->refs);
		if (ret == TR_OK) {
			ret = tr_memory_budget_reserve(d->upstream, bytes);
			if (ret == TR_OK) {
				d->current_bytes += bytes;
				if (lease)
					++d->leases;
			} else {
				(void)tr_refcount_put(&d->refs);
				ret = TR_ERR_NOMEM;
			}
		}
	}
	pthread_mutex_unlock(&d->lock);
	return ret;
}

static void tr_producer_domain_release(struct tr_rpc_record_producer_domain *d,
                                       size_t bytes, bool lease)
{
	pthread_mutex_lock(&d->lock);
	/* 不能在锁外缓存 upstream：Server 可同时将全部剩余计费转出。
	 * close 不清零本地 current，迟到的复制/TX/句柄仍须各归还一次。 */
	if (d->upstream)
		(void)tr_memory_budget_release(d->upstream, bytes);
	d->current_bytes -= bytes;
	if (lease)
		--d->leases;
	pthread_mutex_unlock(&d->lock);
	tr_producer_domain_put(d);
}

int tr_rpc_record_producer_domain_create(
        size_t capacity, struct tr_memory_budget *upstream,
        struct tr_rpc_record_producer_domain **out)
{
	struct tr_rpc_record_producer_domain *d;
	int ret;
	if (!capacity || capacity >= UINT32_MAX - 1U || !upstream ||
	    !upstream->limit_bytes || !out)
		return TR_ERR_INVALID;
	if (*out)
		return TR_ERR_STATE;
	ret = tr_memory_budget_reserve(upstream, sizeof(*d));
	if (ret != TR_OK)
		return ret;
	d = calloc(1, sizeof(*d));
	if (!d) {
		(void)tr_memory_budget_release(upstream, sizeof(*d));
		return TR_ERR_NOMEM;
	}
	if (pthread_mutex_init(&d->lock, NULL) != 0) {
		free(d);
		(void)tr_memory_budget_release(upstream, sizeof(*d));
		return TR_ERR_SYS;
	}
	(void)tr_refcount_init(&d->refs, 1);
	d->upstream = upstream;
	d->current_bytes = sizeof(*d);
	d->capacity = capacity;
	*out = d;
	return TR_OK;
}

int tr_rpc_record_producer_domain_close(
        struct tr_rpc_record_producer_domain **domain)
{
	struct tr_rpc_record_producer_domain *d;
	int ret = TR_OK;
	if (!domain)
		return TR_ERR_INVALID;
	d = *domain;
	if (!d)
		return TR_OK;
	pthread_mutex_lock(&d->lock);
	if (!d->closed) {
		ret = tr_memory_budget_release(d->upstream, d->current_bytes);
		if (ret == TR_OK) {
			d->closed = true;
			d->upstream = NULL;
		}
	}
	pthread_mutex_unlock(&d->lock);
	if (ret != TR_OK)
		return ret;
	*domain = NULL;
	tr_producer_domain_put(d);
	return TR_OK;
}

static void tr_producer_body_release(struct tr_buffer *buffer)
{
	struct tr_producer_body *body = (struct tr_producer_body *)buffer;
	struct tr_rpc_record_producer_domain *domain = body->domain;
	size_t bytes = body->bytes;
	free(body);
	tr_producer_domain_release(domain, bytes, false);
}

int tr_rpc_record_producer_create(struct tr_rpc_record_producer_domain *domain,
                                  uint32_t max_message, bool headers_sent,
                                  uint32_t response_compression_algorithms,
                                  struct tr_rpc_v2_producer **out)
{
	struct tr_rpc_v2_producer *p;
	pthread_condattr_t attr;
	int ret, cleanup_ret;
	if (!domain || !out)
		return TR_ERR_INVALID;
	ret = tr_producer_domain_reserve(domain, sizeof(*p), true);
	if (ret != TR_OK)
		return ret;
	p = calloc(1, sizeof(*p));
	if (!p) {
		ret = TR_ERR_NOMEM;
		goto failed;
	}
	if (pthread_mutex_init(&p->lock, NULL) != 0) {
		ret = TR_ERR_SYS;
		goto failed;
	}
	if (pthread_condattr_init(&attr) != 0) {
		ret = TR_ERR_SYS;
		goto failed_mutex;
	}
	ret = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	if (ret == 0)
		ret = pthread_cond_init(&p->changed, &attr);
	cleanup_ret = pthread_condattr_destroy(&attr);
	if (ret != 0 || cleanup_ret != 0) {
		if (ret == 0)
			(void)pthread_cond_destroy(&p->changed);
		ret = TR_ERR_SYS;
		goto failed_mutex;
	}
	(void)tr_refcount_init(&p->refs, 2);
	atomic_init(&p->abandoned, false);
	p->domain = domain;
	p->app_refs = 1;
	p->max_message = max_message;
	p->response_compression_algorithms = response_compression_algorithms;
	p->initial_closed = headers_sent;
	*out = p;
	return TR_OK;

failed_mutex:
	(void)pthread_mutex_destroy(&p->lock);
failed:
	free(p);
	tr_producer_domain_release(domain, sizeof(*p), true);
	return ret;
}

void tr_rpc_record_producer_put(struct tr_rpc_v2_producer *p)
{
	struct tr_rpc_record_producer_domain *domain;
	if (!p || tr_refcount_put(&p->refs) != 1)
		return;
	/* 库内引用只在 close 成功后归还；每个调用者都持有自己的应用
	 * 引用。因此这里既无等待者，也无锁外复制或传输借用。 */
	domain = p->domain;
	(void)pthread_cond_destroy(&p->changed);
	(void)pthread_mutex_destroy(&p->lock);
	free(p);
	tr_producer_domain_release(domain, sizeof(*p), true);
}

int tr_rpc_v2_producer_retain(struct tr_rpc_v2_producer *p)
{
	int ret;
	if (!p)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&p->lock);
	ret = tr_refcount_get(&p->refs);
	if (ret == TR_OK)
		++p->app_refs;
	pthread_mutex_unlock(&p->lock);
	return ret;
}

void tr_rpc_v2_producer_release(struct tr_rpc_v2_producer *p)
{
	if (!p)
		return;
	pthread_mutex_lock(&p->lock);
	if (--p->app_refs == 0 && !p->finished && !p->closed)
		atomic_store_explicit(&p->abandoned, true,
		                      memory_order_release);
	pthread_mutex_unlock(&p->lock);
	tr_rpc_record_producer_put(p);
}

bool tr_rpc_record_producer_abandoned(struct tr_rpc_v2_producer *p)
{
	return p && atomic_load_explicit(&p->abandoned, memory_order_acquire);
}

static int tr_producer_writable(struct tr_rpc_v2_producer *p)
{
	if (p->closed)
		return TR_ERR_CLOSED;
	if (p->finished)
		return TR_ERR_STATE;
	return p->kind || p->building || p->in_send ? TR_AGAIN : TR_OK;
}

/* BUILDING 在大正文分配/复制前占槽。所有提交共享同一状态，finish
 * 不能越过尚未发布的正文，INITIAL 也不能在首条正文复制期间插队。
 * 锁内没有 malloc、大正文 memcpy、传输调用或业务回调。 */
static int tr_producer_begin(struct tr_rpc_v2_producer *p, bool initial)
{
	int ret;
	pthread_mutex_lock(&p->lock);
	ret = tr_producer_writable(p);
	if (ret == TR_OK && initial && p->initial_closed)
		ret = TR_ERR_STATE;
	if (ret == TR_OK)
		p->building = true;
	pthread_mutex_unlock(&p->lock);
	return ret;
}

static int tr_producer_publish(struct tr_rpc_v2_producer *p, unsigned kind,
                               struct tr_buffer *body, const uint8_t *metadata,
                               uint32_t size, enum tr_rpc_status status,
                               enum tr_rpc_compression compression, int result)
{
	pthread_mutex_lock(&p->lock);
	if (p->closed)
		result = TR_ERR_CLOSED;
	if (result == TR_OK) {
		p->kind = kind;
		p->body = body;
		body = NULL;
		p->metadata_size = size;
		p->compression = compression;
		if (size)
			memcpy(p->metadata, metadata, size);
		p->status = status;
		if (kind == TR_RPC_RECORD_FINAL_STATUS)
			p->finished = true;
		else
			p->initial_closed = true;
	}
	p->building = false;
	/* 失败恢复空槽或关闭都可能兑现等待者的谓词；广播不预约容量。 */
	pthread_cond_broadcast(&p->changed);
	pthread_mutex_unlock(&p->lock);
	tr_buffer_release(body);
	return result;
}

int tr_rpc_v2_producer_send(struct tr_rpc_v2_producer *p,
                            const struct tr_rpc_bytes *message)
{
	return tr_rpc_v2_producer_send_with_compression(
	        p, message, TR_RPC_COMPRESSION_NONE);
}

int tr_rpc_v2_producer_send_with_compression(
        struct tr_rpc_v2_producer *p, const struct tr_rpc_bytes *message,
        enum tr_rpc_compression compression)
{
	struct tr_producer_body *body = NULL;
	size_t bytes, size;
	int ret;
	if (!p || !message || (message->len && !message->data))
		return TR_ERR_INVALID;
	if (compression != TR_RPC_COMPRESSION_NONE &&
	    compression != TR_RPC_COMPRESSION_ZSTD)
		return TR_ERR_INVALID;
	if (compression == TR_RPC_COMPRESSION_ZSTD &&
	    !(p->response_compression_algorithms &
	      TR_RPC_COMPRESSION_MASK_ZSTD))
		return TR_ERR_UNSUPPORTED;
	size = message->len;
	if (size > p->max_message || size > SIZE_MAX - sizeof(*body))
		return TR_ERR_BAD_LENGTH;
	ret = tr_producer_begin(p, false);
	if (ret != TR_OK)
		return ret;
	bytes = sizeof(*body) + message->len;
	ret = tr_producer_domain_reserve(p->domain, bytes, false);
	if (ret == TR_OK) {
		body = malloc(bytes);
		if (!body) {
			tr_producer_domain_release(p->domain, bytes, false);
			ret = TR_ERR_NOMEM;
		} else {
			body->domain = p->domain;
			body->bytes = bytes;
			body->buffer = (struct tr_buffer){
			        .data = body->data,
			        .capacity = message->len,
			        .len = message->len,
			        .release_cb = tr_producer_body_release};
			if (message->len)
				memcpy(body->data, message->data, message->len);
		}
	}
	return tr_producer_publish(p, TR_RPC_RECORD_MESSAGE,
	                           body ? &body->buffer : NULL, NULL, 0,
	                           TR_RPC_STATUS_OK, compression, ret);
}

static int tr_producer_metadata(struct tr_rpc_v2_producer *p, bool initial,
                                enum tr_rpc_status status,
                                const struct tr_rpc_metadata *entries,
                                size_t count)
{
	uint8_t encoded[TR_RPC_METADATA_MAX_BYTES];
	uint32_t size = 0;
	int ret;
	if (!p || status < TR_RPC_STATUS_OK ||
	    status >= (int)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	ret = tr_producer_begin(p, initial);
	if (ret != TR_OK)
		return ret;
	ret = tr_rpc_v2_metadata_encode(entries, count, encoded, &size);
	return tr_producer_publish(p,
	                           initial ? TR_RPC_RECORD_INITIAL_METADATA
	                                   : TR_RPC_RECORD_FINAL_STATUS,
	                           NULL, encoded, size, status,
	                           TR_RPC_COMPRESSION_NONE, ret);
}

int tr_rpc_v2_producer_send_initial_metadata(
        struct tr_rpc_v2_producer *p, const struct tr_rpc_metadata *entries,
        size_t count)
{
	return tr_producer_metadata(p, true, TR_RPC_STATUS_OK, entries, count);
}

int tr_rpc_v2_producer_finish(struct tr_rpc_v2_producer *p,
                              enum tr_rpc_status status,
                              const struct tr_rpc_metadata *trailing,
                              size_t count)
{
	return tr_producer_metadata(p, false, status, trailing, count);
}

int tr_rpc_v2_producer_wait_writable(struct tr_rpc_v2_producer *p,
                                     uint32_t timeout_ms)
{
	struct tr_deadline deadline;
	int ret;
	if (!p)
		return TR_ERR_INVALID;
	if (tr_reactor_in_owner_context() || tr_execution_in_worker_context() ||
	    tr_rpc_record_client_in_callback() ||
	    tr_rpc_record_dispatch_in_callback())
		return TR_ERR_STATE;
	ret = tr_deadline_init_ms(&deadline, timeout_ms);
	if (ret != TR_OK)
		return ret;
	pthread_mutex_lock(&p->lock);
	for (;;) {
		ret = tr_producer_writable(p);
		if (ret != TR_AGAIN && (ret != TR_OK || p->active))
			break;
		if (!timeout_ms) {
			ret = TR_ERR_TIMEOUT;
			break;
		}
		int waited = pthread_cond_timedwait(&p->changed, &p->lock,
		                                    &deadline.absolute);
		if (waited != 0) {
			/* 超时不重新开始总等待。关闭/可写与超时竞争时先复核
			 * 已在同锁下发布的谓词，不能遗漏最终关闭通知。 */
			ret = tr_producer_writable(p);
			if (ret == TR_AGAIN || (ret == TR_OK && !p->active))
				ret = waited == ETIMEDOUT ? TR_ERR_TIMEOUT
				                          : TR_ERR_SYS;
			break;
		}
	}
	pthread_mutex_unlock(&p->lock);
	return ret;
}

int tr_rpc_v2_producer_is_closed(struct tr_rpc_v2_producer *p)
{
	int closed;
	if (!p)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&p->lock);
	closed = p->closed;
	pthread_mutex_unlock(&p->lock);
	return closed;
}

void tr_rpc_record_producer_activate(struct tr_rpc_v2_producer *p)
{
	pthread_mutex_lock(&p->lock);
	if (!p->closed)
		p->active = true;
	pthread_cond_broadcast(&p->changed);
	pthread_mutex_unlock(&p->lock);
}

int tr_rpc_record_producer_take(struct tr_rpc_v2_producer *p,
                                struct tr_rpc_record_producer_command *out)
{
	int ret;
	if (!p || !out)
		return TR_ERR_INVALID;
	ret = pthread_mutex_trylock(&p->lock);
	if (ret != 0)
		return ret == EBUSY ? TR_AGAIN : TR_ERR_SYS;
	if (p->closed) {
		ret = TR_ERR_CLOSED;
	} else if (!p->active || !p->kind || p->building || p->in_send) {
		ret = TR_AGAIN;
	} else {
		*out = (struct tr_rpc_record_producer_command){
		        .kind = p->kind,
		        .status = p->status,
		        .compression = p->compression,
		        .body = p->body,
		        .metadata = p->metadata_size ? p->metadata : NULL,
		        .metadata_size = p->metadata_size};
		p->in_send = true;
		ret = TR_OK;
	}
	pthread_mutex_unlock(&p->lock);
	return ret;
}

void tr_rpc_record_producer_complete(struct tr_rpc_v2_producer *p,
                                     bool accepted)
{
	pthread_mutex_lock(&p->lock);
	if (accepted) {
		/* body 可以已经在 send 栈中释放，仅丢弃旧指针。 */
		p->body = NULL;
		p->kind = 0;
		p->compression = TR_RPC_COMPRESSION_NONE;
		p->metadata_size = 0;
	}
	p->in_send = false;
	if (accepted || p->closed)
		pthread_cond_broadcast(&p->changed);
	pthread_mutex_unlock(&p->lock);
}

int tr_rpc_record_producer_fail(struct tr_rpc_v2_producer *p,
                                enum tr_rpc_status status)
{
	struct tr_buffer *body;
	if (!p || status <= TR_RPC_STATUS_OK ||
	    status >= (int)TR_RPC_STATUS_COUNT)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&p->lock);
	if (p->closed || p->in_send || p->building) {
		int ret = p->closed ? TR_ERR_CLOSED : TR_AGAIN;
		pthread_mutex_unlock(&p->lock);
		return ret;
	}
	body = p->body;
	p->body = NULL;
	if (p->kind != TR_RPC_RECORD_FINAL_STATUS)
		p->metadata_size = 0;
	p->kind = TR_RPC_RECORD_FINAL_STATUS;
	p->status = status;
	p->compression = TR_RPC_COMPRESSION_NONE;
	p->finished = true;
	pthread_cond_broadcast(&p->changed);
	pthread_mutex_unlock(&p->lock);
	tr_buffer_release(body);
	return TR_OK;
}

int tr_rpc_record_producer_close(struct tr_rpc_v2_producer *p)
{
	struct tr_buffer *body;
	if (!p)
		return TR_ERR_INVALID;
	pthread_mutex_lock(&p->lock);
	p->closed = true;
	p->active = false;
	pthread_cond_broadcast(&p->changed);
	if (p->in_send) {
		pthread_mutex_unlock(&p->lock);
		return TR_AGAIN;
	}
	body = p->body;
	p->body = NULL;
	p->kind = 0;
	p->compression = TR_RPC_COMPRESSION_NONE;
	p->metadata_size = 0;
	pthread_mutex_unlock(&p->lock);
	tr_buffer_release(body);
	return TR_OK;
}
