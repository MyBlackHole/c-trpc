/* 自定义分配器和帧头检查属于静态接口，构建必须固定版本并静态链接。 */
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zstd_errors.h>

#include "transform_internal.h"
#include "tr/status.h"

#include <stdlib.h>

_Static_assert(ZSTD_VERSION_NUMBER == 10507,
               "message backend requires Zstandard 1.5.7");

#define TR_ZSTD_STEP_BYTES ((size_t)64 * 1024)

union tr_zstd_allocation {
	max_align_t alignment;
	struct {
		union tr_zstd_allocation *previous;
		union tr_zstd_allocation *next;
		size_t bytes;
	} entry;
};

struct tr_zstd_workspace {
	size_t limit;
	size_t current;
	union tr_zstd_allocation *head;
	enum tr_message_transform_outcome failure;
};

static void *tr_zstd_allocate(void *arg, size_t size)
{
	struct tr_zstd_workspace *workspace = arg;
	union tr_zstd_allocation *allocation;
	size_t bytes;

	if (size > SIZE_MAX - sizeof(*allocation)) {
		workspace->failure = TR_MESSAGE_TRANSFORM_LIMIT;
		return NULL;
	}
	bytes = sizeof(*allocation) + size;
	if (bytes > workspace->limit - workspace->current) {
		workspace->failure = TR_MESSAGE_TRANSFORM_LIMIT;
		return NULL;
	}
	allocation = malloc(bytes);
	if (!allocation) {
		workspace->failure = TR_MESSAGE_TRANSFORM_NOMEM;
		return NULL;
	}
	allocation->entry.bytes = bytes;
	allocation->entry.previous = NULL;
	allocation->entry.next = workspace->head;
	if (workspace->head)
		workspace->head->entry.previous = allocation;
	workspace->head = allocation;
	workspace->current += bytes;
	return allocation + 1;
}

static void tr_zstd_deallocate(void *arg, void *pointer)
{
	struct tr_zstd_workspace *workspace = arg;
	union tr_zstd_allocation *allocation;
	size_t bytes;

	if (!pointer)
		return;
	allocation = (union tr_zstd_allocation *)pointer - 1;
	bytes = allocation->entry.bytes;
	if (allocation->entry.previous)
		allocation->entry.previous->entry.next = allocation->entry.next;
	else
		workspace->head = allocation->entry.next;
	if (allocation->entry.next)
		allocation->entry.next->entry.previous =
		        allocation->entry.previous;
	free(allocation);
	workspace->current -= bytes;
}

static size_t tr_zstd_step_end(size_t position, size_t total)
{
	size_t remaining = total - position;

	return position + (remaining < TR_ZSTD_STEP_BYTES ? remaining
	                                                  : TR_ZSTD_STEP_BYTES);
}

int tr_message_zstd_plan(const struct tr_message_transform_config *config,
                         size_t input_size, size_t *output_capacity)
{
	ZSTD_bounds bounds;
	size_t capacity;

	if (!config || !output_capacity || config->max_workspace_bytes == 0)
		return TR_ERR_INVALID;
	if (ZSTD_versionNumber() != ZSTD_VERSION_NUMBER)
		return TR_ERR_UNSUPPORTED;
	if (config->kind == TR_MESSAGE_ZSTD_ENCODE) {
		if (config->compression_level < 1 ||
		    config->compression_level > ZSTD_maxCLevel())
			return TR_ERR_INVALID;
		bounds = ZSTD_cParam_getBounds(ZSTD_c_windowLog);
		if (input_size > config->limits.max_decoded_bytes)
			return TR_ERR_BAD_LENGTH;
		capacity = ZSTD_compressBound(input_size);
		if (ZSTD_isError(capacity))
			return TR_ERR_BAD_LENGTH;
		if (capacity > config->limits.max_encoded_bytes)
			capacity = config->limits.max_encoded_bytes;
	} else if (config->kind == TR_MESSAGE_ZSTD_DECODE) {
		bounds = ZSTD_dParam_getBounds(ZSTD_d_windowLogMax);
		if (input_size > config->limits.max_encoded_bytes ||
		    config->decoded_size > config->limits.max_decoded_bytes)
			return TR_ERR_BAD_LENGTH;
		capacity = config->decoded_size;
	} else {
		return TR_ERR_UNSUPPORTED;
	}
	if (ZSTD_isError(bounds.error) ||
	    config->max_window_log < (unsigned)bounds.lowerBound ||
	    config->max_window_log > (unsigned)bounds.upperBound ||
	    config->max_window_log >= 64)
		return TR_ERR_INVALID;
	*output_capacity = capacity;
	return TR_OK;
}

static enum tr_message_transform_outcome
tr_zstd_error(struct tr_zstd_workspace *workspace, size_t error, bool decode)
{
	if (workspace->failure != TR_MESSAGE_TRANSFORM_OK)
		return workspace->failure;
	if (ZSTD_getErrorCode(error) == ZSTD_error_dstSize_tooSmall ||
	    ZSTD_getErrorCode(error) ==
	            ZSTD_error_frameParameter_windowTooLarge)
		return TR_MESSAGE_TRANSFORM_LIMIT;
	if (ZSTD_getErrorCode(error) == ZSTD_error_memory_allocation)
		return TR_MESSAGE_TRANSFORM_NOMEM;
	return decode ? TR_MESSAGE_TRANSFORM_BAD_DATA
	              : TR_MESSAGE_TRANSFORM_INTERNAL;
}

static enum tr_message_transform_outcome
tr_zstd_encode(const struct tr_message_transform_config *config,
               const struct tr_message_view *input, uint8_t *output,
               size_t capacity, size_t *output_size,
               tr_message_transform_cancel_fn cancel, void *arg,
               struct tr_zstd_workspace *workspace, ZSTD_customMem memory)
{
	ZSTD_CCtx *context = ZSTD_createCCtx_advanced(memory);
	ZSTD_inBuffer source = {input->data, 0, 0};
	ZSTD_outBuffer destination = {output, 0, 0};
	enum tr_message_transform_outcome outcome =
	        TR_MESSAGE_TRANSFORM_INTERNAL;
	size_t ret;

	if (!context)
		return workspace->failure != TR_MESSAGE_TRANSFORM_OK
		               ? workspace->failure
		               : TR_MESSAGE_TRANSFORM_NOMEM;
	ret = ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel,
	                             config->compression_level);
	if (ZSTD_isError(ret))
		goto error;
	ret = ZSTD_CCtx_setParameter(context, ZSTD_c_windowLog,
	                             (int)config->max_window_log);
	if (ZSTD_isError(ret))
		goto error;
	ret = ZSTD_CCtx_setParameter(context, ZSTD_c_nbWorkers, 0);
	if (ZSTD_isError(ret))
		goto error;
	ret = ZSTD_CCtx_setParameter(context, ZSTD_c_checksumFlag, 1);
	if (ZSTD_isError(ret))
		goto error;
	ret = ZSTD_CCtx_setPledgedSrcSize(context, input->len);
	if (ZSTD_isError(ret))
		goto error;

	for (;;) {
		size_t previous_input = source.pos;
		size_t previous_output = destination.pos;
		ZSTD_EndDirective end;

		if (cancel(arg)) {
			outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
			break;
		}
		source.size = tr_zstd_step_end(source.pos, input->len);
		destination.size = tr_zstd_step_end(destination.pos, capacity);
		end = source.size == input->len ? ZSTD_e_end : ZSTD_e_continue;
		ret = ZSTD_compressStream2(context, &destination, &source, end);
		if (ZSTD_isError(ret))
			goto error;
		if (end == ZSTD_e_end && ret == 0) {
			if (source.pos == input->len) {
				*output_size = destination.pos;
				outcome = TR_MESSAGE_TRANSFORM_OK;
			}
			break;
		}
		if (destination.pos == capacity) {
			outcome = TR_MESSAGE_TRANSFORM_LIMIT;
			break;
		}
		if (source.pos == previous_input &&
		    destination.pos == previous_output)
			break;
	}
	(void)ZSTD_freeCCtx(context);
	return outcome;

error:
	outcome = tr_zstd_error(workspace, ret, false);
	(void)ZSTD_freeCCtx(context);
	return outcome;
}

static enum tr_message_transform_outcome
tr_zstd_decode(const struct tr_message_transform_config *config,
               const struct tr_message_view *input, uint8_t *output,
               size_t capacity, size_t *output_size,
               tr_message_transform_cancel_fn cancel, void *arg,
               struct tr_zstd_workspace *workspace, ZSTD_customMem memory)
{
	ZSTD_frameHeader header;
	ZSTD_DCtx *context;
	ZSTD_inBuffer source = {input->data, 0, 0};
	enum tr_message_transform_outcome outcome =
	        TR_MESSAGE_TRANSFORM_BAD_DATA;
	size_t produced = 0;
	size_t ret = ZSTD_getFrameHeader(&header, input->data, input->len);

	/* 单条消息恰好一个普通帧；拒绝跳过帧、字典、串接和尾随数据。
	 * 不使用帧头声明直接分配，也不把单次解码 API 的窗口参数当成门禁。
	 */
	if (ZSTD_isError(ret) || ret != 0 || header.frameType != ZSTD_frame ||
	    header.dictID != 0)
		return TR_MESSAGE_TRANSFORM_BAD_DATA;
	if (header.windowSize > (UINT64_C(1) << config->max_window_log))
		return TR_MESSAGE_TRANSFORM_LIMIT;
	if (header.frameContentSize != ZSTD_CONTENTSIZE_UNKNOWN &&
	    header.frameContentSize != config->decoded_size)
		return TR_MESSAGE_TRANSFORM_BAD_DATA;
	context = ZSTD_createDCtx_advanced(memory);
	if (!context)
		return workspace->failure != TR_MESSAGE_TRANSFORM_OK
		               ? workspace->failure
		               : TR_MESSAGE_TRANSFORM_NOMEM;
	ret = ZSTD_DCtx_setParameter(context, ZSTD_d_windowLogMax,
	                             (int)config->max_window_log);
	if (ZSTD_isError(ret))
		goto error;

	for (;;) {
		uint8_t excess;
		bool probe = produced == capacity;
		ZSTD_outBuffer destination = {
		        probe ? &excess : output,
		        probe ? 1 : tr_zstd_step_end(produced, capacity),
		        probe ? 0 : produced};
		size_t previous_input = source.pos;
		size_t previous_output = destination.pos;

		if (cancel(arg)) {
			outcome = TR_MESSAGE_TRANSFORM_CANCELLED;
			break;
		}
		source.size = tr_zstd_step_end(source.pos, input->len);
		ret = ZSTD_decompressStream(context, &destination, &source);
		if (ZSTD_isError(ret))
			goto error;
		/* 输出恰满后仍需消费帧尾/校验和；一个字节探针区分合法帧尾
		 * 与真实超长，不写出界，也不接受被截断的输出。
		 */
		if (probe && destination.pos != 0) {
			outcome = TR_MESSAGE_TRANSFORM_LIMIT;
			break;
		}
		if (!probe)
			produced = destination.pos;
		if (ret == 0) {
			if (source.pos == input->len && produced == capacity) {
				*output_size = produced;
				outcome = TR_MESSAGE_TRANSFORM_OK;
			}
			break;
		}
		if (source.pos == previous_input &&
		    destination.pos == previous_output)
			break;
	}
	(void)ZSTD_freeDCtx(context);
	return outcome;

error:
	outcome = tr_zstd_error(workspace, ret, true);
	(void)ZSTD_freeDCtx(context);
	return outcome;
}

enum tr_message_transform_outcome
tr_message_zstd_run(const struct tr_message_transform_config *config,
                    const struct tr_message_view *input, uint8_t *output,
                    size_t capacity, size_t *output_size,
                    tr_message_transform_cancel_fn cancel, void *arg)
{
	struct tr_zstd_workspace workspace = {
	        .limit = config->max_workspace_bytes,
	        .failure = TR_MESSAGE_TRANSFORM_OK,
	};
	ZSTD_customMem memory = {tr_zstd_allocate, tr_zstd_deallocate,
	                         &workspace};
	enum tr_message_transform_outcome outcome;

	if (cancel(arg))
		return TR_MESSAGE_TRANSFORM_CANCELLED;
	if (config->kind == TR_MESSAGE_ZSTD_ENCODE)
		outcome = tr_zstd_encode(config, input, output, capacity,
		                         output_size, cancel, arg, &workspace,
		                         memory);
	else
		outcome = tr_zstd_decode(config, input, output, capacity,
		                         output_size, cancel, arg, &workspace,
		                         memory);

	/* 上下文销毁后不得再有后端使用者。保留分配链以便在库异常遗漏
	 * 回收时仍能实际释放，再报告内部错误，绝不带着泄漏归还工作区额度。
	 */
	if (workspace.head || workspace.current != 0) {
		outcome = TR_MESSAGE_TRANSFORM_INTERNAL;
		while (workspace.head) {
			union tr_zstd_allocation *allocation = workspace.head;

			workspace.head = allocation->entry.next;
			free(allocation);
		}
	}
	return outcome;
}
