#ifndef CODEC_SPIKE_H
#define CODEC_SPIKE_H
#include <stddef.h>
#include <stdint.h>
/* 仅用于候选比较；不是 CTM1、SBE 或公开 SDK 的实现。 */
#define SPIKE_DATA_MAX 65536U
#define SPIKE_FRAME_MAX (SPIKE_DATA_MAX + 512U)
enum format { FORMAT_COMPACT, FORMAT_BLOCK };
enum outcome { OUT_OK, OUT_INVALID, OUT_LIMIT };
struct message {
	uint64_t offset;
	const uint8_t *data;
	uint32_t data_size;
	const uint8_t *label;
	uint32_t label_size;
	int label_present;
};
struct view {
	struct message message;
	const uint8_t *frame;
	size_t frame_size, offset_position, unknown_bytes;
};
enum outcome spike_encode(enum format format, unsigned version,
                          const struct message *input, uint8_t *out,
                          size_t capacity, size_t *written);
enum outcome spike_decode(enum format format, unsigned reader_version,
                          const uint8_t *frame, size_t size,
                          unsigned work_budget, struct view *out);
enum outcome spike_edit_offset(const struct view *source, uint64_t offset,
                               uint8_t *out, size_t capacity);
uint64_t spike_consume(const struct view *view);
void spike_check(void);
#endif
