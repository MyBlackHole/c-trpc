#include "codec.h"
#include <assert.h>
#include <string.h>

static void put32(uint8_t *p, uint32_t v)
{
	for (unsigned i = 0; i < 4; ++i)
		p[i] = (uint8_t)(v >> (8U * i));
}
static uint32_t get32(const uint8_t *p)
{
	uint32_t v = 0;
	for (unsigned i = 0; i < 4; ++i)
		v |= (uint32_t)p[i] << (8U * i);
	return v;
}
static void put64(uint8_t *p, uint64_t v)
{
	for (unsigned i = 0; i < 8; ++i)
		p[i] = (uint8_t)(v >> (8U * i));
}
static uint64_t get64(const uint8_t *p)
{
	uint64_t v = 0;
	for (unsigned i = 0; i < 8; ++i)
		v |= (uint64_t)p[i] << (8U * i);
	return v;
}
static size_t var_size(uint32_t v)
{
	size_t n = 1;
	while (v >= 128U) {
		++n;
		v >>= 7;
	}
	return n;
}
static uint8_t *put_var(uint8_t *p, uint32_t v)
{
	while (v >= 128U) {
		*p++ = (uint8_t)(v | 128U);
		v >>= 7;
	}
	*p++ = (uint8_t)v;
	return p;
}
static int get_var(const uint8_t **cursor, const uint8_t *end, uint32_t *value)
{
	uint32_t v = 0;
	for (unsigned i = 0; i < 5U; ++i) {
		if (*cursor == end)
			return 0;
		uint8_t byte = *(*cursor)++;
		if (i == 4U && (byte & 0xf0U))
			return 0;
		v |= (uint32_t)(byte & 127U) << (7U * i);
		if (!(byte & 128U)) {
			if (i && !byte)
				return 0;
			*value = v;
			return 1;
		}
	}
	return 0;
}
enum outcome spike_encode(enum format format, unsigned version,
                          const struct message *m, uint8_t *out,
                          size_t capacity, size_t *written)
{
	if (!m || !out || !written || (version != 1U && version != 2U) ||
	    (format != FORMAT_COMPACT && format != FORMAT_BLOCK))
		return OUT_INVALID;
	if (m->data_size > SPIKE_DATA_MAX || m->label_size > 128U)
		return OUT_LIMIT;
	if ((m->data_size && !m->data) || (m->label_size && !m->label))
		return OUT_INVALID;
	size_t body = format == FORMAT_COMPACT
	                      ? 10U + 1U + var_size(m->data_size) + m->data_size
	                      : 4U + 8U + 4U + m->data_size;
	if (version == 2U) {
		if (format == FORMAT_BLOCK)
			body += 1U;
		if (m->label_present)
			body += (format == FORMAT_COMPACT
			                 ? 1U + var_size(m->label_size)
			                 : 4U) +
			        m->label_size;
	}
	if (body + 4U > capacity)
		return OUT_LIMIT;
	put32(out, (uint32_t)body);
	uint8_t *p = out + 4U;
	if (format == FORMAT_COMPACT) {
		*p++ = 0x25;
		*p++ = 8U;
		put64(p, m->offset);
		p += 8U;
		*p++ = 0x4c;
		p = put_var(p, m->data_size);
		if (m->data_size)
			memcpy(p, m->data, m->data_size);
		p += m->data_size;
		if (version == 2U && m->label_present) {
			*p++ = 0x6d;
			p = put_var(p, m->label_size);
			if (m->label_size)
				memcpy(p, m->label, m->label_size);
			p += m->label_size;
		}
	} else {
		*p++ = 8U;
		*p++ = 0U;
		*p++ = (uint8_t)version;
		*p++ = 0U;
		put64(p, m->offset);
		p += 8U;
		put32(p, m->data_size);
		p += 4U;
		if (m->data_size)
			memcpy(p, m->data, m->data_size);
		p += m->data_size;
		if (version == 2U) {
			*p++ = m->label_present ? 1U : 0U;
			if (m->label_present) {
				put32(p, m->label_size);
				p += 4U;
				if (m->label_size)
					memcpy(p, m->label, m->label_size);
				p += m->label_size;
			}
		}
	}
	assert((size_t)(p - out) == body + 4U);
	*written = body + 4U;
	return OUT_OK;
}
enum outcome spike_decode(enum format format, unsigned reader_version,
                          const uint8_t *frame, size_t size,
                          unsigned work_budget, struct view *out)
{
	if (!frame || !out || size < 4U || size > SPIKE_FRAME_MAX ||
	    (format != FORMAT_COMPACT && format != FORMAT_BLOCK) ||
	    (reader_version != 1U && reader_version != 2U))
		return OUT_INVALID;
	if (get32(frame) != size - 4U)
		return OUT_INVALID;
	struct view v = {0};
	v.frame = frame;
	v.frame_size = size;
	const uint8_t *p = frame + 4U, *end = frame + size;
	if (format == FORMAT_COMPACT) {
		unsigned prior = 0, have = 0;
		while (p != end) {
			if (!work_budget--)
				return OUT_LIMIT;
			const uint8_t *record_begin = p;
			uint32_t tag, length;
			if (!get_var(&p, end, &tag) ||
			    !get_var(&p, end, &length) ||
			    length > (size_t)(end - p))
				return OUT_INVALID;
			unsigned id = tag >> 5U, kind = tag & 31U;
			if (!id || id <= prior || !kind)
				return OUT_INVALID;
			prior = id;
			if (id == 1U) {
				if (kind != 5U || length != 8U)
					return OUT_INVALID;
				v.message.offset = get64(p);
				v.offset_position = (size_t)(p - frame);
				have |= 1U;
			} else if (id == 2U) {
				if (kind != 12U)
					return OUT_INVALID;
				if (length > SPIKE_DATA_MAX)
					return OUT_LIMIT;
				v.message.data = p;
				v.message.data_size = length;
				have |= 2U;
			} else if (id == 3U && reader_version == 2U) {
				if (kind != 13U)
					return OUT_INVALID;
				if (length > 128U)
					return OUT_LIMIT;
				v.message.label = p;
				v.message.label_size = length;
				v.message.label_present = 1;
			} else {
				v.unknown_bytes +=
				        (size_t)(p - record_begin) + length;
				if (v.unknown_bytes > 256U)
					return OUT_LIMIT;
			}
			p += length;
		}
		/* 同一业务样例均要求 offset 和 data；不是通用 wire-required
		 * 规则。 */
		if (have != 3U)
			return OUT_INVALID;
	} else {
		if ((size_t)(end - p) < 4U)
			return OUT_INVALID;
		unsigned block = p[0] | ((unsigned)p[1] << 8U);
		unsigned version = p[2] | ((unsigned)p[3] << 8U);
		p += 4U;
		if (!version || block < 8U || block > (size_t)(end - p))
			return OUT_INVALID;
		if (work_budget < 2U)
			return OUT_LIMIT;
		work_budget -= 2U;
		v.offset_position = (size_t)(p - frame);
		v.message.offset = get64(p);
		p += block;
		if ((size_t)(end - p) < 4U)
			return OUT_INVALID;
		uint32_t data_size = get32(p);
		p += 4U;
		if (data_size > (size_t)(end - p))
			return OUT_INVALID;
		if (data_size > SPIKE_DATA_MAX)
			return OUT_LIMIT;
		v.message.data = p;
		v.message.data_size = data_size;
		p += data_size;
		if (reader_version == 2U && version >= 2U) {
			if (!work_budget--)
				return OUT_LIMIT;
			if (p == end || *p > 1U)
				return OUT_INVALID;
			v.message.label_present = *p++;
			if (v.message.label_present) {
				if ((size_t)(end - p) < 4U)
					return OUT_INVALID;
				uint32_t label_size = get32(p);
				p += 4U;
				if (label_size > (size_t)(end - p))
					return OUT_INVALID;
				if (label_size > 128U)
					return OUT_LIMIT;
				v.message.label = p;
				v.message.label_size = label_size;
				p += label_size;
			}
		}
		v.unknown_bytes = (size_t)(end - p) + (block - 8U);
		if (v.unknown_bytes > 256U)
			return OUT_LIMIT;
	}
	/* 两候选相同：已知 label 验证为 ASCII 子集，其他
	 * UTF-8/类型不在本探针范围。 */
	if (v.message.label_present)
		for (uint32_t i = 0; i < v.message.label_size; ++i)
			if (v.message.label[i] >= 128U)
				return OUT_INVALID;
	*out = v;
	return OUT_OK;
}
enum outcome spike_edit_offset(const struct view *source, uint64_t offset,
                               uint8_t *out, size_t capacity)
{
	if (!source || !out || source->offset_position > source->frame_size ||
	    source->frame_size - source->offset_position < 8U)
		return OUT_INVALID;
	if (source->frame_size > capacity)
		return OUT_LIMIT;
	memcpy(out, source->frame, source->frame_size);
	put64(out + source->offset_position, offset);
	return OUT_OK;
}
uint64_t spike_consume(const struct view *v)
{
	uint64_t value = v->message.offset;
	for (uint32_t i = 0; i < v->message.data_size; ++i)
		value += v->message.data[i];
	for (uint32_t i = 0; i < v->message.label_size; ++i)
		value += v->message.label[i];
	return value;
}
void spike_check(void)
{
	uint8_t data[SPIKE_DATA_MAX], encoded[SPIKE_FRAME_MAX],
	        edited[SPIKE_FRAME_MAX];
	memset(data, 0xa5, sizeof(data));
	const unsigned lengths[] = {0U, 1U, 127U, 128U, 4096U, SPIKE_DATA_MAX};
	for (unsigned f = 0; f < 2U; ++f)
		for (unsigned version = 1; version <= 2U; ++version)
			for (unsigned i = 0;
			     i < sizeof(lengths) / sizeof(lengths[0]); ++i) {
				struct message m = {
				        UINT64_MAX, data,
				        lengths[i], (const uint8_t *)"new",
				        3U,         1};
				size_t size = 0;
				assert(spike_encode((enum format)f, version, &m,
				                    encoded, sizeof(encoded),
				                    &size) == OUT_OK);
				for (unsigned reader = 1; reader <= 2U;
				     ++reader) {
					struct view v;
					assert(spike_decode((enum format)f,
					                    reader, encoded,
					                    size, 64U,
					                    &v) == OUT_OK);
					assert(v.message.offset == UINT64_MAX &&
					       v.message.data_size ==
					               m.data_size);
					assert(!m.data_size ||
					       !memcmp(v.message.data, data,
					               m.data_size));
					assert(v.message.label_present ==
					       (version == 2U && reader == 2U));
					assert(spike_edit_offset(
					               &v, 7U, edited,
					               sizeof(edited)) ==
					       OUT_OK);
					struct view newer;
					assert(spike_decode((enum format)f, 2U,
					                    edited, size, 64U,
					                    &newer) == OUT_OK);
					assert(newer.message.offset == 7U &&
					       newer.message.label_present ==
					               (version == 2U));
					if (version == 2U)
						assert(newer.message.label_size ==
						               3U &&
						       !memcmp(newer.message
						                       .label,
						               "new", 3U));
				}
				for (size_t cut = 0; cut < size; ++cut) {
					struct view sentinel, out;
					memset(&sentinel, 0x5a,
					       sizeof(sentinel));
					memcpy(&out, &sentinel, sizeof(out));
					assert(spike_decode((enum format)f, 2U,
					                    encoded, cut, 64U,
					                    &out) != OUT_OK);
					assert(!memcmp(&sentinel, &out,
					               sizeof(out)));
					if (cut >= 4U) {
						/* 伪造外层长度后仍须检查内部游标；
						 * 完整去掉 optional
						 * 尾字段是合法消息。 */
						put32(encoded,
						      (uint32_t)(cut - 4U));
						enum outcome result =
						        spike_decode(
						                (enum format)f,
						                2U, encoded,
						                cut, 64U, &out);
						put32(encoded,
						      (uint32_t)(size - 4U));
						if (f == FORMAT_COMPACT &&
						    version == 2U &&
						    cut == size - 5U) {
							assert(result ==
							               OUT_OK &&
							       !out.message
							                .label_present);
						} else {
							assert(result !=
							       OUT_OK);
							assert(!memcmp(
							        &sentinel, &out,
							        sizeof(out)));
						}
					}
				}
				struct view out;
				assert(spike_decode((enum format)f, 2U, encoded,
				                    size, 0U,
				                    &out) == OUT_LIMIT);
				assert(spike_decode((enum format)f, 2U, encoded,
				                    size, 1U,
				                    &out) == OUT_LIMIT);
				if (version == 2U) {
					uint8_t saved = encoded[size - 1U];
					encoded[size - 1U] = 0xffU;
					assert(spike_decode((enum format)f, 2U,
					                    encoded, size, 64U,
					                    &out) ==
					       OUT_INVALID);
					assert(spike_decode((enum format)f, 1U,
					                    encoded, size, 64U,
					                    &out) == OUT_OK);
					encoded[size - 1U] = saved;
				}
				size_t unchanged = SIZE_MAX;
				assert(spike_encode((enum format)f, version, &m,
				                    edited, size - 1U,
				                    &unchanged) == OUT_LIMIT &&
				       unchanged == SIZE_MAX);
			}
	/* 非最短/溢出的标签整数不进入未知字段跳过路径。 */
	const uint8_t malformed_tags[][10] = {
	        {2, 0, 0, 0, 0x80, 0},
	        {6, 0, 0, 0, 0x81, 0x80, 0x80, 0x80, 0x10, 0}};
	const size_t malformed_sizes[] = {6U, 10U};
	for (unsigned i = 0; i < 2U; ++i) {
		struct view out;
		assert(spike_decode(FORMAT_COMPACT, 2U, malformed_tags[i],
		                    malformed_sizes[i], 64U,
		                    &out) == OUT_INVALID);
	}
}
