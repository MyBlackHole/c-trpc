#ifndef TR_FRAME_H
#define TR_FRAME_H

#include "tr/buffer.h"
#include "tr/wire.h"

struct tr_frame {
	struct tr_frame_header header;
	struct tr_buffer *payload;
};

void tr_frame_init(struct tr_frame *frame);
void tr_frame_release(struct tr_frame *frame);

#endif
