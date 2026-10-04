#include "tr/frame.h"

#include <string.h>

void tr_frame_init(struct tr_frame *frame)
{
	if (!frame)
		return;

	memset(frame, 0, sizeof(*frame));
}

void tr_frame_release(struct tr_frame *frame)
{
	if (!frame)
		return;

	if (frame->payload)
		tr_buffer_release(frame->payload);

	memset(frame, 0, sizeof(*frame));
}
