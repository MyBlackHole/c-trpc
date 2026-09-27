#ifndef TR_FACADE_BINDING_H
#define TR_FACADE_BINDING_H

#include <stdint.h>

#define TR_FACADE_BIND_WIRE_SIZE 32U
#define TR_FACADE_BIND_VERSION 1U
#define TR_FACADE_BIND_ID_SIZE 16U

#define TR_FACADE_BIND_LANE_CONTROL 0U
#define TR_FACADE_BIND_LANE_BULK 1U

struct tr_facade_binding {
	uint16_t lane;
	uint8_t identity[TR_FACADE_BIND_ID_SIZE];
};

/* Linux-only facade session identity. No weak PRNG fallback is permitted. */
int tr_facade_binding_generate_id(uint8_t out[TR_FACADE_BIND_ID_SIZE]);

int tr_facade_binding_encode(uint8_t out[TR_FACADE_BIND_WIRE_SIZE],
			     const struct tr_facade_binding *binding);
int tr_facade_binding_decode(const uint8_t *data, uint32_t len,
			     struct tr_facade_binding *binding);

int tr_facade_binding_id_equal(const uint8_t a[TR_FACADE_BIND_ID_SIZE],
			       const uint8_t b[TR_FACADE_BIND_ID_SIZE]);

#endif
