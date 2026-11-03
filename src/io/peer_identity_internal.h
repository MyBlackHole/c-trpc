#ifndef TR_PEER_IDENTITY_INTERNAL_H
#define TR_PEER_IDENTITY_INTERNAL_H

#include <stdint.h>

#include "../../include/tr/peer_identity.h"

struct tr_peer_identity_snapshot;
struct tr_memory_budget;

int tr_peer_identity_snapshot_create(const uint8_t *uri_san,
                                     uint32_t uri_san_len,
                                     struct tr_peer_identity_snapshot **out);
int tr_peer_identity_snapshot_retain(
        struct tr_peer_identity_snapshot *identity);
int tr_peer_identity_snapshot_charge(struct tr_peer_identity_snapshot *identity,
                                     struct tr_memory_budget *budget);
void tr_peer_identity_snapshot_release(
        struct tr_peer_identity_snapshot *identity);
int tr_peer_identity_snapshot_view(
        const struct tr_peer_identity_snapshot *identity,
        struct tr_peer_identity_view *out);

#endif
