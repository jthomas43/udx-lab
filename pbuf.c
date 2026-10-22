
#include "all.h"
#include <assert.h>
#include <stddef.h>

// circular packet buffer, based on libudx's cirbuf. stores packet pointers,
// indexed by pnum & mask. the packets belong to the caller and never move

void pbuf_init(pbuf_t *p, pool_t *pool, uint32_t initial_size) {
    // the mask only works if size is 2^n
    assert(initial_size > 0 && (initial_size & (initial_size - 1)) == 0);

    p->pool = pool;
    p->size = initial_size;
    p->mask = initial_size - 1;

    // zeroed, so every slot starts empty
    p->values = pcalloc(pool, initial_size * sizeof(packet_t *));
    assert(p->values != NULL);
}

// stores pkt under its pnum. an existing packet with the same pnum is
// replaced. if a different pnum holds the slot, double the size until the two
// land in different slots, then rehash everything
void pbuf_set(pbuf_t *p, packet_t *pkt) {
    uint32_t pnum = pkt->u.hdr.pnum;
    packet_t **slot = &p->values[pnum & p->mask];

    if (*slot == NULL || (*slot)->u.hdr.pnum == pnum) {
        *slot = pkt;
        return;
    }

    uint32_t old_size = p->size;
    packet_t **old_values = p->values;
    uint32_t other = (*slot)->u.hdr.pnum;

    // only the colliding packet needs checking: packets in different slots
    // under the old mask stay in different slots under a larger one
    while ((other & p->mask) == (pnum & p->mask)) {
        p->size *= 2;
        p->mask = p->size - 1;

        // pnums differing only in bit 31 would need 2^32 slots
        assert(p->size != 0);
    }

    p->values = pcalloc(p->pool, p->size * sizeof(packet_t *));
    assert(p->values != NULL);

    for (uint32_t i = 0; i < old_size; i++) {
        packet_t *v = old_values[i];

        if (v != NULL) {
            p->values[v->u.hdr.pnum & p->mask] = v;
        }
    }

    p->values[pnum & p->mask] = pkt;

    pfree(p->pool, old_values);
}

packet_t *pbuf_get(pbuf_t *p, uint32_t pnum) {
    packet_t *pkt = p->values[pnum & p->mask];

    // a slot can hold a different pnum with the same low bits
    return (pkt == NULL || pkt->u.hdr.pnum != pnum) ? NULL : pkt;
}

// empties pnum's slot and returns its packet, which the caller now owns
packet_t *pbuf_remove(pbuf_t *p, uint32_t pnum) {
    packet_t *pkt = pbuf_get(p, pnum);

    if (pkt == NULL) {
        return NULL;
    }

    p->values[pnum & p->mask] = NULL;

    return pkt;
}
