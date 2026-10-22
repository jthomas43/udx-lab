#include "all.h"
#include <stddef.h>

// based on libudx's src/udx_stream_table.c. a fixed size hash table of
// stream entries keyed by local_id, chained through hash_next

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

// returns the link pointing at the entry, or at the NULL ending the chain if
// there's no entry, so set and remove can splice in place
static stream_entry_t **lookup(loop_t *loop, uint32_t local_id) {
    int i = local_id % ARRAY_SIZE(loop->stream_table);

    stream_entry_t **p = &loop->stream_table[i];

    while (*p) {
        if ((*p)->local_id == local_id) {
            return p;
        }
        p = &(*p)->hash_next;
    }
    return p;
}

stream_entry_t *stream_entry_get(loop_t *loop, uint32_t id) {
    return *lookup(loop, id);
}

// return entry that was replaced or NULL if no prior entry
stream_entry_t *stream_entry_set(loop_t *loop, stream_entry_t *new) {
    uint32_t id = new->local_id;
    stream_entry_t **p = lookup(loop, id);

    if (*p) {
        stream_entry_t *old = *p;
        new->hash_next = old->hash_next;
        *p = new;
        return old;
    }

    *p = new;
    new->hash_next = NULL;

    return NULL;
}

// return entry that was removed or NULL if no entry found
stream_entry_t *stream_entry_remove(loop_t *loop, uint32_t id) {
    stream_entry_t **p = lookup(loop, id);

    stream_entry_t *ret = *p;

    if (*p) {
        *p = (*p)->hash_next;
    }

    return ret;
}
