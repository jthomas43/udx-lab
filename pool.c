#include "all.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// based on nginx's src/core/ngx_palloc.c, without cleanups, pmemalign,
// reset or logging. chain links are from src/core/ngx_buf.c

#define POOL_ALIGNMENT 16
#define ALIGNMENT sizeof(uintptr_t)

#define align_ptr(p, a) \
    (uint8_t *)(((uintptr_t)(p) + ((uintptr_t)(a) - 1)) & ~((uintptr_t)(a) - 1))

struct pool_large_s {
    pool_large_t *next;
    void *alloc;
};

#define align(d, a) (((d) + ((a) - 1)) & ~((a) - 1))

// room for the header and a couple of large allocation nodes
#define POOL_MIN_SIZE \
    align(sizeof(pool_t) + 2 * sizeof(pool_large_t), POOL_ALIGNMENT)

static void *palloc_small(pool_t *pool, size_t size, int align);
static void *palloc_block(pool_t *pool, size_t size);
static void *palloc_large(pool_t *pool, size_t size);

// nginx: ngx_create_pool
pool_t *pool_create(size_t size) {
    pool_t *p;

    // keeps end aligned, so aligning last can never step past it
    assert(size % POOL_ALIGNMENT == 0);
    assert(size >= POOL_MIN_SIZE);

    if (posix_memalign((void **)&p, POOL_ALIGNMENT, size) != 0) {
        return NULL;
    }

    p->data.last = (uint8_t *)p + sizeof(pool_t);
    p->data.end = (uint8_t *)p + size;
    p->data.next = NULL;
    p->data.failed = 0;

    // anything bigger than a page goes straight to malloc
    size_t max = (size_t)getpagesize() - 1;

    size = size - sizeof(pool_t);
    p->max = (size < max) ? size : max;

    p->current = p;
    p->free_list = NULL;
    p->free_bufs = NULL;
    p->large = NULL;

    return p;
}

// nginx: ngx_destroy_pool
void pool_free(pool_t *pool) {
    // the large list nodes live in the blocks, so free these first
    for (pool_large_t *l = pool->large; l; l = l->next) {
        free(l->alloc);
    }

    pool_t *p = pool;

    while (p) {
        pool_t *n = p->data.next;
        free(p);
        p = n;
    }
}

// nginx: ngx_palloc
void *palloc(pool_t *pool, size_t size) {
    if (size <= pool->max) {
        return palloc_small(pool, size, 1);
    }

    return palloc_large(pool, size);
}

// nginx: ngx_pnalloc
void *palloc_unaligned(pool_t *pool, size_t size) {
    if (size <= pool->max) {
        return palloc_small(pool, size, 0);
    }

    return palloc_large(pool, size);
}

// nginx: ngx_pcalloc
void *pcalloc(pool_t *pool, size_t size) {
    void *p = palloc(pool, size);

    if (p) {
        memset(p, 0, size);
    }

    return p;
}

// nginx: ngx_pfree
// only large allocations can be freed. the list node stays, with alloc
// cleared, so palloc_large can reuse it
int pfree(pool_t *pool, void *p) {
    for (pool_large_t *l = pool->large; l; l = l->next) {
        if (p == l->alloc) {
            free(l->alloc);
            l->alloc = NULL;

            return OK;
        }
    }

    return ERR;
}

// nginx: ngx_alloc_chain_link
// reuse a link from the pool's free list if there is one. the link isn't
// cleared, the caller sets buf and next
chain_t *palloc_chain_link(pool_t *pool) {
    chain_t *cl = pool->free_list;

    if (cl) {
        pool->free_list = cl->next;
        return cl;
    }

    return palloc(pool, sizeof(chain_t));
}

// nginx: ngx_free_chain
// links are small allocations, so they go back on the free list rather
// than through pfree
void pfree_chain_link(pool_t *pool, chain_t *cl) {
    cl->next = pool->free_list;
    pool->free_list = cl;
}

// nginx: ngx_quic_alloc_buf's free_bufs. like links, bufs are small
// allocations, so they're recycled rather than freed. the buf isn't cleared,
// the caller sets it up
buf_t *palloc_buf(pool_t *pool) {
    buf_t *b = pool->free_bufs;

    if (b) {
        pool->free_bufs = b->next;
        return b;
    }

    return palloc(pool, sizeof(buf_t));
}

void pfree_buf(pool_t *pool, buf_t *b) {
    b->next = pool->free_bufs;
    pool->free_bufs = b;
}

static void *palloc_small(pool_t *pool, size_t size, int align) {
    pool_t *p = pool->current;

    do {
        uint8_t *m = p->data.last;

        if (align) {
            m = align_ptr(m, ALIGNMENT);
        }

        assert(m <= p->data.end);

        if ((size_t)(p->data.end - m) >= size) {
            p->data.last = m + size;

            return m;
        }

        p = p->data.next;

    } while (p);

    return palloc_block(pool, size);
}

static void *palloc_block(pool_t *pool, size_t size) {
    size_t psize = (size_t)(pool->data.end - (uint8_t *)pool);
    uint8_t *m;

    if (posix_memalign((void **)&m, POOL_ALIGNMENT, psize) != 0) {
        return NULL;
    }

    pool_t *new = (pool_t *)m;

    new->data.end = m + psize;
    new->data.next = NULL;
    new->data.failed = 0;

    // later blocks only have the pool_data_t header, the rest of
    // pool_t is only valid in the first block
    m += sizeof(pool_data_t);
    m = align_ptr(m, ALIGNMENT);
    new->data.last = m + size;

    // every block we searched failed to fit this allocation. stop searching
    // a block once it has failed more than 4 times
    pool_t *p;

    for (p = pool->current; p->data.next; p = p->data.next) {
        if (p->data.failed++ > 4) {
            pool->current = p->data.next;
        }
    }

    p->data.next = new;

    return m;
}

static void *palloc_large(pool_t *pool, size_t size) {
    void *p = malloc(size);

    if (p == NULL) {
        return NULL;
    }

    // reuse a node emptied by pfree, but only check the first few, so a
    // long list doesn't make every large allocation slow
    int n = 0;

    for (pool_large_t *l = pool->large; l; l = l->next) {
        if (l->alloc == NULL) {
            l->alloc = p;
            return p;
        }

        if (n++ > 3) {
            break;
        }
    }

    pool_large_t *large = palloc_small(pool, sizeof(pool_large_t), 1);

    if (large == NULL) {
        free(p);
        return NULL;
    }

    large->alloc = p;
    large->next = pool->large;
    pool->large = large;

    return p;
}
