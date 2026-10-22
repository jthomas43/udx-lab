#include "assert.h"
#include "queue.h"
#include <stddef.h>

void queue_init(queue_t *q) {
    // empty is the sentinel pointing at itself
    q->prev = q;
    q->next = q;
}

// all insertion operations call this general version
static void queue_insert(queue_t *new, queue_t *prev,
                         queue_t *next) {
    new->next = next;
    new->prev = prev;
    next->prev = new;
    prev->next = new;
}

static void queue_after(queue_t *prev,
                        queue_t *new) {
    queue_insert(new, prev, prev->next);
}

static void queue_before(queue_t *next,
                         queue_t *new) {
    queue_insert(new, next->prev, next);
}

void queue_head(queue_t *q, queue_t *new) {
    queue_after(q, new);
}

void queue_tail(queue_t *q, queue_t *new) {
    queue_before(q, new);
}

bool queue_empty(queue_t *q) {
    return q->next == q;
}

// assumes p must be a member of q
void queue_unlink(queue_t *q) {

    q->prev->next = q->next;
    q->next->prev = q->prev;
    q->next = NULL;
    q->prev = NULL;
}

queue_t *queue_peek(queue_t *q) {
    if (queue_empty(q)) {
        return NULL;
    }
    return q->next;
}

queue_t *queue_shift(queue_t *q) { // queue_dequeue
    queue_t *ret = queue_peek(q);
    if (ret) {
        queue_unlink(ret);
    }
    return ret;
}
