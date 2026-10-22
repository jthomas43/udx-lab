#ifndef UDX_QUEUE_H
#define UDX_QUEUE_H
#include <stdbool.h>
#include <stdint.h>

typedef struct queue_s queue_t;

struct queue_s {
    queue_t *next;
    queue_t *prev;
};

#define queue_data(pointer, type, field) \
    ((type *)((char *)(pointer) - offsetof(type, field)))

#define queue_foreach(q, h) \
    for ((q) = (h)->next; (q) != (h); (q) = (q)->next)

void queue_init(queue_t *q);
void queue_head(queue_t *q, queue_t *p); // unshift
void queue_tail(queue_t *q, queue_t *p); // push
bool queue_empty(queue_t *q);

void queue_unlink(queue_t *q);

queue_t *queue_peek(queue_t *q);

queue_t *queue_shift(queue_t *q);
#endif // UDX_QUEUE_H
