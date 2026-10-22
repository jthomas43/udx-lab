#ifndef event_h_INCLUDED
#define event_h_INCLUDED

#include "queue.h"
#include "rbtree.h"
#include <stdbool.h>
#include <sys/epoll.h>

typedef struct event_s event_t;

#define TIMER_TIME_INFINITE (uint32_t)-1

#define EVENT_UPDATE_TIME 0x01
#define EVENT_POST_EVENTS 0x02

typedef void (*event_handler_fn)(event_t *ev);

struct event_s {
    void *data;

    unsigned write : 1;
    unsigned ready : 1;
    unsigned active : 1;
    unsigned disabled : 1;
    unsigned timer_set : 1;
    unsigned timed_out : 1;
    unsigned posted : 1;
    unsigned closed : 1;
    unsigned instance : 1; // used to detect stale events
    // unsigned cancelable : 1;

    int read_available;
    uint32_t index;

    event_handler_fn handler;
    rbtree_node_t timer;
    queue_t queue; // the posted queue
};

typedef struct epoll_event epoll_event_t;

#endif // event_h_INCLUDED
