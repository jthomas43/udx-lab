#include "all.h"
#include "event.h"
#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

// this is based on nginx's code, except we
// 1. we bet on linux instead of supporting multiple platforms
//   a. thus, we fold src/event/event.c
//           and      src/event/modules/ngx_epoll_module.c
//     into a single file
//     i.e. process_events = ngx_process_events + ngx_epoll_process_events
//

rbtree_t event_timer_rbtree;
static rbtree_node_t event_timer_sentinel;

static queue_t posted_next_events;
static queue_t posted_events;

uint64_t now_ts;

static void delete_posted_event(event_t *ev);
int process_events(loop_t *loop, uint32_t timer, uint32_t flags);

byte_buf_t buf_init(uint8_t *p, size_t len) {
    byte_buf_t buf = {.p = p, .len = len};
    return buf;
}

// get next timer
static uint32_t event_find_timer(void) {
    int32_t timer;

    if (event_timer_rbtree.root == &event_timer_sentinel) {
        return TIMER_TIME_INFINITE;
    }

    rbtree_node_t *root = event_timer_rbtree.root;
    rbtree_node_t *sentinel = event_timer_rbtree.sentinel;
    rbtree_node_t *node = rbtree_min(root, sentinel);

    timer = (int32_t)(node->key - now_ts);

    return (uint32_t)(timer > 0 ? timer : 0);
}

void timer_stop(event_t *ev) {
    // unlike ngnix, our timers don't crash if you stop them
    // while already stopped, it just has no effect.
    // ok to change this later if we prefer to make the user
    // responsible for checking if the timer is sent or not.
    if (ev->timer_set) {
        rbtree_delete(&event_timer_rbtree, &ev->timer);
        ev->timer.left = NULL;
        ev->timer.right = NULL;
        ev->timer.parent = NULL;
        ev->timer_set = 0;
    }
}

// nginx: ngx_add_timer
void timer_start(event_t *ev, uint32_t timer_us) {
    uint32_t key = (uint32_t)now_ts + timer_us;

    if (ev->timer_set) {
        // optimization: skip re-insert if the deadline barely moved (nginx uses 300ms, this code uses 1ms)
        // if ((int32_t)(key - ev->timer.key) < 1000 &&
        //     (int32_t)(ev->timer.key - key) < 1000) {
        //     return;
        // }
        timer_stop(ev);
    }

    ev->timer.key = key;
    rbtree_insert(&event_timer_rbtree, &ev->timer);
    ev->timer_set = 1;
}

// static uint32_t event_id(event_t *event) {
//     // event->data is the socket. the listener socket has no stream
//     socket_t *sock = event->data;
//
//     if (sock && sock->stream) {
//         return sock->stream->entry.local_id;
//     }
//     return 0;
// }

static void event_expire_timers() {

    rbtree_node_t *sentinel = event_timer_rbtree.sentinel;

    for (;;) {
        rbtree_node_t *root = event_timer_rbtree.root;

        if (root == sentinel) {
            return;
        }

        rbtree_node_t *node = rbtree_min(root, sentinel);

        if ((int32_t)(node->key - now_ts) > 0) {
            return;
        }

        event_t *ev = rbt_data(node, event_t, timer);
        // printf("event timer del: %u: %u\n", event_id(ev), ev->timer.key);

        timer_stop(ev);

        ev->timed_out = 1;
        ev->handler(ev); // call the actual event
    }
}

// 64 bits, so now_ts doesn't wrap. timers keep 32-bit keys and compare
// (int32_t)(key - now_ts), which only looks at the low bits
static uint64_t gettime_us() {
    struct timespec ts;
    int rc = clock_gettime(CLOCK_MONOTONIC, &ts);
    assert(rc == 0);

    return (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

// once per loop pass, when epoll returns. handlers that need the time an
// event really happened call it again, e.g. per packet sent or read
void time_update() { now_ts = gettime_us(); }

// nginx: ngx_get_connection
socket_t *get_socket(loop_t *loop, int fd) {
    socket_t *sock = loop->free_sockets;

    if (sock == NULL) {
        printf("no socket available\n");
        return NULL;
    }

    loop->free_sockets = sock->next;
    loop->nfree_sockets--;
    sock->next = NULL;

    // todo:
    // lookup table by fd

    // loop->files[fd] = sock;

    event_t *rev = sock->read;
    event_t *wev = sock->write;

    memset(sock, 0, sizeof(*sock));

    sock->read = rev;
    sock->write = wev;
    sock->fd = fd;
    sock->loop = loop;

    int instance = rev->instance;
    memset(rev, 0, sizeof(*rev));
    memset(wev, 0, sizeof(*wev));

    rev->instance = !instance;
    wev->instance = !instance;

    rev->index = -1;
    wev->index = -1;

    rev->data = sock;
    wev->data = sock;

    wev->write = 1;

    sock->pool = pool_create(256);

    return sock;
}

void socket_free(loop_t *loop, socket_t *sock) {
    sock->next = loop->free_sockets;
    loop->free_sockets = sock;
    loop->nfree_sockets++;

    // loop->files[fd] = NULL;
}

void socket_close(socket_t *sock) {

    pool_t *pool = sock->pool;

    if (sock->fd == -1) {
        printf("already closed\n");
        assert(false && "already closed");
    }

    if (sock->read->timer_set) {
        timer_stop(sock->read);
    }

    if (sock->write->timer_set) {
        timer_stop(sock->write);
    }

    // if (!sock->shared) {
    // ngx_epoll_del_connection(c, NGX_CLOSE_EVENT);
    sock->read->active = 0;
    sock->write->active = 0;
    // }

    if (sock->read->posted) {
        delete_posted_event(sock->read);
    }

    if (sock->write->posted) {
        delete_posted_event(sock->write);
    }
    sock->read->closed = 1;
    sock->write->closed = 1;

    // the push event lives in the stream, in the pool freed below
    if (sock->stream) {
        event_t *push = &sock->stream->push;

        if (push->timer_set) {
            timer_stop(push);
        }

        if (push->posted) {
            delete_posted_event(push);
        }

        // same for pto, it's never posted
        timer_stop(&sock->stream->pto);
    }

    socket_free(sock->loop, sock);

    int fd = sock->fd;
    sock->fd = -1;

    // if (sock->shared) return;

    if (close(fd) == -1) {
        printf("error: close: %s\n", strerror(errno));
    }

    pool_free(pool);
}

// nginx: ngx_epoll_add_event
int event_add(event_t *ev, uint32_t events) {
    socket_t *s = ev->data;
    event_t *other = ev->write ? s->read : s->write;
    int op = EPOLL_CTL_ADD;

    // the fd is already registered for the other direction, keep it
    if (other->active) {
        op = EPOLL_CTL_MOD;
        events |= ev->write ? EPOLLIN : EPOLLOUT;
    }

    epoll_event_t ee;
    ee.events = events;
    ee.data.ptr = (void *)((uintptr_t)s | ev->instance);

    if (epoll_ctl(s->loop->fd, op, s->fd, &ee) == -1) {
        printf("epoll_ctl(%d, %d): %s\n", op, s->fd, strerror(errno));
        return ERR;
    }

    ev->active = 1;

    return OK;
}

static void delete_posted_event(event_t *ev) {
    ev->posted = 0;
    queue_unlink(&ev->queue);
}

static void post_event(event_t *event, queue_t *q);

// ngx_epoll_process_events
int process_events(loop_t *loop, uint32_t timer, uint32_t flags) {

    struct timespec ts;
    struct timespec *p = NULL;

    if (timer != TIMER_TIME_INFINITE) {
        ts.tv_sec = timer / 1000000;
        ts.tv_nsec = (long)(timer % 1000000) * 1000;
        p = &ts;
    }

    int nevents = epoll_pwait2(loop->fd, loop->event_list, loop->nevents, p, NULL);
    epoll_event_t *event_list = loop->event_list;

    int err = (nevents == -1) ? errno : 0;

    if (flags & EVENT_UPDATE_TIME) {
        time_update();
    }

    if (err) {
        if (err == EINTR) {
            printf("epoll_pwait2: EINTR");
        } else {
            printf("epoll_pwait2: fatal error, err=%d\n", err);
        }
        return ERR;
    }

    if (nevents == 0) {
        if (timer != TIMER_TIME_INFINITE) {
            return OK;
        } else {
            assert(0);
            return ERR;
        }
    }

    for (int i = 0; i < nevents; i++) {
        socket_t *s = event_list[i].data.ptr;
        // the last bit of the instance field is set to
        // events instance field when we request reading or writing
        int instance = (uintptr_t)s & 1;
        s = (socket_t *)((uintptr_t)s & (uintptr_t)~1);

        event_t *rev = s->read;

        event_t *wev = s->write;

        if (s->fd == -1 || rev->instance != instance) {
            printf("stale event\n");
            continue;
        }

        int revents = event_list[i].events;
        if (revents & (EPOLLERR | EPOLLHUP)) {
            printf("epoll_wait() error on fd:%d ev=%04XD", s->fd, revents);
            revents |= EPOLLIN | EPOLLOUT;
        }

        if ((revents & EPOLLIN) && rev->active) {
            rev->ready = 1;
            // rev->available = -1;

            if (flags & EVENT_POST_EVENTS) {
                // post_event
                post_event(rev, &posted_events);
            } else {
                rev->handler(rev);
            }
        }

        if ((revents & EPOLLOUT) && wev->active) {
            if (s->fd == -1 || wev->instance != instance) {
                printf("stale write event\n");
                continue;
            }
            wev->ready = 1;
            if (flags & EVENT_POST_EVENTS) {
                post_event(wev, &posted_events);
            } else {
                wev->handler(wev);
            }
        }
    }
    return OK;
}

static void post_event(event_t *event, queue_t *q) {
    if (!event->posted) {
        event->posted = 1;
        queue_tail(q, &event->queue);
    }
}

// nginx: ngx_post_event(ev, &ngx_posted_events)
// runs ev's handler after this iteration's epoll events and timers. posting
// an already posted event does nothing, so several posts coalesce into one call
void event_post(event_t *ev) {
    post_event(ev, &posted_events);
}

// nginx: ngx_event_process_posted
static void process_posted(queue_t *posted) {
    while (!queue_empty(posted)) {
        event_t *ev = queue_data(posted->next, event_t, queue);

        delete_posted_event(ev);
        ev->handler(ev);
    }
}

static void event_process_events_and_timers(loop_t *loop) {
    uint32_t timer = event_find_timer();
    uint32_t delta = 0;

    // if (posted_next_events.len) {
    //   event_move_posted_next();
    //   timer = 0;
    // }

    // something posted outside a handler, e.g. a stream_write before
    // loop_run, shouldn't wait for epoll
    if (!queue_empty(&posted_events)) {
        timer = 0;
    }

    delta = now_ts;
    process_events(loop, timer, EVENT_UPDATE_TIME);
    delta = now_ts - delta;

    event_expire_timers();

    process_posted(&posted_events);
}

int loop_init(loop_t *loop) {

    memset(loop, 0, sizeof(*loop));

    time_update();

    queue_init(&posted_next_events);
    queue_init(&posted_events);

    rbtree_init(&event_timer_rbtree, &event_timer_sentinel,
                rbtree_insert_wrapping_value);

    int rc = epoll_create1(EPOLL_CLOEXEC);

    if (rc < 0) {
        fprintf(stderr, "error creating epoll fd\n");
        return ERR;
    }
    loop->fd = rc;
    loop->nevents = MAX_EVENTS;

    // allocate connections, read_events and write_events. we do this statically

    socket_t *s = loop->socket;
    socket_t *next = NULL;

    for (int i = 0; i < MAX_SOCKET; i++) {
        s[i].next = next;
        s[i].read = &loop->read_event[i];
        s[i].write = &loop->write_event[i];
        s[i].fd = -1;

        next = &s[i];
    }

    loop->free_sockets = next;
    loop->nfree_sockets = MAX_SOCKET;

    return OK;
}

int loop_run(loop_t *loop) {

    queue_init(&posted_next_events);

    while (true) {
        event_process_events_and_timers(loop);

        if (loop->terminate) {
            break;
        }
    }

    return OK;
}
