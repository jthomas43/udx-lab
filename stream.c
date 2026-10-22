

#include "all.h"
#include <assert.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PROTO_MIN_HEADER_SIZE 16
#define PROTO_VERSION 3

// max write buffers one packet's data can span
#define PUSH_MAX_IOV 8

// out-of-order receive buffer. 2^n so offsets map to slots with a mask
#define STREAM_RCV_BUF_SIZE 65536

// ack in-order ack-eliciting packets after this many, or after this long
#define ACK_GAP 2
#define MAX_ACK_DELAY_US 1000

// sacks per packet, 8 bytes each
#define MAX_SACKS 16

static void stream_read_handler(event_t *rev);
static void stream_write_handler(event_t *wev);
static void stream_push_handler(event_t *ev);
static void stream_pto_handler(event_t *ev);
static void stream_set_pto(stream_t *stream);
static void pacing_on_send(stream_t *stream, size_t len);
static int fill_acks(stream_t *stream, packet_t *pkt, sack_t *sacks, int max);

// local is the address the peer sent to (from packet_info_t). if NULL, bind
// to the unconnected socket's address, which may be a wildcard
stream_t *stream_create(socket_t *usock, uint32_t id, uint32_t remote_id, sockaddr_t *remote, sockaddr_t *local) {
    loop_t *loop = usock->loop;

    socklen_t local_len;

    if (local == NULL) {
        local = usock->addr;
        local_len = usock->addr_len;
    } else {
        local_len = addr_to_addrlen(local);
    }

    // ids must be unique, stream_process_packet looks streams up by id
    stream_entry_t *existing = stream_entry_get(loop, id);

    if (existing != NULL && existing->entry_type == ENTRY_STREAM) {
        printf("error: stream_create: stream %u already exists\n", id);
        return NULL;
    }

    if (loop->nfree_sockets == 0) {
        printf("error: stream_create: no free sockets\n");
        return NULL;
    }

    int fd = socket(usock->family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);

    if (fd < 0) {
        return NULL;
    }

    int on = 1;

    int rc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    if (rc) {
        perror("setsockopt");
        close(fd);
        return NULL;
    }

    rc = bind(fd, local, local_len);

    // type 2 race condition: between bind and connect,
    // packets for the unconnected socket may arrive on this one.
    // todo:
    // they must be routed back to the unconnected socket handler

    if (rc != 0) {
        perror("bind");
        close(fd);
        return NULL;
    }

    socklen_t remote_len = addr_to_addrlen(remote);

    rc = connect(fd, remote, remote_len);

    if (rc) {
        perror("connect");
        close(fd);
        return NULL;
    }

    socket_t *sock = get_socket(loop, fd);
    assert(sock != NULL);

    // copied rather than shared, since local may point at the read
    // handler's stack or usock's pool, which can be freed before this socket's
    sock->family = usock->family;
    sock->addr = palloc(sock->pool, local_len);

    if (sock->addr == NULL) {
        socket_close(sock);
        return NULL;
    }

    memcpy(sock->addr, local, local_len);
    sock->addr_len = local_len;

    sockaddr_t *remote_addr = palloc(sock->pool, remote_len);

    if (remote_addr == NULL) {
        socket_close(sock);
        return NULL;
    }

    memcpy(remote_addr, remote, remote_len);

    // lives in the socket's pool, so socket_close frees it
    stream_t *stream = pcalloc(sock->pool, sizeof(stream_t));

    if (stream == NULL) {
        socket_close(sock);
        return NULL;
    }

    sock->read->handler = stream_read_handler;
    sock->write->handler = stream_write_handler;

    sock->stream = stream;
    stream->sock = sock;
    stream->loop = loop;
    stream->entry.local_id = id;
    stream->remote_id = remote_id;

    stream->remote_addr = remote_addr;
    stream->remote_addrlen = remote_len;

    rc = event_add(sock->read, EPOLLIN | EPOLLET);
    assert(rc == OK);

    // edge triggered, so this only fires when a full socket drains
    rc = event_add(sock->write, EPOLLOUT | EPOLLET);
    assert(rc == OK);

    // a new socket is writable. without this the first push would wait for
    // the first EPOLLOUT
    sock->write->ready = 1;

    // grows if the pnums in flight span more than this
    pbuf_init(&stream->packets, sock->pool, 64);

    stream->rcv_buf_size = STREAM_RCV_BUF_SIZE;
    stream->rcv_buf = palloc(sock->pool, stream->rcv_buf_size);
    stream->rcv_offset = 0;

    // the implicit initial window both sides assume
    stream->rcv_max_offset = stream->rcv_buf_size;
    stream->snd_max_offset = STREAM_RCV_BUF_SIZE;

    if (stream->rcv_buf == NULL) {
        socket_close(sock);
        return NULL;
    }

    stream->ll = &stream->write_queue;

    // data is the socket, like the read and write events
    stream->push.data = sock;
    stream->push.handler = stream_push_handler;
    stream->pto.data = sock;
    stream->pto.handler = stream_pto_handler;

    bbr_init(stream);

    // after the last failure point, so a failed create never leaves an
    // entry pointing into a freed pool
    stream->entry.entry_type = ENTRY_STREAM;
    stream_entry_set(loop, &stream->entry);

    stream->send_state = SEND_STATE_ESTABLISHED;
    stream->recv_state = RECV_STATE_ESTABLISHED;

    stream->pto_us = 200000; // 200ms

    stream->inflight_queue = NULL;
    stream->plast_inflight = &stream->inflight_queue;

    return stream;
}

// frees the stream too, it's in the socket's pool
void stream_close(stream_t *stream) {
    // before socket_close, the entry is in the pool it frees
    stream_entry_remove(stream->loop, stream->entry.local_id);

    socket_close(stream->sock);
}

// queue len bytes from p for sending. not copied, p must stay valid until
// it's acked. sending happens in the push handler, after this iteration's
// events, so a burst of writes goes out in as few packets as possible

static int stream_write_internal(write_t *w, stream_t *stream, byte_buf_t b, bool end, stream_ack_cb on_ack);

int stream_write(write_t *w, stream_t *stream, byte_buf_t buf, stream_ack_cb on_ack) {
    return stream_write_internal(w, stream, buf, false, on_ack);
}

int stream_write_end(write_t *w, stream_t *stream, byte_buf_t buf, stream_ack_cb on_ack) {
    return stream_write_internal(w, stream, buf, true, on_ack);
}

static int stream_write_internal(write_t *w, stream_t *stream, byte_buf_t b, bool end, stream_ack_cb on_ack) {
    if (stream->send_state != SEND_STATE_ESTABLISHED) {
        return ERR;
    }

    // before the new data is queued: had the stream run out?
    bbr_check_app_limited(stream);

    memset(w, 0, sizeof(*w));
    w->on_ack = on_ack;
    w->stream = stream;

    pool_t *pool = stream->sock->pool;

    buf_t *buf = palloc_buf(pool);

    if (buf == NULL) {
        return ERR;
    }

    // the unsent bytes are pos..last
    buf->start = b.p;
    buf->pos = b.p;
    buf->last = b.p + b.len;
    buf->end = b.p + b.len;
    buf->next = NULL;

    chain_t *cl = palloc_chain_link(pool);

    if (cl == NULL) {
        pfree_buf(pool, buf);
        return ERR;
    }

    // links from the free list keep their old fields
    cl->buf = buf;
    cl->next = NULL;
    cl->write = w;
    cl->offset = stream->snd_write_offset;

    *stream->ll = cl;
    stream->ll = &cl->next;

    event_post(&stream->push);

    stream->snd_write_offset += b.len;

    if (end) {
        stream->send_state = SEND_STATE_END_QUEUED;
    }
    return OK;
}

// a packet carrying only our ack state. it isn't ack-eliciting, isn't counted
// in flight and isn't kept for retransmission, so it ignores the window
static void stream_send_ack(stream_t *stream) {
    socket_t *sock = stream->sock;
    packet_t pkt = {0};
    sack_t sacks[MAX_SACKS];

    pkt.u.hdr.magic = 0xff;
    pkt.u.hdr.version = PROTO_VERSION;
    pkt.u.hdr.flags = HEADER_MAX_DATA;
    pkt.u.hdr.id = stream->remote_id;
    pkt.u.hdr.pnum = stream->snd_next_pnum;

    int nsacks = fill_acks(stream, &pkt, sacks, MAX_SACKS);
    uint64_t max_data = stream->rcv_max_offset;

    struct iovec iov[3] = {
        {.iov_base = pkt.u.b, .iov_len = PROTO_MIN_HEADER_SIZE},
        {.iov_base = sacks, .iov_len = nsacks * sizeof(sack_t)},
        {.iov_base = &max_data, .iov_len = PROTO_MAX_DATA_FIELD_SIZE},
    };

    struct msghdr msg = {
        .msg_iov = iov,
        .msg_iovlen = 3,
    };

    for (;;) {
        ssize_t n = sendmsg(sock->fd, &msg, 0);

        if (n != -1) {
            break;
        }

        if (errno == EINTR || errno == ECONNREFUSED) {
            continue;
        }

        if (errno == EAGAIN) {
            sock->write->ready = 0;
        } else {
            printf("stream %u sendmsg: %s\n", stream->entry.local_id,
                   strerror(errno));
        }

        return;
    }

    // every packet has a pnum, even ones that aren't tracked
    stream->snd_next_pnum++;
    stream->ack_pending = 0;
}

// packets come from a per-stream free list, so the pool's memory for them is
// bounded by the most ever held at once
static packet_t *packet_alloc(stream_t *stream) {
    packet_t *pkt = stream->free_packets;

    if (pkt != NULL) {
        stream->free_packets = pkt->next;
    } else {
        pkt = palloc(stream->sock->pool, sizeof(packet_t));

        if (pkt == NULL) {
            return NULL;
        }
    }

    memset(pkt, 0, sizeof(*pkt));

    return pkt;
}

static void packet_free(stream_t *stream, packet_t *pkt) {
    pkt->next = stream->free_packets;
    stream->free_packets = pkt;
}

static void inflight_unlink(stream_t *stream, packet_t *pkt) {
    bool head = pkt->pprev == &stream->inflight_queue;

    *pkt->pprev = pkt->next;

    if (pkt->next != NULL) {
        pkt->next->pprev = pkt->pprev;
    } else {
        stream->plast_inflight = pkt->pprev;
    }

    // the head holds the lowest unacked offset. with nothing held, everything
    // sent is acked
    if (head) {
        stream->snd_unacked_offset = stream->inflight_queue
                                         ? stream->inflight_queue->u.hdr.data_offset
                                         : stream->snd_next_offset;
    }
}

// build and send one data packet holding up to limit bytes from offset, which
// is in chain. used for new data, and for retransmits of the lost packet rtx.
// on success the packet takes the next pnum and is tracked in flight. a
// retransmit reuses rtx, so it keeps its place in inflight. returns the data
// bytes sent, or -1 if nothing was sent (EAGAIN or an error), in which case
// nothing is committed
static ssize_t stream_send_data_packet(stream_t *stream, chain_t *chain,
                                       uint64_t offset, size_t limit,
                                       packet_t *rtx) {
    socket_t *sock = stream->sock;
    struct iovec iov[3 + PUSH_MAX_IOV];
    sack_t sacks[MAX_SACKS];
    packet_t pkt = {0};
    uint64_t max_data = stream->rcv_max_offset;

    // bbr draft 5.2.2 OnTransmit
    bbr_on_resume_from_idle(stream);

    // only a bare END, or its retransmit, has no data
    assert(limit > 0 || stream->send_state == SEND_STATE_END_QUEUED ||
           stream->send_state == SEND_STATE_END_SENT);

    // a retransmit must carry all the bytes the lost packet held, so it
    // leaves out the oldest sacks rather than splitting
    bool retransmit = rtx != NULL;
    int max_sacks = MAX_SACKS;

    if (retransmit) {
        assert(limit <= PROTO_MAX_DATA - PROTO_MAX_DATA_FIELD_SIZE - sizeof(sack_t));
        size_t fit = (PROTO_MAX_DATA - PROTO_MAX_DATA_FIELD_SIZE - limit) / sizeof(sack_t);

        if (fit < MAX_SACKS) {
            max_sacks = fit;
        }
    }

    int nsacks = fill_acks(stream, &pkt, sacks, max_sacks);

    // room for one sack is always kept, so any packet can be retransmitted
    // with at least one. a nonzero ack with no sacks would read as
    // everything below it acked
    size_t room = PROTO_MAX_DATA - PROTO_MAX_DATA_FIELD_SIZE -
                  (nsacks ? nsacks : 1) * sizeof(sack_t);
    size_t len = limit < room ? limit : room;

    // iov[0] is the header, iov[1] the sacks, iov[2] the receive window
    int niov = 3;
    size_t skip = chain ? offset - chain->offset : 0;
    size_t left = len;
    chain_t *first = NULL;

    for (chain_t *cl = chain; cl && left && niov < 3 + PUSH_MAX_IOV;
         cl = cl->next, skip = 0) {
        size_t n = cl->buf->last - cl->buf->start - skip;

        if (n == 0) {
            continue;
        }

        if (n > left) {
            n = left;
        }

        // the first link with bytes in the packet holds data_offset
        if (first == NULL) {
            first = cl;
        }

        iov[niov].iov_base = cl->buf->start + skip;
        iov[niov].iov_len = n;
        niov++;
        left -= n;
    }

    // the iov cap can cut new data short. a retransmit spans the same links
    // as the original, so it always fits
    len -= left;
    assert(len > 0 || limit == 0);
    assert(!retransmit || len == limit);

    pkt.u.hdr.magic = 0xff;
    pkt.u.hdr.version = PROTO_VERSION;
    pkt.u.hdr.flags = HEADER_DATA | HEADER_MAX_DATA;
    pkt.u.hdr.id = stream->remote_id;
    pkt.u.hdr.pnum = stream->snd_next_pnum;
    pkt.u.hdr.data_offset = offset;

    // a retransmit of the END packet carries END again
    if ((stream->send_state == SEND_STATE_END_QUEUED ||
         stream->send_state == SEND_STATE_END_SENT) &&
        offset + len == stream->snd_write_offset) {
        pkt.u.hdr.flags |= HEADER_END;
    }

    iov[0].iov_base = pkt.u.b;
    iov[0].iov_len = PROTO_DATA_HEADER_SIZE;
    iov[1].iov_base = sacks;
    iov[1].iov_len = nsacks * sizeof(sack_t);
    iov[2].iov_base = &max_data;
    iov[2].iov_len = PROTO_MAX_DATA_FIELD_SIZE;

    // new data needs a packet before the send, so a failed allocation
    // commits nothing
    packet_t *p = rtx;

    if (p == NULL && (p = packet_alloc(stream)) == NULL) {
        return -1;
    }

    struct msghdr msg = {
        .msg_iov = iov,
        .msg_iovlen = niov,
    };

    for (;;) {
        ssize_t n = sendmsg(sock->fd, &msg, 0);

        if (n != -1) {
            break;
        }

        // an icmp error from an earlier send. reporting it clears it, so
        // retry this packet
        if (errno == EINTR || errno == ECONNREFUSED) {
            continue;
        }

        // the caller rebuilds the same packet when the write handler
        // reposts push
        if (errno == EAGAIN) {
            sock->write->ready = 0;
        } else {
            printf("stream %u sendmsg: %s\n", stream->entry.local_id,
                   strerror(errno));
        }

        if (rtx == NULL) {
            packet_free(stream, p);
        }

        return -1;
    }

    // the loop's time is from when epoll returned, which can be a burst of
    // sends earlier. send_ts, pacing and the pto start from when this packet
    // left
    time_update();

    // END is on the wire, and in pbuf, so it's retransmitted if lost
    if (pkt.u.hdr.flags & HEADER_END &&
        stream->send_state == SEND_STATE_END_QUEUED) {
        stream->send_state = SEND_STATE_END_SENT;
    }

    if (rtx != NULL) {
        // same bytes under a new pnum. the old pnum is forgotten, so a late
        // ack for it does nothing
        pbuf_remove(&stream->packets, rtx->u.hdr.pnum);
    } else {
        p->len = len;
        p->chain = first;

        p->next = NULL;
        p->pprev = stream->plast_inflight;
        *stream->plast_inflight = p;
        stream->plast_inflight = &p->next;
    }

    // nothing in flight: the sampling interval starts now, not at the last
    // ack
    if (stream->inflight_bytes == 0) {
        stream->first_send_ts = now_ts;
        stream->delivered_ts = now_ts;
    }

    p->u = pkt.u;
    p->send_ts = now_ts;
    p->lost = false;

    pbuf_set(&stream->packets, p);

    stream->snd_next_pnum++;
    stream->snd_inflight++;
    stream->inflight_bytes += p->len;

    // bbr draft 4.1.2.2 OnPacketSent: snapshot the delivery state, for the
    // rate sample built when this packet is acked
    p->P.delivered_bytes = stream->delivered_bytes;
    p->P.lost_bytes = stream->lost_bytes;
    p->P.delivered_ts = stream->delivered_ts;
    p->P.first_send_ts = stream->first_send_ts;
    p->P.is_app_limited = stream->app_limited != 0;
    p->P.tx_in_flight_bytes = stream->inflight_bytes; // includes this packet

    stream->snd_last_ts = now_ts;
    pacing_on_send(stream, len);
    stream_set_pto(stream);

    // this packet carried our acks
    stream->ack_pending = 0;

    return len;
}

// rfc 9002 6.2.1: srtt + 4*rttvar + max_ack_delay, doubled for each pto
// that fired without an ack in between
static uint32_t stream_pto_duration(stream_t *stream) {
    uint32_t shift = stream->pto_count < 8 ? stream->pto_count : 8;

    return (stream->pto_us + MAX_ACK_DELAY_US) << shift;
}

// nginx: ngx_quic_set_lost_timer
// the pto runs from the last data packet sent while any are in flight.
// lost packets don't count, push resends them without waiting for a timer
static void stream_set_pto(stream_t *stream) {
    if (stream->snd_inflight == 0) {
        timer_stop(&stream->pto);
        return;
    }

    uint64_t deadline = stream->snd_last_ts + stream_pto_duration(stream);
    int64_t left = (int64_t)(deadline - now_ts);

    timer_start(&stream->pto, left > 0 ? (uint32_t)left : 0);
}

// nginx: ngx_quic_pto_handler
// no ack for a whole pto: the tail of what's in flight, or the acks for it,
// may be lost, and with nothing after it there's nothing to trigger loss
// detection. probe by resending the oldest packet in flight. its ack (or the
// ack of anything after it) moves snd_max_ack past the rest, and
// detect_loss takes it from there
static void stream_pto_handler(event_t *ev) {
    socket_t *sock = ev->data;
    stream_t *stream = sock->stream;

    bbr_check_app_limited(stream);

    // lost packets are a prefix of pbuf, so the first one that isn't lost
    // is the oldest in flight. marking it keeps the prefix
    for (uint32_t pnum = stream->snd_unacked; pnum != stream->snd_next_pnum;
         pnum++) {
        packet_t *pkt = pbuf_get(&stream->packets, pnum);

        if (pkt != NULL && !pkt->lost) {
            pkt->lost = true;
            stream->lost += 1;
            stream->lost_bytes += pkt->len;
            bbr_on_packet_lost(stream, pkt);
            stream->snd_inflight -= 1;
            stream->inflight_bytes -= pkt->len;
            break;
        }
    }

    stream->pto_count++;

    // the resend rearms the timer. if push can't send now (EAGAIN), this
    // keeps it armed for whatever is still in flight
    stream_set_pto(stream);
    event_post(&stream->push);
}

// the push timer serves both the ack delay and pacing, so keep whichever
// deadline is sooner
static void push_timer_start(event_t *ev, uint32_t timer_us) {
    uint32_t key = (uint32_t)now_ts + timer_us;

    if (!ev->timer_set || (int32_t)(ev->timer.key - key) > 0) {
        timer_start(ev, timer_us);
    }
}

// bbr draft 5.6.2 pacing. each data packet pushes snd_next_ts_nanoseconds
// out by its share of the pacing rate. a packet may leave while
// snd_next_ts_nanoseconds is at most a send quantum ahead of now, so packets
// go out in bursts of up to a quantum. returns 0 if a packet may leave now,
// else the us until the schedule catches up, so the next wakeup gets a whole
// quantum
static uint32_t pacing_delay(stream_t *stream) {
    uint64_t rate = stream->bbr.pacing_rate;

    if (rate == 0) {
        return 0;
    }

    uint64_t now_ns = now_ts * 1000;
    uint64_t quantum_ns = (uint64_t)stream->snd_quantum * PROTO_MAX_DATA * 1000000000 / rate;

    if (stream->snd_next_ts_nanoseconds <= now_ns + quantum_ns) {
        return 0;
    }

    uint64_t delay_us = (stream->snd_next_ts_nanoseconds - now_ns + 999) / 1000;

    // timer deadlines compare as int32. a longer delay would land in the past
    return delay_us < INT32_MAX ? delay_us : INT32_MAX;
}

static void pacing_on_send(stream_t *stream, size_t len) {
    uint64_t rate = stream->bbr.pacing_rate;
    uint64_t now_ns = now_ts * 1000;

    if (rate == 0) {
        return;
    }

    // after idle the schedule starts from now. no credit saved up
    if (stream->snd_next_ts_nanoseconds < now_ns) {
        stream->snd_next_ts_nanoseconds = now_ns;
    }

    stream->snd_next_ts_nanoseconds += len * 1000000000 / rate;
}

// nginx: ngx_quic_push_handler + ngx_quic_output
// retransmit lost packets, then send new data, until the window is full,
// there's nothing left, pacing holds the next packet, or the socket returns
// EAGAIN. every data packet carries our ack state. if acks are still owed
// after that, send or delay an ack-only packet. the stream state only
// advances once a send succeeds
static void stream_push_handler(event_t *ev) {
    socket_t *sock = ev->data;
    stream_t *stream = sock->stream;
    event_t *wev = sock->write;
    uint32_t paced_us = 0; // pacing stopped sending, come back after this

    // loss is monotone in pnum, older packets are lost first, so the lost
    // packets are a prefix of the ones still in pbuf. each is resent under a
    // new pnum, so snd_unacked follows the scan
    for (uint32_t pnum = stream->snd_unacked;
         pnum != stream->snd_next_pnum && wev->ready &&
         stream->snd_inflight < stream->max_inflight;
         pnum++) {
        packet_t *pkt = pbuf_get(&stream->packets, pnum);

        if (pkt != NULL) {
            if (!pkt->lost) {
                break;
            }

            if ((paced_us = pacing_delay(stream)) > 0) {
                break;
            }

            // it already left snd_inflight when it was marked lost
            if (stream_send_data_packet(stream, pkt->chain,
                                        pkt->u.hdr.data_offset, pkt->len,
                                        pkt) == -1) {
                return;
            }
        }

        // everything before pnum is gone, acked or resent
        stream->snd_unacked = pnum + 1;
    }

    while (wev->ready && paced_us == 0 &&
           stream->snd_inflight < stream->max_inflight) {
        // the first link with unsent bytes, pos..last, holds snd_next_offset.
        // links before it are sent and waiting for acks
        chain_t *cl = stream->write_queue;

        while (cl && cl->buf->pos == cl->buf->last) {
            cl = cl->next;
        }

        if (cl == NULL) {
            // all data is sent. END written after the last data went out
            // still needs a packet of its own
            if (stream->send_state == SEND_STATE_END_QUEUED &&
                stream_send_data_packet(stream, NULL, stream->snd_next_offset,
                                        0, NULL) == -1) {
                return;
            }

            break;
        }

        assert(cl->offset + (cl->buf->pos - cl->buf->start) ==
               stream->snd_next_offset);

        // the peer drops data past its window. wait for it to advertise more
        if (stream->snd_next_offset >= stream->snd_max_offset) {
            break;
        }

        uint64_t unsent = stream->snd_write_offset - stream->snd_next_offset;
        uint64_t window = stream->snd_max_offset - stream->snd_next_offset;

        if (unsent > window) {
            unsent = window;
        }

        size_t limit = unsent < PROTO_MAX_DATA ? unsent : PROTO_MAX_DATA;

        if ((paced_us = pacing_delay(stream)) > 0) {
            break;
        }

        ssize_t n = stream_send_data_packet(stream, cl, stream->snd_next_offset,
                                            limit, NULL);

        if (n == -1) {
            return;
        }

        // sent, commit: the bytes move from unsent to sent
        stream->snd_next_offset += n;

        for (size_t left = n; left; cl = cl->next) {
            size_t k = cl->buf->last - cl->buf->pos;

            if (k > left) {
                k = left;
            }

            cl->buf->pos += k;
            left -= k;
        }
    }

    // the cwnd, not the data or the socket, stopped sending
    if (wev->ready && stream->snd_inflight >= stream->max_inflight &&
        stream->snd_next_offset < stream->snd_write_offset) {
        stream->is_max_inflight_limited = true;
    }

    if (paced_us > 0) {
        push_timer_start(ev, paced_us);
    }

    // nginx: ngx_quic_generate_ack
    // nothing carried the ack. send it alone if enough packets or time has
    // built up, otherwise come back when the delay runs out
    if (stream->ack_pending > 0 && wev->ready) {
        uint64_t delay = now_ts - stream->ack_delay_start;

        if (stream->ack_pending < ACK_GAP && delay < MAX_ACK_DELAY_US) {
            // if it fires after an ack went out, push just finds nothing to do
            push_timer_start(ev, MAX_ACK_DELAY_US - delay);
            return;
        }

        stream_send_ack(stream);
    }

    // both ENDs are acked: ours by the peer, and theirs by us after the user
    // read everything up to it
    if (stream->send_state == SEND_STATE_END_ACKED &&
        stream->recv_state == RECV_STATE_DATA_READ &&
        stream->ack_pending == 0) {
        // todo: time wait. if our ack of the peer's END is lost, its
        // retransmit hits a closed socket instead of getting acked again
        stream_close(stream);
    }
}

// add [start, end) to the sorted ranges r[0..*n), merging any it overlaps or
// touches. fails if it needs a new range and there are already max
static int ranges_insert(range_t *r, uint32_t *n, uint32_t max, uint64_t start,
                         uint64_t end) {
    // first range that isn't entirely before start
    uint32_t i = 0;

    while (i < *n && r[i].end < start) {
        i++;
    }

    // ranges i..j-1 overlap or touch [start, end), widen to cover them
    uint32_t j = i;

    while (j < *n && r[j].start <= end) {
        if (r[j].start < start) {
            start = r[j].start;
        }
        if (r[j].end > end) {
            end = r[j].end;
        }
        j++;
    }

    if (i == j) {
        if (*n == max) {
            return ERR;
        }

        memmove(&r[i + 1], &r[i], (*n - i) * sizeof(range_t));
        (*n)++;

    } else {
        // collapse i..j-1 into r[i]
        memmove(&r[i + 1], &r[j], (*n - j) * sizeof(range_t));
        *n -= j - i - 1;
    }

    r[i].start = start;
    r[i].end = end;

    return OK;
}

// record a peer pnum we received, for the acks we send. returns true if it
// extends the newest range, i.e. it arrived in order
static bool rcv_pnum_record(stream_t *stream, uint32_t pnum) {
    range_t *r = stream->rcv_pnum_ranges;
    uint32_t n = stream->nrcv_pnum_ranges;

    bool in_order = n == 0 || pnum == r[n - 1].end;

    if (ranges_insert(r, &stream->nrcv_pnum_ranges, MAX_ACK_RANGES, pnum,
                      pnum + 1) == OK) {
        return in_order;
    }

    // full, and older than everything tracked. not worth forgetting a
    // newer range for
    if (pnum < r[0].start) {
        return false;
    }

    // forget the oldest range. the peer never sees acks for those pnums,
    // which costs at most spurious retransmits. acking a pnum we didn't
    // receive would lose data, forgetting one can't
    memmove(&r[0], &r[1], (n - 1) * sizeof(range_t));
    stream->nrcv_pnum_ranges--;

    int rc = ranges_insert(r, &stream->nrcv_pnum_ranges, MAX_ACK_RANGES, pnum,
                           pnum + 1);
    assert(rc == OK);

    return in_order;
}

// fill in our ack state. ack is one past the largest peer pnum received, the
// sacks are the received ranges, newest first. returns the number of sacks
static int fill_acks(stream_t *stream, packet_t *pkt, sack_t *sacks, int max) {
    range_t *r = stream->rcv_pnum_ranges;
    int n = stream->nrcv_pnum_ranges;

    if (n == 0) {
        pkt->u.hdr.ack = 0;
        pkt->u.hdr.nsacks = 0;
        return 0;
    }

    pkt->u.hdr.ack = r[n - 1].end;

    int nsacks = n < max ? n : max;

    for (int i = 0; i < nsacks; i++) {
        sacks[i].start = r[n - 1 - i].start;
        sacks[i].end = r[n - 1 - i].end;
    }

    pkt->u.hdr.nsacks = nsacks;

    return nsacks;
}

// hand len bytes to the user. returns ERR if the callback closed the stream,
// which frees it, so the caller must not touch it again
static int rcv_deliver(stream_t *stream, uint8_t *p, size_t len) {
    event_t *rev = stream->sock->read;

    // advance first, so the callback sees the data as consumed
    stream->rcv_offset += len;

    // the window slides as the user reads. after END, rcv_max_offset is the
    // final size and stays put
    if (stream->recv_state == RECV_STATE_ESTABLISHED) {
        stream->rcv_max_offset = stream->rcv_offset + stream->rcv_buf_size;
    }

    // this delivery reaches the peer's END. push checks whether to close
    bool end = stream->recv_state == RECV_STATE_SIZE_KNOWN &&
               stream->rcv_offset == stream->rcv_max_offset;

    if (end) {
        stream->recv_state = RECV_STATE_DATA_READ;
        event_post(&stream->push);
    }

    if (stream->user_read) {
        stream->user_read(stream, p, len, end);
    }

    return rev->closed ? ERR : OK;
}

// accept the data of a packet at offset. in order data goes straight to the
// user, anything else waits in rcv_buf until the gap before it fills.
// returns 1 if the packet was accepted, 0 if it was dropped. this may call
// user_read, which can close the stream, so the stream must not be used
// after this returns
static int stream_recv_data(stream_t *stream, uint64_t offset, uint8_t *p,
                            size_t len) {
    uint64_t end = offset + len;

    assert(end > stream->rcv_offset);                         // passed validation
    assert(end <= stream->rcv_offset + stream->rcv_buf_size); // passed validation

    // trim the part that was already delivered
    if (offset < stream->rcv_offset) {
        p += stream->rcv_offset - offset;
        offset = stream->rcv_offset;
    }

    range_t *first = &stream->rcv_ranges[0];

    // fast path: in order, and ends before anything buffered. no copy
    if (offset == stream->rcv_offset &&
        (stream->nrcv_ranges == 0 || end < first->start)) {
        rcv_deliver(stream, p, end - offset);
        return 1;
    }

    int rc = ranges_insert(stream->rcv_ranges, &stream->nrcv_ranges,
                           MAX_RCV_RANGES, offset, end);

    // rcv_data_fits checked there's room
    assert(rc == OK);

    // copy into the ring, in two parts if it wraps
    uint32_t mask = stream->rcv_buf_size - 1;

    while (offset < end) {
        uint32_t pos = offset & mask;
        uint64_t n = end - offset;

        if (n > stream->rcv_buf_size - pos) {
            n = stream->rcv_buf_size - pos;
        }

        memcpy(stream->rcv_buf + pos, p, n);
        p += n;
        offset += n;
    }

    // if the first range now starts at rcv_offset, the gap is filled.
    // merged ranges never touch, so only the first can be deliverable
    if (first->start != stream->rcv_offset) {
        return 1;
    }

    uint64_t ready_end = first->end;

    memmove(&stream->rcv_ranges[0], &stream->rcv_ranges[1],
            (stream->nrcv_ranges - 1) * sizeof(range_t));
    stream->nrcv_ranges--;

    while (stream->rcv_offset < ready_end) {
        uint32_t pos = stream->rcv_offset & mask;
        uint64_t n = ready_end - stream->rcv_offset;

        if (n > stream->rcv_buf_size - pos) {
            n = stream->rcv_buf_size - pos;
        }

        if (rcv_deliver(stream, stream->rcv_buf + pos, n) != OK) {
            return 1;
        }
    }

    return 1;
}

static int32_t seq_diff(uint32_t a, uint32_t b) {
    return a - b;
}

static uint32_t max_uint32(uint32_t a, uint32_t b) {
    return a > b ? a : b;
}

// x is a difference of two uint32s, e.g. abs_uint32(a - b). it wrapped if
// b > a, so read it as signed. the true difference must be under 2^31
static uint32_t abs_uint32(uint32_t x) {
    int32_t d = (int32_t)x;
    return d < 0 ? -(uint32_t)d : (uint32_t)d;
}

// static int64_t seq_diff64(uint64_t a, uint64_t b) {
//     return a - b;
// }

// a sack is a received range [start, end) of our pnums. ack is one past the
// largest, and was already checked against snd_next_pnum. sacks for pnums
// that were already acked are harmless, so there's no lower bound
static bool
sack_is_valid(uint32_t ack, uint32_t start, uint32_t end) {
    if (seq_diff(start, end) >= 0)
        return false;
    if (seq_diff(end, ack) > 0)
        return false;
    return true;
}

static bool
validate_data_offset(stream_t *stream, uint64_t offset, size_t len, bool is_end) {
    if (offset + len < offset /* check for offset overflow */) {
        return false;
    }

    // an empty range would break the rcv_ranges invariant. only a bare END
    // has no data, and it never becomes a range
    if (len == 0 && !is_end) {
        return false;
    }

    uint64_t end = offset + len;

    // data before rcv_offset is valid, it's handled as a duplicate

    if (end > stream->rcv_max_offset) {
        return false;
    }

    return true;
}

// nginx: the final size checks in ngx_quic_handle_stream_frame. once END
// sets the final size it can't change. data past it fails the window check,
// since rcv_max_offset is the final size from then on
static bool validate_end(stream_t *stream, int type, uint64_t offset,
                         size_t len) {
    uint64_t end = offset + len;

    if (stream->recv_state != RECV_STATE_ESTABLISHED) {
        return !(type & HEADER_END) || end == stream->rcv_max_offset;
    }

    if (!(type & HEADER_END)) {
        return true;
    }

    // data was already received past the claimed end
    uint64_t highest = stream->nrcv_ranges
                           ? stream->rcv_ranges[stream->nrcv_ranges - 1].end
                           : stream->rcv_offset;

    return end >= highest;
}

// whether the data [offset, end) can be accepted. it can, unless it needs a
// new out-of-order range and they're all in use. checked before the pnum is
// recorded, so data we're going to drop is never acked
static bool rcv_data_fits(stream_t *stream, uint64_t offset, uint64_t end) {
    range_t *r = stream->rcv_ranges;
    uint32_t n = stream->nrcv_ranges;

    if (n < MAX_RCV_RANGES) {
        return true;
    }

    // the delivered part gets trimmed, only the rest needs storing
    if (offset < stream->rcv_offset) {
        offset = stream->rcv_offset;
    }

    // old data, or the fast path: delivered without a range
    if (offset == stream->rcv_offset && end < r[0].start) {
        return true;
    }

    // otherwise it has to merge into a range it overlaps or touches, the
    // same test ranges_insert uses
    for (uint32_t i = 0; i < n; i++) {
        if (r[i].start <= end && offset <= r[i].end) {
            return true;
        }
    }

    return false;
}

static bool sack_validate(stream_t *stream, uint32_t *start, uint32_t end) {
    if (seq_diff(end, stream->snd_next_pnum) > 0)
        return false;
    if (seq_diff(*start, end) >= 0)
        return false;
    if (seq_diff(*start, stream->snd_next_pnum) >= 0)
        return false;
    if (seq_diff(*start, stream->snd_unacked) < 0) {
        // special case - ok to have the start less than snd_unacked,
        // clamp it to snd_unacked though
        if (seq_diff(end, stream->snd_unacked) > 0) {
            *start = stream->snd_unacked;
            return true;
        }
        return false;
    }
    return true;
}

// bbr draft 4.1.2.3 UpdateRateSample, for each packet an ack newly acks. the
// newest one sets the sampling interval
static void rate_sample_update(stream_t *stream, packet_t *pkt, rate_sample_t *rs) {

    rs->new_acked_bytes += pkt->len;

    if (!rs->has_data || seq_diff(pkt->u.hdr.pnum, rs->last_acked_pnum) > 0) {
        rs->has_data = true;
        rs->prior_delivered_bytes = pkt->P.delivered_bytes;
        rs->prior_lost_bytes = pkt->P.lost_bytes;
        rs->prior_ts = pkt->P.delivered_ts;
        rs->is_app_limited = pkt->P.is_app_limited;
        rs->tx_in_flight_bytes = pkt->P.tx_in_flight_bytes;
        rs->send_elapsed_us = pkt->send_ts - pkt->P.first_send_ts;
        rs->ack_elapsed_us = stream->delivered_ts - pkt->P.delivered_ts;
        rs->last_acked_pnum = pkt->u.hdr.pnum;
        stream->first_send_ts = pkt->send_ts;
    }
}

// bbr draft 4.1.2.3 GenerateRateSample, once the ack's packets are removed
// and its losses marked
static void rate_sample_generate(stream_t *stream, rate_sample_t *rs) {

    // the app-limited bubble is acked and gone
    if (stream->app_limited && stream->delivered_bytes > stream->app_limited) {
        stream->app_limited = 0;
    }

    // nothing delivered on this ack
    if (!rs->has_data) {
        return;
    }

    rs->interval_us = max_uint32(rs->send_elapsed_us, rs->ack_elapsed_us);
    rs->delivered_bytes = stream->delivered_bytes - rs->prior_delivered_bytes;
    rs->lost_bytes = stream->lost_bytes - rs->prior_lost_bytes;

    // under an rtt means the acks were compressed. the rate would be too high
    if (rs->interval_us < stream->min_rtt_us) {
        return;
    }

    if (rs->interval_us != 0) {
        rs->delivery_rate = rs->delivered_bytes * 1000000 / rs->interval_us;
    }
}

static void remove_packet(stream_t *stream, uint32_t pnum, rate_sample_t *rs) {
    packet_t *pkt = pbuf_remove(&stream->packets, pnum);

    // acked before, or resent under a new pnum
    if (pkt == NULL) {
        return;
    }

    // a lost packet already left snd_inflight
    if (!pkt->lost) {
        stream->snd_inflight -= 1;
        stream->inflight_bytes -= pkt->len;
    }

    stream->delivered += 1;
    stream->delivered_bytes += pkt->len;
    stream->delivered_ts = now_ts;

    rate_sample_update(stream, pkt, rs);

    inflight_unlink(stream, pkt);
    packet_free(stream, pkt);
}

static uint32_t detect_loss(stream_t *stream, rate_sample_t *rs);

static uint32_t stream_process_ack(stream_t *stream, uint32_t max_ack, sack_t *sacks, int nsacks, rate_sample_t *rs) {
    uint32_t delivered = stream->delivered;

    // bbr draft 4.1.2.3 InitRateSample. rtt_us 0 means no rtt sample

    // first, let's check to see if the highest acked packet is newly transmitted,
    // if so generate an rtt sample
    packet_t *pkt = pbuf_get(&stream->packets, max_ack - 1);
    if (pkt) {
        uint32_t rtt_us = now_ts - pkt->send_ts;
        if (rtt_us == 0) {
            rtt_us = 1;
        }
        stream->latest_rtt_us = rtt_us;
        rs->rtt_us = rtt_us;
        if (stream->min_rtt_us == 0 || rtt_us < stream->min_rtt_us) {
            stream->min_rtt_us = rtt_us;
        }
        // win_filter_apply_min(&stream->rtt_min, time, rtt_us);
        if (stream->srtt_us == 0) {
            stream->srtt_us = rtt_us;
            stream->rttvar_us = rtt_us / 2;
        } else {
            uint32_t delta = abs_uint32(stream->srtt_us - rtt_us);

            stream->rttvar_us = (3 * stream->rttvar_us + delta) / 4;
            stream->srtt_us = (7 * stream->srtt_us + rtt_us) / 8;
        }
        stream->pto_us = max_uint32(stream->srtt_us + 4 * stream->rttvar_us, 1000 /* 1ms floor */);
    }

    if (nsacks == 0) {
        // everything up to max ack has been acked. an ack older than
        // snd_unacked has nothing new, and would wrap the loop through 2^32
        // pnums. ack-only packets use pnums without entering pbuf, so this
        // is common: the peer's data crosses our ack-only packets
        for (uint32_t p = stream->snd_unacked; seq_diff(max_ack, p) > 0; p++) {
            remove_packet(stream, p, rs);
        }
    } else {
        for (sack_t *sack = sacks; sack < sacks + nsacks; sack += 1) {
            uint32_t start = sack->start;
            uint32_t end = sack->end;
            if (sack_validate(stream, &start, end)) {
                for (uint32_t p = start; p != end; p++) {
                    remove_packet(stream, p, rs);
                }
            }
        }
    }

    // max_ack is one past the largest pnum the peer has seen. an older ack
    // arriving out of order mustn't move it back
    if (seq_diff(max_ack, stream->snd_max_ack) > 0) {
        stream->snd_max_ack = max_ack;
    }

    // snd_unacked is the first pnum still in pbuf. lost packets stay until
    // they're resent, so this stops at the first lost or in-flight one
    while (stream->snd_unacked != stream->snd_next_pnum &&
           pbuf_get(&stream->packets, stream->snd_unacked) == NULL) {
        stream->snd_unacked++;
    }

    // nothing left in pbuf after END went out: the END packet, or its
    // retransmit, was acked. push checks whether to close
    if (stream->send_state == SEND_STATE_END_SENT &&
        stream->snd_unacked == stream->snd_next_pnum) {
        stream->send_state = SEND_STATE_END_ACKED;
        event_post(&stream->push);
    }

    delivered = stream->delivered - delivered;

    return delivered;
}

static uint32_t detect_loss(stream_t *stream, rate_sample_t *rs) {
    uint32_t lost = stream->lost;
    for (uint32_t pnum = stream->snd_unacked; seq_diff(pnum, stream->snd_max_ack) < 0; pnum++) {
        packet_t *pkt = pbuf_get(&stream->packets, pnum);
        if (pkt) {
            uint32_t delta_us = now_ts - pkt->send_ts;
            if (pkt->lost) {
                continue;
            }

            uint32_t rtt_us = max_uint32(stream->srtt_us, stream->latest_rtt_us);

            if (seq_diff(stream->snd_max_ack, pnum) > 3 || delta_us > rtt_us + rtt_us / 8) {
                pkt->lost = true;
                stream->lost += 1;
                stream->lost_bytes += pkt->len;
                bbr_on_packet_lost(stream, pkt);
                rs->new_lost_bytes += pkt->len;
                stream->snd_inflight -= 1;
                stream->inflight_bytes -= pkt->len;
            }
        }
    }

    return stream->lost - lost;
}

// free write_queue links whose data is all acked, oldest first. a link that
// ends a write calls the write's on_ack with the stream offset its data ends
// at. returns ERR if a callback closed the stream, which frees it
static int stream_ack_chains(stream_t *stream) {
    event_t *rev = stream->sock->read;
    pool_t *pool = stream->sock->pool;

    for (;;) {
        chain_t *cl = stream->write_queue;

        if (cl == NULL) {
            return OK;
        }

        uint64_t end = cl->offset + (cl->buf->last - cl->buf->start);

        // everything before snd_unacked_offset is acked
        if (end > stream->snd_unacked_offset) {
            return OK;
        }

        // the link END was written with is done when END is acked. a bare
        // END has no bytes, so its bytes being acked says nothing
        if (cl->next == NULL &&
            (stream->send_state == SEND_STATE_END_QUEUED ||
             stream->send_state == SEND_STATE_END_SENT)) {
            return OK;
        }

        // unlink first, so a stream_write from the callback appends to a
        // consistent queue
        stream->write_queue = cl->next;

        if (stream->write_queue == NULL) {
            stream->ll = &stream->write_queue;
        }

        // free before the callback, which may close the stream and free
        // the pool the link is in
        write_t *w = cl->write;
        pfree_buf(pool, cl->buf);
        pfree_chain_link(pool, cl);

        if (w && w->on_ack) {
            w->on_ack(w, end, OK);

            if (rev->closed) {
                return ERR;
            }
        }
    }
}

static int stream_process_packet(stream_t *stream, uint8_t *data, ssize_t len, packet_info_t *pinfo) {
    // todo: type-2-race: detect receive packets for the unconnected
    loop_t *loop = stream->loop;

    if (len < PROTO_MIN_HEADER_SIZE) {
        return 0;
    }

    if (data[0] != 0xff || data[1] != PROTO_VERSION) {
        return 0;
    }

    int type = data[2];
    int nsacks = data[3];

    uint32_t *u = (uint32_t *)data + 1;

    uint32_t local_id = u[0];
    uint32_t pnum = u[1];
    uint32_t max_ack = u[2];

    uint64_t data_offset = 0;
    uint64_t new_max_offset = 0;

    sack_t *sacks = NULL;

    if (type & HEADER_DATA) {
        if (len < PROTO_DATA_HEADER_SIZE) {
            return 0;
        }

        data_offset = *(uint64_t *)(data + 16);
        data += PROTO_DATA_HEADER_SIZE;
        len -= PROTO_DATA_HEADER_SIZE;
    } else {
        data += PROTO_MIN_HEADER_SIZE;
        len -= PROTO_MIN_HEADER_SIZE;
    }

    if (nsacks) {
        // todo: length check
        sacks = (sack_t *)data;
        if (len < nsacks * 8) {
            return 0;
        }

        data += nsacks * 8;
        len -= nsacks * 8;
    }

    if (type & HEADER_MAX_DATA) {
        // todo: length check
        // todo: can we reject the packet if the receiver reneges max data?
        new_max_offset = *(uint64_t *)data;
        data += 8;
        len -= 8;
    }

    // for now, let's assume there is only one stream per ip:port:ip:port tuple
    if (local_id != stream->entry.local_id) {
        return 0;
    }
    // since we're assuming one stream per socket, assert that the stream id lookup matches the global stream lookup table
    stream_entry_t *entry = stream_entry_get(loop, local_id);
    assert((stream_t *)entry == stream);

    // ack is one past the largest pnum the peer received, so 0 means
    // nothing yet. it can't be past what we've sent
    if (seq_diff(max_ack, stream->snd_next_pnum) > 0) {
        return 0;
    }

    if (seq_diff(stream->rcv_minimum_pnum, pnum) > 0) {
        return 0;
    }

    if (type & HEADER_DATA) {
        if (!validate_data_offset(stream, data_offset, len, type & HEADER_END)) {
            return 0;
        }

        if (!validate_end(stream, type, data_offset, len)) {
            return 0;
        }

        // todo: should make an allowance to process valid ACKs?
        // todo: set ack pending and post push?
        // drop it before its pnum is recorded, so it isn't acked
        if (!rcv_data_fits(stream, data_offset, data_offset + len)) {
            return 0;
        }
    }

    // there's nothing to sack before anything was received
    // this is wrong - ack is 32 bit and can roll over
    if (nsacks > 0 && max_ack == 0) {
        return 0;
    }

    for (int i = 0; i < nsacks; i++) {
        if (!sack_is_valid(max_ack, sacks[i].start, sacks[i].end)) {
            return 0;
        }

        // newest first, not overlapping or touching
        if (i > 0 && seq_diff(sacks[i - 1].start, sacks[i].end) <= 0) {
            return 0;
        }
    }

    bool in_order = rcv_pnum_record(stream, pnum);

    // END is only meaningful with data_offset, so it's ignored without DATA.
    // from here on rcv_max_offset is the final size
    if ((type & HEADER_DATA) && (type & HEADER_END) &&
        stream->recv_state == RECV_STATE_ESTABLISHED) {
        stream->recv_state = RECV_STATE_SIZE_KNOWN;
        stream->rcv_max_offset = data_offset + len;
    }

    // only data elicits an ack. acking ack-only packets would have the two
    // sides acking each other's acks forever
    if (type & HEADER_DATA) {
        if (stream->ack_pending == 0) {
            stream->ack_delay_start = now_ts;
        }

        stream->ack_pending++;

        // out of order: ack right away, so the peer detects the loss fast.
        // END too, so the peer can finish without waiting out the delay
        if ((!in_order || (type & HEADER_END)) &&
            stream->ack_pending < ACK_GAP) {
            stream->ack_pending = ACK_GAP;
        }

        event_post(&stream->push);
    }

    if (type & HEADER_DESTROY) {
        // todo: RFC 5961 - prevent blind reset attack
        stream->send_state = SEND_STATE_DESTROYED;
        stream->recv_state = RECV_STATE_DESTROYED;
        stream_close(stream);
        return 1;
    }
    // tcp 9293 - ack processing
    // ack already validated

    bbr_check_app_limited(stream);

    rate_sample_t rs = {0};
    uint32_t lost = 0;
    uint32_t delivered = stream_process_ack(stream, max_ack, sacks, nsacks, &rs);
    if (delivered) {
        lost = detect_loss(stream, &rs);
        rate_sample_generate(stream, &rs);
        bbr_congestion_control(stream, &rs);

        // the peer is getting our packets, so the path is fine. rfc 9002
        // resets the backoff on any ack, but an ack of nothing new may be
        // a stale one, from before the loss
        stream->pto_count = 0;
        stream_set_pto(stream);

        // lost packets to resend, and room in the window for new data
        event_post(&stream->push);

        // on_ack may close the stream
        if (stream_ack_chains(stream) == ERR) {
            return 1;
        }
    }
    (void)lost;

    if (type & HEADER_MAX_DATA && new_max_offset > stream->snd_max_offset) {
        stream->snd_max_offset = new_max_offset;

        // the window opened, there may be data waiting for it
        event_post(&stream->push);
    }

    if (type & HEADER_DATA) {
        // nothing new: a retransmit of delivered data, probably because our
        // ack was lost, or a bare END. its pnum is recorded above so it gets
        // acked. dropping it would make the sender retransmit forever
        if (len == 0 || data_offset + len <= stream->rcv_offset) {
            // a bare END after everything before it was delivered. if there
            // are still gaps, rcv_deliver signals END when they fill
            if (stream->recv_state == RECV_STATE_SIZE_KNOWN &&
                stream->rcv_offset == stream->rcv_max_offset) {
                rcv_deliver(stream, NULL, 0);
            }

            return 1;
        }

        // last, user_read may close the stream
        return stream_recv_data(stream, data_offset, data, len);
    }

    return 1;
}

// nginx: ngx_event_recvmsg, for a connected socket
// edge-triggered, so drain the socket until EAGAIN
static void stream_read_handler(event_t *rev) {
    socket_t *sock = rev->data;
    stream_t *stream = sock->stream;

    // connected, so every packet is from remote_addr to addr. no cmsg needed
    packet_info_t pinfo = {
        .addr = sock->addr,
        .addrlen = sock->addr_len,
    };

    for (;;) {
        ssize_t n = recv(sock->fd, recv_buf, RECV_BUF_SIZE, 0);

        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }

            // an icmp port unreachable for an earlier send. reading clears it,
            // so keep draining
            // todo: count these and close the stream if the peer is gone
            if (errno == ECONNREFUSED) {
                continue;
            }

            if (errno != EAGAIN) {
                printf("stream %u recv: %s\n", stream->entry.local_id, strerror(errno));
            }

            rev->ready = 0;
            return;
        }

        // the time this packet was read, not when the drain started. rtt
        // samples and the rate sampler's ack times use it
        time_update();

        stream_process_packet(stream, recv_buf, n, &pinfo);

        // the stream may have been closed, and its pool with it. rev is in
        // the loop's static array, so it's still safe to check
        if (rev->closed) {
            return;
        }
    }
}

static void stream_write_handler(event_t *wev) {
    socket_t *sock = wev->data;

    event_post(&sock->stream->push);
}
