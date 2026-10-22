#ifndef __ALL_H__
#define __ALL_H__
#include "event.h"
#include "queue.h"
#include "rbtree.h"
#include <stdbool.h>
#include <stdint.h>
#include <sys/socket.h>

// laziness
#define MAX_SOCKET 128

typedef struct socket_s socket_t;
typedef struct stream_s stream_t;
typedef struct loop_s loop_t;
typedef struct write_s write_t;

typedef void (*timer_cb)(timer_t *timer);

typedef struct sockaddr sockaddr_t;

typedef struct {
    sockaddr_t *addr;
    socklen_t addrlen;
} packet_info_t;

typedef struct {
    uint8_t *p;
    size_t len;
} byte_buf_t;

typedef struct buf_s buf_t;

struct buf_s {
    uint8_t *pos;
    uint8_t *last;
    uint8_t *start;
    uint8_t *end;
    buf_t *next; // pool->free_bufs, while the buf is free
};

typedef void (*stream_ack_cb)(write_t *write, uint64_t offset, int status);

typedef struct chain_s chain_t;

struct chain_s {
    buf_t *buf;
    chain_t *next;
    write_t *write;  // users write, for write->on_ack()
    uint64_t offset; // stream offset of buf->start
};

struct write_s {
    stream_t *stream;
    stream_ack_cb on_ack;
};
typedef struct pool_s pool_t;
typedef struct pool_large_s pool_large_t;
typedef struct {
    uint8_t *last;
    uint8_t *end;
    pool_t *next;
    uint32_t failed;
} pool_data_t;

struct pool_s {
    pool_data_t data;
    size_t max;
    pool_t *current;
    chain_t *free_list;
    buf_t *free_bufs;
    pool_large_t *large;
};

typedef void (*socket_recv_cb)(socket_t *socket, uint8_t *bytes, ssize_t status, sockaddr_t *from, packet_info_t *pinfo);
typedef void (*stream_read_cb)(stream_t *stream, uint8_t *bytes, ssize_t status, bool end);
// typedef void (*stream_recv_cb)(stream_t *stream, uint8_t *bytes, ssize_t status);

#define OK 0
#define ERR 1
#define ERR_RESET 2

// loop time, updated after each epoll wait
extern uint64_t now_ts;

struct socket_s {
    int fd;
    int family;
    socket_t *next; // free list
    event_t *read;
    event_t *write;
    loop_t *loop;
    pool_t *pool;
    stream_t *stream; // NULL if not a stream socket

    socket_recv_cb user_recv;

    sockaddr_t *addr;
    socklen_t addr_len;
};

#define HEADER_DATA 0x01
#define HEADER_END 0x02
#define HEADER_DESTROY 0x04
#define HEADER_MESSAGE 0x08
#define HEADER_PING 0x10
#define HEADER_PADDING 0x20
#define HEADER_MAX_DATA 0x40
#define HEADER_DATA_BLOCKED 0x80

#define PROTO_DATA_HEADER_SIZE 24 // with data_offset

// max udp payload. 1200 is quic's minimum, safe on almost any path
#define PROTO_MAX_PACKET 1200
#define PROTO_MAX_DATA (PROTO_MAX_PACKET - PROTO_DATA_HEADER_SIZE)

// every packet advertises the sender's receive window after its sacks:
// HEADER_MAX_DATA and the offset the peer may send up to
#define PROTO_MAX_DATA_FIELD_SIZE 8

typedef struct packet_s packet_t;
// this is a data packet in particular -
// packets without data will not have the data_offset field
struct packet_s {
    union {
        struct {
            uint8_t magic;
            uint8_t version;
            uint8_t flags;
            uint8_t nsacks;
            uint32_t id;
            uint32_t pnum;
            uint32_t ack;
            uint64_t data_offset;
        } hdr;
        uint8_t b[24];
    } u;

    bool lost;
    uint16_t len;     // data bytes after the header
    uint64_t send_ts; // (scheduled) send time
    chain_t *chain;   // write_queue link holding data_offset

    // bbr per-packet info (that isn't redundant with info already recorded)
    struct {
        uint64_t delivered_bytes;
        uint64_t lost_bytes;
        uint64_t delivered_ts;
        uint64_t first_send_ts; // start of send epoch
        bool is_app_limited;
        uint64_t tx_in_flight_bytes;
    } P;

    // stream->inflight, in data offset order. a retransmit reuses the packet,
    // so it keeps its place. next also links stream->free_packets
    packet_t *next;
    packet_t **pprev;
};

typedef struct {
    uint64_t delivered_bytes;
    uint64_t delivery_rate; // bytes/sec
    uint32_t rtt_us;
    uint64_t new_acked_bytes;    // # of bytes acked by the packet generating this sample
    uint64_t new_lost_bytes;     // # of bytes marked lost by the packet generating this sample
    uint64_t tx_in_flight_bytes; // inflight_bytes at time of transmission of the packet that has just been acked
    uint64_t lost_bytes;         // # of bytes lost during the rtt measured by this sample

    // from the newest packet acked, set by rate_sample_update
    bool has_data;
    bool is_app_limited;
    uint64_t prior_delivered_bytes; // P.delivered_bytes
    uint64_t prior_lost_bytes;      // P.lost_bytes
    uint64_t prior_ts;              // P.delivered_ts
    uint32_t send_elapsed_us;
    uint32_t ack_elapsed_us;
    uint32_t interval_us; // max of send_elapsed_us and ack_elapsed_us
    uint32_t last_acked_pnum;
} rate_sample_t;

// the header is sent by casting packet_t, so the wire format is the struct's
// native layout
_Static_assert(sizeof(((packet_t *)0)->u.hdr) == 24, "header has padding");
_Static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
               "wire format is little endian");

// circular buffer of packet pointers, indexed by pnum - pbuf.c. packets
// don't move when it grows
typedef struct {
    uint32_t size;
    uint32_t mask;
    pool_t *pool;
    packet_t **values;
} pbuf_t;

typedef struct stream_entry_s stream_entry_t;

typedef enum {
    ENTRY_UNUSED, // sentinel
    ENTRY_STREAM,
    ENTRY_TIMEWAIT,
} stream_entry_type_t;

struct stream_entry_s {
    uint32_t local_id;
    stream_entry_t *hash_next;
    stream_entry_type_t entry_type;
};

#define MAX_WINDOW 4096

// a range [start, end) of stream bytes or packet numbers
typedef struct {
    uint64_t start;
    uint64_t end;
} range_t;

// out-of-order ranges a stream buffers before dropping packets
#define MAX_RCV_RANGES 32

// received peer pnum ranges a stream tracks for its acks. older ones are
// forgotten
#define MAX_ACK_RANGES 32

// a received pnum range [start, end) on the wire, after the header
typedef struct {
    uint32_t start;
    uint32_t end;
} sack_t;

_Static_assert(sizeof(sack_t) == 8, "sack has padding");

typedef enum {
    SEND_STATE_ESTABLISHED,
    SEND_STATE_END_QUEUED, // stream_write(end), END not on the wire yet
    SEND_STATE_END_SENT,   // a packet carried END
    SEND_STATE_END_ACKED,
    SEND_STATE_DESTROYED,
} send_state_t;

typedef enum {
    RECV_STATE_ESTABLISHED,
    RECV_STATE_SIZE_KNOWN, // END received, possibly OOO
    RECV_STATE_DATA_RECEIVED,
    RECV_STATE_DATA_READ,
    RECV_STATE_DESTROYED,
} recv_state_t;

struct stream_s {
    stream_entry_t entry; // hash-entry super type
    socket_t *sock;       // includes fd nd write / read callbacks
    loop_t *loop;
    chain_t *write_queue;
    chain_t **ll; // pointer to last-link in write queue

    sockaddr_t *remote_addr;
    socklen_t remote_addrlen;
    uint32_t remote_id; // the peer's local_id, sent in our headers

    uint32_t snd_next_pnum;
    uint32_t snd_unacked;     // first unacked packet
    uint32_t snd_max_ack;     // largest ack
    uint64_t snd_next_offset; // bytes packetized
    uint64_t snd_write_offset;
    uint64_t snd_unacked_offset; // next cumulative data ack: inflight's first offset
    uint64_t snd_max_offset;     // remotes max offset advertised

    uint32_t rcv_minimum_pnum;
    uint32_t snd_inflight;   // currently in flight (packets)
    uint64_t inflight_bytes; // payload bytes in flight, excludes headers, sacks and lost packets
    uint32_t max_inflight;   // cwnd (packets)

    bool is_max_inflight_limited; // true if the conection has hit max_inflight in now-rtt
    uint64_t snd_next_ts_nanoseconds; // pacing: when the next data packet may leave
    uint32_t snd_quantum;             // pacing: packets that may leave back to back

    send_state_t send_state;
    recv_state_t recv_state;

    pbuf_t packets;
    // queue_t frames;

    packet_t *inflight_queue; // ordered by data offset!
    packet_t **plast_inflight;
    packet_t *free_packets; // linked by next

    event_t push; // nginx: qc->push. all sending happens in its handler
    event_t pto;

    uint32_t delivered;       // packets acked
    uint32_t lost;            // packets marked lost
    uint64_t delivered_bytes; // payload bytes acked
    uint64_t lost_bytes;      // payload bytes marked lost

    // delivery rate sampling, bbr draft 4.1.2.1.1
    uint64_t delivered_ts;  // when delivered_bytes last grew
    uint64_t first_send_ts; // send_ts of the newest packet acked, or of the first sent after idle
    uint64_t app_limited;   // delivered_bytes + inflight_bytes when marked app limited, 0 if not

    uint8_t *rcv_buf;
    uint32_t rcv_buf_size; // rwnd (bytes)
    uint64_t rcv_offset;   // first byte user has not read. incoming data must be between rcv_offset and rcv_offset+rcv_buf_size
    uint64_t rcv_last;     // highest byte offset received. stream complete when rcv_offset == rcv_last
    // limit on incoming data: the advertised window, or the final size once
    // END is received. a window update must not raise it after END
    uint64_t rcv_max_offset;

    // out-of-order data in rcv_buf, sorted, not overlapping or touching
    range_t rcv_ranges[MAX_RCV_RANGES];
    uint32_t nrcv_ranges;

    // peer pnums received, sorted, for the ack and sacks we send
    range_t rcv_pnum_ranges[MAX_ACK_RANGES];
    uint32_t nrcv_pnum_ranges;

    uint32_t ack_pending;     // ack-eliciting packets received since our last ack
    uint64_t ack_delay_start; // now_us when ack_pending went from 0 to 1

    uint32_t srtt_us;
    uint32_t rttvar_us;
    uint32_t latest_rtt_us;
    uint32_t min_rtt_us; // over the stream's lifetime, 0 until the first sample
    uint32_t pto_us;
    uint32_t pto_count;   // ptos since the last ack of new data, for backoff
    uint64_t snd_last_ts; // when the last data packet went out

    struct {
        enum { BBR_STARTUP,
               BBR_DRAIN,
               BBR_PROBE_BW_DOWN,
               BBR_PROBE_BW_CRUISE,
               BBR_PROBE_BW_REFILL,
               BBR_PROBE_BW_UP,
               BBR_PROBE_RTT } state;
        double pacing_gain;
        double max_inflight_gain; // cwnd_gain
        int round_count;
        bool round_start;
        uint64_t next_round_delivered; // P.delivered_bytes at the end of an rtt
        bool is_idle_restart;
        int drain_start_round;

        uint64_t max_inflight_bytes; // cwnd (bytes). stream->max_inflight is this in packets
        uint64_t pacing_rate;        // bytes/sec, C.pacing_rate. stream.c paces with it

        // bw fields are all bytes/sec
        uint64_t max_bw;
        uint64_t bw_shortterm;
        uint64_t bw;

        uint32_t min_rtt_us; // UINT32_MAX until the first sample
        uint64_t bdp;
        uint32_t extra_acked;
        uint64_t max_inflight;
        uint64_t inflight_longterm;  // UINT64_MAX when unset
        uint64_t inflight_shortterm; // UINT64_MAX when unset
        uint64_t prior_cwnd;         // saved over ProbeRTT

        uint64_t bw_latest;       // 1-rtt max of RS.delivery_rate
        uint64_t inflight_latest; // 1-rtt max of RS.delivered
        uint64_t loss_round_delivered;
        bool loss_round_start;
        int loss_events_in_round; // discontiguous lost ranges, for leaving startup
        uint32_t last_lost_pnum;

        uint64_t max_bw_filter[2]; // windowed max-filter of RS.delivery_rate
        int cycle_count;

        // uint64_t extra_acked_interval_start_ts; //
        // uint64_t extra_acked_delivered;
        // uint64_t extra_acked[2]; // max-filter

        // startup
        bool full_bw_reached;
        bool full_bw_now;
        uint64_t full_bw; // bytes/sec
        int full_bw_count;
        // probe bw
        enum { ACK_INIT,
               ACK_REFILLING,
               ACK_PROBE_STARTING,
               ACK_PROBE_FEEDBACK,
               ACK_PROBE_STOPPING } ack_phase;
        bool is_bw_probe_sample;
        bool is_loss_in_round;
        uint64_t bw_probe_up_acked;
        uint64_t bw_probe_up_acked_per_inc;
        int bw_probe_up_rounds;
        int rounds_since_bw_probe_up;
        uint32_t bw_probe_wait_us;
        uint64_t bw_probe_start_ts;
        bool prev_bw_probe_too_high;
        bool prev_bw_probe_precautionary;

        // undo state omitted for now

        uint64_t min_rtt_ts; // time current min_rtt was found

        uint32_t probe_rtt_min_delay_us;
        uint64_t probe_rtt_min_delay_ts;

        bool is_probe_rtt_expired;
        uint64_t probe_rtt_done_ts;
        bool probe_rtt_round_done;
    } bbr;

    stream_read_cb user_read;
    void *data; // the user's
};

#define MAX_EVENTS 512
struct loop_s {
    // epoll
    int fd;
    epoll_event_t event_list[MAX_EVENTS];
    int nevents;

    int terminate;

    socket_t socket[MAX_SOCKET];
    event_t read_event[MAX_SOCKET];
    event_t write_event[MAX_SOCKET];

    socket_t *free_sockets;
    int nfree_sockets;

    stream_entry_t *stream_table[1024];

    rbtree_t timer;
};

int loop_init(loop_t *loop);
int loop_run(loop_t *loop);

socket_t *get_socket(loop_t *loop, int fd);
int event_add(event_t *ev, uint32_t events);

#define RECV_BUF_SIZE 65536
extern uint8_t recv_buf[RECV_BUF_SIZE];

socket_t *socket_bind(loop_t *loop, sockaddr_t *addr);
socket_t *socket_bind_local(loop_t *loop, const char *port);
stream_t *stream_create(socket_t *sock, uint32_t id, uint32_t remote_id, sockaddr_t *remote,
                        sockaddr_t *local);
int stream_write(write_t *w, stream_t *stream, byte_buf_t b, stream_ack_cb on_ack);
int stream_write_end(write_t *w, stream_t *stream, byte_buf_t b, stream_ack_cb on_ack);

int socket_send(socket_t *sock, sockaddr_t *dst, byte_buf_t b);
void socket_close(socket_t *socket);
void stream_close(stream_t *stream);

void time_update();
void timer_start(event_t *ev, uint32_t timer_us);
void timer_stop(event_t *ev);
void event_post(event_t *ev);

int addr_to_addrlen(sockaddr_t *addr);

// pool - pool.c

pool_t *pool_create(size_t size);
void pool_free(pool_t *pool);
void *palloc(pool_t *pool, size_t size);
void *palloc_unaligned(pool_t *pool, size_t size);
void *pcalloc(pool_t *pool, size_t size);
int pfree(pool_t *pool, void *p);
chain_t *palloc_chain_link(pool_t *pool);
void pfree_chain_link(pool_t *pool, chain_t *cl);
buf_t *palloc_buf(pool_t *pool);
void pfree_buf(pool_t *pool, buf_t *b);

// stream table - stream_table.c

stream_entry_t *stream_entry_get(loop_t *loop, uint32_t id);
// return entry that was replaced or NULL if no prior entry
// new->local_id must be set
stream_entry_t *stream_entry_set(loop_t *loop, stream_entry_t *new);
// return entry that was removed or NULL if no entry found
stream_entry_t *stream_entry_remove(loop_t *loop, uint32_t id);

// circular packet buffer - pbuf.c

void pbuf_init(pbuf_t *p, pool_t *pool, uint32_t initial_size);
void pbuf_set(pbuf_t *p, packet_t *pkt);
packet_t *pbuf_get(pbuf_t *p, uint32_t pnum);
packet_t *pbuf_remove(pbuf_t *p, uint32_t pnum);

byte_buf_t buf_init(uint8_t *p, size_t len);

// bbr
void bbr_init(stream_t *stream);
void bbr_on_resume_from_idle(stream_t *stream);
void bbr_congestion_control(stream_t *stream, rate_sample_t *rs);
void bbr_on_packet_lost(stream_t *stream, packet_t *pkt);
void bbr_check_app_limited(stream_t *stream);
#endif
