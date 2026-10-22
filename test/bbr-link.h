// shared by the test/stream-bbr-*.c tests. include it first, it sets
// _GNU_SOURCE
//
// a writer stream sends nbytes to a reader stream through a bottleneck link
// emulated in this file's sendmsg, which stands in for libc's:
//
//   writer -> reader: a queue of buffer_bytes in front of a link of rate
//                     bytes/sec (whole datagrams), then delay_us. a packet
//                     that doesn't fit in the queue is tail dropped. data
//                     packets are also dropped at random with probability
//                     loss
//   reader -> writer: delay_us, no rate limit
//
// so the path's rtt is 2 * delay_us plus serialization, and its bdp is
// rate * rtt. a link timer hands each packet to the real sendmsg when it
// arrives. it sends on a dup of the sender's socket, so packets still on the
// link get out after their stream closes. the dup keeps the socket in epoll,
// so the loop prints "stale event" once after the writer closes
//
// every SAMPLE_US the writer's bbr state is logged to /tmp/<name>.dat, one
// line per sample:
//
//   time_us state max_bw bw pacing_rate cwnd inflight queue goodput
//   tail_drops random_drops lost_bytes min_rtt_us
//
// rates are bytes/sec, sizes bytes. goodput is payload delivered over the
// last GOODPUT_WINDOW_US. each run of samples in one bbr state is also a line
// in /tmp/<name>-states.dat:
//
//   start_us end_us state
//
// at the end it writes /tmp/<name>.gp and runs gnuplot, if installed, for
// /tmp/<name>-rates.png and /tmp/<name>-inflight.png. each has the bbr state
// as a strip under its time axis
#define _GNU_SOURCE // RTLD_NEXT

#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "../all.h"

#define WRITE_SIZE (64 * 1024)
#define SAMPLE_US 1000
#define GOODPUT_WINDOW_US 20000

// a full data packet from the writer. room for one sack is always kept, but
// the writer never receives data, so it sends none. bbr's rates and goodput
// count payload, the link counts whole datagrams
#define LINK_PAYLOAD                                                           \
    (PROTO_MAX_DATA - sizeof(sack_t) - PROTO_MAX_DATA_FIELD_SIZE)
#define LINK_PACKET                                                            \
    (PROTO_DATA_HEADER_SIZE + PROTO_MAX_DATA_FIELD_SIZE + LINK_PAYLOAD)
#define PAYLOAD_RATE(rate) ((double)(rate) * LINK_PAYLOAD / LINK_PACKET)
// two delays and a full packet's serialization
#define PATH_RTT_US(cfg) (2 * (cfg).delay_us + LINK_PACKET * 1000000 / (cfg).rate)

typedef struct {
    const char *name;      // output files are /tmp/<name>*
    uint64_t rate;         // bottleneck, bytes/sec of whole datagrams
    uint32_t delay_us;     // one way, both directions
    uint64_t buffer_bytes; // the bottleneck's queue
    double loss;           // random loss of data packets, 0..1
    uint64_t nbytes;       // to transfer, a multiple of WRITE_SIZE
    uint32_t seed;
} link_config_t;

// for the tests' checks. steady state is from STEADY_AFTER_US after bbr
// first reaches a probe bw state until the last write is acked
#define STEADY_AFTER_US 100000

typedef struct {
    bool done;            // everything written was acked and read
    bool full_bw_reached; // left startup
    uint64_t steady_ts;   // when steady state began, 0 if it never did
    uint64_t end_ts;      // when the last write was acked
    double goodput;       // payload bytes/sec in steady state
    double mean_queue;    // bytes queued at the bottleneck in steady state
    uint64_t max_queue;   // in steady state
    uint64_t max_bw;      // bbr.max_bw at the end
    uint64_t peak_max_bw; // the highest bbr.max_bw
    uint32_t min_rtt_us;  // bbr.min_rtt_us at the end
    uint32_t tail_drops;
    uint32_t random_drops;
    uint64_t lost_bytes;      // marked lost by the writer
    uint64_t delivered_bytes; // acked
    uint32_t states;          // bit per bbr state visited
} link_result_t;

// the emulator

typedef struct link_pkt_s link_pkt_t;

struct link_pkt_s {
    link_pkt_t *next;
    uint64_t arrive_ts;
    int fd;
    size_t len;
    uint8_t data[];
};

// arrivals are in order on each path, so each is a fifo
typedef struct {
    link_pkt_t *head;
    link_pkt_t **tail;
} link_path_t;

static link_config_t *link_cfg;
static link_result_t *link_res;

static link_path_t fwd = {NULL, &fwd.head}; // writer -> reader
static link_path_t rev = {NULL, &rev.head}; // reader -> writer
static uint64_t link_free_ns; // when the link finishes the last packet queued
static event_t link_timer;

// the streams' connected sockets. a stream closes its socket when it
// finishes, and packets for its peer may still be on the link, so they go
// out on a dup that stays open
static int writer_fd = -1, writer_dup = -1;
static int reader_fd = -1, reader_dup = -1;

static uint32_t rng_state;

static uint32_t rng(void) {
    // xorshift32
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 17;
    rng_state ^= rng_state << 5;
    return rng_state;
}

static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);

static uint64_t link_queue_bytes(void) {
    uint64_t now_ns = now_ts * 1000;

    if (link_free_ns <= now_ns) {
        return 0;
    }

    return (link_free_ns - now_ns) * link_cfg->rate / 1000000000;
}

static void link_arm(void) {
    link_pkt_t *next = fwd.head;

    if (rev.head && (!next || rev.head->arrive_ts < next->arrive_ts)) {
        next = rev.head;
    }

    if (next == NULL) {
        return;
    }

    timer_start(&link_timer, next->arrive_ts > now_ts ? next->arrive_ts - now_ts : 0);
}

static void link_enqueue(link_path_t *path, int fd, const struct msghdr *msg,
                         size_t len, uint64_t arrive_ts) {
    link_pkt_t *pkt = malloc(sizeof(link_pkt_t) + len);
    assert(pkt != NULL);

    pkt->next = NULL;
    pkt->arrive_ts = arrive_ts;
    pkt->fd = fd;
    pkt->len = 0;

    for (size_t i = 0; i < msg->msg_iovlen; i++) {
        memcpy(pkt->data + pkt->len, msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len);
        pkt->len += msg->msg_iov[i].iov_len;
    }

    *path->tail = pkt;
    path->tail = &pkt->next;

    link_arm();
}

static void link_deliver(link_path_t *path) {
    while (path->head && path->head->arrive_ts <= now_ts) {
        link_pkt_t *pkt = path->head;

        path->head = pkt->next;

        if (path->head == NULL) {
            path->tail = &path->head;
        }

        struct iovec iov = {.iov_base = pkt->data, .iov_len = pkt->len};
        struct msghdr msg = {.msg_iov = &iov, .msg_iovlen = 1};

        // a refused or full socket loses the packet, like the network would
        while (real_sendmsg(pkt->fd, &msg, 0) == -1 && errno == EINTR) {
        }

        free(pkt);
    }
}

static void link_timer_handler(event_t *ev) {
    link_deliver(&fwd);
    link_deliver(&rev);
    link_arm();
}

static void link_free_path(link_path_t *path) {
    while (path->head) {
        link_pkt_t *pkt = path->head;

        path->head = pkt->next;
        free(pkt);
    }

    path->tail = &path->head;
}

static int link_nwrites_acked;

// stands in for libc's sendmsg. the streams send every packet with it
ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
    if (real_sendmsg == NULL) {
        real_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
        assert(real_sendmsg != NULL);
    }

    if (fd != writer_fd && fd != reader_fd) {
        return real_sendmsg(fd, msg, flags);
    }

    size_t len = 0;

    for (size_t i = 0; i < msg->msg_iovlen; i++) {
        len += msg->msg_iov[i].iov_len;
    }

    // the time this packet goes onto the link
    time_update();

    if (fd == reader_fd) {
        link_enqueue(&rev, reader_dup, msg, len, now_ts + link_cfg->delay_us);
        return len;
    }

    // random loss hits data packets while data is in flight. there's no time
    // wait yet, so losing the writer's final ack would leave the reader's END
    // unacked
    uint8_t type = ((uint8_t *)msg->msg_iov[0].iov_base)[2];
    int nwrites = link_cfg->nbytes / WRITE_SIZE;

    if ((type & HEADER_DATA) && link_nwrites_acked < nwrites &&
        rng() < link_cfg->loss * UINT32_MAX) {
        link_res->random_drops++;
        return len;
    }

    if (link_queue_bytes() + len > link_cfg->buffer_bytes) {
        link_res->tail_drops++;
        return len;
    }

    uint64_t now_ns = now_ts * 1000;
    uint64_t start_ns = link_free_ns > now_ns ? link_free_ns : now_ns;

    link_free_ns = start_ns + len * 1000000000 / link_cfg->rate;

    uint64_t arrive_ts = (link_free_ns + 999) / 1000 + link_cfg->delay_us;

    link_enqueue(&fwd, writer_dup, msg, len, arrive_ts);

    return len;
}

// the transfer

static loop_t link_loop;
static stream_t *writer;
static stream_t *reader;
static write_t *writes;
static uint8_t pattern[WRITE_SIZE];

static uint64_t nbytes_read;
static bool read_matches = true;
static bool reader_got_end;
static bool reader_end_acked;
static write_t reader_end;

static event_t sample_timer;
static event_t timeout_timer;
static FILE *dat;
static uint64_t start_ts;

// the run of samples in the current bbr state, for the state strip
static FILE *states_dat;
static int run_state = -1;
static uint64_t run_start_us;
static uint64_t last_sample_us;

// delivered_bytes at each sample, for goodput over GOODPUT_WINDOW_US
#define NHISTORY (GOODPUT_WINDOW_US / SAMPLE_US + 1)
static uint64_t delivered_history[NHISTORY];
static uint64_t history_ts[NHISTORY];
static uint32_t nsamples;

// steady state accumulators
static uint64_t steady_delivered;
static double queue_sum;
static uint32_t queue_samples;

static void check_done(void) {
    if (link_nwrites_acked == (int)(link_cfg->nbytes / WRITE_SIZE) &&
        reader_got_end && reader_end_acked) {
        link_res->done = true;
        link_loop.terminate = true;
    }
}

// the writer's state now. the writer is freed once both ends close, so this
// only runs while writes are outstanding
static void sample(void) {
    uint32_t i = nsamples % NHISTORY;
    uint32_t oldest = nsamples >= NHISTORY ? (nsamples + 1) % NHISTORY : 0;

    delivered_history[i] = writer->delivered_bytes;
    history_ts[i] = now_ts;

    double goodput = 0;

    if (now_ts > history_ts[oldest]) {
        goodput = (double)(writer->delivered_bytes - delivered_history[oldest]) * 1000000 /
                  (now_ts - history_ts[oldest]);
    }

    nsamples++;

    int state = writer->bbr.state;
    uint64_t queue = link_queue_bytes();

    link_res->states |= 1u << state;
    link_res->full_bw_reached = writer->bbr.full_bw_reached;
    link_res->max_bw = writer->bbr.max_bw;

    if (writer->bbr.max_bw > link_res->peak_max_bw) {
        link_res->peak_max_bw = writer->bbr.max_bw;
    }

    link_res->min_rtt_us = writer->bbr.min_rtt_us;
    link_res->lost_bytes = writer->lost_bytes;
    link_res->delivered_bytes = writer->delivered_bytes;

    bool probe_bw = state == BBR_PROBE_BW_DOWN || state == BBR_PROBE_BW_CRUISE ||
                    state == BBR_PROBE_BW_REFILL || state == BBR_PROBE_BW_UP;

    if (link_res->steady_ts == 0 && probe_bw) {
        link_res->steady_ts = now_ts + STEADY_AFTER_US;
    }

    if (link_res->steady_ts && now_ts >= link_res->steady_ts) {
        if (steady_delivered == 0) {
            steady_delivered = writer->delivered_bytes;
        }

        queue_sum += queue;
        queue_samples++;

        if (queue > link_res->max_queue) {
            link_res->max_queue = queue;
        }
    }

    uint64_t t = now_ts - start_ts;

    if (state != run_state) {
        if (run_state != -1) {
            fprintf(states_dat, "%" PRIu64 " %" PRIu64 " %d\n", run_start_us, t, run_state);
        }

        run_state = state;
        run_start_us = t;
    }

    last_sample_us = t;

    fprintf(dat, "%" PRIu64 " %d %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64
                 " %" PRIu64 " %.0f %u %u %" PRIu64 " %u\n",
            t, state, writer->bbr.max_bw, writer->bbr.bw,
            writer->bbr.pacing_rate, writer->bbr.max_inflight_bytes,
            writer->inflight_bytes, queue, goodput, link_res->tail_drops,
            link_res->random_drops, writer->lost_bytes, writer->bbr.min_rtt_us);
}

static void sample_handler(event_t *ev) {
    sample();
    timer_start(&sample_timer, SAMPLE_US);
}

static void timeout_handler(event_t *ev) {
    printf("timed out\n");
    link_loop.terminate = true;
}

static void on_write_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);

    link_nwrites_acked++;

    // the last write: take the final sample while the writer still exists
    if (link_nwrites_acked == (int)(link_cfg->nbytes / WRITE_SIZE)) {
        sample();
        timer_stop(&sample_timer);

        link_res->end_ts = now_ts;

        if (link_res->steady_ts && now_ts > link_res->steady_ts) {
            link_res->goodput = (double)(writer->delivered_bytes - steady_delivered) * 1000000 /
                                (now_ts - link_res->steady_ts);
        }
    }

    check_done();
}

static void on_reader_end_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);

    reader_end_acked = true;
    check_done();
}

static void on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    for (ssize_t i = 0; i < len; i++) {
        if (bytes[i] != pattern[(nbytes_read + i) % WRITE_SIZE]) {
            read_matches = false;
            break;
        }
    }

    nbytes_read += len;

    if (end) {
        reader_got_end = true;

        int rc = stream_write_end(&reader_end, stream, buf_init(NULL, 0), on_reader_end_ack);
        assert(rc == OK);
    }
}

// a plot's data panel. the state strip under it shares its time axis, so
// both have fixed margins
static void link_plot_panel(FILE *gp) {
    fprintf(gp,
            "set multiplot\n"
            "set lmargin at screen 0.07\n"
            "set rmargin at screen 0.83\n"
            "set tmargin at screen 0.93\n"
            "set bmargin at screen 0.25\n"
            "set grid\n"
            "set key default\n"
            "set key outside right top Left reverse samplen 2\n"
            "set xrange [0:*]\n"
            "set format x ''\n"
            "unset xlabel\n");
}

// the bbr state under the time axis, a box per run of samples in one state
static void link_plot_states(FILE *gp) {
    fprintf(gp,
            "set tmargin at screen 0.23\n"
            "set bmargin at screen 0.15\n"
            "unset title\n"
            "unset grid\n"
            "set ylabel ''\n"
            "set yrange [0:1]\n"
            "unset ytics\n"
            "set format x '%%g'\n"
            "set xlabel 'time (s)'\n"
            "set key at screen 0.07,0.02 left bottom Left reverse horizontal maxrows 1 "
            "samplen 1 width 1\n"
            "set style fill solid 1.0 noborder\n"
            "plot states using (($1+$2)/2e6):(0.5):($1/1e6):($2/1e6):(0):(1):($3+11) "
            "with boxxyerror lc variable notitle, \\\n"
            "     NaN with boxes lt 11 title 'startup', NaN with boxes lt 12 title 'drain', \\\n"
            "     NaN with boxes lt 13 title 'down', NaN with boxes lt 14 title 'cruise', \\\n"
            "     NaN with boxes lt 15 title 'refill', NaN with boxes lt 16 title 'up', \\\n"
            "     NaN with boxes lt 17 title 'probe_rtt'\n"
            "unset multiplot\n"
            "set ytics\n"
            "set style fill empty\n");
}

static void link_plot(void) {
    const char *n = link_cfg->name;
    char path[256];

    snprintf(path, sizeof(path), "/tmp/%s.gp", n);

    FILE *gp = fopen(path, "w");
    assert(gp != NULL);

    double bdp = link_cfg->rate * 2.0 * link_cfg->delay_us / 1000000;

    fprintf(gp,
            "dat = '/tmp/%s.dat'\n"
            "states = '/tmp/%s-states.dat'\n"
            "set terminal pngcairo noenhanced size 1200,680\n"
            "\n"
            "# a color per bbr state, linetypes 11-17 so they don't move the data's\n"
            "set linetype 11 lc rgb '#9e9e9e'\n"
            "set linetype 12 lc rgb '#795548'\n"
            "set linetype 13 lc rgb '#1e88e5'\n"
            "set linetype 14 lc rgb '#43a047'\n"
            "set linetype 15 lc rgb '#fdd835'\n"
            "set linetype 16 lc rgb '#e53935'\n"
            "set linetype 17 lc rgb '#8e24aa'\n"
            "\n"
            "set output '/tmp/%s-rates.png'\n",
            n, n, n);
    link_plot_panel(gp);
    fprintf(gp,
            "set title '%s: link %.1f MB/s, rtt %.1f ms, buffer %.0f KB, loss %.1f%%'\n"
            "set ylabel 'MB/s'\n"
            "set yrange [0:%f]\n"
            "plot dat using ($1/1e6):($3/1e6) with lines title 'max_bw', \\\n"
            "     dat using ($1/1e6):($5/1e6) with lines title 'pacing_rate', \\\n"
            "     dat using ($1/1e6):($9/1e6) with lines lw 2 title 'goodput', \\\n"
            "     %f with lines dt 2 title 'link payload rate'\n",
            n, link_cfg->rate / 1e6, 2.0 * link_cfg->delay_us / 1000,
            link_cfg->buffer_bytes / 1024.0, link_cfg->loss * 100, link_cfg->rate * 2.5 / 1e6,
            PAYLOAD_RATE(link_cfg->rate) / 1e6);
    link_plot_states(gp);

    fprintf(gp, "\nset output '/tmp/%s-inflight.png'\n", n);
    link_plot_panel(gp);
    fprintf(gp,
            "set title '%s: link %.1f MB/s, rtt %.1f ms, buffer %.0f KB, loss %.1f%%'\n"
            "set ylabel 'KB'\n"
            "set yrange [0:*]\n"
            "plot dat using ($1/1e6):($6/1024) with lines title 'cwnd', \\\n"
            "     dat using ($1/1e6):($7/1024) with lines title 'inflight', \\\n"
            "     dat using ($1/1e6):($8/1024) with lines title 'bottleneck queue', \\\n"
            "     %f with lines dt 2 lc rgb '#616161' title 'bdp', \\\n"
            "     %f with lines dt 3 lw 2 lc rgb 'black' title 'buffer'\n",
            n, link_cfg->rate / 1e6, 2.0 * link_cfg->delay_us / 1000,
            link_cfg->buffer_bytes / 1024.0, link_cfg->loss * 100, bdp / 1024,
            link_cfg->buffer_bytes / 1024.0);
    link_plot_states(gp);

    fclose(gp);

    char cmd[512];

    snprintf(cmd, sizeof(cmd), "gnuplot %s", path);

    if (system("command -v gnuplot > /dev/null") == 0 && system(cmd) == 0) {
        printf("plots: /tmp/%s-rates.png /tmp/%s-inflight.png\n", n, n);
    } else {
        printf("gnuplot failed or isn't installed, plot with: %s\n", cmd);
    }
}

static int link_run(link_config_t *cfg, link_result_t *res) {
    link_cfg = cfg;
    link_res = res;
    memset(res, 0, sizeof(*res));

    // a seed from the command line replays a run
    rng_state = cfg->seed;
    printf("seed=%u\n", rng_state);

    for (int i = 0; i < WRITE_SIZE; i++) {
        pattern[i] = i * 7 + i / 256;
    }

    char path[256];

    snprintf(path, sizeof(path), "/tmp/%s.dat", cfg->name);
    dat = fopen(path, "w");
    assert(dat != NULL);

    snprintf(path, sizeof(path), "/tmp/%s-states.dat", cfg->name);
    states_dat = fopen(path, "w");
    assert(states_dat != NULL);

    loop_init(&link_loop);

    struct sockaddr_in send_addr = {.sin_family = AF_INET, .sin_port = htons(8086)};
    struct sockaddr_in recv_addr = {.sin_family = AF_INET, .sin_port = htons(8087)};

    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    socket_t *send_sock = socket_bind(&link_loop, (sockaddr_t *)&send_addr);
    assert(send_sock != NULL);
    socket_t *recv_sock = socket_bind(&link_loop, (sockaddr_t *)&recv_addr);
    assert(recv_sock != NULL);

    link_timer.handler = link_timer_handler;
    sample_timer.handler = sample_handler;
    timeout_timer.handler = timeout_handler;

    // 5x the time the link needs, plus a margin for startup
    timer_start(&timeout_timer, cfg->nbytes * 5 * 1000000 / cfg->rate + 5000000);

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);

    // each stream sends on its own connected socket
    writer_fd = writer->sock->fd;
    reader_fd = reader->sock->fd;
    writer_dup = dup(writer_fd);
    reader_dup = dup(reader_fd);
    assert(writer_dup != -1 && reader_dup != -1);

    reader->user_read = on_read;

    // all the data up front, so the writer is never app limited. every write
    // is the same pattern, the memory only has to outlive the acks
    int nwrites = cfg->nbytes / WRITE_SIZE;

    writes = calloc(nwrites, sizeof(write_t));
    assert(writes != NULL);

    for (int i = 0; i < nwrites; i++) {
        byte_buf_t b = buf_init(pattern, WRITE_SIZE);
        int rc = i == nwrites - 1 ? stream_write_end(&writes[i], writer, b, on_write_ack)
                                  : stream_write(&writes[i], writer, b, on_write_ack);
        assert(rc == OK);
    }

    start_ts = now_ts;
    timer_start(&sample_timer, SAMPLE_US);

    int rc = loop_run(&link_loop);
    assert(rc == 0);

    timer_stop(&timeout_timer);
    timer_stop(&sample_timer);
    timer_stop(&link_timer);
    link_free_path(&fwd);
    link_free_path(&rev);
    close(writer_dup);
    close(reader_dup);
    free(writes);
    fclose(dat);

    // the last run ends at the last sample
    if (run_state != -1) {
        fprintf(states_dat, "%" PRIu64 " %" PRIu64 " %d\n", run_start_us, last_sample_us,
                run_state);
    }

    fclose(states_dat);

    if (!read_matches || nbytes_read != cfg->nbytes) {
        res->done = false;
    }

    if (res->steady_ts && res->end_ts > res->steady_ts && queue_samples) {
        res->mean_queue = queue_sum / queue_samples;
    }

    double bdp = cfg->rate * 2.0 * cfg->delay_us / 1000000;

    printf("%s: done=%d read=%" PRIu64 " time=%.2fs goodput=%.2fMB/s (link payload %.2f) "
           "max_bw=%.2fMB/s peak=%.2fMB/s min_rtt=%uus queue mean=%.1fKB max=%.1fKB (bdp %.1fKB) "
           "tail_drops=%u random_drops=%u lost=%" PRIu64 " states=0x%x\n",
           cfg->name, res->done, nbytes_read, (res->end_ts - start_ts) / 1e6,
           res->goodput / 1e6, PAYLOAD_RATE(cfg->rate) / 1e6, res->max_bw / 1e6,
           res->peak_max_bw / 1e6,
           res->min_rtt_us, res->mean_queue / 1024, res->max_queue / 1024.0, bdp / 1024,
           res->tail_drops, res->random_drops, res->lost_bytes, res->states);

    link_plot();

    return OK;
}

// checks print every failure, then the test fails if any did

static int link_failed;

static void link_check(bool ok, const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    printf("%s: ", ok ? "ok" : "FAIL");
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);

    if (!ok) {
        link_failed = 1;
    }
}
