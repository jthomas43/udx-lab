#define _GNU_SOURCE // RTLD_NEXT

#include <assert.h>
#include <dlfcn.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include "../all.h"

// writes 4mb in small writes over a lossy path and logs the writer's state to
// DAT_PATH, a line per ack that acks a write and a line per packet sent:
//
//   time_us delivery_rate loss_rate snd_next_offset bytes_acked snd_unacked_offset
//
// the stream doesn't keep its rate samples, so the test makes its own from
// the stream's counters, over the window back to the newest earlier ack at
// least min_rtt_us old. like the draft's rate sample, a shorter window gives
// no sample:
//
//   delivery_rate = delivered bytes / window, in bytes/sec, 0 if no sample
//   loss_rate     = lost bytes / (delivered + lost bytes), over the window
//
// both are NaN on send lines, which only feed the stevens graph.
// then writes a gnuplot script to GP_PATH and runs it, if gnuplot is
// installed, for a delivery rate and loss rate graph, and a stevens graph
//
// loopback doesn't drop packets, so this test's sendmsg drops LOSS_PERCENT
// of the writer's packets and hands the rest to the real one. pass a seed to
// replay a run

#define NBYTES (4 * 1024 * 1024)
// small, so nearly every ack completes a write and gets logged
#define WRITE_SIZE 1024
#define NWRITES (NBYTES / WRITE_SIZE)

#define LOSS_PERCENT 1

#define DAT_PATH "/tmp/stream-rate.dat"
#define GP_PATH "/tmp/stream-rate.gp"
#define RATE_PNG "/tmp/stream-rate-delivery.png"
#define STEVENS_PNG "/tmp/stream-rate-stevens.png"

loop_t loop;
socket_t *send_sock;
socket_t *recv_sock;

stream_t *writer;
stream_t *reader;

event_t timer;

// the user owns written memory until it's acked, a static buffer lives long
// enough
uint8_t data[NBYTES];
write_t writes[NWRITES];

size_t nbytes_read;
bool read_matches = true;
int nwrites_acked;
uint32_t ndropped;

// the writer's connected socket. the stream is freed before the reader
// stops sending, so sendmsg can't look it up through writer
int writer_fd = -1;

bool reader_got_end;
bool reader_end_acked;
write_t reader_end;

FILE *dat;
uint64_t start_ts;
uint64_t last_logged_delivered = UINT64_MAX;

// the writer's counters at each logged ack, the starts of the rate windows
#define NHISTORY 1024

typedef struct {
    uint64_t ts;
    uint64_t delivered_bytes;
    uint64_t lost_bytes;
} history_t;

history_t history[NHISTORY];
uint32_t nhistory;

// the writer is freed once both ends close, so main reports these
uint64_t final_lost_bytes;
uint32_t final_min_rtt_us;

uint32_t rng_state;

static uint32_t rng(void) {
    // xorshift32
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 17;
    rng_state ^= rng_state << 5;
    return rng_state;
}

// snd_next_offset doesn't count this packet yet, which is a packet off at
// most
static void log_send(void) {
    // time starts at the first send
    if (start_ts == 0) {
        start_ts = now_ts;
    }

    fprintf(dat, "%" PRIu64 " NaN NaN %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
            now_ts - start_ts, writer->snd_next_offset,
            writer->delivered_bytes, writer->snd_unacked_offset);
}

// stands in for libc's sendmsg, which the stream calls for every packet. a
// dropped packet reports success, like a send the network then lost
ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
    static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);

    if (real_sendmsg == NULL) {
        real_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
        assert(real_sendmsg != NULL);
    }

    // the writer's fd is only open while the writer is
    if (fd == writer_fd) {
        log_send();
    }

    // only while data is in flight. there's no time wait yet, so if the
    // writer's ack of the reader's END is lost after the writer closes, the
    // reader's END is never acked
    if (fd == writer_fd && nwrites_acked < NWRITES &&
        rng() % 100 < LOSS_PERCENT) {
        ssize_t len = 0;

        for (size_t i = 0; i < msg->msg_iovlen; i++) {
            len += msg->msg_iov[i].iov_len;
        }

        ndropped++;
        return len;
    }

    return real_sendmsg(fd, msg, flags);
}

void timeout(event_t *event) {
    printf("timed out\n");
    loop.terminate = true;
}

static void check_done(void) {
    if (nwrites_acked == NWRITES && reader_got_end && reader_end_acked) {
        loop.terminate = true;
    }
}

// one ack can complete several writes, log it once
static void log_sample(void) {
    if (writer->delivered_bytes == last_logged_delivered) {
        return;
    }

    last_logged_delivered = writer->delivered_bytes;
    final_lost_bytes = writer->lost_bytes;
    final_min_rtt_us = writer->min_rtt_us;

    uint64_t delivery_rate = 0;
    double loss_rate = 0;

    // newest first, for the shortest window that's at least min_rtt_us
    for (uint32_t i = 1; i <= nhistory && i <= NHISTORY; i++) {
        history_t *h = &history[(nhistory - i) % NHISTORY];
        uint64_t window_us = now_ts - h->ts;

        if (window_us == 0 || window_us < writer->min_rtt_us) {
            continue;
        }

        uint64_t delivered = writer->delivered_bytes - h->delivered_bytes;
        uint64_t lost = writer->lost_bytes - h->lost_bytes;

        delivery_rate = delivered * 1000000 / window_us;
        loss_rate = delivered + lost ? (double)lost / (delivered + lost) : 0;
        break;
    }

    history[nhistory % NHISTORY] = (history_t){
        .ts = now_ts,
        .delivered_bytes = writer->delivered_bytes,
        .lost_bytes = writer->lost_bytes,
    };
    nhistory++;

    fprintf(dat, "%" PRIu64 " %" PRIu64 " %f %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
            now_ts - start_ts, delivery_rate, loss_rate,
            writer->snd_next_offset, writer->delivered_bytes,
            writer->snd_unacked_offset);
}

static void on_write_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);

    // acks come in write order, each at the end of its write
    assert(w == &writes[nwrites_acked]);
    assert(offset == (uint64_t)(nwrites_acked + 1) * WRITE_SIZE);

    nwrites_acked++;

    log_sample();
    check_done();
}

static void on_reader_end_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);

    reader_end_acked = true;

    check_done();
}

static void on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    assert(!reader_got_end);
    assert(nbytes_read + len <= NBYTES);

    if (memcmp(bytes, data + nbytes_read, len) != 0) {
        read_matches = false;
    }

    nbytes_read += len;

    if (end) {
        reader_got_end = true;

        int rc = stream_write_end(&reader_end, stream, buf_init(NULL, 0),
                                  on_reader_end_ack);
        assert(rc == OK);
    }
}

static void plot(void) {
    FILE *gp = fopen(GP_PATH, "w");
    assert(gp != NULL);

    fprintf(gp,
            "dat = '" DAT_PATH "'\n"
            "set terminal pngcairo noenhanced size 1200,600\n"
            "set grid\n"
            "set xlabel 'time (ms)'\n"
            "set key top left\n"
            "\n"
            "set output '" RATE_PNG "'\n"
            "set title 'delivery rate and loss rate'\n"
            "set ylabel 'delivery rate (MB/s)'\n"
            "set y2label 'loss rate (lost / (delivered + lost))'\n"
            "set ytics nomirror\n"
            "set y2tics\n"
            "plot dat using ($1/1000):($2 > 0 ? $2/1e6 : 1/0) with points pt 7 ps 0.4 title 'delivery rate', \\\n"
            "     dat using ($1/1000):3 axes x1y2 with impulses title 'loss rate'\n"
            "\n"
            "set output '" STEVENS_PNG "'\n"
            "set title 'stevens graph'\n"
            "set ylabel 'stream offset (bytes)'\n"
            "unset y2label\n"
            "unset y2tics\n"
            "set ytics mirror\n"
            "plot dat using ($1/1000):4 with steps title 'snd_next_offset', \\\n"
            "     dat using ($1/1000):5 with steps title 'bytes_acked', \\\n"
            "     dat using ($1/1000):6 with steps title 'snd_unacked_offset'\n");

    fclose(gp);

    if (system("command -v gnuplot > /dev/null") == 0 &&
        system("gnuplot " GP_PATH) == 0) {
        printf("plots: " RATE_PNG " " STEVENS_PNG "\n");
    } else {
        printf("gnuplot failed or isn't installed, plot with: gnuplot " GP_PATH "\n");
    }
}

int main(int argc, char **argv) {
    // a seed from the command line replays a run
    rng_state = argc > 1 ? (uint32_t)strtoul(argv[1], NULL, 10)
                         : (uint32_t)time(NULL) | 1;
    printf("seed=%u\n", rng_state);

    for (int i = 0; i < NBYTES; i++) {
        data[i] = rng();
    }

    dat = fopen(DAT_PATH, "w");
    assert(dat != NULL);

    loop_init(&loop);

    struct sockaddr_in send_addr;

    send_addr.sin_family = AF_INET;
    send_addr.sin_port = htons(8084);
    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    send_sock = socket_bind(&loop, (sockaddr_t *)&send_addr);
    assert(send_sock != NULL);

    struct sockaddr_in recv_addr;

    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(8085);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    recv_sock = socket_bind(&loop, (sockaddr_t *)&recv_addr);
    assert(recv_sock != NULL);

    timer.handler = timeout;
    timer.data = NULL;
    timer_start(&timer, 10 * 1000 * 1000); // 10 seconds

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);
    writer_fd = writer->sock->fd; // each stream sends on its own connected socket
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);

    reader->user_read = on_read;

    for (int i = 0; i < NWRITES; i++) {
        byte_buf_t b = buf_init(data + i * WRITE_SIZE, WRITE_SIZE);
        int rc;

        if (i == NWRITES - 1) {
            rc = stream_write_end(&writes[i], writer, b, on_write_ack);
        } else {
            rc = stream_write(&writes[i], writer, b, on_write_ack);
        }

        assert(rc == OK);
    }

    int rc = loop_run(&loop);
    assert(rc == 0);

    if (timer.timer_set) {
        timer_stop(&timer);
    }

    fclose(dat);

    printf("acked=%d/%d read=%zu dropped=%u delivered_bytes=%" PRIu64
           " lost_bytes=%" PRIu64 " min_rtt_us=%u\n",
           nwrites_acked, NWRITES, nbytes_read, ndropped,
           last_logged_delivered, final_lost_bytes, final_min_rtt_us);

    assert(nwrites_acked == NWRITES);
    assert(nbytes_read == NBYTES);
    assert(read_matches);
    assert(reader_got_end && reader_end_acked);

    plot();

    return OK;
}
