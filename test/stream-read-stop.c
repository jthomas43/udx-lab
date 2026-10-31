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

// a slow reader: it stops reading after every STOP_AFTER bytes, from inside
// on_read, and starts again STOP_US later from a timer. while it's stopped
// the writer fills the 64 KB window and waits
//
// the reader's first window update after each start, a HEADER_PING packet,
// is dropped. the writer has nothing in flight and doesn't ask, so only the
// reader's pto, resending the unacked update, gets the writer going again

#define NBYTES (1024 * 1024)
#define WRITE_SIZE (64 * 1024)
#define NWRITES (NBYTES / WRITE_SIZE)

#define STOP_AFTER 100000
#define STOP_US 5000

loop_t loop;
socket_t *send_sock;
socket_t *recv_sock;

stream_t *writer;
stream_t *reader;

write_t writes[NWRITES];
uint8_t pattern[WRITE_SIZE];

event_t timeout_timer;
event_t start_timer;

uint64_t nbytes_read;
uint64_t read_since_start;
bool stopped;
int nstops;
int nwindow_full; // starts that found the writer at the edge of the window

int nwrites_acked;
bool reader_got_end;
bool reader_end_acked;
write_t reader_end;

// the sendmsg below

static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);
int reader_fd = -1;
bool drop_update;
int nupdates_dropped;
int nupdates_sent;
int nupdates_resent; // the same window sent again
uint64_t last_update;

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
    if (real_sendmsg == NULL) {
        real_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
        assert(real_sendmsg != NULL);
    }

    uint8_t type = ((uint8_t *)msg->msg_iov[0].iov_base)[2];

    if (fd != reader_fd || !(type & HEADER_PING)) {
        return real_sendmsg(fd, msg, flags);
    }

    // the window follows the sacks, in its own iov
    uint64_t update = *(uint64_t *)msg->msg_iov[2].iov_base;

    nupdates_sent++;

    if (update == last_update) {
        nupdates_resent++;
    }

    last_update = update;

    if (drop_update) {
        drop_update = false;
        nupdates_dropped++;

        size_t len = 0;

        for (size_t i = 0; i < msg->msg_iovlen; i++) {
            len += msg->msg_iov[i].iov_len;
        }

        return len;
    }

    return real_sendmsg(fd, msg, flags);
}

static void timeout(event_t *ev) {
    printf("timed out\n");
    loop.terminate = true;
}

static void check_done(void) {
    if (nwrites_acked == NWRITES && reader_got_end && reader_end_acked) {
        loop.terminate = true;
    }
}

static void on_write_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);
    assert(offset == (uint64_t)(w - writes + 1) * WRITE_SIZE);

    nwrites_acked++;
    check_done();
}

static void on_reader_end_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);

    reader_end_acked = true;
    check_done();
}

static void start_reading(event_t *ev) {
    // the writer never sends past the window
    assert(writer->snd_next_offset <= reader->rcv_max_offset);

    if (writer->snd_next_offset == reader->rcv_max_offset) {
        nwindow_full++;
    }

    stopped = false;
    read_since_start = 0;
    drop_update = true;

    stream_read_start(reader);
}

static void on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    assert(!stopped);

    for (ssize_t i = 0; i < len; i++) {
        assert(bytes[i] == pattern[(nbytes_read + i) % WRITE_SIZE]);
    }

    nbytes_read += len;
    read_since_start += len;

    if (end) {
        reader_got_end = true;

        int rc = stream_write_end(&reader_end, stream, buf_init(NULL, 0), on_reader_end_ack);
        assert(rc == OK);
        return;
    }

    if (read_since_start >= STOP_AFTER) {
        stream_read_stop(stream);
        stopped = true;
        nstops++;
        timer_start(&start_timer, STOP_US);
    }
}

int main(int argc, char **argv) {
    for (int i = 0; i < WRITE_SIZE; i++) {
        pattern[i] = i * 7 + i / 256;
    }

    loop_init(&loop);

    struct sockaddr_in send_addr = {.sin_family = AF_INET, .sin_port = htons(8090)};
    struct sockaddr_in recv_addr = {.sin_family = AF_INET, .sin_port = htons(8091)};

    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    send_sock = socket_bind(&loop, (sockaddr_t *)&send_addr);
    assert(send_sock != NULL);
    recv_sock = socket_bind(&loop, (sockaddr_t *)&recv_addr);
    assert(recv_sock != NULL);

    timeout_timer.handler = timeout;
    start_timer.handler = start_reading;
    timer_start(&timeout_timer, 10 * 1000 * 1000); // 10 seconds

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);

    reader_fd = reader->sock->fd;

    reader->user_read = on_read;

    for (int i = 0; i < NWRITES; i++) {
        byte_buf_t b = buf_init(pattern, WRITE_SIZE);
        int rc = i == NWRITES - 1 ? stream_write_end(&writes[i], writer, b, on_write_ack)
                                  : stream_write(&writes[i], writer, b, on_write_ack);
        assert(rc == OK);
    }

    int rc = loop_run(&loop);
    assert(rc == 0);

    timer_stop(&timeout_timer);
    timer_stop(&start_timer);

    printf("read=%" PRIu64 " stops=%d window_full=%d updates_sent=%d "
           "updates_dropped=%d updates_resent=%d\n",
           nbytes_read, nstops, nwindow_full, nupdates_sent, nupdates_dropped,
           nupdates_resent);

    assert(nwrites_acked == NWRITES);
    assert(nbytes_read == NBYTES);
    assert(reader_got_end && reader_end_acked);
    assert(nstops >= NBYTES / STOP_AFTER - 1);

    // the window, not the reader's speed, held the writer back
    assert(nwindow_full > 0);

    // the window updates were lost, and the reader sent them again
    assert(nupdates_dropped > 0);
    assert(nupdates_resent >= nupdates_dropped);

    return OK;
}
