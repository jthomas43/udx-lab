#include <assert.h>
#include <inttypes.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include "../all.h"

// writes 1mb of random data in 16k chunks, a few at a time, writing the next
// chunk from the on_ack of an earlier one. the last chunk carries END. the
// reader hashes what it reads and ends its side when it reads END

#define NBYTES (1024 * 1024)
#define CHUNK (16 * 1024)
#define NCHUNKS (NBYTES / CHUNK)

// chunks written but not acked yet
#define MAX_OUTSTANDING 8

// from libudx's test/helpers.h
#define HASH_INIT 5381

static inline uint64_t hash(uint64_t prev, uint8_t *data, int len) {
    uint64_t hash = prev;

    for (int i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + data[i];
    }

    return hash;
}

// the user owns written memory until it's acked, so each chunk is its own
// allocation, freed in on_ack. if the stream touched a chunk after acking it,
// asan would catch it
typedef struct {
    write_t w; // first, so the write_t is the chunk
    int index;
    uint8_t data[CHUNK];
} chunk_t;

loop_t loop;
socket_t *send_sock;
socket_t *recv_sock;

stream_t *writer;
stream_t *reader;

event_t timer;

uint64_t write_hash = HASH_INIT;
uint64_t read_hash = HASH_INIT;

size_t nbytes_read;
int nchunks_written;
int nchunks_acked;
int outstanding;

bool reader_got_end;
bool reader_end_acked;
write_t reader_end;

uint32_t rng_state;

static uint32_t rng(void) {
    // xorshift32
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 17;
    rng_state ^= rng_state << 5;
    return rng_state;
}

void timeout(event_t *event) {
    printf("timed out\n");
    loop.terminate = true;
}

static void check_done(void) {
    if (nchunks_acked == NCHUNKS && reader_got_end && reader_end_acked) {
        loop.terminate = true;
    }
}

static void on_chunk_ack(write_t *w, uint64_t offset, int status);

static void write_chunk(void) {
    chunk_t *c = malloc(sizeof(chunk_t));
    assert(c != NULL);

    c->index = nchunks_written;

    for (int i = 0; i < CHUNK; i += 4) {
        uint32_t r = rng();
        memcpy(c->data + i, &r, 4);
    }

    write_hash = hash(write_hash, c->data, CHUNK);

    byte_buf_t b = buf_init(c->data, CHUNK);
    int rc;

    if (nchunks_written == NCHUNKS - 1) {
        rc = stream_write_end(&c->w, writer, b, on_chunk_ack);
    } else {
        rc = stream_write(&c->w, writer, b, on_chunk_ack);
    }

    assert(rc == OK);

    nchunks_written++;
    outstanding++;
}

static void on_chunk_ack(write_t *w, uint64_t offset, int status) {
    chunk_t *c = (chunk_t *)w;

    assert(status == OK);

    // acks come in write order, each at the end of its chunk
    assert(c->index == nchunks_acked);
    assert(offset == (uint64_t)(c->index + 1) * CHUNK);

    nchunks_acked++;
    outstanding--;

    free(c);

    while (nchunks_written < NCHUNKS && outstanding < MAX_OUTSTANDING) {
        write_chunk();
    }

    check_done();
}

static void on_reader_end_ack(write_t *w, uint64_t offset, int status) {
    assert(status == OK);
    assert(offset == 0);

    reader_end_acked = true;

    check_done();
}

static void on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    assert(!reader_got_end);

    read_hash = hash(read_hash, bytes, len);
    nbytes_read += len;

    if (end) {
        reader_got_end = true;

        int rc = stream_write_end(&reader_end, stream, buf_init(NULL, 0),
                                  on_reader_end_ack);
        assert(rc == OK);
    }
}

int main(int argc, char **argv) {
    rng_state = (uint32_t)time(NULL) | 1;
    printf("seed=%u\n", rng_state);

    loop_init(&loop);

    struct sockaddr_in send_addr;

    send_addr.sin_family = AF_INET;
    send_addr.sin_port = htons(8082);
    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    send_sock = socket_bind(&loop, (sockaddr_t *)&send_addr);
    assert(send_sock != NULL);

    struct sockaddr_in recv_addr;

    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(8083);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    recv_sock = socket_bind(&loop, (sockaddr_t *)&recv_addr);
    assert(recv_sock != NULL);

    timer.handler = timeout;
    timer.data = NULL;
    timer_start(&timer, 10 * 1000 * 1000); // 10 seconds

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);

    reader->user_read = on_read;

    while (nchunks_written < MAX_OUTSTANDING) {
        write_chunk();
    }

    int rc = loop_run(&loop);
    assert(rc == 0);

    if (timer.timer_set) {
        timer_stop(&timer);
    }

    printf("written=%d acked=%d read=%zu write_hash=%" PRIx64
           " read_hash=%" PRIx64 "\n",
           nchunks_written, nchunks_acked, nbytes_read, write_hash, read_hash);

    assert(nchunks_acked == NCHUNKS);
    assert(nbytes_read == NBYTES);
    assert(read_hash == write_hash);
    assert(reader_got_end && reader_end_acked);

    return OK;
}
