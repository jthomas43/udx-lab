#include <assert.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "../all.h"

// tail loss: the writer's only packet, "hello world" with END, arrives before
// the reader's stream exists and is dropped. nothing comes back to trigger
// loss detection, so only the pto gets it resent

loop_t loop;
socket_t *send_sock;
socket_t *recv_sock;

struct sockaddr_in send_addr;
struct sockaddr_in recv_addr;

stream_t *writer;
stream_t *reader;

bool reader_end_acked;
bool writer_end_acked;
bool reader_got_end;
char received[64];
size_t nreceived;

uint64_t start_ts;
uint64_t writer_acked_ts;

write_t w[2];

event_t create_timer;
event_t timeout_timer;

void timeout(event_t *event) {
    loop.terminate = true;
}

static void on_ack(write_t *wr, uint64_t offset, int status) {
    stream_t *stream = wr->stream;

    if (stream == reader) {
        reader_end_acked = true;
    }

    if (stream == writer) {
        writer_end_acked = true;
        writer_acked_ts = now_ts;
    }

    printf("stream=%s status=%d\n", stream == reader ? "reader" : "writer", status);

    if (reader_end_acked && writer_end_acked) {
        loop.terminate = true;
    }
}

static void on_read(stream_t *stream, uint8_t *p, ssize_t len, bool end) {
    assert(nreceived + len < sizeof(received));
    memcpy(received + nreceived, p, len);
    nreceived += len;

    if (end) {
        reader_got_end = true;
    }
}

// by now the writer's packet has been dropped
void create_reader(event_t *event) {
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);
    reader->user_read = on_read;

    int rc = stream_write_end(&w[0], reader, buf_init(NULL, 0), on_ack);
    assert(rc == 0);
}

int main(int argc, char **argv) {
    loop_init(&loop);

    send_addr.sin_family = AF_INET;
    send_addr.sin_port = htons(8080);
    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    send_sock = socket_bind(&loop, (sockaddr_t *)&send_addr);

    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(8081);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    recv_sock = socket_bind(&loop, (sockaddr_t *)&recv_addr);

    timeout_timer.handler = timeout;
    timer_start(&timeout_timer, 2 * 1000 * 1000); // 2 seconds

    create_timer.handler = create_reader;
    timer_start(&create_timer, 50 * 1000); // 50ms, well inside the 200ms initial pto

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);

    char msg[] = "hello world";
    int rc = stream_write_end(&w[1], writer, buf_init((uint8_t *)msg, strlen(msg)), on_ack);
    assert(rc == 0);

    start_ts = now_ts;

    rc = loop_run(&loop);
    assert(rc == 0);
    timer_stop(&timeout_timer);
    timer_stop(&create_timer);

    printf("writer acked after %lu us\n", (unsigned long)(writer_acked_ts - start_ts));

    assert(reader_end_acked);
    assert(writer_end_acked);
    assert(reader_got_end);
    assert(nreceived == strlen(msg) && memcmp(received, msg, nreceived) == 0);

    return OK;
}
