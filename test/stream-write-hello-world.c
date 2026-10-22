#include <assert.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "../all.h"

loop_t loop;
socket_t *send_sock;
socket_t *recv_sock;

stream_t *writer;
stream_t *reader;

write_t writer_message;
write_t reader_end;

bool timed_out;
bool reader_end_acked;
bool writer_end_acked;
size_t bytes_read;

event_t timer;

void timeout(event_t *event) {
    loop.terminate = true;
}

byte_buf_t buf_from_string(char *s) {
    return buf_init((uint8_t *)s, strlen(s));
}

static void on_read(stream_t *stream, uint8_t *bytes, ssize_t status, bool end) {
    assert(status >= 0);
    if (status > 0) {
        size_t len = status;
        printf("read '%.*s' (%ld bytes) %s\n", (int)len, bytes, len, end ? "(END)" : "");
        bytes_read += len;
    }
}
typedef void (*stream_read_cb)(stream_t *stream, uint8_t *bytes, ssize_t status, bool end);

static void on_ack(write_t *w, uint64_t offset, int status) {
    stream_t *stream = w->stream;
    if (stream == reader) {
        reader_end_acked = true;
    }
    if (stream == writer) {
        writer_end_acked = true;
    }

    if (reader_end_acked && writer_end_acked) {
        loop.terminate = true;
    }
}

int main(int argc, char **argv) {
    loop_init(&loop);

    struct sockaddr_in send_addr;

    send_addr.sin_family = AF_INET;
    send_addr.sin_port = htons(8080);
    send_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    send_sock = socket_bind(&loop, (sockaddr_t *)&send_addr);

    struct sockaddr_in recv_addr;

    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(8081);
    recv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    recv_sock = socket_bind(&loop, (sockaddr_t *)&recv_addr);

    timer.handler = timeout;
    timer.data = NULL;
    // timer.cancelable = 1;
    timer_start(&timer, 2 * 1000 * 1000); // 2 seconds

    writer = stream_create(send_sock, 0, 1, (sockaddr_t *)&recv_addr, NULL);
    assert(writer != NULL);
    reader = stream_create(recv_sock, 1, 0, (sockaddr_t *)&send_addr, NULL);
    assert(reader != NULL);

    reader->user_read = on_read;

    write_t w[2];

    int rc = stream_write_end(&w[0], reader, buf_init(NULL, 0), on_ack);
    assert(rc == 0);

    byte_buf_t buf = buf_from_string("hello world");

    rc = stream_write_end(&w[1], writer, buf, on_ack);
    assert(rc == 0);

    rc = loop_run(&loop);
    assert(rc == 0);
    timer_stop(&timer);

    assert(reader_end_acked);
    assert(writer_end_acked);

    return OK;
}
