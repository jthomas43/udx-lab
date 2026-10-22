#include <netdb.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "../all.h"

loop_t loop;
socket_t *sock;

event_t timer;

void app_shutdown(event_t *event) {
    printf("in shutdown :)\n");
    loop.terminate = true;
}

int main(int argc, char **argv) {
    loop_init(&loop);

    struct sockaddr_in addr;

    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    sock = socket_bind(&loop, (sockaddr_t *)&addr);

    timer.handler = app_shutdown;
    timer.data = NULL;
    // timer.cancelable = 1;
    timer_start(&timer, 500 * 1000); // 1/2 seconds

    loop_run(&loop);

    return OK;
}
