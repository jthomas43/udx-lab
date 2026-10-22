// struct in6_pktinfo
#define _GNU_SOURCE

#include "all.h"
#include <assert.h>
#include <errno.h>
#include <netdb.h> // getaddrinfo
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

// larger than any udp payload, so datagrams are never truncated. shared by
// every read handler, the loop is single threaded and handlers don't nest
uint8_t recv_buf[RECV_BUF_SIZE];

static void socket_read_handler(event_t *rev);

socket_t *socket_bind(loop_t *loop, sockaddr_t *addr) {

    socklen_t addrlen = addr_to_addrlen(addr);
    int fd = socket(addr->sa_family, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return NULL;
    }

    int on = 1;

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
        perror("setsockopt");
        close(fd);
        return NULL;
    }

    // set reuseport
    // setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &value, sizeof(value))

    // report each packet's destination address, so streams can bind to the
    // address the peer actually used. a v6 socket also gets IP_PKTINFO for
    // v4-mapped packets
    if (setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &on, sizeof(on)) != 0) {
        perror("setsockopt(IP_PKTINFO)");
        close(fd);
        return NULL;
    }

    if (addr->sa_family == AF_INET6 &&
        setsockopt(fd, IPPROTO_IPV6, IPV6_RECVPKTINFO, &on, sizeof(on)) != 0) {
        perror("setsockopt(IPV6_RECVPKTINFO)");
        close(fd);
        return NULL;
    }

    if (bind(fd, addr, addrlen) != 0) {
        close(fd);
        return NULL;
    }

    // the real address, so binding to port 0 records the port the kernel
    // picked. streams bind their connected sockets to it
    struct sockaddr_storage bound;
    socklen_t boundlen = sizeof(bound);

    if (getsockname(fd, (sockaddr_t *)&bound, &boundlen) != 0) {
        perror("getsockname");
        close(fd);
        return NULL;
    }

    addr = (sockaddr_t *)&bound;

    socket_t *sock = get_socket(loop, fd);

    if (sock == NULL) {
        close(fd);
        return NULL;
    }

    sock->family = addr->sa_family;

    sock->addr = palloc(sock->pool, addrlen);

    if (sock->addr == NULL) {
        socket_close(sock);
        return NULL;
    }

    memcpy(sock->addr, addr, addrlen);
    sock->addr_len = addrlen;

    sock->read->handler = socket_read_handler;

    int rc = event_add(sock->read, EPOLLIN | EPOLLET);
    assert(rc == OK);

    return sock;
}

// bind an unconnected socket to the wildcard address on port, trying each
// address getaddrinfo returns. on linux 0.0.0.0 comes first, so this is
// ipv4 in practice
socket_t *socket_bind_local(loop_t *loop, const char *port) {
    struct addrinfo query;
    struct addrinfo *result;

    memset(&query, 0, sizeof(query));

    query.ai_family = AF_UNSPEC;
    query.ai_socktype = SOCK_DGRAM;
    query.ai_flags = AI_PASSIVE | AI_NUMERICSERV;

    int rc = getaddrinfo(NULL, port, &query, &result);

    if (rc != 0) {
        printf("getaddrinfo: %s\n", gai_strerror(rc));
        return NULL;
    }

    socket_t *sock = NULL;

    for (struct addrinfo *p = result; p != NULL; p = p->ai_next) {
        sock = socket_bind(loop, p->ai_addr);

        if (sock != NULL) {
            break;
        }
    }

    freeaddrinfo(result);

    return sock;
}

// nginx: ngx_get_srcaddr_cmsg
// fill local with the socket's own address (for the port), then replace the
// ip with the packet's destination address from the cmsg
static void get_local_addr(socket_t *sock, struct msghdr *msg,
                           struct sockaddr_storage *local) {
    memcpy(local, sock->addr, sock->addr_len);

    for (struct cmsghdr *c = CMSG_FIRSTHDR(msg); c; c = CMSG_NXTHDR(msg, c)) {

        if (c->cmsg_level == IPPROTO_IP && c->cmsg_type == IP_PKTINFO) {
            struct in_pktinfo *pkt = (struct in_pktinfo *)CMSG_DATA(c);

            if (sock->family == AF_INET) {
                struct sockaddr_in *sin = (struct sockaddr_in *)local;
                sin->sin_addr = pkt->ipi_addr;

            } else {
                // v4 packet on a dual-stack v6 socket: ::ffff:a.b.c.d
                struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)local;
                memset(&sin6->sin6_addr, 0, 10);
                memset((uint8_t *)&sin6->sin6_addr + 10, 0xff, 2);
                memcpy((uint8_t *)&sin6->sin6_addr + 12, &pkt->ipi_addr, 4);
            }

        } else if (c->cmsg_level == IPPROTO_IPV6 && c->cmsg_type == IPV6_PKTINFO) {
            struct in6_pktinfo *pkt6 = (struct in6_pktinfo *)CMSG_DATA(c);
            struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)local;

            sin6->sin6_addr = pkt6->ipi6_addr;
        }
    }
}

static void process_packet(socket_t *socket, uint8_t *data, ssize_t len,
                           sockaddr_t *from, packet_info_t *pinfo) {
    // todo: handle 'race type 1': packets that arrived for a stream
    // before it was created - send those packets to that packets stream read handler

    if (socket->user_recv) {
        socket->user_recv(socket, data, len, from, pinfo);
    }
}

// nginx: ngx_event_recvmsg
// edge-triggered, so drain the socket until EAGAIN
static void socket_read_handler(event_t *rev) {
    socket_t *sock = rev->data;

    for (;;) {
        struct sockaddr_storage from;
        struct iovec iov = {.iov_base = recv_buf, .iov_len = sizeof(recv_buf)};

        union {
            struct cmsghdr align;
            uint8_t buf[CMSG_SPACE(sizeof(struct in_pktinfo)) +
                        CMSG_SPACE(sizeof(struct in6_pktinfo))];
        } control;

        // reset every iteration, the kernel overwrites msg_controllen
        struct msghdr msg = {
            .msg_name = &from,
            .msg_namelen = sizeof(from),
            .msg_iov = &iov,
            .msg_iovlen = 1,
            .msg_control = &control,
            .msg_controllen = sizeof(control),
        };

        ssize_t n = recvmsg(sock->fd, &msg, 0);

        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN) {
                printf("recvmsg: %s\n", strerror(errno));
            }
            rev->ready = 0;
            return;
        }

        if (msg.msg_flags & MSG_CTRUNC) {
            printf("recvmsg: control data truncated\n");
        }

        // the cmsg only has the bare ip, so build the full sockaddr here.
        // pinfo points at this stack copy
        struct sockaddr_storage local;
        get_local_addr(sock, &msg, &local);

        packet_info_t pinfo = {
            .addr = (sockaddr_t *)&local,
            .addrlen = sock->addr_len,
        };

        process_packet(sock, recv_buf, n, (sockaddr_t *)&from, &pinfo);

        // the callback may have closed the socket
        if (rev->closed) {
            return;
        }
    }
}

// plain udp send on an unconnected socket, for handshakes. no queueing, a
// full socket buffer just fails
int socket_send(socket_t *sock, sockaddr_t *dst, byte_buf_t b) {
    for (;;) {
        ssize_t n = sendto(sock->fd, b.p, b.len, 0, dst, addr_to_addrlen(dst));

        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            printf("sendto: %s\n", strerror(errno));
            return ERR;
        }

        return OK;
    }
}
int addr_to_addrlen(sockaddr_t *addr) {
    struct sockaddr *sa = (struct sockaddr *)addr;

    switch (sa->sa_family) {
    case AF_INET:
        return sizeof(struct sockaddr_in);
    case AF_INET6:
        return sizeof(struct sockaddr_in6);
    default:
        return 0;
    }
}
