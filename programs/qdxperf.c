/* includes code from iperf2:
 *
 * ---------------------------------------------------------------
 * Copyright (c) 1999,2000,2001,2002,2003
 * The Board of Trustees of the University of Illinois
 * All Rights Reserved.
 *---------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software (Iperf) and associated
 * documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 *
 * Redistributions of source code must retain the above
 * copyright notice, this list of conditions and
 * the following disclaimers.
 *
 *
 * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following
 * disclaimers in the documentation and/or other materials
 * provided with the distribution.
 *
 *
 * Neither the names of the University of Illinois, NCSA,
 * nor the names of its contributors may be used to endorse
 * or promote products derived from this Software without
 * specific prior written permission.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE CONTIBUTORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * ________________________________________________________________
 * National Laboratory for Applied Network Research
 * National Center for Supercomputing Applications
 * University of Illinois at Urbana-Champaign
 * http://www.ncsa.uiuc.edu
 * ________________________________________________________________
 *
 * some functions stdio.c
 * by Mark Gates <mgates@nlanr.net>
 * and Ajay Tirumalla <tirumala@ncsa.uiuc.edu>
 * -------------------------------------------------------------------
 * input and output numbers, converting with kilo, mega, giga
 * ------------------------------------------------------------------- */

// qdxperf: udxperf from libudx's examples, ported to this stream layer.
// iperf-style throughput test: the client writes as fast as the stream
// takes it for -t seconds, the server reads and reports
//
// differences from udxperf:
// - streams don't refuse writes, so the client caps the bytes it has
//   written but not had acked (MAX_QUEUED) and writes more from on_ack
// - both sides send END. the client exits once its END is acked and it has
//   read the server's. the server keeps listening for more clients
// - no max_bw, there's no bandwidth estimate yet. -x prints the stream's
//   window, rtt and loss counters instead

#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../all.h"

// options

static bool is_server;
static bool is_client;
static bool extra_wanted;
static char *addr_string;
static uint16_t port = 5001;

static uint32_t time_us;
static uint32_t interval_us;

// end options

#define CHUNK 0x10000

// bytes written but not acked. more than enough to keep the window full,
// stream_write never blocks so this is the only backpressure
#define MAX_QUEUED (16 * CHUNK)

static loop_t loop;
static socket_t *sock;

static struct sockaddr_in raddr;

static event_t client_complete_timer;

typedef struct {
    stream_t *stream;
    uint32_t start_ts;
    uint32_t end_ts;
    uint32_t time_last_report;
    uint64_t bytes; // server: read, client: sent
    uint64_t bytes_last_report;
    event_t report_timer;
    write_t end_write;
    bool end_acked;
    bool peer_ended;
} qdxperf_client_t;

static uint32_t client_id = 1;
static uint32_t server_id;

static uint64_t bytes_queued;
static uint64_t bytes_acked;

// every write sends the same bytes, so they share one buffer
static uint8_t chunk_bytes[CHUNK];

// byte printing and conversions
// from iperf2

// e.g.
// char buf[20];
// byte_snprintf(buf, sizeof(buf), 10000, 'A');

/* -------------------------------------------------------------------
 * constants for byte_printf
 * ------------------------------------------------------------------- */

/* used as indices into kConversion[], kLabel_Byte[], and kLabel_bit[] */
enum {
  kConv_Unit,
  kConv_Kilo,
  kConv_Mega,
  kConv_Giga,
  kConv_Tera,
  kConv_Peta
};

/* factor to multiply the number by */
static const double kConversion[] =
  {
    1.0,                                   /* unit */
    1.0 / 1024,                            /* kilo */
    1.0 / 1024 / 1024,                     /* mega */
    1.0 / 1024 / 1024 / 1024,              /* giga */
    1.0 / 1024 / 1024 / 1024 / 1024,       /* tera */
    1.0 / 1024 / 1024 / 1024 / 1024 / 1024 /* peta */
};

/* factor to multiply the number by for bits*/
static const double kConversionForBits[] =
  {
    1.0,                                   /* unit */
    1.0 / 1000,                            /* kilo */
    1.0 / 1000 / 1000,                     /* mega */
    1.0 / 1000 / 1000 / 1000,              /* giga */
    1.0 / 1000 / 1000 / 1000 / 1000,       /* tera */
    1.0 / 1000 / 1000 / 1000 / 1000 / 1000 /* peta */
};

/* labels for Byte formats [KMG] */
static const char *kLabel_Byte[] =
  {
    "Byte",
    "KByte",
    "MByte",
    "GByte",
    "TByte",
    "PByte"
};

/* labels for bit formats [kmg] */
static const char *kLabel_bit[] =
  {
    "bit",
    "Kbit",
    "Mbit",
    "Gbit",
    "Tbit",
    "Pbit"
};

/* -------------------------------------------------------------------
 * byte_snprintf
 *
 * Given a number in bytes and a format, converts the number and
 * prints it out with a bits or bytes label.
 *   B, K, M, G, A, P, T for Byte, Kbyte, Mbyte, Gbyte, Tbyte, Pbyte adaptive byte
 *   b, k, m, g, a, p, t for bit,  Kbit,  Mbit,  Gbit, Tbit, Pbit, adaptive bit
 * adaptive picks the "best" one based on the number.
 * outString should be at least 11 chars long
 * (4 digits + space + 5 chars max + null)
 * ------------------------------------------------------------------- */

static void
byte_snprintf (char *buf, int buf_len, double num, int fmtchar) {
  int conv = 0;
  const char *suffix;
  const char *format;
  if (!isupper(fmtchar)) {
    num *= 8;
  }

  switch (toupper(fmtchar)) {
  case 'B':
    conv = kConv_Unit;
    break;
  case 'K':
    conv = kConv_Kilo;
    break;
  case 'M':
    conv = kConv_Mega;
    break;
  case 'G':
    conv = kConv_Giga;
    break;
  case 'T':
    conv = kConv_Tera;
    break;
  case 'P':
    conv = kConv_Peta;
    break;

  default:
  case 'A': {
    double d = num;
    conv = kConv_Unit;

    if (isupper(fmtchar)) {
      while (d >= 1024.0 && conv < kConv_Peta) {
        d /= 1024.0;
        conv++;
      }
    } else {
      while (d >= 1000.0 && conv < kConv_Peta) {
        d /= 1000.0;
        conv++;
      }
    }
    break;
  }
  }

  if (!isupper(fmtchar)) {
    num *= kConversionForBits[conv];
    suffix = kLabel_bit[conv];
  } else {
    num *= kConversion[conv];
    suffix = kLabel_Byte[conv];
  }

  /* print such that we always fit in 4 places */
  if (num < 9.995) {        /* 9.995 would be rounded to 10.0 */
    format = "%4.2f %s";    /* #.## */
  } else if (num < 99.95) { /* 99.95 would be rounded to 100 */
    format = "%4.1f %s";    /* ##.# */
  } else if (num < 999.5) { /* 999.5 would be rounded to 1000 */
    format = "%4.0f %s";    /*  ### */
  } else {                  /* 1000-1024 fits in 4 places
                             * If not using Adaptive sizes then
                             * this code will not control spaces*/
    format = "%4.0f %s";    /* #### */
  }
  snprintf(buf, buf_len, format, num, suffix);
}

static void print_interval(qdxperf_client_t *c, uint64_t bytes, uint32_t start, uint32_t end) {
    stream_t *stream = c->stream;

    double time_sec = (uint32_t)(end - start) / 1e6;

    char bytes_buf[20];
    byte_snprintf(bytes_buf, sizeof bytes_buf, bytes, 'A');
    bytes_buf[19] = '\0';

    char bps_buf[20];
    byte_snprintf(bps_buf, sizeof bps_buf, bytes / time_sec, 'a');
    bps_buf[19] = '\0';

    printf("[%3u] %6.4f-%6.4f sec %s %s/sec", stream->entry.local_id,
           (uint32_t)(start - c->start_ts) / 1e6,
           (uint32_t)(end - c->start_ts) / 1e6, bytes_buf, bps_buf);

    if (is_client && extra_wanted) {
        printf(" cwnd=%u inflight=%u srtt_us=%u rttvar_us=%u lost=%u delivered=%u",
               stream->max_inflight, stream->snd_inflight, stream->srtt_us,
               stream->rttvar_us, stream->lost, stream->delivered);
    }

    printf("\n");
}

static void report_interval(event_t *ev) {
    qdxperf_client_t *c = ev->data;

    print_interval(c, c->bytes - c->bytes_last_report, c->time_last_report, now_ts);

    c->time_last_report = now_ts;
    c->bytes_last_report = c->bytes;

    timer_start(ev, interval_us);
}

static void start_reports(qdxperf_client_t *c) {
    c->start_ts = now_ts;

    if (interval_us) {
        c->time_last_report = now_ts;
        c->report_timer.data = c;
        c->report_timer.handler = report_interval;
        timer_start(&c->report_timer, interval_us);
    }
}

// server

static void server_on_end_ack(write_t *w, uint64_t offset, int status) {
    qdxperf_client_t *c = w->stream->data;

    // the stream closes itself now that both ENDs are acked
    free(c);
}

static void server_on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    qdxperf_client_t *c = stream->data;

    c->bytes += len;

    if (!end) {
        return;
    }

    c->end_ts = now_ts;
    timer_stop(&c->report_timer);

    print_interval(c, c->bytes, c->start_ts, c->end_ts);

    if (stream_write_end(&c->end_write, stream, buf_init(NULL, 0), server_on_end_ack) != OK) {
        printf("[%3u] stream_write_end failed\n", stream->entry.local_id);
    }
}

// a client sends its id, we reply with ours and both sides create the stream
static void server_handshake(socket_t *sock, uint8_t *bytes, ssize_t len, sockaddr_t *from,
                             packet_info_t *pinfo) {
    if (len != 4) {
        printf("bad handshake!\n");
        return;
    }

    uint32_t remote_id;
    memcpy(&remote_id, bytes, 4);

    server_id++;

    // before replying, so the stream is there for the client's first packet
    stream_t *stream = stream_create(sock, server_id, remote_id, from, pinfo->addr);

    if (stream == NULL) {
        printf("stream_create failed\n");
        return;
    }

    qdxperf_client_t *c = calloc(1, sizeof(qdxperf_client_t));
    assert(c != NULL);

    c->stream = stream;
    stream->data = c;
    stream->user_read = server_on_read;

    socket_send(sock, from, buf_init((uint8_t *)&server_id, 4));

    char lstring[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &((struct sockaddr_in *)pinfo->addr)->sin_addr, lstring, sizeof lstring);
    char rstring[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &((struct sockaddr_in *)from)->sin_addr, rstring, sizeof rstring);

    uint16_t rport = ntohs(((struct sockaddr_in *)from)->sin_port);

    start_reports(c);

    printf("[%3u] %s:%u connected with %s:%u\n", server_id, lstring, port, rstring, rport);
}

static void server(void) {
    struct sockaddr_in laddr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };

    sock = socket_bind(&loop, (sockaddr_t *)&laddr);

    if (sock == NULL) {
        printf("bind: could not bind to port %u\n", port);
        exit(1);
    }

    sock->user_recv = server_handshake;

    printf("Server listening on UDP port %u\n", port);
}

// client

static void client_check_done(qdxperf_client_t *c) {
    // the push after this iteration's events acks the server's END, then
    // the stream closes
    if (c->end_acked && c->peer_ended) {
        loop.terminate = true;
    }
}

static void pump_writes(stream_t *stream);

static void on_ack(write_t *w, uint64_t offset, int status) {
    stream_t *stream = w->stream;

    // offset is where this write ends, so everything before it is acked
    bytes_acked = offset;
    free(w);

    pump_writes(stream);
}

static void pump_writes(stream_t *stream) {
    qdxperf_client_t *c = stream->data;

    // client has ended
    while (!c->end_ts && bytes_queued - bytes_acked < MAX_QUEUED) {
        write_t *w = malloc(sizeof(write_t));
        assert(w != NULL);

        int rc = stream_write(w, stream, buf_init(chunk_bytes, CHUNK), on_ack);

        if (rc != OK) {
            printf("stream_write error=%d\n", rc);
        }
        assert(rc == OK && "stream_write");

        bytes_queued += CHUNK;
    }
}

static void write_end_cb(write_t *w, uint64_t offset, int status) {
    qdxperf_client_t *c = w->stream->data;

    printf("write end\n");

    c->end_acked = true;
    client_check_done(c);
}

static void client_on_read(stream_t *stream, uint8_t *bytes, ssize_t len, bool end) {
    qdxperf_client_t *c = stream->data;

    if (end) {
        c->peer_ended = true;
        client_check_done(c);
    }
}

static void finish_timer_cb(event_t *ev) {
    qdxperf_client_t *c = ev->data;
    stream_t *stream = c->stream;

    c->end_ts = now_ts;

    // bytes packetized so far. what's queued behind them still goes out
    // before END, but isn't counted
    c->bytes = stream->snd_next_offset;

    int rc = stream_write_end(&c->end_write, stream, buf_init(NULL, 0), write_end_cb);
    assert(rc == OK && "stream_write_end");

    print_interval(c, c->bytes, c->start_ts, c->end_ts);

    timer_stop(&c->report_timer);
}

static void client_report_interval(event_t *ev) {
    qdxperf_client_t *c = ev->data;

    c->bytes = c->stream->snd_next_offset;
    report_interval(ev);
}

static void client_handshake_response(socket_t *sock, uint8_t *bytes, ssize_t len,
                                      sockaddr_t *from, packet_info_t *pinfo) {
    if (len != 4) {
        printf("bad handshake response\n");
        return;
    }

    memcpy(&server_id, bytes, 4);

    sock->user_recv = NULL;

    stream_t *stream = stream_create(sock, client_id, server_id, from, pinfo->addr);

    if (stream == NULL) {
        printf("stream_create failed\n");
        exit(1);
    }

    qdxperf_client_t *c = calloc(1, sizeof(qdxperf_client_t));
    assert(c != NULL);

    c->stream = stream;
    stream->data = c;
    stream->user_read = client_on_read;

    start_reports(c);
    c->report_timer.handler = client_report_interval;

    client_complete_timer.data = c;
    client_complete_timer.handler = finish_timer_cb;
    timer_start(&client_complete_timer, time_us);

    pump_writes(stream);
}

// send our client id, await a server id
static void client(void) {
    struct sockaddr_in laddr = {
        .sin_family = AF_INET,
        .sin_port = 0,
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };

    sock = socket_bind(&loop, (sockaddr_t *)&laddr);

    if (sock == NULL) {
        printf("bind failed\n");
        exit(1);
    }

    raddr.sin_family = AF_INET;
    raddr.sin_port = htons(port);

    if (inet_pton(AF_INET, addr_string, &raddr.sin_addr) != 1) {
        printf("error parsing address '%s'\n", addr_string);
        exit(1);
    }

    sock->user_recv = client_handshake_response;

    socket_send(sock, (sockaddr_t *)&raddr, buf_init((uint8_t *)&client_id, 4));

    printf("Client connecting to %s:%u\n", addr_string, port);
}

static void usage(void) {
    printf("qdxperf [-s|-c host] [options]\n");
    printf("  -t secs   client: seconds to transmit for (default 10)\n");
    printf("  -i secs   seconds between interval reports\n");
    printf("  -p port   server port (default 5001)\n");
    printf("  -x        client: print stream internals with each report\n");
    exit(0);
}

typedef enum {
    SW_NONE,
    SW_C,
    SW_T,
    SW_I,
    SW_P
} switch_type_t;

int main(int argc, char **argv) {
    switch_type_t sw = SW_NONE;

    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-' && sw == SW_NONE) {
            switch (argv[i][1]) {
            case 'c':
                sw = SW_C;
                is_client = true;
                time_us = 10 * 1000000;
                break;
            case 't':
                sw = SW_T;
                break;
            case 'i':
                sw = SW_I;
                break;
            case 'p':
                sw = SW_P;
                break;
            case 's':
                is_server = true;
                break;
            case 'x':
                extra_wanted = true;
                break;
            default:
                printf("unrecognized switch '%s'\n", argv[i]);
                usage();
            }
        } else {
            switch (sw) {
            case SW_NONE:
                usage();
                break;
            case SW_C:
                sw = SW_NONE;
                addr_string = argv[i];
                break;
            case SW_T:
                sw = SW_NONE;
                // timers are 32 bit microseconds, so at most ~4000s
                time_us = 1e6 * atof(argv[i]);
                if (time_us == 0) {
                    printf("invalid -t option: %s\n", argv[i]);
                    return 1;
                }
                break;
            case SW_I:
                sw = SW_NONE;
                interval_us = 1e6 * atof(argv[i]);
                if (interval_us == 0) {
                    printf("invalid -i option: %s\n", argv[i]);
                    return 1;
                }
                break;
            case SW_P:
                sw = SW_NONE;
                port = atoi(argv[i]);
                if (port == 0) {
                    printf("invalid -p option: %s\n", argv[i]);
                    return 1;
                }
                break;
            }
        }
    }

    if (is_server == is_client) {
        usage();
    }

    // reports show up as they happen when piped to a file
    setvbuf(stdout, NULL, _IOLBF, 0);

    loop_init(&loop);

    if (is_server) {
        server();
    } else {
        client();
    }

    loop_run(&loop);

    return 0;
}
