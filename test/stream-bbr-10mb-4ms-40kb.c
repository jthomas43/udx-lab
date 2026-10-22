#include "bbr-link.h"

// bbr over a 10 MB/s link with a 4 ms rtt, so a 40 KB bdp, behind a
// bdp-sized buffer. it should find the link's rate and lose nothing
//
// the queue isn't empty: the max_bw filter keeps the highest of noisy rate
// samples, so it runs a few percent over the link and pacing at 99% of it
// builds a queue until the next ProbeBW_DOWN drains it. probing again every
// min(bdp, 63) rounds, about 130 ms here, keeps that to a fraction of the
// bdp. the 64 KB receive window also caps inflight at about 1.6 bdp

int main(int argc, char **argv) {
    link_config_t cfg = {
        .name = "stream-bbr-10mb-4ms-40kb",
        .rate = 10 * 1000 * 1000,
        .delay_us = 2000,
        .buffer_bytes = 40 * 1000,
        .nbytes = 320 * WRITE_SIZE, // about 2 s on this link
        .seed = argc > 1 ? strtoul(argv[1], NULL, 10) : (uint32_t)time(NULL) | 1,
    };
    link_result_t res;

    link_run(&cfg, &res);

    double payload_rate = PAYLOAD_RATE(cfg.rate);
    double bdp = cfg.rate * 2.0 * cfg.delay_us / 1000000;

    link_check(res.done, "every byte was delivered, in order");
    link_check(res.steady_ts != 0, "left startup and drain for probe bw");
    link_check(res.goodput >= 0.95 * payload_rate, "goodput %.2f MB/s >= 95%% of the link's %.2f",
               res.goodput / 1e6, payload_rate / 1e6);
    link_check(res.max_bw >= 0.95 * payload_rate && res.max_bw <= 1.10 * payload_rate,
               "max_bw %.2f MB/s within -5%%/+10%% of the link's %.2f", res.max_bw / 1e6,
               payload_rate / 1e6);
    link_check(res.min_rtt_us >= PATH_RTT_US(cfg) && res.min_rtt_us <= PATH_RTT_US(cfg) + 500,
               "min_rtt %u us within 500 us above the path's %u", res.min_rtt_us, PATH_RTT_US(cfg));
    link_check(res.mean_queue <= 0.6 * bdp, "mean queue %.1f KB <= 60%% of the bdp, %.1f KB",
               res.mean_queue / 1024, 0.6 * bdp / 1024);
    link_check(res.max_queue <= bdp, "max queue %.1f KB <= the bdp", res.max_queue / 1024.0);
    link_check(res.tail_drops == 0, "no drops at the bottleneck, %u", res.tail_drops);

    return link_failed;
}
