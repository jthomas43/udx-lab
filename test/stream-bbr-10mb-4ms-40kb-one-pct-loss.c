#include "bbr-link.h"

// the 10 MB/s, 4 ms rtt, 40 KB bdp link, with a bdp-sized buffer, dropping
// 1% of data packets at random
//
// bbr's loss threshold is 2% of the data in flight when a packet was sent.
// the bdp here is about 34 packets, so a single loss in a round is 2.9%: 1%
// random loss reads as congestion often enough to cut most ProbeBW_UP phases
// short and lower bw_shortterm in CRUISE. bbr gets 70-85% of the link. a
// loss-based (reno) flow gets about 1.22 / sqrt(p) packets per rtt, 36% here

int main(int argc, char **argv) {
    link_config_t cfg = {
        .name = "stream-bbr-10mb-4ms-40kb-one-pct-loss",
        .rate = 10 * 1000 * 1000,
        .delay_us = 2000,
        .buffer_bytes = 40 * 1000,
        .loss = 0.01,
        .nbytes = 320 * WRITE_SIZE, // about 2 s on this link
        .seed = argc > 1 ? strtoul(argv[1], NULL, 10) : (uint32_t)time(NULL) | 1,
    };
    link_result_t res;

    link_run(&cfg, &res);

    double payload_rate = PAYLOAD_RATE(cfg.rate);

    link_check(res.done, "every byte was delivered, in order");
    link_check(res.steady_ts != 0, "left startup and drain for probe bw");
    link_check(res.goodput >= 0.65 * payload_rate, "goodput %.2f MB/s >= 65%% of the link's %.2f",
               res.goodput / 1e6, payload_rate / 1e6);
    // max_bw sags between probes, so check that a probe found the link
    link_check(res.peak_max_bw >= 0.95 * payload_rate && res.peak_max_bw <= 1.10 * payload_rate,
               "peak max_bw %.2f MB/s within -5%%/+10%% of the link's %.2f",
               res.peak_max_bw / 1e6, payload_rate / 1e6);

    return link_failed;
}
