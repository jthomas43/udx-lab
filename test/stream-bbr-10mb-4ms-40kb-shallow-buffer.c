#include "bbr-link.h"

// the 10 MB/s, 4 ms rtt, 40 KB bdp link with a buffer of half the bdp.
// startup and each probe up overflow it, so bbr has to learn the safe
// inflight from loss (inflight_longterm) and still fill the link

int main(int argc, char **argv) {
    link_config_t cfg = {
        .name = "stream-bbr-10mb-4ms-40kb-shallow-buffer",
        .rate = 10 * 1000 * 1000,
        .delay_us = 2000,
        .buffer_bytes = 20 * 1000,
        .nbytes = 320 * WRITE_SIZE, // about 2 s on this link
        .seed = argc > 1 ? strtoul(argv[1], NULL, 10) : (uint32_t)time(NULL) | 1,
    };
    link_result_t res;

    link_run(&cfg, &res);

    double payload_rate = PAYLOAD_RATE(cfg.rate);
    double loss_rate = res.delivered_bytes ? (double)res.lost_bytes / res.delivered_bytes : 0;

    link_check(res.done, "every byte was delivered, in order");
    link_check(res.steady_ts != 0, "left startup and drain for probe bw");
    link_check(res.goodput >= 0.90 * payload_rate, "goodput %.2f MB/s >= 90%% of the link's %.2f",
               res.goodput / 1e6, payload_rate / 1e6);
    link_check(res.max_bw >= 0.95 * payload_rate && res.max_bw <= 1.10 * payload_rate,
               "max_bw %.2f MB/s within -5%%/+10%% of the link's %.2f", res.max_bw / 1e6,
               payload_rate / 1e6);
    link_check(loss_rate <= 0.02, "lost %.2f%% of delivered bytes, <= bbr's 2%% loss threshold",
               loss_rate * 100);

    return link_failed;
}
