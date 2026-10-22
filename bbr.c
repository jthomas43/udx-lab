#include <stdlib.h>

#include "all.h"

// bbr congestion control, draft-ietf-ccwg-bbr-06. sizes are payload bytes,
// like stream->inflight_bytes, rates are bytes/sec, times are us. section
// numbers are the draft's
//
// left out for now: undo after spurious loss recovery (5.5.11), and the
// ack aggregation estimate (5.5.9). bbr.extra_acked stays 0

// C.SMSS: the most payload a packet carries, with the one sack that always
// fits and the receive window
#define BBR_SMSS (PROTO_MAX_DATA - sizeof(sack_t) - PROTO_MAX_DATA_FIELD_SIZE)
// rfc 9002 7.2
#define BBR_INITIAL_CWND (10 * BBR_SMSS)
#define BBR_MIN_PIPE_CWND (4 * BBR_SMSS)

#define BBR_STARTUP_PACING_GAIN 2.77
#define BBR_DRAIN_PACING_GAIN 0.5
#define BBR_DEFAULT_CWND_GAIN 2.0
#define BBR_PROBE_RTT_CWND_GAIN 0.5
#define BBR_PACING_MARGIN_PERCENT 1

#define BBR_LOSS_THRESH 0.02
#define BBR_BETA 0.7
#define BBR_HEADROOM 0.15
#define BBR_STARTUP_FULL_LOSS_CNT 6

#define BBR_MIN_RTT_FILTER_LEN_US (10 * 1000 * 1000)
#define BBR_PROBE_RTT_DURATION_US (200 * 1000)
#define BBR_PROBE_RTT_INTERVAL_US (5 * 1000 * 1000)

// "Infinity" in the draft
#define BBR_INF UINT64_MAX
#define BBR_INF_RTT UINT32_MAX

static uint64_t min_u64(uint64_t a, uint64_t b) { return a < b ? a : b; }
static uint64_t max_u64(uint64_t a, uint64_t b) { return a > b ? a : b; }

// C.cwnd. stream.c limits sending in packets, so keep stream->max_inflight
// in step, rounding up so a partial packet's worth still allows one
static uint64_t cwnd(stream_t *stream) { return stream->bbr.max_inflight_bytes; }

static void set_cwnd_bytes(stream_t *stream, uint64_t bytes) {
    stream->bbr.max_inflight_bytes = bytes;
    stream->max_inflight = (bytes + BBR_SMSS - 1) / BBR_SMSS;
}

// gain * bw * min_rtt, in bytes
static uint64_t BDP_multiple(stream_t *stream, uint64_t bw, double gain) {
    if (stream->bbr.min_rtt_us == BBR_INF_RTT) {
        return BBR_INITIAL_CWND; // no rtt sample yet
    }

    return gain * ((double)bw * stream->bbr.min_rtt_us / 1000000);
}

// lost packets are a prefix of pbuf, until push resends them. a lost
// packet waiting for its resend stands in for fast recovery
static bool in_loss_recovery(stream_t *stream) {
    packet_t *pkt = pbuf_get(&stream->packets, stream->snd_unacked);

    return stream->snd_unacked != stream->snd_next_pnum && pkt && pkt->lost;
}

static bool is_in_a_probe_BW_state(stream_t *stream) {
    return stream->bbr.state == BBR_PROBE_BW_DOWN ||
           stream->bbr.state == BBR_PROBE_BW_CRUISE ||
           stream->bbr.state == BBR_PROBE_BW_REFILL ||
           stream->bbr.state == BBR_PROBE_BW_UP;
}

static bool is_probing_BW(stream_t *stream) {
    return stream->bbr.state == BBR_STARTUP ||
           stream->bbr.state == BBR_PROBE_BW_REFILL ||
           stream->bbr.state == BBR_PROBE_BW_UP;
}

// 4.1.2.4 application-limited phases

// no new data, and no bare END, waiting to be packetized
static bool no_unsent_data(stream_t *stream) {
    return stream->snd_next_offset == stream->snd_write_offset &&
           stream->send_state != SEND_STATE_END_QUEUED;
}

static void mark_connection_app_limited(stream_t *stream) {
    stream->app_limited = max_u64(stream->delivered_bytes + stream->inflight_bytes, 1);
}

// nothing queues below the stream, so C.pending_transmissions is 0. lost
// packets are resent as soon as push runs, so C.lost_out > C.retrans_out
// only while one waits
static void check_if_application_limited(stream_t *stream) {
    if (no_unsent_data(stream) &&
        stream->inflight_bytes < cwnd(stream) &&
        !in_loss_recovery(stream)) {
        mark_connection_app_limited(stream);
    }
}

// 5.5.1 packet-timed round trips

static void start_round(stream_t *stream) {
    stream->bbr.next_round_delivered = stream->delivered_bytes;
}

static void init_round_counting(stream_t *stream) {
    stream->bbr.next_round_delivered = 0;
    stream->bbr.round_start = false;
    stream->bbr.round_count = 0;
}

// the draft checks P.delivered of each acked packet. the newest one's is
// the rate sample's prior_delivered_bytes
static void update_round(stream_t *stream, rate_sample_t *rs) {
    if (rs->prior_delivered_bytes >= stream->bbr.next_round_delivered) {
        start_round(stream);
        stream->bbr.round_count++;
        stream->bbr.rounds_since_bw_probe_up++;
        stream->bbr.round_start = true;
    } else {
        stream->bbr.round_start = false;
    }
}

// 5.5.3 - 5.5.6 max_bw. the window is 2 probe bw cycles, so the filter is a
// slot for the previous cycle and one for this cycle

static void update_max_bw(stream_t *stream, rate_sample_t *rs) {
    update_round(stream, rs);

    if (rs->delivery_rate > 0 &&
        (rs->delivery_rate >= stream->bbr.max_bw || !rs->is_app_limited)) {
        uint64_t *filter = stream->bbr.max_bw_filter;

        filter[1] = max_u64(filter[1], rs->delivery_rate);
        stream->bbr.max_bw = max_u64(filter[0], filter[1]);
    }
}

static void advance_max_bw_filter(stream_t *stream) {
    uint64_t *filter = stream->bbr.max_bw_filter;

    stream->bbr.cycle_count++;
    filter[0] = filter[1];
    filter[1] = 0;
    stream->bbr.max_bw = filter[0];
}

// 5.3.4.3 min_rtt, folded into probe rtt's scheduling: probe_rtt_min_delay
// is the min over the last ProbeRTTInterval, min_rtt over MinRTTFilterLen

static void update_min_RTT(stream_t *stream, rate_sample_t *rs) {
    uint64_t now = now_ts;

    stream->bbr.is_probe_rtt_expired =
        now > stream->bbr.probe_rtt_min_delay_ts + BBR_PROBE_RTT_INTERVAL_US;

    // rtt_us 0 is no rtt sample
    if (rs->rtt_us > 0 &&
        (rs->rtt_us < stream->bbr.probe_rtt_min_delay_us ||
         stream->bbr.is_probe_rtt_expired)) {
        stream->bbr.probe_rtt_min_delay_us = rs->rtt_us;
        stream->bbr.probe_rtt_min_delay_ts = now;
    }

    bool min_rtt_expired = now > stream->bbr.min_rtt_ts + BBR_MIN_RTT_FILTER_LEN_US;

    if (stream->bbr.probe_rtt_min_delay_us < stream->bbr.min_rtt_us ||
        min_rtt_expired) {
        stream->bbr.min_rtt_us = stream->bbr.probe_rtt_min_delay_us;
        stream->bbr.min_rtt_ts = stream->bbr.probe_rtt_min_delay_ts;
    }
}

// 5.6.4.2 max_inflight

// QuantizationBudget. offload_budget is C.send_quantum, for quic (5.5.8.2)
static uint64_t quantization_budget(stream_t *stream, uint64_t inflight_cap) {
    uint64_t offload_budget = (uint64_t)stream->snd_quantum * BBR_SMSS;

    inflight_cap = max_u64(inflight_cap, offload_budget);
    inflight_cap = max_u64(inflight_cap, BBR_MIN_PIPE_CWND);

    if (stream->bbr.state == BBR_PROBE_BW_UP) {
        inflight_cap += 2 * BBR_SMSS;
    }

    return inflight_cap;
}

// Inflight(bw, gain)
static uint64_t inflight(stream_t *stream, uint64_t bw, double gain) {
    return quantization_budget(stream, BDP_multiple(stream, bw, gain));
}

static void update_max_inflight(stream_t *stream) {
    uint64_t inflight_cap = BDP_multiple(stream, stream->bbr.bw, stream->bbr.max_inflight_gain);

    inflight_cap += stream->bbr.extra_acked;
    stream->bbr.max_inflight = quantization_budget(stream, inflight_cap);
}

// leave free headroom in the bottleneck for other flows
static uint64_t inflight_with_headroom(stream_t *stream) {
    uint64_t longterm = stream->bbr.inflight_longterm;

    if (longterm == BBR_INF) {
        return BBR_INF;
    }

    uint64_t headroom = max_u64(BBR_SMSS, BBR_HEADROOM * longterm);

    if (headroom >= longterm) {
        return BBR_MIN_PIPE_CWND;
    }

    return max_u64(longterm - headroom, BBR_MIN_PIPE_CWND);
}

// our estimated bdp, unless congestion cut cwnd
static uint64_t target_inflight(stream_t *stream) {
    return min_u64(stream->bbr.bdp, cwnd(stream));
}

// 5.3.1 startup

static void enter_startup(stream_t *stream) {
    stream->bbr.state = BBR_STARTUP;
    stream->bbr.pacing_gain = BBR_STARTUP_PACING_GAIN;
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
}

static void reset_full_BW(stream_t *stream) {
    stream->bbr.full_bw = 0;
    stream->bbr.full_bw_count = 0;
    stream->bbr.full_bw_now = false;
}

static void check_full_BW_reached(stream_t *stream, rate_sample_t *rs) {
    if (stream->bbr.full_bw_now || !stream->bbr.round_start || rs->is_app_limited) {
        return;
    }

    // bw is still growing
    if (rs->delivery_rate >= stream->bbr.full_bw * 1.25) {
        reset_full_BW(stream);
        stream->bbr.full_bw = rs->delivery_rate;
        return;
    }

    // another round without much growth
    stream->bbr.full_bw_count++;
    stream->bbr.full_bw_now = stream->bbr.full_bw_count >= 3;

    if (stream->bbr.full_bw_now) {
        stream->bbr.full_bw_reached = true;
    }
}

// 5.5.10.2
static bool is_inflight_too_high(rate_sample_t *rs) {
    return rs->lost_bytes > rs->tx_in_flight_bytes * BBR_LOSS_THRESH;
}

// 5.3.1.3: leave startup after a round with loss over LossThresh, in at least
// StartupFullLossCnt discontiguous ranges. the draft also wants a full round
// in fast recovery. the stream has no recovery episodes, so this goes by the
// loss round alone
static void check_startup_high_loss(stream_t *stream, rate_sample_t *rs) {
    if (stream->bbr.state != BBR_STARTUP || stream->bbr.full_bw_reached) {
        return;
    }

    if (stream->bbr.loss_round_start &&
        stream->bbr.loss_events_in_round >= BBR_STARTUP_FULL_LOSS_CNT &&
        is_inflight_too_high(rs)) {
        stream->bbr.full_bw_reached = true;
        stream->bbr.inflight_longterm = max_u64(stream->bbr.bdp, stream->bbr.inflight_latest);
    }

    if (stream->bbr.loss_round_start) {
        stream->bbr.loss_events_in_round = 0;
    }
}

static void enter_drain(stream_t *stream);

static void check_startup_done(stream_t *stream, rate_sample_t *rs) {
    check_startup_high_loss(stream, rs);

    if (stream->bbr.state == BBR_STARTUP && stream->bbr.full_bw_reached) {
        enter_drain(stream);
    }
}

// 5.3.2 drain

static void enter_drain(stream_t *stream) {
    stream->bbr.state = BBR_DRAIN;
    stream->bbr.pacing_gain = BBR_DRAIN_PACING_GAIN; // pace slowly
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
    stream->bbr.drain_start_round = stream->bbr.round_count;
}

static void enter_probe_BW(stream_t *stream);

static void check_drain_done(stream_t *stream) {
    if (stream->bbr.state == BBR_DRAIN &&
        (stream->inflight_bytes <= inflight(stream, stream->bbr.bw, 1.0) ||
         stream->bbr.round_count > stream->bbr.drain_start_round + 3)) {
        enter_probe_BW(stream);
    }
}

// 5.5.10.3 congestion signals and the short-term model

static void reset_congestion_signals(stream_t *stream) {
    stream->bbr.is_loss_in_round = false;
    stream->bbr.bw_latest = 0;
    stream->bbr.inflight_latest = 0;
}

static void reset_short_term_model(stream_t *stream) {
    stream->bbr.bw_shortterm = BBR_INF;
    stream->bbr.inflight_shortterm = BBR_INF;
}

// near the start of ack processing
static void update_latest_delivery_signals(stream_t *stream, rate_sample_t *rs) {
    stream->bbr.loss_round_start = false;
    stream->bbr.bw_latest = max_u64(stream->bbr.bw_latest, rs->delivery_rate);
    stream->bbr.inflight_latest = max_u64(stream->bbr.inflight_latest, rs->delivered_bytes);

    if (rs->prior_delivered_bytes >= stream->bbr.loss_round_delivered) {
        stream->bbr.loss_round_delivered = stream->delivered_bytes;
        stream->bbr.loss_round_start = true;
    }
}

// near the end of ack processing
static void advance_latest_delivery_signals(stream_t *stream, rate_sample_t *rs) {
    if (stream->bbr.loss_round_start) {
        stream->bbr.bw_latest = rs->delivery_rate;
        stream->bbr.inflight_latest = rs->delivered_bytes;
    }
}

// the first congestion episode in this cycle
static void init_lower_bounds(stream_t *stream) {
    if (stream->bbr.bw_shortterm == BBR_INF) {
        stream->bbr.bw_shortterm = stream->bbr.max_bw;
    }

    if (stream->bbr.inflight_shortterm == BBR_INF) {
        stream->bbr.inflight_shortterm = cwnd(stream);
    }
}

// once per round with loss
static void loss_lower_bounds(stream_t *stream) {
    stream->bbr.bw_shortterm = max_u64(stream->bbr.bw_latest,
                                       BBR_BETA * stream->bbr.bw_shortterm);
    stream->bbr.inflight_shortterm = max_u64(stream->bbr.inflight_latest,
                                             BBR_BETA * stream->bbr.inflight_shortterm);
}

static void adapt_lower_bounds_from_congestion(stream_t *stream) {
    if (is_probing_BW(stream)) {
        return;
    }

    if (stream->bbr.is_loss_in_round) {
        init_lower_bounds(stream);
        loss_lower_bounds(stream);
    }
}

static void update_congestion_signals(stream_t *stream, rate_sample_t *rs) {
    update_max_bw(stream, rs);

    // wait until the end of the round trip
    if (!stream->bbr.loss_round_start) {
        return;
    }

    adapt_lower_bounds_from_congestion(stream);
    stream->bbr.is_loss_in_round = false;
}

static void bound_BW_for_model(stream_t *stream) {
    stream->bbr.bw = min_u64(stream->bbr.max_bw, stream->bbr.bw_shortterm);
    stream->bbr.bdp = BDP_multiple(stream, stream->bbr.bw, 1.0);
}

// 5.3.3 probe bw

// how long to wait before probing for bandwidth, in rounds and wall clock
static void pick_probe_wait(stream_t *stream) {
    stream->bbr.rounds_since_bw_probe_up = rand() % 2;
    stream->bbr.bw_probe_wait_us = 2 * 1000 * 1000 + rand() % (1000 * 1000);
}

static bool has_elapsed_in_phase(stream_t *stream, uint64_t interval_us) {
    return now_ts > stream->bbr.bw_probe_start_ts + interval_us;
}

// the draft counts reno's rounds in packets
static bool is_reno_coexistence_probe_time(stream_t *stream) {
    uint64_t reno_rounds = target_inflight(stream) / BBR_SMSS;
    uint64_t rounds = min_u64(reno_rounds, 63);

    return (uint64_t)stream->bbr.rounds_since_bw_probe_up >= rounds;
}

// the gains for each state are the table in 5.6.1

static void start_probe_BW_DOWN(stream_t *stream) {
    reset_congestion_signals(stream);
    stream->bbr.bw_probe_up_acked_per_inc = BBR_INF; // not growing inflight_longterm
    pick_probe_wait(stream);
    stream->bbr.bw_probe_start_ts = now_ts; // start wall clock
    stream->bbr.ack_phase = ACK_PROBE_STOPPING;
    start_round(stream);
    stream->bbr.state = BBR_PROBE_BW_DOWN;
    stream->bbr.pacing_gain = 0.9;
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
}

static void start_probe_BW_CRUISE(stream_t *stream) {
    stream->bbr.state = BBR_PROBE_BW_CRUISE;
    stream->bbr.pacing_gain = 1.0;
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
}

static void start_probe_BW_REFILL(stream_t *stream) {
    reset_short_term_model(stream);
    stream->bbr.bw_probe_up_rounds = 0;
    stream->bbr.bw_probe_up_acked = 0;
    stream->bbr.prev_bw_probe_precautionary = false;
    stream->bbr.ack_phase = ACK_REFILLING;
    start_round(stream);
    stream->bbr.state = BBR_PROBE_BW_REFILL;
    stream->bbr.pacing_gain = 1.0;
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
}

// raise inflight_longterm's slope: 1, 2, 4, ... SMSS per round
static void raise_inflight_longterm_slope(stream_t *stream) {
    uint64_t growth_this_round = 1ull << stream->bbr.bw_probe_up_rounds;

    stream->bbr.bw_probe_up_rounds = stream->bbr.bw_probe_up_rounds + 1 < 30
                                         ? stream->bbr.bw_probe_up_rounds + 1
                                         : 30;
    stream->bbr.bw_probe_up_acked_per_inc = max_u64(cwnd(stream) / growth_this_round, BBR_SMSS);
}

static void start_probe_BW_UP(stream_t *stream, rate_sample_t *rs) {
    stream->bbr.ack_phase = ACK_PROBE_STARTING;
    start_round(stream);
    reset_full_BW(stream);
    stream->bbr.full_bw = rs->delivery_rate;
    stream->bbr.state = BBR_PROBE_BW_UP;
    stream->bbr.pacing_gain = 1.25;
    stream->bbr.max_inflight_gain = 2.25;
    raise_inflight_longterm_slope(stream);
}

static void enter_probe_BW(stream_t *stream) {
    stream->bbr.max_inflight_gain = BBR_DEFAULT_CWND_GAIN;
    start_probe_BW_DOWN(stream);
}

// time to go from DOWN or CRUISE to REFILL?
static bool is_time_to_probe_BW(stream_t *stream) {
    if (has_elapsed_in_phase(stream, stream->bbr.bw_probe_wait_us) ||
        is_reno_coexistence_probe_time(stream)) {
        start_probe_BW_REFILL(stream);
        return true;
    }

    return false;
}

// time to go from DOWN to CRUISE?
static bool is_time_to_cruise(stream_t *stream) {
    if (stream->inflight_bytes > inflight_with_headroom(stream)) {
        return false; // not enough headroom
    }

    if (stream->inflight_bytes > inflight(stream, stream->bbr.max_bw, 1.0)) {
        return false; // above the estimated bdp
    }

    return true;
}

// time to go from UP to DOWN?
static bool is_time_to_go_down(stream_t *stream, rate_sample_t *rs) {
    // precautionary bandwidth probing: deceleration
    if (stream->bbr.prev_bw_probe_too_high &&
        stream->inflight_bytes >= stream->bbr.inflight_longterm) {
        stream->bbr.prev_bw_probe_precautionary = true;
        return true;
    }

    if (stream->is_max_inflight_limited && cwnd(stream) >= stream->bbr.inflight_longterm) {
        // bw is limited by inflight_longterm
        reset_full_BW(stream);
        stream->bbr.full_bw = rs->delivery_rate;
    } else if (stream->bbr.full_bw_now) {
        return true; // we estimate we've fully used the path's bw
    }

    return false;
}

static void probe_inflight_longterm_upward(stream_t *stream, rate_sample_t *rs) {
    // not fully using inflight_longterm, so don't grow it
    if (!stream->is_max_inflight_limited || cwnd(stream) < stream->bbr.inflight_longterm) {
        return;
    }

    stream->bbr.bw_probe_up_acked += rs->new_acked_bytes;

    if (stream->bbr.bw_probe_up_acked >= stream->bbr.bw_probe_up_acked_per_inc) {
        uint64_t delta = stream->bbr.bw_probe_up_acked / stream->bbr.bw_probe_up_acked_per_inc;

        stream->bbr.bw_probe_up_acked -= delta * stream->bbr.bw_probe_up_acked_per_inc;
        stream->bbr.inflight_longterm += delta * BBR_SMSS;
    }

    if (stream->bbr.round_start) {
        raise_inflight_longterm_slope(stream);
    }
}

// track the ack phase, and advance the max_bw window and inflight_longterm.
// returns true if it decided a state transition
static bool adapt_long_term_model(stream_t *stream, rate_sample_t *rs) {
    // starting to get bw probing samples
    if (stream->bbr.ack_phase == ACK_PROBE_STARTING && stream->bbr.round_start) {
        stream->bbr.ack_phase = ACK_PROBE_FEEDBACK;
    }

    // end of samples from the bw probing phase
    if (stream->bbr.ack_phase == ACK_PROBE_STOPPING && stream->bbr.round_start) {
        stream->bbr.is_bw_probe_sample = false;
        stream->bbr.ack_phase = ACK_INIT;

        if (is_in_a_probe_BW_state(stream) && !rs->is_app_limited) {
            advance_max_bw_filter(stream);
        }

        // precautionary bandwidth probing: acceleration
        if (is_in_a_probe_BW_state(stream) &&
            stream->bbr.prev_bw_probe_precautionary &&
            !stream->bbr.prev_bw_probe_too_high) {
            start_probe_BW_REFILL(stream);
            return true;
        }
    }

    // the loss rate is safe. adjust the upper bounds upward
    if (!is_inflight_too_high(rs)) {
        if (stream->bbr.inflight_longterm == BBR_INF) {
            return false; // no upper bounds to raise
        }

        if (rs->tx_in_flight_bytes > stream->bbr.inflight_longterm) {
            stream->bbr.inflight_longterm = rs->tx_in_flight_bytes;
        }

        if (stream->bbr.state == BBR_PROBE_BW_UP) {
            probe_inflight_longterm_upward(stream, rs);
        }
    }

    return false;
}

static void update_probe_BW_cycle_phase(stream_t *stream, rate_sample_t *rs) {
    // only steady-state behavior here
    if (!stream->bbr.full_bw_reached) {
        return;
    }

    if (adapt_long_term_model(stream, rs)) {
        return;
    }

    switch (stream->bbr.state) {
    case BBR_PROBE_BW_DOWN:
        if (is_time_to_probe_BW(stream)) {
            return;
        }

        if (is_time_to_cruise(stream)) {
            start_probe_BW_CRUISE(stream);
        }
        break;

    case BBR_PROBE_BW_CRUISE:
        is_time_to_probe_BW(stream);
        break;

    case BBR_PROBE_BW_REFILL:
        // after one round of REFILL, start UP
        if (stream->bbr.round_start) {
            stream->bbr.is_bw_probe_sample = true;
            start_probe_BW_UP(stream, rs);
        }
        break;

    case BBR_PROBE_BW_UP:
        if (is_time_to_go_down(stream, rs)) {
            stream->bbr.prev_bw_probe_too_high = false; // no high loss (yet)
            start_probe_BW_DOWN(stream);
        }
        break;

    default:
        break; // not a probe bw state
    }
}

// 5.6.4.4 last known good cwnd, kept over probe rtt

static void save_cwnd(stream_t *stream) {
    if (!in_loss_recovery(stream) && stream->bbr.state != BBR_PROBE_RTT) {
        stream->bbr.prior_cwnd = cwnd(stream);
    } else {
        stream->bbr.prior_cwnd = max_u64(stream->bbr.prior_cwnd, cwnd(stream));
    }
}

static void restore_cwnd(stream_t *stream) {
    set_cwnd_bytes(stream, max_u64(cwnd(stream), stream->bbr.prior_cwnd));
}

// 5.3.4 probe rtt

static void enter_probe_RTT(stream_t *stream) {
    stream->bbr.state = BBR_PROBE_RTT;
    stream->bbr.pacing_gain = 1.0;
    stream->bbr.max_inflight_gain = BBR_PROBE_RTT_CWND_GAIN;
}

static void exit_probe_RTT(stream_t *stream) {
    reset_short_term_model(stream);

    if (stream->bbr.full_bw_reached) {
        // inflight is already below the bdp, so skip DOWN
        start_probe_BW_DOWN(stream);
        start_probe_BW_CRUISE(stream);
    } else {
        enter_startup(stream);
    }
}

static uint64_t probe_RTT_cwnd(stream_t *stream) {
    uint64_t probe_rtt_cwnd = BDP_multiple(stream, stream->bbr.bw, BBR_PROBE_RTT_CWND_GAIN);

    return max_u64(probe_rtt_cwnd, BBR_MIN_PIPE_CWND);
}

static void check_probe_RTT_done(stream_t *stream) {
    if (stream->bbr.probe_rtt_done_ts != 0 && now_ts > stream->bbr.probe_rtt_done_ts) {
        // schedule the next probe rtt
        stream->bbr.probe_rtt_min_delay_ts = now_ts;
        restore_cwnd(stream);
        exit_probe_RTT(stream);
    }
}

static void handle_probe_RTT(stream_t *stream) {
    // ignore low rate samples during probe rtt
    mark_connection_app_limited(stream);

    if (stream->bbr.probe_rtt_done_ts == 0 &&
        stream->inflight_bytes <= probe_RTT_cwnd(stream)) {
        // wait at least ProbeRTTDuration, and at least one round
        stream->bbr.probe_rtt_done_ts = now_ts + BBR_PROBE_RTT_DURATION_US;
        stream->bbr.probe_rtt_round_done = false;
        start_round(stream);
    } else if (stream->bbr.probe_rtt_done_ts != 0) {
        if (stream->bbr.round_start) {
            stream->bbr.probe_rtt_round_done = true;
        }

        if (stream->bbr.probe_rtt_round_done) {
            check_probe_RTT_done(stream);
        }
    }
}

static void check_probe_RTT(stream_t *stream, rate_sample_t *rs) {
    if (stream->bbr.state != BBR_PROBE_RTT &&
        stream->bbr.is_probe_rtt_expired &&
        !stream->bbr.is_idle_restart) {
        enter_probe_RTT(stream);
        save_cwnd(stream);
        stream->bbr.probe_rtt_done_ts = 0;
        stream->bbr.ack_phase = ACK_PROBE_STOPPING;
        start_round(stream);
    }

    if (stream->bbr.state == BBR_PROBE_RTT) {
        handle_probe_RTT(stream);
    }

    if (rs->delivered_bytes > 0) {
        stream->bbr.is_idle_restart = false;
    }
}

// 5.6.2 pacing rate, 5.6.3 send quantum

static void init_pacing_rate(stream_t *stream) {
    uint64_t srtt_us = stream->srtt_us ? stream->srtt_us : 1000;
    uint64_t nominal_bandwidth = BBR_INITIAL_CWND * 1000000 / srtt_us;

    stream->bbr.pacing_rate = BBR_STARTUP_PACING_GAIN * nominal_bandwidth;
}

static void set_pacing_rate_with_gain(stream_t *stream, double pacing_gain) {
    uint64_t rate = pacing_gain * stream->bbr.bw * (100 - BBR_PACING_MARGIN_PERCENT) / 100;

    if (stream->bbr.full_bw_reached || rate > stream->bbr.pacing_rate) {
        stream->bbr.pacing_rate = rate;
    }
}

static void set_pacing_rate(stream_t *stream) {
    set_pacing_rate_with_gain(stream, stream->bbr.pacing_gain);
}

// stream.c's snd_quantum is packets
static void set_send_quantum(stream_t *stream) {
    uint64_t send_quantum = stream->bbr.pacing_rate / 1000; // 1ms worth

    send_quantum = min_u64(send_quantum, 64 * 1024);
    send_quantum = max_u64(send_quantum, 2 * BBR_SMSS);

    stream->snd_quantum = send_quantum / BBR_SMSS;
}

// 5.6.4 cwnd

static void bound_cwnd_for_probe_RTT(stream_t *stream, uint64_t *c) {
    if (stream->bbr.state == BBR_PROBE_RTT) {
        *c = min_u64(*c, probe_RTT_cwnd(stream));
    }
}

// the volume caps in the table in 5.6.1
static void bound_cwnd_for_model(stream_t *stream, uint64_t *c) {
    uint64_t cap = BBR_INF;

    if (is_in_a_probe_BW_state(stream) && stream->bbr.state != BBR_PROBE_BW_CRUISE) {
        cap = stream->bbr.inflight_longterm;
    } else if (stream->bbr.state == BBR_PROBE_RTT || stream->bbr.state == BBR_PROBE_BW_CRUISE) {
        cap = inflight_with_headroom(stream);
    }

    // inflight_shortterm may be infinite
    cap = min_u64(cap, stream->bbr.inflight_shortterm);
    cap = max_u64(cap, BBR_MIN_PIPE_CWND);
    *c = min_u64(*c, cap);
}

static void set_cwnd(stream_t *stream, rate_sample_t *rs) {
    uint64_t c = cwnd(stream);

    update_max_inflight(stream);

    if (stream->bbr.full_bw_reached) {
        c = min_u64(c + rs->new_acked_bytes, stream->bbr.max_inflight);
    } else if (c < stream->bbr.max_inflight || stream->delivered_bytes < BBR_INITIAL_CWND) {
        c += rs->new_acked_bytes;
    }

    c = max_u64(c, BBR_MIN_PIPE_CWND);
    bound_cwnd_for_probe_RTT(stream, &c);
    bound_cwnd_for_model(stream, &c);

    set_cwnd_bytes(stream, c);
}

// 5.2.3 per-ack steps. the rate sample is already generated

static void update_model_and_state(stream_t *stream, rate_sample_t *rs) {
    update_latest_delivery_signals(stream, rs);
    update_congestion_signals(stream, rs);
    check_full_BW_reached(stream, rs);
    check_startup_done(stream, rs);
    check_drain_done(stream);
    update_probe_BW_cycle_phase(stream, rs);
    update_min_RTT(stream, rs);
    check_probe_RTT(stream, rs);
    advance_latest_delivery_signals(stream, rs);
    bound_BW_for_model(stream);
}

static void update_control_parameters(stream_t *stream, rate_sample_t *rs) {
    set_pacing_rate(stream);
    set_send_quantum(stream);
    set_cwnd(stream, rs);
}

// 5.5.10.2 loss while probing

static void handle_inflight_too_high(stream_t *stream, rate_sample_t *rs) {
    stream->bbr.prev_bw_probe_too_high = true;
    stream->bbr.is_bw_probe_sample = false; // only react once per bw probe

    if (!rs->is_app_limited) {
        stream->bbr.inflight_longterm = max_u64(rs->tx_in_flight_bytes,
                                                target_inflight(stream) * BBR_BETA);
    }

    if (stream->bbr.state == BBR_PROBE_BW_UP) {
        start_probe_BW_DOWN(stream);
    }
}

// the inflight at which losses crossed LossThresh, partway through pkt
static uint64_t inflight_at_loss(packet_t *pkt, rate_sample_t *rs) {
    double size = pkt->len;
    double inflight_prev = rs->tx_in_flight_bytes - size; // in flight before pkt
    double lost_prev = rs->lost_bytes - size;             // lost before pkt
    double lost_prefix = (BBR_LOSS_THRESH * inflight_prev - lost_prev) / (1 - BBR_LOSS_THRESH);

    if (lost_prefix < 0) {
        lost_prefix = 0;
    }

    return inflight_prev + lost_prefix;
}

static void note_loss(stream_t *stream) {
    // first loss in this round trip
    if (!stream->bbr.is_loss_in_round) {
        stream->bbr.loss_round_delivered = stream->delivered_bytes;
    }

    stream->bbr.is_loss_in_round = true;
}

// 5.2.1
void bbr_init(stream_t *stream) {
    stream->bbr.max_bw_filter[0] = 0;
    stream->bbr.max_bw_filter[1] = 0;
    stream->bbr.max_bw = 0;
    stream->bbr.min_rtt_us = stream->srtt_us ? stream->srtt_us : BBR_INF_RTT;
    stream->bbr.min_rtt_ts = now_ts;
    // not in the draft's OnInit, but update_min_RTT compares against it
    stream->bbr.probe_rtt_min_delay_us = stream->bbr.min_rtt_us;
    stream->bbr.probe_rtt_min_delay_ts = now_ts;
    stream->bbr.probe_rtt_done_ts = 0;
    stream->bbr.probe_rtt_round_done = false;
    stream->bbr.prior_cwnd = 0;
    stream->bbr.is_idle_restart = false;
    stream->bbr.extra_acked = 0;
    stream->bbr.full_bw_reached = false;
    stream->bbr.inflight_longterm = BBR_INF;
    stream->bbr.bw_probe_up_acked_per_inc = BBR_INF;
    stream->bbr.bw_probe_up_acked = 0;
    stream->bbr.bw_probe_up_rounds = 0;
    stream->bbr.rounds_since_bw_probe_up = 0;
    stream->bbr.is_bw_probe_sample = false;
    stream->bbr.prev_bw_probe_too_high = false;
    stream->bbr.prev_bw_probe_precautionary = false;
    stream->bbr.ack_phase = ACK_INIT;
    stream->bbr.bw_probe_start_ts = 0;
    stream->bbr.bw_probe_wait_us = 0;
    stream->bbr.loss_round_delivered = 0;
    stream->bbr.loss_round_start = false;
    stream->bbr.loss_events_in_round = 0;

    // the transport's initial cwnd (5.6.4.1)
    set_cwnd_bytes(stream, BBR_INITIAL_CWND);

    reset_congestion_signals(stream);
    reset_short_term_model(stream);
    init_round_counting(stream);
    reset_full_BW(stream);
    init_pacing_rate(stream);
    set_send_quantum(stream);
    enter_startup(stream);
}

// 5.2.2, 5.4: before sending a packet
void bbr_on_resume_from_idle(stream_t *stream) {
    if (stream->inflight_bytes == 0 && stream->app_limited) {
        stream->bbr.is_idle_restart = true;

        if (is_in_a_probe_BW_state(stream)) {
            set_pacing_rate_with_gain(stream, 1.0);
        } else if (stream->bbr.state == BBR_PROBE_RTT) {
            check_probe_RTT_done(stream);
        }
    }
}

// 5.2.3 UpdateOnACK, after rate_sample_generate
void bbr_congestion_control(stream_t *stream, rate_sample_t *rs) {
    // nothing newly acked, nothing to update the model with
    if (!rs->has_data) {
        return;
    }

    update_model_and_state(stream, rs);
    update_control_parameters(stream, rs);

    // C.is_cwnd_limited covers one packet-timed round. stream.c sets it
    // when the cwnd stops a send
    if (stream->bbr.round_start) {
        stream->is_max_inflight_limited = false;
    }
}

// 5.2.4 HandleLostPacket, for each packet marked lost, after its bytes are
// in stream->lost_bytes
void bbr_on_packet_lost(stream_t *stream, packet_t *pkt) {
    // count discontiguous lost ranges, for leaving startup on loss
    if (stream->bbr.loss_events_in_round == 0 ||
        pkt->u.hdr.pnum != stream->bbr.last_lost_pnum + 1) {
        stream->bbr.loss_events_in_round++;
    }

    stream->bbr.last_lost_pnum = pkt->u.hdr.pnum;

    note_loss(stream);

    // not a packet sent while probing bandwidth
    if (!stream->bbr.is_bw_probe_sample) {
        return;
    }

    // judged by this packet's own snapshot, not the newest acked one's
    rate_sample_t rs = {
        .tx_in_flight_bytes = pkt->P.tx_in_flight_bytes, // inflight at transmit
        .lost_bytes = stream->lost_bytes - pkt->P.lost_bytes, // lost since transmit
        .is_app_limited = pkt->P.is_app_limited,
    };

    if (is_inflight_too_high(&rs)) {
        rs.tx_in_flight_bytes = inflight_at_loss(pkt, &rs);
        handle_inflight_too_high(stream, &rs);
    }
}

// 4.1.2.4: on each write, at the start of ack processing, and in timers
// that may send
void bbr_check_app_limited(stream_t *stream) {
    check_if_application_limited(stream);
}
