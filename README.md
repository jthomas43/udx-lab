# udx-lab

UDX experiments that

- require platform support that libuv doesn't (yet) offer
- have dubious value
- or may establish a new ceiling for performance

## protocol changes

- each packet gets a new number, eliminating transmission ambiguity
- data packets carry the 64bit offset of their data
  - allows re-packetization of inflight data
  - receiver enforces the receive window
- receive window sets a new max_offset that increases monotonically

todo:
- 16-bit relative sack
- ack-delay field


## implementation changes

- custom event loop
- per-socket and per-stream memory pools
- bbr v3 (WIP). doesn't (yet) implement undo or extra_acked (ack aggregation
  compensation)

## linux-specific

- microsecond timer resolution
- connect-over-unconnected, so each ip:port:ip:port tuple gets its own buffer
  - also saves a route lookup on transmit, and decouples the stream lifetime
    from the originating unconnected socket

todo:
- GSO
- offload pacer to kernel with SO_TXTIME
- kernel timestamping
- ICMP error handling - ECN, ECONNREFUSED, EMTU, ...
- SO_RXQ_OVFL

## Building and testing

```
make test     # builds and run tests in test/, with asan.
make qdxperf  # an optimized throughput tool, build/qdxperf
```

![BBR on a 10 MB/s link with a 4 ms RTT](stream-bbr-10mb-4ms-40kb-rates.png)

`stream-bbr-10mb-4ms-40kb`: BBR leaves Startup in a few round trips with
`max_bw` at the link's rate, and goodput stays at the 9.73 MB/s of payload the
link carries. The pacing rate goes to 1.25x in each ProbeBW_UP and 0.9x in
ProbeBW_DOWN. Goodput doesn't drop during DOWN: the queue built up in CRUISE
keeps the link busy while it drains. The strip under the time axis is BBR's
state.
