# steady-consumer-bench

Long-lived consumer load generator. Produces a **stable, controllable Fetch /
FetchConsumer QPS** - the other request type (besides `Metadata` with
`mode=alltopics`) whose large responses can actually push the Network Processor
towards saturation.

Unlike `rebalance-storm-bench` (short-lived consumers that churn the
GroupCoordinator), these consumers are long-lived and just `poll()` forever.

## Two modes (decouple Fetch from the group protocol)

- `mode=assign` - `consumer.assign()` a round-robin slice of partitions.
  **No group membership** => no JoinGroup / Heartbeat / FindCoordinator.
  **Pure Fetch load.** Use this to push Fetch QPS without polluting the other
  API counters.
- `mode=subscribe` - `consumer.subscribe(topic)` in a shared group. Produces
  Fetch + Heartbeat + (initial) JoinGroup/FindCoordinator. Use a small consumer
  count here to dial Heartbeat to a target:
  `Heartbeat/s = consumers * 1000 / heartbeat.interval.ms`.

## Fetch math

Each idle long-poll consumer issues `~ 1000 / fetch.max.wait.ms` Fetch req/s:

| `fetch.max.wait.ms` | fetch/s per consumer | consumers for ~218/s |
|---------------------|----------------------|----------------------|
| 500 | ~2  | ~110 |
| 100 | ~10 | ~22  |

To grow the **response size** (Network-Processor probe), raise `fetch.max.bytes`
/ `max.partition.fetch.bytes` and ensure there is data to read (produce to
`big-test` first, or remove `seek.to.end`).

## Build

```bash
./build.sh          # no maven required
# or: mvn -q clean package
```

## Run

```bash
# Pure Fetch, no group noise - ~218 fetch/s target
CONSUMERS=110 ./run.sh mode=assign fetch.max.wait.ms=500

# Fewer consumers, lower wait -> same QPS, less heap
CONSUMERS=22 ./run.sh mode=assign fetch.max.wait.ms=100

# Subscribe mode to also generate a controlled Heartbeat rate
#   9 consumers * 1000/3000 ~= 3 Heartbeat/s
CONSUMERS=9 ./run.sh mode=subscribe heartbeat.interval.ms=3000

# Big responses to load the Network Processor (needs data in big-test)
CONSUMERS=22 ./run.sh mode=assign fetch.max.wait.ms=100 \
  fetch.max.bytes=52428800 max.partition.fetch.bytes=4194304
```

### Key args

| arg | default | meaning |
|-----|---------|---------|
| `topic` | `big-test` | topic to consume |
| `mode` | `assign` | `assign` (pure Fetch) \| `subscribe` (Fetch+Heartbeat) |
| `consumers` | `110` | long-lived consumer threads |
| `fetch.max.wait.ms` | `500` | lower => more Fetch req/s/consumer |
| `fetch.max.bytes` | `52428800` | max bytes per fetch response |
| `seek.to.end` | `false` | assign mode: skip to tail, avoid replaying backlog |
| `heartbeat.interval.ms` | `3000` | subscribe mode: controls Heartbeat/s |

The reporter prints `fetch(poll)/s`, `records/s`, and `MB/s` every second.

## What to watch on the broker

- `kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Fetch` /
  `request=FetchConsumer` - should track the math above.
- `kafka_network_threads_idle_rate` - drops only when responses are large
  (data present + high `fetch.max.bytes`). With empty/idle long-polls it stays
  near 1.0 even at high Fetch QPS - demonstrating "high API QPS != busy Network".
- `kafka.io.threads.idle.rate` - watch for IO-side pressure on large fetches.
