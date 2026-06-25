# raw-metadata-bench

Raw, `NetworkClient`-level Metadata load generator. Unlike `metadata-bench`
(which drives `AdminClient` and is hard-capped at
`max.in.flight.requests.per.connection = 1`), this tool talks the raw protocol
with a configurable **per-connection in-flight count**, so it can reproduce
AutoMQ's server-side pipeline.

## Why this exists

AutoMQ's `SocketServer` pipelines up to **8** unacknowledged requests per
connection before it mutes the channel:

```
core/src/main/scala/kafka/network/SocketServer.scala:1186
  if (channelContext.nextCorrelationId.size() >= 8 && !channel.isMuted) { ... mute ... }
```

Standard Kafka mutes after **1**. `AdminClient` therefore can never drive that
8-in-flight path. This tool can:

- `inflight=1` -> standard-Kafka style: 1 outstanding request, RTT-bound.
- `inflight=8` -> AutoMQ pipeline: up to 8 outstanding requests per connection.

## Modes

- `mode=cluster`   - empty topic list -> tiny response. Use to push raw req/s.
- `mode=alltopics` - `MetadataRequest.Builder.allTopics()` -> **huge** response
  on `big-test` (9000 partitions). This is the mode that actually loads the
  **Network Processor** (serialize + write a large response). Use it to test
  whether high Metadata QPS can drive `kafka_network_threads_idle_rate -> 0`.

## Build

No maven on this box:

```bash
./build.sh
```

If you have maven: `mvn -q clean package` (same jar).

## Run

```bash
# AutoMQ pipeline, huge responses - primary Network-Processor probe
./run.sh mode=alltopics rate=200 inflight=8 connections=4 duration=120

# Compare: same rate but 1 in-flight (standard Kafka behaviour)
./run.sh mode=alltopics rate=200 inflight=1 connections=4 duration=120

# Raw req/s with tiny responses (Network stays idle, just counts QPS)
./run.sh mode=cluster rate=2000 inflight=8 connections=2
```

### Key args

| arg | default | meaning |
|-----|---------|---------|
| `host` | `localhost:9092` | single broker host:port |
| `rate` | `2000` | target Metadata req/s, spread across connections |
| `mode` | `cluster` | `cluster` (tiny) \| `alltopics` (huge) |
| `connections` | `1` | TCP connections / NetworkClients |
| `inflight` | `8` | max in-flight per connection (AutoMQ=8, Kafka=1) |
| `duration` | `0` | seconds (0 = forever) |

The reporter prints achieved `metadata req/s` and `resp MB/s` every second.

## What to watch on the broker

- `kafka_network_threads_idle_rate` (NetworkProcessorAvgIdlePercent) -> 0 means
  Network Processor saturated. Expect this only with `mode=alltopics`.
- `kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request=Metadata`
  high response-send time = Network-bound.
- Compare `inflight=1` vs `inflight=8` at the same `rate`: the 8-in-flight run
  should reach the target rate with far fewer connections and show the pipeline
  effect.
