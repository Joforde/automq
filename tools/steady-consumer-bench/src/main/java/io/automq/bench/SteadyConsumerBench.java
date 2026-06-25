/*
 * Steady (long-lived) consumer load generator for Kafka / AutoMQ.
 *
 * Purpose
 * -------
 * Produce a controllable, *stable* Fetch / FetchConsumer QPS - the other
 * request type (besides Metadata-alltopics) whose large responses can actually
 * drive the Network Processor towards saturation. Unlike rebalance-storm-bench
 * (short-lived consumers, churns Coordinator), these consumers are long-lived
 * and just poll() forever.
 *
 * Two modes decouple Fetch from the group protocol:
 *   - mode=assign    : consumer.assign() a slice of partitions. NO group
 *                      membership => NO JoinGroup/Heartbeat/FindCoordinator.
 *                      Pure Fetch load. Use this to push Fetch QPS without
 *                      polluting the other API counters.
 *   - mode=subscribe : consumer.subscribe(topic) in a shared group. Produces
 *                      Fetch + Heartbeat + (initial) JoinGroup/FindCoordinator.
 *                      Use a small consumer count here to dial Heartbeat to a
 *                      target (Heartbeat/s = consumers * 1000/heartbeat.interval.ms).
 *
 * Fetch math
 * ----------
 * Each polling consumer issues ~ (1000 / fetch.max.wait.ms) Fetch req/s when
 * there is no data ready (long-poll returns on the wait timeout). So:
 *   - fetch.max.wait.ms=500 -> ~2 Fetch/s/consumer  (110 consumers ~= 218/s)
 *   - fetch.max.wait.ms=100 -> ~10 Fetch/s/consumer (22  consumers ~= 218/s)
 * Lower fetch.min.bytes / larger fetch.max.bytes to grow the response size when
 * probing the Network Processor.
 */
package io.automq.bench;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class SteadyConsumerBench {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        cfg.print();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong polls = new AtomicLong();
        AtomicLong records = new AtomicLong();
        AtomicLong bytes = new AtomicLong();

        // Discover partitions once (for assign mode round-robin distribution).
        List<TopicPartition> allPartitions = discoverPartitions(cfg);
        if (cfg.mode.equals("assign") && allPartitions.isEmpty()) {
            throw new IllegalStateException("assign mode: no partitions found for topic " + cfg.topic);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            running.set(false);
        }));

        ExecutorService pool = Executors.newFixedThreadPool(cfg.consumers, r -> {
            Thread t = new Thread(r);
            t.setDaemon(false);
            return t;
        });

        for (int i = 0; i < cfg.consumers; i++) {
            final int id = i;
            // assign mode: give each consumer a round-robin slice of partitions.
            List<TopicPartition> slice = new ArrayList<>();
            if (cfg.mode.equals("assign")) {
                for (int p = id; p < allPartitions.size(); p += cfg.consumers) {
                    slice.add(allPartitions.get(p));
                }
            }
            pool.execute(() -> runConsumer(id, cfg, slice, running, polls, records, bytes));
        }

        long startNs = System.nanoTime();
        long prevPolls = 0, prevRecords = 0, prevBytes = 0, prevNs = startNs;
        while (running.get()) {
            Thread.sleep(1000);
            long now = System.nanoTime();
            long curPolls = polls.get(), curRecords = records.get(), curBytes = bytes.get();
            double dt = (now - prevNs) / 1e9;
            System.out.printf(
                "[%4ds] fetch(poll)/s: %,.0f | records/s: %,.0f | MB/s: %,.1f | consumers=%d mode=%s%n",
                (now - startNs) / 1_000_000_000L,
                (curPolls - prevPolls) / dt,
                (curRecords - prevRecords) / dt,
                (curBytes - prevBytes) / dt / 1e6,
                cfg.consumers, cfg.mode);
            prevPolls = curPolls;
            prevRecords = curRecords;
            prevBytes = curBytes;
            prevNs = now;
        }

        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        System.out.printf("%nStopped after %.1fs. total polls=%,d records=%,d%n",
            (System.nanoTime() - startNs) / 1e9, polls.get(), records.get());
    }

    private static void runConsumer(int id, Config cfg, List<TopicPartition> slice,
                                    AtomicBoolean running, AtomicLong polls,
                                    AtomicLong records, AtomicLong bytes) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(id, cfg))) {
            if (cfg.mode.equals("assign")) {
                consumer.assign(slice);
                if (cfg.seekToEnd) {
                    consumer.seekToEnd(slice);
                }
            } else {
                consumer.subscribe(Collections.singletonList(cfg.topic));
            }

            while (running.get()) {
                ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(cfg.pollMs));
                polls.incrementAndGet();
                if (!recs.isEmpty()) {
                    records.addAndGet(recs.count());
                    long b = 0;
                    for (var r : recs) {
                        b += (r.serializedKeySize() < 0 ? 0 : r.serializedKeySize())
                            + (r.serializedValueSize() < 0 ? 0 : r.serializedValueSize());
                    }
                    bytes.addAndGet(b);
                }
            }
        } catch (Exception e) {
            System.err.printf("consumer-%d error: %s%n", id, e.getMessage());
        }
    }

    private static List<TopicPartition> discoverPartitions(Config cfg) {
        Properties p = consumerProps(-1, cfg);
        // Discovery doesn't need a group; use assign-style props.
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            List<PartitionInfo> infos = c.partitionsFor(cfg.topic, Duration.ofSeconds(30));
            if (infos != null) {
                for (PartitionInfo pi : infos) {
                    tps.add(new TopicPartition(pi.topic(), pi.partition()));
                }
            }
            return tps;
        }
    }

    private static Properties consumerProps(int id, Config cfg) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "steady-consumer-" + (id < 0 ? "discover" : id));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, cfg.fetchMaxWaitMs);
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, cfg.fetchMinBytes);
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, cfg.fetchMaxBytes);
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, cfg.maxPartitionFetchBytes);
        // Only relevant in subscribe mode, but harmless otherwise.
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId);
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, cfg.sessionTimeoutMs);
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, cfg.heartbeatIntervalMs);
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, cfg.maxPollIntervalMs);
        cfg.extra.forEach(p::put);
        return p;
    }

    static final class Config {
        String bootstrap = "localhost:9092";
        String topic = "big-test";
        String groupId = "steady-consumer-bench";
        String mode = "assign";          // assign | subscribe
        int consumers = 110;
        long pollMs = 500;
        int fetchMaxWaitMs = 500;        // ~ (1000/this) fetch req/s/consumer when idle
        int fetchMinBytes = 1;
        int fetchMaxBytes = 52428800;    // 50 MiB - large response when data is present
        int maxPartitionFetchBytes = 1048576;
        boolean seekToEnd = false;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;  // subscribe mode: Heartbeat/s = consumers * 1000/this
        int maxPollIntervalMs = 300000;
        Properties extra = new Properties();

        static Config parse(String[] args) {
            Config c = new Config();
            for (String a : args) {
                int eq = a.indexOf('=');
                if (eq < 0) {
                    if (a.equals("-h") || a.equals("--help")) {
                        usage();
                        System.exit(0);
                    }
                    continue;
                }
                String k = a.substring(0, eq).replaceFirst("^--", "");
                String v = a.substring(eq + 1);
                switch (k) {
                    case "bootstrap":
                    case "bootstrap.servers":
                        c.bootstrap = v; break;
                    case "topic":
                        c.topic = v; break;
                    case "group.id":
                        c.groupId = v; break;
                    case "mode":
                        c.mode = v; break;
                    case "consumers":
                        c.consumers = Integer.parseInt(v); break;
                    case "poll.ms":
                        c.pollMs = Long.parseLong(v); break;
                    case "fetch.max.wait.ms":
                        c.fetchMaxWaitMs = Integer.parseInt(v); break;
                    case "fetch.min.bytes":
                        c.fetchMinBytes = Integer.parseInt(v); break;
                    case "fetch.max.bytes":
                        c.fetchMaxBytes = Integer.parseInt(v); break;
                    case "max.partition.fetch.bytes":
                        c.maxPartitionFetchBytes = Integer.parseInt(v); break;
                    case "seek.to.end":
                        c.seekToEnd = Boolean.parseBoolean(v); break;
                    case "session.timeout.ms":
                        c.sessionTimeoutMs = Integer.parseInt(v); break;
                    case "heartbeat.interval.ms":
                        c.heartbeatIntervalMs = Integer.parseInt(v); break;
                    case "max.poll.interval.ms":
                        c.maxPollIntervalMs = Integer.parseInt(v); break;
                    default:
                        c.extra.put(k, v);
                }
            }
            if (!c.mode.equals("assign") && !c.mode.equals("subscribe")) {
                throw new IllegalArgumentException("mode must be assign | subscribe");
            }
            if (c.consumers < 1) {
                throw new IllegalArgumentException("consumers must be >= 1");
            }
            return c;
        }

        void print() {
            double fetchPerConsumer = 1000.0 / Math.max(1, fetchMaxWaitMs);
            System.out.printf(
                "SteadyConsumerBench: bootstrap=%s topic=%s mode=%s consumers=%d%n"
                    + "  fetch.max.wait.ms=%d (~%.1f fetch/s/consumer idle => ~%.0f fetch/s total)%n"
                    + "  fetch.max.bytes=%d group.id=%s heartbeat.interval.ms=%d%n",
                bootstrap, topic, mode, consumers,
                fetchMaxWaitMs, fetchPerConsumer, fetchPerConsumer * consumers,
                fetchMaxBytes, groupId, heartbeatIntervalMs);
            if (mode.equals("subscribe")) {
                System.out.printf("  subscribe mode => Heartbeat/s ~= %.2f%n",
                    consumers * 1000.0 / heartbeatIntervalMs);
            }
        }

        static void usage() {
            System.out.println("Usage: java -jar steady-consumer-bench.jar [key=value ...]\n"
                + "  bootstrap=localhost:9092   Kafka/AutoMQ bootstrap servers\n"
                + "  topic=big-test             Topic to consume\n"
                + "  mode=assign                assign (pure Fetch, no group) | subscribe (Fetch+Heartbeat)\n"
                + "  consumers=110              Number of long-lived consumer threads\n"
                + "  poll.ms=500                poll() timeout\n"
                + "  fetch.max.wait.ms=500      Lower => more Fetch req/s/consumer\n"
                + "  fetch.min.bytes=1          Raise to force batching\n"
                + "  fetch.max.bytes=52428800   Max bytes per fetch response (grow for big responses)\n"
                + "  max.partition.fetch.bytes=1048576\n"
                + "  seek.to.end=false          assign mode: skip to tail (avoid replaying backlog)\n"
                + "  group.id=steady-consumer-bench  (subscribe mode)\n"
                + "  heartbeat.interval.ms=3000 (subscribe mode) Heartbeat/s = consumers*1000/this\n"
                + "  <any>=<val>                Extra KafkaConsumer config\n");
        }
    }

    private SteadyConsumerBench() {
    }
}
