/*
 * Raw (NetworkClient-level) Metadata load generator for Kafka / AutoMQ.
 *
 * Why this exists
 * ---------------
 * metadata-bench drives AdminClient, whose internal NetworkClient hard-caps
 * max.in.flight.requests.per.connection = 1. AutoMQ's SocketServer, however,
 * pipelines up to 8 unacknowledged requests *per connection* before it mutes
 * the channel (see core/.../network/SocketServer.scala:1186 - the check
 * `channelContext.nextCorrelationId.size() >= 8`). AdminClient therefore can
 * never exercise that pipeline path, and cannot reproduce the per-connection
 * concurrency the real server sees in production.
 *
 * This tool talks the raw protocol via org.apache.kafka.clients.NetworkClient
 * with a configurable maxInFlight (default 8) over a *single* TCP connection,
 * so you can directly compare:
 *   - inflight=1  : standard-Kafka style (1 outstanding req, RTT-bound)
 *   - inflight=8  : AutoMQ pipeline (8 outstanding reqs per connection)
 *
 * Each request is a Metadata request:
 *   - mode=cluster   : empty topic list, allowAutoTopicCreation=false -> tiny response
 *   - mode=alltopics : MetadataRequest.Builder.allTopics() -> HUGE response on big-test
 *
 * The alltopics mode is the one that actually loads the Network Processor
 * (serialize + write a large response), so it is the primary tool for probing
 * whether "high Metadata QPS" can drive kafka_network_threads_idle_rate -> 0.
 */
package io.automq.bench;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public final class RawMetadataBench {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        cfg.print();

        // One client per requested connection. Each NetworkClient owns one
        // Selector/TCP connection to the target node and allows up to
        // cfg.maxInflight unacknowledged requests on that connection.
        List<Conn> conns = new ArrayList<>();
        for (int i = 0; i < cfg.connections; i++) {
            conns.add(new Conn(i, cfg));
        }

        final AtomicLong ok = new AtomicLong();
        final AtomicLong err = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        final long startNs = System.nanoTime();
        final long endNs = cfg.durationSec <= 0 ? Long.MAX_VALUE : startNs + cfg.durationSec * 1_000_000_000L;

        Thread reporter = new Thread(() -> {
            long prevOk = 0, prevErr = 0, prevBytes = 0, prevNs = System.nanoTime();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                long now = System.nanoTime();
                long curOk = ok.get(), curErr = err.get(), curBytes = bytes.get();
                double dt = (now - prevNs) / 1e9;
                System.out.printf(
                    "[%4ds] metadata req/s: ok=%,.0f err=%,.0f | resp MB/s=%,.1f | total ok=%,d err=%,d%n",
                    (now - startNs) / 1_000_000_000L,
                    (curOk - prevOk) / dt,
                    (curErr - prevErr) / dt,
                    (curBytes - prevBytes) / dt / 1e6,
                    curOk, curErr);
                prevOk = curOk;
                prevErr = curErr;
                prevBytes = curBytes;
                prevNs = now;
            }
        }, "reporter");
        reporter.setDaemon(true);
        reporter.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            System.out.printf("%nStopped. total ok=%,d err=%,d over %.1fs%n",
                ok.get(), err.get(), (System.nanoTime() - startNs) / 1e9)));

        // Per-connection target spacing for rate pacing.
        final long intervalNs = (long) (1_000_000_000.0 / (cfg.targetRate / cfg.connections));
        long[] nextSendNs = new long[cfg.connections];
        long base = System.nanoTime();
        for (int i = 0; i < cfg.connections; i++) {
            nextSendNs[i] = base;
        }

        // Single-threaded event loop driving all connections. NetworkClient is
        // not thread-safe, so each connection is polled from this one loop.
        while (System.nanoTime() < endNs) {
            long now = System.nanoTime();
            for (int i = 0; i < conns.size(); i++) {
                Conn c = conns.get(i);
                c.ensureReady(now);

                // Fire as many requests as the rate pacer and inflight budget allow.
                while (nextSendNs[i] <= now
                    && c.canSend(now)
                    && c.inflight() < cfg.maxInflight) {
                    c.send(now, cfg, ok, err, bytes);
                    nextSendNs[i] += intervalNs;
                    // Don't let a stalled connection accumulate a huge backlog.
                    if (nextSendNs[i] < now - intervalNs * 1000) {
                        nextSendNs[i] = now;
                    }
                }
                c.poll();
            }
            // Park briefly to avoid a 100% busy spin when idle.
            parkNanos(200_000L);
        }

        conns.forEach(Conn::close);
        System.out.printf("%nDone. total ok=%,d err=%,d%n", ok.get(), err.get());
    }

    /** One raw NetworkClient bound to a single TCP connection to the target node. */
    private static final class Conn {
        final NetworkClient client;
        final Node node;
        final String nodeIdStr;
        int outstanding;

        Conn(int idx, Config cfg) {
            Time time = Time.SYSTEM;
            LogContext logContext = new LogContext("[raw-metadata-bench-" + idx + "] ");
            Metrics metrics = new Metrics(time);

            // Plaintext channel builder. For SSL/SASL this would need the matching
            // SecurityProtocol + an AbstractConfig populated with the security props;
            // plaintext keeps this tool focused on the pipeline behaviour.
            AbstractConfig clientConfig = new EmptyClientConfig(cfg.extra);
            ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
                SecurityProtocol.PLAINTEXT,
                JaasContext.Type.CLIENT,
                clientConfig,
                new ListenerName("PLAINTEXT"),
                "PLAINTEXT",
                time,
                true,
                logContext);

            Selectable selector = new Selector(
                NetworkReceive.UNLIMITED,
                cfg.connectionMaxIdleMs,
                metrics,
                time,
                "raw-metadata-bench",
                Collections.emptyMap(),
                false,
                channelBuilder,
                logContext);

            String[] hostPort = cfg.host.split(":");
            this.node = new Node(idx, hostPort[0], Integer.parseInt(hostPort[1]));
            this.nodeIdStr = Integer.toString(node.id());

            this.client = new NetworkClient(
                selector,
                new ManualMetadataUpdater(Collections.singletonList(node)),
                "raw-metadata-bench-" + idx,
                cfg.maxInflight,              // maxInFlightRequestsPerConnection
                50L,                          // reconnectBackoffMs
                1000L,                        // reconnectBackoffMaxMs
                64 * 1024,                    // socketSendBuffer
                512 * 1024,                   // socketReceiveBuffer
                cfg.requestTimeoutMs,         // defaultRequestTimeoutMs
                1000L,                        // connectionSetupTimeoutMs
                10000L,                       // connectionSetupTimeoutMaxMs
                time,
                true,                         // discoverBrokerVersions
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.NONE);
        }

        void ensureReady(long now) {
            if (!client.isReady(node, now)) {
                client.ready(node, now);
            }
        }

        boolean canSend(long now) {
            return client.isReady(node, now);
        }

        int inflight() {
            return outstanding;
        }

        void send(long now, Config cfg, AtomicLong ok, AtomicLong err, AtomicLong bytes) {
            MetadataRequest.Builder builder = cfg.mode.equals("alltopics")
                ? MetadataRequest.Builder.allTopics()
                : new MetadataRequest.Builder(Collections.emptyList(), false);

            ClientRequest req = client.newClientRequest(
                nodeIdStr, builder, now, true,
                cfg.requestTimeoutMs,
                (ClientResponse resp) -> {
                    outstanding--;
                    if (resp.wasDisconnected() || resp.authenticationException() != null) {
                        err.incrementAndGet();
                    } else {
                        ok.incrementAndGet();
                        bytes.addAndGet(resp.responseBody() == null ? 0
                            : resp.responseBody().toString().length());
                    }
                });
            outstanding++;
            client.send(req, now);
        }

        void poll() {
            client.poll(0, System.nanoTime() / 1_000_000L);
        }

        void close() {
            try {
                client.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }

    private static void parkNanos(long ns) {
        java.util.concurrent.locks.LockSupport.parkNanos(ns);
    }

    /** Minimal AbstractConfig so ChannelBuilders can read (empty) security props. */
    private static final class EmptyClientConfig extends AbstractConfig {
        EmptyClientConfig(Properties extra) {
            super(new org.apache.kafka.common.config.ConfigDef(), toMap(extra), false);
        }

        private static java.util.Map<String, Object> toMap(Properties p) {
            java.util.Map<String, Object> m = new java.util.HashMap<>();
            p.forEach((k, v) -> m.put(String.valueOf(k), v));
            return m;
        }
    }

    private static final class Config {
        String host = "localhost:9092";
        double targetRate = 2000;
        int connections = 1;
        int maxInflight = 8;          // AutoMQ server pipelines up to 8 per connection
        int durationSec = 0;
        int requestTimeoutMs = 10000;
        long connectionMaxIdleMs = 600000;
        String mode = "cluster";      // cluster | alltopics
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
                    case "host":
                        c.host = v; break;
                    case "rate":
                        c.targetRate = Double.parseDouble(v); break;
                    case "connections":
                        c.connections = Integer.parseInt(v); break;
                    case "inflight":
                        c.maxInflight = Integer.parseInt(v); break;
                    case "duration":
                        c.durationSec = Integer.parseInt(v); break;
                    case "timeout":
                        c.requestTimeoutMs = Integer.parseInt(v); break;
                    case "mode":
                        c.mode = v; break;
                    default:
                        c.extra.put(k, v);
                }
            }
            return c;
        }

        void print() {
            System.out.printf(
                "RawMetadataBench: host=%s rate=%.0f/s mode=%s connections=%d inflight=%d duration=%s%n"
                    + "  (inflight=8 reproduces AutoMQ server pipeline; inflight=1 = standard Kafka)%n",
                host, targetRate, mode, connections, maxInflight,
                durationSec <= 0 ? "forever" : durationSec + "s");
        }

        static void usage() {
            System.out.println("Usage: java -jar raw-metadata-bench.jar [key=value ...]\n"
                + "  host=localhost:9092     Single broker host:port to hammer\n"
                + "  rate=2000               Target Metadata requests/s (spread over connections)\n"
                + "  mode=cluster            cluster (tiny resp) | alltopics (HUGE resp on big-test)\n"
                + "  connections=1           Number of TCP connections / NetworkClients\n"
                + "  inflight=8              Max in-flight requests per connection (AutoMQ=8, Kafka=1)\n"
                + "  duration=0              Seconds to run (0 = forever)\n"
                + "  timeout=10000           Request timeout in ms\n"
                + "  <any>=<val>             Extra client config (e.g. SSL props)\n");
        }
    }

    private RawMetadataBench() {
    }
}
