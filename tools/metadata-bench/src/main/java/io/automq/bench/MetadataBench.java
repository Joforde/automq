/*
 * Metadata request load generator for Kafka / AutoMQ.
 *
 * It fires Metadata requests at a configurable, steady rate so you can
 * reproduce a high "Metadata req/s" line on the broker dashboard.
 *
 * Each AdminClient call maps 1:1 to exactly one Metadata request on the wire:
 *   - mode=cluster  -> describeCluster()  : lightest, broker returns only node info
 *   - mode=alltopics-> listTopics()       : heavy, broker returns metadata for ALL topics
 *   - mode=topics   -> describeTopics(..) : metadata for a fixed list of topics
 *
 * Requests are sent asynchronously by a single pacer thread; an in-flight
 * semaphore bounds concurrency and applies backpressure if the broker can't
 * keep up, so the achieved rate never overshoots the target.
 */
package io.automq.bench;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public final class MetadataBench {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        cfg.print();

        // Build a small pool of AdminClients. One networking thread usually
        // saturates several thousand cheap metadata req/s; add more for headroom.
        List<Admin> admins = new ArrayList<>();
        for (int i = 0; i < cfg.clients; i++) {
            Properties p = new Properties();
            p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap);
            p.put(AdminClientConfig.CLIENT_ID_CONFIG, "metadata-bench-" + i);
            // Keep request timeout short so a slow broker fails fast instead of piling up.
            p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, cfg.requestTimeoutMs);
            p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, cfg.requestTimeoutMs);
            // Reuse a single connection per broker; we want metadata load, not connect load.
            p.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 600000);
            cfg.extra.forEach(p::put);
            admins.add(Admin.create(p));
        }

        final AtomicLong sent = new AtomicLong();
        final AtomicLong ok = new AtomicLong();
        final AtomicLong err = new AtomicLong();
        final Semaphore inflight = new Semaphore(cfg.maxInflight);
        final long startNs = System.nanoTime();
        final long endNs = cfg.durationSec <= 0 ? Long.MAX_VALUE : startNs + cfg.durationSec * 1_000_000_000L;

        // Reporter: prints achieved QPS every second.
        Thread reporter = new Thread(() -> {
            long prevOk = 0, prevErr = 0;
            long prevNs = System.nanoTime();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return;
                }
                long now = System.nanoTime();
                long curOk = ok.get();
                long curErr = err.get();
                double dt = (now - prevNs) / 1e9;
                double okRate = (curOk - prevOk) / dt;
                double errRate = (curErr - prevErr) / dt;
                System.out.printf(
                    "[%4ds] metadata req/s: ok=%,.0f err=%,.0f | inflight=%d | total ok=%,d err=%,d%n",
                    (now - startNs) / 1_000_000_000L, okRate, errRate,
                    cfg.maxInflight - inflight.availablePermits(), curOk, curErr);
                prevOk = curOk;
                prevErr = curErr;
                prevNs = now;
            }
        }, "reporter");
        reporter.setDaemon(true);
        reporter.start();

        // Pacer: releases tokens at a steady rate (nanosecond spacing).
        final long intervalNs = (long) (1_000_000_000.0 / cfg.targetRate);
        long nextSendNs = System.nanoTime();
        int idx = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            System.out.printf("%nStopped. total ok=%,d err=%,d over %.1fs%n",
                ok.get(), err.get(), (System.nanoTime() - startNs) / 1e9)));

        while (System.nanoTime() < endNs) {
            // Pace to the target rate.
            long now = System.nanoTime();
            long wait = nextSendNs - now;
            if (wait > 0) {
                parkNanos(wait);
            }
            nextSendNs += intervalNs;
            // If we fell badly behind (broker slow), don't try to "catch up" in a burst.
            if (nextSendNs < System.nanoTime() - intervalNs * 1000) {
                nextSendNs = System.nanoTime();
            }

            // Backpressure: cap concurrent in-flight requests.
            if (!inflight.tryAcquire()) {
                inflight.acquire();
            }

            Admin admin = admins.get(idx);
            idx = (idx + 1) % admins.size();

            KafkaFuture<?> f = fire(admin, cfg);
            sent.incrementAndGet();
            f.whenComplete((res, ex) -> {
                inflight.release();
                if (ex == null) {
                    ok.incrementAndGet();
                } else {
                    err.incrementAndGet();
                }
            });
        }

        // Drain outstanding requests.
        inflight.acquire(cfg.maxInflight);
        admins.forEach(Admin::close);
        System.out.printf("%nDone. total ok=%,d err=%,d%n", ok.get(), err.get());
    }

    private static KafkaFuture<?> fire(Admin admin, Config cfg) {
        switch (cfg.mode) {
            case "alltopics":
                return admin.listTopics(new ListTopicsOptions().listInternal(true)).names();
            case "topics":
                return admin.describeTopics(cfg.topics,
                    new DescribeTopicsOptions()).allTopicNames();
            case "cluster":
            default:
                return admin.describeCluster().nodes();
        }
    }

    // LockSupport.parkNanos with a tiny busy-spin tail for sub-ms accuracy.
    private static void parkNanos(long ns) {
        if (ns > 1_500_000L) {
            java.util.concurrent.locks.LockSupport.parkNanos(ns - 1_000_000L);
        } else {
            long deadline = System.nanoTime() + ns;
            while (System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        }
    }

    private static final class Config {
        String bootstrap = "localhost:9092";
        double targetRate = 5000;
        int clients = 2;
        int maxInflight = 2000;
        int durationSec = 0; // 0 = run forever
        int requestTimeoutMs = 10000;
        String mode = "cluster"; // cluster | alltopics | topics
        List<String> topics = new ArrayList<>();
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
                    case "rate":
                        c.targetRate = Double.parseDouble(v); break;
                    case "clients":
                        c.clients = Integer.parseInt(v); break;
                    case "inflight":
                        c.maxInflight = Integer.parseInt(v); break;
                    case "duration":
                        c.durationSec = Integer.parseInt(v); break;
                    case "timeout":
                        c.requestTimeoutMs = Integer.parseInt(v); break;
                    case "mode":
                        c.mode = v; break;
                    case "topics":
                        c.topics = Arrays.asList(v.split(",")); break;
                    default:
                        // pass-through to AdminClient (e.g. security.protocol=SASL_SSL)
                        c.extra.put(k, v);
                }
            }
            if (c.mode.equals("topics") && c.topics.isEmpty()) {
                throw new IllegalArgumentException("mode=topics requires topics=t1,t2,...");
            }
            return c;
        }

        void print() {
            System.out.printf(
                "MetadataBench: bootstrap=%s rate=%.0f/s mode=%s clients=%d inflight=%d duration=%s%n",
                bootstrap, targetRate, mode, clients, maxInflight,
                durationSec <= 0 ? "forever" : durationSec + "s");
            if (mode.equals("topics")) {
                System.out.println("  topics=" + topics);
            }
        }

        static void usage() {
            System.out.println("Usage: java -jar metadata-bench.jar [key=value ...]\n" +
                "  bootstrap=host:9092     Kafka/AutoMQ bootstrap servers (default localhost:9092)\n" +
                "  rate=5000               Target Metadata requests per second\n" +
                "  mode=cluster            cluster | alltopics | topics\n" +
                "  topics=t1,t2            Required when mode=topics\n" +
                "  clients=2               Number of AdminClient instances\n" +
                "  inflight=2000           Max concurrent in-flight requests\n" +
                "  duration=0              Seconds to run (0 = forever)\n" +
                "  timeout=10000           Request timeout in ms\n" +
                "  <any>=<val>             Extra AdminClient config (e.g. security.protocol=SASL_SSL)\n");
        }
    }

    private MetadataBench() {
    }
}
