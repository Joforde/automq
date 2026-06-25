package io.automq.bench;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rebalance storm load generator: many consumers share one group.id, subscribe to a
 * topic, and repeatedly close/recreate to trigger continuous group rebalances.
 */
public final class RebalanceStormBench {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.parse(args);
        cfg.print();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong restarts = new AtomicLong();
        AtomicLong rebalances = new AtomicLong();

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
            pool.execute(new ChurnWorker(i, cfg, running, restarts, rebalances));
        }

        long startNs = System.nanoTime();
        long prevRestarts = 0;
        long prevRebalances = 0;
        long prevNs = startNs;

        while (running.get()) {
            Thread.sleep(1000);
            long now = System.nanoTime();
            long curRestarts = restarts.get();
            long curRebalances = rebalances.get();
            double dt = (now - prevNs) / 1e9;
            double restartRate = (curRestarts - prevRestarts) / dt;
            double rebalanceRate = (curRebalances - prevRebalances) / dt;
            System.out.printf(
                "[%4ds] restarts/s: %,.1f | rebalances/s: %,.1f | total restarts: %,d | total rebalances: %,d%n",
                (now - startNs) / 1_000_000_000L,
                restartRate,
                rebalanceRate,
                curRestarts,
                curRebalances);
            prevRestarts = curRestarts;
            prevRebalances = curRebalances;
            prevNs = now;
        }

        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }

        System.out.printf(
            "%nStopped after %.1fs. total restarts=%,d rebalances=%,d%n",
            (System.nanoTime() - startNs) / 1e9,
            restarts.get(),
            rebalances.get());
    }

    static final class Config {
        String bootstrap = "localhost:9092";
        String topic = "big-test";
        String groupId = "rebalance-storm-bench";
        int consumers = 100;
        long aliveMs = 2000;
        long restartDelayMs = 500;
        long restartJitterMs = 1000;
        long staggerMs = 50;
        long pollMs = 100;
        int sessionTimeoutMs = 10000;
        int heartbeatIntervalMs = 3000;
        int maxPollIntervalMs = 30000;
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
                        c.bootstrap = v;
                        break;
                    case "topic":
                        c.topic = v;
                        break;
                    case "group.id":
                        c.groupId = v;
                        break;
                    case "consumers":
                        c.consumers = Integer.parseInt(v);
                        break;
                    case "alive.ms":
                        c.aliveMs = Long.parseLong(v);
                        break;
                    case "restart.delay.ms":
                        c.restartDelayMs = Long.parseLong(v);
                        break;
                    case "restart.jitter.ms":
                        c.restartJitterMs = Long.parseLong(v);
                        break;
                    case "stagger.ms":
                        c.staggerMs = Long.parseLong(v);
                        break;
                    case "poll.ms":
                        c.pollMs = Long.parseLong(v);
                        break;
                    case "session.timeout.ms":
                        c.sessionTimeoutMs = Integer.parseInt(v);
                        break;
                    case "heartbeat.interval.ms":
                        c.heartbeatIntervalMs = Integer.parseInt(v);
                        break;
                    case "max.poll.interval.ms":
                        c.maxPollIntervalMs = Integer.parseInt(v);
                        break;
                    default:
                        c.extra.put(k, v);
                }
            }
            if (c.consumers < 1) {
                throw new IllegalArgumentException("consumers must be >= 1");
            }
            return c;
        }

        void print() {
            int heapMb = recommendedHeapMb(consumers);
            int xmsMb = Math.max(1024, heapMb / 2);
            System.out.printf(
                "RebalanceStormBench: bootstrap=%s topic=%s group.id=%s consumers=%d%n" +
                    "  alive.ms=%d restart.delay.ms=%d restart.jitter.ms=%d stagger.ms=%d poll.ms=%d%n" +
                    "  recommended JVM: -Xms%dM -Xmx%dM -XX:+UseG1GC -Xss512k%n",
                bootstrap,
                topic,
                groupId,
                consumers,
                aliveMs,
                restartDelayMs,
                restartJitterMs,
                staggerMs,
                pollMs,
                xmsMb,
                heapMb);
        }

        /** Rough heap for high-partition topics (e.g. big-test 9000 partitions). */
        static int recommendedHeapMb(int consumers) {
            int mb = 512 + consumers * 48;
            return Math.min(8192, Math.max(1024, mb));
        }

        static void usage() {
            System.out.println("Usage: java -jar rebalance-storm-bench.jar [key=value ...]\n" +
                "  bootstrap=localhost:9092   Kafka/AutoMQ bootstrap servers\n" +
                "  topic=big-test             Topic to subscribe\n" +
                "  group.id=rebalance-storm-bench  Shared consumer group (all workers)\n" +
                "  consumers=100              Number of churn worker threads\n" +
                "  alive.ms=2000              How long each consumer lives before close\n" +
                "  restart.delay.ms=500       Delay after close before recreate\n" +
                "  restart.jitter.ms=1000     Random extra delay per restart\n" +
                "  stagger.ms=50              Initial start delay between workers\n" +
                "  poll.ms=100                Consumer poll interval\n" +
                "  session.timeout.ms=10000   Consumer session timeout\n" +
                "  heartbeat.interval.ms=3000 Consumer heartbeat interval\n" +
                "  max.poll.interval.ms=30000 Consumer max poll interval\n" +
                "  <any>=<val>                Extra KafkaConsumer config\n");
        }
    }

    private RebalanceStormBench() {
    }
}
