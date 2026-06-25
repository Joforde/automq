package io.automq.bench;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One worker thread: repeatedly create a consumer, subscribe, poll, then close.
 * Closing triggers LeaveGroup and rebalance for the shared consumer group.
 */
final class ChurnWorker implements Runnable {

    private final int workerId;
    private final RebalanceStormBench.Config cfg;
    private final AtomicBoolean running;
    private final AtomicLong restarts;
    private final AtomicLong rebalances;

    ChurnWorker(int workerId,
                RebalanceStormBench.Config cfg,
                AtomicBoolean running,
                AtomicLong restarts,
                AtomicLong rebalances) {
        this.workerId = workerId;
        this.cfg = cfg;
        this.running = running;
        this.restarts = restarts;
        this.rebalances = rebalances;
    }

    @Override
    public void run() {
        try {
            Thread.sleep((long) workerId * cfg.staggerMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        while (running.get()) {
            KafkaConsumer<String, String> consumer = null;
            try {
                consumer = new KafkaConsumer<>(consumerProps());
                consumer.subscribe(Collections.singletonList(cfg.topic), rebalanceListener());
                restarts.incrementAndGet();

                long deadline = System.currentTimeMillis() + cfg.aliveMs;
                while (running.get() && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(cfg.pollMs));
                }
            } catch (Exception e) {
                System.err.printf("worker-%d error: %s%n", workerId, e.getMessage());
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close(Duration.ofSeconds(5));
                    } catch (Exception e) {
                        System.err.printf("worker-%d close error: %s%n", workerId, e.getMessage());
                    }
                }
            }

            if (!running.get()) {
                break;
            }

            long delay = cfg.restartDelayMs;
            if (cfg.restartJitterMs > 0) {
                delay += ThreadLocalRandom.current().nextLong(cfg.restartJitterMs + 1);
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId);
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "rebalance-storm-" + workerId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, cfg.sessionTimeoutMs);
        p.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, cfg.heartbeatIntervalMs);
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, cfg.maxPollIntervalMs);
        cfg.extra.forEach(p::put);
        return p;
    }

    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                rebalances.incrementAndGet();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // assignment only; revoked is enough for storm rate
            }
        };
    }
}
