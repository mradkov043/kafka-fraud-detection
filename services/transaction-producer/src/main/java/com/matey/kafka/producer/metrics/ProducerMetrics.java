package com.matey.kafka.producer.metrics;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory metrics collector for the transaction producer.
 *
 * Tracks the lifecycle of a single benchmark "run" and aggregates
 * basic counters (successful / failed sends) as well as latency
 * statistics for producer send operations.
 *
 * The aggregated values are exposed via ProducerMetricsSnapshot
 * and served to the dashboard through /metrics/producer.
 */
@Component
public class ProducerMetrics {

    private final AtomicLong runId = new AtomicLong(0);

    private volatile Instant startTime = Instant.now();
    private volatile double configuredTps = 0.0;

    private final AtomicLong sentSuccess = new AtomicLong(0);
    private final AtomicLong sentFailure = new AtomicLong(0);

    private volatile String lastErrorMessage = null;

    private final List<Long> latenciesMicros = Collections.synchronizedList(new ArrayList<>());

    /**
     * Starts a new logical benchmark run.
     *
     * Resets counters and latency statistics and records the configured
     * target throughput (transactions per second) for this run.
     *
     * configuredTransactionsPerSecond - the target TPS used by the generator
     */
    public synchronized void startNewRun(double configuredTransactionsPerSecond) {
        runId.incrementAndGet();
        startTime = Instant.now();
        configuredTps = configuredTransactionsPerSecond;

        sentSuccess.set(0);
        sentFailure.set(0);
        lastErrorMessage = null;

        latenciesMicros.clear();
    }


    /**
     * Records a successful send and includes the measured latency
     * of the Kafka producer call in microseconds.
     */
    public void recordSuccess(long latencyMicros) {
        sentSuccess.incrementAndGet();
        latenciesMicros.add(latencyMicros);
    }

    public void recordFailure(long latencyMicros, Throwable ex) {
        sentFailure.incrementAndGet();
        latenciesMicros.add(latencyMicros);

        if (ex != null) {
            lastErrorMessage = ex.getMessage();
        }
    }

    // Currently unused
    public void recordSend() {
        sentSuccess.incrementAndGet();
    }

    /**
     * Builds an immutable snapshot of the current metrics that can be
     * serialized to JSON and returned by the metrics endpoint.
     */
    public ProducerMetricsSnapshot snapshot() {
        ProducerMetricsSnapshot snapshot = new ProducerMetricsSnapshot();
        snapshot.setRunId(runId.get());
        snapshot.setStartTime(startTime);
        snapshot.setConfiguredTransactionsPerSecond(configuredTps);

        long ok = sentSuccess.get();
        long fail = sentFailure.get();
        long total = ok + fail;

        snapshot.setSuccessfulSends(ok);
        snapshot.setFailedSends(fail);
        snapshot.setTotalSends(total);
        snapshot.setLastErrorMessage(lastErrorMessage);

        double seconds = Math.max(
                0.000_001,
                Duration.between(startTime, Instant.now()).toMillis() / 1000.0
        );
        snapshot.setEffectiveTransactionsPerSecond(total / seconds);

        synchronized (latenciesMicros) {
            if (!latenciesMicros.isEmpty()) {
                List<Long> sorted = new ArrayList<>(latenciesMicros);
                Collections.sort(sorted);

                snapshot.setAvgLatencyMicros(
                        sorted.stream().mapToLong(Long::longValue).average().orElse(0)
                );

                snapshot.setMinLatencyMicros(sorted.get(0));
                snapshot.setMaxLatencyMicros(sorted.get(sorted.size() - 1));

                int p95Index = (int) Math.min(sorted.size() - 1, sorted.size() * 95 / 100);
                int p99Index = (int) Math.min(sorted.size() - 1, sorted.size() * 99 / 100);

                snapshot.setP95LatencyMicros(sorted.get(p95Index));
                snapshot.setP99LatencyMicros(sorted.get(p99Index));
            }
        }

        return snapshot;
    }
}
