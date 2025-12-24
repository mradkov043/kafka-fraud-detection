package com.matey.kafka.dashboard.metrics;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates fraud-alert metrics in memory so the dashboard can
 * poll a single, small REST endpoint.
 *
 * NOTE: This measures the behaviour of the *Kafka Streams / alert-consumer*
 * pipeline (rules engine + consumer side), not the Kafka broker itself.
 */
@Component
@Slf4j
public class MetricsAggregator {


    private final AtomicLong runId = new AtomicLong(0);

    private volatile Instant startTime = Instant.now();

    private volatile double configuredTps = 0.0;

    private final AtomicLong totalAlerts = new AtomicLong(0);

    private final Map<String, AtomicLong> alertsPerType = new HashMap<>();

    private final AtomicLong latencyCount = new AtomicLong(0);

    /**
     * Sum of all latencies in milliseconds (used for avg).
     */
    private volatile long latencyTotal = 0L;

    private volatile Long latencyMin = null;

    private volatile Long latencyMax = null;

    /**
     * Global percentiles are computed over a bounded reservoir
     * of raw latency samples (in milliseconds).
     */
    private static final int MAX_LATENCY_SAMPLES = 512;

    private final List<Long> latencySamples =
            Collections.synchronizedList(new ArrayList<>());

    /**
     * Per-rule latency aggregates (count, min, max, sum).
     * Percentiles remain global (over all rules) for simplicity.
     */
    private final Map<String, RuleLatencyStats> perRuleLatency = new HashMap<>();

    /**
     * Simple per-second throughput: epochSecond -> alert count.
     *
     * This measures the output rate of the Kafka Streams fraud detection
     * pipeline (how many alerts per second), not the raw broker throughput.
     */
    private final Map<Long, AtomicLong> alertsPerSecond = new ConcurrentHashMap<>();

    /**
     * Called when a new benchmark run starts, so we can reset the state.
     */
    public synchronized void startNewRun(double configuredTransactionsPerSecond) {
        runId.incrementAndGet();
        startTime = Instant.now();
        configuredTps = configuredTransactionsPerSecond;

        totalAlerts.set(0);

        alertsPerType.clear();

        latencyCount.set(0);
        latencyTotal = 0L;
        latencyMin = null;
        latencyMax = null;
        latencySamples.clear();
        perRuleLatency.clear();

        alertsPerSecond.clear();

        log.info("MetricsAggregator: started new run {} with configured TPS={}",
                runId.get(), configuredTransactionsPerSecond);
    }

    /**
     * Record a single fraud alert, as observed by the listener.
     *
     * @param type      alert type (HIGH_AMOUNT / HIGH_FREQUENCY / IMPOSSIBLE_TRAVEL)
     * @param userId    user id
     * @param latencyMs end-to-end latency in milliseconds (may be null if unknown)
     */
    public void recordAlert(String type, String userId, Long latencyMs) {

        if (type == null) {
            type = "UNKNOWN";
        }

        totalAlerts.incrementAndGet();

        alertsPerType
                .computeIfAbsent(type, t -> new AtomicLong(0))
                .incrementAndGet();

        long second = Instant.now().getEpochSecond();
        alertsPerSecond
                .computeIfAbsent(second, s -> new AtomicLong(0))
                .incrementAndGet();

        if (latencyMs != null) {

            // Clamp extreme outliers so charts aren’t dominated by bogus values
            // (e.g. very old timestamps or weird test sequences)
            latencyMs = Math.min(latencyMs, 5000L);

            latencyCount.incrementAndGet();
            latencyTotal += latencyMs;

            if (latencyMin == null || latencyMs < latencyMin) {
                latencyMin = latencyMs;
            }
            if (latencyMax == null || latencyMs > latencyMax) {
                latencyMax = latencyMs;
            }

            // store raw sample for percentile/histogram
            // but keep a bounded in-memory reservoir (up to MAX_LATENCY_SAMPLES)
            // using simple reservoir sampling so long runs do not blow up memory
            synchronized (latencySamples) {
                int currentSize = latencySamples.size();
                long n = latencyCount.get();
                if (currentSize < MAX_LATENCY_SAMPLES) {
                    latencySamples.add(latencyMs);
                } else if (n > 0) {
                    long r = java.util.concurrent.ThreadLocalRandom
                            .current()
                            .nextLong(n);
                    if (r < MAX_LATENCY_SAMPLES) {
                        latencySamples.set((int) r, latencyMs);
                    }
                }
            }

            synchronized (perRuleLatency) {
                RuleLatencyStats stats =
                        perRuleLatency.computeIfAbsent(type, t -> new RuleLatencyStats());
                stats.addSample(latencyMs);
            }
        }
    }


    /**
     * Snapshot of all metrics in a single DTO for the REST endpoint.
     */
    public synchronized DashboardMetricsSnapshot snapshot() {
        DashboardMetricsSnapshot snapshot = new DashboardMetricsSnapshot();

        snapshot.setRunId(runId.get());
        snapshot.setStartTime(startTime);
        snapshot.setConfiguredTps(configuredTps);

        double seconds = Math.max(
                0.000_001,
                Duration.between(startTime, Instant.now()).toMillis() / 1000.0
        );
        snapshot.setUptimeSeconds(seconds);

        long total = totalAlerts.get();
        snapshot.setTotalAlerts(total);

        Map<String, Long> perType = new HashMap<>();
        for (Map.Entry<String, AtomicLong> e : alertsPerType.entrySet()) {
            perType.put(e.getKey(), e.getValue().get());
        }
        snapshot.setAlertsPerType(perType);

        snapshot.setLatencyCount(latencyCount.get());
        snapshot.setLatencyMinMs(latencyMin);
        snapshot.setLatencyMaxMs(latencyMax);
        snapshot.setLatencyAvgMs(
                latencyCount.get() == 0 ? null : (latencyTotal * 1.0 / latencyCount.get())
        );

        snapshot.setLatencyPercentiles(getLatencyPercentiles());
        snapshot.setPerRuleLatency(buildPerRuleLatencySummary());

        // NOTE: throughput series is exposed via getThroughputSeries(),
        // not embedded directly into the snapshot, so that the REST
        // layer can decide the time range and resolution.

        return snapshot;
    }

    // ---- Helper: percentile calculations over reservoir ----

    private PercentilesSummary getLatencyPercentiles() {
        List<Long> copy;
        synchronized (latencySamples) {
            if (latencySamples.isEmpty()) {
                return null;
            }
            copy = new ArrayList<>(latencySamples);
        }

        Collections.sort(copy);
        int n = copy.size();

        PercentilesSummary p = new PercentilesSummary();
        p.setCount(n);

        p.setP50(selectPercentile(copy, 0.50));
        p.setP75(selectPercentile(copy, 0.75));
        p.setP90(selectPercentile(copy, 0.90));
        p.setP95(selectPercentile(copy, 0.95));
        p.setP99(selectPercentile(copy, 0.99));

        DoubleSummaryStatistics stats =
                copy.stream().mapToDouble(Long::doubleValue).summaryStatistics();
        p.setMin(stats.getMin());
        p.setMax(stats.getMax());
        p.setAvg(stats.getAverage());

        return p;
    }

    private static double selectPercentile(List<Long> sorted, double quantile) {
        if (sorted.isEmpty()) {
            return 0.0;
        }
        int n = sorted.size();
        int idx = (int) Math.floor(quantile * (n - 1));
        if (idx < 0) idx = 0;
        if (idx >= n) idx = n - 1;
        return sorted.get(idx);
    }

    // ---- Helper: per-rule latency summary ----

    private Map<String, RuleLatencySummary> buildPerRuleLatencySummary() {
        Map<String, RuleLatencySummary> result = new HashMap<>();
        synchronized (perRuleLatency) {
            for (Map.Entry<String, RuleLatencyStats> e : perRuleLatency.entrySet()) {
                String type = e.getKey();
                RuleLatencyStats stats = e.getValue();

                RuleLatencySummary s = new RuleLatencySummary();
                s.setType(type);
                s.setCount(stats.getCount());
                s.setMinMs(stats.getMin());
                s.setMaxMs(stats.getMax());
                s.setAvgMs(stats.getAverage());

                result.put(type, s);
            }
        }
        return result;
    }

    /**
     * Returns a time series of alerts/second, limited to the last {@code maxPoints} seconds.
     *
     * Each point aggregates all alerts whose alert-consumer observation time
     * fell into that wall-clock second (epoch seconds).
     */
    public List<ThroughputPoint> getThroughputSeries(int maxPoints) {
        if (maxPoints <= 0) {
            maxPoints = 300;
        }

        List<Long> seconds = new ArrayList<>(alertsPerSecond.keySet());
        if (seconds.isEmpty()) {
            return List.of();
        }

        Collections.sort(seconds);

        int size = seconds.size();
        int fromIndex = Math.max(0, size - maxPoints);

        List<ThroughputPoint> result = new ArrayList<>(size - fromIndex);
        for (int i = fromIndex; i < size; i++) {
            Long sec = seconds.get(i);
            AtomicLong counter = alertsPerSecond.get(sec);
            long count = (counter != null) ? counter.get() : 0L;

            ThroughputPoint p = new ThroughputPoint();
            p.setEpochSecond(sec);
            p.setAlerts(count);
            result.add(p);
        }

        return result;
    }

    /**
     * Convenience overload: default to the last 300 seconds of data.
     */
    public List<ThroughputPoint> getThroughputSeries() {
        return getThroughputSeries(300);
    }

    /**
     * Simple mutable accumulator for a rule's latency stats.
     */
    @Data
    public static class RuleLatencyStats {
        private long count;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;
        private long total;

        public void addSample(long latencyMs) {
            count++;
            total += latencyMs;
            if (latencyMs < min) {
                min = latencyMs;
            }
            if (latencyMs > max) {
                max = latencyMs;
            }
        }

        public Double getAverage() {
            return count == 0 ? null : (total * 1.0 / count);
        }
    }

    /**
     * Summary view of rule latency metrics exposed to the REST layer.
     */
    @Data
    public static class RuleLatencySummary {
        private String type;
        private long count;
        private Long minMs;
        private Long maxMs;
        private Double avgMs;
    }

    /**
     * Percentiles summary for global latency distribution.
     */
    @Data
    public static class PercentilesSummary {
        private int count;
        private Double p50;
        private Double p75;
        private Double p90;
        private Double p95;
        private Double p99;
        private Double min;
        private Double max;
        private Double avg;
    }

    /**
     * Throughput point for "alerts per second" chart.
     * epochSecond is UNIX time in seconds.
     */
    @Data
    public static class ThroughputPoint {
        private long epochSecond;
        private long alerts;
    }

    /**
     * Top-level DTO returned from the /metrics snapshot in the alert-consumer service.
     *
     * This is primarily useful for summary cards, per-type tables and latency stats.
     * Throughput series is provided via {@link #getThroughputSeries(int)}.
     */
    @Data
    public static class DashboardMetricsSnapshot {
        private long runId;
        private Instant startTime;
        private double configuredTps;
        private double uptimeSeconds;

        private long totalAlerts;
        private Map<String, Long> alertsPerType;

        private long latencyCount;
        private Long latencyMinMs;
        private Long latencyMaxMs;
        private Double latencyAvgMs;

        private PercentilesSummary latencyPercentiles;

        private Map<String, RuleLatencySummary> perRuleLatency;
    }
}
