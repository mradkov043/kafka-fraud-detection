package com.matey.kafka.fraud.metrics;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory benchmark metrics for the fraud-detection service.
 *
 * Tracks processing throughput and fraud-alert counts per rule.
 *
 * This is used by the dashboard service to display Fraud TPS and alerts-by-type.
 *
 * The metrics can be reset between benchmark runs via {@code POST /metrics/fraud/reset}.
 *
 * (Note: this component intentionally keeps metrics in memory; it is not meant
 * for long-term persistence.)
 */

/**
 * Tracks processing-side metrics for the fraud-detection service:
 *  - how many transactions were processed in total
 *  - effective processing throughput (tx/sec)
 *  - how many alerts were produced in total
 *  - alert counts broken down by type (HIGH_AMOUNT, HIGH_FREQUENCY, IMPOSSIBLE_TRAVEL, ...)
 */
@Component
public class FraudProcessingMetrics {

    private final AtomicLong runId = new AtomicLong(1);

    private volatile Instant startTime = Instant.now();

    private final AtomicLong transactionsProcessedTotal = new AtomicLong(0);
    private final AtomicLong alertsProducedTotal = new AtomicLong(0);

    private final Map<String, AtomicLong> alertsByType = new ConcurrentHashMap<>();

    public void recordTransactionProcessed() {
        transactionsProcessedTotal.incrementAndGet();
    }

    public void recordAlertProduced(String alertType) {
        alertsProducedTotal.incrementAndGet();
        String key = (alertType != null && !alertType.isBlank()) ? alertType : "UNKNOWN";
        alertsByType
                .computeIfAbsent(key, k -> new AtomicLong())
                .incrementAndGet();
    }

    public synchronized void reset() {
        transactionsProcessedTotal.set(0);
        alertsProducedTotal.set(0);
        alertsByType.clear();
        startTime = Instant.now();
        runId.incrementAndGet();
    }

    public Snapshot snapshot() {
        Instant now = Instant.now();
        long seconds = Math.max(1, Duration.between(startTime, now).getSeconds());

        Snapshot s = new Snapshot();
        s.setRunId(runId.get());
        s.setStartTime(startTime.toString());
        s.setTransactionsProcessedTotal(transactionsProcessedTotal.get());
        s.setTransactionsProcessedPerSecond(
                transactionsProcessedTotal.get() / (double) seconds
        );
        s.setAlertsProducedTotal(alertsProducedTotal.get());
        s.setAlertsByTypeSnapshot(getAlertsByTypeSnapshot());
        return s;
    }

    public Map<String, Long> getAlertsByTypeSnapshot() {
        return alertsByType.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get()
                ));
    }

    public static class Snapshot {
        private long runId;
        private String startTime;

        private long transactionsProcessedTotal;
        private double transactionsProcessedPerSecond;

        private long alertsProducedTotal;
        private Map<String, Long> alertsByTypeSnapshot;

        public long getRunId() {
            return runId;
        }

        public void setRunId(long runId) {
            this.runId = runId;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public long getTransactionsProcessedTotal() {
            return transactionsProcessedTotal;
        }

        public void setTransactionsProcessedTotal(long transactionsProcessedTotal) {
            this.transactionsProcessedTotal = transactionsProcessedTotal;
        }

        public double getTransactionsProcessedPerSecond() {
            return transactionsProcessedPerSecond;
        }

        public void setTransactionsProcessedPerSecond(double transactionsProcessedPerSecond) {
            this.transactionsProcessedPerSecond = transactionsProcessedPerSecond;
        }

        public long getAlertsProducedTotal() {
            return alertsProducedTotal;
        }

        public void setAlertsProducedTotal(long alertsProducedTotal) {
            this.alertsProducedTotal = alertsProducedTotal;
        }

        public Map<String, Long> getAlertsByTypeSnapshot() {
            return alertsByTypeSnapshot;
        }

        public void setAlertsByTypeSnapshot(Map<String, Long> alertsByTypeSnapshot) {
            this.alertsByTypeSnapshot = alertsByTypeSnapshot;
        }
    }
}
