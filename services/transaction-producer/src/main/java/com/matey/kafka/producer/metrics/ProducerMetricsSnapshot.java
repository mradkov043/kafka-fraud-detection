package com.matey.kafka.producer.metrics;

import lombok.Data;

import java.time.Instant;

/**
 * Immutable snapshot of the producer metrics, returned by
 * the /metrics/producer endpoint as JSON.
 *
 * It contains the configuration for the current run, counters
 * for successful and failed sends, the effective throughput in
 * transactions per second and basic latency statistics.
 */
@Data
public class ProducerMetricsSnapshot {

    private long runId;

    private Instant startTime;
    private double configuredTransactionsPerSecond;

    private long totalSends;
    private long successfulSends;
    private long failedSends;
    private String lastErrorMessage;

    private double effectiveTransactionsPerSecond;

    private double avgLatencyMicros;
    private long minLatencyMicros;
    private long maxLatencyMicros;
    private long p95LatencyMicros;
    private long p99LatencyMicros;
}
