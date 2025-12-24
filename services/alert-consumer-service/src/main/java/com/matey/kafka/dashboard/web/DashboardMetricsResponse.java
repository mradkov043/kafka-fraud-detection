package com.matey.kafka.dashboard.web;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Shape of the JSON returned by GET /metrics/dashboard,
 * exactly what index.html expects.
 */
@Data
public class DashboardMetricsResponse {

    private Summary summary;

    private Map<String, Long> alertsByType;

    private Map<String, LatencyByType> latencyByType;

    private LatencyPercentiles latencyPercentiles;

    /** optional series for line chart (currently unused / empty) */
    private List<ThroughputPoint> throughputSeries;

    @Data
    public static class Summary {
        private long totalAlerts;
        private double alertsPerSecond;
        private Double latencyAvgMillis;
        private Long latencyMaxMillis;
    }

    @Data
    public static class LatencyByType {
        private long count;
        private Double avgMillis;
        private Long minMillis;
        private Long maxMillis;
    }

    @Data
    public static class LatencyPercentiles {
        private int count;
        private Double p50;
        private Double p75;
        private Double p90;
        private Double p95;
        private Double p99;
    }

    @Data
    public static class ThroughputPoint {
        private long epochSecond;
        private long alerts;
    }
}
