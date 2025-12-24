package com.matey.kafka.fraud.web;

import com.matey.kafka.fraud.metrics.FraudProcessingMetrics;
import com.matey.kafka.fraud.metrics.FraudProcessingMetrics.Snapshot;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API exposing fraud-detection processing metrics for benchmarking.
 *
 * GET  /metrics/fraud       -> snapshot with throughput + alert counts
 * POST /metrics/fraud/reset -> reset between benchmark runs
 */
@RestController
public class FraudMetricsController {

    private final FraudProcessingMetrics metrics;

    public FraudMetricsController(FraudProcessingMetrics metrics) {
        this.metrics = metrics;
    }

    @GetMapping("/metrics/fraud")
    public Snapshot getFraudMetrics() {
        return metrics.snapshot();
    }

    @PostMapping("/metrics/fraud/reset")
    public void resetFraudMetrics() {
        metrics.reset();
    }
}
