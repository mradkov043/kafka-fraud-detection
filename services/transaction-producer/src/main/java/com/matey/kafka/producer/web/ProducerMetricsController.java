package com.matey.kafka.producer.controller;

import com.matey.kafka.producer.metrics.ProducerMetrics;
import com.matey.kafka.producer.metrics.ProducerMetricsSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint that exposes the current producer metrics as JSON.
 *
 * The dashboard service calls /metrics/producer to display
 * the effective producer throughput (transactions per second)
 */
@RestController
@RequiredArgsConstructor
public class ProducerMetricsController {

    private final ProducerMetrics producerMetrics;

    @GetMapping("/metrics/producer")
    public ProducerMetricsSnapshot getMetrics() {
        return producerMetrics.snapshot();
    }
}
