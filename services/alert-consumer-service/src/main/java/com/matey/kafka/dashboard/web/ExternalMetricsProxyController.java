package com.matey.kafka.dashboard.web;

import com.matey.kafka.dashboard.fraud.FraudMetricsClient;
import com.matey.kafka.dashboard.producer.ProducerMetricsClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Proxy endpoints for cross-service metrics.
 *
 * <p>The dashboard frontend calls endpoints from the same origin (this service).
 * To avoid CORS setup and to keep the UI simple, this controller forwards requests
 * to the producer and fraud-detection services and returns their raw JSON.</p>
 */
@RestController
@RequestMapping("/metrics")
public class ExternalMetricsProxyController {

    private final ProducerMetricsClient producerMetricsClient;
    private final FraudMetricsClient fraudMetricsClient;

    public ExternalMetricsProxyController(
            ProducerMetricsClient producerMetricsClient,
            FraudMetricsClient fraudMetricsClient
    ) {
        this.producerMetricsClient = producerMetricsClient;
        this.fraudMetricsClient = fraudMetricsClient;
    }

    /**
     * Forwards the producer metrics JSON (transaction-producer service).
     */
    @GetMapping("/producer")
    public Map<String, Object> producerMetrics() {
        return producerMetricsClient.fetchMetrics();
    }

    /**
     * Forwards the fraud metrics JSON (fraud-detection-service).
     */
    @GetMapping("/fraud")
    public Map<String, Object> fraudMetrics() {
        return fraudMetricsClient.fetchMetrics();
    }
}
