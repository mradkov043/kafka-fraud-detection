package com.matey.kafka.dashboard.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;

/**
 * HTTP client for fetching producer-side metrics.
 *
 * <p>The dashboard calls {@code GET /metrics/producer} (proxied by this service) to display
 * the effective producer throughput (TPS).
 */
@Component
public class ProducerMetricsClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String producerMetricsUrl;

    public ProducerMetricsClient(
            @Value("${app.producer.metrics-url:http://localhost:8081/metrics/producer}")
            String producerMetricsUrl
    ) {
        this.producerMetricsUrl = producerMetricsUrl;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> fetchMetrics() {
        try {
            Map<String, Object> result =
                    restTemplate.getForObject(producerMetricsUrl, Map.class);
            return (result != null) ? result : Collections.emptyMap();
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}
