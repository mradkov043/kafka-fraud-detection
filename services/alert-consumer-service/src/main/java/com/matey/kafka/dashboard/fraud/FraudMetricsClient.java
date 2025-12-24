package com.matey.kafka.dashboard.fraud;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Map;

/**
 * HTTP client for fetching fraud-detection service metrics.
 *
 * <p>This service polls the fraud-detection microservice (Kafka Streams) and forwards
 * the response through {@code /metrics/fraud} so the dashboard frontend can render
 * Fraud TPS and per-rule alert counts without needing cross-origin configuration.</p>
 */
@Component
public class FraudMetricsClient {

    private static final Logger log = LoggerFactory.getLogger(FraudMetricsClient.class);

    private final RestTemplate restTemplate;
    private final String fraudMetricsBaseUrl;

    public FraudMetricsClient(
            @Value("${app.fraud.metrics-url:http://localhost:8082}") String fraudMetricsBaseUrl
    ) {
        this.fraudMetricsBaseUrl = fraudMetricsBaseUrl;
        this.restTemplate = new RestTemplate();
    }

    /**
     * Fetches the fraud-detection service metrics from {@code GET /metrics/fraud}.
     *
     * <p>Returns an empty map on errors so the dashboard UI remains available even when
     * the fraud service is offline.</p>
     */
    @SuppressWarnings("ConstantConditions")
    public Map<String, Object> fetchMetrics() {
        try {
            ResponseEntity<Map<String, Object>> response =
                    restTemplate.exchange(
                            fraudMetricsBaseUrl + "/metrics/fraud",
                            HttpMethod.GET,
                            null,
                            new ParameterizedTypeReference<Map<String, Object>>() {}
                    );

            Map<String, Object> body = response.getBody();
            return body != null ? body : Collections.emptyMap();

        } catch (Exception e) {
            log.warn("Failed to fetch fraud metrics from {}", fraudMetricsBaseUrl, e);
            return Collections.emptyMap();
        }
    }
}
