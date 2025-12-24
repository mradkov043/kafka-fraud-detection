package com.matey.kafka.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Spring Boot entry point for the "alert-consumer" / dashboard service.
 *
 * <p>This service has two responsibilities:
 * <ol>
 *   <li>Consume fraud alerts from Kafka (topic {@code app.kafka.fraud-alerts-topic}) and aggregate
 *   benchmark metrics in-memory (alerts/sec, latency stats, per-rule counts).</li>
 *   <li>Expose a small REST API consumed by the single-page dashboard (index.html), including
 *   {@code /metrics/dashboard}, {@code /metrics/reset}, and proxy endpoints to fetch TPS metrics
 *   from the producer and fraud-detection services.</li>
 * </ol>
 */
@SpringBootApplication
@EnableKafka
public class DashboardServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DashboardServiceApplication.class, args);
    }
}
