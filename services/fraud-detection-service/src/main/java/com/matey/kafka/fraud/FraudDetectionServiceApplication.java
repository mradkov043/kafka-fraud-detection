package com.matey.kafka.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;


/**
 * Spring Boot entry point for the Kafka Streams fraud detection service.
 *
 * <p>This service consumes transaction events from the {@code transactions} topic,
 * evaluates fraud rules (high amount, high frequency, impossible travel),
 * and produces fraud alerts to the {@code alerts} topic.</p>
 *
 * <p>Processing metrics are collected in-memory for benchmarking and are exposed
 * via {@code /metrics/fraud}.</p>
 */
@SpringBootApplication
@EnableKafkaStreams
public class FraudDetectionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectionServiceApplication.class, args);
    }
}
