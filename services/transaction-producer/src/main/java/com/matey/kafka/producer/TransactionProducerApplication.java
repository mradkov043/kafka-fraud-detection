package com.matey.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the transaction producer service.
 * This service is responsible for generating synthetic financial
 * transactions and sending them to the Kafka topic.
 */

@SpringBootApplication
public class TransactionProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionProducerApplication.class, args);
    }
}
