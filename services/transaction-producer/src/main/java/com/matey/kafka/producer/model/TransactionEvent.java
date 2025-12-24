package com.matey.kafka.producer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Simple DTO representing a synthetic financial transaction sent
 * by the producer service to the transactions topic.
 *
 * Fields:
 * <ul>
 *   <li>transactionId – unique id of the transaction</li>
 *   <li>userId – synthetic user identifier</li>
 *   <li>amount – transaction amount in currency units</li>
 *   <li>eventTimestamp – logical event time of the transaction</li>
 *   <li>location – coarse-grained location (e.g. "DE-BERLIN")</li>
 *   <li>producerTimestamp – optional timestamp used in older
 *       transport-latency experiments</li>
 * </ul>
 */
public class TransactionEvent {

    private String transactionId;
    private String userId;
    private BigDecimal amount;
    private Instant eventTimestamp;
    private String location;

    private Instant producerTimestamp;

    public TransactionEvent() {
    }

    public TransactionEvent(String transactionId,
                            String userId,
                            BigDecimal amount,
                            Instant eventTimestamp,
                            String location) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.eventTimestamp = eventTimestamp;
        this.location = location;
    }

    public TransactionEvent(String transactionId,
                            String userId,
                            BigDecimal amount,
                            Instant eventTimestamp,
                            String location,
                            Instant producerTimestamp) {
        this(transactionId, userId, amount, eventTimestamp, location);
        this.producerTimestamp = producerTimestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getUserId() {
        return userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public String getLocation() {
        return location;
    }

    public Instant getProducerTimestamp() {
        return producerTimestamp;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public void setEventTimestamp(Instant eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setProducerTimestamp(Instant producerTimestamp) {
        this.producerTimestamp = producerTimestamp;
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", eventTimestamp=" + eventTimestamp +
                ", location='" + location + '\'' +
                ", producerTimestamp=" + producerTimestamp +
                '}';
    }
}
