package com.matey.kafka.dashboard.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.matey.kafka.dashboard.metrics.MetricsAggregator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Kafka consumer for fraud alerts.
 *
 * <p>Consumes JSON alert messages from the {@code app.kafka.fraud-alerts-topic} topic
 * and updates {@link MetricsAggregator} with counts, throughput and end-to-end latency
 * statistics. The dashboard UI polls the aggregated metrics via REST.</p>
 *
 * <p><b>Latency semantics (unambiguous):</b>
 * If present, {@code producerTimestamp} is treated as the start time (producer-side send time),
 * and latency is computed as {@code consumerNow - producerTimestamp}. This yields a true
 * end-to-end latency from producer emission to this consumer's processing time.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudAlertListener {

    private final ObjectMapper objectMapper;
    private final MetricsAggregator metricsAggregator;

    @Value("${app.kafka.fraud-alerts-topic}")
    private String fraudAlertsTopic;

    /**
     * Consumes a single fraud alert record from Kafka and updates the in-memory
     * {@link MetricsAggregator} used by the dashboard.
     *
     * <p>The alert payload is JSON. We parse it as a {@link JsonNode} to remain
     * tolerant of schema changes between experiments.</p>
     */
    @KafkaListener(
            topics = "${app.kafka.fraud-alerts-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        String value = record.value();
        try {
            JsonNode root = objectMapper.readTree(value);

            String type = getText(root, "type");
            String userId = getText(root, "userId");

            Long latencyMs = computeLatencyMillis(root);

            // userId is currently not aggregated, but is kept as a utility parameter
            metricsAggregator.recordAlert(type, userId, latencyMs);

        } catch (Exception e) {
            log.warn("Failed to deserialize fraud alert from topic {}: value={}",
                    fraudAlertsTopic, value, e);
        }
    }

    /**
     * Safely reads a String field from a JSON node (null if missing).
     */
    private String getText(JsonNode node, String fieldName) {
        JsonNode field = node.get(fieldName);
        if (field != null && !field.isNull()) {
            return field.asText();
        }
        return null;
    }

    /**
     * Computes latency in milliseconds.
     *
     * <p><b>Preferred (unambiguous) path:</b>
     * If {@code producerTimestamp} is present in the alert payload, latency is:
     * <pre>
     * latency = now(consumer) - producerTimestamp
     * </pre>
     *
     * <p><b>Fallback (legacy) path:</b>
     * If {@code producerTimestamp} is missing, latency is computed as:
     * <pre>
     * latency = alertTimestamp - eventTime
     * </pre>
     * where {@code eventTime} is chosen per rule:
     * <ul>
     *   <li>HIGH_AMOUNT: {@code eventTimestamp}</li>
     *   <li>HIGH_FREQUENCY: {@code maxEventTimestamp} (fallback: {@code windowEnd}, then {@code eventTimestamp})</li>
     *   <li>IMPOSSIBLE_TRAVEL: {@code toTimestamp}</li>
     * </ul>
     */
    private Long computeLatencyMillis(JsonNode alertNode) {
        // 1) Preferred path: producerTimestamp -> consumerNow (true end-to-end)
        try {
            JsonNode producerTsNode = alertNode.get("producerTimestamp");
            if (producerTsNode != null && !producerTsNode.isNull()) {
                Instant producerTs = Instant.parse(producerTsNode.asText());
                Instant consumerNow = Instant.now();

                long diff = consumerNow.toEpochMilli() - producerTs.toEpochMilli();
                return (diff >= 0) ? diff : null;
            }
        } catch (Exception e) {
            // If producerTimestamp exists but is malformed, fall back to legacy computation.
            log.debug("Failed to compute E2E latency from producerTimestamp; falling back.", e);
        }

        // 2) Legacy fallback: alertTimestamp - selected event timestamp
        try {
            JsonNode alertTsNode = alertNode.get("alertTimestamp");
            if (alertTsNode == null || alertTsNode.isNull()) {
                return null;
            }
            Instant alertTs = Instant.parse(alertTsNode.asText());

            Instant eventTs = null;

            JsonNode typeNode = alertNode.get("type");
            String type = (typeNode != null && !typeNode.isNull()) ? typeNode.asText() : null;

            if ("HIGH_AMOUNT".equals(type)) {
                JsonNode eventTsNode = alertNode.get("eventTimestamp");
                if (eventTsNode != null && !eventTsNode.isNull()) {
                    eventTs = Instant.parse(eventTsNode.asText());
                }

            } else if ("IMPOSSIBLE_TRAVEL".equals(type)) {
                JsonNode toTsNode = alertNode.get("toTimestamp");
                if (toTsNode != null && !toTsNode.isNull()) {
                    eventTs = Instant.parse(toTsNode.asText());
                }

            } else if ("HIGH_FREQUENCY".equals(type)) {
                JsonNode maxTsNode = alertNode.get("maxEventTimestamp");

                if (maxTsNode != null && !maxTsNode.isNull()) {
                    eventTs = Instant.parse(maxTsNode.asText());
                } else {
                    JsonNode windowEndNode = alertNode.get("windowEnd");
                    if (windowEndNode != null && !windowEndNode.isNull()) {
                        eventTs = Instant.parse(windowEndNode.asText());
                    } else {
                        JsonNode eventTsNode = alertNode.get("eventTimestamp");
                        if (eventTsNode != null && !eventTsNode.isNull()) {
                            eventTs = Instant.parse(eventTsNode.asText());
                        }
                    }
                }

            } else {
                JsonNode eventTsNode = alertNode.get("eventTimestamp");
                if (eventTsNode != null && !eventTsNode.isNull()) {
                    eventTs = Instant.parse(eventTsNode.asText());
                }
            }

            if (eventTs == null) {
                return null;
            }

            long diff = alertTs.toEpochMilli() - eventTs.toEpochMilli();
            return (diff >= 0) ? diff : null;

        } catch (Exception e) {
            log.debug("Failed to compute latency from alert timestamps", e);
            return null;
        }
    }
}
