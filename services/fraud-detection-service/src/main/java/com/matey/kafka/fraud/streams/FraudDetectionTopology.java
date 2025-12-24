package com.matey.kafka.fraud.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.matey.kafka.fraud.metrics.FraudProcessingMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

/**
 * Kafka Streams topology for real-time fraud detection.
 *
 * <p><b>Input:</b> {@code transactions} topic (JSON String payload)</p>
 * <p><b>Output:</b> {@code alerts} topic (JSON String payload)</p>
 *
 * <p><b>Rules:</b>
 * <ul>
 *   <li>HIGH_AMOUNT – flags transactions above a configured threshold</li>
 *   <li>HIGH_FREQUENCY – flags bursts within a configured time window (stateful)</li>
 *   <li>IMPOSSIBLE_TRAVEL – flags unrealistic location changes by implied speed (stateful)</li>
 * </ul>
 * </p>
 *
 * <p><b>State stores:</b>
 * <ul>
 *   <li>{@code high-frequency-store} – per-user sliding window / burst tracking</li>
 *   <li>{@code last-location-store} – per-user last known location + timestamp</li>
 * </ul>
 * </p>
 *
 * <p>Produced alerts are counted in {@link FraudProcessingMetrics} for benchmarking.</p>
 */
@Configuration
@RequiredArgsConstructor
@Slf4j

public class FraudDetectionTopology {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final ObjectMapper objectMapper;
    private final FraudProcessingMetrics fraudProcessingMetrics;

    @Value("${app.kafka.transactions-topic:transactions}")
    private String transactionsTopic;

    @Value("${app.kafka.fraud-alerts-topic:fraud-alerts}")
    private String fraudAlertsTopic;

    @Value("${rules.high-amount.enabled:true}")
    private boolean highAmountEnabled;

    @Value("${rules.high-frequency.enabled:true}")
    private boolean highFrequencyEnabled;

    @Value("${rules.impossible-travel.enabled:true}")
    private boolean impossibleTravelEnabled;

    @Value("${app.fraud.high-amount-threshold:3000.0}")
    private double highAmountThreshold;

    @Value("${app.fraud.high-frequency-window-seconds:30}")
    private long highFrequencyWindowSeconds;

    @Value("${app.fraud.high-frequency-max-transactions:8}")
    private long highFrequencyMaxTransactions;

    @Value("${app.fraud.location-velocity-threshold-kmh:1000.0}")
    private double locationVelocityThresholdKmh;

    @Value("${app.fraud.location-min-distance-km:2000.0}")
    private double locationMinDistanceKm;

    @Value("${app.fraud.location-min-seconds-between-events:60}")
    private long locationMinSecondsBetweenEvents;

    @Value("${app.fraud.location-max-age-minutes:120}")
    private long locationMaxAgeMinutes;

    @Bean
    public KStream<String, String> fraudDetectionStream(StreamsBuilder builder) {

        StoreBuilder<KeyValueStore<String, String>> highFrequencyStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("high-frequency-store"),
                        STRING_SERDE,
                        STRING_SERDE
                );
        builder.addStateStore(highFrequencyStoreBuilder);

        StoreBuilder<KeyValueStore<String, String>> lastLocationStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("last-location-store"),
                        STRING_SERDE,
                        STRING_SERDE
                );
        builder.addStateStore(lastLocationStoreBuilder);

        KStream<String, String> transactions =
                builder.stream(transactionsTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                        .peek((key, value) -> fraudProcessingMetrics.recordTransactionProcessed());

        KStream<String, String> emptyAlerts = transactions.filter((k, v) -> false);

        KStream<String, String> byUser = transactions.selectKey((key, value) -> {
            try {
                JsonNode node = objectMapper.readTree(value);
                JsonNode userNode = node.get("userId");
                if (userNode != null && !userNode.isNull()) {
                    return userNode.asText();
                }
            } catch (Exception e) {
                log.warn("Failed to extract userId, keeping original key. value={}", value, e);
            }
            return key;
        });

        // -----------------------
        // Rule 1: HIGH_AMOUNT
        // -----------------------
        KStream<String, String> highAmountAlerts;
        if (highAmountEnabled) {
            highAmountAlerts = byUser
                    .filter((key, value) -> {
                        try {
                            JsonNode node = objectMapper.readTree(value);
                            JsonNode amountNode = node.get("amount");
                            return amountNode != null && amountNode.asDouble() > highAmountThreshold;
                        } catch (Exception e) {
                            log.warn("Failed to parse transaction JSON for high-amount rule, value={}", value, e);
                            return false;
                        }
                    })
                    .map((key, value) -> {
                        try {
                            JsonNode node = objectMapper.readTree(value);
                            String userId = safeText(node, "userId");
                            String txId = safeText(node, "transactionId");
                            double amount = node.get("amount").asDouble();
                            String eventTs = safeText(node, "eventTimestamp");
                            String alertTs = Instant.now().toString();

                            String producerTs = safeText(node, "producerTimestamp");
                            String producerTsJson = (producerTs != null && !producerTs.isBlank())
                                    ? ("\"" + producerTs + "\"")
                                    : "null";

                            String alertJson = """
                                    {
                                      "type": "HIGH_AMOUNT",
                                      "userId": "%s",
                                      "transactionId": "%s",
                                      "amount": %.2f,
                                      "reason": "Transaction amount exceeds threshold %.2f",
                                      "producerTimestamp": %s,
                                      "eventTimestamp": "%s",
                                      "alertTimestamp": "%s"
                                    }
                                    """.formatted(
                                    userId,
                                    txId,
                                    amount,
                                    highAmountThreshold,
                                    producerTsJson,
                                    eventTs,
                                    alertTs
                            );

                            return KeyValue.pair(userId, alertJson);
                        } catch (Exception e) {
                            log.warn("Failed to build high-amount alert from value={}", value, e);
                            return KeyValue.pair(key, value);
                        }
                    })
                    .peek((key, value) -> fraudProcessingMetrics.recordAlertProduced("HIGH_AMOUNT"));
        } else {
            highAmountAlerts = emptyAlerts;
        }

        // -----------------------
        // Rule 2: HIGH_FREQUENCY
        // -----------------------
        KStream<String, String> highFrequencyAlerts;
        if (highFrequencyEnabled) {
            highFrequencyAlerts = byUser
                    .transform(
                            () -> new HighFrequencySlidingTransformer(
                                    objectMapper,
                                    highFrequencyWindowSeconds,
                                    highFrequencyMaxTransactions
                            ),
                            "high-frequency-store"
                    )
                    .filter((key, value) -> value != null)
                    .peek((key, value) -> fraudProcessingMetrics.recordAlertProduced("HIGH_FREQUENCY"));
        } else {
            highFrequencyAlerts = emptyAlerts;
        }

        // -----------------------------------------
        // Rule 3: IMPOSSIBLE_TRAVEL
        // -----------------------------------------
        KStream<String, String> locationVelocityAlerts;
        if (impossibleTravelEnabled) {
            locationVelocityAlerts = byUser
                    .transform(
                            () -> new LocationVelocityTransformer(
                                    objectMapper,
                                    locationVelocityThresholdKmh,
                                    locationMinDistanceKm,
                                    locationMinSecondsBetweenEvents,
                                    locationMaxAgeMinutes
                            ),
                            "last-location-store"
                    )
                    .filter((key, value) -> value != null)
                    .peek((key, value) -> fraudProcessingMetrics.recordAlertProduced("IMPOSSIBLE_TRAVEL"));
        } else {
            locationVelocityAlerts = emptyAlerts;
        }

        KStream<String, String> mergedAlerts = highAmountAlerts
                .merge(highFrequencyAlerts)
                .merge(locationVelocityAlerts);

        mergedAlerts.to(fraudAlertsTopic, Produced.with(STRING_SERDE, STRING_SERDE));

        return mergedAlerts;
    }

    private static String safeText(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode f = node.get(field);
        if (f == null || f.isNull()) return null;
        return f.asText();
    }

    class HighFrequencySlidingTransformer
            implements org.apache.kafka.streams.kstream.Transformer<String, String, KeyValue<String, String>> {

        private final ObjectMapper objectMapper;
        private final long windowSeconds;
        private final long maxTransactions;
        private KeyValueStore<String, String> store;

        HighFrequencySlidingTransformer(ObjectMapper objectMapper,
                                        long windowSeconds,
                                        long maxTransactions) {
            this.objectMapper = objectMapper;
            this.windowSeconds = windowSeconds;
            this.maxTransactions = maxTransactions;
        }

        @Override
        public void init(ProcessorContext context) {
            this.store = (KeyValueStore<String, String>) context.getStateStore("high-frequency-store");
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            try {
                JsonNode node = objectMapper.readTree(value);
                String userId = safeText(node, "userId");
                String eventTsText = safeText(node, "eventTimestamp");

                String producerTsText = safeText(node, "producerTimestamp");
                if (userId == null || eventTsText == null) {
                    return null;
                }

                long eventTsEpoch = Instant.parse(eventTsText).toEpochMilli();
                long windowMillis = windowSeconds * 1000L;
                long cutoff = eventTsEpoch - windowMillis;

                String prev = store.get(userId);
                Deque<Long> timestamps = new ArrayDeque<>();

                if (prev != null && !prev.isEmpty()) {
                    String[] parts = prev.split(",");
                    for (String p : parts) {
                        if (p.isEmpty()) continue;
                        try {
                            long t = Long.parseLong(p.trim());
                            if (t >= cutoff) {
                                timestamps.addLast(t);
                            }
                        } catch (NumberFormatException ignored) {
                        }
                    }
                }

                timestamps.addLast(eventTsEpoch);

                StringBuilder sb = new StringBuilder();
                for (Long ts : timestamps) {
                    if (sb.length() > 0) sb.append(',');
                    sb.append(ts);
                }
                store.put(userId, sb.toString());

                long count = timestamps.size();
                if (count >= maxTransactions) {
                    long firstTs = timestamps.peekFirst();
                    Instant windowStart = Instant.ofEpochMilli(firstTs);
                    Instant windowEnd = Instant.ofEpochMilli(eventTsEpoch);


                    String producerTsJson = (producerTsText != null && !producerTsText.isBlank()) ? ("\"" + producerTsText + "\"") : "null";
                    String alertTs = Instant.now().toString();

                    String alertJson = """
                            {
                              "type": "HIGH_FREQUENCY",
                              "userId": "%s",
                              "producerTimestamp": %s,
                              "count": %d,
                              "windowStart": "%s",
                              "windowEnd": "%s",
                              "maxEventTimestamp": "%s",
                              "reason": "User exceeded %d transactions in %d seconds",
                              "alertTimestamp": "%s"
                            }
                            """
                            .formatted(
                                    userId,
                                    producerTsJson,
                                    count,
                                    windowStart,
                                    windowEnd,
                                    eventTsText,
                                    maxTransactions,
                                    windowSeconds,
                                    alertTs
                            );

                    return KeyValue.pair(userId, alertJson);
                }

                return null;
            } catch (Exception e) {
                log.warn("Error in HighFrequencySlidingTransformer, key={}, value={}", key, value, e);
                return null;
            }
        }

        @Override
        public void close() {
        }
    }

    class LocationVelocityTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private final ObjectMapper objectMapper;
        private final double thresholdKmh;
        private final long maxGapSeconds;
        private KeyValueStore<String, String> store;

        LocationVelocityTransformer(ObjectMapper objectMapper,
                                    double thresholdKmh,
                                    double unusedMinDistanceKm,
                                    long unusedMinSecondsBetweenEvents,
                                    long maxAgeMinutes) {
            this.objectMapper = objectMapper;
            this.thresholdKmh = thresholdKmh;
            this.maxGapSeconds = maxAgeMinutes * 60L;
        }

        @Override
        public void init(ProcessorContext context) {
            this.store = (KeyValueStore<String, String>) context.getStateStore("last-location-store");
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            try {
                JsonNode current = objectMapper.readTree(value);
                String userId = safeText(current, "userId");
                String location = safeText(current, "location");
                String tsText = safeText(current, "eventTimestamp");

                String producerTsText = safeText(current, "producerTimestamp");
                if (userId == null || location == null || tsText == null) {
                    return null;
                }

                Instant currentTs = Instant.parse(tsText);

                String prevJson = store.get(userId);
                if (prevJson == null) {
                    store.put(userId, value);
                    return null;
                }

                JsonNode previous = objectMapper.readTree(prevJson);
                String prevLocation = safeText(previous, "location");
                String prevTsText = safeText(previous, "eventTimestamp");

                if (prevLocation == null || prevTsText == null) {
                    store.put(userId, value);
                    return null;
                }

                Instant prevTs = Instant.parse(prevTsText);

                long secondsBetween = Duration.between(prevTs, currentTs).getSeconds();
                if (secondsBetween <= 0) {
                    store.put(userId, value);
                    return null;
                }
                if (secondsBetween > maxGapSeconds) {
                    store.put(userId, value);
                    return null;
                }

                double distanceKm = lookupDistanceKm(prevLocation, location);
                if (distanceKm <= 0.0) {
                    store.put(userId, value);
                    return null;
                }

                double hours = secondsBetween / 3600.0;
                double speedKmh = distanceKm / hours;

                if (speedKmh <= this.thresholdKmh) {
                    store.put(userId, value);
                    return null;
                }

                String alertTs = Instant.now().toString();

                String producerTsJson = (producerTsText != null && !producerTsText.isBlank()) ? ("\"" + producerTsText + "\"") : "null";

                String alertJson = """
                    {
                      "type": "IMPOSSIBLE_TRAVEL",
                      "userId": "%s",
                      "producerTimestamp": %s,
                      "fromLocation": "%s",
                      "toLocation": "%s",
                      "fromTimestamp": "%s",
                      "toTimestamp": "%s",
                      "distanceKm": %.2f,
                      "speedKmh": %.2f,
                      "reason": "Inferred travel speed exceeds threshold %.2f km/h",
                      "alertTimestamp": "%s"
                    }
                    """
                        .formatted(
                                userId,
                                producerTsJson,
                                prevLocation,
                                location,
                                prevTs,
                                currentTs,
                                distanceKm,
                                speedKmh,
                                this.thresholdKmh,
                                alertTs
                        );

                store.put(userId, value);

                return KeyValue.pair(userId, alertJson);
            } catch (Exception e) {
                log.warn("Error in LocationVelocityTransformer, key={}, value={}", key, value, e);
                return null;
            }
        }

        @Override
        public void close() {
        }

        private static final Map<String, Double> DISTANCE_KM = Map.ofEntries(
                Map.entry("US-NY|DE-BERLIN", 6385.0),
                Map.entry("DE-BERLIN|US-NY", 6385.0),
                Map.entry("US-NY|JP-TOKYO", 10800.0),
                Map.entry("JP-TOKYO|US-NY", 10800.0),
                Map.entry("DE-BERLIN|JP-TOKYO", 8920.0),
                Map.entry("JP-TOKYO|DE-BERLIN", 8920.0),
                Map.entry("DE-FRANKFURT|US-NY", 6200.0),
                Map.entry("US-NY|DE-FRANKFURT", 6200.0)
        );

        private static double lookupDistanceKm(String from, String to) {
            if (from == null || to == null || from.equals(to)) {
                return 0.0;
            }
            String key1 = from + "|" + to;
            Double d = DISTANCE_KM.get(key1);
            return d != null ? d : 0.0;
        }
    }
}
