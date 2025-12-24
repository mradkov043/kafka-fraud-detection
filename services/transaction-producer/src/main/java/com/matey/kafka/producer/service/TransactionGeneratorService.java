package com.matey.kafka.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.matey.kafka.producer.metrics.ProducerMetrics;
import com.matey.kafka.producer.model.TransactionEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically generates synthetic TransactionEvent records
 * and sends them to the configured Kafka transactions topic.
 *
 * The generator supports two profiles:
 *
 * "normal" – realistic mix of single transactions, bursts,
 * high-amount payments and "impossible travel" pairs
 *
 * "benchmark" – heavier synthetic load with multiple
 * transactions per scheduler tick
 *
 * The effective tick rate is derived from the configured target TPS and
 * an estimate of "transactions per tick" so that the observed throughput
 * stays close to the configuration. Producer send latency is recorded
 * via ProducerMetrics for the benchmark evaluation.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionGeneratorService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ProducerMetrics producerMetrics;

    @Value("${app.kafka.transactions-topic}")
    private String transactionsTopic;

    @Value("${app.generator.transactions-per-second:10}")
    private int transactionsPerSecond;

    @Value("${app.generator.profile:normal}")
    private String profile;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "tx-generator");
                t.setDaemon(true);
                return t;
            });

    private final Random random = new Random();

    private static final double NORMAL_PROB = 0.70;
    private static final double HIGH_AMOUNT_PROB = 0.15;
    private static final double BURST_PROB = 0.13;
    private static final double IMPOSSIBLE_TRAVEL_PROB = 0.02;

    private static final int BURST_MIN = 8;
    private static final int BURST_MAX = 15;

    @PostConstruct
    public void init() {
        log.info("Initializing TransactionGeneratorService with profile='{}', targetTPS={}",
                profile, transactionsPerSecond);
        startGeneratorScheduler();
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down transaction generator");
        scheduler.shutdownNow();
    }

    //Currently unused
    public synchronized void restart(int newTps, String newProfile) {
        log.info("Restarting generator with new config: tps={}, profile={}", newTps, newProfile);
        this.transactionsPerSecond = newTps;
        this.profile = newProfile;

        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        startGeneratorScheduler();
    }

    private void startGeneratorScheduler() {
        log.info("Starting transaction generator scheduler…");

        double expectedPerTick = estimateExpectedTransactionsPerTick();
        if (expectedPerTick <= 0.0) {
            expectedPerTick = 1.0;
        }

        double tickRatePerSecond = ((double) transactionsPerSecond) / expectedPerTick;
        if (tickRatePerSecond <= 0.0) {
            tickRatePerSecond = 1.0;
        }

        long periodMillis = (long) Math.max(1.0, 1000.0 / tickRatePerSecond);

        log.info(
                "Starting transaction generator with target ~{} tx/sec, expectedPerTick≈{}, " +
                        "tickRate≈{} ticks/sec ({} ms between ticks), profile='{}'",
                transactionsPerSecond,
                String.format("%.2f", expectedPerTick),
                String.format("%.2f", tickRatePerSecond),
                periodMillis,
                profile
        );

        producerMetrics.startNewRun(transactionsPerSecond);

        scheduler.scheduleAtFixedRate(
                this::generateTick,
                1000L,
                periodMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void generateTick() {
        try {
            String p = profile == null ? "normal" : profile.toLowerCase();
            switch (p) {
                case "benchmark" -> generateBenchmarkTick();
                case "normal" -> generateNormalTick();
                default -> generateNormalTick();
            }
        } catch (Exception e) {
            log.warn("Error in generateTick", e);
        }
    }

    // -------------------- NORMAL / REALISTIC PROFILE --------------------

    private void generateNormalTick() {
        double r = random.nextDouble();

        if (r < NORMAL_PROB) {
            sendSingleTransaction(false);
        } else if (r < NORMAL_PROB + HIGH_AMOUNT_PROB) {
            sendSingleTransaction(true);
        } else if (r < NORMAL_PROB + HIGH_AMOUNT_PROB + BURST_PROB) {
            int burstSize = randomInt(BURST_MIN, BURST_MAX);
            sendBurst(burstSize, false);
        } else {
            sendImpossibleTravelPair();
        }
    }

    // -------------------- BENCHMARK PROFILE --------------------

    private void generateBenchmarkTick() {
        int txCount = randomInt(10, 20);
        for (int i = 0; i < txCount; i++) {
            sendSingleTransaction(false);
        }
    }


    private void sendSingleTransaction(boolean highAmount) {
        TransactionEvent event = createRandomTransaction(highAmount, null, null);

        event.setProducerTimestamp(Instant.now());

        try {
            String value = objectMapper.writeValueAsString(event);
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(transactionsTopic, event.getUserId(), value);

            if (random.nextDouble() < 0.10) {
                record.headers().add(
                        "producerSendTs",
                        Long.toString(System.currentTimeMillis()).getBytes()
                );
            }

            final long startNanos = System.nanoTime();
            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;

                if (ex != null) {
                    log.error("Failed to send transaction {}", event.getTransactionId(), ex);
                    producerMetrics.recordFailure(elapsedMicros, ex);
                } else if (result != null) {
                    producerMetrics.recordSuccess(elapsedMicros);
                    if (log.isDebugEnabled()) {
                        log.debug("Sent tx {} (user={}, amount={}, loc={}) to partition {} offset {}",
                                event.getTransactionId(),
                                event.getUserId(),
                                event.getAmount(),
                                event.getLocation(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                }
            });
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize transaction", e);
            producerMetrics.recordFailure(0, e);
        }
    }


    private void sendBurst(int count, boolean highAmount) {
        for (int i = 0; i < count; i++) {
            sendSingleTransaction(highAmount);
            sleepQuietly(2);
        }
    }

    /**
     * Deterministic impossible-travel generator.
     * Creates a pair of events for the same user far apart in space, close in time.
     */
    private void sendImpossibleTravelPair() {
        String userId = "user-" + randomInt(1, 500);

        String[][] PAIRS = {
                {"US-NY", "DE-BERLIN"},
                {"DE-BERLIN", "US-NY"},
                {"US-NY", "JP-TOKYO"},
                {"DE-FRANKFURT", "US-NY"}
        };
        String[] pair = PAIRS[random.nextInt(PAIRS.length)];
        String from = pair[0];
        String to   = pair[1];

        int gapMinutes = 10;

        Instant secondTs = Instant.now();
        Instant firstTs  = secondTs.minusSeconds(gapMinutes * 60L);

        TransactionEvent first  = createRandomTransaction(false, userId, from, firstTs);
        TransactionEvent second = createRandomTransaction(false, userId, to,   secondTs);

        first.setProducerTimestamp(Instant.now());
        second.setProducerTimestamp(Instant.now());

        try {
            String v1 = objectMapper.writeValueAsString(first);
            String v2 = objectMapper.writeValueAsString(second);

            sendRaw(userId, v1);
            sleepQuietly(5);
            sendRaw(userId, v2);

        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize impossible-travel pair", e);
        }
    }

    private void sendRaw(String key, String value) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(transactionsTopic, key, value);

        if (random.nextDouble() < 0.10) {
            record.headers().add(
                    "producerSendTs",
                    Long.toString(System.currentTimeMillis()).getBytes()
            );
        }

        final long startNanos = System.nanoTime();
        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;

            if (ex != null) {
                log.error("Failed to send raw transaction", ex);
                producerMetrics.recordFailure(elapsedMicros, ex);
            } else if (result != null) {
                producerMetrics.recordSuccess(elapsedMicros);
            }
        });
    }


    // -------------------- Event factory --------------------

    private TransactionEvent createRandomTransaction(boolean highAmountOverride,
                                                     String fixedUserId,
                                                     String fixedLocation) {
        return createRandomTransaction(highAmountOverride, fixedUserId, fixedLocation, null);
    }

    private TransactionEvent createRandomTransaction(boolean highAmountOverride,
                                                     String fixedUserId,
                                                     String fixedLocation,
                                                     Instant eventTimestampOverride) {

        String userId = fixedUserId != null
                ? fixedUserId
                : "user-" + randomInt(1, 1000);

        String location = fixedLocation != null
                ? fixedLocation
                : randomLocation();

        BigDecimal amount = highAmountOverride
                ? randomAmount(3000, 10000)
                : randomAmount(5, 1500);

        Instant eventTs = (eventTimestampOverride != null)
                ? eventTimestampOverride
                : Instant.now();

        // producer-side TransactionEvent ctor:
        //   TransactionEvent(String transactionId,
        //                    String userId,
        //                    BigDecimal amount,
        //                    Instant eventTimestamp,
        //                    String location)
        return new TransactionEvent(
                UUID.randomUUID().toString(),
                userId,
                amount,
                eventTs,
                location
        );
    }

    private String randomLocation() {
        String[] locations = {"DE-FRANKFURT", "DE-BERLIN", "US-NY", "US-CA", "UK-LON", "FR-PAR"};
        return locations[random.nextInt(locations.length)];
    }

    private BigDecimal randomAmount(int min, int max) {
        double value = min + (max - min) * random.nextDouble();
        return BigDecimal.valueOf(value)
                .setScale(2, RoundingMode.HALF_UP);
    }

    private int randomInt(int minInclusive, int maxInclusive) {
        return random.nextInt((maxInclusive - minInclusive) + 1) + minInclusive;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Rough estimate of how many transactions we emit per scheduler tick.
     *
     * This is used only to derive the scheduler period so that the
     * *measured* producer TPS is close to the configured transactions-per-second.
     *
     * For the current profiles:
     *  - "normal"      (realistic): most ticks send 1 tx, some send bursts
     *    and a few send impossible-travel pairs.
     *  - "benchmark":  each tick sends between 10 and 20 transactions, average ≈15.
     */
    private double estimateExpectedTransactionsPerTick() {
        String p = profile == null ? "" : profile.toLowerCase();

        if ("benchmark".equals(p)) {
            return 15.0;
        }

        // Default / "normal" realistic profile.
        // Probabilities in generateNormalTick():
        //   70% single normal tx
        //   15% single high-amount tx
        //   13% burst of 8–15 tx (mean 11.5)
        //    2% impossible-travel pair (2 tx)
        //
        // E[tx] = 0.70*1 + 0.15*1 + 0.13*11.5 + 0.02*2  ≈ 2.385
        return 2.385;
    }
}
