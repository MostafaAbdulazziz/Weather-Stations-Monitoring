package com.weather.central.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.central.WeatherMessage;
import com.weather.central.archiver.ParquetArchiver;
import com.weather.central.bitcask.BitCask;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class WeatherConsumer {

    private static final String TOPIC = "weather-readings";
    private static final String GROUP = "central-station-group";
    private static final int THREAD_COUNT = 3; // worker threads

    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    // ── Stats per station ────────────────────────────────────────────────────
    private final Map<Long, AtomicLong> receivedPerStation = new ConcurrentHashMap<>();
    private final Map<Long, AtomicLong> droppedPerStation = new ConcurrentHashMap<>();
    private final AtomicLong totalReceived = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    private final BitCask bitCask;
    private final ParquetArchiver archiver;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String kafkaBootstrap;

    // Thread pool for processing records in parallel
    private final ExecutorService workerPool =
            Executors.newFixedThreadPool(THREAD_COUNT);

    // Single Kafka consumer thread
    private final ExecutorService consumerThread =
            Executors.newSingleThreadExecutor();

    private volatile boolean running = true;

    public WeatherConsumer(BitCask bitCask,
                           ParquetArchiver archiver,
                           String kafkaBootstrap) {
        this.bitCask = bitCask;
        this.archiver = archiver;
        this.kafkaBootstrap = kafkaBootstrap;
    }

    // ── Start everything ─────────────────────────────────────────────────────
    public void start() {
        log("INFO", "CONSUMER", "Starting threaded consumer with "
                + THREAD_COUNT + " worker threads");
        consumerThread.submit(this::consumeLoop);
        startStatsReporter();
        log("INFO", "CONSUMER", "Consumer started successfully");
    }

    // ── Main Kafka poll loop (runs on single thread) ──────────────────────────
    private void consumeLoop() {
        KafkaConsumer<String, String> consumer = buildKafkaConsumer();
        consumer.subscribe(List.of(TOPIC));
        log("INFO", "KAFKA", "Subscribed to topic: " + TOPIC
                + " | Bootstrap: " + kafkaBootstrap);

        try {
            while (running) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                if (records.isEmpty()) continue;

                log("DEBUG", "KAFKA", "Polled " + records.count() + " records");

                // Submit each record to worker pool
                List<Future<?>> futures = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records) {
                    Future<?> f = workerPool.submit(
                            () -> processRecord(record));
                    futures.add(f);
                }

                // Wait for all workers to finish before next poll
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (Exception e) {
                        log("ERROR", "WORKER",
                                "Worker failed: " + e.getMessage());
                    }
                }

                // Commit offsets after all records processed
                consumer.commitSync();
            }
        } catch (Exception e) {
            log("ERROR", "KAFKA", "Consumer loop crashed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                archiver.flush();
            } catch (Exception ignored) {
            }
            consumer.close();
            log("INFO", "KAFKA", "Consumer closed cleanly");
        }
    }

    // ── Process a single record (runs on worker thread) ───────────────────────
    private void processRecord(ConsumerRecord<String, String> record) {
        String threadName = Thread.currentThread().getName();
        try {
            WeatherMessage msg = mapper.readValue(
                    record.value(), WeatherMessage.class);

            long stationId = msg.getStation_id();

            // Init counters for new station
            receivedPerStation.computeIfAbsent(stationId,
                    k -> new AtomicLong(0)).incrementAndGet();
            droppedPerStation.computeIfAbsent(stationId,
                    k -> new AtomicLong(0));
            totalReceived.incrementAndGet();

            // Detect dropped messages by gap in sequence number
            // (stored as "last_sno_<id>" in bitcask)
            String lastSnoKey = "last_sno_" + stationId;
            String lastSnoVal = bitCask.get(lastSnoKey);
            if (lastSnoVal != null) {
                long lastSno = Long.parseLong(lastSnoVal);
                long gap = msg.getS_no() - lastSno - 1;
                if (gap > 0) {
                    droppedPerStation.get(stationId).addAndGet(gap);
                    log("WARN", "STATION-" + stationId,
                            "Detected " + gap + " dropped message(s) "
                                    + "[last=" + lastSno
                                    + " current=" + msg.getS_no() + "]");
                }
            }

            // Update last sequence number
            bitCask.put(lastSnoKey, String.valueOf(msg.getS_no()));

            // Update latest reading in BitCask
            bitCask.put(String.valueOf(stationId), record.value());

            // Archive to Parquet
            archiver.archive(msg);

            // Console print
            log("INFO", "STATION-" + stationId,
                    String.format("[%s] seq=%-5d battery=%-6s "
                                    + "humidity=%3d%% temp=%3dF wind=%3dkm/h | "
                                    + "thread=%s",
                            FMT.format(Instant.ofEpochSecond(
                                    msg.getStatus_timestamp())),
                            msg.getS_no(),
                            msg.getBattery_status(),
                            msg.getWeather().getHumidity(),
                            msg.getWeather().getTemperature(),
                            msg.getWeather().getWind_speed(),
                            threadName));

        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log("ERROR", "WORKER",
                    "Failed to process record from partition="
                            + record.partition()
                            + " offset=" + record.offset()
                            + " | " + e.getMessage());
        }
    }

    // ── Stats reporter (prints every 10 seconds) ──────────────────────────────
    private void startStatsReporter() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("\n" + "=".repeat(70));
            System.out.printf("  STATS REPORT @ %s%n",
                    FMT.format(Instant.now()));
            System.out.println("=".repeat(70));
            System.out.printf("  %-12s %-12s %-12s %-12s%n",
                    "Station", "Received", "Dropped", "Drop Rate");
            System.out.println("-".repeat(70));

            long grandTotal = totalReceived.get();
            long grandDropped = droppedPerStation.values().stream()
                    .mapToLong(AtomicLong::get).sum();

            // Sort by station ID
            new TreeMap<>(receivedPerStation).forEach((stationId, received) -> {
                long dropped = droppedPerStation
                        .getOrDefault(stationId, new AtomicLong(0)).get();
                long total = received.get() + dropped;
                double rate = total > 0
                        ? (dropped * 100.0 / total) : 0.0;
                System.out.printf("  %-12d %-12d %-12d %.1f%%%n",
                        stationId, received.get(), dropped, rate);
            });

            System.out.println("-".repeat(70));
            System.out.printf("  %-12s %-12d %-12d %.1f%%%n",
                    "TOTAL", grandTotal, grandDropped,
                    (grandTotal + grandDropped) > 0
                            ? (grandDropped * 100.0 /
                               (grandTotal + grandDropped)) : 0.0);
            System.out.printf("  Errors: %d%n", totalErrors.get());
            System.out.println("=".repeat(70) + "\n");

        }, 10, 10, TimeUnit.SECONDS);
    }

    // ── Graceful shutdown ────────────────────────────────────────────────────
    public void stop() {
        log("INFO", "CONSUMER", "Shutting down...");
        running = false;
        workerPool.shutdown();
        consumerThread.shutdown();
        try {
            workerPool.awaitTermination(5, TimeUnit.SECONDS);
            consumerThread.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("INFO", "CONSUMER", "Shutdown complete");
    }

    // ── Kafka consumer factory ────────────────────────────────────────────────
    private KafkaConsumer<String, String> buildKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaBootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        // Manual commit — we commit after all workers finish
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                "50");
        return new KafkaConsumer<>(props);
    }

    // ── Structured logger ────────────────────────────────────────────────────
    private static void log(String level, String component, String msg) {
        System.out.printf("[%s] [%-5s] [%-12s] %s%n",
                FMT.format(Instant.now()),
                level,
                component,
                msg);
    }
}