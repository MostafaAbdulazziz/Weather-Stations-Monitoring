package com.weather.central.archiver;

import com.weather.central.WeatherMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ParquetArchiver {

    private static final int DEFAULT_BATCH_SIZE = Integer.parseInt(
            System.getenv().getOrDefault("PARQUET_BATCH_SIZE", "100"));
    private static final long DEFAULT_FLUSH_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("PARQUET_FLUSH_MS", "500"));
    private static final int DEFAULT_QUEUE_CAPACITY = Integer.parseInt(
            System.getenv().getOrDefault("PARQUET_QUEUE_CAPACITY", "50000"));

    private static final CompressionCodecName DEFAULT_CODEC = resolveCodec();

    // Schema definition
    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "WeatherRecord",
          "fields": [
            {"name": "station_id",       "type": "long"},
            {"name": "s_no",             "type": "long"},
            {"name": "battery_status",   "type": "string"},
            {"name": "status_timestamp", "type": "long"},
            {"name": "humidity",         "type": "int"},
            {"name": "temperature",      "type": "int"},
            {"name": "wind_speed",       "type": "int"}
          ]
        }
        """;

    private  Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    private  BlockingQueue<WeatherMessage> queue;
    private  List<WeatherMessage> buffer;
    private  Thread worker;
    private  AtomicBoolean running = new AtomicBoolean(true);
    private  ReentrantLock flushLock = new ReentrantLock();
    private  int batchSize;
    private  long flushIntervalMs;

    private String BASE_DIR;

    // 1. Default constructor
    public ParquetArchiver() {
        this(System.getenv().getOrDefault("PARQUET_DIR", "/data"),
                DEFAULT_BATCH_SIZE,
                DEFAULT_FLUSH_INTERVAL_MS,
                DEFAULT_QUEUE_CAPACITY);
    }

    // 2. Directory-only constructor (This is the one your Main app is likely using)
    public ParquetArchiver(String baseDir) {
        this(baseDir, DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_QUEUE_CAPACITY);
    }

    // 3. Master constructor where everything is actually initialized
    public ParquetArchiver(String baseDir, int batchSize, long flushIntervalMs, int queueCapacity) {
        this.BASE_DIR = baseDir;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.buffer = new ArrayList<>(Math.max(1024, batchSize));

        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(BASE_DIR));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create parquet base dir: " + BASE_DIR, e);
        }

        this.worker = new Thread(this::runWriter, "parquet-archiver");
        this.worker.setDaemon(true);
        this.worker.start();
    }


    public void archive(WeatherMessage msg) throws IOException {
        if (!running.get()) {
            throw new IOException("ParquetArchiver is closed");
        }
        try {
            if (!queue.offer(msg, 2, TimeUnit.SECONDS)) {
                throw new IOException("ParquetArchiver queue is full");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing record", e);
        }
    }

    public void flush() throws IOException {
        List<WeatherMessage> drain = new ArrayList<>(batchSize);
        queue.drainTo(drain, batchSize);
        if (!drain.isEmpty()) {
            flushBatch(drain);
        }
    }

    public void close() throws IOException {
        running.set(false);
        worker.interrupt();
        try {
            worker.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flush();
    }

    private void runWriter() {
        long lastFlush = System.currentTimeMillis();
        while (running.get() || !queue.isEmpty()) {
            try {
                WeatherMessage msg = queue.poll(200, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    buffer.add(msg);
                }
                if (buffer.size() < batchSize) {
                    queue.drainTo(buffer, batchSize - buffer.size());
                }

                long now = System.currentTimeMillis();
                if (!buffer.isEmpty() && (buffer.size() >= batchSize || now - lastFlush >= flushIntervalMs)) {
                    flushBatch(new ArrayList<>(buffer));
                    buffer.clear();
                    lastFlush = now;
                }
            } catch (InterruptedException e) {
                if (!running.get()) {
                    break;
                }
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("ParquetArchiver error: " + e);
            }
        }

        if (!buffer.isEmpty()) {
            try {
                flushBatch(new ArrayList<>(buffer));
            } catch (IOException e) {
                System.err.println("ParquetArchiver final flush error: " + e.getMessage());
            }
            buffer.clear();
        }
    }

    private void flushBatch(List<WeatherMessage> batch) throws IOException {
        flushLock.lock();
        try {
            if (batch.isEmpty()) return;

            Map<String, List<WeatherMessage>> partitions = new HashMap<>();
            for (WeatherMessage m : batch) {
                long ts = m.getStatus_timestamp();
                LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochSecond(ts), ZoneOffset.UTC);
                String date = dt.toLocalDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String hour = String.format("%02d", dt.getHour());
                String partitionKey = "date=" + date + "/hour=" + hour + "/station=" + m.getStation_id();
                partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(m);
            }

            for (Map.Entry<String, List<WeatherMessage>> entry : partitions.entrySet()) {
                String dirPath = BASE_DIR + "/" + entry.getKey();
                java.nio.file.Files.createDirectories(java.nio.file.Paths.get(dirPath));

                String filePath = dirPath + "/" + System.currentTimeMillis()
                        + "-" + ThreadLocalRandom.current().nextInt(1000) + ".parquet";

                writeParquet(filePath, entry.getValue());
                System.out.println("Parquet: wrote " + entry.getValue().size()
                        + " records to " + filePath);
            }
        } finally {
            flushLock.unlock();
        }
    }

    private void writeParquet(String filePath,
                               List<WeatherMessage> records) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(DEFAULT_CODEC)
                .build()) {

            for (WeatherMessage m : records) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("station_id",       m.getStation_id());
                record.put("s_no",             m.getS_no());
                record.put("battery_status",   m.getBattery_status());
                record.put("status_timestamp", m.getStatus_timestamp());
                record.put("humidity",   m.getWeather().getHumidity());
                record.put("temperature",m.getWeather().getTemperature());
                record.put("wind_speed", m.getWeather().getWind_speed());
                writer.write(record);
            }
        }
    }

    private static CompressionCodecName resolveCodec() {
        String raw = System.getenv().getOrDefault("PARQUET_CODEC", "UNCOMPRESSED");
        try {
            return CompressionCodecName.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return CompressionCodecName.UNCOMPRESSED;
        }
    }
}
