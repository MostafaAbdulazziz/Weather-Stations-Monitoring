package com.weather.central.archiver;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class DropParquetArchiver {

    private static final int DEFAULT_BATCH_SIZE = Integer.parseInt(
            System.getenv().getOrDefault("DROP_PARQUET_BATCH_SIZE", "10000"));
    private static final long DEFAULT_FLUSH_INTERVAL_MS = Long.parseLong(
            System.getenv().getOrDefault("DROP_PARQUET_FLUSH_MS", "5000"));
    private static final int DEFAULT_QUEUE_CAPACITY = Integer.parseInt(
            System.getenv().getOrDefault("DROP_PARQUET_QUEUE_CAPACITY", "50000"));
    private static final String BASE_DIR;

    static {
        BASE_DIR = System.getenv().getOrDefault("DROP_PARQUET_DIR", "/data/parquet_drops");
    }

    private static final String SCHEMA_JSON = """
        {
          "type": "record",
          "name": "DropRecord",
          "fields": [
            {"name": "station_id",       "type": "long"},
            {"name": "from_s_no",        "type": "long"},
            {"name": "to_s_no",          "type": "long"},
            {"name": "dropped_count",    "type": "int"},
            {"name": "status_timestamp", "type": "long"}
          ]
        }
        """;

    private final Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    private final BlockingQueue<DropEvent> queue;
    private final List<DropEvent> buffer;
    private final Thread worker;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ReentrantLock flushLock = new ReentrantLock();
    private final int batchSize;
    private final long flushIntervalMs;

    public DropParquetArchiver() {
        this(DEFAULT_BATCH_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_QUEUE_CAPACITY);
    }

    public DropParquetArchiver(int batchSize, long flushIntervalMs, int queueCapacity) {
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.buffer = new ArrayList<>(Math.max(1024, batchSize));

        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(BASE_DIR));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create drop parquet base dir: " + BASE_DIR, e);
        }

        this.worker = new Thread(this::runWriter, "drop-parquet-archiver");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void archive(DropEvent event) throws IOException {
        if (!running.get()) {
            throw new IOException("DropParquetArchiver is closed");
        }
        try {
            if (!queue.offer(event, 2, TimeUnit.SECONDS)) {
                throw new IOException("DropParquetArchiver queue is full");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing drop event", e);
        }
    }

    public void flush() throws IOException {
        List<DropEvent> drain = new ArrayList<>(batchSize);
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
                DropEvent event = queue.poll(200, TimeUnit.MILLISECONDS);
                if (event != null) {
                    buffer.add(event);
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
                System.err.println("DropParquetArchiver error: " + e);
            }
        }

        if (!buffer.isEmpty()) {
            try {
                flushBatch(new ArrayList<>(buffer));
            } catch (IOException e) {
                System.err.println("DropParquetArchiver final flush error: " + e.getMessage());
            }
            buffer.clear();
        }
    }

    private void flushBatch(List<DropEvent> batch) throws IOException {
        flushLock.lock();
        try {
            if (batch.isEmpty()) return;

            Map<String, List<DropEvent>> partitions = new HashMap<>();
            for (DropEvent e : batch) {
                LocalDateTime dt = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(e.getStatusTimestamp()), ZoneOffset.UTC);
                String date = dt.toLocalDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String hour = String.format("%02d", dt.getHour());
                String partitionKey = "date=" + date + "/hour=" + hour + "/station=" + e.getStationId();
                partitions.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(e);
            }

            for (Map.Entry<String, List<DropEvent>> entry : partitions.entrySet()) {
                String dirPath = BASE_DIR + "/" + entry.getKey();
                java.nio.file.Files.createDirectories(java.nio.file.Paths.get(dirPath));

                String filePath = dirPath + "/" + System.currentTimeMillis()
                        + "-" + ThreadLocalRandom.current().nextInt(1000) + ".parquet";

                writeParquet(filePath, entry.getValue());
                System.out.println("DropParquet: wrote " + entry.getValue().size()
                        + " records to " + filePath);
            }
        } finally {
            flushLock.unlock();
        }
    }

    private void writeParquet(String filePath, List<DropEvent> records) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (DropEvent e : records) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("station_id", e.getStationId());
                record.put("from_s_no", e.getFromSeq());
                record.put("to_s_no", e.getToSeq());
                record.put("dropped_count", e.getDroppedCount());
                record.put("status_timestamp", e.getStatusTimestamp());
                writer.write(record);
            }
        }
    }
}

