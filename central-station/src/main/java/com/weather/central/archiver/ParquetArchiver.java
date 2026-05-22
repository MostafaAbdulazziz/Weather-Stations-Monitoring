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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ParquetArchiver {

    private static final int BATCH_SIZE = 100;
    private final String baseDir;

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

    private final Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    private final List<WeatherMessage> buffer = new ArrayList<>();
    private final Object bufferLock = new Object();

    // Safe Hadoop config — no security manager, no Kerberos
    private final Configuration hadoopConf;

    public ParquetArchiver(String baseDir) {
        this.baseDir = baseDir;

        // Build a minimal Hadoop config that avoids security checks
        hadoopConf = new Configuration(false); // false = don't load defaults
        hadoopConf.set("hadoop.security.authentication", "simple");
        hadoopConf.set("hadoop.security.authorization", "false");
        hadoopConf.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        hadoopConf.set("fs.defaultFS", "file:///");
        hadoopConf.setClassLoader(Thread.currentThread().getContextClassLoader());

        System.out.println("ParquetArchiver initialized → " + baseDir);
    }

    public void archive(WeatherMessage msg) throws IOException {
        synchronized (bufferLock) {
            buffer.add(msg);
            if (buffer.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    public void flush() throws IOException {
        synchronized (bufferLock) {
            if (buffer.isEmpty()) return;

            // Partition by date and station
            Map<String, List<WeatherMessage>> partitions = new HashMap<>();
            for (WeatherMessage m : buffer) {
                String date = LocalDate.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String key = "date=" + date + "/station=" + m.getStation_id();
                partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(m);
            }

            for (Map.Entry<String, List<WeatherMessage>> entry :
                    partitions.entrySet()) {
                String dirPath = baseDir + "/" + entry.getKey();
                java.nio.file.Files.createDirectories(
                    java.nio.file.Paths.get(dirPath));
                String filePath = dirPath + "/" +
                    System.currentTimeMillis() + ".parquet";
                writeParquet(filePath, entry.getValue());
                System.out.println("Parquet: wrote "
                    + entry.getValue().size() + " records → " + filePath);
            }
            buffer.clear();
        }
    }

    private void writeParquet(String filePath,
                               List<WeatherMessage> records) throws IOException {
        Path path = new Path("file:///" + filePath);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withConf(hadoopConf)          // use our safe config
                .withCompressionCodec(CompressionCodecName.SNAPPY)
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
}
