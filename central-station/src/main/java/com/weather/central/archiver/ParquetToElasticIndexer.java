package com.weather.central.archiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class ParquetToElasticIndexer {

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String parquetDir = params.getOrDefault("parquet-dir",
                System.getenv().getOrDefault("PARQUET_DIR", "/data/parquet"));
        String esUrl = params.getOrDefault("es-url", "http://localhost:9200");
        String index = params.getOrDefault("index", "weather-status");
        int batchSize = Integer.parseInt(params.getOrDefault("batch-size", "1000"));

        List<java.nio.file.Path> parquetFiles = new ArrayList<>();
        try (Stream<java.nio.file.Path> stream = Files.walk(Paths.get(parquetDir))) {
            stream.filter(p -> p.toString().endsWith(".parquet")).forEach(parquetFiles::add);
        }

        if (parquetFiles.isEmpty()) {
            System.out.println("No parquet files found under: " + parquetDir);
            return;
        }

        ObjectMapper mapper = new ObjectMapper();
        HttpClient client = HttpClient.newHttpClient();

        for (java.nio.file.Path file : parquetFiles) {
            indexFile(file, client, mapper, esUrl, index, batchSize);
        }
    }

    private static void indexFile(java.nio.file.Path file,
                                  HttpClient client,
                                  ObjectMapper mapper,
                                  String esUrl,
                                  String index,
                                  int batchSize) throws IOException, InterruptedException {
        System.out.println("Indexing parquet file: " + file);
        Configuration conf = new Configuration();
        Path path = new Path(file.toString());

        List<String> bulkLines = new ArrayList<>(batchSize * 2);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
                .withConf(conf)
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Map<String, Object> doc = new LinkedHashMap<>();
                for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                    Object value = record.get(field.name());
                    if (value instanceof org.apache.avro.util.Utf8) {
                        value = value.toString();
                    }
                    doc.put(field.name(), value);
                }

                bulkLines.add("{\"index\":{\"_index\":\"" + index + "\"}}");
                bulkLines.add(mapper.writeValueAsString(doc));

                if (bulkLines.size() >= batchSize * 2) {
                    sendBulk(client, esUrl, bulkLines);
                    bulkLines.clear();
                }
            }
        }

        if (!bulkLines.isEmpty()) {
            sendBulk(client, esUrl, bulkLines);
        }
    }

    private static void sendBulk(HttpClient client, String esUrl, List<String> bulkLines)
            throws IOException, InterruptedException {
        String payload = String.join("\n", bulkLines) + "\n";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(esUrl + "/_bulk"))
                .header("Content-Type", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Elasticsearch bulk request failed: " + response.statusCode()
                    + " - " + response.body());
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                String[] parts = arg.substring(2).split("=", 2);
                params.put(parts[0], parts[1]);
            }
        }
        return params;
    }
}
