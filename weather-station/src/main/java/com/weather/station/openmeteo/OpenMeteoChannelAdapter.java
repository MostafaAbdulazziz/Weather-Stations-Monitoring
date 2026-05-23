package com.weather.station.openmeteo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.station.WeatherMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OpenMeteoChannelAdapter {

    private static final String TOPIC = "weather-readings";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    // Configurable coordinates (Defaults to Alexandria, Egypt)
    private static final double LATITUDE = 31.2001;
    private static final double LONGITUDE = 29.9187;

    // Dynamically inject coordinates into the API URL
    private static final String OPEN_METEO_URL = String.format(
            "https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s"
                    + "&current=temperature_2m,wind_speed_10m"
                    + "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m",
            LATITUDE, LONGITUDE
    );

    // Thread-safe wrapper to handle background updates safely
    private static volatile OpenMeteoResponse.HourlyData hourlyCache = null;
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public static void main(String[] args) {
        long stationId = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "99"));
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

        System.out.printf("Starting Open-Meteo Live Channel Adapter [Station ID: %d] at Lat/Lon: [%s, %s]%n",
                stationId, LATITUDE, LONGITUDE);

        // 1. Fetch initial dataset synchronously so the loop has data to start with
        try {
            refreshWeatherData();
        } catch (Exception e) {
            System.err.println("Fatal: Failed to fetch initial data from Open-Meteo API: " + e.getMessage());
            return;
        }

        // 2. Schedule background cache refreshes every 10 minutes to grab new metrics smoothly
        ScheduledExecutorService cacheRefresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-refresher");
            t.setDaemon(true);
            return t;
        });
        cacheRefresher.scheduleAtFixedRate(OpenMeteoChannelAdapter::refreshWeatherData, 10, 10, TimeUnit.MINUTES);

        // 3. Initialize Producer Configuration footprints
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long sequenceNumber = 0;
            int dataIndex = 0;

            System.out.println("Adapter ready. Beginning serialization loop processing...");

            while (true) {
                sequenceNumber++;

                // Enforce required 10% drop metric rule cleanly across tracking channels
                if (random.nextDouble() < 0.10) {
                    System.out.println("Adapter [Station " + stationId + "] | Dropped message #" + sequenceNumber);
                    Thread.sleep(1000);
                    continue;
                }

                // Capture local reference of cache to maintain thread-safety during background updates
                OpenMeteoResponse.HourlyData currentData = hourlyCache;
                int totalRecords = currentData.getTime().size();

                // Loop cleanly over the extracted hourly metrics indices sequentially
                int currentBatchIndex = dataIndex % totalRecords;

                // Convert to corporate system schema structures
                WeatherMessage msg = OpenMeteoConverter.toWeatherMessage(
                        stationId,
                        sequenceNumber,
                        currentData,
                        currentBatchIndex
                );

                String outgoingJson = mapper.writeValueAsString(msg);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(stationId), outgoingJson);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Adapter send transaction failed: " + exception.getMessage());
                    }
                });

                System.out.println("Adapter [Station " + stationId + "] | Emitted sequence #" + sequenceNumber + " -> " + outgoingJson);

                dataIndex++;
                Thread.sleep(1000); // 1-second operational velocity cadence matching specifications
            }

        } catch (Exception e) {
            System.err.println("Critical runtime failure in Channel Adapter instance: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cacheRefresher.shutdown();
        }
    }

    /**
     * Executes the live HTTP GET call, parses the JSON payload, and safely updates the cache.
     */
    private static void refreshWeatherData() {
        try {
            System.out.println("Sending HTTP request to Open-Meteo: " + OPEN_METEO_URL);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(OPEN_METEO_URL))
                    .GET()
                    .header("Accept", "application/json")
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("HTTP error while calling Open-Meteo API. Status: " + response.statusCode());
                return;
            }

            OpenMeteoResponse apiResponse = mapper.readValue(response.body(), OpenMeteoResponse.class);
            if (apiResponse != null && apiResponse.getHourly() != null) {
                hourlyCache = apiResponse.getHourly();
                System.out.println("Cache successfully updated with " + hourlyCache.getTime().size() + " fresh records for Alexandria.");
            }
        } catch (Exception e) {
            System.err.println("Failed to refresh weather data cache: " + e.getMessage());
        }
    }
}