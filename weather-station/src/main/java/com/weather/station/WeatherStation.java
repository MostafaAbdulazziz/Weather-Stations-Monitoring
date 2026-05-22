package com.weather.station;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WeatherStation {

    private static final String TOPIC = "weather-readings";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        // Get station ID from environment variable (set in K8s)
        long stationId = Long.parseLong(
                System.getenv().getOrDefault("STATION_ID", "1")
        );

        String kafkaBootstrap = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP", "localhost:9092"
        );

        System.out.println("Starting Weather Station ID: " + stationId);
        System.out.println("Connecting to Kafka at: " + kafkaBootstrap);

        // Kafka producer config
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        long sequenceNumber = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                sequenceNumber++;

                // 10% drop rate — skip sending this message
                if (random.nextDouble() < 0.10) {
                    System.out.println("Station " + stationId
                            + " | Dropped message #" + sequenceNumber);
                    Thread.sleep(1000);
                    continue;
                }

                // Build message
                WeatherMessage msg = new WeatherMessage(
                        stationId,
                        sequenceNumber,
                        randomBatteryStatus(),
                        System.currentTimeMillis() / 1000L,
                        new WeatherMessage.Weather(
                                random.nextInt(101), // humidity 0-100%
                                60 + random.nextInt(61), // temperature 60-120F
                                random.nextInt(151) // wind speed 0-150 km/h
                        )
                );

                String json = mapper.writeValueAsString(msg);

                ProducerRecord<String, String> record
                        = new ProducerRecord<>(TOPIC, String.valueOf(stationId), json);

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("Send failed: " + ex.getMessage());
                    }
                });

                System.out.println("Station " + stationId
                        + " | Sent #" + sequenceNumber + " | " + json);

                Thread.sleep(1000);
            }
        }
    }

    // 30% low, 40% medium, 30% high
    private static String randomBatteryStatus() {
        double r = random.nextDouble();
        if (r < 0.30) {
            return "low"; 
        }else if (r < 0.70) {
            return "medium"; 
        }else {
            return "high";
        }
    }
}
