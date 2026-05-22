package com.weather.central.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.central.WeatherMessage;
import com.weather.central.archiver.ParquetArchiver;
import com.weather.central.bitcask.BitCask;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class WeatherConsumer implements Runnable {

    private static final String TOPIC   = "weather-readings";
    private static final String GROUP   = "central-station-group";

    private final BitCask        bitCask;
    private final ParquetArchiver archiver;
    private final ObjectMapper    mapper = new ObjectMapper();
    private final KafkaConsumer<String, String> consumer;

    public WeatherConsumer(BitCask bitCask,
                           ParquetArchiver archiver,
                           String kafkaBootstrap) {
        this.bitCask  = bitCask;
        this.archiver = archiver;

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(TOPIC));
    }

    @Override
    public void run() {
        System.out.println("WeatherConsumer started, listening on topic: " + TOPIC);
        try {
            while (true) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        WeatherMessage msg = mapper.readValue(
                            record.value(), WeatherMessage.class);

                        // 1. Update BitCask with latest reading
                        bitCask.put(
                            String.valueOf(msg.getStation_id()),
                            record.value()
                        );

                        // 2. Archive to Parquet
                        archiver.archive(msg);

                        System.out.println("Processed station "
                            + msg.getStation_id()
                            + " seq=" + msg.getS_no());

                    } catch (Exception e) {
                        System.err.println("Error processing record: "
                            + e.getMessage());
                    }
                }
            }
        } finally {
            try { archiver.close(); } catch (Exception ignored) {}
            consumer.close();
        }
    }
}
