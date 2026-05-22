package com.weather.central.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.central.WeatherMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class RainDetector implements Runnable {

    private static final String INPUT_TOPIC  = "weather-readings";
    private static final String OUTPUT_TOPIC = "rain-alerts";

    private final KafkaStreams streams;
    private final ObjectMapper mapper = new ObjectMapper();

    public RainDetector(String kafkaBootstrap) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "rain-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(INPUT_TOPIC);

        input.filter((key, value) -> {
            try {
                WeatherMessage msg = mapper.readValue(value, WeatherMessage.class);
                return msg.getWeather().getHumidity() > 70;
            } catch (Exception e) {
                return false;
            }
        })
        .mapValues(value -> {
            try {
                WeatherMessage msg = mapper.readValue(value, WeatherMessage.class);
                return "RAIN ALERT! Station=" + msg.getStation_id()
                    + " Humidity=" + msg.getWeather().getHumidity() + "%"
                    + " Timestamp=" + msg.getStatus_timestamp();
            } catch (Exception e) {
                return "RAIN ALERT: " + value;
            }
        })
        .to(OUTPUT_TOPIC);

        this.streams = new KafkaStreams(builder.build(), props);
    }

    @Override
    public void run() {
        System.out.println("RainDetector started. Monitoring humidity > 70%");
        streams.start();
        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close));
    }
}
