package com.weather.central.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.central.WeatherMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class RainDetector implements Runnable {

    private static final String INPUT_TOPIC  = "weather-readings";
    private static final String OUTPUT_TOPIC = "rain-alerts";

    private static final DateTimeFormatter FMT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

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

        input
            .filter((key, value) -> {
                try {
                    WeatherMessage msg = mapper.readValue(
                            value, WeatherMessage.class);
                    return msg.getWeather().getHumidity() > 70;
                } catch (Exception e) {
                    return false;
                }
            })
            .mapValues(value -> {
                try {
                    WeatherMessage msg = mapper.readValue(
                            value, WeatherMessage.class);

                    String alert = String.format(
                        "[%s] 🌧 RAIN ALERT! station=%-3d humidity=%3d%% " +
                        "temp=%3dF wind=%3dkm/h",
                        FMT.format(Instant.now()),
                        msg.getStation_id(),
                        msg.getWeather().getHumidity(),
                        msg.getWeather().getTemperature(),
                        msg.getWeather().getWind_speed()
                    );

                    // Print to console so you can see it
                    System.out.println(alert);
                    return alert;

                } catch (Exception e) {
                    return "RAIN ALERT (parse error): " + value;
                }
            })
            .to(OUTPUT_TOPIC);

        this.streams = new KafkaStreams(builder.build(), props);
    }

    @Override
    public void run() {
        System.out.printf("[%s] RainDetector started — monitoring humidity > 70%%%n",
                FMT.format(Instant.now()));

        // Print state changes so you know it's running
        streams.setStateListener((newState, oldState) ->
            System.out.printf("[%s] RainDetector state: %s → %s%n",
                FMT.format(Instant.now()), oldState, newState));

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
