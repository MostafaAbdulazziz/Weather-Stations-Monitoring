package com.weather.central;

import com.weather.central.archiver.ParquetArchiver;
import com.weather.central.bitcask.BitCask;
import com.weather.central.consumer.RainDetector;
import com.weather.central.consumer.WeatherConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@SpringBootApplication
public class CentralStationApp {

    public static void main(String[] args) {
        SpringApplication.run(CentralStationApp.class, args);
    }

    @Bean
    public BitCask bitCask() throws IOException {
        String dir = System.getenv().getOrDefault("BITCASK_DIR", "/data/bitcask");
        return new BitCask(dir);
    }

    @Bean
    public ParquetArchiver parquetArchiver() {
        return new ParquetArchiver();
    }

    @Bean
    public WeatherConsumer weatherConsumer(BitCask bitCask,
                                           ParquetArchiver archiver) {
        String kafka = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        WeatherConsumer consumer = new WeatherConsumer(bitCask, archiver, kafka);
        new Thread(consumer, "weather-consumer").start();
        return consumer;
    }

    @Bean
    public RainDetector rainDetector() {
        String kafka = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        RainDetector detector = new RainDetector(kafka);
        new Thread(detector, "rain-detector").start();
        return detector;
    }
}
