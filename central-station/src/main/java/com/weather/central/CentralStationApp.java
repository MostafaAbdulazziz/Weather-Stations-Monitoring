package com.weather.central;

import com.weather.central.archiver.DropParquetArchiver;
import com.weather.central.archiver.ParquetArchiver;
import com.weather.central.bitcask.BitCask;
import com.weather.central.bitcask.BitCaskController;
import com.weather.central.consumer.RainDetector;
import com.weather.central.consumer.WeatherConsumer;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@SpringBootApplication
public class CentralStationApp {

    // Read from env var, fall back to a local folder under home
    @Value("${BITCASK_DIR:#{systemProperties['user.home'] + '/weather-monitoring/bitcask'}}")
    private String bitcaskDir;

    @Value("${PARQUET_DIR:#{systemProperties['user.home'] + '/weather-monitoring/parquet'}}")
    private String parquetDir;

    @Value("${KAFKA_BOOTSTRAP:localhost:9092}")
    private String kafkaBootstrap;

    private WeatherConsumer weatherConsumer;

    public static void main(String[] args) {
        SpringApplication.run(CentralStationApp.class, args);
    }

    // Constructor injection — BitCask gets its dir via @Value
    @Bean
    public BitCask bitCask() throws IOException {
        System.out.println("BitCask directory: " + bitcaskDir);
        return new BitCask(bitcaskDir);
    }

    // Constructor injection — ParquetArchiver gets its dir via @Value
    @Bean
    public ParquetArchiver parquetArchiver() {
        System.out.println("Parquet directory: " + parquetDir);
        return new ParquetArchiver(parquetDir);
    }

    @Bean
    public DropParquetArchiver dropParquetArchiver() {
        return new DropParquetArchiver();
    }

    // WeatherConsumer gets BitCask + ParquetArchiver injected via parameters
    @Bean
    public WeatherConsumer weatherConsumer(BitCask bitCask,
                                           ParquetArchiver archiver,
                                           DropParquetArchiver dropArchiver) {
        System.out.println("Kafka bootstrap: " + kafkaBootstrap);
        weatherConsumer = new WeatherConsumer(bitCask, archiver, dropArchiver, kafkaBootstrap);
        weatherConsumer.start();
        return weatherConsumer;
    }

    @Bean
    public RainDetector rainDetector() {
        RainDetector detector = new RainDetector(kafkaBootstrap);
        new Thread(detector, "rain-detector").start();
        return detector;
    }

    @PreDestroy
    public void onShutdown() {
        if (weatherConsumer != null) weatherConsumer.stop();
    }
}