package com.weather.central.archiver;

import com.weather.central.WeatherMessage;

import java.time.Instant;
import java.util.Random;

public class ParquetArchiverDemo {

    public static void main(String[] args) throws Exception {
        ParquetArchiver archiver = new ParquetArchiver();
        Random random = new Random();
        long baseTs = Instant.now().getEpochSecond();

        for (int i = 0; i < 25; i++) {
            WeatherMessage.Weather weather = new WeatherMessage.Weather(
                    20 + random.nextInt(80),
                    60 + random.nextInt(40),
                    5 + random.nextInt(20)
            );
            WeatherMessage msg = new WeatherMessage(
                    1L,
                    i + 1,
                    i % 3 == 0 ? "low" : (i % 3 == 1 ? "medium" : "high"),
                    baseTs + i,
                    weather
            );
            archiver.archive(msg);
        }

        archiver.close();
        System.out.println("Demo completed. Check PARQUET_DIR for output files.");
    }
}

