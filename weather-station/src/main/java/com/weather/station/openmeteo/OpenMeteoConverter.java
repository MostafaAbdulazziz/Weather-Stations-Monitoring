package com.weather.station.openmeteo;

import com.weather.station.WeatherMessage;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class OpenMeteoConverter {
    private static final Random random = new Random();
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    public static WeatherMessage toWeatherMessage(
            long stationId,
            long sequenceNo,
            OpenMeteoResponse.HourlyData hourly,
            int index) {

        // 1. Convert timestamp from ISO-8601 string to Unix Epoch seconds
        String isoTime = hourly.getTime().get(index);
        long epochSecond = LocalDateTime.parse(isoTime, ISO_FORMATTER)
                .toEpochSecond(ZoneOffset.UTC);

        // 2. Convert Temperature from Celsius to Fahrenheit
        double celsius = hourly.getTemperatures().get(index);
        int fahrenheit = (int) Math.round(celsius * 1.8 + 32);

        // 3. Extract humidity and wind speed
        int humidity = hourly.getHumidities().get(index);
        int windSpeed = (int) Math.round(hourly.getWindSpeeds().get(index));

        // 4. Simulate battery distributions consistent with project specification
        String battery = simulateBatteryStatus();

        // 5. Construct compliant WeatherMessage payload
        WeatherMessage.Weather weatherDetails = new WeatherMessage.Weather(humidity, fahrenheit, windSpeed);
        return new WeatherMessage(
                stationId,
                sequenceNo,
                battery,
                epochSecond,
                weatherDetails
        );
    }

    private static String simulateBatteryStatus() {
        double r = random.nextDouble();
        if (r < 0.30) return "low";
        if (r < 0.70) return "medium";
        return "high";
    }
}