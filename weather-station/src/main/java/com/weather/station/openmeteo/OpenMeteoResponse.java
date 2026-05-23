package com.weather.station.openmeteo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenMeteoResponse {
    private double latitude;
    private double longitude;
    private HourlyData hourly;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HourlyData {
        private List<String> time; // ISO8601 strings

        @JsonProperty("temperature_2m")
        private List<Double> temperatures;

        @JsonProperty("relative_humidity_2m")
        private List<Integer> humidities;

        @JsonProperty("wind_speed_10m")
        private List<Double> windSpeeds;
    }
}