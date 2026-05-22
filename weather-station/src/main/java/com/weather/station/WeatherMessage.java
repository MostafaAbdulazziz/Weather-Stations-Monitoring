package com.weather.station;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WeatherMessage {
    private long station_id;
    private long s_no;
    private String battery_status;
    private long status_timestamp;
    private Weather weather;
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Weather {
        private int humidity;
        private int temperature;
        private int wind_speed;
    }
}
