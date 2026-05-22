FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY target/weather-station-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
ENV STATION_ID=1
ENV KAFKA_BOOTSTRAP=kafka:9092
ENTRYPOINT ["java", "-jar", "app.jar"]
