FROM eclipse-temurin:17-jdk-alpine
WORKDIR /app
COPY target/central-station-1.0-SNAPSHOT.jar app.jar
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV BITCASK_DIR=/data/bitcask
ENV PARQUET_DIR=/data/parquet
RUN mkdir -p /data/bitcask /data/parquet
ENTRYPOINT ["java", "-XX:StartFlightRecording=duration=60s,filename=/data/jfr/recording.jfr", "-jar", "app.jar"]
