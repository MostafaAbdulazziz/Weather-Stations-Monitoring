# Weather Stations Monitoring

## BitCask Client

The BitCask client script wraps a Python implementation that supports the required flags.

```bash
./bitcask_client.sh --view-all
./bitcask_client.sh --view --key=SOME_KEY
./bitcask_client.sh --perf --clients=100
```

You can override the API endpoint using `BASE_URL`.

```bash
BASE_URL="http://localhost:8080/bitcask" ./bitcask_client.sh --view-all
```

## Drop Parquet Archiving

When sequence gaps are detected per station, drop events are written to parquet so you can
count dropped messages per station in Elasticsearch/Kibana.

Environment variables:
- `DROP_PARQUET_DIR` (default `/data/parquet_drops`)
- `DROP_PARQUET_BATCH_SIZE` (default `10000`)
- `DROP_PARQUET_FLUSH_MS` (default `5000`)
- `DROP_PARQUET_QUEUE_CAPACITY` (default `50000`)

## Parquet to Elasticsearch Indexer

The indexer is schema-agnostic and can index both weather and drop parquet datasets.

```bash
mvn -q -f central-station/pom.xml -DskipTests \
  exec:java -Dexec.mainClass=com.weather.central.archiver.ParquetToElasticIndexer \
  -Dexec.args="--parquet-dir=/data/parquet --es-url=http://localhost:9200 --index=weather-status"

mvn -q -f central-station/pom.xml -DskipTests \
  exec:java -Dexec.mainClass=com.weather.central.archiver.ParquetToElasticIndexer \
  -Dexec.args="--parquet-dir=/data/parquet_drops --es-url=http://localhost:9200 --index=weather-drops"
```

Kibana example aggregations:
- `weather-status`: filter `battery_status = low`, then bucket by `station_id`.
- `weather-drops`: sum `dropped_count`, then bucket by `station_id`.
