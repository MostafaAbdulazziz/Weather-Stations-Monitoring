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

## Parquet Archiving

The central station batches weather messages and writes them to parquet files partitioned by
`date`, `hour`, and `station`. Defaults are tuned for larger batch sizes to reduce IO.

Environment variables:
- `PARQUET_DIR` (default `/data/parquet`)
- `PARQUET_BATCH_SIZE` (default `10000`)
- `PARQUET_FLUSH_MS` (default `5000`)
- `PARQUET_QUEUE_CAPACITY` (default `50000`)

## Parquet to Elasticsearch Indexer

To index parquet files into Elasticsearch for Kibana analysis, use the indexer tool:

```bash
mvn -q -f central-station/pom.xml -DskipTests \
  exec:java -Dexec.mainClass=com.weather.central.archiver.ParquetToElasticIndexer \
  -Dexec.args="--parquet-dir=/data/parquet --es-url=http://localhost:9200 --index=weather-status"
```

Supported arguments:
- `--parquet-dir=/path/to/parquet`
- `--es-url=http://localhost:9200`
- `--index=weather-status`
- `--batch-size=1000`
