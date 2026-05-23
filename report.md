# Project Report

Generated: 2026-05-23 10:10:11

## Deliverables Summary
- Source Code: `/home/aboelanwar/Weather-Stations-Monitoring/central-station` and `/home/aboelanwar/Weather-Stations-Monitoring/weather-station`
- Dockerfiles:
  - `/home/aboelanwar/Weather-Stations-Monitoring/central-station/Dockerfile`
  - `/home/aboelanwar/Weather-Stations-Monitoring/weather-station/Dockerfile`
- Kubernetes YAMLs (in `/home/aboelanwar/Weather-Stations-Monitoring/k8s`):
  - `central-station.yaml`
  - `elastic-kibana.yaml`
  - `kafka.yaml`
  - `namespace.yaml`
  - `storage.yaml`
  - `weather-stations.yaml`
  - `zookeeper.yaml`
- Report: `/home/aboelanwar/Weather-Stations-Monitoring/report.md`

## BitCask Keys

Command:
```zsh
curl -s http://localhost:18080/bitcask/keys
```

Output:
```text
["1","10","2","3","4","5","6","7","8","9"]
```

## BitCask All

Command:
```zsh
curl -s http://localhost:18080/bitcask/all
```

Output:
```text
{"1":"{\"station_id\":1,\"s_no\":5506,\"battery_status\":\"high\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":50,\"temperature\":109,\"wind_speed\":126}}","10":"{\"station_id\":10,\"s_no\":5499,\"battery_status\":\"high\",\"status_timestamp\":1779520207,\"weather\":{\"humidity\":21,\"temperature\":101,\"wind_speed\":124}}","2":"{\"station_id\":2,\"s_no\":5504,\"battery_status\":\"high\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":18,\"temperature\":120,\"wind_speed\":47}}","3":"{\"station_id\":3,\"s_no\":5502,\"battery_status\":\"high\",\"status_timestamp\":1779520209,\"weather\":{\"humidity\":84,\"temperature\":78,\"wind_speed\":4}}","4":"{\"station_id\":4,\"s_no\":5504,\"battery_status\":\"high\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":30,\"temperature\":108,\"wind_speed\":20}}","5":"{\"station_id\":5,\"s_no\":5503,\"battery_status\":\"low\",\"status_timestamp\":1779520209,\"weather\":{\"humidity\":5,\"temperature\":73,\"wind_speed\":75}}","6":"{\"station_id\":6,\"s_no\":5503,\"battery_status\":\"high\",\"status_timestamp\":1779520209,\"weather\":{\"humidity\":96,\"temperature\":114,\"wind_speed\":73}}","7":"{\"station_id\":7,\"s_no\":5504,\"battery_status\":\"medium\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":14,\"temperature\":115,\"wind_speed\":96}}","8":"{\"station_id\":8,\"s_no\":5504,\"battery_status\":\"medium\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":33,\"temperature\":78,\"wind_speed\":94}}","9":"{\"station_id\":9,\"s_no\":5501,\"battery_status\":\"low\",\"status_timestamp\":1779520208,\"weather\":{\"humidity\":16,\"temperature\":84,\"wind_speed\":113}}"}
```

## BitCask Get (key=1)

Command:
```zsh
curl -s 'http://localhost:18080/bitcask/get?key=1'
```

Output:
```text
{"key":"1","value":"{\"station_id\":1,\"s_no\":5506,\"battery_status\":\"high\",\"status_timestamp\":1779520210,\"weather\":{\"humidity\":50,\"temperature\":109,\"wind_speed\":126}}","found":"true"}
```

## Kubernetes Pods (weather-monitoring)

Command:
```zsh
kubectl -n weather-monitoring get pods -o wide
```

Output:
```text
NAME                                  READY   STATUS    RESTARTS   AGE   IP             NODE       NOMINATED NODE   READINESS GATES
central-station-674dbbb9ff-t2f6z      1/1     Running   0          39m   10.244.0.201   minikube   <none>           <none>
elasticsearch-988954b85-dbv7c         1/1     Running   0          94m   10.244.0.183   minikube   <none>           <none>
kafka-5d4499dffb-6nxlh                1/1     Running   0          94m   10.244.0.179   minikube   <none>           <none>
kibana-5688c6446b-c8bsc               1/1     Running   0          94m   10.244.0.181   minikube   <none>           <none>
weather-station-1-578f49ff5c-5wbpl    1/1     Running   0          94m   10.244.0.177   minikube   <none>           <none>
weather-station-10-64ccdf5bbf-772p8   1/1     Running   0          94m   10.244.0.191   minikube   <none>           <none>
weather-station-2-6d9bc58fb5-r5852    1/1     Running   0          94m   10.244.0.180   minikube   <none>           <none>
weather-station-3-6bd8c57865-2rfl6    1/1     Running   0          94m   10.244.0.184   minikube   <none>           <none>
weather-station-4-7558bd4485-d6clv    1/1     Running   0          94m   10.244.0.185   minikube   <none>           <none>
weather-station-5-7b6cbdd4b5-b7sl9    1/1     Running   0          94m   10.244.0.186   minikube   <none>           <none>
weather-station-6-57bfdcdb8f-7c7w7    1/1     Running   0          94m   10.244.0.187   minikube   <none>           <none>
weather-station-7-5f86f987fc-6jn2h    1/1     Running   0          94m   10.244.0.188   minikube   <none>           <none>
weather-station-8-6b5776c9d9-7pkm8    1/1     Running   0          94m   10.244.0.189   minikube   <none>           <none>
weather-station-9-66569fb585-x9zc4    1/1     Running   0          94m   10.244.0.190   minikube   <none>           <none>
zookeeper-545fb578d6-wfpzx            1/1     Running   0          94m   10.244.0.182   minikube   <none>           <none>
```

## Kubernetes Services (weather-monitoring)

Command:
```zsh
kubectl -n weather-monitoring get svc
```

Output:
```text
NAME              TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)          AGE
central-station   NodePort    10.109.151.55   <none>        8080:30080/TCP   147m
elasticsearch     ClusterIP   10.97.178.163   <none>        9200/TCP         147m
kafka             ClusterIP   10.101.62.188   <none>        9092/TCP         147m
kibana            NodePort    10.98.6.241     <none>        5601:30601/TCP   147m
zookeeper         ClusterIP   10.104.149.53   <none>        2181/TCP         147m
```

## Kafka Endpoints

Command:
```zsh
kubectl -n weather-monitoring get endpoints kafka
```

Output:
```text
NAME    ENDPOINTS           AGE
kafka   10.244.0.179:9092   147m
Warning: v1 Endpoints is deprecated in v1.33+; use discovery.k8s.io/v1 EndpointSlice
```

## Central Station Logs (tail 50)

Command:
```zsh
kubectl -n weather-monitoring logs -l app=central-station --tail=50 | cat
```

Output:
```text
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=4/1779520208675-635.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=7/1779520208687-410.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=6/1779520208699-433.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=9/1779520208710-57.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=8/1779520208722-189.parquet
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] [INFO ] [STATION-8   ] [2026-05-23 07:10:09] seq=5503  battery=medium humidity=  5% temp= 86F wind=144km/h | thread=pool-3-thread-1
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=8/1779520209152-915.parquet
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] 🌧 RAIN ALERT! station=6   humidity= 96% temp=114F wind= 73km/h
[2026-05-23 07:10:09] [INFO ] [STATION-6   ] [2026-05-23 07:10:09] seq=5503  battery=high   humidity= 96% temp=114F wind= 73km/h | thread=pool-3-thread-3
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] [INFO ] [STATION-5   ] [2026-05-23 07:10:09] seq=5503  battery=low    humidity=  5% temp= 73F wind= 75km/h | thread=pool-3-thread-2
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] [INFO ] [STATION-4   ] [2026-05-23 07:10:09] seq=5503  battery=medium humidity= 66% temp= 99F wind=138km/h | thread=pool-3-thread-1
[2026-05-23 07:10:09] 🌧 RAIN ALERT! station=3   humidity= 84% temp= 78F wind=  4km/h
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] [INFO ] [STATION-3   ] [2026-05-23 07:10:09] seq=5502  battery=high   humidity= 84% temp= 78F wind=  4km/h | thread=pool-3-thread-3
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] [INFO ] [STATION-7   ] [2026-05-23 07:10:09] seq=5503  battery=low    humidity=  9% temp= 94F wind= 42km/h | thread=pool-3-thread-2
[2026-05-23 07:10:09] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:09] 🌧 RAIN ALERT! station=1   humidity= 92% temp= 67F wind=  4km/h
[2026-05-23 07:10:09] [WARN ] [STATION-1   ] Detected 1 dropped message(s) [last=5503 current=5505]
[2026-05-23 07:10:09] [INFO ] [STATION-1   ] [2026-05-23 07:10:09] seq=5505  battery=medium humidity= 92% temp= 67F wind=  4km/h | thread=pool-3-thread-1
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=3/1779520209812-125.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=5/1779520209823-776.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=4/1779520209834-173.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=7/1779520209845-467.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=6/1779520209855-126.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=1/1779520209867-247.parquet
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [INFO ] [STATION-8   ] [2026-05-23 07:10:10] seq=5504  battery=medium humidity= 33% temp= 78F wind= 94km/h | thread=pool-3-thread-3
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [INFO ] [STATION-4   ] [2026-05-23 07:10:10] seq=5504  battery=high   humidity= 30% temp=108F wind= 20km/h | thread=pool-3-thread-2
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=8/1779520210352-169.parquet
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [INFO ] [STATION-7   ] [2026-05-23 07:10:10] seq=5504  battery=medium humidity= 14% temp=115F wind= 96km/h | thread=pool-3-thread-1
2026-05-23T07:10:10.550Z  INFO 1 --- [nio-8080-exec-1] o.a.c.c.C.[Tomcat].[localhost].[/]       : Initializing Spring DispatcherServlet 'dispatcherServlet'
2026-05-23T07:10:10.550Z  INFO 1 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Initializing Servlet 'dispatcherServlet'
2026-05-23T07:10:10.551Z  INFO 1 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Completed initialization in 1 ms
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [INFO ] [STATION-1   ] [2026-05-23 07:10:10] seq=5506  battery=high   humidity= 50% temp=109F wind=126km/h | thread=pool-3-thread-3
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [WARN ] [STATION-2   ] Detected 1 dropped message(s) [last=5502 current=5504]
[2026-05-23 07:10:10] [INFO ] [STATION-2   ] [2026-05-23 07:10:10] seq=5504  battery=high   humidity= 18% temp=120F wind= 47km/h | thread=pool-3-thread-2
[2026-05-23 07:10:10] [DEBUG] [KAFKA       ] Polled 1 records
[2026-05-23 07:10:10] [WARN ] [STATION-10  ] Detected 2 dropped message(s) [last=5499 current=5502]
[2026-05-23 07:10:10] [INFO ] [STATION-10  ] [2026-05-23 07:10:10] seq=5502  battery=low    humidity= 43% temp= 96F wind=118km/h | thread=pool-3-thread-1
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=2/1779520210921-209.parquet
Parquet: wrote 1 records to /data/parquet/date=2026-05-23/hour=07/station=10/1779520210934-55.parquet
```

## Parquet Outputs (weather)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/central-station -- sh -lc 'ls -R /data/parquet | head -n 40'
```

Output:
```text
/data/parquet:
date=2026-05-23

/data/parquet/date=2026-05-23:
hour=05
hour=06
hour=07

/data/parquet/date=2026-05-23/hour=05:
station=2
station=7
station=8
station=9

/data/parquet/date=2026-05-23/hour=05/station=2:
1779514707010-383.parquet
1779515250932-445.parquet

/data/parquet/date=2026-05-23/hour=05/station=7:
1779514866746-585.parquet

/data/parquet/date=2026-05-23/hour=05/station=8:
1779514925355-712.parquet

/data/parquet/date=2026-05-23/hour=05/station=9:
1779514778172-927.parquet
1779514916596-377.parquet
1779515207626-642.parquet

/data/parquet/date=2026-05-23/hour=06:
station=1
station=10
station=2
station=3
station=4
station=5
station=6
station=7
station=8
station=9
```

## Parquet Outputs (drops)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/central-station -- sh -lc 'ls -R /data/parquet_drops | head -n 40'
```

Output:
```text
/data/parquet_drops:
date=2026-05-23

/data/parquet_drops/date=2026-05-23:
hour=06
hour=07

/data/parquet_drops/date=2026-05-23/hour=06:
station=1
station=10
station=2
station=3
station=4
station=5
station=6
station=7
station=8
station=9

/data/parquet_drops/date=2026-05-23/hour=06/station=1:
1779517864795-918.parquet
1779517869808-633.parquet
1779517874971-43.parquet
1779517880080-135.parquet
1779517910783-277.parquet
1779517915893-930.parquet
1779517925986-495.parquet
1779517936185-158.parquet
1779517946214-392.parquet
1779517971622-810.parquet
1779517976623-450.parquet
1779517986625-643.parquet
1779517996720-877.parquet
1779518007004-853.parquet
1779518012012-416.parquet
1779518057647-493.parquet
1779518067827-818.parquet
1779518072906-769.parquet
1779518078027-710.parquet
1779518088152-668.parquet
```

## Sample Parquet File (weather)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/central-station -- sh -lc 'find /data/parquet -type f -name "'*.parquet'"' | head -n 1'
```

Output:
```text
/bin/sh: 1: Syntax error: Unterminated quoted string
```

## Sample Parquet File (drops)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/central-station -- sh -lc 'find /data/parquet_drops -type f -name "'*.parquet'"' | head -n 1'
```

Output:
```text
/bin/sh: 1: Syntax error: Unterminated quoted string
```

## Elasticsearch Indices

Command:
```zsh
kubectl -n weather-monitoring exec deploy/elasticsearch -- sh -lc 'curl -s http://localhost:9200/_cat/indices?v'
```

Output:
```text
health status index          uuid                   pri rep docs.count docs.deleted store.size pri.store.size dataset.size
yellow open   weather-drops  Sg10z0GlQZKMiJozmwK1Ig   1   1         22            0     11.3kb         11.3kb       11.3kb
yellow open   weather-status usOINW6QTlyZ7ZB6i8_meA   1   1       3346            0    297.3kb        297.3kb      297.3kb
```

## Elasticsearch Count (weather-status)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/elasticsearch -- sh -lc 'curl -s http://localhost:9200/weather-status/_count?pretty'
```

Output:
```text
{
  "count" : 3346,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  }
}
```

## Elasticsearch Count (weather-drops)

Command:
```zsh
kubectl -n weather-monitoring exec deploy/elasticsearch -- sh -lc 'curl -s http://localhost:9200/weather-drops/_count?pretty'
```

Output:
```text
{
  "count" : 22,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  }
}
```

## Sample BitCask Directory Listing

Command:
```zsh
ls -1 /home/aboelanwar/Weather-Stations-Monitoring/bitcask | head -n 20
```

Output:
```text
1779487638623.data
1779487704445.data
1779488004450.data
1779488004450.hint
1779488304448.data
1779488304448.hint
1779488604448.data
1779488604448.hint
1779488637274.data
1779488937278.data
1779488937278.hint
1779488998605.data
1779489204707.data
1779489504713.data
1779489504713.hint
1779489547406.data
1779489547406.hint
1779491382662.data
1779491382662.hint
1779491423952.data
```

## Kibana Visualizations

- Screenshot 1: Low-battery count per station (provided by user)
- Screenshot 2: Dropped messages per station (provided by user)
- Screenshot 3: Battery status distribution (30% low - 40% medium - 30% high) (provided by user)
