
## Setup

### Overlord config
```sh
druid.indexer.runner.javaOpts="-server -Xmx4g"
druid.indexer.fork.property.druid.processing.numThreads=4
druid.indexer.fork.property.druid.computation.buffer.size=500000000
```

### common config
```sh
druid.storage.storageDirectory=/Users/hbutani/druid/localStorage
```

## Start all
* start_zk, start_all

## Cleanup everything

* stop_all, stop zk
* In metastore DB: delete FROM druid.druid_segments
* rm -fr /tmp/druid
* rm -f /tmp/zookeeper

## Submit indexing job
```sh
curl -X 'POST' -H 'Content-Type:application/json' -d @/Users/hbutani/sparkline/spark-csv/src/test/scala/com/databricks/spark/csv/tpch/druid/tpch_index_task.json localhost:8090/druid/indexer/v1/task
```

## Queries

### Run a query
```sh
curl -X POST 'http://localhost:8082/druid/v2/?pretty' -H 'content-type: application/json'  -d  @/Users/hbutani/sparkline/spark-csv/src/test/scala/com/databricks/spark/csv/tpch/druid/queries/timeseries1.json
```

### Q1, Pricing Summary Report
```json
```