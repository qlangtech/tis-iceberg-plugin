# Flink Queries — Apache Iceberg™ (latest / 1.12.0)

Iceberg supports streaming and batch reads with Flink's DataStream API and Table API.

## Reading with SQL
```
SET execution.runtime-mode=streaming;   -- or batch
```
### Batch read
```
SET execution.runtime-mode=batch;
SELECT * FROM sample;
```
### Streaming read (incremental from current or a historical snapshot-id)
```
SET execution.runtime-mode=streaming;
SET table.dynamic-table-options.enabled=true;
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s') */;
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987') */;
```
### FLIP-27 source for SQL
```
SET table.exec.iceberg.use-flip27-source=false;   -- default false for Flink 1.19-, true for 1.20+
```
### Reading branches and tags with SQL
```
SELECT * FROM table /*+ OPTIONS('branch'='b1') */;
SELECT * FROM table /*+ OPTIONS('tag'='t1') */;
SELECT * FROM table /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-tag'='t1', 'end-tag'='t2') */;
```

## Reading with DataStream
### Batch
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> batch = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
batch.print();  env.execute("Test Iceberg Batch Read");
```
### Streaming
```
DataStream<RowData> stream = FlinkSource.forRowData().env(env).tableLoader(tableLoader)
  .streaming(true).startSnapshotId(3821550127947089987L).build();
```

## Reading with DataStream (FLIP-27 source, experimental)
```
IcebergSource<RowData> source = IcebergSource.forRowData().tableLoader(tableLoader)
  .assignerFactory(new SimpleSplitAssignerFactory()).build();
DataStream<RowData> batch = env.fromSource(source, WatermarkStrategy.noWatermarks(), "My Iceberg Source", TypeInformation.of(RowData.class));
```
Streaming from latest snapshot, poll every 60s (append-only; CDC read not yet supported):
```
IcebergSource source = IcebergSource.forRowData().tableLoader(tableLoader)
  .assignerFactory(new SimpleSplitAssignerFactory()).streaming(true)
  .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
  .monitorInterval(Duration.ofSeconds(60)).build();
```
Read branch/tag: `.branch("test-branch")` / `.tag("test-tag")` / `.startTag("test-tag")`.

## Read as Avro GenericRecord
`AvroGenericRecordReaderFunction` converts RowData ↔ Avro GenericRecord. Requires `flink-avro` on classpath; cannot use `iceberg-flink-runtime` (shades avro).

## Emitting watermarks
Set `watermarkColumn` (types: timestamp, timestamptz, long). For `long`, use `watermarkTimeUnit`. Watermarks derived from data-file min/max column metrics, emitted once per split. Recommend `read.split.open-file-cost` very large to avoid combining small files into one split. Column-level min-max stats required (default collected for first 100 columns; use `write.metadata.metrics.*` otherwise).

## Read options
```
IcebergSource.forRowData().tableLoader(...).assignerFactory(new SimpleSplitAssignerFactory())
  .streaming(true).streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
  .startSnapshotId(3821550127947089987L).monitorInterval(Duration.ofMillis(10L)).build();
```
SQL hint: `SELECT * FROM tableName /*+ OPTIONS('monitor-interval'='10s') */`

## Inspecting tables (metadata tables)
Append metadata table name after table name, e.g. `db.table$history`, `$metadata_log_entries`, `$snapshots`, `$files`, `$manifests`, `$partitions`, `$all_data_files`, `$all_manifests`, `$refs`.
- `$history` — commit history (with rollback info via `is_current_ancestor`)
- `$snapshots` — valid snapshots (operation, manifest_list, summary incl. `flink.job-id`)
- `$files` / `$all_data_files` — data files + column stats
- `$manifests` / `$all_manifests` — manifest files + partition_summaries
- `$partitions` — per-partition record/file counts, delete counts, last_updated
- `$refs` — named snapshot references (branches/tags) with retention props

> Source: https://iceberg.apache.org/docs/latest/flink-queries/
