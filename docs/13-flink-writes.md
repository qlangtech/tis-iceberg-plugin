# Flink Writes — Apache Iceberg™ (latest / 1.12.0)

Iceberg supports batch and streaming writes with Flink's DataStream API and Table API. The Flink Iceberg sink guarantees **exactly-once** semantics.

## Writing with SQL

Both `INSERT INTO` and `INSERT OVERWRITE` are supported.

### INSERT INTO (append)
```
INSERT INTO `hive_catalog`.`default`.`sample` VALUES (1, 'a');
INSERT INTO `hive_catalog`.`default`.`sample` SELECT id, data from other_kafka_table;
```

### INSERT OVERWRITE
Atomic for Iceberg. Only supported in batch jobs (not streaming). Partitions produced by the SELECT are replaced.
```
INSERT OVERWRITE sample VALUES (1, 'a');
INSERT OVERWRITE `hive_catalog`.`default`.`sample` PARTITION(data='a') SELECT 6;
```
Static vs dynamic partition: if all partition columns get a value → static partition; if only a prefix → dynamic partition. Unpartitioned table is fully overwritten.

### UPSERT
Requires v2 table format + primary key / identifier fields. Two ways:
1. Table property `write.upsert.enabled`:
```
CREATE TABLE `hive_catalog`.`default`.`sample` (
  `id` INT COMMENT 'unique id', `data` STRING NOT NULL,
  PRIMARY KEY(`id`) NOT ENFORCED
) with ('format-version'='2', 'write.upsert.enabled'='true');
```
2. Write option `upsert-enabled` (more flexible):
```
INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */ ...
```
OVERWRITE and UPSERT are mutually exclusive. With a partitioned table, the source columns of partition fields must be in the equality fields.

## Writing with DataStream
```
StreamExecutionEnvironment env = ...;
DataStream<RowData> input = ...;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

// Append
FlinkSink.forRowData(input).tableLoader(tableLoader).append();
// Overwrite
FlinkSink.forRowData(input).tableLoader(tableLoader).overwrite(true).append();
// Upsert (v2 + primary key)
FlinkSink.forRowData(input).tableLoader(tableLoader).upsert(true).append();
// Avro GenericRecord → RowData
FlinkSink.builderFor(dataStream, AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema), FlinkCompatibilityUtil.toTypeInfo(rowType))
  .table(table).tableLoader(tableLoader).append();
// Branch write
FlinkSink.forRowData(input).tableLoader(tableLoader).toBranch("audit-branch").append();
```
Note: `iceberg-flink-runtime` shaded jar shades avro and cannot be used for the Avro GenericRecord path — use non-shaded `iceberg-flink`.

## Metrics
Writer metrics under `IcebergStreamWriter` (tags: table, subtask_index): `lastFlushDurationMs` (Gauge), `flushedDataFiles`/`flushedDeleteFiles`/`flushedReferencedDataFiles` (Counter), `dataFilesSizeHistogram`/`deleteFilesSizeHistogram` (Histogram, needs `flink-metrics-dropwizard`).
Committer metrics under `IcebergFilesCommitter` (tag: table): `lastCheckpointDurationMs`, `lastCommitDurationMs`, `committedDataFilesCount/RecordCount/ByteCount`, `committedDeleteFilesCount/RecordCount/ByteCount`, `elapsedSecondsSinceLastSuccessfulCommit` (key alerting metric).

## Write options
```
FlinkSink.Builder builder = FlinkSink.forRow(dataStream, schema).table(table).tableLoader(tableLoader)
  .set("write-format", "orc").set(FlinkWriteOptions.OVERWRITE_MODE, "true");
```
SQL hints: `INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */ ...`

## Distribution mode
HASH and RANGE supported (`FlinkSink#Builder#distributionMode` or write-options).
- **HASH**: shuffles by partition key (partitioned) or equality fields (non-partitioned), via `keyBy`. Poor with skewed data; parallelism limited by key cardinality.
- **RANGE** (experimental): custom range partitioner uses collected traffic statistics, can handle skew and cluster on non-partition columns. Rows are not sorted within a data file. May take up to two checkpoint cycles to apply new stats.

## Notes
Flink streaming write jobs rely on snapshot summary (`flink.job-id`) to keep the last committed checkpoint ID and store uncommitted temp files. Expiring snapshots / deleting orphan files can corrupt the Flink job — keep the last snapshot created by the Flink job and only delete old-enough orphan files.

## Sink V2 based implementation (IcebergSink, experimental)
`IcebergSink` uses Flink SinkV2 (FLIP-191). SQL: `SET table.exec.iceberg.use-v2-sink=true;`. DataStream: replace `FlinkSink` with `IcebergSink` (use `uidSuffix` instead of `uidPrefix`).
Post-commit table maintenance: `rewriteDataFiles()`, `expireSnapshots()`, `deleteOrphanFiles()`, `convertEqualityDeletes()` (requires equality fields + format v>=3).
```
IcebergSink.forRowData(dataStream).table(table).tableLoader(tableLoader)
  .upsert(true).equalityFieldColumns(List.of("id"))
  .rewriteDataFiles().convertEqualityDeletes().append();
```

## Flink Dynamic Iceberg Sink
`DynamicIcebergSink` routes records to any number of tables, with dynamic table creation / schema / partition evolution, all via the `DynamicRecord` class (no job restart needed).
```
DynamicIcebergSink.forInput(dataStream)
  .generator((inputRecord,out)->out.collect(new DynamicRecord(
     TableIdentifier.of("db","table"), "branch", SCHEMA, (RowData)inputRecord,
     PartitionSpec.unpartitioned(), DistributionMode.HASH, 2)))
  .catalogLoader(CatalogLoader.hive("hive", new Configuration(), Map.of()))
  .writeParallelism(10).immediateTableUpdate(true).append();
```
DynamicRecord fields: TableIdentifier, Branch, Schema, Spec, RowData, DistributionMode (NONE/HASH/null), Parallelism, UpsertMode, EqualityFields.
Supported schema updates: add column, widen type, make required→optional, drop column (disabled by default). Renaming unsupported.
DistributionMode: NONE (round-robin or by equality fields), HASH (by partition/equality), null/forward (bypass shuffle, operator chaining). RANGE not supported (falls back to HASH).
Property precedence: sink properties override table properties.

> Source: https://iceberg.apache.org/docs/latest/flink-writes/
