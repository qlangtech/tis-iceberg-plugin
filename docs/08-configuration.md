# Configuration — Apache Iceberg™ (latest / 1.12.0)

Iceberg tables support table properties to configure table behavior, like the default split size for readers.

## Read properties

| Property | Default | Description |
| --- | --- | --- |
| read.split.target-size | 134217728 (128 MB) | Target size when combining data input splits |
| read.split.metadata-target-size | 33554432 (32 MB) | Target size when combining metadata input splits |
| read.split.planning-lookback | 10 | Number of bins to consider when combining input splits |
| read.split.open-file-cost | 4194304 (4 MB) | The estimated cost to open a file, used as a minimum weight when combining splits |
| read.split.adaptive-size.enabled | true | Controls whether split size is adjusted adaptively to scan size and available parallelism |
| read.parquet.vectorization.enabled | true | Controls whether Parquet vectorized reads are used |
| read.parquet.vectorization.batch-size | 5000 | The batch size for parquet vectorized reads |
| read.orc.vectorization.enabled | false | Controls whether ORC vectorized reads are used |
| read.orc.vectorization.batch-size | 5000 | The batch size for ORC vectorized reads |
| read.data-planning-mode | auto | Mode used to plan data manifests: auto, local, or distributed |
| read.delete-planning-mode | auto | Mode used to plan delete manifests: auto, local, or distributed |

## Write properties

| Property | Default | Description |
| --- | --- | --- |
| write.format.default | parquet | Default file format for the table; parquet, avro, or orc |
| write.delete.format.default | data file format | Default delete file format for the table |
| write.parquet.row-group-size-bytes | 134217728 (128 MB) | Parquet row group size |
| write.parquet.page-size-bytes | 1048576 (1 MB) | Parquet page size |
| write.parquet.compression-codec | zstd | Parquet compression codec: zstd, brotli, lz4, gzip, snappy, uncompressed |
| write.avro.compression-codec | gzip | Avro compression codec: gzip, zstd, snappy, uncompressed |
| write.orc.stripe-size-bytes | 67108864 (64 MB) | Default ORC stripe size, in bytes |
| write.orc.compression-codec | zlib | ORC compression codec: zstd, lz4, lzo, zlib, snappy, none |
| write.metadata.metrics.default | truncate(16) | Default metrics mode for all columns: none, counts, truncate(length), or full |
| write.metadata.metrics.column.col1 | (not set) | Metrics mode for column 'col1' to allow per-column tuning |
| write.metadata.metrics.max-inferred-column-defaults | 100 | Max number of columns for which metrics are collected |
| write.target-file-size-bytes | 536870912 (512 MB) | Controls the size of files generated to target about this many bytes |
| write.delete.target-file-size-bytes | 67108864 (64 MB) | Controls the size of delete files generated |
| write.distribution-mode | engine default | Defines distribution of write data: **none**, **hash**, or **range** |
| write.delete.distribution-mode | (not set) | Defines distribution of write delete data |
| write.update.distribution-mode | (not set) | Defines distribution of write update data |
| write.merge.distribution-mode | (not set) | Defines distribution of write merge data |
| write.wap.enabled | false | Enables write-audit-publish writes |
| write.summary.partition-limit | 0 | Includes partition-level summary stats in snapshot summaries if changed partition count < this limit |
| write.metadata.delete-after-commit.enabled | false | Controls whether to delete oldest **tracked** metadata files after each commit |
| write.metadata.previous-versions-max | 100 | Max number of previous version metadata files to track |
| write.spark.fanout.enabled | false | Enables the fanout writer in Spark that does not require data to be clustered |
| write.object-storage.enabled | false | Enables the object storage location provider that adds a hash component to file paths |
| write.data.path | table location + /data | Base location for data files |
| write.metadata.path | table location + /metadata | Base location for metadata files |
| write.delete.mode | copy-on-write | Mode for delete commands: copy-on-write or merge-on-read (v2+) |
| write.delete.isolation-level | serializable | Isolation level for delete commands: serializable or snapshot |
| write.update.mode | copy-on-write | Mode for update commands: copy-on-write or merge-on-read (v2+) |
| write.update.isolation-level | serializable | Isolation level for update commands |
| write.merge.mode | copy-on-write | Mode for merge commands: copy-on-write or merge-on-read (v2+) |
| write.merge.isolation-level | serializable | Isolation level for merge commands |
| write.delete.granularity | partition | Controls granularity of generated delete files: partition or file |

Metrics modes:
- **none**: No metrics are persisted
- **counts**: Only count metrics (value_counts, null_value_counts, nan_value_counts)
- **truncate(length)**: Persists counts plus truncated bounds. Truncation applies only to string and binary types
- **full**: Persists all metrics including full lower_bounds and upper_bounds

## Encryption properties

| Property | Default | Description |
| --- | --- | --- |
| encryption.key-id | (not set) | ID of the master key of the table |
| encryption.data-key-length | 16 (bytes) | Length of keys used for encryption of table files (16, 24, 32 bytes) |

## Table behavior properties

| Property | Default | Description |
| --- | --- | --- |
| commit.retry.num-retries | 4 | Number of times to retry a commit before failing |
| commit.retry.min-wait-ms | 100 | Minimum wait before retrying a commit |
| commit.retry.max-wait-ms | 60000 (1 min) | Maximum wait before retrying a commit |
| commit.retry.total-timeout-ms | 1800000 (30 min) | Total retry timeout period for a commit |
| commit.status-check.num-retries | 3 | Number of times to check commit success after connection loss |
| commit.manifest.target-size-bytes | 8388608 (8 MB) | Target size when merging manifest files |
| commit.manifest.min-count-to-merge | 100 | Minimum number of manifests to accumulate before merging |
| commit.manifest-merge.enabled | true | Controls whether to automatically merge manifests on writes |
| history.expire.max-snapshot-age-ms | 432000000 (5 days) | Default max age of snapshots to keep while expiring |
| history.expire.min-snapshots-to-keep | 1 | Default min number of snapshots to keep while expiring |
| history.expire.max-ref-age-ms | Long.MAX_VALUE (forever) | Default max age of snapshot references (except main) to keep |
| gc.enabled | true | Allows GC operations such as expiring snapshots and removing orphan files |

## Reserved / Informational / Compatibility properties

| Property | Default | Description |
| --- | --- | --- |
| format-version | 2 | Table's format version (defaults to 2 since 1.4.0) |
| comment | (not set) | A table-level description (informational) |
| compatibility.snapshot-id-inheritance.enabled | false | Enables committing snapshots without explicit snapshot IDs (always true if format version > 1) |

> Source: https://iceberg.apache.org/docs/latest/configuration/
