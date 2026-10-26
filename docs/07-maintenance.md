# Maintenance — Apache Iceberg™ (latest / 1.12.0)

Maintenance operations require the `Table` instance.

## Recommended Maintenance

### Expire Snapshots

Each write to an Iceberg table creates a new *snapshot*, or version, of a table. Snapshots can be used for time-travel queries, or the table can be rolled back to any valid snapshot.

Snapshots accumulate until they are expired by the `expireSnapshots` operation. Regularly expiring snapshots is recommended to delete data files that are no longer needed, and to keep the size of table metadata small.

```
Table table = ...;
long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60 * 24); // 1 day
table.expireSnapshots()
    .expireOlderThan(tsToExpire)
    .commit();
```

Spark action (parallel for large tables):

```
Table table = ...;
SparkActions.get().expireSnapshots(table).expireOlderThan(tsToExpire).execute();
```

Data files are not deleted until they are no longer referenced by a snapshot that may be used for time travel or rollback.

### Remove old metadata files

Iceberg keeps track of table metadata using JSON files. Old metadata files are kept for history by default. Tables with frequent commits (e.g. streaming jobs) may need to regularly clean metadata files.

Each metadata file tracks the older metadata files in the `metadata-log` field. The number of metadata files being tracked is defined by `write.metadata.previous-versions-max`.

To automatically delete older metadata files, set `write.metadata.delete-after-commit.enabled=true`. This keeps some metadata files as tracked (up to `write.metadata.previous-versions-max`), and deletes the oldest metadata file every time a new one is created. It only deletes **tracked** metadata files, not orphaned ones.

Untracked metadata files are also deleted as part of orphan file deletion.

| Property | Default | Description |
| --- | --- | --- |
| `write.metadata.delete-after-commit.enabled` | `false` | Controls whether to delete the oldest **tracked** version metadata files after each table commit |
| `write.metadata.previous-versions-max` | `100` | The max number of previous version metadata files to track |

### Delete orphan files

Use the `deleteOrphanFiles` action to clean up files under a table location that are not referenced by table metadata (e.g. from task/job failures).

```
Table table = ...;
SparkActions.get().deleteOrphanFiles(table).execute();
```

The default retention interval is 3 days. It is dangerous to remove orphan files with a retention interval shorter than the time expected for any write to complete, because in-progress files could be considered orphaned and deleted, corrupting the table.

Iceberg uses the string representations of paths when determining which files to remove. On some file systems the path can change over time but still represent the same file (e.g. changing HDFS authorities). This can lead to data loss when RemoveOrphanFiles is run.

## Optional Maintenance

### Compact data files

Iceberg can compact data files in parallel using Spark with the `rewriteDataFiles` action.

```
Table table = ...;
SparkActions.get().rewriteDataFiles(table)
    .filter(Expressions.equal("date", "2020-08-18"))
    .option("target-file-size-bytes", Long.toString(500 * 1024 * 1024)) // 500 MB
    .execute();
```

### Rewrite manifests

`rewriteManifests` (or the Spark action) re-groups data files into manifests when the write pattern doesn't align with the query pattern.

```
Table table = ...;
SparkActions.get().rewriteManifests(table)
    .rewriteIf(file -> file.length() < 10 * 1024 * 1024) // 10 MB
    .execute();
```

### Rewrite position delete files

`rewritePositionDeletes` performs minor compaction of position delete files and filters dangling records.

### Remove dangling delete files

`removeDanglingDeleteFiles` is a metadata-only operation that drops whole delete files identified as dangling from table metadata.

### Compute table statistics

`computeTableStats` collects NDV (Number of Distinct Values) statistics, writes them to a Puffin statistics file, and registers it in table metadata for cost-based optimization.

### Compute partition statistics

`computePartitionStats` computes partition statistics for a partitioned table and registers the result in table metadata.

## Other Actions

### Delete reachable files

`deleteReachableFiles` deletes all files referenced by a table metadata file (data files, delete files, manifests, manifest lists, metadata files). Used to clean up storage after a table is dropped with `purge` disabled. Irreversible.

### Rewrite table path

`rewriteTablePath` stages a copy of the table's metadata files in which every absolute path starting with the source prefix is replaced by the target prefix. Useful for copying a table to a new location (e.g. disaster recovery).

> Source: https://iceberg.apache.org/docs/latest/maintenance/
