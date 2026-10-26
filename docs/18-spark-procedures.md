# Spark Procedures

> 来源：https://iceberg.apache.org/docs/latest/spark-procedures/
> 版本：Apache Iceberg 1.12.0 (latest)

To use Iceberg in Spark, first configure [Spark catalogs](https://iceberg.apache.org/docs/latest/spark-configuration/).
For Spark 3.x, stored procedures are only available when using [Iceberg SQL extensions](https://iceberg.apache.org/docs/latest/spark-configuration/#sql-extensions) in Spark.
For Spark 4.0, stored procedures are supported natively without requiring the Iceberg SQL extensions. However, note that they are **case-sensitive** in Spark 4.0.

## Usage

Procedures can be used from any configured Iceberg catalog with `CALL`. All procedures are in the namespace `system`.

`CALL` supports passing arguments by name (recommended) or by position. Mixing position and named arguments is not supported.

### Named arguments

All procedure arguments are named. When passing arguments by name, arguments can be in any order and any optional argument can be omitted.

```
CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1);
```

### Positional arguments

When passing arguments by position, only the ending arguments may be omitted if they are optional.

```
CALL catalog_name.system.procedure_name(arg_1, arg_2, ... arg_n);
```

## Snapshot management

### `rollback_to_snapshot`

Roll back a table to a specific snapshot ID.

To roll back to a specific time, use [`rollback_to_timestamp`](https://iceberg.apache.org/docs/latest/spark-procedures/#rollback_to_timestamp).

Info

This procedure invalidates all cached Spark plans that reference the affected table.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `snapshot_id` | ✔️ | long | Snapshot ID to rollback to |

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id` | long | The new current snapshot ID |

**Example**

```
CALL catalog_name.system.rollback_to_snapshot('db.sample', 1);
```

### `rollback_to_timestamp`

Roll back a table to the snapshot that was current at some time.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `timestamp` | ✔️ | timestamp | A timestamp to rollback to |

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id` | long | The new current snapshot ID |

**Example**

```
CALL catalog_name.system.rollback_to_timestamp('db.sample', TIMESTAMP '2021-06-30 00:00:00.000');
```

### `set_current_snapshot`

Sets the current snapshot ID for a table.

Unlike rollback, the snapshot is not required to be an ancestor of the current table state.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `snapshot_id` | | long | Snapshot ID to set as current |
| `ref` | | string | Snapshot Reference (branch or tag) to set as current |

Either `snapshot_id` or `ref` must be provided but not both.

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `previous_snapshot_id` | long | The current snapshot ID before the rollback |
| `current_snapshot_id` | long | The new current snapshot ID |

### `cherrypick_snapshot`

Cherry-picks changes from a snapshot into the current table state.

Cherry-picking creates a new snapshot from an existing snapshot without altering or removing the original.

Only append and dynamic overwrite snapshots can be cherry-picked.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `snapshot_id` | ✔️ | long | The snapshot ID to cherry-pick |

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `source_snapshot_id` | long | The table's current snapshot before the cherry-pick |
| `current_snapshot_id` | long | The snapshot ID created by applying the cherry-pick |

### `publish_changes`

Publish changes from a staged WAP ID into the current table state.

publish_changes creates a new snapshot from an existing snapshot without altering or removing the original.

Only append and dynamic overwrite snapshots can be successfully published.

The `publish_changes` procedure will fail if there are multiple snapshots in the table with the provided `wap_id`.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `wap_id` | ✔️ | string | The wap_id to be published from stage to prod |

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `source_snapshot_id` | long | The table's current snapshot before publishing the change |
| `current_snapshot_id` | long | The snapshot ID created by applying the change |

### `fast_forward`

Fast-forward the current snapshot of one branch to the latest snapshot of another.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `branch` | ✔️ | string | Name of the branch to fast-forward |
| `to` | ✔️ | string | Branch to fast-forward to |

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `branch_updated` | string | Name of the branch that has been fast-forwarded |
| `previous_ref` | long | The snapshot ID before applying fast-forward |
| `updated_ref` | long | The current snapshot ID after applying fast-forward |

## Metadata management

Many [maintenance actions](https://iceberg.apache.org/docs/latest/maintenance/) can be performed using Iceberg stored procedures.

### `expire_snapshots`

Each write/update/delete/upsert/compaction in Iceberg produces a new snapshot while keeping the old data and metadata around for snapshot isolation and time travel. The `expire_snapshots` procedure can be used to remove older snapshots and their files which are no longer needed.

This procedure will remove old snapshots and data files which are uniquely required by those old snapshots. This means the `expire_snapshots` procedure will never remove files which are still required by a non-expired snapshot.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `older_than` | | timestamp | Timestamp before which snapshots will be removed (Default: 5 days ago) |
| `retain_last` | | int | Number of ancestor snapshots to preserve regardless of `older_than` (defaults to 1) |
| `max_concurrent_deletes` | | int | Size of the thread pool used for delete file actions (by default, no thread pool is used) |
| `stream_results` | | boolean | When true, deletion files will be sent to Spark driver by RDD partition. Recommended to set to `true` to prevent Spark driver OOM |
| `snapshot_ids` | | array of long | Array of snapshot IDs to expire. |
| `clean_expired_metadata` | | boolean | When true, cleans up metadata such as partition specs and schemas that are no longer referenced by snapshots. |

If `older_than` and `retain_last` are omitted, the table's [expiration properties](https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties) will be used. Snapshots that are still referenced by branches or tags won't be removed. By default, branches and tags never expire, but their retention policy can be changed with the table property `history.expire.max-ref-age-ms`. The `main` branch never expires.

**Output**

| Output Name | Type | Description |
| --- | --- | --- |
| `deleted_data_files_count` | long | Number of data files deleted by this operation |
| `deleted_position_delete_files_count` | long | Number of position delete files deleted by this operation |
| `deleted_equality_delete_files_count` | long | Number of equality delete files deleted by this operation |
| `deleted_manifest_files_count` | long | Number of manifest files deleted by this operation |
| `deleted_manifest_lists_count` | long | Number of manifest list files deleted by this operation |
| `deleted_statistics_files_count` | long | Number of statistics files deleted by this operation |

### `remove_orphan_files`

Used to remove files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to clean |
| `older_than` | | timestamp | Remove orphan files created before this timestamp (Defaults to 3 days ago) |
| `location` | | string | Directory to look for files in (defaults to the table's location) |
| `dry_run` | | boolean | When true, don't actually remove files (defaults to false) |
| `max_concurrent_deletes` | | int | Size of the thread pool used for delete file actions |
| `stream_results` | | boolean | When true, orphan files will be sent to Spark driver by RDD partition |
| `file_list_view` | | string | Dataset to look for files in (skipping the directory listing) |
| `equal_schemes` | | map | Mapping of file system schemes to be considered equal |
| `equal_authorities` | | map | Mapping of file system authorities to be considered equal |
| `prefix_mismatch_mode` | | string | ERROR / IGNORE / DELETE action when location prefixes mismatch |
| `prefix_listing` | | boolean | When true, use prefix-based file listing via `SupportsPrefixOperations` |

### `rewrite_data_files`

Iceberg can compact data files in parallel using Spark with the `rewriteDataFiles` action. This will combine small files into larger files to reduce metadata overhead and runtime file open cost.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `strategy` | | string | `binpack` or `sort`. Defaults to binpack |
| `sort_order` | | string | For Zorder: `zorder(c1,c2,c3)`; For Hilbert: `hilbert(c1,c2,c3)`; Else comma separated sort orders |
| `options` | | map | Options to be used for actions |
| `where` | | string | predicate as a string used for filtering the files |

**General Options**: `max-concurrent-file-group-rewrites` (5), `partial-progress.enabled` (false), `partial-progress.max-commits` (10), `use-starting-sequence-number` (true), `rewrite-job-order` (none), `target-file-size-bytes` (512 MB), `min-file-size-bytes` (75% of target), `max-file-size-bytes` (180% of target), `min-input-files` (5), `rewrite-all` (false), `max-file-group-size-bytes` (100GB), `max-file-group-input-files`, `delete-file-threshold`, `delete-ratio-threshold` (0.3), `output-spec-id`, `remove-dangling-deletes` (false), `max-files-to-rewrite`, `cache-delete-files` (false).

### `rewrite_manifests`

Rewrite manifests for a table to optimize scan planning.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `use_caching` | | boolean | Use Spark caching during operation (defaults to false) |
| `spec_id` | | int | Spec id of the manifests to rewrite (defaults to current spec id) |
| `sort_by` | | array | List of partition transform names to cluster manifests by |

### `rewrite_position_delete_files`

Iceberg can rewrite position delete files: Minor Compaction (compact small position delete files into larger ones) and Filter Dangling Records (discard records that reference data files that are no longer live).

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to update |
| `options` | | map | Options to be used for procedure |
| `where` | | string | predicate as a string used for filtering the files |

## Table migration

The `snapshot` and `migrate` procedures help test and migrate existing Hive or Spark tables to Iceberg.

### `snapshot`

Create a light-weight temporary copy of a table for testing, without changing the source table.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `source_table` | ✔️ | string | Name of the table to snapshot |
| `table` | ✔️ | string | Name of the new Iceberg table to create |
| `location` | | string | Table location for the new table |
| `properties` | | map | Properties to add to the newly created table |
| `parallelism` | | int | Number of threads to use for file reading (defaults to 1) |
| `ignore_missing_files` | | boolean | When true, skip source data files that cannot be found |

### `migrate`

Replace a table with an Iceberg table, loaded with the source's data files.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to migrate |
| `properties` | | map | Properties for the new Iceberg table |
| `drop_backup` | | boolean | When true, the original table will not be retained as backup (defaults to false) |
| `backup_table_name` | | string | Name of the table that will be retained as backup (defaults to `table_BACKUP_`) |
| `parallelism` | | int | Number of threads to use for file reading (defaults to 1) |
| `ignore_missing_files` | | boolean | When true, skip source data files that cannot be found |

### `add_files`

Attempts to directly add files from a Hive or file based table into a given Iceberg table.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Table which will have files added to |
| `source_table` | ✔️ | string | Table where files should come from, paths are also possible |
| `partition_filter` | | map | A map of partitions in the source table to import from |
| `check_duplicate_files` | | boolean | Whether to prevent files existing in the table from being added (defaults to true) |
| `parallelism` | | int | Number of threads to use for file reading (defaults to 1) |

### `register_table`

Creates a catalog entry for a metadata.json file which already exists but does not have a corresponding catalog identifier.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Table which is to be registered |
| `metadata_file` | ✔️ | string | Metadata file which is to be registered as a new catalog identifier |

## Metadata information

### `ancestors_of`

Report the live snapshot IDs of parents of a specified snapshot.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table to report live snapshot IDs |
| `snapshot_id` | | long | Use a specified snapshot to get the live snapshot IDs of parents |

## Change Data Capture

### `create_changelog_view`

Creates a view that contains the changes from a given table.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the source table for the changelog |
| `changelog_view` | | string | Name of the view to create |
| `options` | | map | A map of Spark read options to use |
| `net_changes` | | boolean | Whether to output net changes. Defaults to false. |
| `compute_updates` | | boolean | Whether to compute pre/post update images. Defaults to true if `identifier_columns` are provided |
| `identifier_columns` | | array | The list of identifier columns to compute updates |

Commonly used Spark read options: `start-snapshot-id`, `end-snapshot-id`, `start-timestamp`, `end-timestamp`.

CDC metadata columns: `_change_type` (INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER), `_change_ordinal`, `_commit_snapshot_id`.

## Table Statistics

### `compute_table_stats`

Calculates the [Number of Distinct Values (NDV) statistics](https://iceberg.apache.org/puffin-spec/#apache-datasketches-theta-v1-blob-type) for a specific table.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table |
| `snapshot_id` | | string | Id of the snapshot to collect stats |
| `columns` | | array | Columns to collect stats |

### `compute_partition_stats`

Computes the [partition stats](https://iceberg.apache.org/spec/#partition-statistics) incrementally and writes the combined result into a `PartitionStatisticsFile`.

| Argument Name | Required? | Type | Description |
| --- | --- | --- | --- |
| `table` | ✔️ | string | Name of the table |
| `snapshot_id` | | string | Id of the snapshot to compute partition stats. Defaults to current snapshot id |

## Table Replication

### `rewrite_table_path`

Stages a copy of the Iceberg table's metadata files where every absolute path source prefix is replaced by the specified target prefix. This can be the starting point to fully or incrementally copy an Iceberg table to a new location.

| Argument Name | Required? | default | Type | Description |
| --- | --- | --- | --- | --- |
| `table` | ✔️ | | string | Name of the table |
| `source_prefix` | ✔️ | | string | The existing prefix to be replaced |
| `target_prefix` | ✔️ | | string | The replacement prefix for `source_prefix` |
| `start_version` | | first metadata.json | string | The chronologically first metadata.json to rewrite |
| `end_version` | | latest metadata.json | string | The chronologically last metadata.json to rewrite |
| `staging_location` | | new directory under metadata | string | The output location for newly rewritten metadata files |
| `create_file_list` | | true | boolean | Whether to generate a file list containing the paths of rewritten metadata |
