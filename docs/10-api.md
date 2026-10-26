# Iceberg Java API — Apache Iceberg™ (latest / 1.12.0)

The main purpose of the Iceberg API is to manage table metadata: schema, partition spec, metadata, and data files.

Table metadata and operations are accessed through the `Table` interface.

## Table metadata

The `Table` interface provides access to table metadata:
- `schema` — current table schema
- `spec` — current partition spec
- `properties` — map of key-value properties
- `currentSnapshot` — current snapshot
- `snapshots` — all valid snapshots
- `snapshot(id)` — a specific snapshot by ID
- `location` — table's base location
- `refresh` — update to the latest version
- `io` — the `FileIO` to read/write table files
- `locationProvider` — creates paths for data/metadata files

## Scanning

### File level
```
TableScan scan = table.newScan();
TableScan filteredScan = scan.filter(Expressions.equal("id", 5));
TableScan scan = table.newScan().filter(Expressions.equal("id", 5)).select("id", "data");
Schema projection = scan.schema();
Iterable<CombinedScanTask> tasks = scan.planTasks();
```
Use `asOfTime` or `useSnapshot` for time travel.

### Row level
```
ScanBuilder scanBuilder = IcebergGenerics.read(table).where(Expressions.lessThan("id", 5));
CloseableIterable<Record> result = scanBuilder.build();   // Record is org.apache.iceberg.data.Record
```

## Update operations

`Table` exposes `PendingUpdate` builders that commit on `commit()`:
- `updateSchema`, `updateSpec`, `updateStatistics`, `updatePartitionStatistics`, `updateProperties`, `updateLocation`
- `expireSnapshots`, `manageSnapshots`
- `newAppend`, `newFastAppend`, `newOverwrite`, `newDelete`, `newRewrite`, `newRowDelta`, `newTransaction`
- `rewriteManifests`, `replaceSortOrder`, `newReplacePartitions`

## Transactions
```
Transaction t = table.newTransaction();
t.newDelete().deleteFromRowFilter(filter).commit();
t.newAppend().appendFile(data).commit();
t.commitTransaction();
```

## Types

Package `org.apache.iceberg.types`. Primitive instances via static `get()` / factory methods:
```
Types.IntegerType.get();
Types.DecimalType.of(9, 2);
```
Nested types:
```
StructType struct = StructType.of(
  Types.NestedField.required(1, "id", Types.IntegerType.get()),
  Types.NestedField.optional(2, "data", Types.StringType.get()));
MapType map = MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get());
ListType list = ListType.ofRequired(1, IntegerType.get());
```

## Expressions

Factory methods in `Expressions`:
- Predicates: `isNull`, `notNull`, `equal`, `notEqual`, `lessThan`, `lessThanOrEqual`, `greaterThan`, `greaterThanOrEqual`, `in`, `notIn`, `startsWith`, `notStartsWith`
- Operations: `and`, `or`, `not`
- Constants: `alwaysTrue`, `alwaysFalse`

Expressions are unbound until bound to a data type (field ID resolution + literal conversion).

## Modules

- `iceberg-common` — utility classes
- `iceberg-api` — public API (expressions, types, tables, operations)
- `iceberg-arrow` — Arrow in-memory type system
- `iceberg-aws` — S3 + Glue catalog
- `iceberg-core` — API implementations + Avro support; **what processing engines should depend on**
- `iceberg-parquet`, `iceberg-orc` — file format modules
- `iceberg-hive-metastore` — Hive metastore Thrift client
- `iceberg-spark`, `iceberg-flink`, `iceberg-mr`, `iceberg-nessie`, `iceberg-data`, `iceberg-runtime`

> Source: https://iceberg.apache.org/docs/latest/api/
