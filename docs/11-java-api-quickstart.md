# Java API Quickstart — Apache Iceberg™ (latest / 1.12.0)

Tables are created using a `Catalog` or a `Tables` implementation.

## Create a table

### Hive catalog
```
HiveCatalog catalog = new HiveCatalog();
catalog.setConf(spark.sparkContext().hadoopConfiguration());
Map<String, String> properties = new HashMap<>();
properties.put("warehouse", "...");
properties.put("uri", "...");
catalog.initialize("hive", properties);

TableIdentifier name = TableIdentifier.of("logging", "logs");
Table table = catalog.createTable(name, schema, spec);
Table table = catalog.loadTable(name);   // load existing
```
`Catalog` defines `createTable`, `loadTable`, `renameTable`, `dropTable`.

### Hadoop catalog
```
Configuration conf = new Configuration();
String warehousePath = "hdfs://host:8020/warehouse_path";
HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
```
Concurrent writes with a Hadoop catalog are not safe with a local FS or S3.

## Schemas
```
Schema schema = new Schema(
  Types.NestedField.required(1, "level", Types.StringType.get()),
  Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
  Types.NestedField.required(3, "message", Types.StringType.get()),
  Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
);
```
When using the Iceberg API directly, type IDs are required. Conversions from Avro/Spark/Parquet auto-assign IDs. Schema creation re-assigns all IDs for uniqueness.

Convert from Avro: `AvroSchemaUtil.toIceberg(avroSchema)`. Convert from Spark: `SparkSchemaUtil.schemaForTable(sparkSession, tableName)`.

## Partitioning
```
PartitionSpec spec = PartitionSpec.builderFor(schema).hour("event_time").identity("level").build();
```

## Branching and Tagging
```
// Create a branch retained 1 week, keep latest 2 snapshots, 1h max snapshot age
table.manageSnapshots().createBranch("test-branch", 3)
  .setMinSnapshotsToKeep("test-branch", 2)
  .setMaxSnapshotAgeMs("test-branch", 3600000)
  .setMaxRefAgeMs("test-branch", 604800000).commit();

// Create a tag
table.manageSnapshots().createTag("historical-tag", 10).setMaxRefAgeMs("historical-tag", 86400000).commit();

// Write to a branch
table.newAppend().appendFile(FILE_A).toBranch("test-branch").commit();
table.newRowDelta().addRows(DATA_FILE).addDeletes(DELETES).toBranch("test-branch").commit();

// Read from a branch/tag
TableScan branchRead = table.newScan().useRef("test-branch");
TableScan tagRead = table.newScan().useRef("audit-tag");

// Fast forward / replace
table.manageSnapshots().replaceBranch("test-branch", 4).commit();
table.manageSnapshots().fastForward("main", "test-branch"); // via Spark procedure in engines

// Update retention / remove
table.manageSnapshots().setMinSnapshotsToKeep("test-branch", 10).setMaxSnapshotAgeMs("test-branch", 7200000).setMaxRefAgeMs("test-branch", 604800000).commit();
table.manageSnapshots().removeBranch("test-branch").commit();
table.manageSnapshots().removeTag("test-tag").commit();
```

> Source: https://iceberg.apache.org/docs/latest/java-api-quickstart/
