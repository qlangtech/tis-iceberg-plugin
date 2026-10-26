# Custom Catalog — Apache Iceberg™ (latest / 1.12.0)

It's possible to read an Iceberg table either from an HDFS path or from a Hive table, and to use a custom metastore in place of Hive. Steps:

- Custom TableOperations
- Custom Catalog
- Custom FileIO
- Custom LocationProvider
- Custom IcebergSource

Note: To work with encrypted tables, custom catalogs must address security requirements.

## Custom table operations implementation

Extend `BaseMetastoreTableOperations` to provide implementation on how to read and write metadata.

Key methods:
- `doRefresh()` — get the metadata location (e.g. from a custom service) and call `refreshFromMetadataLocation(metadataLocation)`
- `doCommit(TableMetadata base, TableMetadata metadata)` — write new metadata via `writeNewMetadata(...)` and atomically update the location via a custom service
- `io()` — provide a `FileIO` used to read/write table metadata files

A `TableOperations` instance is usually obtained by calling `Catalog.newTableOps(TableIdentifier)`.

## Custom catalog implementation

Extend `BaseMetastoreCatalog` to provide default warehouse locations and instantiate `CustomTableOperations`.

- Must have a no-arg constructor to be dynamically loaded; `initialize(String name, Map<String, String> properties)` completes initialization.
- `newTableOps(TableIdentifier)` returns the custom `TableOperations`.
- `defaultWarehouseLocation(TableIdentifier)` builds the table location (s3 or hdfs).
- `dropTable`, `renameTable` delegate to the custom service.

Catalog implementations can be dynamically loaded in most compute engines. For Spark and Flink, specify the `catalog-impl` catalog property. For MapReduce, implement `org.apache.iceberg.mr.CatalogLoader` and set `iceberg.mr.catalog.loader.class`.

## Custom FileIO implementation

Extend `FileIO` to provide implementation to read and write data files (`newInputFile`, `newOutputFile`, `deleteFile`). Can be dynamically loaded in `HadoopCatalog` and `HiveCatalog` via the `io-impl` catalog property.

## Custom location provider implementation

Extend `LocationProvider` to determine the file path to write data. Override `TableOperations.locationProvider()` to use a custom default, or set table property `write.location-provider.impl` for a specific table.

## Custom IcebergSource

Extend `IcebergSource` and implement `findTable(DataSourceOptions, Configuration)` to read from a `CustomCatalog`. Register via `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

> Source: https://iceberg.apache.org/docs/latest/custom-catalog/
