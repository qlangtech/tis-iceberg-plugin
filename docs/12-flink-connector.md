# Flink Connector — Apache Iceberg™ (latest / 1.12.0)

Apache Flink supports creating Iceberg tables directly by specifying `'connector'='iceberg'` in Flink SQL (no explicit Flink catalog required). The SQL `CREATE TABLE ... WITH ('connector'='iceberg', ...)` creates a Flink table in the current Flink catalog (GenericInMemoryCatalog by default) that **maps to** the underlying Iceberg table.

Catalog properties can be set through table properties (see Flink Configuration).

## Table managed in Hive catalog
```
CREATE TABLE flink_table (
  id BIGINT,
  data STRING
) WITH (
  'connector'='iceberg',
  'catalog-name'='hive_prod',
  'uri'='thrift://localhost:9083',
  'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```
Map to a different Iceberg table:
```
CREATE TABLE flink_table (id BIGINT, data STRING) WITH (
  'connector'='iceberg',
  'catalog-name'='hive_prod',
  'catalog-database'='hive_db',
  'catalog-table'='hive_iceberg_table',
  'uri'='thrift://localhost:9083',
  'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```
The underlying catalog database is auto-created if it does not exist when writing.

## Table managed in hadoop catalog
```
CREATE TABLE flink_table (id BIGINT, data STRING) WITH (
  'connector'='iceberg', 'catalog-name'='hadoop_prod',
  'catalog-type'='hadoop', 'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```

## Table managed in REST catalog
```
CREATE TABLE flink_table (id BIGINT, data STRING) WITH (
  'connector'='iceberg', 'catalog-name'='rest_prod',
  'catalog-type'='rest', 'uri'='https://localhost/',
  'credential'='xxxx', 'token'='xxxx', 'scope'='xxxx'
);
```

## Table managed in custom catalog
```
CREATE TABLE flink_table (id BIGINT, data STRING) WITH (
  'connector'='iceberg', 'catalog-name'='custom_prod',
  'catalog-impl'='com.my.custom.CatalogImpl',
  'my-additional-catalog-config'='my-value'
);
```

## Complete example (Hive catalog)
```
CREATE TABLE flink_table (id BIGINT, data STRING) WITH (
  'connector'='iceberg', 'catalog-name'='hive_prod',
  'uri'='thrift://localhost:9083', 'warehouse'='file:///path/to/warehouse'
);
INSERT INTO flink_table VALUES (1,'AAA'),(2,'BBB'),(3,'CCC');
SET execution.result-mode=tableau;
SELECT * FROM flink_table;
```

> Source: https://iceberg.apache.org/docs/latest/flink-connector/
