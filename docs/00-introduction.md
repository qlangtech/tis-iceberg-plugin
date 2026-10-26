# Documentation — Apache Iceberg™ (latest / 1.12.0)

Apache Iceberg is an open table format for huge analytic datasets. Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink, Hive and Impala using a high-performance table format that works just like a SQL table.

## User experience

Iceberg avoids unpleasant surprises. Schema evolution works and won't inadvertently un-delete data. Users don't need to know about partitioning to get fast queries.

- **Schema evolution** supports add, drop, update, or rename, and has no side-effects
- **Hidden partitioning** prevents user mistakes that cause silently incorrect results or extremely slow queries
- **Partition layout evolution** can update the layout of a table as data volume or query patterns change
- **Time travel** enables reproducible queries that use exactly the same table snapshot, or lets users easily examine changes
- **Version rollback** allows users to quickly correct problems by resetting tables to a good state

## Reliability and performance

Iceberg was built for huge tables. Iceberg is used in production where a single table can contain tens of petabytes of data and even these huge tables can be read without a distributed SQL engine.

- **Scan planning is fast** — a distributed SQL engine isn't needed to read a table or find files
- **Advanced filtering** — data files are pruned with partition and column-level stats, using table metadata

Iceberg was designed to solve correctness problems in eventually-consistent cloud object stores.

- **Works with any cloud store** and reduces NN congestion when in HDFS, by avoiding listing and renames
- **Serializable isolation** — table changes are atomic and readers never see partial or uncommitted changes
- **Multiple concurrent writers** use optimistic concurrency and will retry to ensure that compatible updates succeed, even when writes conflict

## Open standard

Iceberg has been designed and developed to be an open community standard with a specification to ensure compatibility across languages and implementations.

Apache Iceberg is open source, and is developed at the Apache Software Foundation.

### Features

- Schema Evolution
- Hidden Partitioning
- Partition Evolution
- Serializable Isolation
- Branching and Tagging
- Optimistic Concurrency
- Advanced Filtering
- Compute Engine Integrations
- REST Catalog
- Multiple language APIs

> Source: https://iceberg.apache.org/docs/latest/
