## catalog

选择 Iceberg 表的 Catalog：已有 Hive 数仓选 Hive Metastore，否则选 Hadoop Catalog。两者都需要选择文件系统实例，数据目录即其 rootDir。

## fileFormat

数据文件格式：Parquet（推荐）、ORC、Avro。压缩编码与格式专属参数在该格式的配置块内。

## targetFileSizeBytes

目标文件大小，默认 512MB。常用 64MB ～ 256MB。

## writeMode

写入模式，当前仅支持追加（Append Mode）。

## autoCreateTable

目标表不存在时是否按源表 Schema 自动建表。分区策略在表设置（所选表）中逐表配置。

## snapshotManagement

快照维护：保留数量与过期天数，并合并小 manifest。

## template

DataX 作业配置 JSON 模板，一般无需修改。
