## catalog

选择 Iceberg 表的 Catalog 管理方式。

Catalog 是 Iceberg 的元数据管理组件，负责维护表的 Schema、分区信息、快照等元数据。支持以下类型：

- **HiveMetastoreCatalog**：使用 Hive Metastore 管理元数据，适用于已有 Hive 数仓的集群。库名取自所选 Hiveserver2 数据源
- **HadoopCatalog**：基于 Hadoop 文件系统管理元数据，适用于没有 Hive 的 Hadoop 集群。数据目录来自所选文件系统实例的 `rootDir`，另需指定库名

## fileFormat

选择数据文件的存储格式。每种格式是一个独立配置块，压缩编码与该格式专属的写入参数都在其内部：

- **Parquet**（推荐）：列式存储格式，压缩比高，适合分析型查询。可配置压缩编码与级别、row group 大小、page 大小/行数上限、字典缓冲区、page 版本、布隆过滤器
- **ORC**：列式存储格式，适合与既有 ORC 生态对接。可配置压缩编码与策略、stripe / block 大小、向量化批大小、布隆过滤器列与误判率
- **Avro**：行式存储格式，适合通用场景与流式写入。可配置压缩编码与级别

格式本身通过表属性 `write.format.default` 下发，各格式的专属参数下发为 `write.parquet.*` / `write.orc.*` / `write.avro.*`。

## targetFileSizeBytes

目标文件大小（字节）。Iceberg 会按该大小切分写入文件，属性名为 `write.target-file-size-bytes`。

- 默认值：536870912（512MB，与 Iceberg 默认值一致）
- 建议区间：64MB ～ 256MB，按数据量与查询模式调整

## writeMode

数据写入模式：

- **Append Mode**：追加写入，新数据追加到表中，不修改已有数据

## autoCreateTable

目标表不存在时的建表策略。开启后，系统按源表 Schema 自动映射类型（源类型 → Iceberg 类型）并建表。分区策略在表设置（所选表）中逐表配置，建表时与表属性一起应用。

## snapshotManagement

快照维护策略。Iceberg 每次写入都会生成新快照，需要定期清理以释放存储：

- **保留快照数量**：无论是否过期，至少保留最近 N 个快照，保证时间旅行仍可用
- **过期天数**：早于该天数的快照会被清理（连同其独占的数据文件）

保存时会额外合并小于 8MB 的 manifest 文件。孤立文件清理（`removeOrphanFiles`）位于 iceberg-actions 模块，未随本插件打包，因此不做。

## template

DataX 作业配置 JSON 模板，用于生成最终提交给 DataX 引擎执行的作业配置。

模板基于 Velocity，可用的变量都挂在 `$writer` 上：

- `$writer.tableName`：目标表名
- `$writer.sourceTableName`：源表名
- `$writer.databaseName`：目标库名
- `$writer.icebergCols`：建表列集合，可 `#foreach` 遍历，每项含 `name` / `type`

默认模板见 `iceberg-writer-tpl.json`，一般情况下无需修改。
