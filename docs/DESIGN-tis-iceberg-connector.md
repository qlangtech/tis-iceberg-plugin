# TIS 数据整合平台 · Iceberg Connector 设计说明书

> 版本：v1.0 (Draft)
> 基于：Apache Iceberg 官方文档 `latest` (1.12.0)
> 关联资料：本仓库 `docs/` 目录下已归档的 Iceberg 各子页面（`00-introduction.md` ~ `18-spark-procedures.md`）
> 适用范围：TIS 数据整合平台对 Apache Iceberg 表格式的支持，包含 **DataX 批式 Connector** 与 **Flink 实时 Connector** 两条技术路线。

---

## 1. 文档概述

### 1.1 目的

本说明书定义 TIS 数据整合平台接入 Apache Iceberg 表格式的整体技术方案，明确以下两类 Connector 的架构、职责边界、关键流程与配置体系：

- **DataX 批式 Connector**：以离线/批量同步为目标，基于 DataX 的 Reader/Writer 插件模型，将上游数据源写入 Iceberg（Iceberg Writer），或从 Iceberg 读取（Iceberg Reader）。
- **Flink 实时 Connector**：以流式/近实时为目标，基于 Iceberg 官方 Flink 集成（Source + Sink），实现 exactly-once 写入、流式读取、UPSERT 等能力。

### 1.2 设计原则

| 原则 | 说明 |
| --- | --- |
| 复用 Iceberg 内核 | 不重复造轮子，批式/流式均直接复用 Iceberg 的 Catalog、Table API、Manifest/Snapshot 机制与文件格式层（见 `06-reliability.md`、`10-api.md`）。 |
| 配置统一 | 批式与流式共享同一套 Catalog 配置与表属性（`table-default.*` / `table-override.*`，见 `15-spark-configuration.md` 与 `08-configuration.md`）。 |
| Schema 演进友好 | 利用 Iceberg 的 Schema Evolution（见 `03-evolution.md`）做到加列无损、类型安全拓宽。 |
| 隐藏分区 | 对上游屏蔽物理分区细节，使用 Iceberg 的 Hidden Partitioning（见 `02-partitioning.md`）。 |
| ACID 与并发 | 批式与流式写入统一依赖 Iceberg 原子提交 + 乐观并发（见 `06-reliability.md`）。 |

### 1.3 参考资料（本仓库已归档）

- 概念与机制：`00-introduction.md`、`01-schemas.md`、`02-partitioning.md`、`03-evolution.md`、`04-branching.md`、`05-performance.md`、`06-reliability.md`、`07-maintenance.md`、`08-configuration.md`
- 扩展与 API：`09-custom-catalog.md`、`10-api.md`、`11-java-api-quickstart.md`
- 引擎集成：`12-flink-connector.md`、`13-flink-writes.md`、`14-flink-queries.md`、`15-spark-configuration.md`、`16-spark-writes.md`、`17-spark-ddl.md`、`18-spark-procedures.md`

---

## 2. 背景与目标

### 2.1 为什么是 Iceberg

Iceberg 是一种面向海量分析数据集的**开放表格式**（open table format），具备：

- **隐藏分区**（Hidden Partitioning）：查询无需关心物理分区字段，分区变换由表维护（`02-partitioning.md`）。
- **模式演进**（Schema Evolution）：加列、改名、改类型顺序、类型安全拓宽均不破坏存量数据（`03-evolution.md`）。
- **可串行化隔离 + 乐观并发**：每次写入为原子提交，O(1) 元数据 RPC，兼容对象存储（`06-reliability.md`）。
- **分支与标签**（Branching & Tagging）：支撑 WAP（Write-Audit-Publish）写审计发布、时间点快照隔离（`04-branching.md`）。
- **多引擎可读**：Spark / Flink / Trino 等共享同一份 Iceberg 元数据，便于 TIS 批流结果相互可见。

### 2.2 TIS 接入目标

| 目标 | 批式 (DataX) | 实时 (Flink) |
| --- | --- | --- |
| 数据写入 Iceberg | ✔ Append / Overwrite | ✔ Append / Upsert / CDC |
| 数据读取 Iceberg | ✔ Snapshot 读取 / Time Travel | ✔ Streaming Read / Batch Read |
| 自动建表 / 分区 | ✔ | ✔ |
| Schema 自动映射与演进 | ✔ | ✔ |
| 表维护（compact/expire） | ✔ (离线调度) | ✔ (post-commit maintenance) |

---

## 3. 总体架构

```
┌──────────────────────────────────────────────────────────────┐
│                        TIS 控制台 / 调度                        │
│  (数据源注册、任务编排、质量校验、监控)                          │
└───────────────┬──────────────────────────┬─────────────────────┘
                │                          │
        ┌───────▼────────┐         ┌───────▼────────┐
        │  DataX 批式引擎 │         │  Flink 流处理   │
        │  (Reader/Writer)│         │  (Source/Sink)  │
        └───────┬────────┘         └───────┬────────┘
                │                          │
        ┌───────▼──────────────────────────▼────────┐
        │        TIS-Iceberg-Plugin (公共基础层)       │
        │  ┌──────────┐ ┌──────────┐ ┌─────────────┐ │
        │  │ Catalog  │ │ Schema/  │ │ Table Config │ │
        │  │ 接入管理  │ │ 类型映射 │ │  属性管理     │ │
        │  └──────────┘ └──────────┘ └─────────────┘ │
        │  ┌──────────┐ ┌──────────┐ ┌─────────────┐ │
        │  │ 分区/演进 │ │ 分支标签 │ │ 表维护Maintenance│ │
        │  └──────────┘ └──────────┘ └─────────────┘ │
        └───────────────┬──────────────────────────┘
                        │  Iceberg Java API / FileIO
        ┌───────────────▼──────────────────────────┐
        │            Iceberg 表格式内核               │
        │  Catalog → Table → Snapshot → Manifest →   │
        │  Data/Delete Files (Parquet/ORC/Avro)      │
        └───────────────┬──────────────────────────┘
                        │
        ┌───────────────▼──────────────────────────┐
        │  存储层：HDFS / S3 / OSS / COS / 对象存储   │
        └───────────────────────────────────────────┘
```

> 说明：**公共基础层** 是批式与流式 Connector 共享的核心。它封装 Catalog 解析、Schema 推导、类型映射、表属性模板、分区演进与维护动作，避免两套引擎重复实现同一逻辑。

---

## 4. 公共基础层设计

### 4.1 Catalog 接入

Iceberg 的 Catalog 决定表元数据存放在哪里、如何加锁与提交。TIS 需将"数据源"概念映射到 Iceberg Catalog 实现（见 `09-custom-catalog.md`、`15-spark-configuration.md`、`10-api.md`）。

| TIS 数据源类型 | Iceberg Catalog 实现 | 关键参数 |
| --- | --- | --- |
| Hive Metastore | `HiveCatalog` (`type=hive`) | `uri=thrift://...` |
| Hadoop / HDFS 目录 | `HadoopCatalog` (`type=hadoop`) | `warehouse=hdfs://nn:8020/warehouse` |
| REST 服务 | `RESTCatalog` (`type=rest`) | `uri=http://...` |
| JDBC | `JdbcCatalog` (`type=jdbc`) | 连接串 + 表名空间 |
| 自定义 | `catalog-impl` 指定实现类 | 自定义配置 |

TIS 侧约定：

- 在 TIS 数据源注册时，选择 Catalog 类型并填入对应连接参数；插件将其翻译为 Iceberg 的 `Catalog` 实例（Java：`CatalogUtil.loadCatalog(...)`，见 `10-api.md`、`11-java-api-quickstart.md`）。
- 通过 `table-default.*` / `table-override.*` 注入全局默认表属性（如 `write.target-file-size-bytes`、`format-version`），其中 `table-override.*` 用于强制合规、不可被建表语句覆盖（见 `15-spark-configuration.md`）。

### 4.2 Schema 与类型映射

Iceberg 类型系统（完整见 `01-schemas.md`）是映射的单一事实来源。TIS 在"建表/同步"阶段，将上游类型统一翻译为 Iceberg 类型：

| Iceberg 类型 | 说明 | 常见上游映射 |
| --- | --- | --- |
| `boolean` | 布尔 | BOOLEAN |
| `int` / `long` | 32/64 位整型 | INT / BIGINT（INT 可安全拓宽为 LONG） |
| `float` / `double` | 单/双精度 | FLOAT → DOUBLE（安全拓宽） |
| `decimal(P,S)` | 定精度 | DECIMAL（P 增大为安全演进） |
| `date` / `time` / `timestamp` / `timestamptz` | 时间类型 | DATE / TIME / TIMESTAMP |
| `string` / `uuid` / `fixed(N)` / `binary` | 字符串/字节 | STRING / VARCHAR / BINARY |
| `struct` / `list` / `map` | 嵌套类型 | JSON/ARRAY/MAP |

设计要点：

- **类型安全拓宽**：仅允许 `int→long`、`float→double`、`decimal(P,S)→decimal(P2,S) (P2>P)`（`17-spark-ddl.md` 的 ALTER COLUMN 规则同样适用于 TIS 自动演进）。
- **标识符字段（Identifier Fields）**：为支持 Flink UPSERT，TIS 在建表时通过 `SET IDENTIFIER FIELDS`（`17-spark-ddl.md`）声明主键等价字段，且这些字段必须为 `NOT NULL`。
- **Schema 演进策略**：当上游新增列时，TIS 发起 `ADD COLUMN`（支持嵌套 `struct`/`array.element`/`map.value`，见 `17-spark-ddl.md`），不阻断存量数据读取。

### 4.3 文件格式与压缩

Iceberg 支持 Parquet（默认）、ORC、Avro。TIS 默认使用 Parquet，并支持通过表属性 `write.format.default` 覆盖（见 `08-configuration.md`、`15-spark-configuration.md`）。压缩编解码（zstd/snappy 等）由 `write.(format).compression-codec` 控制。

### 4.4 表属性与配置体系

集中管理三类配置（详见 `08-configuration.md`）：

- **Read 属性**：`read.split.target-size`、`read.parquet.vectorization.enabled` 等，控制 Scan Planning 与向量化。
- **Write 属性**：`write.target-file-size-bytes`（默认 512MB）、`write.distribution-mode`（none/hash/range）、`write.spark.fanout.enabled` 等。
- **Table-behavior 属性**：`history.expire.max-snapshot-age-ms`、`history.expire.max-ref-age-ms`、format-version 等，控制 Snapshot 保留与格式版本。

配置优先级（见 `15-spark-configuration.md`）：DataSource Options > Session 配置 > Table Properties > 默认值。TIS 将"任务级参数"作为最高优先级注入，保证运维可控。

---

## 5. DataX 批式 Connector 设计

### 5.1 定位

DataX 以 **Reader + Writer** 插件对组织离线同步任务。TIS 在 DataX 体系内新增：

- **IcebergWriter**：将任意 DataX Reader 产出的记录批量提交为 Iceberg Snapshot（Append 或 Overwrite）。
- **IcebergReader**（可选）：将 Iceberg 表按 Snapshot / 时间旅行读取为 DataX Record，供下游 Writer 消费。

批式写入不依赖 Spark/Flink 执行引擎，直接调用 Iceberg Java API（`10-api.md`），保证轻量与资源可控。

### 5.2 IcebergWriter 写入流程

```
上游 Reader 产出 Record 流
        │
        ▼
  TIS 类型转换（4.2 映射为 Iceberg 类型）
        │
        ▼
  按分区/分桶组织（use distribution-mode=hash 默认）
        │  ├─ 同一 Spark/DataX task 内按分区值聚簇
        │  └─ 目标文件大小受 write.target-file-size-bytes 控制（08-configuration.md）
        ▼
  Iceberg Append / Overwrite 提交
        │
        ▼
  原子提交 → 生成新 Snapshot（乐观并发，06-reliability.md）
```

关键设计点：

1. **Append vs Overwrite**
   - 追加同步：等价于 `INSERT INTO`（`16-spark-writes.md`）。
   - 覆盖同步：采用 **dynamic overwrite**（推荐）语义——仅替换 SELECT 结果中出现的分区（`16-spark-writes.md` 的 "Dynamic overwrite"），避免静态覆盖误删其他分区。
2. **Distribution Mode**：默认 `hash`（Iceberg 1.2.0+ 默认值，`16-spark-writes.md`），由插件在写前按分区列做 shuffle/sort，减少打开的文件句柄。若上游已按分区有序，可显式设 `none` + fanout 以省去排序。
3. **Schema Merge**：当上游 schema 与表不一致时，若表开启 `write.spark.accept-any-schema=true` 且写入侧启用 `mergeSchema`，Iceberg 自动补列（`16-spark-writes.md` 的 "Schema Merge"）。TIS 建议在首次检测到新增列时改为显式 `ADD COLUMN` 以保持可审计。
4. **批式一致性**：每次提交为一个 Snapshot，天然原子；并发写入走乐观并发（commit.retry.* 重试，见 `08-configuration.md`）。

### 5.3 IcebergReader（读取）

- 支持指定 `snapshot-id` 或 `as-of-timestamp` 进行 Time Travel（`14-flink-queries.md` 的 read options 同样适用于 Java API 的 Scan 配置）。
- Scan Planning 复用 Iceberg 的 Manifest 元数据过滤（`05-performance.md`），仅加载相关 Data File，配合 `read.split.target-size` 切分任务。
- 支持列裁剪与谓词下推（Parquet/ORC 内置统计 + Puffin 表统计，见 `05-performance.md`、`18-spark-procedures.md` 的 `compute_table_stats`）。

### 5.4 批式场景与 Spark 的关系（可选路径）

对于超大规模或需复杂变换的批式同步，TIS 也可选择 **Spark 作为执行引擎**，复用 Iceberg 的 `DataFrameWriterV2`（`16-spark-writes.md`）与 `CALL` 存储过程（`18-spark-procedures.md`）。此时 DataX Connector 退化为"提交描述"，实际计算交给 Spark。两条路径共享同一 Catalog 与表属性，互不冲突。

---

## 6. Flink 实时 Connector 设计

### 6.1 定位

基于 Iceberg 官方 Flink 集成（`12-flink-connector.md`、`13-flink-writes.md`、`14-flink-queries.md`）。连接器注册为 `'connector'='iceberg'`，通过 `catalog-name` / `catalog-type` 关联公共基础层的 Catalog 实例。

### 6.2 IcebergSink（写入）

核心能力：

- **Exactly-once**：依赖 Iceberg 提交幂等 + Flink Checkpoint，Sink 在 checkpoint 时提交 Snapshot（`13-flink-writes.md`）。
- **写入模式**：
  - `INSERT INTO`（Append）
  - `INSERT OVERWRITE`（覆盖）
  - `UPSERT`：需表为 **v2 format** 且声明 Identifier Fields（`13-flink-writes.md`；标识符字段声明见 `17-spark-ddl.md` 的 `SET IDENTIFIER FIELDS`）。
- **Distribution Mode（Flink 侧）**：`HASH` / `RANGE` / `NONE`，与批式语义一致，控制 shuffle 与文件聚簇（`13-flink-writes.md`、`16-spark-writes.md`）。
- **IcebergSink（SinkV2 / FLIP-191）**：实验性新 API，支持更灵活的提交与自定义算子（`13-flink-writes.md`）；Dynamic Iceberg Sink 进一步简化分区动态扩展。
- **Metrics & 小文件**：Flink 流式写入易产生小文件，需配合 Maintenance（见 6.4）。

### 6.3 IcebergSource（读取）

- **Streaming Read**：通过 `monitor-interval` 周期性发现新 Snapshot，`starting-strategy` 决定起点（`14-flink-queries.md`）。
  - 起点控制：`start-snapshot-id`、`stream-from-timestamp`、`starting-strategy`（如 `TABLE_SCAN_STARTING_STRATEGY`）。
- **Watermark**：流读可基于事件时间生成 Watermark，支撑窗口计算。
- **Batch Read**：一次性全量/增量读取，等价离线扫描。
- **Metadata Tables**：可读取 `$history` / `$snapshots` / `$files` / `$manifests` / `$refs` 等元数据表（`14-flink-queries.md`），用于审计与运维。

### 6.4 Post-commit Maintenance（流式表维护）

Flink 侧可通过 `flink-maintenance.*` 在提交后自动触发轻量维护（`13-flink-writes.md`、`07-maintenance.md`）：

- 周期性 `rewrite_data_files`（小文件合并，binpack/sort）。
- 周期性 `expire_snapshots`（Snapshot 过期，释放空间）。
- 周期性 `rewrite_manifests`（优化 Scan Planning）。

> 重维护动作（如 `remove_orphan_files`、大表 `rewrite_data_files`）建议在 **TIS 离线调度**中执行（见第 7 节），避免占用流作业资源。

---

## 7. 高级能力设计

### 7.1 分区演进（Partition Evolution）

Iceberg 允许在不重写存量数据的前提下新增/替换分区字段（`03-evolution.md`、`17-spark-ddl.md` 的 `ADD/DROP/REPLACE PARTITION FIELD`）。TIS 设计：

- 当用户在控制台调整分区策略（如 `days(ts)` → `hours(ts)`），插件执行 `ADD PARTITION FIELD` 为**元数据操作**，旧数据保留原布局、新数据采用新分区。
- **注意**：动态覆盖语义会随分区变化而改变（新增小时级分区后，覆盖仅命中小时分区）。TIS 在分区演进时需向用户提示此行为（`17-spark-ddl.md` 的 Danger 提示）。

### 7.2 分支与标签（Branching & Tagging）+ WAP

利用 Iceberg 分支实现 **Write-Audit-Publish**（`04-branching.md`）：

1. 数据先写入审计分支 `audit-branch`（SQL：`INSERT INTO tbl.branch_audit ...`，`16-spark-writes.md`）。
2. 质量校验通过后，`publish_changes` 或 `fast_forward` 将分支合并到 `main`（`18-spark-procedures.md`）。
3. 标签（Tag）用于固定重要时间点快照（如"日终快照"），`CREATE TAG ... RETAIN 365 DAYS`（`17-spark-ddl.md`）。

这对 TIS 的"数据质量门禁"场景尤为关键：错误批次只污染分支，不进入主表。

### 7.3 ACID 与并发控制

- **原子提交**：每次写生成新 Metadata JSON + Snapshot，原子替换（`06-reliability.md`）。
- **乐观并发**：提交冲突时按 `commit.retry.*` 重试（`08-configuration.md`）；批式与流式写入同一表时共享此机制。
- **O(1) RPC**：规划扫描仅读取必要 Manifest，不扫全表，适合对象存储（`06-reliability.md`、`05-performance.md`）。
- **锁**：分布式场景下可启用 JDBC / ZooKeeper / Flink-managed 锁（`07-maintenance.md`）。

### 7.4 表维护（Maintenance）总览

| 动作 | 作用 | 推荐触发方 |
| --- | --- | --- |
| `expire_snapshots` | 清理过期 Snapshot 及其独有文件（`18-spark-procedures.md`） | TIS 离线调度 / Flink post-commit |
| `rewrite_data_files` | 小文件合并 / 排序（binpack/sort/zorder/hilbert） | TIS 离线调度（重） |
| `rewrite_manifests` | 优化 Scan Planning | 离线 / Flink post-commit |
| `remove_orphan_files` | 清理无引用孤儿文件（先 dry_run） | TIS 离线调度 |
| `rewrite_position_delete_files` | 合并/清理 position delete | 离线 |
| `compute_table_stats` / `compute_partition_stats` | 生成 Puffin 统计，提升 CBO | 离线 |

TIS 将以上动作封装为**可编排的维护任务**，按表配置 cron 与阈值（如"每天 02:00 对小文件 > N 的表做 rewrite_data_files"）。

---

## 8. 配置项与默认值汇总（节选）

完整表见 `08-configuration.md` 与 `15-spark-configuration.md`，常用项：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `format-version` | 1（建议新表设 2 以支持 UPSERT/delete） | Iceberg 格式版本 |
| `write.target-file-size-bytes` | 536870912 (512MB) | 目标文件大小 |
| `write.distribution-mode` | `hash`（1.2.0+） | 写入分布策略 |
| `read.split.target-size` | 134217728 (128MB) | Scan 切分大小 |
| `history.expire.max-snapshot-age-ms` | 432000000 (5d) | Snapshot 默认保留 |
| `write.spark.accept-any-schema` | false | 是否允许自动补列 |

---

## 9. 实施路线图

| 阶段 | 内容 | 交付 |
| --- | --- | --- |
| P1 基础 | 公共基础层：Catalog 接入、类型映射、建表 DDL 生成 | IcebergWriter（Append）可用 |
| P2 批式完善 | DataX IcebergWriter/Reader、dynamic overwrite、Schema 演进、Time Travel | 离线同步全链路 |
| P3 流式 | Flink IcebergSource/Sink、exactly-once、UPSERT、streaming read | 实时入湖 |
| P4 高阶 | 分区演进、分支/WAP 质量门禁、Maintenance 编排 | 生产级治理 |
| P5 引擎可选 | Spark 执行引擎路径（大批量变换） | 弹性算力 |

---

## 10. 风险与注意事项

1. **Hive Catalog 限制**：HMS 按位置校验 schema 变更，加列 `FIRST/AFTER`、非末列 `DROP COLUMN` 会失败（`17-spark-ddl.md`）。TIS 在使用 Hive Catalog 时应改用 Iceberg 元数据驱动演进，或在必要时关闭 `hive.metastore.disallow.incompatible.col.type.changes`（需权衡 Hive 引擎可读性）。
2. **对象存储一致性**：依赖存储的原子 rename/put；O(1) RPC 假设存储元数据可 listing（`06-reliability.md`）。
3. **小文件**：流式写入默认易碎文件，必须配 Maintenance。
4. **format-version 选择**：UPSERT/equality delete 需 v2；`convertEqualityDeletes` 需 v>=3（`07-maintenance.md`）。
5. **分支覆盖语义变化**：分区演进后动态覆盖范围变化（`17-spark-ddl.md` Danger）。
6. **跨引擎可见性**：Flink 写入的 delete 文件需下游引擎支持 merge-on-read，否则读取侧需 copy-on-write 物化。

---

*本说明书随 Iceberg 版本（当前 latest=1.12.0）与 TIS 插件实现演进持续更新。所有 Iceberg 机制引用均可在 `docs/` 目录对应子页面中查证。*
