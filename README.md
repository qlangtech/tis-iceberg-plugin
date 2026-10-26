# TIS Apache Iceberg 插件

## 概述

这是一个为 TIS（Qlangtech Intelligence System）平台开发的 Apache Iceberg 数据端插件，提供批量读取和增量读取功能，支持与 Apache Iceberg 数据湖的集成。

## 项目结构

```
tis-iceberg-plugin/
├── pom.xml                                    # 根POM文件
├── tis-datax/                                # 批量处理模块
│   └── tis-datax-icebergwriter-plugin/      # DataX Writer插件
│       ├── pom.xml
│       └── src/
│           ├── main/java/                   # 主要代码
│           │   └── com/qlangtech/tis/plugin/iceberg/
│           │       ├── datax/               # DataX相关类
│           │       │   ├── DataxIcebergWriter.java
│           │       │   ├── IcebergSelectedTab.java
│           │       │   ├── IcebergDataXContext.java
│           │       │   ├── partition/      # 分区策略
│           │       │   └── writemode/      # 写入模式
│           │       ├── catalog/            # Catalog实现
│           │       │   ├── IcebergCatalog.java
│           │       │   ├── HiveMetastoreCatalog.java
│           │       │   └── HadoopCatalog.java
│           │       └── utils/              # 工具类
│           └── test/java/                  # 测试代码
└── tis-incr/                               # 增量处理模块
    └── tis-flink-pipeline-iceberg-plugin/  # Flink Pipeline插件
        ├── pom.xml
        └── src/
            └── main/java/
                └── com/qlangtech/tis/plugins/incr/flink/pipeline/iceberg/
                    └── sink/               # Flink Sink实现
                        └── IcebergPipelineSinkFactory.java
```

## 主要功能

### 1. 批量数据处理 (DataX模块)

- **DataxIcebergWriter**: 主要的DataX写入器，支持批量数据导入到Iceberg表
- **Catalog支持**:
  - Hive Metastore Catalog
  - Hadoop Catalog
  - AWS Glue Catalog (待实现)
- **存储复用 TIS 文件系统插件**: Catalog 通过 `fsName` 选用 `HDFS` / `AmazonS3`（含 MinIO）/ `Aliyun-Jindo-HDFS` 实例，
  数据目录即该实例的 `rootDir`，Hadoop 连接参数由它提供
- **写入模式**:
  - Append模式：追加数据
  - Overwrite模式：覆盖数据（待实现）
  - Upsert模式：更新插入（待实现）
- **分区策略**:
  - 无分区
  - 时间分区（年/月/日/小时）
  - 哈希分区（待实现）
  - 范围分区（待实现）

### 2. 增量流处理 (Flink模块)

- **IcebergPipelineSinkFactory**: Flink CDC到Iceberg的实时同步
- **特性**:
  - 支持Schema自动演化
  - 支持Upsert模式
  - 事务性写入保证
  - Checkpoint机制

### 3. 高级特性

- **快照管理**: 自动清理过期快照，维护表的存储效率
- **文件格式支持**: Parquet、ORC、Avro
- **压缩支持**: 按格式可选（Parquet: UNCOMPRESSED/SNAPPY/GZIP/LZ4_RAW/ZSTD；ORC: NONE/ZLIB/SNAPPY/LZ4/ZSTD；Avro: UNCOMPRESSED/SNAPPY/GZIP/ZSTD）
- **时间旅行**: 支持查询历史版本数据（Iceberg特性）

## 依赖版本

- Apache Iceberg: 1.5.0
- Apache Flink: 1.17.1
- Apache Hadoop: 3.3.4
- Apache Hive: 3.1.3
- TIS Plugin Parent: 4.3.0

## 构建项目

```bash
# 在项目根目录执行
mvn clean package

# 生成的插件包位置
# tis-datax/tis-datax-icebergwriter-plugin/target/tis-datax-icebergwriter-plugin-4.3.0.tpi
# tis-incr/tis-flink-pipeline-iceberg-plugin/target/tis-flink-pipeline-iceberg-plugin-4.3.0.tpi
```

## 使用方法

### 1. 批量导入配置示例

在TIS平台中配置DataX任务时，选择Iceberg Writer：

Catalog 不直接填写 warehouse 路径与 Hadoop 配置目录，而是复用 TIS 的文件系统插件
（`HDFS` / `AmazonS3` / `Aliyun-Jindo-HDFS`，含自建 MinIO）：数据目录即该实例的 `rootDir`，
Hadoop 连接参数也由它提供。

```json
{
  "catalog": {
    "impl": "com.qlangtech.tis.plugin.iceberg.catalog.HiveMetastoreCatalog",
    "vals": {
      "catalogName": "iceberg_hive",
      "fsName": "hdfs_default",
      "dbName": "hiveserver2_default"
    }
  },
  "fileFormat": {
    "impl": "com.qlangtech.tis.plugin.iceberg.format.ParquetFileFormat",
    "vals": { "compressionCodec": "zstd" }
  },
  "targetFileSizeBytes": 134217728,
  "writeMode": "append"
}
```

`fsName` 指向一个已配置的文件系统实例：选 `HDFS` 就是 hdfs 集群，选 `AmazonS3` 就是公有云 S3 或自建 MinIO，
不需要为 Iceberg 再单独填 warehouse 路径与 `core-site.xml`。

分区策略是逐表配置的，在表设置（`IcebergSelectedTab`）里设置，不在 writer 上：

```json
{
  "partitionSpec": {
    "impl": "com.qlangtech.tis.plugin.iceberg.datax.partition.TimePartition",
    "vals": { "timeColumn": "create_time", "granularity": "day" }
  }
}
```

### 2. 实时同步配置示例

配置Flink Pipeline任务：

```json
{
  "catalog": "HiveMetastoreCatalog",
  "fileFormat": "parquet",
  "parallelism": 4,
  "enableUpsert": true,
  "equalityFieldColumns": "id,update_time",
  "checkpointInterval": 60000
}
```

## 测试

运行单元测试：

```bash
mvn test
```

运行集成测试需要配置相应的测试环境（Hadoop、Hive Metastore等）。

## 与Paimon插件的对比

| 特性 | Iceberg插件 | Paimon插件 |
|------|------------|-----------|
| 表格式 | Apache Iceberg | Apache Paimon |
| Catalog支持 | Hive、Hadoop、Glue、REST | Hive、FileSystem |
| 分区演化 | 支持 | 有限支持 |
| Schema演化 | 完全支持 | 支持 |
| 时间旅行 | 原生支持 | 支持 |
| 事务 | ACID事务 | ACID事务 |
| 文件格式 | Parquet、ORC、Avro | Parquet、ORC、Avro |
| 生态系统 | 广泛（Spark、Presto、Trino等） | 主要是Flink |

## 注意事项

1. **环境要求**: 需要正确配置Hadoop环境和相应的Catalog服务
2. **版本兼容性**: 确保Iceberg版本与其他组件（Flink、Spark等）兼容
3. **性能调优**: 根据数据量调整文件大小、并行度等参数
4. **数据一致性**: 在实时同步场景中，合理设置checkpoint间隔

## 贡献指南

欢迎提交Issue和Pull Request。在提交代码前，请确保：

1. 所有测试通过
2. 代码符合项目编码规范
3. 新功能有相应的测试覆盖
4. 更新相关文档

## 许可证

本项目遵循与TIS平台相同的许可证。

## 联系方式

- 项目维护：qlangtech.com
- 问题反馈：通过GitHub Issues

## 更新日志

### v4.3.0
- 初始版本发布
- 支持批量数据写入
- 支持Flink实时同步
- 基础Catalog支持（Hive、Hadoop）