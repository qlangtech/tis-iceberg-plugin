## dbName

选择元数据所使用的 Hiveserver2 数据源实例。Metastore 地址（`thrift://…`）与目标库名都直接取自该数据源的配置，因此目标库必须已经在 Hive 中存在。

## clientPoolSize

访问 Metastore 的连接池大小。DataX 会按通道数并发启动多个子任务，每个子任务各持有自己的 Catalog，通道数较多时建议适当调大。
