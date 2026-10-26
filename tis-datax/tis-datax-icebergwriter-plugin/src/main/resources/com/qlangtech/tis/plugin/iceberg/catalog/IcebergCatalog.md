## catalogName

Iceberg Catalog 实例名，用于区分同一 TIS 实例下的多个 Catalog 配置，需保证唯一。

它只作为元数据标识参与 Catalog 初始化，不影响数据文件的存放路径；真正的存储位置由 `fsName` 所选的文件系统决定。

## fsName

选择数据文件所用的文件系统实例（`HDFS` / `AmazonS3` / `Aliyun-Jindo-HDFS` 等）。

数据仓库根路径就是该实例配置的 `rootDir`，Hadoop 连接参数也由该实例提供，因此 HDFS、S3、自建 MinIO 只需要换一个文件系统实例，无需在此重复填写地址与 `core-site.xml`。
