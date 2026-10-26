## databaseName

目标 Iceberg 命名空间。对应 warehouse 根目录下的 `{库名}.db` 目录，库不存在时自动创建。

Hadoop Catalog 不使用 Metastore，表的元数据以 JSON 文件的形式直接存放在 warehouse 下的表目录中，因此 `fsName` 所选的文件系统必须是所有写入节点都能访问的共享存储。

## cacheEnabled

是否在 Catalog 实例内缓存表元数据对象。批处理作业的 Catalog 生命周期与作业一致，单次同步可以关闭以节省内存；常驻服务建议保持开启。
