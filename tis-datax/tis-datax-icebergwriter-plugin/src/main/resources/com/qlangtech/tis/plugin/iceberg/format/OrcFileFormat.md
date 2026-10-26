## compressionCodec

ORC 数据文件的压缩编码，下发为表属性 `write.orc.compression-codec`。

- **NONE**：不压缩
- **ZLIB**：压缩比高，ORC 的默认值（ORC 没有 gzip，zlib 是等价的 deflate 实现）
- **SNAPPY**：压缩速度快
- **LZ4**：解压极快
- **ZSTD**：压缩比与速度兼顾

## compressionStrategy

压缩策略，下发为 `write.orc.compression-strategy`。

- **SPEED**：在压缩级别之间优先压缩速度
- **COMPRESSION**：在压缩级别之间优先压缩比

默认 SPEED。

## stripeSize

ORC stripe 大小，下发为 `write.orc.stripe-size-bytes`（界面按 MB 输入）。

stripe 是 ORC 并行读取与内存管理的基本单位：读取时整条 stripe 需要进入内存，因此 stripe 过大会拉高查询内存峰值。默认 64MB。

## blockSize

ORC block 大小，下发为 `write.orc.block-size-bytes`（界面按 MB 输入）。

block 是 stripe 内部的压缩/解压单位，必须显著小于 stripe。默认 256MB。

## vectorizedBatchSize

向量化读写的批行数，下发为 `write.orc.vectorized.batch-size`（属性名为 `write.orc.vectorized.batch-size`）。默认 1024。

## bloomFilterColumns

需要建立布隆过滤器的列名，多个用英文逗号分隔，下发为 `write.orc.bloom.filter.columns`。

列名必须真实存在于表 schema 中，否则写入期报错。留空则不建布隆过滤器。布隆过滤能显著加速等值查询的裁剪。

## bloomFilterFpp

布隆过滤器的正例误判概率，下发为 `write.orc.bloom.filter.fpp`。

必须严格在 0 和 1 之间（不含端点），否则写入期直接抛异常。值越小误判越少、占用空间越大。默认 0.05。
