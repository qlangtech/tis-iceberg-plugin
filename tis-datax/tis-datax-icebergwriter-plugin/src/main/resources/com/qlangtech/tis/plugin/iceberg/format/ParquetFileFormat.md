## compressionCodec

Parquet 数据文件的压缩编码，下发为表属性 `write.parquet.compression-codec`。

- **UNCOMPRESSED**：不压缩，CPU 开销最小但存储占用最大
- **SNAPPY**：压缩速度快，压缩比中等
- **GZIP**：压缩比高，适合存储归档数据
- **LZ4_RAW**：解压极快，纯 Java 实现（不依赖 Hadoop 本地库）
- **ZSTD**：压缩比与速度兼顾，Iceberg 1.4 起的默认值

LZO / Brotli 依赖本地库，为保证开箱可用未纳入选项。

## compressionLevel

压缩级别，下发为 `write.parquet.compression-level`。

只对 GZIP / ZSTD 编码生效，其他编码会忽略该值。留空则使用编码自身的默认级别。

## rowGroupSize

单个 row group 的字节数，下发为 `write.parquet.row-group-size-bytes`（界面按 MB 输入）。

Row group 是 Parquet 的最小随机读取单位：越大则顺序扫描吞吐越高、元数据越少；越小则点查与内存占用越友好。默认 128MB。

## pageSize

数据页大小，下发为 `write.parquet.page-size-bytes`（界面按 MB 输入）。

页是编码与压缩的基本单位，压缩器只在一个页内工作。页太小压缩比下降，页太大则读取时解压粒度太粗。默认 1MB。

## pageRowLimit

单个数据页的行数上限，下发为 `write.parquet.page-row-limit`。与 `pageSize` 共同决定页的切分点，取两者先到达的条件。默认 20000。

## dictSize

字典编码缓冲区大小，下发为 `write.parquet.dict-size-bytes`（界面按 MB 输入）。

字典编码对重复度高的列（枚举、ID、状态码）压缩效果显著；超过缓冲区则退化为普通编码。默认 2MB。

## pageVersion

Parquet 数据页格式版本，下发为 `write.parquet.page-version`。

- **V1**：兼容性最好，所有读取器都支持
- **V2**：支持更细粒度的嵌套列编码（如 list/map 内部列按页切分），但部分旧版 Parquet 读取器不认

默认 V1。

## bloomFilterMaxBytes

单个列布隆过滤器的最大字节数，下发为 `write.parquet.bloom-filter-max-bytes`（界面按 MB 输入）。

布隆过滤器能显著加速等值查询的裁剪。该参数本身不开启过滤——是否给某列建过滤由列级属性决定；设为 0 相当于全局关闭。默认 1MB。

## bloomFilterAdaptive

是否按实际基数自适应调整布隆过滤器大小，下发为 `write.parquet.bloom-filter-adaptive-enabled`。

开启后按每列的实际去重基数分配空间，避免统一大小导致小基数列浪费、大基数列误判率偏高。默认关闭。
