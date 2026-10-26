## compressionCodec

Avro 数据文件的压缩编码，下发为表属性 `write.avro.compression-codec`。

- **UNCOMPRESSED**：不压缩
- **SNAPPY**：压缩速度快
- **GZIP**：压缩比高，Avro 的默认值
- **ZSTD**：压缩比与速度兼顾

Avro 不支持 LZ4，故不在选项中。

## compressionLevel

压缩级别，下发为 `write.avro.compression-level`。

只对 GZIP / ZSTD 编码生效，其他编码会忽略该值。留空则使用编码自身的默认级别（GZIP 为 9，ZSTD 为 1）。
