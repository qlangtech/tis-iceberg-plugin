package com.qlangtech.tis.plugin.iceberg.format;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IEnumTypeSer;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.iceberg.TableProperties;

import java.util.Map;

/**
 * Parquet 文件格式。
 * <p>
 * 参数与 {@link TableProperties} 中以 {@code PARQUET_} 开头的 key 一一对应，
 * 下发的属性名为 {@code write.parquet.*}。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class ParquetFileFormat extends IcebergFileFormat {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public ParquetCompressionCodec compressionCodec = ParquetCompressionCodec.ZSTD;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.integer})
    public Integer compressionLevel;

    @FormField(ordinal = 2, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize rowGroupSize = MemorySize.VALUE_128_MB;

    @FormField(ordinal = 3, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize pageSize = MemorySize.ofMebiBytes(1);

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer pageRowLimit = 20000;

    @FormField(ordinal = 5, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize dictSize = MemorySize.ofMebiBytes(2);

    @FormField(ordinal = 6, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public ParquetPageVersion pageVersion = ParquetPageVersion.V1;

    @FormField(ordinal = 7, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize bloomFilterMaxBytes = MemorySize.ofMebiBytes(1);

    @FormField(ordinal = 8, type = FormFieldType.ENUM, advance = true)
    public Boolean bloomFilterAdaptive = false;

    @Override
    public org.apache.iceberg.FileFormat getFormat() {
        return org.apache.iceberg.FileFormat.PARQUET;
    }

    @Override
    protected void appendTableProperties(Map<String, String> tableProps) {
        if (this.compressionCodec != null) {
            tableProps.put(TableProperties.PARQUET_COMPRESSION, this.compressionCodec.valueToken());
        }
        // level 只有 gzip / zstd 等少数编码认可，不填则交给 Iceberg 用编码自身的默认值
        if (this.compressionLevel != null) {
            tableProps.put(TableProperties.PARQUET_COMPRESSION_LEVEL, String.valueOf(this.compressionLevel));
        }
        if (this.rowGroupSize != null) {
            tableProps.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(this.rowGroupSize.getBytes()));
        }
        if (this.pageSize != null) {
            tableProps.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(this.pageSize.getBytes()));
        }
        if (this.pageRowLimit != null) {
            tableProps.put(TableProperties.PARQUET_PAGE_ROW_LIMIT, String.valueOf(this.pageRowLimit));
        }
        if (this.dictSize != null) {
            tableProps.put(TableProperties.PARQUET_DICT_SIZE_BYTES, String.valueOf(this.dictSize.getBytes()));
        }
        if (this.pageVersion != null) {
            tableProps.put(TableProperties.PARQUET_PAGE_VERSION, this.pageVersion.valueToken());
        }
        if (this.bloomFilterMaxBytes != null) {
            tableProps.put(TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES,
                    String.valueOf(this.bloomFilterMaxBytes.getBytes()));
        }
        if (this.bloomFilterAdaptive != null) {
            tableProps.put(TableProperties.PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED,
                    String.valueOf(this.bloomFilterAdaptive));
        }
    }

    /**
     * Parquet 压缩编码。{@code valueToken()} 直接取 Parquet 的 {@code CompressionCodecName} 合法取值，
     * 因此不需要再做任何字符串归一化。
     * <p>
     * 刻意不含 LZO / BROTLI：两者依赖本地库，开箱环境不可用。
     */
    public enum ParquetCompressionCodec implements IEnumTypeSer, DescriptorUseableShortComment {
        UNCOMPRESSED("uncompressed", "不压缩"),
        SNAPPY("snappy", "速度快"),
        GZIP("gzip", "压缩比高"),
        LZ4_RAW("lz4_raw", "极快解压"),
        ZSTD("zstd", "综合最优");

        private final String token;
        private final String comment;

        ParquetCompressionCodec(String token, String comment) {
            this.token = token;
            this.comment = comment;
        }

        @Override
        public String valueToken() {
            return token;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    /**
     * Parquet 页版本，取值对应 {@code ParquetProperties.WriterVersion} 的 {@code fromString} 入参
     */
    public enum ParquetPageVersion implements IEnumTypeSer, DescriptorUseableShortComment {
        V1("v1", "1.0版本"),
        V2("v2", "2.0版本");

        private final String token;
        private final String comment;

        ParquetPageVersion(String token, String comment) {
            this.token = token;
            this.comment = comment;
        }

        @Override
        public String valueToken() {
            return token;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Parquet";
        }

        @Override
        public String shortComment() {
            return "列式存储压缩比高";
        }
    }
}
