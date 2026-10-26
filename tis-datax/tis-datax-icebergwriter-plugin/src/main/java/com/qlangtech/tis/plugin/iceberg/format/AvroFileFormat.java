package com.qlangtech.tis.plugin.iceberg.format;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IEnumTypeSer;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.iceberg.TableProperties;

import java.util.Map;

/**
 * Avro 文件格式。
 * <p>
 * 参数与 {@link TableProperties} 中以 {@code AVRO_} 开头的 key 一一对应，
 * 下发的属性名为 {@code write.avro.*}。Avro 侧可调项最少，只有压缩编码与压缩级别。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class AvroFileFormat extends IcebergFileFormat {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public AvroCompressionCodec compressionCodec = AvroCompressionCodec.GZIP;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.integer})
    public Integer compressionLevel;

    @Override
    public org.apache.iceberg.FileFormat getFormat() {
        return org.apache.iceberg.FileFormat.AVRO;
    }

    @Override
    protected void appendTableProperties(Map<String, String> tableProps) {
        if (this.compressionCodec != null) {
            tableProps.put(TableProperties.AVRO_COMPRESSION, this.compressionCodec.valueToken());
        }
        // level 只对 gzip / zstd 有效，不填则用编码自身默认值
        if (this.compressionLevel != null) {
            tableProps.put(TableProperties.AVRO_COMPRESSION_LEVEL, String.valueOf(this.compressionLevel));
        }
    }

    /**
     * Avro 压缩编码，对应 {@code CodecFactory}。不含 lz4：Avro 不支持。
     */
    public enum AvroCompressionCodec implements IEnumTypeSer, DescriptorUseableShortComment {
        UNCOMPRESSED("uncompressed", "不压缩"),
        SNAPPY("snappy", "速度快"),
        GZIP("gzip", "压缩比高"),
        ZSTD("zstd", "综合最优");

        private final String token;
        private final String comment;

        AvroCompressionCodec(String token, String comment) {
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
            return "Avro";
        }

        @Override
        public String shortComment() {
            return "行式存储通用格式";
        }
    }
}
