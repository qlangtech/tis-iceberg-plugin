package com.qlangtech.tis.plugin.iceberg.format;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IEnumTypeSer;
import com.qlangtech.tis.plugin.MemorySize;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.TableProperties;

import java.util.Map;

/**
 * ORC 文件格式。
 * <p>
 * 参数与 {@link TableProperties} 中以 {@code ORC_} 开头的 key 一一对应，
 * 下发的属性名为 {@code write.orc.*}。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class OrcFileFormat extends IcebergFileFormat {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public OrcCompressionCodec compressionCodec = OrcCompressionCodec.ZLIB;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public OrcCompressionStrategy compressionStrategy = OrcCompressionStrategy.SPEED;

    @FormField(ordinal = 2, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize stripeSize = MemorySize.ofMebiBytes(64);

    @FormField(ordinal = 3, type = FormFieldType.MEMORY_SIZE_OF_MEGA, advance = true, validate = {Validator.require})
    public MemorySize blockSize = MemorySize.VALUE_256_MB;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer vectorizedBatchSize = 1024;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, advance = true)
    public String bloomFilterColumns;

    @FormField(ordinal = 6, type = FormFieldType.DECIMAL_NUMBER, advance = true, validate = {Validator.require})
    public Double bloomFilterFpp = 0.05;

    @Override
    public org.apache.iceberg.FileFormat getFormat() {
        return org.apache.iceberg.FileFormat.ORC;
    }

    @Override
    protected void appendTableProperties(Map<String, String> tableProps) {
        if (this.compressionCodec != null) {
            tableProps.put(TableProperties.ORC_COMPRESSION, this.compressionCodec.valueToken());
        }
        if (this.compressionStrategy != null) {
            tableProps.put(TableProperties.ORC_COMPRESSION_STRATEGY, this.compressionStrategy.valueToken());
        }
        if (this.stripeSize != null) {
            tableProps.put(TableProperties.ORC_STRIPE_SIZE_BYTES, String.valueOf(this.stripeSize.getBytes()));
        }
        if (this.blockSize != null) {
            tableProps.put(TableProperties.ORC_BLOCK_SIZE_BYTES, String.valueOf(this.blockSize.getBytes()));
        }
        if (this.vectorizedBatchSize != null) {
            tableProps.put(TableProperties.ORC_WRITE_BATCH_SIZE, String.valueOf(this.vectorizedBatchSize));
        }
        // 不配置布隆过滤列时不下发该属性，交给 Iceberg 默认值（空串=不过滤）
        if (StringUtils.isNotBlank(this.bloomFilterColumns)) {
            tableProps.put(TableProperties.ORC_BLOOM_FILTER_COLUMNS, this.bloomFilterColumns.trim());
        }
        if (this.bloomFilterFpp != null) {
            tableProps.put(TableProperties.ORC_BLOOM_FILTER_FPP, String.valueOf(this.bloomFilterFpp));
        }
    }

    /**
     * ORC 压缩编码。{@code valueToken()} 直接取 ORC 的 {@code CompressionKind} 合法取值。
     */
    public enum OrcCompressionCodec implements IEnumTypeSer, DescriptorUseableShortComment {
        NONE("none", "不压缩"),
        ZLIB("zlib", "压缩比高"),
        SNAPPY("snappy", "速度快"),
        LZ4("lz4", "极快解压"),
        ZSTD("zstd", "综合最优");

        private final String token;
        private final String comment;

        OrcCompressionCodec(String token, String comment) {
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
     * ORC 压缩策略，对应 {@code OrcFile.CompressionStrategy}
     */
    public enum OrcCompressionStrategy implements IEnumTypeSer, DescriptorUseableShortComment {
        SPEED("speed", "速度优先"),
        COMPRESSION("compression", "压缩比优先");

        private final String token;
        private final String comment;

        OrcCompressionStrategy(String token, String comment) {
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
            return "ORC";
        }

        @Override
        public String shortComment() {
            return "列式存储对接ORC生态";
        }

        /**
         * ORC 的 fpp 必须是 (0,1) 之间的概率值，否则写入期 {@code Preconditions} 会直接抛异常
         */
        public boolean validateBloomFilterFpp(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                              String value) {
            double fpp = Double.parseDouble(value);
            if (fpp <= 0 || fpp >= 1) {
                msgHandler.addFieldError(context, fieldName, "布隆过滤误判率必须在 0 和 1 之间（不含端点）");
                return false;
            }
            return true;
        }
    }
}
