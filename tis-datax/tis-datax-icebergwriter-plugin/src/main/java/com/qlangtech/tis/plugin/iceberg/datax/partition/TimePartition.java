package com.qlangtech.tis.plugin.iceberg.datax.partition;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergSelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 时间分区策略（年 / 月 / 日 / 小时）
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 * @see IcebergSelectedTab
 */
public class TimePartition extends IcebergPartition {

    public static final String KEY_TIME_COLUMN = "timeColumn";
    public static final String KEY_GRANULARITY = "granularity";

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require, Validator.db_col_name})
    public String timeColumn;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Granularity granularity = Granularity.day;

    @Override
    public PartitionSpec createPartitionSpec(Schema schema) {
        if (schema.findField(this.timeColumn) == null) {
            throw new IllegalArgumentException("time column '" + this.timeColumn + "' is not exist in schema");
        }
        Objects.requireNonNull(this.granularity, "granularity can not be null");
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        switch (this.granularity) {
            case year -> builder.year(this.timeColumn);
            case month -> builder.month(this.timeColumn);
            case day -> builder.day(this.timeColumn);
            case hour -> builder.hour(this.timeColumn);
        }
        return builder.build();
    }

    @Override
    public List<String> getIncrPartitionKeys() {
        // 编码为 "{transform}:{column}"，与 sink 端 IcebergMetadataApplier#applyPartition 一一对应
        return Collections.singletonList(Objects.requireNonNull(this.granularity, "granularity can not be null")
                + ":" + this.timeColumn);
    }

    /**
     * 时间分区粒度。常量名必须与 Iceberg 的 transform 名逐字一致（{@code year}/{@code month}/{@code day}/
     * {@code hour}）：既用于 {@link PartitionSpec.Builder} 的调用分派，也用于增量侧下发的
     * {@code {transform}:{column}} 编码（sink 端按 transform 名解析）。
     * <p>
     * 小写常量名同时也让存量落盘数据（{@code <granularity>day</granularity>}）继续读得出来 ——
     * XStream 与表单选项都按常量名持久化。
     */
    public enum Granularity implements DescriptorUseableShortComment {
        year("按年分区"),
        month("按月分区"),
        day("按日分区"),
        hour("按小时分区");

        private final String comment;

        Granularity(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return this.comment;
        }
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IcebergPartition> {
        @Override
        public boolean secondVerify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals,
                                    PostFormVals parentPostFormVals) {

            IcebergSelectedTab tab = parentPostFormVals.newInstance();
            if (tab.partitionSpec instanceof TimePartition timePartition) {
//                // 分区列不存在要到建表阶段才炸，前置到表单校验可以省掉一次无用的 dump 调度
//                List<String> colKeys = tab.getColKeys();
//                if (!colKeys.isEmpty() && !colKeys.contains(timePartition.timeColumn)) {
//                    msgHandler.addFieldError(context, KEY_TIME_COLUMN,
//                            "分区字段 '" + timePartition.timeColumn + "' 必须是本次同步的列");
//                    return false;
//                }
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "Time Partition";
        }
    }
}
