package com.qlangtech.tis.plugin.iceberg.datax.partition;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergSelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;

/**
 * 时间分区策略（年 / 月 / 日 / 小时）
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class TimePartition extends IcebergPartition {

    public static final String KEY_TIME_COLUMN = "timeColumn";
    public static final String KEY_GRANULARITY = "granularity";

    @FormField(ordinal = 1, validate = {Validator.require, Validator.db_col_name})
    public String timeColumn;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String granularity;

    @Override
    public PartitionSpec createPartitionSpec(Schema schema) {
        if (schema.findField(this.timeColumn) == null) {
            throw new IllegalArgumentException("time column '" + this.timeColumn + "' is not exist in schema");
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        switch (this.granularity) {
            case "year" -> builder.year(this.timeColumn);
            case "month" -> builder.month(this.timeColumn);
            case "day" -> builder.day(this.timeColumn);
            case "hour" -> builder.hour(this.timeColumn);
            default -> throw new IllegalArgumentException("illegal granularity:" + this.granularity);
        }
        return builder.build();
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
                // 分区列不存在要到建表阶段才炸，前置到表单校验可以省掉一次无用的 dump 调度
                List<String> colKeys = tab.getColKeys();
                if (!colKeys.isEmpty() && !colKeys.contains(timePartition.timeColumn)) {
                    msgHandler.addFieldError(context, KEY_TIME_COLUMN,
                            "分区字段 '" + timePartition.timeColumn + "' 必须是本次同步的列");
                    return false;
                }
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "Time Partition";
        }
    }
}
