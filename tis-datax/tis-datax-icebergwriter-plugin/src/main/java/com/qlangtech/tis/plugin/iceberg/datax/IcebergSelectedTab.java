package com.qlangtech.tis.plugin.iceberg.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.partition.IcebergPartition;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Iceberg 写入端表设置。
 * <p>
 * 分区策略是逐表设置的：同一批同步里不同表的时间列、粒度都可能不同，放在 writer 上只能全批共用一份。
 * 建表时由 {@code DataxIcebergWriter#createTable} 从这里取分区规格。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/26
 */
public class IcebergSelectedTab extends SelectedTab {

    public static final String KEY_PARTITION_SPEC = "partitionSpec";

    @FormField(ordinal = 4, validate = {Validator.require})
    public IcebergPartition partitionSpec;

    /**
     * 依据建表 schema 生成分区规格。
     * <p>
     * 分区规格只能在 schema 确定之后构建（{@link PartitionSpec#builderFor(Schema)}）。
     */
    public PartitionSpec createPartitionSpec(Schema schema) {
        return Objects.requireNonNull(this.partitionSpec, "partitionSpec can not be null").createPartitionSpec(schema);
    }

    public static List<Option> getPtCandidateDateFields() {
        return SelectedTab.getContextOpts((cols) -> cols.stream().filter((col) -> {
            switch (col.getType().getCollapse()) {
                // case STRING:
                case INT:
                case Long:
                case Date:
                    return true;
            }
            return false;
        }));
    }
    /**
     * 增量侧的分区项（{@code {transform}:{column}} 形式），交给 flink-cdc 的 sink 执行端建表
     */
    @Override
    public Optional<List<String>> getPartitionKeys() {
        if (this.partitionSpec == null) {
            return Optional.empty();
        }
        List<String> keys = this.partitionSpec.getIncrPartitionKeys();
        return keys.isEmpty() ? Optional.empty() : Optional.of(keys);
    }

    @TISExtension
    public static class DefaultDescriptor extends SelectedTab.DefaultDescriptor {

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, SelectedTab postFormVals) {
            if (!super.validateAll(msgHandler, context, postFormVals)) {
                return false;
            }
//            IcebergSelectedTab tab = (IcebergSelectedTab) postFormVals;
//            if (tab.partitionSpec instanceof TimePartition timePartition) {
//                // 分区列不存在要到建表阶段才炸，前置到表单校验可以省掉一次无用的 dump 调度
//                List<String> colKeys = tab.getColKeys();
//                if (!colKeys.isEmpty() && !colKeys.contains(timePartition.timeColumn)) {
//                    msgHandler.addFieldError(context, KEY_PARTITION_SPEC,
//                            "分区字段 '" + timePartition.timeColumn + "' 必须是本次同步的列");
//                    return false;
//                }
//            }
            return true;
        }
    }
}
