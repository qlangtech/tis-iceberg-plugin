package com.qlangtech.tis.plugin.iceberg.datax.partition;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.Collections;
import java.util.List;

/**
 * 无分区策略
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class NoPartition extends IcebergPartition {

    @Override
    public PartitionSpec createPartitionSpec(Schema schema) {
        return PartitionSpec.unpartitioned();
    }

    @Override
    public List<String> getIncrPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IcebergPartition> {

        @Override
        public String getDisplayName() {
            return "No Partition";
        }
    }
}
