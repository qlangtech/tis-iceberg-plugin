package com.qlangtech.tis.plugin.iceberg.datax.partition;

import com.qlangtech.tis.extension.Describable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/**
 * Iceberg 分区策略抽象基类。
 * <p>
 * 分区规格只能在 schema 确定之后才能构建（{@link PartitionSpec#builderFor(Schema)}），
 * 因此这里是「传入 schema、产出 spec」的形式。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public abstract class IcebergPartition implements Describable<IcebergPartition> {

    /**
     * 创建分区规格
     */
    public abstract PartitionSpec createPartitionSpec(Schema schema);

    /**
     * 是否为分区表
     */
    public abstract boolean isPartitioned();
}
