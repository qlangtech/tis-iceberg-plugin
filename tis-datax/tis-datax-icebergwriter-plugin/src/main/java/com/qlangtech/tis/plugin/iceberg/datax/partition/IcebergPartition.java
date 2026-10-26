package com.qlangtech.tis.plugin.iceberg.datax.partition;

import com.qlangtech.tis.extension.Describable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;

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
     * 以 {@code {transform}:{column}} 形式表达的分区项，供增量侧（flink-cdc pipeline sink）使用。
     * <p>
     * 增量写入时 Iceberg 表由执行节点上的 sink 创建，插件实例不在场，
     * 无法调用 {@link #createPartitionSpec(Schema)} 传 {@link PartitionSpec} 对象，
     * 只能传这种可以序列化的字符串编码（sink 端由 IcebergMetadataApplier 解码）。
     */
    public abstract List<String> getIncrPartitionKeys();

    /**
     * 是否为分区表
     */
    public abstract boolean isPartitioned();
}
