package com.qlangtech.tis.plugin.iceberg.format;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergTablePropsSetter;
import org.apache.iceberg.TableProperties;

import java.util.Map;

/**
 * Iceberg 数据文件格式抽象基类。
 * <p>
 * 每种格式的可写参数差异很大（Parquet 有 row-group / page / bloom filter，ORC 有 stripe / block /
 * 压缩策略，Avro 只有压缩），因此不再用「一个 fileFormat 枚举 + 一堆公共字段」的方式，
 * 而是把 {@code compressionCodec} 与各自专属参数都下沉到子类。
 * <p>
 * 子类把自己的配置写进 Iceberg 表属性，建表时随 {@code Catalog#createTable} 落盘；
 * Iceberg 的 {@code RegistryBasedFileWriterFactory} 会把建表属性与写入属性合并后交给
 * 各格式的 Writer 上下文，因此这些值在写入期是真正生效的。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 * @see <a href="file:///opt/misc/iceberg/api/src/main/java/org/apache/iceberg/FileFormat.java">org.apache.iceberg.FileFormat</a>
 */
public abstract class IcebergFileFormat implements Describable<IcebergFileFormat>, IcebergTablePropsSetter {

    /**
     * 对应的 Iceberg 文件格式。
     * <p>
     * 方法名刻意不叫 {@code getFileFormat()}，避免与本插件的字段名混淆；
     * 本类也不能直接命名为 {@code FileFormat}，否则与 {@link org.apache.iceberg.FileFormat} 撞名。
     */
    public abstract org.apache.iceberg.FileFormat getFormat();

    /**
     * 先写入 {@code write.format.default}，再由子类追加各自的格式属性
     */
    @Override
    public final void setTableProperties(Map<String, String> tableProps) {
        tableProps.put(TableProperties.DEFAULT_FILE_FORMAT, getFormat().name());
        this.appendTableProperties(tableProps);
    }

    /**
     * 追加本格式专属的表属性
     */
    protected void appendTableProperties(Map<String, String> tableProps) {
        // 默认无格式专属属性
    }

    public abstract static class BasicDescriptor extends Descriptor<IcebergFileFormat> {
    }
}
