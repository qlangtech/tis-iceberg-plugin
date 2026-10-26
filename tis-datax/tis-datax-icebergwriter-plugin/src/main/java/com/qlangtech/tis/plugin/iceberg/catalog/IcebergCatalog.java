package com.qlangtech.tis.plugin.iceberg.catalog;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IRepositoryResourceScannable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergTablePropsSetter;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Iceberg Catalog 抽象基类。
 * <p>
 * Iceberg 1.12 已移除 Schema.Builder，表级配置统一通过
 * {@link Catalog.TableBuilder#withProperties} 下发，因此本类只负责提供
 * {@link Catalog} 实例，属性追加交给 {@link IcebergTablePropsSetter} 的各个实现。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public abstract class IcebergCatalog implements Describable<IcebergCatalog>, IRepositoryResourceScannable,
        IEndTypeGetter, IcebergTablePropsSetter {

    public static final String KEY_FIELD_FS_NAME = ITISFileSystemFactory.KEY_FIELD_NAME_FS_NAME;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.db_col_name})
    public String catalogName;

    /**
     * 存储所用的文件系统实例（HDFS / S3 / MinIO …）。
     * <p>
     * 数据目录与 Hadoop 连接参数都由该实例给出，不在本插件里重复配置
     */
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;

    private transient FileSystemFactory fileSystem;

    public FileSystemFactory getFs() {
        if (this.fileSystem == null) {
            if (StringUtils.isEmpty(this.fsName)) {
                throw new IllegalStateException("prop " + KEY_FIELD_FS_NAME + " can not be empty");
            }
            this.fileSystem = FileSystemFactory.getFsFactory(this.fsName);
        }
        return Objects.requireNonNull(this.fileSystem,
                "fileSystem has not be initialized by " + KEY_FIELD_FS_NAME + ":" + this.fsName);
    }

    /**
     * 数据仓库根路径。
     * <p>
     * 取文件系统实例的绝对根目录（与 TIS 各 writer 定位 dump 目录的方式一致），而不是
     * {@link ITISFileSystemFactory#getFSAddress()}：后者在 HDFS 上只是 namenode 地址（不含 rootDir），
     * 只有 S3 才拼上 rootDir
     */
    public String getWarehouse() {
        return this.getFs().getFileSystem().getRootDir().toString();
    }

    /**
     * Iceberg 各 Catalog 需要的 Hadoop 连接参数，直接取自文件系统实例
     */
    public org.apache.hadoop.conf.Configuration getHadoopConf() {
        return this.getFs().getConfiguration();
    }

    /**
     * 取得 Iceberg Catalog 实例
     */
    public abstract Catalog getCatalog();

    /**
     * 取得构造本 Catalog 所需的属性（{@code type} / {@code warehouse} / {@code uri} …）。
     * <p>
     * 增量写入时这些属性要下发给执行节点，由执行节点自己建 Catalog（那里没有插件实例），
     * 因此不能只提供 {@link #getCatalog()} 返回的成品对象。
     * 返回值不含 Hadoop 连接参数：执行期由 {@link TISIcebergCatalog} 反查本插件补上。
     */
    public abstract Map<String, String> createCatalogOptions();

    /**
     * 目标数据库（Iceberg namespace）。各 Catalog 的取值来源不同：
     * Hadoop Catalog 取配置里的库名，Hive Metastore Catalog 取 hiveserver2 数据源的库名
     */
    public abstract Optional<String> getDBName();

    /**
     * 取得目标库名，未配置时抛异常
     */
    public String getDbNameCanNotBeNull() {
        return this.getDBName().orElseThrow(
                () -> new IllegalStateException("catalog:" + this.catalogName + " relevant databaseName can not be null"));
    }

    /**
     * 将 TIS 的库名（可能是 "db.tab" 形式）转换成本 Catalog 的命名空间
     */
    public Namespace createNamespace() {
        return Namespace.of(getDbNameCanNotBeNull());
    }

    public TableIdentifier createTableIdentifier(String tableName) {
        return TableIdentifier.of(createNamespace(), tableName);
    }

    /**
     * 目标库不存在则创建
     */
    public void createNamespaceIfNotExist(Catalog catalog) {
        if (!(catalog instanceof SupportsNamespaces namespaces)) {
            return;
        }
        Namespace ns = createNamespace();
        if (!namespaces.namespaceExists(ns)) {
            namespaces.createNamespace(ns);
        }
    }

    /**
     * 开始扫描依赖
     */
    @Override
    public void startScanDependency() {
        // 子类按需实现，例如 Hive Metastore 需要扫描 hiveserver2 数据源
    }

    @Override
    public final EndType getEndType() {
        return EndType.Iceberg;
    }

    public abstract static class BasicDescriptor extends Descriptor<IcebergCatalog> {

        public BasicDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_FS_NAME,
                    () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        @Override
        public String getDisplayName() {
            return this.getClass().getSimpleName();
        }
    }
}
