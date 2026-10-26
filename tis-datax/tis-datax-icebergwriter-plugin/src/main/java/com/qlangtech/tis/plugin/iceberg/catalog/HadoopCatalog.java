package com.qlangtech.tis.plugin.iceberg.catalog;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Hadoop 文件系统 Catalog 实现。
 * <p>
 * 存储位置与连接参数全部来自所选的 {@link com.qlangtech.tis.offline.FileSystemFactory}，
 * warehouse 即该文件系统的根路径，因此 HDFS 与 S3/MinIO 走的是同一份配置。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class HadoopCatalog extends IcebergCatalog {

    /**
     * 目标数据库（Iceberg namespace，在 warehouse 下以 {db}.db 目录存在）
     */
    @FormField(ordinal = 3, validate = {Validator.require, Validator.db_col_name})
    public String databaseName;

    @FormField(ordinal = 12, advance = true, type = FormFieldType.ENUM)
    public Boolean cacheEnabled = true;

    private transient org.apache.iceberg.hadoop.HadoopCatalog catalog;

    @Override
    public Optional<String> getDBName() {
        return Optional.ofNullable(this.databaseName);
    }

    @Override
    public Catalog getCatalog() {
        if (catalog == null) {
            synchronized (this) {
                if (catalog == null) {
                    catalog = createCatalog();
                }
            }
        }
        return catalog;
    }

    private org.apache.iceberg.hadoop.HadoopCatalog createCatalog() {
        Configuration conf = this.getHadoopConf();
        String warehouse = this.getWarehouse();

        org.apache.iceberg.hadoop.HadoopCatalog hadoopCatalog =
                new org.apache.iceberg.hadoop.HadoopCatalog(conf, warehouse);
        hadoopCatalog.initialize(this.catalogName, this.createCatalogOptions());
        return hadoopCatalog;
    }

    @Override
    public Map<String, String> createCatalogOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.getWarehouse());
        if (Boolean.FALSE.equals(this.cacheEnabled)) {
            properties.put(CatalogProperties.CACHE_ENABLED, "false");
        }
        return properties;
    }

    @Override
    public void startScanDependency() {
        // 记录对文件系统实例的依赖，分布式执行时需要一并下发
        this.getFs();
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDescriptor {

        @Override
        public String getDisplayName() {
            return "Hadoop Catalog";
        }
    }
}
