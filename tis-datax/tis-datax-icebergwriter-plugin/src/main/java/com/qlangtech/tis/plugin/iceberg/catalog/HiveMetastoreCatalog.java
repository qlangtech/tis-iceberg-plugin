package com.qlangtech.tis.plugin.iceberg.catalog;

import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Hive Metastore Catalog 实现。
 * <p>
 * Metastore 地址取自 Hiveserver2 数据源配置（{@link IHiveConnGetter#getMetaStoreUrls()}），
 * 与 Paimon 的 HiveCatalog 保持一致的取数方式；数据文件存放位置与 Hadoop 连接参数则来自所选的
 * {@link com.qlangtech.tis.offline.FileSystemFactory}。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class HiveMetastoreCatalog extends IcebergCatalog {

    public static final String KEY_DB_NAME = "dbName";

    /**
     * Hiveserver2 数据源实例名，选项由 HiveMetastoreCatalog.json 中的 groovy 脚本动态给出
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 13, advance = true)
    public String clientPoolSize = "5";

    private transient HiveCatalog catalog;

    /**
     * 覆盖基类：目标库取 Hiveserver2 数据源中配置的库名
     */
    @Override
    public Optional<String> getDBName() {
        return Optional.of(getHiveConnGetter().getDbName());
    }

    public IHiveConnGetter getHiveConnGetter() {
        if (StringUtils.isBlank(this.dbName)) {
            throw new IllegalStateException("prop " + KEY_DB_NAME + " can not be null");
        }
        return (IHiveConnGetter) BasicDataSourceFactory.getDs(this.dbName);
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

    private HiveCatalog createCatalog() {
        Configuration conf = this.getHadoopConf();

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(conf);
        hiveCatalog.initialize(this.catalogName, this.createCatalogOptions());
        return hiveCatalog;
    }

    @Override
    public Map<String, String> createCatalogOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.getWarehouse());
        properties.put(CatalogProperties.CLIENT_POOL_SIZE, this.clientPoolSize);
        String metastoreUris = getHiveConnGetter().getMetaStoreUrls();
        if (StringUtils.isNotBlank(metastoreUris)) {
            properties.put(CatalogProperties.URI, metastoreUris);
        }
        return properties;
    }

    @Override
    public void startScanDependency() {
        // 记录对 hiveserver2 数据源与文件系统实例的依赖，分布式执行时需要一并下发
        this.getHiveConnGetter();
        this.getFs();
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDescriptor {

        @Override
        public String getDisplayName() {
            return "Hive Metastore Catalog";
        }
    }
}
