package com.qlangtech.tis.plugin.iceberg.catalog;

import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 供 flink-cdc Iceberg 连接器在执行节点（TaskManager）自建 Catalog 用。
 * <p>
 * flink-cdc 的 {@code IcebergWriter} / {@code IcebergCommitter} / {@code IcebergMetadataApplier}
 * 都在执行节点上调用 {@code CatalogUtil.buildIcebergCatalog(name, catalogOptions, new Configuration())}
 * 现建 Catalog，那里没有 TIS 的插件实例，拿不到 {@code FileSystemFactory} 对应的 Hadoop 连接参数
 * （HDFS 的 fs.defaultFS、S3/MinIO 的 fs.s3a.* 等，全在文件系统实例里）。用默认 Configuration
 * 建出来的 HadoopCatalog / HiveCatalog 在 HDFS 或对象存储上直接不可用。
 * <p>
 * 与 Paimon 侧的 {@code TISPaimonCatalogFactory} 同一思路：把 {@code dataxName} 随 catalog 属性下发，
 * 在这里反查回 {@link DataxIcebergWriter}，复用它的 {@code catalog.getHadoopConf()} 之后再交给
 * 原生 {@link CatalogUtil#buildIcebergCatalog}。本类只做「换 hadoop 配置」这一件事，
 * 具体用哪种 Catalog（Hadoop / Hive Metastore）仍由 {@code catalog.properties.type} 决定，
 * 与原生行为一致。
 * <p>
 * 类名通过 {@link CatalogProperties#CATALOG_IMPL} 下发，由 Iceberg 的 {@code DynConstructors}
 * 用线程上下文 ClassLoader 反射实例化；执行期 TCCL 就是插件自己的 PluginFirstClassLoader，
 * 因此本类必须与插件一起打包（就在 tis-datax-icebergwriter-plugin 里）。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class TISIcebergCatalog implements Catalog, Configurable {

    /**
     * 通过 {@link CatalogProperties#CATALOG_IMPL} 下发本类全名
     */
    public static final String CATALOG_IMPL = TISIcebergCatalog.class.getName();

    /**
     * 与原生实现一致：每次都取最新元数据，避免 schema 变更后读到旧表
     */
    private static final Map<String, String> FIXED_CATALOG_OPTIONS =
            Map.of(CatalogProperties.CACHE_ENABLED, "false");

    /**
     * 构造 catalog 属性：在具体 Catalog 给出的属性上补上 {@code catalog-impl} 与 {@code dataxName}
     *
     * @param catalogOptions 由 {@link IcebergCatalog} 子类给出的目标端属性（type/warehouse/uri/...）
     * @param dataxName      当前增量任务的 DataX 名称，执行期据此反查 writer
     */
    public static Map<String, String> createCatalogOptions(Map<String, String> catalogOptions, String dataxName) {
        if (StringUtils.isEmpty(dataxName)) {
            throw new IllegalArgumentException("param dataxName can not be empty");
        }
        Map<String, String> opts = new HashMap<>(catalogOptions);
        opts.putAll(FIXED_CATALOG_OPTIONS);
        opts.put(CatalogProperties.CATALOG_IMPL, CATALOG_IMPL);
        opts.put(StoreResourceType.DATAX_NAME, dataxName);
        return opts;
    }

    /**
     * flink-cdc 侧传入的配置（即环境默认 Configuration），这里不直接使用：
     * 真正需要的连接参数在执行期从 writer 反查
     */
    private Configuration conf;

    private String name;

    private Map<String, String> properties;

    private transient volatile Catalog delegate;

    public TISIcebergCatalog() {
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    private Catalog getDelegate() {
        Catalog catalog = this.delegate;
        if (catalog == null) {
            synchronized (this) {
                catalog = this.delegate;
                if (catalog == null) {
                    catalog = this.delegate = buildDelegate();
                }
            }
        }
        return catalog;
    }

    private Catalog buildDelegate() {
        String dataXName = this.properties.get(StoreResourceType.DATAX_NAME);
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalStateException("prop " + StoreResourceType.DATAX_NAME + " relevant val can not be empty");
        }
        DataxIcebergWriter writer =
                (DataxIcebergWriter) DataxWriter.load(null, StoreResourceType.DataApp, dataXName, true);

        // 去掉本类专用的两项，避免 CatalogUtil 报「type 与 catalog-impl 不能同时设置」
        Map<String, String> icebergOpts = new HashMap<>(this.properties);
        icebergOpts.remove(CatalogProperties.CATALOG_IMPL);
        icebergOpts.remove(StoreResourceType.DATAX_NAME);

        return CatalogUtil.buildIcebergCatalog(this.name, icebergOpts, writer.catalog.getHadoopConf());
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public List<TableIdentifier> listTables(org.apache.iceberg.catalog.Namespace namespace) {
        return getDelegate().listTables(namespace);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
        return getDelegate().loadTable(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return getDelegate().dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        getDelegate().renameTable(from, to);
    }

    /**
     * 必须显式委托：{@code Catalog#createTable} 的 default 实现走 {@code buildTable}，
     * 而 {@code buildTable} 的 default 实现直接抛 UnsupportedOperationException
     */
    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                             String location, Map<String, String> properties) {
        return getDelegate().createTable(identifier, schema, spec, location, properties);
    }

    @Override
    public void invalidateTable(TableIdentifier identifier) {
        getDelegate().invalidateTable(identifier);
    }

    @Override
    public String toString() {
        return "TISIcebergCatalog{" + this.name + "}";
    }
}
