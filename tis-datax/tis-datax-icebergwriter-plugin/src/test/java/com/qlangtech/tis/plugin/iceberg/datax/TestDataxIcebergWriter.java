package com.qlangtech.tis.plugin.iceberg.datax;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.iceberg.catalog.IcebergCatalog;
import com.qlangtech.tis.plugin.iceberg.catalog.TISIcebergCatalog;
import com.qlangtech.tis.plugin.iceberg.datax.partition.NoPartition;
import com.qlangtech.tis.plugin.iceberg.datax.partition.TimePartition;
import com.qlangtech.tis.plugin.iceberg.format.AvroFileFormat;
import com.qlangtech.tis.plugin.iceberg.format.IcebergFileFormat;
import com.qlangtech.tis.plugin.iceberg.format.OrcFileFormat;
import com.qlangtech.tis.plugin.iceberg.format.ParquetFileFormat;
import com.qlangtech.tis.plugin.iceberg.utils.IcebergUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * 只覆盖不依赖 TIS 运行时的纯逻辑：类型映射、schema 构建、表属性归一化、分区 spec
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class TestDataxIcebergWriter {

    @Test
    public void testCreateSchemaFromCols() {
        List<IcebergColumn> cols = Lists.newArrayList(
                new IcebergColumn(CMeta.create("id", JDBCTypes.BIGINT)),
                new IcebergColumn(CMeta.create("name", JDBCTypes.VARCHAR)),
                new IcebergColumn(CMeta.create("create_time", JDBCTypes.TIMESTAMP)));

        Schema schema = IcebergUtils.createSchema(cols);
        Assert.assertEquals(3, schema.columns().size());
        Assert.assertEquals(Types.LongType.get(), schema.findField("id").type());
        Assert.assertEquals(Types.StringType.get(), schema.findField("name").type());
        Assert.assertEquals(Types.TimestampType.withoutZone(), schema.findField("create_time").type());
        // field id 必须连续唯一，否则写入时 GenericRecord 对不上
        Assert.assertEquals(Lists.newArrayList(1, 2, 3),
                schema.columns().stream().map(Types.NestedField::fieldId).toList());
    }

    @Test
    public void testParquetTableProperties() {
        ParquetFileFormat format = new ParquetFileFormat();
        Map<String, String> props = new HashMap<>();
        format.setTableProperties(props);

        Assert.assertEquals("PARQUET", props.get(TableProperties.DEFAULT_FILE_FORMAT));
        Assert.assertEquals("zstd", props.get(TableProperties.PARQUET_COMPRESSION));
        // 默认值都以 Iceberg 自己的默认值为准，避免与 Iceberg 版本升级后的默认值漂移
        Assert.assertEquals("v1", props.get(TableProperties.PARQUET_PAGE_VERSION));
        Assert.assertEquals("134217728", props.get(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES));
        Assert.assertEquals("1048576", props.get(TableProperties.PARQUET_PAGE_SIZE_BYTES));
        Assert.assertEquals("20000", props.get(TableProperties.PARQUET_PAGE_ROW_LIMIT));
        Assert.assertEquals("2097152", props.get(TableProperties.PARQUET_DICT_SIZE_BYTES));
        Assert.assertEquals("1048576", props.get(TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES));
        Assert.assertEquals("false", props.get(TableProperties.PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED));
        // 未配置压缩级别时不下发该属性，使用编码自身的默认级别
        Assert.assertFalse(props.containsKey(TableProperties.PARQUET_COMPRESSION_LEVEL));

        // 改过编码后 token 直接就是 Parquet 认的名字，不需要额外归一化
        format.compressionCodec = ParquetFileFormat.ParquetCompressionCodec.LZ4_RAW;
        format.compressionLevel = 3;
        props.clear();
        format.setTableProperties(props);
        Assert.assertEquals("lz4_raw", props.get(TableProperties.PARQUET_COMPRESSION));
        Assert.assertEquals("3", props.get(TableProperties.PARQUET_COMPRESSION_LEVEL));
    }

    @Test
    public void testOrcTableProperties() {
        OrcFileFormat format = new OrcFileFormat();
        Map<String, String> props = new HashMap<>();
        format.setTableProperties(props);

        Assert.assertEquals("ORC", props.get(TableProperties.DEFAULT_FILE_FORMAT));
        Assert.assertEquals("zlib", props.get(TableProperties.ORC_COMPRESSION));
        Assert.assertEquals("speed", props.get(TableProperties.ORC_COMPRESSION_STRATEGY));
        Assert.assertEquals("67108864", props.get(TableProperties.ORC_STRIPE_SIZE_BYTES));
        Assert.assertEquals("268435456", props.get(TableProperties.ORC_BLOCK_SIZE_BYTES));
        Assert.assertEquals("1024", props.get(TableProperties.ORC_WRITE_BATCH_SIZE));
        Assert.assertEquals("0.05", props.get(TableProperties.ORC_BLOOM_FILTER_FPP));
        // 未配置布隆过滤列时不下发该属性
        Assert.assertFalse(props.containsKey(TableProperties.ORC_BLOOM_FILTER_COLUMNS));

        format.bloomFilterColumns = " id, order_no ";
        props.clear();
        format.setTableProperties(props);
        Assert.assertEquals("id, order_no", props.get(TableProperties.ORC_BLOOM_FILTER_COLUMNS));
    }

    @Test
    public void testAvroTableProperties() {
        AvroFileFormat format = new AvroFileFormat();
        Map<String, String> props = new HashMap<>();
        format.setTableProperties(props);

        Assert.assertEquals("AVRO", props.get(TableProperties.DEFAULT_FILE_FORMAT));
        Assert.assertEquals("gzip", props.get(TableProperties.AVRO_COMPRESSION));
        Assert.assertFalse(props.containsKey(TableProperties.AVRO_COMPRESSION_LEVEL));
    }

    @Test
    public void testWriterDelegatesToFileFormat() {
        DataxIcebergWriter writer = createWriter(new ParquetFileFormat());
        writer.targetFileSizeBytes = 1048576L;
        Map<String, String> props = new HashMap<>();
        writer.setTableProperties(props);

        Assert.assertEquals("PARQUET", props.get(TableProperties.DEFAULT_FILE_FORMAT));
        Assert.assertEquals("zstd", props.get(TableProperties.PARQUET_COMPRESSION));
        Assert.assertEquals("1048576", props.get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES));
        Assert.assertEquals(FileFormat.PARQUET, writer.getFileFormat());
    }

    @Test
    public void testColumnValueConversion() {
        // 真实 DataX 作业启动时会调用 ColumnCast.bind() 初始化日期格式器，单测里要自己初始化
        ColumnCast.bind(Configuration.from(Collections.emptyMap()));
        IcebergColumn tsCol = new IcebergColumn(CMeta.create("create_time", JDBCTypes.TIMESTAMP));
        // 源端是字符串时间列时 asLong() 会抛 CONVERT_NOT_SUPPORT，必须走 asDate()
        Object tsVal = tsCol.getFieldVal(new StringColumn("2026-10-25 12:34:56"));
        Assert.assertEquals(LocalDateTime.of(2026, 10, 25, 12, 34, 56), tsVal);

        IcebergColumn dateCol = new IcebergColumn(CMeta.create("dt", JDBCTypes.DATE));
        Assert.assertEquals(LocalDate.of(2026, 10, 25),
                dateCol.getFieldVal(new StringColumn("2026-10-25")));

        IcebergColumn strCol = new IcebergColumn(CMeta.create("name", JDBCTypes.VARCHAR));
        Assert.assertNull(strCol.getFieldVal(new StringColumn(null)));
    }

    @Test
    public void testTimePartitionSpec() {
        TimePartition partition = new TimePartition();
        partition.timeColumn = "create_time";
        partition.granularity = TimePartition.Granularity.day;

        Schema schema = IcebergUtils.createSchema(Lists.newArrayList(
                new IcebergColumn(CMeta.create("id", JDBCTypes.BIGINT)),
                new IcebergColumn(CMeta.create("create_time", JDBCTypes.TIMESTAMP))));

        PartitionSpec spec = partition.createPartitionSpec(schema);
        Assert.assertTrue(partition.isPartitioned());
        Assert.assertEquals(1, spec.fields().size());
        Assert.assertEquals("day", spec.fields().get(0).transform().toString());

        // 分区列不存在时要尽早失败，而不是在写文件时 NPE
        partition.timeColumn = "not_exist";
        try {
            partition.createPartitionSpec(schema);
            Assert.fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * 分区策略挂在表设置上，建表时由 tab 解析出 spec
     */
    @Test
    public void testSelectedTabPartitionSpec() {
        IcebergSelectedTab tab = new IcebergSelectedTab();
        tab.name = "t_order";
        tab.partitionSpec = new NoPartition();
        Assert.assertTrue(tab.createPartitionSpec(schema()).isUnpartitioned());

        TimePartition partition = new TimePartition();
        partition.timeColumn = "create_time";
        partition.granularity = TimePartition.Granularity.month;
        tab.partitionSpec = partition;
        PartitionSpec spec = tab.createPartitionSpec(schema());
        Assert.assertEquals(1, spec.fields().size());
        Assert.assertEquals("month", spec.fields().get(0).transform().toString());
    }

    /**
     * 增量侧分区项必须是 {@code {transform}:{column}} 编码：执行节点上没有插件实例，
     * 只能把「怎么分区」压成字符串随 CreateTableEvent 下发。这里锁住编码格式，
     * 防止与 sink 端 {@code IcebergMetadataApplier#applyPartition} 的解析漂移。
     */
    @Test
    public void testIncrPartitionKeys() {
        TimePartition partition = new TimePartition();
        partition.timeColumn = "create_time";
        partition.granularity = TimePartition.Granularity.hour;
        Assert.assertEquals(Collections.singletonList("hour:create_time"), partition.getIncrPartitionKeys());

        // 无分区策略不产生分区项，sink 侧据此走非分区建表
        Assert.assertTrue(new NoPartition().getIncrPartitionKeys().isEmpty());
    }

    /**
     * 表设置是分区项的唯一出口：selectedTab 上没有分区策略时 sink 不能收到任何分区项，
     * 否则会按空列名建分区 spec 而报错。
     */
    @Test
    public void testSelectedTabPartitionKeys() {
        IcebergSelectedTab tab = new IcebergSelectedTab();
        // 老数据 / 框架降级构造的 tab 可能没有分区策略，必须安全降级为「无分区」
        Assert.assertTrue(tab.getPartitionKeys().isEmpty());

        tab.partitionSpec = new NoPartition();
        Assert.assertTrue(tab.getPartitionKeys().isEmpty());

        TimePartition partition = new TimePartition();
        partition.timeColumn = "create_time";
        partition.granularity = TimePartition.Granularity.month;
        tab.partitionSpec = partition;
        Assert.assertEquals(Optional.of(Collections.singletonList("month:create_time")), tab.getPartitionKeys());
    }

    /**
     * 增量建表下发的表属性必须与全量走同一份配置来源，否则同一张表在两条链路上格式/压缩会不一致
     */
    @Test
    public void testCreateTabOptsReusesWriterTableProperties() {
        DataxIcebergWriter writer = createWriter(new OrcFileFormat());
        writer.targetFileSizeBytes = 536870912L;

        Function<SelectedTab, Map<String, String>> tabOptsCreator = writer.createTabOpts();
        // createTabOpts 对每张表给同一份 writer 配置，tab 本身不参与
        IcebergSelectedTab tab = new IcebergSelectedTab();
        tab.name = "t_order";
        Map<String, String> tabOpts = tabOptsCreator.apply(tab);

        Map<String, String> expected = new HashMap<>();
        writer.setTableProperties(expected);
        Assert.assertEquals(expected, tabOpts);
        Assert.assertEquals("ORC", tabOpts.get(TableProperties.DEFAULT_FILE_FORMAT));
        Assert.assertEquals("536870912", tabOpts.get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES));
    }

    /**
     * catalog 属性里必须带上 catalog-impl 与 dataxName：执行节点靠前者反射出
     * {@link TISIcebergCatalog}，靠后者反查 writer 拿到 hadoop 连接参数
     */
    @Test
    public void testCreateCatalogOptions() {
        Map<String, String> catalogOptions = TISIcebergCatalog.createCatalogOptions(
                new StubCatalog().createCatalogOptions(), "iceberg_incr");

        Assert.assertEquals(TISIcebergCatalog.CATALOG_IMPL, catalogOptions.get(CatalogProperties.CATALOG_IMPL));
        Assert.assertEquals("iceberg_incr", catalogOptions.get(StoreResourceType.DATAX_NAME));
        Assert.assertEquals("false", catalogOptions.get(CatalogProperties.CACHE_ENABLED));
        // 具体 Catalog 给的属性要原样保留，否则执行节点不知道该连哪个 warehouse
        Assert.assertEquals(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
                catalogOptions.get(CatalogUtil.ICEBERG_CATALOG_TYPE));
        Assert.assertEquals("file:///tmp/warehouse", catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION));

        try {
            TISIcebergCatalog.createCatalogOptions(new HashMap<>(), null);
            Assert.fail("expect IllegalArgumentException when dataxName is empty");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static Schema schema() {
        return IcebergUtils.createSchema(Lists.newArrayList(
                new IcebergColumn(CMeta.create("id", JDBCTypes.BIGINT)),
                new IcebergColumn(CMeta.create("create_time", JDBCTypes.TIMESTAMP))));
    }

    private static DataxIcebergWriter createWriter(IcebergFileFormat fileFormat) {
        DataxIcebergWriter writer = new DataxIcebergWriter();
        writer.catalog = new StubCatalog();
        writer.fileFormat = fileFormat;
        return writer;
    }

    /**
     * 只用来承载 setTableProperties 的默认实现，不连接任何真实 Catalog
     */
    private static class StubCatalog extends IcebergCatalog {

        @Override
        public Catalog getCatalog() {
            return null;
        }

        @Override
        public Optional<String> getDBName() {
            return Optional.of("test_db");
        }

        /**
         * 增量侧下发给执行节点的 Catalog 属性：固定成 hadoop 类型，便于断言
         * {@code TISIcebergCatalog.createCatalogOptions} 的合并结果
         */
        @Override
        public Map<String, String> createCatalogOptions() {
            Map<String, String> opts = new HashMap<>();
            opts.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
            opts.put(CatalogProperties.WAREHOUSE_LOCATION, "file:///tmp/warehouse");
            return opts;
        }
    }
}
