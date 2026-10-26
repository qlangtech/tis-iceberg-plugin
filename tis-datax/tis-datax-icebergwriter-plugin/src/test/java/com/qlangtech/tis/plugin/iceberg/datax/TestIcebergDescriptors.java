package com.qlangtech.tis.plugin.iceberg.datax;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.iceberg.catalog.HadoopCatalog;
import com.qlangtech.tis.plugin.iceberg.catalog.HiveMetastoreCatalog;
import com.qlangtech.tis.plugin.iceberg.catalog.IcebergCatalog;
import com.qlangtech.tis.plugin.iceberg.datax.partition.NoPartition;
import com.qlangtech.tis.plugin.iceberg.datax.partition.TimePartition;
import com.qlangtech.tis.plugin.iceberg.datax.utils.IcebergAutoCreateTable;
import com.qlangtech.tis.plugin.iceberg.datax.writemode.AppendWriteMode;
import com.qlangtech.tis.plugin.iceberg.format.AvroFileFormat;
import com.qlangtech.tis.plugin.iceberg.format.OrcFileFormat;
import com.qlangtech.tis.plugin.iceberg.format.ParquetFileFormat;
import com.qlangtech.tis.plugin.iceberg.utils.IcebergSnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 描述符构建期校验。
 * <p>
 * {@code @FormField(type = ENUM)} 的选项默认由 Java 枚举反射生成（{@code registerSelectOptions}
 * 只服务于 SELECTABLE），选项缺失会在 {@code getPropertyTypes()} 时抛
 * "enum property can not be empty"，编译期发现不了，必须在这里兜住。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class TestIcebergDescriptors {

    @Test
    @SuppressWarnings("unchecked")
    public void testWriterDescriptor() {
        Descriptor<DataxIcebergWriter> desc = (Descriptor) new DataxIcebergWriter.DefaultDescriptor();
        Map<String, ?> props = desc.getPropertyTypes();

        // fileFormat 是多态 Describable：有三个实现，.json 的 dftVal 必须逐字匹配默认子类 displayName
        PropertyType fileFormat = require(props, "fileFormat");
        Assert.assertEquals("Parquet", fileFormat.getExtraProps().get(PluginExtraProps.KEY_DFTVAL_PROP));
        List<? extends Descriptor> formatDescs = fileFormat.getApplicableDescriptors();
        Assert.assertEquals(3, formatDescs.size());
        for (String expect : List.of("Parquet", "ORC", "Avro")) {
            Assert.assertTrue("fileFormat lack desc:" + expect,
                    formatDescs.stream().anyMatch(d -> expect.equals(d.getDisplayName())));
        }
        // 压缩编码已下沉进各子类，顶层不应再有该字段
        Assert.assertNull(props.get("compressionCodec"));

        // 多态子表单必须能解析到实现类，且 .json 里的 dftVal 要能匹配到某个选项
        assertSingleApplicable(props, "writeMode", "Append Mode");
        assertApplicableContains(props, "catalog", "Hive Metastore Catalog");
        // autoCreateTable 的可选实现由 .json 里的 groovy subDescEnumFilter 过滤
        assertSingleApplicable(props, "autoCreateTable", "on");
        // 分区策略已迁移到表设置（IcebergSelectedTab）上，writer 顶层不应再有该字段
        Assert.assertNull(props.get("partitionSpec"));
    }

    /**
     * 表设置是子表单，{@code @FormField} 的选项/默认值都靠 json/md 与父类合并，单独校验
     */
    @Test
    public void testSelectedTabDescriptor() {
        Descriptor<SelectedTab> desc = new IcebergSelectedTab.DefaultDescriptor();
        Assert.assertEquals(IcebergSelectedTab.class, desc.clazz);
        Map<String, ?> props = desc.getPropertyTypes();
        // No Partition / Time Partition 两种都合法，dftVal 逐字匹配默认子类的 displayName
        assertApplicableContains(props, IcebergSelectedTab.KEY_PARTITION_SPEC, "No Partition");
        assertApplicableContains(props, IcebergSelectedTab.KEY_PARTITION_SPEC, "Time Partition");
        Assert.assertEquals("No Partition", require(props, IcebergSelectedTab.KEY_PARTITION_SPEC)
                .getExtraProps().get(PluginExtraProps.KEY_DFTVAL_PROP));
        // 父类 SelectedTab 的字段仍在
        require(props, SelectedTab.KEY_FIELD_COLS);
        require(props, SelectedTab.KEY_FIELD_PRIMARY_KEYS);
    }

    /**
     * subDescEnumFilter 是运行期 Groovy 脚本，写错了编译期发现不了
     */
    private static void assertSingleApplicable(Map<String, ?> props, String field, String expectDisplayName) {
        List<? extends Descriptor> descs = require(props, field).getApplicableDescriptors();
        Assert.assertEquals("field:" + field + " applicable descs", 1, descs.size());
        Assert.assertEquals(expectDisplayName, descs.get(0).getDisplayName());
    }

    private static void assertApplicableContains(Map<String, ?> props, String field, String expectDisplayName) {
        List<? extends Descriptor> descs = require(props, field).getApplicableDescriptors();
        Assert.assertTrue("field:" + field + " lack desc:" + expectDisplayName,
                descs.stream().anyMatch(d -> expectDisplayName.equals(d.getDisplayName())));
    }

    @Test
    public void testAllEnumFieldsHaveOptions() throws Exception {
        assertEnumOptions(TimePartition.DefaultDescriptor.class);
        assertEnumOptions(HadoopCatalog.DefaultDescriptor.class);
        assertEnumOptions(IcebergSnapshot.DefaultDescriptor.class);
        // 三种文件格式的压缩编码 / page 版本 / 压缩策略都是 Java 枚举，选项靠反射生成，写错拿不到选项
        assertEnumOptions(ParquetFileFormat.DefaultDescriptor.class);
        assertEnumOptions(OrcFileFormat.DefaultDescriptor.class);
        assertEnumOptions(AvroFileFormat.DefaultDescriptor.class);
        // HiveMetastoreCatalog 的 dbName 选项来自 groovy 脚本，取不到数据源时为空是正常的，
        // 这里只验证选项属性存在（即不会被 "enum property can not be empty" 打断）
        Descriptor<?> hiveDesc = new HiveMetastoreCatalog.DefaultDescriptor();
        Assert.assertNotNull(hiveDesc.getPropertyTypes().get("dbName"));
    }

    @Test
    public void testAutoCreateTableDescriptor() {
        // autoCreateTable 的 addComment 字段是 AutoCreateTableColCommentSwitch，选项只能由 @TISExtensible
        // 实现类提供；这里验证子表单能正常构建（父类 json 里的 dftVal=on 需要匹配 DftDesc 的 displayName）
        Descriptor<?> desc = new IcebergAutoCreateTable.Desc();
        Map<String, ?> props = desc.getPropertyTypes();
        require(props, "addComment");
        require(props, "aliasPrefix");
        Assert.assertEquals("on", desc.getDisplayName());
    }

    @Test
    public void testSubDescriptorDisplayNames() throws Exception {
        // 写入模式/分区策略在 .json 中通过 dftVal 匹配 getDisplayName()，名字改了会导致默认值失效
        Assert.assertEquals("Append Mode", new AppendWriteMode.DefaultDescriptor().getDisplayName());
        Assert.assertEquals("No Partition", new NoPartition.DefaultDescriptor().getDisplayName());
        Assert.assertEquals("Time Partition", new TimePartition.DefaultDescriptor().getDisplayName());
        // DataxIcebergWriter.json 中 catalog 的 dftVal 逐字匹配这两个名字
        Assert.assertEquals("Hive Metastore Catalog", new HiveMetastoreCatalog.DefaultDescriptor().getDisplayName());
        Assert.assertEquals("Hadoop Catalog", new HadoopCatalog.DefaultDescriptor().getDisplayName());
    }

    /**
     * 两个 Catalog 的数据目录与 Hadoop 连接参数都改为来自 FileSystemFactory 插件。
     * <p>
     * fsName 是 SELECTABLE，选项来自 registerSelectOptions，没有注册会直接抛异常；而它的控件是
     * "选择 + 新建"（json 里的 creator），不能复用已废弃的 warehouse / hadoopConfDir 手填字段。
     */
    @Test
    public void testCatalogReuseFileSystemFactory() {
        for (Descriptor<?> desc : List.of(new HiveMetastoreCatalog.DefaultDescriptor(),
                new HadoopCatalog.DefaultDescriptor())) {
            Map<String, ?> props = desc.getPropertyTypes();
            PropertyType fsName = require(props, IcebergCatalog.KEY_FIELD_FS_NAME);
            Assert.assertEquals(FormFieldType.SELECTABLE.getIdentity(), fsName.typeIdentity());
            // 已注册了选项回调，取值时才不会抛 "has not been register"（插件库为空是正常的）
            try {
                desc.getSelectOptions(IcebergCatalog.KEY_FIELD_FS_NAME);
            } catch (IllegalStateException e) {
                Assert.fail("fsName select options has not been registered:" + e.getMessage());
            }

            Assert.assertNull(props.get("warehouse"));
            Assert.assertNull(props.get("hadoopConfDir"));
        }
        // DataxIcebergWriter.json 中 fileFormat 的 dftVal 逐字匹配默认格式的 displayName
        Assert.assertEquals("Parquet", new ParquetFileFormat.DefaultDescriptor().getDisplayName());
        Assert.assertEquals("ORC", new OrcFileFormat.DefaultDescriptor().getDisplayName());
        Assert.assertEquals("Avro", new AvroFileFormat.DefaultDescriptor().getDisplayName());
    }

    /**
     * .md / -short.md 的 "## 字段名" 是按字段名索引的：写错名字不会报错，只是帮助静默不显示。
     * 而 -short.md 里的键必须先在 .md 里出现过，否则 PluginExtraProps 直接抛异常。
     */
    @Test
    public void testPropHelpKeysMatchFormFields() throws Exception {
        assertPropHelpKeys(DataxIcebergWriter.class);
        assertPropHelpKeys(IcebergCatalog.class);
        assertPropHelpKeys(HadoopCatalog.class);
        assertPropHelpKeys(HiveMetastoreCatalog.class);
        assertPropHelpKeys(TimePartition.class);
        assertPropHelpKeys(IcebergSelectedTab.class);
        assertPropHelpKeys(IcebergSnapshot.class);
        assertPropHelpKeys(ParquetFileFormat.class);
        assertPropHelpKeys(OrcFileFormat.class);
        assertPropHelpKeys(AvroFileFormat.class);
    }

    private static void assertPropHelpKeys(Class<?> pluginClazz) throws Exception {
        Set<String> fields = new HashSet<>();
        for (Class<?> clazz = pluginClazz; clazz != null && clazz != Object.class; clazz = clazz.getSuperclass()) {
            for (Field f : clazz.getDeclaredFields()) {
                if (f.isAnnotationPresent(FormField.class)) {
                    fields.add(f.getName());
                }
            }
        }
        Assert.assertFalse(pluginClazz.getSimpleName() + " has no @FormField", fields.isEmpty());

        Set<String> mdKeys = readHelpKeys(pluginClazz, pluginClazz.getSimpleName() + ".md");
        Set<String> shortMdKeys = readHelpKeys(pluginClazz, pluginClazz.getSimpleName() + "-short.md");
        for (String key : mdKeys) {
            Assert.assertTrue(pluginClazz.getSimpleName() + ".md unknown field:" + key, fields.contains(key));
        }
        for (String key : shortMdKeys) {
            Assert.assertTrue(pluginClazz.getSimpleName() + "-short.md key must also exist in .md:" + key,
                    mdKeys.contains(key));
        }
    }

    private static Set<String> readHelpKeys(Class<?> pluginClazz, String resourceName) throws Exception {
        Set<String> keys = new HashSet<>();
        try (InputStream i = pluginClazz.getResourceAsStream(resourceName)) {
            if (i == null) {
                // 帮助资源是可选的
                return keys;
            }
            String content = new String(i.readAllBytes(), StandardCharsets.UTF_8);
            for (String line : content.split("\n")) {
                if (line.startsWith("## ")) {
                    keys.add(line.substring(3).trim());
                }
            }
        }
        return keys;
    }

    private static void assertEnumOptions(Class<? extends Descriptor> descClazz) throws Exception {
        Descriptor<?> desc = descClazz.getDeclaredConstructor().newInstance();
        Map<String, ?> props = desc.getPropertyTypes();
        Assert.assertFalse("props can not be empty, desc:" + descClazz.getSimpleName(), props.isEmpty());
        for (Map.Entry<String, ?> entry : props.entrySet()) {
            if (entry.getValue() instanceof PropertyType pt
                    && pt.typeIdentity() == FormFieldType.ENUM.getIdentity()) {
                Assert.assertFalse(descClazz.getEnclosingClass().getSimpleName() + "." + entry.getKey()
                                + " enum options can not be empty",
                        pt.getEnumPropOptions().isEmpty());
            }
        }
    }

    private static PropertyType require(Map<String, ?> props, String field) {
        Object prop = props.get(field);
        Assert.assertNotNull("lack field:" + field, prop);
        return (PropertyType) prop;
    }
}
