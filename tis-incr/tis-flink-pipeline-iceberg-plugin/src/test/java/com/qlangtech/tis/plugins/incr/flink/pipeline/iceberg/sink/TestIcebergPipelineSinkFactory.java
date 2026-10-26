package com.qlangtech.tis.plugins.incr.flink.pipeline.iceberg.sink;

import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 增量 sink 描述符构建期校验。
 * <p>
 * 增量插件自己不声明业务字段：catalog / fileFormat / 分区策略全部复用 {@link DataxIcebergWriter}
 * 与 {@code IcebergSelectedTab}。因此这里的重点是「别重复声明配置」与「基类字段仍在」——
 * 一旦有人在本类上加一份平行的 catalog 字段，全量与增量就会各配一份，迟早漂移。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class TestIcebergPipelineSinkFactory {

    @Test
    public void testSinkDescriptor() {
        IcebergPipelineSinkFactory.DftDesc desc = new IcebergPipelineSinkFactory.DftDesc();
        Map<String, ?> props = desc.getPropertyTypes();

        // 基类 PipelineFlinkCDCSinkFactory 的公共字段
        require(props, "timeZone");
        require(props, "schemaBehavior");

        // 本插件不拥有任何业务字段：目标端配置一律来自 writer
        Set<String> ownFields = new HashSet<>();
        for (java.lang.reflect.Field f : IcebergPipelineSinkFactory.class.getDeclaredFields()) {
            if (f.isAnnotationPresent(com.qlangtech.tis.plugin.annotation.FormField.class)) {
                ownFields.add(f.getName());
            }
        }
        Assert.assertTrue("IcebergPipelineSinkFactory must not declare its own form fields, found:" + ownFields,
                ownFields.isEmpty());

        // Paimon 侧同款判断：schemaBehavior 的默认值是 IGNORE（见 BasicPipelineSinkDescriptor 构造器）
        PropertyType schemaBehavior = require(props, "schemaBehavior");
        Assert.assertFalse("schemaBehavior enum options can not be empty",
                schemaBehavior.getEnumPropOptions().isEmpty());
        Assert.assertTrue("schemaBehavior must contain IGNORE",
                schemaBehavior.getEnumPropOptions().stream()
                        .anyMatch(o -> SchemaChangeBehavior.IGNORE.name().equals(o.getValue())));

        Assert.assertEquals(IEndTypeGetter.EndType.Iceberg, desc.getEndType());
        // 基类 displayName 由 targetType 拼出，改了 targetType 会连带改掉插件显示名
        Assert.assertEquals("FlinkCDC-Pipeline-Sink-Iceberg", desc.getDisplayName());
    }

    /**
     * 增量链路要求 writer 同时实现 {@code IInitWriterTableExecutor} 与
     * {@code ICDCPipelineTableOptionsCreator}：{@code PipelineEventSinkFunc} 的构造器直接强转，
     * 缺任何一个都是运行期 IllegalStateException / ClassCastException，编译期拦不住。
     */
    @Test
    public void testWriterImplementsIncrContracts() throws Exception {
        Class<?> writerClazz = DataxIcebergWriter.class;
        Assert.assertTrue("writer must implement IInitWriterTableExecutor",
                com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor.class.isAssignableFrom(writerClazz));
        Assert.assertTrue("writer must implement ICDCPipelineTableOptionsCreator",
                com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.ICDCPipelineTableOptionsCreator.class
                        .isAssignableFrom(writerClazz));

        // createTabOpts() 不能为 null，否则 SchemaEmitterFunction 构造期即 NPE
        DataxIcebergWriter writer = new DataxIcebergWriter();
        writer.fileFormat = new com.qlangtech.tis.plugin.iceberg.format.ParquetFileFormat();
        writer.targetFileSizeBytes = 1048576L;
        Assert.assertNotNull(writer.createTabOpts());
    }

    /**
     * 表设置必须是 {@code IcebergSelectedTab}：sink 侧靠它拿分区项，降级成父类 SelectedTab
     * 会让分区配置静默失效（表被建成无分区表）。
     */
    @Test
    public void testSelectedTabTypeIsIceberg() {
        DataxIcebergWriter.DefaultDescriptor desc = new DataxIcebergWriter.DefaultDescriptor();
        Assert.assertTrue("select tab desc must be IcebergSelectedTab",
                com.qlangtech.tis.plugin.iceberg.datax.IcebergSelectedTab.class
                        .isAssignableFrom(desc.getRewriterSelectTabDescriptor().clazz));
        Assert.assertTrue(SelectedTab.class.isAssignableFrom(
                desc.getRewriterSelectTabDescriptor().clazz));
    }

    private static PropertyType require(Map<String, ?> props, String field) {
        Object prop = props.get(field);
        Assert.assertNotNull("lack field:" + field, prop);
        return (PropertyType) prop;
    }
}
