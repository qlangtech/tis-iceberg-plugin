package com.qlangtech.tis.plugins.incr.flink.pipeline.iceberg.sink;

import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import org.apache.flink.cdc.common.event.Event;

import java.util.List;

/**
 * Iceberg 实时增量写入（基于 flink-cdc pipeline sink）。
 * <p>
 * 对应实现见 {@code flink-cdc-pipeline-connector-iceberg-tis}（TIS 定制的 flink-cdc Iceberg
 * connector）：表结构、分区与提交都由该连接器负责，本类只负责把 TIS 侧的 writer 配置
 * 翻译成 connector 需要的 catalog / table 选项。
 * <p>
 * 字段全部复用 {@link DataxIcebergWriter}：全量与增量对同一张表的格式、压缩、目标文件大小
 * 必须一致，两侧各配一份迟早漂移。因此本插件不声明自己的 catalog/fileFormat 字段。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/26
 */
public class IcebergPipelineSinkFactory extends PipelineFlinkCDCSinkFactory<DataxIcebergWriter> {

    @Override
    public final ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(this.getClass()));
    }

    @Override
    protected TabSinkFunc<?, ?, Event> createPipelineEventSinkFunc(
            IDataxProcessor dataxProcessor, DataxIcebergWriter writer, List<ISelectedTab> tabs,
            IFlinkColCreator<FlinkCol> sourceFlinkColCreator, IncrStreamFactory streamFactory) {
        return new IcebergPipelineEventSinkFunc(dataxProcessor, this, writer.catalog.getDBName(), tabs,
                sourceFlinkColCreator, null, streamFactory.getParallelism());
    }

    @TISExtension
    public static class DftDesc extends BasicPipelineSinkDescriptor {

        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.Iceberg;
        }
    }
}
