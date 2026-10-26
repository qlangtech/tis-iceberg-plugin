package com.qlangtech.tis.plugins.incr.flink.pipeline.iceberg.sink;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.iceberg.catalog.IcebergCatalog;
import com.qlangtech.tis.plugin.iceberg.format.IcebergFileFormat;
import com.qlangtech.tis.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.pipeline.sink.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.dto.DTOStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Iceberg Pipeline Sink Factory
 * 实现Flink CDC到Iceberg的实时数据同步
 */
public class IcebergPipelineSinkFactory extends PipelineFlinkCDCSinkFactory {

    @FormField(ordinal = 1, validate = {Validator.require})
    public IcebergCatalog catalog;

    /**
     * 文件格式（多态）。压缩编码与各格式专属的写入参数都在具体子类里，
     * 与 DataX 侧 {@code DataxIcebergWriter.fileFormat} 共用同一组实现
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public IcebergFileFormat fileFormat;

    @FormField(ordinal = 3)
    public Long targetFileSizeBytes = 134217728L;  // 128MB

    @FormField(ordinal = 4)
    public Integer parallelism = 1;  // 写入并行度

    @FormField(ordinal = 5, advance = true)
    public Boolean enableUpsert = false;  // 是否启用upsert模式

    @FormField(ordinal = 6, advance = true)
    public String equalityFieldColumns;  // 主键字段，用于upsert

    @FormField(ordinal = 7, advance = true)
    public Integer checkpointInterval = 60000;  // checkpoint间隔（毫秒）

    @FormField(ordinal = 8, advance = true)
    public Boolean enableSchemaEvolution = true;  // 是否启用Schema演化

    /**
     * 汇总本插件要下发给 Iceberg 的表属性：建表属性与 Sink 写入属性共用这一份
     */
    private Map<String, String> createTableProperties() {
        Map<String, String> properties = new HashMap<>();
        Objects.requireNonNull(this.fileFormat, "fileFormat can not be null").setTableProperties(properties);
        if (this.targetFileSizeBytes != null) {
            properties.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(this.targetFileSizeBytes));
        }
        return properties;
    }

    @Override
    protected BasicTISSinkFactory<?> createSinkFactory() {
        return new IcebergSinkFactory(this);
    }

    /**
     * Iceberg Sink Factory实现
     */
    private static class IcebergSinkFactory extends BasicTISSinkFactory<RowData> {

        private final IcebergPipelineSinkFactory factory;

        public IcebergSinkFactory(IcebergPipelineSinkFactory factory) {
            this.factory = factory;
        }

        @Override
        public DataStreamSink<RowData> createSinkFunction(DTOStream stream) throws Exception {
            StreamExecutionEnvironment env = stream.getExecutionEnvironment();

            // 设置checkpoint间隔
            if (factory.checkpointInterval > 0) {
                env.enableCheckpointing(factory.checkpointInterval);
            }

            // 获取Iceberg Catalog
            Catalog icebergCatalog = factory.catalog.getCatalog();
            String database = factory.catalog.getDBName().orElse("default");
            String tableName = stream.getTableName();

            TableIdentifier tableId = TableIdentifier.of(database, tableName);

            // 建表属性与写入属性来自同一份配置，避免两处漂移
            Map<String, String> tableProperties = factory.createTableProperties();

            // 加载或创建表
            Table table;
            if (icebergCatalog.tableExists(tableId)) {
                table = icebergCatalog.loadTable(tableId);
            } else {
                // 创建新表
                Schema schema = createIcebergSchema(stream);
                table = icebergCatalog.createTable(tableId, schema, null, tableProperties);
            }

            // 转换数据流
            DataStream<RowData> rowDataStream = convertToRowData(stream);

            // 创建Iceberg Sink
            FlinkSink.Builder builder = FlinkSink.forRowData(rowDataStream)
                .table(table)
                .tableLoader(IcebergTableLoader.fromCatalog(icebergCatalog, tableId))
                .writeParallelism(factory.parallelism);

            // 文件格式、压缩编码、格式专属参数与目标文件大小一次下发
            builder.setAll(tableProperties);

            // 配置upsert模式
            if (factory.enableUpsert && factory.equalityFieldColumns != null) {
                builder.upsert(true);
                builder.equalityFieldColumns(factory.equalityFieldColumns.split(","));
            }

            // 添加sink
            return builder.append();
        }

        /**
         * 创建Iceberg Schema
         */
        private Schema createIcebergSchema(DTOStream stream) {
            List<FlinkCol> cols = stream.getColsMeta();
            Types.NestedField[] fields = new Types.NestedField[cols.size()];

            for (int i = 0; i < cols.size(); i++) {
                FlinkCol col = cols.get(i);
                org.apache.iceberg.types.Type icebergType = FlinkSchemaUtil.convert(col.getType());

                if (col.isNullable()) {
                    fields[i] = Types.NestedField.optional(i + 1, col.getName(), icebergType);
                } else {
                    fields[i] = Types.NestedField.required(i + 1, col.getName(), icebergType);
                }
            }

            return new Schema(fields);
        }

        /**
         * 转换数据流为RowData
         */
        private DataStream<RowData> convertToRowData(DTOStream stream) {
            return stream.getStream()
                .map(new DTOToRowDataMapper(stream.getColsMeta()));
        }
    }

    /**
     * DTO到RowData的映射器
     */
    private static class DTOToRowDataMapper implements MapFunction<Object, RowData> {
        private final List<FlinkCol> cols;

        public DTOToRowDataMapper(List<FlinkCol> cols) {
            this.cols = cols;
        }

        @Override
        public RowData map(Object value) throws Exception {
            // 实现DTO到RowData的转换逻辑
            // 这里需要根据实际的DTO结构进行转换
            return null; // TODO: 实现转换逻辑
        }
    }

    /**
     * Iceberg表加载器
     */
    private static class IcebergTableLoader {
        public static org.apache.iceberg.flink.TableLoader fromCatalog(
                Catalog catalog, TableIdentifier tableId) {
            return org.apache.iceberg.flink.TableLoader.fromCatalog(
                new IcebergCatalogLoader(catalog), tableId);
        }
    }

    /**
     * Iceberg Catalog加载器
     */
    private static class IcebergCatalogLoader
            implements org.apache.iceberg.flink.CatalogLoader {

        private final Catalog catalog;

        public IcebergCatalogLoader(Catalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public Catalog loadCatalog() {
            return catalog;
        }

        @Override
        public void close() {
            // 关闭资源
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDescriptor {

        @Override
        public String getDisplayName() {
            return "Iceberg Pipeline Sink";
        }
    }
}