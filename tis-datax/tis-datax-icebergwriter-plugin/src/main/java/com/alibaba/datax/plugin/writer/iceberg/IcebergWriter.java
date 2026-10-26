package com.alibaba.datax.plugin.writer.iceberg;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.IJobContainerContext;
import com.alibaba.datax.core.job.ITransformerBuildInfo;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergColumn;
import com.qlangtech.tis.plugin.iceberg.datax.writemode.WriteMode.IcebergTableWriter;
import com.qlangtech.tis.plugin.iceberg.utils.IcebergUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.alibaba.datax.plugin.writer.iceberg.Key.DATABASE_NAME;
import static com.alibaba.datax.plugin.writer.iceberg.Key.SOURCE_TABLE_NAME;
import static com.alibaba.datax.plugin.writer.iceberg.Key.TABLE_NAME;
import static com.alibaba.datax.plugin.writer.iceberg.IcebergWriterErrorCode.ICEBERG_ERROR_DB;
import static com.alibaba.datax.plugin.writer.iceberg.IcebergWriterErrorCode.ICEBERG_ERROR_TABLE;
import static com.alibaba.datax.plugin.writer.iceberg.IcebergWriterErrorCode.ICEBERG_WRITE_ERROR;

/**
 * DataX 执行入口：把上游数据写入 Iceberg 表。
 * <p>
 * 参照 PaimonWriter 实现，写文件的细节（文件格式、分区、提交）由
 * {@link DataxIcebergWriter#writeMode} 对应的 {@link IcebergTableWriter} 负责。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/25
 */
public class IcebergWriter extends Writer {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

    public static class Job extends Writer.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private Configuration sliceConfig;
        private DataxIcebergWriter icebergWriter;
        private List<IcebergColumn> icebergCols;
        private Schema recordSchema;
        private IcebergTableWriter tableWriter;

        @Override
        public void init() {
            IJobContainerContext context = Objects.requireNonNull(this.containerContext, "containerContext can not be null");
            IDataxProcessor processor = DataxProcessor.load(null, context.getCollectionName());
            this.icebergWriter = Objects.requireNonNull((DataxIcebergWriter) processor.getWriter(null)
                    , "icebergWriter can not be null");

            this.sliceConfig = super.getPluginJobConf();
            String sourceTableName = this.sliceConfig.getNecessaryValue(SOURCE_TABLE_NAME, ICEBERG_ERROR_TABLE);
            IDataxReader reader = processor.getReader(null);
            ISelectedTab selectedTab = Objects.requireNonNull(reader.getSelectedTab(sourceTableName)
                    , "sourceTableName:" + sourceTableName + " can not be null");

            Optional<RecordTransformerRules> transformerRules
                    = context.getTransformerBuildCfg().map(ITransformerBuildInfo::getTransformersRules);
            this.icebergCols = this.icebergWriter.createIcebergCols(selectedTab, transformerRules);
            this.recordSchema = IcebergUtils.createSchema(this.icebergCols);

            String tableName = this.sliceConfig.getNecessaryValue(TABLE_NAME, ICEBERG_ERROR_TABLE);
            String dbName = this.sliceConfig.getNecessaryValue(DATABASE_NAME, ICEBERG_ERROR_DB);
            try {
                Table table = this.icebergWriter.catalog.getCatalog()
                        .loadTable(TableIdentifier.of(Namespace.of(dbName), tableName));
                this.tableWriter = this.icebergWriter.createWriter(context.getTaskId(),
                        Objects.requireNonNull(table, "table can not be null"));
            } catch (Exception e) {
                throw DataXException.asDataXException(ICEBERG_ERROR_TABLE, e);
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            Column column;
            IcebergColumn icebergColumn;
            while ((record = recordReceiver.getFromReader()) != null) {
                GenericRecord genericRecord = GenericRecord.create(this.recordSchema);
                for (int i = 0; i < this.icebergCols.size(); i++) {
                    icebergColumn = this.icebergCols.get(i);
                    column = record.getColumn(i);
                    genericRecord.set(i, icebergColumn.getFieldVal(column));
                }
                try {
                    this.tableWriter.write(genericRecord);
                } catch (Exception e) {
                    throw DataXException.asDataXException(ICEBERG_WRITE_ERROR, e);
                }
            }
            try {
                this.tableWriter.commit();
            } catch (Exception e) {
                throw DataXException.asDataXException(ICEBERG_WRITE_ERROR, e);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            if (this.tableWriter != null) {
                try {
                    this.tableWriter.close();
                } catch (Exception e) {
                    LOG.warn("close iceberg table writer faild", e);
                }
            }
        }
    }
}
