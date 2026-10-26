package com.qlangtech.tis.plugin.iceberg.datax.writemode;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;

import java.io.IOException;
import java.util.UUID;

/**
 * 追加写入模式
 */
public class AppendWriteMode extends WriteMode {

    @Override
    public IcebergTableWriter createTableWriter(Table table, FileFormat fileFormat, long targetFileSize,
                                                int taskId) throws IOException {
        return new AppendTableWriter(table, fileFormat, targetFileSize, taskId);
    }

    @Override
    public String getModeName() {
        return "append";
    }

    /**
     * 追加写入器实现
     */
    private static class AppendTableWriter extends BaseTableWriter {

        public AppendTableWriter(Table table, FileFormat fileFormat, long targetFileSize, int taskId) {
            super(table, fileFormat, targetFileSize, taskId);
        }

        @Override
        protected TaskWriter<Record> createTaskWriter() throws IOException {
            Schema schema = table.schema();
            PartitionSpec spec = table.spec();

            FileWriterFactory<Record> fileWriterFactory =
                    new GenericFileWriterFactory.Builder(table)
                            .dataFileFormat(fileFormat)
                            .dataSchema(schema)
                            .build();

            // taskId 在 JVM 内唯一，operationId 用于区分同一次写入作业产生的数据文件
            OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 0, taskId)
                    .format(fileFormat)
                    .defaultSpec(spec)
                    .operationId(UUID.randomUUID().toString())
                    .build();

            if (spec.isUnpartitioned()) {
                return new UnpartitionedWriter<>(spec, fileFormat, fileWriterFactory, fileFactory,
                        table.io(), targetFileSize);
            } else {
                return new PartitionedAppendWriter(spec, fileFormat, fileWriterFactory, fileFactory,
                        table.io(), targetFileSize, schema);
            }
        }

        @Override
        public void commit() throws IOException {
            if (taskWriter == null) {
                return;
            }
            taskWriter.close();

            // 一个 TaskWriter 可能产出多个数据文件（按分区 / 按目标文件大小滚动）
            org.apache.iceberg.AppendFiles append = table.newAppend();
            for (org.apache.iceberg.DataFile dataFile : taskWriter.dataFiles()) {
                append.appendFile(dataFile);
            }
            append.commit();
        }
    }

    /**
     * 分区追加写入器
     */
    private static class PartitionedAppendWriter extends PartitionedFanoutWriter<Record> {

        private final PartitionKey partitionKey;
        private final InternalRecordWrapper wrapper;

        public PartitionedAppendWriter(PartitionSpec spec,
                                       FileFormat format,
                                       FileWriterFactory<Record> fileWriterFactory,
                                       OutputFileFactory fileFactory,
                                       org.apache.iceberg.io.FileIO io,
                                       long targetFileSize,
                                       Schema schema) {
            super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize);
            this.partitionKey = new PartitionKey(spec, schema);
            this.wrapper = new InternalRecordWrapper(schema.asStruct());
        }

        @Override
        protected PartitionKey partition(Record row) {
            partitionKey.partition(wrapper.wrap(row));
            return partitionKey;
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<WriteMode> {

        @Override
        public String getDisplayName() {
            return "Append Mode";
        }
    }
}
