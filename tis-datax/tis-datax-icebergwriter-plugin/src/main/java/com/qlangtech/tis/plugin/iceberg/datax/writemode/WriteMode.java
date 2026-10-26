package com.qlangtech.tis.plugin.iceberg.datax.writemode;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergTablePropsSetter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Iceberg 写入模式抽象基类
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public abstract class WriteMode implements Describable<WriteMode>, IcebergTablePropsSetter {

    /**
     * 创建 Iceberg 表写入器
     *
     * @param table          目标表
     * @param fileFormat     数据文件格式
     * @param targetFileSize 单个数据文件的目标大小
     * @param taskId         当前 DataX 子任务号，用于生成唯一的数据文件名
     */
    public abstract IcebergTableWriter createTableWriter(Table table, FileFormat fileFormat, long targetFileSize,
                                                         int taskId) throws IOException;

    /**
     * 写入模式名称
     */
    public abstract String getModeName();

    /**
     * Iceberg 表写入器接口
     */
    public interface IcebergTableWriter extends Closeable {

        /**
         * 写入一条记录
         */
        void write(GenericRecord record) throws IOException;

        /**
         * 提交写入
         */
        void commit() throws IOException;

        /**
         * 回滚写入
         */
        void rollback() throws IOException;

        /**
         * 获取写入的记录数
         */
        long getWrittenRecords();
    }

    /**
     * 基础写入器实现：TaskWriter 在第一次写入时惰性创建
     */
    public abstract static class BaseTableWriter implements IcebergTableWriter {
        protected final Table table;
        protected final FileFormat fileFormat;
        protected final long targetFileSize;
        protected final int taskId;
        /**
         * iceberg-data 的实现（GenericFileWriterFactory）产出的是 {@link Record}
         */
        protected TaskWriter<Record> taskWriter;
        protected long writtenRecords = 0;

        public BaseTableWriter(Table table, FileFormat fileFormat, long targetFileSize, int taskId) {
            this.table = table;
            this.fileFormat = fileFormat;
            this.targetFileSize = targetFileSize;
            this.taskId = taskId;
        }

        @Override
        public void write(GenericRecord record) throws IOException {
            if (taskWriter == null) {
                taskWriter = createTaskWriter();
            }
            taskWriter.write(record);
            writtenRecords++;
        }

        protected abstract TaskWriter<Record> createTaskWriter() throws IOException;

        @Override
        public long getWrittenRecords() {
            return writtenRecords;
        }

        @Override
        public void close() throws IOException {
            if (taskWriter != null) {
                taskWriter.close();
            }
        }

        @Override
        public void rollback() throws IOException {
            if (taskWriter != null) {
                taskWriter.abort();
            }
        }
    }
}
