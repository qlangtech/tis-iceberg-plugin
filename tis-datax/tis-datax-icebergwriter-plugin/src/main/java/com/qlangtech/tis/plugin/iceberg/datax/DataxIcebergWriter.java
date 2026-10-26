/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.iceberg.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.core.job.ISourceTable;
import com.google.common.collect.Maps;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilderDescriptor;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable.BasicDescriptor;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.iceberg.catalog.IcebergCatalog;
import com.qlangtech.tis.plugin.iceberg.datax.hook.IcebergPostExecutor;
import com.qlangtech.tis.plugin.iceberg.datax.hook.IcebergPreExecutor;
import com.qlangtech.tis.plugin.iceberg.datax.writemode.WriteMode;
import com.qlangtech.tis.plugin.iceberg.datax.writemode.WriteMode.IcebergTableWriter;
import com.qlangtech.tis.plugin.iceberg.format.IcebergFileFormat;
import com.qlangtech.tis.plugin.iceberg.utils.IcebergSnapshot;
import com.qlangtech.tis.plugin.iceberg.utils.IcebergUtils;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.ICDCPipelineTableOptionsCreator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Apache Iceberg 批量数据写入
 * <p>
 * 基于 DataxPaimonWriter 改编，适配 Apache Iceberg 1.12
 *
 * @author: baisui 百岁
 * @create: 2026-09-04
 */
@Public
public class DataxIcebergWriter extends DataxWriter implements IcebergTablePropsSetter,
        KeyedPluginStore.IPluginKeyAware, IDataXBatchPost, IInitWriterTableExecutor,
        ICDCPipelineTableOptionsCreator {

    // ============ 核心配置 ============

    @FormField(ordinal = 1, validate = {Validator.require})
    public IcebergCatalog catalog;

    /**
     * 文件格式（多态）。压缩编码与各格式专属参数都在具体子类里，
     * 见 {@link IcebergFileFormat}
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public IcebergFileFormat fileFormat;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Long targetFileSizeBytes;

    // ============ 写入策略 ============

    @FormField(ordinal = 4, validate = {Validator.require})
    public WriteMode writeMode;

    @FormField(ordinal = 5, validate = {Validator.require})
    public AutoCreateTable autoCreateTable;

    // ============ 高级配置 ============

    @FormField(ordinal = 6, advance = true)
    public IcebergSnapshot snapshotManagement;

    @FormField(ordinal = 7, advance = true, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    private String dataXName;

    @Override
    public void setKey(Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    /**
     * 将本 writer 的配置写进 Iceberg 表属性（table properties），在自动建表时落盘
     */
    @Override
    public void setTableProperties(Map<String, String> tableProps) {
        // write.format.default 与各格式专属属性（压缩编码、row group / stripe 等）由文件格式子类一并写入
        Objects.requireNonNull(this.fileFormat, "fileFormat can not be null").setTableProperties(tableProps);
        if (this.targetFileSizeBytes != null) {
            tableProps.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(this.targetFileSizeBytes));
        }
        this.catalog.setTableProperties(tableProps);
    }

    @Override
    public void startScanDependency() {
        this.catalog.startScanDependency();
    }

    /**
     * 增量（flink-cdc pipeline）建表时下发给 sink 的表属性。
     * <p>
     * 这些属性会随 {@code CreateTableEvent} 的 Schema 一路传到 Iceberg 的
     * {@code Catalog#createTable(..., properties)}，因此文件格式、压缩编码与目标文件大小
     * 都与全量写入保持同一份配置来源（{@link #setTableProperties}）。
     * <p>
     * 分区不走这里：Iceberg 的分区规格必须基于建表 schema 构建，无法用表属性表达，
     * 由 sink 端按 {@code IcebergSelectedTab#partitionSpec} 生成（见 TimePartition）。
     */
    @Override
    public Function<SelectedTab, Map<String, String>> createTabOpts() {
        return (tab) -> {
            Map<String, String> tableProps = Maps.newHashMap();
            this.setTableProperties(tableProps);
            return tableProps;
        };
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxIcebergWriter.class, "iceberg-writer-tpl.json");
    }

    @Override
    public String getTemplate() {
        return Objects.requireNonNull(this.template, "template can not be null");
    }

    @Override
    public IDataxContext getSubTask(Optional<TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        if (StringUtils.isBlank(this.dataXName)) {
            throw new IllegalStateException("param 'dataXName' can not be null");
        }

        // 与全量建表阶段使用同一个库名，避免取不到时静默落到 "default" 库
        String database = this.catalog.getDbNameCanNotBeNull();
        return new IcebergDataXContext(database, tableMap.get(), this.dataXName, transformerRules);
    }

    /**
     * 取得目标表的列定义（已包含 transformer 追加的列）
     */
    public List<IcebergColumn> createIcebergCols(ISelectedTab tab, Optional<RecordTransformerRules> transformerRules) {
        TableMap tabMapper = new TableMap(Optional.of(this.getAutoCreateTableCanNotBeNull()), tab);
        return tabMapper.appendTransformerRuleCols(transformerRules).stream().map(IcebergColumn::new).toList();
    }

    public FileFormat getFileFormat() {
        return Objects.requireNonNull(this.fileFormat, "fileFormat can not be null").getFormat();
    }

    public long getTargetFileSizeBytesCanNotBeNull() {
        return Objects.requireNonNull(this.targetFileSizeBytes, "targetFileSizeBytes can not be null");
    }

    /**
     * 创建 Iceberg 表写入器
     */
    public IcebergTableWriter createWriter(Integer taskId, Table table) {
        try {
            return Objects.requireNonNull(this.writeMode, "writeMode can not be null")
                    .createTableWriter(table, getFileFormat(), getTargetFileSizeBytesCanNotBeNull()
                            , taskId == null ? 0 : taskId);
        } catch (Exception e) {
            throw new RuntimeException("create iceberg table writer faild, table:" + table.name(), e);
        }
    }

    /**
     * 目标表不存在则执行创建逻辑
     *
     * @return true 表示表已经存在
     */
    public boolean executeIfTableNotExist(EntityName entity, Consumer<Catalog> process) {
        Catalog icebergCatalog = this.catalog.getCatalog();
        this.catalog.createNamespaceIfNotExist(icebergCatalog);
        TableIdentifier tableId = TableIdentifier.of(Namespace.of(entity.getDbName()), entity.getTabName());
        if (icebergCatalog.tableExists(tableId)) {
            return true;
        }
        process.accept(icebergCatalog);
        return false;
    }

    /**
     * 依据列定义创建 Iceberg 表（含分区与表属性）。
     * <p>
     * 分区策略是逐表配置的，挂在 {@link IcebergSelectedTab} 上而不是 writer 上：
     * 同一批同步里不同表的时间列、粒度都可能不同。传入的 tab 若不带 Iceberg 表设置
     * （如框架降级构造的 {@code DefaultTab}），则按不分区建表。
     */
    public Table createTable(Catalog icebergCatalog, EntityName entity, List<IcebergColumn> cols,
                             ISelectedTab tab) {
        Schema schema = IcebergUtils.createSchema(cols);
        PartitionSpec spec = (tab instanceof IcebergSelectedTab icebergTab)
                ? icebergTab.createPartitionSpec(schema)
                : PartitionSpec.unpartitioned();
        Map<String, String> tableProps = Maps.newHashMap();
        this.setTableProperties(tableProps);

        TableIdentifier tableId = this.catalog.createTableIdentifier(entity.getTabName());
        return icebergCatalog.createTable(tableId, schema, spec, tableProps);
    }

    @Override
    public boolean isGenerateCreateDDLSwitchOff() {
        // Iceberg 不生成 create table ddl，建表走 Catalog API
        return true;
    }

    @Override
    public AutoCreateTable getAutoCreateTableCanNotBeNull() {
        return Objects.requireNonNull(this.autoCreateTable, "autoCreateTable can not be null");
    }

    @Override
    public void initWriterTable(ISourceTable sourceTable, String sinkTargetTabName, List<String> jdbcUrls)
            throws Exception {
        // ISourceTable 只有表名，没有列元信息；建表在 IcebergPreExecutor 中完成
        throw new UnsupportedOperationException();
    }

    public static List<? extends Descriptor> supportedTableCreator(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalStateException("supportedTableCreator can not be empty");
        }
        return descs.stream().filter((desc) -> {
            EndType endType = ((BasicDescriptor) desc).getEndType();
            return endType != null && endType == EndType.Iceberg;
        }).collect(Collectors.toList());
    }

    /**
     * impl End: IDataXBatchPost
     * ========================================================
     */
    @Override
    public EntityName parseEntity(ISelectedTab tab) {
        if (StringUtils.isEmpty(tab.getName())) {
            throw new IllegalArgumentException("param tabName can not be null");
        }
        // 与全量建表、DataX 模板使用同一份（可能带前缀的）目标表名
        String tabName = new TableMap(Optional.of(this.getAutoCreateTableCanNotBeNull()), tab).getTo();
        return EntityName.create(this.catalog.getDbNameCanNotBeNull(), tabName);
    }

    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.FullDump);
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, EntityName entity,
                                                           ISelectedTab tab) {
        return new IcebergPreExecutor(execContext, this, entity, tab);
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab,
                                                 IDataXGenerateCfgs cfgFileNames) {
        return new IcebergPostExecutor(execContext, this, entity, tab);
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor implements IFlatTableBuilderDescriptor,
            DataxWriter.IRewriteSuFormProperties {

        public DefaultDescriptor() {
            super();
        }
        @Override
        public Descriptor getRewriterSelectTabDescriptor() {
            Class targetClass = IcebergSelectedTab.class;
            return Objects.requireNonNull(TIS.get().getDescriptor(targetClass),
                    "subForm clazz:" + targetClass + " " + "can not find relevant Descriptor");
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Iceberg;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return "Iceberg Writer";
        }

        /**
         * 校验目标文件大小：1MB ~ 10GB
         */
        public boolean validateTargetFileSizeBytes(IFieldErrorHandler msgHandler, Context context,
                                                   String fieldName, String value) {
            long size = Long.parseLong(value);
            if (size < 1048576) {
                msgHandler.addFieldError(context, fieldName, "目标文件大小不能小于1MB");
                return false;
            }
            if (size > 10737418240L) {
                msgHandler.addFieldError(context, fieldName, "目标文件大小不能超过10GB");
                return false;
            }
            return true;
        }
    }
}
