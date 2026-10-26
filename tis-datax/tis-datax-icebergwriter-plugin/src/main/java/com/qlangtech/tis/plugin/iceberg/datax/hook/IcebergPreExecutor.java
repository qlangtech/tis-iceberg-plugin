package com.qlangtech.tis.plugin.iceberg.datax.hook;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import com.qlangtech.tis.plugin.iceberg.datax.IcebergColumn;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.iceberg.catalog.Catalog;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * dump 任务执行前负责创建 Iceberg 表
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2026-10-25
 **/
public class IcebergPreExecutor implements IRemoteTaskPreviousTrigger {

    private final DataxIcebergWriter icebergWriter;
    private final EntityName entity;
    private final ISelectedTab tab;
    private final IExecChainContext execContext;

    public IcebergPreExecutor(IExecChainContext execContext, DataxIcebergWriter icebergWriter, EntityName entity,
                              ISelectedTab tab) {
        this.icebergWriter = Objects.requireNonNull(icebergWriter, "icebergWriter can not be null");
        this.entity = Objects.requireNonNull(entity, "entity can not be null");
        this.tab = Objects.requireNonNull(tab, "tab can not be null");
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
    }

    @Override
    public String getTaskName() {
        return "Initialize_" + this.entity.getTabName();
    }

    @Override
    public void run() {
        this.icebergWriter.executeIfTableNotExist(this.entity, (myCatalog) -> {
            List<IcebergColumn> cols = this.icebergWriter.createIcebergCols(this.tab, getRecordTransformerRules());
            this.createTable(myCatalog, cols);
        });
    }

    Optional<RecordTransformerRules> getRecordTransformerRules() {
        return RecordTransformerRules.loadTransformerRules(null,
                Objects.requireNonNull(this.execContext, "execContext can not be null").getProcessor(),
                this.tab.getName());
    }

    private void createTable(Catalog catalog, List<IcebergColumn> cols) {
        // 分区策略是表设置（IcebergSelectedTab）上的字段，建表时由它解析出 spec
        this.icebergWriter.createTable(catalog, this.entity, cols, this.tab);
    }
}
