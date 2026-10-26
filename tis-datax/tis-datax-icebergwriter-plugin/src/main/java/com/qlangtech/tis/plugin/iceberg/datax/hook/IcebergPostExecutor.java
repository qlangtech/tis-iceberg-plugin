package com.qlangtech.tis.plugin.iceberg.datax.hook;

import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.iceberg.datax.DataxIcebergWriter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * dump 任务执行后做快照维护
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2026-10-25
 **/
public class IcebergPostExecutor implements IRemoteTaskPostTrigger {

    private static final Logger logger = LoggerFactory.getLogger(IcebergPostExecutor.class);

    private final DataxIcebergWriter icebergWriter;
    private final EntityName entity;
    private final ISelectedTab tab;
    private final IExecChainContext execContext;

    public IcebergPostExecutor(IExecChainContext execContext, DataxIcebergWriter icebergWriter, EntityName entity,
                               ISelectedTab tab) {
        this.icebergWriter = Objects.requireNonNull(icebergWriter, "icebergWriter can not be null");
        this.entity = Objects.requireNonNull(entity, "entity can not be null");
        this.tab = Objects.requireNonNull(tab, "tab can not be null");
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
    }

    @Override
    public String getTaskName() {
        return "PostCommit_" + this.entity.getTabName();
    }

    @Override
    public void run() {
        if (this.icebergWriter.snapshotManagement == null) {
            return;
        }
        try {
            Catalog catalog = this.icebergWriter.catalog.getCatalog();
            TableIdentifier identifier = this.icebergWriter.catalog.createTableIdentifier(entity.getTabName());
            logger.info("start snapshot maintenance iceberg table:{}", identifier);
            this.icebergWriter.snapshotManagement.performMaintenance(catalog, identifier);
        } catch (Exception e) {
            throw new RuntimeException("table:" + this.entity.getFullName() + " snapshot maintenance faild", e);
        }
    }
}
