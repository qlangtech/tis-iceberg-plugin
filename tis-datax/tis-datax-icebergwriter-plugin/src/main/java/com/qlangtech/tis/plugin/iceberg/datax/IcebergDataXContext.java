package com.qlangtech.tis.plugin.iceberg.datax;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Iceberg DataX 上下文，充当 DataX job 配置模板（Velocity）的数据源。
 * <p>
 * 模板中通过 {@code $writer.tableName} / {@code $writer.icebergCols} 等取值，
 * 因此这里的方法名即为模板可用的属性名。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class IcebergDataXContext implements IDataxContext {

    private final TableMap tabMap;
    private final String dataXName;
    private final String databaseName;
    private final List<IcebergColumn> icebergCols;

    public IcebergDataXContext(String databaseName, TableMap tabMap, String dataXName,
                               Optional<RecordTransformerRules> transformerRules) {
        this.tabMap = Objects.requireNonNull(tabMap, "tabMap can not be null");
        this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName can not be null");

        List<IColMetaGetter> cols = tabMap.appendTransformerRuleCols(transformerRules);
        this.icebergCols = cols.stream().map(IcebergColumn::new).toList();
    }

    public String getDataxName() {
        return this.dataXName;
    }

    public String getSourceTableName() {
        return this.tabMap.getFrom();
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tabMap.getTo();
    }

    public TableMap getTableMap() {
        return this.tabMap;
    }

    /**
     * @return 目标表的完整标识（库名.表名）
     */
    public String getFullTableName() {
        return EntityName.create(this.databaseName, getTableName()).getFullName();
    }

    public List<IcebergColumn> getIcebergCols() {
        return this.icebergCols;
    }
}
