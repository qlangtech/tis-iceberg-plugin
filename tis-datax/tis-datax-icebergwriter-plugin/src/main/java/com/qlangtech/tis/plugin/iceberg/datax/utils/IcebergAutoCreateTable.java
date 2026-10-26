package com.qlangtech.tis.plugin.iceberg.datax.utils;

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;

import java.util.Optional;

/**
 * Iceberg 自动建表：Iceberg 不走 DDL 语句，建表在
 * {@code com.qlangtech.tis.plugin.iceberg.datax.hook.IcebergPreExecutor} 中通过 Catalog API 完成
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2026-10-25
 **/
public class IcebergAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter,
                                                     TableMap tableMapper, Optional transformers) {
        throw new UnsupportedOperationException();
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {

        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.Iceberg;
        }
    }
}
