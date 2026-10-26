package com.qlangtech.tis.plugin.iceberg.datax;

import java.util.Map;

/**
 * 可以向Iceberg表属性（table properties）中追加配置的组件统一实现该接口。
 * <p>
 * Iceberg 1.12 已经没有 Schema.Builder 了，表级配置通过
 * {@code Catalog#buildTable(...).withProperties(...)} 落盘，因此在建表之前
 * 由各个组件把自己的配置写进同一个属性 Map 中。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public interface IcebergTablePropsSetter {

    /**
     * 向目标表属性中写入配置
     *
     * @param tableProps 表属性，key/value 都会以字符串形式写入表元数据
     */
    default void setTableProperties(Map<String, String> tableProps) {
        // 默认不追加任何配置
    }
}
