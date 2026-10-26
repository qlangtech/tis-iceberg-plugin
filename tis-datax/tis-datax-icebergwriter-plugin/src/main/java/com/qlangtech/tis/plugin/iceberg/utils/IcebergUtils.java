package com.qlangtech.tis.plugin.iceberg.utils;

import com.qlangtech.tis.plugin.iceberg.datax.IcebergColumn;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Iceberg 工具类
 * <p>
 * Iceberg 1.12 已经没有 {@code Schema.Builder}，schema 由
 * {@link Schema#Schema(List)} 直接构造（field id 由调用方按顺序分配）。
 */
public class IcebergUtils {

    /**
     * 由列定义创建 Iceberg Schema
     */
    public static Schema createSchema(List<IcebergColumn> cols) {
        AtomicInteger fieldId = new AtomicInteger(1);
        List<Types.NestedField> fields = cols.stream().map((col) -> {
            int id = fieldId.getAndIncrement();
            return col.isNullable() //
                    ? Types.NestedField.optional(id, col.getName(), col.getIcebergType(), col.getComment()) //
                    : Types.NestedField.required(id, col.getName(), col.getIcebergType(), col.getComment());
        }).toList();
        return new Schema(fields);
    }

    /**
     * 生成文件路径
     */
    public static String generateDataPath(String warehouse, String database, String table) {
        return String.format("%s/%s.db/%s", warehouse, database, table);
    }

    /**
     * 生成元数据路径
     */
    public static String generateMetadataPath(String dataPath) {
        return dataPath + "/metadata";
    }

    /**
     * 生成数据文件路径
     */
    public static String generateDataFilePath(String dataPath) {
        return dataPath + "/data";
    }
}
