package com.qlangtech.tis.plugin.iceberg.datax;

import com.alibaba.datax.common.element.Column;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.function.Function;

/**
 * Iceberg 列定义：负责 TIS 类型到 Iceberg 类型的映射，以及 DataX Column 到
 * Iceberg {@code GenericRecord} 字段值的转换。
 * <p>
 * 字段值的 Java 类型必须与 iceberg-data 的写入实现一致：
 * DATE→{@link LocalDate}、TIME→{@link LocalTime}、TIMESTAMP→{@link LocalDateTime}、
 * BINARY→{@code byte[]}、DECIMAL→{@link java.math.BigDecimal}。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class IcebergColumn {

    private final String name;
    private final boolean nullable;
    private final String comment;
    private final Type type;
    private final Function<Column, Object> valGetter;

    public IcebergColumn(IColMetaGetter colMeta) {
        this.name = colMeta.getName();
        this.nullable = colMeta instanceof com.qlangtech.tis.plugin.ds.CMeta cmeta && cmeta.isNullable();
        this.comment = colMeta instanceof com.qlangtech.tis.plugin.ds.CMeta cmeta ? cmeta.getComment() : null;
        final Pair typeAndGetter = colMeta.getType().accept(new TypeVisitor<Pair>() {
            @Override
            public Pair intType(DataType type) {
                return new Pair(Types.IntegerType.get(), (col) -> col.asLong().intValue());
            }

            @Override
            public Pair bigInt(DataType type) {
                return new Pair(Types.LongType.get(), Column::asLong);
            }

            @Override
            public Pair floatType(DataType type) {
                return new Pair(Types.FloatType.get(), (col) -> col.asDouble().floatValue());
            }

            @Override
            public Pair doubleType(DataType type) {
                return new Pair(Types.DoubleType.get(), Column::asDouble);
            }

            @Override
            public Pair decimalType(DataType type) {
                int[] precisionAndScale = DataTypeMeta.normalizeDecimalPrecisionAndScale(type);
                return new Pair(Types.DecimalType.of(precisionAndScale[0], precisionAndScale[1]),
                        Column::asBigDecimal);
            }

            @Override
            public Pair dateType(DataType type) {
                return new Pair(Types.DateType.get(),
                        (col) -> new java.sql.Date(col.asDate().getTime()).toLocalDate());
            }

            @Override
            public Pair timeType(DataType type) {
                return new Pair(Types.TimeType.get(),
                        (col) -> new java.sql.Time(col.asDate().getTime()).toLocalTime());
            }

            @Override
            public Pair timestampType(DataType type) {
                // 不能直接 asLong()：源端列是字符串时间时只有 asDate() 能解析
                return new Pair(Types.TimestampType.withoutZone(), (col) -> LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(col.asDate().getTime()), ZoneId.systemDefault()));
            }

            @Override
            public Pair bitType(DataType type) {
                return new Pair(Types.BooleanType.get(), (col) -> col.asLong() > 0);
            }

            @Override
            public Pair blobType(DataType type) {
                return new Pair(Types.BinaryType.get(), Column::asBytes);
            }

            @Override
            public Pair varcharType(DataType type) {
                return new Pair(Types.StringType.get(), Column::asString);
            }

            @Override
            public Pair tinyIntType(DataType dataType) {
                return new Pair(Types.IntegerType.get(), (col) -> col.asLong().intValue());
            }

            @Override
            public Pair smallIntType(DataType dataType) {
                return new Pair(Types.IntegerType.get(), (col) -> col.asLong().intValue());
            }

            @Override
            public Pair boolType(DataType dataType) {
                return new Pair(Types.BooleanType.get(), Column::asBoolean);
            }
        });
        this.type = typeAndGetter.type;
        this.valGetter = typeAndGetter.valGetter;
    }

    public String getName() {
        return this.name;
    }

    public Type getIcebergType() {
        return this.type;
    }

    /**
     * 供 Velocity 模板使用，输出 Iceberg 类型的 SQL 字符串表达
     */
    public String getType() {
        return this.type.toString();
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public String getComment() {
        return this.comment;
    }

    /**
     * 将 DataX 的列值转换成 Iceberg GenericRecord 可接受的 Java 对象
     */
    public Object getFieldVal(Column column) {
        Object rawData = column.getRawData();
        if (rawData == null) {
            return null;
        }
        try {
            return this.valGetter.apply(column);
        } catch (Exception e) {
            throw new RuntimeException("colName:" + this.name + ",icebergType:" + this.type, e);
        }
    }

    private static class Pair {
        private final Type type;
        private final Function<Column, Object> valGetter;

        private Pair(Type type, Function<Column, Object> valGetter) {
            this.type = type;
            this.valGetter = valGetter;
        }
    }
}
