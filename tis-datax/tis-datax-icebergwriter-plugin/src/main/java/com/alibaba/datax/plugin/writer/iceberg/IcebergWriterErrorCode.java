package com.alibaba.datax.plugin.writer.iceberg;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/25
 */
public enum IcebergWriterErrorCode implements ErrorCode {
    ICEBERG_ERROR_DB("Iceberg Error DB", "您的参数配置错误."),
    ICEBERG_ERROR_TABLE("Iceberg Error Table", "您的参数配置错误."),
    ICEBERG_PARAM_LOST("Iceberg Param Lost", "您缺失了必须填写的参数值."),
    ICEBERG_WRITE_ERROR("Iceberg Write Error", "向 Iceberg 表写入数据时出现异常.");

    private final String code;
    private final String description;

    IcebergWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
