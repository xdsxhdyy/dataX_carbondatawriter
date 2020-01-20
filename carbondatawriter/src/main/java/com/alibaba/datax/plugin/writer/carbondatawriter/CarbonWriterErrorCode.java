package com.alibaba.datax.plugin.writer.carbondatawriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @Author luoyu
 * @create 2020/1/15 14:51
 */
public enum CarbonWriterErrorCode implements ErrorCode {
    ILLEGAL_VALUES_ERROR("CarbonDataWriter-00", "读出的列和写入数据列不一致"),
    PATH_ERROR("CarbonDataWriter-03", "path 路径为空"),
    CLOUMN_ERROR("CarbonDataWriter-03", "cloumn 列为空"),;
    private final String code;
    private final String description;

    private CarbonWriterErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
