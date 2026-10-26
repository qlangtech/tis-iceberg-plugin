package com.qlangtech.tis.plugin.iceberg.utils;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Objects;

/**
 * Iceberg 快照管理
 * <p>
 * 注意：孤立文件清理（{@code removeOrphanFiles}）在 Iceberg 1.12 中位于
 * iceberg-actions 模块，未随本插件打包，故此处只做快照过期与 manifest 合并。
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2026/10/25
 */
public class IcebergSnapshot implements Describable<IcebergSnapshot> {

    /**
     * 小于该大小的 manifest 参与合并
     */
    private static final long REWRITE_MANIFEST_MAX_SIZE = 8 * 1024 * 1024;

    /**
     * 保留的快照数量
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer snapshotRetainCount = 10;

    /**
     * 过期快照的天数
     */
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer expireOlderThanDays = 7;

    /**
     * 执行快照维护操作
     *
     * @param catalog    Iceberg catalog
     * @param identifier 目标表
     */
    public void performMaintenance(Catalog catalog, TableIdentifier identifier) {
        if (!catalog.tableExists(identifier)) {
            return;
        }
        Table table = Objects.requireNonNull(catalog.loadTable(identifier), "table can not be null");

        // 过期旧快照
        if (this.expireOlderThanDays != null && this.expireOlderThanDays > 0) {
            long expireTimestamp = System.currentTimeMillis()
                    - (this.expireOlderThanDays * 24 * 60 * 60 * 1000L);
            table.expireSnapshots()
                    .expireOlderThan(expireTimestamp)
                    .retainLast(Objects.requireNonNullElse(this.snapshotRetainCount, 1))
                    .commit();
        }

        // 合并小文件 manifest
        table.rewriteManifests()
                .rewriteIf(manifest -> manifest.length() < REWRITE_MANIFEST_MAX_SIZE)
                .commit();
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IcebergSnapshot> {

        @Override
        public String getDisplayName() {
            return "Iceberg Snapshot Management";
        }
    }
}
