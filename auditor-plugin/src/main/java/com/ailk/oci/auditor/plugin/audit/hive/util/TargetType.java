package com.ailk.oci.auditor.plugin.audit.hive.util;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-3
 * Time: 下午6:46
 * To change this template use File | Settings | File Templates.
 */
public enum TargetType {
    DATABASE, TABLE, PARTITION, DUMMYPARTITION, DFS_DIR, LOCAL_DIR, UDF, MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW, INDEX_TABLE;
}
