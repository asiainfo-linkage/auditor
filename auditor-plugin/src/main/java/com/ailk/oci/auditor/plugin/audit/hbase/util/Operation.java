package com.ailk.oci.auditor.plugin.audit.hbase.util;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午5:00
 * To change this template use File | Settings | File Templates.
 */
public interface Operation {
    String getName();

    public static enum Master implements Operation {
        CREATE_TABLE("create_table"),
        DELETE_TABLE("delete_table"),
        MODIFY_TABLE("modify_table"),
        ADD_COLUMN("add_column"),
        MODIFY_COLUMN("modify_column"),
        DELETE_COLUMN("delete_column"),
        ENABLE_TABLE("enable_table"),
        DISABLE_TABLE("disable_table"),
        MOVE("move"),
        SNAPSHOT("snapshot"),
        CLONE_SNAPSHOT("clone_snapshot"),
        RESTORE_SNAPSHOT("restore_snapshot"),
        DELETE_SNAPSHOT("delete_snapshot"),
        ASSIGN("assign"),
        UN_ASSIGN("un_assign"),
        BALANCE("balance"),
        BALANCE_SWITCH("balance_switch"),
        START_MASTER("start_master"),;
        private final String name;

        Master(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static enum Region implements Operation {
        FLUSH("flush"),
        SPLIT("split"),
        COMPACT("compact"),
        COMPACT_SELECTION("compact_selection"),
        GET_CLOSEST_ROW_BEFORE("get_closest_row_before"),
        GET("get"),
        EXISTS("exists"),
        PUT("put"),
        DELETE("delete"),
        CHECK_AND_PUT("check_and_put"),
        CHECK_AND_DELETE("check_and_delete"),
        INCREMENT_COLUMN_VALUE("increment_column_value"),
        APPEND("append"),
        INCREMENT("increment"),
        SCANNER_OPEN("scanner_open"),
        OPEN("open"),
        CLOSE("close"),
        BATCH_MUTATE("close"),
        SCANNER_NEXT("scanner_next"),
        SCANNER_FILTER_ROW("scanner_next"),
        SCANNER_CLOSE("scanner_close"),
        WAL_RESTORE("wal_restore"),
        BULK_LOAD_HFILE("bulk_load_hfile"),
        LOCK_ROW("lock_row"),
        UNLOCK_ROW("unlock_row"),;
        private final String name;

        Region(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

}
