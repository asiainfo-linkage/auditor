package com.ailk.oci.auditor.plugin.audit.hbase.util;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HBaseEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-2
 * Time: 下午6:50
 * To change this template use File | Settings | File Templates.
 */
public class PipelineUtil {
    public static void in(Pipeline pipeline, Operation operation) {
        in(pipeline, operation, null, null, null);
    }

    public static void in(Pipeline pipeline, Operation operation, String table) {
        in(pipeline, operation, table, null, null);
    }

    public static void in(Pipeline pipeline, Operation operation, String tableName, String family) {
        in(pipeline, operation, tableName, family, null);
    }

    public static void in(Pipeline pipeline, Operation operation, String tableName, String family, String qualifier) {
        RequestContext requestContext = RequestContext.get();
        User user = null;
        InetAddress ipAddress = null;
        if (requestContext != null) {
            user = requestContext.getUser();
            ipAddress = requestContext.getRemoteAddress();
        }
        UserInfo userInfo = new UserInfo(user != null ? user.toString() : null);

        Event event = Event.newBuilder()
                .setAllowed(true)
                .setService(pipeline.getServiceName())
                .setUser(userInfo.getUsername())
                .setImpersonator(userInfo.getImpersonator())
                .setHostName(ipAddress != null ? ipAddress.getHostName() : null)
                .setHostAddress(ipAddress != null ? ipAddress.getHostAddress() : null)
                .setLocalTime(System.currentTimeMillis())
                .setOperation(operation.getName())
                .setEvent(HBaseEvent.newBuilder()
                        .setTable(tableName)
                        .setFamily(family)
                        .setQualifier(qualifier)
                        .build())
                .build();
        pipeline.in(event);
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Get get) {
        inWithNavigableSet(pipeline, operation, table, get.getFamilyMap());
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Scan scan) {
        inWithNavigableSet(pipeline, operation, table, scan.getFamilyMap());
    }

    private static void inWithNavigableSet(Pipeline pipeline, Operation operation, String table, Map<byte[], NavigableSet<byte[]>> familyMap) {
        if (familyMap == null) {
            in(pipeline, operation, table);
        } else {
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
                NavigableSet<byte[]> navigableSet = entry.getValue();
                if (navigableSet == null) {
                    in(pipeline, operation, table, Bytes.toString(entry.getKey()));
                } else {
                    for (byte[] qualifier : navigableSet) {
                        in(pipeline, operation, table, Bytes.toString(entry.getKey()), Bytes.toString(qualifier));
                    }
                }
            }
        }
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Increment append) {
        inWithNavigableMap(pipeline, operation, table, append.getFamilyMap());
    }

    private static void inWithNavigableMap(Pipeline pipeline, Operation operation, String table, Map<byte[], NavigableMap<byte[], Long>> familyMap) {
        if (familyMap == null) {
            in(pipeline, operation, table);
        } else {
            for (Map.Entry<byte[], NavigableMap<byte[], Long>> entry : familyMap.entrySet()) {
                NavigableMap<byte[], Long> navigableMap = entry.getValue();
                if (navigableMap == null) {
                    in(pipeline, operation, table, Bytes.toString(entry.getKey()));
                } else {
                    for (byte[] qualifier : navigableMap.keySet()) {
                        in(pipeline, operation, table, Bytes.toString(entry.getKey()), Bytes.toString(qualifier));
                    }
                }
            }
        }
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Put put) {
        inWithKeyValueList(pipeline, operation, table, put.getFamilyMap());
    }

    private static void inWithKeyValueList(Pipeline pipeline, Operation operation, String table, Map<byte[], List<KeyValue>> familyMap) {
        if (familyMap == null) {
            in(pipeline, operation, table);
        } else {
            for (Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()) {
                List<KeyValue> keyValues = entry.getValue();
                if (keyValues == null) {
                    in(pipeline, operation, table, Bytes.toString(entry.getKey()));
                } else {
                    for (KeyValue keyValue : keyValues) {
                        in(pipeline, operation, table, Bytes.toString(entry.getKey()), Bytes.toString(keyValue.getQualifier()));
                    }
                }
            }
        }
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Delete delete) {
        inWithKeyValueList(pipeline, operation, table, delete.getFamilyMap());
    }

    public static void in(Pipeline pipeline, Operation operation, String table, Append append) {
        inWithKeyValueList(pipeline, operation, table, append.getFamilyMap());
    }
}
