package com.ailk.oci.auditor.plugin.audit.hive;

import com.ailk.oci.auditor.pipeline.Pipeline;
import com.ailk.oci.auditor.pipeline.processor.in.filter.DuplicateFilter;
import com.ailk.oci.auditor.pipeline.processor.in.filter.FieldFilter;
import com.ailk.oci.auditor.pipeline.processor.out.EventUploadProcessor;
import com.ailk.oci.auditor.plugin.audit.UserInfo;
import com.ailk.oci.auditor.plugin.audit.hive.util.TargetType;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.Event;
import com.ailk.oci.auditor.protocol.event.pipeline.avro.HiveEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: shaoaq
 * Date: 13-11-3
 * Time: 下午7:02
 * To change this template use File | Settings | File Templates.
 */
public class HiveMetaStoreEventListener extends MetaStoreEventListener {
    private final static Logger LOG = LoggerFactory.getLogger(HiveMetaStoreEventListener.class);
    private Pipeline pipeline;

    public HiveMetaStoreEventListener(Configuration config) {
        super(config);
        pipeline = new Pipeline(new File(config.get("oci.auditor.com.ailk.oci.auditor.pipeline.hive.meta.config", "pipeline.properties")));
        pipeline.setActions(Pipeline.ActionCode.QUEUE_FULL, new QueueFullAction());
        pipeline.setInProcessors(new FieldFilter(), new DuplicateFilter());
        pipeline.setOutProcessors(new EventUploadProcessor());
        pipeline.updateConfig4Processors();
    }

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        in(tableEvent, tableEvent.getTable(), HiveOperation.CREATETABLE.getOperationName());
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        in(tableEvent, tableEvent.getTable(), HiveOperation.DROPTABLE.getOperationName());
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        in(tableEvent, tableEvent.getOldTable(), "ALTERTABLE");
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        in(partitionEvent, partitionEvent.getPartition(), HiveOperation.ALTERTABLE_ADDPARTS.getOperationName());
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        in(partitionEvent, partitionEvent.getPartition(), HiveOperation.ALTERTABLE_DROPPARTS.getOperationName());
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        in(partitionEvent, partitionEvent.getOldPartition(), "ALTERPART");
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        in(dbEvent, dbEvent.getDatabase(), HiveOperation.CREATEDATABASE.getOperationName());
    }


    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        in(dbEvent, dbEvent.getDatabase(), HiveOperation.DROPDATABASE.getOperationName());
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        in(partSetDoneEvent, partSetDoneEvent.getTable(), HiveOperation.LOAD.getOperationName());
        super.onLoadPartitionDone(partSetDoneEvent);
    }

    private void in(ListenerEvent listenerEvent, Database database, String operationName) {
        in(listenerEvent, TargetType.DATABASE, database.getName(), null, operationName);
    }

    private void in(ListenerEvent listenerEvent, Table table, String operation) {
        TargetType targetType = HiveExecHook.getTargetType(TableType.valueOf(table.getTableType()));
        in(listenerEvent, targetType, table.getDbName(), table.getTableName(), operation);
    }

    private void in(ListenerEvent listenerEvent, Partition table, String operation) {
        in(listenerEvent, TargetType.PARTITION, table.getDbName(), table.getTableName(), operation);
    }

    private void in(ListenerEvent listenerEvent, TargetType targetType, String db, String table, String operation) {
        boolean allowed = listenerEvent.getStatus();
        String ugiString = null;
        try {
            UserGroupInformation ugi = ShimLoader.getHadoopShims().getUGIForConf(getConf());
            ugiString = ugi.toString();
        } catch (LoginException e) {
            LOG.warn("failed to get user name", e);
        } catch (IOException e) {
            LOG.warn("failed to get user name", e);
        }
        UserInfo userInfo = new UserInfo(ugiString);
        String[] address = listenerEvent.getHandler().getHiveConf().getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL) ? new String[2] : parseIpAddress(HiveMetaStore.HMSHandler.getIpAddress());
        Event event = Event.newBuilder()
                .setService(pipeline.getServiceName())
                .setAllowed(allowed)
                .setLocalTime(System.currentTimeMillis())
                .setUser(userInfo.getUsername())
                .setImpersonator(userInfo.getImpersonator())
                .setHostName(address[0])
                .setHostAddress(address[1])
                .setOperation(operation)
                .setEvent(HiveEvent.newBuilder()
                        .setDatabase(db)
                        .setTable(table)
                        .setLocation(null)
                        .setTargetType(targetType == null ? null : targetType.name())
                        .setQuery(null)
                        .build()
                )
                .build();
        this.pipeline.in(event);
    }

    private static class QueueFullAction implements Pipeline.Action {
        @Override
        public void action(Pipeline pipeline, Pipeline.ActionCode code) {
            if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_DROP)) {
                LOG.warn("Hive audit logger [" + code.name() + "], dropped");
            } else if (pipeline.getQueueFullPolicy().equals(Pipeline.QUEUE_FULL_POLICY_SHUTDOWN)) {
                try {
                    pipeline.close();
                } catch (Exception e) {
                    LOG.warn("failed to close com.ailk.oci.auditor.pipeline", e);
                }
                ExitUtil.terminate(2, "Hive audit logger [" + code.name() + "], terminating ...");
            }
        }
    }

    protected String[] parseIpAddress(String raw) {
        if (raw != null) {
            int sep = raw.indexOf('/');
            if ((sep >= 0) && (sep < raw.length() - 1)) {
                return new String[]{raw.substring(0, sep), raw.substring(sep + 1)};
            }
        }
        return new String[2];
    }
}
